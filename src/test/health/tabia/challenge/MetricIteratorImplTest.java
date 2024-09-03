package health.tabia.challenge;


import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.*;

class MetricIteratorImplTest {

    static MetricStoreImpl metricStore;
    static List<String> uuids;
    static List<Long> timestamps;
    static int metricsToInsert = 10000;

    @BeforeAll
    static void SetUp() {
        metricStore = new MetricStoreImpl();

        int repeatedMetrics = 10;
        uuids = new ArrayList<>();
        timestamps = new ArrayList<>();

        for (int i = 0; i < metricsToInsert; i++) {
            String name = UUID.randomUUID().toString();
            uuids.add(name);
            long ts = System.currentTimeMillis();
            timestamps.add(ts);
            for (int j = 0; j < repeatedMetrics; j++) {
                metricStore.insert(new Metric(name, ts));
            }
        }
    }

    @Test
    void moveNextReturnsFalseWhenCalledAtEnd() {
        Set<Metric> metricSet = metricStore.getMetrics().get(uuids.get(0));
        MetricIterator iterator = new MetricIteratorImpl(metricSet, timestamps.get(0), timestamps.get(metricsToInsert - 1));
        for (int i = 0; i < 10; i++) {
            iterator.moveNext();
        }
        assertFalse(iterator.moveNext());
    }

    @Test
    void moveNextReturnsTrueWhenCalledAtStart() {
        Set<Metric> metricSet = metricStore.getMetrics().get(uuids.get(0));
        MetricIterator iterator = new MetricIteratorImpl(metricSet, timestamps.get(0), timestamps.get(metricsToInsert - 1));
        assertTrue(iterator.moveNext());
    }


    @Test
    void currentThrowsExceptionWhenCalledBeforeMoveNext() {
        Set<Metric> metricSet = metricStore.getMetrics().get(uuids.get(0));
        MetricIterator iterator = new MetricIteratorImpl(metricSet, timestamps.get(0), timestamps.get(metricsToInsert - 1));
        assertThrows(IllegalStateException.class, iterator::current);
    }

    @Test
    void currentThrowsAfterMoveNextReturnsFalse() {
        Set<Metric> metricSet = metricStore.getMetrics().get(uuids.get(0));
        MetricIterator iterator = new MetricIteratorImpl(metricSet, timestamps.get(0), timestamps.get(metricsToInsert - 1));
        for (int i = 0; i < 10; i++) {
            iterator.moveNext();
        }
        assertFalse(iterator.moveNext());
        assertThrows(IllegalStateException.class, iterator::current);
        try {
            iterator.close();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    @Test
    void canRemoveWithIterator() {
        String metricName = uuids.get(0);
        MetricIterator iterator = new MetricIteratorImpl(metricStore.getMetrics().get(metricName), timestamps.get(0), timestamps.get(metricsToInsert - 1));
        for (int i = 0; i < 5; i++) {
            iterator.moveNext();
        }
        iterator.remove();
        iterator.moveNext();
        iterator.remove();
        assertEquals(8, metricStore.getMetrics().get(metricName).size());
    }

}