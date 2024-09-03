package health.tabia.challenge;

import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertEquals;

class MetricStoreImplTest {

    private static List<Long> timestampsMock;
    int executorTimeoutInSeconds = 10;

    @BeforeAll
    static void Setup() {
        timestampsMock = new ArrayList<>();
        for (long i = 1000000; i < 1000100; i++) {
            timestampsMock.add(i);
        }
    }

    @Test
    void insert() {
        MetricStoreImpl metricStore = new MetricStoreImpl();
        metricStore.insert(new Metric("metric_name", System.currentTimeMillis()));
        assertEquals(1, metricStore.getMetrics().entrySet().size());
        assertEquals(1, metricStore.getMetrics().get("metric_name").size());
    }

    @Test
    void removeAll() {
        MetricStoreImpl metricStore = new MetricStoreImpl();
        int expectedSetSize = 0;

        for (int i = 0; i < 10; i++) {
            metricStore.insert(new Metric("metric_name", System.currentTimeMillis()));
        }
        metricStore.removeAll("metric_name");
        assertEquals(expectedSetSize, metricStore.getMetrics().entrySet().size());
    }

    @Test
    void query() {
        MetricStoreImpl metricStore = new MetricStoreImpl();
        int expectedIteratorCount = 50;

        for (int i = 0; i < 100; i++) {
            long timestamp = timestampsMock.get(i);
            metricStore.insert(new Metric("metric_name", timestamp));
        }
        MetricIterator iterator = metricStore.query("metric_name", 0, 1000049);
        int count = 0;
        while (iterator.moveNext()) {
            count++;
        }
        assertEquals(expectedIteratorCount, count);
    }

    @Test
    void CanInsertSameMetricConcurrently() {
        MetricStoreImpl metricStore = new MetricStoreImpl();
        ExecutorService executor = Executors.newFixedThreadPool(10);

        int metricsToInsert = 100000;
        int expectedSetSize = 1;

        for (int i = 0; i < metricsToInsert; i++) {
            MyInsertRunnable insertRunnable = new MyInsertRunnable(metricStore, "metric_name");
            executor.execute(insertRunnable);
        }

        executor.shutdown();
        try {
            executor.awaitTermination(executorTimeoutInSeconds, TimeUnit.SECONDS);

            assertEquals(expectedSetSize, metricStore.getMetrics().entrySet().size());
            assertEquals(metricsToInsert, metricStore.getMetrics().get("metric_name").size());
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
    }

    @Test
    void CanInsertDifferentMetricsConcurrently() {
        MetricStoreImpl metricStore = new MetricStoreImpl();
        ExecutorService executor = Executors.newFixedThreadPool(10);
        int metricsToInsert = 10000;
        int repeatedMetrics = 10;

        for (int i = 0; i < metricsToInsert; i++) {
            MyInsertRunnable insertRunnable = new MyInsertRunnable(metricStore, UUID.randomUUID().toString());
            for (int j = 0; j < repeatedMetrics; j++) {
                executor.execute(insertRunnable);
            }
        }

        executor.shutdown();
        try {
            executor.awaitTermination(executorTimeoutInSeconds, java.util.concurrent.TimeUnit.SECONDS);

            assertEquals(metricsToInsert, metricStore.getMetrics().entrySet().size());
            assertEquals(repeatedMetrics, metricStore.getMetrics().entrySet().stream().findFirst().get().getValue().size());
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
    }

    @Test
    void CanRemoveAllMetricsConcurrently() {
        MetricStoreImpl metricStore = new MetricStoreImpl();
        ExecutorService executor = Executors.newFixedThreadPool(10);
        int metricsToInsert = 10000;
        int repeatedMetrics = 10;
        List<String> uuids = new ArrayList<>();

        for (int i = 0; i < metricsToInsert; i++) {
            String name = UUID.randomUUID().toString();
            uuids.add(name);
            for (int j = 0; j < repeatedMetrics; j++) {
                metricStore.insert(new Metric(name, System.currentTimeMillis()));
            }
        }

        for (String uuid : uuids) {
            MyRemoveAllRunnable removeAllRunnable = new MyRemoveAllRunnable(metricStore, uuid);
            executor.execute(removeAllRunnable);
        }

        executor.shutdown();
        try {
            executor.awaitTermination(executorTimeoutInSeconds, java.util.concurrent.TimeUnit.SECONDS);

            assertEquals(0, metricStore.getMetrics().entrySet().size());
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
    }


    private static class MyInsertRunnable implements Runnable {
        private final MetricStoreImpl metricStore;
        private final String name;

        public MyInsertRunnable(MetricStoreImpl metricStore, String name) {
            this.metricStore = metricStore;
            this.name = name;
        }

        @Override
        public void run() {
            metricStore.insert(new Metric(name, System.currentTimeMillis()));
        }
    }

    private static class MyRemoveAllRunnable implements Runnable {
        private final MetricStoreImpl metricStore;
        private final String name;

        public MyRemoveAllRunnable(MetricStoreImpl metricStore, String name) {
            this.metricStore = metricStore;
            this.name = name;
        }

        @Override
        public void run() {
            try {
                metricStore.removeAll(name);
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        }
    }
}