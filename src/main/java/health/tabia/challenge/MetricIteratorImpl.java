package health.tabia.challenge;

import java.util.Comparator;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

public class MetricIteratorImpl implements MetricIterator {

    private int currentIndex;
    private Set<Metric> metrics;
    private List<Metric> orderedMetrics;

    public MetricIteratorImpl(Set<Metric> metrics, long from, long to) {
        this.currentIndex = -1;
        this.metrics = metrics;

        /*
         * Making a copy of the metrics that are within the range
         * and sorting them we make sure that the iterator is
         * thread safe since we are not iterating over the original
         * set. Because of this, the iterator may not reflect the
         * changes made to the original set after the iterator was
         * created. This is a tradeoff for thread safety.
         *
         * Another solution would be to iterate over the original
         * set and filter the metrics that are within the range
         * and sort them. This would make the iterator reflect the
         * changes made to the original set but would complicate the
         * implementation of the iterator.
         * */

        this.orderedMetrics = metrics.stream()
                .filter(metric -> metric.getTimestamp() >= from && metric.getTimestamp() <= to)
                .collect(Collectors.toList());
        this.orderedMetrics.sort(Comparator.comparing(Metric::getTimestamp));

    }

    @Override
    public boolean moveNext() {
        if (currentIndex >= orderedMetrics.size() - 1) {
            currentIndex++;
            return false;
        }
        currentIndex++;
        return true;
    }

    @Override
    public Metric current() {
        if (currentIndex < 0 || currentIndex >= orderedMetrics.size()) {
            throw new IllegalStateException();
        }
        return orderedMetrics.get(currentIndex);
    }

    @Override
    public void remove() {
        metrics.remove(current());
    }

    @Override
    public void close() throws Exception {

    }
}
