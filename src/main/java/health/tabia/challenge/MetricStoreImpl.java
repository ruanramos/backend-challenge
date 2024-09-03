package health.tabia.challenge;

import com.google.common.annotations.VisibleForTesting;

import java.util.Collections;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

public class MetricStoreImpl implements MetricStore {

    /*
    * The choice for a hashmap is to make sure we can quickly
    * write to the store, making it O(1) for insertions and
    * O(1) for lookups since we are using a set derived from
    * a concurrent hashmap.
    * */

    ConcurrentHashMap<String, Set<Metric>> metrics = new ConcurrentHashMap<>();

    /*
     * I did not synchonize the insert method since I'm using
     * a concurrent hashmap which is thread safe using the right
     * methods. putIfAbsent runs atomically. The only thing that
     * could be a problem is the add method on the set, but since
     * the set is a concurrent set, it should be thread safe.
     * I also did not synchronize the removeAll method since it
     * is using the ConcurrentHashMap remove method which is thread
     * safe.
     * */

    public void insert(Metric metric) {
        metrics.putIfAbsent(metric.getName(), Collections.newSetFromMap(new ConcurrentHashMap<>()));
        metrics.get(metric.getName()).add(metric);
    }

    public void removeAll(String name) {
        metrics.remove(name);
    }

    public MetricIterator query(String name, long from, long to) {
        return new MetricIteratorImpl(metrics.get(name), from, to);
    }

    @VisibleForTesting
    public Map<String, Set<Metric>> getMetrics() {
        return metrics;
    }

    @VisibleForTesting
    public Set<Metric> getMetrics(String name) {
        return metrics.get(name);
    }
}
