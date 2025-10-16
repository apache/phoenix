package org.apache.phoenix.monitoring;

import java.util.Comparator;
import java.util.TreeMap;

public class TopNTreeMap<K, V> extends TreeMap<K, V> {
    
    private final int limit;

    public TopNTreeMap(int limit, Comparator<? super K> comparator) {
        super(comparator);
        if (limit <= 0) {
            throw new IllegalArgumentException("Limit must be greater than 0");
        }
        this.limit = limit;
    }

    @Override
    public V put(K key, V value) {
        if (this.containsKey(key)) {
            return super.put(key, value);
        }
        if (size() >= limit) {
            K firstKey = this.firstKey();
            Comparator<? super K> comparator = this.comparator();
            if (comparator.compare(key, firstKey) <= 0) {
                return null;
            }
            super.remove(firstKey);
        }
        return super.put(key, value);
    }
}
