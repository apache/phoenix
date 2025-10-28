/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.phoenix.monitoring;

import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.SortedSet;
import javax.annotation.Nullable;

import org.apache.phoenix.thirdparty.com.google.common.collect.ForwardingListMultimap;
import org.apache.phoenix.thirdparty.com.google.common.collect.ListMultimap;
import org.apache.phoenix.thirdparty.com.google.common.collect.Multimap;
import org.apache.phoenix.thirdparty.com.google.common.collect.MultimapBuilder;

/**
 * A "Top-N" ListMultimap that holds a fixed number of total entries (N = maxSize). It sorts keys
 * using the provided comparator, and stores values for each key in an ArrayList (preserving
 * insertion order). When a new element is added and the map is full (size >= maxSize), the
 * "highest" entry (the last value from the highest key) is evicted to make room, but only if the
 * new entry's key is lower than the highest key.
 * @param <K> the type of keys
 * @param <V> the type of values
 */
public class TopNTreeMultiMap<K, V> extends ForwardingListMultimap<K, V> {

  private final ListMultimap<K, V> delegate;
  private final int maxSize;
  private final Comparator<? super K> keyComparator;

  /**
   * Creates a new bounded multimap.
   * @param maxSize       The total number of key-value pairs the map can hold. Must be > 0.
   * @param keyComparator The comparator to sort the keys.
   */
  public TopNTreeMultiMap(int maxSize, Comparator<? super K> keyComparator) {
    if (maxSize <= 0) {
      throw new IllegalArgumentException("maxSize must be positive");
    }
    this.maxSize = maxSize;
    this.keyComparator = keyComparator;
    this.delegate = MultimapBuilder.treeKeys(keyComparator).arrayListValues() // Values are in an
                                                                              // ArrayList
      .build();
  }

  @Override
  protected ListMultimap<K, V> delegate() {
    return delegate;
  }

  /**
   * Returns the comparator used to order the keys in this multimap.
   */
  public Comparator<? super K> keyComparator() {
    return keyComparator;
  }

  /**
   * Gets the backing key set as a SortedSet. Time Complexity: O(1) - Returns a view of the keys,
   * not a copy. The returned SortedSet is backed by the TreeMap used internally by
   * MultimapBuilder.treeKeys(), which provides: - isEmpty(): O(1) - last(): O(log k) where k =
   * number of unique keys
   */
  private SortedSet<K> sortedKeySet() {
    // This cast is safe as per MultimapBuilder.treeKeys() documentation
    return (SortedSet<K>) delegate.keySet();
  }

  /**
   * Adds the key-value pair, evicting the highest entry if the map is full and the new entry's key
   * is smaller than the highest key. Time Complexity: - When not full: O(log k) for TreeMap
   * insertion, where k = number of unique keys - When full: O(log k) for finding last key + O(1)
   * for list removal + O(log k) for insertion = O(log k) overall Note: This is more efficient than
   * a heap-based approach which would be O(n log n) for maintaining sorted order across all
   * entries.
   */
  @Override
  public boolean put(@Nullable K key, @Nullable V value) {
    // Fast path: if the map is not full, just add it
    // delegate.size() is O(1), delegate.put() is O(log k) for TreeMap
    if (delegate.size() < maxSize) {
      return delegate.put(key, value);
    }

    // The map is full. Find the highest key to potentially evict
    SortedSet<K> keySet = sortedKeySet(); // O(1) - returns a view
    if (keySet.isEmpty()) {
      // Map is full but has no keys? Should not happen
      return false;
    }

    K lastKey = keySet.last(); // O(log k) where k = number of unique keys

    // Compare new key to the highest key
    int comp = keyComparator.compare(key, lastKey);

    if (comp >= 0) {
      // New key is higher or equal to the highest key. Reject.
      return false;
    }

    // New key is smaller. Evict the last value from the highest key.
    List<V> lastValues = delegate.get(lastKey);

    if (lastValues == null || lastValues.isEmpty()) {
      // Should not happen if Guava's contract is maintained
      return false;
    }

    // Evict the last value of the highest key
    lastValues.remove(lastValues.size() - 1);

    // Add the new value
    return delegate.put(key, value);
  }

  /**
   * Iterates over the values, applying the bounded 'put' logic for each.
   */
  @Override
  public boolean putAll(@Nullable K key, Iterable<? extends V> values) {
    boolean changed = false;
    for (V value : values) {
      if (put(key, value)) {
        changed = true;
      }
    }
    return changed;
  }

  /**
   * Iterates over the multimap, applying the bounded 'put' logic for each entry.
   */
  @Override
  public boolean putAll(Multimap<? extends K, ? extends V> multimap) {
    boolean changed = false;
    for (Map.Entry<? extends K, ? extends V> entry : multimap.entries()) {
      if (put(entry.getKey(), entry.getValue())) {
        changed = true;
      }
    }
    return changed;
  }
}
