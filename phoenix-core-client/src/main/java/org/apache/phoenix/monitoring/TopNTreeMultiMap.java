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

import org.apache.phoenix.thirdparty.com.google.common.base.Supplier;
import org.apache.phoenix.thirdparty.com.google.common.collect.ForwardingListMultimap;
import org.apache.phoenix.thirdparty.com.google.common.collect.ListMultimap;
import org.apache.phoenix.thirdparty.com.google.common.collect.Multimap;
import org.apache.phoenix.thirdparty.com.google.common.collect.MultimapBuilder;

/**
 * A bounded "Top-N" ListMultimap that maintains a fixed maximum number of entries (N = maxSize).
 * Keys are sorted using the provided comparator, and values for each key are stored in an
 * ArrayList.
 * <p>
 * When adding a new entry to a full map (size >= maxSize), the implementation evicts the last value
 * associated with the highest key to make room, but only if the new entry's key is lower than the
 * current highest key. If the new key is greater than or equal to the highest key, the new entry is
 * rejected.
 * @param <K> the type of keys
 * @param <V> the type of values
 */
public class TopNTreeMultiMap<K, V> extends ForwardingListMultimap<K, V> {

  private final ListMultimap<K, V> delegate;
  private final int maxSize;
  private final Comparator<? super K> keyComparator;

  public static <K, V> TopNTreeMultiMap<K, V> getInstance(int maxSize,
    Comparator<? super K> keyComparator) {
    if (maxSize <= 0) {
      throw new IllegalArgumentException("maxSize must be positive");
    }
    return new TopNTreeMultiMap<>(maxSize, keyComparator);
  }

  /**
   * Creates a new bounded multimap with keys sorted according to the provided comparator.
   * @param maxSize       the maximum total number of key-value pairs this map can hold (must be
   *                      positive)
   * @param keyComparator the comparator used to order keys in this multimap
   */
  private TopNTreeMultiMap(int maxSize, Comparator<? super K> keyComparator) {
    this.maxSize = maxSize;
    this.keyComparator = keyComparator;
    // Use a TreeMap for sorted keys, with ArrayList for storing multiple values per key
    this.delegate = MultimapBuilder.treeKeys(keyComparator).arrayListValues().build();
  }

  @Override
  protected ListMultimap<K, V> delegate() {
    return delegate;
  }

  /**
   * Returns the comparator used to order the keys in this multimap.
   * @return the key comparator
   */
  public Comparator<? super K> keyComparator() {
    return keyComparator;
  }

  /**
   * Returns the backing key set as a SortedSet view (not a copy).
   * <p>
   * Time Complexity: O(1) - Returns a live view of the keys backed by the internal TreeMap.
   * <p>
   * The returned SortedSet supports efficient operations:
   * <ul>
   * <li>isEmpty(): O(1)</li>
   * <li>last(): O(log k) where k = number of unique keys</li>
   * </ul>
   * @return a sorted view of the keys in this multimap
   */
  private SortedSet<K> sortedKeySet() {
    // This cast is safe according to MultimapBuilder.treeKeys() documentation
    return (SortedSet<K>) delegate.keySet();
  }

  /**
   * Adds a key-value pair to this multimap, potentially evicting an existing entry if the map is
   * full.
   * <p>
   * If the map is at capacity and the new key is smaller than the current highest key, the last
   * value associated with the highest key is evicted to make room for the new entry. If the new key
   * is greater than or equal to the highest key, the new entry is rejected.
   * <p>
   * Time Complexity:
   * <ul>
   * <li>When not full: O(log k) for TreeMap insertion, where k = number of unique keys</li>
   * <li>When full: O(log k) for finding last key + O(1) for list removal + O(log k) for insertion =
   * O(log k) overall</li>
   * </ul>
   * @param key           the key to add
   * @param valueSupplier a supplier that provides the value to add
   * @return true if the multimap was modified (entry was added), false otherwise
   */
  public boolean put(@Nullable K key, Supplier<V> valueSupplier) {
    // Fast path: if the map has not reached capacity, add the entry directly
    // Note: delegate.size() is O(1), delegate.put() is O(log k) for TreeMap
    if (delegate.size() < maxSize) {
      return delegate.put(key, valueSupplier.get());
    }

    // Map is at capacity. Identify the highest key to determine if eviction is needed
    SortedSet<K> keySet = sortedKeySet();
    K lastKey = keySet.last();

    // Compare the new key to the current highest key
    int comp = keyComparator.compare(key, lastKey);
    if (comp >= 0) {
      // Reject: new key is greater than or equal to the highest key
      return false;
    }

    // New key is smaller than the highest key. Evict the last value from the highest key.
    List<V> lastValues = delegate.get(lastKey);

    // Remove the last value associated with the highest key.
    // Important: The list returned by get() is a live view, so this removal:
    // 1. Automatically decrements delegate.size()
    // 2. Removes the key entirely if this was its only value
    lastValues.remove(lastValues.size() - 1);

    // Insert the new entry (this increments size back to maxSize)
    return delegate.put(key, valueSupplier.get());
  }

  /**
   * Adds a key-value pair to this multimap, delegating to the Supplier-based put method. See
   * {@link #put(Object, Supplier)} for details on the eviction behavior.
   * @param key   the key to add
   * @param value the value to add
   * @return true if the multimap was modified (entry was added), false otherwise
   */
  @Override
  public boolean put(@Nullable K key, @Nullable V value) {
    return put(key, () -> value);
  }

  /**
   * Adds all values from the provided iterable to the specified key, applying the bounded put logic
   * for each value individually.
   * @param key    the key to associate with the values
   * @param values the iterable of values to add
   * @return true if the multimap was modified by this operation, false otherwise
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
   * Adds all entries from the specified multimap to this multimap, applying the bounded put logic
   * for each entry individually.
   * @param multimap the multimap containing entries to add
   * @return true if this multimap was modified by this operation, false otherwise
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
