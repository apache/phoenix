/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.hbase.regionserver;

import java.util.Comparator;
import java.util.concurrent.ConcurrentSkipListMap;

import org.apache.hadoop.hbase.KeyValue;

/**
 * Like a {@link KeyValueSkipListSet}, but also exposes useful, atomic methods (e.g.
 * {@link #putIfAbsent(KeyValue)}).
 */
public class IndexKeyValueSkipListSet extends KeyValueSkipListSet {

  // this is annoying that we need to keep this extra pointer around here, but its pretty minimal
  // and means we don't need to change the HBase code.
  private ConcurrentSkipListMap<KeyValue, KeyValue> delegate;

  /**
   * Create a new {@link IndexKeyValueSkipListSet} based on the passed comparator.
   * @param comparator to use when comparing keyvalues. It is used both to determine sort order as
   *          well as object equality in the map.
   * @return a map that uses the passed comparator
   */
  public static IndexKeyValueSkipListSet create(Comparator<KeyValue> comparator) {
    ConcurrentSkipListMap<KeyValue, KeyValue> delegate =
        new ConcurrentSkipListMap<KeyValue, KeyValue>(comparator);
    IndexKeyValueSkipListSet ret = new IndexKeyValueSkipListSet(delegate);
    return ret;
  }

  /**
   * @param delegate map to which to delegate all calls
   */
  public IndexKeyValueSkipListSet(ConcurrentSkipListMap<KeyValue, KeyValue> delegate) {
    super(delegate);
    this.delegate = delegate;
  }

  /**
   * Add the passed {@link KeyValue} to the set, only if one is not already set. This is equivalent
   * to
   * <pre>
   * if (!set.containsKey(key))
   *   return set.put(key);
   * else
   *  return map.set(key);
   * </pre>
   * except that the action is performed atomically.
   * @param kv {@link KeyValue} to add
   * @return the previous value associated with the specified key, or <tt>null</tt> if there was no
   *         previously stored key
   * @throws ClassCastException if the specified key cannot be compared with the keys currently in
   *           the map
   * @throws NullPointerException if the specified key is null
   */
  public KeyValue putIfAbsent(KeyValue kv) {
    return this.delegate.putIfAbsent(kv, kv);
  }
}