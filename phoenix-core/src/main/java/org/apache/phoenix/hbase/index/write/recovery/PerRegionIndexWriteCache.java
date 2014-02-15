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
package org.apache.phoenix.hbase.index.write.recovery;

import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.hbase.client.Mutation;
import org.apache.hadoop.hbase.regionserver.HRegion;

import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.Multimap;

import org.apache.phoenix.hbase.index.table.HTableInterfaceReference;


public class PerRegionIndexWriteCache {

  private Map<HRegion, Multimap<HTableInterfaceReference, Mutation>> cache =
      new HashMap<HRegion, Multimap<HTableInterfaceReference, Mutation>>();


  /**
   * Get the edits for the current region. Removes the edits from the cache. To add them back, call
   * {@link #addEdits(HRegion, HTableInterfaceReference, Collection)}.
   * @param region
   * @return Get the edits for the given region. Returns <tt>null</tt> if there are no pending edits
   *         for the region
   */
  public Multimap<HTableInterfaceReference, Mutation> getEdits(HRegion region) {
    return cache.remove(region);
  }

  /**
   * @param region
   * @param table
   * @param collection
   */
  public void addEdits(HRegion region, HTableInterfaceReference table,
      Collection<Mutation> collection) {
    Multimap<HTableInterfaceReference, Mutation> edits = cache.get(region);
    if (edits == null) {
      edits = ArrayListMultimap.<HTableInterfaceReference, Mutation> create();
      cache.put(region, edits);
    }
    edits.putAll(table, collection);
  }
}