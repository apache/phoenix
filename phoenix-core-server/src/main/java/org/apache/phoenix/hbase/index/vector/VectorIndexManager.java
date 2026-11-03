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
package org.apache.phoenix.hbase.index.vector;

import java.io.Closeable;
import java.io.IOException;
import java.util.function.LongSupplier;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.coprocessor.RegionCoprocessorEnvironment;

/**
 * Maintains the graph of one vector index for one data table region, and manages the life cycle of
 * the graph.
 */
public interface VectorIndexManager extends Closeable {

  /**
   * Applies a committed row change of the data table to the graph. The batch calls this method
   * while it holds its row locks, so this method does not throw.
   * @param currentDataRowState the row state before the change, or null for an insert
   * @param nextDataRowState    the row state after the change, or null for a delete
   * @param nextVector          the indexed vector of the row state after the change, or null if it
   *                            has none. The batch evaluates it before its commit.
   */
  void onMutation(Put currentDataRowState, Put nextDataRowState, float[] nextVector);

  /**
   * Returns true after the manager closes. A manager closes when its region closes, or when its
   * index is dropped or created again. A write or a scan that names the index then opens a new
   * manager.
   */
  boolean isClosed();

  /**
   * Opens the manager of the given index for the region of the coprocessor environment.
   * @param env         the coprocessor environment of the region
   * @param indexName   the full name of the index
   * @param oldestBatch a time no later than the timestamp of the oldest write batch of the region
   *                    that has not applied its graph changes, or Long.MAX_VALUE if there is no
   *                    such batch. The replay start of each segment that the manager writes is no
   *                    later than this time.
   * @return the vector index manager
   * @throws IOException if the manager cannot open the index
   */
  static VectorIndexManager create(RegionCoprocessorEnvironment env, String indexName,
    LongSupplier oldestBatch) throws IOException {
    return HnswIndexManager.open(env, indexName, oldestBatch);
  }
}
