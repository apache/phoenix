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
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.coprocessor.RegionCoprocessorEnvironment;

/** Manages incremental updates and lifecycle for region-level vector graph indexes. */
public interface VectorIndexManager extends Closeable {

  /**
   * Processes a committed data table row mutation.
   * @param currentDataRowState previous row state, or null for inserts
   * @param nextDataRowState    updated row state, or null for deletes
   */
  void onMutation(Put currentDataRowState, Put nextDataRowState);

  /**
   * Creates a vector index manager instance for the specified index.
   * @param env       coprocessor environment for the region
   * @param indexName logical index table name
   * @return initialized vector index manager
   * @throws IOException if initialization fails
   */
  static VectorIndexManager create(RegionCoprocessorEnvironment env, String indexName)
    throws IOException {
    return HnswIndexManager.open(env, indexName);
  }
}
