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
package org.apache.phoenix.hbase.index.covered;

import java.io.IOException;

import org.apache.hadoop.hbase.client.Mutation;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.coprocessor.RegionCoprocessorEnvironment;

import org.apache.phoenix.index.BaseIndexCodec;


/**
 * Codec for creating index updates from the current state of a table.
 * <p>
 * Generally, you should extend {@link BaseIndexCodec} instead, so help maintain compatibility as
 * features need to be added to the codec, as well as potentially not haivng to implement some
 * methods.
 */
public interface IndexCodec {

  /**
   * Do any code initialization necessary
   * @param env environment in which the codec is operating
   * @throws IOException if the codec cannot be initalized correctly
   */
  public void initialize(RegionCoprocessorEnvironment env) throws IOException;

  /**
   * Get the index cleanup entries. Currently, this must return just single row deletes (where just
   * the row-key is specified and no columns are returned) mapped to the table name. For instance,
   * to you have an index 'myIndex' with row :
   * 
   * <pre>
   * v1,v2,v3 | CF:CQ0  | rowkey
   *          | CF:CQ1  | rowkey
   * </pre>
   * 
   * To then cleanup this entry, you would just return 'v1,v2,v3', 'myIndex'.
   * @param state the current state of the table that needs to be cleaned up. Generally, you only
   *          care about the latest column values, for each column you are indexing for each index
   *          table.
   * @return the pairs of (deletes, index table name) that should be applied.
 * @throws IOException 
   */
  public Iterable<IndexUpdate> getIndexDeletes(TableState state) throws IOException;

  // table state has the pending update already applied, before calling
  // get the new index entries
  /**
   * Get the index updates for the primary table state, for each index table. The returned
   * {@link Put}s need to be fully specified (including timestamp) to minimize passes over the same
   * key-values multiple times.
   * <p>
   * You must specify the same timestamps on the Put as {@link TableState#getCurrentTimestamp()} so
   * the index entries match the primary table row. This could be managed at a higher level, but
   * would require iterating all the kvs in the Put again - very inefficient when compared to the
   * current interface where you must provide a timestamp anyways (so you might as well provide the
   * right one).
   * @param state the current state of the table that needs to an index update Generally, you only
   *          care about the latest column values, for each column you are indexing for each index
   *          table.
   * @return the pairs of (updates,index table name) that should be applied.
 * @throws IOException 
   */
  public Iterable<IndexUpdate> getIndexUpserts(TableState state) throws IOException;

  /**
   * This allows the codec to dynamically change whether or not indexing should take place for a
   * table. If it doesn't take place, we can save a lot of time on the regular Put patch. By making
   * it dynamic, we can save offlining and then onlining a table just to turn indexing on.
   * <p>
   * We can also be smart about even indexing a given update here too - if the update doesn't
   * contain any columns that we care about indexing, we can save the effort of analyzing the put
   * and further.
   * @param m mutation that should be indexed.
   * @return <tt>true</tt> if indexing is enabled for the given table. This should be on a per-table
   *         basis, as each codec is instantiated per-region.
 * @throws IOException 
   */
  public boolean isEnabled(Mutation m) throws IOException;

  /**
   * Get the batch identifier of the given mutation. Generally, updates to the table will take place
   * in a batch of updates; if we know that the mutation is part of a batch, we can build the state
   * much more intelligently.
   * <p>
   * <b>If you have batches that have multiple updates to the same row state, you must specify a
   * batch id for each batch. Otherwise, we cannot guarantee index correctness</b>
   * @param m mutation that may or may not be part of the batch
   * @return <tt>null</tt> if the mutation is not part of a batch or an id for the batch.
   */
  public byte[] getBatchId(Mutation m);
}