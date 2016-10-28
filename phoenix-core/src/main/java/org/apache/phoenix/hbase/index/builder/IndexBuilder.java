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
package org.apache.phoenix.hbase.index.builder;

import java.io.IOException;
import java.util.Collection;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.Stoppable;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Increment;
import org.apache.hadoop.hbase.client.Mutation;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.coprocessor.RegionCoprocessorEnvironment;
import org.apache.hadoop.hbase.regionserver.MiniBatchOperationInProgress;
import org.apache.hadoop.hbase.util.Pair;
import org.apache.phoenix.hbase.index.Indexer;
import org.apache.phoenix.hbase.index.covered.IndexMetaData;

/**
 * Interface to build updates ({@link Mutation}s) to the index tables, based on the primary table
 * updates.
 * <p>
 * Either all the index updates will be applied to all tables or the primary table will kill itself
 * and will attempt to replay the index edits through the WAL replay mechanism.
 */
public interface IndexBuilder extends Stoppable {

  /** Helper method signature to ensure people don't attempt to extend this class directly */
  public void extendBaseIndexBuilderInstead();

  /**
   * This is always called exactly once on install of {@link Indexer}, before any calls
   * {@link #getIndexUpdate} on
   * @param env in which the builder is running
   * @throws IOException on failure to setup the builder
   */
  public void setup(RegionCoprocessorEnvironment env) throws IOException;

  /**
   * Your opportunity to update any/all index tables based on the update of the primary table row.
   * Its up to your implementation to ensure that timestamps match between the primary and index
   * tables.
   * <p>
   * The mutation is a generic mutation (not a {@link Put} or a {@link Delete}), as it actually
   * corresponds to a batch update. Its important to note that {@link Put}s always go through the
   * batch update code path, so a single {@link Put} will come through here and update the primary
   * table as the only update in the mutation.
   * <p>
   * Implementers must ensure that this method is thread-safe - it could (and probably will) be
   * called concurrently for different mutations, which may or may not be part of the same batch.
   * @param mutation update to the primary table to be indexed.
   * @param context index meta data for the mutation
   * @return a Map of the mutations to make -> target index table name
   * @throws IOException on failure
   */
  public Collection<Pair<Mutation, byte[]>> getIndexUpdate(Mutation mutation, IndexMetaData context) throws IOException;

    /**
     * Build an index update to cleanup the index when we remove {@link KeyValue}s via the normal flush or compaction
     * mechanisms. Currently not implemented by any implementors nor called, but left here to be implemented if we
     * ever need it. In Jesse's words:
     * 
     * Arguably, this is a correctness piece that should be used, but isn't. Basically, it *could* be that
     * if a compaction/flush were to remove a key (too old, too many versions) you might want to cleanup the index table
     * as well, if it were to get out of sync with the primary table. For instance, you might get multiple versions of
     * the same row, which should eventually age of the oldest version. However, in the index table there would only
     * ever be two entries for that row - the first one, matching the original row, and the delete marker for the index
     * update, set when we got a newer version of the primary row. So, a basic HBase scan wouldn't show the index update
     * b/c its covered by the delete marker, but an older timestamp based read would actually show the index row, even
     * after the primary table row is gone due to MAX_VERSIONS requirement.
     *  
     * @param filtered {@link KeyValue}s that previously existed, but won't be included
     * in further output from HBase.
     * @param context TODO
     * 
     * @return a {@link Map} of the mutations to make -> target index table name
     * @throws IOException on failure
     */
  public Collection<Pair<Mutation, byte[]>> getIndexUpdateForFilteredRows(
      Collection<KeyValue> filtered, IndexMetaData context)
      throws IOException;

  /**
   * Notification that a batch of updates has successfully been written.
   * @param miniBatchOp the full batch operation that was written
   */
  public void batchCompleted(MiniBatchOperationInProgress<Mutation> miniBatchOp);

  /**
   * Notification that a batch has been started.
   * <p>
   * Unfortunately, the way HBase has the coprocessor hooks setup, this is actually called
   * <i>after</i> the {@link #getIndexUpdate} methods. Therefore, you will likely need an attribute
   * on your {@link Put}/{@link Delete} to indicate it is a batch operation.
   * @param miniBatchOp the full batch operation to be written
 * @param context TODO
 * @throws IOException 
   */
  public void batchStarted(MiniBatchOperationInProgress<Mutation> miniBatchOp, IndexMetaData context) throws IOException;

  public IndexMetaData getIndexMetaData(MiniBatchOperationInProgress<Mutation> miniBatchOp) throws IOException;
  
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
   * True if mutation has an ON DUPLICATE KEY clause
   * @param m mutation
   * @return true if mutation has ON DUPLICATE KEY expression and false otherwise.
   * @throws IOException
   */
  public boolean isAtomicOp(Mutation m) throws IOException;

  /**
   * Calculate the mutations based on the ON DUPLICATE KEY clause
   * @param inc increment to run against
   * @return list of mutations as a result of executing the ON DUPLICATE KEY clause
   * or null if Increment does not represent an ON DUPLICATE KEY clause.
   */
  public List<Mutation> executeAtomicOp(Increment inc) throws IOException;
}