/*
 * Licensed to the Apache Software Foundation (ASF) under one or more contributor license agreements. See the NOTICE
 * file distributed with this work for additional information regarding copyright ownership. The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with the
 * License. You may obtain a copy of the License at http://www.apache.org/licenses/LICENSE-2.0 Unless required by
 * applicable law or agreed to in writing, software distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the specific language
 * governing permissions and limitations under the License.
 */
package org.apache.phoenix.hbase.index.covered;

import java.io.IOException;
import java.util.Collection;
import java.util.List;

import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Mutation;
import org.apache.hadoop.hbase.coprocessor.RegionCoprocessorEnvironment;
import org.apache.hadoop.hbase.util.Pair;
import org.apache.phoenix.hbase.index.builder.BaseIndexBuilder;
import org.apache.phoenix.hbase.index.covered.data.LocalHBaseState;
import org.apache.phoenix.hbase.index.covered.update.ColumnTracker;
import org.apache.phoenix.hbase.index.covered.update.IndexUpdateManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Build covered indexes for phoenix updates.
 * <p>
 * Before any call to prePut/preDelete, the row has already been locked. This ensures that we don't need to do any extra
 * synchronization in the IndexBuilder.
 * <p>
 * NOTE: This implementation doesn't cleanup the index when we remove a key-value on compaction or flush, leading to a
 * bloated index that needs to be cleaned up by a background process.
 */
public class NonTxIndexBuilder extends BaseIndexBuilder {
    private static final Logger LOGGER = LoggerFactory.getLogger(NonTxIndexBuilder.class);

    @Override
    public void setup(RegionCoprocessorEnvironment env) throws IOException {
        super.setup(env);
    }

    @Override
    public Collection<Pair<Mutation, byte[]>> getIndexUpdate(Mutation mutation, IndexMetaData indexMetaData, LocalHBaseState localHBaseState) throws IOException {
    	// create a state manager, so we can manage each batch
        LocalTableState state = new LocalTableState(localHBaseState, mutation);
        // build the index updates for each group
        IndexUpdateManager manager = new IndexUpdateManager(indexMetaData);

        batchMutationAndAddUpdates(manager, state, mutation, indexMetaData);

        if (LOGGER.isTraceEnabled()) {
            LOGGER.trace("Found index updates for Mutation: " + mutation + "\n" + manager);
        }

        return manager.toMap();
    }

    /**
     * Split the mutation into batches based on the timestamps of each keyvalue. We need to check each key-value in the
     * update to see if it matches the others. Generally, this will be the case, but you can add kvs to a mutation that
     * don't all have the timestamp, so we need to manage everything in batches based on timestamp.
     * <p>
     * Adds all the updates in the {@link Mutation} to the state, as a side-effect.
     * @param state
     *            current state of the row for the mutation.
     * @param m
     *            mutation to batch
     * @param indexMetaData TODO
     * @param updateMap
     *            index updates into which to add new updates. Modified as a side-effect.
     * 
     * @throws IOException
     */
    private void batchMutationAndAddUpdates(IndexUpdateManager manager, LocalTableState state, Mutation m, IndexMetaData indexMetaData) throws IOException {
        // The cells of a mutation are broken up into time stamp batches prior to this call (in Indexer).
        long ts = m.getFamilyCellMap().values().iterator().next().iterator().next().getTimestamp();
        Batch batch = new Batch(ts);
        for (List<Cell> family : m.getFamilyCellMap().values()) {
            for (Cell kv : family) {
                batch.add(kv);
                if(ts != kv.getTimestamp()) {
                    throw new IllegalStateException("Time stamps must match for all cells in a batch");
                }
            }
        }

        addMutationsForBatch(manager, batch, state, indexMetaData);
    }

    /**
     * For a single batch, get all the index updates and add them to the updateMap
     * <p>
     * This method manages cleaning up the entire history of the row from the given timestamp forward for out-of-order
     * (e.g. 'back in time') updates.
     * <p>
     * If things arrive out of order (client is using custom timestamps) we should still see the index in the correct
     * order (assuming we scan after the out-of-order update in finished). Therefore, we when we aren't the most recent
     * update to the index, we need to delete the state at the current timestamp (similar to above), but also issue a
     * delete for the added index updates at the next newest timestamp of any of the columns in the update; we need to
     * cleanup the insert so it looks like it was also deleted at that next newest timestamp. However, its not enough to
     * just update the one in front of us - that column will likely be applied to index entries up the entire history in
     * front of us, which also needs to be fixed up.
     * <p>
     * However, the current update usually will be the most recent thing to be added. In that case, all we need to is
     * issue a delete for the previous index row (the state of the row, without the update applied) at the current
     * timestamp. This gets rid of anything currently in the index for the current state of the row (at the timestamp).
     * Then we can just follow that by applying the pending update and building the index update based on the new row
     * state.
     * 
     * @param updateMap
     *            map to update with new index elements
     * @param batch
     *            timestamp-based batch of edits
     * @param state
     *            local state to update and pass to the codec
     * @param indexMetaData TODO
     * @return <tt>true</tt> if we cleaned up the current state forward (had a back-in-time put), <tt>false</tt>
     *         otherwise
     * @throws IOException
     */
    private boolean addMutationsForBatch(IndexUpdateManager updateMap, Batch batch, LocalTableState state,
            IndexMetaData indexMetaData) throws IOException {

        // need a temporary manager for the current batch. It should resolve any conflicts for the
        // current batch. Essentially, we can get the case where a batch doesn't change the current
        // state of the index (all Puts are covered by deletes), in which case we don't want to add
        // anything
        // A. Get the correct values for the pending state in the batch
        // A.1 start by cleaning up the current state - as long as there are key-values in the batch
        // that are indexed, we need to change the current state of the index. Its up to the codec to
        // determine if we need to make any cleanup given the pending update.
        long batchTs = batch.getTimestamp();
        state.setPendingUpdates(batch.getKvs());
        addCleanupForCurrentBatch(updateMap, batchTs, state, indexMetaData);

        // A.2 do a single pass first for the updates to the current state
        state.applyPendingUpdates();
        addUpdateForGivenTimestamp(batchTs, state, updateMap, indexMetaData);
        // FIXME: PHOENIX-4057 do not attempt to issue index updates
        // for out-of-order mutations since it corrupts the index.
        return false;
    }

    private long addUpdateForGivenTimestamp(long ts, LocalTableState state, IndexUpdateManager updateMap, IndexMetaData indexMetaData)
            throws IOException {
        state.setCurrentTimestamp(ts);
        ts = addCurrentStateMutationsForBatch(updateMap, state, indexMetaData);
        return ts;
    }

    private void addCleanupForCurrentBatch(IndexUpdateManager updateMap, long batchTs, LocalTableState state, IndexMetaData indexMetaData)
            throws IOException {
        // get the cleanup for the current state
        state.setCurrentTimestamp(batchTs);
        addDeleteUpdatesToMap(updateMap, state, batchTs, indexMetaData);
        // ignore any index tracking from the delete
        state.resetTrackedColumns();
    }

    /**
     * Add the necessary mutations for the pending batch on the local state. Handles rolling up through history to
     * determine the index changes after applying the batch (for the case where the batch is back in time).
     * 
     * @param updateMap
     *            to update with index mutations
     * @param state
     *            current state of the table
     * @param indexMetaData TODO
     * @param batch
     *            to apply to the current state
     * @return the minimum timestamp across all index columns requested. If {@link ColumnTracker#isNewestTime(long)}
     *         returns <tt>true</tt> on the returned timestamp, we know that this <i>was not a back-in-time update</i>.
     * @throws IOException
     */
    private long addCurrentStateMutationsForBatch(IndexUpdateManager updateMap, LocalTableState state, IndexMetaData indexMetaData)
            throws IOException {

        // get the index updates for this current batch
        Iterable<IndexUpdate> upserts = codec.getIndexUpserts(
            state, indexMetaData,
            env.getRegionInfo().getStartKey(), env.getRegionInfo().getEndKey(), false);
        state.resetTrackedColumns();

        /*
         * go through all the pending updates. If we are sure that all the entries are the latest timestamp, we can just
         * add the index updates and move on. However, if there are columns that we skip past (based on the timestamp of
         * the batch), we need to roll back up the history. Regardless of whether or not they are the latest timestamp,
         * the entries here are going to be correct for the current batch timestamp, so we add them to the updates. The
         * only thing we really care about it if we need to roll up the history and fix it as we go.
         */
        // timestamp of the next update we need to track
        long minTs = ColumnTracker.NO_NEWER_PRIMARY_TABLE_ENTRY_TIMESTAMP;
        for (IndexUpdate update : upserts) {
            // this is the one bit where we check the timestamps
            final ColumnTracker tracker = update.getIndexedColumns();
            long trackerTs = tracker.getTS();
            // update the next min TS we need to track
            if (trackerTs < minTs) {
                minTs = tracker.getTS();
            }
            
            // FIXME: PHOENIX-4057 do not attempt to issue index updates
            // for out-of-order mutations since it corrupts the index.
            if (tracker.hasNewerTimestamps()) {
                continue;
            }
            
            // only make the put if the index update has been setup
            if (update.isValid()) {
                byte[] table = update.getTableName();
                Mutation mutation = update.getUpdate();
                updateMap.addIndexUpdate(table, mutation);
            }
        }
        return minTs;
    }

    /**
     * Get the index deletes from the codec {@link IndexCodec#getIndexDeletes(TableState, IndexMetaData, byte[], byte[])} and then add them to the
     * update map.
     * <p>
     * Expects the {@link LocalTableState} to already be correctly setup (correct timestamp, updates applied, etc).
     * @param indexMetaData TODO
     * 
     * @throws IOException
     */
    protected void addDeleteUpdatesToMap(IndexUpdateManager updateMap, LocalTableState state, long ts, IndexMetaData indexMetaData)
            throws IOException {
        Iterable<IndexUpdate> cleanup = codec.getIndexDeletes(state, indexMetaData, env.getRegionInfo().getStartKey(), env.getRegionInfo().getEndKey());
        if (cleanup != null) {
            for (IndexUpdate d : cleanup) {
                if (!d.isValid()) {
                    continue;
                }
                // FIXME: PHOENIX-4057 do not attempt to issue index updates
                // for out-of-order mutations since it corrupts the index.
                final ColumnTracker tracker = d.getIndexedColumns();
                if (tracker.hasNewerTimestamps()) {
                    continue;
                }
                
                // override the timestamps in the delete to match the current batch.
                Delete remove = (Delete)d.getUpdate();
                remove.setTimestamp(ts);
                updateMap.addIndexUpdate(d.getTableName(), remove);
            }
        }
    }

    @Override
    public Collection<Pair<Mutation, byte[]>> getIndexUpdateForFilteredRows(Collection<Cell> filtered, IndexMetaData indexMetaData)
            throws IOException {
        // TODO Implement IndexBuilder.getIndexUpdateForFilteredRows
        return null;
    }
}