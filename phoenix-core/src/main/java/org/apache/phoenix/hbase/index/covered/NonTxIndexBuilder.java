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
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.KeyValueUtil;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Mutation;
import org.apache.hadoop.hbase.coprocessor.RegionCoprocessorEnvironment;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.EnvironmentEdgeManager;
import org.apache.hadoop.hbase.util.Pair;
import org.apache.phoenix.hbase.index.builder.BaseIndexBuilder;
import org.apache.phoenix.hbase.index.covered.data.LocalHBaseState;
import org.apache.phoenix.hbase.index.covered.data.LocalTable;
import org.apache.phoenix.hbase.index.covered.update.ColumnTracker;
import org.apache.phoenix.hbase.index.covered.update.IndexUpdateManager;
import org.apache.phoenix.hbase.index.covered.update.IndexedColumnGroup;

import com.google.common.collect.Lists;
import com.google.common.primitives.Longs;

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
    private static final Log LOG = LogFactory.getLog(NonTxIndexBuilder.class);

    protected LocalHBaseState localTable;

    @Override
    public void setup(RegionCoprocessorEnvironment env) throws IOException {
        super.setup(env);
        this.localTable = new LocalTable(env);
    }

    @Override
    public Collection<Pair<Mutation, byte[]>> getIndexUpdate(Mutation mutation, IndexMetaData indexMetaData) throws IOException {
    	// create a state manager, so we can manage each batch
        LocalTableState state = new LocalTableState(env, localTable, mutation);
        // build the index updates for each group
        IndexUpdateManager manager = new IndexUpdateManager(indexMetaData);

        batchMutationAndAddUpdates(manager, state, mutation, indexMetaData);

        if (LOG.isTraceEnabled()) {
            LOG.trace("Found index updates for Mutation: " + mutation + "\n" + manager);
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
        // split the mutation into timestamp-based batches
        Collection<Batch> batches = createTimestampBatchesFromMutation(m);

        // go through each batch of keyvalues and build separate index entries for each
        boolean cleanupCurrentState = !indexMetaData.isImmutableRows();
        for (Batch batch : batches) {
            /*
             * We have to split the work between the cleanup and the update for each group because when we update the
             * current state of the row for the current batch (appending the mutations for the current batch) the next
             * group will see that as the current state, which will can cause the a delete and a put to be created for
             * the next group.
             */
            if (addMutationsForBatch(manager, batch, state, cleanupCurrentState, indexMetaData)) {
                cleanupCurrentState = false;
            }
        }
    }

    /**
     * Batch all the {@link KeyValue}s in a {@link Mutation} by timestamp. Updates any {@link KeyValue} with a timestamp
     * == {@link HConstants#LATEST_TIMESTAMP} to the timestamp at the time the method is called.
     * 
     * @param m
     *            {@link Mutation} from which to extract the {@link KeyValue}s
     * @return the mutation, broken into batches and sorted in ascending order (smallest first)
     */
    protected Collection<Batch> createTimestampBatchesFromMutation(Mutation m) {
        Map<Long, Batch> batches = new HashMap<Long, Batch>();
        for (List<Cell> family : m.getFamilyCellMap().values()) {
            List<KeyValue> familyKVs = KeyValueUtil.ensureKeyValues(family);
            createTimestampBatchesFromKeyValues(familyKVs, batches);
        }
        // sort the batches
        List<Batch> sorted = new ArrayList<Batch>(batches.values());
        Collections.sort(sorted, new Comparator<Batch>() {
            @Override
            public int compare(Batch o1, Batch o2) {
                return Longs.compare(o1.getTimestamp(), o2.getTimestamp());
            }
        });
        return sorted;
    }

    /**
     * Batch all the {@link KeyValue}s in a collection of kvs by timestamp. Updates any {@link KeyValue} with a
     * timestamp == {@link HConstants#LATEST_TIMESTAMP} to the timestamp at the time the method is called.
     * 
     * @param kvs
     *            {@link KeyValue}s to break into batches
     * @param batches
     *            to update with the given kvs
     */
    protected void createTimestampBatchesFromKeyValues(Collection<KeyValue> kvs, Map<Long, Batch> batches) {
        long now = EnvironmentEdgeManager.currentTime();
        byte[] nowBytes = Bytes.toBytes(now);

        // batch kvs by timestamp
        for (KeyValue kv : kvs) {
            long ts = kv.getTimestamp();
            // override the timestamp to the current time, so the index and primary tables match
            // all the keys with LATEST_TIMESTAMP will then be put into the same batch
            if (kv.updateLatestStamp(nowBytes)) {
                ts = now;
            }
            Batch batch = batches.get(ts);
            if (batch == null) {
                batch = new Batch(ts);
                batches.put(ts, batch);
            }
            batch.add(kv);
        }
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
     * @param requireCurrentStateCleanup
     *            <tt>true</tt> if we should should attempt to cleanup the current state of the table, in the event of a
     *            'back in time' batch. <tt>false</tt> indicates we should not attempt the cleanup, e.g. an earlier
     *            batch already did the cleanup.
     * @param indexMetaData TODO
     * @return <tt>true</tt> if we cleaned up the current state forward (had a back-in-time put), <tt>false</tt>
     *         otherwise
     * @throws IOException
     */
    private boolean addMutationsForBatch(IndexUpdateManager updateMap, Batch batch, LocalTableState state,
            boolean requireCurrentStateCleanup, IndexMetaData indexMetaData) throws IOException {

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
        if (!indexMetaData.isImmutableRows()) {
            addCleanupForCurrentBatch(updateMap, batchTs, state, indexMetaData);
        }

        // A.2 do a single pass first for the updates to the current state
        state.applyPendingUpdates();
        long minTs = addUpdateForGivenTimestamp(batchTs, state, updateMap, indexMetaData);
        // if all the updates are the latest thing in the index, we are done - don't go and fix history
        if (ColumnTracker.isNewestTime(minTs)) { return false; }

        // A.3 otherwise, we need to roll up through the current state and get the 'correct' view of the
        // index. after this, we have the correct view of the index, from the batch up to the index
        while (!ColumnTracker.isNewestTime(minTs)) {
            minTs = addUpdateForGivenTimestamp(minTs, state, updateMap, indexMetaData);
        }

        // B. only cleanup the current state if we need to - its a huge waste of effort otherwise.
        if (requireCurrentStateCleanup) {
            // roll back the pending update. This is needed so we can remove all the 'old' index entries.
            // We don't need to do the puts here, but just the deletes at the given timestamps since we
            // just want to completely hide the incorrect entries.
            state.rollback(batch.getKvs());
            // setup state
            state.setPendingUpdates(batch.getKvs());

            // cleanup the pending batch. If anything in the correct history is covered by Deletes used to
            // 'fix' history (same row key and ts), we just drop the delete (we don't want to drop both
            // because the update may have a different set of columns or value based on the update).
            cleanupIndexStateFromBatchOnward(updateMap, batchTs, state, indexMetaData);

            // have to roll the state forward again, so the current state is correct
            state.applyPendingUpdates();
            return true;
        }
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
        Iterable<IndexUpdate> upserts = codec.getIndexUpserts(state, indexMetaData);
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
        List<IndexedColumnGroup> columnHints = new ArrayList<IndexedColumnGroup>();
        for (IndexUpdate update : upserts) {
            // this is the one bit where we check the timestamps
            final ColumnTracker tracker = update.getIndexedColumns();
            long trackerTs = tracker.getTS();
            // update the next min TS we need to track
            if (trackerTs < minTs) {
                minTs = tracker.getTS();
            }
            // track index hints for the next round. Hint if we need an update for that column for the
            // next timestamp. These columns clearly won't need to update as we go through time as they
            // already match the most recent possible thing.
            boolean needsCleanup = false;
            if (tracker.hasNewerTimestamps()) {
                columnHints.add(tracker);
                // this update also needs to be cleaned up at the next timestamp because it not the latest.
                needsCleanup = true;
            }

            // only make the put if the index update has been setup
            if (update.isValid()) {
                byte[] table = update.getTableName();
                Mutation mutation = update.getUpdate();
                updateMap.addIndexUpdate(table, mutation);

                // only make the cleanup if we made a put and need cleanup
                if (needsCleanup) {
                    // there is a TS for the interested columns that is greater than the columns in the
                    // put. Therefore, we need to issue a delete at the same timestamp
                    Delete d = new Delete(mutation.getRow());
                    d.setTimestamp(tracker.getTS());
                    updateMap.addIndexUpdate(table, d);
                }
            }
        }
        return minTs;
    }

    /**
     * Cleanup the index based on the current state from the given batch. Iterates over each timestamp (for the indexed
     * rows) for the current state of the table and cleans up all the existing entries generated by the codec.
     * <p>
     * Adds all pending updates to the updateMap
     * 
     * @param updateMap
     *            updated with the pending index updates from the codec
     * @param batchTs
     *            timestamp from which we should cleanup
     * @param state
     *            current state of the primary table. Should already by setup to the correct state from which we want to
     *            cleanup.
     * @param indexMetaData TODO
     * @throws IOException
     */
    private void cleanupIndexStateFromBatchOnward(IndexUpdateManager updateMap, long batchTs, LocalTableState state, IndexMetaData indexMetaData)
            throws IOException {
        // get the cleanup for the current state
        state.setCurrentTimestamp(batchTs);
        addDeleteUpdatesToMap(updateMap, state, batchTs, indexMetaData);
        Set<ColumnTracker> trackers = state.getTrackedColumns();
        long minTs = ColumnTracker.NO_NEWER_PRIMARY_TABLE_ENTRY_TIMESTAMP;
        for (ColumnTracker tracker : trackers) {
            if (tracker.getTS() < minTs) {
                minTs = tracker.getTS();
            }
        }
        state.resetTrackedColumns();
        if (!ColumnTracker.isNewestTime(minTs)) {
            state.setHints(Lists.newArrayList(trackers));
            cleanupIndexStateFromBatchOnward(updateMap, minTs, state, indexMetaData);
        }
    }

    /**
     * Get the index deletes from the codec {@link IndexCodec#getIndexDeletes(TableState, IndexMetaData)} and then add them to the
     * update map.
     * <p>
     * Expects the {@link LocalTableState} to already be correctly setup (correct timestamp, updates applied, etc).
     * @param indexMetaData TODO
     * 
     * @throws IOException
     */
    protected void addDeleteUpdatesToMap(IndexUpdateManager updateMap, LocalTableState state, long ts, IndexMetaData indexMetaData)
            throws IOException {
        if (indexMetaData.isImmutableRows()) {
            return;
        }
        Iterable<IndexUpdate> cleanup = codec.getIndexDeletes(state, indexMetaData);
        if (cleanup != null) {
            for (IndexUpdate d : cleanup) {
                if (!d.isValid()) {
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
    public Collection<Pair<Mutation, byte[]>> getIndexUpdateForFilteredRows(Collection<KeyValue> filtered, IndexMetaData indexMetaData)
            throws IOException {
        // TODO Implement IndexBuilder.getIndexUpdateForFilteredRows
        return null;
    }
}