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
package org.apache.phoenix.hbase.index.write;

import java.io.IOException;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.MasterNotRunningException;
import org.apache.hadoop.hbase.ZooKeeperConnectionException;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.Mutation;
import org.apache.hadoop.hbase.coprocessor.RegionCoprocessorEnvironment;
import org.apache.hadoop.hbase.util.Pair;
import org.apache.phoenix.hbase.index.exception.MultiIndexWriteFailureException;
import org.apache.phoenix.hbase.index.table.HTableInterfaceReference;
import org.apache.phoenix.hbase.index.util.ImmutableBytesPtr;
import org.apache.phoenix.hbase.index.write.recovery.TrackingParallelWriterIndexCommitter;

import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.Multimap;

/**
 * Used to recover failed index edits during WAL replay
 * <p>
 * We attempt to do the index updates in parallel using a backing threadpool. All threads are daemon threads, so it will
 * not block the region from shutting down.
 */
public class RecoveryIndexWriter extends IndexWriter {

    private static final Log LOG = LogFactory.getLog(RecoveryIndexWriter.class);
    private Set<HTableInterfaceReference> nonExistingTablesList = new HashSet<HTableInterfaceReference>();
    private HBaseAdmin admin;

    /**
     * Directly specify the {@link IndexCommitter} and {@link IndexFailurePolicy}. Both are expected to be fully setup
     * before calling.
     * 
     * @param committer
     * @param policy
     * @param env
     * @throws IOException
     * @throws ZooKeeperConnectionException
     * @throws MasterNotRunningException
     */
    public RecoveryIndexWriter(IndexFailurePolicy policy, RegionCoprocessorEnvironment env, String name)
            throws MasterNotRunningException, ZooKeeperConnectionException, IOException {
        super(new TrackingParallelWriterIndexCommitter(), policy, env, name);
        this.admin = new HBaseAdmin(env.getConfiguration());
    }

    @Override
    public void write(Collection<Pair<Mutation, byte[]>> toWrite, boolean allowLocalUpdates) throws IOException {
        try {
            write(resolveTableReferences(toWrite), allowLocalUpdates);
        } catch (MultiIndexWriteFailureException e) {
            for (HTableInterfaceReference table : e.getFailedTables()) {
                if (!admin.tableExists(table.getTableName())) {
                    LOG.warn("Failure due to non existing table: " + table.getTableName());
                    nonExistingTablesList.add(table);
                } else {
                    throw e;
                }
            }
        }
    }

    /**
     * Convert the passed index updates to {@link HTableInterfaceReference}s.
     * 
     * @param indexUpdates
     *            from the index builder
     * @return pairs that can then be written by an {@link RecoveryIndexWriter}.
     */
    @Override
    protected Multimap<HTableInterfaceReference, Mutation> resolveTableReferences(
            Collection<Pair<Mutation, byte[]>> indexUpdates) {
        Multimap<HTableInterfaceReference, Mutation> updates = ArrayListMultimap
                .<HTableInterfaceReference, Mutation> create();

        // simple map to make lookups easy while we build the map of tables to create
        Map<ImmutableBytesPtr, HTableInterfaceReference> tables = new HashMap<ImmutableBytesPtr, HTableInterfaceReference>(
                updates.size());
        for (Pair<Mutation, byte[]> entry : indexUpdates) {
            byte[] tableName = entry.getSecond();
            ImmutableBytesPtr ptr = new ImmutableBytesPtr(tableName);
            HTableInterfaceReference table = tables.get(ptr);
            if (nonExistingTablesList.contains(table)) {
                LOG.debug("Edits found for non existing table: " + table.getTableName() + " so skipping it!!");
                continue;
            }
            if (table == null) {
                table = new HTableInterfaceReference(ptr);
                tables.put(ptr, table);
            }
            updates.put(table, entry.getFirst());

        }
        return updates;
    }

    @Override
    public void stop(String why) {
        super.stop(why);
        if (admin != null) {
            try {
                admin.close();
            } catch (IOException e) {
                // closing silently
            }
        }
    }
  
}