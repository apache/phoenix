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
package org.apache.phoenix.schema.stat;

import java.io.IOException;
import java.sql.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.CoprocessorEnvironment;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.HTableInterface;
import org.apache.hadoop.hbase.client.HTablePool;
import org.apache.hadoop.hbase.client.Mutation;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.Closeable;
import org.apache.phoenix.jdbc.PhoenixDatabaseMetaData;
import org.apache.phoenix.query.QueryConstants;
import org.apache.phoenix.schema.PDataType;

/**
 * Wrapper to access the statistics table SYSTEM.STATS using the HTable.
 */
public class StatisticsTable implements Closeable {

    private static final Log LOG = LogFactory.getLog(StatisticsTable.class);
    /** Map of the currently open statistics tables */
    private static final Map<String, StatisticsTable> tableMap = new HashMap<String, StatisticsTable>();
    /**
     * @param env
     *            Environment wherein the coprocessor is attempting to update the stats table.
     * @param primaryTableName
     *            name of the primary table on which we should collect stats
     * @return the {@link StatisticsTable} for the given primary table.
     * @throws IOException
     *             if the table cannot be created due to an underlying HTable creation error
     */
    public synchronized static StatisticsTable getStatisticsTableForCoprocessor(CoprocessorEnvironment env,
            byte[] primaryTableName) throws IOException {
        StatisticsTable table = tableMap.get(primaryTableName);
        if (table == null) {
            // Map the statics table and the table with which the statistics is
            // associated. This is a workaround
            HTablePool pool = new HTablePool(env.getConfiguration(), 1);
            HTableInterface hTable = pool.getTable(PhoenixDatabaseMetaData.SYSTEM_STATS_NAME);
            table = new StatisticsTable(hTable, primaryTableName);
            tableMap.put(Bytes.toString(primaryTableName), table);
        }
        return table;
    }

    private final HTableInterface statisticsTable;
    private final byte[] sourceTableName;

    private StatisticsTable(HTableInterface statsTable, byte[] sourceTableName) {
        this.statisticsTable = statsTable;
        this.sourceTableName = sourceTableName;
    }

    public StatisticsTable(Configuration conf, HTableDescriptor source) throws IOException {
        this(new HTable(conf, PhoenixDatabaseMetaData.SYSTEM_STATS_NAME), source.getName());
    }

    /**
     * Close the connection to the table
     */
    @Override
    public void close() throws IOException {
        statisticsTable.close();
    }

    /**
     * Update a list of statistics for a given region.  If the ANALYZE <tablename> query is issued
     * then we use Upsert queries to update the table
     * If the region gets splitted or the major compaction happens we update using HTable.put()
     * @param tablekey - The table name
     * @param schemaName - the schema name associated with the table          
     * @param region name -  the region of the table for which the stats are collected
     * @param tracker - the statistics tracker
     * @param fam -  the family for which the stats is getting collected.
     * @param split - if the updation is caused due to a split
     * @param mutations - list of mutations that collects all the mutations to commit in a batch
     * @param currentTime -  the current time
     * @throws IOException
     *             if we fail to do any of the puts. Any single failure will prevent any future attempts for the remaining list of stats to
     *             update
     */
    public void addStats(String tableName, String regionName, StatisticsTracker tracker, String fam,
            List<Mutation> mutations, long currentTime) throws IOException {
        if (tracker == null) { return; }

        // Add the timestamp header
        formLastUpdatedStatsMutation(tableName, currentTime, mutations);

        byte[] prefix = StatisticsUtils.getRowKey(PDataType.VARCHAR.toBytes(tableName), PDataType.VARCHAR.toBytes(fam),
                PDataType.VARCHAR.toBytes(regionName));
        formStatsUpdateMutation(tracker, fam, mutations, currentTime, prefix);
    }

    public void commitStats(List<Mutation> mutations) throws IOException {
        Object[] res = new Object[mutations.size()];
        try {
            statisticsTable.batch(mutations, res);
        } catch (InterruptedException e) {
            throw new IOException("Exception while adding deletes and puts");
        }
    }

    private void formStatsUpdateMutation(StatisticsTracker tracker, String fam, List<Mutation> mutations,
            long currentTime, byte[] prefix) {
        Put put = new Put(prefix, currentTime);
        if (tracker.getGuidePosts(fam) != null) {
            put.add(QueryConstants.DEFAULT_COLUMN_FAMILY_BYTES, PhoenixDatabaseMetaData.GUIDE_POSTS_BYTES,
                    currentTime, (tracker.getGuidePosts(fam)));
        }
        put.add(QueryConstants.DEFAULT_COLUMN_FAMILY_BYTES, PhoenixDatabaseMetaData.MIN_KEY_BYTES,
                currentTime, PDataType.VARBINARY.toBytes(tracker.getMinKey(fam)));
        put.add(QueryConstants.DEFAULT_COLUMN_FAMILY_BYTES, PhoenixDatabaseMetaData.MAX_KEY_BYTES,
                currentTime, PDataType.VARBINARY.toBytes(tracker.getMaxKey(fam)));
        mutations.add(put);
    }

    private void formLastUpdatedStatsMutation(String tableName, long currentTime, List<Mutation> mutations) throws IOException {
        byte[] prefix = StatisticsUtils.getRowKeyForTSUpdate(PDataType.VARCHAR.toBytes(tableName));
        Put put = new Put(prefix);
        put.add(QueryConstants.DEFAULT_COLUMN_FAMILY_BYTES, PhoenixDatabaseMetaData.LAST_STATS_UPDATE_TIME_BYTES, currentTime,
                PDataType.DATE.toBytes(new Date(currentTime)));
        mutations.add(put);
    }
    
    public void deleteStats(String tableName, String regionName, StatisticsTracker tracker, String fam,
            List<Mutation> mutations, long currentTime)
            throws IOException {
        byte[] prefix = StatisticsUtils.getRowKey(PDataType.VARCHAR.toBytes(tableName), PDataType.VARCHAR.toBytes(fam),
                PDataType.VARCHAR.toBytes(regionName));
        mutations.add(new Delete(prefix, currentTime - 1));
    }

    /**
     * @return the underlying {@link HTableInterface} to which this table is writing
     */
    HTableInterface getUnderlyingTable() {
        return statisticsTable;
    }

    byte[] getSourceTableName() {
        return this.sourceTableName;
    }
}