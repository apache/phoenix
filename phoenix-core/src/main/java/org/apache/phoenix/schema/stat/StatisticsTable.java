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

import java.io.Closeable;
import java.io.IOException;
import java.sql.Date;
import java.util.List;

import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.HTableInterface;
import org.apache.hadoop.hbase.client.Mutation;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.coprocessor.MultiRowMutationProtocol;
import org.apache.phoenix.jdbc.PhoenixDatabaseMetaData;
import org.apache.phoenix.query.QueryConstants;
import org.apache.phoenix.schema.PDataType;
import org.apache.phoenix.util.ByteUtil;
import org.apache.phoenix.util.TimeKeeper;

/**
 * Wrapper to access the statistics table SYSTEM.STATS using the HTable.
 */
public class StatisticsTable implements Closeable {
    /**
<<<<<<< HEAD
     * @param env
     *            Environment wherein the coprocessor is attempting to update the stats table.
=======
     * @param tableName TODO
     * @param clientTimeStamp TODO
     * @param Configuration
     *            Configruation to update the stats table.
>>>>>>> 5668817... PHOENIX-1321 Cleanup setting of timestamps when collecting and using stats
     * @param primaryTableName
     *            name of the primary table on which we should collect stats
     * @return the {@link StatisticsTable} for the given primary table.
     * @throws IOException
     *             if the table cannot be created due to an underlying HTable creation error
     */
    public static StatisticsTable getStatisticsTable(HTableInterface hTable, String tableName, long clientTimeStamp) throws IOException {
        if (clientTimeStamp == HConstants.LATEST_TIMESTAMP) {
            clientTimeStamp = TimeKeeper.SYSTEM.getCurrentTime();
        }
        StatisticsTable statsTable = new StatisticsTable(hTable, tableName, clientTimeStamp);
        statsTable.commitLastStatsUpdatedTime();
        return statsTable;
    }

    private final HTableInterface statisticsTable;
    private final byte[] tableName;
    private final long clientTimeStamp;

    private StatisticsTable(HTableInterface statsTable, String tableName, long clientTimeStamp) {
        this.statisticsTable = statsTable;
        this.tableName = PDataType.VARCHAR.toBytes(tableName);
        this.clientTimeStamp = clientTimeStamp;
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
     * @param tracker - the statistics tracker
     * @param fam -  the family for which the stats is getting collected.
     * @param mutations - list of mutations that collects all the mutations to commit in a batch
     * @param tablekey - The table name
     * @param schemaName - the schema name associated with the table          
     * @param region name -  the region of the table for which the stats are collected
     * @param split - if the updation is caused due to a split
     * @throws IOException
     *             if we fail to do any of the puts. Any single failure will prevent any future attempts for the remaining list of stats to
     *             update
     */
    public void addStats(String regionName, StatisticsCollector tracker, String fam, List<Mutation> mutations) throws IOException {
        if (tracker == null) { return; }

        byte[] prefix = StatisticsUtils.getRowKey(tableName, PDataType.VARCHAR.toBytes(fam),
                PDataType.VARCHAR.toBytes(regionName));
        Put put = new Put(prefix);
        if (tracker.getGuidePosts(fam) != null) {
            put.add(QueryConstants.DEFAULT_COLUMN_FAMILY_BYTES, PhoenixDatabaseMetaData.GUIDE_POSTS_BYTES,
                    clientTimeStamp, (tracker.getGuidePosts(fam)));
        }
        put.add(QueryConstants.DEFAULT_COLUMN_FAMILY_BYTES, PhoenixDatabaseMetaData.MIN_KEY_BYTES,
                clientTimeStamp, PDataType.VARBINARY.toBytes(tracker.getMinKey(fam)));
        put.add(QueryConstants.DEFAULT_COLUMN_FAMILY_BYTES, PhoenixDatabaseMetaData.MAX_KEY_BYTES,
                clientTimeStamp, PDataType.VARBINARY.toBytes(tracker.getMaxKey(fam)));
        // Add our empty column value so queries behave correctly
        put.add(QueryConstants.DEFAULT_COLUMN_FAMILY_BYTES, QueryConstants.EMPTY_COLUMN_BYTES,
                clientTimeStamp, ByteUtil.EMPTY_BYTE_ARRAY);
        mutations.add(put);
    }

    public void commitStats(List<Mutation> mutations) throws IOException {
        if (mutations.size() > 0) {
            byte[] row = mutations.get(0).getRow();
            HTableInterface stats = this.statisticsTable;
            MultiRowMutationProtocol protocol = stats.coprocessorProxy(MultiRowMutationProtocol.class, row);
            protocol.mutateRows(mutations);
        }
    }

    private void commitLastStatsUpdatedTime() throws IOException {
        // Always use wallclock time for this, as it's a mechanism to prevent
        // stats from being collected too often.
        long currentTime = TimeKeeper.SYSTEM.getCurrentTime();
        byte[] prefix = tableName;
        Put put = new Put(prefix);
        put.add(QueryConstants.DEFAULT_COLUMN_FAMILY_BYTES, PhoenixDatabaseMetaData.LAST_STATS_UPDATE_TIME_BYTES, clientTimeStamp,
                PDataType.DATE.toBytes(new Date(currentTime)));
        statisticsTable.put(put);
    }
    
    public void deleteStats(String regionName, StatisticsCollector tracker, String fam, List<Mutation> mutations)
            throws IOException {
        byte[] prefix = StatisticsUtils.getRowKey(tableName, PDataType.VARCHAR.toBytes(fam),
                PDataType.VARCHAR.toBytes(regionName));
        mutations.add(new Delete(prefix, clientTimeStamp - 1));
    }
}