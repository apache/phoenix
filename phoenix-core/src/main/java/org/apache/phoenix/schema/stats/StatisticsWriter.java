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
package org.apache.phoenix.schema.stats;

import java.io.Closeable;
import java.io.IOException;
import java.sql.Date;
import java.util.List;

import org.apache.hadoop.hbase.DoNotRetryIOException;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.HTableInterface;
import org.apache.hadoop.hbase.client.Mutation;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.ipc.CoprocessorRpcChannel;
import org.apache.hadoop.hbase.protobuf.ProtobufUtil;
import org.apache.hadoop.hbase.protobuf.generated.ClientProtos.MutationProto.MutationType;
import org.apache.hadoop.hbase.protobuf.generated.MultiRowMutationProtos.MultiRowMutationService;
import org.apache.hadoop.hbase.protobuf.generated.MultiRowMutationProtos.MutateRowsRequest;
import org.apache.phoenix.jdbc.PhoenixDatabaseMetaData;
import org.apache.phoenix.query.QueryConstants;
import org.apache.phoenix.schema.PDataType;
import org.apache.phoenix.util.ByteUtil;
import org.apache.phoenix.util.TimeKeeper;

import com.google.protobuf.ServiceException;

/**
 * Wrapper to access the statistics table SYSTEM.STATS using the HTable.
 */
public class StatisticsWriter implements Closeable {
    /**
     * @param tableName TODO
     * @param clientTimeStamp TODO
     * @param guidepostDepth 
     * @param Configuration
     *            Configruation to update the stats table.
     * @param primaryTableName
     *            name of the primary table on which we should collect stats
     * @return the {@link StatisticsWriter} for the given primary table.
     * @throws IOException
     *             if the table cannot be created due to an underlying HTable creation error
     */
    public static StatisticsWriter newWriter(HTableInterface hTable, String tableName, long clientTimeStamp) throws IOException {
        if (clientTimeStamp == HConstants.LATEST_TIMESTAMP) {
            clientTimeStamp = TimeKeeper.SYSTEM.getCurrentTime();
        }
        StatisticsWriter statsTable = new StatisticsWriter(hTable, tableName, clientTimeStamp);
        if (clientTimeStamp != StatisticsCollector.NO_TIMESTAMP) { // Otherwise we do this later as we don't know the ts yet
            statsTable.commitLastStatsUpdatedTime();
        }
        return statsTable;
    }

    private final HTableInterface statisticsTable;
    private final byte[] tableName;
    private final long clientTimeStamp;

    private StatisticsWriter(HTableInterface statsTable, String tableName, long clientTimeStamp) {
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
        boolean useMaxTimeStamp = clientTimeStamp == StatisticsCollector.NO_TIMESTAMP;
        long timeStamp = clientTimeStamp;
        if (useMaxTimeStamp) { // When using max timestamp, we write the update time later because we only know the ts now
            timeStamp = tracker.getMaxTimeStamp();
            mutations.add(getLastStatsUpdatedTimePut(timeStamp));
        }
        byte[] prefix = StatisticsUtil.getRowKey(tableName, PDataType.VARCHAR.toBytes(fam),
                PDataType.VARCHAR.toBytes(regionName));
        Put put = new Put(prefix);
        GuidePostsInfo gp = tracker.getGuidePosts(fam);
        if (gp != null) {
            put.add(QueryConstants.DEFAULT_COLUMN_FAMILY_BYTES, PhoenixDatabaseMetaData.GUIDE_POSTS_COUNT_BYTES,
                    timeStamp, PDataType.LONG.toBytes((gp.getGuidePosts().size())));
            put.add(QueryConstants.DEFAULT_COLUMN_FAMILY_BYTES, PhoenixDatabaseMetaData.GUIDE_POSTS_BYTES,
                    timeStamp, PDataType.VARBINARY.toBytes(gp.toBytes()));
            put.add(QueryConstants.DEFAULT_COLUMN_FAMILY_BYTES, PhoenixDatabaseMetaData.GUIDE_POSTS_WIDTH_BYTES,
                    timeStamp, PDataType.LONG.toBytes(gp.getByteCount()));
        }
        put.add(QueryConstants.DEFAULT_COLUMN_FAMILY_BYTES, PhoenixDatabaseMetaData.MIN_KEY_BYTES,
                timeStamp, PDataType.VARBINARY.toBytes(tracker.getMinKey(fam)));
        put.add(QueryConstants.DEFAULT_COLUMN_FAMILY_BYTES, PhoenixDatabaseMetaData.MAX_KEY_BYTES,
                timeStamp, PDataType.VARBINARY.toBytes(tracker.getMaxKey(fam)));
        // Add our empty column value so queries behave correctly
        put.add(QueryConstants.DEFAULT_COLUMN_FAMILY_BYTES, QueryConstants.EMPTY_COLUMN_BYTES,
                timeStamp, ByteUtil.EMPTY_BYTE_ARRAY);
        mutations.add(put);
    }

    private static MutationType getMutationType(Mutation m) throws IOException {
        if (m instanceof Put) {
            return MutationType.PUT;
        } else if (m instanceof Delete) {
            return MutationType.DELETE;
        } else {
            throw new DoNotRetryIOException("Unsupported mutation type in stats commit"
                    + m.getClass().getName());
        }
    }
    public void commitStats(List<Mutation> mutations) throws IOException {
        if (mutations.size() > 0) {
            byte[] row = mutations.get(0).getRow();
            MutateRowsRequest.Builder mrmBuilder = MutateRowsRequest.newBuilder();
            for (Mutation m : mutations) {
                mrmBuilder.addMutationRequest(ProtobufUtil.toMutation(getMutationType(m), m));
            }
            MutateRowsRequest mrm = mrmBuilder.build();
            CoprocessorRpcChannel channel = statisticsTable.coprocessorService(row);
            MultiRowMutationService.BlockingInterface service =
                    MultiRowMutationService.newBlockingStub(channel);
            try {
              service.mutateRows(null, mrm);
            } catch (ServiceException ex) {
              ProtobufUtil.toIOException(ex);
            }
        }
    }

    private Put getLastStatsUpdatedTimePut(long timeStamp) {
        long currentTime = TimeKeeper.SYSTEM.getCurrentTime();
        byte[] prefix = tableName;
        Put put = new Put(prefix);
        put.add(QueryConstants.DEFAULT_COLUMN_FAMILY_BYTES, PhoenixDatabaseMetaData.LAST_STATS_UPDATE_TIME_BYTES, timeStamp,
                PDataType.DATE.toBytes(new Date(currentTime)));
        return put;
    }

    private void commitLastStatsUpdatedTime() throws IOException {
        // Always use wallclock time for this, as it's a mechanism to prevent
        // stats from being collected too often.
        Put put = getLastStatsUpdatedTimePut(clientTimeStamp);
        statisticsTable.put(put);
    }
    
    public void deleteStats(String regionName, StatisticsCollector tracker, String fam, List<Mutation> mutations)
            throws IOException {
        long timeStamp = clientTimeStamp == StatisticsCollector.NO_TIMESTAMP ? tracker.getMaxTimeStamp() : clientTimeStamp;
        byte[] prefix = StatisticsUtil.getRowKey(tableName, PDataType.VARCHAR.toBytes(fam),
                PDataType.VARCHAR.toBytes(regionName));
        mutations.add(new Delete(prefix, timeStamp - 1));
    }
}