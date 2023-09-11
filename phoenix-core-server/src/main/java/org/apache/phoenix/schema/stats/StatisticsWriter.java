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

import static org.apache.phoenix.schema.stats.StatisticsUtil.getAdjustedKey;

import java.io.ByteArrayInputStream;
import java.io.Closeable;
import java.io.DataInput;
import java.io.DataInputStream;
import java.io.EOFException;
import java.io.IOException;
import java.security.PrivilegedExceptionAction;
import java.sql.Date;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.DoNotRetryIOException;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Mutation;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.coprocessor.RegionCoprocessorEnvironment;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.ipc.CoprocessorRpcChannel;
import org.apache.hadoop.hbase.protobuf.ProtobufUtil;
import org.apache.hadoop.hbase.protobuf.generated.ClientProtos.MutationProto.MutationType;
import org.apache.hadoop.hbase.protobuf.generated.MultiRowMutationProtos.MultiRowMutationService;
import org.apache.hadoop.hbase.protobuf.generated.MultiRowMutationProtos.MutateRowsRequest;
import org.apache.hadoop.hbase.regionserver.Region;
import org.apache.hadoop.hbase.security.User;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.phoenix.coprocessorclient.MetaDataProtocol;
import org.apache.phoenix.hbase.index.util.ImmutableBytesPtr;
import org.apache.phoenix.jdbc.PhoenixConnection;
import org.apache.phoenix.jdbc.PhoenixDatabaseMetaData;
import org.apache.phoenix.query.QueryConstants;
import org.apache.phoenix.query.QueryServices;
import org.apache.phoenix.query.QueryServicesOptions;
import org.apache.phoenix.schema.types.PDate;
import org.apache.phoenix.schema.types.PLong;
import org.apache.phoenix.util.ByteUtil;
import org.apache.phoenix.util.EnvironmentEdgeManager;
import org.apache.phoenix.util.MetaDataUtil;
import org.apache.phoenix.util.PrefixByteDecoder;
import org.apache.phoenix.util.SchemaUtil;
import org.apache.phoenix.util.ServerUtil;

import com.google.protobuf.ServiceException;
import org.apache.phoenix.util.ServerUtil.ConnectionFactory;
import org.apache.phoenix.util.ServerUtil.ConnectionType;

/**
 * Wrapper to access the statistics table SYSTEM.STATS using the HTable.
 */
public class StatisticsWriter implements Closeable {

    public static StatisticsWriter newWriter(PhoenixConnection conn, String tableName, long clientTimeStamp)
            throws SQLException {
        Configuration configuration = conn.getQueryServices().getConfiguration();
        long newClientTimeStamp = determineClientTimeStamp(configuration, clientTimeStamp);
        TableName physicalTableName = SchemaUtil.getPhysicalTableName(PhoenixDatabaseMetaData.SYSTEM_STATS_NAME_BYTES, configuration);
        Table statsWriterTable = conn.getQueryServices().getTable(physicalTableName.getName());
        Table statsReaderTable = conn.getQueryServices().getTable(physicalTableName.getName());
        StatisticsWriter statsTable = new StatisticsWriter(statsReaderTable, statsWriterTable, tableName,
                newClientTimeStamp);
        return statsTable;
    }

    /**
     * @param tableName
     *            TODO
     * @param clientTimeStamp
     *            TODO
     * @return the {@link StatisticsWriter} for the given primary table.
     * @throws IOException
     *             if the table cannot be created due to an underlying HTable creation error
     */
    public static StatisticsWriter newWriter(RegionCoprocessorEnvironment env, String tableName, long clientTimeStamp)
            throws IOException {
        Configuration configuration = env.getConfiguration();
        long newClientTimeStamp = determineClientTimeStamp(configuration, clientTimeStamp);
        Table statsWriterTable = ConnectionFactory.getConnection(ConnectionType.DEFAULT_SERVER_CONNECTION, env).getTable(
                SchemaUtil.getPhysicalTableName(PhoenixDatabaseMetaData.SYSTEM_STATS_NAME_BYTES, env.getConfiguration()));
        Table statsReaderTable = ServerUtil.getHTableForCoprocessorScan(env, statsWriterTable);
        StatisticsWriter statsTable = new StatisticsWriter(statsReaderTable, statsWriterTable, tableName,
                newClientTimeStamp);
        return statsTable;
    }

    // Provides a means of clients controlling their timestamps to not use current time
    // when background tasks are updating stats. Instead we track the max timestamp of
    // the cells and use that.
    private static long determineClientTimeStamp(Configuration configuration, long clientTimeStamp) {
        boolean useCurrentTime = configuration.getBoolean(
                QueryServices.STATS_USE_CURRENT_TIME_ATTRIB,
                QueryServicesOptions.DEFAULT_STATS_USE_CURRENT_TIME);
        if (!useCurrentTime) {
            clientTimeStamp = DefaultStatisticsCollector.NO_TIMESTAMP;
        }
        if (clientTimeStamp == HConstants.LATEST_TIMESTAMP) {
            clientTimeStamp = EnvironmentEdgeManager.currentTimeMillis();
        }
        return clientTimeStamp;
    }

    private final Table statsWriterTable;
    // In HBase 0.98.4 or above, the reader and writer will be the same.
    // In pre HBase 0.98.4, there was a bug in using the HTable returned
    // from a coprocessor for scans, so in that case it'll be different.
    private final Table statsReaderTable;
    private final byte[] tableName;
    private final long clientTimeStamp;

    private StatisticsWriter(Table statsReaderTable,
                             Table statsWriterTable, String tableName, long clientTimeStamp) {
        this.statsReaderTable = statsReaderTable;
        this.statsWriterTable = statsWriterTable;
        this.tableName = Bytes.toBytes(tableName);
        this.clientTimeStamp = clientTimeStamp;
    }

    /**
     * Close the connection to the table
     */
    @Override
    public void close() throws IOException {
        statsWriterTable.close();
        statsReaderTable.close();
    }

    /**
     * Update a list of statistics for a given region. If the UPDATE STATISTICS {@code <tablename> } query is issued then we use
     * Upsert queries to update the table If the region gets splitted or the major compaction happens we update using
     * HTable.put()
     * 
     * @param tracker
     *            - the statistics tracker
     * @param cfKey
     *            - the family for which the stats is getting collected.
     * @param mutations
     *            - list of mutations that collects all the mutations to commit in a batch
     * @throws IOException
     *             if we fail to do any of the puts. Any single failure will prevent any future attempts for the
     *             remaining list of stats to update
     */
    @SuppressWarnings("deprecation")
    public void addStats(StatisticsCollector tracker, ImmutableBytesPtr cfKey,
                         List<Mutation> mutations, long guidePostDepth) throws IOException {
        if (tracker == null) { return; }
        boolean useMaxTimeStamp = clientTimeStamp == DefaultStatisticsCollector.NO_TIMESTAMP;
        long timeStamp = clientTimeStamp;
        if (useMaxTimeStamp) { // When using max timestamp, we write the update time later because we only know the ts
                               // now
            timeStamp = tracker.getMaxTimeStamp();
            mutations.add(getLastStatsUpdatedTimePut(timeStamp));
        }
        GuidePostsInfo gps = tracker.getGuidePosts(cfKey);
        if (gps != null) {
            long[] byteCounts = gps.getByteCounts();
            long[] rowCounts = gps.getRowCounts();
            ImmutableBytesWritable keys = gps.getGuidePosts();
            boolean hasGuidePosts = keys.getLength() > 0;
            if (hasGuidePosts) {
                int guidePostCount = 0;
                try (ByteArrayInputStream stream = new ByteArrayInputStream(keys.get(), keys.getOffset(), keys.getLength())) {
                    DataInput input = new DataInputStream(stream);
                    PrefixByteDecoder decoder = new PrefixByteDecoder(gps.getMaxLength());
                    do {
                        ImmutableBytesWritable ptr = decoder.decode(input);
                        addGuidepost(cfKey, mutations, ptr, byteCounts[guidePostCount], rowCounts[guidePostCount], timeStamp);
                        guidePostCount++;
                    } while (decoder != null);
                } catch (EOFException e) { // Ignore as this signifies we're done

                }
                // If we've written guideposts with a guidepost key, then delete the
                // empty guidepost indicator that may have been written by other
                // regions.
                byte[] rowKey = StatisticsUtil.getRowKey(tableName, cfKey, ByteUtil.EMPTY_IMMUTABLE_BYTE_ARRAY);
                Delete delete = new Delete(rowKey, timeStamp);
                mutations.add(delete);
            } else {
                /*
                 * When there is not enough data in the region, we create a guide post with empty
                 * key with the estimated amount of data in it as the guide post width. We can't
                 * determine the expected number of rows here since we don't have the PTable and the
                 * associated schema available to make the row size estimate. We instead will
                 * compute it on the client side when reading out guideposts from the SYSTEM.STATS
                 * table in StatisticsUtil#readStatistics(HTableInterface statsHTable,
                 * GuidePostsKey key, long clientTimeStamp).
                 */
                addGuidepost(cfKey, mutations, ByteUtil.EMPTY_IMMUTABLE_BYTE_ARRAY, guidePostDepth,
                    0, timeStamp);
            }
        }
    }
    
    @SuppressWarnings("deprecation")
    private void addGuidepost(ImmutableBytesPtr cfKey, List<Mutation> mutations, ImmutableBytesWritable ptr, long byteCount, long rowCount, long timeStamp) {
        byte[] prefix = StatisticsUtil.getRowKey(tableName, cfKey, ptr);
        Put put = new Put(prefix);
        put.addColumn(QueryConstants.DEFAULT_COLUMN_FAMILY_BYTES, PhoenixDatabaseMetaData.GUIDE_POSTS_WIDTH_BYTES,
                timeStamp, PLong.INSTANCE.toBytes(byteCount));
        put.addColumn(QueryConstants.DEFAULT_COLUMN_FAMILY_BYTES,
                PhoenixDatabaseMetaData.GUIDE_POSTS_ROW_COUNT_BYTES, timeStamp,
                PLong.INSTANCE.toBytes(rowCount));
        // Add our empty column value so queries behave correctly
        put.addColumn(QueryConstants.DEFAULT_COLUMN_FAMILY_BYTES, QueryConstants.EMPTY_COLUMN_BYTES, timeStamp,
                ByteUtil.EMPTY_BYTE_ARRAY);
        mutations.add(put);
    }

    private static MutationType getMutationType(Mutation m) throws IOException {
        if (m instanceof Put) {
            return MutationType.PUT;
        } else if (m instanceof Delete) {
            return MutationType.DELETE;
        } else {
            throw new DoNotRetryIOException("Unsupported mutation type in stats commit" + m.getClass().getName());
        }
    }

    public void commitStats(final List<Mutation> mutations, final StatisticsCollector statsCollector)
            throws IOException {
        User.runAsLoginUser(new PrivilegedExceptionAction<Void>() {
            @Override
            public Void run() throws Exception {
                commitLastStatsUpdatedTime(statsCollector);
                if (mutations.size() > 0) {
                    byte[] row = mutations.get(0).getRow();
                    MutateRowsRequest.Builder mrmBuilder = MutateRowsRequest.newBuilder();
                    for (Mutation m : mutations) {
                        mrmBuilder.addMutationRequest(ProtobufUtil.toMutation(getMutationType(m), m));
                    }
                    MutateRowsRequest mrm = mrmBuilder.build();
                    CoprocessorRpcChannel channel = statsWriterTable.coprocessorService(row);
                    MultiRowMutationService.BlockingInterface service = MultiRowMutationService
                            .newBlockingStub(channel);
                    try {
                        service.mutateRows(null, mrm);
                    } catch (ServiceException ex) {
                        ProtobufUtil.toIOException(ex);
                    }
                }
                return null;
            }
        });
    }

    private Put getLastStatsUpdatedTimePut(long timeStamp) {
        long currentTime = EnvironmentEdgeManager.currentTimeMillis();
        byte[] prefix = tableName;
        Put put = new Put(prefix);
        put.addColumn(QueryConstants.DEFAULT_COLUMN_FAMILY_BYTES, PhoenixDatabaseMetaData.LAST_STATS_UPDATE_TIME_BYTES,
                timeStamp, PDate.INSTANCE.toBytes(new Date(currentTime)));
        return put;
    }

    private void commitLastStatsUpdatedTime(StatisticsCollector statsCollector) throws IOException {
        long timeStamp = clientTimeStamp == StatisticsCollector.NO_TIMESTAMP ? statsCollector.getMaxTimeStamp() : clientTimeStamp;
        Put put = getLastStatsUpdatedTimePut(timeStamp);
        statsWriterTable.put(put);
    }

    public void deleteStatsForRegion(Region region, StatisticsCollector tracker, ImmutableBytesPtr fam,
            List<Mutation> mutations) throws IOException {
        long timeStamp =
                clientTimeStamp == DefaultStatisticsCollector.NO_TIMESTAMP
                        ? tracker.getMaxTimeStamp() : clientTimeStamp;
        byte[] startKey = region.getRegionInfo().getStartKey();
        byte[] stopKey = region.getRegionInfo().getEndKey();
        List<Result> statsForRegion = new ArrayList<Result>();
        Scan s =
                MetaDataUtil.newTableRowsScan(getAdjustedKey(startKey, tableName, fam, false),
                    getAdjustedKey(stopKey, tableName, fam, true),
                    MetaDataProtocol.MIN_TABLE_TIMESTAMP, clientTimeStamp);
        s.addColumn(QueryConstants.DEFAULT_COLUMN_FAMILY_BYTES, QueryConstants.EMPTY_COLUMN_BYTES);
        try (ResultScanner scanner = statsWriterTable.getScanner(s)) {
            Result result = null;
            while ((result = scanner.next()) != null) {
                statsForRegion.add(result);
            }
        }
        for (Result result : statsForRegion) {
            mutations.add(new Delete(result.getRow(), timeStamp - 1));
        }
    }

}
