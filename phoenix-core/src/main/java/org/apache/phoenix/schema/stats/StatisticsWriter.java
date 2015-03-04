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
import java.util.Collections;
import java.util.List;

import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.DoNotRetryIOException;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.HTableInterface;
import org.apache.hadoop.hbase.client.Mutation;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.coprocessor.RegionCoprocessorEnvironment;
import org.apache.hadoop.hbase.ipc.CoprocessorRpcChannel;
import org.apache.hadoop.hbase.protobuf.ProtobufUtil;
import org.apache.hadoop.hbase.protobuf.generated.ClientProtos.MutationProto.MutationType;
import org.apache.hadoop.hbase.protobuf.generated.MultiRowMutationProtos.MultiRowMutationService;
import org.apache.hadoop.hbase.protobuf.generated.MultiRowMutationProtos.MutateRowsRequest;
import org.apache.hadoop.hbase.regionserver.HRegion;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.phoenix.hbase.index.util.ImmutableBytesPtr;
import org.apache.phoenix.jdbc.PhoenixDatabaseMetaData;
import org.apache.phoenix.query.QueryConstants;
import org.apache.phoenix.schema.SortOrder;
import org.apache.phoenix.schema.types.PDate;
import org.apache.phoenix.schema.types.PLong;
import org.apache.phoenix.schema.types.PVarbinary;
import org.apache.phoenix.util.ByteUtil;
import org.apache.phoenix.util.ServerUtil;
import org.apache.phoenix.util.TimeKeeper;

import com.google.protobuf.ServiceException;

/**
 * Wrapper to access the statistics table SYSTEM.STATS using the HTable.
 */
public class StatisticsWriter implements Closeable {
    /**
     * @param tableName TODO
     * @param clientTimeStamp TODO
     * @return the {@link StatisticsWriter} for the given primary table.
     * @throws IOException
     *             if the table cannot be created due to an underlying HTable creation error
     */
    public static StatisticsWriter newWriter(RegionCoprocessorEnvironment env, String tableName, long clientTimeStamp) throws IOException {
        if (clientTimeStamp == HConstants.LATEST_TIMESTAMP) {
            clientTimeStamp = TimeKeeper.SYSTEM.getCurrentTime();
        }
        HTableInterface statsWriterTable = env.getTable(TableName.valueOf(PhoenixDatabaseMetaData.SYSTEM_STATS_NAME_BYTES));
        HTableInterface statsReaderTable = ServerUtil.getHTableForCoprocessorScan(env, statsWriterTable);
        StatisticsWriter statsTable = new StatisticsWriter(statsReaderTable, statsWriterTable, tableName, clientTimeStamp);
        if (clientTimeStamp != StatisticsCollector.NO_TIMESTAMP) { // Otherwise we do this later as we don't know the ts yet
            statsTable.commitLastStatsUpdatedTime();
        }
        return statsTable;
    }

    private final HTableInterface statsWriterTable;
    // In HBase 0.98.4 or above, the reader and writer will be the same.
    // In pre HBase 0.98.4, there was a bug in using the HTable returned
    // from a coprocessor for scans, so in that case it'll be different.
    private final HTableInterface statsReaderTable;
    private final byte[] tableName;
    private final long clientTimeStamp;

    private StatisticsWriter(HTableInterface statsReaderTable, HTableInterface statsWriterTable, String tableName, long clientTimeStamp) {
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
    }

    public void splitStats(HRegion p, HRegion l, HRegion r, StatisticsCollector tracker, ImmutableBytesPtr cfKey,
            List<Mutation> mutations) throws IOException {
        if (tracker == null) { return; }
        boolean useMaxTimeStamp = clientTimeStamp == StatisticsCollector.NO_TIMESTAMP;
        if (!useMaxTimeStamp) {
            mutations.add(getLastStatsUpdatedTimePut(clientTimeStamp));
        }
        long readTimeStamp = useMaxTimeStamp ? HConstants.LATEST_TIMESTAMP : clientTimeStamp;
        Result result = StatisticsUtil.readRegionStatistics(statsReaderTable, tableName, cfKey, p.getRegionName(),
                readTimeStamp);
        if (result != null && !result.isEmpty()) {
        	Cell cell = result.getColumnLatestCell(QueryConstants.DEFAULT_COLUMN_FAMILY_BYTES, PhoenixDatabaseMetaData.GUIDE_POSTS_BYTES);
        	Cell rowCountCell = result.getColumnLatestCell(QueryConstants.DEFAULT_COLUMN_FAMILY_BYTES, PhoenixDatabaseMetaData.GUIDE_POSTS_ROW_COUNT_BYTES);
            long rowCount = 0;
        	if (cell != null) {
                long writeTimeStamp = useMaxTimeStamp ? cell.getTimestamp() : clientTimeStamp;

                GuidePostsInfo guidePostsRegionInfo = GuidePostsInfo.deserializeGuidePostsInfo(cell.getValueArray(),
                        cell.getValueOffset(), cell.getValueLength(), rowCount);
                byte[] pPrefix = StatisticsUtil.getRowKey(tableName, cfKey, p.getRegionName());
                mutations.add(new Delete(pPrefix, writeTimeStamp));
                
	        	long byteSize = 0;
                Cell byteSizeCell = result.getColumnLatestCell(QueryConstants.DEFAULT_COLUMN_FAMILY_BYTES,
                        PhoenixDatabaseMetaData.GUIDE_POSTS_WIDTH_BYTES);
                int index = Collections.binarySearch(guidePostsRegionInfo.getGuidePosts(), r.getStartKey(),
                        Bytes.BYTES_COMPARATOR);
                int size = guidePostsRegionInfo.getGuidePosts().size();
                int midEndIndex, midStartIndex;
                if (index < 0) {
                    midEndIndex = midStartIndex = -(index + 1);
                } else {
                    // For an exact match, we want to get rid of the exact match guidepost,
                    // since it's replaced by the region boundary.
                    midEndIndex = index;
                    midStartIndex = index + 1;
                }
                double per = (double)(midEndIndex) / size;
                long leftRowCount = 0;
                long rightRowCount = 0;
                long leftByteCount = 0;
                long rightByteCount = 0;
                if (rowCountCell != null) {
                    rowCount = PLong.INSTANCE.getCodec().decodeLong(rowCountCell.getValueArray(),
                            rowCountCell.getValueOffset(), SortOrder.getDefault());
                    leftRowCount = (long)(per * rowCount);
                    rightRowCount = (long)((1 - per) * rowCount);
                }
                if (byteSizeCell != null) {
                    byteSize = PLong.INSTANCE.getCodec().decodeLong(byteSizeCell.getValueArray(),
                            byteSizeCell.getValueOffset(), SortOrder.getDefault());
                    leftByteCount = (long)(per * byteSize);
                    rightByteCount = (long)((1 - per) * byteSize);
                }
	            if (midEndIndex > 0) {
	                GuidePostsInfo lguidePosts = new GuidePostsInfo(leftByteCount, guidePostsRegionInfo
                            .getGuidePosts().subList(0, midEndIndex), leftRowCount);
                    tracker.clear();
	                tracker.addGuidePost(cfKey, lguidePosts, leftByteCount, cell.getTimestamp());
	                addStats(l.getRegionName(), tracker, cfKey, mutations);
	            }
	            if (midStartIndex < size) {
	                GuidePostsInfo rguidePosts = new GuidePostsInfo(rightByteCount, guidePostsRegionInfo
                            .getGuidePosts().subList(midStartIndex, size),
                            rightRowCount);
	                tracker.clear();
	                tracker.addGuidePost(cfKey, rguidePosts, rightByteCount, cell.getTimestamp());
	                addStats(r.getRegionName(), tracker, cfKey, mutations);
	            }
        	}
        }
    }
    
    /**
     * Update a list of statistics for a given region.  If the UPDATE STATISTICS <tablename> query is issued
     * then we use Upsert queries to update the table
     * If the region gets splitted or the major compaction happens we update using HTable.put()
     * @param tracker - the statistics tracker
     * @param cfKey -  the family for which the stats is getting collected.
     * @param mutations - list of mutations that collects all the mutations to commit in a batch
     * @throws IOException
     *             if we fail to do any of the puts. Any single failure will prevent any future attempts for the remaining list of stats to
     *             update
     */
    public void addStats(byte[] regionName, StatisticsCollector tracker, ImmutableBytesPtr cfKey,
            List<Mutation> mutations) throws IOException {
        if (tracker == null) { return; }
        boolean useMaxTimeStamp = clientTimeStamp == StatisticsCollector.NO_TIMESTAMP;
        long timeStamp = clientTimeStamp;
        if (useMaxTimeStamp) { // When using max timestamp, we write the update time later because we only know the ts now
            timeStamp = tracker.getMaxTimeStamp();
            mutations.add(getLastStatsUpdatedTimePut(timeStamp));
        }
        byte[] prefix = StatisticsUtil.getRowKey(tableName, cfKey, regionName);
        Put put = new Put(prefix);
        GuidePostsInfo gp = tracker.getGuidePosts(cfKey);
        if (gp != null) {
            put.add(QueryConstants.DEFAULT_COLUMN_FAMILY_BYTES, PhoenixDatabaseMetaData.GUIDE_POSTS_COUNT_BYTES,
                    timeStamp, PLong.INSTANCE.toBytes((gp.getGuidePosts().size())));
            put.add(QueryConstants.DEFAULT_COLUMN_FAMILY_BYTES, PhoenixDatabaseMetaData.GUIDE_POSTS_BYTES, timeStamp,
                    PVarbinary.INSTANCE.toBytes(gp.serializeGuidePostsInfo()));
            put.add(QueryConstants.DEFAULT_COLUMN_FAMILY_BYTES, PhoenixDatabaseMetaData.GUIDE_POSTS_WIDTH_BYTES,
                    timeStamp, PLong.INSTANCE.toBytes(gp.getByteCount()));
            // Write as long_array
            put.add(QueryConstants.DEFAULT_COLUMN_FAMILY_BYTES, PhoenixDatabaseMetaData.GUIDE_POSTS_ROW_COUNT_BYTES,
                    timeStamp, PLong.INSTANCE.toBytes(gp.getRowCount()));
        }
        // Add our empty column value so queries behave correctly
        put.add(QueryConstants.DEFAULT_COLUMN_FAMILY_BYTES, QueryConstants.EMPTY_COLUMN_BYTES, timeStamp,
                ByteUtil.EMPTY_BYTE_ARRAY);
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
            CoprocessorRpcChannel channel = statsWriterTable.coprocessorService(row);
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
        put.add(QueryConstants.DEFAULT_COLUMN_FAMILY_BYTES,
            PhoenixDatabaseMetaData.LAST_STATS_UPDATE_TIME_BYTES, timeStamp,
            PDate.INSTANCE.toBytes(new Date(currentTime)));
        return put;
    }

    private void commitLastStatsUpdatedTime() throws IOException {
        // Always use wallclock time for this, as it's a mechanism to prevent
        // stats from being collected too often.
        Put put = getLastStatsUpdatedTimePut(clientTimeStamp);
        statsWriterTable.put(put);
    }
    
    public void deleteStats(byte[] regionName, StatisticsCollector tracker, ImmutableBytesPtr fam, List<Mutation> mutations)
            throws IOException {
        long timeStamp = clientTimeStamp == StatisticsCollector.NO_TIMESTAMP ? tracker.getMaxTimeStamp() : clientTimeStamp;
        byte[] prefix = StatisticsUtil.getRowKey(tableName, fam, regionName);
        mutations.add(new Delete(prefix, timeStamp - 1));
    }
}
