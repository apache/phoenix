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
import static org.apache.phoenix.coprocessor.BaseScannerRegionObserver.ANALYZE_TABLE;
import static org.apache.phoenix.schema.types.PDataType.TRUE_BYTES;
import static org.apache.phoenix.util.SchemaUtil.getVarCharLength;

import java.io.IOException;
import java.util.Map;
import java.util.Set;

import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellScanner;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.HTableInterface;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.phoenix.coprocessor.BaseScannerRegionObserver;
import org.apache.phoenix.coprocessor.MetaDataProtocol;
import org.apache.phoenix.jdbc.PhoenixDatabaseMetaData;
import org.apache.phoenix.query.QueryConstants;
import org.apache.phoenix.query.QueryServices;
import org.apache.phoenix.schema.SortOrder;
import org.apache.phoenix.schema.types.PInteger;
import org.apache.phoenix.schema.types.PLong;
import org.apache.phoenix.util.ByteUtil;
import org.apache.phoenix.util.MetaDataUtil;
import org.apache.phoenix.util.SchemaUtil;

import com.google.common.collect.Sets;

/**
 * Simple utility class for managing multiple key parts of the statistic
 */
public class StatisticsUtil {
    /**
     * Indication to client that the statistics estimates were not
     * calculated based on statistics but instead are based on row
     * limits from the query.
     */
    public static final long NOT_STATS_BASED_TS = 0;
    
    private static final Set<TableName> DISABLE_STATS = Sets.newHashSetWithExpectedSize(8);
    // TODO: make this declarative through new DISABLE_STATS column on SYSTEM.CATALOG table.
    // Also useful would be a USE_CURRENT_TIME_FOR_STATS column on SYSTEM.CATALOG table.
    static {
        DISABLE_STATS.add(TableName.valueOf(PhoenixDatabaseMetaData.SYSTEM_CATALOG_NAME));
        DISABLE_STATS.add(TableName.valueOf(PhoenixDatabaseMetaData.SYSTEM_FUNCTION_NAME));
        DISABLE_STATS.add(TableName.valueOf(PhoenixDatabaseMetaData.SYSTEM_SEQUENCE_NAME));
        DISABLE_STATS.add(TableName.valueOf(PhoenixDatabaseMetaData.SYSTEM_STATS_NAME));
        DISABLE_STATS.add(TableName.valueOf(PhoenixDatabaseMetaData.SYSTEM_TASK_NAME));
        DISABLE_STATS.add(SchemaUtil.getPhysicalTableName(PhoenixDatabaseMetaData.SYSTEM_CATALOG_NAME_BYTES,true));
        DISABLE_STATS.add(SchemaUtil.getPhysicalTableName(PhoenixDatabaseMetaData.SYSTEM_FUNCTION_NAME_BYTES,true));
        DISABLE_STATS.add(SchemaUtil.getPhysicalTableName(PhoenixDatabaseMetaData.SYSTEM_SEQUENCE_NAME_BYTES,true));
        DISABLE_STATS.add(SchemaUtil.getPhysicalTableName(PhoenixDatabaseMetaData.SYSTEM_STATS_NAME_BYTES,true));
        DISABLE_STATS.add(SchemaUtil.getPhysicalTableName(PhoenixDatabaseMetaData.SYSTEM_TASK_NAME_BYTES,true));
    }
    
    private StatisticsUtil() {
        // private ctor for utility classes
    }
    

    /** Number of parts in our complex key */
    protected static final int NUM_KEY_PARTS = 3;
    
    public static byte[] getRowKey(byte[] table, ImmutableBytesWritable fam, byte[] guidePostStartKey) {
        return getRowKey(table, fam, new ImmutableBytesWritable(guidePostStartKey,0,guidePostStartKey.length));
    }

    public static byte[] getRowKey(byte[] table, ImmutableBytesWritable fam, ImmutableBytesWritable guidePostStartKey) {
        // always starts with the source table
        int guidePostLength = guidePostStartKey.getLength();
        boolean hasGuidePost = guidePostLength > 0;
        byte[] rowKey = new byte[table.length + fam.getLength() + guidePostLength + (hasGuidePost ? 2 : 1)];
        int offset = 0;
        System.arraycopy(table, 0, rowKey, offset, table.length);
        offset += table.length;
        rowKey[offset++] = QueryConstants.SEPARATOR_BYTE; // assumes stats table columns not DESC
        System.arraycopy(fam.get(), fam.getOffset(), rowKey, offset, fam.getLength());
        if (hasGuidePost) {
            offset += fam.getLength();
            rowKey[offset++] = QueryConstants.SEPARATOR_BYTE; // assumes stats table columns not DESC
            System.arraycopy(guidePostStartKey.get(), 0, rowKey, offset, guidePostLength);
        }
        return rowKey;
    }

    private static byte[] getStartKey(byte[] table, ImmutableBytesWritable fam) {
        return getKey(table, fam, false);
    }
    
    private static byte[] getEndKey(byte[] table, ImmutableBytesWritable fam) {
        byte[] key = getKey(table, fam, true);
        ByteUtil.nextKey(key, key.length);
        return key;
    }
    
    private static byte[] getKey(byte[] table, ImmutableBytesWritable fam, boolean terminateWithSeparator) {
        // always starts with the source table and column family
        byte[] rowKey = new byte[table.length + fam.getLength() + 1 + (terminateWithSeparator ? 1 : 0)];
        int offset = 0;
        System.arraycopy(table, 0, rowKey, offset, table.length);
        offset += table.length;
        rowKey[offset++] = QueryConstants.SEPARATOR_BYTE; // assumes stats table columns not DESC
        System.arraycopy(fam.get(), fam.getOffset(), rowKey, offset, fam.getLength());
        offset += fam.getLength();
        if (terminateWithSeparator) {
            rowKey[offset] = QueryConstants.SEPARATOR_BYTE;
        }
        return rowKey;
    }

    public static byte[] getAdjustedKey(byte[] key, byte[] tableNameBytes, ImmutableBytesWritable cf, boolean nextKey) {
        if (Bytes.compareTo(key, ByteUtil.EMPTY_BYTE_ARRAY) != 0) {
            return getRowKey(tableNameBytes, cf, key); 
        }
        key = getKey(tableNameBytes, cf, nextKey);
        if (nextKey) {
            ByteUtil.nextKey(key, key.length);
        }
        return key;
    }

    public static GuidePostsInfo readStatistics(HTableInterface statsHTable, GuidePostsKey key, long clientTimeStamp)
            throws IOException {
        ImmutableBytesWritable ptr = new ImmutableBytesWritable();
        ptr.set(key.getColumnFamily());
        byte[] tableNameBytes = key.getPhysicalName();
        byte[] startKey = getStartKey(tableNameBytes, ptr);
        byte[] endKey = getEndKey(tableNameBytes, ptr);
        Scan s = MetaDataUtil.newTableRowsScan(startKey, endKey, MetaDataProtocol.MIN_TABLE_TIMESTAMP, clientTimeStamp);
        s.addColumn(QueryConstants.DEFAULT_COLUMN_FAMILY_BYTES, PhoenixDatabaseMetaData.GUIDE_POSTS_WIDTH_BYTES);
        s.addColumn(QueryConstants.DEFAULT_COLUMN_FAMILY_BYTES, PhoenixDatabaseMetaData.GUIDE_POSTS_ROW_COUNT_BYTES);
        s.addColumn(QueryConstants.DEFAULT_COLUMN_FAMILY_BYTES, QueryConstants.EMPTY_COLUMN_BYTES);
        GuidePostsInfoBuilder guidePostsInfoBuilder = new GuidePostsInfoBuilder();
        Cell current = null;
        GuidePostsInfo emptyGuidePost = null;
        try (ResultScanner scanner = statsHTable.getScanner(s)) {
            Result result = null;
            while ((result = scanner.next()) != null) {
                CellScanner cellScanner = result.cellScanner();
                long rowCount = 0;
                long byteCount = 0;
                 while (cellScanner.advance()) {
                    current = cellScanner.current();
                    if (Bytes.equals(current.getQualifierArray(), current.getQualifierOffset(),
                            current.getQualifierLength(), PhoenixDatabaseMetaData.GUIDE_POSTS_ROW_COUNT_BYTES, 0,
                            PhoenixDatabaseMetaData.GUIDE_POSTS_ROW_COUNT_BYTES.length)) {
                        rowCount = PLong.INSTANCE.getCodec().decodeLong(current.getValueArray(),
                                current.getValueOffset(), SortOrder.getDefault());
                    } else if (Bytes.equals(current.getQualifierArray(), current.getQualifierOffset(),
                            current.getQualifierLength(), PhoenixDatabaseMetaData.GUIDE_POSTS_WIDTH_BYTES, 0,
                            PhoenixDatabaseMetaData.GUIDE_POSTS_WIDTH_BYTES.length)) {
                        byteCount = PLong.INSTANCE.getCodec().decodeLong(current.getValueArray(),
                                current.getValueOffset(), SortOrder.getDefault());
                    }
                }
                if (current != null) {
                    int tableNameLength = tableNameBytes.length + 1;
                    int cfOffset = current.getRowOffset() + tableNameLength;
                    int cfLength = getVarCharLength(current.getRowArray(), cfOffset,
                            current.getRowLength() - tableNameLength);
                    ptr.set(current.getRowArray(), cfOffset, cfLength);
                    byte[] cfName = ByteUtil.copyKeyBytesIfNecessary(ptr);
                    byte[] newGPStartKey = getGuidePostsInfoFromRowKey(tableNameBytes, cfName, result.getRow());
                    boolean isEmptyGuidePost = GuidePostsInfo.isEmptyGpsKey(newGPStartKey);
                    // Use the timestamp of the cell as the time at which guidepost was
                    // created/updated
                    long guidePostUpdateTime = current.getTimestamp();
                    if (isEmptyGuidePost) {
                        emptyGuidePost =
                                GuidePostsInfo.createEmptyGuidePost(byteCount, guidePostUpdateTime);
                    } else {
                        guidePostsInfoBuilder.trackGuidePost(
                            new ImmutableBytesWritable(newGPStartKey), byteCount, rowCount,
                            guidePostUpdateTime);
                    }
                }
            }
        }
        // We write a row with an empty KeyValue in the case that stats were generated but without enough data
        // for any guideposts. If we have no rows, it means stats were never generated.
        return current == null ? GuidePostsInfo.NO_GUIDEPOST : guidePostsInfoBuilder.isEmpty() ? emptyGuidePost : guidePostsInfoBuilder.build();
    }

    public static long getGuidePostDepth(int guidepostPerRegion, long guidepostWidth, HTableDescriptor tableDesc) {
        if (guidepostPerRegion > 0) {
            long maxFileSize = HConstants.DEFAULT_MAX_FILE_SIZE;
            if (tableDesc != null) {
                long tableMaxFileSize = tableDesc.getMaxFileSize();
                if (tableMaxFileSize >= 0) {
                    maxFileSize = tableMaxFileSize;
                }
            }
            return maxFileSize / guidepostPerRegion;
        } else {
            return guidepostWidth;
        }
    }
    
    public static byte[] getGuidePostsInfoFromRowKey(byte[] tableNameBytes, byte[] fam, byte[] row) {
	    if (row.length > tableNameBytes.length + 1 + fam.length) {
    		ImmutableBytesWritable ptr = new ImmutableBytesWritable();
    		int gpOffset = tableNameBytes.length + 1 + fam.length + 1;
    		ptr.set(row, gpOffset, row.length - gpOffset);
    		return ByteUtil.copyKeyBytesIfNecessary(ptr);
	    }
	    return ByteUtil.EMPTY_BYTE_ARRAY;
	}

    public static boolean isStatsEnabled(TableName tableName) {
        return !DISABLE_STATS.contains(tableName);
    }

    public static void setScanAttributes(Scan scan, Map<String, Object> statsProps) {
        scan.setCacheBlocks(false);
        scan.setMaxVersions();
        scan.setAttribute(ANALYZE_TABLE, TRUE_BYTES);
        if (statsProps != null) {
            Object gp_width = statsProps.get(QueryServices.STATS_GUIDEPOST_WIDTH_BYTES_ATTRIB);
            if (gp_width != null) {
                scan.setAttribute(BaseScannerRegionObserver.GUIDEPOST_WIDTH_BYTES, PLong.INSTANCE.toBytes(gp_width));
            }
            Object gp_per_region = statsProps.get(QueryServices.STATS_GUIDEPOST_PER_REGION_ATTRIB);
            if (gp_per_region != null) {
                scan.setAttribute(BaseScannerRegionObserver.GUIDEPOST_PER_REGION, PInteger.INSTANCE.toBytes(gp_per_region));
            }
        }
    }
	
}