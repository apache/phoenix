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
import static org.apache.phoenix.util.SchemaUtil.getVarCharLength;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Set;
import java.util.SortedMap;
import java.util.TreeMap;

import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellScanner;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.HTableInterface;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.phoenix.coprocessor.MetaDataProtocol;
import org.apache.phoenix.hbase.index.util.ImmutableBytesPtr;
import org.apache.phoenix.jdbc.PhoenixDatabaseMetaData;
import org.apache.phoenix.query.QueryConstants;
import org.apache.phoenix.schema.SortOrder;
import org.apache.phoenix.schema.types.PLong;
import org.apache.phoenix.util.ByteUtil;
import org.apache.phoenix.util.MetaDataUtil;
import org.apache.phoenix.util.SchemaUtil;

import com.google.common.collect.Sets;

/**
 * Simple utility class for managing multiple key parts of the statistic
 */
public class StatisticsUtil {
    
    private static final Set<TableName> DISABLE_STATS = Sets.newHashSetWithExpectedSize(8);
    // TODO: make this declarative through new DISABLE_STATS column on SYSTEM.CATALOG table.
    // Also useful would be a USE_CURRENT_TIME_FOR_STATS column on SYSTEM.CATALOG table.
    static {
        DISABLE_STATS.add(TableName.valueOf(PhoenixDatabaseMetaData.SYSTEM_CATALOG_NAME));
        DISABLE_STATS.add(TableName.valueOf(PhoenixDatabaseMetaData.SYSTEM_FUNCTION_NAME));
        DISABLE_STATS.add(TableName.valueOf(PhoenixDatabaseMetaData.SYSTEM_SEQUENCE_NAME));
        DISABLE_STATS.add(TableName.valueOf(PhoenixDatabaseMetaData.SYSTEM_STATS_NAME));
        DISABLE_STATS.add(SchemaUtil.getPhysicalTableName(PhoenixDatabaseMetaData.SYSTEM_CATALOG_NAME_BYTES,true));
        DISABLE_STATS.add(SchemaUtil.getPhysicalTableName(PhoenixDatabaseMetaData.SYSTEM_FUNCTION_NAME_BYTES,true));
        DISABLE_STATS.add(SchemaUtil.getPhysicalTableName(PhoenixDatabaseMetaData.SYSTEM_SEQUENCE_NAME_BYTES,true));
        DISABLE_STATS.add(SchemaUtil.getPhysicalTableName(PhoenixDatabaseMetaData.SYSTEM_STATS_NAME_BYTES,true));
    }
    
    private StatisticsUtil() {
        // private ctor for utility classes
    }
    

    /** Number of parts in our complex key */
    protected static final int NUM_KEY_PARTS = 3;
    
    public static byte[] getRowKey(byte[] table, ImmutableBytesPtr fam, byte[] guidePostStartKey) {
        return getRowKey(table, fam, new ImmutableBytesWritable(guidePostStartKey,0,guidePostStartKey.length));
    }

    public static byte[] getRowKey(byte[] table, ImmutableBytesPtr fam, ImmutableBytesWritable guidePostStartKey) {
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

    public static byte[] getKey(byte[] table, ImmutableBytesPtr fam) {
        // always starts with the source table and column family
        byte[] rowKey = new byte[table.length + fam.getLength() + 1];
        int offset = 0;
        System.arraycopy(table, 0, rowKey, offset, table.length);
        offset += table.length;
        rowKey[offset++] = QueryConstants.SEPARATOR_BYTE; // assumes stats table columns not DESC
        System.arraycopy(fam.get(), fam.getOffset(), rowKey, offset, fam.getLength());
        offset += fam.getLength();
        return rowKey;
    }

    public static byte[] copyRow(KeyValue kv) {
        return Arrays.copyOfRange(kv.getRowArray(), kv.getRowOffset(), kv.getRowOffset() + kv.getRowLength());
    }

    public static byte[] getAdjustedKey(byte[] key, byte[] tableNameBytes, ImmutableBytesPtr cf, boolean nextKey) {
        if (Bytes.compareTo(key, ByteUtil.EMPTY_BYTE_ARRAY) != 0) { return getRowKey(tableNameBytes, cf, key); }
        key = ByteUtil.concat(getKey(tableNameBytes, cf), QueryConstants.SEPARATOR_BYTE_ARRAY);
        if (nextKey) {
            ByteUtil.nextKey(key, key.length);
        }
        return key;
    }

    public static List<Result> readStatistics(HTableInterface statsHTable, byte[] tableNameBytes, ImmutableBytesPtr cf,
            byte[] startKey, byte[] stopKey, long clientTimeStamp) throws IOException {
        List<Result> statsForRegion = new ArrayList<Result>();
        Scan s = MetaDataUtil.newTableRowsScan(getAdjustedKey(startKey, tableNameBytes, cf, false),
                getAdjustedKey(stopKey, tableNameBytes, cf, true), MetaDataProtocol.MIN_TABLE_TIMESTAMP,
                clientTimeStamp);
        s.addColumn(QueryConstants.DEFAULT_COLUMN_FAMILY_BYTES, QueryConstants.EMPTY_COLUMN_BYTES);
        ResultScanner scanner = null;
        try {
            scanner = statsHTable.getScanner(s);
            Result result = null;
            while ((result = scanner.next()) != null) {
                statsForRegion.add(result);
            }
        } finally {
            if (scanner != null) {
                scanner.close();
            }
        }
        return statsForRegion;
    }

    public static PTableStats readStatistics(HTableInterface statsHTable, byte[] tableNameBytes, long clientTimeStamp)
            throws IOException {
        ImmutableBytesWritable ptr = new ImmutableBytesWritable();
        Scan s = MetaDataUtil.newTableRowsScan(tableNameBytes, MetaDataProtocol.MIN_TABLE_TIMESTAMP, clientTimeStamp);
        s.addColumn(QueryConstants.DEFAULT_COLUMN_FAMILY_BYTES, PhoenixDatabaseMetaData.GUIDE_POSTS_WIDTH_BYTES);
        s.addColumn(QueryConstants.DEFAULT_COLUMN_FAMILY_BYTES, PhoenixDatabaseMetaData.GUIDE_POSTS_ROW_COUNT_BYTES);
        s.addColumn(QueryConstants.DEFAULT_COLUMN_FAMILY_BYTES, QueryConstants.EMPTY_COLUMN_BYTES);
        ResultScanner scanner = null;
        long timeStamp = MetaDataProtocol.MIN_TABLE_TIMESTAMP;
        TreeMap<byte[], GuidePostsInfoBuilder> guidePostsInfoWriterPerCf = new TreeMap<byte[], GuidePostsInfoBuilder>(Bytes.BYTES_COMPARATOR);
        try {
            scanner = statsHTable.getScanner(s);
            Result result = null;
            while ((result = scanner.next()) != null) {
                CellScanner cellScanner = result.cellScanner();
                long rowCount = 0;
                long byteCount = 0;
                byte[] cfName = null;
                int tableNameLength;
                int cfOffset;
                int cfLength;
                boolean valuesSet = false;
                // Only the two cells with quals GUIDE_POSTS_ROW_COUNT_BYTES and GUIDE_POSTS_BYTES would be retrieved
                while (cellScanner.advance()) {
                    Cell current = cellScanner.current();
                    if (!valuesSet) {
                        tableNameLength = tableNameBytes.length + 1;
                        cfOffset = current.getRowOffset() + tableNameLength;
                        cfLength = getVarCharLength(current.getRowArray(), cfOffset,
                                current.getRowLength() - tableNameLength);
                        ptr.set(current.getRowArray(), cfOffset, cfLength);
                        valuesSet = true;
                    }
                    cfName = ByteUtil.copyKeyBytesIfNecessary(ptr);
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
                    if (current.getTimestamp() > timeStamp) {
                        timeStamp = current.getTimestamp();
                    }
                }
                if (cfName != null) {
                    byte[] newGPStartKey = getGuidePostsInfoFromRowKey(tableNameBytes, cfName, result.getRow());
                    GuidePostsInfoBuilder guidePostsInfoWriter = guidePostsInfoWriterPerCf.get(cfName);
                    if (guidePostsInfoWriter == null) {
                        guidePostsInfoWriter = new GuidePostsInfoBuilder();
                        guidePostsInfoWriterPerCf.put(cfName, guidePostsInfoWriter);
                    }
                    guidePostsInfoWriter.addGuidePosts(newGPStartKey, byteCount, rowCount);
                }
            }
            if (!guidePostsInfoWriterPerCf.isEmpty()) { return new PTableStatsImpl(
                    getGuidePostsPerCf(guidePostsInfoWriterPerCf), timeStamp); }
        } finally {
            if (scanner != null) {
                scanner.close();
            }
        }
        return PTableStats.EMPTY_STATS;
    }

    private static SortedMap<byte[], GuidePostsInfo> getGuidePostsPerCf(
            TreeMap<byte[], GuidePostsInfoBuilder> guidePostsWriterPerCf) {
        TreeMap<byte[], GuidePostsInfo> guidePostsPerCf = new TreeMap<byte[], GuidePostsInfo>(Bytes.BYTES_COMPARATOR);
        for (byte[] key : guidePostsWriterPerCf.keySet()) {
            guidePostsPerCf.put(key, guidePostsWriterPerCf.get(key).build());
        }
        return guidePostsPerCf;
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
	
	
}