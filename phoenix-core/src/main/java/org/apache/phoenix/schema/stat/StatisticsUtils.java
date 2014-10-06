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
import static org.apache.phoenix.util.SchemaUtil.getVarCharLength;

import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.TreeMap;

import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.HTableInterface;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.phoenix.coprocessor.MetaDataProtocol;
import org.apache.phoenix.jdbc.PhoenixDatabaseMetaData;
import org.apache.phoenix.query.QueryConstants;
import org.apache.phoenix.schema.PDataType;
import org.apache.phoenix.schema.PhoenixArray;
import org.apache.phoenix.util.ByteUtil;
import org.apache.phoenix.util.MetaDataUtil;

import com.google.common.collect.Lists;
/**
 * Simple utility class for managing multiple key parts of the statistic
 */
public class StatisticsUtils {
    private StatisticsUtils() {
        // private ctor for utility classes
    }

    /** Number of parts in our complex key */
    protected static final int NUM_KEY_PARTS = 3;

    public static byte[] getRowKey(byte[] table, byte[] fam, byte[] region) {
        // always starts with the source table
        byte[] rowKey = new byte[table.length + fam.length + region.length + 2];
        int offset = 0;
        System.arraycopy(table, 0, rowKey, offset, table.length);
        offset += table.length;
        rowKey[offset++] = QueryConstants.SEPARATOR_BYTE;
        System.arraycopy(fam, 0, rowKey, offset, fam.length);
        offset += fam.length;
        rowKey[offset++] = QueryConstants.SEPARATOR_BYTE;
        System.arraycopy(region, 0, rowKey, offset, region.length);
        return rowKey;
    }
    
    public static PTableStats readStatistics(HTableInterface statsHTable, byte[] tableNameBytes, long clientTimeStamp) throws IOException {
        ImmutableBytesWritable ptr = new ImmutableBytesWritable();
        Scan s = MetaDataUtil.newTableRowsScan(tableNameBytes, MetaDataProtocol.MIN_TABLE_TIMESTAMP, clientTimeStamp);
        s.addColumn(QueryConstants.DEFAULT_COLUMN_FAMILY_BYTES, PhoenixDatabaseMetaData.GUIDE_POSTS_BYTES);
        ResultScanner scanner = statsHTable.getScanner(s);
        try {
            Result result = null;
            TreeMap<byte[], List<byte[]>> guidePostsPerCf = new TreeMap<byte[], List<byte[]>>(Bytes.BYTES_COMPARATOR);
            while ((result = scanner.next()) != null) {
                KeyValue current = result.raw()[0];
                int tableNameLength = tableNameBytes.length + 1;
                int cfOffset = current.getRowOffset() + tableNameLength;
                int cfLength = getVarCharLength(current.getBuffer(), cfOffset, current.getRowLength() - tableNameLength);
                ptr.set(current.getBuffer(), cfOffset, cfLength);
                byte[] cfName = ByteUtil.copyKeyBytesIfNecessary(ptr);
                PhoenixArray array = (PhoenixArray)PDataType.VARBINARY_ARRAY.toObject(current.getBuffer(), current.getValueOffset(), current
                        .getValueLength());
                if (array != null && array.getDimensions() != 0) {
                    List<byte[]> guidePosts = Lists.newArrayListWithExpectedSize(array.getDimensions());                        
                    for (int j = 0; j < array.getDimensions(); j++) {
                        byte[] gp = array.toBytes(j);
                        if (gp.length != 0) {
                            guidePosts.add(gp);
                        }
                    }
                    List<byte[]> gps = guidePostsPerCf.put(cfName, guidePosts);
                    if (gps != null) { // Add guidepost already there from other regions
                        guidePosts.addAll(gps);
                    }
                }
            }
            if (!guidePostsPerCf.isEmpty()) {
                // Sort guideposts, as the order above will depend on the order we traverse
                // each region's worth of guideposts above.
                for (List<byte[]> gps : guidePostsPerCf.values()) {
                    Collections.sort(gps, Bytes.BYTES_COMPARATOR);
                }
                return new PTableStatsImpl(guidePostsPerCf);
            }
        } finally {
            scanner.close();
        }
        return PTableStatsImpl.NO_STATS;
    }
}