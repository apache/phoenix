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
import java.util.Arrays;
import java.util.TreeMap;

import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellScanner;
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
import org.apache.phoenix.util.ByteUtil;
import org.apache.phoenix.util.MetaDataUtil;
/**
 * Simple utility class for managing multiple key parts of the statistic
 */
public class StatisticsUtil {
    private StatisticsUtil() {
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
    
    public static byte[] copyRow(KeyValue kv) {
        return Arrays.copyOfRange(kv.getRowArray(), kv.getRowOffset(), kv.getRowOffset() + kv.getRowLength());
    }

    public static PTableStats readStatistics(HTableInterface statsHTable, byte[] tableNameBytes, long clientTimeStamp)
            throws IOException {
        ImmutableBytesWritable ptr = new ImmutableBytesWritable();
        Scan s = MetaDataUtil.newTableRowsScan(tableNameBytes, MetaDataProtocol.MIN_TABLE_TIMESTAMP, clientTimeStamp);
        s.addColumn(QueryConstants.DEFAULT_COLUMN_FAMILY_BYTES, PhoenixDatabaseMetaData.GUIDE_POSTS_BYTES);
        ResultScanner scanner = statsHTable.getScanner(s);
        Result result = null;
        long timeStamp = MetaDataProtocol.MIN_TABLE_TIMESTAMP;
        TreeMap<byte[], GuidePostsInfo> guidePostsPerCf = new TreeMap<byte[], GuidePostsInfo>(Bytes.BYTES_COMPARATOR);
        while ((result = scanner.next()) != null) {
            CellScanner cellScanner = result.cellScanner();
            while (cellScanner.advance()) {
                Cell current = cellScanner.current();
                int tableNameLength = tableNameBytes.length + 1;
                int cfOffset = current.getRowOffset() + tableNameLength;
                int cfLength = getVarCharLength(current.getRowArray(), cfOffset, current.getRowLength()
                        - tableNameLength);
                ptr.set(current.getRowArray(), cfOffset, cfLength);
                byte[] cfName = ByteUtil.copyKeyBytesIfNecessary(ptr);
                GuidePostsInfo newInfo = GuidePostsInfo.fromBytes(current.getValueArray(), current.getValueOffset(), current.getValueLength());
                GuidePostsInfo oldInfo = guidePostsPerCf.put(cfName, newInfo);
                if (oldInfo != null) {
                    newInfo.combine(oldInfo);
                }
                if (current.getTimestamp() > timeStamp) {
                    timeStamp = current.getTimestamp();
                }
            }
        }
        if (!guidePostsPerCf.isEmpty()) {
            return new PTableStatsImpl(guidePostsPerCf, timeStamp);
        }
        return PTableStats.EMPTY_STATS;
    }
}