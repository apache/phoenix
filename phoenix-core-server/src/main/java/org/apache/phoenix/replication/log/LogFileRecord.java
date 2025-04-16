/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.phoenix.replication.log;

import java.util.Arrays;
import java.util.Collections;
import java.util.Map;
import java.util.Objects;
import java.util.TreeMap;
import org.apache.hadoop.hbase.util.Bytes;

public class LogFileRecord implements LogFile.Record {

    private LogFile.MutationType mutationType;
    private String schemaObjectName;
    private long commitId;
    private byte[] row;
    private long timestamp;
    private Map<byte[], byte[]> columnValues;
    private int serializedLength;

    public LogFileRecord() {
        this.columnValues = new TreeMap<>(Bytes.BYTES_COMPARATOR);
    }

    @Override
    public LogFile.MutationType getMutationType() {
        return mutationType;
    }

    @Override
    public LogFile.Record setMutationType(LogFile.MutationType mutationType) {
        this.mutationType = mutationType;
        return this;
    }

    @Override
    public String getSchemaObjectName() {
        return schemaObjectName;
    }

    @Override
    public LogFile.Record setSchemaObjectName(String schemaObjectName) {
        this.schemaObjectName = schemaObjectName;
        return this;
    }

    @Override
    public long getCommitId() {
        return commitId;
    }

    @Override
    public LogFile.Record setCommitId(long commitId) {
        this.commitId = commitId;
        return this;
    }

    @Override
    public byte[] getRowKey() {
        return row;
    }

    @Override
    public LogFile.Record setRowKey(byte[] rowKey) {
        this.row = rowKey;
        return this;
    }

    @Override
    public long getTimestamp() {
        return timestamp;
    }

    @Override
    public LogFile.Record setTimestamp(long timestamp) {
        this.timestamp = timestamp;
        return this;
    }

    @Override
    public int getSerializedLength() {
        // NOTE: Should be set by the Codec using setSerializedLength after reading or writing
        // the record.
        return this.serializedLength;
    }

    @Override
    public LogFile.Record setSerializedLength(int serializedLength) {
        this.serializedLength = serializedLength;
        return this;
    }

    @Override
    public LogFile.Record clearColumnValues() {
        this.columnValues.clear();
        return this;
    }

    @Override
    public LogFile.Record addColumnValue(byte[] columnName, byte[] value) {
        this.columnValues.put(columnName, value);
        return this;
    }

    @Override
    public int getColumnCount() {
        return columnValues.size();
    }

    @Override
    public Iterable<Map.Entry<byte[], byte[]>> getColumnValues() {
        return Collections.unmodifiableMap(columnValues).entrySet();
    }

    @Override
    public int hashCode() {
        int result = Objects.hash(mutationType, schemaObjectName, commitId, timestamp);
        result = 31 * result + Arrays.hashCode(row);
        result = 31 * result + mapHashCode(columnValues);
        return result;
    }

    // No handy guava or etc helper for this that I am aware of
    private static int mapHashCode(Map<byte[], byte[]> map) {
        int h = 0;
        for (Map.Entry<byte[], byte[]> entry : map.entrySet()) {
            h += Arrays.hashCode(entry.getKey()) ^ Arrays.hashCode(entry.getValue());
        }
        return h;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        LogFileRecord record = (LogFileRecord) o;
        return commitId == record.commitId
            && timestamp == record.timestamp
            && mutationType == record.mutationType
            && Objects.equals(schemaObjectName, record.schemaObjectName)
            && Arrays.equals(row, record.row)
            && mapsEqual(columnValues, record.columnValues);
    }

    // Same
    private static boolean mapsEqual(Map<byte[], byte[]> map1, Map<byte[], byte[]> map2) {
        if (map1.size() != map2.size()) {
            return false;
        }
        for (Map.Entry<byte[], byte[]> entry : map1.entrySet()) {
            byte[] key = entry.getKey();
            byte[] value1 = entry.getValue();
            byte[] value2 = map2.get(key); // We assume key comparison works (use TreeMaps!)
            if (!Arrays.equals(value1, value2)) {
                return false;
            }
        }
        return true;
    }

    @Override
    public String toString() {
        return "LogFileRecord [mutationType=" + mutationType + ", schemaObjectName="
            + schemaObjectName + ", commitId=" + commitId + ", rowKey=" + Bytes.toStringBinary(row)
            + ", timestamp=" + timestamp + ", columnValues=[" + mapToString(columnValues) + "]]";
    }

    private static String mapToString(Map<byte[], byte[]> map) {
        StringBuilder sb = new StringBuilder();
        boolean first = true;
        for (Map.Entry<byte[], byte[]> entry: map.entrySet()) {
            if (first) {
                first = false;
            } else {
                sb.append(", ");
            }
            sb.append(Bytes.toStringBinary(entry.getKey()));
            sb.append('=');
            sb.append(Bytes.toStringBinary(entry.getValue()));
        }
        return sb.toString();
    }

}
