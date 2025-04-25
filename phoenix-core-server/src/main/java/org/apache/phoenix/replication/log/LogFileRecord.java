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

import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.client.Append;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Increment;
import org.apache.hadoop.hbase.client.Mutation;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.phoenix.replication.log.LogFile;

public class LogFileRecord implements LogFile.Record {

    private LogFile.MutationType mutationType;
    private String schemaObjectName;
    private long commitId;
    private byte[] row;
    private long timestamp;
    // Nested map: Family -> Qualifier -> Value
    private Map<byte[], Map<byte[], byte[]>> familyMap;
    private int serializedLength;

    public LogFileRecord() {
        this.familyMap = new TreeMap<>(Bytes.BYTES_COMPARATOR);
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
        this.familyMap.clear();
        return this;
    }

    /**
     * Adds a column family, qualifier, and value to this record.
     *
     * @param family    The column family byte array.
     * @param qualifier The column qualifier byte array.
     * @param value     The value byte array.
     * @return This Record instance for chaining.
     */
    @Override
    public LogFile.Record addColumnValue(byte[] family, byte[] qualifier, byte[] value) {
        Map<byte[], byte[]> qualifierMap = this.familyMap.get(family);
        if (qualifierMap == null) {
            qualifierMap = new TreeMap<>(Bytes.BYTES_COMPARATOR);
            this.familyMap.put(family, qualifierMap);
        }
        qualifierMap.put(qualifier, value);
        return this;
    }

    @Override
    public int getColumnCount() {
        int count = 0;
        for (Map<byte[], byte[]> e: familyMap.values()) {
            count += e.size();
        }
        return count;
    }

    @Override
    public Iterable<Map.Entry<byte[], Map<byte[], byte[]>>> getColumnValues() {
        return Collections.unmodifiableMap(familyMap).entrySet();
    }

    @Override
    public int hashCode() {
        int code = mutationType.hashCode();
        code ^= schemaObjectName.hashCode();
        code ^= Long.hashCode(commitId);
        code ^= Long.hashCode(timestamp);
        code ^= Bytes.hashCode(row);
        code ^= nestedMapHashCode(familyMap);
        return code;
    }

    private static int nestedMapHashCode(Map<byte[], Map<byte[], byte[]>> map) {
        int code = 0;
        for (Map.Entry<byte[], Map<byte[], byte[]>> familyEntry : map.entrySet()) {
            int innerCode = 0;
            for (Map.Entry<byte[], byte[]> qualEntry : familyEntry.getValue().entrySet()) {
                innerCode += Bytes.hashCode(qualEntry.getKey())
                    ^ Bytes.hashCode(qualEntry.getValue());
            }
            code += Bytes.hashCode(familyEntry.getKey()) ^ innerCode;
        }
        return code;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        LogFileRecord record = (LogFileRecord) o;
        return commitId == record.commitId &&
            timestamp == record.timestamp &&
            mutationType == record.mutationType &&
            schemaObjectName.equals(record.schemaObjectName) &&
            Bytes.equals(row, record.row) &&
            nestedMapsEqual(familyMap, record.familyMap);
    }

    private static boolean nestedMapsEqual(Map<byte[], Map<byte[], byte[]>> map1, Map<byte[],
            Map<byte[], byte[]>> map2) {
        if (map1.size() != map2.size()) {
            return false;
        }
        for (Map.Entry<byte[], Map<byte[], byte[]>> entry1 : map1.entrySet()) {
            byte[] key1 = entry1.getKey();
            Map<byte[], byte[]> innerMap1 = entry1.getValue();
            Map<byte[], byte[]> innerMap2 = map2.get(key1);
            if (innerMap2 == null || innerMap1.size() != innerMap2.size()) {
                return false;
            }
            for (Map.Entry<byte[], byte[]> innerEntry1 : innerMap1.entrySet()) {
                byte[] innerKey1 = innerEntry1.getKey();
                byte[] value1 = innerEntry1.getValue();
                byte[] value2 = innerMap2.get(innerKey1);
                if (!Bytes.equals(value1, value2)) {
                    return false;
                }
            }
        }
        return true;
    }

    @Override
    public String toString() {
        return "LogFileRecord [mutationType=" + mutationType + ", schemaObjectName="
            + schemaObjectName + ", commitId=" + commitId + ", rowKey=" + Bytes.toStringBinary(row)
            + ", timestamp=" + timestamp + ", columnValues=[" + nestedMapToString(familyMap)
            + "] ]";
    }

    private static String nestedMapToString(Map<byte[], Map<byte[], byte[]>> map) {
        StringBuilder sb = new StringBuilder();
        sb.append("[");
        boolean firstFamily = true;
        Iterator<Map.Entry<byte[], Map<byte[], byte[]>>> familyI = map.entrySet().iterator();
        while (familyI.hasNext()) {
            Map.Entry<byte[], Map<byte[], byte[]>> entry = familyI.next();
            if (!firstFamily) {
                sb.append("; "); // Separator for families
            }
            firstFamily = false;
            sb.append(Bytes.toStringBinary(entry.getKey())).append(" -> [");
            boolean firstQual = true;
            Iterator<Map.Entry<byte[], byte[]>> qualI = entry.getValue().entrySet().iterator();
            while (qualI.hasNext()) {
                Map.Entry<byte[], byte[]> qualEntry = qualI.next();
                if (!firstQual) {
                    sb.append(", "); // Separator for qualifiers
                }
                firstQual = false;
                sb.append(Bytes.toStringBinary(qualEntry.getKey()));
                sb.append('=');
                sb.append(Bytes.toStringBinary(qualEntry.getValue()));
            }
            sb.append("]");
        }
        sb.append("]");
        return sb.toString();
    }

    // Conversion methods

    public static LogFile.Record fromHBaseMutation(String schemaObject, Mutation mutation,
            long commitId) {
        LogFileRecord record = new LogFileRecord();
        record.setSchemaObjectName(schemaObject);
        record.setCommitId(commitId);
        record.setRowKey(mutation.getRow());
        record.setTimestamp(mutation.getTimestamp());
        // Determine mutation type
        if (mutation instanceof Put) {
            record.setMutationType(LogFile.MutationType.PUT);
        } else if (mutation instanceof Delete) {
            // Need to determine the specific delete type from the first cell
            List<Cell> familyCells = mutation.getFamilyCellMap().isEmpty() ? null
                : mutation.getFamilyCellMap().firstEntry().getValue();
            if (familyCells == null || familyCells.isEmpty()) {
                 // This implies a full row delete (Delete object with no specific cfs)
                 record.setMutationType(LogFile.MutationType.DELETE);
            } else {
                 Cell cell = familyCells.get(0);
                 // NOTE: An invariant check we could perform here would ensure all delete cells
                 // actually have the same type as implied by this code, but the enumeration might
                 // be quite expensive.
                 record.setMutationType(LogFile.MutationType.fromHBaseCellType(cell.getType()));
            }
        } else if (mutation instanceof Append || mutation instanceof Increment) {
            throw new UnsupportedOperationException(
                "Append and Increment mutations are not supported");
        } else {
            throw new UnsupportedOperationException(
                "Unknown mutation type " + mutation.getClass().getName());
        }
        // Populate the familyMap
        mutation.getFamilyCellMap().forEach((family, cells) -> {
            family = Bytes.copy(family);
            Map<byte[], byte[]> qualifierMap = record.familyMap.computeIfAbsent(family,
                k -> new TreeMap<>(Bytes.BYTES_COMPARATOR));
            cells.forEach((cell) ->
                qualifierMap.put(CellUtil.cloneQualifier(cell), CellUtil.cloneValue(cell)));
        });
        return record;
    }

    private static Put toHBasePut(LogFile.Record record) {
        Put put = new Put(record.getRowKey());
        put.setTimestamp(record.getTimestamp());
        record.getColumnValues().iterator().forEachRemaining((e) -> {
            byte[] col = e.getKey();
            e.getValue().forEach((qual, value) ->
                put.addColumn(col, qual, record.getTimestamp(), value));
        });
        return put;
    }

    private static Delete toHBaseDelete(LogFile.Record record) {
        Delete delete = new Delete(record.getRowKey());
        delete.setTimestamp(record.getTimestamp());
        return delete;
    }

    private static Delete toHBaseDeleteColumn(LogFile.Record record) {
        Delete delete = new Delete(record.getRowKey());
        delete.setTimestamp(record.getTimestamp());
        record.getColumnValues().iterator().forEachRemaining((e) -> {
            byte[] col = e.getKey();
            e.getValue().forEach((qual, value) -> delete.addColumn(col, qual));
        });
        return delete;
    }

    private static Delete toHBaseDeleteFamily(LogFile.Record record) {
        Delete delete = new Delete(record.getRowKey());
        delete.setTimestamp(record.getTimestamp());
        Set<byte[]> families = new TreeSet<>(Bytes.BYTES_COMPARATOR);
        record.getColumnValues().iterator().forEachRemaining((e) -> families.add(e.getKey()));
        families.forEach(family -> delete.addFamily(family));
        return delete;
    }

    private static Mutation toHBaseDeleteFamilyVersion(LogFile.Record record) {
        Delete delete = new Delete(record.getRowKey());
        delete.setTimestamp(record.getTimestamp());
        Set<byte[]> families = new TreeSet<>(Bytes.BYTES_COMPARATOR);
        record.getColumnValues().iterator().forEachRemaining((e) -> families.add(e.getKey()));
        families.forEach(family -> delete.addFamilyVersion(family, record.getTimestamp()));
        return delete;
    }

    public static Mutation toHBaseMutation(LogFile.Record record) {
      switch (record.getMutationType()) {
          case PUT:
              return toHBasePut(record);
          case DELETE:
              return toHBaseDelete(record);
          case DELETE_COLUMN:
              return toHBaseDeleteColumn(record);
          case DELETE_FAMILY:
              return toHBaseDeleteFamily(record);
          case DELETE_FAMILY_VERSION:
              return toHBaseDeleteFamilyVersion(record);
          default:
              throw new UnsupportedOperationException("Unhandled mutation type: " + record.getMutationType());
      }
    }

}
