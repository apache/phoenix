/*
 * Copyright 2010 The Apache Software Foundation
 *
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
package org.apache.phoenix.util;

import static org.apache.phoenix.jdbc.PhoenixDatabaseMetaData.COLUMN_MODIFIER;
import static org.apache.phoenix.jdbc.PhoenixDatabaseMetaData.COLUMN_NAME;
import static org.apache.phoenix.jdbc.PhoenixDatabaseMetaData.DATA_TABLE_NAME;
import static org.apache.phoenix.jdbc.PhoenixDatabaseMetaData.DATA_TYPE;
import static org.apache.phoenix.jdbc.PhoenixDatabaseMetaData.IMMUTABLE_ROWS;
import static org.apache.phoenix.jdbc.PhoenixDatabaseMetaData.INDEX_STATE;
import static org.apache.phoenix.jdbc.PhoenixDatabaseMetaData.NULLABLE;
import static org.apache.phoenix.jdbc.PhoenixDatabaseMetaData.ORDINAL_POSITION;
import static org.apache.phoenix.jdbc.PhoenixDatabaseMetaData.SALT_BUCKETS;
import static org.apache.phoenix.jdbc.PhoenixDatabaseMetaData.TABLE_CAT_NAME;
import static org.apache.phoenix.jdbc.PhoenixDatabaseMetaData.TABLE_NAME_NAME;
import static org.apache.phoenix.jdbc.PhoenixDatabaseMetaData.TABLE_SCHEM_NAME;
import static org.apache.phoenix.jdbc.PhoenixDatabaseMetaData.TYPE_SCHEMA_AND_TABLE;
import static org.apache.phoenix.jdbc.PhoenixDatabaseMetaData.TYPE_TABLE_NAME;
import static org.apache.phoenix.jdbc.PhoenixDatabaseMetaData.TYPE_TABLE_NAME_BYTES;

import java.io.IOException;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.KeyValue.Type;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTableInterface;
import org.apache.hadoop.hbase.client.Mutation;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.io.encoding.DataBlockEncoding;
import org.apache.hadoop.hbase.regionserver.HRegion;
import org.apache.hadoop.hbase.regionserver.MultiVersionConsistencyControl;
import org.apache.hadoop.hbase.regionserver.RegionScanner;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.Pair;
import org.apache.phoenix.coprocessor.MetaDataProtocol;
import org.apache.phoenix.exception.SQLExceptionCode;
import org.apache.phoenix.exception.SQLExceptionInfo;
import org.apache.phoenix.jdbc.PhoenixConnection;
import org.apache.phoenix.jdbc.PhoenixDatabaseMetaData;
import org.apache.phoenix.parse.HintNode.Hint;
import org.apache.phoenix.query.ConnectionQueryServices;
import org.apache.phoenix.query.KeyRange;
import org.apache.phoenix.query.QueryConstants;
import org.apache.phoenix.schema.AmbiguousColumnException;
import org.apache.phoenix.schema.ColumnFamilyNotFoundException;
import org.apache.phoenix.schema.ColumnModifier;
import org.apache.phoenix.schema.ColumnNotFoundException;
import org.apache.phoenix.schema.PColumn;
import org.apache.phoenix.schema.PColumnFamily;
import org.apache.phoenix.schema.PDataType;
import org.apache.phoenix.schema.PDatum;
import org.apache.phoenix.schema.PMetaData;
import org.apache.phoenix.schema.PName;
import org.apache.phoenix.schema.PTable;
import org.apache.phoenix.schema.PTableType;
import org.apache.phoenix.schema.RowKeySchema;
import org.apache.phoenix.schema.RowKeySchema.RowKeySchemaBuilder;
import org.apache.phoenix.schema.SaltingUtil;
import org.apache.phoenix.schema.ValueSchema.Field;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;



/**
 * 
 * Static class for various schema-related utilities
 *
 * @author jtaylor
 * @since 0.1
 */
public class SchemaUtil {
    private static final Logger logger = LoggerFactory.getLogger(SchemaUtil.class);
    private static final int VAR_LENGTH_ESTIMATE = 10;
    
    public static final DataBlockEncoding DEFAULT_DATA_BLOCK_ENCODING = DataBlockEncoding.FAST_DIFF;
    public static final RowKeySchema VAR_BINARY_SCHEMA = new RowKeySchemaBuilder(1).addField(new PDatum() {
    
        @Override
        public boolean isNullable() {
            return false;
        }
    
        @Override
        public PDataType getDataType() {
            return PDataType.VARBINARY;
        }
    
        @Override
        public Integer getByteSize() {
            return null;
        }
    
        @Override
        public Integer getMaxLength() {
            return null;
        }
    
        @Override
        public Integer getScale() {
            return null;
        }
    
        @Override
        public ColumnModifier getColumnModifier() {
            return null;
        }
        
    }, false, null).build();
    
    /**
     * May not be instantiated
     */
    private SchemaUtil() {
    }

    /**
     * Join srcRow KeyValues to dstRow KeyValues.  For single-column mode, in the case of multiple
     * column families (which can happen for our conceptual PK to PK joins), we always use the
     * column family from the first dstRow.  TODO: we'll likely need a different coprocessor to
     * handle the PK join case.
     * @param srcRow the source list of KeyValues to join with dstRow KeyValues.  The key will be
     * changed upon joining to match the dstRow key.  The list may not be empty and is left unchanged.
     * @param dstRow the list of KeyValues to which the srcRows are joined.  The list is modified in
     * place and may not be empty.
     */
    public static void joinColumns(List<KeyValue> srcRow, List<KeyValue> dstRow) {
        assert(!dstRow.isEmpty());
        KeyValue dst = dstRow.get(0);
        byte[] dstBuf = dst.getBuffer();
        int dstKeyOffset = dst.getRowOffset();
        int dstKeyLength = dst.getRowLength();
        // Combine columns from both rows
        // The key for the cached KeyValues are modified to match the other key.
        for (KeyValue srcValue : srcRow) {
            byte[] srcBuf = srcValue.getBuffer();
            byte type = srcValue.getType();
            KeyValue dstValue = new KeyValue(dstBuf, dstKeyOffset, dstKeyLength,
                srcBuf, srcValue.getFamilyOffset(), srcValue.getFamilyLength(),
                srcBuf, srcValue.getQualifierOffset(), srcValue.getQualifierLength(),
                HConstants.LATEST_TIMESTAMP, KeyValue.Type.codeToType(type),
                srcBuf, srcValue.getValueOffset(), srcValue.getValueLength());
            dstRow.add(dstValue);
        }
        // Put KeyValues in proper sort order
        // TODO: our tests need this, but otherwise would this be required?
        Collections.sort(dstRow, KeyValue.COMPARATOR);
   }
    
    /**
     * Get the column value of a row.
     * @param result the Result return from iterating through scanner results.
     * @param fam the column family 
     * @param col the column qualifier
     * @param pos the column position
     * @param value updated in place to the bytes representing the column value
     * @return true if the column exists and value was set and false otherwise.
     */
    public static boolean getValue(Result result, byte[] fam, byte[] col, int pos, ImmutableBytesWritable value) {
        KeyValue keyValue = ResultUtil.getColumnLatest(result, fam, col);
        if (keyValue != null) {
            value.set(keyValue.getBuffer(),keyValue.getValueOffset(),keyValue.getValueLength());
            return true;
        }
        return false;
    }

    /**
     * Concatenate two PColumn arrays
     * @param first first array
     * @param second second array
     * @return new combined array
     */
    public static PColumn[] concat(PColumn[] first, PColumn[] second) {
        PColumn[] result = new PColumn[first.length + second.length];
        System.arraycopy(first, 0, result, 0, first.length);
        System.arraycopy(second, 0, result, first.length, second.length);
        return result;
    }
    
    public static boolean isPKColumn(PColumn column) {
        return column.getFamilyName() == null;
    }
    
    /**
     * Estimate the max key length in bytes of the PK for a given table
     * @param table the table
     * @return the max PK length
     */
    public static int estimateKeyLength(PTable table) {
        int maxKeyLength = 0;
        // Calculate the max length of a key (each part must currently be of a fixed width)
        int i = 0;
        List<PColumn> columns = table.getPKColumns();
        while (i < columns.size()) {
            PColumn keyColumn = columns.get(i++);
            Integer byteSize = keyColumn.getByteSize();
            maxKeyLength += (byteSize == null) ? VAR_LENGTH_ESTIMATE : byteSize;
        }
        return maxKeyLength;
    }

    /**
     * Normalize an identifier. If name is surrounded by double quotes,
     * it is used as-is, otherwise the name is upper caased.
     * @param name the parsed identifier
     * @return the normalized identifier
     */
    public static String normalizeIdentifier(String name) {
        if (name == null) {
            return name;
        }
        if (isCaseSensitive(name)) {
            // Don't upper case if in quotes
            return name.substring(1, name.length()-1);
        }
        return name.toUpperCase();
    }

    public static boolean isCaseSensitive(String name) {
        return name.length() > 0 && name.charAt(0)=='"';
    }
    
    public static <T> List<T> concat(List<T> l1, List<T> l2) {
        int size1 = l1.size();
        if (size1 == 0) {
            return l2;
        }
        int size2 = l2.size();
        if (size2 == 0) {
            return l1;
        }
        List<T> l3 = new ArrayList<T>(size1 + size2);
        l3.addAll(l1);
        l3.addAll(l2);
        return l3;
    }

    /**
     * Get the key used in the Phoenix metadata row for a table definition
     * @param schemaName
     * @param tableName
     */
    public static byte[] getTableKey(byte[] schemaName, byte[] tableName) {
        return ByteUtil.concat(schemaName, QueryConstants.SEPARATOR_BYTE_ARRAY, tableName);
    }

    public static byte[] getTableKey(String schemaName, String tableName) {
        return ByteUtil.concat(schemaName == null ? ByteUtil.EMPTY_BYTE_ARRAY : Bytes.toBytes(schemaName), QueryConstants.SEPARATOR_BYTE_ARRAY, Bytes.toBytes(tableName));
    }

    public static String getTableName(String schemaName, String tableName) {
        return getName(schemaName,tableName);
    }

    private static String getName(String optionalQualifier, String name) {
        if (optionalQualifier == null || optionalQualifier.isEmpty()) {
            return name;
        }
        return optionalQualifier + QueryConstants.NAME_SEPARATOR + name;
    }

    public static String getTableName(byte[] schemaName, byte[] tableName) {
        return getName(schemaName, tableName);
    }

    public static String getColumnDisplayName(byte[] cf, byte[] cq) {
        return getName(cf == null || cf.length == 0 || Bytes.compareTo(cf, QueryConstants.DEFAULT_COLUMN_FAMILY_BYTES) == 0 ? ByteUtil.EMPTY_BYTE_ARRAY : cf, cq);
    }

    public static String getColumnDisplayName(String cf, String cq) {
        return getName(cf == null || cf.isEmpty() || QueryConstants.DEFAULT_COLUMN_FAMILY.equals(cf) ? null : cf, cq);
    }

    public static String getMetaDataEntityName(String schemaName, String tableName, String familyName, String columnName) {
        if ((schemaName == null || schemaName.isEmpty()) && (tableName == null || tableName.isEmpty())) {
            return getName(familyName, columnName);
        }
        if ((familyName == null || familyName.isEmpty()) && (columnName == null || columnName.isEmpty())) {
            return getName(schemaName, tableName);
        }
        return getName(getName(schemaName, tableName), getName(familyName, columnName));
    }

    public static String getColumnName(String familyName, String columnName) {
        return getName(familyName, columnName);
    }

    public static byte[] getTableNameAsBytes(String schemaName, String tableName) {
        if (schemaName == null || schemaName.length() == 0) {
            return StringUtil.toBytes(tableName);
        }
        return getTableNameAsBytes(StringUtil.toBytes(schemaName),StringUtil.toBytes(tableName));
    }

    public static byte[] getTableNameAsBytes(byte[] schemaName, byte[] tableName) {
        return getNameAsBytes(schemaName, tableName);
    }

    private static byte[] getNameAsBytes(byte[] nameOne, byte[] nameTwo) {
        if (nameOne == null || nameOne.length == 0) {
            return nameTwo;
        } else if ((nameTwo == null || nameTwo.length == 0)) {
            return nameOne;
        } else {
            return ByteUtil.concat(nameOne, QueryConstants.NAME_SEPARATOR_BYTES, nameTwo);
        }
    }

    public static String getName(byte[] nameOne, byte[] nameTwo) {
        return Bytes.toString(getNameAsBytes(nameOne,nameTwo));
    }

    public static int getVarCharLength(byte[] buf, int keyOffset, int maxLength) {
        return getVarCharLength(buf, keyOffset, maxLength, 1);
    }

    public static int getVarCharLength(byte[] buf, int keyOffset, int maxLength, int skipCount) {
        int length = 0;
        for (int i=0; i<skipCount; i++) {
            while (length < maxLength && buf[keyOffset+length] != QueryConstants.SEPARATOR_BYTE) {
                length++;
            }
            if (i != skipCount-1) { // skip over the separator if it's not the last one.
                length++;
            }
        }
        return length;
    }

    public static int getVarChars(byte[] rowKey, byte[][] rowKeyMetadata) {
        return getVarChars(rowKey, 0, rowKey.length, 0, rowKeyMetadata);
    }
    
    public static int getVarChars(byte[] rowKey, int colMetaDataLength, byte[][] colMetaData) {
        return getVarChars(rowKey, 0, rowKey.length, 0, colMetaDataLength, colMetaData);
    }
    
    public static int getVarChars(byte[] rowKey, int keyOffset, int keyLength, int colMetaDataOffset, byte[][] colMetaData) {
        return getVarChars(rowKey, keyOffset, keyLength, colMetaDataOffset, colMetaData.length, colMetaData);
    }
    
    public static int getVarChars(byte[] rowKey, int keyOffset, int keyLength, int colMetaDataOffset, int colMetaDataLength, byte[][] colMetaData) {
        int i, offset = keyOffset;
        for (i = colMetaDataOffset; i < colMetaDataLength && keyLength > 0; i++) {
            int length = getVarCharLength(rowKey, offset, keyLength);
            byte[] b = new byte[length];
            System.arraycopy(rowKey, offset, b, 0, length);
            offset += length + 1;
            keyLength -= length + 1;
            colMetaData[i] = b;
        }
        return i;
    }
    
    public static String findExistingColumn(PTable table, List<PColumn> columns) {
        for (PColumn column : columns) {
            PName familyName = column.getFamilyName();
            if (familyName == null) {
                try {
                    return table.getPKColumn(column.getName().getString()).getName().getString();
                } catch (ColumnNotFoundException e) {
                    continue;
                }
            } else {
                try {
                    return table.getColumnFamily(familyName.getString()).getColumn(column.getName().getString()).getName().getString();
                } catch (ColumnFamilyNotFoundException e) {
                    continue; // Shouldn't happen
                } catch (ColumnNotFoundException e) {
                    continue;
                }
            }
        }
        return null;
    }

    public static String toString(byte[][] values) {
        if (values == null) {
            return "null";
        }
        StringBuilder buf = new StringBuilder("[");
        for (byte[] value : values) {
            buf.append(Bytes.toStringBinary(value));
            buf.append(',');
        }
        buf.setCharAt(buf.length()-1, ']');
        return buf.toString();
    }

    public static String toString(PDataType type, byte[] value) {
        boolean isString = type.isCoercibleTo(PDataType.VARCHAR);
        return isString ? ("'" + type.toObject(value).toString() + "'") : type.toObject(value).toString();
    }

    public static byte[] getEmptyColumnFamily(List<PColumnFamily> families) {
        return families.isEmpty() ? QueryConstants.EMPTY_COLUMN_BYTES : families.get(0).getName().getBytes();
    }

    public static boolean isMetaTable(byte[] tableName) {
        return Bytes.compareTo(tableName, TYPE_TABLE_NAME_BYTES) == 0;
    }
    
    public static boolean isMetaTable(String schemaName, String tableName) {
        return PhoenixDatabaseMetaData.TYPE_SCHEMA.equals(schemaName) && PhoenixDatabaseMetaData.TYPE_TABLE.equals(tableName);
    }

    // Given the splits and the rowKeySchema, find out the keys that 
    public static byte[][] processSplits(byte[][] splits, LinkedHashSet<PColumn> pkColumns, Integer saltBucketNum, boolean defaultRowKeyOrder) throws SQLException {
        // FIXME: shouldn't this return if splits.length == 0?
        if (splits == null) return null;
        // We do not accept user specified splits if the table is salted and we specify defaultRowKeyOrder. In this case,
        // throw an exception.
        if (splits.length > 0 && saltBucketNum != null && defaultRowKeyOrder) {
            throw new SQLExceptionInfo.Builder(SQLExceptionCode.NO_SPLITS_ON_SALTED_TABLE).build().buildException();
        }
        // If the splits are not specified and table is salted, pre-split the table. 
        if (splits.length == 0 && saltBucketNum != null) {
            splits = SaltingUtil.getSalteByteSplitPoints(saltBucketNum);
        }
        byte[][] newSplits = new byte[splits.length][];
        for (int i=0; i<splits.length; i++) {
            newSplits[i] = processSplit(splits[i], pkColumns); 
        }
        return newSplits;
    }

    // Go through each slot in the schema and try match it with the split byte array. If the split
    // does not confer to the schema, extends its length to match the schema.
    private static byte[] processSplit(byte[] split, LinkedHashSet<PColumn> pkColumns) {
        int pos = 0, offset = 0, maxOffset = split.length;
        Iterator<PColumn> iterator = pkColumns.iterator();
        while (pos < pkColumns.size()) {
            PColumn column = iterator.next();
            if (column.getDataType().isFixedWidth()) { // Fixed width
                int length = column.getByteSize();
                if (maxOffset - offset < length) {
                    // The split truncates the field. Fill in the rest of the part and any fields that
                    // are missing after this field.
                    int fillInLength = length - (maxOffset - offset);
                    fillInLength += estimatePartLength(pos + 1, iterator);
                    return ByteUtil.fillKey(split, split.length + fillInLength);
                }
                // Account for this field, move to next position;
                offset += length;
                pos++;
            } else { // Variable length
                // If we are the last slot, then we are done. Nothing needs to be filled in.
                if (pos == pkColumns.size() - 1) {
                    break;
                }
                while (offset < maxOffset && split[offset] != QueryConstants.SEPARATOR_BYTE) {
                    offset++;
                }
                if (offset == maxOffset) {
                    // The var-length field does not end with a separator and it's not the last field.
                    int fillInLength = 1; // SEPARATOR byte for the current var-length slot.
                    fillInLength += estimatePartLength(pos + 1, iterator);
                    return ByteUtil.fillKey(split, split.length + fillInLength);
                }
                // Move to the next position;
                offset += 1; // skip separator;
                pos++;
            }
        }
        return split;
    }

    // Estimate the key length after pos slot for schema.
    private static int estimatePartLength(int pos, Iterator<PColumn> iterator) {
        int length = 0;
        while (iterator.hasNext()) {
            PColumn column = iterator.next();
            if (column.getDataType().isFixedWidth()) {
                length += column.getByteSize();
            } else {
                length += 1; // SEPARATOR byte.
            }
        }
        return length;
    }
    
    public static final String UPGRADE_TO_2_0 = "UpgradeTo20";
    public static final Integer SYSTEM_TABLE_NULLABLE_VAR_LENGTH_COLUMNS = 3;
    public static final String UPGRADE_TO_2_1 = "UpgradeTo21";

    public static boolean isUpgradeTo2Necessary(ConnectionQueryServices connServices) throws SQLException {
        HTableInterface htable = connServices.getTable(PhoenixDatabaseMetaData.TYPE_TABLE_NAME_BYTES);
        try {
            return (htable.getTableDescriptor().getValue(SchemaUtil.UPGRADE_TO_2_0) == null);
        } catch (IOException e) {
            throw new SQLException(e);
        }
    }
    
    public static void upgradeTo2IfNecessary(HRegion region, int nColumns) throws IOException {
        Scan scan = new Scan();
        scan.setRaw(true);
        scan.setMaxVersions(MetaDataProtocol.DEFAULT_MAX_META_DATA_VERSIONS);
        RegionScanner scanner = region.getScanner(scan);
        int batchSizeBytes = 100 * 1024; // 100K chunks
        int sizeBytes = 0;
        List<Pair<Mutation,Integer>> mutations =  Lists.newArrayListWithExpectedSize(10000);
        MultiVersionConsistencyControl.setThreadReadPoint(scanner.getMvccReadPoint());
        region.startRegionOperation();
        try {
            List<KeyValue> result;
            do {
                result = Lists.newArrayList();
                scanner.nextRaw(result, null);
                for (KeyValue keyValue : result) {
                    KeyValue newKeyValue = SchemaUtil.upgradeTo2IfNecessary(nColumns, keyValue);
                    if (newKeyValue != null) {
                        sizeBytes += newKeyValue.getLength();
                        if (Type.codeToType(newKeyValue.getType()) == Type.Put) {
                            // Delete old value
                            byte[] buf = keyValue.getBuffer();
                            Delete delete = new Delete(keyValue.getRow());
                            KeyValue deleteKeyValue = new KeyValue(buf, keyValue.getRowOffset(), keyValue.getRowLength(),
                                    buf, keyValue.getFamilyOffset(), keyValue.getFamilyLength(),
                                    buf, keyValue.getQualifierOffset(), keyValue.getQualifierLength(),
                                    keyValue.getTimestamp(), Type.Delete,
                                    ByteUtil.EMPTY_BYTE_ARRAY,0,0);
                            delete.addDeleteMarker(deleteKeyValue);
                            mutations.add(new Pair<Mutation,Integer>(delete,null));
                            sizeBytes += deleteKeyValue.getLength();
                            // Put new value
                            Put put = new Put(newKeyValue.getRow());
                            put.add(newKeyValue);
                            mutations.add(new Pair<Mutation,Integer>(put,null));
                        } else if (Type.codeToType(newKeyValue.getType()) == Type.Delete){
                            // Copy delete marker using new key so that it continues
                            // to delete the key value preceding it that will be updated
                            // as well.
                            Delete delete = new Delete(newKeyValue.getRow());
                            delete.addDeleteMarker(newKeyValue);
                            mutations.add(new Pair<Mutation,Integer>(delete,null));
                        }
                        if (sizeBytes >= batchSizeBytes) {
                            commitBatch(region, mutations);
                            mutations.clear();
                            sizeBytes = 0;
                        }
                        
                    }
                }
            } while (!result.isEmpty());
            commitBatch(region, mutations);
        } finally {
            region.closeRegionOperation();
        }
    }
    
    private static void commitBatch(HRegion region, List<Pair<Mutation,Integer>> mutations) throws IOException {
        if (mutations.isEmpty()) {
            return;
        }
        @SuppressWarnings("unchecked")
        Pair<Mutation,Integer>[] mutationArray = new Pair[mutations.size()];
        // TODO: should we use the one that is all or none?
        region.batchMutate(mutations.toArray(mutationArray));
    }
    
    private static KeyValue upgradeTo2IfNecessary(int maxSeparators, KeyValue keyValue) {
        int originalLength = keyValue.getRowLength();
        int length = originalLength;
        int offset = keyValue.getRowOffset();
        byte[] buf = keyValue.getBuffer();
        while (originalLength - length < maxSeparators && buf[offset+length-1] == QueryConstants.SEPARATOR_BYTE) {
            length--;
        }
        if (originalLength == length) {
            return null;
        }
        return new KeyValue(buf, offset, length,
                buf, keyValue.getFamilyOffset(), keyValue.getFamilyLength(),
                buf, keyValue.getQualifierOffset(), keyValue.getQualifierLength(),
                keyValue.getTimestamp(), Type.codeToType(keyValue.getType()),
                buf, keyValue.getValueOffset(), keyValue.getValueLength());
    }

    public static int upgradeColumnCount(String url, Properties info) throws SQLException {
        String upgradeStr = JDBCUtil.findProperty(url, info, UPGRADE_TO_2_0);
        return (upgradeStr == null ? 0 : Integer.parseInt(upgradeStr));
    }

    public static boolean checkIfUpgradeTo2Necessary(ConnectionQueryServices connectionQueryServices, String url,
            Properties info) throws SQLException {
        boolean isUpgrade = upgradeColumnCount(url, info) > 0;
        boolean isUpgradeNecessary = isUpgradeTo2Necessary(connectionQueryServices);
        if (!isUpgrade && isUpgradeNecessary) {
            throw new SQLException("Please run the upgrade script in bin/updateTo2.sh to ensure your data is converted correctly to the 2.0 format");
        }
        if (isUpgrade && !isUpgradeNecessary) {
            info.remove(SchemaUtil.UPGRADE_TO_2_0); // Remove this property and ignore, since upgrade has already been done
            return false;
        }
        return isUpgradeNecessary;
    }

    public static String getEscapedTableName(String schemaName, String tableName) {
        if (schemaName == null || schemaName.length() == 0) {
            return "\"" + tableName + "\"";
        }
        return "\"" + schemaName + "\"." + "\"" + tableName + "\"";
    }

    private static PhoenixConnection addMetaDataColumn(PhoenixConnection conn, long scn, String columnDef) throws SQLException {
        String url = conn.getURL();
        Properties props = conn.getClientInfo();
        PMetaData metaData = conn.getPMetaData();
        props.setProperty(PhoenixRuntime.CURRENT_SCN_ATTRIB, Long.toString(scn));
        PhoenixConnection metaConnection = null;

        Statement stmt = null;
        try {
            metaConnection = new PhoenixConnection(conn.getQueryServices(), url, props, metaData);
            try {
                stmt = metaConnection.createStatement();
                stmt.executeUpdate("ALTER TABLE SYSTEM.\"TABLE\" ADD IF NOT EXISTS " + columnDef);
                return metaConnection;
            } finally {
                if(stmt != null) {
                    stmt.close();
                }
            }
        } finally {
            if(metaConnection != null) {
                metaConnection.close();
            }
        }
    }
    
    public static boolean columnExists(PTable table, String columnName) {
        try {
            table.getColumn(columnName);
            return true;
        } catch (ColumnNotFoundException e) {
            return false;
        } catch (AmbiguousColumnException e) {
            return true;
        }
    }
    
    public static void updateSystemTableTo2(PhoenixConnection metaConnection, PTable table) throws SQLException {
        PTable metaTable = metaConnection.getPMetaData().getTable(TYPE_TABLE_NAME);
        // Execute alter table statement for each column that was added if not already added
        if (table.getTimeStamp() < MetaDataProtocol.MIN_SYSTEM_TABLE_TIMESTAMP - 1) {
            // Causes row key of system table to be upgraded
            if (checkIfUpgradeTo2Necessary(metaConnection.getQueryServices(), metaConnection.getURL(), metaConnection.getClientInfo())) {
                metaConnection.createStatement().executeQuery("select count(*) from " + PhoenixDatabaseMetaData.TYPE_SCHEMA_AND_TABLE).next();
            }
            
            if (metaTable.getTimeStamp() < MetaDataProtocol.MIN_SYSTEM_TABLE_TIMESTAMP - 5 && !columnExists(table, COLUMN_MODIFIER)) {
                metaConnection = addMetaDataColumn(metaConnection, MetaDataProtocol.MIN_SYSTEM_TABLE_TIMESTAMP - 5, COLUMN_MODIFIER + " INTEGER NULL");
            }
            if (metaTable.getTimeStamp() < MetaDataProtocol.MIN_SYSTEM_TABLE_TIMESTAMP - 4 && !columnExists(table, SALT_BUCKETS)) {
                metaConnection = addMetaDataColumn(metaConnection, MetaDataProtocol.MIN_SYSTEM_TABLE_TIMESTAMP - 4, SALT_BUCKETS + " INTEGER NULL");
            }
            if (metaTable.getTimeStamp() < MetaDataProtocol.MIN_SYSTEM_TABLE_TIMESTAMP - 3 && !columnExists(table, DATA_TABLE_NAME)) {
                metaConnection = addMetaDataColumn(metaConnection, MetaDataProtocol.MIN_SYSTEM_TABLE_TIMESTAMP - 3, DATA_TABLE_NAME + " VARCHAR NULL");
            }
            if (metaTable.getTimeStamp() < MetaDataProtocol.MIN_SYSTEM_TABLE_TIMESTAMP - 2 && !columnExists(table, INDEX_STATE)) {
                metaConnection = addMetaDataColumn(metaConnection, MetaDataProtocol.MIN_SYSTEM_TABLE_TIMESTAMP - 2, INDEX_STATE + " VARCHAR NULL");
            }
            if (!columnExists(table, IMMUTABLE_ROWS)) {
                addMetaDataColumn(metaConnection, MetaDataProtocol.MIN_SYSTEM_TABLE_TIMESTAMP - 1, IMMUTABLE_ROWS + " BOOLEAN NULL");
            }
        }
    }
    
    public static void upgradeTo2(PhoenixConnection conn) throws SQLException {
        // Filter VIEWs from conversion
        StringBuilder buf = new StringBuilder();
        ResultSet rs = conn.getMetaData().getTables(null, null, null, new String[] {PTableType.VIEW.getSerializedValue()});
        while (rs.next()) {
            buf.append("(" + TABLE_SCHEM_NAME + " = " + rs.getString(2) + " and " + TABLE_NAME_NAME + " = " + rs.getString(3) + ") or ");
        }
        String filterViews = "";
        if (buf.length() > 0) {
            buf.setLength(buf.length() - " or ".length());
            filterViews = " and not (" + buf.toString() + ")";
        }
        /*
         * Our upgrade hack sets a property on the scan that gets activated by an ungrouped aggregate query. 
         * Our UngroupedAggregateRegionObserver coprocessors will perform the required upgrade.
         * The SYSTEM.TABLE will already have been upgraded, so walk through each user table and upgrade
         * any table with a nullable variable length column (VARCHAR or DECIMAL)
         * at the end.
         */
        String query = "select /*+" + Hint.NO_INTRA_REGION_PARALLELIZATION + "*/ " +
                TABLE_SCHEM_NAME + "," +
                TABLE_NAME_NAME + " ," +
                DATA_TYPE + "," +
                NULLABLE +
                " from " + TYPE_SCHEMA_AND_TABLE + 
                " where " + TABLE_CAT_NAME + " is null " +
                " and " + COLUMN_NAME + " is not null " + filterViews +
                " order by " + TABLE_SCHEM_NAME + "," + TABLE_NAME_NAME + "," + ORDINAL_POSITION + " DESC";
        rs = conn.createStatement().executeQuery(query);
        String currentTableName = null;
        int nColumns = 0;
        boolean skipToNext = false;
        Map<String,Integer> tablesToUpgrade = Maps.newHashMap();
        while (rs.next()) {
            String tableName = getEscapedTableName(rs.getString(1), rs.getString(2));
            if (currentTableName == null) {
                currentTableName = tableName;
            } else if (!currentTableName.equals(tableName)) {
                if (nColumns > 0) {
                    tablesToUpgrade.put(currentTableName, nColumns);
                    nColumns = 0;
                }
                currentTableName = tableName;
                skipToNext = false;
            } else if (skipToNext) {
                continue;
            }
            PDataType dataType = PDataType.fromSqlType(rs.getInt(3));
            if (dataType.isFixedWidth() || rs.getInt(4) != DatabaseMetaData.attributeNullable) {
                skipToNext = true;
            } else {
                nColumns++;
            }
        }
        
        for (Map.Entry<String, Integer> entry : tablesToUpgrade.entrySet()) {
            String msg = "Upgrading " + entry.getKey() + " for " + entry.getValue() + " columns";
            logger.info(msg);
            System.out.println(msg);
            Properties props = new Properties(conn.getClientInfo());
            props.setProperty(UPGRADE_TO_2_0, entry.getValue().toString());
            Connection newConn = DriverManager.getConnection(conn.getURL(), props);
            try {
                rs = newConn.createStatement().executeQuery("select /*+" + Hint.NO_INTRA_REGION_PARALLELIZATION + "*/ count(*) from " + entry.getKey());
                rs.next();
            } finally {
                newConn.close();
            }
        }
        
        ConnectionQueryServices connServices = conn.getQueryServices();
        HTableInterface htable = null;
        HBaseAdmin admin = connServices.getAdmin();
        try {
            htable = connServices.getTable(PhoenixDatabaseMetaData.TYPE_TABLE_NAME_BYTES);
            HTableDescriptor htd = new HTableDescriptor(htable.getTableDescriptor());
            htd.setValue(SchemaUtil.UPGRADE_TO_2_0, Boolean.TRUE.toString());
            admin.disableTable(PhoenixDatabaseMetaData.TYPE_TABLE_NAME_BYTES);
            admin.modifyTable(PhoenixDatabaseMetaData.TYPE_TABLE_NAME_BYTES, htd);
            admin.enableTable(PhoenixDatabaseMetaData.TYPE_TABLE_NAME_BYTES);
        } catch (IOException e) {
            throw new SQLException(e);
        } finally {
            try {
                try {
                    admin.close();
                } finally {
                    if(htable != null) {
                        htable.close();
                    }
                }
            } catch (IOException e) {
                throw new SQLException(e);
            }
        }
    }

    public static String getSchemaNameFromFullName(String tableName) {
        int index = tableName.indexOf(QueryConstants.NAME_SEPARATOR);
        if (index < 0) {
            return ""; 
        }
        return tableName.substring(0, index);
    }

    public static byte[] getTableKeyFromFullName(String fullTableName) {
        int index = fullTableName.indexOf(QueryConstants.NAME_SEPARATOR);
        if (index < 0) {
            return getTableKey(null, fullTableName); 
        }
        String schemaName = fullTableName.substring(0, index);
        String tableName = fullTableName.substring(index+1);
        return getTableKey(schemaName, tableName); 
    }
    
    private static int getTerminatorCount(RowKeySchema schema) {
        int nTerminators = 0;
        for (int i = 0; i < schema.getFieldCount(); i++) {
            Field field = schema.getField(i);
            // We won't have a terminator on the last PK column
            // unless it is variable length and exclusive, but
            // having the extra byte irregardless won't hurt anything
            if (!field.getDataType().isFixedWidth()) {
                nTerminators++;
            }
        }
        return nTerminators;
    }

    public static int getMaxKeyLength(RowKeySchema schema, List<List<KeyRange>> slots) {
        int maxKeyLength = getTerminatorCount(schema);
        for (List<KeyRange> slot : slots) {
            int maxSlotLength = 0;
            for (KeyRange range : slot) {
                int maxRangeLength = Math.max(range.getLowerRange().length, range.getUpperRange().length);
                if (maxSlotLength < maxRangeLength) {
                    maxSlotLength = maxRangeLength;
                }
            }
            maxKeyLength += maxSlotLength;
        }
        return maxKeyLength;
    }
}
