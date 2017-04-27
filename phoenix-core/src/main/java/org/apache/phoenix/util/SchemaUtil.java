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
package org.apache.phoenix.util;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.base.Strings.isNullOrEmpty;
import static org.apache.phoenix.jdbc.PhoenixDatabaseMetaData.SYSTEM_CATALOG_NAME_BYTES;
import static org.apache.phoenix.jdbc.PhoenixDatabaseMetaData.SYSTEM_FUNCTION_NAME_BYTES;
import static org.apache.phoenix.jdbc.PhoenixDatabaseMetaData.SYSTEM_STATS_NAME_BYTES;

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;

import javax.annotation.Nullable;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.io.encoding.DataBlockEncoding;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.phoenix.exception.DataExceedsCapacityException;
import org.apache.phoenix.exception.SQLExceptionCode;
import org.apache.phoenix.exception.SQLExceptionInfo;
import org.apache.phoenix.expression.Expression;
import org.apache.phoenix.hbase.index.util.ImmutableBytesPtr;
import org.apache.phoenix.jdbc.PhoenixConnection;
import org.apache.phoenix.jdbc.PhoenixDatabaseMetaData;
import org.apache.phoenix.query.KeyRange;
import org.apache.phoenix.query.QueryConstants;
import org.apache.phoenix.query.QueryServices;
import org.apache.phoenix.query.QueryServicesOptions;
import org.apache.phoenix.schema.AmbiguousColumnException;
import org.apache.phoenix.schema.ColumnFamilyNotFoundException;
import org.apache.phoenix.schema.ColumnNotFoundException;
import org.apache.phoenix.schema.MetaDataClient;
import org.apache.phoenix.schema.PColumn;
import org.apache.phoenix.schema.PColumnFamily;
import org.apache.phoenix.schema.PDatum;
import org.apache.phoenix.schema.PName;
import org.apache.phoenix.schema.PNameFactory;
import org.apache.phoenix.schema.PTable;
import org.apache.phoenix.schema.PTableType;
import org.apache.phoenix.schema.RowKeySchema;
import org.apache.phoenix.schema.PTable.QualifierEncodingScheme;
import org.apache.phoenix.schema.PTable.ImmutableStorageScheme;
import org.apache.phoenix.schema.RowKeySchema.RowKeySchemaBuilder;
import org.apache.phoenix.schema.SaltingUtil;
import org.apache.phoenix.schema.SortOrder;
import org.apache.phoenix.schema.TableProperty;
import org.apache.phoenix.schema.ValueSchema.Field;
import org.apache.phoenix.schema.types.PDataType;
import org.apache.phoenix.schema.types.PVarbinary;
import org.apache.phoenix.schema.types.PVarchar;

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;

/**
 * 
 * Static class for various schema-related utilities
 *
 * 
 * @since 0.1
 */
public class SchemaUtil {
    private static final int VAR_LENGTH_ESTIMATE = 10;
    private static final int VAR_KV_LENGTH_ESTIMATE = 50;
    public static final String ESCAPE_CHARACTER = "\"";
    public static final DataBlockEncoding DEFAULT_DATA_BLOCK_ENCODING = DataBlockEncoding.FAST_DIFF;
    public static final PDatum VAR_BINARY_DATUM = new PDatum() {
    
        @Override
        public boolean isNullable() {
            return false;
        }
    
        @Override
        public PDataType getDataType() {
            return PVarbinary.INSTANCE;
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
        public SortOrder getSortOrder() {
            return SortOrder.getDefault();
        }
        
    };
    public static final RowKeySchema VAR_BINARY_SCHEMA = new RowKeySchemaBuilder(1).addField(VAR_BINARY_DATUM, false, SortOrder.getDefault()).build();
    public static final String SCHEMA_FOR_DEFAULT_NAMESPACE = "DEFAULT";
    public static final String HBASE_NAMESPACE = "HBASE";
    public static final List<String> NOT_ALLOWED_SCHEMA_LIST = Arrays.asList(SCHEMA_FOR_DEFAULT_NAMESPACE,
            HBASE_NAMESPACE);
    
    /**
     * May not be instantiated
     */
    private SchemaUtil() {
    }

    public static boolean isPKColumn(PColumn column) {
        return column.getFamilyName() == null;
    }
  
    /**
     * Imperfect estimate of row size given a PTable
     * TODO: keep row count in stats table and use total size / row count instead
     * @param table
     * @return estimate of size in bytes of a row
     */
    public static long estimateRowSize(PTable table) {
    	int keyLength = estimateKeyLength(table);
    	long rowSize = 0;
    	for (PColumn column : table.getColumns()) {
    		if (!SchemaUtil.isPKColumn(column)) {
                PDataType type = column.getDataType();
                Integer maxLength = column.getMaxLength();
                int valueLength = !type.isFixedWidth() ? VAR_KV_LENGTH_ESTIMATE : maxLength == null ? type.getByteSize() : maxLength;
    			rowSize += KeyValue.getKeyValueDataStructureSize(keyLength, column.getFamilyName().getBytes().length, column.getName().getBytes().length, valueLength);
    		}
    	}
    	byte[] emptyKeyValueKV = EncodedColumnsUtil.getEmptyKeyValueInfo(table).getFirst();
    	// Empty key value
    	rowSize += KeyValue.getKeyValueDataStructureSize(keyLength, getEmptyColumnFamily(table).length, emptyKeyValueKV.length, 0);
    	return rowSize;
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
            PDataType type = keyColumn.getDataType();
            Integer maxLength = keyColumn.getMaxLength();
            maxKeyLength += !type.isFixedWidth() ? VAR_LENGTH_ESTIMATE : maxLength == null ? type.getByteSize() : maxLength;
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
    
    /**
     * Normalizes the fulltableName . Uses {@linkplain normalizeIdentifier}
     * @param fullTableName
     * @return
     */
    public static String normalizeFullTableName(String fullTableName) {
        String schemaName = SchemaUtil.getSchemaNameFromFullName(fullTableName);
        String tableName = SchemaUtil.getTableNameFromFullName(fullTableName);
        String normalizedTableName = StringUtil.EMPTY_STRING;
        if(!schemaName.isEmpty()) {
            normalizedTableName =  normalizeIdentifier(schemaName) + QueryConstants.NAME_SEPARATOR;
        }
        return normalizedTableName + normalizeIdentifier(tableName);
    }

    public static boolean isCaseSensitive(String name) {
        return name!=null && name.length() > 0 && name.charAt(0)=='"';
    }
    
    private static boolean isExistingTableMappedToPhoenixName(String name) {
        return name != null && name.length() > 0 && name.charAt(0) == '"' && name.indexOf("\"", 1) == name.length() - 1;
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
    public static byte[] getTableKey(byte[] tenantId, byte[] schemaName, byte[] tableName) {
        return ByteUtil.concat(tenantId, QueryConstants.SEPARATOR_BYTE_ARRAY, schemaName, QueryConstants.SEPARATOR_BYTE_ARRAY, tableName);
    }

    /**
     * Get the key used in the Phoenix function data row for a function definition
     * @param tenantId
     * @param functionName
     */
    public static byte[] getFunctionKey(byte[] tenantId, byte[] functionName) {
        return ByteUtil.concat(tenantId, QueryConstants.SEPARATOR_BYTE_ARRAY, functionName);
    }

    public static byte[] getKeyForSchema(String tenantId, String schemaName) {
        return ByteUtil.concat(tenantId == null ? ByteUtil.EMPTY_BYTE_ARRAY : Bytes.toBytes(tenantId),
                QueryConstants.SEPARATOR_BYTE_ARRAY,
                schemaName == null ? ByteUtil.EMPTY_BYTE_ARRAY : Bytes.toBytes(schemaName));
    }

    public static byte[] getTableKey(String tenantId, String schemaName, String tableName) {
        return ByteUtil.concat(tenantId == null  ? ByteUtil.EMPTY_BYTE_ARRAY : Bytes.toBytes(tenantId), QueryConstants.SEPARATOR_BYTE_ARRAY, schemaName == null ? ByteUtil.EMPTY_BYTE_ARRAY : Bytes.toBytes(schemaName), QueryConstants.SEPARATOR_BYTE_ARRAY, Bytes.toBytes(tableName));
    }

    public static byte[] getColumnKey(String tenantId, String schemaName, String tableName, String columnName, String familyName) {
        Preconditions.checkNotNull(columnName,"Column name cannot be null");
        if (familyName == null) {
            return ByteUtil.concat(tenantId == null  ? ByteUtil.EMPTY_BYTE_ARRAY : Bytes.toBytes(tenantId),
                    QueryConstants.SEPARATOR_BYTE_ARRAY, schemaName == null ? ByteUtil.EMPTY_BYTE_ARRAY : Bytes.toBytes(schemaName), 
                    QueryConstants.SEPARATOR_BYTE_ARRAY, Bytes.toBytes(tableName),
                    QueryConstants.SEPARATOR_BYTE_ARRAY, Bytes.toBytes(columnName));
        }
        return ByteUtil.concat(tenantId == null  ? ByteUtil.EMPTY_BYTE_ARRAY : Bytes.toBytes(tenantId),
                QueryConstants.SEPARATOR_BYTE_ARRAY, schemaName == null ? ByteUtil.EMPTY_BYTE_ARRAY : Bytes.toBytes(schemaName), 
                QueryConstants.SEPARATOR_BYTE_ARRAY, Bytes.toBytes(tableName),
                QueryConstants.SEPARATOR_BYTE_ARRAY, Bytes.toBytes(columnName),
                QueryConstants.SEPARATOR_BYTE_ARRAY, Bytes.toBytes(familyName));
    }

    public static String getTableName(String schemaName, String tableName) {
        return getName(schemaName,tableName, false);
    }

    private static String getName(String optionalQualifier, String name, boolean caseSensitive) {
        String cq = caseSensitive ? "\"" + name + "\"" : name;
        if (optionalQualifier == null || optionalQualifier.isEmpty()) {
            return cq;
        }
        String cf = caseSensitive ? "\"" + optionalQualifier + "\"" : optionalQualifier;
        return cf + QueryConstants.NAME_SEPARATOR + cq;
    }

    private static String getName(String name, boolean caseSensitive) {
        String cq = caseSensitive ? "\"" + name + "\"" : name;
        return cq;
    }

    public static String getTableName(byte[] schemaName, byte[] tableName) {
        return getName(schemaName, tableName);
    }

    public static String getColumnDisplayName(byte[] cf, byte[] cq) {
        return getName(cf == null || cf.length == 0 ? ByteUtil.EMPTY_BYTE_ARRAY : cf, cq);
    }

    public static String getColumnDisplayName(String cf, String cq) {
        return getName(cf == null || cf.isEmpty() ? null : cf, cq, false);
    }
    
    public static String getCaseSensitiveColumnDisplayName(String cf, String cq) {
        return getName(cf == null || cf.isEmpty() ? null : cf, cq, true);
    }

    public static String getMetaDataEntityName(String schemaName, String tableName, String familyName, String columnName) {
        if ((schemaName == null || schemaName.isEmpty()) && (tableName == null || tableName.isEmpty())) {
            if (columnName == null || columnName.isEmpty()) {
                return familyName;
            }
            return getName(familyName, columnName, false);
        }
        if ((familyName == null || familyName.isEmpty()) && (columnName == null || columnName.isEmpty())
                && (tableName == null || tableName.equals(MetaDataClient.EMPTY_TABLE))) { return getName(schemaName,
                        false); }
        if ((familyName == null || familyName.isEmpty()) && (columnName == null || columnName.isEmpty())) {
            return getName(schemaName, tableName, false);
        }

        return getName(getName(schemaName, tableName, false), getName(familyName, columnName, false), false);
    }

    public static String getColumnName(String familyName, String columnName) {
        return getName(familyName, columnName, false);
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
                    return table.getColumnFamily(familyName.getString()).getPColumnForColumnName(column.getName().getString()).getName().getString();
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
        return toString(type, value, 0, value.length);
    }

    public static String toString(PDataType type, ImmutableBytesWritable value) {
        return toString(type, value.get(), value.getOffset(), value.getLength());
    }

    public static String toString(PDataType type, byte[] value, int offset, int length) {
        boolean isString = type.isCoercibleTo(PVarchar.INSTANCE);
        return isString ? ("'" + type.toObject(value).toString() + "'") : type.toObject(value, offset, length).toString();
    }

    public static byte[] getEmptyColumnFamily(PName defaultColumnFamily, List<PColumnFamily> families) {
        return families.isEmpty() ? defaultColumnFamily == null ? QueryConstants.DEFAULT_COLUMN_FAMILY_BYTES : defaultColumnFamily.getBytes() : families.get(0).getName().getBytes();
    }

    public static byte[] getEmptyColumnFamily(PTable table) {
        List<PColumnFamily> families = table.getColumnFamilies();
        return families.isEmpty() ? table.getDefaultFamilyName() == null ? QueryConstants.DEFAULT_COLUMN_FAMILY_BYTES : table.getDefaultFamilyName().getBytes() : families.get(0).getName().getBytes();
    }

    public static String getEmptyColumnFamilyAsString(PTable table) {
        List<PColumnFamily> families = table.getColumnFamilies();
        return families.isEmpty() ? table.getDefaultFamilyName() == null ? QueryConstants.DEFAULT_COLUMN_FAMILY : table.getDefaultFamilyName().getString() : families.get(0).getName().getString();
    }

    public static ImmutableBytesPtr getEmptyColumnFamilyPtr(PTable table) {
        List<PColumnFamily> families = table.getColumnFamilies();
        return families.isEmpty() ? table.getDefaultFamilyName() == null ? QueryConstants.DEFAULT_COLUMN_FAMILY_BYTES_PTR : table.getDefaultFamilyName().getBytesPtr() : families.get(0)
                .getName().getBytesPtr();
    }

    public static boolean isMetaTable(byte[] tableName) {
        return Bytes.compareTo(tableName, SYSTEM_CATALOG_NAME_BYTES) == 0 || Bytes.compareTo(tableName,
                SchemaUtil.getPhysicalTableName(SYSTEM_CATALOG_NAME_BYTES, true).getName()) == 0;
    }

    public static boolean isFunctionTable(byte[] tableName) {
        return Bytes.compareTo(tableName, SYSTEM_FUNCTION_NAME_BYTES) == 0 || Bytes.compareTo(tableName,
                SchemaUtil.getPhysicalTableName(SYSTEM_FUNCTION_NAME_BYTES, true).getName()) == 0;
    }

    public static boolean isStatsTable(byte[] tableName) {
        return Bytes.compareTo(tableName, SYSTEM_STATS_NAME_BYTES) == 0 || Bytes.compareTo(tableName,
                SchemaUtil.getPhysicalTableName(SYSTEM_STATS_NAME_BYTES, true).getName()) == 0;
    }

    public static boolean isSequenceTable(byte[] tableName) {
        return Bytes.compareTo(tableName, PhoenixDatabaseMetaData.SYSTEM_SEQUENCE_NAME_BYTES) == 0
                || Bytes.compareTo(tableName, SchemaUtil
                        .getPhysicalTableName(PhoenixDatabaseMetaData.SYSTEM_SEQUENCE_NAME_BYTES, true).getName()) == 0;
    }

    public static boolean isSequenceTable(PTable table) {
        return PhoenixDatabaseMetaData.SYSTEM_SEQUENCE_NAME.equals(table.getName().getString());
    }

    public static boolean isMetaTable(PTable table) {
        return PhoenixDatabaseMetaData.SYSTEM_CATALOG_SCHEMA.equals(table.getSchemaName().getString()) && PhoenixDatabaseMetaData.SYSTEM_CATALOG_TABLE.equals(table.getTableName().getString());
    }
    
    public static boolean isMetaTable(byte[] schemaName, byte[] tableName) {
        return Bytes.compareTo(schemaName, PhoenixDatabaseMetaData.SYSTEM_CATALOG_SCHEMA_BYTES) == 0 && Bytes.compareTo(tableName, PhoenixDatabaseMetaData.SYSTEM_CATALOG_TABLE_BYTES) == 0;
    }
    
    public static boolean isMetaTable(String schemaName, String tableName) {
        return PhoenixDatabaseMetaData.SYSTEM_CATALOG_SCHEMA.equals(schemaName) && PhoenixDatabaseMetaData.SYSTEM_CATALOG_TABLE.equals(tableName);
    }

    public static boolean isSystemTable(byte[] fullTableName) {
        String schemaName = SchemaUtil.getSchemaNameFromFullName(fullTableName);
        if (QueryConstants.SYSTEM_SCHEMA_NAME.equals(schemaName)) return true;
        return false;
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
                int length = SchemaUtil.getFixedByteSize(column);
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
                length += SchemaUtil.getFixedByteSize(column);
            } else {
                length += 1; // SEPARATOR byte.
            }
        }
        return length;
    }
    
    public static String getEscapedTableName(String schemaName, String tableName) {
        if (schemaName == null || schemaName.length() == 0) {
            return "\"" + tableName + "\"";
        }
        return "\"" + schemaName + "\"." + "\"" + tableName + "\"";
    }

    protected static PhoenixConnection addMetaDataColumn(PhoenixConnection conn, long scn, String columnDef) throws SQLException {
        PhoenixConnection metaConnection = null;
        Statement stmt = null;
        try {
            metaConnection = new PhoenixConnection(conn.getQueryServices(), conn, scn);
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
            table.getColumnForColumnName(columnName);
            return true;
        } catch (ColumnNotFoundException e) {
            return false;
        } catch (AmbiguousColumnException e) {
            return true;
        }
    }

    public static String getSchemaNameFromFullName(String tableName) {
        if (isExistingTableMappedToPhoenixName(tableName)) { return StringUtil.EMPTY_STRING; }
        if (tableName.contains(QueryConstants.NAMESPACE_SEPARATOR)) { return getSchemaNameFromFullName(tableName,
                QueryConstants.NAMESPACE_SEPARATOR); }
        return getSchemaNameFromFullName(tableName, QueryConstants.NAME_SEPARATOR);
    }

    public static String getSchemaNameFromFullName(String tableName, String separator) {
        int index = tableName.indexOf(separator);
        if (index < 0) {
            return StringUtil.EMPTY_STRING; 
        }
        return tableName.substring(0, index);
    }
    
    private static int indexOf (byte[] bytes, byte b) {
        for (int i = 0; i < bytes.length; i++) {
            if (bytes[i] == b) {
                return i;
            }
        }
        return -1;
    }
    
    public static String getSchemaNameFromFullName(byte[] tableName) {
        if (tableName == null) {
            return null;
        }
        if (isExistingTableMappedToPhoenixName(Bytes.toString(tableName))) { return StringUtil.EMPTY_STRING; }
        int index = indexOf(tableName, QueryConstants.NAME_SEPARATOR_BYTE);
        if (index < 0) {
            index = indexOf(tableName, QueryConstants.NAMESPACE_SEPARATOR_BYTE);
            if (index < 0) { return StringUtil.EMPTY_STRING; }
        }
        return Bytes.toString(tableName, 0, index);
    }
    
    public static String getTableNameFromFullName(byte[] tableName) {
        if (tableName == null) {
            return null;
        }
        if (isExistingTableMappedToPhoenixName(Bytes.toString(tableName))) { return Bytes.toString(tableName); }
        int index = indexOf(tableName, QueryConstants.NAME_SEPARATOR_BYTE);
        if (index < 0) {
            index = indexOf(tableName, QueryConstants.NAMESPACE_SEPARATOR_BYTE);
            if (index < 0) { return Bytes.toString(tableName); }
        }
        return Bytes.toString(tableName, index+1, tableName.length - index - 1);
    }

    public static String getTableNameFromFullName(String tableName) {
        if (isExistingTableMappedToPhoenixName(tableName)) { return tableName; }
        if (tableName.contains(QueryConstants.NAMESPACE_SEPARATOR)) { return getTableNameFromFullName(tableName,
                QueryConstants.NAMESPACE_SEPARATOR); }
        return getTableNameFromFullName(tableName, QueryConstants.NAME_SEPARATOR);
    }

    public static String getTableNameFromFullName(String tableName, String separator) {
        int index = tableName.indexOf(separator);
        if (index < 0) {
            return tableName; 
        }
        return tableName.substring(index+1, tableName.length());
    }

    public static byte[] getTableKeyFromFullName(String fullTableName) {
        int index = fullTableName.indexOf(QueryConstants.NAME_SEPARATOR);
        if (index < 0) {
            index = fullTableName.indexOf(QueryConstants.NAMESPACE_SEPARATOR);
            if (index < 0) { return getTableKey(null, null, fullTableName); }
        }
        String schemaName = fullTableName.substring(0, index);
        String tableName = fullTableName.substring(index+1);
        return getTableKey(null, schemaName, tableName); 
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
        int maxKeyLength = getTerminatorCount(schema) * 2;
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

    public static int getFixedByteSize(PDatum e) {
        assert(e.getDataType().isFixedWidth());
        Integer maxLength = e.getMaxLength();
        return maxLength == null ? e.getDataType().getByteSize() : maxLength;
    }
    
    public static short getMaxKeySeq(PTable table) {
        int offset = 0;
        if (table.getBucketNum() != null) {
            offset++;
        }
        // TODO: for tenant-specific table on tenant-specific connection,
        // we should subtract one for tenant column and another one for
        // index ID
        return (short)(table.getPKColumns().size() - offset);
    }

    public static int getPKPosition(PTable table, PColumn column) {
        // TODO: when PColumn has getPKPosition, use that instead
        return table.getPKColumns().indexOf(column);
    }
    
    public static String getEscapedFullColumnName(String fullColumnName) {
        if(fullColumnName.startsWith(ESCAPE_CHARACTER)) {
            return fullColumnName;
        }
        int index = fullColumnName.indexOf(QueryConstants.NAME_SEPARATOR);
        if (index < 0) {
            return getEscapedArgument(fullColumnName); 
        }
        String columnFamily = fullColumnName.substring(0,index);
        String columnName = fullColumnName.substring(index+1);
        return getEscapedArgument(columnFamily) + QueryConstants.NAME_SEPARATOR + getEscapedArgument(columnName) ;
    }
    
    public static String getEscapedFullTableName(String fullTableName) {
        final String schemaName = getSchemaNameFromFullName(fullTableName);
        final String tableName = getTableNameFromFullName(fullTableName);
        return getEscapedTableName(schemaName, tableName);
    }
    
    /**
     * Escapes the given argument with {@value #ESCAPE_CHARACTER}
     * @param argument any non null value.
     * @return 
     */
    public static String getEscapedArgument(String argument) {
        Preconditions.checkNotNull(argument,"Argument passed cannot be null");
        return ESCAPE_CHARACTER + argument + ESCAPE_CHARACTER;
    }
    
    /**
     * 
     * @return a fully qualified column name in the format: "CFNAME"."COLNAME" or "COLNAME" depending on whether or not
     * there is a column family name present. 
     */
    public static String getQuotedFullColumnName(PColumn pCol) {
        checkNotNull(pCol);
        String columnName = pCol.getName().getString();
        String columnFamilyName = pCol.getFamilyName() != null ? pCol.getFamilyName().getString() : null;
        return getQuotedFullColumnName(columnFamilyName, columnName);
    }
    
    /**
     * 
     * @return a fully qualified column name in the format: "CFNAME"."COLNAME" or "COLNAME" depending on whether or not
     * there is a column family name present. 
     */
    public static String getQuotedFullColumnName(@Nullable String columnFamilyName, String columnName) {
        checkArgument(!isNullOrEmpty(columnName), "Column name cannot be null or empty");
        return columnFamilyName == null ? ("\"" + columnName + "\"") : ("\"" + columnFamilyName + "\"" + QueryConstants.NAME_SEPARATOR + "\"" + columnName + "\"");
    }

    public static boolean hasHTableDescriptorProps(Map<String, Object> tableProps) {
        int pTablePropCount = 0;
        for (String prop : tableProps.keySet()) {
            if (TableProperty.isPhoenixTableProperty(prop) || prop.equals(MetaDataUtil.DATA_TABLE_NAME_PROP_NAME)) {
                pTablePropCount++;
            }
        }
        return tableProps.size() - pTablePropCount > 0;
    }
    
    /**
     * Replaces all occurrences of {@link #ESCAPE_CHARACTER} with an empty character. 
     * @param fullColumnName
     * @return 
     */
    public static String getUnEscapedFullColumnName(String fullColumnName) {
        checkArgument(!isNullOrEmpty(fullColumnName), "Column name cannot be null or empty");
        fullColumnName = fullColumnName.replaceAll(ESCAPE_CHARACTER, "");
       	return fullColumnName.trim();
    }
    
    /**
     * Return the separator byte to use based on:
     * @param rowKeyOrderOptimizable whether or not the table may optimize descending row keys. If the
     *  table has no descending row keys, this will be true. Also, if the table has been upgraded (using
     *  a new -u option for psql.py), then it'll be true
     * @param isNullValue whether or not the value is null. We use a null byte still if the value is null
     * regardless of sort order since nulls will always sort first this way.
     * @param sortOrder whether the value sorts ascending or descending.
     * @return the byte to use as the separator
     */
    public static byte getSeparatorByte(boolean rowKeyOrderOptimizable, boolean isNullValue, SortOrder sortOrder) {
        return !rowKeyOrderOptimizable || isNullValue || sortOrder == SortOrder.ASC ? QueryConstants.SEPARATOR_BYTE : QueryConstants.DESC_SEPARATOR_BYTE;
    }
    
    public static byte getSeparatorByte(boolean rowKeyOrderOptimizable, boolean isNullValue, Field f) {
        return getSeparatorByte(rowKeyOrderOptimizable, isNullValue, f.getSortOrder());
    }
    
    public static byte getSeparatorByte(boolean rowKeyOrderOptimizable, boolean isNullValue, Expression e) {
        return getSeparatorByte(rowKeyOrderOptimizable, isNullValue, e.getSortOrder());
    }

    /**
     * Get list of ColumnInfos that contain Column Name and its associated
     * PDataType for an import. The supplied list of columns can be null -- if it is non-null,
     * it represents a user-supplied list of columns to be imported.
     *
     * @param conn Phoenix connection from which metadata will be read
     * @param tableName Phoenix table name whose columns are to be checked. Can include a schema
     *                  name
     * @param columns user-supplied list of import columns, can be null
     * @param strict if true, an exception will be thrown if unknown columns are supplied
     */
    public static List<ColumnInfo> generateColumnInfo(Connection conn,
                                                      String tableName, List<String> columns, boolean strict)
            throws SQLException {
        Map<String, Integer> columnNameToTypeMap = Maps.newLinkedHashMap();
        Set<String> ambiguousColumnNames = new HashSet<String>();
        Map<String, Integer> fullColumnNameToTypeMap = Maps.newLinkedHashMap();
        DatabaseMetaData dbmd = conn.getMetaData();
        int unfoundColumnCount = 0;
        // TODO: escape wildcard characters here because we don't want that
        // behavior here
        String escapedTableName = StringUtil.escapeLike(tableName);
        String[] schemaAndTable = escapedTableName.split("\\.");
        ResultSet rs = null;
        try {
            rs = dbmd.getColumns(null, (schemaAndTable.length == 1 ? ""
                            : schemaAndTable[0]),
                    (schemaAndTable.length == 1 ? escapedTableName
                            : schemaAndTable[1]), null);
            while (rs.next()) {
                String colName = rs.getString(QueryUtil.COLUMN_NAME_POSITION);
                String colFam = rs.getString(QueryUtil.COLUMN_FAMILY_POSITION);

                // use family qualifier, if available, otherwise, use column name
                String fullColumn = (colFam==null?colName:String.format("%s.%s",colFam,colName));
                String sqlTypeName = rs.getString(QueryUtil.DATA_TYPE_NAME_POSITION);

                // allow for both bare and family qualified names.
                if (columnNameToTypeMap.keySet().contains(colName)) {
                    ambiguousColumnNames.add(colName);
                }
                columnNameToTypeMap.put(
                        colName,
                        PDataType.fromSqlTypeName(sqlTypeName).getSqlType());
                fullColumnNameToTypeMap.put(
                        fullColumn,
                        PDataType.fromSqlTypeName(sqlTypeName).getSqlType());
            }
            if (columnNameToTypeMap.isEmpty()) {
                throw new IllegalArgumentException("Table " + tableName + " not found");
            }
        } finally {
            if (rs != null) {
                rs.close();
            }
        }
        List<ColumnInfo> columnInfoList = Lists.newArrayList();
        Set<String> unresolvedColumnNames = new TreeSet<String>();
        if (columns == null) {
            // use family qualified names by default, if no columns are specified.
            for (Map.Entry<String, Integer> entry : fullColumnNameToTypeMap
                    .entrySet()) {
                columnInfoList.add(new ColumnInfo(entry.getKey(), entry.getValue()));
            }
        } else {
            // Leave "null" as indication to skip b/c it doesn't exist
            for (int i = 0; i < columns.size(); i++) {
                String columnName = columns.get(i).trim();
                Integer sqlType = null;
                if (fullColumnNameToTypeMap.containsKey(columnName)) {
                    sqlType = fullColumnNameToTypeMap.get(columnName);
                } else if (columnNameToTypeMap.containsKey(columnName)) {
                    if (ambiguousColumnNames.contains(columnName)) {
                        unresolvedColumnNames.add(columnName);
                    }
                    // fall back to bare column name.
                    sqlType = columnNameToTypeMap.get(columnName);
                }
                if (unresolvedColumnNames.size()>0) {
                    StringBuilder exceptionMessage = new StringBuilder();
                    boolean first = true;
                    exceptionMessage.append("Unable to resolve these column names to a single column family:\n");
                    for (String col : unresolvedColumnNames) {
                        if (first) first = false;
                        else exceptionMessage.append(",");
                        exceptionMessage.append(col);
                    }
                    exceptionMessage.append("\nAvailable columns with column families:\n");
                    first = true;
                    for (String col : fullColumnNameToTypeMap.keySet()) {
                        if (first) first = false;
                        else exceptionMessage.append(",");
                        exceptionMessage.append(col);
                    }
                    throw new SQLException(exceptionMessage.toString());
                }

                if (sqlType == null) {
                    if (strict) {
                        throw new SQLExceptionInfo.Builder(
                                SQLExceptionCode.COLUMN_NOT_FOUND)
                                .setColumnName(columnName)
                                .setTableName(tableName).build()
                                .buildException();
                    }
                    unfoundColumnCount++;
                } else {
                    columnInfoList.add(new ColumnInfo(columnName, sqlType));
                }
            }
            if (unfoundColumnCount == columns.size()) {
                throw new SQLExceptionInfo.Builder(
                        SQLExceptionCode.COLUMN_NOT_FOUND)
                        .setColumnName(
                                Arrays.toString(columns.toArray(new String[0])))
                        .setTableName(tableName).build().buildException();
            }
        }
        return columnInfoList;
    }
    
    public static boolean hasRowTimestampColumn(PTable table) {
    	return table.getRowTimestampColPos()>0;
    }

    public static byte[] getTableKey(PTable dataTable) {
        PName tenantId = dataTable.getTenantId();
        PName schemaName = dataTable.getSchemaName();
        return getTableKey(tenantId == null ? ByteUtil.EMPTY_BYTE_ARRAY : tenantId.getBytes(), schemaName == null ? ByteUtil.EMPTY_BYTE_ARRAY : schemaName.getBytes(), dataTable.getTableName().getBytes());
    }

    public static byte[] getSchemaKey(String schemaName) {
        return SchemaUtil.getTableKey(null, schemaName, MetaDataClient.EMPTY_TABLE);
    }

    public static PName getPhysicalHBaseTableName(PName pName, boolean isNamespaceMapped, PTableType type) {
        return getPhysicalHBaseTableName(pName.toString(), isNamespaceMapped, type);
    }

    public static PName getPhysicalHBaseTableName(byte[] tableName, boolean isNamespaceMapped, PTableType type) {
        return getPhysicalHBaseTableName(Bytes.toString(tableName), isNamespaceMapped, type);
    }

    public static TableName getPhysicalTableName(String fullTableName, ReadOnlyProps readOnlyProps) {
        return getPhysicalName(Bytes.toBytes(fullTableName), readOnlyProps);
    }

    public static TableName getPhysicalTableName(byte[] fullTableName, Configuration conf) {
        return getPhysicalTableName(fullTableName, isNamespaceMappingEnabled(
                isSystemTable(fullTableName) ? PTableType.SYSTEM : null, conf));
    }

    public static TableName getPhysicalName(byte[] fullTableName, ReadOnlyProps readOnlyProps) {
        return getPhysicalTableName(fullTableName,
                isNamespaceMappingEnabled(isSystemTable(fullTableName) ? PTableType.SYSTEM : null, readOnlyProps));
    }

    public static TableName getPhysicalTableName(byte[] fullTableName, boolean isNamespaceMappingEnabled) {
        if (indexOf(fullTableName, QueryConstants.NAMESPACE_SEPARATOR_BYTE) > 0
                || !isNamespaceMappingEnabled) { return TableName.valueOf(fullTableName); }
        String tableName = getTableNameFromFullName(fullTableName);
        String schemaName = getSchemaNameFromFullName(fullTableName);
        return TableName.valueOf(schemaName, tableName);
    }

    public static PName getPhysicalHBaseTableName(String tableName, boolean isNamespaceMapped, PTableType type) {
        if (!isNamespaceMapped) { return PNameFactory.newName(tableName); }
        return PNameFactory
                .newName(tableName.replace(QueryConstants.NAME_SEPARATOR, QueryConstants.NAMESPACE_SEPARATOR));
    }

    public static boolean isSchemaCheckRequired(PTableType tableType, ReadOnlyProps props) {
        return PTableType.TABLE.equals(tableType) && isNamespaceMappingEnabled(tableType, props);
    }
    
    public static boolean isNamespaceMappingEnabled(PTableType type, Configuration conf) {
        return conf.getBoolean(QueryServices.IS_NAMESPACE_MAPPING_ENABLED,
                QueryServicesOptions.DEFAULT_IS_NAMESPACE_MAPPING_ENABLED)
                && (type == null || !PTableType.SYSTEM.equals(type)
                        || conf.getBoolean(QueryServices.IS_SYSTEM_TABLE_MAPPED_TO_NAMESPACE,
                                QueryServicesOptions.DEFAULT_IS_SYSTEM_TABLE_MAPPED_TO_NAMESPACE));
    }

    public static boolean isNamespaceMappingEnabled(PTableType type, ReadOnlyProps readOnlyProps) {
        return readOnlyProps.getBoolean(QueryServices.IS_NAMESPACE_MAPPING_ENABLED,
                QueryServicesOptions.DEFAULT_IS_NAMESPACE_MAPPING_ENABLED)
                && (type == null || !PTableType.SYSTEM.equals(type)
                        || readOnlyProps.getBoolean(QueryServices.IS_SYSTEM_TABLE_MAPPED_TO_NAMESPACE,
                                QueryServicesOptions.DEFAULT_IS_SYSTEM_TABLE_MAPPED_TO_NAMESPACE));
    }

    public static byte[] getParentTableNameFromIndexTable(byte[] physicalTableName, String indexPrefix) {
        String tableName = Bytes.toString(physicalTableName);
        return getParentTableNameFromIndexTable(tableName, indexPrefix).getBytes();
    }

    public static String getParentTableNameFromIndexTable(String physicalTableName, String indexPrefix) {
        if (physicalTableName.contains(QueryConstants.NAMESPACE_SEPARATOR)) {
            String schemaNameFromFullName = getSchemaNameFromFullName(physicalTableName,
                    QueryConstants.NAMESPACE_SEPARATOR);
            String tableNameFromFullName = getTableNameFromFullName(physicalTableName,
                    QueryConstants.NAMESPACE_SEPARATOR);
            return schemaNameFromFullName + QueryConstants.NAMESPACE_SEPARATOR
                    + getStrippedName(tableNameFromFullName, indexPrefix);
        }
        return getStrippedName(physicalTableName, indexPrefix);
    }
    
    private static String getStrippedName(String physicalTableName, String indexPrefix) {
        return physicalTableName.indexOf(indexPrefix) == 0 ? physicalTableName.substring(indexPrefix.length())
                : physicalTableName;
    }

    /**
     * Calculate the HBase HTable name.
     *
     * @param schemaName import schema name, can be null
     * @param tableName import table name
     * @return the byte representation of the HTable
     */
    public static String getQualifiedTableName(String schemaName, String tableName) {
        if (schemaName != null) {
            return String.format("%s.%s", normalizeIdentifier(schemaName),
                    normalizeIdentifier(tableName));
        } else {
            return normalizeIdentifier(tableName);
        }
    }

    /**
     * Pads the data in ptr by the required amount for fixed width data types
     */
    public static void padData(String tableName, PColumn column, ImmutableBytesWritable ptr) {
        PDataType type = column.getDataType();
        byte[] byteValue = ptr.get();
        boolean isNull = type.isNull(byteValue);
        Integer maxLength = column.getMaxLength();
        if (!isNull && type.isFixedWidth() && maxLength != null) {
            if (ptr.getLength() < maxLength) {
                type.pad(ptr, maxLength, column.getSortOrder());
            } else if (ptr.getLength() > maxLength) {
                throw new DataExceedsCapacityException(tableName + "." + column.getName().getString() + " may not exceed " + maxLength + " bytes (" + type.toObject(byteValue) + ")");
            }
        }
    }
}
