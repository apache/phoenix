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
import static org.apache.phoenix.jdbc.PhoenixDatabaseMetaData.SYSTEM_STATS_NAME_BYTES;

import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Properties;

import javax.annotation.Nullable;

import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.io.encoding.DataBlockEncoding;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.phoenix.exception.SQLExceptionCode;
import org.apache.phoenix.exception.SQLExceptionInfo;
import org.apache.phoenix.hbase.index.util.ImmutableBytesPtr;
import org.apache.phoenix.jdbc.PhoenixConnection;
import org.apache.phoenix.jdbc.PhoenixDatabaseMetaData;
import org.apache.phoenix.query.KeyRange;
import org.apache.phoenix.query.QueryConstants;
import org.apache.phoenix.schema.AmbiguousColumnException;
import org.apache.phoenix.schema.ColumnFamilyNotFoundException;
import org.apache.phoenix.schema.ColumnNotFoundException;
import org.apache.phoenix.schema.PColumn;
import org.apache.phoenix.schema.PColumnFamily;
import org.apache.phoenix.schema.PDatum;
import org.apache.phoenix.schema.PMetaData;
import org.apache.phoenix.schema.PName;
import org.apache.phoenix.schema.PTable;
import org.apache.phoenix.schema.RowKeySchema;
import org.apache.phoenix.schema.RowKeySchema.RowKeySchemaBuilder;
import org.apache.phoenix.schema.SaltingUtil;
import org.apache.phoenix.schema.SortOrder;
import org.apache.phoenix.schema.ValueSchema.Field;
import org.apache.phoenix.schema.types.PDataType;
import org.apache.phoenix.schema.types.PVarbinary;
import org.apache.phoenix.schema.types.PVarchar;

import com.google.common.base.Preconditions;

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
    	// Empty key value
    	rowSize += KeyValue.getKeyValueDataStructureSize(keyLength, getEmptyColumnFamily(table).length, QueryConstants.EMPTY_COLUMN_BYTES.length, 0);
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

    public static boolean isCaseSensitive(String name) {
        return name!=null && name.length() > 0 && name.charAt(0)=='"';
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

    public static byte[] getTableKey(String tenantId, String schemaName, String tableName) {
        return ByteUtil.concat(tenantId == null  ? ByteUtil.EMPTY_BYTE_ARRAY : Bytes.toBytes(tenantId), QueryConstants.SEPARATOR_BYTE_ARRAY, schemaName == null ? ByteUtil.EMPTY_BYTE_ARRAY : Bytes.toBytes(schemaName), QueryConstants.SEPARATOR_BYTE_ARRAY, Bytes.toBytes(tableName));
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
            return getName(familyName, columnName, false);
        }
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
        boolean isString = type.isCoercibleTo(PVarchar.INSTANCE);
        return isString ? ("'" + type.toObject(value).toString() + "'") : type.toObject(value).toString();
    }

    public static byte[] getEmptyColumnFamily(PName defaultColumnFamily, List<PColumnFamily> families) {
        return families.isEmpty() ? defaultColumnFamily == null ? QueryConstants.DEFAULT_COLUMN_FAMILY_BYTES : defaultColumnFamily.getBytes() : families.get(0).getName().getBytes();
    }

    public static byte[] getEmptyColumnFamily(PTable table) {
        List<PColumnFamily> families = table.getColumnFamilies();
        return families.isEmpty() ? table.getDefaultFamilyName() == null ? QueryConstants.DEFAULT_COLUMN_FAMILY_BYTES : table.getDefaultFamilyName().getBytes() : families.get(0).getName().getBytes();
    }

    public static ImmutableBytesPtr getEmptyColumnFamilyPtr(PTable table) {
        List<PColumnFamily> families = table.getColumnFamilies();
        return families.isEmpty() ? table.getDefaultFamilyName() == null ? QueryConstants.DEFAULT_COLUMN_FAMILY_BYTES_PTR : table.getDefaultFamilyName().getBytesPtr() : families.get(0)
                .getName().getBytesPtr();
    }

    public static boolean isMetaTable(byte[] tableName) {
        return Bytes.compareTo(tableName, SYSTEM_CATALOG_NAME_BYTES) == 0;
    }
    
    public static boolean isStatsTable(byte[] tableName) {
        return Bytes.compareTo(tableName, SYSTEM_STATS_NAME_BYTES) == 0;
    }
    
    public static boolean isSequenceTable(byte[] tableName) {
        return Bytes.compareTo(tableName, PhoenixDatabaseMetaData.SEQUENCE_FULLNAME_BYTES) == 0;
    }

    public static boolean isSequenceTable(PTable table) {
        return PhoenixDatabaseMetaData.SEQUENCE_FULLNAME.equals(table.getName().getString());
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
        String url = conn.getURL();
        Properties props = conn.getClientInfo();
        PMetaData metaData = conn.getMetaDataCache();
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
    
    public static String getSchemaNameFromFullName(String tableName) {
        int index = tableName.indexOf(QueryConstants.NAME_SEPARATOR);
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
        int index = indexOf(tableName, QueryConstants.NAME_SEPARATOR_BYTE);
        if (index < 0) {
            return StringUtil.EMPTY_STRING; 
        }
        return Bytes.toString(tableName, 0, index);
    }
    
    public static String getTableNameFromFullName(byte[] tableName) {
        if (tableName == null) {
            return null;
        }
        int index = indexOf(tableName, QueryConstants.NAME_SEPARATOR_BYTE);
        if (index < 0) {
            return Bytes.toString(tableName); 
        }
        return Bytes.toString(tableName, index+1, tableName.length - index - 1);
    }

    public static String getTableNameFromFullName(String tableName) {
        int index = tableName.indexOf(QueryConstants.NAME_SEPARATOR);
        if (index < 0) {
            return tableName; 
        }
        return tableName.substring(index+1, tableName.length());
    }

    public static byte[] getTableKeyFromFullName(String fullTableName) {
        int index = fullTableName.indexOf(QueryConstants.NAME_SEPARATOR);
        if (index < 0) {
            return getTableKey(null, null, fullTableName); 
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
}
