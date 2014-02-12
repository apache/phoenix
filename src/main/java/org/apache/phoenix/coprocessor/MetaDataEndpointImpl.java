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
package org.apache.phoenix.coprocessor;

import static org.apache.phoenix.jdbc.PhoenixDatabaseMetaData.COLUMN_COUNT_BYTES;
import static org.apache.phoenix.jdbc.PhoenixDatabaseMetaData.COLUMN_MODIFIER;
import static org.apache.phoenix.jdbc.PhoenixDatabaseMetaData.COLUMN_NAME_INDEX;
import static org.apache.phoenix.jdbc.PhoenixDatabaseMetaData.COLUMN_SIZE;
import static org.apache.phoenix.jdbc.PhoenixDatabaseMetaData.DATA_TABLE_NAME_BYTES;
import static org.apache.phoenix.jdbc.PhoenixDatabaseMetaData.DATA_TYPE;
import static org.apache.phoenix.jdbc.PhoenixDatabaseMetaData.DECIMAL_DIGITS;
import static org.apache.phoenix.jdbc.PhoenixDatabaseMetaData.FAMILY_NAME_INDEX;
import static org.apache.phoenix.jdbc.PhoenixDatabaseMetaData.IMMUTABLE_ROWS_BYTES;
import static org.apache.phoenix.jdbc.PhoenixDatabaseMetaData.INDEX_STATE_BYTES;
import static org.apache.phoenix.jdbc.PhoenixDatabaseMetaData.NULLABLE;
import static org.apache.phoenix.jdbc.PhoenixDatabaseMetaData.ORDINAL_POSITION;
import static org.apache.phoenix.jdbc.PhoenixDatabaseMetaData.PK_NAME_BYTES;
import static org.apache.phoenix.jdbc.PhoenixDatabaseMetaData.SALT_BUCKETS_BYTES;
import static org.apache.phoenix.jdbc.PhoenixDatabaseMetaData.SCHEMA_NAME_INDEX;
import static org.apache.phoenix.jdbc.PhoenixDatabaseMetaData.TABLE_FAMILY_BYTES;
import static org.apache.phoenix.jdbc.PhoenixDatabaseMetaData.TABLE_NAME_INDEX;
import static org.apache.phoenix.jdbc.PhoenixDatabaseMetaData.TABLE_SEQ_NUM_BYTES;
import static org.apache.phoenix.jdbc.PhoenixDatabaseMetaData.TABLE_TYPE_BYTES;
import static org.apache.phoenix.util.SchemaUtil.getVarCharLength;
import static org.apache.phoenix.util.SchemaUtil.getVarChars;

import java.io.IOException;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Mutation;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.coprocessor.BaseEndpointCoprocessor;
import org.apache.hadoop.hbase.coprocessor.RegionCoprocessorEnvironment;
import org.apache.hadoop.hbase.filter.FirstKeyOnlyFilter;
import org.apache.hadoop.hbase.regionserver.HRegion;
import org.apache.hadoop.hbase.regionserver.RegionScanner;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.EnvironmentEdgeManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Lists;
import org.apache.hbase.index.util.ImmutableBytesPtr;
import org.apache.hbase.index.util.IndexManagementUtil;
import org.apache.phoenix.cache.GlobalCache;
import org.apache.phoenix.jdbc.PhoenixDatabaseMetaData;
import org.apache.phoenix.query.QueryConstants;
import org.apache.phoenix.schema.AmbiguousColumnException;
import org.apache.phoenix.schema.ColumnFamilyNotFoundException;
import org.apache.phoenix.schema.ColumnModifier;
import org.apache.phoenix.schema.ColumnNotFoundException;
import org.apache.phoenix.schema.PColumn;
import org.apache.phoenix.schema.PColumnFamily;
import org.apache.phoenix.schema.PColumnImpl;
import org.apache.phoenix.schema.PDataType;
import org.apache.phoenix.schema.PIndexState;
import org.apache.phoenix.schema.PName;
import org.apache.phoenix.schema.PNameFactory;
import org.apache.phoenix.schema.PTable;
import org.apache.phoenix.schema.PTableImpl;
import org.apache.phoenix.schema.PTableType;
import org.apache.phoenix.schema.TableNotFoundException;
import org.apache.phoenix.util.ByteUtil;
import org.apache.phoenix.util.IndexUtil;
import org.apache.phoenix.util.KeyValueUtil;
import org.apache.phoenix.util.MetaDataUtil;
import org.apache.phoenix.util.SchemaUtil;
import org.apache.phoenix.util.ServerUtil;

/**
 * 
 * Endpoint co-processor through which all Phoenix metadata mutations flow.
 * We only allow mutations to the latest version of a Phoenix table (i.e. the
 * timeStamp must be increasing).
 * For adding/dropping columns use a sequence number on the table to ensure that
 * the client has the latest version.
 * The timeStamp on the table correlates with the timeStamp on the data row.
 * TODO: we should enforce that a metadata mutation uses a timeStamp bigger than
 * any in use on the data table, b/c otherwise we can end up with data rows that
 * are not valid against a schema row.
 *
 * 
 * @since 0.1
 */
public class MetaDataEndpointImpl extends BaseEndpointCoprocessor implements MetaDataProtocol {
    private static final Logger logger = LoggerFactory.getLogger(MetaDataEndpointImpl.class);

    // KeyValues for Table
    private static final KeyValue TABLE_TYPE_KV = KeyValue.createFirstOnRow(ByteUtil.EMPTY_BYTE_ARRAY, TABLE_FAMILY_BYTES, TABLE_TYPE_BYTES);
    private static final KeyValue TABLE_SEQ_NUM_KV = KeyValue.createFirstOnRow(ByteUtil.EMPTY_BYTE_ARRAY, TABLE_FAMILY_BYTES, TABLE_SEQ_NUM_BYTES);
    private static final KeyValue COLUMN_COUNT_KV = KeyValue.createFirstOnRow(ByteUtil.EMPTY_BYTE_ARRAY, TABLE_FAMILY_BYTES, COLUMN_COUNT_BYTES);
    private static final KeyValue SALT_BUCKETS_KV = KeyValue.createFirstOnRow(ByteUtil.EMPTY_BYTE_ARRAY, TABLE_FAMILY_BYTES, SALT_BUCKETS_BYTES);
    private static final KeyValue PK_NAME_KV = KeyValue.createFirstOnRow(ByteUtil.EMPTY_BYTE_ARRAY, TABLE_FAMILY_BYTES, PK_NAME_BYTES);
    private static final KeyValue DATA_TABLE_NAME_KV = KeyValue.createFirstOnRow(ByteUtil.EMPTY_BYTE_ARRAY, TABLE_FAMILY_BYTES, DATA_TABLE_NAME_BYTES);
    private static final KeyValue INDEX_STATE_KV = KeyValue.createFirstOnRow(ByteUtil.EMPTY_BYTE_ARRAY, TABLE_FAMILY_BYTES, INDEX_STATE_BYTES);
    private static final KeyValue IMMUTABLE_ROWS_KV = KeyValue.createFirstOnRow(ByteUtil.EMPTY_BYTE_ARRAY, TABLE_FAMILY_BYTES, IMMUTABLE_ROWS_BYTES);
    private static final List<KeyValue> TABLE_KV_COLUMNS = Arrays.<KeyValue>asList(
            TABLE_TYPE_KV,
            TABLE_SEQ_NUM_KV,
            COLUMN_COUNT_KV,
            SALT_BUCKETS_KV,
            PK_NAME_KV,
            DATA_TABLE_NAME_KV,
            INDEX_STATE_KV,
            IMMUTABLE_ROWS_KV
            );
    static {
        Collections.sort(TABLE_KV_COLUMNS, KeyValue.COMPARATOR);
    }
    private static final int TABLE_TYPE_INDEX = TABLE_KV_COLUMNS.indexOf(TABLE_TYPE_KV);
    private static final int TABLE_SEQ_NUM_INDEX = TABLE_KV_COLUMNS.indexOf(TABLE_SEQ_NUM_KV);
    private static final int COLUMN_COUNT_INDEX = TABLE_KV_COLUMNS.indexOf(COLUMN_COUNT_KV);
    private static final int SALT_BUCKETS_INDEX = TABLE_KV_COLUMNS.indexOf(SALT_BUCKETS_KV);
    private static final int PK_NAME_INDEX = TABLE_KV_COLUMNS.indexOf(PK_NAME_KV);
    private static final int DATA_TABLE_NAME_INDEX = TABLE_KV_COLUMNS.indexOf(DATA_TABLE_NAME_KV);
    private static final int INDEX_STATE_INDEX = TABLE_KV_COLUMNS.indexOf(INDEX_STATE_KV);
    private static final int IMMUTABLE_ROWS_INDEX = TABLE_KV_COLUMNS.indexOf(IMMUTABLE_ROWS_KV);
    
    // KeyValues for Column
    private static final KeyValue DECIMAL_DIGITS_KV = KeyValue.createFirstOnRow(ByteUtil.EMPTY_BYTE_ARRAY, TABLE_FAMILY_BYTES, Bytes.toBytes(DECIMAL_DIGITS));
    private static final KeyValue COLUMN_SIZE_KV = KeyValue.createFirstOnRow(ByteUtil.EMPTY_BYTE_ARRAY, TABLE_FAMILY_BYTES, Bytes.toBytes(COLUMN_SIZE));
    private static final KeyValue NULLABLE_KV = KeyValue.createFirstOnRow(ByteUtil.EMPTY_BYTE_ARRAY, TABLE_FAMILY_BYTES, Bytes.toBytes(NULLABLE));
    private static final KeyValue DATA_TYPE_KV = KeyValue.createFirstOnRow(ByteUtil.EMPTY_BYTE_ARRAY, TABLE_FAMILY_BYTES, Bytes.toBytes(DATA_TYPE));
    private static final KeyValue ORDINAL_POSITION_KV = KeyValue.createFirstOnRow(ByteUtil.EMPTY_BYTE_ARRAY, TABLE_FAMILY_BYTES, Bytes.toBytes(ORDINAL_POSITION));
    private static final KeyValue COLUMN_MODIFIER_KV = KeyValue.createFirstOnRow(ByteUtil.EMPTY_BYTE_ARRAY, TABLE_FAMILY_BYTES, Bytes.toBytes(COLUMN_MODIFIER));
    private static final List<KeyValue> COLUMN_KV_COLUMNS = Arrays.<KeyValue>asList(
            DECIMAL_DIGITS_KV,
            COLUMN_SIZE_KV,
            NULLABLE_KV,
            DATA_TYPE_KV,
            ORDINAL_POSITION_KV,
            COLUMN_MODIFIER_KV,
            DATA_TABLE_NAME_KV // included in both column and table row for metadata APIs
            );
    static {
        Collections.sort(COLUMN_KV_COLUMNS, KeyValue.COMPARATOR);
    }
    private static final int DECIMAL_DIGITS_INDEX = COLUMN_KV_COLUMNS.indexOf(DECIMAL_DIGITS_KV);
    private static final int COLUMN_SIZE_INDEX = COLUMN_KV_COLUMNS.indexOf(COLUMN_SIZE_KV);
    private static final int NULLABLE_INDEX = COLUMN_KV_COLUMNS.indexOf(NULLABLE_KV);
    private static final int SQL_DATA_TYPE_INDEX = COLUMN_KV_COLUMNS.indexOf(DATA_TYPE_KV);
    private static final int ORDINAL_POSITION_INDEX = COLUMN_KV_COLUMNS.indexOf(ORDINAL_POSITION_KV);
    private static final int COLUMN_MODIFIER_INDEX = COLUMN_KV_COLUMNS.indexOf(COLUMN_MODIFIER_KV);

    private static PName newPName(byte[] keyBuffer, int keyOffset, int keyLength) {
        if (keyLength <= 0) {
            return null;
        }
        int length = getVarCharLength(keyBuffer, keyOffset, keyLength);
        // TODO: PNameImpl that doesn't need to copy the bytes
        byte[] pnameBuf = new byte[length];
        System.arraycopy(keyBuffer, keyOffset, pnameBuf, 0, length);
        return PNameFactory.newName(pnameBuf);
    }
    
    private static Scan newTableRowsScan(byte[] key, long startTimeStamp, long stopTimeStamp) throws IOException {
        Scan scan = new Scan();
        scan.setTimeRange(startTimeStamp, stopTimeStamp);
        scan.setStartRow(key);
        byte[] stopKey = ByteUtil.concat(key, QueryConstants.SEPARATOR_BYTE_ARRAY);
        ByteUtil.nextKey(stopKey, stopKey.length);
        scan.setStopRow(stopKey);
        return scan;
    }

    @Override
    public RegionCoprocessorEnvironment getEnvironment() {
        return (RegionCoprocessorEnvironment)super.getEnvironment();
    }
    
    private PTable buildTable(byte[] key, ImmutableBytesPtr cacheKey, HRegion region, long clientTimeStamp) throws IOException, SQLException {
        Scan scan = newTableRowsScan(key, MIN_TABLE_TIMESTAMP, clientTimeStamp);
        RegionScanner scanner = region.getScanner(scan);
        Map<ImmutableBytesPtr,PTable> metaDataCache = GlobalCache.getInstance(this.getEnvironment()).getMetaDataCache();
        try {
            PTable oldTable = metaDataCache.get(cacheKey);
            long tableTimeStamp = oldTable == null ? MIN_TABLE_TIMESTAMP-1 : oldTable.getTimeStamp();
            PTable newTable;
            newTable = getTable(scanner, clientTimeStamp, tableTimeStamp);
            if (newTable == null) {
                return null;
            }
            if (oldTable == null || tableTimeStamp < newTable.getTimeStamp()) {
                if (logger.isDebugEnabled()) {
                    logger.debug("Caching table " + Bytes.toStringBinary(cacheKey.get(), cacheKey.getOffset(), cacheKey.getLength()) + " at seqNum " + newTable.getSequenceNumber() + " with newer timestamp " + newTable.getTimeStamp() + " versus " + tableTimeStamp);
                }
                oldTable = metaDataCache.put(cacheKey, newTable);
                if (logger.isDebugEnabled()) {
                    if (oldTable == null) {
                        logger.debug("No previously cached table " + Bytes.toStringBinary(cacheKey.get(), cacheKey.getOffset(), cacheKey.getLength()));
                    } else {
                        logger.debug("Previously cached table " + Bytes.toStringBinary(cacheKey.get(), cacheKey.getOffset(), cacheKey.getLength()) + " was at seqNum " + oldTable.getSequenceNumber() + " with timestamp " + oldTable.getTimeStamp());
                    }
                }
            }
            return newTable;
        } finally {
            scanner.close();
        }
    }

    private void addIndexToTable(PName schemaName, PName indexName, PName tableName, long clientTimeStamp, List<PTable> indexes) throws IOException, SQLException {
        byte[] key = SchemaUtil.getTableKey(schemaName.getBytes(), indexName.getBytes());
        PTable indexTable = doGetTable(key, clientTimeStamp);
        if (indexTable == null) {
            ServerUtil.throwIOException("Index not found", new TableNotFoundException(schemaName.getString(), indexName.getString()));
            return;
        }
        indexes.add(indexTable);
    }

    private void addColumnToTable(List<KeyValue> results, PName colName, PName famName, KeyValue[] colKeyValues, List<PColumn> columns) {
        int i = 0;
        int j = 0;
        while (i < results.size() && j < COLUMN_KV_COLUMNS.size()) {
            KeyValue kv = results.get(i);
            KeyValue searchKv = COLUMN_KV_COLUMNS.get(j);
            int cmp = Bytes.compareTo(kv.getBuffer(), kv.getQualifierOffset(), kv.getQualifierLength(), 
                    searchKv.getBuffer(), searchKv.getQualifierOffset(), searchKv.getQualifierLength());
            if (cmp == 0) {
                colKeyValues[j++] = kv;
                i++;
            } else if (cmp > 0) {
                colKeyValues[j++] = null;
            } else {
                i++; // shouldn't happen - means unexpected KV in system table column row
            }
        }
        // COLUMN_SIZE and DECIMAL_DIGIT are optional. NULLABLE, DATA_TYPE and ORDINAL_POSITION_KV are required.
        if (colKeyValues[SQL_DATA_TYPE_INDEX] == null || colKeyValues[NULLABLE_INDEX] == null
                || colKeyValues[ORDINAL_POSITION_INDEX] == null) {
            throw new IllegalStateException("Didn't find all required key values in '" + colName.getString() + "' column metadata row");
        }
        KeyValue columnSizeKv = colKeyValues[COLUMN_SIZE_INDEX];
        Integer maxLength = columnSizeKv == null ? null : PDataType.INTEGER.getCodec().decodeInt(columnSizeKv.getBuffer(), columnSizeKv.getValueOffset(), null);
        KeyValue decimalDigitKv = colKeyValues[DECIMAL_DIGITS_INDEX];
        Integer scale = decimalDigitKv == null ? null : PDataType.INTEGER.getCodec().decodeInt(decimalDigitKv.getBuffer(), decimalDigitKv.getValueOffset(), null);
        KeyValue ordinalPositionKv = colKeyValues[ORDINAL_POSITION_INDEX];
        int position = PDataType.INTEGER.getCodec().decodeInt(ordinalPositionKv.getBuffer(), ordinalPositionKv.getValueOffset(), null);
        KeyValue nullableKv = colKeyValues[NULLABLE_INDEX];
        boolean isNullable = PDataType.INTEGER.getCodec().decodeInt(nullableKv.getBuffer(), nullableKv.getValueOffset(), null) != ResultSetMetaData.columnNoNulls;
        KeyValue sqlDataTypeKv = colKeyValues[SQL_DATA_TYPE_INDEX];
        PDataType dataType = PDataType.fromSqlType(PDataType.INTEGER.getCodec().decodeInt(sqlDataTypeKv.getBuffer(), sqlDataTypeKv.getValueOffset(), null));
        if (maxLength == null && dataType == PDataType.BINARY) dataType = PDataType.VARBINARY; // For backward compatibility.
        KeyValue columnModifierKv = colKeyValues[COLUMN_MODIFIER_INDEX];
        ColumnModifier sortOrder = columnModifierKv == null ? null : ColumnModifier.fromSystemValue(PDataType.INTEGER.getCodec().decodeInt(columnModifierKv.getBuffer(), columnModifierKv.getValueOffset(), null));
        PColumn column = new PColumnImpl(colName, famName, dataType, maxLength, scale, isNullable, position-1, sortOrder);
        columns.add(column);
    }

    private PTable getTable(RegionScanner scanner, long clientTimeStamp, long tableTimeStamp) throws IOException, SQLException {
        List<KeyValue> results = Lists.newArrayList();
        scanner.next(results);
        if (results.isEmpty()) {
            return null;
        }
        KeyValue[] tableKeyValues = new KeyValue[TABLE_KV_COLUMNS.size()];
        KeyValue[] colKeyValues = new KeyValue[COLUMN_KV_COLUMNS.size()];
        
        // Create PTable based on KeyValues from scan
        KeyValue keyValue = results.get(0);
        byte[] keyBuffer = keyValue.getBuffer();
        int keyLength = keyValue.getRowLength();
        int keyOffset = keyValue.getRowOffset();
        PName schemaName = newPName(keyBuffer, keyOffset, keyLength);
        int schemaNameLength = schemaName.getBytes().length;
        int tableNameLength = keyLength-schemaNameLength-1;
        byte[] tableNameBytes = new byte[tableNameLength];
        System.arraycopy(keyBuffer, keyOffset+schemaNameLength+1, tableNameBytes, 0, tableNameLength);
        PName tableName = PNameFactory.newName(tableNameBytes);
        
        int offset = schemaNameLength + tableNameLength + 2;
        // This will prevent the client from continually looking for the current
        // table when we know that there will never be one since we disallow updates
        // unless the table is the latest
        // If we already have a table newer than the one we just found and
        // the client timestamp is less that the existing table time stamp,
        // bump up the timeStamp to right before the client time stamp, since
        // we know it can't possibly change.
        long timeStamp = keyValue.getTimestamp();
//        long timeStamp = tableTimeStamp > keyValue.getTimestamp() && 
//                         clientTimeStamp < tableTimeStamp
//                         ? clientTimeStamp-1 
//                         : keyValue.getTimestamp();

        int i = 0;
        int j = 0;
        while (i < results.size() && j < TABLE_KV_COLUMNS.size()) {
            KeyValue kv = results.get(i);
            KeyValue searchKv = TABLE_KV_COLUMNS.get(j);
            int cmp = Bytes.compareTo(kv.getBuffer(), kv.getQualifierOffset(), kv.getQualifierLength(), 
                    searchKv.getBuffer(), searchKv.getQualifierOffset(), searchKv.getQualifierLength());
            if (cmp == 0) {
                timeStamp = Math.max(timeStamp, kv.getTimestamp()); // Find max timestamp of table header row
                tableKeyValues[j++] = kv;
                i++;
            } else if (cmp > 0) {
                tableKeyValues[j++] = null;
            } else {
                i++; // shouldn't happen - means unexpected KV in system table header row
            }
        }
        // TABLE_TYPE, TABLE_SEQ_NUM and COLUMN_COUNT are required.
        if (tableKeyValues[TABLE_TYPE_INDEX] == null || tableKeyValues[TABLE_SEQ_NUM_INDEX] == null
                || tableKeyValues[COLUMN_COUNT_INDEX] == null) {
            throw new IllegalStateException("Didn't find expected key values for table row in metadata row");
        }
        KeyValue tableTypeKv = tableKeyValues[TABLE_TYPE_INDEX];
        PTableType tableType = PTableType.fromSerializedValue(tableTypeKv.getBuffer()[tableTypeKv.getValueOffset()]);
        KeyValue tableSeqNumKv = tableKeyValues[TABLE_SEQ_NUM_INDEX];
        long tableSeqNum = PDataType.LONG.getCodec().decodeLong(tableSeqNumKv.getBuffer(), tableSeqNumKv.getValueOffset(), null);
        KeyValue columnCountKv = tableKeyValues[COLUMN_COUNT_INDEX];
        int columnCount = PDataType.INTEGER.getCodec().decodeInt(columnCountKv.getBuffer(), columnCountKv.getValueOffset(), null);
        KeyValue pkNameKv = tableKeyValues[PK_NAME_INDEX];
        PName pkName = pkNameKv != null ? newPName(pkNameKv.getBuffer(), pkNameKv.getValueOffset(), pkNameKv.getValueLength()) : null;
        KeyValue saltBucketNumKv = tableKeyValues[SALT_BUCKETS_INDEX];
        Integer saltBucketNum = saltBucketNumKv != null ? (Integer)PDataType.INTEGER.getCodec().decodeInt(saltBucketNumKv.getBuffer(), saltBucketNumKv.getValueOffset(), null) : null;
        KeyValue dataTableNameKv = tableKeyValues[DATA_TABLE_NAME_INDEX];
        PName dataTableName = dataTableNameKv != null ? newPName(dataTableNameKv.getBuffer(), dataTableNameKv.getValueOffset(), dataTableNameKv.getValueLength()) : null;
        KeyValue indexStateKv = tableKeyValues[INDEX_STATE_INDEX];
        PIndexState indexState = indexStateKv == null ? null : PIndexState.fromSerializedValue(indexStateKv.getBuffer()[indexStateKv.getValueOffset()]);
        KeyValue immutableRowsKv = tableKeyValues[IMMUTABLE_ROWS_INDEX];
        boolean isImmutableRows = immutableRowsKv == null ? false : (Boolean)PDataType.BOOLEAN.toObject(immutableRowsKv.getBuffer(), immutableRowsKv.getValueOffset(), immutableRowsKv.getValueLength());
        
        List<PColumn> columns = Lists.newArrayListWithExpectedSize(columnCount);
        List<PTable> indexes = new ArrayList<PTable>();
        while (true) {
            results.clear();
            scanner.next(results);
            if (results.isEmpty()) {
                break;
            }
            KeyValue colKv = results.get(0);
            int colKeyLength = colKv.getRowLength();
            PName colName = newPName(colKv.getBuffer(), colKv.getRowOffset() + offset, colKeyLength-offset);
            int colKeyOffset = offset + colName.getBytes().length + 1;
            PName famName = newPName(colKv.getBuffer(), colKv.getRowOffset() + colKeyOffset, colKeyLength-colKeyOffset);
            if (colName.getString().isEmpty() && famName != null) {
                addIndexToTable(schemaName, famName, tableName, clientTimeStamp, indexes);                
            } else {
                addColumnToTable(results, colName, famName, colKeyValues, columns);
            }
        }
        
        return PTableImpl.makePTable(schemaName, tableName, tableType, indexState, timeStamp, tableSeqNum, pkName, saltBucketNum, columns, dataTableName, indexes, isImmutableRows);
    }

    private PTable buildDeletedTable(byte[] key, ImmutableBytesPtr cacheKey, HRegion region, long clientTimeStamp) throws IOException {
        if (clientTimeStamp == HConstants.LATEST_TIMESTAMP) {
            return null;
        }
        
        Scan scan = newTableRowsScan(key, clientTimeStamp, HConstants.LATEST_TIMESTAMP);
        scan.setFilter(new FirstKeyOnlyFilter());
        scan.setRaw(true);
        RegionScanner scanner = region.getScanner(scan);
        List<KeyValue> results = Lists.<KeyValue>newArrayList();
        scanner.next(results);
        // HBase ignores the time range on a raw scan (HBASE-7362)
        if (!results.isEmpty() && results.get(0).getTimestamp() > clientTimeStamp) {
            KeyValue kv = results.get(0);
            if (kv.isDelete()) {
                Map<ImmutableBytesPtr,PTable> metaDataCache = GlobalCache.getInstance(this.getEnvironment()).getMetaDataCache();
                PTable table = newDeletedTableMarker(kv.getTimestamp());
                metaDataCache.put(cacheKey, table);
                return table;
            }
        }
        return null;
    }

    private static PTable newDeletedTableMarker(long timestamp) {
        return new PTableImpl(timestamp);
    }

    private static boolean isTableDeleted(PTable table) {
        return table.getName() == null;
    }

    private PTable loadTable(RegionCoprocessorEnvironment env, byte[] key, ImmutableBytesPtr cacheKey, long clientTimeStamp, long asOfTimeStamp) throws IOException, SQLException {
        HRegion region = env.getRegion();
        Map<ImmutableBytesPtr,PTable> metaDataCache = GlobalCache.getInstance(this.getEnvironment()).getMetaDataCache();
        PTable table = metaDataCache.get(cacheKey);
        // We always cache the latest version - fault in if not in cache
        if (table != null || (table = buildTable(key, cacheKey, region, asOfTimeStamp)) != null) {
            return table;
        }
        // if not found then check if newer table already exists and add delete marker for timestamp found
        if (table == null && (table=buildDeletedTable(key, cacheKey, region, clientTimeStamp)) != null) {
            return table;
        }
        return null;
    }
    
    @Override
    public MetaDataMutationResult createTable(List<Mutation> tableMetadata) throws IOException {
        byte[][] rowKeyMetaData = new byte[2][];
        MetaDataUtil.getSchemaAndTableName(tableMetadata,rowKeyMetaData);
        byte[] schemaName = rowKeyMetaData[PhoenixDatabaseMetaData.SCHEMA_NAME_INDEX];
        byte[] tableName = rowKeyMetaData[PhoenixDatabaseMetaData.TABLE_NAME_INDEX];
        try {
            byte[] parentTableName = MetaDataUtil.getParentTableName(tableMetadata);
            byte[] lockTableName = parentTableName == null ? tableName : parentTableName;
            byte[] lockKey = SchemaUtil.getTableKey(schemaName, lockTableName);
            byte[] key = parentTableName == null ? lockKey : SchemaUtil.getTableKey(schemaName, tableName);
            byte[] parentKey = parentTableName == null ? null : lockKey;
            
            RegionCoprocessorEnvironment env = getEnvironment();
            HRegion region = env.getRegion();
            MetaDataMutationResult result = checkTableKeyInRegion(lockKey, region);
            if (result != null) {
                return result; 
            }
            List<Integer> lids = Lists.newArrayList(5);
            long clientTimeStamp = MetaDataUtil.getClientTimeStamp(tableMetadata);
            try {
                acquireLock(region, lockKey, lids);
                if (key != lockKey) {
                    acquireLock(region, key, lids);
                }
                // Load parent table first
                PTable parentTable = null;
                ImmutableBytesPtr parentCacheKey = null;
                if (parentKey != null) {
                    parentCacheKey = new ImmutableBytesPtr(parentKey);
                    parentTable = loadTable(env, parentKey, parentCacheKey, clientTimeStamp, clientTimeStamp);
                    if (parentTable == null || isTableDeleted(parentTable)) {
                        return new MetaDataMutationResult(MutationCode.PARENT_TABLE_NOT_FOUND, EnvironmentEdgeManager.currentTimeMillis(), parentTable);
                    }
                    // If parent table isn't at the expected sequence number, then return
                    if (parentTable.getSequenceNumber() != MetaDataUtil.getParentSequenceNumber(tableMetadata)) {
                        return new MetaDataMutationResult(MutationCode.CONCURRENT_TABLE_MUTATION, EnvironmentEdgeManager.currentTimeMillis(), parentTable);
                    }
                }
                // Load child table next
                ImmutableBytesPtr cacheKey = new ImmutableBytesPtr(key);
                // Get as of latest timestamp so we can detect if we have a newer table that already exists
                // without making an additional query
                PTable table = loadTable(env, key, cacheKey, clientTimeStamp, HConstants.LATEST_TIMESTAMP);
                if (table != null) {
                    if (table.getTimeStamp() < clientTimeStamp) {
                        // If the table is older than the client time stamp and it's deleted, continue
                        if (!isTableDeleted(table)) {
                            return new MetaDataMutationResult(MutationCode.TABLE_ALREADY_EXISTS, EnvironmentEdgeManager.currentTimeMillis(), table);
                        }
                    } else {
                        return new MetaDataMutationResult(MutationCode.NEWER_TABLE_FOUND, EnvironmentEdgeManager.currentTimeMillis(), table);
                    }
                }
                // TODO: Switch this to HRegion#batchMutate when we want to support indexes on the system
                // table. Basically, we get all the locks that we don't already hold for all the
                // tableMetadata rows. This ensures we don't have deadlock situations (ensuring primary and
                // then index table locks are held, in that order). For now, we just don't support indexing
                // on the system table. This is an issue because of the way we manage batch mutation in the
                // Indexer.
                region.mutateRowsWithLocks(tableMetadata, Collections.<byte[]>emptySet());
                
                // Invalidate the cache - the next getTable call will add it
                // TODO: consider loading the table that was just created here, patching up the parent table, and updating the cache
                Map<ImmutableBytesPtr,PTable> metaDataCache = GlobalCache.getInstance(this.getEnvironment()).getMetaDataCache();
                if (parentCacheKey != null) {
                    metaDataCache.remove(parentCacheKey);
                }
                metaDataCache.remove(cacheKey);
                // Get timeStamp from mutations - the above method sets it if it's unset
                long currentTimeStamp = MetaDataUtil.getClientTimeStamp(tableMetadata);
                return new MetaDataMutationResult(MutationCode.TABLE_NOT_FOUND, currentTimeStamp, null);
            } finally {
                releaseLocks(region, lids);
            }
        } catch (Throwable t) {
            ServerUtil.throwIOException(SchemaUtil.getTableName(schemaName, tableName), t);
            return null; // impossible
        }
    }

    private static void acquireLock(HRegion region, byte[] key, List<Integer> lids) throws IOException {
        Integer lid = region.getLock(null, key, true);
        if (lid == null) {
            throw new IOException("Failed to acquire lock on " + Bytes.toStringBinary(key));
        }
        lids.add(lid);
    }
    
    private static void releaseLocks(HRegion region, List<Integer> lids) {
        for (Integer lid : lids) {
            region.releaseRowLock(lid);
        }
    }
    
    
    @Override
    public MetaDataMutationResult dropTable(List<Mutation> tableMetadata, String tableType) throws IOException {
        byte[][] rowKeyMetaData = new byte[2][];
        MetaDataUtil.getSchemaAndTableName(tableMetadata,rowKeyMetaData);
        byte[] schemaName = rowKeyMetaData[PhoenixDatabaseMetaData.SCHEMA_NAME_INDEX];
        byte[] tableName = rowKeyMetaData[PhoenixDatabaseMetaData.TABLE_NAME_INDEX];
        // Disallow deletion of a system table
        if (tableType.equals(PTableType.SYSTEM.getSerializedValue())) {
            return new MetaDataMutationResult(MutationCode.UNALLOWED_TABLE_MUTATION, EnvironmentEdgeManager.currentTimeMillis(), null);
        }
        List<byte[]> tableNamesToDelete = Lists.newArrayList();
        try {
            byte[] parentTableName = MetaDataUtil.getParentTableName(tableMetadata);
            byte[] lockTableName = parentTableName == null ? tableName : parentTableName;
            byte[] lockKey = SchemaUtil.getTableKey(schemaName, lockTableName);
            byte[] key = parentTableName == null ? lockKey : SchemaUtil.getTableKey(schemaName, tableName);
            
            RegionCoprocessorEnvironment env = getEnvironment();
            HRegion region = env.getRegion();
            MetaDataMutationResult result = checkTableKeyInRegion(key, region);
            if (result != null) {
                return result; 
            }
            List<Integer> lids = Lists.newArrayList(5);
            try {
                acquireLock(region, lockKey, lids);
                if (key != lockKey) {
                    acquireLock(region, key, lids);
                }
                List<ImmutableBytesPtr> invalidateList = new ArrayList<ImmutableBytesPtr>();
                result = doDropTable(key, schemaName, tableName, PTableType.fromSerializedValue(tableType), tableMetadata, invalidateList, lids, tableNamesToDelete);
                if (result.getMutationCode() != MutationCode.TABLE_ALREADY_EXISTS || result.getTable() == null) {
                    return result;
                }
                Map<ImmutableBytesPtr,PTable> metaDataCache = GlobalCache.getInstance(this.getEnvironment()).getMetaDataCache();
                // Commit the list of deletion.
                region.mutateRowsWithLocks(tableMetadata, Collections.<byte[]>emptySet());
                long currentTime = MetaDataUtil.getClientTimeStamp(tableMetadata);
                for (ImmutableBytesPtr ckey: invalidateList) {
                    metaDataCache.put(ckey, newDeletedTableMarker(currentTime));
                }
                if (parentTableName != null) {
                    ImmutableBytesPtr parentCacheKey = new ImmutableBytesPtr(lockKey);
                    metaDataCache.remove(parentCacheKey);
                }
                return result;
            } finally {
                releaseLocks(region, lids);
            }
        } catch (Throwable t) {
            ServerUtil.throwIOException(SchemaUtil.getTableName(schemaName, tableName), t);
            return null; // impossible
        }
    }

    private MetaDataMutationResult doDropTable(byte[] key, byte[] schemaName, byte[] tableName, PTableType tableType, 
            List<Mutation> rowsToDelete, List<ImmutableBytesPtr> invalidateList, List<Integer> lids, List<byte[]> tableNamesToDelete) throws IOException, SQLException {
        long clientTimeStamp = MetaDataUtil.getClientTimeStamp(rowsToDelete);

        RegionCoprocessorEnvironment env = getEnvironment();
        HRegion region = env.getRegion();
        ImmutableBytesPtr cacheKey = new ImmutableBytesPtr(key);
        
        Map<ImmutableBytesPtr,PTable> metaDataCache = GlobalCache.getInstance(this.getEnvironment()).getMetaDataCache();
        PTable table = metaDataCache.get(cacheKey);
        
        // We always cache the latest version - fault in if not in cache
        if (table != null || (table = buildTable(key, cacheKey, region, HConstants.LATEST_TIMESTAMP)) != null) {
            if (table.getTimeStamp() < clientTimeStamp) {
                // If the table is older than the client time stamp and its deleted, continue
                if (isTableDeleted(table)) {
                    return new MetaDataMutationResult(MutationCode.TABLE_ALREADY_EXISTS, EnvironmentEdgeManager.currentTimeMillis(), null);
                }
                if ( tableType != table.getType())  {
                    // We said to drop a table, but found a view or visa versa
                    return new MetaDataMutationResult(MutationCode.TABLE_NOT_FOUND, EnvironmentEdgeManager.currentTimeMillis(), null);
                }
            } else {
                return new MetaDataMutationResult(MutationCode.NEWER_TABLE_FOUND, EnvironmentEdgeManager.currentTimeMillis(), table);
            }
        }
        if (table == null && buildDeletedTable(key, cacheKey, region, clientTimeStamp) != null) {
            return new MetaDataMutationResult(MutationCode.NEWER_TABLE_FOUND, EnvironmentEdgeManager.currentTimeMillis(), null);
        }
        // Get mutations for main table.
        Scan scan = newTableRowsScan(key, MIN_TABLE_TIMESTAMP, clientTimeStamp);
        RegionScanner scanner = region.getScanner(scan);
        List<KeyValue> results = Lists.newArrayList();
        scanner.next(results);
        if (results.isEmpty()) {
            return new MetaDataMutationResult(MutationCode.TABLE_NOT_FOUND, EnvironmentEdgeManager.currentTimeMillis(), null);
        }
        KeyValue typeKeyValue = KeyValueUtil.getColumnLatest(results, PhoenixDatabaseMetaData.TABLE_FAMILY_BYTES, PhoenixDatabaseMetaData.TABLE_TYPE_BYTES);
        assert(typeKeyValue != null && typeKeyValue.getValueLength() == 1);
        if ( tableType != PTableType.fromSerializedValue(typeKeyValue.getBuffer()[typeKeyValue.getValueOffset()]))  {
            // We said to drop a table, but found a view or visa versa
            return new MetaDataMutationResult(MutationCode.TABLE_NOT_FOUND, EnvironmentEdgeManager.currentTimeMillis(), null);
        }
        if (table.getType() != PTableType.VIEW) { // Add to list of HTables to delete, unless it's a view
            tableNamesToDelete.add(table.getName().getBytes());
        }
        List<byte[]> indexNames = Lists.newArrayList();
        invalidateList.add(cacheKey);
        byte[][] rowKeyMetaData = new byte[4][];
        byte[] rowKey;
        do {
            rowKey = results.get(0).getRow();
            int nColumns = getVarChars(rowKey, rowKeyMetaData);
            if (nColumns == 4 && rowKeyMetaData[PhoenixDatabaseMetaData.COLUMN_NAME_INDEX].length == 0 && rowKeyMetaData[PhoenixDatabaseMetaData.INDEX_NAME_INDEX].length > 0) {
                indexNames.add(rowKeyMetaData[PhoenixDatabaseMetaData.INDEX_NAME_INDEX]);
            }
            @SuppressWarnings("deprecation") // FIXME: Remove when unintentionally deprecated method is fixed (HBASE-7870).
            // FIXME: the version of the Delete constructor without the lock args was introduced
            // in 0.94.4, thus if we try to use it here we can no longer use the 0.94.2 version
            // of the client.
            Delete delete = new Delete(rowKey, clientTimeStamp, null);
            rowsToDelete.add(delete);
            results.clear();
            scanner.next(results);
        } while (!results.isEmpty());
        
        // Recursively delete indexes
        for (byte[] indexName : indexNames) {
            byte[] indexKey = SchemaUtil.getTableKey(schemaName, indexName);
            @SuppressWarnings("deprecation") // FIXME: Remove when unintentionally deprecated method is fixed (HBASE-7870).
            // FIXME: the version of the Delete constructor without the lock args was introduced
            // in 0.94.4, thus if we try to use it here we can no longer use the 0.94.2 version
            // of the client.
            Delete delete = new Delete(indexKey, clientTimeStamp, null);
            rowsToDelete.add(delete);
            acquireLock(region, indexKey, lids);
            MetaDataMutationResult result = doDropTable(indexKey, schemaName, indexName, PTableType.INDEX, rowsToDelete, invalidateList, lids, tableNamesToDelete);
            if (result.getMutationCode() != MutationCode.TABLE_ALREADY_EXISTS || result.getTable() == null) {
                return result;
            }
        }
        
        return new MetaDataMutationResult(MutationCode.TABLE_ALREADY_EXISTS, EnvironmentEdgeManager.currentTimeMillis(), table, tableNamesToDelete);
    }

    private static interface ColumnMutator {
        MetaDataMutationResult updateMutation(PTable table, byte[][] rowKeyMetaData, List<Mutation> tableMetadata, HRegion region, List<ImmutableBytesPtr> invalidateList, List<Integer> lids) throws IOException, SQLException;
    }

    private MetaDataMutationResult mutateColumn(List<Mutation> tableMetadata, ColumnMutator mutator) throws IOException {
        byte[][] rowKeyMetaData = new byte[4][];
        MetaDataUtil.getSchemaAndTableName(tableMetadata,rowKeyMetaData);
        byte[] schemaName = rowKeyMetaData[PhoenixDatabaseMetaData.SCHEMA_NAME_INDEX];
        byte[] tableName = rowKeyMetaData[PhoenixDatabaseMetaData.TABLE_NAME_INDEX];
        try {
            RegionCoprocessorEnvironment env = getEnvironment();
            byte[] key = SchemaUtil.getTableKey(schemaName,tableName);
            HRegion region = env.getRegion();
            MetaDataMutationResult result = checkTableKeyInRegion(key, region);
            if (result != null) {
                return result; 
            }
            List<Integer> lids = Lists.newArrayList(5);
            try {
                acquireLock(region, key, lids);
                ImmutableBytesPtr cacheKey = new ImmutableBytesPtr(key);
                List<ImmutableBytesPtr> invalidateList = new ArrayList<ImmutableBytesPtr>();
                invalidateList.add(cacheKey);
                Map<ImmutableBytesPtr,PTable> metaDataCache = GlobalCache.getInstance(this.getEnvironment()).getMetaDataCache();
                PTable table = metaDataCache.get(cacheKey);
                if (logger.isDebugEnabled()) {
                    if (table == null) {
                        logger.debug("Table " + Bytes.toStringBinary(key) + " not found in cache. Will build through scan");
                    } else {
                        logger.debug("Table " + Bytes.toStringBinary(key) + " found in cache with timestamp " + table.getTimeStamp() + " seqNum " + table.getSequenceNumber());
                    }
                }
                // Get client timeStamp from mutations
                long clientTimeStamp = MetaDataUtil.getClientTimeStamp(tableMetadata);
                if (table == null && (table = buildTable(key, cacheKey, region, HConstants.LATEST_TIMESTAMP)) == null) {
                    // if not found then call newerTableExists and add delete marker for timestamp found
                    if (buildDeletedTable(key, cacheKey, region, clientTimeStamp) != null) {
                        return new MetaDataMutationResult(MutationCode.NEWER_TABLE_FOUND, EnvironmentEdgeManager.currentTimeMillis(), null);
                    }
                    return new MetaDataMutationResult(MutationCode.TABLE_NOT_FOUND, EnvironmentEdgeManager.currentTimeMillis(), null);
                }
                if (table.getTimeStamp() >= clientTimeStamp) {
                    return new MetaDataMutationResult(MutationCode.NEWER_TABLE_FOUND, EnvironmentEdgeManager.currentTimeMillis(), table);
                } else if (isTableDeleted(table)) {
                    return new MetaDataMutationResult(MutationCode.TABLE_NOT_FOUND, EnvironmentEdgeManager.currentTimeMillis(), null);
                }
                    
                long expectedSeqNum = MetaDataUtil.getSequenceNumber(tableMetadata) - 1; // lookup TABLE_SEQ_NUM in tableMetaData
                if (logger.isDebugEnabled()) {
                    logger.debug("For table " + Bytes.toStringBinary(key) + " expecting seqNum " + expectedSeqNum + " and found seqNum " + table.getSequenceNumber() + " with " + table.getColumns().size() + " columns: " + table.getColumns());
                }
                if (expectedSeqNum != table.getSequenceNumber()) {
                    if (logger.isDebugEnabled()) {
                        logger.debug("For table " + Bytes.toStringBinary(key) + " returning CONCURRENT_TABLE_MUTATION due to unexpected seqNum");
                    }
                    return new MetaDataMutationResult(MutationCode.CONCURRENT_TABLE_MUTATION, EnvironmentEdgeManager.currentTimeMillis(), table);
                }
                
                // Disallow mutation of an index table
                PTableType type = table.getType();
                if (type == PTableType.INDEX) {
                    return new MetaDataMutationResult(MutationCode.UNALLOWED_TABLE_MUTATION, EnvironmentEdgeManager.currentTimeMillis(), null);
                }
                result = mutator.updateMutation(table, rowKeyMetaData, tableMetadata, region, invalidateList, lids);
                if (result != null) {
                    return result;
                }
                
                region.mutateRowsWithLocks(tableMetadata, Collections.<byte[]>emptySet());
                // Invalidate from cache
                for (ImmutableBytesPtr invalidateKey : invalidateList) {
                    PTable invalidatedTable = metaDataCache.remove(invalidateKey);
                    if (logger.isDebugEnabled()) {
                        if (invalidatedTable == null) {
                            logger.debug("Attempted to invalidated table key " + Bytes.toStringBinary(cacheKey.get(),cacheKey.getOffset(),cacheKey.getLength()) + " but found no cached table");
                        } else {
                            logger.debug("Invalidated table key " + Bytes.toStringBinary(cacheKey.get(),cacheKey.getOffset(),cacheKey.getLength()) + " with timestamp " + invalidatedTable.getTimeStamp() + " and seqNum " + invalidatedTable.getSequenceNumber());
                        }
                    }
                }
                // Get client timeStamp from mutations, since it may get updated by the mutateRowsWithLocks call
                long currentTime = MetaDataUtil.getClientTimeStamp(tableMetadata);
                return new MetaDataMutationResult(MutationCode.TABLE_ALREADY_EXISTS, currentTime, null);
            } finally {
                releaseLocks(region,lids);
            }
        } catch (Throwable t) {
            ServerUtil.throwIOException(SchemaUtil.getTableName(schemaName, tableName), t);
            return null; // impossible
        }
    }

    @Override
    public MetaDataMutationResult addColumn(List<Mutation> tableMetaData) throws IOException {
        return mutateColumn(tableMetaData, new ColumnMutator() {
            @Override
            public MetaDataMutationResult updateMutation(PTable table, byte[][] rowKeyMetaData, List<Mutation> tableMetaData, HRegion region, List<ImmutableBytesPtr> invalidateList, List<Integer> lids) {
                byte[] schemaName = rowKeyMetaData[SCHEMA_NAME_INDEX];
                byte[] tableName = rowKeyMetaData[TABLE_NAME_INDEX];
                for (Mutation m : tableMetaData) {
                    byte[] key = m.getRow();
                    boolean addingPKColumn = false;
                    int pkCount = getVarChars(key, rowKeyMetaData);
                    if (pkCount > COLUMN_NAME_INDEX 
                            && Bytes.compareTo(schemaName, rowKeyMetaData[SCHEMA_NAME_INDEX]) == 0 
                            && Bytes.compareTo(tableName, rowKeyMetaData[TABLE_NAME_INDEX]) == 0 ) {
                        try {
                            if (pkCount > FAMILY_NAME_INDEX && rowKeyMetaData[PhoenixDatabaseMetaData.FAMILY_NAME_INDEX].length > 0) {
                                PColumnFamily family = table.getColumnFamily(rowKeyMetaData[PhoenixDatabaseMetaData.FAMILY_NAME_INDEX]);
                                family.getColumn(rowKeyMetaData[PhoenixDatabaseMetaData.COLUMN_NAME_INDEX]);
                            } else if (pkCount > COLUMN_NAME_INDEX && rowKeyMetaData[PhoenixDatabaseMetaData.COLUMN_NAME_INDEX].length > 0) {
                                addingPKColumn = true;
                                table.getPKColumn(new String(rowKeyMetaData[PhoenixDatabaseMetaData.COLUMN_NAME_INDEX]));
                            } else {
                                continue;
                            }
                            return new MetaDataMutationResult(MutationCode.COLUMN_ALREADY_EXISTS, EnvironmentEdgeManager.currentTimeMillis(), table);
                        } catch (ColumnFamilyNotFoundException e) {
                            continue;
                        } catch (ColumnNotFoundException e) {
                            if (addingPKColumn) {
                                // Add all indexes to invalidate list, as they will all be adding the same PK column
                                // No need to lock them, as we have the parent table lock at this point
                                for (PTable index : table.getIndexes()) {
                                    invalidateList.add(new ImmutableBytesPtr(SchemaUtil.getTableKey(index.getSchemaName().getBytes(),index.getTableName().getBytes())));
                                }
                            }
                            continue;
                        }
                    }
                }
                return null;
            }
        });
    }
    
    @Override
    public MetaDataMutationResult dropColumn(List<Mutation> tableMetaData) throws IOException {
        final long clientTimeStamp = MetaDataUtil.getClientTimeStamp(tableMetaData);
        final List<byte[]> tableNamesToDelete = Lists.newArrayList();
        return mutateColumn(tableMetaData, new ColumnMutator() {
            @SuppressWarnings("deprecation")
            @Override
            public MetaDataMutationResult updateMutation(PTable table, byte[][] rowKeyMetaData, List<Mutation> tableMetaData, HRegion region, List<ImmutableBytesPtr> invalidateList, List<Integer> lids) throws IOException, SQLException {
                byte[] schemaName = rowKeyMetaData[SCHEMA_NAME_INDEX];
                byte[] tableName = rowKeyMetaData[TABLE_NAME_INDEX];
                boolean deletePKColumn = false;
                List<Mutation> additionalTableMetaData = Lists.newArrayList();
                for (Mutation m : tableMetaData) {
                    if (m instanceof Delete) {
                        byte[] key = m.getRow();
                        int pkCount = getVarChars(key, rowKeyMetaData);
                        if (pkCount > COLUMN_NAME_INDEX 
                                && Bytes.compareTo(schemaName, rowKeyMetaData[SCHEMA_NAME_INDEX]) == 0 
                                && Bytes.compareTo(tableName, rowKeyMetaData[TABLE_NAME_INDEX]) == 0 ) {
                            PColumn columnToDelete = null;
                            try {
                                if (pkCount > FAMILY_NAME_INDEX && rowKeyMetaData[PhoenixDatabaseMetaData.FAMILY_NAME_INDEX].length > 0) {
                                    PColumnFamily family = table.getColumnFamily(rowKeyMetaData[PhoenixDatabaseMetaData.FAMILY_NAME_INDEX]);
                                    columnToDelete = family.getColumn(rowKeyMetaData[PhoenixDatabaseMetaData.COLUMN_NAME_INDEX]);
                                } else if (pkCount > COLUMN_NAME_INDEX && rowKeyMetaData[PhoenixDatabaseMetaData.COLUMN_NAME_INDEX].length > 0) {
                                    deletePKColumn = true;
                                    columnToDelete = table.getPKColumn(new String(rowKeyMetaData[PhoenixDatabaseMetaData.COLUMN_NAME_INDEX]));
                                } else {
                                    continue;
                                }
                                // Look for columnToDelete in any indexes. If found as PK column, get lock and drop the index. If found as covered column, delete from index (do this client side?).
                                // In either case, invalidate index if the column is in it
                                for (PTable index : table.getIndexes()) {
                                    try {
                                        String indexColumnName = IndexUtil.getIndexColumnName(columnToDelete);
                                        PColumn indexColumn = index.getColumn(indexColumnName);
                                        byte[] indexKey = SchemaUtil.getTableKey(index.getSchemaName().getBytes(), index.getTableName().getBytes());
                                        // If index contains the column in it's PK, then drop it
                                        if (SchemaUtil.isPKColumn(indexColumn)) {
                                            // Since we're dropping the index, lock it to ensure that a change in index state doesn't
                                            // occur while we're dropping it.
                                            acquireLock(region, indexKey, lids);
                                            // Drop the index table. The doDropTable will expand this to all of the table rows and invalidate the index table
                                            additionalTableMetaData.add(new Delete(indexKey, clientTimeStamp, null));
                                            byte[] linkKey = MetaDataUtil.getParentLinkKey(schemaName, tableName, index.getTableName().getBytes());
                                            // Drop the link between the data table and the index table
                                            additionalTableMetaData.add(new Delete(linkKey, clientTimeStamp, null));
                                            doDropTable(indexKey, index.getSchemaName().getBytes(), index.getTableName().getBytes(), index.getType(), additionalTableMetaData, invalidateList, lids, tableNamesToDelete);
                                            // TODO: return in result?
                                        } else {
                                            invalidateList.add(new ImmutableBytesPtr(indexKey));
                                        }
                                    } catch (ColumnNotFoundException e) {
                                    } catch (AmbiguousColumnException e) {
                                    }
                                }
                            } catch (ColumnFamilyNotFoundException e) {
                                return new MetaDataMutationResult(MutationCode.COLUMN_NOT_FOUND, EnvironmentEdgeManager.currentTimeMillis(), table, columnToDelete);
                            } catch (ColumnNotFoundException e) {
                                return new MetaDataMutationResult(MutationCode.COLUMN_NOT_FOUND, EnvironmentEdgeManager.currentTimeMillis(), table, columnToDelete);
                            }
                        }
                    }
                }
                if (deletePKColumn) {
                    if (table.getPKColumns().size() == 1) {
                        return new MetaDataMutationResult(MutationCode.NO_PK_COLUMNS, EnvironmentEdgeManager.currentTimeMillis(), null);
                    }
                }
                tableMetaData.addAll(additionalTableMetaData);
                return null;
            }
        });
        
    }
    
    private static MetaDataMutationResult checkTableKeyInRegion(byte[] key, HRegion region) {
        byte[] startKey = region.getStartKey();
        byte[] endKey = region.getEndKey();
        if (Bytes.compareTo(startKey, key) <= 0 && (Bytes.compareTo(HConstants.LAST_ROW, endKey) == 0 || Bytes.compareTo(key, endKey) < 0)) {
            return null; // normal case;
        }
        return new MetaDataMutationResult(MutationCode.TABLE_NOT_IN_REGION, EnvironmentEdgeManager.currentTimeMillis(), null);
    }

    @Override
    public MetaDataMutationResult getTable(byte[] schemaName, byte[] tableName, long tableTimeStamp, long clientTimeStamp) throws IOException {
        try {
            byte[] key = SchemaUtil.getTableKey(schemaName, tableName);
            
            // get the co-processor environment
            RegionCoprocessorEnvironment env = getEnvironment();
            // TODO: check that key is within region.getStartKey() and region.getEndKey()
            // and return special code to force client to lookup region from meta.
            HRegion region = env.getRegion();
            MetaDataMutationResult result = checkTableKeyInRegion(key, region);
            if (result != null) {
                return result; 
            }
            
            long currentTime = EnvironmentEdgeManager.currentTimeMillis();
            PTable table = doGetTable(key, clientTimeStamp);
            if (table == null) {
                return new MetaDataMutationResult(MutationCode.TABLE_NOT_FOUND, currentTime, null);
            }
            return new MetaDataMutationResult(MutationCode.TABLE_ALREADY_EXISTS, currentTime, table.getTimeStamp() != tableTimeStamp ? table : null);
        } catch (Throwable t) {
            ServerUtil.throwIOException(SchemaUtil.getTableName(schemaName, tableName), t);
            return null; // impossible
        }
    }

    private PTable doGetTable(byte[] key, long clientTimeStamp) throws IOException, SQLException {
        ImmutableBytesPtr cacheKey = new ImmutableBytesPtr(key);
        Map<ImmutableBytesPtr,PTable> metaDataCache = GlobalCache.getInstance(this.getEnvironment()).getMetaDataCache();
        PTable table = metaDataCache.get(cacheKey);
        // We only cache the latest, so we'll end up building the table with every call if the client connection has specified an SCN.
        // TODO: If we indicate to the client that we're returning an older version, but there's a newer version available, the client
        // can safely not call this, since we only allow modifications to the latest.
        if (table != null && table.getTimeStamp() < clientTimeStamp) {
            // Table on client is up-to-date with table on server, so just return
            if (isTableDeleted(table)) {
                return null;
            }
            return table;
        }
        // Ask Lars about the expense of this call - if we don't take the lock, we still won't get partial results
        // get the co-processor environment
        RegionCoprocessorEnvironment env = getEnvironment();
        // TODO: check that key is within region.getStartKey() and region.getEndKey()
        // and return special code to force client to lookup region from meta.
        HRegion region = env.getRegion();
        /*
         * Lock directly on key, though it may be an index table.
         * This will just prevent a table from getting rebuilt
         * too often.
         */
        Integer lid = region.getLock(null, key, true);
        if (lid == null) {
            throw new IOException("Failed to acquire lock on " + Bytes.toStringBinary(key));
        }
        try {
            // Try cache again in case we were waiting on a lock
            table = metaDataCache.get(cacheKey);
            // We only cache the latest, so we'll end up building the table with every call if the client connection has specified an SCN.
            // TODO: If we indicate to the client that we're returning an older version, but there's a newer version available, the client
            // can safely not call this, since we only allow modifications to the latest.
            if (table != null && table.getTimeStamp() < clientTimeStamp) {
                // Table on client is up-to-date with table on server, so just return
                if (isTableDeleted(table)) {
                    return null;
                }
                return table;
            }
            // Query for the latest table first, since it's not cached
            table = buildTable(key, cacheKey, region, HConstants.LATEST_TIMESTAMP);
            if (table != null && table.getTimeStamp() < clientTimeStamp) {
                return table;
            }
            // Otherwise, query for an older version of the table - it won't be cached 
            return buildTable(key, cacheKey, region, clientTimeStamp);
        } finally {
            if (lid != null) region.releaseRowLock(lid);
        }
    }

    @Override
    public void clearCache() {
        Map<ImmutableBytesPtr,PTable> metaDataCache = GlobalCache.getInstance(this.getEnvironment()).getMetaDataCache();
        metaDataCache.clear();
    }

    @Override
    public long getVersion() {
        // The first 3 bytes of the long is used to encoding the HBase version as major.minor.patch.
        // The next 4 bytes of the value is used to encode the Phoenix version as major.minor.patch.
        long version = MetaDataUtil.encodeHBaseAndPhoenixVersions(this.getEnvironment().getHBaseVersion());
        
        // The last byte is used to communicate whether or not mutable secondary indexing
        // was configured properly.
        RegionCoprocessorEnvironment env = getEnvironment();
        version = MetaDataUtil.encodeMutableIndexConfiguredProperly(
                version, 
                IndexManagementUtil.isWALEditCodecSet(env.getConfiguration()));
        return version;
    }

    @Override
    public MetaDataMutationResult updateIndexState(List<Mutation> tableMetadata) throws IOException {
        byte[][] rowKeyMetaData = new byte[2][];
        MetaDataUtil.getSchemaAndTableName(tableMetadata,rowKeyMetaData);
        byte[] schemaName = rowKeyMetaData[PhoenixDatabaseMetaData.SCHEMA_NAME_INDEX];
        byte[] tableName = rowKeyMetaData[PhoenixDatabaseMetaData.TABLE_NAME_INDEX];
        try {
            RegionCoprocessorEnvironment env = getEnvironment();
            byte[] key = SchemaUtil.getTableKey(schemaName,tableName);
            HRegion region = env.getRegion();
            MetaDataMutationResult result = checkTableKeyInRegion(key, region);
            if (result != null) {
                return result; 
            }
            long timeStamp = MetaDataUtil.getClientTimeStamp(tableMetadata);
            ImmutableBytesPtr cacheKey = new ImmutableBytesPtr(key);
            List<KeyValue> newKVs = tableMetadata.get(0).getFamilyMap().get(TABLE_FAMILY_BYTES);
            KeyValue newKV = newKVs.get(0);
            PIndexState newState =  PIndexState.fromSerializedValue(newKV.getBuffer()[newKV.getValueOffset()]);
            Integer lid = region.getLock(null, key, true);
            if (lid == null) {
                throw new IOException("Failed to acquire lock on " + Bytes.toStringBinary(key));
            }
            try {
                Get get = new Get(key);
                get.setTimeRange(PTable.INITIAL_SEQ_NUM, timeStamp);
                get.addColumn(TABLE_FAMILY_BYTES, INDEX_STATE_BYTES);
                Result currentResult = region.get(get);
                if (currentResult.raw().length == 0) {
                    return new MetaDataMutationResult(MutationCode.TABLE_NOT_FOUND, EnvironmentEdgeManager.currentTimeMillis(), null);
                }
                KeyValue currentStateKV = currentResult.raw()[0];
                PIndexState currentState = PIndexState.fromSerializedValue(currentStateKV.getBuffer()[currentStateKV.getValueOffset()]);
                // Detect invalid transitions
                if (currentState == PIndexState.BUILDING) {
                    if (newState == PIndexState.USABLE) {
                        return new MetaDataMutationResult(MutationCode.UNALLOWED_TABLE_MUTATION, EnvironmentEdgeManager.currentTimeMillis(), null);
                    }
                } else if (currentState == PIndexState.DISABLE) {
                    if (newState != PIndexState.BUILDING && newState != PIndexState.DISABLE) {
                        return new MetaDataMutationResult(MutationCode.UNALLOWED_TABLE_MUTATION, EnvironmentEdgeManager.currentTimeMillis(), null);
                    }
                    // Done building, but was disable before that, so that in disabled state
                    if (newState == PIndexState.ACTIVE) {
                        newState = PIndexState.DISABLE;
                    }
                }

                if (currentState == PIndexState.BUILDING && newState != PIndexState.ACTIVE) {
                    timeStamp = currentStateKV.getTimestamp();
                }
                if ((currentState == PIndexState.UNUSABLE && newState == PIndexState.ACTIVE) || (currentState == PIndexState.ACTIVE && newState == PIndexState.UNUSABLE)) {
                    newState = PIndexState.INACTIVE;
                    newKVs.set(0, KeyValueUtil.newKeyValue(key, TABLE_FAMILY_BYTES, INDEX_STATE_BYTES, timeStamp, Bytes.toBytes(newState.getSerializedValue())));
                } else if (currentState == PIndexState.INACTIVE && newState == PIndexState.USABLE) {
                    newState = PIndexState.ACTIVE;
                    newKVs.set(0, KeyValueUtil.newKeyValue(key, TABLE_FAMILY_BYTES, INDEX_STATE_BYTES, timeStamp, Bytes.toBytes(newState.getSerializedValue())));
                }
                if (currentState != newState) {
                    region.mutateRowsWithLocks(tableMetadata, Collections.<byte[]>emptySet());
                    // Invalidate from cache
                    Map<ImmutableBytesPtr,PTable> metaDataCache = GlobalCache.getInstance(this.getEnvironment()).getMetaDataCache();
                    metaDataCache.remove(cacheKey);
                }
                // Get client timeStamp from mutations, since it may get updated by the mutateRowsWithLocks call
                long currentTime = MetaDataUtil.getClientTimeStamp(tableMetadata);
                return new MetaDataMutationResult(MutationCode.TABLE_ALREADY_EXISTS, currentTime, null);
            } finally {
                region.releaseRowLock(lid);
            }
        } catch (Throwable t) {
            ServerUtil.throwIOException(SchemaUtil.getTableName(schemaName, tableName), t);
            return null; // impossible
        }
    }
}
