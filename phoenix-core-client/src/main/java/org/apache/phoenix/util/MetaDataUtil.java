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

import static org.apache.phoenix.jdbc.PhoenixDatabaseMetaData.*;
import static org.apache.phoenix.util.SchemaUtil.getVarChars;

import java.nio.charset.StandardCharsets;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.*;

import org.apache.phoenix.schema.ColumnFamilyNotFoundException;
import org.apache.phoenix.schema.ColumnNotFoundException;
import org.apache.phoenix.schema.PColumn;
import org.apache.phoenix.schema.PColumnFamily;
import org.apache.phoenix.schema.PName;
import org.apache.phoenix.schema.PNameFactory;
import org.apache.phoenix.schema.PTable;
import org.apache.phoenix.schema.PTableType;
import org.apache.phoenix.schema.SequenceKey;
import org.apache.phoenix.schema.SortOrder;
import org.apache.phoenix.schema.TableNotFoundException;
import org.apache.phoenix.schema.TableProperty;
import org.apache.phoenix.schema.types.PVarchar;
import org.apache.phoenix.thirdparty.com.google.common.annotations.VisibleForTesting;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.ExtendedCell;
import org.apache.hadoop.hbase.ExtendedCellBuilder;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.TagUtil;
import org.apache.hadoop.hbase.client.ColumnFamilyDescriptor;
import org.apache.hadoop.hbase.client.ColumnFamilyDescriptorBuilder;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Mutation;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.TableDescriptor;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.phoenix.coprocessorclient.MetaDataProtocol;
import org.apache.phoenix.exception.SQLExceptionCode;
import org.apache.phoenix.hbase.index.util.GenericKeyValueBuilder;
import org.apache.phoenix.hbase.index.util.ImmutableBytesPtr;
import org.apache.phoenix.hbase.index.util.IndexManagementUtil;
import org.apache.phoenix.hbase.index.util.KeyValueBuilder;
import org.apache.phoenix.hbase.index.util.VersionUtil;
import org.apache.phoenix.jdbc.PhoenixConnection;
import org.apache.phoenix.jdbc.PhoenixDatabaseMetaData;
import org.apache.phoenix.query.QueryConstants;
import org.apache.phoenix.schema.PTable.IndexType;
import org.apache.phoenix.schema.PTable.LinkType;

import org.apache.phoenix.schema.types.PBoolean;
import org.apache.phoenix.schema.types.PDataType;
import org.apache.phoenix.schema.types.PInteger;
import org.apache.phoenix.schema.types.PLong;
import org.apache.phoenix.schema.types.PSmallint;
import org.apache.phoenix.schema.types.PUnsignedTinyint;
import org.apache.phoenix.thirdparty.com.google.common.collect.ImmutableMap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.phoenix.thirdparty.com.google.common.collect.ImmutableList;
import org.apache.phoenix.thirdparty.com.google.common.collect.Iterables;
import org.apache.phoenix.thirdparty.com.google.common.collect.Lists;

public class MetaDataUtil {
    private static final Logger LOGGER = LoggerFactory.getLogger(MetaDataUtil.class);
  
    public static final String VIEW_INDEX_TABLE_PREFIX = "_IDX_";
    public static final String LOCAL_INDEX_TABLE_PREFIX = "_LOCAL_IDX_";
    public static final String VIEW_INDEX_SEQUENCE_PREFIX = "_SEQ_";
    public static final String VIEW_INDEX_SEQUENCE_NAME_PREFIX = "_ID_";
    public static final byte[] VIEW_INDEX_SEQUENCE_PREFIX_BYTES = Bytes.toBytes(VIEW_INDEX_SEQUENCE_PREFIX);
    public static final String VIEW_INDEX_ID_COLUMN_NAME = "_INDEX_ID";
    public static final String PARENT_TABLE_KEY = "PARENT_TABLE";
    public static final String IS_VIEW_INDEX_TABLE_PROP_NAME = "IS_VIEW_INDEX_TABLE";
    public static final byte[] IS_VIEW_INDEX_TABLE_PROP_BYTES = Bytes.toBytes(IS_VIEW_INDEX_TABLE_PROP_NAME);

    public static final String IS_LOCAL_INDEX_TABLE_PROP_NAME = "IS_LOCAL_INDEX_TABLE";
    public static final byte[] IS_LOCAL_INDEX_TABLE_PROP_BYTES = Bytes.toBytes(IS_LOCAL_INDEX_TABLE_PROP_NAME);

    public static final String DATA_TABLE_NAME_PROP_NAME = "DATA_TABLE_NAME";

    public static final byte[] DATA_TABLE_NAME_PROP_BYTES = Bytes.toBytes(DATA_TABLE_NAME_PROP_NAME);

    private static final Map<MajorMinorVersion, MajorMinorVersion> ALLOWED_SERVER_CLIENT_MAJOR_VERSION =
            ImmutableMap.of(
                    new MajorMinorVersion(5, 1), new MajorMinorVersion(4, 16)
            );

    // See PHOENIX-3955
    public static final List<String> SYNCED_DATA_TABLE_AND_INDEX_COL_FAM_PROPERTIES = ImmutableList.of(
            ColumnFamilyDescriptorBuilder.TTL,
            ColumnFamilyDescriptorBuilder.KEEP_DELETED_CELLS,
            ColumnFamilyDescriptorBuilder.REPLICATION_SCOPE);

    public static Put getLastDDLTimestampUpdate(byte[] tableHeaderRowKey,
                                                     long clientTimestamp,
                                                     long lastDDLTimestamp) {
        //use client timestamp as the timestamp of the Cell, to match the other Cells that might
        // be created by this DDL. But the actual value will be a _server_ timestamp
        Put p = new Put(tableHeaderRowKey, clientTimestamp);
        byte[] lastDDLTimestampBytes = PLong.INSTANCE.toBytes(lastDDLTimestamp);
        p.addColumn(PhoenixDatabaseMetaData.TABLE_FAMILY_BYTES,
            PhoenixDatabaseMetaData.LAST_DDL_TIMESTAMP_BYTES, lastDDLTimestampBytes);
        return p;
    }

    public static Put getExternalSchemaIdUpdate(byte[] tableHeaderRowKey,
                                                String externalSchemaId) {
        //use client timestamp as the timestamp of the Cell, to match the other Cells that might
        // be created by this DDL. But the actual value will be a _server_ timestamp
        Put p = new Put(tableHeaderRowKey);
        byte[] externalSchemaIdBytes = PVarchar.INSTANCE.toBytes(externalSchemaId);
        p.addColumn(PhoenixDatabaseMetaData.TABLE_FAMILY_BYTES,
            PhoenixDatabaseMetaData.EXTERNAL_SCHEMA_ID_BYTES, externalSchemaIdBytes);
        return p;
    }

    /**
     * Checks if a table is meant to be queried directly (and hence is relevant to external
     * systems tracking Phoenix schema)
     * @param tableType
     * @return True if a table or view, false otherwise (such as for an index, system table, or
     * subquery)
     */
    public static boolean isTableDirectlyQueried(PTableType tableType) {
        return tableType.equals(PTableType.TABLE) || tableType.equals(PTableType.VIEW);
    }

    public static class ClientServerCompatibility {

        private int errorCode;
        private boolean isCompatible;

        ClientServerCompatibility() {
            this.errorCode = 0;
        }

        public int getErrorCode() {
            return this.errorCode;
        }

        void setErrorCode(int errorCode) {
            this.errorCode = errorCode;
        }

        public boolean getIsCompatible() {
            return this.isCompatible;
        }

        void setCompatible(boolean isCompatible) {
            this.isCompatible = isCompatible;
        }
    }

    public static ClientServerCompatibility areClientAndServerCompatible(long serverHBaseAndPhoenixVersion) {
        // As of 3.0, we allow a client and server to differ for the minor version.
        // Care has to be taken to upgrade the server before the client, as otherwise
        // the client may call expressions that don't yet exist on the server.
        // Differing by the patch version has always been allowed.
        // Only differing by the major version is not allowed.
        return areClientAndServerCompatible(MetaDataUtil.decodePhoenixVersion(serverHBaseAndPhoenixVersion), MetaDataProtocol.PHOENIX_MAJOR_VERSION, MetaDataProtocol.PHOENIX_MINOR_VERSION);
    }

    // Default scope for testing
    @VisibleForTesting
    static ClientServerCompatibility areClientAndServerCompatible(int serverVersion, int clientMajorVersion, int clientMinorVersion) {
        // A server and client with the same major and minor version number must be compatible.
        // So it's important that we roll the PHOENIX_MAJOR_VERSION or PHOENIX_MINOR_VERSION
        // when we make an incompatible change.
        ClientServerCompatibility compatibility = new ClientServerCompatibility();
        if (VersionUtil.encodeMinPatchVersion(clientMajorVersion, clientMinorVersion) > serverVersion) { // Client major and minor cannot be ahead of server
            compatibility.setErrorCode(SQLExceptionCode.OUTDATED_JARS.getErrorCode());
            compatibility.setCompatible(false);
            return compatibility;
        } else if (VersionUtil.encodeMaxMinorVersion(clientMajorVersion) < serverVersion) { // Client major version must at least be up to server major version
            MajorMinorVersion serverMajorMinorVersion = new MajorMinorVersion(
                    VersionUtil.decodeMajorVersion(serverVersion),
                    VersionUtil.decodeMinorVersion(serverVersion));
            MajorMinorVersion clientMajorMinorVersion =
                    new MajorMinorVersion(clientMajorVersion, clientMinorVersion);
            if (!clientMajorMinorVersion.equals(
                    ALLOWED_SERVER_CLIENT_MAJOR_VERSION.get(serverMajorMinorVersion))) {
                // Incompatible if not whitelisted by ALLOWED_SERVER_CLIENT_MAJOR_VERSION
                compatibility.setErrorCode(SQLExceptionCode
                        .INCOMPATIBLE_CLIENT_SERVER_JAR.getErrorCode());
                compatibility.setCompatible(false);
                return compatibility;
            }
        }
        compatibility.setCompatible(true);
        return compatibility;
    }

    // Given the encoded integer representing the phoenix version in the encoded version value.
    // The second byte in int would be the major version, 3rd byte minor version, and 4th byte 
    // patch version.
    public static int decodePhoenixVersion(long version) {
        return (int) ((version << Byte.SIZE * 4) >>> Byte.SIZE * 5);
    }
    
    // TODO: generalize this to use two bytes to return a SQL error code instead
    public static long encodeHasIndexWALCodec(long version, boolean isValid) {
        if (!isValid) {
            return version | 1;
        }
        return version;
    }
    
    public static boolean decodeHasIndexWALCodec(long version) {
        return (version & 0xF) == 0;
    }

    // Given the encoded integer representing the client hbase version in the encoded version value.
    // The second byte in int would be the major version, 3rd byte minor version, and 4th byte 
    // patch version.
    public static int decodeHBaseVersion(long version) {
        return (int) (version >>> Byte.SIZE * 5);
    }

    public static String decodeHBaseVersionAsString(int version) {
        int major = (version >>> Byte.SIZE  * 2) & 0xFF;
        int minor = (version >>> Byte.SIZE  * 1) & 0xFF;
        int patch = version & 0xFF;
        return major + "." + minor + "." + patch;
    }
    
    // Given the encoded integer representing the phoenix version in the encoded version value.
    // The second byte in int would be the major version, 3rd byte minor version, and 4th byte
    // patch version.
    public static boolean decodeTableNamespaceMappingEnabled(long version) {
        return ((int)((version << Byte.SIZE * 3) >>> Byte.SIZE * 7) & 0x1) != 0;
    }

    // The first three bytes of the long encode the HBase version as major.minor.patch.
    // The fourth byte is isTableNamespaceMappingEnabled
    // The fifth to seventh bytes of the value encode the Phoenix version as major.minor.patch.
    // The eights byte encodes whether the WAL codec is correctly installed
    /**
     * Encode HBase and Phoenix version along with some server-side config information such as whether WAL codec is
     * installed (necessary for non transactional, mutable secondar indexing), and whether systemNamespace mapping is enabled.
     * 
     * @param hbaseVersionStr
     * @param config
     * @return long value sent back during initialization of a cluster connection.
     */
    public static long encodeVersion(String hbaseVersionStr, Configuration config) {
        long hbaseVersion = VersionUtil.encodeVersion(hbaseVersionStr);
        long isTableNamespaceMappingEnabled = SchemaUtil.isNamespaceMappingEnabled(PTableType.TABLE,
                new ReadOnlyProps(config.iterator())) ? 1 : 0;
        long phoenixVersion = VersionUtil.encodeVersion(MetaDataProtocol.PHOENIX_MAJOR_VERSION,
                MetaDataProtocol.PHOENIX_MINOR_VERSION, MetaDataProtocol.PHOENIX_PATCH_NUMBER);
        long walCodec = IndexManagementUtil.isWALEditCodecSet(config) ? 0 : 1;
        long version =
        // Encode HBase major, minor, patch version
        (hbaseVersion << (Byte.SIZE * 5))
                // Encode if table namespace mapping is enabled on the server side
                // Note that we DO NOT return information on whether system tables are mapped
                // on the server side
                | (isTableNamespaceMappingEnabled << (Byte.SIZE * 4))
                // Encode Phoenix major, minor, patch version
                | (phoenixVersion << (Byte.SIZE * 1))
                // Encode whether or not non transactional, mutable secondary indexing was configured properly.
                | walCodec;
        return version;
    }
    
    public static byte[] getTenantIdAndSchemaAndTableName(Mutation someRow) {
        byte[][] rowKeyMetaData = new byte[3][];
        getVarChars(someRow.getRow(), 3, rowKeyMetaData);
        return ByteUtil.concat(rowKeyMetaData[0], rowKeyMetaData[1], rowKeyMetaData[2]);
    }

    public static byte[] getTenantIdAndSchemaAndTableName(Result result) {
        byte[][] rowKeyMetaData = new byte[3][];
        getVarChars(result.getRow(), 3, rowKeyMetaData);
        return ByteUtil.concat(rowKeyMetaData[0], rowKeyMetaData[1], rowKeyMetaData[2]);
    }

    public static void getTenantIdAndSchemaAndTableName(List<Mutation> tableMetadata, byte[][] rowKeyMetaData) {
        Mutation m = getTableHeaderRow(tableMetadata);
        getVarChars(m.getRow(), 3, rowKeyMetaData);
    }

    public static int getBaseColumnCount(List<Mutation> tableMetadata) {
        int result = -1;
        for (Mutation mutation : tableMetadata) {
            for (List<Cell> cells : mutation.getFamilyCellMap().values()) {
                for (Cell cell : cells) {
                    // compare using offsets
                    if (Bytes.compareTo(cell.getQualifierArray(), cell.getQualifierOffset(), cell.getQualifierLength(), PhoenixDatabaseMetaData.BASE_COLUMN_COUNT_BYTES, 0,
                        PhoenixDatabaseMetaData.BASE_COLUMN_COUNT_BYTES.length) == 0)
                    if (Bytes.contains(cell.getQualifierArray(), PhoenixDatabaseMetaData.BASE_COLUMN_COUNT_BYTES)) {
                        result = PInteger.INSTANCE.getCodec()
                            .decodeInt(cell.getValueArray(), cell.getValueOffset(), SortOrder.ASC);
                    }
                }
            }
        }
        return result;
    }

    public static void mutatePutValue(Put somePut, byte[] family, byte[] qualifier, byte[] newValue) {
        NavigableMap<byte[], List<Cell>> familyCellMap = somePut.getFamilyCellMap();
        List<Cell> cells = familyCellMap.get(family);
        List<Cell> newCells = Lists.newArrayList();
        if (cells != null) {
            for (Cell cell : cells) {
                if (Bytes.compareTo(cell.getQualifierArray(), cell.getQualifierOffset(), cell.getQualifierLength(),
                    qualifier, 0, qualifier.length) == 0) {
                    Cell replacementCell = new KeyValue(cell.getRowArray(), cell.getRowOffset(), cell.getRowLength(),
                        cell.getFamilyArray(), cell.getFamilyOffset(), cell.getFamilyLength(), cell.getQualifierArray(),
                        cell.getQualifierOffset(), cell.getQualifierLength(), cell.getTimestamp(),
                        KeyValue.Type.codeToType(cell.getType().getCode()), newValue, 0, newValue.length);
                    newCells.add(replacementCell);
                } else {
                    newCells.add(cell);
                }
            }
            familyCellMap.put(family, newCells);
        }
    }

    /**
     * Iterates over the cells that are mutated by the put operation for the given column family and
     * column qualifier and conditionally modifies those cells to add a tags list. We only add tags
     * if the cell value does not match the passed valueArray. If we always want to add tags to
     * these cells, we can pass in a null valueArray
     * @param somePut Put operation
     * @param family column family of the cells
     * @param qualifier column qualifier of the cells
     * @param cellBuilder ExtendedCellBuilder object
     * @param valueArray byte array of values or null
     * @param tagArray byte array of tags to add to the cells
     */
    public static void conditionallyAddTagsToPutCells(Put somePut, byte[] family, byte[] qualifier,
            ExtendedCellBuilder cellBuilder, byte[] valueArray, byte[] tagArray) {
        NavigableMap<byte[], List<Cell>> familyCellMap = somePut.getFamilyCellMap();
        List<Cell> cells = familyCellMap.get(family);
        List<Cell> newCells = Lists.newArrayList();
        if (cells != null) {
            for (Cell cell : cells) {
                if (Bytes.compareTo(cell.getQualifierArray(), cell.getQualifierOffset(),
                        cell.getQualifierLength(), qualifier, 0, qualifier.length) == 0 &&
                        (valueArray == null || !CellUtil.matchingValue(cell, valueArray))) {
                    ExtendedCell extendedCell = cellBuilder
                            .setRow(cell.getRowArray(), cell.getRowOffset(), cell.getRowLength())
                            .setFamily(cell.getFamilyArray(), cell.getFamilyOffset(),
                                    cell.getFamilyLength())
                            .setQualifier(cell.getQualifierArray(), cell.getQualifierOffset(),
                                    cell.getQualifierLength())
                            .setValue(cell.getValueArray(), cell.getValueOffset(),
                                    cell.getValueLength())
                            .setTimestamp(cell.getTimestamp())
                            .setType(cell.getType())
                            .setTags(TagUtil.concatTags(tagArray, cell))
                            .build();
                    // Replace existing cell with a cell that has the custom tags list
                    newCells.add(extendedCell);
                } else {
                    // Add cell as is
                    newCells.add(cell);
                }
            }
            familyCellMap.put(family, newCells);
        }
    }

    public static Put cloneDeleteToPutAndAddColumn(Delete delete, byte[] family, byte[] qualifier, byte[] value) {
        NavigableMap<byte[], List<Cell>> familyCellMap = delete.getFamilyCellMap();
        List<Cell> cells = familyCellMap.get(family);
        Cell cell = Iterables.getFirst(cells, null);
        if (cell == null) {
            throw new RuntimeException("Empty cells for delete for family: " + Bytes.toStringBinary(family));
        }
        byte[] rowArray = new byte[cell.getRowLength()];
        System.arraycopy(cell.getRowArray(), cell.getRowOffset(), rowArray, 0, cell.getRowLength());
        Put put = new Put(rowArray, delete.getTimestamp());
        put.addColumn(family, qualifier, delete.getTimestamp(), value);
        return put;
    }


    public static void getTenantIdAndFunctionName(List<Mutation> functionMetadata, byte[][] rowKeyMetaData) {
        Mutation m = getTableHeaderRow(functionMetadata);
        getVarChars(m.getRow(), 2, rowKeyMetaData);
    }

    /**
     * Only return the parent table name if it has the same tenant id and schema name as the current
     * table (this is only used to lock the parent table of indexes)
     */
    public static byte[] getParentTableName(List<Mutation> tableMetadata) {
        if (tableMetadata.size() == 1) {
            return null;
        }
        byte[][] rowKeyMetaData = new byte[3][];
        // get the tenantId, schema name and table name for the current table
        getTenantIdAndSchemaAndTableName(tableMetadata, rowKeyMetaData);
        byte[] tenantId = rowKeyMetaData[PhoenixDatabaseMetaData.TENANT_ID_INDEX];
        byte[] schemaName = rowKeyMetaData[PhoenixDatabaseMetaData.SCHEMA_NAME_INDEX];
        byte[] tableName = rowKeyMetaData[PhoenixDatabaseMetaData.TABLE_NAME_INDEX];
        // get the tenantId, schema name and table name for the parent table
        Mutation m = getParentTableHeaderRow(tableMetadata);
        getVarChars(m.getRow(), 3, rowKeyMetaData);
        if (Bytes.compareTo(tenantId, rowKeyMetaData[PhoenixDatabaseMetaData.TENANT_ID_INDEX]) == 0
                && Bytes.compareTo(schemaName,
                    rowKeyMetaData[PhoenixDatabaseMetaData.SCHEMA_NAME_INDEX]) == 0
                && Bytes.compareTo(tableName,
                    rowKeyMetaData[PhoenixDatabaseMetaData.TABLE_NAME_INDEX]) == 0) {
            return null;
        }
        return rowKeyMetaData[PhoenixDatabaseMetaData.TABLE_NAME_INDEX];
    }
    
    public static long getSequenceNumber(Mutation tableMutation) {
        List<Cell> kvs = tableMutation.getFamilyCellMap().get(PhoenixDatabaseMetaData.TABLE_FAMILY_BYTES);
        if (kvs != null) {
            for (Cell kv : kvs) { // list is not ordered, so search. TODO: we could potentially assume the position
                if (isSequenceNumber(kv)) {
                    return PLong.INSTANCE.getCodec().decodeLong(kv.getValueArray(), kv.getValueOffset(), SortOrder.getDefault());
                }
            }
        }
        throw new IllegalStateException();
    }

    public static long getSequenceNumber(List<Mutation> tableMetaData) {
        return getSequenceNumber(getPutOnlyTableHeaderRow(tableMetaData));
    }

    public static boolean isSequenceNumber(Mutation m) {
        boolean foundSequenceNumber = false;
        for (Cell kv : m.getFamilyCellMap().get(TABLE_FAMILY_BYTES)) {
            if (isSequenceNumber(kv)) {
                foundSequenceNumber = true;
                break;
            }
        }
        return foundSequenceNumber;
    }
    public static boolean isSequenceNumber(Cell kv) {
        return CellUtil.matchingQualifier(kv, PhoenixDatabaseMetaData.TABLE_SEQ_NUM_BYTES);
    }

    public static PTableType getTableType(List<Mutation> tableMetaData, KeyValueBuilder builder,
      ImmutableBytesWritable value) {
        if (getMutationValue(getPutOnlyTableHeaderRow(tableMetaData),
            PhoenixDatabaseMetaData.TABLE_TYPE_BYTES, builder, value)) {
            return PTableType.fromSerializedValue(value.get()[value.getOffset()]);
        }
        return null;
    }

    public static boolean isNameSpaceMapped(List<Mutation> tableMetaData, KeyValueBuilder builder,
            ImmutableBytesWritable value) {
        if (getMutationValue(getPutOnlyTableHeaderRow(tableMetaData),
            PhoenixDatabaseMetaData.IS_NAMESPACE_MAPPED_BYTES, builder, value)) {
            return (boolean)PBoolean.INSTANCE.toObject(ByteUtil.copyKeyBytesIfNecessary(value));
        }
        return false;
    }

    public static int getSaltBuckets(List<Mutation> tableMetaData, KeyValueBuilder builder,
      ImmutableBytesWritable value) {
        if (getMutationValue(getPutOnlyTableHeaderRow(tableMetaData),
            PhoenixDatabaseMetaData.SALT_BUCKETS_BYTES, builder, value)) {
            return PInteger.INSTANCE.getCodec().decodeInt(value, SortOrder.getDefault());
        }
        return 0;
    }

    public static long getParentSequenceNumber(List<Mutation> tableMetaData) {
        return getSequenceNumber(getParentTableHeaderRow(tableMetaData));
    }
    
    public static Mutation getTableHeaderRow(List<Mutation> tableMetaData) {
        return tableMetaData.get(0);
    }

  /**
   * Get the mutation who's qualifier matches the passed key
   * <p>
   * We need to pass in an {@link ImmutableBytesPtr} to pass the result back to make life easier
   * when dealing with a regular {@link KeyValue} vs. a custom KeyValue as the latter may not
   * support things like {@link KeyValue#getBuffer()}
   * @param headerRow mutation to check
   * @param key to check
   * @param builder that created the {@link KeyValue KeyValues} in the {@link Mutation}
   * @param ptr to point to the KeyValue's value if found
   * @return true if the KeyValue was found and false otherwise
   */
  public static boolean getMutationValue(Mutation headerRow, byte[] key,
      KeyValueBuilder builder, ImmutableBytesWritable ptr) {
        List<Cell> kvs = headerRow.getFamilyCellMap().get(PhoenixDatabaseMetaData.TABLE_FAMILY_BYTES);
        if (kvs != null) {
            for (Cell cell : kvs) {
                KeyValue kv = PhoenixKeyValueUtil.maybeCopyCell(cell);
                if (builder.compareQualifier(kv, key, 0, key.length) ==0) {
                    builder.getValueAsPtr(kv, ptr);
                    return true;
                }
            }
        }
        return false;
    }

    public static KeyValue getMutationValue(Mutation headerRow, byte[] key,
        KeyValueBuilder builder) {
        List<Cell> kvs = headerRow.getFamilyCellMap().get(PhoenixDatabaseMetaData.TABLE_FAMILY_BYTES);
        if (kvs != null) {
            for (Cell cell : kvs) {
                KeyValue kv = org.apache.hadoop.hbase.KeyValueUtil.ensureKeyValue(cell);
                if (builder.compareQualifier(kv, key, 0, key.length) ==0) {
                    return kv;
                }
            }
        }
        return null;
    }

    public static boolean setMutationValue(Mutation headerRow, byte[] key,
        KeyValueBuilder builder, KeyValue keyValue) {
        List<Cell> kvs = headerRow.getFamilyCellMap().get(PhoenixDatabaseMetaData.TABLE_FAMILY_BYTES);
        if (kvs != null) {
            for (Cell cell : kvs) {
                KeyValue kv = org.apache.hadoop.hbase.KeyValueUtil.ensureKeyValue(cell);
                if (builder.compareQualifier(kv, key, 0, key.length) ==0) {
                    KeyValueBuilder.addQuietly(headerRow, keyValue);
                    return true;
                }
            }
        }
        return false;
    }

    public static List<Cell> getTableCellsFromMutations(List<Mutation> tableMetaData) {
        List<Cell> tableCells = Lists.newArrayList();
        byte[] tableKey = tableMetaData.get(0).getRow();
        for (int k = 0; k < tableMetaData.size(); k++) {
            Mutation m = tableMetaData.get(k);
            if (Bytes.equals(m.getRow(), tableKey)) {
                tableCells.addAll(getCellList(m));
            }
        }
        return tableCells;
    }

    public static List<List<Cell>> getColumnAndLinkCellsFromMutations(List<Mutation> tableMetaData) {
        //skip the first mutation because it's the table header row with table-specific information
        //all the rest of the mutations are either from linking rows or column definition rows
        List<List<Cell>> allColumnsCellList = Lists.newArrayList();
        byte[] tableKey = tableMetaData.get(0).getRow();
        for (int k = 1; k < tableMetaData.size(); k++) {
            Mutation m = tableMetaData.get(k);
            //filter out mutations for the table header row and TABLE_SEQ_NUM and parent table
            //rows such as a view's column qualifier count
            if (!Bytes.equals(m.getRow(), tableKey) && !(!isLinkType(m) && isSequenceNumber(m)
                && !isParentTableColumnQualifierCounter(m, tableKey))) {
                List<Cell> listToAdd = getCellList(m);
                if (listToAdd != null && listToAdd.size() > 0) {
                    allColumnsCellList.add(listToAdd);
                }
            }
        }
        return allColumnsCellList;
    }

    private static List<Cell> getCellList(Mutation m) {
        List<Cell> cellList = Lists.newArrayList();
        for (Cell c : m.getFamilyCellMap().get(TABLE_FAMILY_BYTES)) {
            //Mutations will mark NULL columns as deletes, whereas when we read
            //from HBase we just won't get Cells for those columns. To use Mutation cells
            //with code expecting Cells read from HBase results, we have to purge those
            //Delete mutations
            if (c != null && !CellUtil.isDelete(c)) {
                cellList.add(c);
            }
        }
        return cellList;
    }

    /**
     * Returns the first Put element in <code>tableMetaData</code>. There could be leading Delete elements before the
     * table header row
     */
    public static Put getPutOnlyTableHeaderRow(List<Mutation> tableMetaData) {
        for (Mutation m : tableMetaData) {
            if (m instanceof Put) { return (Put) m; }
        }
        throw new IllegalStateException("No table header row found in table metadata");
    }
    
    public static Put getPutOnlyAutoPartitionColumn(PTable parentTable, List<Mutation> tableMetaData) {
        int autoPartitionPutIndex = parentTable.isMultiTenant() ? 2: 1;
        int i=0;
        for (Mutation m : tableMetaData) {
            if (m instanceof Put && i++==autoPartitionPutIndex) { return (Put) m; }
        }
        throw new IllegalStateException("No auto partition column row found in table metadata");
    }

    public static Mutation getParentTableHeaderRow(List<Mutation> tableMetaData) {
        return tableMetaData.get(tableMetaData.size()-1);
    }

    public static long getClientTimeStamp(List<Mutation> tableMetadata) {
        Mutation m = tableMetadata.get(0);
        return getClientTimeStamp(m);
    }    

    public static long getClientTimeStamp(Mutation m) {
        Collection<List<Cell>> kvs = m.getFamilyCellMap().values();
        // Empty if Mutation is a Delete
        // TODO: confirm that Delete timestamp is reset like Put
        return kvs.isEmpty() ? m.getTimestamp() : kvs.iterator().next().get(0).getTimestamp();
    }    

    public static byte[] getParentLinkKey(String tenantId, String schemaName, String tableName, String indexName) {
        return ByteUtil.concat(tenantId == null ? ByteUtil.EMPTY_BYTE_ARRAY : Bytes.toBytes(tenantId), QueryConstants.SEPARATOR_BYTE_ARRAY, schemaName == null ? ByteUtil.EMPTY_BYTE_ARRAY : Bytes.toBytes(schemaName), QueryConstants.SEPARATOR_BYTE_ARRAY, Bytes.toBytes(tableName), QueryConstants.SEPARATOR_BYTE_ARRAY, QueryConstants.SEPARATOR_BYTE_ARRAY, Bytes.toBytes(indexName));
    }

    public static byte[] getParentLinkKey(byte[] tenantId, byte[] schemaName, byte[] tableName, byte[] indexName) {
        return ByteUtil.concat(tenantId == null ? ByteUtil.EMPTY_BYTE_ARRAY : tenantId, QueryConstants.SEPARATOR_BYTE_ARRAY, schemaName == null ? ByteUtil.EMPTY_BYTE_ARRAY : schemaName, QueryConstants.SEPARATOR_BYTE_ARRAY, tableName, QueryConstants.SEPARATOR_BYTE_ARRAY, QueryConstants.SEPARATOR_BYTE_ARRAY, indexName);
    }
    
    public static byte[] getChildLinkKey(PName parentTenantId, PName parentSchemaName, PName parentTableName, PName viewTenantId, PName viewName) {
        return ByteUtil.concat(parentTenantId == null ? ByteUtil.EMPTY_BYTE_ARRAY : parentTenantId.getBytes(), QueryConstants.SEPARATOR_BYTE_ARRAY, 
                        parentSchemaName == null ? ByteUtil.EMPTY_BYTE_ARRAY : parentSchemaName.getBytes(), QueryConstants.SEPARATOR_BYTE_ARRAY, 
                        parentTableName.getBytes(), QueryConstants.SEPARATOR_BYTE_ARRAY,
                        viewTenantId == null ? ByteUtil.EMPTY_BYTE_ARRAY : viewTenantId.getBytes(), QueryConstants.SEPARATOR_BYTE_ARRAY, 
                        viewName.getBytes());
    }
    
    public static Cell getCell(List<Cell> cells, byte[] cq) {
        for (Cell cell : cells) {
            if (Bytes.compareTo(cell.getQualifierArray(), cell.getQualifierOffset(), cell.getQualifierLength(), cq, 0, cq.length) == 0) {
                return cell;
            }
        }
        return null;
    }
    
    public static boolean isMultiTenant(Mutation m, KeyValueBuilder builder, ImmutableBytesWritable ptr) {
        if (getMutationValue(m, PhoenixDatabaseMetaData.MULTI_TENANT_BYTES, builder, ptr)) {
            return Boolean.TRUE.equals(PBoolean.INSTANCE.toObject(ptr));
        }
        return false;
    }
    
    public static boolean isTransactional(Mutation m, KeyValueBuilder builder, ImmutableBytesWritable ptr) {
        if (getMutationValue(m, PhoenixDatabaseMetaData.TRANSACTIONAL_BYTES, builder, ptr)) {
            return Boolean.TRUE.equals(PBoolean.INSTANCE.toObject(ptr));
        }
        return false;
    }
    
    public static boolean isSalted(Mutation m, KeyValueBuilder builder, ImmutableBytesWritable ptr) {
        return MetaDataUtil.getMutationValue(m, PhoenixDatabaseMetaData.SALT_BUCKETS_BYTES, builder, ptr);
    }
    
    public static byte[] getViewIndexPhysicalName(byte[] physicalTableName) {
        return getIndexPhysicalName(physicalTableName, VIEW_INDEX_TABLE_PREFIX);
    }

    public static String getViewIndexPhysicalName(String physicalTableName) {
        return getIndexPhysicalName(physicalTableName, VIEW_INDEX_TABLE_PREFIX);
    }

    public static String getNamespaceMappedName(PName tableName, boolean isNamespaceMapped) {
        String logicalName = tableName.getString();
        if (isNamespaceMapped) {
            logicalName = logicalName.replace(QueryConstants.NAME_SEPARATOR, QueryConstants.NAMESPACE_SEPARATOR);
        }
        return logicalName;
    }

    public static String getViewIndexPhysicalName(PName logicalTableName, boolean isNamespaceMapped) {
        String logicalName = getNamespaceMappedName(logicalTableName, isNamespaceMapped);
        return getIndexPhysicalName(logicalName, VIEW_INDEX_TABLE_PREFIX);
    }

    private static byte[] getIndexPhysicalName(byte[] physicalTableName, String indexPrefix) {
        return Bytes.toBytes(getIndexPhysicalName(Bytes.toString(physicalTableName), indexPrefix));
    }

    private static String getIndexPhysicalName(String physicalTableName, String indexPrefix) {
        if (physicalTableName.contains(QueryConstants.NAMESPACE_SEPARATOR)) {
            String schemaName = SchemaUtil.getSchemaNameFromFullName(physicalTableName,
                    QueryConstants.NAMESPACE_SEPARATOR);
            String tableName = SchemaUtil.getTableNameFromFullName(physicalTableName,
                    QueryConstants.NAMESPACE_SEPARATOR);
            return (schemaName + QueryConstants.NAMESPACE_SEPARATOR + indexPrefix + tableName);
        }
        return indexPrefix + physicalTableName;
    }

    public static byte[] getLocalIndexPhysicalName(byte[] physicalTableName) {
        return getIndexPhysicalName(physicalTableName, LOCAL_INDEX_TABLE_PREFIX);
    }

    public static String getLocalIndexTableName(String tableName) {
        return LOCAL_INDEX_TABLE_PREFIX + tableName;
    }

    public static String getLocalIndexSchemaName(String schemaName) {
        return schemaName;
    }  

    public static String getLocalIndexUserTableName(String localIndexTableName) {
        if (localIndexTableName.contains(QueryConstants.NAMESPACE_SEPARATOR)) {
            String schemaName = SchemaUtil.getSchemaNameFromFullName(localIndexTableName,
                    QueryConstants.NAMESPACE_SEPARATOR);
            String tableName = SchemaUtil.getTableNameFromFullName(localIndexTableName,
                    QueryConstants.NAMESPACE_SEPARATOR);
            String userTableName = tableName.substring(LOCAL_INDEX_TABLE_PREFIX.length());
            return (schemaName + QueryConstants.NAMESPACE_SEPARATOR + userTableName);
        } else {
            String schemaName = SchemaUtil.getSchemaNameFromFullName(localIndexTableName);
            if (!schemaName.isEmpty()) schemaName = schemaName.substring(LOCAL_INDEX_TABLE_PREFIX.length());
            String tableName = localIndexTableName.substring(
                    (schemaName.isEmpty() ? 0 : (schemaName.length() + QueryConstants.NAME_SEPARATOR.length()))
                            + LOCAL_INDEX_TABLE_PREFIX.length());
            return SchemaUtil.getTableName(schemaName, tableName);
        }
    }

    public static String getViewIndexUserTableName(String viewIndexTableName) {
        if (viewIndexTableName.contains(QueryConstants.NAMESPACE_SEPARATOR)) {
            String schemaName = SchemaUtil.getSchemaNameFromFullName(viewIndexTableName,
                    QueryConstants.NAMESPACE_SEPARATOR);
            String tableName = SchemaUtil.getTableNameFromFullName(viewIndexTableName,
                    QueryConstants.NAMESPACE_SEPARATOR);
            String userTableName = tableName.substring(VIEW_INDEX_TABLE_PREFIX.length());
            return (schemaName + QueryConstants.NAMESPACE_SEPARATOR + userTableName);
        } else {
            String schemaName = SchemaUtil.getSchemaNameFromFullName(viewIndexTableName);
            if (!schemaName.isEmpty()) schemaName = schemaName.substring(VIEW_INDEX_TABLE_PREFIX.length());
            String tableName = viewIndexTableName.substring(
                    (schemaName.isEmpty() ? 0 : (schemaName.length() + QueryConstants.NAME_SEPARATOR.length()))
                            + VIEW_INDEX_TABLE_PREFIX.length());
            return SchemaUtil.getTableName(schemaName, tableName);
        }
    }

    public static String getOldViewIndexSequenceSchemaName(PName physicalName, boolean isNamespaceMapped) {
        if (!isNamespaceMapped) { return VIEW_INDEX_SEQUENCE_PREFIX + physicalName.getString(); }
        return SchemaUtil.getSchemaNameFromFullName(physicalName.toString());
    }

    public static String getOldViewIndexSequenceName(PName physicalName, PName tenantId, boolean isNamespaceMapped) {
        if (!isNamespaceMapped) { return VIEW_INDEX_SEQUENCE_NAME_PREFIX + (tenantId == null ? "" : tenantId); }
        return SchemaUtil.getTableNameFromFullName(physicalName.toString()) + VIEW_INDEX_SEQUENCE_NAME_PREFIX;
    }

    public static SequenceKey getOldViewIndexSequenceKey(String tenantId, PName physicalName, int nSaltBuckets,
                                                      boolean isNamespaceMapped) {
        // Create global sequence of the form: <prefixed base table name><tenant id>
        // rather than tenant-specific sequence, as it makes it much easier
        // to cleanup when the physical table is dropped, as we can delete
        // all global sequences leading with <prefix> + physical name.
        String schemaName = getOldViewIndexSequenceSchemaName(physicalName, isNamespaceMapped);
        String tableName = getOldViewIndexSequenceName(physicalName, PNameFactory.newName(tenantId), isNamespaceMapped);
        return new SequenceKey(isNamespaceMapped ? tenantId : null, schemaName, tableName, nSaltBuckets);
    }

    public static String getViewIndexSequenceSchemaName(PName logicalBaseTableName, boolean isNamespaceMapped) {
        if (!isNamespaceMapped) {
            String baseTableName = SchemaUtil.getParentTableNameFromIndexTable(logicalBaseTableName.getString(),
                MetaDataUtil.VIEW_INDEX_TABLE_PREFIX);
            return SchemaUtil.getSchemaNameFromFullName(baseTableName);
        } else {
            return SchemaUtil.getSchemaNameFromFullName(logicalBaseTableName.toString());
        }

    }

    public static String getViewIndexSequenceName(PName physicalName, PName tenantId, boolean isNamespaceMapped) {
        return SchemaUtil.getTableNameFromFullName(physicalName.toString()) + VIEW_INDEX_SEQUENCE_NAME_PREFIX;
    }

    /**
     *
     * @param tenantId No longer used, but kept in signature for backwards compatibility
     * @param physicalName Name of physical view index table
     * @param nSaltBuckets Number of salt buckets
     * @param isNamespaceMapped Is namespace mapping enabled
     * @return SequenceKey for the ViewIndexId
     */
    public static SequenceKey getViewIndexSequenceKey(String tenantId, PName physicalName, int nSaltBuckets,
            boolean isNamespaceMapped) {
        // Create global sequence of the form: <prefixed base table name>.
        // We can't use a tenant-owned or escaped sequence because of collisions,
        // with other view indexes that may be global or owned by other tenants that
        // also use this same physical view index table. It's also much easier
        // to cleanup when the physical table is dropped, as we can delete
        // all global sequences leading with <prefix> + physical name.
        String schemaName = getViewIndexSequenceSchemaName(physicalName, isNamespaceMapped);
        String tableName = getViewIndexSequenceName(physicalName, null, isNamespaceMapped);
        return new SequenceKey(null, schemaName, tableName, nSaltBuckets);
    }

    public static PDataType getViewIndexIdDataType() {
       return PLong.INSTANCE;
    }

    public static PDataType getLegacyViewIndexIdDataType() {
        return PSmallint.INSTANCE;
    }

    public static String getViewIndexIdColumnName() {
        return VIEW_INDEX_ID_COLUMN_NAME;
    }

    public static boolean hasViewIndexTable(PhoenixConnection connection, PName physicalName) throws SQLException {
        return hasViewIndexTable(connection, physicalName.getBytes());
    }

    public static boolean hasViewIndexTable(PhoenixConnection connection, byte[] physicalTableName)
            throws SQLException {
        byte[] physicalIndexName = MetaDataUtil.getViewIndexPhysicalName(physicalTableName);
        try {
            TableDescriptor desc = connection.getQueryServices().getTableDescriptor(physicalIndexName);
            return desc != null && Boolean.TRUE.equals(PBoolean.INSTANCE.toObject(desc.getValue(IS_VIEW_INDEX_TABLE_PROP_BYTES)));
        } catch (TableNotFoundException e) {
            return false;
        }
    }

    public static boolean hasLocalIndexTable(PhoenixConnection connection, PName physicalName) throws SQLException {
        return hasLocalIndexTable(connection, physicalName.getBytes());
    }

    public static boolean hasLocalIndexTable(PhoenixConnection connection, byte[] physicalTableName) throws SQLException {
        try {
            TableDescriptor desc = connection.getQueryServices().getTableDescriptor(physicalTableName);
            if (desc == null ) {
                return false;
            }
            return hasLocalIndexColumnFamily(desc);
        } catch (TableNotFoundException e) {
            return false;
        }
    }

    public static boolean hasLocalIndexColumnFamily(TableDescriptor desc) {
        for (ColumnFamilyDescriptor cf : desc.getColumnFamilies()) {
            if (cf.getNameAsString().startsWith(QueryConstants.LOCAL_INDEX_COLUMN_FAMILY_PREFIX)) {
                return true;
            }
        }
        return false;
    }

    public static List<byte[]> getNonLocalIndexColumnFamilies(TableDescriptor desc) {
    	List<byte[]> families = new ArrayList<byte[]>(desc.getColumnFamilies().length);
        for (ColumnFamilyDescriptor cf : desc.getColumnFamilies()) {
            if (!cf.getNameAsString().startsWith(QueryConstants.LOCAL_INDEX_COLUMN_FAMILY_PREFIX)) {
            	families.add(cf.getName());
            }
        }
    	return families;
    }

    public static List<byte[]> getLocalIndexColumnFamilies(PhoenixConnection conn, byte[] physicalTableName) throws SQLException {
        TableDescriptor desc = conn.getQueryServices().getTableDescriptor(physicalTableName);
        if (desc == null ) {
            return Collections.emptyList();
        }
        List<byte[]> families = new ArrayList<byte[]>(desc.getColumnFamilies().length / 2);
        for (ColumnFamilyDescriptor cf : desc.getColumnFamilies()) {
            if (cf.getNameAsString().startsWith(QueryConstants.LOCAL_INDEX_COLUMN_FAMILY_PREFIX)) {
                families.add(cf.getName());
            }
        }
        return families;
    }

    public static void deleteViewIndexSequences(PhoenixConnection connection, PName name, boolean isNamespaceMapped)
            throws SQLException {
        String schemaName = getViewIndexSequenceSchemaName(name, isNamespaceMapped);
        String sequenceName = getViewIndexSequenceName(name, null, isNamespaceMapped);
        String delQuery = String.format(" DELETE FROM " + PhoenixDatabaseMetaData.SYSTEM_SEQUENCE
                + " WHERE " + PhoenixDatabaseMetaData.SEQUENCE_SCHEMA  + " %s AND "
                + PhoenixDatabaseMetaData.SEQUENCE_NAME + " = ? ",
            schemaName.length() > 0 ? "= ? " : " IS NULL");
        try (PreparedStatement delSeqStmt = connection.prepareStatement(delQuery)) {
            if (schemaName.length() > 0) {
                delSeqStmt.setString(1, schemaName);
                delSeqStmt.setString(2, sequenceName);
            } else {
                delSeqStmt.setString(1, sequenceName);
            }
            delSeqStmt.executeUpdate();
        }
    }

    public static boolean propertyNotAllowedToBeOutOfSync(String colFamProp) {
        return SYNCED_DATA_TABLE_AND_INDEX_COL_FAM_PROPERTIES.contains(colFamProp);
    }

    public static Map<String, Object> getSyncedProps(ColumnFamilyDescriptor defaultCFDesc) {
        Map<String, Object> syncedProps = new HashMap<>();
        if (defaultCFDesc != null) {
            for (String propToKeepInSync: SYNCED_DATA_TABLE_AND_INDEX_COL_FAM_PROPERTIES) {
                syncedProps.put(propToKeepInSync, Bytes.toString(
                        defaultCFDesc.getValue(Bytes.toBytes(propToKeepInSync))));
            }
        }
        return syncedProps;
    }

    public static Scan newTableRowsScan(byte[] key, long startTimeStamp, long stopTimeStamp){
        return newTableRowsScan(key, null, startTimeStamp, stopTimeStamp);
    }

	public static Scan newTableRowsScan(byte[] startKey, byte[] stopKey, long startTimeStamp, long stopTimeStamp) {
		Scan scan = new Scan();
		ScanUtil.setTimeRange(scan, startTimeStamp, stopTimeStamp);
        scan.withStartRow(startKey);
		if (stopKey == null) {
			stopKey = ByteUtil.concat(startKey, QueryConstants.SEPARATOR_BYTE_ARRAY);
			ByteUtil.nextKey(stopKey, stopKey.length);
		}
        scan.withStopRow(stopKey);
		return scan;
	}

    public static LinkType getLinkType(Mutation tableMutation) {
        return getLinkType(tableMutation.getFamilyCellMap().get(PhoenixDatabaseMetaData.TABLE_FAMILY_BYTES));
    }

    public static LinkType getLinkType(Collection<Cell> kvs) {
        if (kvs != null) {
            for (Cell kv : kvs) {
                if (isLinkType(kv))  {
                    return LinkType.fromSerializedValue(PUnsignedTinyint.INSTANCE.getCodec().
                        decodeByte(kv.getValueArray(), kv.getValueOffset(),
                        SortOrder.getDefault())); }
            }
        }
        return null;
    }

    public static boolean isLocalIndex(String physicalName) {
        if (physicalName.contains(LOCAL_INDEX_TABLE_PREFIX)) { return true; }
        return false;
    }

    public static boolean isLinkType(Cell kv) {
        return CellUtil.matchingQualifier(kv, PhoenixDatabaseMetaData.LINK_TYPE_BYTES);
    }

    public static boolean isLinkType(Mutation m) {
        boolean foundLinkType = false;
        for (Cell kv : m.getFamilyCellMap().get(TABLE_FAMILY_BYTES)) {
            if (isLinkType(kv)) {
                foundLinkType = true;
                break;
            }
        }
        return foundLinkType;
    }

    public static boolean isParentTableColumnQualifierCounter(Mutation m, byte[] tableRow) {
        boolean foundCQCounter = false;
        for (Cell kv : m.getFamilyCellMap().get(TABLE_FAMILY_BYTES)) {
            if (isParentTableColumnQualifierCounter(kv, tableRow)) {
                foundCQCounter = true;
                break;
            }
        }
        return foundCQCounter;
    }
    public static boolean isParentTableColumnQualifierCounter(Cell kv, byte[] tableRow) {
        byte[][] tableRowKeyMetaData = new byte[5][];
        getVarChars(tableRow, tableRowKeyMetaData);
        byte[] tableName = tableRowKeyMetaData[TABLE_NAME_INDEX];

        byte[][] columnRowKeyMetaData = new byte[5][];
        int nColumns = getVarChars(kv.getRowArray(), kv.getRowOffset(), kv.getRowLength(),
            0, columnRowKeyMetaData);
        if (nColumns == 5) {
            byte[] columnTableName = columnRowKeyMetaData[TABLE_NAME_INDEX];
            if (!Bytes.equals(tableName, columnTableName)) {
                return CellUtil.matchingQualifier(kv, COLUMN_QUALIFIER_BYTES);
            }
        }
        return false;
    }

    public static boolean isViewIndex(String physicalName) {
        if (physicalName.contains(QueryConstants.NAMESPACE_SEPARATOR)) {
            return SchemaUtil.getTableNameFromFullName(physicalName).startsWith(VIEW_INDEX_TABLE_PREFIX);
        } else {
            return physicalName.startsWith(VIEW_INDEX_TABLE_PREFIX);
        }
    }

    public static String getAutoPartitionColumnName(PTable parentTable) {
        List<PColumn> parentTableColumns = parentTable.getPKColumns();
        PColumn column = parentTableColumns.get(getAutoPartitionColIndex(parentTable));
        return column.getName().getString();
    }

    // this method should only be called on the parent table (since it has the _SALT column)
    public static int getAutoPartitionColIndex(PTable parentTable) {
        boolean isMultiTenant = parentTable.isMultiTenant();
        boolean isSalted = parentTable.getBucketNum()!=null;
        return (isMultiTenant && isSalted) ? 2 : (isMultiTenant || isSalted) ? 1 : 0;
    }

    public static boolean isHColumnProperty(String propName) {
        return ColumnFamilyDescriptorBuilder.getDefaultValues().containsKey(propName);
    }

    public static boolean isHTableProperty(String propName) {
        return !isHColumnProperty(propName) && !TableProperty.isPhoenixTableProperty(propName);
    }

    public static boolean isLocalIndexFamily(ImmutableBytesPtr cfPtr) {
        return cfPtr.getLength() >= QueryConstants.LOCAL_INDEX_COLUMN_FAMILY_PREFIX_BYTES.length &&
               Bytes.compareTo(cfPtr.get(), cfPtr.getOffset(), QueryConstants.LOCAL_INDEX_COLUMN_FAMILY_PREFIX_BYTES.length, QueryConstants.LOCAL_INDEX_COLUMN_FAMILY_PREFIX_BYTES, 0, QueryConstants.LOCAL_INDEX_COLUMN_FAMILY_PREFIX_BYTES.length) == 0;
    }
    
    public static boolean isLocalIndexFamily(byte[] cf) {
        return Bytes.startsWith(cf, QueryConstants.LOCAL_INDEX_COLUMN_FAMILY_PREFIX_BYTES);
    }
    
    public static final byte[] getPhysicalTableRowForView(PTable view) {
        byte[] physicalTableSchemaName = Bytes.toBytes(SchemaUtil.getSchemaNameFromFullName(view.getPhysicalName().getString()));
        byte[] physicalTableName = Bytes.toBytes(SchemaUtil.getTableNameFromFullName(view.getPhysicalName().getString()));
        return SchemaUtil.getTableKey(ByteUtil.EMPTY_BYTE_ARRAY, physicalTableSchemaName, physicalTableName);
    }

    /**
     * Extract mutations of link type {@link PTable.LinkType#CHILD_TABLE} from the list of mutations.
     * The child link mutations will be sent to SYSTEM.CHILD_LINK and other mutations to SYSTEM.CATALOG
     * @param metadataMutations total list of mutations
     * @return list of mutations pertaining to parent-child links
     */
	public static List<Mutation> removeChildLinkMutations(List<Mutation> metadataMutations) {
		List<Mutation> childLinkMutations = Lists.newArrayList();
		Iterator<Mutation> iter = metadataMutations.iterator();
		while (iter.hasNext()) {
			Mutation m = iter.next();
			for (Cell kv : m.getFamilyCellMap().get(PhoenixDatabaseMetaData.TABLE_FAMILY_BYTES)) {
				// remove mutations of link type LinkType.CHILD_TABLE
				if ((Bytes.compareTo(kv.getQualifierArray(), kv.getQualifierOffset(), kv.getQualifierLength(),
						PhoenixDatabaseMetaData.LINK_TYPE_BYTES, 0,
						PhoenixDatabaseMetaData.LINK_TYPE_BYTES.length) == 0)
						&& ((Bytes.compareTo(kv.getValueArray(), kv.getValueOffset(), kv.getValueLength(),
								LinkType.CHILD_TABLE.getSerializedValueAsByteArray(), 0,
								LinkType.CHILD_TABLE.getSerializedValueAsByteArray().length) == 0))) {
					childLinkMutations.add(m);
					iter.remove();
				}
			}
		}
		return childLinkMutations;
	}

	public static IndexType getIndexType(List<Mutation> tableMetaData, KeyValueBuilder builder,
            ImmutableBytesWritable value) {
        if (getMutationValue(getPutOnlyTableHeaderRow(tableMetaData), PhoenixDatabaseMetaData.INDEX_TYPE_BYTES, builder,
                value)) { return IndexType.fromSerializedValue(value.get()[value.getOffset()]); }
        return null;
    }

	/**
     * Retrieve the viewIndexId datatype from create request.
     *
     * @see MetaDataEndpointImpl#createTable(com.google.protobuf.RpcController,
     *      org.apache.phoenix.coprocessor.generated.MetaDataProtos.CreateTableRequest,
     *      com.google.protobuf.RpcCallback)
     */
    public static PDataType<?> getIndexDataType(List<Mutation> tableMetaData,
            KeyValueBuilder builder, ImmutableBytesWritable value) {
        if (getMutationValue(getPutOnlyTableHeaderRow(tableMetaData),
                PhoenixDatabaseMetaData.VIEW_INDEX_ID_DATA_TYPE_BYTES, builder, value)) {
            return PDataType.fromTypeId(
                    PInteger.INSTANCE.getCodec().decodeInt(value, SortOrder.getDefault()));
        }
        return getLegacyViewIndexIdDataType();
    }

    public static boolean getChangeDetectionEnabled(List<Mutation> tableMetaData) {
        KeyValueBuilder builder = GenericKeyValueBuilder.INSTANCE;
        ImmutableBytesWritable value = new ImmutableBytesWritable();
        if (getMutationValue(getPutOnlyTableHeaderRow(tableMetaData),
            PhoenixDatabaseMetaData.CHANGE_DETECTION_ENABLED_BYTES, builder, value)) {
            return Boolean.TRUE.equals(PBoolean.INSTANCE.toObject(value.get(),
                value.getOffset(),
                value.getLength()));
        } else {
            return false;
        }
    }

    public static PColumn getColumn(int pkCount, byte[][] rowKeyMetaData, PTable table) throws ColumnFamilyNotFoundException, ColumnNotFoundException {
        PColumn col = null;
        if (pkCount > FAMILY_NAME_INDEX
            && rowKeyMetaData[PhoenixDatabaseMetaData.FAMILY_NAME_INDEX].length > 0) {
            PColumnFamily family =
                table.getColumnFamily(rowKeyMetaData[PhoenixDatabaseMetaData.FAMILY_NAME_INDEX]);
            col =
                family.getPColumnForColumnNameBytes(rowKeyMetaData[PhoenixDatabaseMetaData.COLUMN_NAME_INDEX]);
        } else if (pkCount > COLUMN_NAME_INDEX
            && rowKeyMetaData[PhoenixDatabaseMetaData.COLUMN_NAME_INDEX].length > 0) {
            col = table.getPKColumn(new String(
                rowKeyMetaData[PhoenixDatabaseMetaData.COLUMN_NAME_INDEX], StandardCharsets.UTF_8));
        }
        return col;
    }

    public static void deleteFromStatsTable(PhoenixConnection connection,
                                            PTable table, List<byte[]> physicalTableNames,
                                            List<MetaDataProtocol.SharedTableState> sharedTableStates)
            throws SQLException {
        boolean isAutoCommit = connection.getAutoCommit();
        try {
            connection.setAutoCommit(true);
            Set<String> physicalTablesSet = new HashSet<>();
            physicalTablesSet.add(table.getPhysicalName().getString());
            for (byte[] physicalTableName:physicalTableNames) {
                physicalTablesSet.add(Bytes.toString(physicalTableName));
            }
            for (MetaDataProtocol.SharedTableState s: sharedTableStates) {
                physicalTablesSet.add(s.getPhysicalNames().get(0).getString());
            }
            StringBuilder buf = new StringBuilder("DELETE FROM SYSTEM.STATS WHERE PHYSICAL_NAME IN (");
            for (int i = 0; i < physicalTablesSet.size(); i++) {
                buf.append(" ?,");
            }
            buf.setCharAt(buf.length() - 1, ')');
            if (table.getIndexType()==IndexType.LOCAL) {
                buf.append(" AND COLUMN_FAMILY IN(");
                if (table.getColumnFamilies().isEmpty()) {
                    buf.append("'" + QueryConstants.DEFAULT_LOCAL_INDEX_COLUMN_FAMILY + "',");
                } else {
                    buf.append(QueryUtil.generateInListParams(table
                        .getColumnFamilies().size()));
                }
                buf.setCharAt(buf.length() - 1, ')');
            }
            try (PreparedStatement delStatsStmt = connection.prepareStatement(buf.toString())) {
                int param = 0;
                Iterator itr = physicalTablesSet.iterator();
                while (itr.hasNext()) {
                    delStatsStmt.setString(++param, itr.next().toString());
                }
                if (table.getIndexType() == IndexType.LOCAL
                    && !table.getColumnFamilies().isEmpty()) {
                    for (PColumnFamily cf : table.getColumnFamilies()) {
                        delStatsStmt.setString(++param, cf.getName().getString());
                    }
                }
                delStatsStmt.execute();
            }
        } finally {
            connection.setAutoCommit(isAutoCommit);
        }
    }
}
