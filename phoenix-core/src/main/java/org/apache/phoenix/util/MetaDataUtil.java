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

import static org.apache.phoenix.jdbc.PhoenixDatabaseMetaData.COLUMN_NAME_INDEX;
import static org.apache.phoenix.jdbc.PhoenixDatabaseMetaData.FAMILY_NAME_INDEX;
import static org.apache.phoenix.util.SchemaUtil.getVarChars;

import java.io.IOException;
import java.sql.SQLException;
import java.util.*;

import com.google.common.annotations.VisibleForTesting;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.HRegionLocation;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.Tag;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.HConnection;
import org.apache.hadoop.hbase.client.HConnectionManager;
import org.apache.hadoop.hbase.client.Mutation;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.coprocessor.RegionCoprocessorEnvironment;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.protobuf.ProtobufUtil;
import org.apache.hadoop.hbase.protobuf.RequestConverter;
import org.apache.hadoop.hbase.protobuf.generated.AdminProtos.AdminService;
import org.apache.hadoop.hbase.protobuf.generated.AdminProtos.GetRegionInfoRequest;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.ipc.RemoteException;
import org.apache.phoenix.coprocessor.MetaDataEndpointImpl;
import org.apache.phoenix.coprocessor.MetaDataProtocol;
import org.apache.phoenix.exception.SQLExceptionCode;
import org.apache.phoenix.hbase.index.util.GenericKeyValueBuilder;
import org.apache.phoenix.hbase.index.util.ImmutableBytesPtr;
import org.apache.phoenix.hbase.index.util.IndexManagementUtil;
import org.apache.phoenix.hbase.index.util.KeyValueBuilder;
import org.apache.phoenix.hbase.index.util.VersionUtil;
import org.apache.phoenix.jdbc.PhoenixConnection;
import org.apache.phoenix.jdbc.PhoenixDatabaseMetaData;
import org.apache.phoenix.query.QueryConstants;
import org.apache.phoenix.schema.*;
import org.apache.phoenix.schema.PTable.IndexType;
import org.apache.phoenix.schema.PTable.LinkType;
import org.apache.phoenix.schema.PTable.ViewType;
import org.apache.phoenix.schema.types.PBoolean;
import org.apache.phoenix.schema.types.PDataType;
import org.apache.phoenix.schema.types.PInteger;
import org.apache.phoenix.schema.types.PLong;
import org.apache.phoenix.schema.types.PSmallint;
import org.apache.phoenix.schema.types.PUnsignedTinyint;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.google.protobuf.ServiceException;


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

    // See PHOENIX-3955
    public static final List<String> SYNCED_DATA_TABLE_AND_INDEX_COL_FAM_PROPERTIES = ImmutableList.of(
            HColumnDescriptor.TTL,
            HColumnDescriptor.KEEP_DELETED_CELLS,
            HColumnDescriptor.REPLICATION_SCOPE);

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
            compatibility.setErrorCode(SQLExceptionCode.INCOMPATIBLE_CLIENT_SERVER_JAR.getErrorCode());
            compatibility.setCompatible(false);
            return compatibility;
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

    // The first 3 bytes of the long is used to encoding the HBase version as major.minor.patch.
    // The next 4 bytes of the value is used to encode the Phoenix version as major.minor.patch.
    /**
     * Encode HBase and Phoenix version along with some server-side config information such as whether WAL codec is
     * installed (necessary for non transactional, mutable secondar indexing), and whether systemNamespace mapping is enabled.
     * 
     * @param env
     *            RegionCoprocessorEnvironment to access HBase version and Configuration.
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
                // Encode if systemMappingEnabled are enabled on the server side
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
                        KeyValue.Type.codeToType(cell.getTypeByte()), newValue, 0, newValue.length);
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
     * @param valueArray byte array of values or null
     * @param tagArray byte array of tags to add to the cells
     */
    public static void conditionallyAddTagsToPutCells(Put somePut, byte[] family, byte[] qualifier,
            byte[] valueArray, byte[] tagArray) {
        NavigableMap<byte[], List<Cell>> familyCellMap = somePut.getFamilyCellMap();
        List<Cell> cells = familyCellMap.get(family);
        List<Cell> newCells = Lists.newArrayList();
        if (cells != null) {
            for (Cell cell : cells) {
                if (Bytes.compareTo(cell.getQualifierArray(), cell.getQualifierOffset(),
                        cell.getQualifierLength(), qualifier, 0, qualifier.length) == 0 &&
                        (valueArray == null || !CellUtil.matchingValue(cell, valueArray))) {
                    final byte[] combinedTags =
                            ByteUtil.concat(CellUtil.getTagArray(cell), tagArray);
                    Cell newCell = CellUtil.createCell(
                            CellUtil.cloneRow(cell),
                            CellUtil.cloneFamily(cell),
                            CellUtil.cloneQualifier(cell),
                            cell.getTimestamp(),
                            KeyValue.Type.codeToType(cell.getTypeByte()),
                            CellUtil.cloneValue(cell),
                            combinedTags);
                    // Replace existing cell with a cell that has the custom tags list
                    newCells.add(newCell);
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
        Put put = new Put(rowArray, delete.getTimeStamp());
        put.addColumn(family, qualifier, delete.getTimeStamp(), value);
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
                if (Bytes.compareTo(kv.getQualifierArray(), kv.getQualifierOffset(), kv.getQualifierLength(), PhoenixDatabaseMetaData.TABLE_SEQ_NUM_BYTES, 0, PhoenixDatabaseMetaData.TABLE_SEQ_NUM_BYTES.length) == 0) {
                    return PLong.INSTANCE.getCodec().decodeLong(kv.getValueArray(), kv.getValueOffset(), SortOrder.getDefault());
                }
            }
        }
        throw new IllegalStateException();
    }

    public static long getSequenceNumber(List<Mutation> tableMetaData) {
        return getSequenceNumber(getPutOnlyTableHeaderRow(tableMetaData));
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
                KeyValue kv = org.apache.hadoop.hbase.KeyValueUtil.ensureKeyValue(cell);
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
        return kvs.isEmpty() ? m.getTimeStamp() : kvs.iterator().next().get(0).getTimestamp();
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

    public static String getViewIndexSequenceSchemaName(PName physicalName, boolean isNamespaceMapped) {
        if (!isNamespaceMapped) {
            String baseTableName = SchemaUtil.getParentTableNameFromIndexTable(physicalName.getString(),
                MetaDataUtil.VIEW_INDEX_TABLE_PREFIX);
            return SchemaUtil.getSchemaNameFromFullName(baseTableName);
        } else {
            return SchemaUtil.getSchemaNameFromFullName(physicalName.toString());
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
            HTableDescriptor desc = connection.getQueryServices().getTableDescriptor(physicalIndexName);
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
            HTableDescriptor desc = connection.getQueryServices().getTableDescriptor(physicalTableName);
            if(desc == null ) return false;
            return hasLocalIndexColumnFamily(desc);
        } catch (TableNotFoundException e) {
            return false;
        }
    }

    public static boolean hasLocalIndexColumnFamily(HTableDescriptor desc) {
        for (HColumnDescriptor cf : desc.getColumnFamilies()) {
            if (cf.getNameAsString().startsWith(QueryConstants.LOCAL_INDEX_COLUMN_FAMILY_PREFIX)) {
                return true;
            }
        }
        return false;
    }

    public static List<byte[]> getNonLocalIndexColumnFamilies(HTableDescriptor desc) {
    	List<byte[]> families = new ArrayList<byte[]>(desc.getColumnFamilies().length);
        for (HColumnDescriptor cf : desc.getColumnFamilies()) {
            if (!cf.getNameAsString().startsWith(QueryConstants.LOCAL_INDEX_COLUMN_FAMILY_PREFIX)) {
            	families.add(cf.getName());
            }
        }
    	return families;
    }

    public static List<byte[]> getLocalIndexColumnFamilies(PhoenixConnection conn, byte[] physicalTableName) throws SQLException {
        HTableDescriptor desc = conn.getQueryServices().getTableDescriptor(physicalTableName);
        if(desc == null ) return Collections.emptyList();
        List<byte[]> families = new ArrayList<byte[]>(desc.getColumnFamilies().length / 2);
        for (HColumnDescriptor cf : desc.getColumnFamilies()) {
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
        connection.createStatement().executeUpdate("DELETE FROM " + PhoenixDatabaseMetaData.SYSTEM_SEQUENCE + " WHERE "
                + PhoenixDatabaseMetaData.SEQUENCE_SCHEMA
                + (schemaName.length() > 0 ? "='" + schemaName + "'" : " IS NULL") + (isNamespaceMapped
                        ? " AND " + PhoenixDatabaseMetaData.SEQUENCE_NAME + " = '" + sequenceName + "'" : ""));

    }
    
    /**
     * This function checks if all regions of a table is online
     * @param table
     * @return true when all regions of a table are online
     * @throws IOException
     * @throws
     */
    public static boolean tableRegionsOnline(Configuration conf, PTable table) {
        try (HConnection hcon =
               HConnectionManager.getConnection(conf)) {
            List<HRegionLocation> locations = hcon.locateRegions(
              org.apache.hadoop.hbase.TableName.valueOf(table.getPhysicalName().getBytes()));

            for (HRegionLocation loc : locations) {
                try {
                    ServerName sn = loc.getServerName();
                    if (sn == null) continue;

                    AdminService.BlockingInterface admin = hcon.getAdmin(sn);
                    GetRegionInfoRequest request = RequestConverter.buildGetRegionInfoRequest(
                        loc.getRegionInfo().getRegionName());

                    admin.getRegionInfo(null, request);
                } catch (ServiceException e) {
                    IOException ie = ProtobufUtil.getRemoteException(e);
                    LOGGER.debug("Region " + loc.getRegionInfo().getEncodedName() + " isn't online due to:" + ie);
                    return false;
                } catch (RemoteException e) {
                    LOGGER.debug("Cannot get region " + loc.getRegionInfo().getEncodedName() + " info due to error:" + e);
                    return false;
                }
            }
        } catch (IOException ex) {
            LOGGER.warn("tableRegionsOnline failed due to:", ex);
            return false;
        }
        return true;
    }

    public static boolean propertyNotAllowedToBeOutOfSync(String colFamProp) {
        return SYNCED_DATA_TABLE_AND_INDEX_COL_FAM_PROPERTIES.contains(colFamProp);
    }

    public static Map<String, Object> getSyncedProps(HColumnDescriptor defaultCFDesc) {
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
		scan.setStartRow(startKey);
		if (stopKey == null) {
			stopKey = ByteUtil.concat(startKey, QueryConstants.SEPARATOR_BYTE_ARRAY);
			ByteUtil.nextKey(stopKey, stopKey.length);
		}
		scan.setStopRow(stopKey);
		return scan;
	}

    public static LinkType getLinkType(Mutation tableMutation) {
        return getLinkType(tableMutation.getFamilyCellMap().get(PhoenixDatabaseMetaData.TABLE_FAMILY_BYTES));
    }

    public static LinkType getLinkType(Collection<Cell> kvs) {
        if (kvs != null) {
            for (Cell kv : kvs) {
                if (Bytes.compareTo(kv.getQualifierArray(), kv.getQualifierOffset(), kv.getQualifierLength(),
                    PhoenixDatabaseMetaData.LINK_TYPE_BYTES, 0,
                    PhoenixDatabaseMetaData.LINK_TYPE_BYTES.length) == 0) { return LinkType
                    .fromSerializedValue(PUnsignedTinyint.INSTANCE.getCodec().decodeByte(kv.getValueArray(),
                        kv.getValueOffset(), SortOrder.getDefault())); }
            }
        }
        return null;
    }

    public static boolean isLocalIndex(String physicalName) {
        if (physicalName.contains(LOCAL_INDEX_TABLE_PREFIX)) { return true; }
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

    public static String getJdbcUrl(RegionCoprocessorEnvironment env) {
        String zkQuorum = env.getConfiguration().get(HConstants.ZOOKEEPER_QUORUM);
        String zkClientPort = env.getConfiguration().get(HConstants.ZOOKEEPER_CLIENT_PORT,
            Integer.toString(HConstants.DEFAULT_ZOOKEPER_CLIENT_PORT));
        String zkParentNode = env.getConfiguration().get(HConstants.ZOOKEEPER_ZNODE_PARENT,
            HConstants.DEFAULT_ZOOKEEPER_ZNODE_PARENT);
        return PhoenixRuntime.JDBC_PROTOCOL + PhoenixRuntime.JDBC_PROTOCOL_SEPARATOR + zkQuorum
            + PhoenixRuntime.JDBC_PROTOCOL_SEPARATOR + zkClientPort
            + PhoenixRuntime.JDBC_PROTOCOL_SEPARATOR + zkParentNode;
    }

    public static boolean isHColumnProperty(String propName) {
        return HColumnDescriptor.getDefaultValues().containsKey(propName);
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
			for (KeyValue kv : m.getFamilyMap().get(PhoenixDatabaseMetaData.TABLE_FAMILY_BYTES)) {
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
            col = table.getPKColumn(new String(rowKeyMetaData[PhoenixDatabaseMetaData.COLUMN_NAME_INDEX]));
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
            Set<String> columnFamilies  = new HashSet<>();
            physicalTablesSet.add(table.getPhysicalName().getString());
            for(byte[] physicalTableName:physicalTableNames) {
                physicalTablesSet.add(Bytes.toString(physicalTableName));
            }
            for(MetaDataProtocol.SharedTableState s: sharedTableStates) {
                physicalTablesSet.add(s.getPhysicalNames().get(0).getString());
            }
            StringBuilder buf = new StringBuilder("DELETE FROM SYSTEM.STATS WHERE PHYSICAL_NAME IN (");
            Iterator itr = physicalTablesSet.iterator();
            while(itr.hasNext()) {
                buf.append("'" + itr.next() + "',");
            }
            buf.setCharAt(buf.length() - 1, ')');
            if(table.getIndexType()==IndexType.LOCAL) {
                buf.append(" AND COLUMN_FAMILY IN(");
                if (table.getColumnFamilies().isEmpty()) {
                    buf.append("'" + QueryConstants.DEFAULT_LOCAL_INDEX_COLUMN_FAMILY + "',");
                } else {
                    for(PColumnFamily cf : table.getColumnFamilies()) {
                        buf.append("'" + cf.getName().getString() + "',");
                    }
                }
                buf.setCharAt(buf.length() - 1, ')');
            }
            connection.createStatement().execute(buf.toString());
        } finally {
            connection.setAutoCommit(isAutoCommit);
        }
    }
}
