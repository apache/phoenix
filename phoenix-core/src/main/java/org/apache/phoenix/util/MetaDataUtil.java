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

import static org.apache.phoenix.util.SchemaUtil.getVarChars;

import java.io.IOException;
import java.sql.SQLException;
import java.util.Collection;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.HRegionLocation;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.client.HConnection;
import org.apache.hadoop.hbase.client.HConnectionManager;
import org.apache.hadoop.hbase.client.Mutation;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.protobuf.ProtobufUtil;
import org.apache.hadoop.hbase.protobuf.RequestConverter;
import org.apache.hadoop.hbase.protobuf.generated.AdminProtos.AdminService;
import org.apache.hadoop.hbase.protobuf.generated.AdminProtos.GetRegionInfoRequest;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.ipc.RemoteException;
import org.apache.phoenix.coprocessor.MetaDataProtocol;
import org.apache.phoenix.hbase.index.util.ImmutableBytesPtr;
import org.apache.phoenix.hbase.index.util.KeyValueBuilder;
import org.apache.phoenix.hbase.index.util.VersionUtil;
import org.apache.phoenix.jdbc.PhoenixConnection;
import org.apache.phoenix.jdbc.PhoenixDatabaseMetaData;
import org.apache.phoenix.query.QueryConstants;
import org.apache.phoenix.schema.PName;
import org.apache.phoenix.schema.PTable;
import org.apache.phoenix.schema.PTableType;
import org.apache.phoenix.schema.SequenceKey;
import org.apache.phoenix.schema.SortOrder;
import org.apache.phoenix.schema.TableNotFoundException;
import org.apache.phoenix.schema.types.PBoolean;
import org.apache.phoenix.schema.types.PDataType;
import org.apache.phoenix.schema.types.PLong;
import org.apache.phoenix.schema.types.PSmallint;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.protobuf.ServiceException;


public class MetaDataUtil {
    private static final Logger logger = LoggerFactory.getLogger(MetaDataUtil.class);
  
    public static final String VIEW_INDEX_TABLE_PREFIX = "_IDX_";
    public static final byte[] VIEW_INDEX_TABLE_PREFIX_BYTES = Bytes.toBytes(VIEW_INDEX_TABLE_PREFIX);
    public static final String LOCAL_INDEX_TABLE_PREFIX = "_LOCAL_IDX_";
    public static final byte[] LOCAL_INDEX_TABLE_PREFIX_BYTES = Bytes.toBytes(LOCAL_INDEX_TABLE_PREFIX);
    public static final String VIEW_INDEX_SEQUENCE_PREFIX = "_SEQ_";
    public static final String VIEW_INDEX_SEQUENCE_NAME_PREFIX = "_ID_";
    public static final byte[] VIEW_INDEX_SEQUENCE_PREFIX_BYTES = Bytes.toBytes(VIEW_INDEX_SEQUENCE_PREFIX);
    public static final String VIEW_INDEX_ID_COLUMN_NAME = "_INDEX_ID";
    public static final String PARENT_TABLE_KEY = "PARENT_TABLE";
    public static final byte[] PARENT_TABLE_KEY_BYTES = Bytes.toBytes("PARENT_TABLE");
    
    public static boolean areClientAndServerCompatible(long serverHBaseAndPhoenixVersion) {
        // As of 3.0, we allow a client and server to differ for the minor version.
        // Care has to be taken to upgrade the server before the client, as otherwise
        // the client may call expressions that don't yet exist on the server.
        // Differing by the patch version has always been allowed.
        // Only differing by the major version is not allowed.
        return areClientAndServerCompatible(MetaDataUtil.decodePhoenixVersion(serverHBaseAndPhoenixVersion), MetaDataProtocol.PHOENIX_MAJOR_VERSION, MetaDataProtocol.PHOENIX_MINOR_VERSION);
    }

    // Default scope for testing
    static boolean areClientAndServerCompatible(int serverVersion, int clientMajorVersion, int clientMinorVersion) {
        // A server and client with the same major and minor version number must be compatible.
        // So it's important that we roll the PHOENIX_MAJOR_VERSION or PHOENIX_MINOR_VERSION
        // when we make an incompatible change.
        return VersionUtil.encodeMinPatchVersion(clientMajorVersion, clientMinorVersion) <= serverVersion && // Minor major and minor cannot be ahead of server
                VersionUtil.encodeMaxMinorVersion(clientMajorVersion) >= serverVersion; // Major version must at least be up to server version
    }

    // Given the encoded integer representing the phoenix version in the encoded version value.
    // The second byte in int would be the major version, 3rd byte minor version, and 4th byte 
    // patch version.
    public static int decodePhoenixVersion(long version) {
        return (int) ((version << Byte.SIZE * 3) >>> Byte.SIZE * 4);
    }
    
    // TODO: generalize this to use two bytes to return a SQL error code instead
    public static long encodeMutableIndexConfiguredProperly(long version, boolean isValid) {
        if (!isValid) {
            return version | 1;
        }
        return version;
    }
    
    public static boolean decodeMutableIndexConfiguredProperly(long version) {
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

    public static int encodePhoenixVersion() {
        return VersionUtil.encodeVersion(MetaDataProtocol.PHOENIX_MAJOR_VERSION, MetaDataProtocol.PHOENIX_MINOR_VERSION,
                MetaDataProtocol.PHOENIX_PATCH_NUMBER);
    }

    public static long encodeHBaseAndPhoenixVersions(String hbaseVersion) {
        return (((long) VersionUtil.encodeVersion(hbaseVersion)) << (Byte.SIZE * 5)) |
                (((long) VersionUtil.encodeVersion(MetaDataProtocol.PHOENIX_MAJOR_VERSION, MetaDataProtocol.PHOENIX_MINOR_VERSION,
                        MetaDataProtocol.PHOENIX_PATCH_NUMBER)) << (Byte.SIZE * 1));
    }

    public static void getTenantIdAndSchemaAndTableName(List<Mutation> tableMetadata, byte[][] rowKeyMetaData) {
        Mutation m = getTableHeaderRow(tableMetadata);
        getVarChars(m.getRow(), 3, rowKeyMetaData);
    }
    
    public static byte[] getParentTableName(List<Mutation> tableMetadata) {
        if (tableMetadata.size() == 1) {
            return null;
        }
        byte[][] rowKeyMetaData = new byte[3][];
        getTenantIdAndSchemaAndTableName(tableMetadata, rowKeyMetaData);
        byte[] schemaName = rowKeyMetaData[PhoenixDatabaseMetaData.SCHEMA_NAME_INDEX];
        byte[] tableName = rowKeyMetaData[PhoenixDatabaseMetaData.TABLE_NAME_INDEX];
        Mutation m = getParentTableHeaderRow(tableMetadata);
        getVarChars(m.getRow(), 3, rowKeyMetaData);
        if (   Bytes.compareTo(schemaName, rowKeyMetaData[PhoenixDatabaseMetaData.SCHEMA_NAME_INDEX]) == 0
            && Bytes.compareTo(tableName, rowKeyMetaData[PhoenixDatabaseMetaData.TABLE_NAME_INDEX]) == 0) {
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
        return getSequenceNumber(getTableHeaderRow(tableMetaData));
    }
    
    public static PTableType getTableType(List<Mutation> tableMetaData, KeyValueBuilder builder,
      ImmutableBytesPtr value) {
        if (getMutationValue(getPutOnlyTableHeaderRow(tableMetaData),
            PhoenixDatabaseMetaData.TABLE_TYPE_BYTES, builder, value)) {
            return PTableType.fromSerializedValue(value.get()[value.getOffset()]);
        }
        return null;
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

    /**
     * Returns the first Put element in <code>tableMetaData</code>. There could be leading Delete elements before the
     * table header row
     */
    public static Mutation getPutOnlyTableHeaderRow(List<Mutation> tableMetaData) {
        for (Mutation m : tableMetaData) {
            if (m instanceof Put) { return m; }
        }
        throw new IllegalStateException("No table header row found in table meatadata");
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
    
    public static boolean isMultiTenant(Mutation m, KeyValueBuilder builder, ImmutableBytesWritable ptr) {
        if (getMutationValue(m, PhoenixDatabaseMetaData.MULTI_TENANT_BYTES, builder, ptr)) {
            return Boolean.TRUE.equals(PBoolean.INSTANCE.toObject(ptr));
        }
        return false;
    }
    
    public static boolean isSalted(Mutation m, KeyValueBuilder builder, ImmutableBytesWritable ptr) {
        return MetaDataUtil.getMutationValue(m, PhoenixDatabaseMetaData.SALT_BUCKETS_BYTES, builder, ptr);
    }
    
    public static byte[] getViewIndexPhysicalName(byte[] physicalTableName) {
        return ByteUtil.concat(VIEW_INDEX_TABLE_PREFIX_BYTES, physicalTableName);
    }

    public static String getViewIndexTableName(String tableName) {
        return VIEW_INDEX_TABLE_PREFIX + tableName;
    }

    public static String getViewIndexSchemaName(String schemaName) {
        return schemaName;
    }

    public static byte[] getLocalIndexPhysicalName(byte[] physicalTableName) {
        return ByteUtil.concat(LOCAL_INDEX_TABLE_PREFIX_BYTES, physicalTableName);
    }
    
    public static String getLocalIndexTableName(String tableName) {
        return LOCAL_INDEX_TABLE_PREFIX + tableName;
    }
    
    public static String getLocalIndexSchemaName(String schemaName) {
        return schemaName;
    }  

    public static String getUserTableName(String localIndexTableName) {
        String schemaName = SchemaUtil.getSchemaNameFromFullName(localIndexTableName);
        if(!schemaName.isEmpty()) schemaName = schemaName.substring(LOCAL_INDEX_TABLE_PREFIX.length());
        String tableName = localIndexTableName.substring((schemaName.isEmpty() ? 0 : (schemaName.length() + QueryConstants.NAME_SEPARATOR.length()))
            + LOCAL_INDEX_TABLE_PREFIX.length());
        return SchemaUtil.getTableName(schemaName, tableName);
    }

    public static String getViewIndexSchemaName(PName physicalName) {
        return VIEW_INDEX_SEQUENCE_PREFIX + physicalName.getString();
    }
    
    public static SequenceKey getViewIndexSequenceKey(String tenantId, PName physicalName, int nSaltBuckets) {
        // Create global sequence of the form: <prefixed base table name><tenant id>
        // rather than tenant-specific sequence, as it makes it much easier
        // to cleanup when the physical table is dropped, as we can delete
        // all global sequences leading with <prefix> + physical name.
        String schemaName = getViewIndexSchemaName(physicalName);
        String tableName = VIEW_INDEX_SEQUENCE_NAME_PREFIX + (tenantId == null ? "" : tenantId);
        return new SequenceKey(null, schemaName, tableName, nSaltBuckets);
    }

    public static PDataType getViewIndexIdDataType() {
        return PSmallint.INSTANCE;
    }

    public static String getViewIndexIdColumnName() {
        return VIEW_INDEX_ID_COLUMN_NAME;
    }

    public static boolean hasViewIndexTable(PhoenixConnection connection, PName name) throws SQLException {
        return hasViewIndexTable(connection, name.getBytes());
    }
    
    public static boolean hasViewIndexTable(PhoenixConnection connection, String schemaName, String tableName) throws SQLException {
        return hasViewIndexTable(connection, SchemaUtil.getTableNameAsBytes(schemaName, tableName));
    }
    
    public static boolean hasViewIndexTable(PhoenixConnection connection, byte[] physicalTableName) throws SQLException {
        byte[] physicalIndexName = MetaDataUtil.getViewIndexPhysicalName(physicalTableName);
        try {
            HTableDescriptor desc = connection.getQueryServices().getTableDescriptor(physicalIndexName);
            return desc != null && Boolean.TRUE.equals(PBoolean.INSTANCE.toObject(desc.getValue(IS_VIEW_INDEX_TABLE_PROP_BYTES)));
        } catch (TableNotFoundException e) {
            return false;
        }
    }

    public static boolean hasLocalIndexTable(PhoenixConnection connection, PName name) throws SQLException {
        return hasLocalIndexTable(connection, name.getBytes());
    }

    public static boolean hasLocalIndexTable(PhoenixConnection connection, String schemaName, String tableName) throws SQLException {
        return hasLocalIndexTable(connection, SchemaUtil.getTableNameAsBytes(schemaName, tableName));
    }

    public static boolean hasLocalIndexTable(PhoenixConnection connection, byte[] physicalTableName) throws SQLException {
        byte[] physicalIndexName = MetaDataUtil.getLocalIndexPhysicalName(physicalTableName);
        try {
            HTableDescriptor desc = connection.getQueryServices().getTableDescriptor(physicalIndexName);
            return desc != null && Boolean.TRUE.equals(PBoolean.INSTANCE.toObject(desc.getValue(IS_LOCAL_INDEX_TABLE_PROP_BYTES)));
        } catch (TableNotFoundException e) {
            return false;
        }
    }

    public static void deleteViewIndexSequences(PhoenixConnection connection, PName name) throws SQLException {
        String schemaName = getViewIndexSchemaName(name);
        connection.createStatement().executeUpdate("DELETE FROM " + PhoenixDatabaseMetaData.SEQUENCE_FULLNAME_ESCAPED + 
                " WHERE " + PhoenixDatabaseMetaData.TENANT_ID + " IS NULL AND " + 
                PhoenixDatabaseMetaData.SEQUENCE_SCHEMA + " = '" + schemaName + "'");
        
    }
    
    /**
     * This function checks if all regions of a table is online
     * @param table
     * @return true when all regions of a table are online
     * @throws IOException
     * @throws
     */
    public static boolean tableRegionsOnline(Configuration conf, PTable table) {
        HConnection hcon = null;

        try {
            hcon = HConnectionManager.getConnection(conf);
            List<HRegionLocation> locations = hcon.locateRegions(
                org.apache.hadoop.hbase.TableName.valueOf(table.getTableName().getBytes()));

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
                    logger.debug("Region " + loc.getRegionInfo().getEncodedName() + " isn't online due to:" + ie);
                    return false;
                } catch (RemoteException e) {
                    logger.debug("Cannot get region " + loc.getRegionInfo().getEncodedName() + " info due to error:" + e);
                    return false;
                }
            }
        } catch (IOException ex) {
            logger.warn("tableRegionsOnline failed due to:" + ex);
            return false;
        } finally {
            if (hcon != null) {
                try {
                    hcon.close();
                } catch (IOException ignored) {
                }
            }
        }

        return true;
    }

    public static final String IS_VIEW_INDEX_TABLE_PROP_NAME = "IS_VIEW_INDEX_TABLE";
    public static final byte[] IS_VIEW_INDEX_TABLE_PROP_BYTES = Bytes.toBytes(IS_VIEW_INDEX_TABLE_PROP_NAME);

    public static final String IS_LOCAL_INDEX_TABLE_PROP_NAME = "IS_LOCAL_INDEX_TABLE";
    public static final byte[] IS_LOCAL_INDEX_TABLE_PROP_BYTES = Bytes.toBytes(IS_LOCAL_INDEX_TABLE_PROP_NAME);

    public static Scan newTableRowsScan(byte[] key, long startTimeStamp, long stopTimeStamp)
            throws IOException {
        Scan scan = new Scan();
        scan.setTimeRange(startTimeStamp, stopTimeStamp);
        scan.setStartRow(key);
        byte[] stopKey = ByteUtil.concat(key, QueryConstants.SEPARATOR_BYTE_ARRAY);
        ByteUtil.nextKey(stopKey, stopKey.length);
        scan.setStopRow(stopKey);
        return scan;
    }
}
