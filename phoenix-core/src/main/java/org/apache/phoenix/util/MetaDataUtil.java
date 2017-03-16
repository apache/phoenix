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
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.HRegionLocation;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.client.HConnection;
import org.apache.hadoop.hbase.client.HConnectionManager;
import org.apache.hadoop.hbase.client.Mutation;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.coprocessor.RegionCoprocessorEnvironment;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.protobuf.ProtobufUtil;
import org.apache.hadoop.hbase.protobuf.RequestConverter;
import org.apache.hadoop.hbase.protobuf.generated.AdminProtos.AdminService;
import org.apache.hadoop.hbase.protobuf.generated.AdminProtos.GetRegionInfoRequest;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.ipc.RemoteException;
import org.apache.phoenix.coprocessor.MetaDataProtocol;
import org.apache.phoenix.hbase.index.util.ImmutableBytesPtr;
import org.apache.phoenix.hbase.index.util.IndexManagementUtil;
import org.apache.phoenix.hbase.index.util.KeyValueBuilder;
import org.apache.phoenix.hbase.index.util.VersionUtil;
import org.apache.phoenix.jdbc.PhoenixConnection;
import org.apache.phoenix.jdbc.PhoenixDatabaseMetaData;
import org.apache.phoenix.query.QueryConstants;
import org.apache.phoenix.schema.PColumn;
import org.apache.phoenix.schema.PName;
import org.apache.phoenix.schema.PNameFactory;
import org.apache.phoenix.schema.PTable;
import org.apache.phoenix.schema.PTable.LinkType;
import org.apache.phoenix.schema.PTableType;
import org.apache.phoenix.schema.SequenceKey;
import org.apache.phoenix.schema.SortOrder;
import org.apache.phoenix.schema.TableNotFoundException;
import org.apache.phoenix.schema.TableProperty;
import org.apache.phoenix.schema.types.PBoolean;
import org.apache.phoenix.schema.types.PDataType;
import org.apache.phoenix.schema.types.PLong;
import org.apache.phoenix.schema.types.PSmallint;
import org.apache.phoenix.schema.types.PUnsignedTinyint;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.protobuf.ServiceException;


public class MetaDataUtil {
    private static final Logger logger = LoggerFactory.getLogger(MetaDataUtil.class);
  
    public static final String VIEW_INDEX_TABLE_PREFIX = "_IDX_";
    public static final String LOCAL_INDEX_TABLE_PREFIX = "_LOCAL_IDX_";
    public static final String VIEW_INDEX_SEQUENCE_PREFIX = "_SEQ_";
    public static final String VIEW_INDEX_SEQUENCE_NAME_PREFIX = "_ID_";
    public static final byte[] VIEW_INDEX_SEQUENCE_PREFIX_BYTES = Bytes.toBytes(VIEW_INDEX_SEQUENCE_PREFIX);
    private static final String VIEW_INDEX_ID_COLUMN_NAME = "_INDEX_ID";
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
    
    public static void getTenantIdAndSchemaAndTableName(List<Mutation> tableMetadata, byte[][] rowKeyMetaData) {
        Mutation m = getTableHeaderRow(tableMetadata);
        getVarChars(m.getRow(), 3, rowKeyMetaData);
    }

    public static void getTenantIdAndFunctionName(List<Mutation> functionMetadata, byte[][] rowKeyMetaData) {
        Mutation m = getTableHeaderRow(functionMetadata);
        getVarChars(m.getRow(), 2, rowKeyMetaData);
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

    public static String getViewIndexTableName(String tableName) {
        return VIEW_INDEX_TABLE_PREFIX + tableName;
    }

    public static String getViewIndexSchemaName(String schemaName) {
        return schemaName;
    }
    
    public static String getViewIndexName(String schemaName, String tableName) {
        return SchemaUtil.getTableName(getViewIndexSchemaName(schemaName), getViewIndexTableName(tableName));
    }

    public static byte[] getIndexPhysicalName(byte[] physicalTableName, String indexPrefix) {
        return getIndexPhysicalName(Bytes.toString(physicalTableName), indexPrefix).getBytes();
    }

    public static String getIndexPhysicalName(String physicalTableName, String indexPrefix) {
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

    public static String getViewIndexSequenceSchemaName(PName physicalName, boolean isNamespaceMapped) {
        if (!isNamespaceMapped) { return VIEW_INDEX_SEQUENCE_PREFIX + physicalName.getString(); }
        return SchemaUtil.getSchemaNameFromFullName(physicalName.toString());
    }

    public static String getViewIndexSequenceName(PName physicalName, PName tenantId, boolean isNamespaceMapped) {
        if (!isNamespaceMapped) { return VIEW_INDEX_SEQUENCE_NAME_PREFIX + (tenantId == null ? "" : tenantId); }
        return SchemaUtil.getTableNameFromFullName(physicalName.toString()) + VIEW_INDEX_SEQUENCE_NAME_PREFIX;
    }

    public static SequenceKey getViewIndexSequenceKey(String tenantId, PName physicalName, int nSaltBuckets,
            boolean isNamespaceMapped) {
        // Create global sequence of the form: <prefixed base table name><tenant id>
        // rather than tenant-specific sequence, as it makes it much easier
        // to cleanup when the physical table is dropped, as we can delete
        // all global sequences leading with <prefix> + physical name.
        String schemaName = getViewIndexSequenceSchemaName(physicalName, isNamespaceMapped);
        String tableName = getViewIndexSequenceName(physicalName, PNameFactory.newName(tenantId), isNamespaceMapped);
        return new SequenceKey(isNamespaceMapped ? tenantId : null, schemaName, tableName, nSaltBuckets);
    }

    public static PDataType getViewIndexIdDataType() {
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

    public static final String DATA_TABLE_NAME_PROP_NAME = "DATA_TABLE_NAME";

    public static final byte[] DATA_TABLE_NAME_PROP_BYTES = Bytes.toBytes(DATA_TABLE_NAME_PROP_NAME);



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
        List<Cell> kvs = tableMutation.getFamilyCellMap().get(PhoenixDatabaseMetaData.TABLE_FAMILY_BYTES);
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
}
