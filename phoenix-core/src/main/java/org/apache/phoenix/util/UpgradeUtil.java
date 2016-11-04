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

import static com.google.common.base.Preconditions.checkNotNull;
import static org.apache.phoenix.coprocessor.MetaDataProtocol.CURRENT_CLIENT_VERSION;
import static org.apache.phoenix.coprocessor.MetaDataProtocol.getVersion;
import static org.apache.phoenix.jdbc.PhoenixDatabaseMetaData.ARRAY_SIZE;
import static org.apache.phoenix.jdbc.PhoenixDatabaseMetaData.CACHE_SIZE;
import static org.apache.phoenix.jdbc.PhoenixDatabaseMetaData.COLUMN_FAMILY;
import static org.apache.phoenix.jdbc.PhoenixDatabaseMetaData.COLUMN_NAME;
import static org.apache.phoenix.jdbc.PhoenixDatabaseMetaData.COLUMN_SIZE;
import static org.apache.phoenix.jdbc.PhoenixDatabaseMetaData.CURRENT_VALUE;
import static org.apache.phoenix.jdbc.PhoenixDatabaseMetaData.CYCLE_FLAG;
import static org.apache.phoenix.jdbc.PhoenixDatabaseMetaData.DATA_TABLE_NAME;
import static org.apache.phoenix.jdbc.PhoenixDatabaseMetaData.DATA_TYPE;
import static org.apache.phoenix.jdbc.PhoenixDatabaseMetaData.DECIMAL_DIGITS;
import static org.apache.phoenix.jdbc.PhoenixDatabaseMetaData.INCREMENT_BY;
import static org.apache.phoenix.jdbc.PhoenixDatabaseMetaData.LIMIT_REACHED_FLAG;
import static org.apache.phoenix.jdbc.PhoenixDatabaseMetaData.LINK_TYPE;
import static org.apache.phoenix.jdbc.PhoenixDatabaseMetaData.MAX_VALUE;
import static org.apache.phoenix.jdbc.PhoenixDatabaseMetaData.MIN_VALUE;
import static org.apache.phoenix.jdbc.PhoenixDatabaseMetaData.ORDINAL_POSITION;
import static org.apache.phoenix.jdbc.PhoenixDatabaseMetaData.SORT_ORDER;
import static org.apache.phoenix.jdbc.PhoenixDatabaseMetaData.START_WITH;
import static org.apache.phoenix.jdbc.PhoenixDatabaseMetaData.SYSTEM_CATALOG_NAME;
import static org.apache.phoenix.jdbc.PhoenixDatabaseMetaData.SYSTEM_CATALOG_SCHEMA;
import static org.apache.phoenix.jdbc.PhoenixDatabaseMetaData.SYSTEM_CATALOG_TABLE;
import static org.apache.phoenix.jdbc.PhoenixDatabaseMetaData.TABLE_NAME;
import static org.apache.phoenix.jdbc.PhoenixDatabaseMetaData.TABLE_SCHEM;
import static org.apache.phoenix.jdbc.PhoenixDatabaseMetaData.TABLE_SEQ_NUM;
import static org.apache.phoenix.jdbc.PhoenixDatabaseMetaData.TABLE_TYPE;
import static org.apache.phoenix.jdbc.PhoenixDatabaseMetaData.TENANT_ID;
import static org.apache.phoenix.jdbc.PhoenixDatabaseMetaData.VIEW_INDEX_ID;
import static org.apache.phoenix.query.QueryConstants.BASE_TABLE_BASE_COLUMN_COUNT;
import static org.apache.phoenix.query.QueryConstants.DIVERGED_VIEW_BASE_COLUMN_COUNT;

import java.io.IOException;
import java.sql.Connection;
import java.sql.Date;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.text.Format;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.TimeoutException;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTableInterface;
import org.apache.hadoop.hbase.client.Mutation;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.regionserver.LocalIndexSplitter;
import org.apache.hadoop.hbase.snapshot.SnapshotCreationException;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.phoenix.coprocessor.MetaDataEndpointImpl;
import org.apache.phoenix.coprocessor.MetaDataProtocol;
import org.apache.phoenix.coprocessor.MetaDataProtocol.MetaDataMutationResult;
import org.apache.phoenix.coprocessor.MetaDataProtocol.MutationCode;
import org.apache.phoenix.jdbc.PhoenixConnection;
import org.apache.phoenix.jdbc.PhoenixDatabaseMetaData;
import org.apache.phoenix.query.QueryConstants;
import org.apache.phoenix.query.QueryServices;
import org.apache.phoenix.schema.MetaDataClient;
import org.apache.phoenix.schema.PName;
import org.apache.phoenix.schema.PNameFactory;
import org.apache.phoenix.schema.PTable;
import org.apache.phoenix.schema.PTable.IndexType;
import org.apache.phoenix.schema.PTable.LinkType;
import org.apache.phoenix.schema.PTableType;
import org.apache.phoenix.schema.SaltingUtil;
import org.apache.phoenix.schema.SortOrder;
import org.apache.phoenix.schema.TableNotFoundException;
import org.apache.phoenix.schema.types.PBinary;
import org.apache.phoenix.schema.types.PBoolean;
import org.apache.phoenix.schema.types.PChar;
import org.apache.phoenix.schema.types.PDataType;
import org.apache.phoenix.schema.types.PDecimal;
import org.apache.phoenix.schema.types.PDouble;
import org.apache.phoenix.schema.types.PFloat;
import org.apache.phoenix.schema.types.PInteger;
import org.apache.phoenix.schema.types.PLong;
import org.apache.phoenix.schema.types.PVarbinary;
import org.apache.phoenix.schema.types.PVarchar;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Objects;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;

public class UpgradeUtil {
    private static final Logger logger = LoggerFactory.getLogger(UpgradeUtil.class);
    private static final byte[] SEQ_PREFIX_BYTES = ByteUtil.concat(QueryConstants.SEPARATOR_BYTE_ARRAY, Bytes.toBytes("_SEQ_"));
    public static final byte[] UPGRADE_TO_4_7_COLUMN_NAME = Bytes.toBytes("UPGRADE_TO_4_7");
    /**
     * Attribute for Phoenix's internal purposes only. When this attribute is set on a phoenix connection, then
     * the upgrade code for upgrading the cluster to the new minor release is not triggered. Note that presence 
     * of this attribute overrides a true value for {@value QueryServices#AUTO_UPGRADE_ENABLED}.     
     */
    private static final String DO_NOT_UPGRADE = "DoNotUpgrade";
    public static String UPSERT_BASE_COLUMN_COUNT_IN_HEADER_ROW = "UPSERT "
            + "INTO SYSTEM.CATALOG "
            + "(TENANT_ID, TABLE_SCHEM, TABLE_NAME, COLUMN_NAME, COLUMN_FAMILY, BASE_COLUMN_COUNT) "
            + "VALUES (?, ?, ?, ?, ?, ?) ";

    public static String SELECT_BASE_COLUMN_COUNT_FROM_HEADER_ROW = "SELECT "
            + "BASE_COLUMN_COUNT "
            + "FROM SYSTEM.CATALOG "
            + "WHERE "
            + "COLUMN_NAME IS NULL "
            + "AND "
            + "COLUMN_FAMILY IS NULL "
            + "AND "
            + "TENANT_ID %s "
            + "AND "
            + "TABLE_SCHEM %s "
            + "AND "
            + "TABLE_NAME = ? "
            ;
    
    private static final String UPDATE_LINK =
            "UPSERT INTO " + SYSTEM_CATALOG_SCHEMA + ".\"" + SYSTEM_CATALOG_TABLE + "\"( " +
            TENANT_ID + "," +
            TABLE_SCHEM + "," +
            TABLE_NAME + "," +
            COLUMN_FAMILY + "," +
            LINK_TYPE + "," +
            TABLE_SEQ_NUM +"," +
            TABLE_TYPE +
            ") SELECT " + TENANT_ID + "," + TABLE_SCHEM + "," + TABLE_NAME + ",'%s' AS "
            + COLUMN_FAMILY + " ," + LINK_TYPE + "," + TABLE_SEQ_NUM + "," + TABLE_TYPE +" FROM " + SYSTEM_CATALOG_SCHEMA + ".\""
            + SYSTEM_CATALOG_TABLE + "\" WHERE  (" + TABLE_SCHEM + "=? OR (" + TABLE_SCHEM + " IS NULL AND ? IS NULL)) AND " + TABLE_NAME + "=? AND " + COLUMN_FAMILY + "=? AND " + LINK_TYPE + " = "
            + LinkType.PHYSICAL_TABLE.getSerializedValue();

    private static final String DELETE_LINK = "DELETE FROM " + SYSTEM_CATALOG_SCHEMA + "." + SYSTEM_CATALOG_TABLE
            + " WHERE (" + TABLE_SCHEM + "=? OR (" + TABLE_SCHEM + " IS NULL AND ? IS NULL)) AND " + TABLE_NAME + "=? AND " + COLUMN_FAMILY + "=? AND " + LINK_TYPE + " = " + LinkType.PHYSICAL_TABLE.getSerializedValue();
    
    private static final String GET_VIEWS_QUERY = "SELECT " + TENANT_ID + "," + TABLE_SCHEM + "," + TABLE_NAME
            + " FROM " + SYSTEM_CATALOG_SCHEMA + "." + SYSTEM_CATALOG_TABLE + " WHERE " + COLUMN_FAMILY + " = ? AND "
            + LINK_TYPE + " = " + LinkType.PHYSICAL_TABLE.getSerializedValue() + " AND ( " + TABLE_TYPE + "=" + "'"
            + PTableType.VIEW.getSerializedValue() + "' OR " + TABLE_TYPE + " IS NULL) ORDER BY "+TENANT_ID;
    
    private UpgradeUtil() {
    }

    private static byte[] getSequenceSnapshotName() {
        return Bytes.toBytes("_BAK_" + PhoenixDatabaseMetaData.SYSTEM_SEQUENCE_NAME);
    }
    
    private static void createSequenceSnapshot(HBaseAdmin admin, PhoenixConnection conn) throws SQLException {
        byte[] tableName = getSequenceSnapshotName();
        HColumnDescriptor columnDesc = new HColumnDescriptor(PhoenixDatabaseMetaData.SYSTEM_SEQUENCE_FAMILY_BYTES);
        HTableDescriptor desc = new HTableDescriptor(TableName.valueOf(tableName));
        desc.addFamily(columnDesc);
        try {
            admin.createTable(desc);
            copyTable(conn, PhoenixDatabaseMetaData.SYSTEM_SEQUENCE_NAME_BYTES, tableName);
        } catch (IOException e) {
            throw ServerUtil.parseServerException(e);
        }
    }
    
    private static void restoreSequenceSnapshot(HBaseAdmin admin, PhoenixConnection conn) throws SQLException {
        byte[] tableName = getSequenceSnapshotName();
        copyTable(conn, tableName, PhoenixDatabaseMetaData.SYSTEM_SEQUENCE_NAME_BYTES);
    }
    
    private static void deleteSequenceSnapshot(HBaseAdmin admin) throws SQLException {
        byte[] tableName = getSequenceSnapshotName();
        try {
            admin.disableTable(tableName);;
            admin.deleteTable(tableName);
        } catch (IOException e) {
            throw ServerUtil.parseServerException(e);
        }
    }

    @SuppressWarnings("deprecation")
    private static void copyTable(PhoenixConnection conn, byte[] sourceName, byte[] targetName) throws SQLException {
        int batchSizeBytes = 100 * 1024; // 100K chunks
        int sizeBytes = 0;
        List<Mutation> mutations =  Lists.newArrayListWithExpectedSize(10000);

        Scan scan = new Scan();
        scan.setRaw(true);
        scan.setMaxVersions(MetaDataProtocol.DEFAULT_MAX_META_DATA_VERSIONS);
        ResultScanner scanner = null;
        HTableInterface source = null;
        HTableInterface target = null;
        try {
            source = conn.getQueryServices().getTable(sourceName);
            target = conn.getQueryServices().getTable(targetName);
            scanner = source.getScanner(scan);
            Result result;
             while ((result = scanner.next()) != null) {
                for (KeyValue keyValue : result.raw()) {
                    sizeBytes += keyValue.getLength();
                    if (KeyValue.Type.codeToType(keyValue.getType()) == KeyValue.Type.Put) {
                        // Put new value
                        Put put = new Put(keyValue.getRow());
                        put.add(keyValue);
                        mutations.add(put);
                    } else if (KeyValue.Type.codeToType(keyValue.getType()) == KeyValue.Type.Delete){
                        // Copy delete marker using new key so that it continues
                        // to delete the key value preceding it that will be updated
                        // as well.
                        Delete delete = new Delete(keyValue.getRow());
                        delete.addDeleteMarker(keyValue);
                        mutations.add(delete);
                    }
                }
                if (sizeBytes >= batchSizeBytes) {
                    logger.info("Committing bactch of temp rows");
                    target.batch(mutations);
                    mutations.clear();
                    sizeBytes = 0;
                }
            }
            if (!mutations.isEmpty()) {
                logger.info("Committing last bactch of temp rows");
                target.batch(mutations);
            }
            logger.info("Successfully completed copy");
        } catch (SQLException e) {
            throw e;
        } catch (Exception e) {
            throw ServerUtil.parseServerException(e);
        } finally {
            try {
                if (scanner != null) scanner.close();
            } finally {
                try {
                    if (source != null) source.close();
                } catch (IOException e) {
                    logger.warn("Exception during close of source table",e);
                } finally {
                    try {
                        if (target != null) target.close();
                    } catch (IOException e) {
                        logger.warn("Exception during close of target table",e);
                    }
                }
            }
        }
    }
    
    private static void preSplitSequenceTable(PhoenixConnection conn, int nSaltBuckets) throws SQLException {
        HBaseAdmin admin = conn.getQueryServices().getAdmin();
        boolean snapshotCreated = false;
        boolean success = false;
        try {
            if (nSaltBuckets <= 0) {
                return;
            }
            logger.warn("Pre-splitting SYSTEM.SEQUENCE table " + nSaltBuckets + "-ways. This may take some time - please do not close window.");
            HTableDescriptor desc = admin.getTableDescriptor(PhoenixDatabaseMetaData.SYSTEM_SEQUENCE_NAME_BYTES);
            createSequenceSnapshot(admin, conn);
            snapshotCreated = true;
            admin.disableTable(PhoenixDatabaseMetaData.SYSTEM_SEQUENCE_NAME);
            admin.deleteTable(PhoenixDatabaseMetaData.SYSTEM_SEQUENCE_NAME);
            byte[][] splitPoints = SaltingUtil.getSalteByteSplitPoints(nSaltBuckets);
            admin.createTable(desc, splitPoints);
            restoreSequenceSnapshot(admin, conn);
            success = true;
            logger.warn("Completed pre-splitting SYSTEM.SEQUENCE table");
        } catch (IOException e) {
            throw new SQLException("Unable to pre-split SYSTEM.SEQUENCE table", e);
        } finally {
            try {
                if (snapshotCreated && success) {
                    try {
                        deleteSequenceSnapshot(admin);
                    } catch (SQLException e) {
                        logger.warn("Exception while deleting SYSTEM.SEQUENCE snapshot during pre-split", e);
                    }
                }
            } finally {
                try {
                    admin.close();
                } catch (IOException e) {
                    logger.warn("Exception while closing admin during pre-split", e);
                }
            }
        }
    }

    public static PhoenixConnection upgradeLocalIndexes(PhoenixConnection metaConnection)
            throws SQLException, IOException, org.apache.hadoop.hbase.TableNotFoundException {
        Properties props = PropertiesUtil.deepCopy(metaConnection.getClientInfo());
        Long originalScn = null;
        String str = props.getProperty(PhoenixRuntime.CURRENT_SCN_ATTRIB);
        if (str != null) {
            originalScn = Long.valueOf(str);
        }
        props.setProperty(PhoenixRuntime.CURRENT_SCN_ATTRIB, Long.toString(HConstants.LATEST_TIMESTAMP));
        PhoenixConnection globalConnection = null;
        PhoenixConnection toReturn = null;
        globalConnection = new PhoenixConnection(metaConnection, metaConnection.getQueryServices(), props);
        SQLException sqlEx = null;
        try (HBaseAdmin admin = globalConnection.getQueryServices().getAdmin()) {
            ResultSet rs = globalConnection.createStatement().executeQuery("SELECT TABLE_SCHEM, TABLE_NAME, DATA_TABLE_NAME, TENANT_ID, MULTI_TENANT, SALT_BUCKETS FROM SYSTEM.CATALOG  "
                    + "      WHERE COLUMN_NAME IS NULL"
                    + "           AND COLUMN_FAMILY IS NULL"
                    + "           AND INDEX_TYPE=" + IndexType.LOCAL.getSerializedValue());
            boolean droppedLocalIndexes = false;
            while (rs.next()) {
                if(!droppedLocalIndexes) {
                    HTableDescriptor[] localIndexTables = admin.listTables(MetaDataUtil.LOCAL_INDEX_TABLE_PREFIX+".*");
                    String localIndexSplitter = LocalIndexSplitter.class.getName();
                    for (HTableDescriptor table : localIndexTables) {
                        HTableDescriptor dataTableDesc = admin.getTableDescriptor(TableName.valueOf(MetaDataUtil.getLocalIndexUserTableName(table.getNameAsString())));
                        HColumnDescriptor[] columnFamilies = dataTableDesc.getColumnFamilies();
                        boolean modifyTable = false;
                        for(HColumnDescriptor cf : columnFamilies) {
                            String localIndexCf = QueryConstants.LOCAL_INDEX_COLUMN_FAMILY_PREFIX+cf.getNameAsString();
                            if(dataTableDesc.getFamily(Bytes.toBytes(localIndexCf))==null){
                                HColumnDescriptor colDef =
                                        new HColumnDescriptor(localIndexCf);
                                for(Entry<ImmutableBytesWritable, ImmutableBytesWritable>keyValue: cf.getValues().entrySet()){
                                    colDef.setValue(keyValue.getKey().copyBytes(), keyValue.getValue().copyBytes());
                                }
                                dataTableDesc.addFamily(colDef);
                                modifyTable = true;
                            }
                        }
                        List<String> coprocessors = dataTableDesc.getCoprocessors();
                        for(String coprocessor:  coprocessors) {
                            if(coprocessor.equals(localIndexSplitter)) {
                                dataTableDesc.removeCoprocessor(localIndexSplitter);
                                modifyTable = true;
                            }
                        }
                        if(modifyTable) {
                            admin.modifyTable(dataTableDesc.getName(), dataTableDesc);
                        }
                    }
                    admin.disableTables(MetaDataUtil.LOCAL_INDEX_TABLE_PREFIX+".*");
                    admin.deleteTables(MetaDataUtil.LOCAL_INDEX_TABLE_PREFIX+".*");
                    droppedLocalIndexes = true;
                }
                String schemaName = rs.getString(1);
                String indexTableName = rs.getString(2);
                String dataTableName = rs.getString(3);
                String tenantId = rs.getString(4);
                boolean multiTenantTable = rs.getBoolean(5);
                int numColumnsToSkip = 1 + (multiTenantTable ? 1 : 0);
                String getColumns =
                        "SELECT COLUMN_NAME, COLUMN_FAMILY FROM SYSTEM.CATALOG  WHERE TABLE_SCHEM "
                                + (schemaName == null ? "IS NULL " : "='" + schemaName+ "'")
                                + " AND TENANT_ID "+(tenantId == null ? "IS NULL " : "='" + tenantId + "'")
                                + " and TABLE_NAME='" + indexTableName
                                + "' AND COLUMN_NAME IS NOT NULL AND KEY_SEQ > "+ numColumnsToSkip +" ORDER BY KEY_SEQ";
                ResultSet getColumnsRs = globalConnection.createStatement().executeQuery(getColumns);
                List<String> indexedColumns = new ArrayList<String>(1);
                List<String> coveredColumns = new ArrayList<String>(1);
                
                while (getColumnsRs.next()) {
                    String column = getColumnsRs.getString(1);
                    String columnName = IndexUtil.getDataColumnName(column);
                    String columnFamily = IndexUtil.getDataColumnFamilyName(column);
                    if (getColumnsRs.getString(2) == null) {
                        if (columnFamily != null && !columnFamily.isEmpty()) {
                            if (SchemaUtil.normalizeIdentifier(columnFamily).equals(QueryConstants.DEFAULT_COLUMN_FAMILY)) {
                                indexedColumns.add(columnName);
                            } else {
                                indexedColumns.add(SchemaUtil.getCaseSensitiveColumnDisplayName(
                                    columnFamily, columnName));
                            }
                        } else {
                            indexedColumns.add(columnName);
                        }
                    } else {
                        coveredColumns.add(SchemaUtil.normalizeIdentifier(columnFamily)
                                .equals(QueryConstants.DEFAULT_COLUMN_FAMILY) ? columnName
                                : SchemaUtil.getCaseSensitiveColumnDisplayName(
                                    columnFamily, columnName));
                    }
                }
                StringBuilder createIndex = new StringBuilder("CREATE LOCAL INDEX ");
                createIndex.append(indexTableName);
                createIndex.append(" ON ");
                createIndex.append(SchemaUtil.getTableName(schemaName, dataTableName));
                createIndex.append("(");
                for (int i = 0; i < indexedColumns.size(); i++) {
                    createIndex.append(indexedColumns.get(i));
                    if (i < indexedColumns.size() - 1) {
                        createIndex.append(",");
                    }
                }
                createIndex.append(")");
               
                if (!coveredColumns.isEmpty()) {
                    createIndex.append(" INCLUDE(");
                    for (int i = 0; i < coveredColumns.size(); i++) {
                        createIndex.append(coveredColumns.get(i));
                        if (i < coveredColumns.size() - 1) {
                            createIndex.append(",");
                        }
                    }
                    createIndex.append(")");
                }
                createIndex.append(" ASYNC");
                logger.info("Index creation query is : " + createIndex.toString());
                logger.info("Dropping the index " + indexTableName
                    + " to clean up the index details from SYSTEM.CATALOG.");
                PhoenixConnection localConnection = null;
                if (tenantId != null) {
                    props.setProperty(PhoenixRuntime.TENANT_ID_ATTRIB, tenantId);
                    localConnection = new PhoenixConnection(globalConnection, globalConnection.getQueryServices(), props);
                }
                try {
                    (localConnection == null ? globalConnection : localConnection).createStatement().execute(
                        "DROP INDEX IF EXISTS " + indexTableName + " ON "
                                + SchemaUtil.getTableName(schemaName, dataTableName));
                    logger.info("Recreating the index " + indexTableName);
                    (localConnection == null ? globalConnection : localConnection).createStatement().execute(createIndex.toString());
                    logger.info("Created the index " + indexTableName);
                } finally {
                    props.remove(PhoenixRuntime.TENANT_ID_ATTRIB);
                    if (localConnection != null) {
                        sqlEx = closeConnection(localConnection, sqlEx);
                        if (sqlEx != null) {
                            throw sqlEx;
                        }
                    }
                }
            }
            globalConnection.createStatement().execute("DELETE FROM SYSTEM.CATALOG WHERE SUBSTR(TABLE_NAME,0,11)='"+MetaDataUtil.LOCAL_INDEX_TABLE_PREFIX+"'");
            if (originalScn != null) {
                props.setProperty(PhoenixRuntime.CURRENT_SCN_ATTRIB, Long.toString(originalScn));
            }
            toReturn = new PhoenixConnection(globalConnection, globalConnection.getQueryServices(), props);
        } catch (SQLException e) {
            sqlEx = e;
        } finally {
            sqlEx = closeConnection(metaConnection, sqlEx);
            sqlEx = closeConnection(globalConnection, sqlEx);
            if (sqlEx != null) {
                throw sqlEx;
            }
        }
        return toReturn;
    }

    public static PhoenixConnection disableViewIndexes(PhoenixConnection connParam) throws SQLException, IOException, InterruptedException, TimeoutException {
        Properties props = PropertiesUtil.deepCopy(connParam.getClientInfo());
        Long originalScn = null;
        String str = props.getProperty(PhoenixRuntime.CURRENT_SCN_ATTRIB);
        if (str != null) {
            originalScn = Long.valueOf(str);
        }
        // don't use the passed timestamp as scn because we want to query all view indexes up to now.
        props.setProperty(PhoenixRuntime.CURRENT_SCN_ATTRIB, Long.toString(HConstants.LATEST_TIMESTAMP));
        Set<String> physicalTables = new HashSet<>();
        SQLException sqlEx = null;
        PhoenixConnection globalConnection = null;
        PhoenixConnection toReturn = null;
        try {
            globalConnection = new PhoenixConnection(connParam, connParam.getQueryServices(), props);
            String tenantId = null;
            try (HBaseAdmin admin = globalConnection.getQueryServices().getAdmin()) {
                String fetchViewIndexes = "SELECT " + TENANT_ID + ", " + TABLE_SCHEM + ", " + TABLE_NAME + 
                        ", " + DATA_TABLE_NAME + " FROM " + SYSTEM_CATALOG_NAME + " WHERE " + VIEW_INDEX_ID
                        + " IS NOT NULL";
                String disableIndexDDL = "ALTER INDEX %s ON %s DISABLE"; 
                try (ResultSet rs = globalConnection.createStatement().executeQuery(fetchViewIndexes)) {
                    while (rs.next()) {
                        tenantId = rs.getString(1);
                        String indexSchema = rs.getString(2);
                        String indexName = rs.getString(3);
                        String viewName = rs.getString(4);
                        String fullIndexName = SchemaUtil.getTableName(indexSchema, indexName);
                        String fullViewName = SchemaUtil.getTableName(indexSchema, viewName);
                        PTable viewPTable = null;
                        // Disable the view index and truncate the underlying hbase table. 
                        // Users would need to rebuild the view indexes. 
                        if (tenantId != null && !tenantId.isEmpty()) {
                            Properties newProps = PropertiesUtil.deepCopy(globalConnection.getClientInfo());
                            newProps.setProperty(PhoenixRuntime.TENANT_ID_ATTRIB, tenantId);
                            PTable indexPTable = null;
                            try (PhoenixConnection tenantConnection = new PhoenixConnection(globalConnection, globalConnection.getQueryServices(), newProps)) {
                                viewPTable = PhoenixRuntime.getTable(tenantConnection, fullViewName);
                                tenantConnection.createStatement().execute(String.format(disableIndexDDL, indexName, fullViewName));
                                indexPTable = PhoenixRuntime.getTable(tenantConnection, fullIndexName);
                            }

                            int offset = indexPTable.getBucketNum() != null ? 1 : 0;
                            int existingTenantIdPosition = ++offset; // positions are stored 1 based
                            int existingViewIdxIdPosition = ++offset;
                            int newTenantIdPosition = existingViewIdxIdPosition;
                            int newViewIdxPosition = existingTenantIdPosition;
                            String tenantIdColumn = indexPTable.getColumns().get(existingTenantIdPosition - 1).getName().getString();
                            int index = 0;
                            String updatePosition =
                                    "UPSERT INTO "
                                            + SYSTEM_CATALOG_NAME
                                            + " ( "
                                            + TENANT_ID
                                            + ","
                                            + TABLE_SCHEM
                                            + ","
                                            + TABLE_NAME
                                            + ","
                                            + COLUMN_NAME
                                            + ","
                                            + COLUMN_FAMILY
                                            + ","
                                            + ORDINAL_POSITION
                                            + ") SELECT "
                                            + TENANT_ID
                                            + ","
                                            + TABLE_SCHEM
                                            + ","
                                            + TABLE_NAME
                                            + ","
                                            + COLUMN_NAME
                                            + ","
                                            + COLUMN_FAMILY
                                            + ","
                                            + "?"
                                            + " FROM "
                                            + SYSTEM_CATALOG_NAME
                                            + " WHERE "
                                            + TENANT_ID
                                            + " = ? "
                                            + " AND "
                                            + TABLE_NAME
                                            + " = ? "
                                            + " AND "
                                            + (indexSchema == null ? TABLE_SCHEM + " IS NULL" : TABLE_SCHEM + " = ? ") 
                                            + " AND "
                                            + COLUMN_NAME 
                                            + " = ? ";
                            // update view index position
                            try (PreparedStatement s = globalConnection.prepareStatement(updatePosition)) {
                                index = 0;
                                s.setInt(++index, newViewIdxPosition);
                                s.setString(++index, tenantId);
                                s.setString(++index, indexName);
                                if (indexSchema != null) {
                                    s.setString(++index, indexSchema);
                                }
                                s.setString(++index, MetaDataUtil.getViewIndexIdColumnName());
                                s.executeUpdate();
                            }
                            // update tenant id position
                            try (PreparedStatement s = globalConnection.prepareStatement(updatePosition)) {
                                index = 0;
                                s.setInt(++index, newTenantIdPosition);
                                s.setString(++index, tenantId);
                                s.setString(++index, indexName);
                                if (indexSchema != null) {
                                    s.setString(++index, indexSchema);
                                }
                                s.setString(++index, tenantIdColumn);
                                s.executeUpdate();
                            }
                            globalConnection.commit();
                        } else {
                            viewPTable = PhoenixRuntime.getTable(globalConnection, fullViewName);
                            globalConnection.createStatement().execute(String.format(disableIndexDDL, indexName, fullViewName));
                        }
                        String indexPhysicalTableName = MetaDataUtil.getViewIndexTableName(viewPTable.getPhysicalName().getString());
                        if (physicalTables.add(indexPhysicalTableName)) {
                            final TableName tableName = TableName.valueOf(indexPhysicalTableName);
                            admin.disableTable(tableName);
                            admin.truncateTable(tableName, false);
                        }
                    }
                }
            }
            if (originalScn != null) {
                props.setProperty(PhoenixRuntime.CURRENT_SCN_ATTRIB, Long.toString(originalScn));
            }
            toReturn = new PhoenixConnection(globalConnection, globalConnection.getQueryServices(), props);
        } catch (SQLException e) {
            sqlEx = e;
        } finally {
            sqlEx = closeConnection(connParam, sqlEx);
            sqlEx = closeConnection(globalConnection, sqlEx);
            if (sqlEx != null) {
                throw sqlEx;
            }
        }
        return toReturn;
    }
    
    
    public static SQLException closeConnection(PhoenixConnection conn, SQLException sqlEx) {
        SQLException toReturn = sqlEx;
        try {
            conn.close();
        } catch (SQLException e) {
            if (toReturn != null) {
                toReturn.setNextException(e);
            } else {
                toReturn = e;
            }
        }
        return toReturn;
    }
    @SuppressWarnings("deprecation")
    public static boolean upgradeSequenceTable(PhoenixConnection conn, int nSaltBuckets, PTable oldTable) throws SQLException {
        logger.info("Upgrading SYSTEM.SEQUENCE table");

        byte[] seqTableKey = SchemaUtil.getTableKey(null, PhoenixDatabaseMetaData.SYSTEM_SEQUENCE_SCHEMA, PhoenixDatabaseMetaData.SYSTEM_SEQUENCE_TABLE);
        HTableInterface sysTable = conn.getQueryServices().getTable(PhoenixDatabaseMetaData.SYSTEM_CATALOG_NAME_BYTES);
        try {
            logger.info("Setting SALT_BUCKETS property of SYSTEM.SEQUENCE to " + SaltingUtil.MAX_BUCKET_NUM);
            KeyValue saltKV = KeyValueUtil.newKeyValue(seqTableKey, 
                    PhoenixDatabaseMetaData.TABLE_FAMILY_BYTES,
                    PhoenixDatabaseMetaData.SALT_BUCKETS_BYTES,
                    MetaDataProtocol.MIN_SYSTEM_TABLE_TIMESTAMP,
                    PInteger.INSTANCE.toBytes(nSaltBuckets));
            Put saltPut = new Put(seqTableKey);
            saltPut.add(saltKV);
            // Prevent multiple clients from doing this upgrade
            if (!sysTable.checkAndPut(seqTableKey,
                    PhoenixDatabaseMetaData.TABLE_FAMILY_BYTES,
                    PhoenixDatabaseMetaData.SALT_BUCKETS_BYTES, null, saltPut)) {
                if (oldTable == null) { // Unexpected, but to be safe just run pre-split code
                    preSplitSequenceTable(conn, nSaltBuckets);
                    return true;
                }
                // If upgrading from 4.2.0, then we need this special case of pre-splitting the table.
                // This is needed as a fix for https://issues.apache.org/jira/browse/PHOENIX-1401 
                if (oldTable.getTimeStamp() == MetaDataProtocol.MIN_SYSTEM_TABLE_TIMESTAMP_4_2_0) {
                    byte[] oldSeqNum = PLong.INSTANCE.toBytes(oldTable.getSequenceNumber());
                    KeyValue seqNumKV = KeyValueUtil.newKeyValue(seqTableKey, 
                            PhoenixDatabaseMetaData.TABLE_FAMILY_BYTES,
                            PhoenixDatabaseMetaData.TABLE_SEQ_NUM_BYTES,
                            MetaDataProtocol.MIN_SYSTEM_TABLE_TIMESTAMP,
                            PLong.INSTANCE.toBytes(MetaDataProtocol.MIN_SYSTEM_TABLE_TIMESTAMP));
                    Put seqNumPut = new Put(seqTableKey);
                    seqNumPut.add(seqNumKV);
                    // Increment TABLE_SEQ_NUM in checkAndPut as semaphore so that only single client
                    // pre-splits the sequence table.
                    if (sysTable.checkAndPut(seqTableKey,
                            PhoenixDatabaseMetaData.TABLE_FAMILY_BYTES,
                            PhoenixDatabaseMetaData.TABLE_SEQ_NUM_BYTES, oldSeqNum, seqNumPut)) {
                        preSplitSequenceTable(conn, nSaltBuckets);
                        return true;
                    }
                }
                logger.info("SYSTEM.SEQUENCE table has already been upgraded");
                return false;
            }
            
            // if the SYSTEM.SEQUENCE table is at 4.1.0 or before then we need to salt the table
            // and pre-split it.
            if (oldTable.getTimeStamp() <= MetaDataProtocol.MIN_SYSTEM_TABLE_TIMESTAMP_4_1_0) {
                int batchSizeBytes = 100 * 1024; // 100K chunks
                int sizeBytes = 0;
                List<Mutation> mutations =  Lists.newArrayListWithExpectedSize(10000);

                boolean success = false;
                Scan scan = new Scan();
                scan.setRaw(true);
                scan.setMaxVersions(MetaDataProtocol.DEFAULT_MAX_META_DATA_VERSIONS);
                HTableInterface seqTable = conn.getQueryServices().getTable(PhoenixDatabaseMetaData.SYSTEM_SEQUENCE_NAME_BYTES);
                try {
                    boolean committed = false;
                    logger.info("Adding salt byte to all SYSTEM.SEQUENCE rows");
                    ResultScanner scanner = seqTable.getScanner(scan);
                    try {
                        Result result;
                        while ((result = scanner.next()) != null) {
                            for (KeyValue keyValue : result.raw()) {
                                KeyValue newKeyValue = addSaltByte(keyValue, nSaltBuckets);
                                if (newKeyValue != null) {
                                    sizeBytes += newKeyValue.getLength();
                                    if (KeyValue.Type.codeToType(newKeyValue.getType()) == KeyValue.Type.Put) {
                                        // Delete old value
                                        byte[] buf = keyValue.getBuffer();
                                        Delete delete = new Delete(keyValue.getRow());
                                        KeyValue deleteKeyValue = new KeyValue(buf, keyValue.getRowOffset(), keyValue.getRowLength(),
                                                buf, keyValue.getFamilyOffset(), keyValue.getFamilyLength(),
                                                buf, keyValue.getQualifierOffset(), keyValue.getQualifierLength(),
                                                keyValue.getTimestamp(), KeyValue.Type.Delete,
                                                ByteUtil.EMPTY_BYTE_ARRAY,0,0);
                                        delete.addDeleteMarker(deleteKeyValue);
                                        mutations.add(delete);
                                        sizeBytes += deleteKeyValue.getLength();
                                        // Put new value
                                        Put put = new Put(newKeyValue.getRow());
                                        put.add(newKeyValue);
                                        mutations.add(put);
                                    } else if (KeyValue.Type.codeToType(newKeyValue.getType()) == KeyValue.Type.Delete){
                                        // Copy delete marker using new key so that it continues
                                        // to delete the key value preceding it that will be updated
                                        // as well.
                                        Delete delete = new Delete(newKeyValue.getRow());
                                        delete.addDeleteMarker(newKeyValue);
                                        mutations.add(delete);
                                    }
                                }
                                if (sizeBytes >= batchSizeBytes) {
                                    logger.info("Committing bactch of SYSTEM.SEQUENCE rows");
                                    seqTable.batch(mutations);
                                    mutations.clear();
                                    sizeBytes = 0;
                                    committed = true;
                                }
                            }
                        }
                        if (!mutations.isEmpty()) {
                            logger.info("Committing last bactch of SYSTEM.SEQUENCE rows");
                            seqTable.batch(mutations);
                        }
                        preSplitSequenceTable(conn, nSaltBuckets);
                        logger.info("Successfully completed upgrade of SYSTEM.SEQUENCE");
                        success = true;
                        return true;
                    } catch (InterruptedException e) {
                        throw ServerUtil.parseServerException(e);
                    } finally {
                        try {
                            scanner.close();
                        } finally {
                            if (!success) {
                                if (!committed) { // Try to recover by setting salting back to off, as we haven't successfully committed anything
                                    // Don't use Delete here as we'd never be able to change it again at this timestamp.
                                    KeyValue unsaltKV = KeyValueUtil.newKeyValue(seqTableKey, 
                                            PhoenixDatabaseMetaData.TABLE_FAMILY_BYTES,
                                            PhoenixDatabaseMetaData.SALT_BUCKETS_BYTES,
                                            MetaDataProtocol.MIN_SYSTEM_TABLE_TIMESTAMP,
                                            PInteger.INSTANCE.toBytes(0));
                                    Put unsaltPut = new Put(seqTableKey);
                                    unsaltPut.add(unsaltKV);
                                    try {
                                        sysTable.put(unsaltPut);
                                        success = true;
                                    } finally {
                                        if (!success) logger.error("SYSTEM.SEQUENCE TABLE LEFT IN CORRUPT STATE");
                                    }
                                } else { // We're screwed b/c we've already committed some salted sequences...
                                    logger.error("SYSTEM.SEQUENCE TABLE LEFT IN CORRUPT STATE");
                                }
                            }
                        }
                    }
                } catch (IOException e) {
                    throw ServerUtil.parseServerException(e);
                } finally {
                    try {
                        seqTable.close();
                    } catch (IOException e) {
                        logger.warn("Exception during close",e);
                    }
                }
            }
            return false;
        } catch (IOException e) {
            throw ServerUtil.parseServerException(e);
        } finally {
            try {
                sysTable.close();
            } catch (IOException e) {
                logger.warn("Exception during close",e);
            }
        }
        
    }
    
    @SuppressWarnings("deprecation")
    private static KeyValue addSaltByte(KeyValue keyValue, int nSaltBuckets) {
        byte[] buf = keyValue.getBuffer();
        int length = keyValue.getRowLength();
        int offset = keyValue.getRowOffset();
        boolean isViewSeq = length > SEQ_PREFIX_BYTES.length && Bytes.compareTo(SEQ_PREFIX_BYTES, 0, SEQ_PREFIX_BYTES.length, buf, offset, SEQ_PREFIX_BYTES.length) == 0;
        if (!isViewSeq && nSaltBuckets == 0) {
            return null;
        }
        byte[] newBuf;
        if (isViewSeq) { // We messed up the name for the sequences for view indexes so we'll take this opportunity to fix it
            if (buf[length-1] == 0) { // Global indexes on views have trailing null byte
                length--;
            }
            byte[][] rowKeyMetaData = new byte[3][];
            SchemaUtil.getVarChars(buf, offset, length, 0, rowKeyMetaData);
            byte[] schemaName = rowKeyMetaData[PhoenixDatabaseMetaData.SCHEMA_NAME_INDEX];
            byte[] unprefixedSchemaName = new byte[schemaName.length - MetaDataUtil.VIEW_INDEX_SEQUENCE_PREFIX_BYTES.length];
            System.arraycopy(schemaName, MetaDataUtil.VIEW_INDEX_SEQUENCE_PREFIX_BYTES.length, unprefixedSchemaName, 0, unprefixedSchemaName.length);
            byte[] tableName = rowKeyMetaData[PhoenixDatabaseMetaData.TABLE_NAME_INDEX];
            PName physicalName = PNameFactory.newName(unprefixedSchemaName);
            // Reformulate key based on correct data
            newBuf = MetaDataUtil.getViewIndexSequenceKey(tableName == null ? null : Bytes.toString(tableName),
                    physicalName, nSaltBuckets, false).getKey();
        } else {
            newBuf = new byte[length + 1];
            System.arraycopy(buf, offset, newBuf, SaltingUtil.NUM_SALTING_BYTES, length);
            newBuf[0] = SaltingUtil.getSaltingByte(newBuf, SaltingUtil.NUM_SALTING_BYTES, length, nSaltBuckets);
        }
        return new KeyValue(newBuf, 0, newBuf.length,
                buf, keyValue.getFamilyOffset(), keyValue.getFamilyLength(),
                buf, keyValue.getQualifierOffset(), keyValue.getQualifierLength(),
                keyValue.getTimestamp(), KeyValue.Type.codeToType(keyValue.getType()),
                buf, keyValue.getValueOffset(), keyValue.getValueLength());
    }
    
    /**
     * Upgrade the metadata in the catalog table to enable adding columns to tables with views
     * @param oldMetaConnection caller should take care of closing the passed connection appropriately
     * @throws SQLException
     */
    public static void upgradeTo4_5_0(PhoenixConnection oldMetaConnection) throws SQLException {
        PhoenixConnection metaConnection = null;
        try {
            // Need to use own connection with max time stamp to be able to read all data from SYSTEM.CATALOG 
            metaConnection = new PhoenixConnection(oldMetaConnection, HConstants.LATEST_TIMESTAMP);
            logger.info("Upgrading metadata to support adding columns to tables with views");
            String getBaseTableAndViews = "SELECT "
                    + COLUMN_FAMILY + " AS BASE_PHYSICAL_TABLE, "
                    + TENANT_ID + ", "
                    + TABLE_SCHEM + " AS VIEW_SCHEMA, "
                    + TABLE_NAME + " AS VIEW_NAME "
                    + "FROM " + SYSTEM_CATALOG_NAME 
                    + " WHERE " + COLUMN_FAMILY + " IS NOT NULL " // column_family column points to the physical table name.
                    + " AND " + COLUMN_NAME + " IS NULL "
                    + " AND " + LINK_TYPE + " = ? ";
            // Build a map of base table name -> list of views on the table. 
            Map<String, List<ViewKey>> parentTableViewsMap = new HashMap<>();
            try (PreparedStatement stmt = metaConnection.prepareStatement(getBaseTableAndViews)) {
                // Get back view rows that have links back to the base physical table. This takes care
                // of cases when we have a hierarchy of views too.
                stmt.setByte(1, LinkType.PHYSICAL_TABLE.getSerializedValue());
                try (ResultSet rs = stmt.executeQuery()) {
                    while (rs.next()) {
                        // this is actually SCHEMANAME.TABLENAME
                        String parentTable = rs.getString("BASE_PHYSICAL_TABLE");
                        String tenantId = rs.getString(TENANT_ID);
                        String viewSchema = rs.getString("VIEW_SCHEMA");
                        String viewName = rs.getString("VIEW_NAME");
                        List<ViewKey> viewKeysList = parentTableViewsMap.get(parentTable);
                        if (viewKeysList == null) {
                            viewKeysList = new ArrayList<>();
                            parentTableViewsMap.put(parentTable, viewKeysList);
                        }
                        viewKeysList.add(new ViewKey(tenantId, viewSchema, viewName));
                    }
                }
            }
            boolean clearCache = false;
            for (Entry<String, List<ViewKey>> entry : parentTableViewsMap.entrySet()) {
                // Fetch column information for the base physical table
                String physicalTable = entry.getKey();
                String baseTableSchemaName = SchemaUtil.getSchemaNameFromFullName(physicalTable).equals(StringUtil.EMPTY_STRING) ? null : SchemaUtil.getSchemaNameFromFullName(physicalTable);
                String baseTableName = SchemaUtil.getTableNameFromFullName(physicalTable);
                List<ColumnDetails> basePhysicalTableColumns = new ArrayList<>(); 

                // Columns fetched in order of ordinal position
                String fetchColumnInfoForBasePhysicalTable = "SELECT " +
                        COLUMN_NAME + "," +
                        COLUMN_FAMILY + "," +
                        DATA_TYPE + "," +
                        COLUMN_SIZE + "," +
                        DECIMAL_DIGITS + "," +
                        ORDINAL_POSITION + "," +
                        SORT_ORDER + "," +
                        ARRAY_SIZE + " " +
                        "FROM SYSTEM.CATALOG " +
                        "WHERE " +
                        "TABLE_SCHEM %s " +
                        "AND TABLE_NAME = ? " +
                        "AND COLUMN_NAME IS NOT NULL " +
                        "ORDER BY " + 
                        ORDINAL_POSITION;

                PreparedStatement stmt = null;
                if (baseTableSchemaName == null) {
                    fetchColumnInfoForBasePhysicalTable =
                            String.format(fetchColumnInfoForBasePhysicalTable, "IS NULL ");
                    stmt = metaConnection.prepareStatement(fetchColumnInfoForBasePhysicalTable);
                    stmt.setString(1, baseTableName);
                } else {
                    fetchColumnInfoForBasePhysicalTable =
                            String.format(fetchColumnInfoForBasePhysicalTable, " = ? ");
                    stmt = metaConnection.prepareStatement(fetchColumnInfoForBasePhysicalTable);
                    stmt.setString(1, baseTableSchemaName);
                    stmt.setString(2, baseTableName);
                }

                try (ResultSet rs = stmt.executeQuery()) {
                    while (rs.next()) {
                        basePhysicalTableColumns.add(new ColumnDetails(rs.getString(COLUMN_FAMILY), rs
                                .getString(COLUMN_NAME), rs.getInt(ORDINAL_POSITION), rs
                                .getInt(DATA_TYPE), rs.getInt(COLUMN_SIZE), rs.getInt(DECIMAL_DIGITS),
                                rs.getInt(SORT_ORDER), rs.getInt(ARRAY_SIZE)));
                    }
                }

                // Fetch column information for all the views on the base physical table ordered by ordinal position.
                List<ViewKey> viewKeys = entry.getValue();
                StringBuilder sb = new StringBuilder();
                sb.append("SELECT " + 
                        TENANT_ID + "," +
                        TABLE_SCHEM + "," +
                        TABLE_NAME + "," +
                        COLUMN_NAME + "," +
                        COLUMN_FAMILY + "," +
                        DATA_TYPE + "," +
                        COLUMN_SIZE + "," +
                        DECIMAL_DIGITS + "," +
                        ORDINAL_POSITION + "," +
                        SORT_ORDER + "," +
                        ARRAY_SIZE + " " + 
                        "FROM SYSTEM.CATALOG " +
                        "WHERE " +
                        COLUMN_NAME + " IS NOT NULL " +
                        "AND " +
                        ORDINAL_POSITION + " <= ? " + // fetch only those columns that would impact setting of base column count
                        "AND " +
                        "(" + TENANT_ID+ ", " + TABLE_SCHEM + ", " + TABLE_NAME + ") IN (");

                int numViews = viewKeys.size();
                for (int i = 0; i < numViews; i++) {
                    sb.append(" (?, ?, ?) ");
                    if (i < numViews - 1) {
                        sb.append(", ");
                    }
                }
                sb.append(" ) ");
                sb.append(" GROUP BY " +
                        TENANT_ID + "," +
                        TABLE_SCHEM + "," +
                        TABLE_NAME + "," +
                        COLUMN_NAME + "," +
                        COLUMN_FAMILY + "," +
                        DATA_TYPE + "," +
                        COLUMN_SIZE + "," +
                        DECIMAL_DIGITS + "," +
                        ORDINAL_POSITION + "," +
                        SORT_ORDER + "," +
                        ARRAY_SIZE + " " + 
                        "ORDER BY " + 
                        TENANT_ID + "," + TABLE_SCHEM + ", " + TABLE_NAME + ", " + ORDINAL_POSITION);
                String fetchViewColumnsSql = sb.toString();
                stmt = metaConnection.prepareStatement(fetchViewColumnsSql);
                int numColsInBaseTable = basePhysicalTableColumns.size();
                stmt.setInt(1, numColsInBaseTable);
                int paramIndex = 1;
                stmt.setInt(paramIndex++, numColsInBaseTable);
                for (ViewKey view : viewKeys) {
                    stmt.setString(paramIndex++, view.tenantId);
                    stmt.setString(paramIndex++, view.schema);
                    stmt.setString(paramIndex++, view.name);
                }
                String currentTenantId = null;
                String currentViewSchema = null;
                String currentViewName = null;
                try (ResultSet rs = stmt.executeQuery()) {
                    int numBaseTableColsMatched = 0;
                    boolean ignore = false;
                    boolean baseColumnCountUpserted = false;
                    while (rs.next()) {
                        String viewTenantId = rs.getString(TENANT_ID);
                        String viewSchema = rs.getString(TABLE_SCHEM);
                        String viewName = rs.getString(TABLE_NAME);
                        if (!(Objects.equal(viewTenantId, currentTenantId) && Objects.equal(viewSchema, currentViewSchema) && Objects.equal(viewName, currentViewName))) {
                            // We are about to iterate through columns of a different view. Check whether base column count was upserted.
                            // If it wasn't then it is likely the case that a column inherited from the base table was dropped from view.
                            if (currentViewName != null && !baseColumnCountUpserted && numBaseTableColsMatched < numColsInBaseTable) {
                                upsertBaseColumnCountInHeaderRow(metaConnection, currentTenantId, currentViewSchema, currentViewName, DIVERGED_VIEW_BASE_COLUMN_COUNT);
                                clearCache = true;
                            }
                            // reset the values as we are now going to iterate over columns of a new view.
                            numBaseTableColsMatched = 0;
                            currentTenantId = viewTenantId;
                            currentViewSchema = viewSchema;
                            currentViewName = viewName;
                            ignore = false;
                            baseColumnCountUpserted = false;
                        }
                        if (!ignore) {
                            /*
                             * Iterate over all the columns of the base physical table and the columns of the view. Compare the
                             * two till one of the following happens: 
                             * 
                             * 1) We run into a view column which is different from column in the base physical table.
                             * This means that the view has diverged from the base physical table. In such a case
                             * we will set a special value for the base column count. That special value will also be used
                             * on the server side to filter out the diverged view so that meta-data changes on the base 
                             * physical table are not propagated to it.
                             * 
                             * 2) Every physical table column is present in the view. In that case we set the base column count
                             * as the number of columns in the base physical table. At that point we ignore rest of the columns
                             * of the view.
                             * 
                             */
                            ColumnDetails baseTableColumn = basePhysicalTableColumns.get(numBaseTableColsMatched);
                            String columName = rs.getString(COLUMN_NAME);
                            String columnFamily = rs.getString(COLUMN_FAMILY);
                            int ordinalPos = rs.getInt(ORDINAL_POSITION);
                            int dataType = rs.getInt(DATA_TYPE);
                            int columnSize = rs.getInt(COLUMN_SIZE);
                            int decimalDigits = rs.getInt(DECIMAL_DIGITS);
                            int sortOrder = rs.getInt(SORT_ORDER);
                            int arraySize = rs.getInt(ARRAY_SIZE);
                            ColumnDetails viewColumn = new ColumnDetails(columnFamily, columName, ordinalPos, dataType, columnSize, decimalDigits, sortOrder, arraySize);
                            if (baseTableColumn.equals(viewColumn)) {
                                numBaseTableColsMatched++;
                                if (numBaseTableColsMatched == numColsInBaseTable) {
                                    upsertBaseColumnCountInHeaderRow(metaConnection, viewTenantId, viewSchema, viewName, numColsInBaseTable);
                                    // No need to ignore the rest of the columns of the view here since the
                                    // query retrieved only those columns that had ordinal position <= numColsInBaseTable
                                    baseColumnCountUpserted = true;
                                    clearCache = true;
                                }
                            } else {
                                // special value to denote that the view has diverged from the base physical table.
                                upsertBaseColumnCountInHeaderRow(metaConnection, viewTenantId, viewSchema, viewName, DIVERGED_VIEW_BASE_COLUMN_COUNT);
                                baseColumnCountUpserted = true;
                                clearCache = true;
                                // ignore rest of the rows for the view.
                                ignore = true;
                            }
                        }
                    }
                }
                // set base column count for the header row of the base table too. We use this information
                // to figure out whether the upgrade is in progress or hasn't started.
                upsertBaseColumnCountInHeaderRow(metaConnection, null, baseTableSchemaName, baseTableName, BASE_TABLE_BASE_COLUMN_COUNT);
                metaConnection.commit();
            }
            // clear metadata cache on region servers to force loading of the latest metadata
            if (clearCache) {
                metaConnection.getQueryServices().clearCache();
            }
        } finally {
            if (metaConnection != null) {
                metaConnection.close();
            }
        }
    }
    
    private static void upsertBaseColumnCountInHeaderRow(PhoenixConnection metaConnection,
            String tenantId, String schemaName, String viewOrTableName, int baseColumnCount)
            throws SQLException {
        try (PreparedStatement stmt =
                metaConnection.prepareStatement(UPSERT_BASE_COLUMN_COUNT_IN_HEADER_ROW)) {
            stmt.setString(1, tenantId);
            stmt.setString(2, schemaName);
            stmt.setString(3, viewOrTableName);
            stmt.setString(4, null);
            stmt.setString(5, null);
            stmt.setInt(6, baseColumnCount);
            stmt.executeUpdate();
        }
    }
    
    private static class ColumnDetails {

        @Override
        public int hashCode() {
            final int prime = 31;
            int result = 1;
            result = prime * result + columnName.hashCode();
            result = prime * result + ((columnFamily == null) ? 0 : columnFamily.hashCode());
            result = prime * result + arraySize;
            result = prime * result + dataType;
            result = prime * result + maxLength;
            result = prime * result + ordinalValue;
            result = prime * result + scale;
            result = prime * result + sortOrder;
            return result;
        }

        @Override
        public boolean equals(Object obj) {
            if (this == obj) return true;
            if (obj == null) return false;
            if (getClass() != obj.getClass()) return false;
            ColumnDetails other = (ColumnDetails) obj;
            if (!columnName.equals(other.columnName)) return false;
            if (columnFamily == null) {
                if (other.columnFamily != null) return false;
            } else if (!columnFamily.equals(other.columnFamily)) return false;
            if (arraySize != other.arraySize) return false;
            if (dataType != other.dataType) return false;
            if (maxLength != other.maxLength) return false;
            if (ordinalValue != other.ordinalValue) return false;
            if (scale != other.scale) return false;
            if (sortOrder != other.sortOrder) return false;
            return true;
        }

        @Nullable
        private final String columnFamily;

        @Nonnull
        private final String columnName;

        private final int ordinalValue;

        private final int dataType;

        private final int maxLength;

        private final int scale;

        private final int sortOrder;

        private final int arraySize;

        ColumnDetails(String columnFamily, String columnName, int ordinalValue, int dataType,
                int maxLength, int scale, int sortOrder, int arraySize) {
            checkNotNull(columnName);
            checkNotNull(ordinalValue);
            checkNotNull(dataType);
            this.columnFamily = columnFamily;
            this.columnName = columnName;
            this.ordinalValue = ordinalValue;
            this.dataType = dataType;
            this.maxLength = maxLength;
            this.scale = scale;
            this.sortOrder = sortOrder;
            this.arraySize = arraySize;
        }

    }
    
    private static class ViewKey {

        @Override
        public int hashCode() {
            final int prime = 31;
            int result = 1;
            result = prime * result + ((tenantId == null) ? 0 : tenantId.hashCode());
            result = prime * result + name.hashCode();
            result = prime * result + ((schema == null) ? 0 : schema.hashCode());
            return result;
        }

        @Override
        public boolean equals(Object obj) {
            if (this == obj) return true;
            if (obj == null) return false;
            if (getClass() != obj.getClass()) return false;
            ViewKey other = (ViewKey) obj;
            if (tenantId == null) {
                if (other.tenantId != null) return false;
            } else if (!tenantId.equals(other.tenantId)) return false;
            if (!name.equals(other.name)) return false;
            if (schema == null) {
                if (other.schema != null) return false;
            } else if (!schema.equals(other.schema)) return false;
            return true;
        }

        @Nullable
        private final String tenantId;

        @Nullable
        private final String schema;

        @Nonnull
        private final String name;

        private ViewKey(String tenantId, String schema, String viewName) {
            this.tenantId = tenantId;
            this.schema = schema;
            this.name = viewName;
        }
    }

    private static String getTableRVC(List<String> tableNames) {
        StringBuilder query = new StringBuilder("(");
        for (int i = 0; i < tableNames.size(); i+=3) {
            String tenantId = tableNames.get(i);
            String schemaName = tableNames.get(i+1);
            String tableName = tableNames.get(i+2);
            query.append('(');
            query.append(tenantId == null ? "null" : ("'" + tenantId + "'"));
            query.append(',');
            query.append(schemaName == null ? "null" : ("'" + schemaName + "'"));
            query.append(',');
            query.append("'" + tableName + "'");
            query.append("),");
        }
        // Replace trailing , with ) to end IN expression
        query.setCharAt(query.length()-1, ')');
        return query.toString();
    }
    
    private static List<String> addPhysicalTables(PhoenixConnection conn, ResultSet rs, PTableType otherType, Set<String> physicalTables) throws SQLException {
        List<String> tableNames = Lists.newArrayListWithExpectedSize(1024);
        while (rs.next()) {
            tableNames.add(rs.getString(1));
            tableNames.add(rs.getString(2));
            tableNames.add(rs.getString(3));
        }
        if (tableNames.isEmpty()) {
            return Collections.emptyList();
        }
        
        List<String> otherTables = Lists.newArrayListWithExpectedSize(tableNames.size());
        // Find the header rows for tables that have not been upgraded already.
        // We don't care about views, as the row key cannot be different than the table.
        // We need this query to find physical tables which won't have a link row.
        String query = "SELECT TENANT_ID,TABLE_SCHEM,TABLE_NAME,TABLE_TYPE\n" + 
                "FROM SYSTEM.CATALOG (ROW_KEY_ORDER_OPTIMIZABLE BOOLEAN)\n" + 
                "WHERE COLUMN_NAME IS NULL\n" + 
                "AND COLUMN_FAMILY IS NULL\n" + 
                "AND ROW_KEY_ORDER_OPTIMIZABLE IS NULL\n" +
                "AND TABLE_TYPE IN ('" + PTableType.TABLE.getSerializedValue() + "','" + otherType.getSerializedValue() + "')\n" +
                "AND (TENANT_ID, TABLE_SCHEM, TABLE_NAME) IN " + getTableRVC(tableNames);
        rs = conn.createStatement().executeQuery(query);
        
        while (rs.next()) {
            if (PTableType.TABLE.getSerializedValue().equals(rs.getString(4))) {
                physicalTables.add(SchemaUtil.getTableName(rs.getString(2), rs.getString(3)));
            } else {
                otherTables.add(rs.getString(1));
                otherTables.add(rs.getString(2));
                otherTables.add(rs.getString(3));
            }
        }
        return otherTables;
    }
    
    // Return all types that are descending and either:
    // 1) variable length, which includes all array types (PHOENIX-2067)
    // 2) fixed length with padding (PHOENIX-2120)
    // 3) float and double (PHOENIX-2171)
    // We exclude VARBINARY as we no longer support DESC for it.
    private static String getAffectedDataTypes() {
        StringBuilder buf = new StringBuilder("(" 
                + PVarchar.INSTANCE.getSqlType() + "," +
                + PChar.INSTANCE.getSqlType() + "," +
                + PBinary.INSTANCE.getSqlType() + "," +
                + PFloat.INSTANCE.getSqlType() + "," +
                + PDouble.INSTANCE.getSqlType() + "," +
                + PDecimal.INSTANCE.getSqlType() + ","
                );
        for (PDataType type : PDataType.values()) {
            if (type.isArrayType()) {
                buf.append(type.getSqlType());
                buf.append(',');
            }
        }
        buf.setCharAt(buf.length()-1, ')');
        return buf.toString();
    }
    
    
    /**
     * Identify the tables that are DESC VARBINARY as this is no longer supported
     */
    public static List<String> getPhysicalTablesWithDescVarbinaryRowKey(PhoenixConnection conn) throws SQLException {
        String query = "SELECT TENANT_ID,TABLE_SCHEM,TABLE_NAME\n" + 
                "FROM SYSTEM.CATALOG cat1\n" + 
                "WHERE COLUMN_NAME IS NOT NULL\n" + 
                "AND COLUMN_FAMILY IS NULL\n" + 
                "AND SORT_ORDER = " + SortOrder.DESC.getSystemValue() + "\n" + 
                "AND DATA_TYPE = " + PVarbinary.INSTANCE.getSqlType() + "\n" +
                "GROUP BY TENANT_ID,TABLE_SCHEM,TABLE_NAME";
        return getPhysicalTablesWithDescRowKey(query, conn);
    }
    
    /**
     * Identify the tables that need to be upgraded due to PHOENIX-2067 and PHOENIX-2120
     */
    public static List<String> getPhysicalTablesWithDescRowKey(PhoenixConnection conn) throws SQLException {
        String query = "SELECT TENANT_ID,TABLE_SCHEM,TABLE_NAME\n" + 
                "FROM SYSTEM.CATALOG cat1\n" + 
                "WHERE COLUMN_NAME IS NOT NULL\n" + 
                "AND COLUMN_FAMILY IS NULL\n" + 
                "AND ( ( SORT_ORDER = " + SortOrder.DESC.getSystemValue() + "\n" + 
                "        AND DATA_TYPE IN " + getAffectedDataTypes() + ")\n" +
                "    OR ( SORT_ORDER = " + SortOrder.ASC.getSystemValue() + "\n" + 
                "         AND DATA_TYPE = " + PBinary.INSTANCE.getSqlType() + "\n" +
                "         AND COLUMN_SIZE > 1 ) )\n" +
                "GROUP BY TENANT_ID,TABLE_SCHEM,TABLE_NAME";
        return getPhysicalTablesWithDescRowKey(query, conn);
    }
    
    /**
     * Identify the tables that need to be upgraded due to PHOENIX-2067
     */
    private static List<String> getPhysicalTablesWithDescRowKey(String query, PhoenixConnection conn) throws SQLException {
        // First query finds column rows of tables that need to be upgraded.
        // We cannot tell if the column is from a table, view, or index however.
        ResultSet rs = conn.createStatement().executeQuery(query);
        Set<String> physicalTables = Sets.newHashSetWithExpectedSize(1024);
        List<String> remainingTableNames = addPhysicalTables(conn, rs, PTableType.INDEX, physicalTables);
        if (!remainingTableNames.isEmpty()) {
            // Find tables/views for index
            String indexLinkQuery = "SELECT TENANT_ID,TABLE_SCHEM,TABLE_NAME\n" + 
                    "FROM SYSTEM.CATALOG\n" + 
                    "WHERE COLUMN_NAME IS NULL\n" + 
                    "AND (TENANT_ID, TABLE_SCHEM, COLUMN_FAMILY) IN " + getTableRVC(remainingTableNames) + "\n" +
                    "AND LINK_TYPE = " + LinkType.INDEX_TABLE.getSerializedValue();
             rs = conn.createStatement().executeQuery(indexLinkQuery);
             remainingTableNames = addPhysicalTables(conn, rs, PTableType.VIEW, physicalTables);
             if (!remainingTableNames.isEmpty()) {
                 // Find physical table name from views, splitting on '.' to get schema name and table name
                 String physicalLinkQuery = "SELECT null, " + 
                 " CASE WHEN INSTR(COLUMN_FAMILY,'.') = 0 THEN NULL ELSE SUBSTR(COLUMN_FAMILY,1,INSTR(COLUMN_FAMILY,'.')) END,\n" + 
                 " CASE WHEN INSTR(COLUMN_FAMILY,'.') = 0 THEN COLUMN_FAMILY ELSE SUBSTR(COLUMN_FAMILY,INSTR(COLUMN_FAMILY,'.')+1) END\n" + 
                         "FROM SYSTEM.CATALOG\n" + 
                         "WHERE COLUMN_NAME IS NULL\n" + 
                         "AND COLUMN_FAMILY IS NOT NULL\n" + 
                         "AND (TENANT_ID, TABLE_SCHEM, TABLE_NAME) IN " + getTableRVC(remainingTableNames) + "\n" +
                         "AND LINK_TYPE = " + LinkType.PHYSICAL_TABLE.getSerializedValue();
                 rs = conn.createStatement().executeQuery(physicalLinkQuery);
                 // Add any tables (which will all be physical tables) which have not already been upgraded.
                 addPhysicalTables(conn, rs, PTableType.TABLE, physicalTables);
             }
        }
        List<String> sortedPhysicalTables = new ArrayList<String>(physicalTables);
        Collections.sort(sortedPhysicalTables);
        return sortedPhysicalTables;
    }

    private static void upgradeDescVarLengthRowKeys(PhoenixConnection upgradeConn, PhoenixConnection globalConn, String schemaName, String tableName, boolean isTable, boolean bypassUpgrade) throws SQLException {
        String physicalName = SchemaUtil.getTableName(schemaName, tableName);
        long currentTime = System.currentTimeMillis();
        String snapshotName = physicalName + "_" + currentTime;
        HBaseAdmin admin = null;
        if (isTable && !bypassUpgrade) {
            admin = globalConn.getQueryServices().getAdmin();
        }
        boolean restoreSnapshot = false;
        boolean success = false;
        try {
            if (isTable && !bypassUpgrade) {
                String msg = "Taking snapshot of physical table " + physicalName + " prior to upgrade...";
                System.out.println(msg);
                logger.info(msg);
                admin.disableTable(physicalName);
                admin.snapshot(snapshotName, physicalName);
                admin.enableTable(physicalName);
                restoreSnapshot = true;
            }
            String escapedTableName = SchemaUtil.getEscapedTableName(schemaName, tableName);
            String tenantInfo = "";
            PName tenantId = PName.EMPTY_NAME;
            if (upgradeConn.getTenantId() != null) {
                tenantId = upgradeConn.getTenantId();
                tenantInfo = " for tenant " + tenantId.getString();
            }
            String msg = "Starting upgrade of " + escapedTableName + tenantInfo + "...";
            System.out.println(msg);
            logger.info(msg);
            ResultSet rs;
            if (!bypassUpgrade) {
                rs = upgradeConn.createStatement().executeQuery("SELECT /*+ NO_INDEX */ count(*) FROM " + escapedTableName);
                rs.next(); // Run query
            }
            List<String> tableNames = Lists.newArrayListWithExpectedSize(1024);
            tableNames.add(tenantId == PName.EMPTY_NAME ? null : tenantId.getString());
            tableNames.add(schemaName);
            tableNames.add(tableName);
            // Find views to mark as upgraded
            if (isTable) {
                String query =
                        "SELECT TENANT_ID,TABLE_SCHEM,TABLE_NAME\n" + 
                        "FROM SYSTEM.CATALOG\n" + 
                        "WHERE COLUMN_NAME IS NULL\n" + 
                        "AND COLUMN_FAMILY = '" + physicalName + "'" +
                        "AND LINK_TYPE = " + LinkType.PHYSICAL_TABLE.getSerializedValue();
                rs = globalConn.createStatement().executeQuery(query);
                while (rs.next()) {
                    tableNames.add(rs.getString(1));
                    tableNames.add(rs.getString(2));
                    tableNames.add(rs.getString(3));
                }
            }
            // Mark the table and views as upgraded now
            for (int i = 0; i < tableNames.size(); i += 3) {
                String theTenantId = tableNames.get(i);
                String theSchemaName = tableNames.get(i+1);
                String theTableName = tableNames.get(i+2);
                globalConn.createStatement().execute("UPSERT INTO " + PhoenixDatabaseMetaData.SYSTEM_CATALOG_NAME + 
                    " (" + PhoenixDatabaseMetaData.TENANT_ID + "," +
                           PhoenixDatabaseMetaData.TABLE_SCHEM + "," +
                           PhoenixDatabaseMetaData.TABLE_NAME + "," +
                           MetaDataEndpointImpl.ROW_KEY_ORDER_OPTIMIZABLE + " BOOLEAN"
                   + ") VALUES (" +
                           "'" + (theTenantId == null ? StringUtil.EMPTY_STRING : theTenantId) + "'," +
                           "'" + (theSchemaName == null ? StringUtil.EMPTY_STRING : theSchemaName) + "'," +
                           "'" + theTableName + "'," +
                           "TRUE)");
            }
            globalConn.commit();
            for (int i = 0; i < tableNames.size(); i += 3) {
                String theTenantId = tableNames.get(i);
                String theSchemaName = tableNames.get(i+1);
                String theTableName = tableNames.get(i+2);
                globalConn.getQueryServices().clearTableFromCache(
                        theTenantId == null ? ByteUtil.EMPTY_BYTE_ARRAY : Bytes.toBytes(theTenantId),
                        theSchemaName == null ? ByteUtil.EMPTY_BYTE_ARRAY : Bytes.toBytes(schemaName),
                        Bytes.toBytes(theTableName), HConstants.LATEST_TIMESTAMP);
            }
            success = true;
            msg = "Completed upgrade of " + escapedTableName + tenantInfo;
            System.out.println(msg);
            logger.info(msg);
        } catch (Exception e) {
            logger.error("Exception during upgrade of " + physicalName + ":", e);
        } finally {
            boolean restored = false;
            try {
                if (!success && restoreSnapshot) {
                    admin.disableTable(physicalName);
                    admin.restoreSnapshot(snapshotName, false);
                    admin.enableTable(physicalName);
                    String msg = "Restored snapshot of " + physicalName + " due to failure of upgrade";
                    System.out.println(msg);
                    logger.info(msg);
                }
                restored = true;
            } catch (Exception e) {
                logger.warn("Unable to restoring snapshot " + snapshotName + " after failed upgrade", e);
            } finally {
                try {
                    if (restoreSnapshot && restored) {
                        admin.deleteSnapshot(snapshotName);
                    }
                } catch (Exception e) {
                    logger.warn("Unable to delete snapshot " + snapshotName + " after upgrade:", e);
                } finally {
                    try {
                        if (admin != null) {
                            admin.close();
                        }
                    } catch (IOException e) {
                        logger.warn("Unable to close admin after upgrade:", e);
                    }
                }
            }
        }
    }
    
    private static boolean isInvalidTableToUpgrade(PTable table) throws SQLException {
        return (table.getType() != PTableType.TABLE || // Must be a table
            table.getTenantId() != null || // Must be global
            !table.getPhysicalName().equals(table.getName())); // Must be the physical table
    }
    /**
     * Upgrade tables and their indexes due to a bug causing descending row keys to have a row key that
     * prevents them from being sorted correctly (PHOENIX-2067).
     */
    public static void upgradeDescVarLengthRowKeys(PhoenixConnection conn, List<String> tablesToUpgrade, boolean bypassUpgrade) throws SQLException {
        if (tablesToUpgrade.isEmpty()) {
            return;
        }
        List<PTable> tablesNeedingUpgrading = Lists.newArrayListWithExpectedSize(tablesToUpgrade.size());
        List<String> invalidTables = Lists.newArrayListWithExpectedSize(tablesToUpgrade.size());
        for (String fullTableName : tablesToUpgrade) {
            PTable table = PhoenixRuntime.getTable(conn, fullTableName);
            if (isInvalidTableToUpgrade(table)) {
                invalidTables.add(fullTableName);
            } else {
                tablesNeedingUpgrading.add(table);
            }
        }
        if (!invalidTables.isEmpty()) {
            StringBuilder buf = new StringBuilder("Only physical tables should be upgraded as their views and indexes will be updated with them: ");
            for (String fullTableName : invalidTables) {
                buf.append(fullTableName);
                buf.append(' ');
            }
            throw new SQLException(buf.toString());
        }
        PhoenixConnection upgradeConn = new PhoenixConnection(conn, true, true);
        try {
            upgradeConn.setAutoCommit(true);
            for (PTable table : tablesNeedingUpgrading) {
                boolean wasUpgraded = false;
                if (!table.rowKeyOrderOptimizable()) {
                    wasUpgraded = true;
                    upgradeDescVarLengthRowKeys(upgradeConn, conn, table.getSchemaName().getString(), table.getTableName().getString(), true, bypassUpgrade);
                }
                
                // Upgrade global indexes
                for (PTable index : table.getIndexes()) {
                    if (!index.rowKeyOrderOptimizable() && index.getIndexType() != IndexType.LOCAL) {
                        wasUpgraded = true;
                        upgradeDescVarLengthRowKeys(upgradeConn, conn, index.getSchemaName().getString(), index.getTableName().getString(), false, bypassUpgrade);
                    }
                }
                
                String sharedViewIndexName = Bytes.toString(MetaDataUtil.getViewIndexPhysicalName(table.getName().getBytes()));
                // Upgrade view indexes
                wasUpgraded |= upgradeSharedIndex(upgradeConn, conn, sharedViewIndexName, bypassUpgrade);
                String sharedLocalIndexName = Bytes.toString(MetaDataUtil.getLocalIndexPhysicalName(table.getName().getBytes()));
                // Upgrade local indexes
                wasUpgraded |= upgradeSharedIndex(upgradeConn, conn, sharedLocalIndexName, bypassUpgrade);
                
                if (!wasUpgraded) {
                    System.out.println("Upgrade not required for this table or its indexes: " + table.getName().getString());
                }
            }
        } finally {
            upgradeConn.close();
        }
    }
    
    /**
     * Upgrade shared indexes by querying for all that are associated with our
     * physical table.
     * @return true if any upgrades were performed and false otherwise.
     */
    private static boolean upgradeSharedIndex(PhoenixConnection upgradeConn, PhoenixConnection globalConn, String physicalName, boolean bypassUpgrade) throws SQLException {
        String query =
                "SELECT TENANT_ID,TABLE_SCHEM,TABLE_NAME\n" + 
                "FROM SYSTEM.CATALOG cat1\n" + 
                "WHERE COLUMN_NAME IS NULL\n" + 
                "AND COLUMN_FAMILY = '" + physicalName + "'\n" + 
                "AND LINK_TYPE = " + LinkType.PHYSICAL_TABLE.getSerializedValue() + "\n" +
                "ORDER BY TENANT_ID";
        ResultSet rs = globalConn.createStatement().executeQuery(query);
        String lastTenantId = null;
        Connection conn = globalConn;
        String url = globalConn.getURL();
        boolean wasUpgraded = false;
        while (rs.next()) {
            String fullTableName = SchemaUtil.getTableName(
                    rs.getString(PhoenixDatabaseMetaData.TABLE_SCHEM),
                    rs.getString(PhoenixDatabaseMetaData.TABLE_NAME));
            String tenantId = rs.getString(1);
            if (tenantId != null && !tenantId.equals(lastTenantId))  {
                if (lastTenantId != null) {
                    conn.close();
                }
                // Open tenant-specific connection when we find a new one
                Properties props = new Properties(globalConn.getClientInfo());
                props.setProperty(PhoenixRuntime.TENANT_ID_ATTRIB, tenantId);
                conn = DriverManager.getConnection(url, props);
                lastTenantId = tenantId;
            }
            PTable table = PhoenixRuntime.getTable(conn, fullTableName);
            String tableTenantId = table.getTenantId() == null ? null : table.getTenantId().getString();
            if (Objects.equal(lastTenantId, tableTenantId) && !table.rowKeyOrderOptimizable()) {
                upgradeDescVarLengthRowKeys(upgradeConn, globalConn, table.getSchemaName().getString(), table.getTableName().getString(), false, bypassUpgrade);
                wasUpgraded = true;
            }
        }
        rs.close();
        if (lastTenantId != null) {
            conn.close();
        }
        return wasUpgraded;
    }

    public static void addRowKeyOrderOptimizableCell(List<Mutation> tableMetadata, byte[] tableHeaderRowKey, long clientTimeStamp) {
        Put put = new Put(tableHeaderRowKey, clientTimeStamp);
        put.addColumn(PhoenixDatabaseMetaData.TABLE_FAMILY_BYTES,
                MetaDataEndpointImpl.ROW_KEY_ORDER_OPTIMIZABLE_BYTES, PBoolean.INSTANCE.toBytes(true));
        tableMetadata.add(put);
    }

    public static boolean truncateStats(HTableInterface metaTable, HTableInterface statsTable)
            throws IOException, InterruptedException {
        byte[] statsTableKey = SchemaUtil.getTableKey(null, PhoenixDatabaseMetaData.SYSTEM_SCHEMA_NAME,
                PhoenixDatabaseMetaData.SYSTEM_STATS_TABLE);
        List<Cell> columnCells = metaTable.get(new Get(statsTableKey))
                .getColumnCells(PhoenixDatabaseMetaData.TABLE_FAMILY_BYTES, QueryConstants.EMPTY_COLUMN_BYTES);
        long timestamp;
        if (!columnCells.isEmpty() && (timestamp = columnCells.get(0)
                .getTimestamp()) < MetaDataProtocol.MIN_SYSTEM_TABLE_TIMESTAMP_4_7_0) {

            KeyValue upgradeKV = KeyValueUtil.newKeyValue(statsTableKey, PhoenixDatabaseMetaData.TABLE_FAMILY_BYTES,
                    UPGRADE_TO_4_7_COLUMN_NAME, timestamp, PBoolean.INSTANCE.toBytes(true));
            Put upgradePut = new Put(statsTableKey);
            upgradePut.add(upgradeKV);

            // check for null in UPGRADE_TO_4_7_COLUMN_NAME in checkAndPut so that only single client
            // drop the rows of SYSTEM.STATS
            if (metaTable.checkAndPut(statsTableKey, PhoenixDatabaseMetaData.TABLE_FAMILY_BYTES,
                    UPGRADE_TO_4_7_COLUMN_NAME, null, upgradePut)) {
                List<Mutation> mutations = Lists.newArrayListWithExpectedSize(1000);
                Scan scan = new Scan();
                scan.setRaw(true);
                scan.setMaxVersions();
                ResultScanner statsScanner = statsTable.getScanner(scan);
                Result r;
                mutations.clear();
                int count = 0;
                while ((r = statsScanner.next()) != null) {
                    Delete delete = null;
                    for (KeyValue keyValue : r.raw()) {
                        if (KeyValue.Type.codeToType(keyValue.getType()) == KeyValue.Type.Put) {
                            if (delete == null) {
                                delete = new Delete(keyValue.getRow());
                            }
                            KeyValue deleteKeyValue = new KeyValue(keyValue.getRowArray(), keyValue.getRowOffset(),
                                    keyValue.getRowLength(), keyValue.getFamilyArray(), keyValue.getFamilyOffset(),
                                    keyValue.getFamilyLength(), keyValue.getQualifierArray(),
                                    keyValue.getQualifierOffset(), keyValue.getQualifierLength(),
                                    keyValue.getTimestamp(), KeyValue.Type.Delete, ByteUtil.EMPTY_BYTE_ARRAY, 0, 0);
                            delete.addDeleteMarker(deleteKeyValue);
                        }
                    }
                    if (delete != null) {
                        mutations.add(delete);
                        if (count > 10) {
                            statsTable.batch(mutations);
                            mutations.clear();
                            count = 0;
                        }
                        count++;
                    }
                }
                if (!mutations.isEmpty()) {
                    statsTable.batch(mutations);
                }
                return true;
            }
        }
        return false;
    }

    private static void mapTableToNamespace(HBaseAdmin admin, HTableInterface metatable, String srcTableName,
            String destTableName, ReadOnlyProps props, Long ts, String phoenixTableName, PTableType pTableType,PName tenantId)
                    throws SnapshotCreationException, IllegalArgumentException, IOException, InterruptedException,
                    SQLException {
        srcTableName = SchemaUtil.normalizeIdentifier(srcTableName);
        if (!SchemaUtil.isNamespaceMappingEnabled(pTableType,
                props)) { throw new IllegalArgumentException(SchemaUtil.isSystemTable(srcTableName.getBytes())
                        ? "For system table " + QueryServices.IS_SYSTEM_TABLE_MAPPED_TO_NAMESPACE
                                + " also needs to be enabled along with " + QueryServices.IS_NAMESPACE_MAPPING_ENABLED
                        : QueryServices.IS_NAMESPACE_MAPPING_ENABLED + " is not enabled"); }
        boolean srcTableExists=admin.tableExists(srcTableName);
        // we need to move physical table in actual namespace for TABLE and Index
        if (srcTableExists && (PTableType.TABLE.equals(pTableType)
                || PTableType.INDEX.equals(pTableType) || PTableType.SYSTEM.equals(pTableType))) {
            boolean destTableExists=admin.tableExists(destTableName);
            if (!destTableExists) {
                String snapshotName = QueryConstants.UPGRADE_TABLE_SNAPSHOT_PREFIX + srcTableName;
                logger.info("Disabling table " + srcTableName + " ..");
                admin.disableTable(srcTableName);
                logger.info(String.format("Taking snapshot %s of table %s..", snapshotName, srcTableName));
                admin.snapshot(snapshotName, srcTableName);
                logger.info(
                        String.format("Restoring snapshot %s in destination table %s..", snapshotName, destTableName));
                admin.cloneSnapshot(Bytes.toBytes(snapshotName), Bytes.toBytes(destTableName));
                logger.info(String.format("deleting old table %s..", srcTableName));
                admin.deleteTable(srcTableName);
                logger.info(String.format("deleting snapshot %s..", snapshotName));
                admin.deleteSnapshot(snapshotName);
            }
        }

        byte[] tableKey = SchemaUtil.getTableKey(tenantId != null ? tenantId.getString() : null,
                SchemaUtil.getSchemaNameFromFullName(phoenixTableName),
                SchemaUtil.getTableNameFromFullName(phoenixTableName));
        List<Cell> columnCells = metatable.get(new Get(tableKey))
                .getColumnCells(PhoenixDatabaseMetaData.TABLE_FAMILY_BYTES, QueryConstants.EMPTY_COLUMN_BYTES);
        if (ts == null) {
            if (!columnCells.isEmpty()) {
                ts = columnCells.get(0).getTimestamp();
            } else if (PTableType.SYSTEM != pTableType) { throw new IllegalArgumentException(
                    "Timestamp passed is null and cannot derive timestamp for " + tableKey + " from meta table!!"); }
        }
        if (ts != null) {
            // Update flag to represent table is mapped to namespace
            logger.info(String.format("Updating meta information of phoenix table '%s' to map to namespace..",
                    phoenixTableName));
            Put put = new Put(tableKey, ts);
            put.addColumn(QueryConstants.DEFAULT_COLUMN_FAMILY_BYTES, PhoenixDatabaseMetaData.IS_NAMESPACE_MAPPED_BYTES,
                    PBoolean.INSTANCE.toBytes(Boolean.TRUE));
            metatable.put(put);
        }
    }

    /*
     * Method to map existing phoenix table to a namespace. Should not be use if tables has views and indexes ,instead
     * use map table utility in psql.py
     */
    public static void mapTableToNamespace(HBaseAdmin admin, HTableInterface metatable, String tableName,
            ReadOnlyProps props, Long ts, PTableType pTableType, PName tenantId) throws SnapshotCreationException,
                    IllegalArgumentException, IOException, InterruptedException, SQLException {
        String destTablename = SchemaUtil
                .normalizeIdentifier(SchemaUtil.getPhysicalTableName(tableName, props).getNameAsString());
        mapTableToNamespace(admin, metatable, tableName, destTablename, props, ts, tableName, pTableType, tenantId);
    }

    public static void upgradeTable(PhoenixConnection conn, String srcTable) throws SQLException,
            SnapshotCreationException, IllegalArgumentException, IOException, InterruptedException {
        ReadOnlyProps readOnlyProps = conn.getQueryServices().getProps();
        if (conn.getSchema() != null) { throw new IllegalArgumentException(
                "Schema should not be set for connection!!"); }
        if (!SchemaUtil.isNamespaceMappingEnabled(PTableType.TABLE,
                readOnlyProps)) { throw new IllegalArgumentException(
                        QueryServices.IS_NAMESPACE_MAPPING_ENABLED + " is not enabled!!"); }
        try (HBaseAdmin admin = conn.getQueryServices().getAdmin();
                HTableInterface metatable = conn.getQueryServices()
                        .getTable(SchemaUtil
                                .getPhysicalName(PhoenixDatabaseMetaData.SYSTEM_CATALOG_NAME_BYTES, readOnlyProps)
                                .getName());) {
            String tableName = SchemaUtil.normalizeIdentifier(srcTable);
            String schemaName = SchemaUtil.getSchemaNameFromFullName(tableName);
            // Confirm table is not already upgraded
            PTable table = PhoenixRuntime.getTable(conn, tableName);
            
            // Upgrade is not required if schemaName is not present.
            if (schemaName.equals("") && !PTableType.VIEW
                    .equals(table.getType())) { throw new IllegalArgumentException("Table doesn't have schema name"); }

            if (table.isNamespaceMapped()) { throw new IllegalArgumentException("Table is already upgraded"); }
            if (!schemaName.equals("")) {
                logger.info(String.format("Creating schema %s..", schemaName));
                conn.createStatement().execute("CREATE SCHEMA IF NOT EXISTS " + schemaName);
            }
            String oldPhysicalName = table.getPhysicalName().getString();
            String newPhysicalTablename = SchemaUtil.normalizeIdentifier(
                    SchemaUtil.getPhysicalTableName(oldPhysicalName, readOnlyProps).getNameAsString());
            logger.info(String.format("Upgrading %s %s..", table.getType(), tableName));
            // Upgrade the data or main table
            mapTableToNamespace(admin, metatable, tableName, newPhysicalTablename, readOnlyProps,
                    PhoenixRuntime.getCurrentScn(readOnlyProps), tableName, table.getType(),conn.getTenantId());
            // clear the cache and get new table
            conn.getQueryServices().clearTableFromCache(
                    conn.getTenantId() == null ? ByteUtil.EMPTY_BYTE_ARRAY : conn.getTenantId().getBytes(),
                    table.getSchemaName().getBytes(), table.getTableName().getBytes(),
                    PhoenixRuntime.getCurrentScn(readOnlyProps));
            MetaDataMutationResult result = new MetaDataClient(conn).updateCache(conn.getTenantId(),schemaName,
                    SchemaUtil.getTableNameFromFullName(tableName),true);
            if (result.getMutationCode() != MutationCode.TABLE_ALREADY_EXISTS) { throw new TableNotFoundException(
                    tableName); }
            table = result.getTable();
            
            // check whether table is properly upgraded before upgrading indexes
            if (table.isNamespaceMapped()) {
                for (PTable index : table.getIndexes()) {
                    String srcTableName = index.getPhysicalName().getString();
                    String destTableName = null;
                    String phoenixTableName = index.getName().getString();
                    boolean updateLink = true;
                    if (srcTableName.contains(QueryConstants.NAMESPACE_SEPARATOR)) {
                        // Skip already migrated
                        logger.info(String.format("skipping as it seems index '%s' is already upgraded..",
                                index.getName()));
                        continue;
                    }
                    if (MetaDataUtil.isLocalIndex(srcTableName)) {
                        logger.info(String.format("local index '%s' found with physical hbase table name ''..",
                                index.getName(), srcTableName));
                        destTableName = Bytes
                                .toString(MetaDataUtil.getLocalIndexPhysicalName(newPhysicalTablename.getBytes()));
                        // update parent_table property in local index table descriptor
                        conn.createStatement()
                                .execute(String.format("ALTER TABLE %s set " + MetaDataUtil.PARENT_TABLE_KEY + "='%s'",
                                        phoenixTableName, table.getPhysicalName()));
                    } else if (MetaDataUtil.isViewIndex(srcTableName)) {
                        logger.info(String.format("View index '%s' found with physical hbase table name ''..",
                                index.getName(), srcTableName));
                        destTableName = Bytes
                                .toString(MetaDataUtil.getViewIndexPhysicalName(newPhysicalTablename.getBytes()));
                    } else {
                        logger.info(String.format("Global index '%s' found with physical hbase table name ''..",
                                index.getName(), srcTableName));
                        destTableName = SchemaUtil
                                .getPhysicalTableName(index.getPhysicalName().getString(), readOnlyProps)
                                .getNameAsString();
                    }
                    logger.info(String.format("Upgrading index %s..", index.getName()));
                    if (!(table.getType() == PTableType.VIEW && !MetaDataUtil.isViewIndex(srcTableName)
                            && IndexType.LOCAL != index.getIndexType())) {
                        mapTableToNamespace(admin, metatable, srcTableName, destTableName, readOnlyProps,
                                PhoenixRuntime.getCurrentScn(readOnlyProps), phoenixTableName, index.getType(),
                                conn.getTenantId());
                    }
                    if (updateLink) {
                        logger.info(String.format("Updating link information for index '%s' ..", index.getName()));
                        updateLink(conn, srcTableName, destTableName,index.getSchemaName(),index.getTableName());
                        conn.commit();
                    }
                    conn.getQueryServices().clearTableFromCache(
                            conn.getTenantId() == null ? ByteUtil.EMPTY_BYTE_ARRAY : conn.getTenantId().getBytes(),
                            index.getSchemaName().getBytes(), index.getTableName().getBytes(),
                            PhoenixRuntime.getCurrentScn(readOnlyProps));
                }
                updateIndexesSequenceIfPresent(conn, table);
                conn.commit();

            } else {
                throw new RuntimeException("Error: problem occured during upgrade. Table is not upgraded successfully");
            }
            if (table.getType() == PTableType.VIEW) {
                updateLink(conn, oldPhysicalName, newPhysicalTablename,table.getSchemaName(),table.getTableName());
                conn.commit();
            }
        }
    }

    private static void updateIndexesSequenceIfPresent(PhoenixConnection connection, PTable dataTable)
            throws SQLException {
        PName tenantId = connection.getTenantId();
        PName physicalName = dataTable.getPhysicalName();
        PName oldPhysicalName = PNameFactory.newName(
                physicalName.toString().replace(QueryConstants.NAMESPACE_SEPARATOR, QueryConstants.NAME_SEPARATOR));
        String oldSchemaName = MetaDataUtil.getViewIndexSequenceSchemaName(oldPhysicalName, false);
        String newSchemaName = MetaDataUtil.getViewIndexSequenceSchemaName(physicalName, true);
        String newSequenceName = MetaDataUtil.getViewIndexSequenceName(physicalName, tenantId, true);
        // create new entry with new schema format
        String upsert = "UPSERT INTO " + PhoenixDatabaseMetaData.SYSTEM_SEQUENCE + " SELECT  REGEXP_SPLIT("
                + PhoenixDatabaseMetaData.SEQUENCE_NAME + ",'_')[3] ,\'" + newSchemaName + "\',\'" + newSequenceName
                + "\'," + START_WITH + "," + CURRENT_VALUE + "," + INCREMENT_BY + "," + CACHE_SIZE + "," + MIN_VALUE
                + "," + MAX_VALUE + "," + CYCLE_FLAG + "," + LIMIT_REACHED_FLAG + " FROM "
                + PhoenixDatabaseMetaData.SYSTEM_SEQUENCE + " WHERE " + PhoenixDatabaseMetaData.TENANT_ID
                + " IS NULL AND " + PhoenixDatabaseMetaData.SEQUENCE_SCHEMA + " = '" + oldSchemaName + "'";
        connection.createStatement().executeUpdate(upsert);
        // delete old sequence
        MetaDataUtil.deleteViewIndexSequences(connection, oldPhysicalName, false);
    }

    private static void updateLink(PhoenixConnection conn, String srcTableName, String destTableName, PName schemaName, PName tableName)
            throws SQLException {
        PreparedStatement updateLinkStatment = conn.prepareStatement(String.format(UPDATE_LINK,destTableName));
        updateLinkStatment.setString(1, schemaName.getString());
        updateLinkStatment.setString(2, schemaName.getString());
        updateLinkStatment.setString(3, tableName.getString());
        updateLinkStatment.setString(4, srcTableName);
        
        updateLinkStatment.execute();
        PreparedStatement deleteLinkStatment = conn.prepareStatement(DELETE_LINK);
        deleteLinkStatment.setString(1, schemaName.getString());
        deleteLinkStatment.setString(2, schemaName.getString());
        deleteLinkStatment.setString(3, tableName.getString());
        deleteLinkStatment.setString(4, srcTableName);
        deleteLinkStatment.execute();
        
    }
    
    public static void mapChildViewsToNamespace(PhoenixConnection conn, String table, Properties props)
            throws SQLException, SnapshotCreationException, IllegalArgumentException, IOException,
            InterruptedException {
        PreparedStatement preparedStatment = conn.prepareStatement(GET_VIEWS_QUERY);
        preparedStatment.setString(1, SchemaUtil.normalizeIdentifier(table));
        ResultSet rs = preparedStatment.executeQuery();
        String tenantId = null;
        String prevTenantId = null;
        PhoenixConnection passedConn = conn;
        while (rs.next()) {
            tenantId = rs.getString(1);
            if (prevTenantId != tenantId) {
                if (tenantId != null) {
                    props.setProperty(PhoenixRuntime.TENANT_ID_ATTRIB, tenantId);
                } else {
                    props.remove(PhoenixRuntime.TENANT_ID_ATTRIB);
                }
                if (passedConn != conn) {
                    conn.close();
                }
                conn = DriverManager.getConnection(conn.getURL(), props).unwrap(PhoenixConnection.class);
            }
            String viewName=SchemaUtil.getTableName(rs.getString(2), rs.getString(3));
            logger.info(String.format("Upgrading view %s for tenantId %s..", viewName,tenantId));
            UpgradeUtil.upgradeTable(conn, viewName);
            prevTenantId = tenantId;
        }
        if (passedConn != conn) {
            conn.close();
        }
    }

    public static final String getSysCatalogSnapshotName(long currentSystemTableTimestamp) {
        String tableString = SYSTEM_CATALOG_NAME;
        Format formatter = new SimpleDateFormat("yyyyMMddHHmmss");
        String date = formatter.format(new Date(System.currentTimeMillis()));
        String upgradingFrom = getVersion(currentSystemTableTimestamp);
        return "SNAPSHOT_" + tableString + "_" + upgradingFrom + "_TO_" + CURRENT_CLIENT_VERSION + "_" + date;
    }
    
    public static boolean isNoUpgradeSet(Properties props) {
        return Boolean.compare(true, Boolean.valueOf(props.getProperty(DO_NOT_UPGRADE))) == 0;
    }
    
    public static void doNotUpgradeOnFirstConnection(Properties props) {
        props.setProperty(DO_NOT_UPGRADE, String.valueOf(true));
    }
}