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
import static org.apache.phoenix.jdbc.PhoenixDatabaseMetaData.ARRAY_SIZE;
import static org.apache.phoenix.jdbc.PhoenixDatabaseMetaData.COLUMN_FAMILY;
import static org.apache.phoenix.jdbc.PhoenixDatabaseMetaData.COLUMN_NAME;
import static org.apache.phoenix.jdbc.PhoenixDatabaseMetaData.COLUMN_SIZE;
import static org.apache.phoenix.jdbc.PhoenixDatabaseMetaData.DATA_TYPE;
import static org.apache.phoenix.jdbc.PhoenixDatabaseMetaData.DECIMAL_DIGITS;
import static org.apache.phoenix.jdbc.PhoenixDatabaseMetaData.LINK_TYPE;
import static org.apache.phoenix.jdbc.PhoenixDatabaseMetaData.ORDINAL_POSITION;
import static org.apache.phoenix.jdbc.PhoenixDatabaseMetaData.SORT_ORDER;
import static org.apache.phoenix.jdbc.PhoenixDatabaseMetaData.SYSTEM_CATALOG_NAME;
import static org.apache.phoenix.jdbc.PhoenixDatabaseMetaData.TABLE_NAME;
import static org.apache.phoenix.jdbc.PhoenixDatabaseMetaData.TABLE_SCHEM;
import static org.apache.phoenix.jdbc.PhoenixDatabaseMetaData.TENANT_ID;
import static org.apache.phoenix.query.QueryConstants.BASE_TABLE_BASE_COLUMN_COUNT;
import static org.apache.phoenix.query.QueryConstants.DIVERGED_VIEW_BASE_COLUMN_COUNT;

import java.io.IOException;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTableInterface;
import org.apache.hadoop.hbase.client.Mutation;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.phoenix.coprocessor.MetaDataProtocol;
import org.apache.phoenix.jdbc.PhoenixConnection;
import org.apache.phoenix.jdbc.PhoenixDatabaseMetaData;
import org.apache.phoenix.query.QueryConstants;
import org.apache.phoenix.schema.PName;
import org.apache.phoenix.schema.PNameFactory;
import org.apache.phoenix.schema.PTable;
import org.apache.phoenix.schema.PTable.LinkType;
import org.apache.phoenix.schema.SaltingUtil;
import org.apache.phoenix.schema.types.PInteger;
import org.apache.phoenix.schema.types.PLong;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Objects;
import com.google.common.collect.Lists;

public class UpgradeUtil {
    private static final Logger logger = LoggerFactory.getLogger(UpgradeUtil.class);
    private static final byte[] SEQ_PREFIX_BYTES = ByteUtil.concat(QueryConstants.SEPARATOR_BYTE_ARRAY, Bytes.toBytes("_SEQ_"));
    
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
    
    private UpgradeUtil() {
    }

    private static byte[] getSequenceSnapshotName() {
        return Bytes.toBytes("_BAK_" + PhoenixDatabaseMetaData.SEQUENCE_FULLNAME);
    }
    
    private static void createSequenceSnapshot(HBaseAdmin admin, PhoenixConnection conn) throws SQLException {
        byte[] tableName = getSequenceSnapshotName();
        HColumnDescriptor columnDesc = new HColumnDescriptor(PhoenixDatabaseMetaData.SEQUENCE_FAMILY_BYTES);
        HTableDescriptor desc = new HTableDescriptor(TableName.valueOf(tableName));
        desc.addFamily(columnDesc);
        try {
            admin.createTable(desc);
            copyTable(conn, PhoenixDatabaseMetaData.SEQUENCE_FULLNAME_BYTES, tableName);
        } catch (IOException e) {
            throw ServerUtil.parseServerException(e);
        }
    }
    
    private static void restoreSequenceSnapshot(HBaseAdmin admin, PhoenixConnection conn) throws SQLException {
        byte[] tableName = getSequenceSnapshotName();
        copyTable(conn, tableName, PhoenixDatabaseMetaData.SEQUENCE_FULLNAME_BYTES);
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
            HTableDescriptor desc = admin.getTableDescriptor(PhoenixDatabaseMetaData.SEQUENCE_FULLNAME_BYTES);
            createSequenceSnapshot(admin, conn);
            snapshotCreated = true;
            admin.disableTable(PhoenixDatabaseMetaData.SEQUENCE_FULLNAME);
            admin.deleteTable(PhoenixDatabaseMetaData.SEQUENCE_FULLNAME);
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
    
    @SuppressWarnings("deprecation")
    public static boolean upgradeSequenceTable(PhoenixConnection conn, int nSaltBuckets, PTable oldTable) throws SQLException {
        logger.info("Upgrading SYSTEM.SEQUENCE table");

        byte[] seqTableKey = SchemaUtil.getTableKey(null, PhoenixDatabaseMetaData.SEQUENCE_SCHEMA_NAME, PhoenixDatabaseMetaData.SEQUENCE_TABLE_NAME);
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
                HTableInterface seqTable = conn.getQueryServices().getTable(PhoenixDatabaseMetaData.SEQUENCE_FULLNAME_BYTES);
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
            newBuf = MetaDataUtil.getViewIndexSequenceKey(tableName == null ? null : Bytes.toString(tableName), physicalName, nSaltBuckets).getKey();
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
            // Need to use own connection without any SCN to be able to read all data from SYSTEM.CATALOG 
            metaConnection = new PhoenixConnection(oldMetaConnection);
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
                             * This means that the view has divorced itself from the base physical table. In such a case
                             * we will set a special value for the base column count. That special value will also be used
                             * on the server side to filter out the divorced view so that meta-data changes on the base 
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
                                // special value to denote that the view has divorced itself from the base physical table.
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

}
