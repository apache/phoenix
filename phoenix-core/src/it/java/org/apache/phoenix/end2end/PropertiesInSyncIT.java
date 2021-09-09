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
package org.apache.phoenix.end2end;

import org.apache.hadoop.hbase.KeepDeletedCells;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.ColumnFamilyDescriptor;
import org.apache.hadoop.hbase.client.ColumnFamilyDescriptorBuilder;
import org.apache.hadoop.hbase.client.TableDescriptor;
import org.apache.hadoop.hbase.client.TableDescriptorBuilder;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.Pair;
import org.apache.phoenix.exception.SQLExceptionCode;
import org.apache.phoenix.jdbc.PhoenixConnection;
import org.apache.phoenix.schema.PName;
import org.apache.phoenix.schema.PTable;
import org.apache.phoenix.schema.PTableType;
import org.apache.phoenix.util.MetaDataUtil;
import org.apache.phoenix.util.PhoenixRuntime;
import org.apache.phoenix.util.SchemaUtil;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Properties;
import java.util.Set;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;
import static org.apache.phoenix.query.QueryConstants.DEFAULT_COLUMN_FAMILY_BYTES;
import static org.apache.phoenix.util.MetaDataUtil.SYNCED_DATA_TABLE_AND_INDEX_COL_FAM_PROPERTIES;
import static org.apache.phoenix.util.MetaDataUtil.VIEW_INDEX_TABLE_PREFIX;
import static org.apache.phoenix.util.UpgradeUtil.UPSERT_UPDATE_CACHE_FREQUENCY;
import static org.apache.phoenix.util.UpgradeUtil.syncTableAndIndexProperties;
import static org.apache.phoenix.util.UpgradeUtil.syncUpdateCacheFreqAllIndexes;
import static org.apache.phoenix.end2end.index.IndexMetadataIT.assertUpdateCacheFreq;

/**
 * Test properties that need to be kept in sync amongst all column families and indexes of a table
 */
@Category(ParallelStatsDisabledTest.class)
public class PropertiesInSyncIT extends ParallelStatsDisabledIT {
    private static final String COL_FAM1 = "CF1";
    private static final String COL_FAM2 = "CF2";
    private static final String NEW_CF = "NEW_CF";
    private static final String DUMMY_PROP_VALUE = "dummy";
    private static final int INITIAL_TTL_VALUE = 700;
    private static final KeepDeletedCells INITIAL_KEEP_DELETED_CELLS_VALUE = KeepDeletedCells.TRUE;
    private static final int INITIAL_REPLICATION_SCOPE_VALUE = 1;
    private static final int INITIAL_UPDATE_CACHE_FREQUENCY = 100;
    private static final int INITIAL_UPDATE_CACHE_FREQUENCY_VIEWS = 900;
    private static final int MODIFIED_TTL_VALUE = INITIAL_TTL_VALUE + 300;
    private static final KeepDeletedCells MODIFIED_KEEP_DELETED_CELLS_VALUE =
            (INITIAL_KEEP_DELETED_CELLS_VALUE == KeepDeletedCells.TRUE) ? KeepDeletedCells.FALSE: KeepDeletedCells.TRUE;
    private static final int MODIFIED_REPLICATION_SCOPE_VALUE = (INITIAL_REPLICATION_SCOPE_VALUE == 1) ? 0 : 1;
    private static final int MODIFIED_UPDATE_CACHE_FREQUENCY = INITIAL_UPDATE_CACHE_FREQUENCY + 300;
    private static final int MODIFIED_UPDATE_CACHE_FREQUENCY_VIEWS = INITIAL_UPDATE_CACHE_FREQUENCY_VIEWS + 300;


    // Test that we disallow specifying synced properties to be set per column family when creating a table
    @Test
    public void testDisallowSyncedPropsToBeSetColFamSpecificCreateTable() throws Exception {
        Connection conn = DriverManager.getConnection(getUrl(), new Properties());
        String tableName = generateUniqueName();
        for (String propName: SYNCED_DATA_TABLE_AND_INDEX_COL_FAM_PROPERTIES) {
            try {
                conn.createStatement().execute("create table " + tableName
                        + " (id INTEGER not null primary key, "
                        + COL_FAM1 + ".name varchar(10), " + COL_FAM2 + ".flag boolean) "
                        + COL_FAM1 + "." + propName + "=" + DUMMY_PROP_VALUE);
                fail("Should fail with SQLException when setting synced property for a specific column family");
            } catch (SQLException sqlE) {
                assertEquals("Should fail to set synced property for a specific column family",
                        SQLExceptionCode.COLUMN_FAMILY_NOT_ALLOWED_FOR_PROPERTY.getErrorCode(), sqlE.getErrorCode());
            }
        }
        conn.close();
    }

    // Test that all column families have the same value of synced properties when creating a table
    @Test
    public void testSyncedPropsAllColFamsCreateTable() throws Exception {
        Connection conn = DriverManager.getConnection(getUrl(), new Properties());
        String tableName = createBaseTableWithProps(conn);
        verifyHBaseColumnFamilyProperties(tableName, conn, false, false);
        conn.close();
    }

    // Test that we disallow specifying synced properties to be set when creating an index on a physical table or a view
    @Test
    public void testDisallowSyncedPropsToBeSetCreateIndex() throws Exception {
        Connection conn = DriverManager.getConnection(getUrl(), new Properties());
        String tableName = createBaseTableWithProps(conn);
        String localIndexName = tableName + "_LOCAL_IDX";
        String globalIndexName = tableName + "_GLOBAL_IDX";
        String viewName = "VIEW_" + tableName;
        conn.createStatement().execute("create view " + viewName
                + " (new_col SMALLINT) as select * from " + tableName + " where id > 1");
        for (String propName: SYNCED_DATA_TABLE_AND_INDEX_COL_FAM_PROPERTIES) {
            try {
                conn.createStatement().execute("create local index " + localIndexName
                        + " on " + tableName + "(name) "
                        + propName + "=" + DUMMY_PROP_VALUE);
                fail("Should fail with SQLException when setting synced property for a local index");
            } catch (SQLException sqlE) {
                assertEquals("Should fail to set synced property for a local index",
                        SQLExceptionCode.CANNOT_SET_OR_ALTER_PROPERTY_FOR_INDEX.getErrorCode(), sqlE.getErrorCode());
            }
            try {
                conn.createStatement().execute("create index " + globalIndexName
                        + " on " + tableName + "(flag) "
                        + propName + "=" + DUMMY_PROP_VALUE);
                fail("Should fail with SQLException when setting synced property for a global index");
            } catch (SQLException sqlE) {
                assertEquals("Should fail to set synced property for a global index",
                        SQLExceptionCode.CANNOT_SET_OR_ALTER_PROPERTY_FOR_INDEX.getErrorCode(), sqlE.getErrorCode());
            }
            try {
                conn.createStatement().execute("create index view_index"
                        + " on " + viewName + " (flag)" + propName + "=" + DUMMY_PROP_VALUE);
                fail("Should fail with SQLException when setting synced property for a view index");
            } catch (SQLException sqlE) {
                assertEquals("Should fail to set synced property for a view index",
                        SQLExceptionCode.CANNOT_SET_OR_ALTER_PROPERTY_FOR_INDEX.getErrorCode(), sqlE.getErrorCode());
            }
        }
        conn.close();
    }

    // Test that indexes have the same value of synced properties as their base table
    @Test
    public void testSyncedPropsBaseTableCreateIndex() throws Exception {
        Connection conn = DriverManager.getConnection(getUrl(), new Properties());
        String tableName = createBaseTableWithProps(conn);
        createIndexTable(conn, tableName, PTable.IndexType.LOCAL).getSecond();
        String globalIndexName = createIndexTable(conn, tableName, PTable.IndexType.GLOBAL).getSecond();

        // We pass the base table as the physical HBase table since our check includes checking the local index column family too
        verifyHBaseColumnFamilyProperties(tableName, conn, false, false);
        verifyHBaseColumnFamilyProperties(globalIndexName, conn, false, false);
        conn.close();
    }

    // Test that the physical view index table has the same value of synced properties as its base table
    @Test
    public void testSyncedPropsBaseTableCreateViewIndex() throws Exception {
        Connection conn = DriverManager.getConnection(getUrl(), new Properties());
        String tableName = createBaseTableWithProps(conn);
        String viewIndexName = createIndexTable(conn, tableName, null).getSecond();

        verifyHBaseColumnFamilyProperties(tableName, conn, false, false);
        verifyHBaseColumnFamilyProperties(viewIndexName, conn, false, false);
        conn.close();
    }

    // Test that we disallow specifying synced properties to be set per column family when altering a table
    @Test
    public void testDisallowSyncedPropsToBeSetColFamSpecificAlterTable() throws Exception {
        Connection conn = DriverManager.getConnection(getUrl(), new Properties());
        String tableName = createBaseTableWithProps(conn);
        StringBuilder alterAllSyncedPropsString = new StringBuilder();
        String modPropString = COL_FAM1 + ".%s=" + DUMMY_PROP_VALUE + ",";
        for (String propName: SYNCED_DATA_TABLE_AND_INDEX_COL_FAM_PROPERTIES) {
            try {
                conn.createStatement().execute("alter table " + tableName
                        + " set " + COL_FAM1 + "." + propName + "=" + DUMMY_PROP_VALUE);
                fail("Should fail with SQLException when altering synced property for a specific column family");
            } catch (SQLException sqlE) {
                assertEquals("Should fail to alter synced property for a specific column family",
                        SQLExceptionCode.COLUMN_FAMILY_NOT_ALLOWED_FOR_PROPERTY.getErrorCode(), sqlE.getErrorCode());
            }
            alterAllSyncedPropsString.append(String.format(modPropString, propName));
        }

        // Test the same when we try to set all of these properties at once
        try {
            conn.createStatement().execute("alter table " + tableName + " set "
                    + alterAllSyncedPropsString.substring(0, alterAllSyncedPropsString.length() - 1));
            fail("Should fail with SQLException when altering synced properties for a specific column family");
        } catch (SQLException sqlE) {
            assertEquals("Should fail to alter synced properties for a specific column family",
                    SQLExceptionCode.COLUMN_FAMILY_NOT_ALLOWED_FOR_PROPERTY.getErrorCode(), sqlE.getErrorCode());
        }
        conn.close();
    }

    // Test that any alteration of the synced properties gets propagated to all indexes and the physical view index table
    @Test
    public void testAlterSyncedPropsPropagateToAllIndexesAndViewIndex() throws Exception {
        Connection conn = DriverManager.getConnection(getUrl(), new Properties());
        String tableName = createBaseTableWithProps(conn);
        Set<String> tablesToCheck = new HashSet<>();
        tablesToCheck.add(tableName);
        for (int i=0; i<2; i++) {
            tablesToCheck.add(createIndexTable(conn, tableName, PTable.IndexType.LOCAL).getSecond());
            tablesToCheck.add(createIndexTable(conn, tableName, PTable.IndexType.GLOBAL).getSecond());
        }
        // Create a view and view index
        tablesToCheck.add(createIndexTable(conn, tableName, null).getSecond());

        // Now alter the base table's properties. This should get propagated to all index tables
        conn.createStatement().execute("alter table " + tableName + " set TTL=" + MODIFIED_TTL_VALUE
                + ",KEEP_DELETED_CELLS=" + MODIFIED_KEEP_DELETED_CELLS_VALUE
                + ",REPLICATION_SCOPE=" + MODIFIED_REPLICATION_SCOPE_VALUE);

        for (String table: tablesToCheck) {
            verifyHBaseColumnFamilyProperties(table, conn, true, false);
        }

        // Any indexes created henceforth should have the modified properties
        String newGlobalIndex = createIndexTable(conn, tableName, PTable.IndexType.GLOBAL).getSecond();
        String newViewIndex = createIndexTable(conn, tableName, null).getSecond();
        verifyHBaseColumnFamilyProperties(newGlobalIndex, conn, true, false);
        verifyHBaseColumnFamilyProperties(newViewIndex, conn, true, false);
        conn.close();
    }

    // Test that any if we add a column family to a base table, it gets the synced properties
    @Test
    public void testAlterTableAddColumnFamilyGetsSyncedProps() throws Exception {
        Connection conn = DriverManager.getConnection(getUrl(), new Properties());
        String tableName = createBaseTableWithProps(conn);

        // Test that we are not allowed to set any property to be kept in sync, specific to the new column family to be added
        for (String propName: SYNCED_DATA_TABLE_AND_INDEX_COL_FAM_PROPERTIES) {
            try {
                conn.createStatement().execute(
                        "alter table " + tableName + " add " + NEW_CF + ".new_column varchar(2) "
                                + NEW_CF + "." + propName + "=" + DUMMY_PROP_VALUE);
                fail("Should fail with SQLException when altering synced property for a specific column family when adding a column");
            } catch (SQLException sqlE) {
                assertEquals("Should fail to alter synced property for a specific column family when adding a column",
                        SQLExceptionCode.COLUMN_FAMILY_NOT_ALLOWED_FOR_PROPERTY.getErrorCode(), sqlE.getErrorCode());
            }
        }

        // Test that when we add a new column (belonging to a new column family) and set any property that should be
        // in sync, then the property is modified for all existing column families of the base table and its indexes
        Set<String> tablesToCheck = new HashSet<>();
        tablesToCheck.add(tableName);
        for (int i=0; i<2; i++) {
            tablesToCheck.add(createIndexTable(conn, tableName, PTable.IndexType.LOCAL).getSecond());
            tablesToCheck.add(createIndexTable(conn, tableName, PTable.IndexType.GLOBAL).getSecond());
        }
        // Create a view and view index
        tablesToCheck.add(createIndexTable(conn, tableName, null).getSecond());

        // Now add a new column family while simultaneously modifying properties to be kept in sync,
        // as well as a property which does not need to be kept in sync. Properties to be kept in sync
        // should get propagated to all index tables and already existing column families
        conn.createStatement().execute(
                "alter table " + tableName + " add " + NEW_CF + ".new_column varchar(2) "
                + "KEEP_DELETED_CELLS=" + MODIFIED_KEEP_DELETED_CELLS_VALUE
                + ",REPLICATION_SCOPE=" + MODIFIED_REPLICATION_SCOPE_VALUE
                + ",BLOCKSIZE=300000");

        for (String table: tablesToCheck) {
            verifyHBaseColumnFamilyProperties(table, conn, true, true);
        }
        try (Admin admin = conn.unwrap(PhoenixConnection.class).getQueryServices().getAdmin()) {
            ColumnFamilyDescriptor[] columnFamilies = admin.getDescriptor(TableName.valueOf(tableName))
                    .getColumnFamilies();
            for (ColumnFamilyDescriptor cfd: columnFamilies) {
                if (cfd.getNameAsString().equals(NEW_CF)) {
                    assertEquals("Newly added column fmaily should have updated property",
                            300000, cfd.getBlocksize());
                } else {
                    assertEquals("Existing column families should have default value for property",
                            ColumnFamilyDescriptorBuilder.DEFAULT_BLOCKSIZE, cfd.getBlocksize());
                }
            }
        }
        conn.close();
    }

    // Test that we disallow altering a synced property for a global index table
    @Test
    public void testDisallowAlterGlobalIndexTable() throws Exception {
        Connection conn = DriverManager.getConnection(getUrl(), new Properties());
        String tableName = createBaseTableWithProps(conn);
        String globalIndexName = createIndexTable(conn, tableName, PTable.IndexType.GLOBAL).getSecond();
        for (String propName: SYNCED_DATA_TABLE_AND_INDEX_COL_FAM_PROPERTIES) {
            try {
                conn.createStatement().execute("alter table " + globalIndexName + " set "
                + propName + "=" + DUMMY_PROP_VALUE);
                fail("Should fail with SQLException when altering synced property for a global index");
            } catch (SQLException sqlE) {
                assertEquals("Should fail to alter synced property for a global index",
                        SQLExceptionCode.CANNOT_SET_OR_ALTER_PROPERTY_FOR_INDEX.getErrorCode(), sqlE.getErrorCode());
            }
        }
        conn.close();
    }

    // Test the upgrade code path for old client to new phoenix server cases in which the client
    // may have tables which have column families and indexes whose properties are out of sync
    @Test
    public void testOldClientSyncPropsUpgradePath() throws Exception {
        Connection conn = DriverManager.getConnection(getUrl(), new Properties());

        String baseTableName = createBaseTableWithProps(conn);
        String baseTableName1 = createBaseTableWithProps(conn);
        Set<String> createdTables = new HashSet<>();
        createdTables.add(baseTableName);
        createdTables.add(baseTableName1);
        // Create different indexes on the base table
        for (int i=0; i<2; i++) {
            createdTables.add(createIndexTable(conn, baseTableName, PTable.IndexType.GLOBAL).getSecond());
            createdTables.add(createIndexTable(conn, baseTableName, PTable.IndexType.LOCAL).getSecond());
            createdTables.add(createIndexTable(conn, baseTableName, null).getSecond());
            createdTables.add(createIndexTable(conn, baseTableName1, PTable.IndexType.GLOBAL).getSecond());
            createdTables.add(createIndexTable(conn, baseTableName1, PTable.IndexType.LOCAL).getSecond());
            createdTables.add(createIndexTable(conn, baseTableName1, null).getSecond());
        }
        for (String t: createdTables) {
            verifyHBaseColumnFamilyProperties(t, conn, false, false);
        }

        try (Admin admin = conn.unwrap(PhoenixConnection.class).getQueryServices().getAdmin()) {
            for (String tableName: createdTables) {
                final TableDescriptor tableDescriptor;
                final ColumnFamilyDescriptor defaultCF;
                if (MetaDataUtil.isViewIndex(tableName)) {
                    // We won't be able to get the PTable for a view index table
                    tableDescriptor = conn.unwrap(PhoenixConnection.class).getQueryServices()
                            .getTableDescriptor(Bytes.toBytes(tableName));
                    defaultCF = tableDescriptor.getColumnFamily(DEFAULT_COLUMN_FAMILY_BYTES);
                } else {
                    PTable table = PhoenixRuntime.getTable(conn, tableName);
                    tableDescriptor = conn.unwrap(PhoenixConnection.class).getQueryServices()
                            .getTableDescriptor(table.getPhysicalName().getBytes());
                    defaultCF = tableDescriptor.getColumnFamily(SchemaUtil.getEmptyColumnFamily(table));
                }

                TableDescriptorBuilder tableDescBuilder = TableDescriptorBuilder.newBuilder(tableDescriptor);
                if (tableName.equals(baseTableName) || tableName.equals(baseTableName1)) {
                    for (ColumnFamilyDescriptor cfd: tableDescriptor.getColumnFamilies()) {
                        // Modify all column families except the default column family for the base tables
                        if (!cfd.equals(defaultCF)) {
                            ColumnFamilyDescriptorBuilder cfdb = ColumnFamilyDescriptorBuilder.newBuilder(cfd);
                            modifySyncedPropsForCF(cfdb);
                            tableDescBuilder.modifyColumnFamily(cfdb.build());
                        }
                    }
                } else {
                    for (ColumnFamilyDescriptor cfd: tableDescriptor.getColumnFamilies()) {
                        // Modify all column families for other tables
                        ColumnFamilyDescriptorBuilder cfdb = ColumnFamilyDescriptorBuilder.newBuilder(cfd);
                        modifySyncedPropsForCF(cfdb);
                        tableDescBuilder.modifyColumnFamily(cfdb.build());
                    }
                }
                admin.modifyTable(tableDescBuilder.build());
            }
        }
        // Now synchronize required properties and verify HBase metadata property values
        PhoenixConnection upgradeConn = conn.unwrap(PhoenixConnection.class);
        // Simulate an upgrade by setting the upgrade flag
        upgradeConn.setRunningUpgrade(true);
        syncTableAndIndexProperties(upgradeConn);
        for (String t: createdTables) {
            verifyHBaseColumnFamilyProperties(t, conn, false, false);
        }
        conn.close();
    }

    @Test
    public void testOldClientSyncUpdateCacheFreqUpgradePath() throws Exception {
        PTable base, index;
        String baseTableName, viewName, viewName2;
        Map<String, Set<String>> createdTablesAndViews = new HashMap<>();

        try (Connection conn = DriverManager.getConnection(getUrl(), new Properties())) {
            baseTableName = createBaseTableWithProps(conn);
            createdTablesAndViews.put(baseTableName, new HashSet<>());
            Set<String> indexes = createdTablesAndViews.get(baseTableName);
            indexes.add(createIndexTable(conn, baseTableName, PTable.IndexType.GLOBAL).getSecond());
            indexes.add(createIndexTable(conn, baseTableName, PTable.IndexType.LOCAL).getFirst());

            viewName = createViewOnBaseTableOrView(conn, baseTableName);
            createdTablesAndViews.put(viewName, new HashSet<>());
            indexes = createdTablesAndViews.get(viewName);
            indexes.add(createIndexTable(conn, viewName, PTable.IndexType.GLOBAL).getSecond());

            viewName2 = createViewOnBaseTableOrView(conn, viewName);
            createdTablesAndViews.put(viewName2, new HashSet<>());
            indexes = createdTablesAndViews.get(viewName2);
            indexes.add(createIndexTable(conn, viewName2, PTable.IndexType.LOCAL).getFirst());

            // Intentionally make UPDATE_CACHE_FREQUENCY out of sync for indexes
            PreparedStatement stmt = conn.prepareStatement(UPSERT_UPDATE_CACHE_FREQUENCY);
            for (String tableOrViewName : createdTablesAndViews.keySet()) {
                base = PhoenixRuntime.getTable(conn, tableOrViewName);
                for (String indexTableName : createdTablesAndViews.get(tableOrViewName)) {
                    index = PhoenixRuntime.getTable(conn, indexTableName);
                    PName tenantId = index.getTenantId();
                    stmt.setString(1, tenantId == null ? null : tenantId.getString());
                    stmt.setString(2, index.getSchemaName().getString());
                    stmt.setString(3, index.getTableName().getString());
                    stmt.setLong(4, base.getType() == PTableType.TABLE ?
                            MODIFIED_UPDATE_CACHE_FREQUENCY : MODIFIED_UPDATE_CACHE_FREQUENCY_VIEWS);
                    stmt.addBatch();
                }
            }
            stmt.executeBatch();
            conn.commit();

            // Clear the server-side cache so that we get the latest built PTables
            conn.unwrap(PhoenixConnection.class).getQueryServices().clearCache();
            // Verify that the modified values are reflected
            for (String tableOrViewName : createdTablesAndViews.keySet()) {
                assertUpdateCacheFreq(conn, tableOrViewName, baseTableName.equals(tableOrViewName) ?
                        INITIAL_UPDATE_CACHE_FREQUENCY : INITIAL_UPDATE_CACHE_FREQUENCY_VIEWS);
                for (String indexName : createdTablesAndViews.get(tableOrViewName)) {
                    assertUpdateCacheFreq(conn, indexName, baseTableName.equals(tableOrViewName) ?
                            MODIFIED_UPDATE_CACHE_FREQUENCY : MODIFIED_UPDATE_CACHE_FREQUENCY_VIEWS);
                }
            }

            PhoenixConnection upgradeConn = conn.unwrap(PhoenixConnection.class);
            upgradeConn.setRunningUpgrade(true);
            syncUpdateCacheFreqAllIndexes(upgradeConn,
                    PhoenixRuntime.getTableNoCache(conn, baseTableName));

            conn.unwrap(PhoenixConnection.class).getQueryServices().clearCache();
            // Verify that indexes have the synced values for UPDATE_CACHE_FREQUENCY
            for (String tableOrViewName : createdTablesAndViews.keySet()) {
                long expectedVal = baseTableName.equals(tableOrViewName) ?
                        INITIAL_UPDATE_CACHE_FREQUENCY : INITIAL_UPDATE_CACHE_FREQUENCY_VIEWS;
                assertUpdateCacheFreq(conn, tableOrViewName, expectedVal);
                for (String indexOnTableOrView : createdTablesAndViews.get(tableOrViewName)) {
                    assertUpdateCacheFreq(conn, indexOnTableOrView, expectedVal);
                }
            }
        }
    }

    /**
     * Helper method to modify the synced properties for a column family descriptor
     * @param cfdb The column family descriptor builder object
     * @throws SQLException
     */
    private void modifySyncedPropsForCF(ColumnFamilyDescriptorBuilder cfdb) throws SQLException {
        cfdb.setTimeToLive(MODIFIED_TTL_VALUE);
        cfdb.setKeepDeletedCells(MODIFIED_KEEP_DELETED_CELLS_VALUE);
        cfdb.setScope(MODIFIED_REPLICATION_SCOPE_VALUE);
    }

    /**
     * Helper method to create or alter a base table with specific values set for properties to be kept in sync
     * @param conn Phoenix connection
     * @return Name of the HBase table created
     * @throws SQLException
     */
    private String createBaseTableWithProps(Connection conn) throws SQLException {
        String tableName = generateUniqueName();
        conn.createStatement().execute("create table " + tableName
                + " (id INTEGER not null primary key, type varchar(5), "
                + COL_FAM1 + ".name varchar(10), " + COL_FAM2 + ".flag boolean) "
                + "TTL=" + INITIAL_TTL_VALUE + ",KEEP_DELETED_CELLS=" + INITIAL_KEEP_DELETED_CELLS_VALUE
                + ",REPLICATION_SCOPE=" + INITIAL_REPLICATION_SCOPE_VALUE
                + ",UPDATE_CACHE_FREQUENCY=" + INITIAL_UPDATE_CACHE_FREQUENCY);
        return tableName;
    }

    /**
     * Helper method to create an index table on a base table.
     * @param conn Phoenix connection
     * @param baseTableName Name of the HBase base table on which to create an index
     * @param indexType LOCAL, GLOBAL or if we pass in null as the indexType,
     *                 we create a view and an index on that view for the given base table
     * @return A pair consisting of the index name and the name of the physical HBase table
     * corresponding to the index created
     * @throws SQLException
     */
    private Pair<String,String> createIndexTable(Connection conn, String baseTableName,
            PTable.IndexType indexType) throws SQLException {
        // Create a view on top of the base table and then an index on that view
        if (indexType == null) {
            String viewName = createViewOnBaseTableOrView(conn, baseTableName);
            String viewIndexName = VIEW_INDEX_TABLE_PREFIX + baseTableName;
            conn.createStatement().execute("create index view_index_" + generateUniqueName()
                    + " on " + viewName + " (flag)");
            return new Pair<>(viewIndexName, viewIndexName);
        }
        switch(indexType) {
        case LOCAL:
            String localIndexName = baseTableName + "_LOCAL_" + generateUniqueName();
            conn.createStatement().execute(
                    "create local index " + localIndexName + " on " + baseTableName + "(flag)");
            return new Pair<>(localIndexName, baseTableName);
        case GLOBAL:
            String globalIndexName = baseTableName + "_GLOBAL_" + generateUniqueName();
            conn.createStatement()
                    .execute("create index " + globalIndexName + " on " + baseTableName + "(name)");
            return new Pair<>(globalIndexName, globalIndexName);
        default:
            return new Pair<>(baseTableName, baseTableName);
        }
    }

    private String createViewOnBaseTableOrView(Connection conn, String baseTableOrView) throws SQLException {
        String viewName = "VIEW_" + baseTableOrView + "_" + generateUniqueName();
        conn.createStatement().execute("create view " + viewName
                + " (" + generateUniqueName() + " SMALLINT) as select * from "
                + baseTableOrView + " where id > 1 UPDATE_CACHE_FREQUENCY="
                + INITIAL_UPDATE_CACHE_FREQUENCY_VIEWS);
        return viewName;
    }

    /**
     * Helper method to verify HBase column family properties
     * @param tableName Physical HBase table whose properties are to be verified
     * @param conn Phoenix connection
     * @param propModified true if we have altered any of the properties to be kept in sync, false otherwise
     * @param ignoreTTL We cannot modfiy a table level property when adding a column, so in those cases,
     *                 ignore the check for TTL modification
     * @throws Exception
     */
    private void verifyHBaseColumnFamilyProperties(String tableName, Connection conn, boolean propModified,
            boolean ignoreTTL) throws Exception {
        final int expectedTTL = propModified ? MODIFIED_TTL_VALUE:INITIAL_TTL_VALUE;
        final KeepDeletedCells expectedKeepDeletedCells = propModified ? MODIFIED_KEEP_DELETED_CELLS_VALUE: INITIAL_KEEP_DELETED_CELLS_VALUE;
        final int expectedReplicationScope = propModified ? MODIFIED_REPLICATION_SCOPE_VALUE:INITIAL_REPLICATION_SCOPE_VALUE;

        try (Admin admin = conn.unwrap(PhoenixConnection.class).getQueryServices().getAdmin()) {
            // Note that this includes the local index column family as well
            ColumnFamilyDescriptor[] columnFamilies = admin.getDescriptor(TableName.valueOf(tableName))
                    .getColumnFamilies();
            for (ColumnFamilyDescriptor cfd: columnFamilies) {
                if (!ignoreTTL) {
                    assertEquals("Mismatch in TTL", expectedTTL, cfd.getTimeToLive());
                }
                assertEquals("Mismatch in KEEP_DELETED_CELLS", expectedKeepDeletedCells, cfd.getKeepDeletedCells());
                assertEquals("Mismatch in REPLICATION_SCOPE", expectedReplicationScope, cfd.getScope());
            }
        }
    }
}
