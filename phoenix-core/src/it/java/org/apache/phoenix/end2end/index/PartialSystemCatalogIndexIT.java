package org.apache.phoenix.end2end.index;

import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.coprocessor.RegionCoprocessorEnvironment;
import org.apache.phoenix.coprocessor.TaskRegionObserver;
import org.apache.phoenix.coprocessorclient.BaseScannerRegionObserverConstants;
import org.apache.phoenix.end2end.LocalHBaseIT;
import org.apache.phoenix.end2end.ParallelStatsDisabledIT;
import org.apache.phoenix.end2end.ViewTTLIT;
import org.apache.phoenix.jdbc.PhoenixDatabaseMetaData;
import org.apache.phoenix.query.PhoenixTestBuilder;
import org.apache.phoenix.query.QueryServices;
import org.apache.phoenix.query.QueryServicesOptions;
import org.apache.phoenix.schema.PTable;
import org.apache.phoenix.schema.PTableType;
import org.apache.phoenix.thirdparty.com.google.common.collect.Lists;
import org.apache.phoenix.util.ReadOnlyProps;
import org.apache.phoenix.util.SchemaUtil;
import org.apache.phoenix.util.TableViewFinderResult;
import org.apache.phoenix.util.TestUtil;
import org.apache.phoenix.util.ViewUtil;
import org.junit.BeforeClass;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import static org.apache.phoenix.query.PhoenixTestBuilder.DDLDefaults.COLUMN_TYPES;
import static org.apache.phoenix.query.PhoenixTestBuilder.DDLDefaults.TENANT_VIEW_COLUMNS;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

public class PartialSystemCatalogIndexIT extends ParallelStatsDisabledIT {
    static final Logger LOGGER = LoggerFactory.getLogger(ViewTTLIT.class);
    static final int VIEW_TTL_10_SECS = 10;
    static final int VIEW_TTL_300_SECS = 300;
    static final int VIEW_TTL_120_SECS = 120;

    static final String SYS_CATALOG_VIEW_HEADER_SQL = "SELECT TTL FROM SYSTEM.CATALOG "
            + "WHERE %s AND TABLE_SCHEM = '%s' AND TABLE_NAME = '%s' AND TABLE_TYPE = 'v'";

    static final String SYS_CATALOG_VIEW_INDEX_HEADER_SQL = "SELECT VIEW_INDEX_ID FROM SYSTEM.CATALOG "
            + "WHERE %s AND TABLE_SCHEM = '%s' AND TABLE_NAME = '%s' AND TABLE_TYPE = 'i' AND LINK_TYPE IS NULL";

    static final String SYS_CATALOG_SYS_INDEX_TABLE_SQL = "SELECT count(*) FROM SYSTEM.CATALOG " +
            "WHERE TABLE_SCHEM = 'SYSTEM' AND TABLE_NAME = '%s'";

    static final String SYS_CATALOG_INDEX_TABLE_LINK_SQL = "SELECT count(*) FROM SYSTEM.CATALOG " +
            "WHERE %s AND TABLE_SCHEM = '%s' AND TABLE_NAME = '%s' AND TABLE_TYPE = 'i'" +
            " AND LINK_TYPE = 1";

    static final String SYS_CATALOG_IDX_INDEX_TABLE_LINK_SQL = "SELECT \":COLUMN_FAMILY\" FROM %s " +
            "WHERE %s AND \":TABLE_SCHEM\" = '%s' AND \":TABLE_NAME\" = '%s'" ;

    static final String SYS_CATALOG_IDX_VIEW_HEADER_SQL = "SELECT \"0:VIEW_STATEMENT\" FROM %s " +
            "WHERE %s AND \":TABLE_SCHEM\" = '%s' AND \":TABLE_NAME\" = '%s'" ;

    static final String SYS_CATALOG_IDX_VIEW_INDEX_HEADER_SQL = "SELECT \": DECODE_VIEW_INDEX_ID(VIEW_INDEX_ID,VIEW_INDEX_ID_DATA_TYPE)\" FROM %s " +
            "WHERE %s AND \":TABLE_SCHEM\" = '%s' AND \":TABLE_NAME\" = '%s'" ;

    private static RegionCoprocessorEnvironment TaskRegionEnvironment;

    @BeforeClass
    public static void doSetup() throws Exception {
        // Turn on the View TTL feature
        Map<String, String> DEFAULT_PROPERTIES = new HashMap<String, String>() {{
            put(QueryServices.PHOENIX_TABLE_TTL_ENABLED, String.valueOf(true));
            put(QueryServices.LONG_VIEW_INDEX_ENABLED_ATTRIB, String.valueOf(true));
            put("hbase.procedure.remote.dispatcher.delay.msec", "0");
            // no max lookback
            put(BaseScannerRegionObserverConstants.PHOENIX_MAX_LOOKBACK_AGE_CONF_KEY, Integer.toString(0));
            put(QueryServices.PHOENIX_VIEW_TTL_ENABLED, Boolean.toString(true));
            put(QueryServices.PHOENIX_VIEW_TTL_TENANT_VIEWS_PER_SCAN_LIMIT, String.valueOf(1));
            put(QueryServices.TASK_HANDLING_INTERVAL_MS_ATTRIB,
                    Long.toString(Long.MAX_VALUE));
            put(QueryServices.TASK_HANDLING_INITIAL_DELAY_MS_ATTRIB,
                    Long.toString(Long.MAX_VALUE));
        }};

        setUpTestDriver(new ReadOnlyProps(ReadOnlyProps.EMPTY_PROPS,
                DEFAULT_PROPERTIES.entrySet().iterator()));

        TaskRegionEnvironment =
                getUtility()
                        .getRSForFirstRegionInTable(
                                PhoenixDatabaseMetaData.SYSTEM_TASK_HBASE_TABLE_NAME)
                        .getRegions(PhoenixDatabaseMetaData.SYSTEM_TASK_HBASE_TABLE_NAME)
                        .get(0).getCoprocessorHost()
                        .findCoprocessorEnvironment(TaskRegionObserver.class.getName());



    }


    void assertSystemCatalogHasIndexTableLinks(String tenantId, String schemaName,
            String tableName) throws SQLException {

        try (Connection connection = DriverManager.getConnection(getUrl())) {
            Statement stmt = connection.createStatement();
            String tenantClause = tenantId == null || tenantId.isEmpty() ?
                    "TENANT_ID IS NULL" :
                    String.format("TENANT_ID = '%s'", tenantId);
            String sql = String
                    .format(SYS_CATALOG_INDEX_TABLE_LINK_SQL, tenantClause, schemaName, tableName);
            stmt.execute(sql);
            ResultSet rs = stmt.getResultSet();
            int numRows = rs.next() ? rs.getInt(1) : 0;

            assertEquals(String.format("Expected rows do not match for schema = %s, table = %s",
                    schemaName, tableName), 1, numRows);
        }
    }

    void assertSystemCatalogHasViewIndexHeaderRelatedColumns(String tenantId, String schemaName,
            String tableName, boolean exists) throws SQLException {

        try (Connection connection = DriverManager.getConnection(getUrl())) {
            Statement stmt = connection.createStatement();
            String tenantClause = tenantId == null || tenantId.isEmpty() ?
                    "TENANT_ID IS NULL" :
                    String.format("TENANT_ID = '%s'", tenantId);
            String sql = String
                    .format(SYS_CATALOG_VIEW_INDEX_HEADER_SQL, tenantClause, schemaName, tableName);
            stmt.execute(sql);
            ResultSet rs = stmt.getResultSet();
            String viewIndexId = rs.next() ? rs.getString(1) : null;
            if (exists) {
                assertNotNull(String.format("Expected rows do not match for schema = %s, table = %s",
                        schemaName, tableName), viewIndexId);
            } else {
                assertNull(String.format("Expected rows do not match for schema = %s, table = %s",
                        schemaName, tableName), viewIndexId);
            }

        }
    }

    void assertSystemCatalogHasViewHeaderRelatedColumns(String tenantId, String schemaName,
            String tableName, boolean exists, long ttlValueExpected) throws SQLException {

        try (Connection connection = DriverManager.getConnection(getUrl())) {
            Statement stmt = connection.createStatement();
            String tenantClause = tenantId == null || tenantId.isEmpty() ?
                    "TENANT_ID IS NULL" :
                    String.format("TENANT_ID = '%s'", tenantId);
            String sql = String
                    .format(SYS_CATALOG_VIEW_HEADER_SQL, tenantClause, schemaName, tableName);
            stmt.execute(sql);
            ResultSet rs = stmt.getResultSet();
            if (exists) {
                String ttlStr = rs.next() ? rs.getString(1) : null;
                long actualTTLValueReturned = ttlStr != null ? Integer.valueOf(ttlStr): 0;
                assertEquals(String.format("Expected rows do not match for schema = %s, table = %s",
                        schemaName, tableName), ttlValueExpected, actualTTLValueReturned);
            } else {
                assertFalse(String.format("Rows do exists for schema = %s, table = %s",
                        schemaName, tableName), rs.next());

            }
        }
    }

    String stripQuotes(String name) {
        return name.replace("\"", "");
    }

    void assertSystemCatalogIndexTable(String systemCatalogIndexName, boolean exists) throws SQLException {

        try (Connection connection = DriverManager.getConnection(getUrl())) {
            Statement stmt = connection.createStatement();
            String sql = String.format(SYS_CATALOG_SYS_INDEX_TABLE_SQL, systemCatalogIndexName,
                    systemCatalogIndexName);
            stmt.execute(sql);
            ResultSet rs = stmt.getResultSet();
            rs.next();
            assertTrue(String.format("Expected rows do not match for index-table = SYSTEM.%s",
                    systemCatalogIndexName), exists ? rs.getInt(1) > 0 : rs.getInt(1) == 0 );
        }
    }


    void assertSystemCatalogIndexHaveIndexTableLinks(String systemCatalogIndexName,
            String tenantId, String schemaName,
            String tableName, boolean exists, String indexName) throws SQLException {

        try (Connection connection = DriverManager.getConnection(getUrl())) {
            Statement stmt = connection.createStatement();
            String tenantClause = tenantId == null || tenantId.isEmpty() ?
                    "\":TENANT_ID\" IS NULL" :
                    String.format("\":TENANT_ID\" = '%s'", tenantId);
            String sql = String.format(SYS_CATALOG_IDX_INDEX_TABLE_LINK_SQL, systemCatalogIndexName,
                            tenantClause, schemaName, tableName);
            stmt.execute(sql);
            ResultSet rs = stmt.getResultSet();
            String colFamilyStr = rs.next() ? rs.getString(1) : null;
            if (exists) {
                assertEquals(String.format("Expected rows do not match for schema = %s, table = %s",
                        schemaName, tableName), indexName, colFamilyStr);
            } else {
                assertNull(String.format("Zero rows expected for schema = %s, table = %s",
                        schemaName, tableName), colFamilyStr);
            }
        }
    }

    void assertSystemCatalogIndexHaveViewHeaders(String systemCatalogIndexName,
            String tenantId, String schemaName,
            String tableName, boolean exists) throws SQLException {

        try (Connection connection = DriverManager.getConnection(getUrl())) {
            Statement stmt = connection.createStatement();
            String tenantClause = tenantId == null || tenantId.isEmpty() ?
                    "\":TENANT_ID\" IS NULL" :
                    String.format("\":TENANT_ID\" = '%s'", tenantId);
            String sql = String.format(SYS_CATALOG_IDX_VIEW_HEADER_SQL, systemCatalogIndexName,
                    tenantClause, schemaName, tableName);
            stmt.execute(sql);
            ResultSet rs = stmt.getResultSet();
            String viewStmt = rs.next() ? rs.getString(1) : null;
            if (exists) {
                assertNotNull(String.format("Expected rows do not match for schema = %s, table = %s",
                        schemaName, tableName), viewStmt);
            } else {
                assertNull(String.format("Zero rows expected for schema = %s, table = %s",
                        schemaName, tableName), viewStmt);
            }
        }
    }


    void assertSystemCatalogIndexHaveViewIndexHeaders(String systemCatalogIndexName,
            String tenantId, String schemaName,
            String tableName, boolean exists) throws SQLException {

        try (Connection connection = DriverManager.getConnection(getUrl())) {
            Statement stmt = connection.createStatement();
            String tenantClause = tenantId == null || tenantId.isEmpty() ?
                    "\":TENANT_ID\" IS NULL" :
                    String.format("\":TENANT_ID\" = '%s'", tenantId);
            String sql = String.format(SYS_CATALOG_IDX_VIEW_INDEX_HEADER_SQL, systemCatalogIndexName,
                    tenantClause, schemaName, tableName);
            stmt.execute(sql);
            ResultSet rs = stmt.getResultSet();
            Integer viewIndexId = rs.next() ? rs.getInt(1) : null;
            if (exists) {
                assertNotNull(String.format("Expected rows do not match for schema = %s, table = %s",
                        schemaName, tableName), viewIndexId);
            } else {
                assertNull(String.format("Zero rows expected for schema = %s, table = %s",
                        schemaName, tableName), viewIndexId);
            }
        }
    }

    void dropSystemCatalogIndex(String sysIndexName) throws SQLException {
        try (Connection conn = DriverManager.getConnection(getUrl());
                Statement stmt = conn.createStatement()) {
            stmt.execute(String.format("drop index %s ON SYSTEM.CATALOG", sysIndexName));
            conn.commit();
        }
    }

    void dropTableWithChildViews(String baseTable, int numTaskRuns) throws Exception {
        // Drop the base table

        try (Connection conn = DriverManager.getConnection(getUrl())) {
            conn.setAutoCommit(true);
            // Empty the task table first.
            conn.createStatement()
                    .execute("DELETE " + " FROM " + PhoenixDatabaseMetaData.SYSTEM_TASK_NAME);

            String dropTableSQL = String.format("DROP TABLE IF EXISTS %s CASCADE", baseTable);
            conn.createStatement().execute(dropTableSQL);
            // Run DropChildViewsTask to complete the tasks for dropping child views. The depth of the view tree is 2,
            // so we expect that this will be done in two task handling runs as each non-root level will be processed
            // in one run
            TaskRegionObserver.SelfHealingTask task =
                    new TaskRegionObserver.SelfHealingTask(
                            TaskRegionEnvironment, QueryServicesOptions.DEFAULT_TASK_HANDLING_MAX_INTERVAL_MS);
            for (int i = 0; i < numTaskRuns; i++) {
                task.run();
            }

            assertTaskColumns(conn, PTable.TaskStatus.COMPLETED.toString(), PTable.TaskType.DROP_CHILD_VIEWS,
                    null, null, null, null, null);

            // Views should be dropped by now
            TableName linkTable = TableName.valueOf(PhoenixDatabaseMetaData.SYSTEM_CHILD_LINK_NAME_BYTES);
            TableViewFinderResult childViewsResult = new TableViewFinderResult();
            ViewUtil.findAllRelatives(getUtility().getConnection().getTable(linkTable),
                    HConstants.EMPTY_BYTE_ARRAY,
                    SchemaUtil.getSchemaNameFromFullName(baseTable).getBytes(),
                    SchemaUtil.getTableNameFromFullName(baseTable).getBytes(),
                    PTable.LinkType.CHILD_TABLE,
                    childViewsResult);
            assertEquals(0, childViewsResult.getLinks().size());
        }


    }

    static void assertTaskColumns(Connection conn, String expectedStatus, PTable.TaskType taskType,
            String expectedTableName, String expectedTenantId, String expectedSchema, Timestamp expectedTs,
            String expectedIndexName)
            throws SQLException {
        ResultSet rs = conn.createStatement().executeQuery("SELECT * " +
                " FROM " + PhoenixDatabaseMetaData.SYSTEM_TASK_NAME +
                " WHERE " + PhoenixDatabaseMetaData.TASK_TYPE + " = " +
                taskType.getSerializedValue());
        assertTrue(rs.next());
        String taskStatus = rs.getString(PhoenixDatabaseMetaData.TASK_STATUS);
        assertEquals(expectedStatus, taskStatus);

        if (expectedTableName != null) {
            String tableName = rs.getString(PhoenixDatabaseMetaData.TABLE_NAME);
            assertEquals(expectedTableName, tableName);
        }

        if (expectedTenantId != null) {
            String tenantId = rs.getString(PhoenixDatabaseMetaData.TENANT_ID);
            assertEquals(expectedTenantId, tenantId);
        }

        if (expectedSchema != null) {
            String schema = rs.getString(PhoenixDatabaseMetaData.TABLE_SCHEM);
            assertEquals(expectedSchema, schema);
        }

        if (expectedTs != null) {
            Timestamp ts = rs.getTimestamp(PhoenixDatabaseMetaData.TASK_TS);
            assertEquals(expectedTs, ts);
        }

        if (expectedIndexName != null) {
            String data = rs.getString(PhoenixDatabaseMetaData.TASK_DATA);
            assertEquals(true, data.contains("\"IndexName\":\"" + expectedIndexName));
        }
    }

    private List<String> getExplain(String query, Properties props) throws SQLException {
        List<String> explainPlan = new ArrayList<>();
        try(Connection conn = DriverManager.getConnection(getUrl(), props);
                PreparedStatement statement = conn.prepareStatement("EXPLAIN " + query);
                ResultSet rs = statement.executeQuery()) {
            while(rs.next()) {
                String plan = rs.getString(1);
                explainPlan.add(plan);
            }
        }
        return explainPlan;
    }


    protected PhoenixTestBuilder.SchemaBuilder createLevel2TenantViewWithGlobalLevelTTL(
            int globalTTL,
            PhoenixTestBuilder.SchemaBuilder.TenantViewOptions tenantViewOptions,
            PhoenixTestBuilder.SchemaBuilder.TenantViewIndexOptions tenantViewIndexOptions,
            boolean allowIndex) throws Exception {
        // Define the test schema.
        // 1. Table with columns => (ORG_ID, KP, COL1, COL2, COL3), PK => (ORG_ID, KP)
        // 2. GlobalView with columns => (ID, COL4, COL5, COL6), PK => (ID)
        // 3. Tenant with columns => (ZID, COL7, COL8, COL9), PK => (ZID)
        final PhoenixTestBuilder.SchemaBuilder schemaBuilder = new PhoenixTestBuilder.SchemaBuilder(getUrl());

        PhoenixTestBuilder.SchemaBuilder.TableOptions
                tableOptions = PhoenixTestBuilder.SchemaBuilder.TableOptions.withDefaults();
        tableOptions.setTableProps("COLUMN_ENCODED_BYTES=0,MULTI_TENANT=true");

        PhoenixTestBuilder.SchemaBuilder.GlobalViewOptions
                globalViewOptions = PhoenixTestBuilder.SchemaBuilder.GlobalViewOptions.withDefaults();
        // View TTL is set to 300s => 300000 ms
        globalViewOptions.setTableProps(String.format("TTL=%d", globalTTL));

        PhoenixTestBuilder.SchemaBuilder.GlobalViewIndexOptions globalViewIndexOptions
                = PhoenixTestBuilder.SchemaBuilder.GlobalViewIndexOptions.withDefaults();
        globalViewIndexOptions.setLocal(false);

        PhoenixTestBuilder.SchemaBuilder.TenantViewOptions
                tenantViewWithOverrideOptions = PhoenixTestBuilder.SchemaBuilder.TenantViewOptions.withDefaults();
        if (tenantViewOptions != null) {
            tenantViewWithOverrideOptions = tenantViewOptions;
        }
        PhoenixTestBuilder.SchemaBuilder.TenantViewIndexOptions
                tenantViewIndexOverrideOptions = PhoenixTestBuilder.SchemaBuilder.TenantViewIndexOptions.withDefaults();
        if (tenantViewIndexOptions != null) {
            tenantViewIndexOverrideOptions = tenantViewIndexOptions;
        }
        if (allowIndex) {
            schemaBuilder.withTableOptions(tableOptions)
                    .withGlobalViewOptions(globalViewOptions)
                    .withGlobalViewIndexOptions(globalViewIndexOptions)
                    .withTenantViewOptions(tenantViewWithOverrideOptions)
                    .withTenantViewIndexOptions(tenantViewIndexOverrideOptions)
                    .buildWithNewTenant();
        } else {
            schemaBuilder.withTableOptions(tableOptions)
                    .withGlobalViewOptions(globalViewOptions)
                    .withTenantViewOptions(tenantViewWithOverrideOptions)
                    .buildWithNewTenant();
        }
        return schemaBuilder;
    }

    protected PhoenixTestBuilder.SchemaBuilder createLevel1TenantView(
            PhoenixTestBuilder.SchemaBuilder.TenantViewOptions tenantViewOptions,
            PhoenixTestBuilder.SchemaBuilder.TenantViewIndexOptions tenantViewIndexOptions) throws Exception {
        // Define the test schema.
        // 1. Table with default columns => (ORG_ID, KP, COL1, COL2, COL3), PK => (ORG_ID, KP)
        // 2. Tenant with default columns => (ZID, COL7, COL8, COL9), PK => (ZID)
        final PhoenixTestBuilder.SchemaBuilder schemaBuilder = new PhoenixTestBuilder.SchemaBuilder(getUrl());

        PhoenixTestBuilder.SchemaBuilder.TableOptions
                tableOptions = PhoenixTestBuilder.SchemaBuilder.TableOptions.withDefaults();
        tableOptions.setTableProps("COLUMN_ENCODED_BYTES=0,MULTI_TENANT=true");

        PhoenixTestBuilder.SchemaBuilder.TenantViewOptions
                tenantViewOverrideOptions = PhoenixTestBuilder.SchemaBuilder.TenantViewOptions.withDefaults();
        if (tenantViewOptions != null) {
            tenantViewOverrideOptions = tenantViewOptions;
        }
        PhoenixTestBuilder.SchemaBuilder.TenantViewIndexOptions
                tenantViewIndexOverrideOptions = PhoenixTestBuilder.SchemaBuilder.TenantViewIndexOptions.withDefaults();
        if (tenantViewIndexOptions != null) {
            tenantViewIndexOverrideOptions = tenantViewIndexOptions;
        }

        schemaBuilder.withTableOptions(tableOptions)
                .withTenantViewOptions(tenantViewOverrideOptions)
                .withTenantViewIndexOptions(tenantViewIndexOverrideOptions).buildNewView();
        return schemaBuilder;
    }

    @Test
    public void testIndexesOfIndexTableLinkTypeCondition() throws Exception {

        PhoenixTestBuilder.SchemaBuilder.TenantViewOptions
                tenantViewOptions = new PhoenixTestBuilder.SchemaBuilder.TenantViewOptions();
        tenantViewOptions.setTenantViewColumns(Lists.newArrayList(TENANT_VIEW_COLUMNS));
        tenantViewOptions.setTenantViewColumnTypes(Lists.newArrayList(COLUMN_TYPES));

        // Create 2 level view
        final PhoenixTestBuilder.SchemaBuilder
                schemaBuilder = createLevel2TenantViewWithGlobalLevelTTL(VIEW_TTL_300_SECS, tenantViewOptions, null,
                true);

        String tenantId = schemaBuilder.getDataOptions().getTenantId();
        String fullBaseTableName = schemaBuilder.getEntityTableName();
        String schemaName = stripQuotes(
                SchemaUtil.getSchemaNameFromFullName(schemaBuilder.getEntityTenantViewName()));
        String globalViewName = stripQuotes(
                SchemaUtil.getTableNameFromFullName(schemaBuilder.getEntityGlobalViewName()));
        String tenantViewName = stripQuotes(
                SchemaUtil.getTableNameFromFullName(schemaBuilder.getEntityTenantViewName()));
        String globalIndexName = stripQuotes(
                SchemaUtil.getTableNameFromFullName(schemaBuilder.getEntityGlobalViewIndexName()));
        String tenantIndexName = stripQuotes(
                SchemaUtil.getTableNameFromFullName(schemaBuilder.getEntityTenantViewIndexName()));

        // Assert View Header rows exists for global view
        assertSystemCatalogHasViewHeaderRelatedColumns("", schemaName, globalViewName, true, VIEW_TTL_300_SECS);
        // Assert View Header rows exists for tenant view
        assertSystemCatalogHasViewHeaderRelatedColumns(tenantId, schemaName, tenantViewName, true, 0);

        // Assert index table link rows (link_type = 1) exists in SYSTEM. CATALOG
        assertSystemCatalogHasIndexTableLinks(null, schemaName, globalViewName);
        assertSystemCatalogHasIndexTableLinks(tenantId, schemaName, tenantViewName);

        //Create the SYSTEM.CATALOG index for Index Table links
        try (Connection conn = DriverManager.getConnection(getUrl());
                Statement stmt = conn.createStatement()) {
            stmt.execute("CREATE INDEX IF NOT EXISTS SYS_INDEX_TABLE_LINK_IDX ON SYSTEM.CATALOG(TENANT_ID, TABLE_SCHEM, TABLE_NAME, TABLE_TYPE) WHERE TABLE_SCHEM <> 'SYSTEM' AND TABLE_TYPE = 'i' AND LINK_TYPE = 1");
            conn.commit();
        }

        // Assert System Catalog index table has been created
        assertSystemCatalogIndexTable("SYS_INDEX_TABLE_LINK_IDX", true);
        // Assert appropriate rows are inserted in the SYSTEM.CATALOG index tables
        assertSystemCatalogIndexHaveIndexTableLinks("SYSTEM.SYS_INDEX_TABLE_LINK_IDX", null, schemaName, globalViewName,
                true, globalIndexName);
        assertSystemCatalogIndexHaveIndexTableLinks("SYSTEM.SYS_INDEX_TABLE_LINK_IDX", tenantId, schemaName, tenantViewName,
                true, tenantIndexName);

        LOGGER.info("Dropping base table " + fullBaseTableName);
        dropTableWithChildViews(fullBaseTableName, 2);
        assertSystemCatalogHasViewHeaderRelatedColumns("", schemaName, globalViewName,
                false, VIEW_TTL_300_SECS);
        assertSystemCatalogHasViewHeaderRelatedColumns(tenantId, schemaName, tenantViewName,
                false, 0);

        // Assert appropriate rows are dropped/deleted in the SYSTEM.CATALOG index tables
        assertSystemCatalogIndexHaveIndexTableLinks("SYSTEM.SYS_INDEX_TABLE_LINK_IDX", null, schemaName, globalViewName, false, null);
        assertSystemCatalogIndexHaveIndexTableLinks("SYSTEM.SYS_INDEX_TABLE_LINK_IDX", tenantId, schemaName, tenantViewName, false, null);

        dropSystemCatalogIndex("SYS_INDEX_TABLE_LINK_IDX");

        // Assert System Catalog index table dropped
        assertSystemCatalogIndexTable("SYSTEM.SYS_INDEX_TABLE_LINK_IDX", false);
    }

    @Test
    public void testIndexesOfViewAndIndexHeadersCondition() throws Exception {

        PhoenixTestBuilder.SchemaBuilder.TenantViewOptions
                tenantViewOptions = PhoenixTestBuilder.SchemaBuilder.TenantViewOptions.withDefaults();
        // View TTL is set to 120s => 120000 ms
        tenantViewOptions.setTableProps(String.format("TTL=%d", VIEW_TTL_120_SECS));

        final PhoenixTestBuilder.SchemaBuilder
                schemaBuilder = createLevel1TenantView(tenantViewOptions, null);
        String tenantId = schemaBuilder.getDataOptions().getTenantId();
        String fullBaseTableName = schemaBuilder.getEntityTableName();
        String schemaName = stripQuotes(
                SchemaUtil.getSchemaNameFromFullName(schemaBuilder.getEntityTenantViewName()));
        String tenantViewName = stripQuotes(
                SchemaUtil.getTableNameFromFullName(schemaBuilder.getEntityTenantViewName()));
        String indexOnTenantViewName = String
                .format("IDX_%s", stripQuotes(schemaBuilder.getEntityKeyPrefix()));

        // TABLE_TYPE = 'v'
        // Expected 1 rows - one for TenantView.
        // Since the TTL property values are being set,
        // we expect the view header columns to show up in regular queries
        assertSystemCatalogHasViewHeaderRelatedColumns(tenantId, schemaName, tenantViewName,
                true, VIEW_TTL_120_SECS);
        // Assert index header rows (link_type IS NULL AND TABLE_TYPE = 'i') exists in SYSTEM. CATALOG
        assertSystemCatalogHasViewIndexHeaderRelatedColumns(tenantId, schemaName, indexOnTenantViewName,true);


        try (Connection conn = DriverManager.getConnection(getUrl());
                Statement stmt = conn.createStatement()) {
            //TestUtil.dumpTable(conn, TableName.valueOf(PhoenixDatabaseMetaData.SYSTEM_CATALOG_NAME_BYTES));
            stmt.execute("CREATE INDEX IF NOT EXISTS SYS_VIEW_HDR_IDX ON SYSTEM.CATALOG(TENANT_ID, TABLE_SCHEM, TABLE_NAME, COLUMN_NAME, COLUMN_FAMILY) INCLUDE (TABLE_TYPE, VIEW_STATEMENT, TTL, ROW_KEY_MATCHER) WHERE TABLE_SCHEM <> 'SYSTEM' AND TABLE_TYPE = 'v'");
            stmt.execute("CREATE INDEX IF NOT EXISTS SYS_VIEW_INDEX_HDR_IDX ON SYSTEM.CATALOG(DECODE_VIEW_INDEX_ID(VIEW_INDEX_ID, VIEW_INDEX_ID_DATA_TYPE), TENANT_ID, TABLE_SCHEM, TABLE_NAME) INCLUDE(TABLE_TYPE, LINK_TYPE, VIEW_INDEX_ID, VIEW_INDEX_ID_DATA_TYPE)  WHERE TABLE_SCHEM <> 'SYSTEM' AND TABLE_TYPE = 'i' AND LINK_TYPE IS NULL AND VIEW_INDEX_ID IS NOT NULL");
            conn.commit();
        }

        /**
         * Testing creation of SYS_INDEX rows
         */

        // Assert System Catalog index table has been created
        assertSystemCatalogIndexTable("SYS_VIEW_HDR_IDX", true);
        assertSystemCatalogIndexTable("SYS_VIEW_INDEX_HDR_IDX", true);
        // Assert appropriate rows are inserted in the SYSTEM.CATALOG index tables
        assertSystemCatalogIndexHaveViewHeaders("SYSTEM.SYS_VIEW_HDR_IDX", tenantId, schemaName, tenantViewName, true);
        assertSystemCatalogIndexHaveViewIndexHeaders("SYSTEM.SYS_VIEW_INDEX_HDR_IDX", tenantId, schemaName, indexOnTenantViewName, true);

        /**
         * Testing explain plans
         */

        List<String> plans = getExplain("select TENANT_ID, TABLE_SCHEM, TABLE_NAME, COLUMN_NAME, COLUMN_FAMILY, TABLE_TYPE FROM SYSTEM.CATALOG WHERE TABLE_SCHEM <> 'SYSTEM' AND TABLE_TYPE = 'v'", new Properties());
        assertEquals("CLIENT PARALLEL 1-WAY FULL SCAN OVER SYSTEM.SYS_VIEW_HDR_IDX", plans.get(0));

        plans = getExplain("select VIEW_INDEX_ID, VIEW_INDEX_ID_DATA_TYPE FROM SYSTEM.CATALOG WHERE TABLE_SCHEM <> 'SYSTEM' AND TABLE_TYPE = 'i' AND LINK_TYPE IS NULL AND VIEW_INDEX_ID IS NOT NULL", new Properties());
        assertEquals("CLIENT PARALLEL 1-WAY FULL SCAN OVER SYSTEM.SYS_VIEW_INDEX_HDR_IDX", plans.get(0));

        /**
         * Testing cleanup of SYS_INDEX rows after dropping tables and views
         */
        LOGGER.info("Dropping base table " + fullBaseTableName);
        dropTableWithChildViews(fullBaseTableName, 1);
        // Assert view header rows (link_type IS NULL AND TABLE_TYPE = 'v') does not exists in SYSTEM.CATALOG
        assertSystemCatalogHasViewHeaderRelatedColumns(tenantId, schemaName, tenantViewName,
                false, VIEW_TTL_120_SECS);
        // Assert index header rows (link_type IS NULL AND TABLE_TYPE = 'i') does not exists in SYSTEM.CATALOG
        assertSystemCatalogHasViewIndexHeaderRelatedColumns(tenantId, schemaName, tenantViewName,false);

        // Assert appropriate rows are dropped/deleted in the SYSTEM.CATALOG index tables
        assertSystemCatalogIndexHaveViewHeaders("SYSTEM.SYS_VIEW_HDR_IDX", tenantId, schemaName, tenantViewName, false);
        assertSystemCatalogIndexHaveViewIndexHeaders("SYSTEM.SYS_VIEW_INDEX_HDR_IDX", tenantId, schemaName, tenantViewName, false);

        dropSystemCatalogIndex("SYS_VIEW_HDR_IDX");
        dropSystemCatalogIndex("SYS_VIEW_INDEX_HDR_IDX");

        // Assert System Catalog index table dropped
        assertSystemCatalogIndexTable("SYSTEM.SYS_VIEW_HDR_IDX", false);
        assertSystemCatalogIndexTable("SYSTEM.SYS_VIEW_INDEX_HDR_IDX", false);
    }

}
