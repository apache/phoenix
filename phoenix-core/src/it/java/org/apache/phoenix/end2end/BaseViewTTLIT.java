package org.apache.phoenix.end2end;

import org.apache.hadoop.hbase.CompareOperator;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.CompactionState;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.filter.BinaryComparator;
import org.apache.hadoop.hbase.filter.FilterList;
import org.apache.hadoop.hbase.filter.QualifierFilter;
import org.apache.hadoop.hbase.filter.RowFilter;
import org.apache.hadoop.hbase.filter.SubstringComparator;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.phoenix.compile.QueryPlan;
import org.apache.phoenix.coprocessor.BaseScannerRegionObserver;
import org.apache.phoenix.jdbc.PhoenixConnection;
import org.apache.phoenix.jdbc.PhoenixDatabaseMetaData;
import org.apache.phoenix.jdbc.PhoenixResultSet;
import org.apache.phoenix.jdbc.PhoenixStatement;
import org.apache.phoenix.query.PhoenixTestBuilder;
import org.apache.phoenix.query.QueryConstants;
import org.apache.phoenix.query.QueryServices;
import org.apache.phoenix.schema.PTable;
import org.apache.phoenix.schema.types.PDataType;
import org.apache.phoenix.thirdparty.com.google.common.base.Joiner;
import org.apache.phoenix.thirdparty.com.google.common.base.Preconditions;
import org.apache.phoenix.thirdparty.com.google.common.collect.Lists;
import org.apache.phoenix.thirdparty.com.google.common.collect.Maps;
import org.apache.phoenix.util.EnvironmentEdgeManager;
import org.apache.phoenix.util.ManualEnvironmentEdge;
import org.apache.phoenix.util.PhoenixRuntime;
import org.apache.phoenix.util.ReadOnlyProps;
import org.apache.phoenix.util.SchemaUtil;
import org.apache.phoenix.util.TestUtil;
import org.junit.After;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Random;
import java.util.Set;

import static org.apache.phoenix.query.PhoenixTestBuilder.DDLDefaults.COLUMN_TYPES;
import static org.apache.phoenix.query.PhoenixTestBuilder.DDLDefaults.MAX_ROWS;
import static org.apache.phoenix.query.PhoenixTestBuilder.DDLDefaults.TENANT_VIEW_COLUMNS;
import static org.apache.phoenix.query.PhoenixTestBuilder.DDLDefaults.TENANT_VIEW_INCLUDE_COLUMNS;
import static org.apache.phoenix.query.PhoenixTestBuilder.DDLDefaults.TENANT_VIEW_INDEX_COLUMNS;
import static org.apache.phoenix.query.PhoenixTestBuilder.DDLDefaults.TENANT_VIEW_PK_COLUMNS;
import static org.apache.phoenix.query.PhoenixTestBuilder.DDLDefaults.TENANT_VIEW_PK_TYPES;
import static org.apache.phoenix.util.PhoenixRuntime.TENANT_ID_ATTRIB;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

public abstract class BaseViewTTLIT extends LocalHBaseIT{
    static final Logger LOGGER = LoggerFactory.getLogger(ViewTTLIT.class);
    static final int VIEW_TTL_10_SECS = 10;
    static final int VIEW_TTL_300_SECS = 300;
    static final int VIEW_TTL_120_SECS = 120;
    static final String ORG_ID_FMT = "00D0x000%s";
    static final String ID_FMT = "00A0y000%07d";
    static final String ZID_FMT = "00B0y000%07d";
    static final String ALTER_TTL_SQL
            = "ALTER VIEW \"%s\".\"%s\" set TTL=%s";

    static final String ALTER_SQL_WITH_NO_TTL
            = "ALTER VIEW \"%s\".\"%s\" ADD IF NOT EXISTS %s CHAR(10)";
    static final int DEFAULT_NUM_ROWS = 5;

    static final String COL1_FMT = "a%05d";
    static final String COL2_FMT = "b%05d";
    static final String COL3_FMT = "c%05d";
    static final String COL4_FMT = "d%05d";
    static final String COL5_FMT = "e%05d";
    static final String COL6_FMT = "f%05d";
    static final String COL7_FMT = "g%05d";
    static final String COL8_FMT = "h%05d";
    static final String COL9_FMT = "i%05d";
    static final int MAX_COMPACTION_CHECKS = 1000;

    static final String TTL_HEADER_SQL = "SELECT TTL FROM SYSTEM.CATALOG "
            + "WHERE %s AND TABLE_SCHEM = '%s' AND TABLE_NAME = '%s' AND TABLE_TYPE = '%s'";

    ManualEnvironmentEdge injectEdge;

    protected static void setUpTestDriver(ReadOnlyProps props) throws Exception {
        setUpTestDriver(props, props);
    }

    @Before
    public void beforeTest(){
        EnvironmentEdgeManager.reset();
        injectEdge = new ManualEnvironmentEdge();
        injectEdge.setValue(EnvironmentEdgeManager.currentTimeMillis());
    }

    @After
    public synchronized void afterTest() throws Exception {
        EnvironmentEdgeManager.reset();
    }

    void resetEnvironmentEdgeManager(){
        EnvironmentEdgeManager.reset();
        injectEdge = new ManualEnvironmentEdge();
        injectEdge.setValue(EnvironmentEdgeManager.currentTimeMillis());
    }

    // Scans the HBase rows directly and asserts
    void assertUsingHBaseRows(byte[] hbaseTableName,
            long minTimestamp, int expectedRows) throws IOException, SQLException {

        try (Table tbl = driver.getConnectionQueryServices(getUrl(), TestUtil.TEST_PROPERTIES)
                .getTable(hbaseTableName)) {

            Scan allRows = new Scan();
            allRows.setTimeRange(minTimestamp, HConstants.LATEST_TIMESTAMP);
            ResultScanner scanner = tbl.getScanner(allRows);
            int numMatchingRows = 0;
            for (Result result = scanner.next(); result != null; result = scanner.next()) {
                numMatchingRows++;
            }
            assertEquals(String.format("Expected rows do match for table = %s at timestamp %d",
                    Bytes.toString(hbaseTableName), minTimestamp), expectedRows, numMatchingRows);
        }
    }

    // Scans the HBase rows directly for the view ttl related header rows column and asserts
    void assertViewHeaderRowsHavePhoenixTTLRelatedCells(String schemaName,
            long minTimestamp, boolean rawScan, int expectedRows) throws IOException, SQLException {

        FilterList filterList = new FilterList(FilterList.Operator.MUST_PASS_ALL);
        RowFilter schemaNameFilter = new RowFilter(CompareOperator.EQUAL,
                new SubstringComparator(schemaName));
        QualifierFilter phoenixTTLQualifierFilter = new QualifierFilter(
                CompareOperator.EQUAL,
                new BinaryComparator(PhoenixDatabaseMetaData.TTL_BYTES));
        filterList.addFilter(schemaNameFilter);
        filterList.addFilter(phoenixTTLQualifierFilter);
        try (Table tbl = driver.getConnectionQueryServices(getUrl(), TestUtil.TEST_PROPERTIES)
                .getTable(PhoenixDatabaseMetaData.SYSTEM_CATALOG_NAME_BYTES)) {

            Scan allRows = new Scan();
            allRows.setRaw(rawScan);
            allRows.setTimeRange(minTimestamp, HConstants.LATEST_TIMESTAMP);
            allRows.setFilter(filterList);
            ResultScanner scanner = tbl.getScanner(allRows);
            int numMatchingRows = 0;
            for (Result result = scanner.next(); result != null; result = scanner.next()) {
                numMatchingRows += result.containsColumn(QueryConstants.DEFAULT_COLUMN_FAMILY_BYTES,
                        PhoenixDatabaseMetaData.TTL_BYTES) ? 1 : 0;
            }
            assertEquals(String.format("Expected rows do not match for table = %s at timestamp %d",
                    Bytes.toString(PhoenixDatabaseMetaData.SYSTEM_CATALOG_NAME_BYTES),
                    minTimestamp), expectedRows, numMatchingRows);
        }

    }

    void assertSyscatHavePhoenixTTLRelatedColumns(String tenantId, String schemaName,
            String tableName, String tableType, long ttlValueExpected) throws SQLException {

        try (Connection connection = DriverManager.getConnection(getUrl())) {
            Statement stmt = connection.createStatement();
            String tenantClause = tenantId == null || tenantId.isEmpty() ?
                    "TENANT_ID IS NULL" :
                    String.format("TENANT_ID = '%s'", tenantId);
            String sql = String
                    .format(TTL_HEADER_SQL, tenantClause, schemaName, tableName, tableType);
            stmt.execute(sql);
            ResultSet rs = stmt.getResultSet();
            long actualTTLValueReturned = rs.next() ? rs.getLong(1) : 0;

            assertEquals(String.format("Expected rows do not match for schema = %s, table = %s",
                    schemaName, tableName), ttlValueExpected, actualTTLValueReturned);
        }
    }

    String stripQuotes(String name) {
        return name.replace("\"", "");
    }


    void upsertDataAndRunValidations(long viewTTL, int numRowsToUpsert,
            PhoenixTestBuilder.DataWriter dataWriter, PhoenixTestBuilder.DataReader dataReader, PhoenixTestBuilder.SchemaBuilder schemaBuilder)
            throws Exception {

        //Insert for the first time and validate them.
        validateExpiredRowsAreNotReturnedUsingData(viewTTL, upsertData(dataWriter, numRowsToUpsert),
                dataReader, schemaBuilder);

        // Update the above rows and validate the same.
        validateExpiredRowsAreNotReturnedUsingData(viewTTL, upsertData(dataWriter, numRowsToUpsert),
                dataReader, schemaBuilder);

    }

    void validateExpiredRowsAreNotReturnedUsingCounts(long viewTTL, PhoenixTestBuilder.DataReader dataReader,
            PhoenixTestBuilder.SchemaBuilder schemaBuilder) throws IOException, SQLException {

        String tenantConnectUrl =
                getUrl() + ';' + TENANT_ID_ATTRIB + '=' + schemaBuilder.getDataOptions().getTenantId();

        // Verify before TTL expiration
        try (Connection readConnection = DriverManager.getConnection(tenantConnectUrl)) {
            dataReader.setConnection(readConnection);
            org.apache.phoenix.thirdparty.com.google.common.collect.Table<String, String, Object> fetchedData
                    = fetchData(dataReader);
            assertNotNull("Fetched data should not be null", fetchedData);
            assertTrue("Rows should exists before expiration",
                    fetchedData.rowKeySet().size() > 0);
        }

        // Verify after TTL expiration
        long scnTimestamp = EnvironmentEdgeManager.currentTimeMillis();
        Properties props = new Properties();
        props.setProperty("CurrentSCN", Long.toString(scnTimestamp + (2 * viewTTL * 1000)));
        try (Connection readConnection = DriverManager.getConnection(tenantConnectUrl, props)) {

            dataReader.setConnection(readConnection);
            org.apache.phoenix.thirdparty.com.google.common.collect.Table<String, String, Object> fetchedData =
                    fetchData(dataReader);
            assertNotNull("Fetched data should not be null", fetchedData);
            assertEquals("Expired rows should not be fetched", 0,
                    fetchedData.rowKeySet().size());
        }
    }

    void validateExpiredRowsAreNotReturnedUsingData(long viewTTL,
            org.apache.phoenix.thirdparty.com.google.common.collect.Table<String, String, Object> upsertedData,
            PhoenixTestBuilder.DataReader dataReader, PhoenixTestBuilder.SchemaBuilder schemaBuilder) throws SQLException {

        String tenantConnectUrl =
                getUrl() + ';' + TENANT_ID_ATTRIB + '=' + schemaBuilder.getDataOptions().getTenantId();

        // Verify before TTL expiration
        Properties props = new Properties();
        long scnTimestamp = EnvironmentEdgeManager.currentTimeMillis() + 1;
        props.setProperty("CurrentSCN", Long.toString(scnTimestamp));
        props.setProperty(QueryServices.COLLECT_REQUEST_LEVEL_METRICS, String.valueOf(true));
        try (Connection readConnection = DriverManager.getConnection(tenantConnectUrl, props)) {

            dataReader.setConnection(readConnection);
            org.apache.phoenix.thirdparty.com.google.common.collect.Table<String, String, Object>
                    fetchedData =
                    fetchData(dataReader);
            assertNotNull("Upserted data should not be null", upsertedData);
            assertNotNull("Fetched data should not be null", fetchedData);

            verifyRowsBeforeTTLExpiration(upsertedData, fetchedData);
        }

        // Verify after TTL expiration
        props.setProperty("CurrentSCN", Long.toString(scnTimestamp + (2 * viewTTL * 1000)));
        try (Connection readConnection = DriverManager.getConnection(tenantConnectUrl, props)) {

            dataReader.setConnection(readConnection);
            org.apache.phoenix.thirdparty.com.google.common.collect.Table<String, String, Object>
                    fetchedData =
                    fetchData(dataReader);
            assertNotNull("Fetched data should not be null", fetchedData);
            assertEquals("Expired rows should not be fetched", 0, fetchedData.rowKeySet().size());
        }

    }

    void validateRowsAreNotMaskedUsingCounts(long probeTimestamp, PhoenixTestBuilder.DataReader dataReader,
            PhoenixTestBuilder.SchemaBuilder schemaBuilder) throws SQLException {

        String tenantConnectUrl =
                getUrl() + ';' + TENANT_ID_ATTRIB + '=' + schemaBuilder.getDataOptions()
                        .getTenantId();

        // Verify rows exists (not masked) at current time
        long scnTimestamp = EnvironmentEdgeManager.currentTimeMillis() + 1;
        Properties props = new Properties();
        props.setProperty("CurrentSCN", Long.toString(scnTimestamp ));
        try (Connection readConnection = DriverManager.getConnection(tenantConnectUrl, props)) {

            dataReader.setConnection(readConnection);
            org.apache.phoenix.thirdparty.com.google.common.collect.Table<String, String, Object>
                    fetchedData =
                    fetchData(dataReader);
            assertNotNull("Fetched data should not be null", fetchedData);
            assertTrue("Rows should exists before ttl expiration (now)",
                    fetchedData.rowKeySet().size() > 0);
        }

        // Verify rows exists (not masked) at probed timestamp
        props.setProperty("CurrentSCN", Long.toString(probeTimestamp));
        try (Connection readConnection = DriverManager.getConnection(tenantConnectUrl, props)) {

            dataReader.setConnection(readConnection);
            org.apache.phoenix.thirdparty.com.google.common.collect.Table<String, String, Object>
                    fetchedData =
                    fetchData(dataReader);
            assertNotNull("Fetched data should not be null", fetchedData);
            assertTrue("Rows should exists before ttl expiration (probe-timestamp)",
                    fetchedData.rowKeySet().size() > 0);
        }
    }

    static void verifyRowsBeforeTTLExpiration(
            org.apache.phoenix.thirdparty.com.google.common.collect.Table<String, String, Object> upsertedData,
            org.apache.phoenix.thirdparty.com.google.common.collect.Table<String, String, Object> fetchedData) {

        Set<String> upsertedRowKeys = upsertedData.rowKeySet();
        Set<String> fetchedRowKeys = fetchedData.rowKeySet();
        assertNotNull("Upserted row keys should not be null", upsertedRowKeys);
        assertNotNull("Fetched row keys should not be null", fetchedRowKeys);
        assertEquals(String.format("Rows upserted and fetched do not match, upserted=%d, fetched=%d",
                        upsertedRowKeys.size(), fetchedRowKeys.size()),
                upsertedRowKeys, fetchedRowKeys);

        Set<String> fetchedCols = fetchedData.columnKeySet();
        for (String rowKey : fetchedRowKeys) {
            for (String columnKey : fetchedCols) {
                Object upsertedValue = upsertedData.get(rowKey, columnKey);
                Object fetchedValue = fetchedData.get(rowKey, columnKey);
                assertNotNull("Upserted values should not be null", upsertedValue);
                assertNotNull("Fetched values should not be null", fetchedValue);
                assertEquals("Values upserted and fetched do not match",
                        upsertedValue, fetchedValue);
            }
        }
    }

    org.apache.phoenix.thirdparty.com.google.common.collect.Table<String, String, Object> upsertData(
            PhoenixTestBuilder.DataWriter dataWriter, int numRowsToUpsert) throws Exception {
        // Upsert rows
        dataWriter.upsertRows(1, numRowsToUpsert);
        return dataWriter.getDataTable();
    }

    org.apache.phoenix.thirdparty.com.google.common.collect.Table<String, String, Object> fetchData(
            PhoenixTestBuilder.DataReader dataReader) throws SQLException {

        dataReader.readRows();
        return dataReader.getDataTable();
    }

    long majorCompact(TableName table, long scnTimestamp, boolean flushOnly) throws Exception {
        try (org.apache.hadoop.hbase.client.Connection connection = ConnectionFactory.createConnection(getUtility().getConfiguration())) {
            Admin admin = connection.getAdmin();
            if (!admin.tableExists(table)) {
                return 0;
            }

            admin.flush(table);
            if (flushOnly) {
                return 0;
            }

            EnvironmentEdgeManager.injectEdge(injectEdge);
            injectEdge.setValue(scnTimestamp);

            long compactionRequestedSCN = admin.getLastMajorCompactionTimestamp(table);
            long lastCompactionTimestamp = 0;
            admin.majorCompact(table);
            int numChecks = 0;
            while ( (++numChecks < MAX_COMPACTION_CHECKS) &&
                    ((lastCompactionTimestamp = admin.getLastMajorCompactionTimestamp(table))
                            <= compactionRequestedSCN)
                    || (admin.getCompactionState(table)).equals(CompactionState.MAJOR)
                    || admin.getCompactionState(table).equals(CompactionState.MAJOR_AND_MINOR)) {
                LOGGER.info(String.format("WAITING .............................%d, %d", lastCompactionTimestamp, compactionRequestedSCN));
                Thread.sleep(100);
            }
            LOGGER.info(String.format("DONE WAITING .............................%d, %d, %d",
                    lastCompactionTimestamp, compactionRequestedSCN, numChecks));
            if (numChecks >= MAX_COMPACTION_CHECKS) {
                throw new IOException("Could not complete compaction checks");
            }
            return lastCompactionTimestamp;
        }
    }

    void validateAfterMajorCompaction(
            String schemaName, String tableName, boolean isMultiTenantIndexTable,
            long earliestTimestamp,  int ttlInSecs,
            boolean flushOnly,
            int expectedHBaseTableRows) throws Exception {

        long scnTimestamp = EnvironmentEdgeManager.currentTimeMillis() + (ttlInSecs * 1000);

        String hbaseBaseTableName = SchemaUtil.getTableName(
                schemaName,tableName);
        String viewIndexSchemaName = String
                .format("_IDX_%s", schemaName);
        String hbaseViewIndexTableName =
                SchemaUtil.getTableName(viewIndexSchemaName, tableName);

        // Compact expired rows.
        if (!isMultiTenantIndexTable) {
            LOGGER.info(String.format("########## compacting table = %s", hbaseBaseTableName));
            majorCompact(TableName.valueOf(hbaseBaseTableName), scnTimestamp, flushOnly);
        } else {
            LOGGER.info(String.format("########## compacting table = %s", hbaseViewIndexTableName));
            majorCompact(TableName.valueOf(hbaseViewIndexTableName), scnTimestamp, flushOnly);
        }

        // Validate compaction using hbase
        byte[] hbaseBaseTableNameBytes = SchemaUtil.getTableNameAsBytes(
                schemaName,tableName);
        byte[] hbaseViewIndexTableNameBytes =
                SchemaUtil.getTableNameAsBytes(viewIndexSchemaName, tableName);

        // Validate compacted rows using hbase
        if (!isMultiTenantIndexTable) {
            assertUsingHBaseRows(
                    hbaseBaseTableNameBytes,
                    earliestTimestamp,
                    expectedHBaseTableRows);
        } else {
            assertUsingHBaseRows(
                    hbaseViewIndexTableNameBytes,
                    earliestTimestamp,
                    expectedHBaseTableRows);

        }
    }

    List<PhoenixTestBuilder.SchemaBuilder.OtherOptions> getTableAndGlobalAndTenantColumnFamilyOptions() {

        List<PhoenixTestBuilder.SchemaBuilder.OtherOptions> testCases = Lists.newArrayList();

        PhoenixTestBuilder.SchemaBuilder.OtherOptions
                testCaseWhenAllCFMatchAndAllDefault = new PhoenixTestBuilder.SchemaBuilder.OtherOptions();
        testCaseWhenAllCFMatchAndAllDefault.setTestName("testCaseWhenAllCFMatchAndAllDefault");
        testCaseWhenAllCFMatchAndAllDefault
                .setTableCFs(Lists.newArrayList((String) null, null, null));
        testCaseWhenAllCFMatchAndAllDefault
                .setGlobalViewCFs(Lists.newArrayList((String) null, null, null));
        testCaseWhenAllCFMatchAndAllDefault
                .setTenantViewCFs(Lists.newArrayList((String) null, null, null));
        testCases.add(testCaseWhenAllCFMatchAndAllDefault);

        PhoenixTestBuilder.SchemaBuilder.OtherOptions
                testCaseWhenAllCFMatchAndSame = new PhoenixTestBuilder.SchemaBuilder.OtherOptions();
        testCaseWhenAllCFMatchAndSame.setTestName("testCaseWhenAllCFMatchAndSame");
        testCaseWhenAllCFMatchAndSame.setTableCFs(Lists.newArrayList("A", "A", "A"));
        testCaseWhenAllCFMatchAndSame.setGlobalViewCFs(Lists.newArrayList("A", "A", "A"));
        testCaseWhenAllCFMatchAndSame.setTenantViewCFs(Lists.newArrayList("A", "A", "A"));
        testCases.add(testCaseWhenAllCFMatchAndSame);

        PhoenixTestBuilder.SchemaBuilder.OtherOptions
                testCaseWhenAllCFMatch = new PhoenixTestBuilder.SchemaBuilder.OtherOptions();
        testCaseWhenAllCFMatch.setTestName("testCaseWhenAllCFMatch");
        testCaseWhenAllCFMatch.setTableCFs(Lists.newArrayList(null, "A", "B"));
        testCaseWhenAllCFMatch.setGlobalViewCFs(Lists.newArrayList(null, "A", "B"));
        testCaseWhenAllCFMatch.setTenantViewCFs(Lists.newArrayList(null, "A", "B"));
        testCases.add(testCaseWhenAllCFMatch);

        PhoenixTestBuilder.SchemaBuilder.OtherOptions
                testCaseWhenTableCFsAreDiff = new PhoenixTestBuilder.SchemaBuilder.OtherOptions();
        testCaseWhenTableCFsAreDiff.setTestName("testCaseWhenTableCFsAreDiff");
        testCaseWhenTableCFsAreDiff.setTableCFs(Lists.newArrayList(null, "A", "B"));
        testCaseWhenTableCFsAreDiff.setGlobalViewCFs(Lists.newArrayList("A", "A", "B"));
        testCaseWhenTableCFsAreDiff.setTenantViewCFs(Lists.newArrayList("A", "A", "B"));
        testCases.add(testCaseWhenTableCFsAreDiff);

        PhoenixTestBuilder.SchemaBuilder.OtherOptions
                testCaseWhenGlobalAndTenantCFsAreDiff = new PhoenixTestBuilder.SchemaBuilder.OtherOptions();
        testCaseWhenGlobalAndTenantCFsAreDiff.setTestName("testCaseWhenGlobalAndTenantCFsAreDiff");
        testCaseWhenGlobalAndTenantCFsAreDiff.setTableCFs(Lists.newArrayList(null, "A", "B"));
        testCaseWhenGlobalAndTenantCFsAreDiff.setGlobalViewCFs(Lists.newArrayList("A", "A", "A"));
        testCaseWhenGlobalAndTenantCFsAreDiff.setTenantViewCFs(Lists.newArrayList("B", "B", "B"));
        testCases.add(testCaseWhenGlobalAndTenantCFsAreDiff);

        return testCases;
    }

    void runValidations(long viewTTL,
            org.apache.phoenix.thirdparty.com.google.common.collect.Table<String, String, Object> table,
            PhoenixTestBuilder.DataReader dataReader, PhoenixTestBuilder.SchemaBuilder schemaBuilder)
            throws Exception {

        //Insert for the first time and validate them.
        validateExpiredRowsAreNotReturnedUsingData(viewTTL, table,
                dataReader, schemaBuilder);

        // Update the above rows and validate the same.
        validateExpiredRowsAreNotReturnedUsingData(viewTTL, table,
                dataReader, schemaBuilder);

    }



    protected void testMajorCompactWithSaltedIndexedTenantView() throws Exception {

        // View TTL is set in seconds (for e.g 10 secs)
        int viewTTL = VIEW_TTL_10_SECS;
        PhoenixTestBuilder.SchemaBuilder.TableOptions
                tableOptions = PhoenixTestBuilder.SchemaBuilder.TableOptions.withDefaults();
        String tableProps = "COLUMN_ENCODED_BYTES=0,DEFAULT_COLUMN_FAMILY='0'";
        tableOptions.setTableProps(tableProps);
        tableOptions.setSaltBuckets(1);
        for (boolean isMultiTenant : Lists.newArrayList(true, false)) {

            resetEnvironmentEdgeManager();
            tableOptions.setMultiTenant(isMultiTenant);
            PhoenixTestBuilder.SchemaBuilder.DataOptions dataOptions =  isMultiTenant ?
                    PhoenixTestBuilder.SchemaBuilder.DataOptions.withDefaults() :
                    PhoenixTestBuilder.SchemaBuilder.DataOptions.withPrefix("SALTED");

            // OID, KP for non multi-tenanted views
            int viewCounter = 1;
            String orgId =
                    String.format(PhoenixTestBuilder.DDLDefaults.DEFAULT_ALT_TENANT_ID_FMT,
                            viewCounter,
                            dataOptions.getUniqueName());
            String keyPrefix = "SLT";

            // Define the test schema.
            final PhoenixTestBuilder.SchemaBuilder
                    schemaBuilder = new PhoenixTestBuilder.SchemaBuilder(getUrl());

            if (isMultiTenant) {
                PhoenixTestBuilder.SchemaBuilder.TenantViewOptions
                        tenantViewOptions = PhoenixTestBuilder.SchemaBuilder.TenantViewOptions.withDefaults();
                tenantViewOptions.setTableProps(String.format("TTL=%d", viewTTL));
                schemaBuilder
                        .withTableOptions(tableOptions)
                        .withTenantViewOptions(tenantViewOptions)
                        .withDataOptions(dataOptions)
                        .withTenantViewIndexDefaults()
                        .buildWithNewTenant();
            } else {

                PhoenixTestBuilder.SchemaBuilder.GlobalViewOptions
                        globalViewOptions = new PhoenixTestBuilder.SchemaBuilder.GlobalViewOptions();
                globalViewOptions.setSchemaName(PhoenixTestBuilder.DDLDefaults.DEFAULT_SCHEMA_NAME);
                globalViewOptions.setGlobalViewColumns(Lists.newArrayList(TENANT_VIEW_COLUMNS));
                globalViewOptions.setGlobalViewColumnTypes(Lists.newArrayList(COLUMN_TYPES));
                globalViewOptions.setGlobalViewPKColumns(Lists.newArrayList(TENANT_VIEW_PK_COLUMNS));
                globalViewOptions.setGlobalViewPKColumnTypes(Lists.newArrayList(TENANT_VIEW_PK_TYPES));
                globalViewOptions.setTableProps(String.format("TTL=%d", viewTTL));

                PhoenixTestBuilder.SchemaBuilder.GlobalViewIndexOptions
                        globalViewIndexOptions = new PhoenixTestBuilder.SchemaBuilder.GlobalViewIndexOptions();
                globalViewIndexOptions.setGlobalViewIndexColumns(
                        Lists.newArrayList(TENANT_VIEW_INDEX_COLUMNS));
                globalViewIndexOptions.setGlobalViewIncludeColumns(
                        Lists.newArrayList(TENANT_VIEW_INCLUDE_COLUMNS));

                globalViewOptions.setGlobalViewCondition(String.format(
                        "SELECT * FROM %s.%s WHERE OID = '%s' AND KP = '%s'",
                        dataOptions.getSchemaName(), dataOptions.getTableName(), orgId, keyPrefix));
                PhoenixTestBuilder.SchemaBuilder.ConnectOptions
                        connectOptions = new PhoenixTestBuilder.SchemaBuilder.ConnectOptions();
                connectOptions.setUseGlobalConnectionOnly(true);

                schemaBuilder
                        .withTableOptions(tableOptions)
                        .withGlobalViewOptions(globalViewOptions)
                        .withDataOptions(dataOptions)
                        .withConnectOptions(connectOptions)
                        .withGlobalViewIndexOptions(globalViewIndexOptions)
                        .build();
            }

            // Define the test data.
            PhoenixTestBuilder.DataSupplier dataSupplier = new PhoenixTestBuilder.DataSupplier() {

                @Override public List<Object> getValues(int rowIndex) {
                    Random rnd = new Random();
                    String oid = orgId;
                    String kp = keyPrefix;
                    String zid = String.format(ZID_FMT, rowIndex);
                    String col1 = String.format(COL1_FMT, rowIndex + rnd.nextInt(MAX_ROWS));
                    String col2 = String.format(COL2_FMT, rowIndex + rnd.nextInt(MAX_ROWS));
                    String col3 = String.format(COL3_FMT, rowIndex + rnd.nextInt(MAX_ROWS));
                    String col7 = String.format(COL7_FMT, rowIndex + rnd.nextInt(MAX_ROWS));
                    String col8 = String.format(COL8_FMT, rowIndex + rnd.nextInt(MAX_ROWS));
                    String col9 = String.format(COL9_FMT, rowIndex + rnd.nextInt(MAX_ROWS));
                    return isMultiTenant ?
                            Lists.newArrayList(
                                    new Object[] { zid, col1, col2, col3, col7, col8, col9 }) :
                            Lists.newArrayList(
                                    new Object[] { oid, kp, zid, col1, col2, col3, col7, col8, col9 });
                }
            };

            long earliestTimestamp = EnvironmentEdgeManager.currentTimeMillis();
            // Create a test data reader/writer for the above schema.
            PhoenixTestBuilder.DataWriter dataWriter = new PhoenixTestBuilder.BasicDataWriter();
            PhoenixTestBuilder.DataReader dataReader = new PhoenixTestBuilder.BasicDataReader();

            List<String> columns = isMultiTenant ?
                    Lists.newArrayList("ZID", "COL1", "COL2", "COL3",
                            "COL7", "COL8", "COL9") :
                    Lists.newArrayList("OID", "KP", "ZID", "COL1", "COL2", "COL3",
                            "COL7", "COL8", "COL9") ;
            List<String> rowKeyColumns = isMultiTenant ?
                    Lists.newArrayList("ZID") :
                    Lists.newArrayList("OID", "KP", "ZID");
            String connectUrl = isMultiTenant ?
                    getUrl() + ';' + TENANT_ID_ATTRIB + '=' +
                            schemaBuilder.getDataOptions().getTenantId() :
                    getUrl();
            try (Connection writeConnection = DriverManager.getConnection(connectUrl)) {
                writeConnection.setAutoCommit(true);
                dataWriter.setConnection(writeConnection);
                dataWriter.setDataSupplier(dataSupplier);
                dataWriter.setUpsertColumns(columns);
                dataWriter.setRowKeyColumns(rowKeyColumns);
                dataWriter.setTargetEntity(
                        isMultiTenant ?
                                schemaBuilder.getEntityTenantViewName() :
                                schemaBuilder.getEntityGlobalViewName());

                dataReader.setValidationColumns(columns);
                dataReader.setRowKeyColumns(rowKeyColumns);
                dataReader.setDML(String.format("SELECT %s from %s", Joiner.on(",").join(columns),
                        isMultiTenant ?
                                schemaBuilder.getEntityTenantViewName() :
                                schemaBuilder.getEntityGlobalViewName()));
                dataReader.setTargetEntity(
                        isMultiTenant ?
                                schemaBuilder.getEntityTenantViewName() :
                                schemaBuilder.getEntityGlobalViewName());

                // Validate data before and after ttl expiration.
                upsertDataAndRunValidations(viewTTL, DEFAULT_NUM_ROWS, dataWriter, dataReader,
                        schemaBuilder);
            }

            PTable table = schemaBuilder.getBaseTable();
            // validate multi-tenanted base table
            validateAfterMajorCompaction(
                    table.getSchemaName().toString(),
                    table.getTableName().toString(),
                    false,
                    earliestTimestamp,
                    VIEW_TTL_10_SECS,
                    false,
                    0
            );

            // validate multi-tenanted index table
            validateAfterMajorCompaction(
                    table.getSchemaName().toString(),
                    table.getTableName().toString(),
                    true,
                    earliestTimestamp,
                    VIEW_TTL_10_SECS,
                    false,
                    0
            );
        }

    }

    protected void testMajorCompactWithOnlyTenantView() throws Exception {

        // View TTL is set in seconds (for e.g 10 secs)
        int viewTTL = VIEW_TTL_10_SECS;
        PhoenixTestBuilder.SchemaBuilder.TableOptions
                tableOptions = PhoenixTestBuilder.SchemaBuilder.TableOptions.withDefaults();
        String tableProps = "MULTI_TENANT=true,COLUMN_ENCODED_BYTES=0,DEFAULT_COLUMN_FAMILY='0'";
        tableOptions.setTableProps(tableProps);

        PhoenixTestBuilder.SchemaBuilder.TenantViewOptions
                tenantViewOptions = PhoenixTestBuilder.SchemaBuilder.TenantViewOptions.withDefaults();
        tenantViewOptions.setTableProps(String.format("TTL=%d", viewTTL));

        // Define the test schema.
        final PhoenixTestBuilder.SchemaBuilder schemaBuilder = new PhoenixTestBuilder.SchemaBuilder(getUrl());
        schemaBuilder
                .withTableOptions(tableOptions)
                .withTenantViewOptions(tenantViewOptions)
                .buildWithNewTenant();

        // Define the test data.
        PhoenixTestBuilder.DataSupplier dataSupplier = new PhoenixTestBuilder.DataSupplier() {

            @Override public List<Object> getValues(int rowIndex) {
                Random rnd = new Random();
                String zid = String.format(ZID_FMT, rowIndex);
                String col1 = String.format(COL1_FMT, rowIndex + rnd.nextInt(MAX_ROWS));
                String col2 = String.format(COL2_FMT, rowIndex + rnd.nextInt(MAX_ROWS));
                String col3 = String.format(COL3_FMT, rowIndex + rnd.nextInt(MAX_ROWS));
                String col7 = String.format(COL7_FMT, rowIndex + rnd.nextInt(MAX_ROWS));
                String col8 = String.format(COL8_FMT, rowIndex + rnd.nextInt(MAX_ROWS));
                String col9 = String.format(COL9_FMT, rowIndex + rnd.nextInt(MAX_ROWS));
                return Lists.newArrayList(new Object[] { zid, col1, col2, col3, col7, col8, col9 });
            }
        };

        long earliestTimestamp = EnvironmentEdgeManager.currentTimeMillis();
        // Create a test data reader/writer for the above schema.
        PhoenixTestBuilder.DataWriter dataWriter = new PhoenixTestBuilder.BasicDataWriter();
        PhoenixTestBuilder.DataReader dataReader = new PhoenixTestBuilder.BasicDataReader();

        List<String> columns = Lists.newArrayList("ZID", "COL1", "COL2", "COL3",
                "COL7", "COL8", "COL9");
        List<String> rowKeyColumns = Lists.newArrayList("ZID");
        String tenantConnectUrl =
                getUrl() + ';' + TENANT_ID_ATTRIB + '=' + schemaBuilder.getDataOptions().getTenantId();
        try (Connection writeConnection = DriverManager.getConnection(tenantConnectUrl)) {
            writeConnection.setAutoCommit(true);
            dataWriter.setConnection(writeConnection);
            dataWriter.setDataSupplier(dataSupplier);
            dataWriter.setUpsertColumns(columns);
            dataWriter.setRowKeyColumns(rowKeyColumns);
            dataWriter.setTargetEntity(schemaBuilder.getEntityTenantViewName());

            dataReader.setValidationColumns(columns);
            dataReader.setRowKeyColumns(rowKeyColumns);
            dataReader.setDML(String.format("SELECT %s from %s", Joiner.on(",").join(columns),
                    schemaBuilder.getEntityTenantViewName()));
            dataReader.setTargetEntity(schemaBuilder.getEntityTenantViewName());

            // Validate data before and after ttl expiration.
            upsertDataAndRunValidations(viewTTL, DEFAULT_NUM_ROWS, dataWriter, dataReader,
                    schemaBuilder);
        }

        long scnTimestamp = EnvironmentEdgeManager.currentTimeMillis() + (viewTTL * 1000);
        PTable table = schemaBuilder.getBaseTable();
        // validate multi-tenanted base table
        validateAfterMajorCompaction(
                table.getSchemaName().toString(),
                table.getTableName().toString(),
                false,
                earliestTimestamp,
                VIEW_TTL_10_SECS,
                false,
                0
        );
    }

    protected void testMajorCompactFromMultipleGlobalIndexes() throws Exception {

        // View TTL is set in seconds (for e.g 10 secs)
        int viewTTL = VIEW_TTL_10_SECS;
        PhoenixTestBuilder.SchemaBuilder.TableOptions
                tableOptions = PhoenixTestBuilder.SchemaBuilder.TableOptions.withDefaults();
        String tableProps = "MULTI_TENANT=true,COLUMN_ENCODED_BYTES=0,DEFAULT_COLUMN_FAMILY='0'";
        tableOptions.setTableProps(tableProps);

        PhoenixTestBuilder.SchemaBuilder.GlobalViewOptions
                globalViewOptions = PhoenixTestBuilder.SchemaBuilder.GlobalViewOptions.withDefaults();
        globalViewOptions.setTableProps(String.format("TTL=%d", viewTTL));

        PhoenixTestBuilder.SchemaBuilder.GlobalViewIndexOptions
                globalViewIndexOptions = PhoenixTestBuilder.SchemaBuilder.GlobalViewIndexOptions.withDefaults();

        PhoenixTestBuilder.SchemaBuilder.TenantViewOptions
                tenantViewOptions = new PhoenixTestBuilder.SchemaBuilder.TenantViewOptions();
        tenantViewOptions.setTenantViewColumns(Lists.newArrayList(TENANT_VIEW_COLUMNS));
        tenantViewOptions.setTenantViewColumnTypes(Lists.newArrayList(COLUMN_TYPES));

        // TODO handle Local Index cases
        // Test cases :
        // Local vs Global indexes, various column family options.
        for (boolean isIndex1Local : Lists.newArrayList(true, false)) {
            for (boolean isIndex2Local : Lists.newArrayList(true, false)) {
                for (PhoenixTestBuilder.SchemaBuilder.OtherOptions options : getTableAndGlobalAndTenantColumnFamilyOptions()) {

                    resetEnvironmentEdgeManager();
                    // Define the test schema.
                    final PhoenixTestBuilder.SchemaBuilder
                            schemaBuilder = new PhoenixTestBuilder.SchemaBuilder(getUrl());

                    schemaBuilder
                            .withTableOptions(tableOptions)
                            .withGlobalViewOptions(globalViewOptions)
                            .withGlobalViewIndexOptions(globalViewIndexOptions)
                            .withTenantViewOptions(tenantViewOptions)
                            .withOtherOptions(options).build();

                    String index1Name;
                    String index2Name;
                    try (Connection globalConn = DriverManager.getConnection(getUrl());
                            final Statement statement = globalConn.createStatement()) {

                        index1Name = String.format("IDX_%s_%s",
                                schemaBuilder.getEntityGlobalViewName().replaceAll("\\.", "_"),
                                "COL4");

                        final String index1Str = String.format("CREATE %s INDEX IF NOT EXISTS "
                                        + "%s ON %s (%s) INCLUDE (%s)", isIndex1Local ? "LOCAL" : "", index1Name,
                                schemaBuilder.getEntityGlobalViewName(), "COL4", "COL5"
                        );
                        statement.execute(index1Str);

                        index2Name = String.format("IDX_%s_%s",
                                schemaBuilder.getEntityGlobalViewName().replaceAll("\\.", "_"),
                                "COL5");

                        final String index2Str = String.format("CREATE %s INDEX IF NOT EXISTS "
                                        + "%s ON %s (%s) INCLUDE (%s)", isIndex2Local ? "LOCAL" : "", index2Name,
                                schemaBuilder.getEntityGlobalViewName(), "COL5", "COL6"
                        );
                        statement.execute(index2Str);
                    }

                    long earliestTimestamp = EnvironmentEdgeManager.currentTimeMillis();
                    // Create multiple tenants, add data and run validations
                    for (int tenant : Arrays.asList(new Integer[] { 1, 2, 3 })) {
                        // build schema for tenant
                        schemaBuilder.buildWithNewTenant();
                        String tenantId = schemaBuilder.getDataOptions().getTenantId();

                        // Define the test data.
                        PhoenixTestBuilder.DataSupplier
                                dataSupplier = new PhoenixTestBuilder.DataSupplier() {

                            @Override public List<Object> getValues(int rowIndex) {
                                Random rnd = new Random();
                                String id = String.format(ID_FMT, rowIndex);
                                //String zid = String.format(ZID_FMT, rowIndex);
                                String col4 = String.format(COL4_FMT, rowIndex + rnd.nextInt(MAX_ROWS));
                                String col5 = String.format(COL5_FMT, rowIndex + rnd.nextInt(MAX_ROWS));
                                String col6 = String.format(COL6_FMT, rowIndex + rnd.nextInt(MAX_ROWS));
                                String col7 = String.format(COL7_FMT, rowIndex + rnd.nextInt(MAX_ROWS));
                                String col8 = String.format(COL8_FMT, rowIndex + rnd.nextInt(MAX_ROWS));
                                String col9 = String.format(COL9_FMT, rowIndex + rnd.nextInt(MAX_ROWS));

                                return Lists.newArrayList(
                                        new Object[] { id, col4, col5, col6, col7, col8, col9 });
                            }
                        };

                        // Create a test data reader/writer for the above schema.
                        PhoenixTestBuilder.DataWriter
                                dataWriter = new PhoenixTestBuilder.BasicDataWriter();
                        List<String> columns = Lists.newArrayList(
                                "ID", "COL4", "COL5", "COL6", "COL7", "COL8", "COL9");
                        List<String> rowKeyColumns = Lists.newArrayList("ID");
                        String tenantConnectUrl =
                                getUrl() + ';' + TENANT_ID_ATTRIB + '=' + tenantId;
                        try (Connection writeConnection =
                                DriverManager.getConnection(tenantConnectUrl)) {
                            writeConnection.setAutoCommit(true);
                            dataWriter.setConnection(writeConnection);
                            dataWriter.setDataSupplier(dataSupplier);
                            dataWriter.setUpsertColumns(columns);
                            dataWriter.setRowKeyColumns(rowKeyColumns);
                            dataWriter.setTargetEntity(schemaBuilder.getEntityTenantViewName());
                            upsertData(dataWriter, DEFAULT_NUM_ROWS);

                            // Case : count(1) sql
                            PhoenixTestBuilder.DataReader
                                    dataReader = new PhoenixTestBuilder.BasicDataReader();
                            dataReader.setValidationColumns(Arrays.asList("num_rows"));
                            dataReader.setRowKeyColumns(Arrays.asList("num_rows"));
                            dataReader.setDML(String.format(
                                    "SELECT /* +NO_INDEX */ count(1) as num_rows from %s HAVING count(1) > 0",
                                    schemaBuilder.getEntityTenantViewName()));
                            dataReader.setTargetEntity(schemaBuilder.getEntityTenantViewName());

                            // Validate data before and after ttl expiration.
                            validateExpiredRowsAreNotReturnedUsingCounts(viewTTL, dataReader, schemaBuilder);
                        }
                    }

                    // Compact data and index rows
                    long scnTimestamp = EnvironmentEdgeManager.currentTimeMillis() +
                            (viewTTL * 1000);
                    PTable table = schemaBuilder.getBaseTable();
                    // validate multi-tenanted base table
                    validateAfterMajorCompaction(
                            table.getSchemaName().toString(),
                            table.getTableName().toString(),
                            false,
                            earliestTimestamp,
                            VIEW_TTL_10_SECS,
                            false,
                            0
                    );
                    // validate multi-tenanted index table
                    validateAfterMajorCompaction(
                            table.getSchemaName().toString(),
                            table.getTableName().toString(),
                            true,
                            earliestTimestamp,
                            VIEW_TTL_10_SECS,
                            false,
                            0
                    );

                }
            }
        }
    }

    protected void testMajorCompactFromMultipleTenantIndexes() throws Exception {

        // View TTL is set in seconds (for e.g 10 secs)
        int viewTTL = VIEW_TTL_10_SECS;
        PhoenixTestBuilder.SchemaBuilder.TableOptions
                tableOptions = PhoenixTestBuilder.SchemaBuilder.TableOptions.withDefaults();
        String tableProps = "MULTI_TENANT=true,COLUMN_ENCODED_BYTES=0,DEFAULT_COLUMN_FAMILY='0'";
        tableOptions.setTableProps(tableProps);

        PhoenixTestBuilder.SchemaBuilder.GlobalViewOptions
                globalViewOptions = PhoenixTestBuilder.SchemaBuilder.GlobalViewOptions.withDefaults();

        PhoenixTestBuilder.SchemaBuilder.TenantViewOptions
                tenantViewOptions = new PhoenixTestBuilder.SchemaBuilder.TenantViewOptions();
        tenantViewOptions.setTenantViewColumns(Lists.newArrayList(TENANT_VIEW_COLUMNS));
        tenantViewOptions.setTenantViewColumnTypes(Lists.newArrayList(COLUMN_TYPES));
        tenantViewOptions.setTableProps(String.format("TTL=%d", viewTTL));

        PhoenixTestBuilder.SchemaBuilder.TenantViewIndexOptions
                tenantViewIndexOptions = PhoenixTestBuilder.SchemaBuilder.TenantViewIndexOptions.withDefaults();

        // TODO handle Local Index cases
        // Test cases :
        // Local vs Global indexes, various column family options.
        for (boolean isIndex1Local : Lists.newArrayList(true, false)) {
            for (boolean isIndex2Local : Lists.newArrayList(true, false)) {
                for (PhoenixTestBuilder.SchemaBuilder.OtherOptions options : getTableAndGlobalAndTenantColumnFamilyOptions()) {

                    resetEnvironmentEdgeManager();
                    // Define the test schema.
                    final PhoenixTestBuilder.SchemaBuilder
                            schemaBuilder = new PhoenixTestBuilder.SchemaBuilder(getUrl());
                    schemaBuilder.withTableOptions(tableOptions)
                            .withGlobalViewOptions(globalViewOptions)
                            .withTenantViewOptions(tenantViewOptions)
                            .withTenantViewIndexOptions(tenantViewIndexOptions)
                            .withOtherOptions(options).build();

                    PTable table = schemaBuilder.getBaseTable();
                    String schemaName = table.getSchemaName().getString();
                    String tableName = table.getTableName().getString();

                    long earliestTimestamp = EnvironmentEdgeManager.currentTimeMillis();
                    Map<String, List<String>> mapOfTenantIndexes = Maps.newHashMap();
                    // Create multiple tenants, add data and run validations
                    for (int tenant : Arrays.asList(new Integer[] { 1, 2, 3 })) {
                        // build schema for tenant
                        schemaBuilder.buildWithNewTenant();

                        String tenantId = schemaBuilder.getDataOptions().getTenantId();
                        String tenantConnectUrl = getUrl() + ';' + TENANT_ID_ATTRIB + '='
                                + tenantId;
                        try (Connection tenantConn = DriverManager.getConnection(tenantConnectUrl);
                                final Statement statement = tenantConn.createStatement()) {
                            PhoenixConnection phxConn = tenantConn.unwrap(PhoenixConnection.class);

                            String index1Name = String.format("IDX_%s_%s",
                                    schemaBuilder.getEntityTenantViewName().replaceAll("\\.", "_"),
                                    "COL9");

                            final String index1Str = String.format("CREATE %s INDEX IF NOT EXISTS "
                                            + "%s ON %s (%s) INCLUDE (%s)", isIndex1Local ? "LOCAL" : "",
                                    index1Name, schemaBuilder.getEntityTenantViewName(), "COL9",
                                    "COL8");
                            statement.execute(index1Str);

                            String index2Name = String.format("IDX_%s_%s",
                                    schemaBuilder.getEntityTenantViewName().replaceAll("\\.", "_"),
                                    "COL7");

                            final String index2Str = String.format("CREATE %s INDEX IF NOT EXISTS "
                                            + "%s ON %s (%s) INCLUDE (%s)", isIndex2Local ? "LOCAL" : "",
                                    index2Name, schemaBuilder.getEntityTenantViewName(), "COL7",
                                    "COL8");
                            statement.execute(index2Str);

                            String defaultViewIndexName = String.format("IDX_%s", SchemaUtil
                                    .getTableNameFromFullName(
                                            schemaBuilder.getEntityTenantViewName()));
                            // Collect the indexes for the tenants
                            mapOfTenantIndexes.put(tenantId,
                                    Arrays.asList(defaultViewIndexName, index1Name, index2Name));
                        }

                        // Define the test data.
                        PhoenixTestBuilder.DataSupplier
                                dataSupplier = new PhoenixTestBuilder.DataSupplier() {

                            @Override public List<Object> getValues(int rowIndex) {
                                Random rnd = new Random();
                                String id = String.format(ID_FMT, rowIndex);
                                String col4 = String
                                        .format(COL4_FMT, rowIndex + rnd.nextInt(MAX_ROWS));
                                String col5 = String
                                        .format(COL5_FMT, rowIndex + rnd.nextInt(MAX_ROWS));
                                String col6 = String
                                        .format(COL6_FMT, rowIndex + rnd.nextInt(MAX_ROWS));
                                String col7 = String
                                        .format(COL7_FMT, rowIndex + rnd.nextInt(MAX_ROWS));
                                String col8 = String
                                        .format(COL8_FMT, rowIndex + rnd.nextInt(MAX_ROWS));
                                String col9 = String
                                        .format(COL9_FMT, rowIndex + rnd.nextInt(MAX_ROWS));

                                return Lists.newArrayList(
                                        new Object[] { id, col4, col5, col6, col7, col8, col9 });
                            }
                        };

                        // Create a test data reader/writer for the above schema.
                        PhoenixTestBuilder.DataWriter
                                dataWriter = new PhoenixTestBuilder.BasicDataWriter();
                        List<String> columns = Lists
                                .newArrayList("ID", "COL4", "COL5", "COL6", "COL7", "COL8", "COL9");
                        List<String> rowKeyColumns = Lists.newArrayList("ID");
                        try (Connection writeConnection = DriverManager
                                .getConnection(tenantConnectUrl)) {
                            writeConnection.setAutoCommit(true);
                            dataWriter.setConnection(writeConnection);
                            dataWriter.setDataSupplier(dataSupplier);
                            dataWriter.setUpsertColumns(columns);
                            dataWriter.setRowKeyColumns(rowKeyColumns);
                            dataWriter.setTargetEntity(schemaBuilder.getEntityTenantViewName());
                            upsertData(dataWriter, DEFAULT_NUM_ROWS);

                            // Case : count(1) sql
                            PhoenixTestBuilder.DataReader
                                    dataReader = new PhoenixTestBuilder.BasicDataReader();
                            dataReader.setValidationColumns(Arrays.asList("num_rows"));
                            dataReader.setRowKeyColumns(Arrays.asList("num_rows"));
                            dataReader.setDML(String
                                    .format("SELECT /* +NO_INDEX */ count(1) as num_rows from %s HAVING count(1) > 0",
                                            schemaBuilder.getEntityTenantViewName()));
                            dataReader.setTargetEntity(schemaBuilder.getEntityTenantViewName());

                            // Validate data before and after ttl expiration.
                            validateExpiredRowsAreNotReturnedUsingCounts(viewTTL, dataReader,
                                    schemaBuilder);
                        }
                    }

                    // Compact data and index rows
                    long scnTimestamp = EnvironmentEdgeManager.currentTimeMillis() +
                            (viewTTL * 1000);

                    // validate multi-tenanted base table
                    validateAfterMajorCompaction(
                            table.getSchemaName().toString(),
                            table.getTableName().toString(),
                            false,
                            earliestTimestamp,
                            VIEW_TTL_10_SECS,
                            false,
                            0
                    );

                    // validate multi-tenanted index table
                    validateAfterMajorCompaction(
                            table.getSchemaName().toString(),
                            table.getTableName().toString(),
                            true,
                            earliestTimestamp,
                            VIEW_TTL_10_SECS,
                            false,
                            0
                    );

                }
            }
        }
    }

    protected void testMajorCompactWithSaltedIndexedBaseTables() throws Exception {

        // View TTL is set in seconds (for e.g 10 secs)
        int viewTTL = VIEW_TTL_10_SECS;
        PhoenixTestBuilder.SchemaBuilder.TableOptions
                tableOptions = PhoenixTestBuilder.SchemaBuilder.TableOptions.withDefaults();
        String tableProps = "COLUMN_ENCODED_BYTES=0,DEFAULT_COLUMN_FAMILY='0',TTL=10";
        tableOptions.setTableProps(tableProps);
        tableOptions.setSaltBuckets(1);
        tableOptions.getTablePKColumns().add("ZID");
        tableOptions.getTablePKColumnTypes().add("CHAR(15)");
        for (boolean isMultiTenant : Lists.newArrayList(true, false)) {

            resetEnvironmentEdgeManager();
            tableOptions.setMultiTenant(isMultiTenant);
            PhoenixTestBuilder.SchemaBuilder.DataOptions dataOptions =  isMultiTenant ?
                    PhoenixTestBuilder.SchemaBuilder.DataOptions.withDefaults() :
                    PhoenixTestBuilder.SchemaBuilder.DataOptions.withPrefix("SALTED");

            // OID, KP for non multi-tenanted views
            int viewCounter = 1;
            String orgId =
                    String.format(PhoenixTestBuilder.DDLDefaults.DEFAULT_ALT_TENANT_ID_FMT,
                            viewCounter,
                            dataOptions.getUniqueName());
            String keyPrefix = "SLT";

            // Define the test schema.
            final PhoenixTestBuilder.SchemaBuilder
                    schemaBuilder = new PhoenixTestBuilder.SchemaBuilder(getUrl());

            if (isMultiTenant) {
                PhoenixTestBuilder.SchemaBuilder.TenantViewOptions
                        tenantViewOptions = PhoenixTestBuilder.SchemaBuilder.TenantViewOptions.withDefaults();
                tenantViewOptions.getTenantViewPKColumns().clear();
                tenantViewOptions.getTenantViewPKColumnTypes().clear();
                schemaBuilder
                        .withTableOptions(tableOptions)
                        .withTableIndexDefaults()
                        .withTenantViewOptions(tenantViewOptions)
                        .withDataOptions(dataOptions)
                        .withTenantViewIndexDefaults()
                        .buildWithNewTenant();
            } else {

                PhoenixTestBuilder.SchemaBuilder.GlobalViewOptions
                        globalViewOptions = new PhoenixTestBuilder.SchemaBuilder.GlobalViewOptions();
                globalViewOptions.setSchemaName(PhoenixTestBuilder.DDLDefaults.DEFAULT_SCHEMA_NAME);
                globalViewOptions.setGlobalViewColumns(Lists.newArrayList(TENANT_VIEW_COLUMNS));
                globalViewOptions.setGlobalViewColumnTypes(Lists.newArrayList(COLUMN_TYPES));
                //globalViewOptions.setGlobalViewPKColumns(Lists.newArrayList(TENANT_VIEW_PK_COLUMNS));
                //globalViewOptions.setGlobalViewPKColumnTypes(Lists.newArrayList(TENANT_VIEW_PK_TYPES));

                PhoenixTestBuilder.SchemaBuilder.GlobalViewIndexOptions
                        globalViewIndexOptions = new PhoenixTestBuilder.SchemaBuilder.GlobalViewIndexOptions();
                globalViewIndexOptions.setGlobalViewIndexColumns(
                        Lists.newArrayList(TENANT_VIEW_INDEX_COLUMNS));
                globalViewIndexOptions.setGlobalViewIncludeColumns(
                        Lists.newArrayList(TENANT_VIEW_INCLUDE_COLUMNS));

                globalViewOptions.setGlobalViewCondition(String.format(
                        "SELECT * FROM %s.%s WHERE OID = '%s' AND KP = '%s'",
                        dataOptions.getSchemaName(), dataOptions.getTableName(), orgId, keyPrefix));
                PhoenixTestBuilder.SchemaBuilder.ConnectOptions
                        connectOptions = new PhoenixTestBuilder.SchemaBuilder.ConnectOptions();
                connectOptions.setUseGlobalConnectionOnly(true);

                schemaBuilder
                        .withTableOptions(tableOptions)
                        .withTableIndexDefaults()
                        .withGlobalViewOptions(globalViewOptions)
                        .withDataOptions(dataOptions)
                        .withConnectOptions(connectOptions)
                        .withGlobalViewIndexOptions(globalViewIndexOptions)
                        .build();
            }

            // Define the test data.
            PhoenixTestBuilder.DataSupplier dataSupplier = new PhoenixTestBuilder.DataSupplier() {

                @Override public List<Object> getValues(int rowIndex) {
                    Random rnd = new Random();
                    String oid = orgId;
                    String kp = keyPrefix;
                    String zid = String.format(ZID_FMT, rowIndex);
                    String col1 = String.format(COL1_FMT, rowIndex + rnd.nextInt(MAX_ROWS));
                    String col2 = String.format(COL2_FMT, rowIndex + rnd.nextInt(MAX_ROWS));
                    String col3 = String.format(COL3_FMT, rowIndex + rnd.nextInt(MAX_ROWS));
                    String col7 = String.format(COL7_FMT, rowIndex + rnd.nextInt(MAX_ROWS));
                    String col8 = String.format(COL8_FMT, rowIndex + rnd.nextInt(MAX_ROWS));
                    String col9 = String.format(COL9_FMT, rowIndex + rnd.nextInt(MAX_ROWS));
                    return isMultiTenant ?
                            Lists.newArrayList(
                                    new Object[] { zid, col1, col2, col3, col7, col8, col9 }) :
                            Lists.newArrayList(
                                    new Object[] { oid, kp, zid, col1, col2, col3, col7, col8, col9 });
                }
            };

            long earliestTimestamp = EnvironmentEdgeManager.currentTimeMillis();
            // Create a test data reader/writer for the above schema.
            PhoenixTestBuilder.DataWriter dataWriter = new PhoenixTestBuilder.BasicDataWriter();
            PhoenixTestBuilder.DataReader dataReader = new PhoenixTestBuilder.BasicDataReader();

            List<String> columns = isMultiTenant ?
                    Lists.newArrayList("ZID", "COL1", "COL2", "COL3",
                            "COL7", "COL8", "COL9") :
                    Lists.newArrayList("OID", "KP", "ZID", "COL1", "COL2", "COL3",
                            "COL7", "COL8", "COL9") ;
            List<String> rowKeyColumns = isMultiTenant ?
                    Lists.newArrayList("ZID") :
                    Lists.newArrayList("OID", "KP", "ZID");
            String connectUrl = isMultiTenant ?
                    getUrl() + ';' + TENANT_ID_ATTRIB + '=' +
                            schemaBuilder.getDataOptions().getTenantId() :
                    getUrl();
            try (Connection writeConnection = DriverManager.getConnection(connectUrl)) {
                writeConnection.setAutoCommit(true);
                dataWriter.setConnection(writeConnection);
                dataWriter.setDataSupplier(dataSupplier);
                dataWriter.setUpsertColumns(columns);
                dataWriter.setRowKeyColumns(rowKeyColumns);
                dataWriter.setTargetEntity(
                        isMultiTenant ?
                                schemaBuilder.getEntityTenantViewName() :
                                schemaBuilder.getEntityGlobalViewName());

                dataReader.setValidationColumns(columns);
                dataReader.setRowKeyColumns(rowKeyColumns);
                dataReader.setDML(String.format("SELECT %s from %s", Joiner.on(",").join(columns),
                        isMultiTenant ?
                                schemaBuilder.getEntityTenantViewName() :
                                schemaBuilder.getEntityGlobalViewName()));
                dataReader.setTargetEntity(
                        isMultiTenant ?
                                schemaBuilder.getEntityTenantViewName() :
                                schemaBuilder.getEntityGlobalViewName());

                // Validate data before and after ttl expiration.
                upsertDataAndRunValidations(viewTTL, DEFAULT_NUM_ROWS, dataWriter, dataReader,
                        schemaBuilder);
            }

            PTable table = schemaBuilder.getBaseTable();
            // validate multi-tenanted base table
            validateAfterMajorCompaction(
                    table.getSchemaName().toString(),
                    table.getTableName().toString(),
                    false,
                    earliestTimestamp,
                    VIEW_TTL_10_SECS,
                    false,
                    0
            );

            String fullTableIndexName = schemaBuilder.getPhysicalTableIndexName(false);
            // validate base index table
            validateAfterMajorCompaction(
                    SchemaUtil.getSchemaNameFromFullName(fullTableIndexName),
                    SchemaUtil.getTableNameFromFullName(fullTableIndexName),
                    false,
                    earliestTimestamp,
                    VIEW_TTL_10_SECS,
                    false,
                    0
            );

            // validate multi-tenanted index table
            validateAfterMajorCompaction(
                    table.getSchemaName().toString(),
                    table.getTableName().toString(),
                    true,
                    earliestTimestamp,
                    VIEW_TTL_10_SECS,
                    false,
                    0
            );
        }

    }

    private void deleteData(
            boolean useGlobalConnection,
            String tenantId,
            String viewName,
            long scnTimestamp) throws SQLException {

        Properties props = new Properties();
        props.setProperty("CurrentSCN", Long.toString(scnTimestamp));
        String connectUrl = useGlobalConnection ?
                getUrl() :
                getUrl() + ';' + TENANT_ID_ATTRIB + '=' + tenantId;

        try (Connection deleteConnection = DriverManager.getConnection(connectUrl, props);
                final Statement statement = deleteConnection.createStatement()) {
            deleteConnection.setAutoCommit(true);

            final String deleteIfExpiredStatement = String.format("select /*+NO_INDEX*/ count(*) from  %s", viewName);
            Preconditions.checkNotNull(deleteIfExpiredStatement);

            final PhoenixStatement pstmt = statement.unwrap(PhoenixStatement.class);
            // Optimize the query plan so that we potentially use secondary indexes
            final QueryPlan queryPlan = pstmt.optimizeQuery(deleteIfExpiredStatement);
            final Scan scan = queryPlan.getContext().getScan();

            PTable table = PhoenixRuntime.getTable(deleteConnection, tenantId, viewName);
            byte[] emptyColumnFamilyName = SchemaUtil.getEmptyColumnFamily(table);
            byte[] emptyColumnName =
                    table.getEncodingScheme() == PTable.QualifierEncodingScheme.NON_ENCODED_QUALIFIERS ?
                            QueryConstants.EMPTY_COLUMN_BYTES :
                            table.getEncodingScheme().encode(QueryConstants.ENCODED_EMPTY_COLUMN_NAME);

            scan.setAttribute(BaseScannerRegionObserver.EMPTY_COLUMN_FAMILY_NAME, emptyColumnFamilyName);
            scan.setAttribute(BaseScannerRegionObserver.EMPTY_COLUMN_QUALIFIER_NAME, emptyColumnName);
            scan.setAttribute(BaseScannerRegionObserver.DELETE_PHOENIX_TTL_EXPIRED, PDataType.TRUE_BYTES);
            scan.setAttribute(BaseScannerRegionObserver.TTL, Bytes.toBytes(Long.valueOf(table.getTTL())));

            PhoenixResultSet
                    rs = pstmt.newResultSet(queryPlan.iterator(), queryPlan.getProjector(), queryPlan.getContext());
            while (rs.next());
        }
    }


    private void deleteIndexData(boolean useGlobalConnection,
            String tenantId,
            String indexName,
            long scnTimestamp) throws SQLException {

        Properties props = new Properties();
        props.setProperty("CurrentSCN", Long.toString(scnTimestamp));
        String connectUrl = useGlobalConnection ?
                getUrl() :
                getUrl() + ';' + TENANT_ID_ATTRIB + '=' + tenantId;

        try (Connection deleteConnection = DriverManager.getConnection(connectUrl, props);
                final Statement statement = deleteConnection.createStatement()) {
            deleteConnection.setAutoCommit(true);

            final String deleteIfExpiredStatement = String.format("select count(*) from %s", indexName);
            Preconditions.checkNotNull(deleteIfExpiredStatement);

            final PhoenixStatement pstmt = statement.unwrap(PhoenixStatement.class);
            // Optimize the query plan so that we potentially use secondary indexes
            final QueryPlan queryPlan = pstmt.optimizeQuery(deleteIfExpiredStatement);
            final Scan scan = queryPlan.getContext().getScan();

            PTable table = PhoenixRuntime.getTable(deleteConnection, tenantId, indexName);

            byte[] emptyColumnFamilyName = SchemaUtil.getEmptyColumnFamily(table);
            byte[] emptyColumnName =
                    table.getEncodingScheme() == PTable.QualifierEncodingScheme.NON_ENCODED_QUALIFIERS ?
                            QueryConstants.EMPTY_COLUMN_BYTES :
                            table.getEncodingScheme().encode(QueryConstants.ENCODED_EMPTY_COLUMN_NAME);

            scan.setAttribute(BaseScannerRegionObserver.EMPTY_COLUMN_FAMILY_NAME, emptyColumnFamilyName);
            scan.setAttribute(BaseScannerRegionObserver.EMPTY_COLUMN_QUALIFIER_NAME, emptyColumnName);
            scan.setAttribute(BaseScannerRegionObserver.DELETE_PHOENIX_TTL_EXPIRED, PDataType.TRUE_BYTES);
            scan.setAttribute(BaseScannerRegionObserver.TTL, Bytes.toBytes(Long.valueOf(table.getTTL())));
            PhoenixResultSet
                    rs = pstmt.newResultSet(queryPlan.iterator(), queryPlan.getProjector(), queryPlan.getContext());
            while (rs.next());
        }
    }


}