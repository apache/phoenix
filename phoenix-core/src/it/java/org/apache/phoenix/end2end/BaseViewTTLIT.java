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

import org.apache.hadoop.hbase.CompareOperator;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.CompactionState;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.RegionInfo;
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
import org.apache.phoenix.jdbc.PhoenixConnection;
import org.apache.phoenix.jdbc.PhoenixDatabaseMetaData;
import org.apache.phoenix.query.PhoenixTestBuilder;
import org.apache.phoenix.query.QueryConstants;
import org.apache.phoenix.query.QueryServices;
import org.apache.phoenix.query.PhoenixTestBuilder.BasicDataReader;
import org.apache.phoenix.query.PhoenixTestBuilder.BasicDataWriter;
import org.apache.phoenix.query.PhoenixTestBuilder.DataReader;
import org.apache.phoenix.query.PhoenixTestBuilder.DataSupplier;
import org.apache.phoenix.query.PhoenixTestBuilder.DataWriter;
import org.apache.phoenix.query.PhoenixTestBuilder.SchemaBuilder;
import org.apache.phoenix.query.PhoenixTestBuilder.SchemaBuilder.ConnectOptions;
import org.apache.phoenix.query.PhoenixTestBuilder.SchemaBuilder.DataOptions;
import org.apache.phoenix.query.PhoenixTestBuilder.SchemaBuilder.GlobalViewIndexOptions;
import org.apache.phoenix.query.PhoenixTestBuilder.SchemaBuilder.GlobalViewOptions;
import org.apache.phoenix.query.PhoenixTestBuilder.SchemaBuilder.OtherOptions;
import org.apache.phoenix.query.PhoenixTestBuilder.SchemaBuilder.TableOptions;
import org.apache.phoenix.query.PhoenixTestBuilder.SchemaBuilder.TenantViewIndexOptions;
import org.apache.phoenix.query.PhoenixTestBuilder.SchemaBuilder.TenantViewOptions;

import org.apache.phoenix.schema.PName;
import org.apache.phoenix.schema.PTable;

import org.apache.phoenix.schema.PTableKey;
import org.apache.phoenix.schema.types.PDataType;
import org.apache.phoenix.thirdparty.com.google.common.base.Joiner;
import org.apache.phoenix.thirdparty.com.google.common.collect.Lists;
import org.apache.phoenix.thirdparty.com.google.common.collect.Maps;
import org.apache.phoenix.util.EnvironmentEdgeManager;
import org.apache.phoenix.util.ManualEnvironmentEdge;
import org.apache.phoenix.util.ReadOnlyProps;
import org.apache.phoenix.util.SchemaUtil;
import org.apache.phoenix.util.TestUtil;
import org.junit.After;
import org.junit.Before;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Types;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Random;
import java.util.Set;

import static java.util.Arrays.asList;
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

public abstract class BaseViewTTLIT extends ParallelStatsDisabledIT {
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
    static final int MAX_COMPACTION_CHECKS = 200;

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

    public void testResetServerCache() {
        try {
            clearCache(true, true, Arrays.asList(new String[] {"00DNA0000000MXY"}));
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    private void clearCache(boolean globalFixNeeded, boolean tenantFixNeeded, List<String> allTenants)
            throws SQLException {

        if (globalFixNeeded) {
            try (PhoenixConnection globalConnection = DriverManager.getConnection(getUrl()).unwrap(PhoenixConnection.class)) {
                clearCache(globalConnection, "PLATFORM_ENTITY", "DECISION_TABLE_RECORDSET");
            }
        }
        if (tenantFixNeeded || globalFixNeeded) {
            for (String tenantId : allTenants) {
                String tenantURL = getUrl() + ';' + TENANT_ID_ATTRIB + '=' + tenantId;
                try (Connection tenantConnection = DriverManager.getConnection(tenantURL)) {
                    clearCache(tenantConnection, "PLATFORM_ENTITY", "195");
                }
            }
        }
    }

    private static void clearCache(Connection tenantConnection, String schemaName, String tableName) throws SQLException {

        PhoenixConnection currentConnection = tenantConnection.unwrap(PhoenixConnection.class);
        PName tenantIdName = currentConnection.getTenantId();
        String tenantId = tenantIdName == null ? "" : tenantIdName.getString();

        // Clear server side cache
        currentConnection.unwrap(PhoenixConnection.class).getQueryServices().clearTableFromCache(
                Bytes.toBytes(tenantId), Bytes.toBytes(schemaName), Bytes.toBytes(tableName), 0);

        // Clear connection cache
        currentConnection.getMetaDataCache().removeTable(currentConnection.getTenantId(),
                String.format("%s.%s", schemaName, tableName), null, 0);
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
            String ttlStr = rs.next() ? rs.getString(1) : null;
            long actualTTLValueReturned = ttlStr != null ? Integer.valueOf(ttlStr): 0;

            assertEquals(String.format("Expected rows do not match for schema = %s, table = %s",
                    schemaName, tableName), ttlValueExpected, actualTTLValueReturned);
        }
    }

    String stripQuotes(String name) {
        return name.replace("\"", "");
    }


    protected SchemaBuilder createLevel2TenantViewWithGlobalLevelTTL(
            TenantViewOptions tenantViewOptions,
            TenantViewIndexOptions tenantViewIndexOptions,
            boolean allowIndex) throws Exception {
        // Define the test schema.
        // 1. Table with columns => (ORG_ID, KP, COL1, COL2, COL3), PK => (ORG_ID, KP)
        // 2. GlobalView with columns => (ID, COL4, COL5, COL6), PK => (ID)
        // 3. Tenant with columns => (ZID, COL7, COL8, COL9), PK => (ZID)
        final SchemaBuilder schemaBuilder = new SchemaBuilder(getUrl());

        TableOptions tableOptions = TableOptions.withDefaults();
        tableOptions.setTableProps("COLUMN_ENCODED_BYTES=0,MULTI_TENANT=true");

        GlobalViewOptions
                globalViewOptions = GlobalViewOptions.withDefaults();
        // View TTL is set to 300s => 300000 ms
        globalViewOptions.setTableProps("TTL=300");

        GlobalViewIndexOptions globalViewIndexOptions
                = GlobalViewIndexOptions.withDefaults();
        globalViewIndexOptions.setLocal(false);

        TenantViewOptions
                tenantViewWithOverrideOptions = TenantViewOptions.withDefaults();
        if (tenantViewOptions != null) {
            tenantViewWithOverrideOptions = tenantViewOptions;
        }
        TenantViewIndexOptions
                tenantViewIndexOverrideOptions = TenantViewIndexOptions.withDefaults();
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
                    .withTenantViewIndexOptions(tenantViewIndexOverrideOptions)
                    .buildWithNewTenant();
        }
        return schemaBuilder;
    }

    protected SchemaBuilder createLevel2TenantViewWithTenantLevelTTL(
            TenantViewOptions tenantViewOptions, TenantViewIndexOptions tenantViewIndexOptions)
            throws Exception {
        // Define the test schema.
        // 1. Table with columns => (ORG_ID, KP, COL1, COL2, COL3), PK => (ORG_ID, KP)
        // 2. GlobalView with columns => (ID, COL4, COL5, COL6), PK => (ID)
        // 3. Tenant with columns => (ZID, COL7, COL8, COL9), PK => (ZID)
        final SchemaBuilder schemaBuilder = new SchemaBuilder(getUrl());

        TableOptions
                tableOptions = TableOptions.withDefaults();
        tableOptions.setTableProps("COLUMN_ENCODED_BYTES=0,MULTI_TENANT=true");

        GlobalViewOptions
                globalViewOptions = GlobalViewOptions.withDefaults();

        TenantViewOptions
                tenantViewWithOverrideOptions = TenantViewOptions.withDefaults();
        // View TTL is set to 300s => 300000 ms
        tenantViewWithOverrideOptions.setTableProps("TTL=300");
        if (tenantViewOptions != null) {
            tenantViewWithOverrideOptions = tenantViewOptions;
        }
        TenantViewIndexOptions
                tenantViewIndexOverrideOptions = TenantViewIndexOptions.withDefaults();
        if (tenantViewIndexOptions != null) {
            tenantViewIndexOverrideOptions = tenantViewIndexOptions;
        }
        schemaBuilder.withTableOptions(tableOptions).withGlobalViewOptions(globalViewOptions)
                .withTenantViewOptions(tenantViewWithOverrideOptions)
                .withTenantViewIndexOptions(tenantViewIndexOverrideOptions).buildWithNewTenant();
        return schemaBuilder;
    }

    protected SchemaBuilder createLevel1TenantView(
            TenantViewOptions tenantViewOptions,
            TenantViewIndexOptions tenantViewIndexOptions) throws Exception {
        // Define the test schema.
        // 1. Table with default columns => (ORG_ID, KP, COL1, COL2, COL3), PK => (ORG_ID, KP)
        // 2. Tenant with default columns => (ZID, COL7, COL8, COL9), PK => (ZID)
        final SchemaBuilder schemaBuilder = new SchemaBuilder(getUrl());

        TableOptions
                tableOptions = TableOptions.withDefaults();
        tableOptions.setTableProps("COLUMN_ENCODED_BYTES=0,MULTI_TENANT=true");

        TenantViewOptions
                tenantViewOverrideOptions = TenantViewOptions.withDefaults();
        if (tenantViewOptions != null) {
            tenantViewOverrideOptions = tenantViewOptions;
        }
        TenantViewIndexOptions
                tenantViewIndexOverrideOptions = TenantViewIndexOptions.withDefaults();
        if (tenantViewIndexOptions != null) {
            tenantViewIndexOverrideOptions = tenantViewIndexOptions;
        }

        schemaBuilder.withTableOptions(tableOptions)
                .withTenantViewOptions(tenantViewOverrideOptions)
                .withTenantViewIndexOptions(tenantViewIndexOverrideOptions).buildNewView();
        return schemaBuilder;
    }


    void upsertDataAndRunValidations(long viewTTL, int numRowsToUpsert,
            DataWriter dataWriter, DataReader dataReader, SchemaBuilder schemaBuilder)
            throws Exception {

        //Insert for the first time and validate them.
        validateExpiredRowsAreNotReturnedUsingData(viewTTL, upsertData(dataWriter, numRowsToUpsert),
                dataReader, schemaBuilder);

        // Update the above rows and validate the same.
        validateExpiredRowsAreNotReturnedUsingData(viewTTL, upsertData(dataWriter, numRowsToUpsert),
                dataReader, schemaBuilder);

    }

    void validateExpiredRowsAreNotReturnedUsingCounts(long viewTTL, DataReader dataReader,
            SchemaBuilder schemaBuilder) throws IOException, SQLException {

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
            DataReader dataReader, SchemaBuilder schemaBuilder) throws SQLException {

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

    void validateRowsAreNotMaskedUsingCounts(long probeTimestamp, DataReader dataReader,
            SchemaBuilder schemaBuilder) throws SQLException {

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
            DataWriter dataWriter, int numRowsToUpsert) throws Exception {
        // Upsert rows
        dataWriter.upsertRows(1, numRowsToUpsert);
        return dataWriter.getDataTable();
    }

    org.apache.phoenix.thirdparty.com.google.common.collect.Table<String, String, Object> fetchData(
            DataReader dataReader) throws SQLException {

        dataReader.readRows();
        return dataReader.getDataTable();
    }

    void majorCompact(TableName table, long scnTimestamp, boolean flushOnly) throws Exception {
        try (org.apache.hadoop.hbase.client.Connection connection =
                ConnectionFactory.createConnection(getUtility().getConfiguration())) {
            Admin admin = connection.getAdmin();
            if (!admin.tableExists(table)) {
                return;
            }

            admin.flush(table);
            if (flushOnly) {
                return;
            }
            EnvironmentEdgeManager.injectEdge(injectEdge);
            injectEdge.setValue(scnTimestamp);
            TestUtil.majorCompact(getUtility(), table);
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
            LOGGER.info(String.format("########## compacting shared index = %s",
                    hbaseViewIndexTableName));
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

    List<OtherOptions> getTableAndGlobalAndTenantColumnFamilyOptions() {

        List<OtherOptions> testCases = Lists.newArrayList();

        OtherOptions
                testCaseWhenAllCFMatchAndAllDefault = new OtherOptions();
        testCaseWhenAllCFMatchAndAllDefault.setTestName("testCaseWhenAllCFMatchAndAllDefault");
        testCaseWhenAllCFMatchAndAllDefault
                .setTableCFs(Lists.newArrayList((String) null, null, null));
        testCaseWhenAllCFMatchAndAllDefault
                .setGlobalViewCFs(Lists.newArrayList((String) null, null, null));
        testCaseWhenAllCFMatchAndAllDefault
                .setTenantViewCFs(Lists.newArrayList((String) null, null, null));
        testCases.add(testCaseWhenAllCFMatchAndAllDefault);

        OtherOptions
                testCaseWhenAllCFMatchAndSame = new OtherOptions();
        testCaseWhenAllCFMatchAndSame.setTestName("testCaseWhenAllCFMatchAndSame");
        testCaseWhenAllCFMatchAndSame.setTableCFs(Lists.newArrayList("A", "A", "A"));
        testCaseWhenAllCFMatchAndSame.setGlobalViewCFs(Lists.newArrayList("A", "A", "A"));
        testCaseWhenAllCFMatchAndSame.setTenantViewCFs(Lists.newArrayList("A", "A", "A"));
        testCases.add(testCaseWhenAllCFMatchAndSame);

        OtherOptions
                testCaseWhenAllCFMatch = new OtherOptions();
        testCaseWhenAllCFMatch.setTestName("testCaseWhenAllCFMatch");
        testCaseWhenAllCFMatch.setTableCFs(Lists.newArrayList(null, "A", "B"));
        testCaseWhenAllCFMatch.setGlobalViewCFs(Lists.newArrayList(null, "A", "B"));
        testCaseWhenAllCFMatch.setTenantViewCFs(Lists.newArrayList(null, "A", "B"));
        testCases.add(testCaseWhenAllCFMatch);

        OtherOptions
                testCaseWhenTableCFsAreDiff = new OtherOptions();
        testCaseWhenTableCFsAreDiff.setTestName("testCaseWhenTableCFsAreDiff");
        testCaseWhenTableCFsAreDiff.setTableCFs(Lists.newArrayList(null, "A", "B"));
        testCaseWhenTableCFsAreDiff.setGlobalViewCFs(Lists.newArrayList("A", "A", "B"));
        testCaseWhenTableCFsAreDiff.setTenantViewCFs(Lists.newArrayList("A", "A", "B"));
        testCases.add(testCaseWhenTableCFsAreDiff);

        OtherOptions
                testCaseWhenGlobalAndTenantCFsAreDiff = new OtherOptions();
        testCaseWhenGlobalAndTenantCFsAreDiff.setTestName("testCaseWhenGlobalAndTenantCFsAreDiff");
        testCaseWhenGlobalAndTenantCFsAreDiff.setTableCFs(Lists.newArrayList(null, "A", "B"));
        testCaseWhenGlobalAndTenantCFsAreDiff.setGlobalViewCFs(Lists.newArrayList("A", "A", "A"));
        testCaseWhenGlobalAndTenantCFsAreDiff.setTenantViewCFs(Lists.newArrayList("B", "B", "B"));
        testCases.add(testCaseWhenGlobalAndTenantCFsAreDiff);

        return testCases;
    }

    void runValidations(long viewTTL,
            org.apache.phoenix.thirdparty.com.google.common.collect.Table<String, String, Object> table,
            DataReader dataReader, SchemaBuilder schemaBuilder)
            throws Exception {

        //Insert for the first time and validate them.
        validateExpiredRowsAreNotReturnedUsingData(viewTTL, table,
                dataReader, schemaBuilder);

        // Update the above rows and validate the same.
        validateExpiredRowsAreNotReturnedUsingData(viewTTL, table,
                dataReader, schemaBuilder);

    }

    /**
     * Test case:
     * TTL set at the view level
     * Salted
     * MultiTenant and NonMultiTenanted Tables and Views
     * Global and Tenant Indexes
     * @throws Exception
     */
    protected void testMajorCompactWithSaltedIndexedTenantView() throws Exception {

        // View TTL is set in seconds (for e.g 10 secs)
        int viewTTL = VIEW_TTL_10_SECS;
        TableOptions
                tableOptions = TableOptions.withDefaults();
        String tableProps = "COLUMN_ENCODED_BYTES=0,DEFAULT_COLUMN_FAMILY='0'";
        tableOptions.setTableProps(tableProps);
        tableOptions.setSaltBuckets(3);
        for (boolean isMultiTenant : Lists.newArrayList(true, false)) {

            resetEnvironmentEdgeManager();
            tableOptions.setMultiTenant(isMultiTenant);
            DataOptions dataOptions =  isMultiTenant ?
                    DataOptions.withDefaults() :
                    DataOptions.withPrefix("SALTED");

            // OID, KP for non multi-tenanted views
            int tenantNumber = dataOptions.getNextTenantNumber();
            String orgId =
                    String.format(PhoenixTestBuilder.DDLDefaults.DEFAULT_ALT_TENANT_ID_FMT,
                            tenantNumber,
                            dataOptions.getUniqueName());
            String keyPrefix = "SLT";

            // Define the test schema.
            final SchemaBuilder
                    schemaBuilder = new SchemaBuilder(getUrl());

            if (isMultiTenant) {
                TenantViewOptions
                        tenantViewOptions = TenantViewOptions.withDefaults();
                tenantViewOptions.setTableProps(String.format("TTL=%d", viewTTL));
                schemaBuilder
                        .withTableOptions(tableOptions)
                        .withTenantViewOptions(tenantViewOptions)
                        .withDataOptions(dataOptions)
                        .withTenantViewIndexDefaults()
                        .buildWithNewTenant();
            } else {

                GlobalViewOptions
                        globalViewOptions = new GlobalViewOptions();
                globalViewOptions.setSchemaName(PhoenixTestBuilder.DDLDefaults.DEFAULT_SCHEMA_NAME);
                globalViewOptions.setGlobalViewColumns(Lists.newArrayList(TENANT_VIEW_COLUMNS));
                globalViewOptions.setGlobalViewColumnTypes(Lists.newArrayList(COLUMN_TYPES));
                globalViewOptions.setGlobalViewPKColumns(Lists.newArrayList(TENANT_VIEW_PK_COLUMNS));
                globalViewOptions.setGlobalViewPKColumnTypes(Lists.newArrayList(TENANT_VIEW_PK_TYPES));
                globalViewOptions.setTableProps(String.format("TTL=%d", viewTTL));

                GlobalViewIndexOptions
                        globalViewIndexOptions = new GlobalViewIndexOptions();
                globalViewIndexOptions.setGlobalViewIndexColumns(
                        Lists.newArrayList(TENANT_VIEW_INDEX_COLUMNS));
                globalViewIndexOptions.setGlobalViewIncludeColumns(
                        Lists.newArrayList(TENANT_VIEW_INCLUDE_COLUMNS));

                globalViewOptions.setGlobalViewCondition(String.format(
                        "SELECT * FROM %s.%s WHERE OID = '%s' AND KP = '%s'",
                        dataOptions.getSchemaName(), dataOptions.getTableName(), orgId, keyPrefix));
                ConnectOptions
                        connectOptions = new ConnectOptions();
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
            DataSupplier dataSupplier = new DataSupplier() {

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
            DataWriter dataWriter = new BasicDataWriter();
            DataReader dataReader = new BasicDataReader();

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
                upsertDataAndRunValidations(viewTTL, DEFAULT_NUM_ROWS * 3, dataWriter, dataReader,
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

    /**
     * Test case:
     * TTL set at the Tenant view level
     * MultiTenant Views
     * @throws Exception
     */
    protected void testMajorCompactWithOnlyTenantView() throws Exception {

        // View TTL is set in seconds (for e.g 10 secs)
        int viewTTL = VIEW_TTL_10_SECS;
        TableOptions
                tableOptions = TableOptions.withDefaults();
        String tableProps = "MULTI_TENANT=true,COLUMN_ENCODED_BYTES=0,DEFAULT_COLUMN_FAMILY='0'";
        tableOptions.setTableProps(tableProps);

        TenantViewOptions
                tenantViewOptions = TenantViewOptions.withDefaults();
        tenantViewOptions.setTableProps(String.format("TTL=%d", viewTTL));

        // Define the test schema.
        final SchemaBuilder schemaBuilder = new SchemaBuilder(getUrl());
        schemaBuilder
                .withTableOptions(tableOptions)
                .withTenantViewOptions(tenantViewOptions)
                .buildWithNewTenant();

        // Define the test data.
        DataSupplier dataSupplier = new DataSupplier() {

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
        DataWriter dataWriter = new BasicDataWriter();
        DataReader dataReader = new BasicDataReader();

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

    /**
     * Test case:
     * TTL set at the Global View level
     * MultiTenant Tables and Views
     * Global and Local Indexes
     * @throws Exception
     */

    protected void testMajorCompactFromMultipleGlobalIndexes() throws Exception {

        // View TTL is set in seconds (for e.g 10 secs)
        int viewTTL = VIEW_TTL_10_SECS;
        TableOptions
                tableOptions = TableOptions.withDefaults();
        String tableProps = "MULTI_TENANT=true,COLUMN_ENCODED_BYTES=0,DEFAULT_COLUMN_FAMILY='0'";
        tableOptions.setTableProps(tableProps);

        GlobalViewOptions
                globalViewOptions = GlobalViewOptions.withDefaults();
        globalViewOptions.setTableProps(String.format("TTL=%d", viewTTL));

        GlobalViewIndexOptions
                globalViewIndexOptions = GlobalViewIndexOptions.withDefaults();

        TenantViewOptions
                tenantViewOptions = new TenantViewOptions();
        tenantViewOptions.setTenantViewColumns(Lists.newArrayList(TENANT_VIEW_COLUMNS));
        tenantViewOptions.setTenantViewColumnTypes(Lists.newArrayList(COLUMN_TYPES));

        // Test cases :
        // Local vs Global indexes, various column family options.
        for (boolean isIndex1Local : Lists.newArrayList(true, false)) {
            for (boolean isIndex2Local : Lists.newArrayList(true, false)) {
                for (OtherOptions options : getTableAndGlobalAndTenantColumnFamilyOptions()) {

                    resetEnvironmentEdgeManager();
                    // Define the test schema.
                    final SchemaBuilder
                            schemaBuilder = new SchemaBuilder(getUrl());

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

                    PTable table = schemaBuilder.getBaseTable();
                    String schemaName = table.getSchemaName().getString();
                    String tableName = table.getTableName().getString();

                    long earliestTimestamp = EnvironmentEdgeManager.currentTimeMillis();
                    // Create multiple tenants, add data and run validations
                    for (int tenant : Arrays.asList(new Integer[] { 1, 2, 3 })) {
                        // build schema for tenant
                        DataOptions dataOptions = schemaBuilder.getDataOptions();
                        dataOptions.getNextTenantId();
                        schemaBuilder.buildWithNewTenant();

                        String tenantId = schemaBuilder.getDataOptions().getTenantId();
                        LOGGER.debug(String.format("Building new view with tenant id %s on %s.%s",
                                tenantId, schemaName, tableName));

                        // Define the test data.
                        DataSupplier
                                dataSupplier = new DataSupplier() {

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
                        DataWriter
                                dataWriter = new BasicDataWriter();
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
                            DataReader
                                    dataReader = new BasicDataReader();
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
                    // validate multi-tenanted base table
                    validateAfterMajorCompaction(
                            schemaName,
                            tableName,
                            false,
                            earliestTimestamp,
                            VIEW_TTL_10_SECS,
                            false,
                            0
                    );
                    // validate multi-tenanted index table
                    validateAfterMajorCompaction(
                            schemaName,
                            tableName,
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

    /**
     * Test case:
     * TTL set at the Tenant View level
     * MultiTenant Tables and Views
     * Tenant and Local Indexes
     * @throws Exception
     */
    protected void testMajorCompactFromMultipleTenantIndexes() throws Exception {

        // View TTL is set in seconds (for e.g 10 secs)
        int viewTTL = VIEW_TTL_10_SECS;
        TableOptions
                tableOptions = TableOptions.withDefaults();
        String tableProps = "MULTI_TENANT=true,COLUMN_ENCODED_BYTES=0,DEFAULT_COLUMN_FAMILY='0'";
        tableOptions.setTableProps(tableProps);

        GlobalViewOptions
                globalViewOptions = GlobalViewOptions.withDefaults();

        TenantViewOptions
                tenantViewOptions = new TenantViewOptions();
        tenantViewOptions.setTenantViewColumns(Lists.newArrayList(TENANT_VIEW_COLUMNS));
        tenantViewOptions.setTenantViewColumnTypes(Lists.newArrayList(COLUMN_TYPES));
        tenantViewOptions.setTableProps(String.format("TTL=%d", viewTTL));

        TenantViewIndexOptions
                tenantViewIndexOptions = TenantViewIndexOptions.withDefaults();

        // Test cases :
        // Local vs Global indexes, various column family options.
        for (boolean isIndex1Local : Lists.newArrayList(true, false)) {
            for (boolean isIndex2Local : Lists.newArrayList(true, false)) {
                for (OtherOptions options : getTableAndGlobalAndTenantColumnFamilyOptions()) {

                    resetEnvironmentEdgeManager();
                    // Define the test schema.
                    final SchemaBuilder
                            schemaBuilder = new SchemaBuilder(getUrl());
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
                        DataOptions dataOptions = schemaBuilder.getDataOptions();
                        dataOptions.getNextTenantId();
                        schemaBuilder.buildWithNewTenant();

                        String tenantId = schemaBuilder.getDataOptions().getTenantId();
                        LOGGER.debug(String.format("Building new view with tenant id %s on %s.%s",
                                tenantId, schemaName, tableName));
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
                        DataSupplier
                                dataSupplier = new DataSupplier() {

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
                        DataWriter
                                dataWriter = new BasicDataWriter();
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
                            DataReader
                                    dataReader = new BasicDataReader();
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

    /**
     * Test case:
     * TTL set at the table level
     * MultiTenant and Non-MultiTenant Tables and
     * Single level views.
     * @throws Exception
     */

    protected void testMajorCompactWithSimpleIndexedBaseTables() throws Exception {

        // View TTL is set in seconds (for e.g 10 secs)
        int viewTTL = VIEW_TTL_10_SECS;
        TableOptions
                tableOptions = TableOptions.withDefaults();
        String tableProps = "COLUMN_ENCODED_BYTES=0,DEFAULT_COLUMN_FAMILY='0',TTL=10";
        tableOptions.setTableProps(tableProps);
        tableOptions.getTablePKColumns().add("ZID");
        tableOptions.getTablePKColumnTypes().add("CHAR(15)");
        for (boolean isMultiTenant : Lists.newArrayList(true, false)) {

            resetEnvironmentEdgeManager();
            tableOptions.setMultiTenant(isMultiTenant);
            DataOptions dataOptions =  isMultiTenant ?
                    DataOptions.withDefaults() :
                    DataOptions.withPrefix("TABLE1");

            // OID, KP for non multi-tenanted views
            int viewCounter = 1;
            String orgId =
                    String.format(PhoenixTestBuilder.DDLDefaults.DEFAULT_ALT_TENANT_ID_FMT,
                            viewCounter,
                            dataOptions.getUniqueName());
            String keyPrefix = "T01";

            // Define the test schema.
            final SchemaBuilder
                    schemaBuilder = new SchemaBuilder(getUrl());

            if (isMultiTenant) {
                TenantViewOptions
                        tenantViewOptions = TenantViewOptions.withDefaults();
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

                GlobalViewOptions
                        globalViewOptions = new GlobalViewOptions();
                globalViewOptions.setSchemaName(PhoenixTestBuilder.DDLDefaults.DEFAULT_SCHEMA_NAME);
                globalViewOptions.setGlobalViewColumns(Lists.newArrayList(TENANT_VIEW_COLUMNS));
                globalViewOptions.setGlobalViewColumnTypes(Lists.newArrayList(COLUMN_TYPES));

                GlobalViewIndexOptions
                        globalViewIndexOptions = new GlobalViewIndexOptions();
                globalViewIndexOptions.setGlobalViewIndexColumns(
                        Lists.newArrayList(TENANT_VIEW_INDEX_COLUMNS));
                globalViewIndexOptions.setGlobalViewIncludeColumns(
                        Lists.newArrayList(TENANT_VIEW_INCLUDE_COLUMNS));

                globalViewOptions.setGlobalViewCondition(String.format(
                        "SELECT * FROM %s.%s WHERE OID = '%s' AND KP = '%s'",
                        dataOptions.getSchemaName(), dataOptions.getTableName(), orgId, keyPrefix));
                ConnectOptions
                        connectOptions = new ConnectOptions();
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
            DataSupplier dataSupplier = new DataSupplier() {

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
            DataWriter dataWriter = new BasicDataWriter();
            DataReader dataReader = new BasicDataReader();

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

    /**
     * Test case:
     * TTL set at the table level
     * Salted
     * MultiTenant and NonMultiTenanted Tables and Views
     * No Global and Tenant Indexes
     * @throws Exception
     */
    protected void testMajorCompactWithSaltedIndexedBaseTables() throws Exception {

        // View TTL is set in seconds (for e.g 10 secs)
        int viewTTL = VIEW_TTL_10_SECS;
        TableOptions
                tableOptions = TableOptions.withDefaults();
        String tableProps = "COLUMN_ENCODED_BYTES=0,DEFAULT_COLUMN_FAMILY='0',TTL=10";
        tableOptions.setTableProps(tableProps);
        tableOptions.setSaltBuckets(1);
        tableOptions.getTablePKColumns().add("ZID");
        tableOptions.getTablePKColumnTypes().add("CHAR(15)");
        for (boolean isMultiTenant : Lists.newArrayList(true, false)) {

            resetEnvironmentEdgeManager();
            tableOptions.setMultiTenant(isMultiTenant);
            DataOptions dataOptions =  isMultiTenant ?
                    DataOptions.withDefaults() :
                    DataOptions.withPrefix("SALTED");

            // OID, KP for non multi-tenanted views
            int viewCounter = 1;
            String orgId =
                    String.format(PhoenixTestBuilder.DDLDefaults.DEFAULT_ALT_TENANT_ID_FMT,
                            viewCounter,
                            dataOptions.getUniqueName());
            String keyPrefix = "SLT";

            // Define the test schema.
            final SchemaBuilder
                    schemaBuilder = new SchemaBuilder(getUrl());

            if (isMultiTenant) {
                TenantViewOptions
                        tenantViewOptions = TenantViewOptions.withDefaults();
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

                GlobalViewOptions
                        globalViewOptions = new GlobalViewOptions();
                globalViewOptions.setSchemaName(PhoenixTestBuilder.DDLDefaults.DEFAULT_SCHEMA_NAME);
                globalViewOptions.setGlobalViewColumns(Lists.newArrayList(TENANT_VIEW_COLUMNS));
                globalViewOptions.setGlobalViewColumnTypes(Lists.newArrayList(COLUMN_TYPES));
                //globalViewOptions.setGlobalViewPKColumns(Lists.newArrayList(TENANT_VIEW_PK_COLUMNS));
                //globalViewOptions.setGlobalViewPKColumnTypes(Lists.newArrayList(TENANT_VIEW_PK_TYPES));

                GlobalViewIndexOptions
                        globalViewIndexOptions = new GlobalViewIndexOptions();
                globalViewIndexOptions.setGlobalViewIndexColumns(
                        Lists.newArrayList(TENANT_VIEW_INDEX_COLUMNS));
                globalViewIndexOptions.setGlobalViewIncludeColumns(
                        Lists.newArrayList(TENANT_VIEW_INCLUDE_COLUMNS));

                globalViewOptions.setGlobalViewCondition(String.format(
                        "SELECT * FROM %s.%s WHERE OID = '%s' AND KP = '%s'",
                        dataOptions.getSchemaName(), dataOptions.getTableName(), orgId, keyPrefix));
                ConnectOptions
                        connectOptions = new ConnectOptions();
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
            DataSupplier dataSupplier = new DataSupplier() {

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
            DataWriter dataWriter = new BasicDataWriter();
            DataReader dataReader = new BasicDataReader();

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

    /**
     * Test case:
     * TTL set at the global view level
     * MultiTenant Tables and Views
     * Global and Local Indexes
     * Various Column Family options
     * @throws Exception
     */
    protected void testMajorCompactWithVariousViewsAndOptions() throws Exception {

        // View TTL is set in seconds (for e.g 10 secs)
        int viewTTL = VIEW_TTL_10_SECS;

        // Define the test schema
        TableOptions
                tableOptions = TableOptions.withDefaults();
        String tableProps = "MULTI_TENANT=true,COLUMN_ENCODED_BYTES=0,DEFAULT_COLUMN_FAMILY='0'";
        tableOptions.setTableProps(tableProps);

        GlobalViewOptions
                globalViewOptions = GlobalViewOptions.withDefaults();
        globalViewOptions.setTableProps(String.format("TTL=%d", viewTTL));

        GlobalViewIndexOptions
                globalViewIndexOptions = GlobalViewIndexOptions.withDefaults();

        TenantViewOptions
                tenantViewOptions = new TenantViewOptions();
        tenantViewOptions.setTenantViewColumns(Lists.newArrayList(TENANT_VIEW_COLUMNS));
        tenantViewOptions.setTenantViewColumnTypes(Lists.newArrayList(COLUMN_TYPES));

        TenantViewIndexOptions
                tenantViewIndexOptions = TenantViewIndexOptions.withDefaults();
        // TODO handle case when both global and tenant are Local Index cases
        // Test cases :
        // Local vs Global indexes, Tenant vs Global views, various column family options.
        for (boolean isGlobalViewLocal : Lists.newArrayList( true, false)) {
            for (boolean isTenantViewLocal : Lists.newArrayList( true, false)) {
                for (OtherOptions options : getTableAndGlobalAndTenantColumnFamilyOptions()) {

                    /**
                     * TODO:
                     * Need to revisit, not sure why the compaction does complete?
                     */
                    if (isGlobalViewLocal && isTenantViewLocal) continue;

                    resetEnvironmentEdgeManager();
                    globalViewIndexOptions.setLocal(isGlobalViewLocal);
                    tenantViewIndexOptions.setLocal(isTenantViewLocal);

                    final SchemaBuilder
                            schemaBuilder = new SchemaBuilder(getUrl());
                    schemaBuilder.withTableOptions(tableOptions)
                            .withGlobalViewOptions(globalViewOptions)
                            .withGlobalViewIndexOptions(globalViewIndexOptions)
                            .withTenantViewOptions(tenantViewOptions)
                            .withTenantViewIndexOptions(tenantViewIndexOptions)
                            .withOtherOptions(options)
                            .buildWithNewTenant();

                    // Define the test data.
                    DataSupplier
                            dataSupplier = new DataSupplier() {

                        @Override public List<Object> getValues(int rowIndex) {
                            Random rnd = new Random();
                            String id = String.format(ID_FMT, rowIndex);
                            String col1 = String.format(COL1_FMT, rowIndex + rnd.nextInt(MAX_ROWS));
                            String col2 = String.format(COL2_FMT, rowIndex + rnd.nextInt(MAX_ROWS));
                            String col3 = String.format(COL3_FMT, rowIndex + rnd.nextInt(MAX_ROWS));
                            String col4 = String.format(COL4_FMT, rowIndex + rnd.nextInt(MAX_ROWS));
                            String col5 = String.format(COL5_FMT, rowIndex + rnd.nextInt(MAX_ROWS));
                            String col6 = String.format(COL6_FMT, rowIndex + rnd.nextInt(MAX_ROWS));
                            String col7 = String.format(COL7_FMT, rowIndex + rnd.nextInt(MAX_ROWS));
                            String col8 = String.format(COL8_FMT, rowIndex + rnd.nextInt(MAX_ROWS));
                            String col9 = String.format(COL9_FMT, rowIndex + rnd.nextInt(MAX_ROWS));

                            return Lists.newArrayList(
                                    new Object[] { id, col1, col2, col3, col4, col5, col6,
                                            col7, col8, col9 });
                        }
                    };

                    long earliestTimestamp = EnvironmentEdgeManager.currentTimeMillis();
                    // Create a test data reader/writer for the above schema.
                    DataWriter
                            dataWriter = new BasicDataWriter();
                    DataReader
                            dataReader = new BasicDataReader();

                    List<String> columns =
                            Lists.newArrayList("ID",
                                    "COL1", "COL2", "COL3", "COL4", "COL5",
                                    "COL6", "COL7", "COL8", "COL9");
                    List<String> rowKeyColumns = Lists.newArrayList("ID");
                    String tenantConnectUrl =
                            getUrl() + ';' + TENANT_ID_ATTRIB + '=' +
                                    schemaBuilder.getDataOptions().getTenantId();
                    try (Connection writeConnection = DriverManager
                            .getConnection(tenantConnectUrl)) {
                        writeConnection.setAutoCommit(true);
                        dataWriter.setConnection(writeConnection);
                        dataWriter.setDataSupplier(dataSupplier);
                        dataWriter.setUpsertColumns(columns);
                        dataWriter.setRowKeyColumns(rowKeyColumns);
                        dataWriter.setTargetEntity(schemaBuilder.getEntityTenantViewName());

                        dataReader.setValidationColumns(columns);
                        dataReader.setRowKeyColumns(rowKeyColumns);
                        dataReader.setDML(String
                                .format("SELECT %s from %s", Joiner.on(",").join(columns),
                                        schemaBuilder.getEntityTenantViewName()));
                        dataReader.setTargetEntity(schemaBuilder.getEntityTenantViewName());

                        // Validate data before and after ttl expiration.
                        upsertDataAndRunValidations(viewTTL, DEFAULT_NUM_ROWS, dataWriter,
                                dataReader, schemaBuilder);
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
        }
    }

    /**
     * Test case:
     * TTL set at the tenant view level only for some tenants.
     * MultiTenant Tables and Views
     * @throws Exception
     */
    protected void testMajorCompactWhenTTLSetForSomeTenants() throws Exception {

        // View TTL is set in seconds (for e.g 10 secs)
        int viewTTL = VIEW_TTL_10_SECS;
        TableOptions
                tableOptions = TableOptions.withDefaults();
        tableOptions.getTableColumns().clear();
        tableOptions.getTableColumnTypes().clear();

        GlobalViewOptions
                globalViewOptions = GlobalViewOptions.withDefaults();

        TenantViewOptions
                tenantViewOptions = new TenantViewOptions();
        tenantViewOptions.setTenantViewColumns(asList("ZID", "COL7", "COL8", "COL9"));
        tenantViewOptions
                .setTenantViewColumnTypes(asList("CHAR(15)", "VARCHAR", "VARCHAR", "VARCHAR"));

        OtherOptions
                testCaseWhenAllCFMatchAndAllDefault = new OtherOptions();
        testCaseWhenAllCFMatchAndAllDefault.setTestName("testCaseWhenAllCFMatchAndAllDefault");
        testCaseWhenAllCFMatchAndAllDefault
                .setTableCFs(Lists.newArrayList((String) null, null, null));
        testCaseWhenAllCFMatchAndAllDefault
                .setGlobalViewCFs(Lists.newArrayList((String) null, null, null));
        testCaseWhenAllCFMatchAndAllDefault
                .setTenantViewCFs(Lists.newArrayList((String) null, null, null, null));

        final SchemaBuilder schemaBuilder = new SchemaBuilder(getUrl());
        schemaBuilder.withTableOptions(tableOptions).withGlobalViewOptions(globalViewOptions)
                .withTenantViewOptions(tenantViewOptions)
                .withDataOptionsDefaults()
                .withOtherOptions(testCaseWhenAllCFMatchAndAllDefault);


        Map<String, List<String>> mapOfTenantIndexes = Maps.newHashMap();

        long earliestTimestamp = EnvironmentEdgeManager.currentTimeMillis();
        Set<Integer> hasTTLSet = new HashSet<>(Arrays.asList(new Integer[] { 2, 3, 7 }));
        Set<Integer> tenantSet = new HashSet<>(Arrays.asList(new Integer[] { 1, 2, 3, 4, 5, 6, 7 }));
        int nonCompactedTenantSet = tenantSet.size() - hasTTLSet.size();
        boolean isIndex1Local = false;
        boolean isIndex2Local = false;
        for (int tenant : tenantSet) {
            // Set TTL only when tenant in hasTTLSet
            if (hasTTLSet.contains(tenant)) {
                tenantViewOptions.setTableProps(String.format("TTL=%d", viewTTL));
            }
            // build schema for tenant
            schemaBuilder.getDataOptions().setTenantId(null);
            schemaBuilder.buildWithNewTenant();
            // reset the TTL attribute for the next view
            tenantViewOptions.setTableProps("");

            PTable table = schemaBuilder.getBaseTable();
            String schemaName = table.getSchemaName().getString();
            String tableName = table.getTableName().getString();

            String tenantId = schemaBuilder.getDataOptions().getTenantId();
            LOGGER.debug(String.format("Building new view with tenant id %s on %s.%s",
                    tenantId, schemaName, tableName));
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
            //final String groupById = String.format(ID_FMT, 0);
            DataSupplier dataSupplier = new DataSupplier() {

                @Override public List<Object> getValues(int rowIndex) {
                    Random rnd = new Random();
                    String id = String.format(ID_FMT, rowIndex);
                    String zid = String.format(ZID_FMT, rowIndex);
                    String col4 = String.format(COL4_FMT, rowIndex + rnd.nextInt(MAX_ROWS));
                    String col5 = String.format(COL5_FMT, rowIndex + rnd.nextInt(MAX_ROWS));
                    String col6 = String.format(COL6_FMT, rowIndex + rnd.nextInt(MAX_ROWS));
                    String col7 = String.format(COL7_FMT, rowIndex + rnd.nextInt(MAX_ROWS));
                    String col8 = String.format(COL8_FMT, rowIndex + rnd.nextInt(MAX_ROWS));
                    String col9 = String.format(COL9_FMT, rowIndex + rnd.nextInt(MAX_ROWS));

                    return Lists.newArrayList(
                            new Object[] { id, zid, col4, col5, col6, col7, col8, col9 });
                }
            };

            // Create a test data reader/writer for the above schema.
            DataWriter dataWriter = new BasicDataWriter();
            List<String> columns =
                    Lists.newArrayList("ID", "ZID", "COL4", "COL5", "COL6", "COL7", "COL8", "COL9");
            List<String> rowKeyColumns = Lists.newArrayList("ID", "ZID");
            try (Connection writeConnection = DriverManager.getConnection(tenantConnectUrl)) {
                writeConnection.setAutoCommit(true);
                dataWriter.setConnection(writeConnection);
                dataWriter.setDataSupplier(dataSupplier);
                dataWriter.setUpsertColumns(columns);
                dataWriter.setRowKeyColumns(rowKeyColumns);
                dataWriter.setTargetEntity(schemaBuilder.getEntityTenantViewName());
                upsertData(dataWriter, DEFAULT_NUM_ROWS);

                // Case : count(1) sql
                DataReader dataReader = new BasicDataReader();
                dataReader.setValidationColumns(Arrays.asList("num_rows"));
                dataReader.setRowKeyColumns(Arrays.asList("num_rows"));
                dataReader.setDML(String
                        .format("SELECT count(1) as num_rows from %s HAVING count(1) > 0",
                                schemaBuilder.getEntityTenantViewName()));
                dataReader.setTargetEntity(schemaBuilder.getEntityTenantViewName());

                long scnTimestamp = EnvironmentEdgeManager.currentTimeMillis();

                // Validate data before and after ttl expiration.
                if (hasTTLSet.contains(tenant)) {
                    validateExpiredRowsAreNotReturnedUsingCounts(viewTTL, dataReader, schemaBuilder);
                } else {
                    validateRowsAreNotMaskedUsingCounts(scnTimestamp, dataReader, schemaBuilder);
                }

                // Case : group by sql
                dataReader.setValidationColumns(Arrays.asList("num_rows"));
                dataReader.setRowKeyColumns(Arrays.asList("num_rows"));
                dataReader.setDML(String
                        .format("SELECT count(1) as num_rows from %s GROUP BY ID HAVING count(1) > 0",
                                schemaBuilder.getEntityTenantViewName()));

                dataReader.setTargetEntity(schemaBuilder.getEntityTenantViewName());

                // Validate data before and after ttl expiration.
                if (hasTTLSet.contains(tenant)) {
                    validateExpiredRowsAreNotReturnedUsingCounts(viewTTL, dataReader, schemaBuilder);
                } else {
                    validateRowsAreNotMaskedUsingCounts(scnTimestamp, dataReader, schemaBuilder);
                }
            }
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
                nonCompactedTenantSet * DEFAULT_NUM_ROWS
        );

        // validate multi-tenanted index table
        validateAfterMajorCompaction(
                table.getSchemaName().toString(),
                table.getTableName().toString(),
                true,
                earliestTimestamp,
                VIEW_TTL_10_SECS,
                false,
                2 * nonCompactedTenantSet * DEFAULT_NUM_ROWS
        );

    }

    /**
     * Test case:
     * TTL set at different (global vs tenant) view levels
     * MultiTenant Tables and Views with various TenantId datatypes
     * Table split into multiple regions.
     * @throws Exception
     */

    protected void testMajorCompactWithVariousTenantIdTypesAndRegions(PDataType tenantType) throws Exception {

        resetEnvironmentEdgeManager();
        boolean isIndex1Local = false;
        boolean isIndex2Local = false;

        // View TTL is set in seconds (for e.g 10 secs)
        int viewTTL = VIEW_TTL_10_SECS;
        String tenantTypeName = tenantType.getSqlTypeName();
        TableOptions tableOptions = TableOptions.withDefaults();
        tableOptions.setTableProps("");
        tableOptions.setTableProps("COLUMN_ENCODED_BYTES=0,MULTI_TENANT=true,DEFAULT_COLUMN_FAMILY='0'");
        tableOptions.setTablePKColumns(Arrays.asList("OID", "KP"));
        tableOptions.setTablePKColumnTypes(Arrays.asList(tenantTypeName, "CHAR(3)"));

        GlobalViewOptions globalViewOptions = GlobalViewOptions.withDefaults();

        TenantViewOptions tenantViewOptions = new TenantViewOptions();
        tenantViewOptions.setTenantViewColumns(asList("ZID", "COL7", "COL8", "COL9"));
        tenantViewOptions
                .setTenantViewColumnTypes(asList("CHAR(15)", "VARCHAR", "VARCHAR", "VARCHAR"));

        DataOptions dataOptions = DataOptions.withDefaults();

        OtherOptions testCaseWhenAllCFMatchAndAllDefault = new OtherOptions();
        testCaseWhenAllCFMatchAndAllDefault.setTestName("testCaseWhenAllCFMatchAndAllDefault");
        testCaseWhenAllCFMatchAndAllDefault
                .setTableCFs(Lists.newArrayList((String) null, null, null));
        testCaseWhenAllCFMatchAndAllDefault
                .setGlobalViewCFs(Lists.newArrayList((String) null, null, null));
        testCaseWhenAllCFMatchAndAllDefault
                .setTenantViewCFs(Lists.newArrayList((String) null, null, null, null));


        final SchemaBuilder schemaBuilder = new SchemaBuilder(getUrl());
        schemaBuilder
                .withTableOptions(tableOptions)
                .withGlobalViewOptions(globalViewOptions)
                .withTenantViewOptions(tenantViewOptions)
                .withDataOptions(dataOptions)
                .withOtherOptions(testCaseWhenAllCFMatchAndAllDefault);

        int tenantNum = dataOptions.getTenantNumber();
        try (Connection globalConnection = DriverManager.getConnection(getUrl())) {
            String entityTableName = SchemaUtil.getTableName(
                    dataOptions.getSchemaName(), dataOptions.getTableName());
            String CO_BASE_TBL_TEMPLATE =
                    "CREATE TABLE IF NOT EXISTS %s " +
                            "(OID %s NOT NULL,KP CHAR(3) NOT NULL, " +
                            "COL1 VARCHAR, COL2 VARCHAR, COL3 VARCHAR " +
                            "CONSTRAINT pk PRIMARY KEY (OID,KP)) " +
                            "MULTI_TENANT=true,COLUMN_ENCODED_BYTES=0,DEFAULT_COLUMN_FAMILY='0' " +
                            "SPLIT ON (?, ?, ?)";
            String createBaseTableSQL = String.format(CO_BASE_TBL_TEMPLATE,
                    entityTableName, tenantType.getSqlTypeName());
            try (PreparedStatement pstmt = globalConnection.prepareStatement(createBaseTableSQL)) {
                switch (tenantType.getSqlType()) {
                case Types.VARCHAR:
                case Types.CHAR:
                    pstmt.setString(1, String.format("00D0t%04d", tenantNum + 3 ));
                    pstmt.setString(2, String.format("00D0t%04d", tenantNum + 5 ));
                    pstmt.setString(3, String.format("00D0t%04d", tenantNum + 7 ));
                    break;
                case Types.INTEGER:
                    pstmt.setInt(1, 300000);
                    pstmt.setInt(2, 500000);
                    pstmt.setInt(3, 700000);
                    break;
                case Types.BIGINT:
                    pstmt.setLong(1, 30000000000l);
                    pstmt.setLong(2, 50000000000l);
                    pstmt.setLong(3, 70000000000l);
                    break;
                }
                pstmt.execute();
            }

            schemaBuilder.setTableCreated();
            PTableKey
                    tableKey =
                    new PTableKey(null, SchemaUtil.normalizeFullTableName(entityTableName));
            schemaBuilder.setBaseTable(
                    globalConnection.unwrap(PhoenixConnection.class).getTable(tableKey));
        }

        OtherOptions otherOptions = OtherOptions.withDefaults();
        otherOptions.setTenantViewCFs(asList(null, null, null, null));

        Set<Integer> globalSet = new HashSet<>(Arrays.asList(new Integer[] { 1, 2, 3, 4}));
        Set<Integer> hasGlobalTTLSet = new HashSet<>(Arrays.asList(new Integer[] { 2, 3 }));
        Set<Integer> tenantSet = new HashSet<>(Arrays.asList(new Integer[] { 1, 4, 6, 8}));
        Set<Integer> hasTenantTTLSet = new HashSet<>(Arrays.asList(new Integer[] { 1, 6 }));

        int numGlobalIndex = 2;
        int numTenantIndex = 2;
        int nonCompactedGlobalSet = globalSet.size() - hasGlobalTTLSet.size(); // == 2;
        int nonCompactedTenantSet = (globalSet.size() * tenantSet.size()) // Total # views
                - (hasGlobalTTLSet.size() * tenantSet.size()) // # global views compacted
                - (hasTenantTTLSet.size() * nonCompactedGlobalSet); // #tenant views compacted
        int nonCompactedTableRows = nonCompactedTenantSet * DEFAULT_NUM_ROWS;
        int nonCompactedTenantIndexRows = nonCompactedTenantSet * DEFAULT_NUM_ROWS * numTenantIndex ;
        int nonCompactedGlobalIndexRows = nonCompactedGlobalSet * DEFAULT_NUM_ROWS * numGlobalIndex * tenantSet.size();

        String baseGlobalViewName = dataOptions.getGlobalViewName();
        long earliestTimestamp = EnvironmentEdgeManager.currentTimeMillis();
        for (int globalView : globalSet) {
            for (int tenant : tenantSet) {

                String globalViewName = String.format("%s_%d", baseGlobalViewName, globalView);
                dataOptions.setGlobalViewName(globalViewName);
                dataOptions.setKeyPrefix(String.format("KP%d", globalView));
                dataOptions.setTenantViewName(String.format("Z%d%d", globalView, tenant));

                globalViewOptions.setTableProps("");
                tenantViewOptions.setTableProps("");
                // Set TTL only when hasGlobalTTLSet OR hasTenantTTLSet
                // View TTL is set to 10s => 10000 ms
                if (hasGlobalTTLSet.contains(globalView)) {
                    globalViewOptions.setTableProps(String.format("TTL=%d", viewTTL));
                } else if (hasTenantTTLSet.contains(tenant)) {
                    tenantViewOptions.setTableProps(String.format("TTL=%d", viewTTL));
                }

                if (schemaBuilder.getDataOptions() != null) {
                    // build schema for tenant
                    switch (tenantType.getSqlType()) {
                    case Types.VARCHAR:
                    case Types.CHAR:
                        schemaBuilder.getDataOptions().setTenantId(dataOptions.getNextTenantId());
                        break;
                    case Types.INTEGER:
                        schemaBuilder.getDataOptions().setTenantId(Integer.toString(tenant*100000));
                        break;
                    case Types.BIGINT:
                        schemaBuilder.getDataOptions().setTenantId(Long.toString(tenant*10000000000l));
                        break;
                    }
                }
                schemaBuilder
                        .withTableOptions(tableOptions)
                        .withGlobalViewOptions(globalViewOptions)
                        .withTenantViewOptions(tenantViewOptions)
                        .withDataOptions(dataOptions)
                        .withOtherOptions(otherOptions)
                        .buildWithNewTenant();

                try (Connection globalConn = DriverManager.getConnection(getUrl());
                        final Statement statement = globalConn.createStatement()) {

                    String index1Name = String.format("IDX_%s_%s",
                            schemaBuilder.getEntityGlobalViewName().replaceAll("\\.", "_"),
                            "COL4");

                    final String index1Str = String.format("CREATE %s INDEX IF NOT EXISTS "
                                    + "%s ON %s (%s) INCLUDE (%s)", isIndex1Local ? "LOCAL" : "", index1Name,
                            schemaBuilder.getEntityGlobalViewName(), "COL4", "COL5"
                    );
                    statement.execute(index1Str);

                    String index2Name = String.format("IDX_%s_%s",
                            schemaBuilder.getEntityGlobalViewName().replaceAll("\\.", "_"),
                            "COL5");

                    final String index2Str = String.format("CREATE %s INDEX IF NOT EXISTS "
                                    + "%s ON %s (%s) INCLUDE (%s)", isIndex2Local ? "LOCAL" : "", index2Name,
                            schemaBuilder.getEntityGlobalViewName(), "COL5", "COL6"
                    );
                    statement.execute(index2Str);
                }

                String tenantConnectUrl =
                        getUrl() + ';' + TENANT_ID_ATTRIB + '=' +
                                schemaBuilder.getDataOptions().getTenantId();

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
                }

                // Define the test data.
                DataSupplier
                        dataSupplier = new DataSupplier() {

                    @Override public List<Object> getValues(int rowIndex) {
                        Random rnd = new Random();
                        String id = String.format(ID_FMT, rowIndex);
                        String zid = String.format(ZID_FMT, rowIndex);
                        String col1 = String.format(COL1_FMT, rowIndex + rnd.nextInt(MAX_ROWS));
                        String col2 = String.format(COL2_FMT, rowIndex + rnd.nextInt(MAX_ROWS));
                        String col3 = String.format(COL3_FMT, rowIndex + rnd.nextInt(MAX_ROWS));
                        String col4 = String.format(COL4_FMT, rowIndex + rnd.nextInt(MAX_ROWS));
                        String col5 = String.format(COL5_FMT, rowIndex + rnd.nextInt(MAX_ROWS));
                        String col6 = String.format(COL6_FMT, rowIndex + rnd.nextInt(MAX_ROWS));
                        String col7 = String.format(COL7_FMT, rowIndex + rnd.nextInt(MAX_ROWS));
                        String col8 = String.format(COL8_FMT, rowIndex + rnd.nextInt(MAX_ROWS));
                        String col9 = String.format(COL9_FMT, rowIndex + rnd.nextInt(MAX_ROWS));

                        return Lists.newArrayList(
                                new Object[] { id, col1, col2, col3, col4, col5, col6,
                                        zid, col7, col8, col9 });
                    }
                };

                // Create a test data reader/writer for the above schema.
                DataWriter
                        dataWriter = new BasicDataWriter();
                DataReader
                        dataReader = new BasicDataReader();

                List<String> columns =
                        Lists.newArrayList("ID",
                                "COL1", "COL2", "COL3", "COL4", "COL5",
                                "COL6", "ZID", "COL7", "COL8", "COL9");
                List<String> rowKeyColumns = Lists.newArrayList("ID", "ZID");

                try (Connection writeConnection = DriverManager
                        .getConnection(tenantConnectUrl)) {
                    writeConnection.setAutoCommit(true);
                    dataWriter.setConnection(writeConnection);
                    dataWriter.setDataSupplier(dataSupplier);
                    dataWriter.setUpsertColumns(columns);
                    dataWriter.setRowKeyColumns(rowKeyColumns);
                    dataWriter.setTargetEntity(schemaBuilder.getEntityTenantViewName());
                    org.apache.phoenix.thirdparty.com.google.common.collect.Table<String, String, Object>
                            upsertedData =
                            upsertData(dataWriter, DEFAULT_NUM_ROWS);

                    dataReader.setValidationColumns(columns);
                    dataReader.setRowKeyColumns(rowKeyColumns);
                    dataReader.setDML(String
                            .format("SELECT %s from %s", Joiner.on(",").join(columns),
                                    schemaBuilder.getEntityTenantViewName()));
                    dataReader.setTargetEntity(schemaBuilder.getEntityTenantViewName());
                    long scnTimestamp = EnvironmentEdgeManager.currentTimeMillis();

                    if (hasGlobalTTLSet.contains(globalView) || hasTenantTTLSet.contains(tenant)) {
                        LOGGER.debug("Validating {}, {}, {}", schemaBuilder.getDataOptions().getTenantId(), globalView, tenant);
                        // Validate data before and after ttl expiration.
                        validateExpiredRowsAreNotReturnedUsingData(viewTTL, upsertedData,
                                dataReader, schemaBuilder);
                    } else {
                        validateRowsAreNotMaskedUsingCounts(
                                scnTimestamp,
                                dataReader,
                                schemaBuilder);
                    }

                    // Case : count(1) sql (uses index)
                    dataReader.setValidationColumns(Arrays.asList("num_rows"));
                    dataReader.setRowKeyColumns(Arrays.asList("num_rows"));
                    dataReader.setDML(String
                            .format("SELECT count(1) as num_rows from %s HAVING count(1) > 0",
                                    schemaBuilder.getEntityTenantViewName()));
                    dataReader.setTargetEntity(schemaBuilder.getEntityTenantViewName());


                    // Validate data before and after ttl expiration.
                    if (hasGlobalTTLSet.contains(globalView) || hasTenantTTLSet.contains(tenant)) {
                        LOGGER.debug("Validating {}, {}, {}", schemaBuilder.getDataOptions().getTenantId(), globalView, tenant);
                        validateExpiredRowsAreNotReturnedUsingCounts(viewTTL, dataReader, schemaBuilder);
                    } else {
                        validateRowsAreNotMaskedUsingCounts(scnTimestamp, dataReader, schemaBuilder);
                    }

                    // Case : group by sql (does not use index)
                    dataReader.setValidationColumns(Arrays.asList("num_rows"));
                    dataReader.setRowKeyColumns(Arrays.asList("num_rows"));
                    dataReader.setDML(String
                            .format("SELECT count(1) as num_rows from %s GROUP BY ID HAVING count(1) > 0",
                                    schemaBuilder.getEntityTenantViewName()));

                    dataReader.setTargetEntity(schemaBuilder.getEntityTenantViewName());

                    // Validate data before and after ttl expiration.
                    if (hasGlobalTTLSet.contains(globalView) || hasTenantTTLSet.contains(tenant)) {
                        LOGGER.debug("Validating {}, {}, {}", schemaBuilder.getDataOptions().getTenantId(), globalView, tenant);
                        validateExpiredRowsAreNotReturnedUsingCounts(viewTTL, dataReader, schemaBuilder);
                    } else {
                        validateRowsAreNotMaskedUsingCounts(scnTimestamp, dataReader, schemaBuilder);
                    }
                }

            }


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
                nonCompactedTableRows
        );

        // validate multi-tenanted index table
        validateAfterMajorCompaction(
                table.getSchemaName().toString(),
                table.getTableName().toString(),
                true,
                earliestTimestamp,
                VIEW_TTL_10_SECS,
                false,
                (nonCompactedTenantIndexRows + nonCompactedGlobalIndexRows)
        );

    }

    /**
     * Test special case:
     * When there are overlapping row key prefixes.
     * This can occur when the TENANT_ID and global view PARTITION_KEY overlap.
     * @throws Exception
     */

    protected void testTenantViewsWIthOverlappingRowPrefixes() throws Exception {
        // View TTL is set in seconds (for e.g 10 secs)
        int viewTTL = VIEW_TTL_10_SECS;
        // Define the test schema.
        // 1. Table with columns => (ORG_ID, KP, COL1, COL2, COL3), PK => (ORG_ID, KP)
        // 2. GlobalView with columns => (ID, COL4, COL5, COL6), PK => (ID)
        // 3. Tenant with columns => (ZID, COL7, COL8, COL9), PK => (ZID)
        final SchemaBuilder schemaBuilder = new SchemaBuilder(getUrl());

        TableOptions
                tableOptions = TableOptions.withDefaults();
        tableOptions.setTableProps("");
        tableOptions.setTableProps("COLUMN_ENCODED_BYTES=0,MULTI_TENANT=true,DEFAULT_COLUMN_FAMILY='0'");
        tableOptions.setTablePKColumns(Arrays.asList("OID", "OOID"));
        tableOptions.setTablePKColumnTypes(Arrays.asList("CHAR(15)", "CHAR(15)"));

        GlobalViewOptions
                globalViewOptions = GlobalViewOptions.withDefaults();
        DataOptions dataOptions = DataOptions.withDefaults();

        TenantViewOptions
                tenantViewWithOverrideOptions = TenantViewOptions.withDefaults();

        schemaBuilder.withTableOptions(tableOptions)
                .withTenantViewOptions(tenantViewWithOverrideOptions);

        long earliestTimestamp = EnvironmentEdgeManager.currentTimeMillis();

        for (String keyPrefix : Arrays.asList("00D0t0001000001", "00D0t0002000001")) {
            if (keyPrefix.compareTo("00D0t0001000001") == 0) {
                dataOptions.setGlobalViewName(dataOptions.getGlobalViewName() + "_1");
                dataOptions.setTenantViewName("Z01");
                dataOptions.setKeyPrefix("00D0t0002000001");
                // View TTL is set to 300s => 300000 ms
                globalViewOptions.setTableProps("TTL=10");
            } else {
                dataOptions.setGlobalViewName(dataOptions.getGlobalViewName() + "_2");
                dataOptions.setTenantViewName("Z02");
                dataOptions.setKeyPrefix("00D0t0001000001");
                globalViewOptions.setTableProps("TTL=NONE");
            }

            schemaBuilder
                    .withDataOptions(dataOptions)
                    .withGlobalViewOptions(globalViewOptions)
                    .buildWithNewTenant();

            // Define the test data.
            DataSupplier
                    dataSupplier = new DataSupplier() {

                @Override public List<Object> getValues(int rowIndex) {
                    Random rnd = new Random();
                    String id = String.format(ID_FMT, rowIndex);
                    String zid = String.format(ZID_FMT, rowIndex);
                    String col1 = String.format(COL1_FMT, rowIndex + rnd.nextInt(MAX_ROWS));
                    String col2 = String.format(COL2_FMT, rowIndex + rnd.nextInt(MAX_ROWS));
                    String col3 = String.format(COL3_FMT, rowIndex + rnd.nextInt(MAX_ROWS));
                    String col4 = String.format(COL4_FMT, rowIndex + rnd.nextInt(MAX_ROWS));
                    String col5 = String.format(COL5_FMT, rowIndex + rnd.nextInt(MAX_ROWS));
                    String col6 = String.format(COL6_FMT, rowIndex + rnd.nextInt(MAX_ROWS));
                    String col7 = String.format(COL7_FMT, rowIndex + rnd.nextInt(MAX_ROWS));
                    String col8 = String.format(COL8_FMT, rowIndex + rnd.nextInt(MAX_ROWS));
                    String col9 = String.format(COL9_FMT, rowIndex + rnd.nextInt(MAX_ROWS));

                    return Lists.newArrayList(
                            new Object[] { id, col1, col2, col3, col4, col5, col6,
                                    zid, col7, col8, col9 });
                }
            };

            // Create a test data reader/writer for the above schema.
            DataWriter
                    dataWriter = new BasicDataWriter();
            DataReader
                    dataReader = new BasicDataReader();

            List<String> columns =
                    Lists.newArrayList("ID",
                            "COL1", "COL2", "COL3", "COL4", "COL5",
                            "COL6", "ZID", "COL7", "COL8", "COL9");
            List<String> rowKeyColumns = Lists.newArrayList("ID", "ZID");
            String tenantConnectUrl =
                    getUrl() + ';' + TENANT_ID_ATTRIB + '=' +
                            schemaBuilder.getDataOptions().getTenantId();
            try (Connection writeConnection = DriverManager
                    .getConnection(tenantConnectUrl)) {
                writeConnection.setAutoCommit(true);
                dataWriter.setConnection(writeConnection);
                dataWriter.setDataSupplier(dataSupplier);
                dataWriter.setUpsertColumns(columns);
                dataWriter.setRowKeyColumns(rowKeyColumns);
                dataWriter.setTargetEntity(schemaBuilder.getEntityTenantViewName());
                org.apache.phoenix.thirdparty.com.google.common.collect.Table<String, String, Object>
                        upsertedData =
                upsertData(dataWriter, DEFAULT_NUM_ROWS);

                dataReader.setValidationColumns(columns);
                dataReader.setRowKeyColumns(rowKeyColumns);
                dataReader.setDML(String
                        .format("SELECT %s from %s", Joiner.on(",").join(columns),
                                schemaBuilder.getEntityTenantViewName()));
                dataReader.setTargetEntity(schemaBuilder.getEntityTenantViewName());
                long scnTimestamp = EnvironmentEdgeManager.currentTimeMillis();

                if (keyPrefix.compareTo("00D0t0001000001") == 0) {
                    // Validate data before and after ttl expiration.
                    validateExpiredRowsAreNotReturnedUsingData(viewTTL, upsertedData,
                            dataReader, schemaBuilder);
                } else {
                    validateRowsAreNotMaskedUsingCounts(
                            scnTimestamp,
                            dataReader,
                            schemaBuilder);
                }
            }


        }

        PTable table = schemaBuilder.getBaseTable();
        // validate multi-tenanted base table
        validateAfterMajorCompaction(
                table.getSchemaName().toString(),
                table.getTableName().toString(),
                false,
                earliestTimestamp,
                viewTTL,
                false,
                DEFAULT_NUM_ROWS
        );


    }

    /**
     * Test case:
     * TTL set at different (global vs tenant) view levels
     * MultiTenant Tables and Views
     * Multiple Global and Tenant Indexes
     * @throws Exception
     */
    protected void testMajorCompactWithGlobalAndTenantViewHierarchy() throws Exception {
        // View TTL is set in seconds (for e.g 10 secs)
        int viewTTL = VIEW_TTL_10_SECS;
        boolean isIndex1Local = false;
        boolean isIndex2Local = false;
        // Define the test schema.
        // 1. Table with columns => (ORG_ID, KP, COL1, COL2, COL3), PK => (ORG_ID, KP)
        // 2. GlobalView with columns => (ID, COL4, COL5, COL6), PK => (ID)
        // 3. Tenant with columns => (ZID, COL7, COL8, COL9), PK => (ZID)
        final SchemaBuilder schemaBuilder = new SchemaBuilder(getUrl());

        TableOptions tableOptions = TableOptions.withDefaults();
        tableOptions.setTableProps("");
        tableOptions.setTableProps("COLUMN_ENCODED_BYTES=0,MULTI_TENANT=true,DEFAULT_COLUMN_FAMILY='0'");
        tableOptions.setTablePKColumns(Arrays.asList("OID", "KP"));
        tableOptions.setTablePKColumnTypes(Arrays.asList("CHAR(15)", "CHAR(3)"));

        GlobalViewOptions globalViewOptions = GlobalViewOptions.withDefaults();

        TenantViewOptions tenantViewOptions = new TenantViewOptions();
        tenantViewOptions.setTenantViewColumns(asList("ZID", "COL7", "COL8", "COL9"));
        tenantViewOptions.setTenantViewColumnTypes(asList("VARCHAR", "VARCHAR", "VARCHAR", "VARCHAR"));

        DataOptions dataOptions = DataOptions.withDefaults();
        OtherOptions otherOptions = OtherOptions.withDefaults();
        otherOptions.setTenantViewCFs(asList(null, null, null, null));

        Set<Integer> hasGlobalTTLSet = new HashSet<>(Arrays.asList(new Integer[] { 2, 3 }));
        Set<Integer> globalSet = new HashSet<>(Arrays.asList(new Integer[] { 1, 2, 3}));
        Set<Integer> tenantSet = new HashSet<>(Arrays.asList(new Integer[] { 1, 2, 3, 4}));
        Set<Integer> hasTenantTTLSet = new HashSet<>(Arrays.asList(new Integer[] { 2, 3 }));

        int numGlobalIndex = 2;
        int numTenantIndex = 2;
        int nonCompactedGlobalSet = globalSet.size() - hasGlobalTTLSet.size(); // == 4;
        int nonCompactedTenantSet = (globalSet.size() * tenantSet.size()) // Total # views
                - (hasGlobalTTLSet.size() * tenantSet.size()) // # global views compacted
                - (hasTenantTTLSet.size() * nonCompactedGlobalSet); // #tenant views compacted
        int nonCompactedTableRows = nonCompactedTenantSet * DEFAULT_NUM_ROWS;
        int nonCompactedTenantIndexRows = nonCompactedTenantSet * DEFAULT_NUM_ROWS * numTenantIndex ;
        int nonCompactedGlobalIndexRows = nonCompactedGlobalSet * DEFAULT_NUM_ROWS * numGlobalIndex * tenantSet.size();
        Map<Integer, String> tenantIds = new HashMap<>();
        for (int tenant : tenantSet) {
            tenantIds.put(tenant, dataOptions.getNextTenantId());
        }

        String baseGlobalViewName = dataOptions.getGlobalViewName();
        long earliestTimestamp = EnvironmentEdgeManager.currentTimeMillis();
        for (int globalView : globalSet) {
            for (int tenant : tenantSet) {
                String globalViewName = String.format("%s_%d", baseGlobalViewName, globalView);
                dataOptions.setGlobalViewName(globalViewName);
                dataOptions.setKeyPrefix(String.format("KP%d", globalView));
                dataOptions.setTenantViewName(String.format("Z%d%d", globalView, tenant));

                globalViewOptions.setTableProps("");
                tenantViewOptions.setTableProps("");
                // Set TTL only when hasGlobalTTLSet OR hasTenantTTLSet
                // View TTL is set to 10s => 10000 ms
                if (hasGlobalTTLSet.contains(globalView)) {
                    globalViewOptions.setTableProps(String.format("TTL=%d", viewTTL));
                } else if (hasTenantTTLSet.contains(tenant)) {
                    tenantViewOptions.setTableProps(String.format("TTL=%d", viewTTL));
                }

                // build schema for tenant
                if (schemaBuilder.getDataOptions() != null) {
                    schemaBuilder.getDataOptions().setTenantId(null);
                }
                dataOptions.setTenantId(tenantIds.get(tenant));
                schemaBuilder
                        .withTableOptions(tableOptions)
                        .withGlobalViewOptions(globalViewOptions)
                        .withTenantViewOptions(tenantViewOptions)
                        .withDataOptions(dataOptions)
                        .withOtherOptions(otherOptions)
                        .buildWithNewTenant();

                try (Connection globalConn = DriverManager.getConnection(getUrl());
                        final Statement statement = globalConn.createStatement()) {

                    String index1Name = String.format("IDX_%s_%s",
                            schemaBuilder.getEntityGlobalViewName().replaceAll("\\.", "_"),
                            "COL4");

                    final String index1Str = String.format("CREATE %s INDEX IF NOT EXISTS "
                                    + "%s ON %s (%s) INCLUDE (%s)", isIndex1Local ? "LOCAL" : "", index1Name,
                            schemaBuilder.getEntityGlobalViewName(), "COL4", "COL5"
                    );
                    statement.execute(index1Str);

                    String index2Name = String.format("IDX_%s_%s",
                            schemaBuilder.getEntityGlobalViewName().replaceAll("\\.", "_"),
                            "COL5");

                    final String index2Str = String.format("CREATE %s INDEX IF NOT EXISTS "
                                    + "%s ON %s (%s) INCLUDE (%s)", isIndex2Local ? "LOCAL" : "", index2Name,
                            schemaBuilder.getEntityGlobalViewName(), "COL5", "COL6"
                    );
                    statement.execute(index2Str);
                }

                String tenantConnectUrl =
                        getUrl() + ';' + TENANT_ID_ATTRIB + '=' +
                                schemaBuilder.getDataOptions().getTenantId();

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
                }

                // Define the test data.
                DataSupplier
                        dataSupplier = new DataSupplier() {

                    @Override public List<Object> getValues(int rowIndex) {
                        Random rnd = new Random();
                        String id = String.format(ID_FMT, rowIndex);
                        String zid = String.format(ZID_FMT, rowIndex);
                        String col1 = String.format(COL1_FMT, rowIndex + rnd.nextInt(MAX_ROWS));
                        String col2 = String.format(COL2_FMT, rowIndex + rnd.nextInt(MAX_ROWS));
                        String col3 = String.format(COL3_FMT, rowIndex + rnd.nextInt(MAX_ROWS));
                        String col4 = String.format(COL4_FMT, rowIndex + rnd.nextInt(MAX_ROWS));
                        String col5 = String.format(COL5_FMT, rowIndex + rnd.nextInt(MAX_ROWS));
                        String col6 = String.format(COL6_FMT, rowIndex + rnd.nextInt(MAX_ROWS));
                        String col7 = String.format(COL7_FMT, rowIndex + rnd.nextInt(MAX_ROWS));
                        String col8 = String.format(COL8_FMT, rowIndex + rnd.nextInt(MAX_ROWS));
                        String col9 = String.format(COL9_FMT, rowIndex + rnd.nextInt(MAX_ROWS));

                        return Lists.newArrayList(
                                new Object[] { id, col1, col2, col3, col4, col5, col6,
                                        zid, col7, col8, col9 });
                    }
                };

                // Create a test data reader/writer for the above schema.
                DataWriter
                        dataWriter = new BasicDataWriter();
                DataReader
                        dataReader = new BasicDataReader();

                List<String> columns =
                        Lists.newArrayList("ID",
                                "COL1", "COL2", "COL3", "COL4", "COL5",
                                "COL6", "ZID", "COL7", "COL8", "COL9");
                List<String> rowKeyColumns = Lists.newArrayList("ID", "ZID");

                try (Connection writeConnection = DriverManager
                        .getConnection(tenantConnectUrl)) {
                    writeConnection.setAutoCommit(true);
                    dataWriter.setConnection(writeConnection);
                    dataWriter.setDataSupplier(dataSupplier);
                    dataWriter.setUpsertColumns(columns);
                    dataWriter.setRowKeyColumns(rowKeyColumns);
                    dataWriter.setTargetEntity(schemaBuilder.getEntityTenantViewName());
                    org.apache.phoenix.thirdparty.com.google.common.collect.Table<String, String, Object>
                            upsertedData =
                            upsertData(dataWriter, DEFAULT_NUM_ROWS);

                    dataReader.setValidationColumns(columns);
                    dataReader.setRowKeyColumns(rowKeyColumns);
                    dataReader.setDML(String
                            .format("SELECT %s from %s", Joiner.on(",").join(columns),
                                    schemaBuilder.getEntityTenantViewName()));
                    dataReader.setTargetEntity(schemaBuilder.getEntityTenantViewName());
                    long scnTimestamp = EnvironmentEdgeManager.currentTimeMillis();

                    if (hasGlobalTTLSet.contains(globalView) || hasTenantTTLSet.contains(tenant)) {
                        // Validate data before and after ttl expiration.
                        validateExpiredRowsAreNotReturnedUsingData(viewTTL, upsertedData,
                                dataReader, schemaBuilder);
                    } else {
                        validateRowsAreNotMaskedUsingCounts(
                                scnTimestamp,
                                dataReader,
                                schemaBuilder);
                    }
                }

            }


        }


        PTable table = schemaBuilder.getBaseTable();
        // validate multi-tenanted base table
        validateAfterMajorCompaction(
                table.getSchemaName().toString(),
                table.getTableName().toString(),
                false,
                earliestTimestamp,
                viewTTL,
                false,
                nonCompactedTableRows
        );
        // validate multi-tenanted index table
        validateAfterMajorCompaction(
                table.getSchemaName().toString(),
                table.getTableName().toString(),
                true,
                earliestTimestamp,
                VIEW_TTL_10_SECS,
                false,
                (nonCompactedTenantIndexRows + nonCompactedGlobalIndexRows)
        );

    }

}