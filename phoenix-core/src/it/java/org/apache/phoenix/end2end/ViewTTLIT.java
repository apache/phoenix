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

import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.filter.BinaryComparator;
import org.apache.hadoop.hbase.filter.CompareFilter;
import org.apache.hadoop.hbase.filter.FilterList;
import org.apache.hadoop.hbase.filter.QualifierFilter;
import org.apache.hadoop.hbase.filter.RowFilter;
import org.apache.hadoop.hbase.filter.SubstringComparator;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.phoenix.compile.QueryPlan;
import org.apache.phoenix.coprocessor.BaseScannerRegionObserver;
import org.apache.phoenix.exception.SQLExceptionCode;
import org.apache.phoenix.jdbc.PhoenixConnection;
import org.apache.phoenix.jdbc.PhoenixDatabaseMetaData;
import org.apache.phoenix.jdbc.PhoenixResultSet;
import org.apache.phoenix.jdbc.PhoenixStatement;
import org.apache.phoenix.query.PhoenixTestBuilder.BasicDataReader;
import org.apache.phoenix.query.PhoenixTestBuilder.BasicDataWriter;
import org.apache.phoenix.query.PhoenixTestBuilder.DataReader;
import org.apache.phoenix.query.PhoenixTestBuilder.DataSupplier;
import org.apache.phoenix.query.PhoenixTestBuilder.DataWriter;
import org.apache.phoenix.query.PhoenixTestBuilder.SchemaBuilder;
import org.apache.phoenix.query.PhoenixTestBuilder.SchemaBuilder.GlobalViewIndexOptions;
import org.apache.phoenix.query.PhoenixTestBuilder.SchemaBuilder.GlobalViewOptions;
import org.apache.phoenix.query.PhoenixTestBuilder.SchemaBuilder.OtherOptions;
import org.apache.phoenix.query.PhoenixTestBuilder.SchemaBuilder.TableOptions;
import org.apache.phoenix.query.PhoenixTestBuilder.SchemaBuilder.TenantViewIndexOptions;
import org.apache.phoenix.query.PhoenixTestBuilder.SchemaBuilder.TenantViewOptions;
import org.apache.phoenix.query.QueryConstants;
import org.apache.phoenix.query.QueryServices;
import org.apache.phoenix.schema.PTable;
import org.apache.phoenix.schema.PTableType;
import org.apache.phoenix.schema.types.PDataType;
import org.apache.phoenix.thirdparty.com.google.common.base.Joiner;
import org.apache.phoenix.thirdparty.com.google.common.base.Preconditions;
import org.apache.phoenix.thirdparty.com.google.common.collect.Lists;
import org.apache.phoenix.thirdparty.com.google.common.collect.Maps;
import org.apache.phoenix.util.EnvironmentEdgeManager;
import org.apache.phoenix.util.LogUtil;
import org.apache.phoenix.util.PhoenixRuntime;
import org.apache.phoenix.util.ScanUtil;
import org.apache.phoenix.util.SchemaUtil;
import org.apache.phoenix.util.TestUtil;
import org.junit.Assert;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Random;
import java.util.Set;

import static java.util.Arrays.asList;
import static org.apache.phoenix.query.PhoenixTestBuilder.DDLDefaults.COLUMN_TYPES;
import static org.apache.phoenix.query.PhoenixTestBuilder.DDLDefaults.DEFAULT_SCHEMA_NAME;
import static org.apache.phoenix.query.PhoenixTestBuilder.DDLDefaults.MAX_ROWS;
import static org.apache.phoenix.query.PhoenixTestBuilder.DDLDefaults.TENANT_VIEW_COLUMNS;
import static org.apache.phoenix.util.PhoenixRuntime.TENANT_ID_ATTRIB;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

@Category(NeedsOwnMiniClusterTest.class)
public class ViewTTLIT extends ParallelStatsDisabledIT {
    private static final Logger LOGGER = LoggerFactory.getLogger(ViewTTLIT.class);
    private static final String ORG_ID_FMT = "00D0x000%s";
    private static final String ID_FMT = "00A0y000%07d";
    private static final String ZID_FMT = "00B0y000%07d";
    private static final String PHOENIX_TTL_HEADER_SQL = "SELECT PHOENIX_TTL FROM SYSTEM.CATALOG "
            + "WHERE %s AND TABLE_SCHEM = '%s' AND TABLE_NAME = '%s' AND TABLE_TYPE = '%s'";

    private static final String ALTER_PHOENIX_TTL_SQL
            = "ALTER VIEW \"%s\".\"%s\" set PHOENIX_TTL=%s";

    private static final String ALTER_SQL_WITH_NO_TTL
            = "ALTER VIEW \"%s\".\"%s\" ADD IF NOT EXISTS %s CHAR(10)";
    private static final int DEFAULT_NUM_ROWS = 5;

    private static final String COL1_FMT = "a%05d";
    private static final String COL2_FMT = "b%05d";
    private static final String COL3_FMT = "c%05d";
    private static final String COL4_FMT = "d%05d";
    private static final String COL5_FMT = "e%05d";
    private static final String COL6_FMT = "f%05d";
    private static final String COL7_FMT = "g%05d";
    private static final String COL8_FMT = "h%05d";
    private static final String COL9_FMT = "i%05d";

    // Scans the HBase rows directly and asserts
    private void assertUsingHBaseRows(byte[] hbaseTableName,
            long minTimestamp, int expectedRows) throws IOException, SQLException {

        try (Table tbl = driver.getConnectionQueryServices(getUrl(), TestUtil.TEST_PROPERTIES)
                .getTable(hbaseTableName)) {

            Scan allRows = new Scan();
            allRows.setTimeRange(minTimestamp, minTimestamp + Integer.MAX_VALUE);
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
    private void assertViewHeaderRowsHavePhoenixTTLRelatedCells(String schemaName,
            long minTimestamp, boolean rawScan, int expectedRows) throws IOException, SQLException {

        FilterList filterList = new FilterList(FilterList.Operator.MUST_PASS_ALL);
        RowFilter schemaNameFilter = new RowFilter(CompareFilter.CompareOp.EQUAL,
                new SubstringComparator(schemaName));
        QualifierFilter phoenixTTLQualifierFilter = new QualifierFilter(
                CompareFilter.CompareOp.EQUAL,
                new BinaryComparator(PhoenixDatabaseMetaData.PHOENIX_TTL_BYTES));
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
                        PhoenixDatabaseMetaData.PHOENIX_TTL_BYTES) ? 1 : 0;
            }
            assertEquals(String.format("Expected rows do not match for table = %s at timestamp %d",
                    Bytes.toString(PhoenixDatabaseMetaData.SYSTEM_CATALOG_NAME_BYTES),
                    minTimestamp), expectedRows, numMatchingRows);
        }

    }

    private void assertSyscatHavePhoenixTTLRelatedColumns(String tenantId, String schemaName,
            String tableName, String tableType, long ttlValueExpected) throws SQLException {

        try (Connection connection = DriverManager.getConnection(getUrl())) {
            Statement stmt = connection.createStatement();
            String tenantClause = tenantId == null || tenantId.isEmpty() ?
                    "TENANT_ID IS NULL" :
                    String.format("TENANT_ID = '%s'", tenantId);
            String sql = String
                    .format(PHOENIX_TTL_HEADER_SQL, tenantClause, schemaName, tableName, tableType);
            stmt.execute(sql);
            ResultSet rs = stmt.getResultSet();
            long actualTTLValueReturned = rs.next() ? rs.getLong(1) : 0;

            assertEquals(String.format("Expected rows do not match for schema = %s, table = %s",
                    schemaName, tableName), ttlValueExpected, actualTTLValueReturned);
        }
    }

    private String stripQuotes(String name) {
        return name.replace("\"", "");
    }

    private SchemaBuilder createLevel2TenantViewWithGlobalLevelTTL(
            TenantViewOptions tenantViewOptions, TenantViewIndexOptions tenantViewIndexOptions)
            throws Exception {
        // Define the test schema.
        // 1. Table with columns => (ORG_ID, KP, COL1, COL2, COL3), PK => (ORG_ID, KP)
        // 2. GlobalView with columns => (ID, COL4, COL5, COL6), PK => (ID)
        // 3. Tenant with columns => (ZID, COL7, COL8, COL9), PK => (ZID)
        final SchemaBuilder schemaBuilder = new SchemaBuilder(getUrl());

        TableOptions tableOptions = TableOptions.withDefaults();
        tableOptions.setTableProps("COLUMN_ENCODED_BYTES=0,MULTI_TENANT=true");

        GlobalViewOptions globalViewOptions = GlobalViewOptions.withDefaults();
        // Phoenix TTL is set to 300s => 300000 ms
        globalViewOptions.setTableProps("PHOENIX_TTL=300");

        SchemaBuilder.GlobalViewIndexOptions globalViewIndexOptions
                = SchemaBuilder.GlobalViewIndexOptions.withDefaults();
        globalViewIndexOptions.setLocal(false);

        TenantViewOptions tenantViewWithOverrideOptions = TenantViewOptions.withDefaults();
        if (tenantViewOptions != null) {
            tenantViewWithOverrideOptions = tenantViewOptions;
        }
        TenantViewIndexOptions tenantViewIndexOverrideOptions = TenantViewIndexOptions.withDefaults();
        if (tenantViewIndexOptions != null) {
            tenantViewIndexOverrideOptions = tenantViewIndexOptions;
        }
        schemaBuilder.withTableOptions(tableOptions).withGlobalViewOptions(globalViewOptions)
                .withGlobalViewIndexOptions(globalViewIndexOptions)
                .withTenantViewOptions(tenantViewWithOverrideOptions)
                .withTenantViewIndexOptions(tenantViewIndexOverrideOptions).buildWithNewTenant();
        return schemaBuilder;
    }

    private SchemaBuilder createLevel2TenantViewWithTenantLevelTTL(
            TenantViewOptions tenantViewOptions, TenantViewIndexOptions tenantViewIndexOptions)
            throws Exception {
        // Define the test schema.
        // 1. Table with columns => (ORG_ID, KP, COL1, COL2, COL3), PK => (ORG_ID, KP)
        // 2. GlobalView with columns => (ID, COL4, COL5, COL6), PK => (ID)
        // 3. Tenant with columns => (ZID, COL7, COL8, COL9), PK => (ZID)
        final SchemaBuilder schemaBuilder = new SchemaBuilder(getUrl());

        TableOptions tableOptions = TableOptions.withDefaults();
        tableOptions.setTableProps("COLUMN_ENCODED_BYTES=0,MULTI_TENANT=true");

        GlobalViewOptions globalViewOptions = GlobalViewOptions.withDefaults();

        SchemaBuilder.GlobalViewIndexOptions globalViewIndexOptions
                = SchemaBuilder.GlobalViewIndexOptions.withDefaults();
        globalViewIndexOptions.setLocal(false);

        TenantViewOptions tenantViewWithOverrideOptions = TenantViewOptions.withDefaults();
        // Phoenix TTL is set to 300s => 300000 ms
        tenantViewWithOverrideOptions.setTableProps("PHOENIX_TTL=300");
        if (tenantViewOptions != null) {
            tenantViewWithOverrideOptions = tenantViewOptions;
        }
        TenantViewIndexOptions tenantViewIndexOverrideOptions = TenantViewIndexOptions.withDefaults();
        if (tenantViewIndexOptions != null) {
            tenantViewIndexOverrideOptions = tenantViewIndexOptions;
        }
        schemaBuilder.withTableOptions(tableOptions).withGlobalViewOptions(globalViewOptions)
                .withGlobalViewIndexOptions(globalViewIndexOptions)
                .withTenantViewOptions(tenantViewWithOverrideOptions)
                .withTenantViewIndexOptions(tenantViewIndexOverrideOptions).buildWithNewTenant();
        return schemaBuilder;
    }

    private SchemaBuilder createLevel1TenantView(TenantViewOptions tenantViewOptions,
            TenantViewIndexOptions tenantViewIndexOptions) throws Exception {
        // Define the test schema.
        // 1. Table with default columns => (ORG_ID, KP, COL1, COL2, COL3), PK => (ORG_ID, KP)
        // 2. Tenant with default columns => (ZID, COL7, COL8, COL9), PK => (ZID)
        final SchemaBuilder schemaBuilder = new SchemaBuilder(getUrl());

        TableOptions tableOptions = TableOptions.withDefaults();
        tableOptions.setTableProps("COLUMN_ENCODED_BYTES=0,MULTI_TENANT=true");

        TenantViewOptions tenantViewOverrideOptions = TenantViewOptions.withDefaults();
        if (tenantViewOptions != null) {
            tenantViewOverrideOptions = tenantViewOptions;
        }
        TenantViewIndexOptions tenantViewIndexOverrideOptions = TenantViewIndexOptions.withDefaults();
        if (tenantViewIndexOptions != null) {
            tenantViewIndexOverrideOptions = tenantViewIndexOptions;
        }

        schemaBuilder.withTableOptions(tableOptions)
                .withTenantViewOptions(tenantViewOverrideOptions)
                .withTenantViewIndexOptions(tenantViewIndexOverrideOptions).buildNewView();
        return schemaBuilder;
    }

    /**
     * -----------------
     * Test methods
     * -----------------
     */

    @Test
    public void testWithBasicGlobalViewWithNoPhoenixTTLDefined() throws Exception {

        long startTime = EnvironmentEdgeManager.currentTimeMillis();

        // Define the test schema.
        // 1. Table with default columns => (ORG_ID, KP, COL1, COL2, COL3), PK => (ORG_ID, KP)
        // 2. GlobalView with default columns => (ID, COL4, COL5, COL6), PK => (ID)
        final SchemaBuilder schemaBuilder = new SchemaBuilder(getUrl());
        schemaBuilder.withTableDefaults().withGlobalViewDefaults().build();

        // Expected 2 rows - one for Table and GlobalView each.
        // Since the PHOENIX_TTL property values are not being set,
        // we expect the view header columns to show up in raw scans only.
        assertViewHeaderRowsHavePhoenixTTLRelatedCells(
                schemaBuilder.getTableOptions().getSchemaName(), startTime, true, 2);
    }

    @Test
    public void testPhoenixTTLWithTableLevelTTLFails() throws Exception {

        // Define the test schema.
        // 1. Table with default columns => (ORG_ID, KP, COL1, COL2, COL3), PK => (ORG_ID, KP)
        // 2. Tenant with default columns => (ZID, COL7, COL8, COL9), PK => (ZID)
        final SchemaBuilder schemaBuilder = new SchemaBuilder(getUrl());

        TableOptions tableOptions = TableOptions.withDefaults();
        tableOptions.setTableProps("COLUMN_ENCODED_BYTES=0,MULTI_TENANT=true,TTL=100");

        TenantViewOptions tenantViewOptions = TenantViewOptions.withDefaults();
        tenantViewOptions.setTableProps("PHOENIX_TTL=1000");
        try {
            schemaBuilder.withTableOptions(tableOptions).withTenantViewOptions(tenantViewOptions)
                    .buildNewView();
            fail();
        } catch (SQLException e) {
            assertEquals(SQLExceptionCode.CANNOT_SET_OR_ALTER_PHOENIX_TTL_FOR_TABLE_WITH_TTL
                    .getErrorCode(), e.getErrorCode());
        }
    }

    @Test
    public void testPhoenixTTLWithViewIndexFails() throws Exception {

        TenantViewIndexOptions tenantViewIndexOptions = TenantViewIndexOptions.withDefaults();
        tenantViewIndexOptions.setIndexProps("PHOENIX_TTL=1000");
        try {
            final SchemaBuilder schemaBuilder = createLevel1TenantView(null,
                    tenantViewIndexOptions);
            fail();
        } catch (SQLException e) {
            assertEquals(SQLExceptionCode.PHOENIX_TTL_SUPPORTED_FOR_VIEWS_ONLY.getErrorCode(),
                    e.getErrorCode());
        }
    }

    @Test
    public void testPhoenixTTLForLevelOneView() throws Exception {
        long startTime = EnvironmentEdgeManager.currentTimeMillis();

        TenantViewOptions tenantViewOptions = TenantViewOptions.withDefaults();
        // Phoenix TTL is set to 120s => 120000 ms
        tenantViewOptions.setTableProps("PHOENIX_TTL=120");

        final SchemaBuilder schemaBuilder = createLevel1TenantView(tenantViewOptions, null);
        String tenantId = schemaBuilder.getDataOptions().getTenantId();
        String schemaName = stripQuotes(
                SchemaUtil.getSchemaNameFromFullName(schemaBuilder.getEntityTenantViewName()));
        String tenantViewName = stripQuotes(
                SchemaUtil.getTableNameFromFullName(schemaBuilder.getEntityTenantViewName()));
        String indexOnTenantViewName = String
                .format("IDX_%s", stripQuotes(schemaBuilder.getEntityKeyPrefix()));

        // Expected 2 rows - one for TenantView and ViewIndex each.
        // Since the PHOENIX_TTL property values are being set,
        // we expect the view header columns to show up in regular scans too.
        assertViewHeaderRowsHavePhoenixTTLRelatedCells(
                schemaBuilder.getTableOptions().getSchemaName(), startTime, false, 2);
        assertSyscatHavePhoenixTTLRelatedColumns(tenantId, schemaName, tenantViewName,
                PTableType.VIEW.getSerializedValue(), 120000);
        assertSyscatHavePhoenixTTLRelatedColumns(tenantId, schemaName, indexOnTenantViewName,
                PTableType.INDEX.getSerializedValue(), 120000);

    }

    @Test
    public void testPhoenixTTLForLevelTwoView() throws Exception {
        long startTime = EnvironmentEdgeManager.currentTimeMillis();

        final SchemaBuilder schemaBuilder = createLevel2TenantViewWithGlobalLevelTTL(null, null);

        String tenantId = schemaBuilder.getDataOptions().getTenantId();
        String schemaName = stripQuotes(
                SchemaUtil.getSchemaNameFromFullName(schemaBuilder.getEntityTenantViewName()));
        String globalViewName = stripQuotes(
                SchemaUtil.getTableNameFromFullName(schemaBuilder.getEntityGlobalViewName()));
        String tenantViewName = stripQuotes(
                SchemaUtil.getTableNameFromFullName(schemaBuilder.getEntityTenantViewName()));
        String indexOnGlobalViewName = String.format("IDX_%s", globalViewName);
        String indexOnTenantViewName = String
                .format("IDX_%s", stripQuotes(schemaBuilder.getEntityKeyPrefix()));

        // Expected 4 rows - one for GlobalView, one for TenantView and ViewIndex each.
        // Since the PHOENIX_TTL property values are being set,
        // we expect the view header columns to show up in regular scans too.
        assertViewHeaderRowsHavePhoenixTTLRelatedCells(
                schemaBuilder.getTableOptions().getSchemaName(), startTime, false, 4);
        assertSyscatHavePhoenixTTLRelatedColumns("", schemaName, globalViewName,
                PTableType.VIEW.getSerializedValue(), 300000);
        assertSyscatHavePhoenixTTLRelatedColumns("", schemaName, indexOnGlobalViewName,
                PTableType.INDEX.getSerializedValue(), 300000);
        // Since the PHOENIX_TTL property values are not being overridden,
        // we expect the TTL value to be same as the global view.
        assertSyscatHavePhoenixTTLRelatedColumns(tenantId, schemaName, tenantViewName,
                PTableType.VIEW.getSerializedValue(), 300000);
        assertSyscatHavePhoenixTTLRelatedColumns(tenantId, schemaName, indexOnTenantViewName,
                PTableType.INDEX.getSerializedValue(), 300000);
    }

    @Test
    public void testPhoenixTTLForWhenTTLIsZero() throws Exception {
        long startTime = EnvironmentEdgeManager.currentTimeMillis();

        TenantViewOptions tenantViewOptions = TenantViewOptions.withDefaults();
        // Client can also specify PHOENIX_TTL=NONE
        tenantViewOptions.setTableProps("PHOENIX_TTL=0");
        final SchemaBuilder schemaBuilder = createLevel1TenantView(tenantViewOptions, null);

        String tenantId = schemaBuilder.getDataOptions().getTenantId();
        String schemaName = stripQuotes(
                SchemaUtil.getSchemaNameFromFullName(schemaBuilder.getEntityTenantViewName()));
        String tenantViewName = stripQuotes(
                SchemaUtil.getTableNameFromFullName(schemaBuilder.getEntityTenantViewName()));
        String indexOnTenantViewName = String
                .format("IDX_%s", stripQuotes(schemaBuilder.getEntityKeyPrefix()));

        // Expected 3 deleted rows - one for Table, one for TenantView and ViewIndex each.
        // Since the PHOENIX_TTL property values are not being set or being set to zero,
        // we expect the view header columns to show up in raw scans only.
        assertViewHeaderRowsHavePhoenixTTLRelatedCells(
                schemaBuilder.getTableOptions().getSchemaName(), startTime, true, 3);
        assertSyscatHavePhoenixTTLRelatedColumns(tenantId, schemaName, tenantViewName,
                PTableType.VIEW.getSerializedValue(), 0);
        assertSyscatHavePhoenixTTLRelatedColumns(tenantId, schemaName, indexOnTenantViewName,
                PTableType.INDEX.getSerializedValue(), 0);

    }

    @Test
    public void testPhoenixTTLWithAlterView() throws Exception {
        long startTime = EnvironmentEdgeManager.currentTimeMillis();

        TenantViewOptions tenantViewOptions = TenantViewOptions.withDefaults();
        // Client can also specify PHOENIX_TTL=0
        tenantViewOptions.setTableProps("PHOENIX_TTL=NONE");
        final SchemaBuilder schemaBuilder = createLevel1TenantView(tenantViewOptions, null);

        String tenantId = schemaBuilder.getDataOptions().getTenantId();
        String schemaName = stripQuotes(
                SchemaUtil.getSchemaNameFromFullName(schemaBuilder.getEntityTenantViewName()));
        String tenantViewName = stripQuotes(
                SchemaUtil.getTableNameFromFullName(schemaBuilder.getEntityTenantViewName()));
        String indexOnTenantViewName = String
                .format("IDX_%s", stripQuotes(schemaBuilder.getEntityKeyPrefix()));

        // Expected 3 deleted rows - one for Table, one for TenantView and ViewIndex each.
        // Since the PHOENIX_TTL property values are not being set or being set to zero,
        // we expect the view header columns to show up in raw scans only.
        assertViewHeaderRowsHavePhoenixTTLRelatedCells(
                schemaBuilder.getTableOptions().getSchemaName(), startTime, true, 3);
        assertSyscatHavePhoenixTTLRelatedColumns(tenantId, schemaName, tenantViewName,
                PTableType.VIEW.getSerializedValue(), 0);
        assertSyscatHavePhoenixTTLRelatedColumns(tenantId, schemaName, indexOnTenantViewName,
                PTableType.INDEX.getSerializedValue(), 0);

        String tenantURL = getUrl() + ';' + TENANT_ID_ATTRIB + '=' + tenantId;
        try (Connection connection = DriverManager.getConnection(tenantURL)) {
            try (Statement stmt = connection.createStatement()) {
                // Phoenix TTL is set to 120s => 120000 ms
                String sql = String
                        .format(ALTER_PHOENIX_TTL_SQL, schemaName, tenantViewName, "120");
                stmt.execute(sql);
            }
        }

        // Expected 2 rows - one for TenantView and ViewIndex each.
        // Since the PHOENIX_TTL property values are being set,
        // we expect the view header columns to show up in regular scans too.
        assertViewHeaderRowsHavePhoenixTTLRelatedCells(
                schemaBuilder.getTableOptions().getSchemaName(), startTime, false, 2);
        assertSyscatHavePhoenixTTLRelatedColumns(tenantId, schemaName, tenantViewName,
                PTableType.VIEW.getSerializedValue(), 120000);
        assertSyscatHavePhoenixTTLRelatedColumns(tenantId, schemaName, indexOnTenantViewName,
                PTableType.INDEX.getSerializedValue(), 120000);

    }

    @Test
    public void testCreateViewWithParentPhoenixTTLFails() throws Exception {
        try {
            TenantViewOptions tenantViewWithOverrideOptions = TenantViewOptions.withDefaults();
            // Phoenix TTL is set to 120s => 120000 ms
            tenantViewWithOverrideOptions.setTableProps("PHOENIX_TTL=120");
            final SchemaBuilder schemaBuilder = createLevel2TenantViewWithGlobalLevelTTL(
                    tenantViewWithOverrideOptions, null);
            fail();
        } catch (SQLException e) {
            assertEquals(SQLExceptionCode.CANNOT_SET_OR_ALTER_PHOENIX_TTL.getErrorCode(),
                    e.getErrorCode());
        }
    }

    @Test
    public void testAlterViewWithParentPhoenixTTLFails() throws Exception {
        long startTime = EnvironmentEdgeManager.currentTimeMillis();

        // Phoenix TTL is set to 300s
        final SchemaBuilder schemaBuilder = createLevel2TenantViewWithGlobalLevelTTL(null, null);

        String tenantId = schemaBuilder.getDataOptions().getTenantId();
        String schemaName = stripQuotes(
                SchemaUtil.getSchemaNameFromFullName(schemaBuilder.getEntityTenantViewName()));
        String globalViewName = stripQuotes(
                SchemaUtil.getTableNameFromFullName(schemaBuilder.getEntityGlobalViewName()));
        String tenantViewName = stripQuotes(
                SchemaUtil.getTableNameFromFullName(schemaBuilder.getEntityTenantViewName()));
        String indexOnGlobalViewName = String.format("IDX_%s", globalViewName);
        String indexOnTenantViewName = String
                .format("IDX_%s", stripQuotes(schemaBuilder.getEntityKeyPrefix()));

        // Expected 4 rows - one for GlobalView, one for TenantView and ViewIndex each.
        // Since the PHOENIX_TTL property values are being set,
        // we expect the view header columns to show up in regular scans too.
        assertViewHeaderRowsHavePhoenixTTLRelatedCells(
                schemaBuilder.getTableOptions().getSchemaName(), startTime, false, 4);
        assertSyscatHavePhoenixTTLRelatedColumns("", schemaName, globalViewName,
                PTableType.VIEW.getSerializedValue(), 300000);
        assertSyscatHavePhoenixTTLRelatedColumns("", schemaName, indexOnGlobalViewName,
                PTableType.INDEX.getSerializedValue(), 300000);
        // Since the PHOENIX_TTL property values are not being overridden,
        // we expect the TTL value to be same as the global view.
        assertSyscatHavePhoenixTTLRelatedColumns(tenantId, schemaName, tenantViewName,
                PTableType.VIEW.getSerializedValue(), 300000);
        assertSyscatHavePhoenixTTLRelatedColumns(tenantId, schemaName, indexOnTenantViewName,
                PTableType.INDEX.getSerializedValue(), 300000);

        String tenantURL = getUrl() + ';' + TENANT_ID_ATTRIB + '=' + tenantId;
        try (Connection connection = DriverManager.getConnection(tenantURL)) {
            try (Statement stmt = connection.createStatement()) {
                // Phoenix TTL is set to 120s => 120000 ms
                String sql = String
                        .format(ALTER_PHOENIX_TTL_SQL, schemaName, tenantViewName, "120");
                stmt.execute(sql);
                fail();
            }
        } catch (SQLException e) {
            assertEquals(SQLExceptionCode.CANNOT_SET_OR_ALTER_PHOENIX_TTL.getErrorCode(),
                    e.getErrorCode());
        }
    }

    @Test
    public void testAlterViewWithChildLevelPhoenixTTLFails() throws Exception {
        long startTime = EnvironmentEdgeManager.currentTimeMillis();

        // Phoenix TTL is set to 300s
        final SchemaBuilder schemaBuilder = createLevel2TenantViewWithTenantLevelTTL(null, null);

        String tenantId = schemaBuilder.getDataOptions().getTenantId();
        String schemaName = stripQuotes(
                SchemaUtil.getSchemaNameFromFullName(schemaBuilder.getEntityTenantViewName()));
        String globalViewName = stripQuotes(
                SchemaUtil.getTableNameFromFullName(schemaBuilder.getEntityGlobalViewName()));
        String tenantViewName = stripQuotes(
                SchemaUtil.getTableNameFromFullName(schemaBuilder.getEntityTenantViewName()));
        String indexOnGlobalViewName = String.format("IDX_%s", globalViewName);
        String indexOnTenantViewName = String
                .format("IDX_%s", stripQuotes(schemaBuilder.getEntityKeyPrefix()));

        // Expected 2 rows - one for TenantView and ViewIndex each.
        // Since the PHOENIX_TTL property values are being set,
        // we expect the view header columns to show up in regular scans too.
        assertViewHeaderRowsHavePhoenixTTLRelatedCells(
                schemaBuilder.getTableOptions().getSchemaName(), startTime, false, 2);
        assertSyscatHavePhoenixTTLRelatedColumns(tenantId, schemaName, tenantViewName,
                PTableType.VIEW.getSerializedValue(), 300000);
        assertSyscatHavePhoenixTTLRelatedColumns(tenantId, schemaName, indexOnTenantViewName,
                PTableType.INDEX.getSerializedValue(), 300000);

        try (Connection connection = DriverManager.getConnection(getUrl())) {
            try (Statement stmt = connection.createStatement()) {
                // Phoenix TTL is set to 120s => 120000 ms
                String sql = String
                        .format(ALTER_PHOENIX_TTL_SQL, schemaName, globalViewName, "120");
                stmt.execute(sql);
                fail();
            }
        } catch (SQLException e) {
            assertEquals(SQLExceptionCode.CANNOT_SET_OR_ALTER_PHOENIX_TTL.getErrorCode(),
                    e.getErrorCode());
        }
    }

    @Test
    public void testAlterViewWithNoPhoenixTTLSucceed() throws Exception {
        long startTime = EnvironmentEdgeManager.currentTimeMillis();

        // Phoenix TTL is set to 300s
        final SchemaBuilder schemaBuilder = createLevel2TenantViewWithTenantLevelTTL(null, null);

        String tenantId = schemaBuilder.getDataOptions().getTenantId();
        String schemaName = stripQuotes(
                SchemaUtil.getSchemaNameFromFullName(schemaBuilder.getEntityTenantViewName()));
        String globalViewName = stripQuotes(
                SchemaUtil.getTableNameFromFullName(schemaBuilder.getEntityGlobalViewName()));
        String tenantViewName = stripQuotes(
                SchemaUtil.getTableNameFromFullName(schemaBuilder.getEntityTenantViewName()));
        String indexOnGlobalViewName = String.format("IDX_%s", globalViewName);
        String indexOnTenantViewName = String
                .format("IDX_%s", stripQuotes(schemaBuilder.getEntityKeyPrefix()));

        // Expected 2 rows - one for TenantView and ViewIndex each.
        // Since the PHOENIX_TTL property values are being set,
        // we expect the view header columns to show up in regular scans too.
        assertViewHeaderRowsHavePhoenixTTLRelatedCells(
                schemaBuilder.getTableOptions().getSchemaName(), startTime, false, 2);
        assertSyscatHavePhoenixTTLRelatedColumns(tenantId, schemaName, tenantViewName,
                PTableType.VIEW.getSerializedValue(), 300000);
        assertSyscatHavePhoenixTTLRelatedColumns(tenantId, schemaName, indexOnTenantViewName,
                PTableType.INDEX.getSerializedValue(), 300000);

        // ALTER global view
        try (Connection connection = DriverManager.getConnection(getUrl())) {
            try (Statement stmt = connection.createStatement()) {
                String sql = String
                        .format(ALTER_SQL_WITH_NO_TTL, schemaName, globalViewName, "COL_30");
                stmt.execute(sql);
            }
        }

        // ALTER tenant view
        String tenantURL = getUrl() + ';' + TENANT_ID_ATTRIB + '=' + tenantId;
        try (Connection connection = DriverManager.getConnection(tenantURL)) {
            try (Statement stmt = connection.createStatement()) {
                String sql = String
                        .format(ALTER_SQL_WITH_NO_TTL, schemaName, tenantViewName, "COL_100");
                stmt.execute(sql);
            }
        }

    }

    @Test
    public void testResetPhoenixTTL() throws Exception {
        long startTime = EnvironmentEdgeManager.currentTimeMillis();

        final SchemaBuilder schemaBuilder = createLevel2TenantViewWithGlobalLevelTTL(null, null);
        String tenantId = schemaBuilder.getDataOptions().getTenantId();
        String schemaName = stripQuotes(
                SchemaUtil.getSchemaNameFromFullName(schemaBuilder.getEntityTenantViewName()));
        String globalViewName = stripQuotes(
                SchemaUtil.getTableNameFromFullName(schemaBuilder.getEntityGlobalViewName()));
        String tenantViewName = stripQuotes(
                SchemaUtil.getTableNameFromFullName(schemaBuilder.getEntityTenantViewName()));
        String indexOnGlobalViewName = String.format("IDX_%s", globalViewName);
        String indexOnTenantViewName = String
                .format("IDX_%s", stripQuotes(schemaBuilder.getEntityKeyPrefix()));

        try (Connection connection = DriverManager.getConnection(getUrl())) {
            try (Statement stmt = connection.createStatement()) {
                // Phoenix TTL is set to 'NONE'
                String sql = String
                        .format(ALTER_PHOENIX_TTL_SQL, schemaName, globalViewName, "'NONE'");
                stmt.execute(sql);
            }
        }

        // Expected 4 rows - one for GlobalView, one for TenantView and ViewIndex each.
        // Since the PHOENIX_TTL property values for global view are being reset,
        // we expect the view header columns value to be set to zero.
        assertViewHeaderRowsHavePhoenixTTLRelatedCells(
                schemaBuilder.getTableOptions().getSchemaName(), startTime, false, 4);
        assertSyscatHavePhoenixTTLRelatedColumns("", schemaName, globalViewName,
                PTableType.VIEW.getSerializedValue(), 0);
        assertSyscatHavePhoenixTTLRelatedColumns("", schemaName, indexOnGlobalViewName,
                PTableType.INDEX.getSerializedValue(), 0);
        // Since the PHOENIX_TTL property values for the tenant view are not being reset,
        // we expect the TTL value to be same as before.
        assertSyscatHavePhoenixTTLRelatedColumns(tenantId, schemaName, tenantViewName,
                PTableType.VIEW.getSerializedValue(), 300000);
        assertSyscatHavePhoenixTTLRelatedColumns(tenantId, schemaName, indexOnTenantViewName,
                PTableType.INDEX.getSerializedValue(), 300000);
    }


    @Test
    public void testWithTenantViewAndNoGlobalView() throws Exception {
        // PHOENIX TTL is set in seconds (for e.g 10 secs)
        long phoenixTTL = 10;
        TableOptions tableOptions = TableOptions.withDefaults();
        tableOptions.getTableColumns().clear();
        tableOptions.getTableColumnTypes().clear();

        TenantViewOptions tenantViewOptions = TenantViewOptions.withDefaults();
        tenantViewOptions.setTableProps(String.format("PHOENIX_TTL=%d", phoenixTTL));

        // Define the test schema.
        final SchemaBuilder schemaBuilder = new SchemaBuilder(getUrl());
        schemaBuilder.withTableOptions(tableOptions).withTenantViewOptions(tenantViewOptions)
                .build();

        // Define the test data.
        DataSupplier dataSupplier = new DataSupplier() {

            @Override public List<Object> getValues(int rowIndex) {
                Random rnd = new Random();
                String zid = String.format(ID_FMT, rowIndex);
                String col7 = String.format(COL7_FMT, rowIndex + rnd.nextInt(MAX_ROWS));
                String col8 = String.format(COL8_FMT, rowIndex + rnd.nextInt(MAX_ROWS));
                String col9 = String.format(COL9_FMT, rowIndex + rnd.nextInt(MAX_ROWS));
                return Lists.newArrayList(new Object[] { zid, col7, col8, col9 });
            }
        };

        // Create a test data reader/writer for the above schema.
        DataWriter dataWriter = new BasicDataWriter();
        DataReader dataReader = new BasicDataReader();

        List<String> columns = Lists.newArrayList("ZID", "COL7", "COL8", "COL9");
        List<String> rowKeyColumns = Lists.newArrayList("ZID");
        String
                tenantConnectUrl =
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
            upsertDataAndRunValidations(phoenixTTL, DEFAULT_NUM_ROWS, dataWriter, dataReader,
                    schemaBuilder);
        }
    }

    @Test
    public void testWithSQLUsingIndexWithCoveredColsUpdates() throws Exception {

        // PHOENIX TTL is set in seconds (for e.g 10 secs)
        long phoenixTTL = 10;
        TableOptions tableOptions = TableOptions.withDefaults();
        tableOptions.getTableColumns().clear();
        tableOptions.getTableColumnTypes().clear();

        GlobalViewOptions globalViewOptions = SchemaBuilder.GlobalViewOptions.withDefaults();
        globalViewOptions.setTableProps(String.format("PHOENIX_TTL=%d", phoenixTTL));

        GlobalViewIndexOptions
                globalViewIndexOptions =
                SchemaBuilder.GlobalViewIndexOptions.withDefaults();

        TenantViewOptions tenantViewOptions = new TenantViewOptions();
        tenantViewOptions.setTenantViewColumns(asList("ZID", "COL7", "COL8", "COL9"));
        tenantViewOptions
                .setTenantViewColumnTypes(asList("CHAR(15)", "VARCHAR", "VARCHAR", "VARCHAR"));

        OtherOptions testCaseWhenAllCFMatchAndAllDefault = new OtherOptions();
        testCaseWhenAllCFMatchAndAllDefault.setTestName("testCaseWhenAllCFMatchAndAllDefault");
        testCaseWhenAllCFMatchAndAllDefault
                .setTableCFs(Lists.newArrayList((String) null, null, null));
        testCaseWhenAllCFMatchAndAllDefault
                .setGlobalViewCFs(Lists.newArrayList((String) null, null, null));
        testCaseWhenAllCFMatchAndAllDefault
                .setTenantViewCFs(Lists.newArrayList((String) null, null, null, null));

        for (boolean isGlobalIndexLocal : Lists.newArrayList(true, false)) {
            globalViewIndexOptions.setLocal(isGlobalIndexLocal);

            // Define the test schema.
            final SchemaBuilder schemaBuilder = new SchemaBuilder(getUrl());
            schemaBuilder.withTableOptions(tableOptions).withGlobalViewOptions(globalViewOptions)
                    .withGlobalViewIndexOptions(globalViewIndexOptions)
                    .withTenantViewOptions(tenantViewOptions)
                    .withOtherOptions(testCaseWhenAllCFMatchAndAllDefault).build();

            // Define the test data.
            final List<String> outerCol4s = Lists.newArrayList();
            DataSupplier dataSupplier = new DataSupplier() {
                String col4ForWhereClause;

                @Override public List<Object> getValues(int rowIndex) {
                    Random rnd = new Random();
                    String id = String.format(ID_FMT, rowIndex);
                    String zid = String.format(ZID_FMT, rowIndex);
                    String col4 = String.format(COL4_FMT, rowIndex + rnd.nextInt(MAX_ROWS));

                    // Store the col4 data to be used later in a where clause
                    outerCol4s.add(col4);
                    String col5 = String.format(COL5_FMT, rowIndex + rnd.nextInt(MAX_ROWS));
                    String col6 = String.format(COL6_FMT, rowIndex + rnd.nextInt(MAX_ROWS));
                    String col7 = String.format(COL7_FMT, rowIndex + rnd.nextInt(MAX_ROWS));
                    String col8 = String.format(COL8_FMT, rowIndex + rnd.nextInt(MAX_ROWS));
                    String col9 = String.format(COL9_FMT, rowIndex + rnd.nextInt(MAX_ROWS));

                    return Lists
                            .newArrayList(new Object[] { id, zid, col4, col5, col6, col7, col8, col9 });
                }
            };

            // Create a test data reader/writer for the above schema.
            DataWriter dataWriter = new BasicDataWriter();
            DataReader dataReader = new BasicDataReader();

            List<String> columns =
                    Lists.newArrayList("ID", "ZID", "COL4", "COL5", "COL6", "COL7", "COL8", "COL9");
            List<String> rowKeyColumns = Lists.newArrayList("COL6");
            String tenantConnectUrl =
                    getUrl() + ';' + TENANT_ID_ATTRIB + '=' + schemaBuilder.getDataOptions().getTenantId();
            try (Connection writeConnection = DriverManager.getConnection(tenantConnectUrl)) {
                writeConnection.setAutoCommit(true);
                dataWriter.setConnection(writeConnection);
                dataWriter.setDataSupplier(dataSupplier);
                dataWriter.setUpsertColumns(columns);
                dataWriter.setRowKeyColumns(rowKeyColumns);
                dataWriter.setTargetEntity(schemaBuilder.getEntityTenantViewName());

                // Upsert data for validation
                upsertData(dataWriter, DEFAULT_NUM_ROWS);

                dataReader.setValidationColumns(rowKeyColumns);
                dataReader.setRowKeyColumns(rowKeyColumns);
                dataReader.setDML(String.format("SELECT col6 from %s where col4 = '%s'",
                        schemaBuilder.getEntityTenantViewName(), outerCol4s.get(1)));
                dataReader.setTargetEntity(schemaBuilder.getEntityTenantViewName());

                // Validate data before and after ttl expiration.
                validateExpiredRowsAreNotReturnedUsingCounts(phoenixTTL, dataReader, schemaBuilder);
            }
        }
    }

    /**
     * Ensure/validate that empty columns for the index are still updated even when covered columns
     * are not updated.
     *
     * @throws Exception
     */
    @Test
    public void testWithSQLUsingIndexAndNoCoveredColsUpdates() throws Exception {

        // PHOENIX TTL is set in seconds (for e.g 10 secs)
        long phoenixTTL = 10;
        TableOptions tableOptions = TableOptions.withDefaults();
        tableOptions.getTableColumns().clear();
        tableOptions.getTableColumnTypes().clear();

        GlobalViewOptions globalViewOptions = SchemaBuilder.GlobalViewOptions.withDefaults();
        globalViewOptions.setTableProps(String.format("PHOENIX_TTL=%d", phoenixTTL));

        GlobalViewIndexOptions globalViewIndexOptions =
                SchemaBuilder.GlobalViewIndexOptions.withDefaults();

        TenantViewOptions tenantViewOptions = new TenantViewOptions();
        tenantViewOptions.setTenantViewColumns(asList("ZID", "COL7", "COL8", "COL9"));
        tenantViewOptions
                .setTenantViewColumnTypes(asList("CHAR(15)", "VARCHAR", "VARCHAR", "VARCHAR"));

        OtherOptions testCaseWhenAllCFMatchAndAllDefault = new OtherOptions();
        testCaseWhenAllCFMatchAndAllDefault.setTestName("testCaseWhenAllCFMatchAndAllDefault");
        testCaseWhenAllCFMatchAndAllDefault
                .setTableCFs(Lists.newArrayList((String) null, null, null));
        testCaseWhenAllCFMatchAndAllDefault
                .setGlobalViewCFs(Lists.newArrayList((String) null, null, null));
        testCaseWhenAllCFMatchAndAllDefault
                .setTenantViewCFs(Lists.newArrayList((String) null, null, null, null));

        for (boolean isGlobalIndexLocal : Lists.newArrayList(true, false)) {
            globalViewIndexOptions.setLocal(isGlobalIndexLocal);

            // Define the test schema.
            final SchemaBuilder schemaBuilder = new SchemaBuilder(getUrl());
            schemaBuilder.withTableOptions(tableOptions).withGlobalViewOptions(globalViewOptions)
                    .withGlobalViewIndexOptions(globalViewIndexOptions)
                    .withTenantViewOptions(tenantViewOptions)
                    .withOtherOptions(testCaseWhenAllCFMatchAndAllDefault).build();

            // Define the test data.
            final List<String> outerCol4s = Lists.newArrayList();
            DataSupplier dataSupplier = new DataSupplier() {

                @Override public List<Object> getValues(int rowIndex) {
                    Random rnd = new Random();
                    String id = String.format(ID_FMT, rowIndex);
                    String zid = String.format(ZID_FMT, rowIndex);
                    String col4 = String.format(COL4_FMT, rowIndex + rnd.nextInt(MAX_ROWS));

                    // Store the col4 data to be used later in a where clause
                    outerCol4s.add(col4);
                    String col5 = String.format(COL5_FMT, rowIndex + rnd.nextInt(MAX_ROWS));
                    String col6 = String.format(COL6_FMT, rowIndex + rnd.nextInt(MAX_ROWS));
                    String col7 = String.format(COL7_FMT, rowIndex + rnd.nextInt(MAX_ROWS));
                    String col8 = String.format(COL8_FMT, rowIndex + rnd.nextInt(MAX_ROWS));
                    String col9 = String.format(COL9_FMT, rowIndex + rnd.nextInt(MAX_ROWS));

                    return Lists
                            .newArrayList(new Object[] { id, zid, col4, col5, col6, col7, col8, col9 });
                }
            };

            // Create a test data reader/writer for the above schema.
            DataWriter dataWriter = new BasicDataWriter();
            DataReader dataReader = new BasicDataReader();

            List<String> columns =
                    Lists.newArrayList("ID", "ZID", "COL4", "COL5", "COL6", "COL7", "COL8", "COL9");
            List<String> nonCoveredColumns =
                    Lists.newArrayList("ID", "ZID", "COL5", "COL7", "COL8", "COL9");
            List<String> rowKeyColumns = Lists.newArrayList("COL6");
            String tenantConnectUrl =
                    getUrl() + ';' + TENANT_ID_ATTRIB + '=' + schemaBuilder.getDataOptions().getTenantId();
            try (Connection writeConnection = DriverManager.getConnection(tenantConnectUrl)) {
                writeConnection.setAutoCommit(true);
                dataWriter.setConnection(writeConnection);
                dataWriter.setDataSupplier(dataSupplier);
                dataWriter.setUpsertColumns(columns);
                dataWriter.setRowKeyColumns(rowKeyColumns);
                dataWriter.setTargetEntity(schemaBuilder.getEntityTenantViewName());

                // Upsert data for validation
                upsertData(dataWriter, DEFAULT_NUM_ROWS);

                dataReader.setValidationColumns(rowKeyColumns);
                dataReader.setRowKeyColumns(rowKeyColumns);
                dataReader.setDML(String.format("SELECT col6 from %s where col4 = '%s'",
                        schemaBuilder.getEntityTenantViewName(), outerCol4s.get(1)));
                dataReader.setTargetEntity(schemaBuilder.getEntityTenantViewName());

                // Validate data before and after ttl expiration.
                validateExpiredRowsAreNotReturnedUsingCounts(phoenixTTL, dataReader, schemaBuilder);

                // Now update the above data but not modifying the covered columns.
                // Ensure/validate that empty columns for the index are still updated.

                // Data supplier where covered and included (col4 and col6) columns are not updated.
                DataSupplier dataSupplierForNonCoveredColumns = new DataSupplier() {

                    @Override public List<Object> getValues(int rowIndex) {
                        Random rnd = new Random();
                        String id = String.format(ID_FMT, rowIndex);
                        String zid = String.format(ZID_FMT, rowIndex);
                        String col5 = String.format(COL5_FMT, rowIndex + rnd.nextInt(MAX_ROWS));
                        String col7 = String.format(COL7_FMT, rowIndex + rnd.nextInt(MAX_ROWS));
                        String col8 = String.format(COL8_FMT, rowIndex + rnd.nextInt(MAX_ROWS));
                        String col9 = String.format(COL9_FMT, rowIndex + rnd.nextInt(MAX_ROWS));

                        return Lists.newArrayList(new Object[] { id, zid, col5, col7, col8, col9 });
                    }
                };

                // Upsert data for validation with non covered columns
                dataWriter.setDataSupplier(dataSupplierForNonCoveredColumns);
                dataWriter.setUpsertColumns(nonCoveredColumns);
                upsertData(dataWriter, DEFAULT_NUM_ROWS);

                List<String> rowKeyColumns1 = Lists.newArrayList("ID", "COL6");
                dataReader.setValidationColumns(rowKeyColumns1);
                dataReader.setRowKeyColumns(rowKeyColumns1);
                dataReader.setDML(String.format("SELECT id, col6 from %s where col4 = '%s'",
                        schemaBuilder.getEntityTenantViewName(), outerCol4s.get(1)));

                // Validate data before and after ttl expiration.
                validateExpiredRowsAreNotReturnedUsingCounts(phoenixTTL, dataReader, schemaBuilder);
            }
        }

    }


    /**
     * Ensure/validate that correct parent's phoenix ttl value is used when queries are using the
     * index.
     *
     * @throws Exception
     */
    @Test
    public void testWithSQLUsingIndexAndMultiLevelViews() throws Exception {

        // PHOENIX TTL is set in seconds (for e.g 10 secs)
        long phoenixTTL = 10;
        TableOptions tableOptions = TableOptions.withDefaults();
        tableOptions.getTableColumns().clear();
        tableOptions.getTableColumnTypes().clear();

        GlobalViewOptions globalViewOptions = SchemaBuilder.GlobalViewOptions.withDefaults();
        globalViewOptions.setTableProps(String.format("PHOENIX_TTL=%d", phoenixTTL));

        GlobalViewIndexOptions globalViewIndexOptions =
                SchemaBuilder.GlobalViewIndexOptions.withDefaults();
        globalViewIndexOptions.setLocal(false);

        TenantViewOptions tenantViewOptions = new TenantViewOptions();
        tenantViewOptions.setTenantViewColumns(asList("ZID", "COL7", "COL8", "COL9"));
        tenantViewOptions
                .setTenantViewColumnTypes(asList("CHAR(15)", "VARCHAR", "VARCHAR", "VARCHAR"));


        OtherOptions testCaseWhenAllCFMatchAndAllDefault = new OtherOptions();
        testCaseWhenAllCFMatchAndAllDefault.setTestName("testCaseWhenAllCFMatchAndAllDefault");
        testCaseWhenAllCFMatchAndAllDefault
                .setTableCFs(Lists.newArrayList((String) null, null, null));
        testCaseWhenAllCFMatchAndAllDefault
                .setGlobalViewCFs(Lists.newArrayList((String) null, null, null));
        testCaseWhenAllCFMatchAndAllDefault
                .setTenantViewCFs(Lists.newArrayList((String) null, null, null, null));

        // Define the test schema.
        final SchemaBuilder schemaBuilder = new SchemaBuilder(getUrl());
        schemaBuilder.withTableOptions(tableOptions).withGlobalViewOptions(globalViewOptions)
                .withGlobalViewIndexOptions(globalViewIndexOptions)
                .withTenantViewOptions(tenantViewOptions)
                .withOtherOptions(testCaseWhenAllCFMatchAndAllDefault).build();


        String level3ViewName = String.format("%s.%s",
                DEFAULT_SCHEMA_NAME, "E11");
        String level3ViewCreateSQL = String.format("CREATE VIEW IF NOT EXISTS %s AS SELECT * FROM %s",
                level3ViewName,
                schemaBuilder.getEntityTenantViewName());
        String tConnectUrl =
                getUrl() + ';' + TENANT_ID_ATTRIB + '=' + schemaBuilder.getDataOptions().getTenantId();
        try (Connection tConnection = DriverManager.getConnection(tConnectUrl)) {
            tConnection.createStatement().execute(level3ViewCreateSQL);
        }


        // Define the test data.
        final List<String> outerCol4s = Lists.newArrayList();
        DataSupplier dataSupplier = new DataSupplier() {

            @Override public List<Object> getValues(int rowIndex) {
                Random rnd = new Random();
                String id = String.format(ID_FMT, rowIndex);
                String zid = String.format(ZID_FMT, rowIndex);
                String col4 = String.format(COL4_FMT, rowIndex + rnd.nextInt(MAX_ROWS));

                // Store the col4 data to be used later in a where clause
                outerCol4s.add(col4);
                String col5 = String.format(COL5_FMT, rowIndex + rnd.nextInt(MAX_ROWS));
                String col6 = String.format(COL6_FMT, rowIndex + rnd.nextInt(MAX_ROWS));
                String col7 = String.format(COL7_FMT, rowIndex + rnd.nextInt(MAX_ROWS));
                String col8 = String.format(COL8_FMT, rowIndex + rnd.nextInt(MAX_ROWS));
                String col9 = String.format(COL9_FMT, rowIndex + rnd.nextInt(MAX_ROWS));

                return Lists
                        .newArrayList(new Object[] { id, zid, col4, col5, col6, col7, col8, col9 });
            }
        };

        // Create a test data reader/writer for the above schema.
        DataWriter dataWriter = new BasicDataWriter();
        DataReader dataReader = new BasicDataReader();

        List<String> columns =
                Lists.newArrayList("ID", "ZID", "COL4", "COL5", "COL6", "COL7", "COL8", "COL9");
        List<String> nonCoveredColumns =
                Lists.newArrayList("ID", "ZID", "COL5", "COL7", "COL8", "COL9");
        List<String> rowKeyColumns = Lists.newArrayList("COL6");
        String tenantConnectUrl =
                getUrl() + ';' + TENANT_ID_ATTRIB + '=' + schemaBuilder.getDataOptions().getTenantId();
        try (Connection writeConnection = DriverManager.getConnection(tenantConnectUrl)) {
            writeConnection.setAutoCommit(true);
            dataWriter.setConnection(writeConnection);
            dataWriter.setDataSupplier(dataSupplier);
            dataWriter.setUpsertColumns(columns);
            dataWriter.setRowKeyColumns(rowKeyColumns);
            dataWriter.setTargetEntity(level3ViewName);

            // Upsert data for validation
            upsertData(dataWriter, DEFAULT_NUM_ROWS);

            dataReader.setValidationColumns(rowKeyColumns);
            dataReader.setRowKeyColumns(rowKeyColumns);
            dataReader.setDML(String.format("SELECT col6 from %s where col4 = '%s'",
                    level3ViewName, outerCol4s.get(1)));
            dataReader.setTargetEntity(level3ViewName);

            // Validate data before and after ttl expiration.
            validateExpiredRowsAreNotReturnedUsingCounts(phoenixTTL, dataReader, schemaBuilder);

            // Now update the above data but not modifying the covered columns.
            // Ensure/validate that empty columns for the index are still updated.

            // Data supplier where covered and included (col4 and col6) columns are not updated.
            DataSupplier dataSupplierForNonCoveredColumns = new DataSupplier() {

                @Override public List<Object> getValues(int rowIndex) {
                    Random rnd = new Random();
                    String id = String.format(ID_FMT, rowIndex);
                    String zid = String.format(ZID_FMT, rowIndex);
                    String col5 = String.format(COL5_FMT, rowIndex + rnd.nextInt(MAX_ROWS));
                    String col7 = String.format(COL7_FMT, rowIndex + rnd.nextInt(MAX_ROWS));
                    String col8 = String.format(COL8_FMT, rowIndex + rnd.nextInt(MAX_ROWS));
                    String col9 = String.format(COL9_FMT, rowIndex + rnd.nextInt(MAX_ROWS));

                    return Lists.newArrayList(new Object[] { id, zid, col5, col7, col8, col9 });
                }
            };

            // Upsert data for validation with non covered columns
            dataWriter.setDataSupplier(dataSupplierForNonCoveredColumns);
            dataWriter.setUpsertColumns(nonCoveredColumns);
            upsertData(dataWriter, DEFAULT_NUM_ROWS);

            List<String> rowKeyColumns1 = Lists.newArrayList("ID", "COL6");
            dataReader.setValidationColumns(rowKeyColumns1);
            dataReader.setRowKeyColumns(rowKeyColumns1);
            dataReader.setDML(String.format("SELECT id, col6 from %s where col4 = '%s'",
                    level3ViewName, outerCol4s.get(1)));

            // Validate data before and after ttl expiration.
            validateExpiredRowsAreNotReturnedUsingCounts(phoenixTTL, dataReader, schemaBuilder);

        }
    }

    @Test
    public void testWithVariousSQLs() throws Exception {

        // PHOENIX TTL is set in seconds (for e.g 10 secs)
        long phoenixTTL = 10;
        TableOptions tableOptions = TableOptions.withDefaults();
        tableOptions.getTableColumns().clear();
        tableOptions.getTableColumnTypes().clear();

        GlobalViewOptions globalViewOptions = SchemaBuilder.GlobalViewOptions.withDefaults();
        globalViewOptions.setTableProps(String.format("PHOENIX_TTL=%d", phoenixTTL));

        GlobalViewIndexOptions globalViewIndexOptions =
                SchemaBuilder.GlobalViewIndexOptions.withDefaults();
        globalViewIndexOptions.setLocal(false);

        TenantViewOptions tenantViewOptions = new TenantViewOptions();
        tenantViewOptions.setTenantViewColumns(asList("ZID", "COL7", "COL8", "COL9"));
        tenantViewOptions.setTenantViewColumnTypes(asList("CHAR(15)", "VARCHAR", "VARCHAR", "VARCHAR"));

        OtherOptions testCaseWhenAllCFMatchAndAllDefault = new OtherOptions();
        testCaseWhenAllCFMatchAndAllDefault.setTestName("testCaseWhenAllCFMatchAndAllDefault");
        testCaseWhenAllCFMatchAndAllDefault
                .setTableCFs(Lists.newArrayList((String) null, null, null));
        testCaseWhenAllCFMatchAndAllDefault
                .setGlobalViewCFs(Lists.newArrayList((String) null, null, null));
        testCaseWhenAllCFMatchAndAllDefault
                .setTenantViewCFs(Lists.newArrayList((String) null, null, null, null));

        // Define the test schema.
        final SchemaBuilder schemaBuilder = new SchemaBuilder(getUrl());
        schemaBuilder.withTableOptions(tableOptions).withGlobalViewOptions(globalViewOptions)
                .withGlobalViewIndexOptions(globalViewIndexOptions)
                .withTenantViewOptions(tenantViewOptions)
                .withOtherOptions(testCaseWhenAllCFMatchAndAllDefault).build();

        // Define the test data.
        final String groupById = String.format(ID_FMT, 0);
        DataSupplier dataSupplier = new DataSupplier() {

            @Override public List<Object> getValues(int rowIndex) {
                Random rnd = new Random();
                String zid = String.format(ZID_FMT, rowIndex);
                String col4 = String.format(COL4_FMT, rowIndex + rnd.nextInt(MAX_ROWS));
                String col5 = String.format(COL5_FMT, rowIndex + rnd.nextInt(MAX_ROWS));
                String col6 = String.format(COL6_FMT, rowIndex + rnd.nextInt(MAX_ROWS));
                String col7 = String.format(COL7_FMT, rowIndex + rnd.nextInt(MAX_ROWS));
                String col8 = String.format(COL8_FMT, rowIndex + rnd.nextInt(MAX_ROWS));
                String col9 = String.format(COL9_FMT, rowIndex + rnd.nextInt(MAX_ROWS));

                return Lists.newArrayList(
                        new Object[] { groupById, zid, col4, col5, col6, col7, col8, col9 });
            }
        };

        // Create a test data reader/writer for the above schema.
        DataWriter dataWriter = new BasicDataWriter();
        List<String> columns =
                Lists.newArrayList("ID", "ZID", "COL4", "COL5", "COL6", "COL7", "COL8", "COL9");
        List<String> rowKeyColumns = Lists.newArrayList("ID", "ZID");
        String tenantConnectUrl =
                getUrl() + ';' + TENANT_ID_ATTRIB + '=' + schemaBuilder.getDataOptions().getTenantId();
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

            // Validate data before and after ttl expiration.
            validateExpiredRowsAreNotReturnedUsingCounts(phoenixTTL, dataReader, schemaBuilder);

            // Case : group by sql
            dataReader.setValidationColumns(Arrays.asList("num_rows"));
            dataReader.setRowKeyColumns(Arrays.asList("num_rows"));
            dataReader.setDML(String
                    .format("SELECT count(1) as num_rows from %s GROUP BY ID HAVING count(1) > 0",
                            schemaBuilder.getEntityTenantViewName(), groupById));

            dataReader.setTargetEntity(schemaBuilder.getEntityTenantViewName());

            // Validate data before and after ttl expiration.
            validateExpiredRowsAreNotReturnedUsingCounts(phoenixTTL, dataReader, schemaBuilder);
        }
    }

    @Test
    public void testWithVariousSQLsForMultipleTenants() throws Exception {

        // PHOENIX TTL is set in seconds (for e.g 10 secs)
        long phoenixTTL = 10;
        TableOptions tableOptions = TableOptions.withDefaults();
        tableOptions.getTableColumns().clear();
        tableOptions.getTableColumnTypes().clear();

        GlobalViewOptions globalViewOptions = SchemaBuilder.GlobalViewOptions.withDefaults();
        globalViewOptions.setTableProps(String.format("PHOENIX_TTL=%d", phoenixTTL));

        GlobalViewIndexOptions globalViewIndexOptions =
                SchemaBuilder.GlobalViewIndexOptions.withDefaults();
        globalViewIndexOptions.setLocal(false);

        TenantViewOptions tenantViewOptions = new TenantViewOptions();
        tenantViewOptions.setTenantViewColumns(asList("ZID", "COL7", "COL8", "COL9"));
        tenantViewOptions
                .setTenantViewColumnTypes(asList("CHAR(15)", "VARCHAR", "VARCHAR", "VARCHAR"));

        OtherOptions testCaseWhenAllCFMatchAndAllDefault = new OtherOptions();
        testCaseWhenAllCFMatchAndAllDefault.setTestName("testCaseWhenAllCFMatchAndAllDefault");
        testCaseWhenAllCFMatchAndAllDefault
                .setTableCFs(Lists.newArrayList((String) null, null, null));
        testCaseWhenAllCFMatchAndAllDefault
                .setGlobalViewCFs(Lists.newArrayList((String) null, null, null));
        testCaseWhenAllCFMatchAndAllDefault
                .setTenantViewCFs(Lists.newArrayList((String) null, null, null, null));

        final SchemaBuilder schemaBuilder = new SchemaBuilder(getUrl());
        schemaBuilder.withTableOptions(tableOptions).withGlobalViewOptions(globalViewOptions)
                .withGlobalViewIndexOptions(globalViewIndexOptions)
                .withTenantViewOptions(tenantViewOptions)
                .withOtherOptions(testCaseWhenAllCFMatchAndAllDefault);

        for (int tenant : Arrays.asList(new Integer[] { 1, 2, 3 })) {
            // build schema for tenant
            schemaBuilder.buildWithNewTenant();

            // Define the test data.
            final String groupById = String.format(ID_FMT, 0);
            DataSupplier dataSupplier = new DataSupplier() {

                @Override public List<Object> getValues(int rowIndex) {
                    Random rnd = new Random();
                    String zid = String.format(ZID_FMT, rowIndex);
                    String col4 = String.format(COL4_FMT, rowIndex + rnd.nextInt(MAX_ROWS));
                    String col5 = String.format(COL5_FMT, rowIndex + rnd.nextInt(MAX_ROWS));
                    String col6 = String.format(COL6_FMT, rowIndex + rnd.nextInt(MAX_ROWS));
                    String col7 = String.format(COL7_FMT, rowIndex + rnd.nextInt(MAX_ROWS));
                    String col8 = String.format(COL8_FMT, rowIndex + rnd.nextInt(MAX_ROWS));
                    String col9 = String.format(COL9_FMT, rowIndex + rnd.nextInt(MAX_ROWS));

                    return Lists.newArrayList(
                            new Object[] { groupById, zid, col4, col5, col6, col7, col8, col9 });
                }
            };

            // Create a test data reader/writer for the above schema.
            DataWriter dataWriter = new BasicDataWriter();
            List<String> columns =
                    Lists.newArrayList("ID", "ZID", "COL4", "COL5", "COL6", "COL7", "COL8", "COL9");
            List<String> rowKeyColumns = Lists.newArrayList("ID", "ZID");
            String tenantConnectUrl =
                    getUrl() + ';' + TENANT_ID_ATTRIB + '=' + schemaBuilder.getDataOptions().getTenantId();
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

                // Validate data before and after ttl expiration.
                validateExpiredRowsAreNotReturnedUsingCounts(phoenixTTL, dataReader, schemaBuilder);

                // Case : group by sql
                dataReader.setValidationColumns(Arrays.asList("num_rows"));
                dataReader.setRowKeyColumns(Arrays.asList("num_rows"));
                dataReader.setDML(String
                        .format("SELECT count(1) as num_rows from %s GROUP BY ID HAVING count(1) > 0",
                                schemaBuilder.getEntityTenantViewName()));

                dataReader.setTargetEntity(schemaBuilder.getEntityTenantViewName());

                // Validate data before and after ttl expiration.
                validateExpiredRowsAreNotReturnedUsingCounts(phoenixTTL, dataReader, schemaBuilder);
            }
        }
    }

    @Test
    public void testWithVariousSQLsForMultipleViews() throws Exception {

        // PHOENIX TTL is set in seconds (for e.g 10 secs)
        long phoenixTTL = 10;
        TableOptions tableOptions = TableOptions.withDefaults();
        tableOptions.getTableColumns().clear();
        tableOptions.getTableColumnTypes().clear();

        TenantViewOptions tenantViewOptions = new TenantViewOptions();
        tenantViewOptions.setTenantViewColumns(asList("ZID", "COL7", "COL8", "COL9"));
        tenantViewOptions
                .setTenantViewColumnTypes(asList("CHAR(15)", "VARCHAR", "VARCHAR", "VARCHAR"));

        tenantViewOptions.setTableProps(String.format("PHOENIX_TTL=%d", phoenixTTL));

        OtherOptions testCaseWhenAllCFMatchAndAllDefault = new OtherOptions();
        testCaseWhenAllCFMatchAndAllDefault.setTestName("testCaseWhenAllCFMatchAndAllDefault");
        testCaseWhenAllCFMatchAndAllDefault
                .setTableCFs(Lists.newArrayList((String) null, null, null));
        testCaseWhenAllCFMatchAndAllDefault
                .setGlobalViewCFs(Lists.newArrayList((String) null, null, null));
        testCaseWhenAllCFMatchAndAllDefault
                .setTenantViewCFs(Lists.newArrayList((String) null, null, null, null));

        final SchemaBuilder schemaBuilder = new SchemaBuilder(getUrl());
        schemaBuilder.withTableOptions(tableOptions).withTenantViewOptions(tenantViewOptions)
                .withOtherOptions(testCaseWhenAllCFMatchAndAllDefault);

        for (int view : Arrays.asList(new Integer[] { 1, 2, 3 })) {
            // build schema for new view
            schemaBuilder.buildNewView();

            // Define the test data.
            DataSupplier dataSupplier = new DataSupplier() {

                @Override public List<Object> getValues(int rowIndex) {
                    Random rnd = new Random();
                    String zid = String.format(ZID_FMT, rowIndex);
                    String col7 = String.format(COL7_FMT, rowIndex + rnd.nextInt(MAX_ROWS));
                    String col8 = String.format(COL8_FMT, rowIndex + rnd.nextInt(MAX_ROWS));
                    String col9 = String.format(COL9_FMT, rowIndex + rnd.nextInt(MAX_ROWS));

                    return Lists.newArrayList(new Object[] { zid, col7, col8, col9 });
                }
            };

            // Create a test data reader/writer for the above schema.
            DataWriter dataWriter = new BasicDataWriter();
            List<String> columns = Lists.newArrayList("ZID", "COL7", "COL8", "COL9");
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
                upsertData(dataWriter, DEFAULT_NUM_ROWS);

                // Case : count(1) sql
                DataReader dataReader = new BasicDataReader();
                dataReader.setValidationColumns(Arrays.asList("num_rows"));
                dataReader.setRowKeyColumns(Arrays.asList("num_rows"));
                dataReader.setDML(String
                        .format("SELECT count(1) as num_rows from %s HAVING count(1) > 0",
                                schemaBuilder.getEntityTenantViewName()));
                dataReader.setTargetEntity(schemaBuilder.getEntityTenantViewName());

                // Validate data before and after ttl expiration.
                validateExpiredRowsAreNotReturnedUsingCounts(phoenixTTL, dataReader, schemaBuilder);

                // Case : group by sql
                dataReader.setValidationColumns(Arrays.asList("num_rows"));
                dataReader.setRowKeyColumns(Arrays.asList("num_rows"));
                dataReader.setDML(String
                        .format("SELECT count(1) as num_rows from %s GROUP BY ZID HAVING count(1) > 0",
                                schemaBuilder.getEntityTenantViewName()));

                dataReader.setTargetEntity(schemaBuilder.getEntityTenantViewName());

                // Validate data before and after ttl expiration.
                validateExpiredRowsAreNotReturnedUsingCounts(phoenixTTL, dataReader, schemaBuilder);
            }
        }
    }

    @Test
    public void testWithTenantViewAndGlobalViewAndVariousOptions() throws Exception {

        // PHOENIX TTL is set in seconds (for e.g 10 secs)
        long phoenixTTL = 10;

        // Define the test schema
        TableOptions tableOptions = TableOptions.withDefaults();
        String tableProps = "MULTI_TENANT=true,COLUMN_ENCODED_BYTES=0,DEFAULT_COLUMN_FAMILY='0'";
        tableOptions.setTableProps(tableProps);

        GlobalViewOptions globalViewOptions = SchemaBuilder.GlobalViewOptions.withDefaults();
        globalViewOptions.setTableProps(String.format("PHOENIX_TTL=%d", phoenixTTL));

        GlobalViewIndexOptions globalViewIndexOptions = GlobalViewIndexOptions.withDefaults();

        TenantViewOptions tenantViewOptions = new TenantViewOptions();
        tenantViewOptions.setTenantViewColumns(Lists.newArrayList(TENANT_VIEW_COLUMNS));
        tenantViewOptions.setTenantViewColumnTypes(Lists.newArrayList(COLUMN_TYPES));

        TenantViewIndexOptions tenantViewIndexOptions = TenantViewIndexOptions.withDefaults();
        // Test cases :
        // Local vs Global indexes, Tenant vs Global views, various column family options.
        for (boolean isGlobalViewLocal : Lists.newArrayList(true, false)) {
            for (boolean isTenantViewLocal : Lists.newArrayList(true, false)) {
                for (OtherOptions options : getTableAndGlobalAndTenantColumnFamilyOptions()) {

                    /**
                     * TODO:
                     * Need to revisit indexing code path,
                     * as there are some left over rows after delete in these cases.
                     */
                    if (!isGlobalViewLocal && !isTenantViewLocal) continue;

                    globalViewIndexOptions.setLocal(isGlobalViewLocal);
                    tenantViewIndexOptions.setLocal(isTenantViewLocal);

                    final SchemaBuilder schemaBuilder = new SchemaBuilder(getUrl());
                    schemaBuilder.withTableOptions(tableOptions)
                            .withGlobalViewOptions(globalViewOptions)
                            .withGlobalViewIndexOptions(globalViewIndexOptions)
                            .withTenantViewOptions(tenantViewOptions)
                            .withTenantViewIndexOptions(tenantViewIndexOptions)
                            .withOtherOptions(options)
                            .buildWithNewTenant();

                    // Define the test data.
                    DataSupplier dataSupplier = new DataSupplier() {

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
                    DataWriter dataWriter = new BasicDataWriter();
                    DataReader dataReader = new BasicDataReader();

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
                        upsertDataAndRunValidations(phoenixTTL, DEFAULT_NUM_ROWS, dataWriter,
                                dataReader, schemaBuilder);
                    }

                    PTable table = schemaBuilder.getBaseTable();
                    String schemaName = table.getSchemaName().getString();
                    String tableName = table.getTableName().getString();

                    long scnTimestamp = EnvironmentEdgeManager.currentTimeMillis() +
                            (phoenixTTL * 1000);
                    // Delete expired data rows using global connection.
                    deleteData(true,
                            null,
                            schemaBuilder.getEntityGlobalViewName(),
                            scnTimestamp);

                    if (schemaBuilder.isGlobalViewIndexEnabled() &&
                            schemaBuilder.isGlobalViewIndexCreated()) {
                        String viewIndexName = String
                                .format("%s.IDX_%s",
                                        schemaName,
                                        SchemaUtil.getTableNameFromFullName(
                                                schemaBuilder.getEntityGlobalViewName()));
                        // Delete expired index rows using global connection.
                        deleteIndexData(true,
                                null,
                                viewIndexName,
                                scnTimestamp);
                    }

                    if (schemaBuilder.isTenantViewIndexEnabled() &&
                            schemaBuilder.isTenantViewIndexCreated()) {
                        String viewIndexName = String
                                .format("%s.IDX_%s",
                                        schemaName,
                                        SchemaUtil.getTableNameFromFullName(
                                                schemaBuilder.getEntityTenantViewName()));

                        // Delete expired index rows using tenant connection.
                        deleteIndexData(false,
                                schemaBuilder.getDataOptions().getTenantId(),
                                viewIndexName,
                                scnTimestamp);
                    }
                    // Verify after deleting TTL expired data.
                    Properties props = new Properties();
                    props.setProperty("CurrentSCN", Long.toString(scnTimestamp));

                    try (Connection readConnection = DriverManager
                            .getConnection(tenantConnectUrl, props)) {

                        dataReader.setConnection(readConnection);
                        org.apache.phoenix.thirdparty.com.google.common.collect.Table<String, String, Object>
                                fetchedData =
                                fetchData(dataReader);
                        assertTrue("Deleted rows should not be fetched",
                                fetchedData.rowKeySet().size() == 0);
                    }

                    byte[] hbaseBaseTableName = SchemaUtil.getTableNameAsBytes(
                            schemaName,tableName);
                    String viewIndexSchemaName = String
                            .format("_IDX_%s", table.getSchemaName().getString());
                    byte[] hbaseViewIndexTableName =
                            SchemaUtil.getTableNameAsBytes(viewIndexSchemaName, tableName);
                    // Validate deletes using hbase
                    assertUsingHBaseRows(hbaseBaseTableName, earliestTimestamp, 0);
                    assertUsingHBaseRows(hbaseViewIndexTableName, earliestTimestamp, 0);
                }
            }
        }
    }

    /**
     * ************************************************************
     * Case: Build schema with TTL set by the tenant view.
     * TTL for GLOBAL_VIEW - 300000ms (not set)
     * TTL for TENANT_VIEW - 300000ms
     * ************************************************************
     */

    @Test
    public void testGlobalAndTenantViewTTLInheritance1() throws Exception {
        // PHOENIX TTL is set in seconds (for e.g 200 secs)
        long tenantPhoenixTTL = 200;

        // Define the test schema.
        // 1. Table with default columns => (ORG_ID, KP, COL1, COL2, COL3), PK => (ORG_ID, KP)
        // 2. GlobalView with default columns => (ID, COL4, COL5, COL6), PK => (ID)
        // 3. Tenant with default columns => (ZID, COL7, COL8, COL9), PK => (ZID)
        final SchemaBuilder schemaBuilder = new SchemaBuilder(getUrl());

        TableOptions tableOptions = TableOptions.withDefaults();
        tableOptions.setTableProps("COLUMN_ENCODED_BYTES=0,MULTI_TENANT=true");

        SchemaBuilder.GlobalViewOptions globalViewOptions =
                SchemaBuilder.GlobalViewOptions.withDefaults();

        SchemaBuilder.GlobalViewIndexOptions globalViewIndexOptions =
                SchemaBuilder.GlobalViewIndexOptions.withDefaults();
        globalViewIndexOptions.setLocal(false);

        TenantViewOptions tenantViewWithOverrideOptions = new TenantViewOptions();
        tenantViewWithOverrideOptions.setTenantViewColumns(asList("ZID", "COL7", "COL8", "COL9"));
        tenantViewWithOverrideOptions
                .setTenantViewColumnTypes(asList("CHAR(15)", "VARCHAR", "VARCHAR", "VARCHAR"));
        tenantViewWithOverrideOptions.setTableProps(String.format("PHOENIX_TTL=%d", tenantPhoenixTTL));

        OtherOptions testCaseWhenAllCFMatchAndAllDefault = new OtherOptions();
        testCaseWhenAllCFMatchAndAllDefault.setTestName("testCaseWhenAllCFMatchAndAllDefault");
        testCaseWhenAllCFMatchAndAllDefault
                .setTableCFs(Lists.newArrayList((String) null, null, null));
        testCaseWhenAllCFMatchAndAllDefault
                .setGlobalViewCFs(Lists.newArrayList((String) null, null, null));
        testCaseWhenAllCFMatchAndAllDefault
                .setTenantViewCFs(Lists.newArrayList((String) null, null, null, null));

        /**
         * ************************************************************
         * Build schema with TTL set by the tenant view.
         * ************************************************************
         */

        schemaBuilder.withTableOptions(tableOptions).withGlobalViewOptions(globalViewOptions)
                .withGlobalViewIndexOptions(globalViewIndexOptions)
                .withTenantViewOptions(tenantViewWithOverrideOptions).withTenantViewIndexDefaults()
                .withOtherOptions(testCaseWhenAllCFMatchAndAllDefault).buildWithNewTenant();

        // Define the test data.
        final String id = String.format(ID_FMT, 0);
        DataSupplier dataSupplier = new DataSupplier() {

            @Override public List<Object> getValues(int rowIndex) {
                Random rnd = new Random();
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
                        new Object[] { id, zid, col1, col2, col3, col4, col5, col6, col7, col8,
                                       col9 });
            }
        };

        // Create a test data reader/writer for the above schema.
        DataWriter dataWriter = new BasicDataWriter();
        List<String> columns =
                Lists.newArrayList("ID", "ZID",
                        "COL1", "COL2", "COL3", "COL4", "COL5", "COL6",
                        "COL7", "COL8", "COL9");
        List<String> rowKeyColumns = Lists.newArrayList("ID", "ZID");
        String tenant1ConnectUrl =
                getUrl() + ';' + TENANT_ID_ATTRIB + '=' + schemaBuilder.getDataOptions().getTenantId();
        try (Connection writeConnection = DriverManager.getConnection(tenant1ConnectUrl)) {
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

            // Validate data exists before ttl expiration.
            long probeTimestamp = EnvironmentEdgeManager.currentTimeMillis()
                    + ((tenantPhoenixTTL * 1000) / 2);
            validateRowsAreNotMaskedUsingCounts(probeTimestamp, dataReader, schemaBuilder);
            // Validate data before and after ttl expiration.
            // Use the tenant phoenix ttl since that is what the view has set.
            validateExpiredRowsAreNotReturnedUsingCounts(tenantPhoenixTTL, dataReader, schemaBuilder);
        }
    }

    /**
     * ************************************************************
     * Case: Build schema with TTL set by the global view.
     * TTL for GLOBAL_VIEW - 300000ms
     * TTL for TENANT_VIEW - 300000ms (not set, uses global view ttl)
     * ************************************************************
     */

    @Test
    public void testGlobalAndTenantViewTTLInheritance2() throws Exception {
        // PHOENIX TTL is set in seconds (for e.g 300 secs)
        long globalPhoenixTTL = 300;

        // Define the test schema.
        // 1. Table with default columns => (ORG_ID, KP, COL1, COL2, COL3), PK => (ORG_ID, KP)
        // 2. GlobalView with default columns => (ID, COL4, COL5, COL6), PK => (ID)
        // 3. Tenant with default columns => (ZID, COL7, COL8, COL9), PK => (ZID)
        final SchemaBuilder schemaBuilder = new SchemaBuilder(getUrl());

        TableOptions tableOptions = TableOptions.withDefaults();
        tableOptions.setTableProps("COLUMN_ENCODED_BYTES=0,MULTI_TENANT=true");

        SchemaBuilder.GlobalViewOptions globalViewOptions =
                SchemaBuilder.GlobalViewOptions.withDefaults();
        globalViewOptions.setTableProps(String.format("PHOENIX_TTL=%d", globalPhoenixTTL));

        SchemaBuilder.GlobalViewIndexOptions globalViewIndexOptions =
                SchemaBuilder.GlobalViewIndexOptions.withDefaults();
        globalViewIndexOptions.setLocal(false);

        TenantViewOptions tenantViewWithOverrideOptions = new TenantViewOptions();
        tenantViewWithOverrideOptions.setTenantViewColumns(asList("ZID", "COL7", "COL8", "COL9"));
        tenantViewWithOverrideOptions
                .setTenantViewColumnTypes(asList("CHAR(15)", "VARCHAR", "VARCHAR", "VARCHAR"));

        OtherOptions testCaseWhenAllCFMatchAndAllDefault = new OtherOptions();
        testCaseWhenAllCFMatchAndAllDefault.setTestName("testCaseWhenAllCFMatchAndAllDefault");
        testCaseWhenAllCFMatchAndAllDefault
                .setTableCFs(Lists.newArrayList((String) null, null, null));
        testCaseWhenAllCFMatchAndAllDefault
                .setGlobalViewCFs(Lists.newArrayList((String) null, null, null));
        testCaseWhenAllCFMatchAndAllDefault
                .setTenantViewCFs(Lists.newArrayList((String) null, null, null, null));

        /**
         * ************************************************************
         * Case: Build schema with TTL set by the global view.
         * ************************************************************
         */

        schemaBuilder.withTableOptions(tableOptions).withGlobalViewOptions(globalViewOptions)
                .withGlobalViewIndexOptions(globalViewIndexOptions)
                .withTenantViewOptions(tenantViewWithOverrideOptions).withTenantViewIndexDefaults()
                .withOtherOptions(testCaseWhenAllCFMatchAndAllDefault).buildWithNewTenant();

        // Define the test data.
        final String id = String.format(ID_FMT, 0);
        DataSupplier dataSupplier = new DataSupplier() {

            @Override public List<Object> getValues(int rowIndex) {
                Random rnd = new Random();
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
                        new Object[] { id, zid, col1, col2, col3, col4, col5, col6, col7, col8,
                                       col9 });
            }
        };

        // Create a test data reader/writer for the above schema.
        DataWriter dataWriter = new BasicDataWriter();
        List<String> columns =
                Lists.newArrayList("ID", "ZID",
                        "COL1", "COL2", "COL3", "COL4", "COL5", "COL6",
                        "COL7", "COL8", "COL9");
        List<String> rowKeyColumns = Lists.newArrayList("ID", "ZID");
        String tenant1ConnectUrl =
                getUrl() + ';' + TENANT_ID_ATTRIB + '=' + schemaBuilder.getDataOptions().getTenantId();
        try (Connection writeConnection = DriverManager.getConnection(tenant1ConnectUrl)) {
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

            // Validate data exists before ttl expiration.
            long probeTimestamp = EnvironmentEdgeManager.currentTimeMillis()
                    + ((globalPhoenixTTL * 1000) / 2);
            validateRowsAreNotMaskedUsingCounts(probeTimestamp, dataReader, schemaBuilder);
            // Validate data before and after ttl expiration.
            // Use the global phoenix ttl since that is what the view has inherited.
            validateExpiredRowsAreNotReturnedUsingCounts(globalPhoenixTTL, dataReader, schemaBuilder);
        }
    }


    @Test
    public void testScanAttributes() throws Exception {

        // PHOENIX TTL is set in seconds (for e.g 10 secs)
        long phoenixTTL = 10;
        TableOptions tableOptions = TableOptions.withDefaults();
        tableOptions.getTableColumns().clear();
        tableOptions.getTableColumnTypes().clear();

        TenantViewOptions tenantViewOptions = TenantViewOptions.withDefaults();
        tenantViewOptions.setTableProps(String.format("PHOENIX_TTL=%d", phoenixTTL));

        // Define the test schema.
        final SchemaBuilder schemaBuilder = new SchemaBuilder(getUrl());
        schemaBuilder
                .withTableOptions(tableOptions)
                .withTenantViewOptions(tenantViewOptions)
                .build();

        String viewName = schemaBuilder.getEntityTenantViewName();

        Properties props = new Properties();
        String tenantConnectUrl =
                getUrl() + ';' + TENANT_ID_ATTRIB + '=' + schemaBuilder.getDataOptions().getTenantId();

        // Test setting masking expired rows property
        try (Connection conn = DriverManager.getConnection(tenantConnectUrl, props);
                final Statement statement = conn.createStatement()) {
            conn.setAutoCommit(true);

            final String stmtString = String.format("select * from  %s", viewName);
            Preconditions.checkNotNull(stmtString);
            final PhoenixStatement pstmt = statement.unwrap(PhoenixStatement.class);
            final QueryPlan queryPlan = pstmt.optimizeQuery(stmtString);

            PTable table = PhoenixRuntime
                    .getTable(conn, schemaBuilder.getDataOptions().getTenantId(), viewName);

            PhoenixResultSet
                    rs = pstmt.newResultSet(queryPlan.iterator(), queryPlan.getProjector(), queryPlan.getContext());
            Assert.assertFalse("Should not have any rows", rs.next());
            Assert.assertEquals("Should have atleast one element", 1, queryPlan.getScans().size());
            Assert.assertEquals("PhoenixTTL does not match",
                    phoenixTTL*1000, ScanUtil.getPhoenixTTL(queryPlan.getScans().get(0).get(0)));
            Assert.assertTrue("Masking attribute should be set",
                    ScanUtil.isMaskTTLExpiredRows(queryPlan.getScans().get(0).get(0)));
            Assert.assertFalse("Delete Expired attribute should not set",
                    ScanUtil.isDeleteTTLExpiredRows(queryPlan.getScans().get(0).get(0)));
        }

        // Test setting delete expired rows property
        try (Connection conn = DriverManager.getConnection(tenantConnectUrl, props);
                final Statement statement = conn.createStatement()) {
            conn.setAutoCommit(true);

            final String stmtString = String.format("select * from  %s", viewName);
            Preconditions.checkNotNull(stmtString);
            final PhoenixStatement pstmt = statement.unwrap(PhoenixStatement.class);
            final QueryPlan queryPlan = pstmt.optimizeQuery(stmtString);
            final Scan scan = queryPlan.getContext().getScan();

            PTable table = PhoenixRuntime
                    .getTable(conn, schemaBuilder.getDataOptions().getTenantId(), viewName);

            byte[] emptyColumnFamilyName = SchemaUtil.getEmptyColumnFamily(table);
            byte[] emptyColumnName =
                    table.getEncodingScheme() == PTable.QualifierEncodingScheme.NON_ENCODED_QUALIFIERS ?
                            QueryConstants.EMPTY_COLUMN_BYTES :
                            table.getEncodingScheme().encode(QueryConstants.ENCODED_EMPTY_COLUMN_NAME);

            scan.setAttribute(BaseScannerRegionObserver.EMPTY_COLUMN_FAMILY_NAME, emptyColumnFamilyName);
            scan.setAttribute(BaseScannerRegionObserver.EMPTY_COLUMN_QUALIFIER_NAME, emptyColumnName);
            scan.setAttribute(BaseScannerRegionObserver.DELETE_PHOENIX_TTL_EXPIRED, PDataType.TRUE_BYTES);
            scan.setAttribute(BaseScannerRegionObserver.PHOENIX_TTL, Bytes.toBytes(Long.valueOf(table.getPhoenixTTL())));

            PhoenixResultSet
                    rs = pstmt.newResultSet(queryPlan.iterator(), queryPlan.getProjector(), queryPlan.getContext());
            Assert.assertFalse("Should not have any rows", rs.next());
            Assert.assertEquals("Should have atleast one element", 1, queryPlan.getScans().size());
            Assert.assertEquals("PhoenixTTL does not match",
                    phoenixTTL*1000, ScanUtil.getPhoenixTTL(queryPlan.getScans().get(0).get(0)));
            Assert.assertFalse("Masking attribute should not be set",
                    ScanUtil.isMaskTTLExpiredRows(queryPlan.getScans().get(0).get(0)));
            Assert.assertTrue("Delete Expired attribute should be set",
                    ScanUtil.isDeleteTTLExpiredRows(queryPlan.getScans().get(0).get(0)));
        }
    }

    @Test
    public void testDeleteWithOnlyTenantView() throws Exception {

        // PHOENIX TTL is set in seconds (for e.g 10 secs)
        long phoenixTTL = 10;
        TableOptions tableOptions = TableOptions.withDefaults();
        String tableProps = "MULTI_TENANT=true,COLUMN_ENCODED_BYTES=0,DEFAULT_COLUMN_FAMILY='0'";
        tableOptions.setTableProps(tableProps);

        TenantViewOptions tenantViewOptions = TenantViewOptions.withDefaults();
        tenantViewOptions.setTableProps(String.format("PHOENIX_TTL=%d", phoenixTTL));

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
            upsertDataAndRunValidations(phoenixTTL, DEFAULT_NUM_ROWS, dataWriter, dataReader,
                    schemaBuilder);
        }

        long scnTimestamp = EnvironmentEdgeManager.currentTimeMillis() + (phoenixTTL * 1000);
        // Delete expired rows using tenant connection.
        deleteData(false,
                schemaBuilder.getDataOptions().getTenantId(),
                schemaBuilder.getEntityTenantViewName(),
                scnTimestamp);

        // Verify after deleting TTL expired data.
        Properties props = new Properties();
        props.setProperty("CurrentSCN", Long.toString(scnTimestamp));

        try (Connection readConnection = DriverManager.getConnection(tenantConnectUrl, props)) {

            dataReader.setConnection(readConnection);
            org.apache.phoenix.thirdparty.com.google.common.collect.Table<String, String, Object>
                    fetchedData =
                    fetchData(dataReader);
            assertEquals("Deleted rows should not be fetched", 0,fetchedData.rowKeySet().size());
        }

        // Validate deletes using hbase
        PTable table = schemaBuilder.getBaseTable();
        String schemaName = table.getSchemaName().getString();
        String tableName = table.getTableName().getString();
        byte[] hbaseBaseTableName = SchemaUtil.getTableNameAsBytes(schemaName,tableName);
        assertUsingHBaseRows(hbaseBaseTableName, earliestTimestamp, 0);
    }

    @Test
    public void testDeleteFromMultipleGlobalIndexes() throws Exception {

        // PHOENIX TTL is set in seconds (for e.g 10 secs)
        long phoenixTTL = 10;
        TableOptions tableOptions = TableOptions.withDefaults();
        String tableProps = "MULTI_TENANT=true,COLUMN_ENCODED_BYTES=0,DEFAULT_COLUMN_FAMILY='0'";
        tableOptions.setTableProps(tableProps);

        GlobalViewOptions globalViewOptions = SchemaBuilder.GlobalViewOptions.withDefaults();
        globalViewOptions.setTableProps(String.format("PHOENIX_TTL=%d", phoenixTTL));

        GlobalViewIndexOptions globalViewIndexOptions = GlobalViewIndexOptions.withDefaults();

        TenantViewOptions tenantViewOptions = new TenantViewOptions();
        tenantViewOptions.setTenantViewColumns(Lists.newArrayList(TENANT_VIEW_COLUMNS));
        tenantViewOptions.setTenantViewColumnTypes(Lists.newArrayList(COLUMN_TYPES));

        // Test cases :
        // Local vs Global indexes, various column family options.
        for (boolean isIndex1Local : Lists.newArrayList(true, false)) {
            for (boolean isIndex2Local : Lists.newArrayList(true, false)) {
                for (OtherOptions options : getTableAndGlobalAndTenantColumnFamilyOptions()) {
                    // Define the test schema.
                    final SchemaBuilder schemaBuilder = new SchemaBuilder(getUrl());

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
                        DataSupplier dataSupplier = new DataSupplier() {

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
                        DataWriter dataWriter = new BasicDataWriter();
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
                            DataReader dataReader = new BasicDataReader();
                            dataReader.setValidationColumns(Arrays.asList("num_rows"));
                            dataReader.setRowKeyColumns(Arrays.asList("num_rows"));
                            dataReader.setDML(String.format(
                                    "SELECT /* +NO_INDEX */ count(1) as num_rows from %s HAVING count(1) > 0",
                                            schemaBuilder.getEntityTenantViewName()));
                            dataReader.setTargetEntity(schemaBuilder.getEntityTenantViewName());

                            // Validate data before and after ttl expiration.
                            validateExpiredRowsAreNotReturnedUsingCounts(phoenixTTL, dataReader, schemaBuilder);
                        }
                    }

                    PTable table = schemaBuilder.getBaseTable();
                    String schemaName = table.getSchemaName().getString();
                    String tableName = table.getTableName().getString();
                    byte[] hbaseBaseTableName = SchemaUtil.getTableNameAsBytes(schemaName,tableName);

                    // Delete data and index rows
                    long scnTimestamp = EnvironmentEdgeManager.currentTimeMillis() +
                            (phoenixTTL * 1000);

                    // Delete expired data rows using global connection.
                    deleteData(true,
                            null,
                            schemaBuilder.getEntityGlobalViewName(),
                            scnTimestamp);

                    String defaultViewIndexName = String
                            .format("%s.IDX_%s",
                                    schemaName,
                                    SchemaUtil.getTableNameFromFullName(
                                            schemaBuilder.getEntityGlobalViewName()));
                    // Delete expired index (default) rows using global connection.
                    deleteIndexData(true,
                            null,
                            defaultViewIndexName,
                            scnTimestamp);

                    // Delete expired index(1) rows using global connection.
                    String viewIndex1Name = String.format("%s.%s", schemaName, index1Name);
                    deleteIndexData(true,
                            null,
                            viewIndex1Name,
                            scnTimestamp);

                    // Delete expired index(2) rows using global connection.
                    String viewIndex2Name = String.format("%s.%s", schemaName, index2Name);
                    deleteIndexData(true,
                            null,
                            viewIndex2Name,
                            scnTimestamp);

                    String viewIndexSchemaName = String
                            .format("_IDX_%s", schemaName);
                    byte[] hbaseViewIndexTableName =
                            SchemaUtil.getTableNameAsBytes(viewIndexSchemaName, tableName);

                    assertUsingHBaseRows(hbaseBaseTableName, earliestTimestamp, 0);
                    assertUsingHBaseRows(hbaseViewIndexTableName, earliestTimestamp, 0);
                }
            }
        }
    }

    @Test public void testDeleteFromMultipleTenantIndexes() throws Exception {

        // PHOENIX TTL is set in seconds (for e.g 10 secs)
        long phoenixTTL = 10;
        TableOptions tableOptions = TableOptions.withDefaults();
        String tableProps = "MULTI_TENANT=true,COLUMN_ENCODED_BYTES=0,DEFAULT_COLUMN_FAMILY='0'";
        tableOptions.setTableProps(tableProps);

        GlobalViewOptions globalViewOptions = SchemaBuilder.GlobalViewOptions.withDefaults();

        TenantViewOptions tenantViewOptions = new TenantViewOptions();
        tenantViewOptions.setTenantViewColumns(Lists.newArrayList(TENANT_VIEW_COLUMNS));
        tenantViewOptions.setTenantViewColumnTypes(Lists.newArrayList(COLUMN_TYPES));
        tenantViewOptions.setTableProps(String.format("PHOENIX_TTL=%d", phoenixTTL));

        TenantViewIndexOptions tenantViewIndexOptions = TenantViewIndexOptions.withDefaults();

        // Test cases :
        // Local vs Global indexes, various column family options.
        for (boolean isIndex1Local : Lists.newArrayList(true, false)) {
            for (boolean isIndex2Local : Lists.newArrayList(true, false)) {
                for (OtherOptions options : getTableAndGlobalAndTenantColumnFamilyOptions()) {
                    // Define the test schema.
                    final SchemaBuilder schemaBuilder = new SchemaBuilder(getUrl());
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
                        DataSupplier dataSupplier = new DataSupplier() {

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
                        DataWriter dataWriter = new BasicDataWriter();
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
                            DataReader dataReader = new BasicDataReader();
                            dataReader.setValidationColumns(Arrays.asList("num_rows"));
                            dataReader.setRowKeyColumns(Arrays.asList("num_rows"));
                            dataReader.setDML(String
                                    .format("SELECT /* +NO_INDEX */ count(1) as num_rows from %s HAVING count(1) > 0",
                                            schemaBuilder.getEntityTenantViewName()));
                            dataReader.setTargetEntity(schemaBuilder.getEntityTenantViewName());

                            // Validate data before and after ttl expiration.
                            validateExpiredRowsAreNotReturnedUsingCounts(phoenixTTL, dataReader,
                                    schemaBuilder);
                        }
                    }
                    // Delete data and index rows
                    long scnTimestamp = EnvironmentEdgeManager.currentTimeMillis() + (phoenixTTL
                            * 1000);

                    for (Map.Entry<String, List<String>> entry : mapOfTenantIndexes.entrySet()) {
                        // Delete expired data rows using tenant connection.
                        deleteData(false, entry.getKey(), schemaBuilder.getEntityTenantViewName(),
                                scnTimestamp);
                        for (String indexName : entry.getValue()) {
                            // Delete expired index rows using tenant connection.
                            String viewIndexName = String.format("%s.%s", schemaName, indexName);
                            deleteIndexData(false, entry.getKey(), viewIndexName, scnTimestamp);
                        }
                    }

                    byte[] hbaseBaseTableName = SchemaUtil
                            .getTableNameAsBytes(schemaName, tableName);
                    String viewIndexSchemaName = String.format("_IDX_%s", schemaName);
                    byte[] hbaseViewIndexTableName = SchemaUtil
                            .getTableNameAsBytes(viewIndexSchemaName, tableName);

                    assertUsingHBaseRows(hbaseBaseTableName, earliestTimestamp, 0);
                    assertUsingHBaseRows(hbaseViewIndexTableName, earliestTimestamp, 0);
                }
            }
        }
    }

    private void upsertDataAndRunValidations(long phoenixTTL, int numRowsToUpsert,
            DataWriter dataWriter, DataReader dataReader, SchemaBuilder schemaBuilder)
            throws Exception {

        //Insert for the first time and validate them.
        validateExpiredRowsAreNotReturnedUsingData(phoenixTTL, upsertData(dataWriter, numRowsToUpsert),
                dataReader, schemaBuilder);

        // Update the above rows and validate the same.
        validateExpiredRowsAreNotReturnedUsingData(phoenixTTL, upsertData(dataWriter, numRowsToUpsert),
                dataReader, schemaBuilder);

    }

    private void validateExpiredRowsAreNotReturnedUsingCounts(long phoenixTTL, DataReader dataReader,
            SchemaBuilder schemaBuilder) throws SQLException {

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
        props.setProperty("CurrentSCN", Long.toString(scnTimestamp + (2 * phoenixTTL * 1000)));
        try (Connection readConnection = DriverManager.getConnection(tenantConnectUrl, props)) {

            dataReader.setConnection(readConnection);
            org.apache.phoenix.thirdparty.com.google.common.collect.Table<String, String, Object> fetchedData =
                    fetchData(dataReader);
            assertNotNull("Fetched data should not be null", fetchedData);
            assertEquals("Expired rows should not be fetched", 0,
                    fetchedData.rowKeySet().size());
        }
    }

    private void validateExpiredRowsAreNotReturnedUsingData(long phoenixTTL,
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
        props.setProperty("CurrentSCN", Long.toString(scnTimestamp + (2 * phoenixTTL * 1000)));
        try (Connection readConnection = DriverManager.getConnection(tenantConnectUrl, props)) {

            dataReader.setConnection(readConnection);
            org.apache.phoenix.thirdparty.com.google.common.collect.Table<String, String, Object>
                    fetchedData =
                    fetchData(dataReader);
            assertNotNull("Fetched data should not be null", fetchedData);
            assertEquals("Expired rows should not be fetched", 0, fetchedData.rowKeySet().size());
        }

    }

    private void validateRowsAreNotMaskedUsingCounts(long probeTimestamp, DataReader dataReader,
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

    private void verifyRowsBeforeTTLExpiration(
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

    private org.apache.phoenix.thirdparty.com.google.common.collect.Table<String, String, Object> upsertData(
            DataWriter dataWriter, int numRowsToUpsert) throws Exception {
        // Upsert rows
        dataWriter.upsertRows(1, numRowsToUpsert);
        return dataWriter.getDataTable();
    }

    private org.apache.phoenix.thirdparty.com.google.common.collect.Table<String, String, Object> fetchData(
            DataReader dataReader) throws SQLException {

        dataReader.readRows();
        return dataReader.getDataTable();
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
            scan.setAttribute(BaseScannerRegionObserver.PHOENIX_TTL, Bytes.toBytes(Long.valueOf(table.getPhoenixTTL())));

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
            scan.setAttribute(BaseScannerRegionObserver.PHOENIX_TTL, Bytes.toBytes(Long.valueOf(table.getPhoenixTTL())));
            PhoenixResultSet
                    rs = pstmt.newResultSet(queryPlan.iterator(), queryPlan.getProjector(), queryPlan.getContext());
            while (rs.next());
        }
    }

    private List<OtherOptions> getTableAndGlobalAndTenantColumnFamilyOptions() {

        List<OtherOptions> testCases = Lists.newArrayList();

        OtherOptions testCaseWhenAllCFMatchAndAllDefault = new OtherOptions();
        testCaseWhenAllCFMatchAndAllDefault.setTestName("testCaseWhenAllCFMatchAndAllDefault");
        testCaseWhenAllCFMatchAndAllDefault
                .setTableCFs(Lists.newArrayList((String) null, null, null));
        testCaseWhenAllCFMatchAndAllDefault
                .setGlobalViewCFs(Lists.newArrayList((String) null, null, null));
        testCaseWhenAllCFMatchAndAllDefault
                .setTenantViewCFs(Lists.newArrayList((String) null, null, null));
        testCases.add(testCaseWhenAllCFMatchAndAllDefault);

        OtherOptions testCaseWhenAllCFMatchAndSame = new OtherOptions();
        testCaseWhenAllCFMatchAndSame.setTestName("testCaseWhenAllCFMatchAndSame");
        testCaseWhenAllCFMatchAndSame.setTableCFs(Lists.newArrayList("A", "A", "A"));
        testCaseWhenAllCFMatchAndSame.setGlobalViewCFs(Lists.newArrayList("A", "A", "A"));
        testCaseWhenAllCFMatchAndSame.setTenantViewCFs(Lists.newArrayList("A", "A", "A"));
        testCases.add(testCaseWhenAllCFMatchAndSame);

        OtherOptions testCaseWhenAllCFMatch = new OtherOptions();
        testCaseWhenAllCFMatch.setTestName("testCaseWhenAllCFMatch");
        testCaseWhenAllCFMatch.setTableCFs(Lists.newArrayList(null, "A", "B"));
        testCaseWhenAllCFMatch.setGlobalViewCFs(Lists.newArrayList(null, "A", "B"));
        testCaseWhenAllCFMatch.setTenantViewCFs(Lists.newArrayList(null, "A", "B"));
        testCases.add(testCaseWhenAllCFMatch);

        OtherOptions testCaseWhenTableCFsAreDiff = new OtherOptions();
        testCaseWhenTableCFsAreDiff.setTestName("testCaseWhenTableCFsAreDiff");
        testCaseWhenTableCFsAreDiff.setTableCFs(Lists.newArrayList(null, "A", "B"));
        testCaseWhenTableCFsAreDiff.setGlobalViewCFs(Lists.newArrayList("A", "A", "B"));
        testCaseWhenTableCFsAreDiff.setTenantViewCFs(Lists.newArrayList("A", "A", "B"));
        testCases.add(testCaseWhenTableCFsAreDiff);

        OtherOptions testCaseWhenGlobalAndTenantCFsAreDiff = new OtherOptions();
        testCaseWhenGlobalAndTenantCFsAreDiff.setTestName("testCaseWhenGlobalAndTenantCFsAreDiff");
        testCaseWhenGlobalAndTenantCFsAreDiff.setTableCFs(Lists.newArrayList(null, "A", "B"));
        testCaseWhenGlobalAndTenantCFsAreDiff.setGlobalViewCFs(Lists.newArrayList("A", "A", "A"));
        testCaseWhenGlobalAndTenantCFsAreDiff.setTenantViewCFs(Lists.newArrayList("B", "B", "B"));
        testCases.add(testCaseWhenGlobalAndTenantCFsAreDiff);

        return testCases;
    }

    private void runValidations(long phoenixTTL,
            org.apache.phoenix.thirdparty.com.google.common.collect.Table<String, String, Object> table,
            DataReader dataReader, SchemaBuilder schemaBuilder)
            throws Exception {

        //Insert for the first time and validate them.
        validateExpiredRowsAreNotReturnedUsingData(phoenixTTL, table,
                dataReader, schemaBuilder);

        // Update the above rows and validate the same.
        validateExpiredRowsAreNotReturnedUsingData(phoenixTTL, table,
                dataReader, schemaBuilder);

    }
}
