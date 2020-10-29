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
import org.apache.phoenix.exception.SQLExceptionCode;
import org.apache.phoenix.jdbc.PhoenixDatabaseMetaData;
import org.apache.phoenix.query.PhoenixTestBuilder.SchemaBuilder;
import org.apache.phoenix.query.PhoenixTestBuilder.SchemaBuilder.GlobalViewOptions;
import org.apache.phoenix.query.PhoenixTestBuilder.SchemaBuilder.TableOptions;
import org.apache.phoenix.query.PhoenixTestBuilder.SchemaBuilder.TenantViewIndexOptions;
import org.apache.phoenix.query.PhoenixTestBuilder.SchemaBuilder.TenantViewOptions;
import org.apache.phoenix.query.QueryConstants;
import org.apache.phoenix.schema.PTableType;
import org.apache.phoenix.util.EnvironmentEdgeManager;
import org.apache.phoenix.util.SchemaUtil;
import org.apache.phoenix.util.TestUtil;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

import static org.apache.phoenix.util.PhoenixRuntime.TENANT_ID_ATTRIB;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

public class ViewTTLIT extends ParallelStatsDisabledIT {

    private static final Logger LOGGER = LoggerFactory.getLogger(ViewTTLIT.class);
    private static final String ORG_ID_FMT = "00D0x000%s";
    private static final String ID_FMT = "00A0y000%07d";
    private static final String PHOENIX_TTL_HEADER_SQL = "SELECT PHOENIX_TTL FROM SYSTEM.CATALOG "
            + "WHERE %s AND TABLE_SCHEM = '%s' AND TABLE_NAME = '%s' AND TABLE_TYPE = '%s'";

    private static final String ALTER_PHOENIX_TTL_SQL
            = "ALTER VIEW \"%s\".\"%s\" set PHOENIX_TTL=%s";

    private static final String ALTER_SQL_WITH_NO_TTL
            = "ALTER VIEW \"%s\".\"%s\" ADD IF NOT EXISTS %s CHAR(10)";

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

    @Test public void testWithBasicGlobalViewWithNoPhoenixTTLDefined() throws Exception {

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

    @Test public void testPhoenixTTLWithTableLevelTTLFails() throws Exception {

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

    @Test public void testPhoenixTTLWithViewIndexFails() throws Exception {

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

    @Test public void testPhoenixTTLForLevelOneView() throws Exception {
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

    @Test public void testPhoenixTTLForLevelTwoView() throws Exception {
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

    @Test public void testPhoenixTTLForWhenTTLIsZero() throws Exception {
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

    @Test public void testPhoenixTTLWithAlterView() throws Exception {
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

    @Test public void testCreateViewWithParentPhoenixTTLFails() throws Exception {
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

    @Test public void testAlterViewWithParentPhoenixTTLFails() throws Exception {
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

    @Test public void testAlterViewWithChildLevelPhoenixTTLFails() throws Exception {
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

    @Test public void testAlterViewWithNoPhoenixTTLSucceed() throws Exception {
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

    @Test public void testResetPhoenixTTL() throws Exception {
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
}
