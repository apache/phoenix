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

import com.google.common.base.Joiner;
import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
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
import org.apache.phoenix.schema.PTableType;
import org.apache.phoenix.util.SchemaUtil;
import org.apache.phoenix.util.TestUtil;
import org.junit.Test;

import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import static java.util.Arrays.asList;
import static org.apache.phoenix.query.PhoenixTestBuilder.DDLDefaults.MAX_ROWS;
import static org.apache.phoenix.util.PhoenixRuntime.TENANT_ID_ATTRIB;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public class ViewTTLIT extends ParallelStatsDisabledIT {

    private static final String
            VIEW_TTL_HEADER_SQL =
            "SELECT VIEW_TTL FROM SYSTEM.CATALOG "
                    + "WHERE %s AND TABLE_SCHEM = '%s' AND TABLE_NAME = '%s' AND TABLE_TYPE = '%s'";

    private static final String ALTER_VIEW_TTL_SQL = "ALTER VIEW %s.%s set VIEW_TTL=%d";
    private static final int DEFAULT_NUM_ROWS = 5;

    // Scans the HBase rows directly for the view ttl related header rows column and asserts
    private void assertViewHeaderRowsHaveViewTTLRelatedCells(String schemaName, long minTimestamp,
            boolean rawScan, int expectedRows) throws IOException, SQLException {

        FilterList filterList = new FilterList(FilterList.Operator.MUST_PASS_ALL);
        RowFilter
                schemaNameFilter =
                new RowFilter(CompareFilter.CompareOp.EQUAL, new SubstringComparator(schemaName));
        QualifierFilter
                viewTTLQualifierFilter =
                new QualifierFilter(CompareFilter.CompareOp.EQUAL,
                        new BinaryComparator(PhoenixDatabaseMetaData.VIEW_TTL_BYTES));
        filterList.addFilter(schemaNameFilter);
        filterList.addFilter(viewTTLQualifierFilter);
        try (Table tbl = driver.getConnectionQueryServices(getUrl(), TestUtil.TEST_PROPERTIES)
                .getTable(PhoenixDatabaseMetaData.SYSTEM_CATALOG_NAME_BYTES)) {

            Scan allRows = new Scan();
            allRows.setRaw(rawScan);
            allRows.setTimeRange(minTimestamp, HConstants.LATEST_TIMESTAMP);
            allRows.setFilter(filterList);
            ResultScanner scanner = tbl.getScanner(allRows);
            int numMatchingRows = 0;
            for (Result result = scanner.next(); result != null; result = scanner.next()) {
                numMatchingRows +=
                        result.containsColumn(QueryConstants.DEFAULT_COLUMN_FAMILY_BYTES,
                                PhoenixDatabaseMetaData.VIEW_TTL_BYTES) ? 1 : 0;
            }
            assertEquals(String.format("Expected rows do not match for table = %s at timestamp %d",
                    Bytes.toString(PhoenixDatabaseMetaData.SYSTEM_CATALOG_NAME_BYTES),
                    minTimestamp), expectedRows, numMatchingRows);
        }
    }

    private void assertSyscatHaveViewTTLRelatedColumns(String tenantId, String schemaName,
            String tableName, String tableType, long ttlValueExpected) throws SQLException {

        try (Connection connection = DriverManager.getConnection(getUrl())) {
            Statement stmt = connection.createStatement();
            String
                    tenantClause =
                    tenantId == null || tenantId.isEmpty() ?
                            "TENANT_ID IS NULL" :
                            String.format("TENANT_ID = '%s'", tenantId);
            String
                    sql =
                    String.format(VIEW_TTL_HEADER_SQL, tenantClause, schemaName, tableName,
                            tableType);
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

    /**
     * -----------------
     * Test methods
     * -----------------
     */

    @Test public void testWithBasicGlobalViewWithNoViewTTLDefined() throws Exception {

        long startTime = System.currentTimeMillis();

        // Define the test schema.
        // 1. Table with columns => (ORG_ID, KP, COL1, COL2, COL3), PK => (ORG_ID, KP)
        // 2. GlobalView with columns => (ID, COL4, COL5, COL6), PK => (ID)
        final SchemaBuilder schemaBuilder = new SchemaBuilder(getUrl());
        schemaBuilder.withTableDefaults().withGlobalViewDefaults().build();

        // Expected 2 rows - one for Table and GlobalView each.
        // Since the VIEW_TTL property values are not being set,
        // we expect the view header columns to show up in raw scans only.
        assertViewHeaderRowsHaveViewTTLRelatedCells(schemaBuilder.getTableOptions().getSchemaName(),
                startTime, true, 2);
    }

    @Test public void testViewTTLWithTableLevelTTLFails() throws Exception {

        // Define the test schema.
        // 1. Table with columns => (ORG_ID, KP, COL1, COL2, COL3), PK => (ORG_ID, KP)
        // 2. Tenant with columns => (ZID, COL7, COL8, COL9), PK => (ZID)
        final SchemaBuilder schemaBuilder = new SchemaBuilder(getUrl());

        TableOptions tableOptions = TableOptions.withDefaults();
        tableOptions.setTableProps("COLUMN_ENCODED_BYTES=0,MULTI_TENANT=true,TTL=100");

        TenantViewOptions tenantViewOptions = TenantViewOptions.withDefaults();
        tenantViewOptions.setTableProps("VIEW_TTL=1000");
        try {
            schemaBuilder.withTableOptions(tableOptions).withTenantViewOptions(tenantViewOptions)
                    .buildNewView();
            fail();
        } catch (SQLException e) {
            assertEquals(
                    SQLExceptionCode.CANNOT_SET_OR_ALTER_VIEW_TTL_FOR_TABLE_WITH_TTL.getErrorCode(),
                    e.getErrorCode());
        }
    }

    @Test public void testViewTTLWithViewIndexFails() throws Exception {

        // Define the test schema.
        // 1. Table with columns => (ORG_ID, KP, COL1, COL2, COL3), PK => (ORG_ID, KP)
        // 2. Tenant with columns => (ZID, COL7, COL8, COL9), PK => (ZID)
        final SchemaBuilder schemaBuilder = new SchemaBuilder(getUrl());

        TableOptions tableOptions = TableOptions.withDefaults();
        tableOptions.setTableProps("COLUMN_ENCODED_BYTES=0,MULTI_TENANT=true");

        TenantViewIndexOptions tenantViewIndexOptions = TenantViewIndexOptions.withDefaults();
        tenantViewIndexOptions.setIndexProps("VIEW_TTL=1000");
        try {
            schemaBuilder.withTableOptions(tableOptions).withTenantViewDefaults()
                    .withTenantViewIndexOptions(tenantViewIndexOptions).buildNewView();
            fail();
        } catch (SQLException e) {
            assertEquals(SQLExceptionCode.VIEW_TTL_SUPPORTED_FOR_VIEWS_ONLY.getErrorCode(),
                    e.getErrorCode());
        }
    }

    @Test public void testViewTTLForLevelOneView() throws Exception {
        long startTime = System.currentTimeMillis();

        // Define the test schema.
        // 1. Table with columns => (ORG_ID, KP, COL1, COL2, COL3), PK => (ORG_ID, KP)
        // 2. Tenant with columns => (ZID, COL7, COL8, COL9), PK => (ZID)
        final SchemaBuilder schemaBuilder = new SchemaBuilder(getUrl());

        TableOptions tableOptions = TableOptions.withDefaults();
        tableOptions.setTableProps("COLUMN_ENCODED_BYTES=0,MULTI_TENANT=true");

        TenantViewOptions tenantViewOptions = TenantViewOptions.withDefaults();
        tenantViewOptions.setTableProps("VIEW_TTL=1000");
        schemaBuilder.withTableOptions(tableOptions).withTenantViewOptions(tenantViewOptions)
                .withTenantViewIndexDefaults().buildNewView();

        String tenantId = schemaBuilder.getDataOptions().getTenantId();
        String
                schemaName =
                stripQuotes(SchemaUtil
                        .getSchemaNameFromFullName(schemaBuilder.getEntityTenantViewName()));
        String
                tenantViewName =
                stripQuotes(SchemaUtil
                        .getTableNameFromFullName(schemaBuilder.getEntityTenantViewName()));
        String
                indexOnTenantViewName =
                String.format("IDX_%s", stripQuotes(schemaBuilder.getEntityKeyPrefix()));

        // Expected 2 rows - one for TenantView and ViewIndex each.
        // Since the VIEW_TTL property values are being set,
        // we expect the view header columns to show up in regular scans too.
        assertViewHeaderRowsHaveViewTTLRelatedCells(schemaBuilder.getTableOptions().getSchemaName(),
                startTime, false, 2);
        // Since the VIEW_TTL property values are not being overriden,
        // we expect the TTL value to be different from the global view.
        assertSyscatHaveViewTTLRelatedColumns(tenantId, schemaName, tenantViewName,
                PTableType.VIEW.getSerializedValue(), 1000);
        assertSyscatHaveViewTTLRelatedColumns(tenantId, schemaName, indexOnTenantViewName,
                PTableType.INDEX.getSerializedValue(), 1000);

    }

    @Test public void testViewTTLForLevelTwoView() throws Exception {
        long startTime = System.currentTimeMillis();

        // Define the test schema.
        // 1. Table with columns => (ORG_ID, KP, COL1, COL2, COL3), PK => (ORG_ID, KP)
        // 2. GlobalView with columns => (ID, COL4, COL5, COL6), PK => (ID)
        // 3. Tenant with columns => (ZID, COL7, COL8, COL9), PK => (ZID)
        final SchemaBuilder schemaBuilder = new SchemaBuilder(getUrl());

        TableOptions tableOptions = TableOptions.withDefaults();
        tableOptions.setTableProps("COLUMN_ENCODED_BYTES=0,MULTI_TENANT=true");

        SchemaBuilder.GlobalViewOptions
                globalViewOptions =
                SchemaBuilder.GlobalViewOptions.withDefaults();
        globalViewOptions.setTableProps("VIEW_TTL=300000");

        SchemaBuilder.GlobalViewIndexOptions
                globalViewIndexOptions =
                SchemaBuilder.GlobalViewIndexOptions.withDefaults();
        globalViewIndexOptions.setLocal(false);

        TenantViewOptions tenantViewWithOverrideOptions = TenantViewOptions.withDefaults();
        tenantViewWithOverrideOptions.setTableProps("VIEW_TTL=1000");
        schemaBuilder.withTableOptions(tableOptions).withGlobalViewOptions(globalViewOptions)
                .withGlobalViewIndexOptions(globalViewIndexOptions)
                .withTenantViewOptions(tenantViewWithOverrideOptions).withTenantViewIndexDefaults()
                .buildWithNewTenant();

        String tenantId = schemaBuilder.getDataOptions().getTenantId();
        String
                schemaName =
                stripQuotes(SchemaUtil
                        .getSchemaNameFromFullName(schemaBuilder.getEntityTenantViewName()));
        String
                globalViewName =
                stripQuotes(SchemaUtil
                        .getTableNameFromFullName(schemaBuilder.getEntityGlobalViewName()));
        String
                tenantViewName =
                stripQuotes(SchemaUtil
                        .getTableNameFromFullName(schemaBuilder.getEntityTenantViewName()));
        String indexOnGlobalViewName = String.format("IDX_%s", globalViewName);
        String
                indexOnTenantViewName =
                String.format("IDX_%s", stripQuotes(schemaBuilder.getEntityKeyPrefix()));

        // Expected 4 rows - one for GlobalView, one for TenantView and ViewIndex each.
        // Since the VIEW_TTL property values are being set,
        // we expect the view header columns to show up in regular scans too.
        assertViewHeaderRowsHaveViewTTLRelatedCells(schemaBuilder.getTableOptions().getSchemaName(),
                startTime, false, 4);
        assertSyscatHaveViewTTLRelatedColumns("", schemaName, globalViewName,
                PTableType.VIEW.getSerializedValue(), 300000);
        assertSyscatHaveViewTTLRelatedColumns("", schemaName, indexOnGlobalViewName,
                PTableType.INDEX.getSerializedValue(), 300000);
        // Since the VIEW_TTL property values are being overriden,
        // we expect the TTL value to be different from the global view.
        assertSyscatHaveViewTTLRelatedColumns(tenantId, schemaName, tenantViewName,
                PTableType.VIEW.getSerializedValue(), 1000);
        assertSyscatHaveViewTTLRelatedColumns(tenantId, schemaName, indexOnTenantViewName,
                PTableType.INDEX.getSerializedValue(), 1000);

        // Without override
        startTime = System.currentTimeMillis();

        TenantViewOptions tenantViewWithoutOverrideOptions = TenantViewOptions.withDefaults();
        schemaBuilder.withTableOptions(tableOptions).withGlobalViewOptions(globalViewOptions)
                .withGlobalViewIndexOptions(globalViewIndexOptions)
                .withTenantViewOptions(tenantViewWithoutOverrideOptions)
                .withTenantViewIndexDefaults().buildWithNewTenant();

        tenantId = schemaBuilder.getDataOptions().getTenantId();
        schemaName =
                stripQuotes(SchemaUtil
                        .getSchemaNameFromFullName(schemaBuilder.getEntityTenantViewName()));
        globalViewName =
                stripQuotes(SchemaUtil
                        .getTableNameFromFullName(schemaBuilder.getEntityGlobalViewName()));
        tenantViewName =
                stripQuotes(SchemaUtil
                        .getTableNameFromFullName(schemaBuilder.getEntityTenantViewName()));
        indexOnGlobalViewName = String.format("IDX_%s", globalViewName);
        indexOnTenantViewName =
                String.format("IDX_%s", stripQuotes(schemaBuilder.getEntityKeyPrefix()));

        // Expected 2 rows - one for TenantView and ViewIndex each.
        // Since the VIEW_TTL property values are being set,
        // we expect the view header columns to show up in regular scans too.
        assertViewHeaderRowsHaveViewTTLRelatedCells(schemaBuilder.getTableOptions().getSchemaName(),
                startTime, false, 2);
        assertSyscatHaveViewTTLRelatedColumns("", schemaName, globalViewName,
                PTableType.VIEW.getSerializedValue(), 300000);
        assertSyscatHaveViewTTLRelatedColumns("", schemaName, indexOnGlobalViewName,
                PTableType.INDEX.getSerializedValue(), 300000);
        // Since the VIEW_TTL property values are not being overriden,
        // we expect the TTL value to be same as the global view.
        assertSyscatHaveViewTTLRelatedColumns(tenantId, schemaName, tenantViewName,
                PTableType.VIEW.getSerializedValue(), 300000);
        assertSyscatHaveViewTTLRelatedColumns(tenantId, schemaName, indexOnTenantViewName,
                PTableType.INDEX.getSerializedValue(), 300000);
    }

    @Test public void testViewTTLForWhenTTLIsZero() throws Exception {
        long startTime = System.currentTimeMillis();

        // Define the test schema.
        // 1. Table with columns => (ORG_ID, KP, COL1, COL2, COL3), PK => (ORG_ID, KP)
        // 2. Tenant with columns => (ZID, COL7, COL8, COL9), PK => (ZID)
        final SchemaBuilder schemaBuilder = new SchemaBuilder(getUrl());

        TableOptions tableOptions = TableOptions.withDefaults();
        tableOptions.setTableProps("COLUMN_ENCODED_BYTES=0,MULTI_TENANT=true");

        TenantViewOptions tenantViewOptions = TenantViewOptions.withDefaults();
        // Client can also specify VIEW_TTL=NONE
        tenantViewOptions.setTableProps("VIEW_TTL=0");
        schemaBuilder.withTableOptions(tableOptions).withTenantViewOptions(tenantViewOptions)
                .withTenantViewIndexDefaults().buildNewView();

        String tenantId = schemaBuilder.getDataOptions().getTenantId();
        String
                schemaName =
                stripQuotes(SchemaUtil
                        .getSchemaNameFromFullName(schemaBuilder.getEntityTenantViewName()));
        String
                tenantViewName =
                stripQuotes(SchemaUtil
                        .getTableNameFromFullName(schemaBuilder.getEntityTenantViewName()));
        String
                indexOnTenantViewName =
                String.format("IDX_%s", stripQuotes(schemaBuilder.getEntityKeyPrefix()));

        // Expected 3 deleted rows - one for Table, one for TenantView and ViewIndex each.
        // Since the VIEW_TTL property values are not being set or being set to zero,
        // we expect the view header columns to show up in raw scans only.
        assertViewHeaderRowsHaveViewTTLRelatedCells(schemaBuilder.getTableOptions().getSchemaName(),
                startTime, true, 3);
        // Since the VIEW_TTL property values are not being overriden,
        // we expect the TTL value to be different from the global view.
        assertSyscatHaveViewTTLRelatedColumns(tenantId, schemaName, tenantViewName,
                PTableType.VIEW.getSerializedValue(), 0);
        assertSyscatHaveViewTTLRelatedColumns(tenantId, schemaName, indexOnTenantViewName,
                PTableType.INDEX.getSerializedValue(), 0);
    }

    @Test public void testViewTTLWithAlterView() throws Exception {
        long startTime = System.currentTimeMillis();

        // Define the test schema.
        // 1. Table with columns => (ORG_ID, KP, COL1, COL2, COL3), PK => (ORG_ID, KP)
        // 2. Tenant with columns => (ZID, COL7, COL8, COL9), PK => (ZID)
        final SchemaBuilder schemaBuilder = new SchemaBuilder(getUrl());

        TableOptions tableOptions = TableOptions.withDefaults();
        tableOptions.setTableProps("COLUMN_ENCODED_BYTES=0,MULTI_TENANT=true");

        TenantViewOptions tenantViewOptions = TenantViewOptions.withDefaults();
        // Client can also specify VIEW_TTL=0
        tenantViewOptions.setTableProps("VIEW_TTL=NONE");
        schemaBuilder.withTableOptions(tableOptions).withTenantViewOptions(tenantViewOptions)
                .withTenantViewIndexDefaults().buildNewView();

        String tenantId = schemaBuilder.getDataOptions().getTenantId();
        String
                schemaName =
                stripQuotes(SchemaUtil
                        .getSchemaNameFromFullName(schemaBuilder.getEntityTenantViewName()));
        String
                tenantViewName =
                stripQuotes(SchemaUtil
                        .getTableNameFromFullName(schemaBuilder.getEntityTenantViewName()));
        String
                indexOnTenantViewName =
                String.format("IDX_%s", stripQuotes(schemaBuilder.getEntityKeyPrefix()));

        // Expected 3 deleted rows - one for Table, one for TenantView and ViewIndex each.
        // Since the VIEW_TTL property values are not being set or being set to zero,
        // we expect the view header columns to show up in raw scans only.
        assertViewHeaderRowsHaveViewTTLRelatedCells(schemaBuilder.getTableOptions().getSchemaName(),
                startTime, true, 3);
        // Since the VIEW_TTL property values are not being overriden,
        // we expect the TTL value to be different from the global view.
        assertSyscatHaveViewTTLRelatedColumns(tenantId, schemaName, tenantViewName,
                PTableType.VIEW.getSerializedValue(), 0);
        assertSyscatHaveViewTTLRelatedColumns(tenantId, schemaName, indexOnTenantViewName,
                PTableType.INDEX.getSerializedValue(), 0);

        String tenantURL = getUrl() + ';' + TENANT_ID_ATTRIB + '=' + tenantId;
        try (Connection connection = DriverManager.getConnection(tenantURL)) {
            Statement stmt = connection.createStatement();
            String sql = String.format(ALTER_VIEW_TTL_SQL, schemaName, tenantViewName, 1000);
            stmt.execute(sql);
        }

        // Expected 2 rows - one for TenantView and ViewIndex each.
        // Since the VIEW_TTL property values are being set,
        // we expect the view header columns to show up in regular scans too.
        assertViewHeaderRowsHaveViewTTLRelatedCells(schemaBuilder.getTableOptions().getSchemaName(),
                startTime, false, 2);
        // Since the VIEW_TTL property values are not being overriden,
        // we expect the TTL value to be different from the global view.
        assertSyscatHaveViewTTLRelatedColumns(tenantId, schemaName, tenantViewName,
                PTableType.VIEW.getSerializedValue(), 1000);
        assertSyscatHaveViewTTLRelatedColumns(tenantId, schemaName, indexOnTenantViewName,
                PTableType.INDEX.getSerializedValue(), 1000);
    }

    @Test public void testViewTTLForLevelTwoViewWithNoIndexes() throws Exception {
        long startTime = System.currentTimeMillis();

        // Define the test schema.
        // 1. Table with columns => (ORG_ID, KP, COL1, COL2, COL3), PK => (ORG_ID, KP)
        // 2. GlobalView with columns => (ID, COL4, COL5, COL6), PK => (ID)
        // 3. Tenant with columns => (ZID, COL7, COL8, COL9), PK => (ZID)
        final SchemaBuilder schemaBuilder = new SchemaBuilder(getUrl());

        TableOptions tableOptions = TableOptions.withDefaults();
        tableOptions.setTableProps("COLUMN_ENCODED_BYTES=0,MULTI_TENANT=true");

        SchemaBuilder.GlobalViewOptions
                globalViewOptions =
                SchemaBuilder.GlobalViewOptions.withDefaults();
        globalViewOptions.setTableProps("VIEW_TTL=300000");

        TenantViewOptions tenantViewWithOverrideOptions = TenantViewOptions.withDefaults();
        tenantViewWithOverrideOptions.setTableProps("VIEW_TTL=10000");
        schemaBuilder.withTableOptions(tableOptions).withGlobalViewOptions(globalViewOptions)
                .withTenantViewOptions(tenantViewWithOverrideOptions).buildWithNewTenant();

        String tenantId = schemaBuilder.getDataOptions().getTenantId();
        String
                schemaName =
                stripQuotes(SchemaUtil
                        .getSchemaNameFromFullName(schemaBuilder.getEntityTenantViewName()));
        String
                globalViewName =
                stripQuotes(SchemaUtil
                        .getTableNameFromFullName(schemaBuilder.getEntityGlobalViewName()));
        String
                tenantViewName =
                stripQuotes(SchemaUtil
                        .getTableNameFromFullName(schemaBuilder.getEntityTenantViewName()));

        // Expected 2 rows - one for GlobalView, one for TenantView  each.
        // Since the VIEW_TTL property values are being set,
        // we expect the view header columns to show up in regular scans too.
        assertViewHeaderRowsHaveViewTTLRelatedCells(schemaBuilder.getTableOptions().getSchemaName(),
                startTime, false, 2);
        assertSyscatHaveViewTTLRelatedColumns("", schemaName, globalViewName,
                PTableType.VIEW.getSerializedValue(), 300000);
        // Since the VIEW_TTL property values are not being overriden,
        // we expect the TTL value to be different from the global view.
        assertSyscatHaveViewTTLRelatedColumns(tenantId, schemaName, tenantViewName,
                PTableType.VIEW.getSerializedValue(), 10000);

        // Without override
        startTime = System.currentTimeMillis();

        TenantViewOptions tenantViewWithoutOverrideOptions = TenantViewOptions.withDefaults();
        schemaBuilder.withTableOptions(tableOptions).withGlobalViewOptions(globalViewOptions)
                .withTenantViewOptions(tenantViewWithoutOverrideOptions).buildWithNewTenant();

        tenantId = schemaBuilder.getDataOptions().getTenantId();
        schemaName =
                stripQuotes(SchemaUtil
                        .getSchemaNameFromFullName(schemaBuilder.getEntityTenantViewName()));
        globalViewName =
                stripQuotes(SchemaUtil
                        .getTableNameFromFullName(schemaBuilder.getEntityGlobalViewName()));
        tenantViewName =
                stripQuotes(SchemaUtil
                        .getTableNameFromFullName(schemaBuilder.getEntityTenantViewName()));

        // Expected 1 rows - one for TenantView each.
        // Since the VIEW_TTL property values are being set,
        // we expect the view header columns to show up in regular scans too.
        assertViewHeaderRowsHaveViewTTLRelatedCells(schemaBuilder.getTableOptions().getSchemaName(),
                startTime, false, 1);
        assertSyscatHaveViewTTLRelatedColumns("", schemaName, globalViewName,
                PTableType.VIEW.getSerializedValue(), 300000);
        // Since the VIEW_TTL property values are not being overriden,
        // we expect the TTL value to be same as the global view.
        assertSyscatHaveViewTTLRelatedColumns(tenantId, schemaName, tenantViewName,
                PTableType.VIEW.getSerializedValue(), 300000);
    }

    @Test public void testWithTenantViewAndNoGlobalView() throws Exception {

        long viewTTL = 10000;
        TableOptions tableOptions = TableOptions.withDefaults();
        tableOptions.getTableColumns().clear();
        tableOptions.getTableColumnTypes().clear();

        TenantViewOptions tenantViewOptions = TenantViewOptions.withDefaults();
        tenantViewOptions.setTableProps(String.format("VIEW_TTL=%d", viewTTL));

        // Define the test schema.
        final SchemaBuilder schemaBuilder = new SchemaBuilder(getUrl());
        schemaBuilder.withTableOptions(tableOptions).withTenantViewOptions(tenantViewOptions)
                .build();

        // Define the test data.
        DataSupplier dataSupplier = new DataSupplier() {

            @Override public List<Object> getValues(int rowIndex) {
                Random rnd = new Random();
                String zid = String.format("00A0y000%07d", rowIndex);
                String col7 = String.format("g%05d", rowIndex + rnd.nextInt(MAX_ROWS));
                String col8 = String.format("h%05d", rowIndex + rnd.nextInt(MAX_ROWS));
                String col9 = String.format("i%05d", rowIndex + rnd.nextInt(MAX_ROWS));
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
                getUrl() + ';' + TENANT_ID_ATTRIB + '=' + schemaBuilder.getDataOptions()
                        .getTenantId();
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
    }

    @Test public void testWithSQLUsingIndexWithCoveredColsUpdates() throws Exception {

        long viewTTL = 10000;
        TableOptions tableOptions = TableOptions.withDefaults();
        tableOptions.getTableColumns().clear();
        tableOptions.getTableColumnTypes().clear();

        GlobalViewOptions globalViewOptions = SchemaBuilder.GlobalViewOptions.withDefaults();
        globalViewOptions.setTableProps(String.format("VIEW_TTL=%d", viewTTL));

        GlobalViewIndexOptions
                globalViewIndexOptions =
                SchemaBuilder.GlobalViewIndexOptions.withDefaults();
        globalViewIndexOptions.setLocal(false);

        TenantViewOptions tenantViewOptions = new TenantViewOptions();
        tenantViewOptions.setTenantViewColumns(asList("ZID", "COL7", "COL8", "COL9"));
        tenantViewOptions
                .setTenantViewColumnTypes(asList("CHAR(15)", "VARCHAR", "VARCHAR", "VARCHAR"));

        tenantViewOptions.setTableProps(String.format("VIEW_TTL=%d", viewTTL));

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
        final List<String> outerCol4s = Lists.newArrayList();
        DataSupplier dataSupplier = new DataSupplier() {
            String col4ForWhereClause;

            @Override public List<Object> getValues(int rowIndex) {
                Random rnd = new Random();
                String id = String.format("00A0y000%07d", rowIndex);
                String zid = String.format("00B0y000%07d", rowIndex);
                String col4 = String.format("d%05d", rowIndex + rnd.nextInt(MAX_ROWS));

                // Store the col4 data to be used later in a where clause
                outerCol4s.add(col4);
                String col5 = String.format("e%05d", rowIndex + rnd.nextInt(MAX_ROWS));
                String col6 = String.format("f%05d", rowIndex + rnd.nextInt(MAX_ROWS));
                String col7 = String.format("g%05d", rowIndex + rnd.nextInt(MAX_ROWS));
                String col8 = String.format("h%05d", rowIndex + rnd.nextInt(MAX_ROWS));
                String col9 = String.format("i%05d", rowIndex + rnd.nextInt(MAX_ROWS));

                return Lists
                        .newArrayList(new Object[] { id, zid, col4, col5, col6, col7, col8, col9 });
            }
        };

        // Create a test data reader/writer for the above schema.
        DataWriter dataWriter = new BasicDataWriter();
        DataReader dataReader = new BasicDataReader();

        List<String>
                columns =
                Lists.newArrayList("ID", "ZID", "COL4", "COL5", "COL6", "COL7", "COL8", "COL9");
        List<String> rowKeyColumns = Lists.newArrayList("COL6");
        String
                tenantConnectUrl =
                getUrl() + ';' + TENANT_ID_ATTRIB + '=' + schemaBuilder.getDataOptions()
                        .getTenantId();
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
            validateExpiredRowsAreNotReturnedUsingCounts(viewTTL, dataReader, schemaBuilder);
        }
    }

    /**
     * Ensure/validate that empty columns for the index are still updated even when covered columns
     * are not updated.
     *
     * @throws Exception
     */
    @Test public void testWithSQLUsingIndexAndNoCoveredColsUpdates() throws Exception {

        long viewTTL = 10000;
        TableOptions tableOptions = TableOptions.withDefaults();
        tableOptions.getTableColumns().clear();
        tableOptions.getTableColumnTypes().clear();

        GlobalViewOptions globalViewOptions = SchemaBuilder.GlobalViewOptions.withDefaults();
        globalViewOptions.setTableProps(String.format("VIEW_TTL=%d", viewTTL));

        GlobalViewIndexOptions
                globalViewIndexOptions =
                SchemaBuilder.GlobalViewIndexOptions.withDefaults();
        globalViewIndexOptions.setLocal(false);

        TenantViewOptions tenantViewOptions = new TenantViewOptions();
        tenantViewOptions.setTenantViewColumns(asList("ZID", "COL7", "COL8", "COL9"));
        tenantViewOptions
                .setTenantViewColumnTypes(asList("CHAR(15)", "VARCHAR", "VARCHAR", "VARCHAR"));

        tenantViewOptions.setTableProps(String.format("VIEW_TTL=%d", viewTTL));

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
        final List<String> outerCol4s = Lists.newArrayList();
        DataSupplier dataSupplier = new DataSupplier() {

            @Override public List<Object> getValues(int rowIndex) {
                Random rnd = new Random();
                String id = String.format("00A0y000%07d", rowIndex);
                String zid = String.format("00B0y000%07d", rowIndex);
                String col4 = String.format("d%05d", rowIndex + rnd.nextInt(MAX_ROWS));

                // Store the col4 data to be used later in a where clause
                outerCol4s.add(col4);
                String col5 = String.format("e%05d", rowIndex + rnd.nextInt(MAX_ROWS));
                String col6 = String.format("f%05d", rowIndex + rnd.nextInt(MAX_ROWS));
                String col7 = String.format("g%05d", rowIndex + rnd.nextInt(MAX_ROWS));
                String col8 = String.format("h%05d", rowIndex + rnd.nextInt(MAX_ROWS));
                String col9 = String.format("i%05d", rowIndex + rnd.nextInt(MAX_ROWS));

                return Lists
                        .newArrayList(new Object[] { id, zid, col4, col5, col6, col7, col8, col9 });
            }
        };

        // Create a test data reader/writer for the above schema.
        DataWriter dataWriter = new BasicDataWriter();
        DataReader dataReader = new BasicDataReader();

        List<String>
                columns =
                Lists.newArrayList("ID", "ZID", "COL4", "COL5", "COL6", "COL7", "COL8", "COL9");
        List<String>
                nonCoveredColumns =
                Lists.newArrayList("ID", "ZID", "COL5", "COL7", "COL8", "COL9");
        List<String> rowKeyColumns = Lists.newArrayList("COL6");
        String
                tenantConnectUrl =
                getUrl() + ';' + TENANT_ID_ATTRIB + '=' + schemaBuilder.getDataOptions()
                        .getTenantId();
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
            validateExpiredRowsAreNotReturnedUsingCounts(viewTTL, dataReader, schemaBuilder);

            // Now update the above data but not modifying the covered columns.
            // Ensure/validate that empty columns for the index are still updated.

            // Data supplier where covered and included (col4 and col6) columns are not updated.
            DataSupplier dataSupplierForNonCoveredColumns = new DataSupplier() {

                @Override public List<Object> getValues(int rowIndex) {
                    Random rnd = new Random();
                    String id = String.format("00A0y000%07d", rowIndex);
                    String zid = String.format("00B0y000%07d", rowIndex);
                    String col5 = String.format("e%05d", rowIndex + rnd.nextInt(MAX_ROWS));
                    String col7 = String.format("g%05d", rowIndex + rnd.nextInt(MAX_ROWS));
                    String col8 = String.format("h%05d", rowIndex + rnd.nextInt(MAX_ROWS));
                    String col9 = String.format("i%05d", rowIndex + rnd.nextInt(MAX_ROWS));

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
            validateExpiredRowsAreNotReturnedUsingCounts(viewTTL, dataReader, schemaBuilder);

        }
    }

    @Test public void testWithVariousSQLs() throws Exception {

        long viewTTL = 10000;
        TableOptions tableOptions = TableOptions.withDefaults();
        tableOptions.getTableColumns().clear();
        tableOptions.getTableColumnTypes().clear();

        GlobalViewOptions globalViewOptions = SchemaBuilder.GlobalViewOptions.withDefaults();
        globalViewOptions.setTableProps(String.format("VIEW_TTL=%d", viewTTL));

        GlobalViewIndexOptions
                globalViewIndexOptions =
                SchemaBuilder.GlobalViewIndexOptions.withDefaults();
        globalViewIndexOptions.setLocal(false);

        TenantViewOptions tenantViewOptions = new TenantViewOptions();
        tenantViewOptions.setTenantViewColumns(asList("ZID", "COL7", "COL8", "COL9"));
        tenantViewOptions
                .setTenantViewColumnTypes(asList("CHAR(15)", "VARCHAR", "VARCHAR", "VARCHAR"));

        tenantViewOptions.setTableProps(String.format("VIEW_TTL=%d", viewTTL));

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
        final String groupById = String.format("00A0y000%07d", 0);
        DataSupplier dataSupplier = new DataSupplier() {

            @Override public List<Object> getValues(int rowIndex) {
                Random rnd = new Random();
                String zid = String.format("00B0y000%07d", rowIndex);
                String col4 = String.format("d%05d", rowIndex + rnd.nextInt(MAX_ROWS));
                String col5 = String.format("e%05d", rowIndex + rnd.nextInt(MAX_ROWS));
                String col6 = String.format("f%05d", rowIndex + rnd.nextInt(MAX_ROWS));
                String col7 = String.format("g%05d", rowIndex + rnd.nextInt(MAX_ROWS));
                String col8 = String.format("h%05d", rowIndex + rnd.nextInt(MAX_ROWS));
                String col9 = String.format("i%05d", rowIndex + rnd.nextInt(MAX_ROWS));

                return Lists.newArrayList(
                        new Object[] { groupById, zid, col4, col5, col6, col7, col8, col9 });
            }
        };

        // Create a test data reader/writer for the above schema.
        DataWriter dataWriter = new BasicDataWriter();
        List<String>
                columns =
                Lists.newArrayList("ID", "ZID", "COL4", "COL5", "COL6", "COL7", "COL8", "COL9");
        List<String> rowKeyColumns = Lists.newArrayList("ID", "ZID");
        String
                tenantConnectUrl =
                getUrl() + ';' + TENANT_ID_ATTRIB + '=' + schemaBuilder.getDataOptions()
                        .getTenantId();
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
            validateExpiredRowsAreNotReturnedUsingCounts(viewTTL, dataReader, schemaBuilder);

            // Case : group by sql
            dataReader.setValidationColumns(Arrays.asList("num_rows"));
            dataReader.setRowKeyColumns(Arrays.asList("num_rows"));
            dataReader.setDML(String
                    .format("SELECT count(1) as num_rows from %s GROUP BY ID HAVING count(1) > 0",
                            schemaBuilder.getEntityTenantViewName(), groupById));

            dataReader.setTargetEntity(schemaBuilder.getEntityTenantViewName());

            // Validate data before and after ttl expiration.
            validateExpiredRowsAreNotReturnedUsingCounts(viewTTL, dataReader, schemaBuilder);
        }
    }

    @Test public void testWithVariousSQLsForMultipleTenants() throws Exception {

        long viewTTL = 10000;
        TableOptions tableOptions = TableOptions.withDefaults();
        tableOptions.getTableColumns().clear();
        tableOptions.getTableColumnTypes().clear();

        GlobalViewOptions globalViewOptions = SchemaBuilder.GlobalViewOptions.withDefaults();
        globalViewOptions.setTableProps(String.format("VIEW_TTL=%d", viewTTL));

        GlobalViewIndexOptions
                globalViewIndexOptions =
                SchemaBuilder.GlobalViewIndexOptions.withDefaults();
        globalViewIndexOptions.setLocal(false);

        TenantViewOptions tenantViewOptions = new TenantViewOptions();
        tenantViewOptions.setTenantViewColumns(asList("ZID", "COL7", "COL8", "COL9"));
        tenantViewOptions
                .setTenantViewColumnTypes(asList("CHAR(15)", "VARCHAR", "VARCHAR", "VARCHAR"));

        tenantViewOptions.setTableProps(String.format("VIEW_TTL=%d", viewTTL));

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
            final String groupById = String.format("00A0y000%07d", 0);
            DataSupplier dataSupplier = new DataSupplier() {

                @Override public List<Object> getValues(int rowIndex) {
                    Random rnd = new Random();
                    String zid = String.format("00B0y000%07d", rowIndex);
                    String col4 = String.format("d%05d", rowIndex + rnd.nextInt(MAX_ROWS));
                    String col5 = String.format("e%05d", rowIndex + rnd.nextInt(MAX_ROWS));
                    String col6 = String.format("f%05d", rowIndex + rnd.nextInt(MAX_ROWS));
                    String col7 = String.format("g%05d", rowIndex + rnd.nextInt(MAX_ROWS));
                    String col8 = String.format("h%05d", rowIndex + rnd.nextInt(MAX_ROWS));
                    String col9 = String.format("i%05d", rowIndex + rnd.nextInt(MAX_ROWS));

                    return Lists.newArrayList(
                            new Object[] { groupById, zid, col4, col5, col6, col7, col8, col9 });
                }
            };

            // Create a test data reader/writer for the above schema.
            DataWriter dataWriter = new BasicDataWriter();
            List<String>
                    columns =
                    Lists.newArrayList("ID", "ZID", "COL4", "COL5", "COL6", "COL7", "COL8", "COL9");
            List<String> rowKeyColumns = Lists.newArrayList("ID", "ZID");
            String
                    tenantConnectUrl =
                    getUrl() + ';' + TENANT_ID_ATTRIB + '=' + schemaBuilder.getDataOptions()
                            .getTenantId();
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
                validateExpiredRowsAreNotReturnedUsingCounts(viewTTL, dataReader, schemaBuilder);

                // Case : group by sql
                dataReader.setValidationColumns(Arrays.asList("num_rows"));
                dataReader.setRowKeyColumns(Arrays.asList("num_rows"));
                dataReader.setDML(String
                        .format("SELECT count(1) as num_rows from %s GROUP BY ID HAVING count(1) > 0",
                                schemaBuilder.getEntityTenantViewName()));

                dataReader.setTargetEntity(schemaBuilder.getEntityTenantViewName());

                // Validate data before and after ttl expiration.
                validateExpiredRowsAreNotReturnedUsingCounts(viewTTL, dataReader, schemaBuilder);
            }
        }
    }

    @Test public void testWithVariousSQLsForMultipleViews() throws Exception {

        long viewTTL = 10000;
        TableOptions tableOptions = TableOptions.withDefaults();
        tableOptions.getTableColumns().clear();
        tableOptions.getTableColumnTypes().clear();

        TenantViewOptions tenantViewOptions = new TenantViewOptions();
        tenantViewOptions.setTenantViewColumns(asList("ZID", "COL7", "COL8", "COL9"));
        tenantViewOptions
                .setTenantViewColumnTypes(asList("CHAR(15)", "VARCHAR", "VARCHAR", "VARCHAR"));

        tenantViewOptions.setTableProps(String.format("VIEW_TTL=%d", viewTTL));

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
                    String zid = String.format("00B0y000%07d", rowIndex);
                    String col7 = String.format("g%05d", rowIndex + rnd.nextInt(MAX_ROWS));
                    String col8 = String.format("h%05d", rowIndex + rnd.nextInt(MAX_ROWS));
                    String col9 = String.format("i%05d", rowIndex + rnd.nextInt(MAX_ROWS));

                    return Lists.newArrayList(new Object[] { zid, col7, col8, col9 });
                }
            };

            // Create a test data reader/writer for the above schema.
            DataWriter dataWriter = new BasicDataWriter();
            List<String> columns = Lists.newArrayList("ZID", "COL7", "COL8", "COL9");
            List<String> rowKeyColumns = Lists.newArrayList("ZID");
            String
                    tenantConnectUrl =
                    getUrl() + ';' + TENANT_ID_ATTRIB + '=' + schemaBuilder.getDataOptions()
                            .getTenantId();
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
                validateExpiredRowsAreNotReturnedUsingCounts(viewTTL, dataReader, schemaBuilder);

                // Case : group by sql
                dataReader.setValidationColumns(Arrays.asList("num_rows"));
                dataReader.setRowKeyColumns(Arrays.asList("num_rows"));
                dataReader.setDML(String
                        .format("SELECT count(1) as num_rows from %s GROUP BY ZID HAVING count(1) > 0",
                                schemaBuilder.getEntityTenantViewName()));

                dataReader.setTargetEntity(schemaBuilder.getEntityTenantViewName());

                // Validate data before and after ttl expiration.
                validateExpiredRowsAreNotReturnedUsingCounts(viewTTL, dataReader, schemaBuilder);
            }
        }
    }

    @Test public void testWithTenantViewAndGlobalViewAndVariousOptions() throws Exception {
        long viewTTL = 10000;

        // Define the test schema
        TableOptions tableOptions = TableOptions.withDefaults();

        GlobalViewOptions globalViewOptions = SchemaBuilder.GlobalViewOptions.withDefaults();
        globalViewOptions.setTableProps(String.format("VIEW_TTL=%d", viewTTL));

        GlobalViewIndexOptions globalViewIndexOptions = GlobalViewIndexOptions.withDefaults();

        TenantViewOptions tenantViewOptions = new TenantViewOptions();
        tenantViewOptions.setTenantViewColumns(asList("ZID", "COL7", "COL8", "COL9"));
        tenantViewOptions
                .setTenantViewColumnTypes(asList("CHAR(15)", "VARCHAR", "VARCHAR", "VARCHAR"));
        tenantViewOptions.setTableProps(String.format("VIEW_TTL=%d", viewTTL));

        TenantViewIndexOptions tenantViewIndexOptions = TenantViewIndexOptions.withDefaults();

        OtherOptions testCaseWhenAllCFMatchAndAllDefault = new OtherOptions();
        testCaseWhenAllCFMatchAndAllDefault.setTestName("testCaseWhenAllCFMatchAndAllDefault");
        testCaseWhenAllCFMatchAndAllDefault
                .setTableCFs(Lists.newArrayList((String) null, null, null));
        testCaseWhenAllCFMatchAndAllDefault
                .setGlobalViewCFs(Lists.newArrayList((String) null, null, null));
        testCaseWhenAllCFMatchAndAllDefault
                .setTenantViewCFs(Lists.newArrayList((String) null, null, null, null));

        for (String additionalProps : Lists
                .newArrayList("COLUMN_ENCODED_BYTES=0", "DEFAULT_COLUMN_FAMILY='0'")) {

            StringBuilder withTableProps = new StringBuilder();
            withTableProps.append("MULTI_TENANT=true,").append(additionalProps);

            for (boolean isGlobalViewLocal : Lists.newArrayList(true, false)) {
                for (boolean isTenantViewLocal : Lists.newArrayList(true, false)) {

                    tableOptions.setTableProps(withTableProps.toString());
                    globalViewIndexOptions.setLocal(isGlobalViewLocal);
                    tenantViewIndexOptions.setLocal(isTenantViewLocal);
                    OtherOptions otherOptions = testCaseWhenAllCFMatchAndAllDefault;

                    final SchemaBuilder schemaBuilder = new SchemaBuilder(getUrl());
                    schemaBuilder.withTableOptions(tableOptions)
                            .withGlobalViewOptions(globalViewOptions)
                            .withGlobalViewIndexOptions(globalViewIndexOptions)
                            .withTenantViewOptions(tenantViewOptions)
                            .withTenantViewIndexOptions(tenantViewIndexOptions)
                            .withOtherOptions(testCaseWhenAllCFMatchAndAllDefault)
                            .buildWithNewTenant();

                    // Define the test data.
                    DataSupplier dataSupplier = new DataSupplier() {

                        @Override public List<Object> getValues(int rowIndex) {
                            Random rnd = new Random();
                            String id = String.format("00A0y000%07d", rowIndex);
                            String zid = String.format("00B0y000%07d", rowIndex);
                            String col1 = String.format("a%05d", rowIndex + rnd.nextInt(MAX_ROWS));
                            String col2 = String.format("b%05d", rowIndex + rnd.nextInt(MAX_ROWS));
                            String col3 = String.format("c%05d", rowIndex + rnd.nextInt(MAX_ROWS));
                            String col4 = String.format("d%05d", rowIndex + rnd.nextInt(MAX_ROWS));
                            String col5 = String.format("e%05d", rowIndex + rnd.nextInt(MAX_ROWS));
                            String col6 = String.format("f%05d", rowIndex + rnd.nextInt(MAX_ROWS));
                            String col7 = String.format("g%05d", rowIndex + rnd.nextInt(MAX_ROWS));
                            String col8 = String.format("h%05d", rowIndex + rnd.nextInt(MAX_ROWS));
                            String col9 = String.format("i%05d", rowIndex + rnd.nextInt(MAX_ROWS));

                            return Lists.newArrayList(
                                    new Object[] { id, zid, col1, col2, col3, col4, col5, col6,
                                            col7, col8, col9 });
                        }
                    };

                    // Create a test data reader/writer for the above schema.
                    DataWriter dataWriter = new BasicDataWriter();
                    DataReader dataReader = new BasicDataReader();

                    List<String>
                            columns =
                            Lists.newArrayList("ID", "ZID", "COL1", "COL2", "COL3", "COL4", "COL5",
                                    "COL6", "COL7", "COL8", "COL9");
                    List<String> rowKeyColumns = Lists.newArrayList("ID", "ZID");
                    String
                            tenantConnectUrl =
                            getUrl() + ';' + TENANT_ID_ATTRIB + '=' + schemaBuilder.getDataOptions()
                                    .getTenantId();
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

                    long scnTimestamp = System.currentTimeMillis() + viewTTL;
                    // Delete data by simulating expiration.
                    deleteData(schemaBuilder, scnTimestamp);

                    // Verify after deleting TTL expired data.
                    Properties props = new Properties();
                    props.setProperty("CurrentSCN", Long.toString(scnTimestamp));
                    try (Connection readConnection = DriverManager
                            .getConnection(tenantConnectUrl, props)) {

                        dataReader.setConnection(readConnection);
                        com.google.common.collect.Table<String, String, Object>
                                fetchedData =
                                fetchData(dataReader);
                        assertTrue("Expired rows should not be fetched",
                                fetchedData.rowKeySet().size() == 0);
                    }
                }
            }
        }
    }

    @Test public void testGlobalAndTenantViewTTLInheritance() throws Exception {
        long globalViewTTL = 300000;
        long tenantViewTTL = 30000;

        // Define the test schema.
        // 1. Table with default columns => (ORG_ID, KP, COL1, COL2, COL3), PK => (ORG_ID, KP)
        // 2. GlobalView with default columns => (ID, COL4, COL5, COL6), PK => (ID)
        // 3. Tenant with default columns => (ZID, COL7, COL8, COL9), PK => (ZID)
        final SchemaBuilder schemaBuilder = new SchemaBuilder(getUrl());

        TableOptions tableOptions = TableOptions.withDefaults();
        tableOptions.setTableProps("COLUMN_ENCODED_BYTES=0,MULTI_TENANT=true");

        SchemaBuilder.GlobalViewOptions
                globalViewOptions =
                SchemaBuilder.GlobalViewOptions.withDefaults();
        globalViewOptions.setTableProps(String.format("VIEW_TTL=%d", globalViewTTL));

        SchemaBuilder.GlobalViewIndexOptions
                globalViewIndexOptions =
                SchemaBuilder.GlobalViewIndexOptions.withDefaults();
        globalViewIndexOptions.setLocal(false);

        TenantViewOptions tenantViewWithOverrideOptions = new TenantViewOptions();
        tenantViewWithOverrideOptions.setTenantViewColumns(asList("ZID", "COL7", "COL8", "COL9"));
        tenantViewWithOverrideOptions
                .setTenantViewColumnTypes(asList("CHAR(15)", "VARCHAR", "VARCHAR", "VARCHAR"));
        tenantViewWithOverrideOptions.setTableProps(String.format("VIEW_TTL=%d", tenantViewTTL));

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
         * Case 1: Build schema with TTL overridden by the tenant view.
         * TTL for GLOBAL_VIEW - 300000
         * TTL for TENANT_VIEW - 30000
         * ************************************************************
         */
        schemaBuilder.withTableOptions(tableOptions).withGlobalViewOptions(globalViewOptions)
                .withGlobalViewIndexOptions(globalViewIndexOptions)
                .withTenantViewOptions(tenantViewWithOverrideOptions).withTenantViewIndexDefaults()
                .withOtherOptions(testCaseWhenAllCFMatchAndAllDefault).buildWithNewTenant();

        // Define the test data.
        final String id = String.format("00A0y000%07d", 0);
        DataSupplier dataSupplier = new DataSupplier() {

            @Override public List<Object> getValues(int rowIndex) {
                Random rnd = new Random();
                String zid = String.format("00B0y000%07d", rowIndex);
                String col1 = String.format("a%05d", rowIndex + rnd.nextInt(MAX_ROWS));
                String col2 = String.format("b%05d", rowIndex + rnd.nextInt(MAX_ROWS));
                String col3 = String.format("c%05d", rowIndex + rnd.nextInt(MAX_ROWS));
                String col4 = String.format("d%05d", rowIndex + rnd.nextInt(MAX_ROWS));
                String col5 = String.format("e%05d", rowIndex + rnd.nextInt(MAX_ROWS));
                String col6 = String.format("f%05d", rowIndex + rnd.nextInt(MAX_ROWS));
                String col7 = String.format("g%05d", rowIndex + rnd.nextInt(MAX_ROWS));
                String col8 = String.format("h%05d", rowIndex + rnd.nextInt(MAX_ROWS));
                String col9 = String.format("i%05d", rowIndex + rnd.nextInt(MAX_ROWS));

                return Lists.newArrayList(
                        new Object[] { id, zid, col1, col2, col3, col4, col5, col6, col7, col8,
                                col9 });
            }
        };

        // Create a test data reader/writer for the above schema.
        DataWriter dataWriter = new BasicDataWriter();
        List<String>
                columns =
                Lists.newArrayList("ID", "ZID", "COL1", "COL2", "COL3", "COL4", "COL5", "COL6",
                        "COL7", "COL8", "COL9");
        List<String> rowKeyColumns = Lists.newArrayList("ID", "ZID");
        String
                tenant1ConnectUrl =
                getUrl() + ';' + TENANT_ID_ATTRIB + '=' + schemaBuilder.getDataOptions()
                        .getTenantId();
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

            // Validate data before and after ttl expiration.
            // Use the ttl overridden by the tenant.
            validateExpiredRowsAreNotReturnedUsingCounts(tenantViewTTL, dataReader, schemaBuilder);
        }

        /**
         * ************************************************************
         * Case 2: Build schema with TTL NOT overridden by the tenant view.
         * TTL for GLOBAL_VIEW - 300000
         * TTL for TENANT_VIEW - 300000
         * ************************************************************
         */
        TenantViewOptions tenantViewWithoutOverrideOptions = new TenantViewOptions();
        tenantViewWithoutOverrideOptions
                .setTenantViewColumns(asList("ZID", "COL7", "COL8", "COL9"));
        tenantViewWithoutOverrideOptions
                .setTenantViewColumnTypes(asList("CHAR(15)", "VARCHAR", "VARCHAR", "VARCHAR"));

        schemaBuilder.withTableOptions(tableOptions).withGlobalViewOptions(globalViewOptions)
                .withGlobalViewIndexOptions(globalViewIndexOptions)
                .withTenantViewOptions(tenantViewWithoutOverrideOptions)
                .withTenantViewIndexDefaults().withOtherOptions(testCaseWhenAllCFMatchAndAllDefault)
                .buildWithNewTenant();

        String
                tenant2ConnectUrl =
                getUrl() + ';' + TENANT_ID_ATTRIB + '=' + schemaBuilder.getDataOptions()
                        .getTenantId();
        try (Connection writeConnection = DriverManager.getConnection(tenant2ConnectUrl)) {
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
            long probeTimestamp = System.currentTimeMillis() + globalViewTTL / 2;
            validateRowsAreNotMaskedUsingCounts(probeTimestamp, dataReader, schemaBuilder);
            // Validate data before and after ttl expiration.
            // Use the global view ttl since that is what the view has inherited.
            validateExpiredRowsAreNotReturnedUsingCounts(globalViewTTL, dataReader, schemaBuilder);
        }
    }

    @Test public void testDeleteIfExpiredOnTenantView() throws Exception {

        long viewTTL = 100;
        TableOptions tableOptions = TableOptions.withDefaults();
        tableOptions.getTableColumns().clear();
        tableOptions.getTableColumnTypes().clear();

        TenantViewOptions tenantViewOptions = TenantViewOptions.withDefaults();
        tenantViewOptions.setTableProps(String.format("VIEW_TTL=%d", viewTTL));

        // Define the test schema.
        final SchemaBuilder schemaBuilder = new SchemaBuilder(getUrl());
        schemaBuilder.withTableOptions(tableOptions).withTenantViewOptions(tenantViewOptions)
                .build();

        // Define the test data.
        DataSupplier dataSupplier = new DataSupplier() {

            @Override public List<Object> getValues(int rowIndex) {
                Random rnd = new Random();
                String zid = String.format("00A0y000%07d", rowIndex);
                String col7 = String.format("g%05d", rowIndex + rnd.nextInt(MAX_ROWS));
                String col8 = String.format("h%05d", rowIndex + rnd.nextInt(MAX_ROWS));
                String col9 = String.format("i%05d", rowIndex + rnd.nextInt(MAX_ROWS));
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
                getUrl() + ';' + TENANT_ID_ATTRIB + '=' + schemaBuilder.getDataOptions()
                        .getTenantId();
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

        // Delete expired rows.
        deleteData(schemaBuilder, viewTTL);
        // Since we
        TimeUnit.MILLISECONDS.sleep(2 * viewTTL);

        // Verify after deleting TTL expired data.
        long scnTimestamp = System.currentTimeMillis() + viewTTL;
        Properties props = new Properties();
        props.setProperty("CurrentSCN", Long.toString(scnTimestamp));
        try (Connection readConnection = DriverManager.getConnection(tenantConnectUrl, props)) {

            dataReader.setConnection(readConnection);
            com.google.common.collect.Table<String, String, Object>
                    fetchedData =
                    fetchData(dataReader);
            assertTrue("Expired rows should not be fetched", fetchedData.rowKeySet().size() == 0);
        }
    }

    private void upsertDataAndRunValidations(long viewTTL, int numRowsToUpsert,
            DataWriter dataWriter, DataReader dataReader, SchemaBuilder schemaBuilder)
            throws IOException, SQLException {

        //Insert for the first time and validate them.
        validateExpiredRowsAreNotReturnedUsingData(viewTTL, upsertData(dataWriter, numRowsToUpsert),
                dataReader, schemaBuilder);

        // Update the above rows and validate the same.
        validateExpiredRowsAreNotReturnedUsingData(viewTTL, upsertData(dataWriter, numRowsToUpsert),
                dataReader, schemaBuilder);

    }

    private void validateExpiredRowsAreNotReturnedUsingCounts(long viewTTL, DataReader dataReader,
            SchemaBuilder schemaBuilder) throws SQLException {

        String
                tenantConnectUrl =
                getUrl() + ';' + TENANT_ID_ATTRIB + '=' + schemaBuilder.getDataOptions()
                        .getTenantId();

        // Verify before TTL expiration
        try (Connection readConnection = DriverManager.getConnection(tenantConnectUrl)) {

            dataReader.setConnection(readConnection);
            com.google.common.collect.Table<String, String, Object>
                    fetchedData =
                    fetchData(dataReader);
            assertTrue("Fetched data should not be null", fetchedData != null);
            assertTrue("Rows should exists before expiration", fetchedData.rowKeySet().size() > 0);
        }

        // Verify after TTL expiration
        long scnTimestamp = System.currentTimeMillis();
        Properties props = new Properties();
        props.setProperty("CurrentSCN", Long.toString(scnTimestamp + (2 * viewTTL)));
        try (Connection readConnection = DriverManager.getConnection(tenantConnectUrl, props)) {

            dataReader.setConnection(readConnection);
            com.google.common.collect.Table<String, String, Object>
                    fetchedData =
                    fetchData(dataReader);
            assertTrue("Fetched data should not be null", fetchedData != null);
            assertTrue("Expired rows should not be fetched", fetchedData.rowKeySet().size() == 0);
        }
    }

    private void validateExpiredRowsAreNotReturnedUsingData(long viewTTL,
            com.google.common.collect.Table<String, String, Object> upsertedData,
            DataReader dataReader, SchemaBuilder schemaBuilder) throws SQLException {

        String
                tenantConnectUrl =
                getUrl() + ';' + TENANT_ID_ATTRIB + '=' + schemaBuilder.getDataOptions()
                        .getTenantId();

        // Verify before TTL expiration
        try (Connection readConnection = DriverManager.getConnection(tenantConnectUrl)) {

            dataReader.setConnection(readConnection);
            com.google.common.collect.Table<String, String, Object>
                    fetchedData =
                    fetchData(dataReader);
            assertTrue("Upserted data should not be null", upsertedData != null);
            assertTrue("Fetched data should not be null", fetchedData != null);

            verifyRowsBeforeTTLExpiration(upsertedData, fetchedData);
        }

        // Verify after TTL expiration
        long scnTimestamp = System.currentTimeMillis();
        Properties props = new Properties();
        props.setProperty("CurrentSCN", Long.toString(scnTimestamp + (2 * viewTTL)));
        try (Connection readConnection = DriverManager.getConnection(tenantConnectUrl, props)) {

            dataReader.setConnection(readConnection);
            com.google.common.collect.Table<String, String, Object>
                    fetchedData =
                    fetchData(dataReader);
            assertTrue("Fetched data should not be null", fetchedData != null);
            assertTrue("Expired rows should not be fetched", fetchedData.rowKeySet().size() == 0);
        }

    }

    private void validateRowsAreNotMaskedUsingCounts(long probeTimestamp, DataReader dataReader,
            SchemaBuilder schemaBuilder) throws SQLException {

        String
                tenantConnectUrl =
                getUrl() + ';' + TENANT_ID_ATTRIB + '=' + schemaBuilder.getDataOptions()
                        .getTenantId();

        // Verify rows exists (not masked) at current time
        try (Connection readConnection = DriverManager.getConnection(tenantConnectUrl)) {

            dataReader.setConnection(readConnection);
            com.google.common.collect.Table<String, String, Object>
                    fetchedData =
                    fetchData(dataReader);
            assertTrue("Fetched data should not be null", fetchedData != null);
            assertTrue("Rows should exists before ttl expiration (now)",
                    fetchedData.rowKeySet().size() > 0);
        }

        // Verify rows exists (not masked) at probed timestamp
        Properties props = new Properties();
        props.setProperty("CurrentSCN", Long.toString(probeTimestamp));
        try (Connection readConnection = DriverManager.getConnection(tenantConnectUrl, props)) {

            dataReader.setConnection(readConnection);
            com.google.common.collect.Table<String, String, Object>
                    fetchedData =
                    fetchData(dataReader);
            assertTrue("Fetched data should not be null", fetchedData != null);
            assertTrue("Rows should exists before ttl expiration (probe-timestamp)",
                    fetchedData.rowKeySet().size() > 0);
        }
    }

    private void verifyRowsBeforeTTLExpiration(
            com.google.common.collect.Table<String, String, Object> upsertedData,
            com.google.common.collect.Table<String, String, Object> fetchedData) {

        Set<String> upsertedRowKeys = upsertedData.rowKeySet();
        Set<String> fetchedRowKeys = fetchedData.rowKeySet();
        assertTrue("Upserted row keys should not be null", upsertedRowKeys != null);
        assertTrue("Fetched row keys should not be null", fetchedRowKeys != null);
        assertTrue("Rows upserted and fetched do not match",
                upsertedRowKeys.equals(fetchedRowKeys));

        Set<String> fetchedCols = fetchedData.columnKeySet();
        for (String rowKey : fetchedRowKeys) {
            for (String columnKey : fetchedCols) {
                Object upsertedValue = upsertedData.get(rowKey, columnKey);
                Object fetchedValue = fetchedData.get(rowKey, columnKey);
                assertTrue("Upserted values should not be null", upsertedValue != null);
                assertTrue("Fetched values should not be null", fetchedValue != null);
                assertTrue("Values upserted and fetched do not match",
                        upsertedValue.equals(fetchedValue));
            }
        }
    }

    private com.google.common.collect.Table<String, String, Object> upsertData(
            DataWriter dataWriter, int numRowsToUpsert) throws SQLException {
        // Upsert rows
        dataWriter.upsertRows(numRowsToUpsert);
        return dataWriter.getDataTable();
    }

    private com.google.common.collect.Table<String, String, Object> fetchData(DataReader dataReader)
            throws SQLException {

        dataReader.readRows();
        return dataReader.getDataTable();
    }

    private void deleteData(SchemaBuilder schemaBuilder, long viewTTL) throws SQLException {

        String viewName = schemaBuilder.getEntityTenantViewName();

        Properties props = new Properties();
        String
                tenantConnectUrl =
                getUrl() + ';' + TENANT_ID_ATTRIB + '=' + schemaBuilder.getDataOptions()
                        .getTenantId();

        try (Connection deleteConnection = DriverManager.getConnection(tenantConnectUrl, props);
                final Statement statement = deleteConnection.createStatement()) {

            deleteConnection.setAutoCommit(true);

            final String
                    deleteIfExpiredStatement =
                    String.format("delete from %s where %s", viewName, String.format(
                            "((TO_NUMBER(CURRENT_TIME()) - TO_NUMBER(phoenix_row_timestamp())) < %d)",
                            viewTTL));
            Preconditions.checkNotNull(deleteIfExpiredStatement);

            final PhoenixStatement pstmt = statement.unwrap(PhoenixStatement.class);
            int deleteCount = pstmt.executeUpdate(deleteIfExpiredStatement);
        }
    }
}
