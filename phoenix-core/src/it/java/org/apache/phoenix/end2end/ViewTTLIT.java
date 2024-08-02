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
import org.apache.phoenix.coprocessorclient.BaseScannerRegionObserverConstants;
import org.apache.phoenix.coprocessor.BaseScannerRegionObserver;
import org.apache.phoenix.exception.SQLExceptionCode;
import org.apache.phoenix.jdbc.PhoenixConnection;
import org.apache.phoenix.jdbc.PhoenixResultSet;
import org.apache.phoenix.jdbc.PhoenixStatement;
import org.apache.phoenix.query.PhoenixTestBuilder;
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
import org.apache.phoenix.query.QueryConstants;
import org.apache.phoenix.query.QueryServices;
import org.apache.phoenix.schema.PTable;
import org.apache.phoenix.schema.PTableType;
import org.apache.phoenix.schema.types.PDataType;
import org.apache.phoenix.schema.types.PInteger;
import org.apache.phoenix.schema.types.PLong;
import org.apache.phoenix.schema.types.PVarchar;
import org.apache.phoenix.thirdparty.com.google.common.base.Joiner;
import org.apache.phoenix.thirdparty.com.google.common.base.Preconditions;
import org.apache.phoenix.thirdparty.com.google.common.collect.Lists;
import org.apache.phoenix.util.EnvironmentEdgeManager;
import org.apache.phoenix.util.ManualEnvironmentEdge;
import org.apache.phoenix.util.ReadOnlyProps;
import org.apache.phoenix.util.ScanUtil;
import org.apache.phoenix.util.SchemaUtil;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Random;

import static java.util.Arrays.asList;
import static org.apache.phoenix.query.PhoenixTestBuilder.DDLDefaults.COLUMN_TYPES;
import static org.apache.phoenix.query.PhoenixTestBuilder.DDLDefaults.DEFAULT_SCHEMA_NAME;
import static org.apache.phoenix.query.PhoenixTestBuilder.DDLDefaults.MAX_ROWS;
import static org.apache.phoenix.query.PhoenixTestBuilder.DDLDefaults.TENANT_VIEW_COLUMNS;
import static org.apache.phoenix.query.PhoenixTestBuilder.DDLDefaults.TENANT_VIEW_INCLUDE_COLUMNS;
import static org.apache.phoenix.query.PhoenixTestBuilder.DDLDefaults.TENANT_VIEW_INDEX_COLUMNS;
import static org.apache.phoenix.query.PhoenixTestBuilder.DDLDefaults.TENANT_VIEW_PK_COLUMNS;
import static org.apache.phoenix.query.PhoenixTestBuilder.DDLDefaults.TENANT_VIEW_PK_TYPES;
import static org.apache.phoenix.util.PhoenixRuntime.TENANT_ID_ATTRIB;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

@Category(NeedsOwnMiniClusterTest.class)
public class ViewTTLIT extends BaseViewTTLIT {

    @BeforeClass
    public static void doSetup() throws Exception {
        // Turn on the View TTL feature
        Map<String, String> DEFAULT_PROPERTIES = new HashMap<String, String>() {{
            put(QueryServices.PHOENIX_TABLE_TTL_ENABLED, String.valueOf(true));
            put(QueryServices.LONG_VIEW_INDEX_ENABLED_ATTRIB, String.valueOf(false));
            put("hbase.procedure.remote.dispatcher.delay.msec", "0");
            // no max lookback
            put(BaseScannerRegionObserverConstants.PHOENIX_MAX_LOOKBACK_AGE_CONF_KEY, Integer.toString(0));
            put(QueryServices.PHOENIX_VIEW_TTL_ENABLED, Boolean.toString(true));
            put(QueryServices.PHOENIX_VIEW_TTL_TENANT_VIEWS_PER_SCAN_LIMIT, String.valueOf(1));
        }};

        setUpTestDriver(new ReadOnlyProps(ReadOnlyProps.EMPTY_PROPS,
                        DEFAULT_PROPERTIES.entrySet().iterator()));
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
        // Since the TTL property values are not being set,
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
        tenantViewOptions.setTableProps("TTL=1000");
        try {
            schemaBuilder.withTableOptions(tableOptions).withTenantViewOptions(tenantViewOptions)
                    .buildNewView();
            fail();
        } catch (SQLException e) {
            assertEquals(SQLExceptionCode.TTL_ALREADY_DEFINED_IN_HIERARCHY
                    .getErrorCode(), e.getErrorCode());
        }
    }

    @Test
    public void testPhoenixTTLWithViewIndexFails() throws Exception {

        TenantViewIndexOptions tenantViewIndexOptions = TenantViewIndexOptions.withDefaults();
        tenantViewIndexOptions.setIndexProps("TTL=1000");
        try {
            final SchemaBuilder schemaBuilder = createLevel1TenantView(null,
                    tenantViewIndexOptions);
            fail();
        } catch (SQLException e) {
            assertEquals(SQLExceptionCode.CANNOT_SET_OR_ALTER_PROPERTY_FOR_INDEX.getErrorCode(),
                    e.getErrorCode());
        }
    }

    @Test
    public void testPhoenixTTLForLevelOneView() throws Exception {
        long startTime = EnvironmentEdgeManager.currentTimeMillis();

        TenantViewOptions tenantViewOptions = TenantViewOptions.withDefaults();
        // View TTL is set to 120s => 120000 ms
        tenantViewOptions.setTableProps(String.format("TTL=%d", VIEW_TTL_120_SECS));

        final SchemaBuilder schemaBuilder = createLevel1TenantView(tenantViewOptions, null);
        String tenantId = schemaBuilder.getDataOptions().getTenantId();
        String schemaName = stripQuotes(
                SchemaUtil.getSchemaNameFromFullName(schemaBuilder.getEntityTenantViewName()));
        String tenantViewName = stripQuotes(
                SchemaUtil.getTableNameFromFullName(schemaBuilder.getEntityTenantViewName()));
        String indexOnTenantViewName = String
                .format("IDX_%s", stripQuotes(schemaBuilder.getEntityKeyPrefix()));

        // Expected 1 rows - one for TenantView.
        // Since the TTL property values are being set,
        // we expect the view header columns to show up in regular scans too.
        assertViewHeaderRowsHavePhoenixTTLRelatedCells(
                schemaBuilder.getTableOptions().getSchemaName(), startTime, false, 1);
        assertSyscatHavePhoenixTTLRelatedColumns(tenantId, schemaName, tenantViewName,
                PTableType.VIEW.getSerializedValue(), 120);
        assertSyscatHavePhoenixTTLRelatedColumns(tenantId, schemaName, indexOnTenantViewName,
                PTableType.INDEX.getSerializedValue(), 0);

    }

    @Test
    public void testPhoenixTTLForLevelTwoView() throws Exception {
        long startTime = EnvironmentEdgeManager.currentTimeMillis();

        final SchemaBuilder schemaBuilder = createLevel2TenantViewWithGlobalLevelTTL(null, null,
                false);

        String tenantId = schemaBuilder.getDataOptions().getTenantId();
        String schemaName = stripQuotes(
                SchemaUtil.getSchemaNameFromFullName(schemaBuilder.getEntityTenantViewName()));
        String globalViewName = stripQuotes(
                SchemaUtil.getTableNameFromFullName(schemaBuilder.getEntityGlobalViewName()));
        String tenantViewName = stripQuotes(
                SchemaUtil.getTableNameFromFullName(schemaBuilder.getEntityTenantViewName()));

        // Expected 1 rows - one for GlobalView.
        // Since the TTL property values are being set,
        // we expect the view header columns to show up in regular scans too.
        assertViewHeaderRowsHavePhoenixTTLRelatedCells(
                schemaBuilder.getTableOptions().getSchemaName(), startTime, false, 1);
        assertSyscatHavePhoenixTTLRelatedColumns("", schemaName, globalViewName,
                PTableType.VIEW.getSerializedValue(), 300);
        // Since the TTL property values are not being overridden,
        // we expect the TTL value to be not set for the tenant view.
        assertSyscatHavePhoenixTTLRelatedColumns(tenantId, schemaName, tenantViewName,
                PTableType.VIEW.getSerializedValue(), 0);
    }

    @Test
    public void testPhoenixTTLForWhenTTLIsZero() throws Exception {
        long startTime = EnvironmentEdgeManager.currentTimeMillis();

        TenantViewOptions tenantViewOptions = TenantViewOptions.withDefaults();
        // Client can also specify TTL=NONE
        tenantViewOptions.setTableProps("TTL=0");
        final SchemaBuilder schemaBuilder = createLevel1TenantView(tenantViewOptions, null);

        String tenantId = schemaBuilder.getDataOptions().getTenantId();
        String schemaName = stripQuotes(
                SchemaUtil.getSchemaNameFromFullName(schemaBuilder.getEntityTenantViewName()));
        String tenantViewName = stripQuotes(
                SchemaUtil.getTableNameFromFullName(schemaBuilder.getEntityTenantViewName()));
        String indexOnTenantViewName = String
                .format("IDX_%s", stripQuotes(schemaBuilder.getEntityKeyPrefix()));

        // Expected 3 deleted rows - one for Table, one for TenantView and ViewIndex each.
        // Since the TTL property values are not being set or being set to zero,
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
        int ttl = VIEW_TTL_120_SECS;

        TenantViewOptions tenantViewOptions = TenantViewOptions.withDefaults();
        // Client can also specify TTL=0
        tenantViewOptions.setTableProps("TTL=NONE");
        final SchemaBuilder schemaBuilder = createLevel1TenantView(tenantViewOptions, null);

        String tenantId = schemaBuilder.getDataOptions().getTenantId();
        String schemaName = stripQuotes(
                SchemaUtil.getSchemaNameFromFullName(schemaBuilder.getEntityTenantViewName()));
        String tenantViewName = stripQuotes(
                SchemaUtil.getTableNameFromFullName(schemaBuilder.getEntityTenantViewName()));
        String indexOnTenantViewName = String
                .format("IDX_%s", stripQuotes(schemaBuilder.getEntityKeyPrefix()));

        // Expected 3 deleted rows - one for Table, one for TenantView and ViewIndex each.
        // Since the TTL property values are not being set or being set to zero,
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
                // View TTL is set to 120s => 120000 ms
                String sql = String
                        .format(ALTER_TTL_SQL, schemaName, tenantViewName, String.valueOf(ttl));
                stmt.execute(sql);
            }
        }

        // Expected 1 rows - one for TenantView
        // The ViewIndex will inherit the TTL from the view, the actual value in SYSCAT will be 0.
        // Since the TTL property values are being set,
        // we expect the view header columns to show up in regular scans too.
        assertViewHeaderRowsHavePhoenixTTLRelatedCells(
                schemaBuilder.getTableOptions().getSchemaName(), startTime, false, 1);
        assertSyscatHavePhoenixTTLRelatedColumns(tenantId, schemaName, tenantViewName,
                PTableType.VIEW.getSerializedValue(), ttl);
        assertSyscatHavePhoenixTTLRelatedColumns(tenantId, schemaName, indexOnTenantViewName,
                PTableType.INDEX.getSerializedValue(), 0);

    }

    @Test
    public void testTTLAlreadyDefinedInHierarchyWhenCreateView() throws Exception {
        try {
            TenantViewOptions tenantViewWithOverrideOptions = TenantViewOptions.withDefaults();
            // View TTL is set to 120s => 120000 ms
            tenantViewWithOverrideOptions.setTableProps(String.format("TTL=%d", VIEW_TTL_120_SECS));
            final SchemaBuilder schemaBuilder = createLevel2TenantViewWithGlobalLevelTTL(
                    tenantViewWithOverrideOptions, null, false);
            fail();
        } catch (SQLException e) {
            assertEquals(SQLExceptionCode.TTL_ALREADY_DEFINED_IN_HIERARCHY.getErrorCode(),
                    e.getErrorCode());
        }
    }

    @Test
    public void testTTLAlreadyDefinedInHierarchyWhenAlterTenantView() throws Exception {
        long startTime = EnvironmentEdgeManager.currentTimeMillis();

        // View TTL is set to 300s
        final SchemaBuilder schemaBuilder = createLevel2TenantViewWithGlobalLevelTTL(null, null,
                false);

        String tenantId = schemaBuilder.getDataOptions().getTenantId();
        String schemaName = stripQuotes(
                SchemaUtil.getSchemaNameFromFullName(schemaBuilder.getEntityTenantViewName()));
        String globalViewName = stripQuotes(
                SchemaUtil.getTableNameFromFullName(schemaBuilder.getEntityGlobalViewName()));
        String tenantViewName = stripQuotes(
                SchemaUtil.getTableNameFromFullName(schemaBuilder.getEntityTenantViewName()));

        // Expected 1 rows - one for GlobalView.
        // Since the PHOENIX_TTL property values are being set,
        // we expect the view header columns to show up in regular scans too.
        assertViewHeaderRowsHavePhoenixTTLRelatedCells(
                schemaBuilder.getTableOptions().getSchemaName(), startTime, false, 1);
        assertSyscatHavePhoenixTTLRelatedColumns("", schemaName, globalViewName,
                PTableType.VIEW.getSerializedValue(), 300);
        // Since the TTL is set at the global level
        // we expect the TTL value to be not set (= 0) at tenant level.
        assertSyscatHavePhoenixTTLRelatedColumns(tenantId, schemaName, tenantViewName,
                PTableType.VIEW.getSerializedValue(), 0);

        String tenantURL = getUrl() + ';' + TENANT_ID_ATTRIB + '=' + tenantId;
        try (Connection connection = DriverManager.getConnection(tenantURL)) {
            try (Statement stmt = connection.createStatement()) {
                // View TTL is set to 120s => 120000 ms
                String sql = String
                        .format(ALTER_TTL_SQL, schemaName, tenantViewName, "120");
                stmt.execute(sql);
                fail();
            }
        } catch (SQLException e) {
            assertEquals(SQLExceptionCode.TTL_ALREADY_DEFINED_IN_HIERARCHY.getErrorCode(),
                    e.getErrorCode());
        }
    }

    @Test
    public void testTTLAlreadyDefinedInHierarchyWhenAlterGlobalView() throws Exception {
        long startTime = EnvironmentEdgeManager.currentTimeMillis();

        // View TTL is set to 300s
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

        // Expected 1 rows - one for TenantView.
        // Since the TTL property values are being set,
        // we expect the view header columns to show up in regular scans too.
        assertViewHeaderRowsHavePhoenixTTLRelatedCells(
                schemaBuilder.getTableOptions().getSchemaName(), startTime, false, 1);
        assertSyscatHavePhoenixTTLRelatedColumns(tenantId, schemaName, tenantViewName,
                PTableType.VIEW.getSerializedValue(), 300);
        assertSyscatHavePhoenixTTLRelatedColumns(tenantId, schemaName, indexOnTenantViewName,
                PTableType.INDEX.getSerializedValue(), 0);

        try (Connection connection = DriverManager.getConnection(getUrl())) {
            try (Statement stmt = connection.createStatement()) {
                // View TTL is set to 120s => 120000 ms
                String sql = String
                        .format(ALTER_TTL_SQL, schemaName, globalViewName, "120");
                stmt.execute(sql);
                fail();
            }
        } catch (SQLException e) {
            assertEquals(SQLExceptionCode.TTL_ALREADY_DEFINED_IN_HIERARCHY.getErrorCode(),
                    e.getErrorCode());
        }
    }

    @Test
    public void testAlterViewWithNoTTLPropertySucceed() throws Exception {
        long startTime = EnvironmentEdgeManager.currentTimeMillis();

        // View TTL is set to 300s
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

        // Expected 1 rows - one for TenantView.
        // Since the TTL property values are being set,
        // we expect the view header columns to show up in regular scans too.
        assertViewHeaderRowsHavePhoenixTTLRelatedCells(
                schemaBuilder.getTableOptions().getSchemaName(), startTime, false, 1);
        assertSyscatHavePhoenixTTLRelatedColumns(tenantId, schemaName, tenantViewName,
                PTableType.VIEW.getSerializedValue(), 300);
        assertSyscatHavePhoenixTTLRelatedColumns(tenantId, schemaName, indexOnTenantViewName,
                PTableType.INDEX.getSerializedValue(), 0);

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
    public void testResetViewTTL() throws Exception {
        long startTime = EnvironmentEdgeManager.currentTimeMillis();

        final SchemaBuilder schemaBuilder = createLevel2TenantViewWithGlobalLevelTTL(null, null,
                false);
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
                // View TTL is set to 'NONE'
                String sql = String
                        .format(ALTER_TTL_SQL, schemaName, globalViewName, "'NONE'");
                stmt.execute(sql);
            }
        }

        // Expected 4 rows (DeleteColumn) - one for Table,
        // GlobalView, TenantView and Tenant view index.
        // Since the PHOENIX_TTL property values for global view are being reset,
        // we expect the view header columns value to be set to zero.
        assertViewHeaderRowsHavePhoenixTTLRelatedCells(
                schemaBuilder.getTableOptions().getSchemaName(), startTime, true, 4);
        assertSyscatHavePhoenixTTLRelatedColumns("", schemaName, globalViewName,
                PTableType.VIEW.getSerializedValue(), 0);
        assertSyscatHavePhoenixTTLRelatedColumns("", schemaName, indexOnGlobalViewName,
                PTableType.INDEX.getSerializedValue(), 0);
        // Since the TTL property values for the tenant view are not being reset,
        // we expect the TTL value to be same as before.
        assertSyscatHavePhoenixTTLRelatedColumns(tenantId, schemaName, tenantViewName,
                PTableType.VIEW.getSerializedValue(), 0);
        assertSyscatHavePhoenixTTLRelatedColumns(tenantId, schemaName, indexOnTenantViewName,
                PTableType.INDEX.getSerializedValue(), 0);
    }


    @Test
    public void testWithTenantViewAndNoGlobalView() throws Exception {
        // View TTL is set in seconds (for e.g 10 secs)
        int viewTTL = VIEW_TTL_10_SECS;
        TableOptions tableOptions = TableOptions.withDefaults();
        tableOptions.getTableColumns().clear();
        tableOptions.getTableColumnTypes().clear();


        for (boolean isMultiTenant : Lists.newArrayList(true, false)) {

            resetEnvironmentEdgeManager();
            tableOptions.setMultiTenant(isMultiTenant);
            DataOptions dataOptions =  isMultiTenant ?
                    DataOptions.withDefaults() :
                    DataOptions.withPrefix("CUSTOM_OBJECT");

            // OID, KP for non multi-tenanted views
            int viewCounter = 1;
            String orgId =
                    String.format(PhoenixTestBuilder.DDLDefaults.DEFAULT_ALT_TENANT_ID_FMT,
                            viewCounter,
                            dataOptions.getUniqueName());
            String keyPrefix = "C01";

            // Define the test schema.
            final SchemaBuilder schemaBuilder = new SchemaBuilder(getUrl());

            if (isMultiTenant) {
                TenantViewOptions tenantViewOptions = TenantViewOptions.withDefaults();
                tenantViewOptions.setTableProps(String.format("TTL=%d", viewTTL));
                schemaBuilder
                        .withTableOptions(tableOptions)
                        .withTenantViewOptions(tenantViewOptions)
                        .withDataOptions(dataOptions)
                        .withTenantViewIndexDefaults()
                        .buildWithNewTenant();
            } else {

                GlobalViewOptions globalViewOptions = new GlobalViewOptions();
                globalViewOptions.setSchemaName(PhoenixTestBuilder.DDLDefaults.DEFAULT_SCHEMA_NAME);
                globalViewOptions.setGlobalViewColumns(Lists.newArrayList(TENANT_VIEW_COLUMNS));
                globalViewOptions.setGlobalViewColumnTypes(Lists.newArrayList(COLUMN_TYPES));
                globalViewOptions.setGlobalViewPKColumns(Lists.newArrayList(TENANT_VIEW_PK_COLUMNS));
                globalViewOptions.setGlobalViewPKColumnTypes(Lists.newArrayList(TENANT_VIEW_PK_TYPES));
                globalViewOptions.setTableProps(String.format("TTL=%d", viewTTL));

                GlobalViewIndexOptions globalViewIndexOptions = new GlobalViewIndexOptions();
                globalViewIndexOptions.setGlobalViewIndexColumns(
                        Lists.newArrayList(TENANT_VIEW_INDEX_COLUMNS));
                globalViewIndexOptions.setGlobalViewIncludeColumns(
                        Lists.newArrayList(TENANT_VIEW_INCLUDE_COLUMNS));

                globalViewOptions.setGlobalViewCondition(String.format(
                        "SELECT * FROM %s.%s WHERE OID = '%s' AND KP = '%s'",
                        dataOptions.getSchemaName(), dataOptions.getTableName(), orgId, keyPrefix));
                ConnectOptions connectOptions = new ConnectOptions();
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
                    String zid = String.format(ID_FMT, rowIndex);
                    String col7 = String.format(COL7_FMT, rowIndex + rnd.nextInt(MAX_ROWS));
                    String col8 = String.format(COL8_FMT, rowIndex + rnd.nextInt(MAX_ROWS));
                    String col9 = String.format(COL9_FMT, rowIndex + rnd.nextInt(MAX_ROWS));
                    return isMultiTenant ?
                            Lists.newArrayList(new Object[] { zid, col7, col8, col9 }) :
                            Lists.newArrayList(new Object[] { oid, kp, zid, col7, col8, col9 });
                }
            };

            // Create a test data reader/writer for the above schema.
            DataWriter dataWriter = new BasicDataWriter();
            DataReader dataReader = new BasicDataReader();

            List<String> columns = isMultiTenant ?
                    Lists.newArrayList("ZID", "COL7", "COL8", "COL9"):
                    Lists.newArrayList("OID", "KP", "ZID", "COL7", "COL8", "COL9");
            List<String> rowKeyColumns =
                    isMultiTenant ?
                    Lists.newArrayList("ZID"):
                    Lists.newArrayList("OID", "KP", "ZID");
            String
                    tenantConnectUrl =
                    getUrl() + ';' + TENANT_ID_ATTRIB + '=' + schemaBuilder.getDataOptions().getTenantId();
            try (Connection writeConnection = DriverManager.getConnection(tenantConnectUrl)) {
                writeConnection.setAutoCommit(true);
                dataWriter.setConnection(writeConnection);
                dataWriter.setDataSupplier(dataSupplier);
                dataWriter.setUpsertColumns(columns);
                dataWriter.setRowKeyColumns(rowKeyColumns);
                dataWriter.setTargetEntity (
                        isMultiTenant ?
                                schemaBuilder.getEntityTenantViewName() :
                                schemaBuilder.getEntityGlobalViewName()
                        );

                dataReader.setValidationColumns(columns);
                dataReader.setRowKeyColumns(rowKeyColumns);
                dataReader.setDML(String.format("SELECT %s from %s", Joiner.on(",").join(columns),
                        isMultiTenant ?
                                schemaBuilder.getEntityTenantViewName() :
                                schemaBuilder.getEntityGlobalViewName()
                        ));
                dataReader.setTargetEntity(
                        isMultiTenant ?
                                schemaBuilder.getEntityTenantViewName() :
                                schemaBuilder.getEntityGlobalViewName()
                );

                // Validate data before and after ttl expiration.
                upsertDataAndRunValidations(viewTTL, DEFAULT_NUM_ROWS, dataWriter, dataReader,
                        schemaBuilder);
            }

        }

    }

    @Test
    public void testWithSQLUsingIndexWithCoveredColsUpdates() throws Exception {

        // View TTL is set in seconds (for e.g 10 secs)
        int viewTTL = VIEW_TTL_10_SECS;
        TableOptions tableOptions = TableOptions.withDefaults();
        tableOptions.getTableColumns().clear();
        tableOptions.getTableColumnTypes().clear();

        GlobalViewOptions globalViewOptions = SchemaBuilder.GlobalViewOptions.withDefaults();
        globalViewOptions.setTableProps(String.format("TTL=%d", viewTTL));

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
                validateExpiredRowsAreNotReturnedUsingCounts(viewTTL, dataReader, schemaBuilder);
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

        // View TTL is set in seconds (for e.g 10 secs)
        int viewTTL = VIEW_TTL_10_SECS;
        TableOptions tableOptions = TableOptions.withDefaults();
        tableOptions.getTableColumns().clear();
        tableOptions.getTableColumnTypes().clear();

        GlobalViewOptions globalViewOptions = SchemaBuilder.GlobalViewOptions.withDefaults();
        globalViewOptions.setTableProps(String.format("TTL=%d", viewTTL));

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
                validateExpiredRowsAreNotReturnedUsingCounts(viewTTL, dataReader, schemaBuilder);

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
                validateExpiredRowsAreNotReturnedUsingCounts(viewTTL, dataReader, schemaBuilder);
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

        // View TTL is set in seconds (for e.g 10 secs)
        int viewTTL = VIEW_TTL_10_SECS;
        TableOptions tableOptions = TableOptions.withDefaults();
        tableOptions.getTableColumns().clear();
        tableOptions.getTableColumnTypes().clear();

        GlobalViewOptions globalViewOptions = SchemaBuilder.GlobalViewOptions.withDefaults();
        globalViewOptions.setTableProps(String.format("TTL=%d", viewTTL));

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
            validateExpiredRowsAreNotReturnedUsingCounts(viewTTL, dataReader, schemaBuilder);

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
            validateExpiredRowsAreNotReturnedUsingCounts(viewTTL, dataReader, schemaBuilder);

        }
    }

    @Test
    public void testWithVariousSQLs() throws Exception {

        // View TTL is set in seconds (for e.g 10 secs)
        int viewTTL = VIEW_TTL_10_SECS;
        TableOptions tableOptions = TableOptions.withDefaults();
        tableOptions.getTableColumns().clear();
        tableOptions.getTableColumnTypes().clear();

        GlobalViewOptions globalViewOptions = SchemaBuilder.GlobalViewOptions.withDefaults();
        globalViewOptions.setTableProps(String.format("TTL=%d", viewTTL));

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

    @Test
    public void testWithVariousSQLsForMultipleTenants() throws Exception {

        // View TTL is set in seconds (for e.g 10 secs)
        int viewTTL = VIEW_TTL_10_SECS;
        TableOptions tableOptions = TableOptions.withDefaults();
        tableOptions.getTableColumns().clear();
        tableOptions.getTableColumnTypes().clear();

        GlobalViewOptions globalViewOptions = SchemaBuilder.GlobalViewOptions.withDefaults();
        globalViewOptions.setTableProps(String.format("TTL=%d", viewTTL));

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


    @Test
    public void testWithVariousSQLsForMultipleViews() throws Exception {

        // View TTL is set in seconds (for e.g 10 secs)
        int viewTTL = VIEW_TTL_10_SECS;
        TableOptions tableOptions = TableOptions.withDefaults();
        tableOptions.getTableColumns().clear();
        tableOptions.getTableColumnTypes().clear();

        TenantViewOptions tenantViewOptions = new TenantViewOptions();
        tenantViewOptions.setTenantViewColumns(asList("ZID", "COL7", "COL8", "COL9"));
        tenantViewOptions
                .setTenantViewColumnTypes(asList("CHAR(15)", "VARCHAR", "VARCHAR", "VARCHAR"));

        tenantViewOptions.setTableProps(String.format("TTL=%d", viewTTL));

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


    /**
     * ************************************************************
     * Case: Build schema with TTL set by the tenant view.
     * TTL for GLOBAL_VIEW - 300000ms (not set)
     * TTL for TENANT_VIEW - 300000ms
     * ************************************************************
     */

    @Test
    public void testGlobalAndTenantViewTTLInheritance1() throws Exception {
        // View TTL is set in seconds (for e.g 200 secs)
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
        tenantViewWithOverrideOptions.setTableProps(String.format("TTL=%d", tenantPhoenixTTL));

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
        // View TTL is set in seconds (for e.g 300 secs)
        long globalViewTTL = VIEW_TTL_300_SECS;

        // Define the test schema.
        // 1. Table with default columns => (ORG_ID, KP, COL1, COL2, COL3), PK => (ORG_ID, KP)
        // 2. GlobalView with default columns => (ID, COL4, COL5, COL6), PK => (ID)
        // 3. Tenant with default columns => (ZID, COL7, COL8, COL9), PK => (ZID)
        final SchemaBuilder schemaBuilder = new SchemaBuilder(getUrl());

        TableOptions tableOptions = TableOptions.withDefaults();
        tableOptions.setTableProps("COLUMN_ENCODED_BYTES=0,MULTI_TENANT=true");

        SchemaBuilder.GlobalViewOptions globalViewOptions =
                SchemaBuilder.GlobalViewOptions.withDefaults();
        globalViewOptions.setTableProps(String.format("TTL=%d", globalViewTTL));

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
                    + ((globalViewTTL * 1000) / 2);
            validateRowsAreNotMaskedUsingCounts(probeTimestamp, dataReader, schemaBuilder);
            // Validate data before and after ttl expiration.
            // Use the global phoenix ttl since that is what the view has inherited.
            validateExpiredRowsAreNotReturnedUsingCounts(globalViewTTL, dataReader, schemaBuilder);
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
        tenantViewOptions.setTableProps(String.format("TTL=%d", phoenixTTL));

        // Define the test schema.
        final SchemaBuilder schemaBuilder = new SchemaBuilder(getUrl());
        schemaBuilder.withTableOptions(tableOptions).withTenantViewOptions(tenantViewOptions)
                .build();

        String viewName = schemaBuilder.getEntityTenantViewName();

        Properties props = new Properties();
        String
                tenantConnectUrl =
                getUrl() + ';' + TENANT_ID_ATTRIB + '=' + schemaBuilder.getDataOptions()
                        .getTenantId();

        // Test setting masking expired rows property
        try (PhoenixConnection conn = (PhoenixConnection) DriverManager.getConnection(
                tenantConnectUrl, props); final Statement statement = conn.createStatement()) {
            conn.setAutoCommit(true);

            final String stmtString = String.format("select * from  %s", viewName);
            Preconditions.checkNotNull(stmtString);
            final PhoenixStatement pstmt = statement.unwrap(PhoenixStatement.class);
            final QueryPlan queryPlan = pstmt.optimizeQuery(stmtString);

            PTable table = conn.getTable(schemaBuilder.getDataOptions().getTenantId(), viewName);

            PhoenixResultSet
                    rs =
                    pstmt.newResultSet(queryPlan.iterator(), queryPlan.getProjector(),
                            queryPlan.getContext());
            Assert.assertFalse("Should not have any rows", rs.next());
            Assert.assertEquals("Should have atleast one element", 1, queryPlan.getScans().size());
            Assert.assertEquals("PhoenixTTL does not match", phoenixTTL,
                    ScanUtil.getTTL(queryPlan.getScans().get(0).get(0)));
            Assert.assertTrue("Masking attribute should be set",
                    ScanUtil.isMaskTTLExpiredRows(queryPlan.getScans().get(0).get(0)));
            Assert.assertFalse("Delete Expired attribute should not set",
                    ScanUtil.isDeleteTTLExpiredRows(queryPlan.getScans().get(0).get(0)));
        }

        // Test setting delete expired rows property
        try (PhoenixConnection conn = (PhoenixConnection) DriverManager.getConnection(
                tenantConnectUrl, props); final Statement statement = conn.createStatement()) {
            conn.setAutoCommit(true);

            final String stmtString = String.format("select * from  %s", viewName);
            Preconditions.checkNotNull(stmtString);
            final PhoenixStatement pstmt = statement.unwrap(PhoenixStatement.class);
            final QueryPlan queryPlan = pstmt.optimizeQuery(stmtString);
            final Scan scan = queryPlan.getContext().getScan();

            PTable table = conn.getTable(schemaBuilder.getDataOptions().getTenantId(), viewName);

            byte[] emptyColumnFamilyName = SchemaUtil.getEmptyColumnFamily(table);
            byte[]
                    emptyColumnName =
                    table.getEncodingScheme() == PTable.QualifierEncodingScheme.NON_ENCODED_QUALIFIERS ?
                            QueryConstants.EMPTY_COLUMN_BYTES :
                            table.getEncodingScheme()
                                    .encode(QueryConstants.ENCODED_EMPTY_COLUMN_NAME);

            scan.setAttribute(BaseScannerRegionObserverConstants.EMPTY_COLUMN_FAMILY_NAME,
                    emptyColumnFamilyName);
            scan.setAttribute(BaseScannerRegionObserverConstants.EMPTY_COLUMN_QUALIFIER_NAME,
                    emptyColumnName);
            scan.setAttribute(BaseScannerRegionObserverConstants.DELETE_PHOENIX_TTL_EXPIRED,
                    PDataType.TRUE_BYTES);
            scan.setAttribute(BaseScannerRegionObserverConstants.TTL,
                    Bytes.toBytes(Long.valueOf(table.getTTL())));

            PhoenixResultSet
                    rs =
                    pstmt.newResultSet(queryPlan.iterator(), queryPlan.getProjector(),
                            queryPlan.getContext());
            Assert.assertFalse("Should not have any rows", rs.next());
            Assert.assertEquals("Should have atleast one element", 1, queryPlan.getScans().size());
            Assert.assertEquals("PhoenixTTL does not match", phoenixTTL,
                    ScanUtil.getTTL(queryPlan.getScans().get(0).get(0)));
            Assert.assertFalse("Masking attribute should not be set",
                    ScanUtil.isMaskTTLExpiredRows(queryPlan.getScans().get(0).get(0)));
            Assert.assertTrue("Delete Expired attribute should be set",
                    ScanUtil.isDeleteTTLExpiredRows(queryPlan.getScans().get(0).get(0)));
        }
    }

    @Test
    public void testMajorCompactWithSimpleIndexedBaseTables() throws Exception {
        super.testMajorCompactWithSimpleIndexedBaseTables();
    }

    @Test
    public void testMajorCompactFromMultipleGlobalIndexes() throws Exception {
        super.testMajorCompactFromMultipleGlobalIndexes();
    }

    @Test
    public void testMajorCompactFromMultipleTenantIndexes() throws Exception {
        super.testMajorCompactFromMultipleTenantIndexes();
    }
    @Test
    public void testMajorCompactWithOnlyTenantView() throws Exception {
        super.testMajorCompactWithOnlyTenantView();
    }
    @Test
    public void testMajorCompactWithSaltedIndexedTenantView() throws Exception {
        super.testMajorCompactWithSaltedIndexedTenantView();
    }
    @Test
    public void testMajorCompactWithSaltedIndexedBaseTables() throws Exception {
        super.testMajorCompactWithSaltedIndexedBaseTables();
    }
    @Test
    public void testMajorCompactWithVariousViewsAndOptions() throws Exception {
        super.testMajorCompactWithVariousViewsAndOptions();
    }
    @Test
    public void testMajorCompactWhenTTLSetForSomeTenants() throws Exception {
        super.testMajorCompactWhenTTLSetForSomeTenants();
    }
    @Test
    public void testMajorCompactWithGlobalAndTenantViewHierarchy() throws Exception {
        super.testMajorCompactWithGlobalAndTenantViewHierarchy();
    }

    @Test
    public void testMajorCompactWithVariousTenantIdTypesAndRegions() throws Exception {
        super.testMajorCompactWithVariousTenantIdTypesAndRegions(PVarchar.INSTANCE);
        super.testMajorCompactWithVariousTenantIdTypesAndRegions(PInteger.INSTANCE);
        super.testMajorCompactWithVariousTenantIdTypesAndRegions(PLong.INSTANCE);
    }

    @Test
    public void testTenantViewsWIthOverlappingRowPrefixes() throws Exception {
        super.testTenantViewsWIthOverlappingRowPrefixes();
    }

}
