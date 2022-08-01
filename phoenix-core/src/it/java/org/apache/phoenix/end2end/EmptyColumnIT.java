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

import static org.apache.phoenix.util.PhoenixRuntime.TENANT_ID_ATTRIB;
import static org.apache.phoenix.query.PhoenixTestBuilder.DDLDefaults.MAX_ROWS;
import static org.apache.phoenix.query.PhoenixTestBuilder.DDLDefaults.COLUMN_TYPES;
import static org.apache.phoenix.query.PhoenixTestBuilder.DDLDefaults.GLOBAL_VIEW_COLUMNS;
import static org.apache.phoenix.query.PhoenixTestBuilder.DDLDefaults.TENANT_VIEW_COLUMNS;
import static org.apache.phoenix.query.PhoenixTestBuilder.SchemaBuilder.ConnectOptions;
import static org.apache.phoenix.query.PhoenixTestBuilder.SchemaBuilder.OtherOptions;
import static org.apache.phoenix.query.PhoenixTestBuilder.SchemaBuilder.GlobalViewOptions;
import static org.apache.phoenix.query.PhoenixTestBuilder.SchemaBuilder.GlobalViewIndexOptions;
import static org.apache.phoenix.query.PhoenixTestBuilder.SchemaBuilder.TableIndexOptions;
import static org.apache.phoenix.query.PhoenixTestBuilder.SchemaBuilder.TableOptions;
import static org.apache.phoenix.query.PhoenixTestBuilder.SchemaBuilder.TenantViewOptions;
import static org.apache.phoenix.query.PhoenixTestBuilder.SchemaBuilder.TenantViewIndexOptions;
import static org.junit.Assert.assertEquals;

import java.io.IOException;
import java.sql.DriverManager;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.List;
import java.util.Random;

import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellComparator;
import org.apache.hadoop.hbase.CellScanner;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.phoenix.query.QueryConstants;
import org.apache.phoenix.query.PhoenixTestBuilder.SchemaBuilder;
import org.apache.phoenix.query.PhoenixTestBuilder.DataSupplier;
import org.apache.phoenix.query.PhoenixTestBuilder.DataWriter;
import org.apache.phoenix.query.PhoenixTestBuilder.BasicDataWriter;
import org.apache.phoenix.schema.PTable;
import org.apache.phoenix.util.IndexUtil;
import org.apache.phoenix.util.SchemaUtil;
import org.apache.phoenix.util.TestUtil;
import org.junit.Ignore;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.apache.phoenix.thirdparty.com.google.common.collect.Lists;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Each column in HBase can have a different timestamp depending on when it gets updated.
 * The timestamp of the EMPTY_COLUMN can serve as the timestamp of the row (a row in PHOENIX)
 * and can be used to determine TTL expiration of a row in PHOENIX.
 * These tests are to validate our expectations of the EMPTY_COLUMN behavior.
 * Checks :-
 * 1. Behavior for various tables, views (global and tenant), indexes (global and local)
 * 2. Behavior under different CF's combinations.
 * 3. Behavior under different COLUMN ENCODINGS
 */
@Category(ParallelStatsDisabledTest.class)
public class EmptyColumnIT extends ParallelStatsDisabledIT {

    private static final Logger LOGGER = LoggerFactory.getLogger(EmptyColumnIT.class);
    private static int DEFAULT_NUM_ROWS = 5;

    // Scans the HBase rows directly for the empty column and asserts
    private void assertAllHBaseRowsHaveEmptyColumnCell(byte[] hbaseTableName,
            byte[] emptyColumnFamilyName, byte[] emptyColumnName, long minTimestamp,
            int expectedRows) throws IOException, SQLException {

        try (Table tbl = driver.getConnectionQueryServices(getUrl(), TestUtil.TEST_PROPERTIES)
                .getTable(hbaseTableName)) {

            Scan allRows = new Scan();
            allRows.setTimeRange(minTimestamp, minTimestamp + Integer.MAX_VALUE);
            ResultScanner scanner = tbl.getScanner(allRows);
            int numMatchingRows = getNumRowsWithEmptyColumnAndMaxTimestamp(scanner,
                    emptyColumnFamilyName, emptyColumnName);

            assertEquals(String.format("Expected rows do match for table = %s at timestamp %d",
                    Bytes.toString(hbaseTableName), minTimestamp), expectedRows, numMatchingRows);
        }
    }


    // return the number of rows that contain the empty column with the the max timestamp.
    private int getNumRowsWithEmptyColumnAndMaxTimestamp(ResultScanner scanner,
            byte[] emptyColumnFamilyName, byte[] emptyColumnName) {

        int numMatchingRows = 0;
        try {
            for (Result result = scanner.next(); result != null; result = scanner.next()) {
                boolean emptyColumnHasMaxTimestamp = true;
                if (result.containsColumn(emptyColumnFamilyName, emptyColumnName)) {
                    Cell emptyColumnCell =
                            result.getColumnLatestCell(emptyColumnFamilyName, emptyColumnName);
                    CellScanner cellScanner = result.cellScanner();
                    while (cellScanner.advance()) {
                        Cell current = cellScanner.current();
                        // if current.timestamp > emptyColumnCell.timestamp then it returns -1
                        if (CellComparator.getInstance().compareTimestamps(current, emptyColumnCell) < 0) {
                            emptyColumnHasMaxTimestamp &= false;
                        }
                    }
                    if (emptyColumnHasMaxTimestamp) {
                        numMatchingRows++;
                    }
                }
            }
        }
        catch(Exception e) {
            LOGGER.info(e.getLocalizedMessage());
        }
        return numMatchingRows;
    }

    /**
     * -----------------
     * Test methods
     * -----------------
     */

    @Ignore("fails with java.lang.ArrayIndexOutOfBoundsException: -1 , "
			+ "fails for both Table and GlobalView indexes")
    // https://issues.apache.org/jira/browse/PHOENIX-5317
    // https://issues.apache.org/jira/browse/PHOENIX-5322
    public void testWithBasicTenantViewAndTableIndex() throws Exception {

        // Define the test schema.
        OtherOptions otherOptions = new OtherOptions();
        otherOptions.setTableCFs(Lists.newArrayList(null, "A", "B"));

        TableOptions tableOptions = TableOptions.withDefaults();
        tableOptions.getTablePKColumns().add("ID");
        tableOptions.getTablePKColumnTypes().add("CHAR(15)");

        GlobalViewOptions globalViewOptions = new GlobalViewOptions();
        globalViewOptions.setGlobalViewColumns(GLOBAL_VIEW_COLUMNS);
        globalViewOptions.setGlobalViewColumnTypes(COLUMN_TYPES);
        otherOptions.setGlobalViewCFs(Lists.newArrayList(null, "A", "B"));

        final SchemaBuilder schemaBuilder = new SchemaBuilder(getUrl());
        schemaBuilder.withTableOptions(tableOptions).withGlobalViewOptions(globalViewOptions)
                .withSimpleTenantView().withOtherOptions(otherOptions).build();

        schemaBuilder.withTableIndexDefaults().build();

        // Define the test data.
        DataSupplier dataSupplier = new DataSupplier() {
            @Override public List<Object> getValues(int rowIndex) {
                Random rnd = new Random();
                String id = String.format("00A0y000%07d", rowIndex);
                String col1 = String.format("a%05d", rowIndex + rnd.nextInt(MAX_ROWS));
                String col2 = String.format("b%05d", rowIndex + rnd.nextInt(MAX_ROWS));
                String col3 = String.format("c%05d", rowIndex + rnd.nextInt(MAX_ROWS));
                String col4 = String.format("d%05d", rowIndex + rnd.nextInt(MAX_ROWS));
                String col5 = String.format("e%05d", rowIndex + rnd.nextInt(MAX_ROWS));
                String col6 = String.format("f%05d", rowIndex + rnd.nextInt(MAX_ROWS));
                return Lists.newArrayList(new Object[] { id, col1, col2, col3, col4, col5, col6 });
            }
        };

        // Create a test data writer for the above schema.
        DataWriter dataWriter = new BasicDataWriter();
        String
                tenantConnectUrl =
                getUrl() + ';' + TENANT_ID_ATTRIB + '=' + schemaBuilder.getDataOptions()
                        .getTenantId();
        try (Connection connection = DriverManager.getConnection(tenantConnectUrl)) {
            connection.setAutoCommit(true);
            dataWriter.setConnection(connection);
            dataWriter.setDataSupplier(dataSupplier);
            dataWriter.setUpsertColumns(
                    Lists.newArrayList("ID", "COL1", "COL2", "COL3", "COL4", "COL5", "COL6"));
            dataWriter.setTargetEntity(schemaBuilder.getEntityTenantViewName());

            // Write the data and run validations
            ExpectedTestResults
                    expectedTestResults =
                    new ExpectedTestResults(DEFAULT_NUM_ROWS, DEFAULT_NUM_ROWS, 0);
            upsertDataAndRunValidations(DEFAULT_NUM_ROWS, expectedTestResults, dataWriter,
                    schemaBuilder, null);
        }
    }

    @Ignore //(" Fails with java.lang.ArrayIndexOutOfBoundsException: 127")
    // https://issues.apache.org/jira/browse/PHOENIX-5317
    // https://issues.apache.org/jira/browse/PHOENIX-5322
    public void testWhenCustomTenantViewWithPKAndGlobalIndex() throws Exception {

        final SchemaBuilder schemaBuilder = new SchemaBuilder(getUrl());
        schemaBuilder.withTableDefaults().withGlobalViewDefaults().withGlobalViewIndexDefaults()
                .withTenantViewDefaults().build();

        // Define the test data.
        DataSupplier dataSupplier = new DataSupplier() {

            @Override public List<Object> getValues(int rowIndex) {
                String id = String.format("00A0y000%07d", rowIndex);
                String zid = String.format("0050z000%07d", rowIndex);

                return Lists.newArrayList(
                        new Object[] { id, zid, "a", "b", "c", "d", "e", "f", "g", "h", "i" });
            }
        };

        // Create a test data writer for the above schema.
        DataWriter dataWriter = new BasicDataWriter();
        String
                tenantConnectUrl =
                getUrl() + ';' + TENANT_ID_ATTRIB + '=' + schemaBuilder.getDataOptions()
                        .getTenantId();
        try (Connection connection = DriverManager.getConnection(tenantConnectUrl)) {
            connection.setAutoCommit(true);
            dataWriter.setConnection(connection);
            dataWriter.setDataSupplier(dataSupplier);
            dataWriter.setUpsertColumns(
                    Lists.newArrayList("ID", "ZID",
							"COL1", "COL2", "COL3",
							"COL4", "COL5", "COL6",
                            "COL7", "COL8", "COL9"));
            dataWriter.setTargetEntity(schemaBuilder.getEntityTenantViewName());

            // Write the data and run validations
            ExpectedTestResults
                    expectedTestResults =
                    new ExpectedTestResults(DEFAULT_NUM_ROWS, 0, 0);
            upsertDataAndRunValidations(DEFAULT_NUM_ROWS, expectedTestResults, dataWriter,
                    schemaBuilder, null);
        }
    }

    @Test public void testWithBasicTableAndNoAdditionalCols() throws Exception {

        // Define the test schema.
        TableOptions tableOptions = TableOptions.withDefaults();
        tableOptions.getTablePKColumns().add("ID");
        tableOptions.getTablePKColumnTypes().add("CHAR(15)");
        tableOptions.getTableColumns().clear();
        tableOptions.getTableColumnTypes().clear();

        final SchemaBuilder schemaBuilder = new SchemaBuilder(getUrl());
        schemaBuilder.withTableOptions(tableOptions).build();

        // Define the test data.
        DataSupplier dataSupplier = new DataSupplier() {

            final String
                    orgId =
                    String.format("00D0x000%s", schemaBuilder.getDataOptions().getUniqueName());
            final String kp = SchemaUtil.normalizeIdentifier(schemaBuilder.getEntityKeyPrefix());

            @Override public List<Object> getValues(int rowIndex) {
                String id = String.format("00A0y000%07d", rowIndex);
                return Lists.newArrayList(new Object[] { orgId, kp, id });
            }
        };

        // Create a test data writer for the above schema.
        DataWriter dataWriter = new BasicDataWriter();
        try (Connection connection = DriverManager.getConnection(getUrl())) {
            connection.setAutoCommit(true);
            dataWriter.setConnection(connection);
            dataWriter.setDataSupplier(dataSupplier);
            dataWriter.setUpsertColumns(Lists.newArrayList("OID", "KP", "ID"));
            dataWriter.setTargetEntity(schemaBuilder.getEntityTableName());

            // Write the data and run validations
            ExpectedTestResults
                    expectedTestResults =
                    new ExpectedTestResults(DEFAULT_NUM_ROWS, 0, 0);
            upsertDataAndRunValidations(DEFAULT_NUM_ROWS, expectedTestResults, dataWriter,
                    schemaBuilder, null);
        }
    }

    @Test public void testWithGlobalViewAndNoAdditionalCols() throws Exception {

        // Define the test schema.
        TableOptions tableOptions = TableOptions.withDefaults();
        tableOptions.getTableColumns().clear();
        tableOptions.getTableColumnTypes().clear();

        GlobalViewOptions globalViewOptions = GlobalViewOptions.withDefaults();
        globalViewOptions.getGlobalViewColumns().clear();
        globalViewOptions.getGlobalViewColumnTypes().clear();

        final SchemaBuilder schemaBuilder = new SchemaBuilder(getUrl());
        schemaBuilder.withTableOptions(tableOptions).withGlobalViewOptions(globalViewOptions)
                .build();

        // Define the test data.
        DataSupplier dataSupplier = new DataSupplier() {

            final String
                    orgId =
                    String.format("00D0x000%s", schemaBuilder.getDataOptions().getUniqueName());
            final String kp = SchemaUtil.normalizeIdentifier(schemaBuilder.getEntityKeyPrefix());

            @Override public List<Object> getValues(int rowIndex) {
                String id = String.format("00A0y000%07d", rowIndex);
                return Lists.newArrayList(new Object[] { orgId, kp, id });
            }
        };

        // Create a test data writer for the above schema.
        DataWriter dataWriter = new BasicDataWriter();
        try (Connection connection = DriverManager.getConnection(getUrl())) {
            connection.setAutoCommit(true);
            dataWriter.setConnection(connection);
            dataWriter.setDataSupplier(dataSupplier);
            dataWriter.setUpsertColumns(Lists.newArrayList("OID", "KP", "ID"));
            dataWriter.setTargetEntity(schemaBuilder.getEntityGlobalViewName());

            // Write the data and run validations
            ExpectedTestResults
                    expectedTestResults =
                    new ExpectedTestResults(DEFAULT_NUM_ROWS, 0, 0);
            upsertDataAndRunValidations(DEFAULT_NUM_ROWS, expectedTestResults, dataWriter,
                    schemaBuilder, null);
        }
    }

    @Test public void testWithCustomTenantViewAndTenantOnlyColumns() throws Exception {

        TableOptions tableOptions = TableOptions.withDefaults();
        tableOptions.getTableColumns().clear();
        tableOptions.getTableColumnTypes().clear();

        // Define the test schema.
        final SchemaBuilder schemaBuilder = new SchemaBuilder(getUrl());
        schemaBuilder.withTableOptions(tableOptions).withTenantViewDefaults().build();

        // Define the test data.
        DataSupplier dataSupplier = new DataSupplier() {

            final String
                    orgId =
                    String.format("00D0x000%s", schemaBuilder.getDataOptions().getUniqueName());
            final String kp = SchemaUtil.normalizeIdentifier(schemaBuilder.getEntityKeyPrefix());

            @Override public List<Object> getValues(int rowIndex) {
                Random rnd = new Random();
                String zid = String.format("00A0y000%07d", rowIndex);
                String col7 = String.format("g%05d", rowIndex + rnd.nextInt(MAX_ROWS));
                String col8 = String.format("h%05d", rowIndex + rnd.nextInt(MAX_ROWS));
                String col9 = String.format("i%05d", rowIndex + rnd.nextInt(MAX_ROWS));
                return Lists.newArrayList(new Object[] { orgId, kp, zid, col7, col8, col9 });
            }
        };

        // Create a test data writer for the above schema.
        DataWriter dataWriter = new BasicDataWriter();
        String
                tenantConnectUrl =
                getUrl() + ';' + TENANT_ID_ATTRIB + '=' + schemaBuilder.getDataOptions()
                        .getTenantId();
        try (Connection connection = DriverManager.getConnection(tenantConnectUrl)) {
            connection.setAutoCommit(true);
            dataWriter.setConnection(connection);
            dataWriter.setDataSupplier(dataSupplier);
            dataWriter.setUpsertColumns(
                    Lists.newArrayList("OID", "KP", "ZID", "COL7", "COL8", "COL9"));
            dataWriter.setTargetEntity(schemaBuilder.getEntityTenantViewName());

            // dataSupplier.upsertValues column positions to be used for partial updates.
            List<Integer> columnsForPartialUpdates = Lists.newArrayList(0, 1, 2, 3, 5);
            // Write the data and run validations
            ExpectedTestResults
                    expectedTestResults =
                    new ExpectedTestResults(DEFAULT_NUM_ROWS, 0, 0);
            upsertDataAndRunValidations(DEFAULT_NUM_ROWS, expectedTestResults, dataWriter,
                    schemaBuilder, columnsForPartialUpdates);
        }
    }

    /**
     * This test uses a simple table and NO indexes and
     * run with different combinations of table properties and column family options.
     */
    @Test public void testWhenTableWithNoIndexAndVariousOptions() throws Exception {

        for (String additionalProps : Lists
                .newArrayList("COLUMN_ENCODED_BYTES=0", "DEFAULT_COLUMN_FAMILY='Z'")) {

            StringBuilder withTableProps = new StringBuilder();
            withTableProps.append("MULTI_TENANT=true,").append(additionalProps);

            for (OtherOptions options : getTableColumnFamilyOptions()) {

                // Define the test schema.
                TableOptions tableOptions = TableOptions.withDefaults();
                tableOptions.getTablePKColumns().add("ID");
                tableOptions.getTablePKColumnTypes().add("CHAR(15)");
                tableOptions.setTableProps(withTableProps.toString());

                final SchemaBuilder schemaBuilder = new SchemaBuilder(getUrl());
                schemaBuilder.withTableOptions(tableOptions).withOtherOptions(options).build();

                // Define the test data.
                DataSupplier dataSupplier = new DataSupplier() {

                    final String
                            orgId =
                            String.format("00D0x000%s",
                                    schemaBuilder.getDataOptions().getUniqueName());
                    final String
                            kp =
                            SchemaUtil.normalizeIdentifier(schemaBuilder.getEntityKeyPrefix());

                    @Override public List<Object> getValues(int rowIndex) {
                        Random rnd = new Random();
                        String id = String.format("00A0y000%07d", rowIndex);
                        String col1 = String.format("a%05d", rowIndex + rnd.nextInt(MAX_ROWS));
                        String col2 = String.format("b%05d", rowIndex + rnd.nextInt(MAX_ROWS));
                        String col3 = String.format("c%05d", rowIndex + rnd.nextInt(MAX_ROWS));
                        return Lists.newArrayList(new Object[] { orgId, kp, id, col1, col2, col3 });
                    }
                };

                // Create a test data writer for the above schema.
                DataWriter dataWriter = new BasicDataWriter();
                try (Connection connection = DriverManager.getConnection(getUrl())) {
                    connection.setAutoCommit(true);
                    dataWriter.setConnection(connection);
                    dataWriter.setDataSupplier(dataSupplier);
                    dataWriter.setUpsertColumns(
                            Lists.newArrayList("OID", "KP", "ID", "COL1", "COL2", "COL3"));
                    dataWriter.setTargetEntity(schemaBuilder.getEntityTableName());

                    // dataSupplier.upsertValues column positions to be used for partial updates.
                    List<Integer> columnsForPartialUpdates = Lists.newArrayList(0, 1, 2, 3, 5);
                    // Write the data and run validations
                    ExpectedTestResults
                            expectedTestResults =
                            new ExpectedTestResults(DEFAULT_NUM_ROWS, 0, 0);
                    upsertDataAndRunValidations(DEFAULT_NUM_ROWS, expectedTestResults, dataWriter,
                            schemaBuilder, columnsForPartialUpdates);
                }
            }
        }
    }

    /**
     * This test uses a simple table and index and
     * runs with different combinations of
	 * table properties, index types (local or global) and column family options.
     */
    @Test public void testWhenTableWithIndexAndVariousOptions() throws Exception {

        // Run for different combinations of
		// table properties, index types (local or global) and column family options.
        for (String additionalProps : Lists
                .newArrayList("COLUMN_ENCODED_BYTES=0", "DEFAULT_COLUMN_FAMILY='Z'")) {

            StringBuilder withTableProps = new StringBuilder();
            withTableProps.append("MULTI_TENANT=true,").append(additionalProps);

            for (boolean isTableIndexLocal : Lists.newArrayList(true, false)) {
                for (OtherOptions options : getTableColumnFamilyOptions()) {

                    // Define the test table schema.
                    TableOptions tableOptions = TableOptions.withDefaults();
                    tableOptions.getTablePKColumns().add("ID");
                    tableOptions.getTablePKColumnTypes().add("CHAR(15)");
                    tableOptions.setTableProps(withTableProps.toString());

                    // Define the index on the test table.
                    TableIndexOptions tableIndexOptions = TableIndexOptions.withDefaults();
                    tableIndexOptions.setLocal(isTableIndexLocal);

                    // Build the schema with the above options
                    final SchemaBuilder schemaBuilder = new SchemaBuilder(getUrl());
                    schemaBuilder.withTableOptions(tableOptions)
                            .withTableIndexOptions(tableIndexOptions).withOtherOptions(options)
                            .build();

                    // Define the test data provider.
                    DataSupplier dataSupplier = new DataSupplier() {

                        final String
                                orgId =
                                String.format("00D0x000%s",
                                        schemaBuilder.getDataOptions().getUniqueName());
                        final String
                                kp =
                                SchemaUtil.normalizeIdentifier(schemaBuilder.getEntityKeyPrefix());

                        @Override public List<Object> getValues(int rowIndex) {
                            Random rnd = new Random();
                            String id = String.format("00A0y000%07d", rowIndex);
                            String col1 = String.format("a%05d", rowIndex + rnd.nextInt(MAX_ROWS));
                            String col2 = String.format("b%05d", rowIndex + rnd.nextInt(MAX_ROWS));
                            String col3 = String.format("c%05d", rowIndex + rnd.nextInt(MAX_ROWS));
                            return Lists
                                    .newArrayList(new Object[] { orgId, kp, id, col1, col2, col3 });
                        }
                    };

                    // Create a test data writer for the above schema.
                    DataWriter dataWriter = new BasicDataWriter();
                    try (Connection connection = DriverManager.getConnection(getUrl())) {
                        connection.setAutoCommit(true);
                        dataWriter.setConnection(connection);
                        dataWriter.setDataSupplier(dataSupplier);
                        dataWriter.setUpsertColumns(
                                Lists.newArrayList("OID", "KP", "ID", "COL1", "COL2", "COL3"));
                        dataWriter.setTargetEntity(schemaBuilder.getEntityTableName());

                        // dataSupplier.upsertValues column positions to be used for partial updates.
                        List<Integer> columnsForPartialUpdates = Lists.newArrayList(0, 1, 2, 3, 5);
                        // COL3 is the include column for the table index in this schema => index pos of 2
                        // and there are no global and view indexes.
                        List<Integer>
                                includeColumnPositionOfIndexes =
                                Lists.newArrayList(2, null, null);

                        /**
                         * When table indexes are local i.e index rows are co-located.
                         * AND
                         * When there are more than one index and
						 * 		the CFs of the include columns match.
                         * Then the # of index rows in the table (when local) and
						 * 		in the index table (when global)
                         * is => # of rows * # of indexes
                         *
                         * But in this schema there is only one index =>
                         * # of index rows = # of upserted rows.
                         */

                        // Write the data and run validations
                        ExpectedTestResults
                                expectedTestResults =
                                new ExpectedTestResults(DEFAULT_NUM_ROWS, DEFAULT_NUM_ROWS, 0);
                        upsertDataAndRunValidations(DEFAULT_NUM_ROWS, expectedTestResults,
                                dataWriter, schemaBuilder, columnsForPartialUpdates,
                                includeColumnPositionOfIndexes);
                    }
                }
            }
        }
    }

    /**
     * This test uses a tenant view hierarchy => table -> globalView -> tenantView
     * Run with different combinations of
	 * table properties, index types (local or global) and column family options.
     */
    @Test public void testWhenCustomTenantViewWithNoIndexAndVariousOptions() throws Exception {
        for (String additionalProps : Lists
                .newArrayList("COLUMN_ENCODED_BYTES=0", "DEFAULT_COLUMN_FAMILY='Z'")) {

            StringBuilder withTableProps = new StringBuilder();
            withTableProps.append("MULTI_TENANT=true,").append(additionalProps);

            for (OtherOptions options : getTableAndGlobalAndTenantColumnFamilyOptions()) {

                // Define the test schema
                TableOptions tableOptions = TableOptions.withDefaults();
                tableOptions.setTableProps(withTableProps.toString());

                TenantViewOptions tenantViewOptions = new TenantViewOptions();
                tenantViewOptions.setTenantViewColumns(Lists.newArrayList(TENANT_VIEW_COLUMNS));
                tenantViewOptions.setTenantViewColumnTypes(Lists.newArrayList(COLUMN_TYPES));

                final SchemaBuilder schemaBuilder = new SchemaBuilder(getUrl());
                schemaBuilder.withTableOptions(tableOptions).withGlobalViewDefaults()
                        .withTenantViewOptions(tenantViewOptions).withOtherOptions(options).build();

                // Define the test data.
                DataSupplier dataSupplier = new DataSupplier() {
                    @Override public List<Object> getValues(int rowIndex) {
                        Random rnd = new Random();
                        String id = String.format("00A0y000%07d", rowIndex);
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
                                new Object[] { id, col1, col2, col3, col4, col5, col6, col7, col8,
                                        col9 });
                    }
                };

                // Create a test data writer for the above schema.
                DataWriter dataWriter = new BasicDataWriter();
                String
                        tenantConnectUrl =
                        getUrl() + ';' + TENANT_ID_ATTRIB + '=' + schemaBuilder.getDataOptions()
                                .getTenantId();
                try (Connection connection = DriverManager.getConnection(tenantConnectUrl)) {
                    connection.setAutoCommit(true);
                    dataWriter.setConnection(connection);
                    dataWriter.setDataSupplier(dataSupplier);
                    dataWriter.setUpsertColumns(
                            Lists.newArrayList("ID",
									"COL1", "COL2", "COL3",
									"COL4", "COL5", "COL6",
                                    "COL7", "COL8", "COL9"));
                    dataWriter.setTargetEntity(schemaBuilder.getEntityTenantViewName());

                    // dataSupplier.upsertValues column positions to be used for partial updates.
                    List<Integer> columnsForPartialUpdates = Lists.newArrayList(0, 7, 9);
                    // Write the data and run validations
                    ExpectedTestResults
                            expectedTestResults =
                            new ExpectedTestResults(DEFAULT_NUM_ROWS, 0, 0);
                    upsertDataAndRunValidations(DEFAULT_NUM_ROWS, expectedTestResults, dataWriter,
                            schemaBuilder, columnsForPartialUpdates);
                }
            }
        }
    }

    @Ignore("Fails with - "
			+ "Expected rows do match for table = xxxx at timestamp xxx expected:<5> but was:<10>")
    // https://issues.apache.org/jira/browse/PHOENIX-5476
    public void testWhenCustomTenantViewWithIndexAndVariousOptions() throws Exception {
        for (String additionalProps : Lists
                .newArrayList("COLUMN_ENCODED_BYTES=0", "DEFAULT_COLUMN_FAMILY='Z'")) {

            StringBuilder withTableProps = new StringBuilder();
            withTableProps.append("MULTI_TENANT=true,").append(additionalProps);

            for (boolean isGlobalViewLocal : Lists.newArrayList(true, false)) {
                for (boolean isTenantViewLocal : Lists.newArrayList(true, false)) {
                    for (OtherOptions options : getTableAndGlobalAndTenantColumnFamilyOptions()) {

                        // Define the test schema
                        TableOptions tableOptions = TableOptions.withDefaults();
                        tableOptions.setTableProps(withTableProps.toString());

                        GlobalViewIndexOptions
                                globalViewIndexOptions =
                                GlobalViewIndexOptions.withDefaults();
                        globalViewIndexOptions.setLocal(isGlobalViewLocal);

                        TenantViewOptions tenantViewOptions = new TenantViewOptions();
                        tenantViewOptions
                                .setTenantViewColumns(Lists.newArrayList(TENANT_VIEW_COLUMNS));
                        tenantViewOptions
                                .setTenantViewColumnTypes(Lists.newArrayList(COLUMN_TYPES));

                        TenantViewIndexOptions
                                tenantViewIndexOptions =
                                TenantViewIndexOptions.withDefaults();
                        tenantViewIndexOptions.setLocal(isTenantViewLocal);

                        final SchemaBuilder schemaBuilder = new SchemaBuilder(getUrl());
                        schemaBuilder.withTableOptions(tableOptions).withGlobalViewDefaults()
                                .withGlobalViewIndexOptions(globalViewIndexOptions)
                                .withTenantViewOptions(tenantViewOptions)
                                .withTenantViewIndexOptions(tenantViewIndexOptions)
                                .withOtherOptions(options).build();

                        // Define the test data.
                        DataSupplier dataSupplier = new DataSupplier() {

                            @Override public List<Object> getValues(int rowIndex) {
                                Random rnd = new Random();
                                String id = String.format("00A0y000%07d", rowIndex);
                                String
                                        col1 =
                                        String.format("a%05d", rowIndex + rnd.nextInt(MAX_ROWS));
                                String
                                        col2 =
                                        String.format("b%05d", rowIndex + rnd.nextInt(MAX_ROWS));
                                String
                                        col3 =
                                        String.format("c%05d", rowIndex + rnd.nextInt(MAX_ROWS));
                                String
                                        col4 =
                                        String.format("d%05d", rowIndex + rnd.nextInt(MAX_ROWS));
                                String
                                        col5 =
                                        String.format("e%05d", rowIndex + rnd.nextInt(MAX_ROWS));
                                String
                                        col6 =
                                        String.format("f%05d", rowIndex + rnd.nextInt(MAX_ROWS));
                                String
                                        col7 =
                                        String.format("g%05d", rowIndex + rnd.nextInt(MAX_ROWS));
                                String
                                        col8 =
                                        String.format("h%05d", rowIndex + rnd.nextInt(MAX_ROWS));
                                String
                                        col9 =
                                        String.format("i%05d", rowIndex + rnd.nextInt(MAX_ROWS));

                                return Lists.newArrayList(
                                        new Object[] { id, col1, col2, col3, col4, col5, col6, col7,
                                                col8, col9 });
                            }
                        };

                        // Create a test data writer for the above schema.
                        DataWriter dataWriter = new BasicDataWriter();
                        String
                                tenantConnectUrl =
                                getUrl() + ';' + TENANT_ID_ATTRIB + '=' + schemaBuilder
                                        .getDataOptions().getTenantId();
                        try (Connection connection = DriverManager
                                .getConnection(tenantConnectUrl)) {
                            connection.setAutoCommit(true);
                            dataWriter.setConnection(connection);
                            dataWriter.setDataSupplier(dataSupplier);
                            dataWriter.setUpsertColumns(
                                    Lists.newArrayList("ID", "COL1", "COL2", "COL3", "COL4", "COL5",
                                            "COL6", "COL7", "COL8", "COL9"));
                            dataWriter.setTargetEntity(schemaBuilder.getEntityTenantViewName());

                            // upsertValues column positions to be used for partial updates.
                            List<Integer> columnsForPartialUpdates = Lists.newArrayList(0, 7, 9);
                            // No index for table.
                            // COL6 is the include column for the global view index => index pos of 2
                            // COL7 is the include column for the tenant view index => index pos of 0
                            List<Integer>
                                    includeColumnPositionOfIndexes =
                                    Lists.newArrayList(null, 2, 0);
                            // Write the data and run validations
                            try {
                                LOGGER.debug(String.format("### BEGIN %s",
                                        schemaBuilder.getOtherOptions().getTestName()));
                                /**
                                 * WHEN global and tenant view indexes are
								 * either both local or both global i.e index rows are co-located.
                                 * AND
                                 * WHEN the CFs of the include columns match.
                                 * THEN the # of index rows in the table (when local) and
								 * 		in the index table (when global)
                                 * # of index rows => # of upserted rows * # of indexes
                                 * ELSE
                                 * # of index rows => # of upserted rows
                                 */
                                ExpectedTestResults
                                        expectedTestResults =
                                        new ExpectedTestResults(DEFAULT_NUM_ROWS, 0,
                                                DEFAULT_NUM_ROWS);
                                boolean colocated = !(isGlobalViewLocal ^ isTenantViewLocal);
                                boolean
                                        cfsAreNull =
                                        (options.getGlobalViewCFs().get(2) == null
                                                && options.getGlobalViewCFs().get(2) == options
                                                .getTenantViewCFs().get(0));
                                boolean
                                        cfsAreNotNullButEqual =
                                        !(cfsAreNull) && (options.getGlobalViewCFs().get(2)
                                                .equalsIgnoreCase(
                                                        options.getTenantViewCFs().get(0)));
                                if (colocated && (cfsAreNull || cfsAreNotNullButEqual)) {
                                    expectedTestResults =
                                            new ExpectedTestResults(DEFAULT_NUM_ROWS, 0,
                                                    DEFAULT_NUM_ROWS * 2);
                                }
                                upsertDataAndRunValidations(DEFAULT_NUM_ROWS, expectedTestResults,
                                        dataWriter, schemaBuilder, columnsForPartialUpdates,
                                        includeColumnPositionOfIndexes);
                                LOGGER.debug(String.format(
                                        "### Case => [GlobalView (local) = %b, "
												+ "TenantView (local) = %b] : %s",
                                        isGlobalViewLocal, isTenantViewLocal, "Passed"));
                            } catch (AssertionError ae) {
                                LOGGER.debug(String.format(
                                        "### Case => [GlobalView (local) = %b, "
												+ "TenantView (local) = %b] : %s",
                                        isGlobalViewLocal, isTenantViewLocal, ae.getMessage()));
                            } finally {
                                LOGGER.debug(String.format("### END %s",
                                        schemaBuilder.getOtherOptions().getTestName()));
                            }
                        }
                    }
                }
            }
        }
    }

    private void upsertDataAndRunValidations(int numRowsToUpsert,
            ExpectedTestResults expectedTestResults, DataWriter dataWriter,
            SchemaBuilder schemaBuilder, List<Integer> overriddenColumnsPositions)
            throws Exception {

        //Insert for the first time and validate them.
        validateEmptyColumnsAreUpdated(upsertData(dataWriter, numRowsToUpsert), expectedTestResults,
                schemaBuilder, Lists.newArrayList(new Integer[] {}));
        // Update the above rows and validate the same.
        validateEmptyColumnsAreUpdated(upsertData(dataWriter, numRowsToUpsert), expectedTestResults,
                schemaBuilder, Lists.newArrayList(new Integer[] {}));

        if (overriddenColumnsPositions != null && overriddenColumnsPositions.size() > 0) {
            dataWriter.setColumnPositionsToUpdate(overriddenColumnsPositions);
        }
        // Upsert and validate the partially updated rows.
        validateEmptyColumnsAreUpdated(upsertData(dataWriter, numRowsToUpsert), expectedTestResults,
                schemaBuilder, Lists.newArrayList(new Integer[] {}));
    }

    private void upsertDataAndRunValidations(int numRowsToUpsert,
            ExpectedTestResults expectedTestResults, DataWriter dataWriter,
            SchemaBuilder schemaBuilder, List<Integer> overriddenColumnsPositions,
            List<Integer> indexedCFPositions) throws Exception {

        //Insert for the first time and validate them.
        validateEmptyColumnsAreUpdated(upsertData(dataWriter, numRowsToUpsert), expectedTestResults,
                schemaBuilder, indexedCFPositions);
        // Update the above rows and validate the same.
        validateEmptyColumnsAreUpdated(upsertData(dataWriter, numRowsToUpsert), expectedTestResults,
                schemaBuilder, indexedCFPositions);

        if (overriddenColumnsPositions != null && overriddenColumnsPositions.size() > 0) {
            dataWriter.setColumnPositionsToUpdate(overriddenColumnsPositions);
        }
        // Upsert and validate the partially updated rows.
        validateEmptyColumnsAreUpdated(upsertData(dataWriter, numRowsToUpsert), expectedTestResults,
                schemaBuilder, indexedCFPositions);
    }

    private long upsertData(DataWriter dataWriter, int numRowsToUpsert) throws Exception {
        // Upsert rows
        long earliestTimestamp = System.currentTimeMillis();
        for (int i = 0; i < numRowsToUpsert; i++) {
            dataWriter.upsertRow(i);
        }
        return earliestTimestamp;
    }

    private void validateEmptyColumnsAreUpdated(long earliestTimestamp,
            ExpectedTestResults expectedTestResults, SchemaBuilder schemaBuilder,
            List<Integer> indexedCFPositions) throws IOException, SQLException {

        // Base table's empty CF and column name
        PTable table = schemaBuilder.getBaseTable();
        byte[] emptyColumnFamilyName = SchemaUtil.getEmptyColumnFamily(table);
        byte[]
                emptyColumnName =
                table.getEncodingScheme() == PTable.QualifierEncodingScheme.NON_ENCODED_QUALIFIERS ?
                        QueryConstants.EMPTY_COLUMN_BYTES :
                        table.getEncodingScheme().encode(QueryConstants.ENCODED_EMPTY_COLUMN_NAME);

        byte[]
                hbaseBaseTableName =
                SchemaUtil.getTableNameAsBytes(table.getSchemaName().getString(),
                        table.getTableName().getString());
        byte[] hbaseIndexTableName = Bytes.toBytes("");
        byte[] indexColumnFamilyName = Bytes.toBytes("");

        byte[] hbaseGlobalViewIndexTableName = Bytes.toBytes("");
        byte[] globalViewIndexCFName = Bytes.toBytes("");

        byte[] hbaseTenantViewIndexTableName = Bytes.toBytes("");
        byte[] tenantViewIndexCFName = Bytes.toBytes("");

        boolean assertOnIndexTable = false;
        boolean assertOnGlobalIndexTable = false;
        boolean assertOnTenantIndexTable = false;

        // Find the index CF name when table index exists.
        if (schemaBuilder.isTableIndexEnabled() && schemaBuilder.isTableIndexCreated()
                && indexedCFPositions.size() > 0) {
            String tableIndexName = String.format("IDX_%s", table.getTableName().getString());
            // The table holding the index data - depends on whether index is local or global.
            hbaseIndexTableName =
                    schemaBuilder.getTableIndexOptions().isLocal() ?
                            hbaseBaseTableName :
                            SchemaUtil.getTableNameAsBytes(table.getSchemaName().getString(),
                                    tableIndexName);
            String
                    testDataColumnFamilyName =
                    schemaBuilder.getOtherOptions().getTableCFs().get(indexedCFPositions.get(0));
            String
                    dataColumnFamilyName =
                    testDataColumnFamilyName == null ?
                            Bytes.toString(emptyColumnFamilyName) :
                            testDataColumnFamilyName;
            indexColumnFamilyName =
                    Bytes.toBytes(schemaBuilder.getTableIndexOptions().isLocal() ?
                            IndexUtil.getLocalIndexColumnFamily(dataColumnFamilyName) :
                            dataColumnFamilyName);
            LOGGER.debug(String.format("### Table Index CF name : %s, Table Index table name : %s",
                    Bytes.toString(indexColumnFamilyName), Bytes.toString(hbaseIndexTableName)));
            assertOnIndexTable = true;
        }

        // Find the index CF name when global view index exists.
        if (schemaBuilder.isGlobalViewIndexEnabled() && schemaBuilder.isGlobalViewIndexCreated()
                && indexedCFPositions.size() > 0) {
            String
                    viewIndexSchemaName =
                    String.format("_IDX_%s", table.getSchemaName().getString());
            hbaseGlobalViewIndexTableName =
                    schemaBuilder.getGlobalViewIndexOptions().isLocal() ?
                            hbaseBaseTableName :
                            SchemaUtil.getTableNameAsBytes(viewIndexSchemaName,
                                    table.getTableName().getString());

            String
                    testDataColumnFamilyName =
                    schemaBuilder.getOtherOptions().getGlobalViewCFs()
                            .get(indexedCFPositions.get(1));
            String
                    dataColumnFamilyName =
                    testDataColumnFamilyName == null ?
                            Bytes.toString(emptyColumnFamilyName) :
                            testDataColumnFamilyName;
            globalViewIndexCFName =
                    Bytes.toBytes(schemaBuilder.getGlobalViewIndexOptions().isLocal() ?
                            IndexUtil.getLocalIndexColumnFamily(dataColumnFamilyName) :
                            dataColumnFamilyName);
            LOGGER.info(String.format(
                    "### Global View Index CF name : %s, Global View Index table name : %s",
                    Bytes.toString(indexColumnFamilyName),
                    Bytes.toString(hbaseGlobalViewIndexTableName)));
            assertOnGlobalIndexTable = true;
        }

        // Find the index CF name when tenant view index exists.
        if (schemaBuilder.isTenantViewIndexEnabled() && schemaBuilder.isTenantViewIndexCreated()
                && indexedCFPositions.size() > 0) {
            String
                    viewIndexSchemaName =
                    String.format("_IDX_%s", table.getSchemaName().getString());
            hbaseTenantViewIndexTableName =
                    schemaBuilder.getTenantViewIndexOptions().isLocal() ?
                            hbaseBaseTableName :
                            SchemaUtil.getTableNameAsBytes(viewIndexSchemaName,
                                    table.getTableName().getString());

            String
                    testDataColumnFamilyName =
                    schemaBuilder.getOtherOptions().getTenantViewCFs()
                            .get(indexedCFPositions.get(2));
            String
                    dataColumnFamilyName =
                    testDataColumnFamilyName == null ?
                            Bytes.toString(emptyColumnFamilyName) :
                            testDataColumnFamilyName;
            tenantViewIndexCFName =
                    Bytes.toBytes(schemaBuilder.getTenantViewIndexOptions().isLocal() ?
                            IndexUtil.getLocalIndexColumnFamily(dataColumnFamilyName) :
                            dataColumnFamilyName);
            LOGGER.info(String.format("### Tenant Index CF name : %s, Tenant Index table name : %s",
                    Bytes.toString(indexColumnFamilyName),
                    Bytes.toString(hbaseTenantViewIndexTableName)));
            assertOnTenantIndexTable = true;
        }

        // Assert on base table rows
        assertAllHBaseRowsHaveEmptyColumnCell(hbaseBaseTableName, emptyColumnFamilyName,
                emptyColumnName, earliestTimestamp, expectedTestResults.numTableRowsExpected);
        // Assert on index table rows
        if (assertOnIndexTable) {
            assertAllHBaseRowsHaveEmptyColumnCell(hbaseIndexTableName, indexColumnFamilyName,
                    emptyColumnName, earliestTimestamp,
                    expectedTestResults.numTableIndexRowsExpected);
        }
        // Assert on global view index table rows
        if (assertOnGlobalIndexTable) {
            assertAllHBaseRowsHaveEmptyColumnCell(hbaseGlobalViewIndexTableName,
                    globalViewIndexCFName, emptyColumnName, earliestTimestamp,
                    expectedTestResults.numViewIndexRowsExpected);
        }
        // Assert on tenant view index table rows
        if (assertOnTenantIndexTable) {
            assertAllHBaseRowsHaveEmptyColumnCell(hbaseTenantViewIndexTableName,
                    tenantViewIndexCFName, emptyColumnName, earliestTimestamp,
                    expectedTestResults.numViewIndexRowsExpected);
        }
    }

    private List<ConnectOptions> getConnectOptions() {
        List<ConnectOptions> testCases = Lists.newArrayList();

        ConnectOptions defaultConnectOptions = new ConnectOptions();
        testCases.add(defaultConnectOptions);

        ConnectOptions globalOnlyConnectOptions = new ConnectOptions();
        globalOnlyConnectOptions.setUseGlobalConnectionOnly(true);
        testCases.add(globalOnlyConnectOptions);

        ConnectOptions tenantOnlyConnectOptions = new ConnectOptions();
        tenantOnlyConnectOptions.setUseTenantConnectionForGlobalView(true);
        testCases.add(tenantOnlyConnectOptions);

        return testCases;
    }

    private List<OtherOptions> getTableColumnFamilyOptions() {

        List<OtherOptions> testCases = Lists.newArrayList();

        OtherOptions testCaseWhenAllCFMatch = new OtherOptions();
        testCaseWhenAllCFMatch.setTestName("testCaseWhenAllCFMatch");
        testCaseWhenAllCFMatch.setTableCFs(Lists.newArrayList("A", "A", "A"));
        testCases.add(testCaseWhenAllCFMatch);

        OtherOptions testCaseWhenManyCFs = new OtherOptions();
        testCaseWhenManyCFs.setTestName("testCaseWhenManyCFs");
        testCaseWhenManyCFs.setTableCFs(Lists.newArrayList(null, "A", "B"));
        testCases.add(testCaseWhenManyCFs);

        OtherOptions testCaseWhenAllCFsAreSpecified = new OtherOptions();
        testCaseWhenAllCFsAreSpecified.setTestName("testCaseWhenAllCFsAreSpecified");
        testCaseWhenAllCFsAreSpecified.setTableCFs(Lists.newArrayList("A", "A", "B"));
        testCases.add(testCaseWhenAllCFsAreSpecified);

        OtherOptions testCaseWhenDefaultCFs = new OtherOptions();
        testCaseWhenDefaultCFs.setTestName("testCaseWhenDefaultCFs");
        testCaseWhenDefaultCFs.setTableCFs(Lists.newArrayList((String) null, null, null));
        testCases.add(testCaseWhenDefaultCFs);
        return testCases;

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

        OtherOptions testCaseWhenNoDefaultCF = new OtherOptions();
        testCaseWhenNoDefaultCF.setTestName("testCaseWhenNoDefaultCF");
        testCaseWhenNoDefaultCF.setTableCFs(Lists.newArrayList("A", "A", "B"));
        testCaseWhenNoDefaultCF.setGlobalViewCFs(Lists.newArrayList("A", "A", "B"));
        testCaseWhenNoDefaultCF.setTenantViewCFs(Lists.newArrayList("A", "A", "B"));
        testCases.add(testCaseWhenNoDefaultCF);

        OtherOptions testCaseWhenNoDefaultCFAndGlobalAndTenantCFsAreDiff = new OtherOptions();
        testCaseWhenNoDefaultCFAndGlobalAndTenantCFsAreDiff
                .setTestName("testCaseWhenNoDefaultCFAndGlobalAndTenantCFsAreDiff");
        testCaseWhenNoDefaultCFAndGlobalAndTenantCFsAreDiff
                .setTableCFs(Lists.newArrayList("A", "A", "B"));
        testCaseWhenNoDefaultCFAndGlobalAndTenantCFsAreDiff
                .setGlobalViewCFs(Lists.newArrayList("B", "B", "B"));
        testCaseWhenNoDefaultCFAndGlobalAndTenantCFsAreDiff
                .setTenantViewCFs(Lists.newArrayList("A", "A", "A"));
        testCases.add(testCaseWhenNoDefaultCFAndGlobalAndTenantCFsAreDiff);

		/*
		// This fails even with "phoenix.view.allowNewColumnFamily" == true
		OtherOptions testCaseWhenTableAndViewCFAreDisjoint = new OtherOptions();
		testCaseWhenTableAndViewCFAreDisjoint.tableCFs = Lists.newArrayList("A", "A", "B");
		testCaseWhenTableAndViewCFAreDisjoint.globalViewCFs = Lists.newArrayList("A", "C", "D");
		testCaseWhenTableAndViewCFAreDisjoint.tenantViewCFs = Lists.newArrayList("A", "E", "F");
		testCases.add(testCaseWhenTableAndViewCFAreDisjoint);
		*/

        return testCases;
    }

    private static class ExpectedTestResults {
        int numTableRowsExpected;
        int numTableIndexRowsExpected;
        int numViewIndexRowsExpected;

        public ExpectedTestResults(int numTableRowsExpected, int numTableIndexRowsExpected,
                int numViewIndexRowsExpected) {
            this.numTableRowsExpected = numTableRowsExpected;
            this.numTableIndexRowsExpected = numTableIndexRowsExpected;
            this.numViewIndexRowsExpected = numViewIndexRowsExpected;
        }
    }

}
