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

import org.apache.phoenix.thirdparty.com.google.common.collect.Maps;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.mapreduce.Counter;
import org.apache.hadoop.mapreduce.CounterGroup;
import org.apache.phoenix.compat.hbase.HbaseCompatCapabilities;
import org.apache.phoenix.compat.hbase.coprocessor.CompatBaseScannerRegionObserver;
import org.apache.phoenix.coprocessor.GlobalIndexRegionScanner;
import org.apache.phoenix.coprocessor.IndexRepairRegionScanner;
import org.apache.phoenix.hbase.index.IndexRegionObserver;
import org.apache.phoenix.index.IndexMaintainer;
import org.apache.phoenix.jdbc.PhoenixConnection;
import org.apache.phoenix.mapreduce.index.IndexTool;
import org.apache.phoenix.mapreduce.index.IndexTool.IndexDisableLoggingType;
import org.apache.phoenix.mapreduce.index.IndexTool.IndexVerifyType;
import org.apache.phoenix.mapreduce.index.IndexVerificationOutputRepository;
import org.apache.phoenix.mapreduce.index.IndexVerificationOutputRow;
import org.apache.phoenix.mapreduce.index.IndexVerificationResultRepository;
import org.apache.phoenix.query.QueryServices;
import org.apache.phoenix.schema.PTable;
import org.apache.phoenix.util.EnvironmentEdgeManager;
import org.apache.phoenix.util.IndexScrutiny;
import org.apache.phoenix.util.ManualEnvironmentEdge;
import org.apache.phoenix.util.PhoenixRuntime;
import org.apache.phoenix.util.PropertiesUtil;
import org.apache.phoenix.util.ReadOnlyProps;
import org.apache.phoenix.util.SchemaUtil;
import org.apache.phoenix.util.TestUtil;
import org.junit.After;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import static org.apache.phoenix.mapreduce.index.IndexVerificationResultRepository.RESULT_TABLE_NAME;
import static org.apache.phoenix.mapreduce.index.PhoenixIndexToolJobCounters.AFTER_REPAIR_EXTRA_UNVERIFIED_INDEX_ROW_COUNT;
import static org.apache.phoenix.mapreduce.index.PhoenixIndexToolJobCounters.AFTER_REPAIR_EXTRA_VERIFIED_INDEX_ROW_COUNT;
import static org.apache.phoenix.mapreduce.index.PhoenixIndexToolJobCounters.BEFORE_REBUILD_BEYOND_MAXLOOKBACK_INVALID_INDEX_ROW_COUNT;
import static org.apache.phoenix.mapreduce.index.PhoenixIndexToolJobCounters.BEFORE_REBUILD_INVALID_INDEX_ROW_COUNT;
import static org.apache.phoenix.mapreduce.index.PhoenixIndexToolJobCounters.BEFORE_REBUILD_UNVERIFIED_INDEX_ROW_COUNT;
import static org.apache.phoenix.mapreduce.index.PhoenixIndexToolJobCounters.BEFORE_REPAIR_EXTRA_UNVERIFIED_INDEX_ROW_COUNT;
import static org.apache.phoenix.mapreduce.index.PhoenixIndexToolJobCounters.BEFORE_REPAIR_EXTRA_VERIFIED_INDEX_ROW_COUNT;
import static org.apache.phoenix.mapreduce.index.PhoenixIndexToolJobCounters.SCANNED_DATA_ROW_COUNT;
import static org.apache.phoenix.query.QueryConstants.VERIFIED_BYTES;
import static org.apache.phoenix.util.TestUtil.TEST_PROPERTIES;
import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.junit.Assume.assumeTrue;

@Category(NeedsOwnMiniClusterTest.class)
@RunWith(Parameterized.class)
public class IndexRepairRegionScannerIT extends ParallelStatsDisabledIT {

    private static final Logger LOGGER = LoggerFactory.getLogger(IndexRepairRegionScannerIT.class);
    private final String tableDDLOptions;
    private final String indexDDLOptions;
    private boolean mutable;
    @Rule
    public ExpectedException exceptionRule = ExpectedException.none();

    public IndexRepairRegionScannerIT(boolean mutable, boolean singleCellIndex) {
        StringBuilder optionBuilder = new StringBuilder();
        StringBuilder indexOptionBuilder = new StringBuilder();
        this.mutable = mutable;
        if (!mutable) {
            optionBuilder.append(" IMMUTABLE_ROWS=true ");
        }
        if (singleCellIndex) {
            if (!(optionBuilder.length() == 0)) {
                optionBuilder.append(",");
            }
            optionBuilder.append(" IMMUTABLE_STORAGE_SCHEME=ONE_CELL_PER_COLUMN, COLUMN_ENCODED_BYTES=0 ");
            indexOptionBuilder.append(" IMMUTABLE_STORAGE_SCHEME=SINGLE_CELL_ARRAY_WITH_OFFSETS,COLUMN_ENCODED_BYTES=2");
        }
        optionBuilder.append(" SPLIT ON(1,2)");
        this.indexDDLOptions = indexOptionBuilder.toString();
        this.tableDDLOptions = optionBuilder.toString();
    }

    @Parameterized.Parameters(name = "mutable={0}, singleCellIndex={1}")
    public static synchronized Collection<Object[]> data() {
        return Arrays.asList(new Object[][] {
                {true, true},
                {true, false},
                {false, true},
                {false, false}});
    }

    @BeforeClass
    public static synchronized void doSetup() throws Exception {
        // below settings are needed to enforce major compaction
        Map<String, String> props = Maps.newHashMapWithExpectedSize(3);
        props.put(CompatBaseScannerRegionObserver.PHOENIX_MAX_LOOKBACK_AGE_CONF_KEY, Integer.toString(0));
        props.put(QueryServices.GLOBAL_INDEX_ROW_AGE_THRESHOLD_TO_DELETE_MS_ATTRIB, Long.toString(0));
        // to force multiple verification tasks to be spawned so that we can exercise the page splitting logic
        props.put(GlobalIndexRegionScanner.INDEX_VERIFY_ROW_COUNTS_PER_TASK_CONF_KEY, Long.toString(2));
        setUpTestDriver(new ReadOnlyProps(props.entrySet().iterator()));
    }

    @Before
    public void createIndexToolTables() throws Exception {
        Properties props = PropertiesUtil.deepCopy(TEST_PROPERTIES);
        try (Connection conn = DriverManager.getConnection(getUrl(), props)) {
            IndexTool.createIndexToolTables(conn);
        }
        resetIndexRegionObserverFailPoints();
    }

    @After
    public void cleanup() throws Exception {
        boolean refCountLeaked = isAnyStoreRefCountLeaked();
        Properties props = PropertiesUtil.deepCopy(TEST_PROPERTIES);
        try (Connection conn = DriverManager.getConnection(getUrl(), props)) {
            deleteAllRows(conn,
                TableName.valueOf(IndexVerificationOutputRepository.OUTPUT_TABLE_NAME_BYTES));
            deleteAllRows(conn,
                TableName.valueOf(IndexVerificationResultRepository.RESULT_TABLE_NAME));
        }
        EnvironmentEdgeManager.reset();
        resetIndexRegionObserverFailPoints();
        assertFalse("refCount leaked", refCountLeaked);
    }

    private void setIndexRowStatusesToVerified(Connection conn, String dataTableFullName, String indexTableFullName) throws Exception {
        PTable pDataTable = PhoenixRuntime.getTable(conn, dataTableFullName);
        PTable pIndexTable = PhoenixRuntime.getTable(conn, indexTableFullName);
        Table hTable = conn.unwrap(PhoenixConnection.class).getQueryServices()
                .getTable(pIndexTable.getPhysicalName().getBytes());
        Scan scan = new Scan();
        PhoenixConnection phoenixConnection = conn.unwrap(PhoenixConnection.class);
        IndexMaintainer indexMaintainer = pIndexTable.getIndexMaintainer(pDataTable, phoenixConnection);
        scan.addColumn(indexMaintainer.getEmptyKeyValueFamily().copyBytesIfNecessary(), indexMaintainer.getEmptyKeyValueQualifier());
        ResultScanner scanner = hTable.getScanner(scan);
        for (Result result = scanner.next(); result != null; result = scanner.next()) {
            Put put = new Put(result.getRow());
            put.addColumn(indexMaintainer.getEmptyKeyValueFamily().copyBytesIfNecessary(),
                    indexMaintainer.getEmptyKeyValueQualifier(), result.rawCells()[0].getTimestamp(), VERIFIED_BYTES);
            hTable.put(put);
        }
    }

    private void initTablesAndAddExtraRowsToIndex(Connection conn, String schemaName, String dataTableName,
            String indexTableName, int NROWS) throws Exception {
        String dataTableFullName = SchemaUtil.getTableName(schemaName, dataTableName);
        String indexTableFullName = SchemaUtil.getTableName(schemaName, indexTableName);

        conn.createStatement().execute("CREATE TABLE " + dataTableFullName
            + " (ID INTEGER NOT NULL PRIMARY KEY, VAL1 INTEGER, VAL2 INTEGER) " + tableDDLOptions);
        PreparedStatement dataPreparedStatement =
            conn.prepareStatement("UPSERT INTO " + dataTableFullName + " VALUES(?,?,?)");
        for (int i = 1; i <= NROWS; i++) {
            dataPreparedStatement.setInt(1, i);
            dataPreparedStatement.setInt(2, i + 1);
            dataPreparedStatement.setInt(3, i * 2);
            dataPreparedStatement.execute();
        }
        conn.commit();
        conn.createStatement().execute(String.format(
            "CREATE INDEX %s ON %s (VAL1) INCLUDE (VAL2)", indexTableName, dataTableFullName));

        // Add extra index rows
        PreparedStatement indexPreparedStatement =
            conn.prepareStatement("UPSERT INTO " + indexTableFullName + " VALUES(?,?,?)");

        for (int i = NROWS + 1; i <= 2 * NROWS; i++) {
            indexPreparedStatement.setInt(1, i + 1); // the indexed column
            indexPreparedStatement.setInt(2, i); // the data pk column
            indexPreparedStatement.setInt(3, i * 2); // the included column
            indexPreparedStatement.execute();
        }
        conn.commit();

        // Set all index row statuses to verified so that read verify will not fix them. We want them to be fixed
        // by IndexRepairRegionScanner
        setIndexRowStatusesToVerified(conn, dataTableFullName, indexTableFullName);
    }

    private void truncateIndexToolTables() throws IOException {
        getUtility().getHBaseAdmin().disableTable(TableName.valueOf(IndexVerificationOutputRepository.OUTPUT_TABLE_NAME));
        getUtility().getHBaseAdmin().truncateTable(TableName.valueOf(IndexVerificationOutputRepository.OUTPUT_TABLE_NAME), true);
        getUtility().getHBaseAdmin().disableTable(TableName.valueOf(RESULT_TABLE_NAME));
        getUtility().getHBaseAdmin().truncateTable(TableName.valueOf(RESULT_TABLE_NAME), true);
    }

    private void dumpIndexToolMRJobCounters(IndexTool indexTool) throws IOException {
        CounterGroup mrJobCounters = IndexToolIT.getMRJobCounters(indexTool);
        for (Counter counter : mrJobCounters) {
            LOGGER.info(String.format("%s=%d", counter.getName(), counter.getValue()));
        }
    }

    private void assertExtraCounters(IndexTool indexTool, long extraVerified, long extraUnverified,
            boolean isBefore) throws IOException {
        CounterGroup mrJobCounters = IndexToolIT.getMRJobCounters(indexTool);

        if (isBefore) {
            assertEquals(extraVerified,
                mrJobCounters.findCounter(BEFORE_REPAIR_EXTRA_VERIFIED_INDEX_ROW_COUNT.name()).getValue());
            assertEquals(extraUnverified,
                mrJobCounters.findCounter(BEFORE_REPAIR_EXTRA_UNVERIFIED_INDEX_ROW_COUNT.name()).getValue());
        } else {
            assertEquals(extraVerified,
                mrJobCounters.findCounter(AFTER_REPAIR_EXTRA_VERIFIED_INDEX_ROW_COUNT.name()).getValue());
            assertEquals(extraUnverified,
                mrJobCounters.findCounter(AFTER_REPAIR_EXTRA_UNVERIFIED_INDEX_ROW_COUNT.name()).getValue());
        }
    }

    private void assertDisableLogging(Connection conn, int expectedExtraRows, int expectedPITRows,
        IndexTool.IndexVerifyType verifyType,
        IndexTool.IndexDisableLoggingType disableLoggingType,
        byte[] expectedPhase,
        String schemaName, String dataTableName,
        String indexTableName, String indexTableFullName,
        int expectedStatus) throws Exception {

        IndexTool tool = IndexToolIT.runIndexTool(getUtility().getConfiguration(), false, schemaName, dataTableName,
            indexTableName,
            null,
            expectedStatus, verifyType, disableLoggingType, "-fi");
        assertNotNull(tool);

        try {
            assertExtraCounters(tool, expectedExtraRows, 0, true);
        } catch (AssertionError e) {
            dumpIndexToolMRJobCounters(tool);
            throw e;
        }

        byte[] indexTableFullNameBytes = Bytes.toBytes(indexTableFullName);
        IndexVerificationOutputRepository outputRepository =
            new IndexVerificationOutputRepository(indexTableFullNameBytes, conn);
        List<IndexVerificationOutputRow> rows =
            outputRepository.getAllOutputRows();
        try {
            if (expectedPITRows == 0) {
                assertTrue(rows.isEmpty());
            } else {
                // https://issues.apache.org/jira/browse/HBASE-17361 HTable#Put() is not threadsafe
                // in releases < HBase 2.0 so occasionally we may fail to add some rows to PIT table
                assertTrue(expectedPITRows >= rows.size());
            }
        } catch (AssertionError e) {
            TestUtil.dumpTable(conn, TableName.valueOf(IndexVerificationOutputRepository.OUTPUT_TABLE_NAME));
            throw e;
        }
        if (expectedPITRows > 0) {
            assertArrayEquals(expectedPhase, rows.get(0).getPhaseValue());
        }
    }

    static private void resetIndexRegionObserverFailPoints() {
        IndexRegionObserver.setFailPreIndexUpdatesForTesting(false);
        IndexRegionObserver.setFailDataTableUpdatesForTesting(false);
        IndexRegionObserver.setFailPostIndexUpdatesForTesting(false);
    }

    static private void commitWithException(Connection conn) {
        try {
            conn.commit();
            resetIndexRegionObserverFailPoints();
            fail();
        } catch (Exception e) {
            // this is expected
        }
    }

    @Test
    public void testRepairExtraIndexRows() throws Exception {
        final int NROWS = 20;
        String schemaName = generateUniqueName();
        String dataTableName = generateUniqueName();
        String dataTableFullName = SchemaUtil.getTableName(schemaName, dataTableName);
        String indexTableName = generateUniqueName();
        String indexTableFullName = SchemaUtil.getTableName(schemaName, indexTableName);
        Properties props = PropertiesUtil.deepCopy(TEST_PROPERTIES);

        try (Connection conn = DriverManager.getConnection(getUrl(), props)) {
            initTablesAndAddExtraRowsToIndex(conn, schemaName, dataTableName, indexTableName, NROWS);

            // do index rebuild without -fi and check with scrutiny that index tool failed to fix the extra rows
            IndexToolIT.runIndexTool(false, schemaName, dataTableName,
                indexTableName, null, 0, IndexVerifyType.BEFORE);

            boolean failed;
            try {
                IndexScrutiny.scrutinizeIndex(conn, dataTableFullName, indexTableFullName);
                failed = false;
            } catch (AssertionError e) {
                failed = true;
            }
            assertTrue(failed);

            // now repair the index with -fi
            IndexTool indexTool = IndexToolIT.runIndexTool(false, schemaName, dataTableName,
                indexTableName, null, 0, IndexVerifyType.BEFORE, "-fi");

            long actualRowCount = IndexScrutiny.scrutinizeIndex(conn, dataTableFullName, indexTableFullName);
            assertEquals(NROWS, actualRowCount);

            assertExtraCounters(indexTool, NROWS, 0, true);
        }
    }

    @Test
    public void testRepairExtraIndexRows_PostIndexUpdateFailure_overwrite() throws Exception {
        if (!mutable) {
            return;
        }
        final int NROWS = 4;
        String schemaName = generateUniqueName();
        String dataTableName = generateUniqueName();
        String dataTableFullName = SchemaUtil.getTableName(schemaName, dataTableName);
        String indexTableName = generateUniqueName();
        String indexTableFullName = SchemaUtil.getTableName(schemaName, indexTableName);
        Properties props = PropertiesUtil.deepCopy(TEST_PROPERTIES);

        try (Connection conn = DriverManager.getConnection(getUrl(), props)) {
            conn.createStatement().execute("CREATE TABLE " + dataTableFullName
                + " (ID INTEGER NOT NULL PRIMARY KEY, VAL1 INTEGER, VAL2 INTEGER) " + tableDDLOptions);
            conn.createStatement().execute(String.format(
                "CREATE INDEX %s ON %s (VAL1) INCLUDE (VAL2)", indexTableName, dataTableFullName));

            PreparedStatement dataPreparedStatement =
                conn.prepareStatement("UPSERT INTO " + dataTableFullName + " VALUES(?,?,?)");
            for (int i = 1; i <= NROWS; i++) {
                dataPreparedStatement.setInt(1, i);
                dataPreparedStatement.setInt(2, i + 1);
                dataPreparedStatement.setInt(3, i * 2);
                dataPreparedStatement.execute();
            }
            conn.commit();

            IndexRegionObserver.setFailPostIndexUpdatesForTesting(true);
            conn.createStatement().execute("UPSERT INTO " + dataTableFullName + " VALUES(3, 100, 200)");
            conn.commit();
            IndexRegionObserver.setFailPostIndexUpdatesForTesting(false);

            IndexTool indexTool = IndexToolIT.runIndexTool(false, schemaName, dataTableName,
                indexTableName, null, 0, IndexVerifyType.BEFORE, "-fi");

            CounterGroup mrJobCounters = IndexToolIT.getMRJobCounters(indexTool);

            assertEquals(2,
                mrJobCounters.findCounter(BEFORE_REBUILD_BEYOND_MAXLOOKBACK_INVALID_INDEX_ROW_COUNT.name()).getValue());
            assertEquals(2,
                mrJobCounters.findCounter(BEFORE_REBUILD_UNVERIFIED_INDEX_ROW_COUNT.name()).getValue());
            assertEquals(0,
                mrJobCounters.findCounter(BEFORE_REPAIR_EXTRA_VERIFIED_INDEX_ROW_COUNT.name()).getValue());
            assertEquals(0,
                mrJobCounters.findCounter(BEFORE_REPAIR_EXTRA_UNVERIFIED_INDEX_ROW_COUNT.name()).getValue());

            indexTool = IndexToolIT.runIndexTool(false, schemaName, dataTableName,
                indexTableName, null, 0, IndexVerifyType.ONLY, "-fi");
            mrJobCounters = IndexToolIT.getMRJobCounters(indexTool);
            assertEquals(0,
                mrJobCounters.findCounter(BEFORE_REBUILD_INVALID_INDEX_ROW_COUNT.name()).getValue());
            assertEquals(0,
                mrJobCounters.findCounter(BEFORE_REBUILD_UNVERIFIED_INDEX_ROW_COUNT.name()).getValue());
            assertEquals(0,
                mrJobCounters.findCounter(BEFORE_REPAIR_EXTRA_VERIFIED_INDEX_ROW_COUNT.name()).getValue());
            assertEquals(0,
                mrJobCounters.findCounter(BEFORE_REPAIR_EXTRA_UNVERIFIED_INDEX_ROW_COUNT.name()).getValue());

            long actualRowCount = IndexScrutiny.scrutinizeIndex(conn, dataTableFullName, indexTableFullName);
            assertEquals(NROWS, actualRowCount);
        }
    }

    @Test
    public void testRepairExtraIndexRows_PostIndexUpdateFailure_delete() throws Exception {
        if (!mutable) {
            return;
        }
        final int NROWS = 4;
        String schemaName = generateUniqueName();
        String dataTableName = generateUniqueName();
        String dataTableFullName = SchemaUtil.getTableName(schemaName, dataTableName);
        String indexTableName = generateUniqueName();
        String indexTableFullName = SchemaUtil.getTableName(schemaName, indexTableName);
        Properties props = PropertiesUtil.deepCopy(TEST_PROPERTIES);

        try (Connection conn = DriverManager.getConnection(getUrl(), props)) {
            conn.createStatement().execute("CREATE TABLE " + dataTableFullName
                + " (ID INTEGER NOT NULL PRIMARY KEY, VAL1 INTEGER, VAL2 INTEGER) " + tableDDLOptions);
            conn.createStatement().execute(String.format(
                "CREATE INDEX %s ON %s (VAL1) INCLUDE (VAL2)", indexTableName, dataTableFullName));

            PreparedStatement dataPreparedStatement =
                conn.prepareStatement("UPSERT INTO " + dataTableFullName + " VALUES(?,?,?)");
            for (int i = 1; i <= NROWS; i++) {
                dataPreparedStatement.setInt(1, i);
                dataPreparedStatement.setInt(2, i + 1);
                dataPreparedStatement.setInt(3, i * 2);
                dataPreparedStatement.execute();
            }
            conn.commit();

            IndexRegionObserver.setFailPostIndexUpdatesForTesting(true);
            conn.createStatement().execute("DELETE FROM " + dataTableFullName + " WHERE ID = 3");
            conn.commit();
            IndexRegionObserver.setFailPostIndexUpdatesForTesting(false);
            TestUtil.doMajorCompaction(conn, dataTableFullName);

            IndexTool indexTool = IndexToolIT.runIndexTool(false, schemaName, dataTableName,
                indexTableName, null, 0, IndexVerifyType.BEFORE, "-fi");

            CounterGroup mrJobCounters = IndexToolIT.getMRJobCounters(indexTool);

            assertEquals(0,
                mrJobCounters.findCounter(BEFORE_REBUILD_INVALID_INDEX_ROW_COUNT.name()).getValue());
            assertEquals(0,
                mrJobCounters.findCounter(BEFORE_REBUILD_UNVERIFIED_INDEX_ROW_COUNT.name()).getValue());
            assertEquals(0,
                mrJobCounters.findCounter(BEFORE_REPAIR_EXTRA_VERIFIED_INDEX_ROW_COUNT.name()).getValue());
            assertEquals(1,
                mrJobCounters.findCounter(BEFORE_REPAIR_EXTRA_UNVERIFIED_INDEX_ROW_COUNT.name()).getValue());

            indexTool = IndexToolIT.runIndexTool(false, schemaName, dataTableName,
                indexTableName, null, 0, IndexVerifyType.ONLY, "-fi");
            mrJobCounters = IndexToolIT.getMRJobCounters(indexTool);

            assertEquals(0,
                mrJobCounters.findCounter(BEFORE_REBUILD_INVALID_INDEX_ROW_COUNT.name()).getValue());
            assertEquals(0,
                mrJobCounters.findCounter(BEFORE_REBUILD_UNVERIFIED_INDEX_ROW_COUNT.name()).getValue());
            assertEquals(0,
                mrJobCounters.findCounter(BEFORE_REPAIR_EXTRA_VERIFIED_INDEX_ROW_COUNT.name()).getValue());
            assertEquals(0,
                mrJobCounters.findCounter(BEFORE_REPAIR_EXTRA_UNVERIFIED_INDEX_ROW_COUNT.name()).getValue());

            long actualRowCount = IndexScrutiny.scrutinizeIndex(conn, dataTableFullName, indexTableFullName);
            assertEquals(NROWS - 1, actualRowCount);
        }
    }

    @Test
    public void testRepairExtraIndexRows_DataTableUpdateFailure() throws Exception {
        if (!mutable) {
            return;
        }
        final int NROWS = 20;
        String schemaName = generateUniqueName();
        String dataTableName = generateUniqueName();
        String dataTableFullName = SchemaUtil.getTableName(schemaName, dataTableName);
        String indexTableName = generateUniqueName();
        String indexTableFullName = SchemaUtil.getTableName(schemaName, indexTableName);
        Properties props = PropertiesUtil.deepCopy(TEST_PROPERTIES);

        try (Connection conn = DriverManager.getConnection(getUrl(), props)) {
            conn.createStatement().execute("CREATE TABLE " + dataTableFullName
                + " (ID INTEGER NOT NULL PRIMARY KEY, VAL1 INTEGER, VAL2 INTEGER) " + tableDDLOptions);
            conn.createStatement().execute(String.format(
                "CREATE INDEX %s ON %s (VAL1) INCLUDE (VAL2)", indexTableName, dataTableFullName));

            IndexRegionObserver.setFailDataTableUpdatesForTesting(true);

            PreparedStatement dataPreparedStatement =
                conn.prepareStatement("UPSERT INTO " + dataTableFullName + " VALUES(?,?,?)");
            for (int i = 1; i <= NROWS; i++) {
                dataPreparedStatement.setInt(1, i);
                dataPreparedStatement.setInt(2, i + 1);
                dataPreparedStatement.setInt(3, i * 2);
                dataPreparedStatement.execute();
            }
            commitWithException(conn);
            IndexRegionObserver.setFailDataTableUpdatesForTesting(true);

            IndexTool indexTool = IndexToolIT.runIndexTool(false, schemaName, dataTableName,
                indexTableName, null, 0, IndexVerifyType.BEFORE, "-fi");

            long actualRowCount = IndexScrutiny.scrutinizeIndex(conn, dataTableFullName, indexTableFullName);
            assertEquals(0, actualRowCount);

            assertExtraCounters(indexTool, 0, NROWS, true);
        }
    }

    @Test
    public void testPITRow() throws Exception {
        final int NROWS = 1;
        String schemaName = generateUniqueName();
        String dataTableName = generateUniqueName();
        String dataTableFullName = SchemaUtil.getTableName(schemaName, dataTableName);
        String indexTableName = generateUniqueName();
        String indexTableFullName = SchemaUtil.getTableName(schemaName, indexTableName);
        Properties props = PropertiesUtil.deepCopy(TEST_PROPERTIES);

        try (Connection conn = DriverManager.getConnection(getUrl(), props)) {
            initTablesAndAddExtraRowsToIndex(conn, schemaName, dataTableName, indexTableName, NROWS);

            IndexToolIT.runIndexTool(false, schemaName, dataTableName,
                indexTableName, null, 0, IndexVerifyType.ONLY, "-fi");

            Cell cell = IndexToolIT.getErrorMessageFromIndexToolOutputTable(conn, dataTableFullName, indexTableFullName);
            String expectedErrorMsg = IndexRepairRegionScanner.ERROR_MESSAGE_EXTRA_INDEX_ROW;
            String actualErrorMsg = Bytes.toString(cell.getValueArray(), cell.getValueOffset(), cell.getValueLength());
            assertTrue(actualErrorMsg.contains(expectedErrorMsg));
        }
    }

    @Test
    public void testVerifyAfterExtraIndexRows() throws Exception {
        final int NROWS = 20;
        String schemaName = generateUniqueName();
        String dataTableName = generateUniqueName();
        String dataTableFullName = SchemaUtil.getTableName(schemaName, dataTableName);
        String indexTableName = generateUniqueName();
        String indexTableFullName = SchemaUtil.getTableName(schemaName, indexTableName);
        Properties props = PropertiesUtil.deepCopy(TEST_PROPERTIES);

        try (Connection conn = DriverManager.getConnection(getUrl(), props)) {
            initTablesAndAddExtraRowsToIndex(conn, schemaName, dataTableName, indexTableName, NROWS);

            // Run -v AFTER and check it doesn't fix the extra rows and the job fails
            IndexTool indexTool = IndexToolIT.runIndexTool(false, schemaName, dataTableName,
                indexTableName, null, -1, IndexVerifyType.AFTER, "-fi");

            boolean failed;
            try {
                IndexScrutiny.scrutinizeIndex(conn, dataTableFullName, indexTableFullName);
                failed = false;
            } catch (AssertionError e) {
                failed = true;
            }
            assertTrue(failed);

            // job failed so no counters are output
        }
    }

    @Test
    public void testVerifyBothExtraIndexRows() throws Exception {
        final int NROWS = 20;
        String schemaName = generateUniqueName();
        String dataTableName = generateUniqueName();
        String dataTableFullName = SchemaUtil.getTableName(schemaName, dataTableName);
        String indexTableName = generateUniqueName();
        String indexTableFullName = SchemaUtil.getTableName(schemaName, indexTableName);
        Properties props = PropertiesUtil.deepCopy(TEST_PROPERTIES);

        try (Connection conn = DriverManager.getConnection(getUrl(), props)) {
            initTablesAndAddExtraRowsToIndex(conn, schemaName, dataTableName, indexTableName, NROWS);

            IndexTool indexTool = IndexToolIT.runIndexTool(false, schemaName, dataTableName,
                indexTableName, null, 0, IndexVerifyType.BOTH, "-fi");

            long actualRowCount = IndexScrutiny.scrutinizeIndex(conn, dataTableFullName, indexTableFullName);
            assertEquals(NROWS, actualRowCount);

            assertExtraCounters(indexTool, 0, 0, false);
        }
    }

    @Test
    public void testOverrideIndexRebuildPageSizeFromIndexTool() throws Exception {
        String schemaName = generateUniqueName();
        String dataTableName = generateUniqueName();
        String dataTableFullName = SchemaUtil.getTableName(schemaName, dataTableName);
        String indexTableName = generateUniqueName();
        String indexTableFullName = SchemaUtil.getTableName(schemaName, indexTableName);
        Properties props = PropertiesUtil.deepCopy(TEST_PROPERTIES);
        final int NROWS = 20;

        try (Connection conn = DriverManager.getConnection(getUrl(), props)) {
            initTablesAndAddExtraRowsToIndex(conn, schemaName, dataTableName, indexTableName, NROWS);

            Configuration conf = new Configuration(getUtility().getConfiguration());
            conf.set(QueryServices.INDEX_REBUILD_PAGE_SIZE_IN_ROWS, Long.toString(2));
            IndexTool indexTool = IndexToolIT.runIndexTool(conf, false, schemaName, dataTableName,
                indexTableName, null, 0, IndexVerifyType.BEFORE, IndexDisableLoggingType.NONE,"-fi");

            long actualRowCount = IndexScrutiny.scrutinizeIndex(conn, dataTableFullName, indexTableFullName);
            assertEquals(NROWS, actualRowCount);

            assertExtraCounters(indexTool, NROWS, 0, true);
        }
    }

    @Test
    public void testViewIndexExtraRows() throws Exception {
        Properties props = PropertiesUtil.deepCopy(TEST_PROPERTIES);
        try (Connection conn = DriverManager.getConnection(getUrl(), props)) {
            String schemaName = generateUniqueName();
            String dataTableName = generateUniqueName();
            String dataTableFullName = SchemaUtil.getTableName(schemaName, dataTableName);
            String viewName = generateUniqueName();
            String viewFullName = SchemaUtil.getTableName(schemaName, viewName);
            String indexTableName1 = generateUniqueName();
            String indexTableFullName1 = SchemaUtil.getTableName(schemaName, indexTableName1);
            String indexTableName2 = generateUniqueName();
            String indexTableFullName2 = SchemaUtil.getTableName(schemaName, indexTableName2);

            conn.createStatement().execute("CREATE TABLE " + dataTableFullName
                + " (ID INTEGER NOT NULL PRIMARY KEY, VAL1 INTEGER, VAL2 INTEGER) "
                + tableDDLOptions);
            conn.commit();
            conn.createStatement().execute("CREATE VIEW " + viewFullName + " AS SELECT * FROM " + dataTableFullName);
            conn.commit();
            // Insert a row
            conn.createStatement().execute("UPSERT INTO " + viewFullName + " values (1, 2, 4)");
            conn.commit();

            conn.createStatement().execute(String.format(
                "CREATE INDEX %s ON %s (VAL1) INCLUDE (VAL2)", indexTableName1, viewFullName));
            conn.createStatement().execute(String.format(
                "CREATE INDEX %s ON %s (VAL2) INCLUDE (VAL1)", indexTableName2, viewFullName));

            // directly insert a row into index
            conn.createStatement().execute("UPSERT INTO " + indexTableFullName1 + " VALUES (4, 2, 8)");
            conn.createStatement().execute("UPSERT INTO " + indexTableFullName2 + " VALUES (8, 2, 4)");
            conn.commit();
            setIndexRowStatusesToVerified(conn, viewFullName, indexTableFullName1);

            IndexTool indexTool = IndexToolIT.runIndexTool(false, schemaName, viewName,
                indexTableName1, null, 0, IndexVerifyType.BEFORE, "-fi");
            assertExtraCounters(indexTool, 1, 0, true);

            indexTool = IndexToolIT.runIndexTool(false, schemaName, viewName,
                indexTableName2, null, 0, IndexVerifyType.BEFORE, "-fi");
            assertExtraCounters(indexTool, 1, 0, true);

            String indexTablePhysicalName = "_IDX" + dataTableFullName;
            byte[] indexTableFullNameBytes = Bytes.toBytes(indexTablePhysicalName);
            IndexVerificationOutputRepository outputRepository =
                new IndexVerificationOutputRepository(indexTableFullNameBytes, conn);
            List<IndexVerificationOutputRow> rows =
                outputRepository.getAllOutputRows();
            try {
                assertEquals(2, rows.size());
            } catch (AssertionError e) {
                TestUtil.dumpTable(conn, TableName.valueOf(IndexVerificationOutputRepository.OUTPUT_TABLE_NAME));
                throw e;
            }
        }
    }

    @Test
    public void testFromIndexToolForIncrementalVerify() throws Exception {
        assumeTrue(HbaseCompatCapabilities.isRawFilterSupported());
        final int NROWS = 4;
        ManualEnvironmentEdge customEdge = new ManualEnvironmentEdge();
        String schemaName = generateUniqueName();
        String dataTableName = generateUniqueName();
        String dataTableFullName = SchemaUtil.getTableName(schemaName, dataTableName);
        String indexTableName = generateUniqueName();
        String indexTableFullName = SchemaUtil.getTableName(schemaName, indexTableName);
        long delta = 2;
        Properties props = PropertiesUtil.deepCopy(TEST_PROPERTIES);
        try (Connection conn = DriverManager.getConnection(getUrl(), props)) {
            long t0 = EnvironmentEdgeManager.currentTimeMillis();

            conn.createStatement().execute("CREATE TABLE " + dataTableFullName
                + " (ID INTEGER NOT NULL PRIMARY KEY, VAL1 INTEGER, VAL2 INTEGER) " + tableDDLOptions);
            PreparedStatement dataPreparedStatement =
                conn.prepareStatement("UPSERT INTO " + dataTableFullName + " VALUES(?,?,?)");
            for (int i = 1; i <= NROWS; i++) {
                dataPreparedStatement.setInt(1, i);
                dataPreparedStatement.setInt(2, i + 1);
                dataPreparedStatement.setInt(3, i * 2);
                dataPreparedStatement.execute();
            }
            conn.commit();

            conn.createStatement().execute(String.format(
                "CREATE INDEX %s ON %s (VAL1) INCLUDE (VAL2)", indexTableName, dataTableFullName));

            customEdge.setValue(EnvironmentEdgeManager.currentTimeMillis());
            EnvironmentEdgeManager.injectEdge(customEdge);
            customEdge.incrementValue(delta);
            long t1 = customEdge.currentTime();

            IndexTool it;
            it = IndexToolIT.runIndexTool(false, schemaName, dataTableName,
                indexTableName, null, 0, IndexVerifyType.ONLY,
                "-fi", "-st", String.valueOf(t0), "-et", String.valueOf(t1));

            CounterGroup mrJobCounters;
            mrJobCounters = IndexToolIT.getMRJobCounters(it);
            assertEquals(NROWS,
                mrJobCounters.findCounter(SCANNED_DATA_ROW_COUNT.name()).getValue());
            assertEquals(0,
                mrJobCounters.findCounter(BEFORE_REPAIR_EXTRA_VERIFIED_INDEX_ROW_COUNT.name()).getValue());
            assertEquals(0,
                mrJobCounters.findCounter(BEFORE_REPAIR_EXTRA_UNVERIFIED_INDEX_ROW_COUNT.name()).getValue());

            // Add extra index rows
            PreparedStatement indexPreparedStatement =
                conn.prepareStatement("UPSERT INTO " + indexTableFullName + " VALUES(?,?,?)");
            for (int i = NROWS + 1; i <= 2 * NROWS; i++) {
                indexPreparedStatement.setInt(1, i + 1); // the indexed column
                indexPreparedStatement.setInt(2, i); // the data pk column
                indexPreparedStatement.setInt(3, i * 2); // the included column
                indexPreparedStatement.execute();
            }
            conn.commit();

            // Set all index row statuses to verified so that read verify will not fix them.
            // We want them to be fixed by IndexRepairRegionScanner
            setIndexRowStatusesToVerified(conn, dataTableFullName, indexTableFullName);
            customEdge.incrementValue(delta);
            long t2 = customEdge.currentTime();
            it = IndexToolIT.runIndexTool(false, schemaName, dataTableName,
                indexTableName, null, 0, IndexVerifyType.ONLY,
                "-fi", "-st", String.valueOf(t1), "-et", String.valueOf(t2));

            // incremental verification should only scan NROWS instead of total 2*NROWS
            mrJobCounters = IndexToolIT.getMRJobCounters(it);
            assertEquals(NROWS,
                mrJobCounters.findCounter(SCANNED_DATA_ROW_COUNT.name()).getValue());
            assertEquals(NROWS,
                mrJobCounters.findCounter(BEFORE_REPAIR_EXTRA_VERIFIED_INDEX_ROW_COUNT.name()).getValue());
            assertEquals(0,
                mrJobCounters.findCounter(BEFORE_REPAIR_EXTRA_UNVERIFIED_INDEX_ROW_COUNT.name()).getValue());

            // now run another verification over the entire window [t0, t2]
            it = IndexToolIT.runIndexTool(false, schemaName, dataTableName,
                indexTableName, null, 0, IndexVerifyType.ONLY,
                "-fi", "-st", String.valueOf(t0), "-et", String.valueOf(t2));

            mrJobCounters = IndexToolIT.getMRJobCounters(it);
            assertEquals(2*NROWS,
                mrJobCounters.findCounter(SCANNED_DATA_ROW_COUNT.name()).getValue());
            assertEquals(NROWS,
                mrJobCounters.findCounter(BEFORE_REPAIR_EXTRA_VERIFIED_INDEX_ROW_COUNT.name()).getValue());
            assertEquals(0,
                mrJobCounters.findCounter(BEFORE_REPAIR_EXTRA_UNVERIFIED_INDEX_ROW_COUNT.name()).getValue());
        } finally {
            EnvironmentEdgeManager.reset();
        }
    }

    @Test
    public void testDisableOutputLogging() throws Exception {
        if (!mutable) {
            return;
        }
        final int NROWS = 4;
        String schemaName = generateUniqueName();
        String dataTableName = generateUniqueName();
        String dataTableFullName = SchemaUtil.getTableName(schemaName, dataTableName);
        String indexTableName = generateUniqueName();
        String indexTableFullName = SchemaUtil.getTableName(schemaName, indexTableName);
        Properties props = PropertiesUtil.deepCopy(TEST_PROPERTIES);

        try(Connection conn = DriverManager.getConnection(getUrl(), props)) {
            conn.createStatement().execute("CREATE TABLE " + dataTableFullName
                + " (ID INTEGER NOT NULL PRIMARY KEY, VAL1 INTEGER, VAL2 INTEGER) " + tableDDLOptions);
            PreparedStatement dataPreparedStatement =
                conn.prepareStatement("UPSERT INTO " + dataTableFullName + " VALUES(?,?,?)");
            for (int i = 1; i <= NROWS; i++) {
                dataPreparedStatement.setInt(1, i);
                dataPreparedStatement.setInt(2, i + 1);
                dataPreparedStatement.setInt(3, i * 2);
                dataPreparedStatement.execute();
            }
            conn.commit();
            conn.createStatement().execute(String.format(
                "CREATE INDEX %s ON %s (VAL1) INCLUDE (VAL2)", indexTableName, dataTableFullName));

            // Add extra index rows
            PreparedStatement indexPreparedStatement =
                conn.prepareStatement("UPSERT INTO " + indexTableFullName + " VALUES(?,?,?)");
            for (int i = NROWS + 1; i <= 2 * NROWS; i++) {
                indexPreparedStatement.setInt(1, i + 1); // the indexed column
                indexPreparedStatement.setInt(2, i); // the data pk column
                indexPreparedStatement.setInt(3, i * 2); // the included column
                indexPreparedStatement.execute();
            }
            conn.commit();

            // Set all index row statuses to verified so that read verify will not fix them.
            // We want them to be fixed by IndexRepairRegionScanner
            setIndexRowStatusesToVerified(conn, dataTableFullName, indexTableFullName);

            // run the index MR job as ONLY so the index doesn't get rebuilt. Should be NROWS number
            // of extra rows. We pass in --disable-logging BEFORE to silence the output logging to
            // PHOENIX_INDEX_TOOL
            assertDisableLogging(conn, NROWS, 0, IndexTool.IndexVerifyType.ONLY,
                IndexTool.IndexDisableLoggingType.BEFORE, null, schemaName, dataTableName, indexTableName,
                indexTableFullName, 0);
            truncateIndexToolTables();

            // logging to PHOENIX_INDEX_TOOL enabled
            assertDisableLogging(conn, NROWS, NROWS, IndexTool.IndexVerifyType.ONLY,
                IndexTool.IndexDisableLoggingType.NONE,
                IndexVerificationOutputRepository.PHASE_BEFORE_VALUE,schemaName,
                dataTableName, indexTableName,
                indexTableFullName, 0);
            truncateIndexToolTables();

            assertDisableLogging(conn, NROWS, 0, IndexTool.IndexVerifyType.BEFORE,
                IndexTool.IndexDisableLoggingType.BEFORE,
                null, schemaName,
                dataTableName, indexTableName,
                indexTableFullName, 0);
        }
    }

    public void deleteAllRows(Connection conn, TableName tableName) throws SQLException,
            IOException, InterruptedException {
        Scan scan = new Scan();
        Admin admin = conn.unwrap(PhoenixConnection.class).getQueryServices().
                getAdmin();
        org.apache.hadoop.hbase.client.Connection hbaseConn = admin.getConnection();
        Table table = hbaseConn.getTable(tableName);
        boolean deletedRows = false;
        try (ResultScanner scanner = table.getScanner(scan)) {
            for (Result r : scanner) {
                Delete del = new Delete(r.getRow());
                table.delete(del);
                deletedRows = true;
            }
        } catch (Exception e) {
            //if the table doesn't exist, we have no rows to delete. Easier to catch
            //than to pre-check for existence
        }
        //don't flush/compact if we didn't write anything, because we'll hang forever
        if (deletedRows) {
            getUtility().getAdmin().flush(tableName);
            TestUtil.majorCompact(getUtility(), tableName);
        }
    }

}
