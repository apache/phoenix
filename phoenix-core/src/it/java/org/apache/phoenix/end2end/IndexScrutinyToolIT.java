/*
 * Licensed to the Apache Software Foundation (ASF) under one or more contributor license
 * agreements. See the NOTICE file distributed with this work for additional information regarding
 * copyright ownership. The ASF licenses this file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License. You may obtain a
 * copy of the License at http://www.apache.org/licenses/LICENSE-2.0 Unless required by applicable
 * law or agreed to in writing, software distributed under the License is distributed on an "AS IS"
 * BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License
 * for the specific language governing permissions and limitations under the License.
 */
package org.apache.phoenix.end2end;

import static org.apache.phoenix.mapreduce.index.PhoenixScrutinyJobCounters.BAD_COVERED_COL_VAL_COUNT;
import static org.apache.phoenix.mapreduce.index.PhoenixScrutinyJobCounters.BATCHES_PROCESSED_COUNT;
import static org.apache.phoenix.mapreduce.index.PhoenixScrutinyJobCounters.INVALID_ROW_COUNT;
import static org.apache.phoenix.mapreduce.index.PhoenixScrutinyJobCounters.VALID_ROW_COUNT;
import static org.apache.phoenix.util.TestUtil.TEST_PROPERTIES;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.util.Arrays;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Random;
import java.util.TreeSet;
import java.util.UUID;

import com.google.common.collect.Sets;
import org.apache.commons.io.IOUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.apache.hadoop.mapreduce.Counters;
import org.apache.hadoop.mapreduce.Job;
import org.apache.phoenix.mapreduce.CsvBulkImportUtil;
import org.apache.phoenix.mapreduce.index.IndexScrutinyTableOutput;
import org.apache.phoenix.mapreduce.index.IndexScrutinyTool;
import org.apache.phoenix.mapreduce.index.IndexScrutinyTool.OutputFormat;
import org.apache.phoenix.mapreduce.index.IndexScrutinyTool.SourceTable;
import org.apache.phoenix.mapreduce.index.PhoenixScrutinyJobCounters;
import org.apache.phoenix.mapreduce.index.SourceTargetColumnNames;
import org.apache.phoenix.mapreduce.util.PhoenixConfigurationUtil;
import org.apache.phoenix.query.BaseTest;
import org.apache.phoenix.schema.PTable;
import org.apache.phoenix.util.PhoenixRuntime;
import org.apache.phoenix.util.PropertiesUtil;
import org.apache.phoenix.util.ReadOnlyProps;
import org.apache.phoenix.util.SchemaUtil;
import org.junit.After;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

/**
 * Tests for the {@link IndexScrutinyTool}
 */
@Category(NeedsOwnMiniClusterTest.class)
@RunWith(Parameterized.class)
public class IndexScrutinyToolIT extends BaseTest {

    private String dataTableDdl;
    private String indexTableDdl;

    private static final String UPSERT_SQL = "UPSERT INTO %s VALUES(?,?,?,?)";

    private static final String INDEX_UPSERT_SQL =
        "UPSERT INTO %s (\"0:NAME\", \":ID\", \"0:ZIP\", \"0:EMPLOY_DATE\") values (?,?,?,?)";

    private static final String DELETE_SQL = "DELETE FROM %s ";

    private String schemaName;
    private String dataTableName;
    private String dataTableFullName;
    private String indexTableName;
    private String indexTableFullName;
    private String outputDir;

    private Connection conn;

    private PreparedStatement dataTableUpsertStmt;

    private PreparedStatement indexTableUpsertStmt;

    private long testTime;

    @Parameterized.Parameters
    public static Collection<Object[]> data() {
        return Arrays.asList(new Object[][] {
            { "CREATE TABLE %s (ID INTEGER NOT NULL PRIMARY KEY, NAME VARCHAR, ZIP INTEGER, EMPLOY_DATE TIMESTAMP, EMPLOYER VARCHAR)", "CREATE LOCAL INDEX %s ON %s (NAME, EMPLOY_DATE) INCLUDE (ZIP)" },
            { "CREATE TABLE %s (ID INTEGER NOT NULL PRIMARY KEY, NAME VARCHAR, ZIP INTEGER, EMPLOY_DATE TIMESTAMP, EMPLOYER VARCHAR) SALT_BUCKETS=2", "CREATE INDEX %s ON %s (NAME, EMPLOY_DATE) INCLUDE (ZIP)" } });
    }

    public IndexScrutinyToolIT(String dataTableDdl, String indexTableDdl) {
        this.dataTableDdl = dataTableDdl;
        this.indexTableDdl = indexTableDdl;
    }

    @BeforeClass
    public static void doSetup() throws Exception {
        Map<String, String> props = Maps.newHashMap();
        setUpTestDriver(new ReadOnlyProps(props.entrySet().iterator()));
    }

    /**
     * Create the test data and index tables
     */
    @Before
    public void setup() throws SQLException {
        generateUniqueTableNames();
        createTestTable(getUrl(), String.format(dataTableDdl, dataTableFullName));
        createTestTable(getUrl(),
            String.format(indexTableDdl, indexTableName, dataTableFullName));
        Properties props = PropertiesUtil.deepCopy(TEST_PROPERTIES);
        conn = DriverManager.getConnection(getUrl(), props);
        String dataTableUpsert = String.format(UPSERT_SQL, dataTableFullName);
        dataTableUpsertStmt = conn.prepareStatement(dataTableUpsert);
        String indexTableUpsert = String.format(INDEX_UPSERT_SQL, indexTableFullName);
        indexTableUpsertStmt = conn.prepareStatement(indexTableUpsert);
        conn.setAutoCommit(false);
        testTime = System.currentTimeMillis();
    }

    @After
    public void teardown() throws SQLException {
        if (conn != null) {
            conn.close();
        }
    }

    /**
     * Tests a data table that is correctly indexed. Scrutiny should report all rows as valid.
     */
    @Test
    public void testValidIndex() throws Exception {
        // insert two rows
        upsertRow(dataTableUpsertStmt, 1, "name-1", 94010);
        upsertRow(dataTableUpsertStmt, 2, "name-2", 95123);
        conn.commit();

        int numDataRows = countRows(dataTableFullName);
        int numIndexRows = countRows(indexTableFullName);

        // scrutiny should report everything as ok
        List<Job> completedJobs = runScrutiny(schemaName, dataTableName, indexTableName);
        Job job = completedJobs.get(0);
        assertTrue(job.isSuccessful());
        Counters counters = job.getCounters();
        assertEquals(2, getCounterValue(counters, VALID_ROW_COUNT));
        assertEquals(0, getCounterValue(counters, INVALID_ROW_COUNT));

        // make sure row counts weren't modified by scrutiny
        assertEquals(numDataRows, countRows(dataTableFullName));
        assertEquals(numIndexRows, countRows(indexTableFullName));
    }

    /**
     * Tests an index with the same # of rows as the data table, but one of the index rows is
     * incorrect Scrutiny should report the invalid rows.
     */
    @Test
    public void testEqualRowCountIndexIncorrect() throws Exception {
        // insert one valid row
        upsertRow(dataTableUpsertStmt, 1, "name-1", 94010);
        conn.commit();

        // disable the index and insert another row which is not indexed
        disableIndex();
        upsertRow(dataTableUpsertStmt, 2, "name-2", 95123);
        conn.commit();

        // insert a bad row into the index
        upsertIndexRow("badName", 2, 9999);
        conn.commit();

        // scrutiny should report the bad row
        List<Job> completedJobs = runScrutiny(schemaName, dataTableName, indexTableName);
        Job job = completedJobs.get(0);
        assertTrue(job.isSuccessful());
        Counters counters = job.getCounters();
        assertEquals(1, getCounterValue(counters, VALID_ROW_COUNT));
        assertEquals(1, getCounterValue(counters, INVALID_ROW_COUNT));
    }

    /**
     * Tests an index where the index pk is correct (indexed col values are indexed correctly), but
     * a covered index value is incorrect. Scrutiny should report the invalid row
     */
    @Test
    public void testCoveredValueIncorrect() throws Exception {
        // insert one valid row
        upsertRow(dataTableUpsertStmt, 1, "name-1", 94010);
        conn.commit();

        // disable index and insert another data row
        disableIndex();
        upsertRow(dataTableUpsertStmt, 2, "name-2", 95123);
        conn.commit();

        // insert a bad index row for the above data row
        upsertIndexRow("name-2", 2, 9999);
        conn.commit();

        // scrutiny should report the bad row
        List<Job> completedJobs = runScrutiny(schemaName, dataTableName, indexTableName);
        Job job = completedJobs.get(0);
        assertTrue(job.isSuccessful());
        Counters counters = job.getCounters();
        assertEquals(1, getCounterValue(counters, VALID_ROW_COUNT));
        assertEquals(1, getCounterValue(counters, INVALID_ROW_COUNT));
        assertEquals(1, getCounterValue(counters, BAD_COVERED_COL_VAL_COUNT));
    }

    /**
     * Test batching of row comparisons Inserts 1001 rows, with some random bad rows, and runs
     * scrutiny with batchsize of 10,
     */
    @Test
    public void testBatching() throws Exception {
        // insert 1001 data and index rows
        int numTestRows = 1001;
        for (int i = 0; i < numTestRows; i++) {
            upsertRow(dataTableUpsertStmt, i, "name-" + i, i + 1000);
        }
        conn.commit();

        disableIndex();

        // randomly delete some rows from the index
        Random random = new Random();
        for (int i = 0; i < 100; i++) {
            int idToDelete = random.nextInt(numTestRows);
            deleteRow(indexTableFullName, "WHERE \":ID\"=" + idToDelete);
        }
        conn.commit();
        int numRows = countRows(indexTableFullName);
        int numDeleted = numTestRows - numRows;

        // run scrutiny with batch size of 10
        List<Job> completedJobs =
                runScrutiny(schemaName, dataTableName, indexTableName, System.currentTimeMillis(),
                    10L);
        Job job = completedJobs.get(0);
        assertTrue(job.isSuccessful());
        Counters counters = job.getCounters();
        assertEquals(numTestRows - numDeleted, getCounterValue(counters, VALID_ROW_COUNT));
        assertEquals(numDeleted, getCounterValue(counters, INVALID_ROW_COUNT));
        assertEquals(numTestRows / 10 + numTestRows % 10,
            getCounterValue(counters, BATCHES_PROCESSED_COUNT));
    }

    /**
     * Tests when there are more data table rows than index table rows Scrutiny should report the
     * number of incorrect rows
     */
    @Test
    public void testMoreDataRows() throws Exception {
        upsertRow(dataTableUpsertStmt, 1, "name-1", 95123);
        conn.commit();
        disableIndex();
        // these rows won't have a corresponding index row
        upsertRow(dataTableUpsertStmt, 2, "name-2", 95124);
        upsertRow(dataTableUpsertStmt, 3, "name-3", 95125);
        conn.commit();

        List<Job> completedJobs = runScrutiny(schemaName, dataTableName, indexTableName);
        Job job = completedJobs.get(0);
        assertTrue(job.isSuccessful());
        Counters counters = job.getCounters();
        assertEquals(1, getCounterValue(counters, VALID_ROW_COUNT));
        assertEquals(2, getCounterValue(counters, INVALID_ROW_COUNT));
    }

    /**
     * Tests when there are more index table rows than data table rows Scrutiny should report the
     * number of incorrect rows when run with the index as the source table
     */
    @Test
    public void testMoreIndexRows() throws Exception {
        upsertRow(dataTableUpsertStmt, 1, "name-1", 95123);
        conn.commit();
        disableIndex();
        // these index rows won't have a corresponding data row
        upsertIndexRow("name-2", 2, 95124);
        upsertIndexRow("name-3", 3, 95125);
        conn.commit();

        List<Job> completedJobs =
                runScrutiny(schemaName, dataTableName, indexTableName, System.currentTimeMillis(),
                    10L, SourceTable.INDEX_TABLE_SOURCE);
        Job job = completedJobs.get(0);
        assertTrue(job.isSuccessful());
        Counters counters = job.getCounters();
        assertEquals(1, getCounterValue(counters, VALID_ROW_COUNT));
        assertEquals(2, getCounterValue(counters, INVALID_ROW_COUNT));
    }

    /**
     * Tests running with both the index and data tables as the source table If we have an
     * incorrectly indexed row, it should be reported in each direction
     */
    @Test
    public void testBothDataAndIndexAsSource() throws Exception {
        // insert one valid row
        upsertRow(dataTableUpsertStmt, 1, "name-1", 94010);
        conn.commit();

        // disable the index and insert another row which is not indexed
        disableIndex();
        upsertRow(dataTableUpsertStmt, 2, "name-2", 95123);
        conn.commit();

        // insert a bad row into the index
        upsertIndexRow("badName", 2, 9999);
        conn.commit();

        List<Job> completedJobs =
                runScrutiny(schemaName, dataTableName, indexTableName, System.currentTimeMillis(),
                    10L, SourceTable.BOTH);
        assertEquals(2, completedJobs.size());
        for (Job job : completedJobs) {
            assertTrue(job.isSuccessful());
            Counters counters = job.getCounters();
            assertEquals(1, getCounterValue(counters, VALID_ROW_COUNT));
            assertEquals(1, getCounterValue(counters, INVALID_ROW_COUNT));
        }
    }

    /**
     * Tests that with the output to file option set, the scrutiny tool outputs invalid rows to file
     */
    @Test
    public void testOutputInvalidRowsToFile() throws Exception {
        insertOneValid_OneBadVal_OneMissingTarget();

        String[] argValues =
                getArgValues(schemaName, dataTableName, indexTableName, System.currentTimeMillis(),
                    10L, SourceTable.DATA_TABLE_SOURCE, true, OutputFormat.FILE, null);
        runScrutiny(argValues);

        // check the output files
        Path outputPath = CsvBulkImportUtil.getOutputPath(new Path(outputDir), dataTableFullName);
        DistributedFileSystem fs = getUtility().getDFSCluster().getFileSystem();
        List<Path> paths = Lists.newArrayList();
        Path firstPart = null;
        for (FileStatus outputFile : fs.listStatus(outputPath)) {
            if (outputFile.getPath().getName().startsWith("part")) {
                if (firstPart == null) {
                    firstPart = outputFile.getPath();
                } else {
                    paths.add(outputFile.getPath());
                }
            }
        }
        if (dataTableDdl.contains("SALT_BUCKETS")) {
            fs.concat(firstPart, paths.toArray(new Path[0]));
        }
        Path outputFilePath = firstPart;
        assertTrue(fs.exists(outputFilePath));
        FSDataInputStream fsDataInputStream = fs.open(outputFilePath);
        BufferedReader reader = new BufferedReader(new InputStreamReader(fsDataInputStream));
        TreeSet<String> lines = Sets.newTreeSet();
        try {
            String line = null;
            while ((line = reader.readLine()) != null) {
                lines.add(line);
            }
        } finally {
            IOUtils.closeQuietly(reader);
            IOUtils.closeQuietly(fsDataInputStream);
        }
        Iterator<String> lineIterator = lines.iterator();
        assertEquals(
            "[2, name-2, " + new Timestamp(testTime).toString() + ", 95123]\t[2, name-2, " + new Timestamp(testTime)
                .toString() + ", 9999]", lineIterator.next());
        assertEquals("[3, name-3, " + new Timestamp(testTime).toString() + ", 95123]\tTarget row not found",
            lineIterator.next());

    }

    /**
     * Tests writing of results to the output table
     */
    @Test
    public void testOutputInvalidRowsToTable() throws Exception {
        insertOneValid_OneBadVal_OneMissingTarget();
        String[] argValues =
                getArgValues(schemaName, dataTableName, indexTableName, System.currentTimeMillis(),
                    10L, SourceTable.DATA_TABLE_SOURCE, true, OutputFormat.TABLE, null);
        List<Job> completedJobs = runScrutiny(argValues);

        // check that the output table contains the invalid rows
        long scrutinyTimeMillis =
                PhoenixConfigurationUtil
                        .getScrutinyExecuteTimestamp(completedJobs.get(0).getConfiguration());
        String invalidRowsQuery =
                IndexScrutinyTableOutput.getSqlQueryAllInvalidRows(conn, getColNames(),
                    scrutinyTimeMillis);
        ResultSet rs = conn.createStatement().executeQuery(invalidRowsQuery + " ORDER BY ID asc");
        assertTrue(rs.next());
        assertEquals(dataTableFullName,
            rs.getString(IndexScrutinyTableOutput.SOURCE_TABLE_COL_NAME));
        assertEquals(indexTableFullName,
            rs.getString(IndexScrutinyTableOutput.TARGET_TABLE_COL_NAME));
        assertTrue(rs.getBoolean("HAS_TARGET_ROW"));
        assertEquals(2, rs.getInt("ID"));
        assertEquals(2, rs.getInt(":ID"));
        assertEquals(95123, rs.getInt("ZIP"));
        assertEquals(9999, rs.getInt("0:ZIP")); // covered col zip incorrect
        assertTrue(rs.next());
        assertEquals(dataTableFullName,
            rs.getString(IndexScrutinyTableOutput.SOURCE_TABLE_COL_NAME));
        assertEquals(indexTableFullName,
            rs.getString(IndexScrutinyTableOutput.TARGET_TABLE_COL_NAME));
        assertFalse(rs.getBoolean("HAS_TARGET_ROW"));
        assertEquals(3, rs.getInt("ID"));
        assertEquals(null, rs.getObject(":ID")); // null for missing target row
        assertFalse(rs.next());

        // check that the job results were written correctly to the metadata table
        assertMetadataTableValues(argValues, scrutinyTimeMillis, invalidRowsQuery);
    }

    /**
     * Tests that the config for max number of output rows is observed
     */
    @Test
    public void testMaxOutputRows() throws Exception {
        insertOneValid_OneBadVal_OneMissingTarget();
        // set max to 1.  There are two bad rows, but only 1 should get written to output table
        String[] argValues =
                getArgValues(schemaName, dataTableName, indexTableName, System.currentTimeMillis(),
                    10L, SourceTable.DATA_TABLE_SOURCE, true, OutputFormat.TABLE, new Long(1));
        List<Job> completedJobs = runScrutiny(argValues);
        long scrutinyTimeMillis =
                PhoenixConfigurationUtil
                        .getScrutinyExecuteTimestamp(completedJobs.get(0).getConfiguration());
        String invalidRowsQuery =
                IndexScrutinyTableOutput.getSqlQueryAllInvalidRows(conn, getColNames(),
                    scrutinyTimeMillis);
        ResultSet rs = conn.createStatement().executeQuery(invalidRowsQuery);
        assertTrue(rs.next());
        if (dataTableDdl.contains("SALT_BUCKETS")) {
            assertTrue(rs.next());
            assertFalse(rs.next());
        } else {
            assertFalse(rs.next());
        }
    }

    private SourceTargetColumnNames getColNames() throws SQLException {
        PTable pdataTable = PhoenixRuntime.getTable(conn, dataTableFullName);
        PTable pindexTable = PhoenixRuntime.getTable(conn, indexTableFullName);
        SourceTargetColumnNames columnNames =
                new SourceTargetColumnNames.DataSourceColNames(pdataTable, pindexTable);
        return columnNames;
    }

    // inserts one valid data/index row, one data row with a missing index row,
    // and one data row with an index row that has a bad covered col val
    private void insertOneValid_OneBadVal_OneMissingTarget() throws SQLException {
        // insert one valid row
        upsertRow(dataTableUpsertStmt, 1, "name-1", 94010);
        conn.commit();

        // disable the index and insert another row which is not indexed
        disableIndex();
        upsertRow(dataTableUpsertStmt, 2, "name-2", 95123);
        upsertRow(dataTableUpsertStmt, 3, "name-3", 95123);
        conn.commit();

        // insert a bad index row for one of the above data rows
        upsertIndexRow("name-2", 2, 9999);
        conn.commit();
    }

    private void assertMetadataTableValues(String[] argValues, long scrutinyTimeMillis,
            String invalidRowsQuery) throws SQLException {
        ResultSet rs;
        ResultSet metadataRs =
                IndexScrutinyTableOutput.queryAllMetadata(conn, dataTableFullName,
                    indexTableFullName, scrutinyTimeMillis);
        assertTrue(metadataRs.next());
        List<? extends Object> expected =
            Arrays.asList(dataTableFullName, indexTableFullName, scrutinyTimeMillis,
                SourceTable.DATA_TABLE_SOURCE.name(), Arrays.toString(argValues), 3L, 0L, 1L,
                2L, 1L, 1L, "[\"ID\" INTEGER, \"NAME\" VARCHAR, \"EMPLOY_DATE\" TIMESTAMP, \"ZIP\" INTEGER]",
                "[\":ID\" INTEGER, \"0:NAME\" VARCHAR, \"0:EMPLOY_DATE\" DECIMAL, \"0:ZIP\" INTEGER]", invalidRowsQuery);
        if (dataTableDdl.contains("SALT_BUCKETS")) {
            expected = Arrays.asList(dataTableFullName, indexTableFullName, scrutinyTimeMillis,
                SourceTable.DATA_TABLE_SOURCE.name(), Arrays.toString(argValues), 3L, 0L, 1L,
                2L, 1L, 2L, "[\"ID\" INTEGER, \"NAME\" VARCHAR, \"EMPLOY_DATE\" TIMESTAMP, \"ZIP\" INTEGER]",
                "[\":ID\" INTEGER, \"0:NAME\" VARCHAR, \"0:EMPLOY_DATE\" DECIMAL, \"0:ZIP\" INTEGER]", invalidRowsQuery);
        }

        assertRsValues(metadataRs, expected);
        String missingTargetQuery = metadataRs.getString("INVALID_ROWS_QUERY_MISSING_TARGET");
        rs = conn.createStatement().executeQuery(missingTargetQuery);
        assertTrue(rs.next());
        assertEquals(3, rs.getInt("ID"));
        assertFalse(rs.next());
        String badCoveredColQuery = metadataRs.getString("INVALID_ROWS_QUERY_BAD_COVERED_COL_VAL");
        rs = conn.createStatement().executeQuery(badCoveredColQuery);
        assertTrue(rs.next());
        assertEquals(2, rs.getInt("ID"));
        assertFalse(rs.next());
    }

    // assert the result set contains the expected values in the given order
    private void assertRsValues(ResultSet rs, List<? extends Object> expected) throws SQLException {
        for (int i = 0; i < expected.size(); i++) {
            assertEquals(expected.get(i), rs.getObject(i + 1));
        }
    }

    private void generateUniqueTableNames() {
        schemaName = generateUniqueName();
        dataTableName = generateUniqueName();
        dataTableFullName = SchemaUtil.getTableName(schemaName, dataTableName);
        indexTableName = generateUniqueName();
        indexTableFullName = SchemaUtil.getTableName(schemaName, indexTableName);
    }

    private int countRows(String tableFullName) throws SQLException {
        ResultSet count = conn.createStatement().executeQuery("select count(*) from " + tableFullName);
        count.next();
        int numRows = count.getInt(1);
        return numRows;
    }

    private void upsertIndexRow(String name, int id, int zip) throws SQLException {
        indexTableUpsertStmt.setString(1, name);
        indexTableUpsertStmt.setInt(2, id); // id
        indexTableUpsertStmt.setInt(3, zip); // bad zip
        indexTableUpsertStmt.setTimestamp(4, new Timestamp(testTime));
        indexTableUpsertStmt.executeUpdate();
    }

    private void disableIndex() throws SQLException {
        conn.createStatement().execute(
            String.format("ALTER INDEX %s ON %S disable", indexTableName, dataTableFullName));
        conn.commit();
    }

    private long getCounterValue(Counters counters, Enum<PhoenixScrutinyJobCounters> counter) {
        return counters.findCounter(counter).getValue();
    }

    private String[] getArgValues(String schemaName, String dataTable, String indxTable, long ts,
            Long batchSize, SourceTable sourceTable, boolean outputInvalidRows,
            OutputFormat outputFormat, Long maxOutputRows) {
        final List<String> args = Lists.newArrayList();
        if (schemaName != null) {
            args.add("-s");
            args.add(schemaName);
        }
        args.add("-dt");
        args.add(dataTable);
        args.add("-it");
        args.add(indxTable);

        // TODO test snapshot reads
        // if(useSnapshot) {
        // args.add("-snap");
        // }

        if (OutputFormat.FILE.equals(outputFormat)) {
            args.add("-op");
            outputDir = "/tmp/" + UUID.randomUUID().toString();
            args.add(outputDir);
        }
        args.add("-t");
        args.add(String.valueOf(ts));
        args.add("-run-foreground");
        if (batchSize != null) {
            args.add("-b");
            args.add(String.valueOf(batchSize));
        }

        // default to using data table as the source table
        args.add("-src");
        if (sourceTable == null) {
            args.add(SourceTable.DATA_TABLE_SOURCE.name());
        } else {
            args.add(sourceTable.name());
        }
        if (outputInvalidRows) {
            args.add("-o");
        }
        if (outputFormat != null) {
            args.add("-of");
            args.add(outputFormat.name());
        }
        if (maxOutputRows != null) {
            args.add("-om");
            args.add(maxOutputRows.toString());
        }
        return args.toArray(new String[0]);
    }

    private List<Job> runScrutiny(String schemaName, String dataTableName, String indexTableName)
            throws Exception {
        return runScrutiny(schemaName, dataTableName, indexTableName, System.currentTimeMillis());
    }

    private List<Job> runScrutiny(String schemaName, String dataTableName, String indexTableName,
            long ts) throws Exception {
        return runScrutiny(schemaName, dataTableName, indexTableName, ts, null, null);
    }

    private List<Job> runScrutiny(String schemaName, String dataTableName, String indexTableName,
            long ts, Long batchSize) throws Exception {
        return runScrutiny(schemaName, dataTableName, indexTableName, ts, batchSize, null);
    }

    private List<Job> runScrutiny(String schemaName, String dataTableName, String indexTableName,
            long ts, Long batchSize, SourceTable sourceTable) throws Exception {
        final String[] cmdArgs =
                getArgValues(schemaName, dataTableName, indexTableName, ts, batchSize, sourceTable,
                    false, null, null);
        return runScrutiny(cmdArgs);
    }

    private List<Job> runScrutiny(String[] cmdArgs) throws Exception {
        IndexScrutinyTool scrutiny = new IndexScrutinyTool();
        Configuration conf = new Configuration(getUtility().getConfiguration());
        scrutiny.setConf(conf);
        int status = scrutiny.run(cmdArgs);
        assertEquals(0, status);
        return scrutiny.getJobs();
    }

    private void upsertRow(PreparedStatement stmt, int id, String name, int zip)
            throws SQLException {
        int index = 1;
        // insert row
        stmt.setInt(index++, id);
        stmt.setString(index++, name);
        stmt.setInt(index++, zip);
        stmt.setTimestamp(index++, new Timestamp(testTime));
        stmt.executeUpdate();
    }

    private int deleteRow(String fullTableName, String whereCondition) throws SQLException {
        String deleteSql = String.format(DELETE_SQL, indexTableFullName) + whereCondition;
        PreparedStatement deleteStmt = conn.prepareStatement(deleteSql);
        return deleteStmt.executeUpdate();
    }

}
