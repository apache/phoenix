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
package org.apache.phoenix.schema.stats;

import static org.apache.phoenix.jdbc.PhoenixDatabaseMetaData.SYSTEM_CATALOG_SCHEMA;
import static org.apache.phoenix.jdbc.PhoenixDatabaseMetaData.SYSTEM_STATS_TABLE;
import static org.apache.phoenix.mapreduce.util.PhoenixConfigurationUtil.MAPREDUCE_JOB_TYPE;
import static org.apache.phoenix.mapreduce.util.PhoenixConfigurationUtil.MRJobType.UPDATE_STATS;
import static org.apache.phoenix.util.TestUtil.TEST_PROPERTIES;
import static org.apache.phoenix.util.TestUtil.getAllSplits;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.IOException;
import java.sql.Array;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Random;
import java.util.regex.Pattern;

import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.HRegionLocation;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.ColumnFamilyDescriptorBuilder;
import org.apache.phoenix.compile.ExplainPlan;
import org.apache.phoenix.compile.ExplainPlanAttributes;
import org.apache.phoenix.jdbc.PhoenixPreparedStatement;
import org.apache.phoenix.thirdparty.com.google.common.collect.Lists;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.coprocessor.RegionCoprocessorEnvironment;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.mapreduce.Job;
import org.apache.phoenix.coprocessor.UngroupedAggregateRegionObserver;
import org.apache.phoenix.jdbc.PhoenixConnection;
import org.apache.phoenix.jdbc.PhoenixDatabaseMetaData;
import org.apache.phoenix.query.BaseTest;
import org.apache.phoenix.query.ConnectionQueryServices;
import org.apache.phoenix.query.KeyRange;
import org.apache.phoenix.query.QueryServices;
import org.apache.phoenix.query.QueryServicesOptions;
import org.apache.phoenix.schema.PTable;
import org.apache.phoenix.schema.PTableImpl;
import org.apache.phoenix.schema.PTableKey;
import org.apache.phoenix.transaction.PhoenixTransactionProvider.Feature;
import org.apache.phoenix.transaction.TransactionFactory;
import org.apache.phoenix.util.MetaDataUtil;
import org.apache.phoenix.util.PhoenixRuntime;
import org.apache.phoenix.util.PropertiesUtil;
import org.apache.phoenix.util.ReadOnlyProps;
import org.apache.phoenix.util.SchemaUtil;
import org.apache.phoenix.util.TestUtil;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Ignore;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.phoenix.thirdparty.com.google.common.collect.Maps;

/**
 * Base Test class for all Statistics Collection
 * Tests stats collection with various scenario parameters
 * 1. Column Encoding
 * 2. Transactions
 * 3. Namespaces
 * 4. Stats collection via SQL or MR job
 */
@RunWith(Parameterized.class)
public abstract class BaseStatsCollectorIT extends BaseTest {

    private static final Logger LOGGER = LoggerFactory.getLogger(BaseStatsCollectorIT.class);

    private final String tableDDLOptions;
    private final boolean columnEncoded;
    private String tableName;
    private String schemaName;
    private String fullTableName;
    private String physicalTableName;
    private final boolean userTableNamespaceMapped;
    private final boolean mutable;
    private final String transactionProvider;
    private static final int defaultGuidePostWidth = 20;
    private boolean collectStatsOnSnapshot;

    protected BaseStatsCollectorIT(boolean userTableNamespaceMapped, boolean collectStatsOnSnapshot) {
        this(false, null, userTableNamespaceMapped, false, collectStatsOnSnapshot);
    }

    protected BaseStatsCollectorIT(boolean mutable, String transactionProvider, boolean columnEncoded) {
        this(mutable, transactionProvider, false, columnEncoded, false);
    }

    private BaseStatsCollectorIT(boolean mutable, String transactionProvider,
                                 boolean userTableNamespaceMapped, boolean columnEncoded, boolean collectStatsOnSnapshot) {
        this.transactionProvider = transactionProvider;
        StringBuilder sb = new StringBuilder();
        if (columnEncoded) {
            sb.append("COLUMN_ENCODED_BYTES=4");        
        } else {
            sb.append("COLUMN_ENCODED_BYTES=0");
        }
        
        if (transactionProvider != null) {
            sb.append(",TRANSACTIONAL=true, TRANSACTION_PROVIDER='" + transactionProvider + "'");
        }
        if (!mutable) {
            sb.append(",IMMUTABLE_ROWS=true");
            if (!columnEncoded) {
                sb.append(",IMMUTABLE_STORAGE_SCHEME="+PTableImpl.ImmutableStorageScheme.ONE_CELL_PER_COLUMN);
            }
        }
        this.tableDDLOptions = sb.toString();
        this.userTableNamespaceMapped = userTableNamespaceMapped;
        this.columnEncoded = columnEncoded;
        this.mutable = mutable;
        this.collectStatsOnSnapshot = collectStatsOnSnapshot;
    }

    @BeforeClass
    public static synchronized void doSetup() throws Exception {
        // disable name space mapping at global level on both client and server side
        Map<String, String> serverProps = Maps.newHashMapWithExpectedSize(7);
        serverProps.put(QueryServices.IS_NAMESPACE_MAPPING_ENABLED, Boolean.FALSE.toString());
        serverProps.put(QueryServices.STATS_GUIDEPOST_WIDTH_BYTES_ATTRIB, Long.toString(defaultGuidePostWidth));
        Map<String, String> clientProps = Maps.newHashMapWithExpectedSize(2);
        clientProps.put(QueryServices.IS_NAMESPACE_MAPPING_ENABLED, Boolean.FALSE.toString());
        clientProps.put(QueryServices.STATS_GUIDEPOST_WIDTH_BYTES_ATTRIB, Long.toString(defaultGuidePostWidth));
        setUpTestDriver(new ReadOnlyProps(serverProps.entrySet().iterator()), new ReadOnlyProps(clientProps.entrySet().iterator()));
    }
    
    @Before
    public void generateTableNames() throws SQLException {
        schemaName = generateUniqueName();
        if (userTableNamespaceMapped) {
            try (Connection conn = getConnection()) {
                conn.createStatement().execute("CREATE SCHEMA " + schemaName);
            }
        }
        tableName = "T_" + generateUniqueName();
        fullTableName = SchemaUtil.getTableName(schemaName, tableName);
        physicalTableName = SchemaUtil.getPhysicalHBaseTableName(schemaName, tableName, userTableNamespaceMapped).getString();
    }

    private Connection getConnection() throws SQLException {
        return getConnection(Integer.MAX_VALUE);
    }

    private Connection getConnection(Integer statsUpdateFreq) throws SQLException {
        Properties props = PropertiesUtil.deepCopy(TEST_PROPERTIES);
        props.setProperty(QueryServices.EXPLAIN_CHUNK_COUNT_ATTRIB, Boolean.TRUE.toString());
        props.setProperty(QueryServices.EXPLAIN_ROW_COUNT_ATTRIB, Boolean.TRUE.toString());
        props.setProperty(QueryServices.STATS_UPDATE_FREQ_MS_ATTRIB, Integer.toString(statsUpdateFreq));
        props.setProperty(QueryServices.IS_NAMESPACE_MAPPING_ENABLED, Boolean.toString(userTableNamespaceMapped));
        return DriverManager.getConnection(getUrl(), props);
    }

    private void collectStatistics(Connection conn, String fullTableName) throws Exception {
        collectStatistics(conn, fullTableName, null);
    }

    private void collectStatistics(Connection conn, String fullTableName,
                                   String guidePostWidth) throws Exception {

        if (collectStatsOnSnapshot) {
            collectStatsOnSnapshot(conn, fullTableName, guidePostWidth);
            invalidateStats(conn, fullTableName);
        } else {
            String updateStatisticsSql = "UPDATE STATISTICS " + fullTableName;
            if (guidePostWidth != null) {
                updateStatisticsSql += " SET \"" + QueryServices.STATS_GUIDEPOST_WIDTH_BYTES_ATTRIB + "\" = " + guidePostWidth;
            }
            LOGGER.info("Running SQL to collect stats: " + updateStatisticsSql);
            conn.createStatement().execute(updateStatisticsSql);
        }
    }

    private void collectStatsOnSnapshot(Connection conn, String fullTableName,
                                        String guidePostWidth) throws Exception {
        if (guidePostWidth != null) {
            conn.createStatement().execute("ALTER TABLE " + fullTableName + " SET GUIDE_POSTS_WIDTH = " + guidePostWidth);
        }
        runUpdateStatisticsTool(fullTableName);
    }

    // Run UpdateStatisticsTool in foreground with manage snapshot option
    private void runUpdateStatisticsTool(String fullTableName) {
        UpdateStatisticsTool tool = new UpdateStatisticsTool();
        tool.setConf(utility.getConfiguration());
        String randomDir = getUtility().getRandomDir().toString();
        final String[] cmdArgs = getArgValues(fullTableName, randomDir);
        try {
            int status = tool.run(cmdArgs);
            assertEquals("MR Job should complete successfully", 0, status);
            Admin hBaseAdmin = utility.getAdmin();
            assertEquals("Snapshot should be automatically deleted when UpdateStatisticsTool has completed",
                    0, hBaseAdmin.listSnapshots(Pattern.compile(tool.getSnapshotName())).size());
        } catch (Exception e) {
            fail("Exception when running UpdateStatisticsTool for " + tableName + " Exception: " + e);
        } finally {
            Job job = tool.getJob();
            assertEquals("MR Job should have been configured with UPDATE_STATS job type",
                    job.getConfiguration().get(MAPREDUCE_JOB_TYPE), UPDATE_STATS.name());
        }
    }

    private String[] getArgValues(String fullTableName, String randomDir) {
        final List<String> args = Lists.newArrayList();
        args.add("-t");
        args.add(fullTableName);
        args.add("-d");
        args.add(randomDir);
        args.add("-runfg");
        args.add("-ms");
        return args.toArray(new String[0]);
    }

    @Test
    public void testUpdateEmptyStats() throws Exception {
        Connection conn = getConnection();
        conn.setAutoCommit(true);
        conn.createStatement().execute(
                "CREATE TABLE " + fullTableName +" ( k CHAR(1) PRIMARY KEY )"  + tableDDLOptions);
        collectStatistics(conn, fullTableName);
        ExplainPlan plan = conn.prepareStatement("SELECT * FROM " + fullTableName)
            .unwrap(PhoenixPreparedStatement.class).optimizeQuery().getExplainPlan();
        ExplainPlanAttributes planAttributes = plan.getPlanStepsAsAttributes();
        assertEquals(1, (int) planAttributes.getSplitsChunk());
        assertEquals(0, (long) planAttributes.getEstimatedRows());
        assertEquals(20, (long) planAttributes.getEstimatedSizeInBytes());
        assertEquals("PARALLEL 1-WAY", planAttributes.getIteratorTypeAndScanSize());
        assertEquals("FULL SCAN ", planAttributes.getExplainScanType());
        assertEquals(physicalTableName, planAttributes.getTableName());
        assertEquals("SERVER FILTER BY " + (columnEncoded ? "FIRST KEY ONLY" :
                "EMPTY COLUMN ONLY"), planAttributes.getServerWhereFilter());
        conn.close();
    }

    @Test
    public void testSomeUpdateEmptyStats() throws Exception {
        Connection conn = getConnection();
        conn.setAutoCommit(true);
        conn.createStatement().execute(
                "CREATE TABLE " + fullTableName +" ( k VARCHAR PRIMARY KEY, a.v1 VARCHAR, b.v2 VARCHAR ) " + tableDDLOptions + (tableDDLOptions.isEmpty() ? "" : ",") + "SALT_BUCKETS = 3");
        conn.createStatement().execute("UPSERT INTO " + fullTableName + "(k,v1) VALUES('a','123456789')");
        collectStatistics(conn, fullTableName);
                
        // if we are using the ONE_CELL_PER_COLUMN_FAMILY storage scheme, we will have the single kv even though there are no values for col family v2

        ExplainPlan plan = conn.prepareStatement(
            "SELECT v2 FROM " + fullTableName + " WHERE v2='foo'")
            .unwrap(PhoenixPreparedStatement.class).optimizeQuery().getExplainPlan();
        ExplainPlanAttributes planAttributes = plan.getPlanStepsAsAttributes();
        assertEquals(columnEncoded && !mutable ? 4 : 3,
            (int) planAttributes.getSplitsChunk());
        assertEquals(columnEncoded && !mutable ? 1 : 0,
            (long) planAttributes.getEstimatedRows());
        assertEquals(columnEncoded && !mutable ? 38 : 20,
            (long) planAttributes.getEstimatedSizeInBytes());
        assertEquals("PARALLEL 3-WAY", planAttributes.getIteratorTypeAndScanSize());
        assertEquals("FULL SCAN ", planAttributes.getExplainScanType());
        assertEquals(physicalTableName, planAttributes.getTableName());
        assertEquals("SERVER FILTER BY B.V2 = 'foo'",
            planAttributes.getServerWhereFilter());
        assertEquals("CLIENT MERGE SORT", planAttributes.getClientSortAlgo());

        long estimatedSizeInBytes = columnEncoded ? 28
            : TransactionFactory.Provider.OMID.name().equals(transactionProvider)
            ? 38 : 34;
        plan = conn.prepareStatement(
            "SELECT * FROM " + fullTableName)
            .unwrap(PhoenixPreparedStatement.class).optimizeQuery().getExplainPlan();
        planAttributes = plan.getPlanStepsAsAttributes();
        assertEquals(4, (int) planAttributes.getSplitsChunk());
        assertEquals(1, (long) planAttributes.getEstimatedRows());
        assertEquals(estimatedSizeInBytes,
            (long) planAttributes.getEstimatedSizeInBytes());
        assertEquals("PARALLEL 3-WAY", planAttributes.getIteratorTypeAndScanSize());
        assertEquals("FULL SCAN ", planAttributes.getExplainScanType());
        assertEquals(physicalTableName, planAttributes.getTableName());
        assertNull(planAttributes.getServerWhereFilter());
        assertEquals("CLIENT MERGE SORT", planAttributes.getClientSortAlgo());

        plan = conn.prepareStatement(
            "SELECT * FROM " + fullTableName + " WHERE k = 'a'")
            .unwrap(PhoenixPreparedStatement.class).optimizeQuery().getExplainPlan();
        planAttributes = plan.getPlanStepsAsAttributes();
        assertEquals(1, (int) planAttributes.getSplitsChunk());
        assertEquals(1, (long) planAttributes.getEstimatedRows());
        assertEquals(columnEncoded ? 204 : 202,
            (long) planAttributes.getEstimatedSizeInBytes());
        assertEquals("PARALLEL 1-WAY", planAttributes.getIteratorTypeAndScanSize());
        assertEquals("POINT LOOKUP ON 1 KEY ", planAttributes.getExplainScanType());
        assertEquals(physicalTableName, planAttributes.getTableName());
        assertNull(planAttributes.getServerWhereFilter());
        assertEquals("CLIENT MERGE SORT", planAttributes.getClientSortAlgo());

        conn.close();
    }
    
    @Test
    public void testUpdateStats() throws Exception {
		Connection conn;
        PreparedStatement stmt;
        ResultSet rs;
        Properties props = PropertiesUtil.deepCopy(TEST_PROPERTIES);
        conn = getConnection();
        conn.createStatement().execute(
                "CREATE TABLE " + fullTableName +" ( k VARCHAR, a_string_array VARCHAR(100) ARRAY[4], b_string_array VARCHAR(100) ARRAY[4] \n"
                        + " CONSTRAINT pk PRIMARY KEY (k, b_string_array DESC))"
                		+ tableDDLOptions );
        String[] s;
        Array array;
        conn = upsertValues(props, fullTableName);
        collectStatistics(conn, fullTableName);
        rs = conn.createStatement().executeQuery("EXPLAIN SELECT k FROM " + fullTableName);
        rs.next();
        long rows1 = (Long) rs.getObject(PhoenixRuntime.EXPLAIN_PLAN_ESTIMATED_ROWS_READ_COLUMN);
        stmt = upsertStmt(conn, fullTableName);
        stmt.setString(1, "z");
        s = new String[] { "xyz", "def", "ghi", "jkll", null, null, "xxx" };
        array = conn.createArrayOf("VARCHAR", s);
        stmt.setArray(2, array);
        s = new String[] { "zya", "def", "ghi", "jkll", null, null, null, "xxx" };
        array = conn.createArrayOf("VARCHAR", s);
        stmt.setArray(3, array);
        stmt.execute();
        conn.commit();
        collectStatistics(conn, fullTableName);
        rs = conn.createStatement().executeQuery("EXPLAIN SELECT k FROM " + fullTableName);
        rs.next();
        long rows2 = (Long) rs.getObject(PhoenixRuntime.EXPLAIN_PLAN_ESTIMATED_ROWS_READ_COLUMN);
        assertNotEquals(rows1, rows2);
        conn.close();
    }

    private void testNoDuplicatesAfterUpdateStats(String splitKey) throws Throwable {
        Connection conn = getConnection();
        PreparedStatement stmt;
        ResultSet rs;
        conn.createStatement()
                .execute("CREATE TABLE " + fullTableName
                        + " ( k VARCHAR, c1.a bigint,c2.b bigint CONSTRAINT pk PRIMARY KEY (k))"+ tableDDLOptions
                        + (splitKey != null ? " split on (" + splitKey + ")" : "") );
        conn.createStatement().execute("upsert into " + fullTableName + " values ('abc',1,3)");
        conn.createStatement().execute("upsert into " + fullTableName + " values ('def',2,4)");
        conn.commit();
        collectStatistics(conn, fullTableName);
        rs = conn.createStatement().executeQuery("SELECT k FROM " + fullTableName + " order by k desc");
        assertTrue(rs.next());
        assertEquals("def", rs.getString(1));
        assertTrue(rs.next());
        assertEquals("abc", rs.getString(1));
        assertTrue(!rs.next());
        conn.close();
    }

    @Test
    public void testNoDuplicatesAfterUpdateStatsWithSplits() throws Throwable {
        testNoDuplicatesAfterUpdateStats("'abc','def'");
    }

    @Test
    public void testNoDuplicatesAfterUpdateStatsWithDesc() throws Throwable {
        testNoDuplicatesAfterUpdateStats(null);
    }

    private Connection upsertValues(Properties props, String tableName) throws SQLException, IOException,
            InterruptedException {
        Connection conn;
        PreparedStatement stmt;
        conn = getConnection();
        stmt = upsertStmt(conn, tableName);
        stmt.setString(1, "a");
        String[] s = new String[] { "abc", "def", "ghi", "jkll", null, null, "xxx" };
        Array array = conn.createArrayOf("VARCHAR", s);
        stmt.setArray(2, array);
        s = new String[] { "abc", "def", "ghi", "jkll", null, null, null, "xxx" };
        array = conn.createArrayOf("VARCHAR", s);
        stmt.setArray(3, array);
        stmt.execute();
        conn.commit();
        stmt = upsertStmt(conn, tableName);
        stmt.setString(1, "b");
        s = new String[] { "xyz", "def", "ghi", "jkll", null, null, "xxx" };
        array = conn.createArrayOf("VARCHAR", s);
        stmt.setArray(2, array);
        s = new String[] { "zya", "def", "ghi", "jkll", null, null, null, "xxx" };
        array = conn.createArrayOf("VARCHAR", s);
        stmt.setArray(3, array);
        stmt.execute();
        conn.commit();
        stmt = upsertStmt(conn, tableName);
        stmt.setString(1, "c");
        s = new String[] { "xyz", "def", "ghi", "jkll", null, null, "xxx" };
        array = conn.createArrayOf("VARCHAR", s);
        stmt.setArray(2, array);
        s = new String[] { "zya", "def", "ghi", "jkll", null, null, null, "xxx" };
        array = conn.createArrayOf("VARCHAR", s);
        stmt.setArray(3, array);
        stmt.execute();
        conn.commit();
        stmt = upsertStmt(conn, tableName);
        stmt.setString(1, "d");
        s = new String[] { "xyz", "def", "ghi", "jkll", null, null, "xxx" };
        array = conn.createArrayOf("VARCHAR", s);
        stmt.setArray(2, array);
        s = new String[] { "zya", "def", "ghi", "jkll", null, null, null, "xxx" };
        array = conn.createArrayOf("VARCHAR", s);
        stmt.setArray(3, array);
        stmt.execute();
        conn.commit();
        stmt = upsertStmt(conn, tableName);
        stmt.setString(1, "b");
        s = new String[] { "xyz", "def", "ghi", "jkll", null, null, "xxx" };
        array = conn.createArrayOf("VARCHAR", s);
        stmt.setArray(2, array);
        s = new String[] { "zya", "def", "ghi", "jkll", null, null, null, "xxx" };
        array = conn.createArrayOf("VARCHAR", s);
        stmt.setArray(3, array);
        stmt.execute();
        conn.commit();
        stmt = upsertStmt(conn, tableName);
        stmt.setString(1, "e");
        s = new String[] { "xyz", "def", "ghi", "jkll", null, null, "xxx" };
        array = conn.createArrayOf("VARCHAR", s);
        stmt.setArray(2, array);
        s = new String[] { "zya", "def", "ghi", "jkll", null, null, null, "xxx" };
        array = conn.createArrayOf("VARCHAR", s);
        stmt.setArray(3, array);
        stmt.execute();
        conn.commit();
        return conn;
    }

    private PreparedStatement upsertStmt(Connection conn, String tableName) throws SQLException {
        PreparedStatement stmt;
        stmt = conn.prepareStatement("UPSERT INTO " + tableName + " VALUES(?,?,?)");
        return stmt;
    }

    @Test
    @Ignore //TODO remove this once  https://issues.apache.org/jira/browse/TEPHRA-208 is fixed
    public void testCompactUpdatesStats() throws Exception {
        testCompactUpdatesStats(0, fullTableName);
    }
    
    @Test
    @Ignore //TODO remove this once  https://issues.apache.org/jira/browse/TEPHRA-208 is fixed
    public void testCompactUpdatesStatsWithMinStatsUpdateFreq() throws Exception {
        testCompactUpdatesStats(QueryServicesOptions.DEFAULT_STATS_UPDATE_FREQ_MS, fullTableName);
    }
    
    private static void invalidateStats(Connection conn, String tableName) throws SQLException {
        PTable ptable = conn.unwrap(PhoenixConnection.class)
                .getMetaDataCache().getTableRef(new PTableKey(null, tableName))
                .getTable();
        byte[] name = ptable.getPhysicalName().getBytes();
        conn.unwrap(PhoenixConnection.class).getQueryServices().invalidateStats(new GuidePostsKey(name, SchemaUtil.getEmptyColumnFamily(ptable)));
    }
    
    private void testCompactUpdatesStats(Integer statsUpdateFreq, String tableName) throws Exception {
        int nRows = 10;
        Connection conn = getConnection(statsUpdateFreq);
        PreparedStatement stmt;
        conn.createStatement().execute("CREATE TABLE " + tableName + "(k CHAR(1) PRIMARY KEY, v INTEGER, w INTEGER) "
                + (!tableDDLOptions.isEmpty() ? tableDDLOptions + "," : "") 
                + ColumnFamilyDescriptorBuilder.KEEP_DELETED_CELLS + "=" + Boolean.FALSE);
        stmt = conn.prepareStatement("UPSERT INTO " + tableName + " VALUES(?,?,?)");
        for (int i = 0; i < nRows; i++) {
            stmt.setString(1, Character.toString((char) ('a' + i)));
            stmt.setInt(2, i);
            stmt.setInt(3, i);
            stmt.executeUpdate();
        }
        conn.commit();

        TestUtil.doMajorCompaction(conn, physicalTableName);
        
        if (statsUpdateFreq != 0) {
            invalidateStats(conn, tableName);
        } else {
            // Confirm that when we have a non zero STATS_UPDATE_FREQ_MS_ATTRIB, after we run
            // UPDATATE STATISTICS, the new statistics are faulted in as expected.
            List<KeyRange>keyRanges = getAllSplits(conn, tableName);
            assertNotEquals(nRows+1, keyRanges.size());
            // If we've set MIN_STATS_UPDATE_FREQ_MS_ATTRIB, an UPDATE STATISTICS will invalidate the cache
            // and forcing the new stats to be pulled over.
            int rowCount = conn.createStatement().executeUpdate("UPDATE STATISTICS " + tableName);
            assertEquals(10, rowCount);
        }
        List<KeyRange>keyRanges = getAllSplits(conn, tableName);
        assertEquals(nRows+1, keyRanges.size());
        
        int nDeletedRows = conn.createStatement().executeUpdate("DELETE FROM " + tableName + " WHERE V < " + nRows / 2);
        conn.commit();
        assertEquals(5, nDeletedRows);
        
        Scan scan = new Scan();
        scan.setRaw(true);
        PhoenixConnection phxConn = conn.unwrap(PhoenixConnection.class);
        try (Table htable = phxConn.getQueryServices().getTable(Bytes.toBytes(tableName))) {
            ResultScanner scanner = htable.getScanner(scan);
            Result result;
            while ((result = scanner.next())!=null) {
                System.out.println(result);
            }
        }

        TestUtil.doMajorCompaction(conn, physicalTableName);
        
        scan = new Scan();
        scan.setRaw(true);
        phxConn = conn.unwrap(PhoenixConnection.class);
        try (Table htable = phxConn.getQueryServices().getTable(Bytes.toBytes(tableName))) {
            ResultScanner scanner = htable.getScanner(scan);
            Result result;
            while ((result = scanner.next())!=null) {
                System.out.println(result);
            }
        }
        
        if (statsUpdateFreq != 0) {
            invalidateStats(conn, tableName);
        } else {
            assertEquals(nRows+1, keyRanges.size());
            // If we've set STATS_UPDATE_FREQ_MS_ATTRIB, an UPDATE STATISTICS will invalidate the cache
            // and force us to pull over the new stats
            int rowCount = conn.createStatement().executeUpdate("UPDATE STATISTICS " + tableName);
            assertEquals(5, rowCount);
        }
        keyRanges = getAllSplits(conn, tableName);
        assertEquals(nRows/2+1, keyRanges.size());
        ResultSet rs = conn.createStatement().executeQuery("SELECT SUM(GUIDE_POSTS_ROW_COUNT) FROM "
                + "\""+ SYSTEM_CATALOG_SCHEMA + "\".\"" + SYSTEM_STATS_TABLE + "\"" + " WHERE PHYSICAL_NAME='" + physicalTableName + "'");
        rs.next();
        assertEquals(nRows - nDeletedRows, rs.getLong(1));
    }

    @Test
    public void testWithMultiCF() throws Exception {
        int nRows = 20;
        Connection conn = getConnection(0);
        PreparedStatement stmt;
        conn.createStatement().execute(
                "CREATE TABLE " + fullTableName
                        + "(k VARCHAR PRIMARY KEY, a.v INTEGER, b.v INTEGER, c.v INTEGER NULL, d.v INTEGER NULL) "
                        + tableDDLOptions );
        stmt = conn.prepareStatement("UPSERT INTO " + fullTableName + " VALUES(?,?, ?, ?, ?)");
        int queryTimeout = conn.unwrap(PhoenixConnection.class).getQueryServices().getProps()
                .getInt(QueryServices.THREAD_TIMEOUT_MS_ATTRIB,
                        QueryServicesOptions.DEFAULT_THREAD_TIMEOUT_MS);
        byte[] val = new byte[250];
        for (int i = 0; i < nRows; i++) {
            stmt.setString(1, Character.toString((char)('a' + i)) + Bytes.toString(val));
            stmt.setInt(2, i);
            stmt.setInt(3, i);
            stmt.setInt(4, i);
            stmt.setInt(5, i);
            stmt.executeUpdate();
        }
        conn.commit();
        stmt = conn.prepareStatement("UPSERT INTO " + fullTableName + "(k, c.v, d.v) VALUES(?,?,?)");
        for (int i = 0; i < 5; i++) {
            stmt.setString(1, Character.toString((char)('a' + 'z' + i)) + Bytes.toString(val));
            stmt.setInt(2, i);
            stmt.setInt(3, i);
            stmt.executeUpdate();
        }
        conn.commit();

        ResultSet rs;
        collectStatistics(conn, fullTableName);
        List<KeyRange> keyRanges = getAllSplits(conn, fullTableName);
        assertEquals(26, keyRanges.size());

        long sizeInBytes = columnEncoded ? (mutable ? 12530 : 13902)
            : (TransactionFactory.Provider.OMID.name().equals(transactionProvider))
            ? 25044 : 12420;

        ExplainPlan plan = conn.prepareStatement(
            "SELECT * FROM " + fullTableName)
            .unwrap(PhoenixPreparedStatement.class).optimizeQuery().getExplainPlan();
        ExplainPlanAttributes planAttributes = plan.getPlanStepsAsAttributes();
        assertEquals(26, (int) planAttributes.getSplitsChunk());
        assertEquals(25, (long) planAttributes.getEstimatedRows());
        assertEquals(sizeInBytes,
            (long) planAttributes.getEstimatedSizeInBytes());
        assertEquals("PARALLEL 1-WAY",
            planAttributes.getIteratorTypeAndScanSize());
        assertEquals("FULL SCAN ", planAttributes.getExplainScanType());
        assertEquals(physicalTableName, planAttributes.getTableName());

        ConnectionQueryServices services = conn.unwrap(PhoenixConnection.class).getQueryServices();
        List<HRegionLocation> regions =
                services.getAllTableRegions(Bytes.toBytes(physicalTableName), queryTimeout);
        assertEquals(1, regions.size());

        collectStatistics(conn, fullTableName, Long.toString(1000));
        keyRanges = getAllSplits(conn, fullTableName);
        boolean oneCellPerColFamliyStorageScheme = !mutable && columnEncoded;
        boolean hasShadowCells = TransactionFactory.Provider.OMID.name().equals(transactionProvider);
        assertEquals(oneCellPerColFamliyStorageScheme ? 13 : hasShadowCells ? 23 : 12, keyRanges.size());

        rs = conn
                .createStatement()
                .executeQuery(
                        "SELECT COLUMN_FAMILY,SUM(GUIDE_POSTS_ROW_COUNT),SUM(GUIDE_POSTS_WIDTH),COUNT(*) from \"SYSTEM\".STATS where PHYSICAL_NAME = '"
                                + physicalTableName + "' GROUP BY COLUMN_FAMILY ORDER BY COLUMN_FAMILY");

        assertTrue(rs.next());
        assertEquals("A", rs.getString(1));
        assertEquals(24, rs.getInt(2));
        assertEquals(columnEncoded ? ( mutable ? 12252 : 13624 ) : hasShadowCells ? 24756 : 12144, rs.getInt(3));
        assertEquals(oneCellPerColFamliyStorageScheme ? 12 : hasShadowCells ? 22 : 11, rs.getInt(4));

        assertTrue(rs.next());
        assertEquals("B", rs.getString(1));
        assertEquals(oneCellPerColFamliyStorageScheme ? 24 : 20, rs.getInt(2));
        assertEquals(columnEncoded ? ( mutable ? 5600 : 6972 ) : hasShadowCells ? 11260 : 5540, rs.getInt(3));
        assertEquals(oneCellPerColFamliyStorageScheme ? 6 : hasShadowCells ? 10 : 5, rs.getInt(4));

        assertTrue(rs.next());
        assertEquals("C", rs.getString(1));
        assertEquals(24, rs.getInt(2));
        assertEquals(columnEncoded ? ( mutable ? 6724 : 6988 ) : hasShadowCells ? 13520 : 6652, rs.getInt(3));
        assertEquals(hasShadowCells ? 12 : 6, rs.getInt(4));

        assertTrue(rs.next());
        assertEquals("D", rs.getString(1));
        assertEquals(24, rs.getInt(2));
        assertEquals(columnEncoded ? ( mutable ? 6724 : 6988 ) : hasShadowCells ? 13520 : 6652, rs.getInt(3));
        assertEquals(hasShadowCells ? 12 : 6, rs.getInt(4));

        assertFalse(rs.next());
        
        // Disable stats
        conn.createStatement().execute("ALTER TABLE " + fullTableName + 
                " SET " + PhoenixDatabaseMetaData.GUIDE_POSTS_WIDTH + "=0");
        collectStatistics(conn, fullTableName);
        // Assert that there are no more guideposts
        rs = conn.createStatement().executeQuery("SELECT count(1) FROM " + PhoenixDatabaseMetaData.SYSTEM_STATS_NAME + 
                " WHERE " + PhoenixDatabaseMetaData.PHYSICAL_NAME + "='" + physicalTableName + "' AND " + PhoenixDatabaseMetaData.COLUMN_FAMILY + " IS NOT NULL");
        assertTrue(rs.next());
        assertEquals(0, rs.getLong(1));
        assertFalse(rs.next());

        plan = conn.prepareStatement(
            "SELECT * FROM " + fullTableName)
            .unwrap(PhoenixPreparedStatement.class).optimizeQuery().getExplainPlan();
        planAttributes = plan.getPlanStepsAsAttributes();
        assertEquals(1, (int) planAttributes.getSplitsChunk());
        assertNull(planAttributes.getEstimatedRows());
        assertNull(planAttributes.getEstimatedSizeInBytes());
        assertEquals("PARALLEL 1-WAY",
          planAttributes.getIteratorTypeAndScanSize());
        assertEquals("FULL SCAN ", planAttributes.getExplainScanType());
        assertEquals(physicalTableName, planAttributes.getTableName());
    }

    @Test
    public void testRowCountAndByteCounts() throws Exception {
        Connection conn = getConnection();
        String ddl = "CREATE TABLE " + fullTableName + " (t_id VARCHAR NOT NULL,\n" + "k1 INTEGER NOT NULL,\n"
                + "k2 INTEGER NOT NULL,\n" + "C3.k3 INTEGER,\n" + "C2.v1 VARCHAR,\n"
                + "CONSTRAINT pk PRIMARY KEY (t_id, k1, k2)) " + tableDDLOptions + " split on ('e','j','o')";
        conn.createStatement().execute(ddl);
        String[] strings = { "a", "b", "c", "d", "e", "f", "g", "h", "i", "j", "k", "l", "m", "n", "o", "p", "q", "r",
                "s", "t", "u", "v", "w", "x", "y", "z" };
        for (int i = 0; i < 26; i++) {
            conn.createStatement().execute(
                    "UPSERT INTO " + fullTableName + " values('" + strings[i] + "'," + i + "," + (i + 1) + ","
                            + (i + 2) + ",'" + strings[25 - i] + "')");
        }
        conn.commit();
        ResultSet rs;
        collectStatistics(conn, fullTableName, Long.toString(20L));
        Random r = new Random();
        int count = 0;
        boolean hasShadowCells = TransactionFactory.Provider.OMID.name().equals(transactionProvider);
        while (count < 4) {
            int startIndex = r.nextInt(strings.length);
            int endIndex = r.nextInt(strings.length - startIndex) + startIndex;
            long rows = endIndex - startIndex;
            long c2Bytes = rows * (columnEncoded ? ( mutable ? 37 : 48 ) : 35);
            String physicalTableName = SchemaUtil.getPhysicalTableName(Bytes.toBytes(fullTableName), userTableNamespaceMapped).toString();
            rs = conn.createStatement().executeQuery(
                    "SELECT COLUMN_FAMILY,SUM(GUIDE_POSTS_ROW_COUNT),SUM(GUIDE_POSTS_WIDTH) from \"SYSTEM\".STATS where PHYSICAL_NAME = '"
                            + physicalTableName + "' AND GUIDE_POST_KEY>= cast('" + strings[startIndex]
                            + "' as varbinary) AND  GUIDE_POST_KEY<cast('" + strings[endIndex]
                            + "' as varbinary) and COLUMN_FAMILY='C2' group by COLUMN_FAMILY");
            if (startIndex < endIndex) {
                assertTrue(rs.next());
                assertEquals("C2", rs.getString(1));
                assertEquals(rows, rs.getLong(2));
                // OMID with the shadow cells it creates will have more bytes, but getting
                // an exact byte count based on the number or rows is not possible because
                // it is variable on a row-by-row basis.
                long sumOfGuidePostsWidth = rs.getLong(3);
                assertTrue(hasShadowCells ? sumOfGuidePostsWidth > c2Bytes : sumOfGuidePostsWidth == c2Bytes);
                count++;
            }
        }
    }

    @Test
    public void testRowCountWhenNumKVsExceedCompactionScannerThreshold() throws Exception {
        StringBuilder sb = new StringBuilder(200);
        sb.append("CREATE TABLE " + fullTableName + "(PK1 VARCHAR NOT NULL, ");
        int numRows = 10;
        try (Connection conn = getConnection()) {
            int compactionScannerKVThreshold =
                    conn.unwrap(PhoenixConnection.class).getQueryServices().getConfiguration()
                            .getInt(HConstants.COMPACTION_KV_MAX,
                                HConstants.COMPACTION_KV_MAX_DEFAULT);
            int numKvColumns = compactionScannerKVThreshold * 2;
            for (int i = 1; i <= numKvColumns; i++) {
                sb.append("KV" + i + " VARCHAR");
                if (i < numKvColumns) {
                    sb.append(", ");
                }
            }
            sb.append(" CONSTRAINT PK PRIMARY KEY (PK1))");
            String ddl = sb.toString();
            conn.createStatement().execute(ddl);
            sb = new StringBuilder(200);
            sb.append("UPSERT INTO " + fullTableName + " VALUES (");
            for (int i = 1; i <= numKvColumns + 1; i++) {
                sb.append("?");
                if (i < numKvColumns + 1) {
                    sb.append(", ");
                }
            }
            sb.append(")");
            String dml = sb.toString();
            PreparedStatement stmt = conn.prepareStatement(dml);
            String keyValue = "KVVVVVV";
            for (int j = 1; j <= numRows; j++) {
                for (int i = 1; i <= numKvColumns + 1; i++) {
                    if (i == 1) {
                        stmt.setString(1, "" + j);
                    } else {
                        stmt.setString(i, keyValue);
                    }
                }
                stmt.executeUpdate();
            }
            conn.commit();
            collectStatistics(conn, fullTableName);
            String q = "SELECT SUM(GUIDE_POSTS_ROW_COUNT) FROM SYSTEM.STATS WHERE PHYSICAL_NAME = '" + physicalTableName + "'";
            ResultSet rs = conn.createStatement().executeQuery(q);
            rs.next();
            assertEquals("Number of expected rows in stats table after update stats didn't match!", numRows, rs.getInt(1));
            conn.createStatement().executeUpdate("DELETE FROM SYSTEM.STATS WHERE PHYSICAL_NAME = '" + physicalTableName + "'");
            conn.commit();
            TestUtil.doMajorCompaction(conn, physicalTableName);
            q = "SELECT SUM(GUIDE_POSTS_ROW_COUNT) FROM SYSTEM.STATS WHERE PHYSICAL_NAME = '" + physicalTableName + "'";
            rs = conn.createStatement().executeQuery(q);
            rs.next();
            assertEquals("Number of expected rows in stats table after major compaction didn't match", numRows, rs.getInt(1));
        }
    }

    private void verifyGuidePostGenerated(ConnectionQueryServices queryServices,
            String tableName, String[] familyNames,
            long guidePostWidth, boolean emptyGuidePostExpected) throws Exception {
        try (Table statsHTable =
                queryServices.getTable(
                        SchemaUtil.getPhysicalName(PhoenixDatabaseMetaData.SYSTEM_STATS_NAME_BYTES,
                                queryServices.getProps()).getName())) {
            for (String familyName : familyNames) {
                GuidePostsInfo gps =
                        StatisticsUtil.readStatistics(statsHTable,
                                new GuidePostsKey(Bytes.toBytes(tableName), Bytes.toBytes(familyName)),
                                HConstants.LATEST_TIMESTAMP);
                assertTrue(emptyGuidePostExpected ? gps.isEmptyGuidePost() : !gps.isEmptyGuidePost());
                assertTrue(gps.getByteCounts()[0] >= guidePostWidth);
                assertTrue(gps.getGuidePostTimestamps()[0] > 0);
            }
        }
    }
    
    @Test
    public void testEmptyGuidePostGeneratedWhenDataSizeLessThanGPWidth() throws Exception {
        try (Connection conn = getConnection()) {
            long guidePostWidth = 20000000;
            conn.createStatement()
                    .execute("CREATE TABLE " + fullTableName
                            + " ( k INTEGER, c1.a bigint,c2.b bigint CONSTRAINT pk PRIMARY KEY (k)) GUIDE_POSTS_WIDTH="
                            + guidePostWidth + ", SALT_BUCKETS = 4");
            conn.createStatement().execute("upsert into " + fullTableName + " values (100,1,3)");
            conn.createStatement().execute("upsert into " + fullTableName + " values (101,2,4)");
            conn.commit();
            collectStatistics(conn, fullTableName);
            ConnectionQueryServices queryServices =
                    conn.unwrap(PhoenixConnection.class).getQueryServices();
            verifyGuidePostGenerated(queryServices, physicalTableName, new String[] {"C1", "C2"}, guidePostWidth, true);
        }
    }

    @Test
    public void testCollectingAllVersionsOfCells() throws Exception {
        try (Connection conn = getConnection()) {
            long guidePostWidth = 70;
            String ddl =
                    "CREATE TABLE " + fullTableName + " (k INTEGER PRIMARY KEY, c1.a bigint, c2.b bigint)"
                            + " GUIDE_POSTS_WIDTH=" + guidePostWidth
                            + ", USE_STATS_FOR_PARALLELIZATION=true" + ", VERSIONS=3";
            conn.createStatement().execute(ddl);
            conn.createStatement().execute("upsert into " + fullTableName + " values (100,100,3)");
            conn.commit();
            collectStatistics(conn, fullTableName);

            ConnectionQueryServices queryServices =
                    conn.unwrap(PhoenixConnection.class).getQueryServices();

            // The table only has one row. All cells just has one version, and the data size of the row
            // is less than the guide post width, so we generate empty guide post.
            verifyGuidePostGenerated(queryServices, physicalTableName, new String[] {"C1", "C2"}, guidePostWidth, true);

            conn.createStatement().execute("upsert into " + fullTableName + " values (100,101,4)");
            conn.commit();
            collectStatistics(conn, fullTableName);

            // We updated the row. Now each cell has two versions, and the data size of the row
            // is >= the guide post width, so we generate non-empty guide post.
            verifyGuidePostGenerated(queryServices, physicalTableName, new String[] {"C1", "C2"}, guidePostWidth, false);
        }
    }

    @Test
    public void testGuidePostWidthUsedInDefaultStatsCollector() throws Exception {
        String baseTable = generateUniqueName();
        try (Connection conn = DriverManager.getConnection(getUrl())) {
            String ddl =
                    "CREATE TABLE " + baseTable
                            + " (k INTEGER PRIMARY KEY, a bigint, b bigint, c bigint) "
                            + tableDDLOptions;
            BaseTest.createTestTable(getUrl(), ddl, null, null);
            conn.createStatement().execute("upsert into " + baseTable + " values (100,1,1,1)");
            conn.createStatement().execute("upsert into " + baseTable + " values (101,2,2,2)");
            conn.createStatement().execute("upsert into " + baseTable + " values (102,3,3,3)");
            conn.createStatement().execute("upsert into " + baseTable + " values (103,4,4,4)");
            conn.createStatement().execute("upsert into " + baseTable + " values (104,5,5,5)");
            conn.createStatement().execute("upsert into " + baseTable + " values (105,6,6,6)");
            conn.createStatement().execute("upsert into " + baseTable + " values (106,7,7,7)");
            conn.createStatement().execute("upsert into " + baseTable + " values (107,8,8,8)");
            conn.createStatement().execute("upsert into " + baseTable + " values (108,9,9,9)");
            conn.createStatement().execute("upsert into " + baseTable + " values (109,10,10,10)");
            conn.commit();
            DefaultStatisticsCollector statsCollector = getDefaultStatsCollectorForTable(baseTable);
            statsCollector.init();
            assertEquals(defaultGuidePostWidth, statsCollector.getGuidePostDepth());

            // ok let's create a global index now and see what guide post width is used for it
            String globalIndex = "GI_" + generateUniqueName();
            ddl = "CREATE INDEX " + globalIndex + " ON " + baseTable + " (a) INCLUDE (b) ";
            conn.createStatement().execute(ddl);
            statsCollector = getDefaultStatsCollectorForTable(globalIndex);
            statsCollector.init();
            assertEquals(defaultGuidePostWidth, statsCollector.getGuidePostDepth());

            // let's check out local index too
            if (transactionProvider == null ||
                    !TransactionFactory.getTransactionProvider(
                            TransactionFactory.Provider.valueOf(transactionProvider)).isUnsupported(Feature.ALLOW_LOCAL_INDEX)) {
                String localIndex = "LI_" + generateUniqueName();
                ddl = "CREATE LOCAL INDEX " + localIndex + " ON " + baseTable + " (b) INCLUDE (c) ";
                conn.createStatement().execute(ddl);
                // local indexes reside on the same table as base data table
                statsCollector = getDefaultStatsCollectorForTable(baseTable);
                statsCollector.init();
                assertEquals(defaultGuidePostWidth, statsCollector.getGuidePostDepth());
            }

            // now let's create a view and an index on it and see what guide post width is used for
            // it
            String view = "V_" + generateUniqueName();
            ddl = "CREATE VIEW " + view + " AS SELECT * FROM " + baseTable;
            conn.createStatement().execute(ddl);
            String viewIndex = "VI_" + generateUniqueName();
            ddl = "CREATE INDEX " + viewIndex + " ON " + view + " (b)";
            conn.createStatement().execute(ddl);
            String viewIndexTableName = MetaDataUtil.getViewIndexPhysicalName(baseTable);
            statsCollector = getDefaultStatsCollectorForTable(viewIndexTableName);
            statsCollector.init();
            assertEquals(defaultGuidePostWidth, statsCollector.getGuidePostDepth());
            /*
             * Fantastic! Now let's change the guide post width of the base table. This should
             * change the guide post width we are using in DefaultStatisticsCollector for all
             * indexes too.
             */
            long newGpWidth = 500;
            conn.createStatement()
                    .execute("ALTER TABLE " + baseTable + " SET GUIDE_POSTS_WIDTH=" + newGpWidth);

            // base table and local index
            statsCollector = getDefaultStatsCollectorForTable(baseTable);
            statsCollector.init();
            assertEquals(newGpWidth, statsCollector.getGuidePostDepth());

            // global index table
            statsCollector = getDefaultStatsCollectorForTable(globalIndex);
            statsCollector.init();
            assertEquals(newGpWidth, statsCollector.getGuidePostDepth());

            // view index table
            statsCollector = getDefaultStatsCollectorForTable(viewIndexTableName);
            statsCollector.init();
            assertEquals(newGpWidth, statsCollector.getGuidePostDepth());
        }
    }

    private DefaultStatisticsCollector getDefaultStatsCollectorForTable(String tableName)
            throws Exception {
        RegionCoprocessorEnvironment env = getRegionEnvrionment(tableName);
        return (DefaultStatisticsCollector) StatisticsCollectorFactory
                .createStatisticsCollector(env, tableName, System.currentTimeMillis(), null, null);
    }

    private RegionCoprocessorEnvironment getRegionEnvrionment(String tableName)
            throws IOException, InterruptedException {
        return getUtility().getMiniHBaseCluster().getRegions(TableName.valueOf(tableName)).get(0)
                .getCoprocessorHost()
                .findCoprocessorEnvironment(UngroupedAggregateRegionObserver.class.getName());
    }
}
