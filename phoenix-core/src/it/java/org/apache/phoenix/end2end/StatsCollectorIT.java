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

import static org.apache.phoenix.util.TestUtil.TEST_PROPERTIES;
import static org.apache.phoenix.util.TestUtil.getAllSplits;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.sql.Array;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Random;

import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HRegionLocation;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.phoenix.hbase.index.util.ImmutableBytesPtr;
import org.apache.phoenix.jdbc.PhoenixConnection;
import org.apache.phoenix.jdbc.PhoenixDatabaseMetaData;
import org.apache.phoenix.query.ConnectionQueryServices;
import org.apache.phoenix.query.KeyRange;
import org.apache.phoenix.query.QueryServices;
import org.apache.phoenix.query.QueryServicesOptions;
import org.apache.phoenix.util.PropertiesUtil;
import org.apache.phoenix.util.QueryUtil;
import org.apache.phoenix.util.ReadOnlyProps;
import org.apache.phoenix.util.SchemaUtil;
import org.apache.phoenix.util.TestUtil;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;

import com.google.common.collect.Maps;

@RunWith(Parameterized.class)
public class StatsCollectorIT extends ParallelStatsEnabledIT {
    private final String tableDDLOptions;
    private String tableName;
    private String schemaName;
    private String fullTableName;
        
    @BeforeClass
    @Shadower(classBeingShadowed = ParallelStatsEnabledIT.class)
    public static void doSetup() throws Exception {
        Map<String,String> props = Maps.newHashMapWithExpectedSize(10);
        // Must update config before starting server
        props.put(QueryServices.STATS_GUIDEPOST_WIDTH_BYTES_ATTRIB, Long.toString(20));
        props.put(QueryServices.EXPLAIN_CHUNK_COUNT_ATTRIB, Boolean.TRUE.toString());
        props.put(QueryServices.EXPLAIN_ROW_COUNT_ATTRIB, Boolean.TRUE.toString());
        props.put(QueryServices.TRANSACTIONS_ENABLED, Boolean.toString(true));
        props.put(QueryServices.STATS_UPDATE_FREQ_MS_ATTRIB, Long.toString(Long.MAX_VALUE));
        setUpTestDriver(new ReadOnlyProps(props.entrySet().iterator()));
    }
    
    public StatsCollectorIT( boolean transactional) {
        this.tableDDLOptions= transactional ? " TRANSACTIONAL=true" : "";
    }
    
    @Before
    public void generateTableNames() {
        schemaName = TestUtil.DEFAULT_SCHEMA_NAME;
        tableName = "T_" + generateUniqueName();
        fullTableName = SchemaUtil.getTableName(schemaName, tableName);
    }

    @Parameters(name="transactional = {0}")
    public static Collection<Boolean> data() {
        return Arrays.asList(false,true);
    }

    @Test
    public void testUpdateEmptyStats() throws Exception {
        Connection conn;
        Properties props = PropertiesUtil.deepCopy(TEST_PROPERTIES);
        conn = DriverManager.getConnection(getUrl(), props);
        conn.setAutoCommit(true);
        conn.createStatement().execute(
                "CREATE TABLE " + fullTableName +" ( k CHAR(1) PRIMARY KEY )"  + tableDDLOptions);
        conn.createStatement().execute("UPDATE STATISTICS " + tableName);
        ResultSet rs = conn.createStatement().executeQuery("EXPLAIN SELECT * FROM " + fullTableName);
        String explainPlan = QueryUtil.getExplainPlan(rs);
        assertEquals(
                "CLIENT 1-CHUNK 0 ROWS 0 BYTES PARALLEL 1-WAY FULL SCAN OVER " + fullTableName + "\n" + 
                "    SERVER FILTER BY FIRST KEY ONLY",
                explainPlan);
        conn.close();
    }
    
    @Test
    public void testSomeUpdateEmptyStats() throws Exception {
        Connection conn;
        Properties props = PropertiesUtil.deepCopy(TEST_PROPERTIES);
        conn = DriverManager.getConnection(getUrl(), props);
        conn.setAutoCommit(true);
        conn.createStatement().execute(
                "CREATE TABLE " + fullTableName +" ( k VARCHAR PRIMARY KEY, a.v1 VARCHAR, b.v2 VARCHAR ) " + tableDDLOptions + (tableDDLOptions.isEmpty() ? "" : ",") + "SALT_BUCKETS = 3");
        conn.createStatement().execute("UPSERT INTO " + fullTableName + "(k,v1) VALUES('a','123456789')");
        conn.createStatement().execute("UPDATE STATISTICS " + fullTableName);
        ResultSet rs;
        String explainPlan;
        rs = conn.createStatement().executeQuery("EXPLAIN SELECT v2 FROM " + fullTableName + " WHERE v2='foo'");
        explainPlan = QueryUtil.getExplainPlan(rs);
        assertEquals(
                "CLIENT 3-CHUNK 0 ROWS 0 BYTES PARALLEL 3-WAY FULL SCAN OVER " + fullTableName + "\n" +
                "    SERVER FILTER BY B.V2 = 'foo'\n" + 
                "CLIENT MERGE SORT",
                explainPlan);
        rs = conn.createStatement().executeQuery("EXPLAIN SELECT * FROM " + fullTableName);
        explainPlan = QueryUtil.getExplainPlan(rs);
        assertEquals(
                "CLIENT 4-CHUNK 1 ROWS 34 BYTES PARALLEL 3-WAY FULL SCAN OVER " + fullTableName + "\n" +
                "CLIENT MERGE SORT",
                explainPlan);
        rs = conn.createStatement().executeQuery("EXPLAIN SELECT * FROM " + fullTableName + " WHERE k = 'a'");
        explainPlan = QueryUtil.getExplainPlan(rs);
        assertEquals(
                "CLIENT 1-CHUNK 1 ROWS 202 BYTES PARALLEL 1-WAY POINT LOOKUP ON 1 KEY OVER " + fullTableName + "\n" +
                "CLIENT MERGE SORT",
                explainPlan);
        
        conn.close();
    }
    
    @Test
    public void testUpdateStats() throws SQLException, IOException,
			InterruptedException {
		Connection conn;
        PreparedStatement stmt;
        ResultSet rs;
        Properties props = PropertiesUtil.deepCopy(TEST_PROPERTIES);
        conn = DriverManager.getConnection(getUrl(), props);
        conn.createStatement().execute(
                "CREATE TABLE " + fullTableName +" ( k VARCHAR, a_string_array VARCHAR(100) ARRAY[4], b_string_array VARCHAR(100) ARRAY[4] \n"
                        + " CONSTRAINT pk PRIMARY KEY (k, b_string_array DESC))"
                		+ tableDDLOptions );
        String[] s;
        Array array;
        conn = upsertValues(props, tableName);
        // CAll the update statistics query here. If already major compaction has run this will not get executed.
        stmt = conn.prepareStatement("UPDATE STATISTICS " + tableName);
        stmt.execute();
        stmt = upsertStmt(conn, tableName);
        stmt.setString(1, "z");
        s = new String[] { "xyz", "def", "ghi", "jkll", null, null, "xxx" };
        array = conn.createArrayOf("VARCHAR", s);
        stmt.setArray(2, array);
        s = new String[] { "zya", "def", "ghi", "jkll", null, null, null, "xxx" };
        array = conn.createArrayOf("VARCHAR", s);
        stmt.setArray(3, array);
        stmt.execute();
        conn.close();
        conn = DriverManager.getConnection(getUrl(), props);
        // This analyze would not work
        stmt = conn.prepareStatement("UPDATE STATISTICS " + tableName);
        stmt.execute();
        rs = conn.createStatement().executeQuery("SELECT k FROM " + tableName);
        assertTrue(rs.next());
        conn.close();
    }

    private void testNoDuplicatesAfterUpdateStats(String splitKey) throws Throwable {
        Connection conn;
        PreparedStatement stmt;
        ResultSet rs;
        Properties props = PropertiesUtil.deepCopy(TEST_PROPERTIES);
        conn = DriverManager.getConnection(getUrl(), props);
        conn.createStatement()
                .execute("CREATE TABLE " + fullTableName
                        + " ( k VARCHAR, c1.a bigint,c2.b bigint CONSTRAINT pk PRIMARY KEY (k))"+ tableDDLOptions
                        + (splitKey != null ? " split on (" + splitKey + ")" : "") );
        conn.createStatement().execute("upsert into " + fullTableName + " values ('abc',1,3)");
        conn.createStatement().execute("upsert into " + fullTableName + " values ('def',2,4)");
        conn.commit();
        stmt = conn.prepareStatement("UPDATE STATISTICS " + fullTableName);
        stmt.execute();
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

    @Test
    public void testUpdateStatsWithMultipleTables() throws Throwable {
        String fullTableName2 = SchemaUtil.getTableName(schemaName, "T_" + generateUniqueName());
        Connection conn;
        PreparedStatement stmt;
        ResultSet rs;
        Properties props = PropertiesUtil.deepCopy(TEST_PROPERTIES);
        conn = DriverManager.getConnection(getUrl(), props);
        conn.createStatement().execute(
                "CREATE TABLE " + fullTableName +" ( k VARCHAR, a_string_array VARCHAR(100) ARRAY[4], b_string_array VARCHAR(100) ARRAY[4] \n"
                        + " CONSTRAINT pk PRIMARY KEY (k, b_string_array DESC))" + tableDDLOptions );
        conn.createStatement().execute(
                "CREATE TABLE " + fullTableName2 +" ( k VARCHAR, a_string_array VARCHAR(100) ARRAY[4], b_string_array VARCHAR(100) ARRAY[4] \n"
                        + " CONSTRAINT pk PRIMARY KEY (k, b_string_array DESC))" + tableDDLOptions );
        String[] s;
        Array array;
        conn = upsertValues(props, fullTableName);
        conn = upsertValues(props, fullTableName2);
        // CAll the update statistics query here
        stmt = conn.prepareStatement("UPDATE STATISTICS "+fullTableName);
        stmt.execute();
        stmt = conn.prepareStatement("UPDATE STATISTICS "+fullTableName2);
        stmt.execute();
        stmt = upsertStmt(conn, fullTableName);
        stmt.setString(1, "z");
        s = new String[] { "xyz", "def", "ghi", "jkll", null, null, "xxx" };
        array = conn.createArrayOf("VARCHAR", s);
        stmt.setArray(2, array);
        s = new String[] { "zya", "def", "ghi", "jkll", null, null, null, "xxx" };
        array = conn.createArrayOf("VARCHAR", s);
        stmt.setArray(3, array);
        stmt.execute();
        stmt = upsertStmt(conn, fullTableName2);
        stmt.setString(1, "z");
        s = new String[] { "xyz", "def", "ghi", "jkll", null, null, "xxx" };
        array = conn.createArrayOf("VARCHAR", s);
        stmt.setArray(2, array);
        s = new String[] { "zya", "def", "ghi", "jkll", null, null, null, "xxx" };
        array = conn.createArrayOf("VARCHAR", s);
        stmt.setArray(3, array);
        stmt.execute();
        conn.close();
        conn = DriverManager.getConnection(getUrl(), props);
        // This analyze would not work
        stmt = conn.prepareStatement("UPDATE STATISTICS "+fullTableName2);
        stmt.execute();
        rs = conn.createStatement().executeQuery("SELECT k FROM "+fullTableName2);
        assertTrue(rs.next());
        conn.close();
    }

    private Connection upsertValues(Properties props, String tableName) throws SQLException, IOException,
            InterruptedException {
        Connection conn;
        PreparedStatement stmt;
        conn = DriverManager.getConnection(getUrl(), props);
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

    private void compactTable(Connection conn, String tableName) throws Exception {
        TestUtil.doMajorCompaction(conn, tableName);
    }
    
    @Test
    public void testCompactUpdatesStats() throws Exception {
        testCompactUpdatesStats(null, fullTableName);
    }
    
    @Test
    public void testCompactUpdatesStatsWithMinStatsUpdateFreq() throws Exception {
        testCompactUpdatesStats(QueryServicesOptions.DEFAULT_STATS_UPDATE_FREQ_MS, fullTableName);
    }
    
    private void testCompactUpdatesStats(Integer minStatsUpdateFreq, String tableName) throws Exception {
        int nRows = 10;
        Connection conn;
        PreparedStatement stmt;
        Properties props = PropertiesUtil.deepCopy(TEST_PROPERTIES);
        conn = DriverManager.getConnection(getUrl(), props);
        conn.createStatement().execute("CREATE TABLE " + tableName + "(k CHAR(1) PRIMARY KEY, v INTEGER, w INTEGER) "
                + HColumnDescriptor.KEEP_DELETED_CELLS + "=" + Boolean.FALSE);
        stmt = conn.prepareStatement("UPSERT INTO " + tableName + " VALUES(?,?,?)");
        for (int i = 0; i < nRows; i++) {
            stmt.setString(1, Character.toString((char) ('a' + i)));
            stmt.setInt(2, i);
            stmt.setInt(3, i);
            stmt.executeUpdate();
        }
        conn.commit();
        
        compactTable(conn, tableName);
        if (minStatsUpdateFreq == null) {
            ImmutableBytesPtr ptr = new ImmutableBytesPtr(Bytes.toBytes(tableName));
            conn.unwrap(PhoenixConnection.class).getQueryServices().invalidateStats(ptr);
        } else {
            // Confirm that when we have a non zero MIN_STATS_UPDATE_FREQ_MS_ATTRIB, after we run
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
        
        compactTable(conn, tableName);
        if (minStatsUpdateFreq == null) {
            ImmutableBytesPtr ptr = new ImmutableBytesPtr(Bytes.toBytes(tableName));
            conn.unwrap(PhoenixConnection.class).getQueryServices().invalidateStats(ptr);
        }
        
        keyRanges = getAllSplits(conn, tableName);
        if (minStatsUpdateFreq != null) {
            assertEquals(nRows+1, keyRanges.size());
            // If we've set MIN_STATS_UPDATE_FREQ_MS_ATTRIB, an UPDATE STATISTICS will invalidate the cache
            // and force us to pull over the new stats
            int rowCount = conn.createStatement().executeUpdate("UPDATE STATISTICS " + tableName);
            assertEquals(5, rowCount);
            keyRanges = getAllSplits(conn, tableName);
        }
        assertEquals(nRows/2+1, keyRanges.size());
        ResultSet rs = conn.createStatement().executeQuery("SELECT SUM(GUIDE_POSTS_ROW_COUNT) FROM "
                + PhoenixDatabaseMetaData.SYSTEM_STATS_NAME + " WHERE PHYSICAL_NAME='" + tableName + "'");
        rs.next();
        assertEquals(nRows - nDeletedRows, rs.getLong(1));
    }

    @Test
    public void testWithMultiCF() throws Exception {
        int nRows = 20;
        Connection conn;
        PreparedStatement stmt;
        Properties props = PropertiesUtil.deepCopy(TEST_PROPERTIES);
        conn = DriverManager.getConnection(getUrl(), props);
        conn.createStatement().execute(
                "CREATE TABLE " + fullTableName
                        + "(k VARCHAR PRIMARY KEY, a.v INTEGER, b.v INTEGER, c.v INTEGER NULL, d.v INTEGER NULL) ");
        stmt = conn.prepareStatement("UPSERT INTO " + fullTableName + " VALUES(?,?, ?, ?, ?)");
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
        TestUtil.analyzeTable(conn, fullTableName);
        List<KeyRange> keyRanges = getAllSplits(conn, fullTableName);
        assertEquals(26, keyRanges.size());
        rs = conn.createStatement().executeQuery("EXPLAIN SELECT * FROM " + fullTableName);
        assertEquals("CLIENT 26-CHUNK 25 ROWS 12420 BYTES PARALLEL 1-WAY FULL SCAN OVER " + fullTableName,
                QueryUtil.getExplainPlan(rs));

        ConnectionQueryServices services = conn.unwrap(PhoenixConnection.class).getQueryServices();
        List<HRegionLocation> regions = services.getAllTableRegions(Bytes.toBytes(fullTableName));
        assertEquals(1, regions.size());

        TestUtil.analyzeTable(conn, fullTableName);
        String query = "UPDATE STATISTICS " + fullTableName + " SET \""
                + QueryServices.STATS_GUIDEPOST_WIDTH_BYTES_ATTRIB + "\"=" + Long.toString(1000);
        conn.createStatement().execute(query);
        keyRanges = getAllSplits(conn, fullTableName);
        assertEquals(12, keyRanges.size());

        rs = conn
                .createStatement()
                .executeQuery(
                        "SELECT COLUMN_FAMILY,SUM(GUIDE_POSTS_ROW_COUNT),SUM(GUIDE_POSTS_WIDTH),COUNT(*) from SYSTEM.STATS where PHYSICAL_NAME = '"
                                + fullTableName + "' GROUP BY COLUMN_FAMILY ORDER BY COLUMN_FAMILY");

        assertTrue(rs.next());
        assertEquals("A", rs.getString(1));
        assertEquals(24, rs.getInt(2));
        assertEquals(12144, rs.getInt(3));
        assertEquals(11, rs.getInt(4));

        assertTrue(rs.next());
        assertEquals("B", rs.getString(1));
        assertEquals(20, rs.getInt(2));
        assertEquals(5540, rs.getInt(3));
        assertEquals(5, rs.getInt(4));

        assertTrue(rs.next());
        assertEquals("C", rs.getString(1));
        assertEquals(24, rs.getInt(2));
        assertEquals(6652, rs.getInt(3));
        assertEquals(6, rs.getInt(4));

        assertTrue(rs.next());
        assertEquals("D", rs.getString(1));
        assertEquals(24, rs.getInt(2));
        assertEquals(6652, rs.getInt(3));
        assertEquals(6, rs.getInt(4));

        assertFalse(rs.next());
    }

    @Test
    public void testRowCountAndByteCounts() throws SQLException {
        Connection conn;
        Properties props = PropertiesUtil.deepCopy(TEST_PROPERTIES);
        conn = DriverManager.getConnection(getUrl(), props);
        String ddl = "CREATE TABLE " + fullTableName + " (t_id VARCHAR NOT NULL,\n" + "k1 INTEGER NOT NULL,\n"
                + "k2 INTEGER NOT NULL,\n" + "C3.k3 INTEGER,\n" + "C2.v1 VARCHAR,\n"
                + "CONSTRAINT pk PRIMARY KEY (t_id, k1, k2)) split on ('e','j','o')";
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
        String query = "UPDATE STATISTICS " + fullTableName + " SET \""
                + QueryServices.STATS_GUIDEPOST_WIDTH_BYTES_ATTRIB + "\"=" + Long.toString(20);
        conn.createStatement().execute(query);
        Random r = new Random();
        int count = 0;
        while (count < 4) {
            int startIndex = r.nextInt(strings.length);
            int endIndex = r.nextInt(strings.length - startIndex) + startIndex;
            long rows = endIndex - startIndex;
            long c2Bytes = rows * 35;
            System.out.println(rows + ":" + startIndex + ":" + endIndex);
            rs = conn.createStatement().executeQuery(
                    "SELECT COLUMN_FAMILY,SUM(GUIDE_POSTS_ROW_COUNT),SUM(GUIDE_POSTS_WIDTH) from SYSTEM.STATS where PHYSICAL_NAME = '"
                            + fullTableName + "' AND GUIDE_POST_KEY>= cast('" + strings[startIndex]
                            + "' as varbinary) AND  GUIDE_POST_KEY<cast('" + strings[endIndex]
                            + "' as varbinary) and COLUMN_FAMILY='C2' group by COLUMN_FAMILY");
            if (startIndex < endIndex) {
                assertTrue(rs.next());
                assertEquals("C2", rs.getString(1));
                assertEquals(rows, rs.getLong(2));
                assertEquals(c2Bytes, rs.getLong(3));
                count++;
            }
        }
    }
}
