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
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertTrue;

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

import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.phoenix.jdbc.PhoenixConnection;
import org.apache.phoenix.query.ConnectionQueryServices;
import org.apache.phoenix.query.KeyRange;
import org.apache.phoenix.query.QueryServices;
import org.apache.phoenix.query.QueryServicesOptions;
import org.apache.phoenix.util.PropertiesUtil;
import org.apache.phoenix.util.ReadOnlyProps;
import org.junit.BeforeClass;
import org.junit.Test;

import com.google.common.collect.Maps;


public class StatsCollectorIT extends StatsCollectorAbstractIT {
    private static final String STATS_TEST_TABLE_NAME = "S";
        
    @BeforeClass
    public static void doSetup() throws Exception {
        Map<String,String> props = Maps.newHashMapWithExpectedSize(3);
        // Must update config before starting server
        props.put(QueryServices.STATS_GUIDEPOST_WIDTH_BYTES_ATTRIB, Long.toString(20));
        props.put(QueryServices.EXPLAIN_CHUNK_COUNT_ATTRIB, Boolean.TRUE.toString());
        props.put(QueryServices.QUEUE_SIZE_ATTRIB, Integer.toString(1024));
        setUpTestDriver(new ReadOnlyProps(props.entrySet().iterator()));
    }

    @Test
    public void testUpdateStatsForTheTable() throws Throwable {
        Connection conn;
        PreparedStatement stmt;
        ResultSet rs;
        Properties props = PropertiesUtil.deepCopy(TEST_PROPERTIES);
        // props.setProperty(PhoenixRuntime.CURRENT_SCN_ATTRIB, Long.toString(ts + 10));
        conn = DriverManager.getConnection(getUrl(), props);
        conn.createStatement().execute(
                "CREATE TABLE t ( k VARCHAR, a_string_array VARCHAR(100) ARRAY[4], b_string_array VARCHAR(100) ARRAY[4] \n"
                        + " CONSTRAINT pk PRIMARY KEY (k, b_string_array DESC)) \n");
        String[] s;
        Array array;
        conn = upsertValues(props, "t");
        // CAll the update statistics query here. If already major compaction has run this will not get executed.
        stmt = conn.prepareStatement("UPDATE STATISTICS T");
        stmt.execute();
        stmt = upsertStmt(conn, "t");
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
        stmt = conn.prepareStatement("UPDATE STATISTICS T");
        stmt.execute();
        rs = conn.createStatement().executeQuery("SELECT k FROM T");
        assertTrue(rs.next());
        conn.close();
    }

    @Test
    public void testUpdateStatsWithMultipleTables() throws Throwable {
        Connection conn;
        PreparedStatement stmt;
        ResultSet rs;
        Properties props = PropertiesUtil.deepCopy(TEST_PROPERTIES);
        // props.setProperty(PhoenixRuntime.CURRENT_SCN_ATTRIB, Long.toString(ts + 10));
        conn = DriverManager.getConnection(getUrl(), props);
        conn.createStatement().execute(
                "CREATE TABLE x ( k VARCHAR, a_string_array VARCHAR(100) ARRAY[4], b_string_array VARCHAR(100) ARRAY[4] \n"
                        + " CONSTRAINT pk PRIMARY KEY (k, b_string_array DESC)) \n");
        conn.createStatement().execute(
                "CREATE TABLE z ( k VARCHAR, a_string_array VARCHAR(100) ARRAY[4], b_string_array VARCHAR(100) ARRAY[4] \n"
                        + " CONSTRAINT pk PRIMARY KEY (k, b_string_array DESC)) \n");
        String[] s;
        Array array;
        conn = upsertValues(props, "x");
        conn = upsertValues(props, "z");
        // CAll the update statistics query here
        stmt = conn.prepareStatement("UPDATE STATISTICS X");
        stmt.execute();
        stmt = conn.prepareStatement("UPDATE STATISTICS Z");
        stmt.execute();
        stmt = upsertStmt(conn, "x");
        stmt.setString(1, "z");
        s = new String[] { "xyz", "def", "ghi", "jkll", null, null, "xxx" };
        array = conn.createArrayOf("VARCHAR", s);
        stmt.setArray(2, array);
        s = new String[] { "zya", "def", "ghi", "jkll", null, null, null, "xxx" };
        array = conn.createArrayOf("VARCHAR", s);
        stmt.setArray(3, array);
        stmt.execute();
        stmt = upsertStmt(conn, "z");
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
        stmt = conn.prepareStatement("UPDATE STATISTICS Z");
        stmt.execute();
        rs = conn.createStatement().executeQuery("SELECT k FROM Z");
        assertTrue(rs.next());
        conn.close();
    }

    private Connection upsertValues(Properties props, String tableName) throws SQLException, IOException,
            InterruptedException {
        Connection conn;
        PreparedStatement stmt;
        // props.setProperty(PhoenixRuntime.CURRENT_SCN_ATTRIB, Long.toString(ts + 30));
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

    private void compactTable(Connection conn, String tableName) throws IOException, InterruptedException, SQLException {
        ConnectionQueryServices services = conn.unwrap(PhoenixConnection.class).getQueryServices();
        HBaseAdmin admin = services.getAdmin();
        try {
            admin.flush(tableName);
            admin.majorCompact(tableName);
            Thread.sleep(10000); // FIXME: how do we know when compaction is done?
        } finally {
            admin.close();
        }
    }
    
    @Test
    public void testCompactUpdatesStats() throws Exception {
        testCompactUpdatesStats(null, STATS_TEST_TABLE_NAME + 1);
    }
    
    @Test
    public void testCompactUpdatesStatsWithMinStatsUpdateFreq() throws Exception {
        testCompactUpdatesStats(QueryServicesOptions.DEFAULT_STATS_UPDATE_FREQ_MS, STATS_TEST_TABLE_NAME + 2);
    }
    
    private void testCompactUpdatesStats(Integer minStatsUpdateFreq, String tableName) throws Exception {
        int nRows = 10;
        Connection conn;
        PreparedStatement stmt;
        Properties props = PropertiesUtil.deepCopy(TEST_PROPERTIES);
        if (minStatsUpdateFreq != null) {
            props.setProperty(QueryServices.MIN_STATS_UPDATE_FREQ_MS_ATTRIB, minStatsUpdateFreq.toString());
        }
        conn = DriverManager.getConnection(getUrl(), props);
        conn.createStatement().execute("CREATE TABLE " + tableName + "(k CHAR(1) PRIMARY KEY, v INTEGER) " + HColumnDescriptor.KEEP_DELETED_CELLS + "=" + Boolean.FALSE);
        stmt = conn.prepareStatement("UPSERT INTO " + tableName + " VALUES(?,?)");
        for (int i = 0; i < nRows; i++) {
            stmt.setString(1, Character.toString((char) ('a' + i)));
            stmt.setInt(2, i);
            stmt.executeUpdate();
        }
        conn.commit();
        
        compactTable(conn, tableName);
        if (minStatsUpdateFreq == null) {
            conn.unwrap(PhoenixConnection.class).getQueryServices().clearCache();
        }
        // Confirm that when we have a non zero MIN_STATS_UPDATE_FREQ_MS_ATTRIB, after we run
        // UPDATATE STATISTICS, the new statistics are faulted in as expected.
        if (minStatsUpdateFreq != null) {
            List<KeyRange>keyRanges = getAllSplits(conn, tableName);
            assertNotEquals(nRows+1, keyRanges.size());
            // If we've set MIN_STATS_UPDATE_FREQ_MS_ATTRIB, an UPDATE STATISTICS will invalidate the cache
            // and forcing the new stats to be pulled over.
            int rowCount = conn.createStatement().executeUpdate("UPDATE STATISTICS " + tableName);
            assertEquals(0, rowCount);
        }
        List<KeyRange>keyRanges = getAllSplits(conn, tableName);
        assertEquals(nRows+1, keyRanges.size());
        
        int nDeletedRows = conn.createStatement().executeUpdate("DELETE FROM " + tableName + " WHERE V < 5");
        conn.commit();
        assertEquals(5, nDeletedRows);
        
        compactTable(conn, tableName);
        if (minStatsUpdateFreq == null) {
            conn.unwrap(PhoenixConnection.class).getQueryServices().clearCache();
        }
        
        keyRanges = getAllSplits(conn, tableName);
        if (minStatsUpdateFreq != null) {
            assertEquals(nRows+1, keyRanges.size());
            // If we've set MIN_STATS_UPDATE_FREQ_MS_ATTRIB, an UPDATE STATISTICS will invalidate the cache
            // and force us to pull over the new stats
            int rowCount = conn.createStatement().executeUpdate("UPDATE STATISTICS " + tableName);
            assertEquals(0, rowCount);
            keyRanges = getAllSplits(conn, tableName);
        }
        assertEquals(nRows/2+1, keyRanges.size());
        
    }
}
