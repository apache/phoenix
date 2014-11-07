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
import org.apache.hadoop.hbase.HRegionLocation;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.phoenix.jdbc.PhoenixConnection;
import org.apache.phoenix.query.ConnectionQueryServices;
import org.apache.phoenix.query.KeyRange;
import org.apache.phoenix.query.QueryServices;
import org.apache.phoenix.util.PropertiesUtil;
import org.apache.phoenix.util.QueryUtil;
import org.apache.phoenix.util.ReadOnlyProps;
import org.apache.phoenix.util.TestUtil;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import com.google.common.collect.Maps;

@Category(NeedsOwnMiniClusterTest.class)
public class StatsCollectorIT extends BaseOwnClusterHBaseManagedTimeIT {
    private static final String STATS_TEST_TABLE_NAME = "S";
    private static final byte[] STATS_TEST_TABLE_BYTES = Bytes.toBytes(STATS_TEST_TABLE_NAME);
        
    @BeforeClass
    public static void doSetup() throws Exception {
        Map<String,String> props = Maps.newHashMapWithExpectedSize(5);
        // Must update config before starting server
        props.put(QueryServices.STATS_GUIDEPOST_WIDTH_BYTES_ATTRIB, Long.toString(20));
        props.put(QueryServices.THREAD_POOL_SIZE_ATTRIB, Integer.toString(10));
        props.put(QueryServices.QUEUE_SIZE_ATTRIB, Integer.toString(1000));
        props.put(QueryServices.EXPLAIN_CHUNK_COUNT_ATTRIB, Boolean.TRUE.toString());
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
        flush(tableName);
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
        flush(tableName);
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
        flush(tableName);
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
        flush(tableName);
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
        flush(tableName);
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
        flush(tableName);
        return conn;
    }

    private void flush(String tableName) throws IOException, InterruptedException {
        //utility.getHBaseAdmin().flush(tableName.toUpperCase());
    }

    private PreparedStatement upsertStmt(Connection conn, String tableName) throws SQLException {
        PreparedStatement stmt;
        stmt = conn.prepareStatement("UPSERT INTO " + tableName + " VALUES(?,?,?)");
        return stmt;
    }

    private void compactTable(Connection conn) throws IOException, InterruptedException, SQLException {
        ConnectionQueryServices services = conn.unwrap(PhoenixConnection.class).getQueryServices();
        HBaseAdmin admin = services.getAdmin();
        try {
            admin.flush(STATS_TEST_TABLE_NAME);
            admin.majorCompact(STATS_TEST_TABLE_NAME);
            Thread.sleep(10000); // FIXME: how do we know when compaction is done?
        } finally {
            admin.close();
        }
        services.clearCache();
    }
    
    @Test
    public void testCompactUpdatesStats() throws Exception {
        int nRows = 10;
        Connection conn;
        PreparedStatement stmt;
        Properties props = PropertiesUtil.deepCopy(TEST_PROPERTIES);
        conn = DriverManager.getConnection(getUrl(), props);
        conn.createStatement().execute("CREATE TABLE " + STATS_TEST_TABLE_NAME + "(k CHAR(1) PRIMARY KEY, v INTEGER) " + HColumnDescriptor.KEEP_DELETED_CELLS + "=" + Boolean.FALSE);
        stmt = conn.prepareStatement("UPSERT INTO " + STATS_TEST_TABLE_NAME + " VALUES(?,?)");
        for (int i = 0; i < nRows; i++) {
            stmt.setString(1, Character.toString((char) ('a' + i)));
            stmt.setInt(2, i);
            stmt.executeUpdate();
        }
        conn.commit();
        
        compactTable(conn);
        conn = DriverManager.getConnection(getUrl(), props);
        List<KeyRange>keyRanges = getAllSplits(conn, STATS_TEST_TABLE_NAME);
        assertEquals(nRows+1, keyRanges.size());
        
        int nDeletedRows = conn.createStatement().executeUpdate("DELETE FROM " + STATS_TEST_TABLE_NAME + " WHERE V < 5");
        conn.commit();
        assertEquals(5, nDeletedRows);
        
        compactTable(conn);
        
        keyRanges = getAllSplits(conn, STATS_TEST_TABLE_NAME);
        assertEquals(nRows/2+1, keyRanges.size());
        
    }


    private void splitTable(Connection conn, byte[] splitPoint) throws IOException, InterruptedException, SQLException {
        ConnectionQueryServices services = conn.unwrap(PhoenixConnection.class).getQueryServices();
        int nRegionsNow = services.getAllTableRegions(STATS_TEST_TABLE_BYTES).size();
        HBaseAdmin admin = services.getAdmin();
        try {
            admin.split(STATS_TEST_TABLE_BYTES, splitPoint);
            int nTries = 0;
            int nRegions;
            do {
                Thread.sleep(2000);
                services.clearTableRegionCache(STATS_TEST_TABLE_BYTES);
                nRegions = services.getAllTableRegions(STATS_TEST_TABLE_BYTES).size();
                nTries++;
            } while (nRegions == nRegionsNow && nTries < 10);
            if (nRegions == nRegionsNow) {
                throw new IOException("Failed to complete split within alloted time");
            }
            // FIXME: I see the commit of the stats finishing before this with a lower timestamp that the scan timestamp,
            // yet without this sleep, the query finds the old data. Seems like an HBase bug and a potentially serious one.

            Thread.sleep(5000);
        } finally {
            admin.close();
        }
    }
    
    @Test
    public void testSplitUpdatesStats() throws Exception {
        int nRows = 10;
        Connection conn;
        PreparedStatement stmt;
        Properties props = PropertiesUtil.deepCopy(TEST_PROPERTIES);
        conn = DriverManager.getConnection(getUrl(), props);
        conn.createStatement().execute("CREATE TABLE " + STATS_TEST_TABLE_NAME + "(k VARCHAR PRIMARY KEY, v INTEGER) " + HColumnDescriptor.KEEP_DELETED_CELLS + "=" + Boolean.FALSE);
        stmt = conn.prepareStatement("UPSERT INTO " + STATS_TEST_TABLE_NAME + " VALUES(?,?)");
        for (int i = 0; i < nRows; i++) {
            stmt.setString(1, Character.toString((char) ('a' + i)));
            stmt.setInt(2, i);
            stmt.executeUpdate();
        }
        conn.commit();
        
        ResultSet rs;
        TestUtil.analyzeTable(conn, STATS_TEST_TABLE_NAME);
        List<KeyRange>keyRanges = getAllSplits(conn, STATS_TEST_TABLE_NAME);
        assertEquals(nRows+1, keyRanges.size());
        rs = conn.createStatement().executeQuery("EXPLAIN SELECT * FROM " + STATS_TEST_TABLE_NAME);
        assertEquals("CLIENT " + (nRows+1) + "-CHUNK " + "PARALLEL 1-WAY FULL SCAN OVER " + STATS_TEST_TABLE_NAME, QueryUtil.getExplainPlan(rs));

        ConnectionQueryServices services = conn.unwrap(PhoenixConnection.class).getQueryServices();
        List<HRegionLocation> regions = services.getAllTableRegions(STATS_TEST_TABLE_BYTES);
        assertEquals(1, regions.size());
 
        rs = conn.createStatement().executeQuery("SELECT GUIDE_POSTS_COUNT, REGION_NAME FROM SYSTEM.STATS WHERE PHYSICAL_NAME='"+STATS_TEST_TABLE_NAME+"' AND REGION_NAME IS NOT NULL");
        assertTrue(rs.next());
        assertEquals(nRows, rs.getLong(1));
        assertEquals(regions.get(0).getRegionInfo().getRegionNameAsString(), rs.getString(2));
        assertFalse(rs.next());

        byte[] midPoint = Bytes.toBytes(Character.toString((char) ('a' + (nRows/2))));
        splitTable(conn, midPoint);
        
        keyRanges = getAllSplits(conn, STATS_TEST_TABLE_NAME);
        assertEquals(nRows+1, keyRanges.size()); // Same number as before because split was at guidepost
        
        regions = services.getAllTableRegions(STATS_TEST_TABLE_BYTES);
        assertEquals(2, regions.size());
        rs = conn.createStatement().executeQuery("SELECT GUIDE_POSTS_COUNT, REGION_NAME FROM SYSTEM.STATS WHERE PHYSICAL_NAME='"+STATS_TEST_TABLE_NAME+"' AND REGION_NAME IS NOT NULL");
        assertTrue(rs.next());
        assertEquals(nRows/2, rs.getLong(1));
        assertEquals(regions.get(0).getRegionInfo().getRegionNameAsString(), rs.getString(2));
        assertTrue(rs.next());
        assertEquals(nRows/2 - 1, rs.getLong(1));
        assertEquals(regions.get(1).getRegionInfo().getRegionNameAsString(), rs.getString(2));
        assertFalse(rs.next());

        byte[] midPoint2 = Bytes.toBytes("cj");
        splitTable(conn, midPoint2);
        
        keyRanges = getAllSplits(conn, STATS_TEST_TABLE_NAME);
        assertEquals(nRows+2, keyRanges.size()); // One extra due to split between guideposts
        
        regions = services.getAllTableRegions(STATS_TEST_TABLE_BYTES);
        assertEquals(3, regions.size());
        rs = conn.createStatement().executeQuery("SELECT GUIDE_POSTS_COUNT, REGION_NAME FROM SYSTEM.STATS WHERE PHYSICAL_NAME='"+STATS_TEST_TABLE_NAME+"' AND REGION_NAME IS NOT NULL");
        assertTrue(rs.next());
        assertEquals(3, rs.getLong(1));
        assertEquals(regions.get(0).getRegionInfo().getRegionNameAsString(), rs.getString(2));
        assertTrue(rs.next());
        assertEquals(2, rs.getLong(1));
        assertEquals(regions.get(1).getRegionInfo().getRegionNameAsString(), rs.getString(2));
        assertTrue(rs.next());
        assertEquals(nRows/2 - 1, rs.getLong(1));
        assertEquals(regions.get(2).getRegionInfo().getRegionNameAsString(), rs.getString(2));
        assertFalse(rs.next());
        
        conn.close();
    }
}
