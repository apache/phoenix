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

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HRegionLocation;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.phoenix.jdbc.PhoenixConnection;
import org.apache.phoenix.query.ConnectionQueryServices;
import org.apache.phoenix.query.KeyRange;
import org.apache.phoenix.query.QueryServices;
import org.apache.phoenix.schema.stats.GuidePostsInfo;
import org.apache.phoenix.util.PropertiesUtil;
import org.apache.phoenix.util.QueryUtil;
import org.apache.phoenix.util.ReadOnlyProps;
import org.apache.phoenix.util.TestUtil;
import org.junit.BeforeClass;
import org.junit.Test;

import com.google.common.collect.Maps;

public class StatsCollectorWithSplitsAndMultiCFIT extends StatsCollectorAbstractIT {
    private static final String STATS_TEST_TABLE_NAME_NEW = "S_NEW";
    private static final byte[] STATS_TEST_TABLE_NEW_BYTES = Bytes.toBytes(STATS_TEST_TABLE_NAME_NEW);

    @BeforeClass
    public static void doSetup() throws Exception {
        Map<String, String> props = Maps.newHashMapWithExpectedSize(3);
        // Must update config before starting server
        props.put(QueryServices.STATS_GUIDEPOST_WIDTH_BYTES_ATTRIB, Long.toString(1000));
        props.put(QueryServices.EXPLAIN_CHUNK_COUNT_ATTRIB, Boolean.TRUE.toString());
        props.put(QueryServices.QUEUE_SIZE_ATTRIB, Integer.toString(1024));
        setUpTestDriver(new ReadOnlyProps(props.entrySet().iterator()));
    }

    @Test
    public void testWithMultiCF() throws Exception {
        int nRows = 20;
        Connection conn;
        PreparedStatement stmt;
        Properties props = PropertiesUtil.deepCopy(TEST_PROPERTIES);
        conn = DriverManager.getConnection(getUrl(), props);
        conn.createStatement().execute(
                "CREATE TABLE " + STATS_TEST_TABLE_NAME_NEW
                        + "(k VARCHAR PRIMARY KEY, a.v INTEGER, b.v INTEGER, c.v INTEGER NULL, d.v INTEGER NULL) ");
        stmt = conn.prepareStatement("UPSERT INTO " + STATS_TEST_TABLE_NAME_NEW + " VALUES(?,?, ?, ?, ?)");
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
        stmt = conn.prepareStatement("UPSERT INTO " + STATS_TEST_TABLE_NAME_NEW + "(k, c.v, d.v) VALUES(?,?,?)");
        for (int i = 0; i < 5; i++) {
            stmt.setString(1, Character.toString((char)('a' + 'z' + i)) + Bytes.toString(val));
            stmt.setInt(2, i);
            stmt.setInt(3, i);
            stmt.executeUpdate();
        }
        conn.commit();

        ResultSet rs;
        TestUtil.analyzeTable(conn, STATS_TEST_TABLE_NAME_NEW);
        List<KeyRange> keyRanges = getAllSplits(conn, STATS_TEST_TABLE_NAME_NEW);
        assertEquals(12, keyRanges.size());
        rs = conn.createStatement().executeQuery("EXPLAIN SELECT * FROM " + STATS_TEST_TABLE_NAME_NEW);
        assertEquals("CLIENT " + (12) + "-CHUNK " + "PARALLEL 1-WAY FULL SCAN OVER " + STATS_TEST_TABLE_NAME_NEW,
                QueryUtil.getExplainPlan(rs));

        ConnectionQueryServices services = conn.unwrap(PhoenixConnection.class).getQueryServices();
        List<HRegionLocation> regions = services.getAllTableRegions(STATS_TEST_TABLE_NEW_BYTES);
        assertEquals(1, regions.size());

        rs = conn.createStatement().executeQuery(
                "SELECT GUIDE_POSTS_COUNT, REGION_NAME, GUIDE_POSTS_ROW_COUNT FROM SYSTEM.STATS WHERE PHYSICAL_NAME='"
                        + STATS_TEST_TABLE_NAME_NEW + "' AND REGION_NAME IS NOT NULL");
        assertTrue(rs.next());
        assertEquals(11, (rs.getLong(1)));
        assertEquals(regions.get(0).getRegionInfo().getRegionNameAsString(), rs.getString(2));
        // I get 25 here because for A there is an entry added with the default col
        assertEquals(25, rs.getLong(3));
        assertTrue(rs.next());
        assertEquals(5, (rs.getLong(1)));
        assertEquals(regions.get(0).getRegionInfo().getRegionNameAsString(), rs.getString(2));
        // for b the count is lesser by 5
        assertEquals(20, rs.getLong(3));
        assertTrue(rs.next());
        assertEquals(6, (rs.getLong(1)));
        assertEquals(regions.get(0).getRegionInfo().getRegionNameAsString(), rs.getString(2));
        assertEquals(25, rs.getLong(3));
        assertTrue(rs.next());
        assertEquals(6, (rs.getLong(1)));
        assertEquals(regions.get(0).getRegionInfo().getRegionNameAsString(), rs.getString(2));
        assertEquals(25, rs.getLong(3));
        assertFalse(rs.next());
        Collection<GuidePostsInfo> infos = TestUtil.getGuidePostsList(conn, STATS_TEST_TABLE_NAME_NEW);
        long[] rowCountArr = new long[]{25, 20, 25, 25};
        // CF A alone has double the bytecount because it has column qualifier A and column qualifier _0
        long[] byteCountArr = new long[]{12120, 5540, 6652, 6652};
        int i = 0;
        for(GuidePostsInfo info : infos) {
            assertRowCountAndByteCount(info, rowCountArr[i], byteCountArr[i]);
            i++;
        }
        
        TestUtil.analyzeTable(conn, STATS_TEST_TABLE_NAME_NEW);
        String query = "UPDATE STATISTICS " + STATS_TEST_TABLE_NAME_NEW + " SET \"" + QueryServices.STATS_GUIDEPOST_WIDTH_BYTES_ATTRIB + "\"=" + Long.toString(2000);
        conn.createStatement().execute(query);
        keyRanges = getAllSplits(conn, STATS_TEST_TABLE_NAME_NEW);
        assertEquals(6, keyRanges.size());
    }

    protected void assertRowCountAndByteCount(GuidePostsInfo info, long rowCount, long byteCount) {
        assertEquals(info.getRowCount(), rowCount);
        assertEquals(info.getByteCount(), byteCount);
    }

    @Test
    public void testSplitUpdatesStats() throws Exception {
        int nRows = 20;
        Connection conn;
        PreparedStatement stmt;
        Properties props = PropertiesUtil.deepCopy(TEST_PROPERTIES);
        conn = DriverManager.getConnection(getUrl(), props);
        conn.createStatement().execute(
                "CREATE TABLE " + STATS_TEST_TABLE_NAME + "(k VARCHAR PRIMARY KEY, v INTEGER) "
                        + HColumnDescriptor.KEEP_DELETED_CELLS + "=" + Boolean.FALSE);
        stmt = conn.prepareStatement("UPSERT INTO " + STATS_TEST_TABLE_NAME + " VALUES(?,?)");
        byte[] val = new byte[250];
        for (int i = 0; i < nRows; i++) {
            stmt.setString(1, Character.toString((char)('a' + i)) + Bytes.toString(val));
            stmt.setInt(2, i);
            stmt.executeUpdate();
        }
        conn.commit();

        ResultSet rs;
        TestUtil.analyzeTable(conn, STATS_TEST_TABLE_NAME);
        Collection<GuidePostsInfo> infos = TestUtil.getGuidePostsList(conn, STATS_TEST_TABLE_NAME);
        for (GuidePostsInfo info : infos) {
            assertEquals(20, info.getRowCount());
            assertEquals(11020, info.getByteCount());
            break;
        }
        List<KeyRange> keyRanges = getAllSplits(conn, STATS_TEST_TABLE_NAME);
        assertEquals((nRows / 2) + 1, keyRanges.size());
        rs = conn.createStatement().executeQuery("EXPLAIN SELECT * FROM " + STATS_TEST_TABLE_NAME);
        assertEquals("CLIENT " + ((nRows / 2) + 1) + "-CHUNK " + "PARALLEL 1-WAY FULL SCAN OVER "
                + STATS_TEST_TABLE_NAME, QueryUtil.getExplainPlan(rs));

        ConnectionQueryServices services = conn.unwrap(PhoenixConnection.class).getQueryServices();
        List<HRegionLocation> regions = services.getAllTableRegions(STATS_TEST_TABLE_BYTES);
        assertEquals(1, regions.size());

        rs = conn.createStatement().executeQuery(
                "SELECT GUIDE_POSTS_COUNT, REGION_NAME, GUIDE_POSTS_ROW_COUNT FROM SYSTEM.STATS WHERE PHYSICAL_NAME='"
                        + STATS_TEST_TABLE_NAME + "' AND REGION_NAME IS NOT NULL");
        assertTrue(rs.next());
        assertEquals(nRows / 2, (rs.getLong(1)));
        assertEquals(regions.get(0).getRegionInfo().getRegionNameAsString(), rs.getString(2));
        //PhoenixArray arr = (PhoenixArray)rs.getArray(3);
        assertEquals(20, rs.getLong(3));
        assertFalse(rs.next());

        byte[] midPoint = Bytes.toBytes(Character.toString((char)('a' + (5))));
        splitTable(conn, midPoint, STATS_TEST_TABLE_BYTES);

        keyRanges = getAllSplits(conn, STATS_TEST_TABLE_NAME);
        assertEquals(12, keyRanges.size()); // Same number as before because split was at guidepost

        regions = services.getAllTableRegions(STATS_TEST_TABLE_BYTES);
        assertEquals(2, regions.size());
        rs = conn.createStatement().executeQuery(
                "SELECT GUIDE_POSTS_COUNT, REGION_NAME, "
                        + "GUIDE_POSTS_ROW_COUNT FROM SYSTEM.STATS WHERE PHYSICAL_NAME='" + STATS_TEST_TABLE_NAME
                        + "' AND REGION_NAME IS NOT NULL");
        assertTrue(rs.next());
        assertEquals(2, rs.getLong(1));
        assertEquals(regions.get(0).getRegionInfo().getRegionNameAsString(), rs.getString(2));
        //assertEquals(5, rs.getLong(3));
        assertTrue(rs.next());
        assertEquals(8, rs.getLong(1));
        assertEquals(regions.get(1).getRegionInfo().getRegionNameAsString(), rs.getString(2));
        // This could even be 15 if the compaction thread completes after the update from split
        //assertEquals(16, rs.getLong(3));
        assertFalse(rs.next());

        byte[] midPoint2 = Bytes.toBytes("cj");
        splitTable(conn, midPoint2, STATS_TEST_TABLE_BYTES);

        keyRanges = getAllSplits(conn, STATS_TEST_TABLE_NAME);
        assertEquals((nRows / 2) + 3, keyRanges.size()); // One extra due to split between guideposts

        regions = services.getAllTableRegions(STATS_TEST_TABLE_BYTES);
        assertEquals(3, regions.size());
        rs = conn.createStatement().executeQuery(
                "SELECT GUIDE_POSTS_COUNT, REGION_NAME, GUIDE_POSTS_ROW_COUNT FROM SYSTEM.STATS WHERE PHYSICAL_NAME='"
                        + STATS_TEST_TABLE_NAME + "' AND REGION_NAME IS NOT NULL");
        assertTrue(rs.next());
        assertEquals(1, rs.getLong(1));
        assertEquals(regions.get(0).getRegionInfo().getRegionNameAsString(), rs.getString(2));
        // This value varies based on whether compaction updates or split updates the GPs
        //assertEquals(3, rs.getLong(3));
        assertTrue(rs.next());
        assertEquals(1, rs.getLong(1));
        assertEquals(regions.get(1).getRegionInfo().getRegionNameAsString(), rs.getString(2));
        //assertEquals(2, rs.getLong(3));
        assertTrue(rs.next());
        assertEquals(8, rs.getLong(1));
        assertEquals(regions.get(2).getRegionInfo().getRegionNameAsString(), rs.getString(2));
        //assertEquals(16, rs.getLong(3));
        assertFalse(rs.next());
        rs = conn.createStatement().executeQuery("EXPLAIN SELECT * FROM " + STATS_TEST_TABLE_NAME);
        assertEquals("CLIENT " + ((nRows / 2) + 3) + "-CHUNK " + "PARALLEL 1-WAY FULL SCAN OVER "
                + STATS_TEST_TABLE_NAME, QueryUtil.getExplainPlan(rs));

        TestUtil.analyzeTable(conn, STATS_TEST_TABLE_NAME);
        rs = conn.createStatement().executeQuery("EXPLAIN SELECT * FROM " + STATS_TEST_TABLE_NAME);
        assertEquals("CLIENT " + ((nRows / 2) + 2) + "-CHUNK " + "PARALLEL 1-WAY FULL SCAN OVER "
                + STATS_TEST_TABLE_NAME, QueryUtil.getExplainPlan(rs));
        infos = TestUtil.getGuidePostsList(conn, STATS_TEST_TABLE_NAME);
        for (GuidePostsInfo info : infos) {
            assertEquals(20, info.getRowCount());
            assertEquals(9918, info.getByteCount());
            break;
        }
        conn.close();
    }
}
