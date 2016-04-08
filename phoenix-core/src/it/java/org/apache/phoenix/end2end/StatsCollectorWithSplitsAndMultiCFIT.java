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
import static org.junit.Assert.assertTrue;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Random;

import org.apache.hadoop.hbase.HRegionLocation;
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
        conn.createStatement().execute("CREATE TABLE " + STATS_TEST_TABLE_NAME_NEW
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

        TestUtil.analyzeTable(conn, STATS_TEST_TABLE_NAME_NEW);
        String query = "UPDATE STATISTICS " + STATS_TEST_TABLE_NAME_NEW + " SET \""
                + QueryServices.STATS_GUIDEPOST_WIDTH_BYTES_ATTRIB + "\"=" + Long.toString(250);
        conn.createStatement().execute(query);
        keyRanges = getAllSplits(conn, STATS_TEST_TABLE_NAME_NEW);
        assertEquals(26, keyRanges.size());

        rs = conn.createStatement().executeQuery(
                "SELECT COLUMN_FAMILY,SUM(GUIDE_POSTS_ROW_COUNT),SUM(GUIDE_POSTS_WIDTH),COUNT(*) from SYSTEM.STATS where PHYSICAL_NAME = '"
                        + STATS_TEST_TABLE_NAME_NEW + "' GROUP BY COLUMN_FAMILY ORDER BY COLUMN_FAMILY");

        assertTrue(rs.next());
        assertEquals("A", rs.getString(1));
        assertEquals(25, rs.getInt(2));
        assertEquals(12530, rs.getInt(3));
        assertEquals(25, rs.getInt(4));

        assertTrue(rs.next());
        assertEquals("B", rs.getString(1));
        assertEquals(20, rs.getInt(2));
        assertEquals(5600, rs.getInt(3));
        assertEquals(20, rs.getInt(4));

        assertTrue(rs.next());
        assertEquals("C", rs.getString(1));
        assertEquals(25, rs.getInt(2));
        assertEquals(7005, rs.getInt(3));
        assertEquals(25, rs.getInt(4));

        assertTrue(rs.next());
        assertEquals("D", rs.getString(1));
        assertEquals(25, rs.getInt(2));
        assertEquals(7005, rs.getInt(3));
        assertEquals(25, rs.getInt(4));

    }

    @Test
    public void testRowCountAndByteCounts() throws SQLException {
        Connection conn;
        Properties props = PropertiesUtil.deepCopy(TEST_PROPERTIES);
        conn = DriverManager.getConnection(getUrl(), props);
        String tableName = "T";
        String ddl = "CREATE TABLE " + tableName + " (t_id VARCHAR NOT NULL,\n" + "k1 INTEGER NOT NULL,\n"
                + "k2 INTEGER NOT NULL,\n" + "C3.k3 INTEGER,\n" + "C2.v1 VARCHAR,\n"
                + "CONSTRAINT pk PRIMARY KEY (t_id, k1, k2)) split on ('e','j','o')";
        conn.createStatement().execute(ddl);
        String[] strings = { "a", "b", "c", "d", "e", "f", "g", "h", "i", "j", "k", "l", "m", "n", "o", "p", "q", "r",
                "s", "t", "u", "v", "w", "x", "y", "z" };
        for (int i = 0; i < 26; i++) {
            conn.createStatement().execute("UPSERT INTO " + tableName + " values('" + strings[i] + "'," + i + ","
                    + (i + 1) + "," + (i + 2) + ",'" + strings[25 - i] + "')");
        }
        conn.commit();
        ResultSet rs;
        String query = "UPDATE STATISTICS " + tableName + " SET \"" + QueryServices.STATS_GUIDEPOST_WIDTH_BYTES_ATTRIB
                + "\"=" + Long.toString(20);
        conn.createStatement().execute(query);
        Random r = new Random();
        int count = 0;
        while (count < 4) {
            int startIndex = r.nextInt(strings.length);
            int endIndex = r.nextInt(strings.length - startIndex) + startIndex;
            long rows = endIndex - startIndex;
            long c2Bytes = rows * 37;
            System.out.println(rows + ":" + startIndex + ":" + endIndex);
            rs = conn.createStatement().executeQuery(
                    "SELECT COLUMN_FAMILY,SUM(GUIDE_POSTS_ROW_COUNT),SUM(GUIDE_POSTS_WIDTH) from SYSTEM.STATS where PHYSICAL_NAME = '"
                            + tableName + "' AND GUIDE_POST_KEY>= cast('" + strings[startIndex]
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
