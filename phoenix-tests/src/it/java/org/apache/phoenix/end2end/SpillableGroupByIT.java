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
import static org.apache.phoenix.util.TestUtil.createGroupByTestTable;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Map;
import java.util.Properties;

import org.apache.phoenix.compile.ExplainPlan;
import org.apache.phoenix.compile.ExplainPlanAttributes;
import org.apache.phoenix.exception.SQLExceptionCode;
import org.apache.phoenix.jdbc.PhoenixPreparedStatement;
import org.apache.phoenix.query.QueryServices;
import org.apache.phoenix.util.PropertiesUtil;
import org.apache.phoenix.util.ReadOnlyProps;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.apache.phoenix.thirdparty.com.google.common.collect.Maps;

/*
 * Run in own cluster since it updates QueryServices.MAX_MEMORY_SIZE_ATTRIB
 * and we wouldn't want that to be set for other tests sharing the same
 * cluster.
 */

@Category(NeedsOwnMiniClusterTest.class)
public class SpillableGroupByIT extends BaseOwnClusterIT {

    private static final int NUM_ROWS_INSERTED = 1000;
    
    // covers: COUNT, SUM, AVG, MIN, MAX 
    private static String GROUPBY1 = "select "
            + "count(*), sum(appcpu), avg(appcpu), uri, min(id), max(id) from %s "
            + "group by uri";
    
    private static String GROUPBY2 = "select count(distinct uri) from %s";
    
    private int id;

    @BeforeClass
    public static synchronized void doSetup() throws Exception {
        Map<String, String> props = Maps.newHashMapWithExpectedSize(11);
        // Set a very small cache size to force plenty of spilling
        props.put(QueryServices.GROUPBY_MAX_CACHE_SIZE_ATTRIB,
                Integer.toString(1));
        props.put(QueryServices.GROUPBY_SPILLABLE_ATTRIB, String.valueOf(true));
        props.put(QueryServices.GROUPBY_SPILL_FILES_ATTRIB,
                Integer.toString(1));
        // Large enough to not run out of memory, but small enough to spill
        props.put(QueryServices.MAX_MEMORY_SIZE_ATTRIB, Integer.toString(40000));
        
        // Set guidepost width, but disable stats
        props.put(QueryServices.STATS_GUIDEPOST_WIDTH_BYTES_ATTRIB, Long.toString(20));
        props.put(QueryServices.STATS_COLLECTION_ENABLED, Boolean.toString(false));
        props.put(QueryServices.EXPLAIN_CHUNK_COUNT_ATTRIB, Boolean.TRUE.toString());
        props.put(QueryServices.EXPLAIN_ROW_COUNT_ATTRIB, Boolean.TRUE.toString());
        props.put(QueryServices.PHOENIX_SERVER_PAGE_SIZE_MS, Long.toString(60000));
        // Must update config before starting server
        setUpTestDriver(new ReadOnlyProps(props.entrySet().iterator()));
    }

    private void createTable(Connection conn, String tableName) throws Exception {
        createGroupByTestTable(conn, tableName);
    }

    private void loadData(Connection conn, String tableName) throws SQLException {
        int groupFactor = NUM_ROWS_INSERTED / 2;
        for (int i = 0; i < NUM_ROWS_INSERTED; i++) {
            insertRow(conn, tableName, Integer.toString(i % (groupFactor)), 10);

            if ((i % 1000) == 0) {
                conn.commit();
            }
        }
        conn.commit();
    }

    private void loadUniqueURIData(Connection conn, String tableName, int rowsToInsert) throws SQLException {
        for (int i = 0; i < rowsToInsert; i++) {
            insertRow(conn, tableName, Integer.toString(i), 10);

            if ((i % 1000) == 0) {
                conn.commit();
            }
        }
        conn.commit();
    }

    private void insertRow(Connection conn, String tableName, String uri, int appcpu)
            throws SQLException {
        PreparedStatement statement = conn.prepareStatement("UPSERT INTO "
                + tableName + "(id, uri, appcpu) values (?,?,?)");
        statement.setString(1, String.valueOf(id));
        statement.setString(2, uri);
        statement.setInt(3, appcpu);
        statement.executeUpdate();
        id++;
    }

    
    @Test
    public void testScanUri() throws Exception {
        String tableName = generateUniqueName();
        Properties props = PropertiesUtil.deepCopy(TEST_PROPERTIES);
        Connection conn = DriverManager.getConnection(getUrl(), props);
        createTable(conn, tableName);
        loadData(conn, tableName);
        props = PropertiesUtil.deepCopy(TEST_PROPERTIES);
        Statement stmt = conn.createStatement();
        ResultSet rs = stmt.executeQuery(String.format(GROUPBY1, tableName));

        int count = 0;
        while (rs.next()) {
            String uri = rs.getString(4);
            assertEquals(2, rs.getInt(1));
            assertEquals(20, rs.getInt(2));
            assertEquals(10, rs.getInt(3));
            int a = Integer.valueOf(rs.getString(5)).intValue();
            int b = Integer.valueOf(rs.getString(6)).intValue();
            assertEquals(Integer.valueOf(uri).intValue(), Math.min(a, b));
            assertEquals(NUM_ROWS_INSERTED / 2 + Integer.valueOf(uri), Math.max(a, b));
            count++;
        }
        assertEquals(NUM_ROWS_INSERTED / 2, count);
        
        conn.createStatement();
        rs = stmt.executeQuery("SELECT appcpu FROM " + tableName + " group by appcpu limit 1");

        assertTrue(rs.next());
        assertEquals(10,rs.getInt(1));
        assertFalse(rs.next());
        
        stmt = conn.createStatement();
        rs = stmt.executeQuery("SELECT to_number(uri) FROM " + tableName + " group by to_number(uri) limit 100");
        count = 0;
        while (rs.next()) {
            count++;
        }
        assertEquals(100, count);
    }

    @Test
    public void testStatisticsAreNotWritten() throws SQLException {
        String tableName = generateUniqueName();
        Properties props = PropertiesUtil.deepCopy(TEST_PROPERTIES);
        Connection conn = DriverManager.getConnection(getUrl(), props);
        Statement stmt = conn.createStatement();
        stmt.execute("CREATE TABLE " + tableName + " (ID INTEGER NOT NULL PRIMARY KEY, NAME VARCHAR)");
        stmt.execute("UPSERT INTO " + tableName + " VALUES (1, 'NAME1')");
        stmt.execute("UPSERT INTO " + tableName + " VALUES (2, 'NAME2')");
        stmt.execute("UPSERT INTO " + tableName + " VALUES (3, 'NAME3')");
        conn.commit();
        try {
            stmt.execute("UPDATE STATISTICS " + tableName);
            Assert.fail("Update Statistics SQL should have failed");
        } catch (SQLException e) {
            Assert.assertEquals("StatsCollectionDisabledOnServerException expected",
                    1401, e.getErrorCode());
            Assert.assertEquals("StatsCollectionDisabledOnServerException expected",
                    "STS01", e.getSQLState());
        }
        ResultSet rs = stmt.executeQuery("SELECT * FROM \"SYSTEM\".STATS");
        assertFalse(rs.next());
        rs.close();
        stmt.close();
        String query = "SELECT * FROM " + tableName;
        ExplainPlan plan = conn.prepareStatement(query)
            .unwrap(PhoenixPreparedStatement.class).optimizeQuery()
            .getExplainPlan();
        ExplainPlanAttributes explainPlanAttributes =
            plan.getPlanStepsAsAttributes();
        assertEquals(new Integer(1), explainPlanAttributes.getSplitsChunk());
        assertEquals("PARALLEL 1-WAY",
            explainPlanAttributes.getIteratorTypeAndScanSize());
        assertEquals("FULL SCAN ",
            explainPlanAttributes.getExplainScanType());
        assertEquals(tableName, explainPlanAttributes.getTableName());
        conn.close();
    }
    
    @Test
    public void testDistinctCountFails() throws Exception {
        String tableName = generateUniqueName();
        Properties props = PropertiesUtil.deepCopy(TEST_PROPERTIES);
        Connection conn = DriverManager.getConnection(getUrl(), props);
        createTable(conn, tableName);
        loadUniqueURIData(conn, tableName, 1000);
        props = PropertiesUtil.deepCopy(TEST_PROPERTIES);
        Statement stmt = conn.createStatement();
        ResultSet rs = stmt.executeQuery(String.format(GROUPBY2, tableName));
        try {
            rs.next();
            fail();
        } catch (SQLException e) {
            assertEquals(SQLExceptionCode.INSUFFICIENT_MEMORY.getErrorCode(),e.getErrorCode());
        }

    }
}