/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you maynot use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicablelaw or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.phoenix.end2end;

import static org.apache.phoenix.util.TestUtil.TEST_PROPERTIES;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ParameterMetaData;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Types;
import java.util.Arrays;
import java.util.Collection;
import java.util.Map;
import java.util.Properties;

import org.apache.phoenix.query.QueryServices;
import org.apache.phoenix.util.PropertiesUtil;
import org.apache.phoenix.util.QueryUtil;
import org.apache.phoenix.util.ReadOnlyProps;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;

import com.google.common.collect.Maps;

@RunWith(Parameterized.class)
public class QueryWithOffsetIT extends BaseOwnClusterHBaseManagedTimeIT {
    
    private String tableName = "T";
    private final String[] strings = { "a", "b", "c", "d", "e", "f", "g", "h", "i", "j", "k", "l", "m", "n", "o", "p",
            "q", "r", "s", "t", "u", "v", "w", "x", "y", "z" };
    private final String ddl;
    private final boolean isSalted;

    public QueryWithOffsetIT(String preSplit) {
        this.tableName=tableName + "_" + preSplit.charAt(2);
        this.ddl = "CREATE TABLE " + tableName + " (t_id VARCHAR NOT NULL,\n" + "k1 INTEGER NOT NULL,\n"
                + "k2 INTEGER NOT NULL,\n" + "C3.k3 INTEGER,\n" + "C2.v1 VARCHAR,\n"
                + "CONSTRAINT pk PRIMARY KEY (t_id, k1, k2)) " + preSplit;
        this.isSalted = preSplit.startsWith(" SALT_BUCKETS");
    }

    @BeforeClass
    public static void doSetup() throws Exception {
        Map<String, String> props = Maps.newHashMapWithExpectedSize(1);
        props.put(QueryServices.FORCE_ROW_KEY_ORDER_ATTRIB, Boolean.toString(true));
        // Must update config before starting server
        setUpTestDriver(new ReadOnlyProps(props.entrySet().iterator()));
    }

    @Parameters(name="preSplit = {0}")
    public static Collection<String> data() {
        return Arrays.asList(new String[] { " SPLIT ON ('e','i','o')", " SALT_BUCKETS=10" });
    }

    @Test
    public void testLimitOffset() throws SQLException {
        Connection conn;
        Properties props = PropertiesUtil.deepCopy(TEST_PROPERTIES);
        conn = DriverManager.getConnection(getUrl(), props);
        createTestTable(getUrl(), ddl);
        initTableValues(conn);
        int limit = 10;
        int offset = 10;
        updateStatistics(conn);
        ResultSet rs;
        rs = conn.createStatement()
                .executeQuery("SELECT t_id from " + tableName + " order by t_id limit " + limit + " offset " + offset);
        int i = 0;
        while (i < limit) {
            assertTrue(rs.next());
            assertEquals("Expected string didn't match for i = " + i, strings[offset + i], rs.getString(1));
            i++;
        }

        limit = 35;
        rs = conn.createStatement().executeQuery("SELECT t_id from " + tableName + " union all SELECT t_id from "
                + tableName + " offset " + offset + " FETCH FIRST " + limit + " rows only");
        i = 0;
        while (i++ < strings.length - offset) {
            assertTrue(rs.next());
            assertEquals(strings[offset + i - 1], rs.getString(1));
        }
        i = 0;
        while (i++ < limit - strings.length - offset) {
            assertTrue(rs.next());
            assertEquals(strings[i - 1], rs.getString(1));
        }
        conn.close();
    }

    @Test
    public void testOffsetSerialQueryExecutedOnServer() throws SQLException {
        Connection conn;
        Properties props = PropertiesUtil.deepCopy(TEST_PROPERTIES);
        conn = DriverManager.getConnection(getUrl(), props);
        int offset = 10;
        createTestTable(getUrl(), ddl);
        initTableValues(conn);
        updateStatistics(conn);
        String query = "SELECT t_id from " + tableName + " offset " + offset;
        ResultSet rs = conn.createStatement().executeQuery("EXPLAIN " + query);
        if(!isSalted){
            assertEquals("CLIENT SERIAL 1-WAY FULL SCAN OVER T_P\n" + "    SERVER FILTER BY FIRST KEY ONLY\n"
                    + "    SERVER OFFSET " + offset, QueryUtil.getExplainPlan(rs));
        }else{
            assertEquals("CLIENT PARALLEL 10-WAY FULL SCAN OVER T_A\n" + "    SERVER FILTER BY FIRST KEY ONLY\n"
                    + "CLIENT MERGE SORT\n" + "CLIENT OFFSET " + offset, QueryUtil.getExplainPlan(rs));
        }
        rs = conn.createStatement().executeQuery(query);
        int i = 0;
        while (i++ < strings.length - offset) {
            assertTrue(rs.next());
            assertEquals(strings[offset + i - 1], rs.getString(1));
        }
        query = "SELECT t_id from " + tableName + " ORDER BY v1 offset " + offset;
        rs = conn.createStatement().executeQuery("EXPLAIN " + query);
        if (!isSalted) {
            assertEquals("CLIENT PARALLEL 5-WAY FULL SCAN OVER T_P\n" + "    SERVER SORTED BY [C2.V1]\n"
                    + "CLIENT MERGE SORT\n" + "CLIENT OFFSET " + offset, QueryUtil.getExplainPlan(rs));
        } else {
            assertEquals("CLIENT PARALLEL 10-WAY FULL SCAN OVER T_A\n" + "    SERVER SORTED BY [C2.V1]\n"
                    + "CLIENT MERGE SORT\n" + "CLIENT OFFSET " + offset, QueryUtil.getExplainPlan(rs));
        }
        conn.close();
    }

    @Test
    public void testOffsetWithoutLimit() throws SQLException {
        Connection conn;
        Properties props = PropertiesUtil.deepCopy(TEST_PROPERTIES);
        conn = DriverManager.getConnection(getUrl(), props);
        int offset = 10;
        createTestTable(getUrl(), ddl);
        initTableValues(conn);
        updateStatistics(conn);
        ResultSet rs;
        rs = conn.createStatement()
                .executeQuery("SELECT t_id from " + tableName + " order by t_id offset " + offset + " row");
        int i = 0;
        while (i++ < strings.length - offset) {
            assertTrue(rs.next());
            assertEquals(strings[offset + i - 1], rs.getString(1));
        }

        rs = conn.createStatement().executeQuery(
                "SELECT k3, count(*) from " + tableName + " group by k3 order by k3 desc offset " + offset + " row");

        i = 0;
        while (i++ < strings.length - offset) {
            assertTrue(rs.next());
            assertEquals(strings.length - offset - i + 2, rs.getInt(1));
        }

        rs = conn.createStatement().executeQuery("SELECT t_id from " + tableName + " union all SELECT t_id from "
                + tableName + " offset " + offset + " rows");
        i = 0;
        while (i++ < strings.length - offset) {
            assertTrue(rs.next());
            assertEquals(strings[offset + i - 1], rs.getString(1));
        }
        i = 0;
        while (i++ < strings.length) {
            assertTrue(rs.next());
            assertEquals(strings[i - 1], rs.getString(1));
        }
        conn.close();
    }

    @Test
    public void testMetaDataWithOffset() throws SQLException {
        Connection conn;
        Properties props = PropertiesUtil.deepCopy(TEST_PROPERTIES);
        conn = DriverManager.getConnection(getUrl(), props);
        createTestTable(getUrl(), ddl);
        initTableValues(conn);
        updateStatistics(conn);
        PreparedStatement stmt = conn.prepareStatement("SELECT * from " + tableName + " offset ?");
        ParameterMetaData pmd = stmt.getParameterMetaData();
        assertEquals(1, pmd.getParameterCount());
        assertEquals(Types.INTEGER, pmd.getParameterType(1));
        stmt.setInt(1, 10);
        ResultSet rs = stmt.executeQuery();
        ResultSetMetaData md = rs.getMetaData();
        assertEquals(5, md.getColumnCount());
    }
    
    private void initTableValues(Connection conn) throws SQLException {
        for (int i = 0; i < 26; i++) {
            conn.createStatement().execute("UPSERT INTO " + tableName + " values('" + strings[i] + "'," + i + ","
                    + (i + 1) + "," + (i + 2) + ",'" + strings[25 - i] + "')");
        }
        conn.commit();
    }

    private void updateStatistics(Connection conn) throws SQLException {
        String query = "UPDATE STATISTICS " + tableName + " SET \"" + QueryServices.STATS_GUIDEPOST_WIDTH_BYTES_ATTRIB
                + "\"=" + Long.toString(500);
        conn.createStatement().execute(query);
    }

}
