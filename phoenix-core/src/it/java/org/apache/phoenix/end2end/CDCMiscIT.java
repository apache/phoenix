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

import com.google.gson.Gson;
import org.apache.phoenix.exception.SQLExceptionCode;
import org.apache.phoenix.query.QueryServices;
import org.apache.phoenix.schema.PColumn;
import org.apache.phoenix.schema.PIndexState;
import org.apache.phoenix.schema.PTable;
import org.apache.phoenix.schema.TableProperty;
import org.apache.phoenix.util.CDCUtil;
import org.apache.phoenix.util.PhoenixRuntime;
import org.apache.phoenix.util.TestUtil;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

@RunWith(Parameterized.class)
@Category(ParallelStatsDisabledTest.class)
public class CDCMiscIT extends ParallelStatsDisabledIT {
    private final boolean forView;

    public CDCMiscIT(boolean forView) {
        this.forView = forView;
    }

    @Parameterized.Parameters(name = "forVieiw={0}")
    public static synchronized Collection<Boolean[]> data() {
        return Arrays.asList(new Boolean[][] {
                { false}, { true }
        });
    }

    private void assertCDCState(Connection conn, String cdcName, String expInclude,
                                int idxType) throws SQLException {
        try (ResultSet rs = conn.createStatement().executeQuery("SELECT cdc_include FROM " +
                "system.catalog WHERE table_name = '" + cdcName +
                "' AND column_name IS NULL and column_family IS NULL")) {
            assertEquals(true, rs.next());
            assertEquals(expInclude, rs.getString(1));
        }
        try (ResultSet rs = conn.createStatement().executeQuery("SELECT index_type FROM " +
                "system.catalog WHERE table_name = '" + CDCUtil.getCDCIndexName(cdcName) +
                "' AND column_name IS NULL and column_family IS NULL")) {
                assertEquals(true, rs.next());
            assertEquals(idxType, rs.getInt(1));
        }
    }

    private void assertPTable(String cdcName, Set<PTable.CDCChangeScope> expIncludeScopes,
                              String datatableName)
            throws SQLException {
        Properties props = new Properties();
        Connection conn = DriverManager.getConnection(getUrl(), props);
        PTable table = PhoenixRuntime.getTable(conn, cdcName);
        assertEquals(expIncludeScopes, table.getCDCIncludeScopes());
        assertEquals(expIncludeScopes, TableProperty.INCLUDE.getPTableValue(table));
        assertNull(table.getIndexState()); // Index state should be null for CDC.
        assertNull(table.getIndexType()); // This is not an index.
        assertEquals(datatableName, table.getParentName().getString());
        assertEquals(CDCUtil.getCDCIndexName(cdcName), table.getPhysicalName().getString());
    }

    private void assertSaltBuckets(String cdcName, Integer nbuckets) throws SQLException {
        Properties props = new Properties();
        Connection conn = DriverManager.getConnection(getUrl(), props);
        PTable cdcTable = PhoenixRuntime.getTable(conn, cdcName);
        assertEquals(nbuckets, cdcTable.getBucketNum());
        PTable indexTable = PhoenixRuntime.getTable(conn, CDCUtil.getCDCIndexName(cdcName));
        assertEquals(nbuckets, indexTable.getBucketNum());
    }

    private void createAndWait(Connection conn, String tableName, String cdcName, String cdc_sql)
            throws Exception {
        conn.createStatement().execute(cdc_sql);
        IndexToolIT.runIndexTool(false, null, tableName,
                "\""+CDCUtil.getCDCIndexName(cdcName)+"\"");
        TestUtil.waitForIndexState(conn, CDCUtil.getCDCIndexName(cdcName), PIndexState.ACTIVE);
    }

    @Test
    public void testCreate() throws Exception {
        Properties props = new Properties();
        Connection conn = DriverManager.getConnection(getUrl(), props);
        String tableName = generateUniqueName();
        conn.createStatement().execute(
                "CREATE TABLE  " + tableName + " ( k INTEGER PRIMARY KEY," + " v1 INTEGER,"
                        + " v2 DATE)");
        if (forView) {
            String viewName = generateUniqueName();
            conn.createStatement().execute(
                    "CREATE VIEW " + viewName + " AS SELECT * FROM " + tableName);
            tableName = viewName;
        }
        String cdcName = generateUniqueName();

        try {
            conn.createStatement().execute("CREATE CDC " + cdcName
                    + " ON NON_EXISTENT_TABLE");
            fail("Expected to fail due to non-existent table");
        } catch (SQLException e) {
            assertEquals(SQLExceptionCode.TABLE_UNDEFINED.getErrorCode(), e.getErrorCode());
        }

        try {
            conn.createStatement().execute("CREATE CDC " + cdcName
                    + " ON " + tableName + " INCLUDE (abc)");
            fail("Expected to fail due to invalid INCLUDE");
        } catch (SQLException e) {
            assertEquals(SQLExceptionCode.UNKNOWN_INCLUDE_CHANGE_SCOPE.getErrorCode(),
                    e.getErrorCode());
            assertTrue(e.getMessage().endsWith("abc"));
        }

        String cdc_sql = "CREATE CDC " + cdcName + " ON " + tableName;
        createAndWait(conn, tableName, cdcName, cdc_sql);
        assertCDCState(conn, cdcName, null, 3);

        try {
            conn.createStatement().execute(cdc_sql);
            fail("Expected to fail due to duplicate index");
        } catch (SQLException e) {
            assertEquals(SQLExceptionCode.TABLE_ALREADY_EXIST.getErrorCode(), e.getErrorCode());
            assertTrue(e.getMessage().endsWith(cdcName));
        }

        conn.createStatement().execute("CREATE CDC IF NOT EXISTS " + cdcName + " ON " + tableName +
                " INCLUDE (pre, post) INDEX_TYPE=g");

        cdcName = generateUniqueName();
        cdc_sql = "CREATE CDC " + cdcName + " ON " + tableName +
                " INCLUDE (pre, post) INDEX_TYPE=g";
        createAndWait(conn, tableName, cdcName, cdc_sql);
        assertCDCState(conn, cdcName, "PRE,POST", 3);
        assertPTable(cdcName, new HashSet<>(
                Arrays.asList(PTable.CDCChangeScope.PRE, PTable.CDCChangeScope.POST)), tableName);

        cdcName = generateUniqueName();
        cdc_sql = "CREATE CDC " + cdcName + " ON " + tableName + " INDEX_TYPE=l";
        createAndWait(conn, tableName, cdcName, cdc_sql);
        assertCDCState(conn, cdcName, null, 2);
        assertPTable(cdcName, null, tableName);

        // Indexes on views don't support salt buckets and is currently silently ignored.
        if (! forView) {
            cdcName = generateUniqueName();
            cdc_sql = "CREATE CDC " + cdcName + " ON " + tableName + " SALT_BUCKETS = 4";
            createAndWait(conn, tableName, cdcName, cdc_sql);
            assertSaltBuckets(cdcName, 4);
        }

        conn.close();
    }

    @Test
    public void testCreateCDCMultitenant() throws Exception {
        Properties props = new Properties();
        Connection conn = DriverManager.getConnection(getUrl(), props);
        String tableName = generateUniqueName();
        conn.createStatement().execute("CREATE TABLE  " + tableName +
                " (tenantId INTEGER NOT NULL, k INTEGER NOT NULL," + " v1 INTEGER, v2 DATE, " +
                "CONSTRAINT pk PRIMARY KEY (tenantId, k)) MULTI_TENANT=true");
        String cdcName = generateUniqueName();
        conn.createStatement().execute("CREATE CDC " + cdcName + " ON " + tableName);

        PTable indexTable = PhoenixRuntime.getTable(conn, CDCUtil.getCDCIndexName(cdcName));
        List<PColumn> idxPkColumns = indexTable.getPKColumns();
        assertEquals(":TENANTID", idxPkColumns.get(0).getName().getString());
        assertEquals(": PHOENIX_ROW_TIMESTAMP()", idxPkColumns.get(1).getName().getString());
        assertEquals(":K", idxPkColumns.get(2).getName().getString());

        PTable cdcTable = PhoenixRuntime.getTable(conn, cdcName);
        List<PColumn> cdcPkColumns = cdcTable.getPKColumns();
        assertEquals("PHOENIX_ROW_TIMESTAMP()", cdcPkColumns.get(0).getName().getString());
        assertEquals("TENANTID", cdcPkColumns.get(1).getName().getString());
        assertEquals("K", cdcPkColumns.get(2).getName().getString());
    }

    public void testDropCDC () throws SQLException {
        Properties props = new Properties();
        Connection conn = DriverManager.getConnection(getUrl(), props);
        String tableName = generateUniqueName();
        conn.createStatement().execute(
                "CREATE TABLE  " + tableName + " ( k INTEGER PRIMARY KEY," + " v1 INTEGER,"
                        + " v2 DATE)");
        String cdcName = generateUniqueName();

        String drop_cdc_sql = "DROP CDC " + cdcName + " ON " + tableName;
        conn.createStatement().execute(drop_cdc_sql);

        try (ResultSet rs = conn.createStatement().executeQuery("SELECT cdc_include FROM " +
                "system.catalog WHERE table_name = '" + cdcName +
                "' AND column_name IS NULL and column_family IS NULL")) {
            assertEquals(false, rs.next());
        }
        try (ResultSet rs = conn.createStatement().executeQuery("SELECT index_type FROM " +
                "system.catalog WHERE table_name = '" + CDCUtil.getCDCIndexName(cdcName) +
                "' AND column_name IS NULL and column_family IS NULL")) {
            assertEquals(false, rs.next());
        }

        try {
            conn.createStatement().execute(drop_cdc_sql);
            fail("Expected to fail as cdc table doesn't exist");
        } catch (SQLException e) {
            assertEquals(SQLExceptionCode.TABLE_UNDEFINED.getErrorCode(), e.getErrorCode());
            assertTrue(e.getMessage().endsWith(cdcName));
        }
    }

    @Test
    public void testDropCDCIndex () throws SQLException {
        Properties props = new Properties();
        Connection conn = DriverManager.getConnection(getUrl(), props);
        String tableName = generateUniqueName();
        conn.createStatement().execute(
                "CREATE TABLE  " + tableName + " ( k INTEGER PRIMARY KEY," + " v1 INTEGER,"
                        + " v2 DATE)");
        String cdcName = generateUniqueName();
        String cdc_sql = "CREATE CDC " + cdcName + " ON " + tableName;
        conn.createStatement().execute(cdc_sql);
        assertCDCState(conn, cdcName, null, 3);
        String drop_cdc_index_sql = "DROP INDEX \"" + CDCUtil.getCDCIndexName(cdcName) + "\" ON " + tableName;
        try {
            conn.createStatement().execute(drop_cdc_index_sql);
        } catch (SQLException e) {
            assertEquals(SQLExceptionCode.CANNOT_DROP_CDC_INDEX.getErrorCode(), e.getErrorCode());
            assertTrue(e.getMessage().endsWith(CDCUtil.getCDCIndexName(cdcName)));
        }
    }

    private void assertResultSet(ResultSet rs) throws Exception{
        Gson gson = new Gson();
        assertEquals(true, rs.next());
        assertEquals(1, rs.getInt(2));
        assertEquals(new HashMap(){{put("V1", 100d);}}, gson.fromJson(rs.getString(3),
                HashMap.class));
        assertEquals(true, rs.next());
        assertEquals(2, rs.getInt(2));
        assertEquals(new HashMap(){{put("V1", 200d);}}, gson.fromJson(rs.getString(3),
                HashMap.class));
        assertEquals(true, rs.next());
        assertEquals(1, rs.getInt(2));
        assertEquals(new HashMap(){{put("V1", 101d);}}, gson.fromJson(rs.getString(3),
                HashMap.class));
        assertEquals(false, rs.next());
        rs.close();
    }

    private Connection newConnection() throws SQLException {
        Properties props = new Properties();
        props.put(QueryServices.TASK_HANDLING_INTERVAL_MS_ATTRIB, Long.toString(Long.MAX_VALUE));
        props.put("hbase.client.scanner.timeout.period", "6000000");
        props.put("phoenix.query.timeoutMs", "6000000");
        props.put("zookeeper.session.timeout", "6000000");
        props.put("hbase.rpc.timeout", "6000000");
        return DriverManager.getConnection(getUrl(), props);
    }

    @Test
    public void testSelectCDC() throws Exception {
        Connection conn = newConnection();
        String tableName = generateUniqueName();
        conn.createStatement().execute(
                "CREATE TABLE  " + tableName + " ( k INTEGER PRIMARY KEY," + " v1 INTEGER)");
        conn.createStatement().execute("UPSERT INTO " + tableName + " (k, v1) VALUES (1, 100)");
        conn.createStatement().execute("UPSERT INTO " + tableName + " (k, v1) VALUES (2, 200)");
        conn.commit();
        Thread.sleep(10);
        conn.createStatement().execute("UPSERT INTO " + tableName + " (k, v1) VALUES (1, 101)");
        conn.commit();
        String cdcName = generateUniqueName();
        String cdc_sql = "CREATE CDC " + cdcName
                + " ON " + tableName;
        createAndWait(conn, tableName, cdcName, cdc_sql);
        assertCDCState(conn, cdcName, null, 3);
        // NOTE: To debug the query execution, add the below condition where you need a breakpoint.
        //      if (<table>.getTableName().getString().equals("N000002") ||
        //                 <table>.getTableName().getString().equals("__CDC__N000002")) {
        //          "".isEmpty();
        //      }
        assertResultSet(conn.createStatement().executeQuery("SELECT * FROM " + cdcName));
        assertResultSet(conn.createStatement().executeQuery("SELECT * FROM " + cdcName +
                " WHERE PHOENIX_ROW_TIMESTAMP() < NOW()"));
        assertResultSet(conn.createStatement().executeQuery("SELECT " +
                "/*+ CDC_INCLUDE(PRE, POST) */ * FROM " + cdcName));
        assertResultSet(conn.createStatement().executeQuery("SELECT " +
                "PHOENIX_ROW_TIMESTAMP(), K, \"CDC JSON\" FROM " + cdcName));

        HashMap<String, int[]> testQueries = new HashMap<String, int[]>() {{
            put("SELECT 'dummy', k FROM " + cdcName, new int [] {2, 1});
            put("SELECT * FROM " + cdcName +
                    " ORDER BY k ASC", new int [] {1, 1, 2});
            put("SELECT * FROM " + cdcName +
                    " ORDER BY k DESC", new int [] {2, 1, 1});
            put("SELECT * FROM " + cdcName +
                    " ORDER BY PHOENIX_ROW_TIMESTAMP() ASC", new int [] {1, 2, 1});
        }};
        for (Map.Entry<String, int[]> testQuery: testQueries.entrySet()) {
            try (ResultSet rs = conn.createStatement().executeQuery(testQuery.getKey())) {
                for (int k:  testQuery.getValue()) {
                    assertEquals(true, rs.next());
                    assertEquals(k, rs.getInt(2));
                }
                assertEquals(false, rs.next());
            }
        }

        try (ResultSet rs = conn.createStatement().executeQuery(
                "SELECT * FROM " + cdcName + " WHERE PHOENIX_ROW_TIMESTAMP() > NOW()")) {
            assertEquals(false, rs.next());
        }
        try (ResultSet rs = conn.createStatement().executeQuery("SELECT 'abc' FROM " + cdcName)) {
            assertEquals(true, rs.next());
            assertEquals("abc", rs.getString(1));
            assertEquals(true, rs.next());
            assertEquals("abc", rs.getString(1));
            assertEquals(false, rs.next());
        }
    }

    @Test
    public void testSelectCDCBadIncludeSpec() throws Exception {
        Connection conn = newConnection();
        String tableName = generateUniqueName();
        conn.createStatement().execute(
                "CREATE TABLE  " + tableName + " ( k INTEGER PRIMARY KEY," + " v1 INTEGER)");
        String cdcName = generateUniqueName();
        String cdc_sql = "CREATE CDC  " + cdcName
                + " ON " + tableName;
        conn.createStatement().execute(cdc_sql);
        try {
            conn.createStatement().executeQuery("SELECT " +
                    "/*+ CDC_INCLUDE(DUMMY) */ * FROM " + cdcName);
            fail("Expected to fail due to invalid CDC INCLUDE hint");
        }
        catch (SQLException e) {
            assertEquals(SQLExceptionCode.UNKNOWN_INCLUDE_CHANGE_SCOPE.getErrorCode(),
                    e.getErrorCode());
            assertTrue(e.getMessage().endsWith("DUMMY"));
        }
    }

    @Test
    public void testSelectTimeRangeQueries() throws Exception {
        Connection conn = newConnection();
        String tableName = generateUniqueName();
        conn.createStatement().execute(
                "CREATE TABLE  " + tableName + " ( k INTEGER PRIMARY KEY," + " v1 INTEGER)");
        String cdcName = generateUniqueName();
        String cdc_sql = "CREATE CDC " + cdcName
                + " ON " + tableName;
        conn.createStatement().execute(cdc_sql);
        Timestamp ts1 = new Timestamp(System.currentTimeMillis());
        conn.createStatement().execute("UPSERT INTO " + tableName + " (k, v1) VALUES (1, 100)");
        conn.createStatement().execute("UPSERT INTO " + tableName + " (k, v1) VALUES (2, 200)");
        conn.commit();
        Thread.sleep(10);
        Timestamp ts2 = new Timestamp(System.currentTimeMillis());
        conn.createStatement().execute("UPSERT INTO " + tableName + " (k, v1) VALUES (1, 101)");
        conn.createStatement().execute("UPSERT INTO " + tableName + " (k, v1) VALUES (3, 300)");
        conn.commit();
        Thread.sleep(10);
        Timestamp ts3 = new Timestamp(System.currentTimeMillis());
        conn.createStatement().execute("UPSERT INTO " + tableName + " (k, v1) VALUES (1, 101)");
        conn.createStatement().execute("DELETE FROM " + tableName + " WHERE k = 2");
        Timestamp ts4 = new Timestamp(System.currentTimeMillis());

        String sel_sql = "SELECT * FROM " + cdcName + " WHERE PHOENIX_ROW_TIMESTAMP() >= ? AND " +
                "PHOENIX_ROW_TIMESTAMP() <= ?";
        Object[] testDataSets = new Object[] {
                new Object[] {ts1, ts2, new int[] {1, 2}}/*,
                new Object[] {ts2, ts3, new int[] {1, 3}},
                new Object[] {ts3, ts4, new int[] {1}}*/
        };
        PreparedStatement stmt = conn.prepareStatement(sel_sql);
        for (int i = 0; i < testDataSets.length; ++i) {
            Object[] testData = (Object[]) testDataSets[i];
            stmt.setTimestamp(1, (Timestamp) testData[0]);
            stmt.setTimestamp(2, (Timestamp) testData[1]);
            try (ResultSet rs = stmt.executeQuery()) {
                for (int k:  (int[]) testData[2]) {
                    assertEquals(true, rs.next());
                    assertEquals(k, rs.getInt(2));
                }
                assertEquals(false, rs.next());
            }
        }
    }

    // Temporary test case used as a reference for debugging and comparing against the CDC query.
    @Test
    public void testSelectUncoveredIndex() throws Exception {
        Connection conn = newConnection();
        String tableName = generateUniqueName();
        conn.createStatement().execute(
                "CREATE TABLE  " + tableName + " (k INTEGER PRIMARY KEY, v1 INTEGER)");
        conn.createStatement().execute("UPSERT INTO " + tableName + " (k, v1) VALUES" +
                " (1, 100)");
        conn.createStatement().execute("UPSERT INTO " + tableName + " (k, v1) VALUES" +
                " (2, 200)");
        conn.commit();
        String indexName = generateUniqueName();
        String index_sql = "CREATE UNCOVERED INDEX " + indexName
                + " ON " + tableName + "(PHOENIX_ROW_TIMESTAMP())";
        conn.createStatement().execute(index_sql);
        //ResultSet rs =
        //        conn.createStatement().executeQuery("SELECT /*+ INDEX(" + tableName +
        //                " " + indexName + ") */ * FROM " + tableName);
        ResultSet rs =
                conn.createStatement().executeQuery("SELECT /*+ INDEX(" + tableName +
                        " " + indexName + ") */ K, V1, PHOENIX_ROW_TIMESTAMP() FROM " + tableName);
        assertEquals(true, rs.next());
        assertEquals(1, rs.getInt(1));
        assertEquals(100, rs.getInt(2));
        assertEquals(true, rs.next());
        assertEquals(2, rs.getInt(1));
        assertEquals(200, rs.getInt(2));
        assertEquals(false, rs.next());
    }
}
