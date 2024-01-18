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
import org.apache.phoenix.schema.PTable;
import org.apache.phoenix.schema.TableProperty;
import org.apache.phoenix.util.CDCUtil;
import org.apache.phoenix.util.PhoenixRuntime;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;

import static org.apache.phoenix.query.QueryConstants.CHANGE_IMAGE;
import static org.apache.phoenix.query.QueryConstants.DELETE_EVENT_TYPE;
import static org.apache.phoenix.query.QueryConstants.EVENT_TYPE;
import static org.apache.phoenix.query.QueryConstants.POST_IMAGE;
import static org.apache.phoenix.query.QueryConstants.PRE_IMAGE;
import static org.apache.phoenix.query.QueryConstants.UPSERT_EVENT_TYPE;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

@Category(ParallelStatsDisabledTest.class)
public class CDCMiscIT extends ParallelStatsDisabledIT {
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

    @Test
    public void testCreate() throws SQLException {
        Properties props = new Properties();
        Connection conn = DriverManager.getConnection(getUrl(), props);
        String tableName = generateUniqueName();
        conn.createStatement().execute(
                "CREATE TABLE  " + tableName + " ( k INTEGER PRIMARY KEY," + " v1 INTEGER,"
                        + " v2 DATE)");
        String cdcName = generateUniqueName();

        try {
            conn.createStatement().execute("CREATE CDC " + cdcName
                    + " ON NON_EXISTENT_TABLE (PHOENIX_ROW_TIMESTAMP())");
            fail("Expected to fail due to non-existent table");
        } catch (SQLException e) {
            assertEquals(SQLExceptionCode.TABLE_UNDEFINED.getErrorCode(), e.getErrorCode());
        }

        try {
            conn.createStatement().execute("CREATE CDC " + cdcName
                    + " ON " + tableName +"(UNKNOWN_FUNCTION())");
            fail("Expected to fail due to invalid function");
        } catch (SQLException e) {
            assertEquals(SQLExceptionCode.FUNCTION_UNDEFINED.getErrorCode(), e.getErrorCode());
        }

        try {
            conn.createStatement().execute("CREATE CDC " + cdcName
                    + " ON " + tableName +"(NOW())");
            fail("Expected to fail due to non-deterministic function");
        } catch (SQLException e) {
            assertEquals(SQLExceptionCode.NON_DETERMINISTIC_EXPRESSION_NOT_ALLOWED_IN_INDEX.
                    getErrorCode(), e.getErrorCode());
        }

        try {
            conn.createStatement().execute("CREATE CDC " + cdcName
                    + " ON " + tableName +"(ROUND(v1))");
            fail("Expected to fail due to non-date expression in the index PK");
        } catch (SQLException e) {
            assertEquals(SQLExceptionCode.INCORRECT_DATATYPE_FOR_EXPRESSION.getErrorCode(),
                    e.getErrorCode());
        }

        try {
            conn.createStatement().execute("CREATE CDC " + cdcName
                    + " ON " + tableName +"(v1)");
            fail("Expected to fail due to non-date column in the index PK");
        } catch (SQLException e) {
            assertEquals(SQLExceptionCode.INCORRECT_DATATYPE_FOR_EXPRESSION.getErrorCode(),
                    e.getErrorCode());
        }

        String cdc_sql = "CREATE CDC " + cdcName
                + " ON " + tableName + "(PHOENIX_ROW_TIMESTAMP())";
        conn.createStatement().execute(cdc_sql);
        assertCDCState(conn, cdcName, null, 3);

        try {
            conn.createStatement().execute(cdc_sql);
            fail("Expected to fail due to duplicate index");
        } catch (SQLException e) {
            assertEquals(SQLExceptionCode.TABLE_ALREADY_EXIST.getErrorCode(), e.getErrorCode());
            assertTrue(e.getMessage().endsWith(cdcName));
        }
        conn.createStatement().execute("CREATE CDC IF NOT EXISTS " + cdcName + " ON " + tableName +
                "(v2) INCLUDE (pre, post) INDEX_TYPE=g");

        cdcName = generateUniqueName();
        conn.createStatement().execute("CREATE CDC " + cdcName + " ON " + tableName +
                "(v2) INCLUDE (pre, post) INDEX_TYPE=g");
        assertCDCState(conn, cdcName, "PRE,POST", 3);
        assertPTable(cdcName, new HashSet<>(
                Arrays.asList(PTable.CDCChangeScope.PRE, PTable.CDCChangeScope.POST)), tableName);

        cdcName = generateUniqueName();
        conn.createStatement().execute("CREATE CDC " + cdcName + " ON " + tableName +
                "(v2) INDEX_TYPE=l");
        assertCDCState(conn, cdcName, null, 2);
        assertPTable(cdcName, null, tableName);

        String viewName = generateUniqueName();
        conn.createStatement().execute("CREATE VIEW " + viewName + " AS SELECT * FROM " +
                tableName);
        cdcName = generateUniqueName();
        try {
            conn.createStatement().execute("CREATE CDC " + cdcName + " ON " + viewName +
                    "(PHOENIX_ROW_TIMESTAMP())");
            fail("Expected to fail on VIEW");
        }
        catch(SQLException e) {
            assertEquals(SQLExceptionCode.INVALID_TABLE_TYPE_FOR_CDC.getErrorCode(),
                    e.getErrorCode());
            assertTrue(e.getMessage().endsWith(
                    SQLExceptionCode.INVALID_TABLE_TYPE_FOR_CDC.getMessage() + " tableType=VIEW"));
        }

        cdcName = generateUniqueName();
        conn.createStatement().execute("CREATE CDC " + cdcName
                + " ON " + tableName + "(PHOENIX_ROW_TIMESTAMP()) SALT_BUCKETS = 4");
        assertSaltBuckets(cdcName, 4);

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
        conn.createStatement().execute("CREATE CDC " + cdcName + " ON " + tableName +
                "(PHOENIX_ROW_TIMESTAMP())");

        PTable indexTable = PhoenixRuntime.getTable(conn, CDCUtil.getCDCIndexName(cdcName));
        List<PColumn> idxPkColumns = indexTable.getPKColumns();
        assertEquals(":TENANTID", idxPkColumns.get(0).getName().getString());
        assertEquals(": PHOENIX_ROW_TIMESTAMP()", idxPkColumns.get(1).getName().getString());
        assertEquals(":K", idxPkColumns.get(2).getName().getString());

        PTable cdcTable = PhoenixRuntime.getTable(conn, cdcName);
        List<PColumn> cdcPkColumns = cdcTable.getPKColumns();
        assertEquals(" PHOENIX_ROW_TIMESTAMP()", cdcPkColumns.get(0).getName().getString());
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
        String cdc_sql = "CREATE CDC " + cdcName
                + " ON " + tableName + "(PHOENIX_ROW_TIMESTAMP())";
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

    private void assertResultSet(ResultSet rs, Set<PTable.CDCChangeScope> cdcChangeScopeSet) throws Exception{
        Gson gson = new Gson();
        assertEquals(true, rs.next());
        assertEquals(1, rs.getInt(2));
        Map<String, Object> row1 = new HashMap<String, Object>(){{
            put(EVENT_TYPE, UPSERT_EVENT_TYPE);
        }};
        if (cdcChangeScopeSet == null || cdcChangeScopeSet.contains(PTable.CDCChangeScope.POST)) {
            row1.put(POST_IMAGE, new HashMap<String, Double>() {{
                put("V1", 100d);
                put("V2", 1000d);
            }});
        }
        if (cdcChangeScopeSet == null || cdcChangeScopeSet.contains(PTable.CDCChangeScope.CHANGE)) {
            row1.put(CHANGE_IMAGE, new HashMap<String, Double>() {{
                put("V1", 100d);
                put("V2", 1000d);
            }});
        }
        if (cdcChangeScopeSet == null || cdcChangeScopeSet.contains(PTable.CDCChangeScope.PRE)) {
            row1.put(PRE_IMAGE, new HashMap<String, Double>() {{
            }});
        }
        assertEquals(row1, gson.fromJson(rs.getString(3),
                HashMap.class));

        assertEquals(true, rs.next());
        assertEquals(2, rs.getInt(2));
        Map<String, Object> row2 = new HashMap<String, Object>(){{
            put(EVENT_TYPE, UPSERT_EVENT_TYPE);
        }};
        if (cdcChangeScopeSet == null || cdcChangeScopeSet.contains(PTable.CDCChangeScope.POST)) {
            row2.put(POST_IMAGE, new HashMap<String, Double>() {{
                put("V1", 200d);
                put("V2", 2000d);
            }});
        }
        if (cdcChangeScopeSet == null || cdcChangeScopeSet.contains(PTable.CDCChangeScope.CHANGE)) {
            row2.put(CHANGE_IMAGE, new HashMap<String, Double>() {{
                put("V1", 200d);
                put("V2", 2000d);
            }});
        }
        if (cdcChangeScopeSet == null || cdcChangeScopeSet.contains(PTable.CDCChangeScope.PRE)) {
            row2.put(PRE_IMAGE, new HashMap<String, Double>() {{
            }});
        }
        assertEquals(row2, gson.fromJson(rs.getString(3),
                HashMap.class));
        assertEquals(true, rs.next());
        assertEquals(1, rs.getInt(2));
        Map<String, Object> row3 = new HashMap<String, Object>(){{
            put(EVENT_TYPE, UPSERT_EVENT_TYPE);
        }};
        if (cdcChangeScopeSet == null || cdcChangeScopeSet.contains(PTable.CDCChangeScope.POST)) {
            row3.put(POST_IMAGE, new HashMap<String, Double>() {{
                put("V1", 101d);
                put("V2", 1000d);
            }});
        }
        if (cdcChangeScopeSet == null || cdcChangeScopeSet.contains(PTable.CDCChangeScope.CHANGE)) {
            row3.put(CHANGE_IMAGE, new HashMap<String, Double>() {{
                put("V1", 101d);
            }});
        }
        if (cdcChangeScopeSet == null || cdcChangeScopeSet.contains(PTable.CDCChangeScope.PRE)) {
            row3.put(PRE_IMAGE, new HashMap<String, Double>() {{
                put("V1", 100d);
                put("V2", 1000d);
            }});
        }
        assertEquals(row3, gson.fromJson(rs.getString(3),
                HashMap.class));

        assertEquals(true, rs.next());
        assertEquals(1, rs.getInt(2));
        Map<String, Object> row4 = new HashMap<String, Object>(){{
            put(EVENT_TYPE, DELETE_EVENT_TYPE);
        }};
        if (cdcChangeScopeSet == null || cdcChangeScopeSet.contains(PTable.CDCChangeScope.POST)) {
            row4.put(POST_IMAGE, new HashMap<String, Double>() {{
            }});
        }
        if (cdcChangeScopeSet == null || cdcChangeScopeSet.contains(PTable.CDCChangeScope.CHANGE)) {
            row4.put(CHANGE_IMAGE, new HashMap<String, Double>() {{
            }});
        }
        if (cdcChangeScopeSet == null || cdcChangeScopeSet.contains(PTable.CDCChangeScope.PRE)) {
            row4.put(PRE_IMAGE, new HashMap<String, Double>() {{
                put("V1", 101d);
                put("V2", 1000d);
            }});
        }
        assertEquals(row4, gson.fromJson(rs.getString(3),
                HashMap.class));

        assertEquals(true, rs.next());
        assertEquals(1, rs.getInt(2));
        Map<String, Object> row5 = new HashMap<String, Object>(){{
            put(EVENT_TYPE, UPSERT_EVENT_TYPE);
        }};
        if (cdcChangeScopeSet == null || cdcChangeScopeSet.contains(PTable.CDCChangeScope.POST)) {
            row5.put(POST_IMAGE, new HashMap<String, Double>() {{
                put("V1", 102d);
                put("V2", 1002d);
            }});
        }
        if (cdcChangeScopeSet == null || cdcChangeScopeSet.contains(PTable.CDCChangeScope.CHANGE)) {
            row5.put(CHANGE_IMAGE, new HashMap<String, Double>() {{
                put("V1", 102d);
                put("V2", 1002d);
            }});
        }
        if (cdcChangeScopeSet == null || cdcChangeScopeSet.contains(PTable.CDCChangeScope.PRE)) {
            row5.put(PRE_IMAGE, new HashMap<String, Double>() {{
            }});
        }
        assertEquals(row5, gson.fromJson(rs.getString(3),
                HashMap.class));

        assertEquals(true, rs.next());
        assertEquals(1, rs.getInt(2));
        Map<String, Object> row6 = new HashMap<String, Object>(){{
            put(EVENT_TYPE, DELETE_EVENT_TYPE);
        }};
        if (cdcChangeScopeSet == null || cdcChangeScopeSet.contains(PTable.CDCChangeScope.POST)) {
            row6.put(POST_IMAGE, new HashMap<String, Double>() {{
            }});
        }
        if (cdcChangeScopeSet == null || cdcChangeScopeSet.contains(PTable.CDCChangeScope.CHANGE)) {
            row6.put(CHANGE_IMAGE, new HashMap<String, Double>() {{
            }});
        }
        if (cdcChangeScopeSet == null || cdcChangeScopeSet.contains(PTable.CDCChangeScope.PRE)) {
            row6.put(PRE_IMAGE, new HashMap<String, Double>() {{
                put("V1", 102d);
                put("V2", 1002d);
            }});
        }
        assertEquals(row6, gson.fromJson(rs.getString(3),
                HashMap.class));

        assertEquals(false, rs.next());
        rs.close();
    }

    @Test
    public void testSelectCDC() throws Exception {
        Properties props = new Properties();
        props.put(QueryServices.TASK_HANDLING_INTERVAL_MS_ATTRIB, Long.toString(Long.MAX_VALUE));
        props.put("hbase.client.scanner.timeout.period", "6000000");
        props.put("phoenix.query.timeoutMs", "6000000");
        props.put("zookeeper.session.timeout", "6000000");
        props.put("hbase.rpc.timeout", "6000000");
        Connection conn = DriverManager.getConnection(getUrl(), props);
        String tableName = generateUniqueName();
        conn.createStatement().execute(
                "CREATE TABLE  " + tableName + " ( k INTEGER PRIMARY KEY," + " v1 INTEGER, v2 INTEGER)");
        String cdcName = generateUniqueName();

        conn.createStatement().execute("UPSERT INTO " + tableName + " (k, v1, v2) VALUES (1, 100, 1000)");
        conn.createStatement().execute("UPSERT INTO " + tableName + " (k, v1, v2) VALUES (2, 200, 2000)");
        conn.commit();
        Thread.sleep(1000);
        conn.createStatement().execute("UPSERT INTO " + tableName + " (k, v1) VALUES (1, 101)");
        conn.commit();
        //conn.createStatement().execute("ALTER TABLE " + tableName + " DROP COLUMN v2");
        conn.createStatement().execute("DELETE FROM " + tableName + " WHERE k=1");
        conn.commit();
        conn.createStatement().execute("UPSERT INTO " + tableName + " (k, v1, v2) VALUES (1, 102, 1002)");
        conn.commit();
        conn.createStatement().execute("DELETE FROM " + tableName + " WHERE k=1");
        conn.commit();
        // NOTE: To debug the query execution, add the below condition where you need a breakpoint.
        //      if (<table>.getTableName().getString().equals("N000002") ||
        //                 <table>.getTableName().getString().equals("__CDC__N000002")) {
        //          "".isEmpty();
        //      }
        String cdc_sql = "CREATE CDC " + cdcName
                + " ON " + tableName + "(PHOENIX_ROW_TIMESTAMP())";
        conn.createStatement().execute(cdc_sql);
        assertCDCState(conn, cdcName, null, 3);

        assertResultSet(conn.createStatement().executeQuery("SELECT * FROM " + cdcName), null);
        assertResultSet(conn.createStatement().executeQuery("SELECT * FROM " + cdcName +
                " WHERE PHOENIX_ROW_TIMESTAMP() < NOW()"), null);
        assertResultSet(conn.createStatement().executeQuery("SELECT /*+ INCLUDE(PRE, POST) */ * " +
                "FROM " + cdcName), new HashSet<PTable.CDCChangeScope>(
                        Arrays.asList(PTable.CDCChangeScope.PRE, PTable.CDCChangeScope.POST)));
        assertResultSet(conn.createStatement().executeQuery("SELECT " +
                "PHOENIX_ROW_TIMESTAMP(), K, \"CDC JSON\" FROM " + cdcName), null);

        // Have to Debug it further,
//        HashMap<String, int[]> testQueries = new HashMap<String, int[]>() {{
//            put("SELECT 'dummy', k FROM " + cdcName, new int [] {2, 1});
//            put("SELECT * FROM " + cdcName +
//                    " ORDER BY k ASC", new int [] {1, 1, 2});
//            put("SELECT * FROM " + cdcName +
//                    " ORDER BY k DESC", new int [] {2, 1, 1});
//            put("SELECT * FROM " + cdcName +
//                    " ORDER BY PHOENIX_ROW_TIMESTAMP() ASC", new int [] {1, 2, 1});
//        }};
//        for (Map.Entry<String, int[]> testQuery: testQueries.entrySet()) {
//            try (ResultSet rs = conn.createStatement().executeQuery(testQuery.getKey())) {
//                for (int k:  testQuery.getValue()) {
//                    assertEquals(true, rs.next());
//                    assertEquals(k, rs.getInt(2));
//                }
//                assertEquals(false, rs.next());
//            }
//        }

        try (ResultSet rs = conn.createStatement().executeQuery(
                "SELECT * FROM " + cdcName + " WHERE PHOENIX_ROW_TIMESTAMP() > NOW()")) {
            assertEquals(false, rs.next());
        }
//        try (ResultSet rs = conn.createStatement().executeQuery("SELECT 'abc' FROM " + cdcName)) {
//            assertEquals(true, rs.next());
//            assertEquals("abc", rs.getString(1));
//            assertEquals(true, rs.next());
//            assertEquals("abc", rs.getString(1));
//            assertEquals(true, rs.next());
//            assertEquals("abc", rs.getString(1));
//            assertEquals(true, rs.next());
//            assertEquals("abc", rs.getString(1));
//            assertEquals(true, rs.next());
//            assertEquals("abc", rs.getString(1));
//            assertEquals(true, rs.next());
//            assertEquals("abc", rs.getString(1));
//            assertEquals(false, rs.next());
//        }
    }

    // Temporary test case used as a reference for debugging and comparing against the CDC query.
    @Test
    public void testSelectUncoveredIndex() throws Exception {
        Properties props = new Properties();
        props.put(QueryServices.TASK_HANDLING_INTERVAL_MS_ATTRIB, Long.toString(Long.MAX_VALUE));
        props.put("hbase.client.scanner.timeout.period", "6000000");
        props.put("phoenix.query.timeoutMs", "6000000");
        props.put("zookeeper.session.timeout", "6000000");
        props.put("hbase.rpc.timeout", "6000000");
        Connection conn = DriverManager.getConnection(getUrl(), props);
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
