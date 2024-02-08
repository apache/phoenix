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
import org.apache.hadoop.hbase.KeepDeletedCells;
import org.apache.phoenix.exception.SQLExceptionCode;
import org.apache.phoenix.execute.DescVarLengthFastByteComparisons;
import org.apache.phoenix.query.QueryServices;
import org.apache.phoenix.schema.PColumn;
import org.apache.phoenix.schema.PIndexState;
import org.apache.phoenix.schema.PTable;
import org.apache.phoenix.schema.TableProperty;
import org.apache.phoenix.util.CDCUtil;
import org.apache.phoenix.util.EnvironmentEdgeManager;
import org.apache.phoenix.util.ManualEnvironmentEdge;
import org.apache.phoenix.util.PhoenixRuntime;
import org.apache.phoenix.util.TestUtil;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.sql.Connection;
import java.sql.Date;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Base64;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;

import static org.apache.phoenix.query.QueryConstants.CDC_CHANGE_IMAGE;
import static org.apache.phoenix.query.QueryConstants.CDC_DELETE_EVENT_TYPE;
import static org.apache.phoenix.query.QueryConstants.CDC_EVENT_TYPE;
import static org.apache.phoenix.query.QueryConstants.CDC_POST_IMAGE;
import static org.apache.phoenix.query.QueryConstants.CDC_PRE_IMAGE;
import static org.apache.phoenix.query.QueryConstants.CDC_UPSERT_EVENT_TYPE;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

@RunWith(Parameterized.class)
@Category(ParallelStatsDisabledTest.class)
public class CDCMiscIT extends ParallelStatsDisabledIT {
    private final boolean forView;
    private ManualEnvironmentEdge injectEdge;

    public CDCMiscIT(boolean forView) {
        this.forView = forView;
    }

    @Parameterized.Parameters(name = "forVieiw={0}")
    public static synchronized Collection<Boolean[]> data() {
        return Arrays.asList(new Boolean[][] {
                { false}, { true }
        });
    }

    @Before
    public void beforeTest(){
        EnvironmentEdgeManager.reset();
        injectEdge = new ManualEnvironmentEdge();
        injectEdge.setValue(EnvironmentEdgeManager.currentTimeMillis());
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

    private void assertResultSet(ResultSet rs, Set<PTable.CDCChangeScope> cdcChangeScopeSet) throws Exception{
        Gson gson = new Gson();
        assertEquals(true, rs.next());
        assertEquals(1, rs.getInt(2));
        Map<String, Object> row1 = new HashMap<String, Object>(){{
            put(CDC_EVENT_TYPE, CDC_UPSERT_EVENT_TYPE);
        }};
        if (cdcChangeScopeSet != null && cdcChangeScopeSet.contains(PTable.CDCChangeScope.POST)) {
            Map<String, Double> postImage = new HashMap<>();
            postImage.put("V1", 100d);
            postImage.put("V2", 1000d);
            row1.put(CDC_POST_IMAGE, postImage);
        }
        if (cdcChangeScopeSet == null || cdcChangeScopeSet.contains(PTable.CDCChangeScope.CHANGE)) {
            Map<String, Double> changeImage = new HashMap<>();
            changeImage.put("V1", 100d);
            changeImage.put("V2", 1000d);
            row1.put(CDC_CHANGE_IMAGE, changeImage);
        }
        if (cdcChangeScopeSet != null && cdcChangeScopeSet.contains(PTable.CDCChangeScope.PRE)) {
            row1.put(CDC_PRE_IMAGE, new HashMap<String, Double>() {{
            }});
        }
        assertEquals(row1, gson.fromJson(rs.getString(3),
                HashMap.class));

        assertEquals(true, rs.next());
        assertEquals(2, rs.getInt(2));
        Map<String, Object> row2 = new HashMap<String, Object>(){{
            put(CDC_EVENT_TYPE, CDC_UPSERT_EVENT_TYPE);
        }};
        if (cdcChangeScopeSet != null && cdcChangeScopeSet.contains(PTable.CDCChangeScope.POST)) {
            Map<String, Double> postImage = new HashMap<>();
            postImage.put("V1", 200d);
            postImage.put("V2", 2000d);
            row2.put(CDC_POST_IMAGE, postImage);
        }
        if (cdcChangeScopeSet == null || cdcChangeScopeSet.contains(PTable.CDCChangeScope.CHANGE)) {
            Map<String, Double> changeImage = new HashMap<>();
            changeImage.put("V1", 200d);
            changeImage.put("V2", 2000d);
            row2.put(CDC_CHANGE_IMAGE, changeImage);
        }
        if (cdcChangeScopeSet != null && cdcChangeScopeSet.contains(PTable.CDCChangeScope.PRE)) {
            row2.put(CDC_PRE_IMAGE, new HashMap<String, Double>() {{
            }});
        }
        assertEquals(row2, gson.fromJson(rs.getString(3),
                HashMap.class));
        assertEquals(true, rs.next());
        assertEquals(1, rs.getInt(2));
        Map<String, Object> row3 = new HashMap<String, Object>(){{
            put(CDC_EVENT_TYPE, CDC_UPSERT_EVENT_TYPE);
        }};
        if (cdcChangeScopeSet != null && cdcChangeScopeSet.contains(PTable.CDCChangeScope.POST)) {
            Map<String, Double> postImage = new HashMap<>();
            postImage.put("V1", 101d);
            postImage.put("V2", 1000d);
            row3.put(CDC_POST_IMAGE, postImage);
        }
        if (cdcChangeScopeSet == null || cdcChangeScopeSet.contains(PTable.CDCChangeScope.CHANGE)) {
            Map<String, Double> changeImage = new HashMap<>();
            changeImage.put("V1", 101d);
            row3.put(CDC_CHANGE_IMAGE, changeImage);
        }
        if (cdcChangeScopeSet != null && cdcChangeScopeSet.contains(PTable.CDCChangeScope.PRE)) {
            Map<String, Double> preImage = new HashMap<>();
            preImage.put("V1", 100d);
            preImage.put("V2", 1000d);
            row3.put(CDC_PRE_IMAGE, preImage);
        }
        assertEquals(row3, gson.fromJson(rs.getString(3),
                HashMap.class));

        assertEquals(true, rs.next());
        assertEquals(1, rs.getInt(2));
        Map<String, Object> row4 = new HashMap<String, Object>(){{
            put(CDC_EVENT_TYPE, CDC_DELETE_EVENT_TYPE);
        }};
        if (cdcChangeScopeSet != null && cdcChangeScopeSet.contains(PTable.CDCChangeScope.POST)) {
            row4.put(CDC_POST_IMAGE, new HashMap<String, Double>() {{
            }});
        }
        if (cdcChangeScopeSet == null || cdcChangeScopeSet.contains(PTable.CDCChangeScope.CHANGE)) {
            row4.put(CDC_CHANGE_IMAGE, new HashMap<String, Double>() {{
            }});
        }
        if (cdcChangeScopeSet != null && cdcChangeScopeSet.contains(PTable.CDCChangeScope.PRE)) {
            Map<String, Double> preImage = new HashMap<>();
            preImage.put("V1", 101d);
            preImage.put("V2", 1000d);
            row4.put(CDC_PRE_IMAGE, preImage);
        }
        assertEquals(row4, gson.fromJson(rs.getString(3),
                HashMap.class));

        assertEquals(true, rs.next());
        assertEquals(1, rs.getInt(2));
        Map<String, Object> row5 = new HashMap<String, Object>(){{
            put(CDC_EVENT_TYPE, CDC_UPSERT_EVENT_TYPE);
        }};
        if (cdcChangeScopeSet != null && cdcChangeScopeSet.contains(PTable.CDCChangeScope.POST)) {
            Map<String, Double> postImage = new HashMap<>();
            postImage.put("V1", 102d);
            postImage.put("V2", 1002d);
            row5.put(CDC_POST_IMAGE, postImage);
        }
        if (cdcChangeScopeSet == null || cdcChangeScopeSet.contains(PTable.CDCChangeScope.CHANGE)) {
            Map<String, Double> changeImage = new HashMap<>();
            changeImage.put("V1", 102d);
            changeImage.put("V2", 1002d);
            row5.put(CDC_CHANGE_IMAGE, changeImage);
        }
        if (cdcChangeScopeSet != null && cdcChangeScopeSet.contains(PTable.CDCChangeScope.PRE)) {
            row5.put(CDC_PRE_IMAGE, new HashMap<String, Double>() {{
            }});
        }
        assertEquals(row5, gson.fromJson(rs.getString(3),
                HashMap.class));

        assertEquals(true, rs.next());
        assertEquals(1, rs.getInt(2));
        Map<String, Object> row6 = new HashMap<String, Object>(){{
            put(CDC_EVENT_TYPE, CDC_DELETE_EVENT_TYPE);
        }};
        if (cdcChangeScopeSet != null && cdcChangeScopeSet.contains(PTable.CDCChangeScope.POST)) {
            row6.put(CDC_POST_IMAGE, new HashMap<String, Double>() {{
            }});
        }
        if (cdcChangeScopeSet == null || cdcChangeScopeSet.contains(PTable.CDCChangeScope.CHANGE)) {
            row6.put(CDC_CHANGE_IMAGE, new HashMap<String, Double>() {{
            }});
        }
        if (cdcChangeScopeSet != null && cdcChangeScopeSet.contains(PTable.CDCChangeScope.PRE)) {
            Map<String, Double> preImage = new HashMap<>();
            preImage.put("V1", 102d);
            preImage.put("V2", 1002d);
            row6.put(CDC_PRE_IMAGE, preImage);
        }
        assertEquals(row6, gson.fromJson(rs.getString(3),
                HashMap.class));

        assertEquals(true, rs.next());
        assertEquals(2, rs.getInt(2));
        Map<String, Object> row7 = new HashMap<String, Object>(){{
            put(CDC_EVENT_TYPE, CDC_UPSERT_EVENT_TYPE);
        }};
        if (cdcChangeScopeSet != null && cdcChangeScopeSet.contains(PTable.CDCChangeScope.POST)) {
            Map<String, Double> postImage = new HashMap<>();
            postImage.put("V1", 201d);
            postImage.put("V2", null);
            row7.put(CDC_POST_IMAGE, postImage);
        }
        if (cdcChangeScopeSet == null || cdcChangeScopeSet.contains(PTable.CDCChangeScope.CHANGE)) {
            Map<String, Double> changeImage = new HashMap<>();
            changeImage.put("V1", 201d);
            changeImage.put("V2", null);
            row7.put(CDC_CHANGE_IMAGE, changeImage);
        }
        if (cdcChangeScopeSet != null && cdcChangeScopeSet.contains(PTable.CDCChangeScope.PRE)) {
            Map<String, Double> preImage = new HashMap<>();
            preImage.put("V1", 200d);
            preImage.put("V2", 2000d);
            row7.put(CDC_PRE_IMAGE, preImage);
        }
        assertEquals(row7, gson.fromJson(rs.getString(3),
                HashMap.class));

        assertEquals(true, rs.next());
        assertEquals(1, rs.getInt(2));
        Map<String, Object> row8 = new HashMap<String, Object>(){{
            put(CDC_EVENT_TYPE, CDC_UPSERT_EVENT_TYPE);
        }};
        if (cdcChangeScopeSet != null && cdcChangeScopeSet.contains(PTable.CDCChangeScope.POST)) {
            Map<String, Double> postImage = new HashMap<>();
            postImage.put("V1", 103d);
            postImage.put("V2", 1003d);
            row8.put(CDC_POST_IMAGE, postImage);
        }
        if (cdcChangeScopeSet == null || cdcChangeScopeSet.contains(PTable.CDCChangeScope.CHANGE)) {
            Map<String, Double> changeImage = new HashMap<>();
            changeImage.put("V1", 103d);
            changeImage.put("V2", 1003d);
            row8.put(CDC_CHANGE_IMAGE, changeImage);
        }
        if (cdcChangeScopeSet != null && cdcChangeScopeSet.contains(PTable.CDCChangeScope.PRE)) {
            Map<String, Double> preImage = new HashMap<>();
            row8.put(CDC_PRE_IMAGE, preImage);
        }
        assertEquals(row8, gson.fromJson(rs.getString(3),
                HashMap.class));

        assertEquals(true, rs.next());
        assertEquals(1, rs.getInt(2));
        Map<String, Object> row9 = new HashMap<String, Object>(){{
            put(CDC_EVENT_TYPE, CDC_DELETE_EVENT_TYPE);
        }};
        if (cdcChangeScopeSet != null && cdcChangeScopeSet.contains(PTable.CDCChangeScope.POST)) {
            row9.put(CDC_POST_IMAGE, new HashMap<String, Double>() {{
            }});
        }
        if (cdcChangeScopeSet == null || cdcChangeScopeSet.contains(PTable.CDCChangeScope.CHANGE)) {
            row9.put(CDC_CHANGE_IMAGE, new HashMap<String, Double>() {{
            }});
        }
        if (cdcChangeScopeSet != null && cdcChangeScopeSet.contains(PTable.CDCChangeScope.PRE)) {
            Map<String, Double> preImage = new HashMap<>();
            preImage.put("V1", 103d);
            preImage.put("V2", 1003d);
            row9.put(CDC_PRE_IMAGE, preImage);
        }
        assertEquals(row9, gson.fromJson(rs.getString(3),
                HashMap.class));

        assertEquals(true, rs.next());
        assertEquals(1, rs.getInt(2));
        Map<String, Object> row10 = new HashMap<String, Object>(){{
            put(CDC_EVENT_TYPE, CDC_UPSERT_EVENT_TYPE);
        }};
        if (cdcChangeScopeSet != null && cdcChangeScopeSet.contains(PTable.CDCChangeScope.POST)) {
            Map<String, Double> postImage = new HashMap<>();
            postImage.put("V1", 104d);
            postImage.put("V2", 1004d);
            row10.put(CDC_POST_IMAGE, postImage);
        }
        if (cdcChangeScopeSet == null || cdcChangeScopeSet.contains(PTable.CDCChangeScope.CHANGE)) {
            Map<String, Double> changeImage = new HashMap<>();
            changeImage.put("V1", 104d);
            changeImage.put("V2", 1004d);
            row10.put(CDC_CHANGE_IMAGE, changeImage);
        }
        if (cdcChangeScopeSet != null && cdcChangeScopeSet.contains(PTable.CDCChangeScope.PRE)) {
            Map<String, Double> preImage = new HashMap<>();
            row10.put(CDC_PRE_IMAGE, preImage);
        }
        assertEquals(row10, gson.fromJson(rs.getString(3),
                HashMap.class));

        assertEquals(true, rs.next());
        assertEquals(1, rs.getInt(2));
        Map<String, Object> row11 = new HashMap<String, Object>(){{
            put(CDC_EVENT_TYPE, CDC_DELETE_EVENT_TYPE);
        }};
        if (cdcChangeScopeSet != null && cdcChangeScopeSet.contains(PTable.CDCChangeScope.POST)) {
            row11.put(CDC_POST_IMAGE, new HashMap<String, Double>() {{
            }});
        }
        if (cdcChangeScopeSet == null || cdcChangeScopeSet.contains(PTable.CDCChangeScope.CHANGE)) {
            row11.put(CDC_CHANGE_IMAGE, new HashMap<String, Double>() {{
            }});
        }
        if (cdcChangeScopeSet != null && cdcChangeScopeSet.contains(PTable.CDCChangeScope.PRE)) {
            Map<String, Double> preImage = new HashMap<>();
            preImage.put("V1", 104d);
            preImage.put("V2", 1004d);
            row11.put(CDC_PRE_IMAGE, preImage);
        }
        assertEquals(row11, gson.fromJson(rs.getString(3),
                HashMap.class));

        assertEquals(false, rs.next());
        rs.close();
    }

    private Connection newConnection() throws SQLException {
        Properties props = new Properties();
        // Use these only for debugging.
        //props.put(QueryServices.TASK_HANDLING_INTERVAL_MS_ATTRIB, Long.toString(Long.MAX_VALUE));
        //props.put("hbase.client.scanner.timeout.period", "6000000");
        //props.put("phoenix.query.timeoutMs", "6000000");
        //props.put("zookeeper.session.timeout", "6000000");
        //props.put("hbase.rpc.timeout", "6000000");
        return DriverManager.getConnection(getUrl(), props);
    }

    @Test
    public void testSelectCDC() throws Exception {
        Connection conn = newConnection();
        String tableName = generateUniqueName();
        conn.createStatement().execute(
                "CREATE TABLE  " + tableName + " ( k INTEGER PRIMARY KEY," + " v1 INTEGER, v2 INTEGER)");
        String cdcName = generateUniqueName();
        String cdc_sql = "CREATE CDC " + cdcName
                + " ON " + tableName;
        createAndWait(conn, tableName, cdcName, cdc_sql);
        assertCDCState(conn, cdcName, null, 3);
        EnvironmentEdgeManager.injectEdge(injectEdge);
        injectEdge.setValue(System.currentTimeMillis());
        conn.createStatement().execute("UPSERT INTO " + tableName + " (k, v1, v2) VALUES (1, 100, 1000)");
        injectEdge.incrementValue(100);
        conn.createStatement().execute("UPSERT INTO " + tableName + " (k, v1, v2) VALUES (2, 200, 2000)");
        conn.commit();
        injectEdge.incrementValue(100);
        conn.createStatement().execute("UPSERT INTO " + tableName + " (k, v1) VALUES (1, 101)");
        conn.commit();
        injectEdge.incrementValue(100);
        conn.createStatement().execute("DELETE FROM " + tableName + " WHERE k=1");
        conn.commit();
        injectEdge.incrementValue(100);
        conn.createStatement().execute("UPSERT INTO " + tableName + " (k, v1, v2) VALUES (1, 102, 1002)");
        conn.commit();
        injectEdge.incrementValue(100);
        conn.createStatement().execute("DELETE FROM " + tableName + " WHERE k=1");
        conn.commit();
        injectEdge.incrementValue(100);
        conn.createStatement().execute("UPSERT INTO " + tableName + " (k, v1, v2) VALUES (2, 201, NULL)");
        conn.commit();
        injectEdge.incrementValue(100);
        conn.createStatement().execute("UPSERT INTO " + tableName + " (k, v1, v2) VALUES (1, 103, 1003)");
        conn.commit();
        injectEdge.incrementValue(100);
        conn.createStatement().execute("DELETE FROM " + tableName + " WHERE k=1");
        conn.commit();
        injectEdge.incrementValue(100);
        conn.createStatement().execute("UPSERT INTO " + tableName + " (k, v1, v2) VALUES (1, 104, 1004)");
        conn.commit();
        injectEdge.incrementValue(100);
        conn.createStatement().execute("DELETE FROM " + tableName + " WHERE k=1");
        conn.commit();
        // NOTE: To debug the query execution, add the below condition where you need a breakpoint.
        //      if (<table>.getTableName().getString().equals("N000002") ||
        //                 <table>.getTableName().getString().equals("__CDC__N000002")) {
        //          "".isEmpty();
        //      }

        assertResultSet(conn.createStatement().executeQuery("SELECT * FROM " + cdcName), null);
        assertResultSet(conn.createStatement().executeQuery("SELECT * FROM " + cdcName +
                " WHERE \"PHOENIX_ROW_TIMESTAMP()\" < NOW()"), null);
        assertResultSet(conn.createStatement().executeQuery("SELECT /*+ CDC_INCLUDE(PRE, POST) */ * " +
                "FROM " + cdcName), new HashSet<PTable.CDCChangeScope>(
                        Arrays.asList(PTable.CDCChangeScope.PRE, PTable.CDCChangeScope.POST)));
        assertResultSet(conn.createStatement().executeQuery("SELECT " +
                "PHOENIX_ROW_TIMESTAMP(), K, \"CDC JSON\" FROM " + cdcName), null);

        HashMap<String, int[]> testQueries = new HashMap<String, int[]>() {{
            put("SELECT 'dummy', k FROM " + cdcName, new int [] {1, 2, 1, 1, 1, 1, 2, 1, 1, 1, 1});
            put("SELECT * FROM " + cdcName +
                    " ORDER BY k ASC", new int [] {1, 1, 1, 1, 1, 1, 1, 1, 1, 2, 2});
            put("SELECT * FROM " + cdcName +
                    " ORDER BY k DESC", new int [] {2, 2, 1, 1, 1, 1, 1, 1, 1, 1, 1});
            put("SELECT * FROM " + cdcName +
                    " ORDER BY PHOENIX_ROW_TIMESTAMP() ASC", new int [] {1, 2, 1, 1, 1, 1, 2, 1, 1, 1, 1});
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

    @Test
    public void testSelectCDCRebuildIndex() throws Exception {
        Properties props = new Properties();
        props.put(QueryServices.TASK_HANDLING_INTERVAL_MS_ATTRIB, Long.toString(Long.MAX_VALUE));
        props.put("hbase.client.scanner.timeout.period", "6000000");
        props.put("phoenix.query.timeoutMs", "6000000");
        props.put("zookeeper.session.timeout", "6000000");
        props.put("hbase.rpc.timeout", "6000000");
        Connection conn = DriverManager.getConnection(getUrl(), props);
        String tableName = generateUniqueName();
        conn.createStatement().execute(
                "CREATE TABLE  " + tableName + " ( k INTEGER PRIMARY KEY," + " v1 INTEGER, v2 INTEGER, v3 INTEGER, v4 INTEGER)");
        String cdcName = generateUniqueName();

        conn.createStatement().execute("UPSERT INTO " + tableName + " (k, v1, v2) VALUES (1, 100, 1000)");
        conn.createStatement().execute("UPSERT INTO " + tableName + " (k, v1, v2) VALUES (2, 200, 2000)");
        conn.commit();
        Thread.sleep(10);
        conn.createStatement().execute("UPSERT INTO " + tableName + " (k, v1) VALUES (1, 101)");
        conn.commit();

        conn.createStatement().execute("ALTER TABLE " + tableName + " DROP COLUMN v4");
        conn.createStatement().execute("DELETE FROM " + tableName + " WHERE k=1");
        conn.commit();

        conn.createStatement().execute("UPSERT INTO " + tableName + " (k, v1, v2) VALUES (1, 102, 1002)");
        conn.commit();
        conn.createStatement().execute("DELETE FROM " + tableName + " WHERE k=1");
        conn.commit();
        conn.createStatement().execute("UPSERT INTO " + tableName + " (k, v1, v2) VALUES (2, 201, NULL)");
        conn.commit();
        Thread.sleep(10);
        conn.createStatement().execute("UPSERT INTO " + tableName + " (k, v1, v2) VALUES (1, 103, 1003)");
        conn.commit();
        Thread.sleep(10);
        conn.createStatement().execute("DELETE FROM " + tableName + " WHERE k=1");
        conn.commit();
        Thread.sleep(10);
        conn.createStatement().execute("UPSERT INTO " + tableName + " (k, v1, v2) VALUES (1, 104, 1004)");
        conn.commit();
        Thread.sleep(10);
        conn.createStatement().execute("DELETE FROM " + tableName + " WHERE k=1");
        conn.commit();

        String cdc_sql = "CREATE CDC " + cdcName
                + " ON " + tableName;
        createAndWait(conn, tableName, cdcName, cdc_sql);
        assertCDCState(conn, cdcName, null, 3);
        assertResultSet(conn.createStatement().executeQuery(
                "SELECT /*+ CDC_INCLUDE(PRE, POST, CHANGE) */ * " + "FROM " + cdcName),
                new HashSet<>(Arrays.asList(PTable.CDCChangeScope.PRE,
                        PTable.CDCChangeScope.POST, PTable.CDCChangeScope.CHANGE)));
    }

    @Test
    public void testSelectCDCWithDDL() throws Exception {
        Properties props = new Properties();
        props.put(QueryServices.TASK_HANDLING_INTERVAL_MS_ATTRIB, Long.toString(Long.MAX_VALUE));
        props.put("hbase.client.scanner.timeout.period", "6000000");
        props.put("phoenix.query.timeoutMs", "6000000");
        props.put("zookeeper.session.timeout", "6000000");
        props.put("hbase.rpc.timeout", "6000000");
        Connection conn = DriverManager.getConnection(getUrl(), props);
        String tableName = generateUniqueName();
        conn.createStatement().execute(
                "CREATE TABLE  " + tableName + " ( k INTEGER PRIMARY KEY," + " v0 INTEGER, v1 INTEGER, v1v2 INTEGER, v2 INTEGER, v3 INTEGER)");
        String cdcName = generateUniqueName();
        String cdc_sql = "CREATE CDC " + cdcName
                + " ON " + tableName;
        createAndWait(conn, tableName, cdcName, cdc_sql);
        assertCDCState(conn, cdcName, null, 3);

        conn.createStatement().execute("ALTER TABLE " + tableName + " DROP COLUMN v0");
        conn.commit();
        conn.createStatement().execute("UPSERT INTO " + tableName + " (k, v1, v2) VALUES (1, 100, 1000)");
        conn.createStatement().execute("UPSERT INTO " + tableName + " (k, v1, v2) VALUES (2, 200, 2000)");
        conn.commit();
        Thread.sleep(10);
        conn.createStatement().execute("UPSERT INTO " + tableName + " (k, v1) VALUES (1, 101)");
        conn.commit();
        conn.createStatement().execute("ALTER TABLE " + tableName + " DROP COLUMN v3");
        conn.commit();
        conn.createStatement().execute("DELETE FROM " + tableName + " WHERE k=1");
        conn.commit();
        conn.createStatement().execute("UPSERT INTO " + tableName + " (k, v1, v2) VALUES (1, 102, 1002)");
        conn.commit();
        conn.createStatement().execute("DELETE FROM " + tableName + " WHERE k=1");
        conn.commit();
        conn.createStatement().execute("UPSERT INTO " + tableName + " (k, v1, v2) VALUES (2, 201, NULL)");
        conn.commit();
        Thread.sleep(10);
        conn.createStatement().execute("UPSERT INTO " + tableName + " (k, v1, v2) VALUES (1, 103, 1003)");
        conn.commit();
        Thread.sleep(10);
        conn.createStatement().execute("DELETE FROM " + tableName + " WHERE k=1");
        conn.commit();
        Thread.sleep(10);
        conn.createStatement().execute("UPSERT INTO " + tableName + " (k, v1, v2) VALUES (1, 104, 1004)");
        conn.commit();
        Thread.sleep(10);
        conn.createStatement().execute("DELETE FROM " + tableName + " WHERE k=1");
        conn.commit();
        conn.createStatement().execute("ALTER TABLE " + tableName + " DROP COLUMN v1v2");
        conn.commit();
        conn.createStatement().execute("ALTER TABLE " + tableName + " ADD v4 INTEGER");
        conn.commit();

        assertResultSet(conn.createStatement().executeQuery("SELECT * FROM " + cdcName), null);
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

    private void assertCDCBinaryAndDateColumn(ResultSet rs,
                                              List<byte []> byteColumnValues,
                                              List<Date> dateColumnValues,
                                              Timestamp timestamp) throws Exception {
        assertEquals(true, rs.next());
        assertEquals(1, rs.getInt(2));

        Gson gson = new Gson();
        Map<String, Object> row1 = new HashMap<String, Object>(){{
            put(CDC_EVENT_TYPE, CDC_UPSERT_EVENT_TYPE);
        }};
        Map<String, Object> postImage = new HashMap<>();
        postImage.put("A_BINARY",
                Base64.getEncoder().encodeToString(byteColumnValues.get(0)));
        postImage.put("D", dateColumnValues.get(0).toString());
        postImage.put("T", timestamp.toString());
        row1.put(CDC_POST_IMAGE, postImage);
        Map<String, Object> changeImage = new HashMap<>();
        changeImage.put("A_BINARY",
                Base64.getEncoder().encodeToString(byteColumnValues.get(0)));
        changeImage.put("D", dateColumnValues.get(0).toString());
        changeImage.put("T", timestamp.toString());
        row1.put(CDC_CHANGE_IMAGE, changeImage);
        row1.put(CDC_PRE_IMAGE, new HashMap<String, String>() {{
        }});
        assertEquals(row1, gson.fromJson(rs.getString(3),
                HashMap.class));

        assertEquals(true, rs.next());
        assertEquals(2, rs.getInt(2));
        HashMap<String, Object> row2Json = gson.fromJson(rs.getString(3), HashMap.class);
        String row2BinaryColStr = (String) ((Map)((Map)row2Json.get(CDC_CHANGE_IMAGE))).get("A_BINARY");
        byte[] row2BinaryCol = Base64.getDecoder().decode(row2BinaryColStr);

        assertEquals(0, DescVarLengthFastByteComparisons.compareTo(byteColumnValues.get(1),
                0, byteColumnValues.get(1).length, row2BinaryCol, 0, row2BinaryCol.length));
    }

    @Test
    public void testCDCBinaryAndDateColumn() throws Exception {
        Properties props = new Properties();
        props.put(QueryServices.TASK_HANDLING_INTERVAL_MS_ATTRIB, Long.toString(Long.MAX_VALUE));
        props.put("hbase.client.scanner.timeout.period", "6000000");
        props.put("phoenix.query.timeoutMs", "6000000");
        props.put("zookeeper.session.timeout", "6000000");
        props.put("hbase.rpc.timeout", "6000000");
        Connection conn = DriverManager.getConnection(getUrl(), props);
        String tableName = generateUniqueName();
        List<byte []> byteColumnValues = new ArrayList<>();
        byteColumnValues.add( new byte[] {0,0,0,0,0,0,0,0,0,1});
        byteColumnValues.add(new byte[] {0,0,0,0,0,0,0,0,0,2});
        List<Date> dateColumnValues = new ArrayList<>();
        dateColumnValues.add(Date.valueOf("2024-02-01"));
        dateColumnValues.add(Date.valueOf("2024-01-31"));
        Timestamp timestampColumnValue = Timestamp.valueOf("2024-01-31 12:12:14");
        try {

            conn.createStatement().execute("CREATE TABLE  " + tableName +
                    " ( k INTEGER PRIMARY KEY," + " a_binary binary(10), d Date, t TIMESTAMP)");

            String upsertQuery = "UPSERT INTO " + tableName + " (k, a_binary, d, t) VALUES (?, ?, ?, ?)";
            PreparedStatement stmt = conn.prepareStatement(upsertQuery);
            stmt.setInt(1, 1);
            stmt.setBytes(2, byteColumnValues.get(0));
            stmt.setDate(3, dateColumnValues.get(0));
            stmt.setTimestamp(4, timestampColumnValue);
            stmt.execute();
            stmt.setInt(1, 2);
            stmt.setBytes(2, byteColumnValues.get(1));
            stmt.setDate(3, dateColumnValues.get(1));
            stmt.setTimestamp(4, timestampColumnValue);
            stmt.execute();
            conn.commit();

            String cdcName = generateUniqueName();
            String cdc_sql = "CREATE CDC " + cdcName
                    + " ON " + tableName;
            createAndWait(conn, tableName, cdcName, cdc_sql);
            assertCDCState(conn, cdcName, null, 3);
            assertCDCBinaryAndDateColumn(conn.createStatement().executeQuery
                    ("SELECT /*+ CDC_INCLUDE(PRE, POST, CHANGE) */ * " + "FROM " + cdcName),
                    byteColumnValues, dateColumnValues, timestampColumnValue);
        } finally {
            conn.close();
        }
    }
}
