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
import org.apache.phoenix.execute.DescVarLengthFastByteComparisons;
import org.apache.phoenix.hbase.index.IndexRegionObserver;
import org.apache.phoenix.schema.PTable;
import org.apache.phoenix.util.EnvironmentEdgeManager;
import org.apache.phoenix.util.ManualEnvironmentEdge;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.sql.Connection;
import java.sql.Date;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Base64;
import java.util.Calendar;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.apache.phoenix.query.QueryConstants.CDC_CHANGE_IMAGE;
import static org.apache.phoenix.query.QueryConstants.CDC_DELETE_EVENT_TYPE;
import static org.apache.phoenix.query.QueryConstants.CDC_EVENT_TYPE;
import static org.apache.phoenix.query.QueryConstants.CDC_POST_IMAGE;
import static org.apache.phoenix.query.QueryConstants.CDC_PRE_IMAGE;
import static org.apache.phoenix.query.QueryConstants.CDC_UPSERT_EVENT_TYPE;
import static org.apache.phoenix.schema.PTable.QualifierEncodingScheme.NON_ENCODED_QUALIFIERS;
import static org.apache.phoenix.schema.PTable.QualifierEncodingScheme.TWO_BYTE_QUALIFIERS;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

// NOTE: To debug the query execution, add the below condition or the equivalent where you need a
// breakpoint.
//      if (<table>.getTableName().getString().equals("N000002") ||
//                 <table>.getTableName().getString().equals("__CDC__N000002")) {
//          "".isEmpty();
//      }
@RunWith(Parameterized.class)
@Category(ParallelStatsDisabledTest.class)
public class CDCQueryIT extends CDCBaseIT {
    // Offset of the first column, depending on whether PHOENIX_ROW_TIMESTAMP() is in the schema
    // or not.
    private static final int COL_OFFSET = 1;
    private final boolean forView;
    private final boolean dataFirst;
    private final PTable.QualifierEncodingScheme encodingScheme;
    private final boolean multitenant;
    private final int nSaltBuckets;
    private ManualEnvironmentEdge injectEdge;

    public CDCQueryIT(Boolean forView, Boolean dataFirst,
                      PTable.QualifierEncodingScheme encodingScheme, boolean multitenant,
                      Integer nSaltBuckets) {
        this.forView = forView;
        this.dataFirst = dataFirst;
        this.encodingScheme = encodingScheme;
        this.multitenant = multitenant;
        this.nSaltBuckets = nSaltBuckets;
    }

    @Parameterized.Parameters(name = "forView={0} dataFirst={1}, encodingScheme={2}, " +
            "multitenant={3}, nSaltBuckets={4}")
    public static synchronized Collection<Object[]> data() {
        return Arrays.asList(new Object[][] {
                { Boolean.FALSE, Boolean.FALSE, TWO_BYTE_QUALIFIERS, Boolean.FALSE, 0 },
                { Boolean.FALSE, Boolean.TRUE, TWO_BYTE_QUALIFIERS, Boolean.FALSE, 0 },
                { Boolean.FALSE, Boolean.FALSE, NON_ENCODED_QUALIFIERS, Boolean.TRUE, 0 },
                { Boolean.FALSE, Boolean.FALSE, NON_ENCODED_QUALIFIERS, Boolean.FALSE, 4 },
                { Boolean.TRUE, Boolean.FALSE, TWO_BYTE_QUALIFIERS, Boolean.FALSE, 0 },
        });
    }

    @Before
    public void beforeTest(){
        EnvironmentEdgeManager.reset();
        injectEdge = new ManualEnvironmentEdge();
        injectEdge.setValue(EnvironmentEdgeManager.currentTimeMillis());
    }

    private void assertResultSet(ResultSet rs, Set<PTable.CDCChangeScope> cdcChangeScopeSet)
            throws Exception{
        Gson gson = new Gson();
        assertEquals(true, rs.next());
        assertEquals(1, rs.getInt(1 + COL_OFFSET));
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
        assertEquals(row1, gson.fromJson(rs.getString(2 + COL_OFFSET),
                HashMap.class));

        assertEquals(true, rs.next());
        assertEquals(2, rs.getInt(1 + COL_OFFSET));
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
        assertEquals(row2, gson.fromJson(rs.getString(2 + COL_OFFSET),
                HashMap.class));
        assertEquals(true, rs.next());
        assertEquals(1, rs.getInt(1 + COL_OFFSET));
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
        assertEquals(row3, gson.fromJson(rs.getString(2 + COL_OFFSET),
                HashMap.class));

        assertEquals(true, rs.next());
        assertEquals(1, rs.getInt(1 + COL_OFFSET));
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
        assertEquals(row4, gson.fromJson(rs.getString(2 + COL_OFFSET),
                HashMap.class));

        assertEquals(true, rs.next());
        assertEquals(1, rs.getInt(1 + COL_OFFSET));
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
        assertEquals(row5, gson.fromJson(rs.getString(2 + COL_OFFSET),
                HashMap.class));

        assertEquals(true, rs.next());
        assertEquals(1, rs.getInt(1 + COL_OFFSET));
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
        assertEquals(row6, gson.fromJson(rs.getString(2 + COL_OFFSET),
                HashMap.class));

        assertEquals(true, rs.next());
        assertEquals(2, rs.getInt(1 + COL_OFFSET));
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
        assertEquals(row7, gson.fromJson(rs.getString(2 + COL_OFFSET),
                HashMap.class));

        assertEquals(true, rs.next());
        assertEquals(1, rs.getInt(1 + COL_OFFSET));
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
        assertEquals(row8, gson.fromJson(rs.getString(2 + COL_OFFSET),
                HashMap.class));

        assertEquals(true, rs.next());
        assertEquals(1, rs.getInt(1 + COL_OFFSET));
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
        assertEquals(row9, gson.fromJson(rs.getString(2 + COL_OFFSET),
                HashMap.class));

        assertEquals(true, rs.next());
        assertEquals(1, rs.getInt(1 + COL_OFFSET));
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
        assertEquals(row10, gson.fromJson(rs.getString(2 + COL_OFFSET),
                HashMap.class));

        assertEquals(true, rs.next());
        assertEquals(1, rs.getInt(1 + COL_OFFSET));
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
        assertEquals(row11, gson.fromJson(rs.getString(2 + COL_OFFSET),
                HashMap.class));

        assertEquals(false, rs.next());
        rs.close();
    }

    @Test
    public void testSelectCDC() throws Exception {
        String cdcName, cdc_sql;
        String tableName = generateUniqueName();
        try (Connection conn = newConnection()) {
            createTable(conn, "CREATE TABLE  " + tableName + " (" +
                    (multitenant ? "TENANT_ID CHAR(5) NOT NULL, " : "") +
                    "k INTEGER NOT NULL, v1 INTEGER, v2 INTEGER, CONSTRAINT PK PRIMARY KEY " +
                    (multitenant ? "(TENANT_ID, k) " : "(k)") + ")", encodingScheme, multitenant);
            if (forView) {
                String viewName = generateUniqueName();
                createTable(conn, "CREATE VIEW " + viewName + " AS SELECT * FROM " + tableName,
                        encodingScheme, false);
                tableName = viewName;
            }
            cdcName = generateUniqueName();
            cdc_sql = "CREATE CDC " + cdcName + " ON " + tableName;
            if (!dataFirst) {
                createCDCAndWait(conn, tableName, cdcName, cdc_sql, encodingScheme, nSaltBuckets);
            }
        }

        String tenantId = "1000";
        String[] tenantids = {tenantId};
        if (multitenant) {
            tenantids = new String[] {tenantId, "2000"};
        }
        for (String tid: tenantids) {
            try (Connection conn = newConnection(tid)) {
                EnvironmentEdgeManager.injectEdge(injectEdge);
                injectEdge.setValue(System.currentTimeMillis());
                conn.createStatement().execute("UPSERT INTO " + tableName +
                        " (k, v1, v2) VALUES (1, 100, 1000)");
                injectEdge.incrementValue(100);
                conn.createStatement().execute("UPSERT INTO " + tableName +
                        " (k, v1, v2) VALUES (2, 200, 2000)");
                conn.commit();
                injectEdge.incrementValue(100);
                conn.createStatement().execute("UPSERT INTO " + tableName +
                        " (k, v1) VALUES (1, 101)");
                conn.commit();
                injectEdge.incrementValue(100);
                conn.createStatement().execute("DELETE FROM " + tableName +
                        " WHERE k=1");
                conn.commit();
                injectEdge.incrementValue(100);
                conn.createStatement().execute("UPSERT INTO " + tableName +
                        " (k, v1, v2) VALUES (1, 102, 1002)");
                conn.commit();
                injectEdge.incrementValue(100);
                conn.createStatement().execute("DELETE FROM " + tableName +
                        " WHERE k=1");
                conn.commit();
                injectEdge.incrementValue(100);
                conn.createStatement().execute("UPSERT INTO " + tableName +
                        " (k, v1, v2) VALUES (2, 201, NULL)");
                conn.commit();
                injectEdge.incrementValue(100);
                conn.createStatement().execute("UPSERT INTO " + tableName +
                        " (k, v1, v2) VALUES (1, 103, 1003)");
                conn.commit();
                injectEdge.incrementValue(100);
                conn.createStatement().execute("DELETE FROM " + tableName + " WHERE k=1");
                conn.commit();
                injectEdge.incrementValue(100);
                conn.createStatement().execute("UPSERT INTO " + tableName +
                        " (k, v1, v2) VALUES (1, 104, 1004)");
                conn.commit();
                injectEdge.incrementValue(100);
                conn.createStatement().execute("DELETE FROM " + tableName + " WHERE k=1");
                conn.commit();
                EnvironmentEdgeManager.reset();
            }
        }
        if (dataFirst) {
            try (Connection conn = newConnection()) {
                createCDCAndWait(conn, tableName, cdcName, cdc_sql, encodingScheme, nSaltBuckets);
            }
        }

        try (Connection conn = newConnection(null)) {
            ResultSet rs = conn.createStatement().executeQuery("SELECT * FROM " + tableName);
            // TODO: Check RS, existence of CDC shouldn't cause the regular query path to fail.
        }

        try (Connection conn = newConnection(tenantId)) {
            assertResultSet(conn.createStatement().executeQuery("SELECT * FROM " + cdcName), null);
            assertResultSet(conn.createStatement().executeQuery("SELECT /*+ CDC_INCLUDE(PRE, POST) */ * " +
                    "FROM " + cdcName), new HashSet<PTable.CDCChangeScope>(
                    Arrays.asList(PTable.CDCChangeScope.PRE, PTable.CDCChangeScope.POST)));
            assertResultSet(conn.createStatement().executeQuery("SELECT " +
                    "PHOENIX_ROW_TIMESTAMP(), K, \"CDC JSON\" FROM " + cdcName), null);

            HashMap<String, int[]> testQueries = new HashMap<String, int[]>() {{
                put("SELECT 'dummy', k FROM " + cdcName, new int[]{1, 2, 1, 1, 1, 1, 2, 1, 1, 1,
                        1});
                put("SELECT PHOENIX_ROW_TIMESTAMP(), k FROM " + cdcName +
                        " ORDER BY k ASC", new int[]{1, 1, 1, 1, 1, 1, 1, 1, 1, 2, 2});
                put("SELECT PHOENIX_ROW_TIMESTAMP(), k FROM " + cdcName +
                        " ORDER BY k DESC", new int[]{2, 2, 1, 1, 1, 1, 1, 1, 1, 1, 1});
                //put("SELECT PHOENIX_ROW_TIMESTAMP(), k FROM " + cdcName +
                //        " ORDER BY PHOENIX_ROW_TIMESTAMP() DESC", new int[]{1, 1, 1, 1, 2, 1, 1, 1,
                //        2, 1});
            }};
            for (Map.Entry<String, int[]> testQuery : testQueries.entrySet()) {
                try (ResultSet rs = conn.createStatement().executeQuery(testQuery.getKey())) {
                    for (int i = 0; i < testQuery.getValue().length; ++i) {
                        int k = testQuery.getValue()[i];
                        assertEquals(true, rs.next());
                        assertEquals("Index: " + i + " for query: " + testQuery.getKey(),
                                k, rs.getInt(2));
                    }
                    assertEquals(false, rs.next());
                }
            }
        }
    }

    @Test
    public void testSelectTimeRangeQueries() throws Exception {
        String cdcName, cdc_sql;
        String tableName = generateUniqueName();
        try (Connection conn = newConnection()) {
            createTable(conn, "CREATE TABLE  " + tableName + " (" +
                    (multitenant ? "TENANT_ID CHAR(5) NOT NULL, " : "") +
                    "k INTEGER NOT NULL, v1 INTEGER, CONSTRAINT PK PRIMARY KEY " +
                    (multitenant ? "(TENANT_ID, k) " : "(k)") + ")", encodingScheme, multitenant);
            if (forView) {
                String viewName = generateUniqueName();
                createTable(conn, "CREATE VIEW " + viewName + " AS SELECT * FROM " + tableName,
                        encodingScheme, false);
                tableName = viewName;
            }
            cdcName = generateUniqueName();
            cdc_sql = "CREATE CDC " + cdcName + " ON " + tableName;
            if (!dataFirst) {
                createCDCAndWait(conn, tableName, cdcName, cdc_sql, encodingScheme, nSaltBuckets);
            }
        }

        EnvironmentEdgeManager.injectEdge(injectEdge);

        String tenantId = multitenant ? "1000" : null;
        String[] tenantids = {tenantId};
        if (multitenant) {
            tenantids = new String[] {tenantId, "2000"};
        }

        Timestamp ts1 = new Timestamp(System.currentTimeMillis());
        Calendar cal = Calendar.getInstance();
        cal.setTimeInMillis(ts1.getTime());
        injectEdge.setValue(ts1.getTime());

        for (String tid: tenantids) {
            try (Connection conn = newConnection(tid)) {
                conn.createStatement().execute("UPSERT INTO " + tableName + " (k, v1) VALUES (1, 100)");
                conn.commit();
            }
        }

        injectEdge.incrementValue(100);

        for (String tid: tenantids) {
            try (Connection conn = newConnection(tid)) {
                conn.createStatement().execute("UPSERT INTO " + tableName + " (k, v1) VALUES (2, 200)");
                conn.commit();
            }
        }

        injectEdge.incrementValue(100);
        cal.add(Calendar.MILLISECOND, 200);
        Timestamp ts2 = new Timestamp(cal.getTime().getTime());
        injectEdge.incrementValue(100);

        for (String tid: tenantids) {
            try (Connection conn = newConnection(tid)) {
                conn.createStatement().execute("UPSERT INTO " + tableName + " (k, v1) VALUES (1, 101)");
                conn.commit();
                injectEdge.incrementValue(100);
                conn.createStatement().execute("UPSERT INTO " + tableName + " (k, v1) VALUES (3, 300)");
                conn.commit();
            }
        }

        injectEdge.incrementValue(100);
        cal.add(Calendar.MILLISECOND, 300);
        Timestamp ts3 = new Timestamp(cal.getTime().getTime());
        //dumpTable(CDCUtil.getCDCIndexName(cdcName));
        injectEdge.incrementValue(100);

        for (String tid: tenantids) {
            try (Connection conn = newConnection(tid)) {
                conn.createStatement().execute("UPSERT INTO " + tableName + " (k, v1) VALUES (1, 101)");
                conn.commit();
                injectEdge.incrementValue(100);
                conn.createStatement().execute("DELETE FROM " + tableName + " WHERE k = 2");
                conn.commit();
            }
        }

        injectEdge.incrementValue(100);
        cal.add(Calendar.MILLISECOND, 300);
        Timestamp ts4 = new Timestamp(cal.getTime().getTime());
        EnvironmentEdgeManager.reset();

        if (dataFirst) {
            try (Connection conn = newConnection()) {
                createCDCAndWait(conn, tableName, cdcName, cdc_sql, encodingScheme, nSaltBuckets);
            }
        }

        try (Connection conn = newConnection(tenantId)) {
            String sel_sql =
                    "SELECT to_char(phoenix_row_timestamp()), k, \"CDC JSON\" FROM " + cdcName +
                            " WHERE PHOENIX_ROW_TIMESTAMP() >= ? AND PHOENIX_ROW_TIMESTAMP() <= ?";
            Object[] testDataSets = new Object[] {
                    new Object[] {ts1, ts2, new int[] {1, 2}},
                    new Object[] {ts2, ts3, new int[] {1, 3}},
                    new Object[] {ts3, ts4, new int[] {1, 2}},
                    new Object[] {ts1, ts4, new int[] {1, 2, 1, 3, 1, 2}},
            };
            PreparedStatement stmt = conn.prepareStatement(sel_sql);
            // For debug: uncomment to see the exact results logged to console.
            //for (int i = 0; i < testDataSets.length; ++i) {
            //    Object[] testData = (Object[]) testDataSets[i];
            //    stmt.setTimestamp(1, (Timestamp) testData[0]);
            //    stmt.setTimestamp(2, (Timestamp) testData[1]);
            //    try (ResultSet rs = stmt.executeQuery()) {
            //        System.out.println("----- Test data set: " + i);
            //        while (rs.next()) {
            //            System.out.println("----- " + rs.getString(1) + " " + rs.getInt(2) + " "  + rs.getString(3));
            //        }
            //    }
            //}
            for (int i = 0; i < testDataSets.length; ++i) {
                Object[] testData = (Object[]) testDataSets[i];
                stmt.setTimestamp(1, (Timestamp) testData[0]);
                stmt.setTimestamp(2, (Timestamp) testData[1]);
                try (ResultSet rs = stmt.executeQuery()) {
                    for (int j = 0; j < ((int[]) testData[2]).length; ++j) {
                        int k = ((int[]) testData[2])[j];
                        assertEquals(" Index: " + j + " Test data set: " + i,
                                true, rs.next());
                        assertEquals(" Index: " + j + " Test data set: " + i,
                                k, rs.getInt(1 + COL_OFFSET));
                    }
                    assertEquals("Test data set: " + i, false, rs.next());
                }
            }

            PreparedStatement pstmt = conn.prepareStatement(
                    "SELECT * FROM " + cdcName + " WHERE PHOENIX_ROW_TIMESTAMP() > ?");
            pstmt.setTimestamp(1, ts4);
            try (ResultSet rs = pstmt.executeQuery()) {
                assertEquals(false, rs.next());
            }
        }
    }

    @Test
    public void testSelectCDCWithDDL() throws Exception {
        String tableName = generateUniqueName();
        String datatableName = tableName;
        String cdcName, cdc_sql;
        try (Connection conn = newConnection()) {
            createTable(conn, "CREATE TABLE  " + tableName + " (" +
                    (multitenant ? "TENANT_ID CHAR(5) NOT NULL, " : "") +
                    "k INTEGER NOT NULL, v0 INTEGER, v1 INTEGER, v1v2 INTEGER, v2 INTEGER, " +
                    "v3 INTEGER, CONSTRAINT PK PRIMARY KEY " +
                    (multitenant ? "(TENANT_ID, k) " : "(k)") + ")", encodingScheme, multitenant);
            if (forView) {
                String viewName = generateUniqueName();
                createTable(conn, "CREATE VIEW " + viewName + " AS SELECT * FROM " + tableName,
                        encodingScheme, false);
                tableName = viewName;
            }

            cdcName = generateUniqueName();
            cdc_sql = "CREATE CDC " + cdcName + " ON " + tableName;
            if (!dataFirst) {
                createCDCAndWait(conn, tableName, cdcName, cdc_sql, encodingScheme, nSaltBuckets);
            }
            conn.createStatement().execute("ALTER TABLE " + datatableName + " DROP COLUMN v0");
        }

        String tenantId = multitenant ? "1000" : null;
        try (Connection conn = newConnection(tenantId)) {
            conn.createStatement().execute("UPSERT INTO " + tableName + " (k, v1, v2) VALUES (1, 100, 1000)");
            conn.createStatement().execute("UPSERT INTO " + tableName + " (k, v1, v2) VALUES (2, 200, 2000)");
            conn.commit();
            Thread.sleep(10);
            conn.createStatement().execute("UPSERT INTO " + tableName + " (k, v1) VALUES (1, 101)");
            conn.commit();
        }
        try (Connection conn = newConnection()) {
            conn.createStatement().execute("ALTER TABLE " + datatableName + " DROP COLUMN v3");
        }
        try (Connection conn = newConnection(tenantId)) {
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
        }
        try (Connection conn = newConnection()) {
            conn.createStatement().execute("ALTER TABLE " + datatableName + " DROP COLUMN v1v2");
            conn.createStatement().execute("ALTER TABLE " + datatableName + " ADD v4 INTEGER");
        }

        if (dataFirst) {
            try (Connection conn = newConnection()) {
                createCDCAndWait(conn, tableName, cdcName, cdc_sql, encodingScheme, nSaltBuckets);
            }
        }

        try (Connection conn = newConnection(tenantId)) {
            assertResultSet(conn.createStatement().executeQuery("SELECT * FROM " + cdcName), null);
        }
    }

    private void assertCDCBinaryAndDateColumn(ResultSet rs,
                                              List<byte []> byteColumnValues,
                                              List<Date> dateColumnValues,
                                              Timestamp timestamp) throws Exception {
        assertEquals(true, rs.next());
        assertEquals(1, rs.getInt(1 + COL_OFFSET));

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
        assertEquals(row1, gson.fromJson(rs.getString(2 + COL_OFFSET),
                HashMap.class));

        assertEquals(true, rs.next());
        assertEquals(2, rs.getInt(1  + COL_OFFSET));
        HashMap<String, Object> row2Json = gson.fromJson(rs.getString(2  + COL_OFFSET),
                HashMap.class);
        String row2BinaryColStr = (String) ((Map)((Map)row2Json.get(CDC_CHANGE_IMAGE))).get("A_BINARY");
        byte[] row2BinaryCol = Base64.getDecoder().decode(row2BinaryColStr);

        assertEquals(0, DescVarLengthFastByteComparisons.compareTo(byteColumnValues.get(1),
                0, byteColumnValues.get(1).length, row2BinaryCol, 0, row2BinaryCol.length));
    }

    @Test
    public void testCDCBinaryAndDateColumn() throws Exception {
        String tableName = generateUniqueName();
        List<byte []> byteColumnValues = new ArrayList<>();
        byteColumnValues.add( new byte[] {0,0,0,0,0,0,0,0,0,1});
        byteColumnValues.add(new byte[] {0,0,0,0,0,0,0,0,0,2});
        List<Date> dateColumnValues = new ArrayList<>();
        dateColumnValues.add(Date.valueOf("2024-02-01"));
        dateColumnValues.add(Date.valueOf("2024-01-31"));
        Timestamp timestampColumnValue = Timestamp.valueOf("2024-01-31 12:12:14");
        String cdcName, cdc_sql;
        try (Connection conn = newConnection()) {
            createTable(conn, "CREATE TABLE  " + tableName + " (" +
                    (multitenant ? "TENANT_ID CHAR(5) NOT NULL, " : "") +
                    "k INTEGER NOT NULL, a_binary binary(10), d Date, t TIMESTAMP, " +
                    "CONSTRAINT PK PRIMARY KEY " +
                    (multitenant ? "(TENANT_ID, k) " : "(k)") + ")", encodingScheme, multitenant);
            if (forView) {
                String viewName = generateUniqueName();
                createTable(conn, "CREATE VIEW " + viewName + " AS SELECT * FROM " + tableName,
                        encodingScheme, false);
                tableName = viewName;
            }
            cdcName = generateUniqueName();
            cdc_sql = "CREATE CDC " + cdcName + " ON " + tableName;
            if (!dataFirst) {
                createCDCAndWait(conn, tableName, cdcName, cdc_sql, encodingScheme, nSaltBuckets);
            }
        }

        String tenantId = multitenant ? "1000" : null;
        try (Connection conn = newConnection(tenantId)) {
            String upsertQuery = "UPSERT INTO " + tableName + " (k, a_binary, d, t) VALUES (?, ?, ?, ?)";
            PreparedStatement stmt = conn.prepareStatement(upsertQuery);
            stmt.setInt(1, 1);
            stmt.setBytes(2, byteColumnValues.get(0));
            stmt.setDate(3, dateColumnValues.get(0));
            stmt.setTimestamp(4, timestampColumnValue);
            stmt.execute();
            conn.commit();
            stmt.setInt(1, 2);
            stmt.setBytes(2, byteColumnValues.get(1));
            stmt.setDate(3, dateColumnValues.get(1));
            stmt.setTimestamp(4, timestampColumnValue);
            stmt.execute();
            conn.commit();
        }

        if (dataFirst) {
            try (Connection conn = newConnection()) {
                createCDCAndWait(conn, tableName, cdcName, cdc_sql, encodingScheme, nSaltBuckets);
            }
        }

        try (Connection conn = newConnection(tenantId)) {
            assertCDCBinaryAndDateColumn(conn.createStatement().executeQuery
                    ("SELECT /*+ CDC_INCLUDE(PRE, POST, CHANGE) */ * " + "FROM " + cdcName),
                    byteColumnValues, dateColumnValues, timestampColumnValue);
        }
    }

    @Test
    public void testSelectCDCFailDataTableUpdate() throws Exception {
        Connection conn = newConnection();
        String tableName = generateUniqueName();
        createTable(conn, "CREATE TABLE  " + tableName +
                " ( k INTEGER PRIMARY KEY," + " v1 INTEGER, v2 INTEGER)", encodingScheme);
        if (forView) {
            String viewName = generateUniqueName();
            createTable(conn, "CREATE VIEW " + viewName + " AS SELECT * FROM " + tableName,
                    encodingScheme);
            tableName = viewName;
        }
        String cdcName = generateUniqueName();
        String cdc_sql = "CREATE CDC " + cdcName
                + " ON " + tableName;
        if (! dataFirst) {
            createCDCAndWait(conn, tableName, cdcName, cdc_sql, encodingScheme, nSaltBuckets);
        }
        IndexRegionObserver.setFailDataTableUpdatesForTesting(true);
        EnvironmentEdgeManager.injectEdge(injectEdge);
        injectEdge.setValue(System.currentTimeMillis());
        conn.createStatement().execute("UPSERT INTO " + tableName + " (k, v1, v2) VALUES (1, 100, 1000)");
        injectEdge.incrementValue(100);
        conn.createStatement().execute("UPSERT INTO " + tableName + " (k, v1, v2) VALUES (2, 200, 2000)");
        commitWithException(conn);

        injectEdge.incrementValue(100);
        conn.createStatement().execute("UPSERT INTO " + tableName + " (k, v1) VALUES (1, 101)");
        commitWithException(conn);

        injectEdge.incrementValue(100);
        conn.createStatement().execute("DELETE FROM " + tableName + " WHERE k=1");
        commitWithException(conn);

        injectEdge.incrementValue(100);
        conn.createStatement().execute("UPSERT INTO " + tableName + " (k, v1, v2) VALUES (1, 102, 1002)");
        commitWithException(conn);

        injectEdge.incrementValue(100);
        conn.createStatement().execute("DELETE FROM " + tableName + " WHERE k=1");
        commitWithException(conn);

        injectEdge.incrementValue(100);
        conn.createStatement().execute("UPSERT INTO " + tableName + " (k, v1, v2) VALUES (2, 201, NULL)");
        commitWithException(conn);

        injectEdge.incrementValue(100);
        conn.createStatement().execute("UPSERT INTO " + tableName + " (k, v1, v2) VALUES (1, 103, 1003)");
        commitWithException(conn);

        injectEdge.incrementValue(100);
        conn.createStatement().execute("DELETE FROM " + tableName + " WHERE k=1");
        commitWithException(conn);

        injectEdge.incrementValue(100);
        conn.createStatement().execute("UPSERT INTO " + tableName + " (k, v1, v2) VALUES (1, 104, 1004)");
        commitWithException(conn);

        injectEdge.incrementValue(100);
        conn.createStatement().execute("DELETE FROM " + tableName + " WHERE k=1");
        commitWithException(conn);
        IndexRegionObserver.setFailDataTableUpdatesForTesting(false);
        EnvironmentEdgeManager.reset();

        if (dataFirst) {
            createCDCAndWait(conn, tableName, cdcName, cdc_sql, encodingScheme, nSaltBuckets);
        }

        ResultSet rs = conn.createStatement().executeQuery("SELECT * FROM " + cdcName);
        assertEquals(false, rs.next());
    }

    static private void commitWithException(Connection conn) {
        try {
            conn.commit();
            IndexRegionObserver.setFailPreIndexUpdatesForTesting(false);
            IndexRegionObserver.setFailDataTableUpdatesForTesting(false);
            IndexRegionObserver.setFailPostIndexUpdatesForTesting(false);
            fail();
        } catch (Exception e) {
            // this is expected
        }
    }

}
