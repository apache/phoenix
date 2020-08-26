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

import static java.util.Collections.singletonList;
import static org.apache.phoenix.util.PhoenixRuntime.TENANT_ID_ATTRIB;
import static org.apache.phoenix.util.TestUtil.TEST_PROPERTIES;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Properties;

import com.google.common.base.Function;
import com.google.common.base.Joiner;
import com.google.common.collect.Lists;
import org.apache.phoenix.compile.QueryPlan;
import org.apache.phoenix.iterate.ExplainTable;
import org.apache.phoenix.schema.SortOrder;
import org.apache.phoenix.schema.TypeMismatchException;
import org.apache.phoenix.schema.types.PDataType;
import org.apache.phoenix.schema.types.PInteger;
import org.apache.phoenix.util.PhoenixRuntime;
import org.apache.phoenix.util.PropertiesUtil;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;


public class InListIT extends ParallelStatsDisabledIT {
    private static final String TENANT_SPECIFIC_URL1 = getUrl() + ';' + TENANT_ID_ATTRIB + "=tenant1";
    private static boolean isInitialized = false;
    private static String tableName = generateUniqueName();
    private static String tableName2 = generateUniqueName();
    private static String descViewName = generateUniqueName();
    private static String ascViewName = generateUniqueName();
    private static String viewName1 = generateUniqueName();
    private static String viewName2 = generateUniqueName();
    private static String prefix = generateUniqueName();

    @Before
    public void setup() throws Exception {
        if(isInitialized){
            return;
        }
        initializeTables();
        isInitialized = true;
    }

    @After
    public void cleanUp() throws SQLException {
        deleteTenantData(descViewName);
        deleteTenantData(viewName1);
        deleteTenantData(viewName2);
        deleteTenantData(ascViewName);
        deleteTenantData(tableName);
        deleteTenantData(tableName2);
    }

    @Test
    public void testLeadingPKWithTrailingRVC() throws Exception {
        Properties props = PropertiesUtil.deepCopy(TEST_PROPERTIES);
        Connection conn = DriverManager.getConnection(getUrl(), props);
        String tableName = generateUniqueName();
        conn.createStatement().execute("CREATE TABLE " + tableName
                + "( col1 VARCHAR NOT NULL,"
                + "  col2 VARCHAR NOT NULL, "
                + "  id VARCHAR NOT NULL,"
                + "  CONSTRAINT pk PRIMARY KEY (col1, col2, id))");

        conn.createStatement().execute("upsert into " + tableName + " (col1, col2, id) values ('a', 'b', 'c')");
        conn.createStatement().execute("upsert into " + tableName + " (col1, col2, id) values ('a', 'b', 'd')");
        conn.commit();

        ResultSet rs = conn.createStatement().executeQuery("select id from " + tableName + " WHERE col1 = 'a' and ((col2, id) IN (('b', 'c'),('b', 'e')))");
        assertTrue(rs.next());
        assertEquals("c", rs.getString(1));
        assertFalse(rs.next());

        conn.close();
    }

    @Test
    public void testLeadingPKWithTrailingRVC2() throws Exception {
        Properties props = PropertiesUtil.deepCopy(TEST_PROPERTIES);
        Connection conn = DriverManager.getConnection(getUrl(), props);
        String tableName = generateUniqueName();
        conn.createStatement().execute("CREATE TABLE " + tableName + " ( user VARCHAR, tenant_id VARCHAR(5) NOT NULL,tenant_type_id VARCHAR(3) NOT NULL,  id INTEGER NOT NULL CONSTRAINT pk PRIMARY KEY (tenant_id, tenant_type_id, id))");

        conn.createStatement().execute("upsert into " + tableName + " (tenant_id, tenant_type_id, id, user) values ('a', 'a', 1, 'BonA')");
        conn.createStatement().execute("upsert into " + tableName + " (tenant_id, tenant_type_id, id, user) values ('a', 'a', 2, 'BonB')");
        conn.commit();

        ResultSet rs = conn.createStatement().executeQuery("select id from " + tableName + " WHERE tenant_id = 'a' and tenant_type_id = 'a' and ((id, user) IN ((1, 'BonA'),(1, 'BonB')))");
        assertTrue(rs.next());
        assertEquals(1, rs.getInt(1));
        assertFalse(rs.next());

        conn.close();
    }

    /**
     * Builds the DDL statement that will create a table with the given properties.
     * Assumes 5 pk columns of the given type.
     * Adds a non primary key column named "nonPk"
     * @param tableName  the name to use for the table
     * @param pkType  the data type of the primary key columns
     * @param saltBuckets  the number of salt buckets if the table is salted, otherwise 0
     * @param isMultiTenant  whether or not the table needs a tenant_id column
     * @return  the final DDL statement
     */
    private static String createTableDDL(String tableName, PDataType pkType, int saltBuckets, boolean isMultiTenant) {
        StringBuilder ddlBuilder = new StringBuilder();
        ddlBuilder.append("CREATE TABLE ").append(tableName).append(" ( ");

        // column declarations
        if(isMultiTenant) {
            ddlBuilder.append("tenantId VARCHAR(5) NOT NULL, ");
        }
        for(int i = 0; i < 5; i++) {
            ddlBuilder.append("pk").append(i + 1).append(" ").append(pkType.getSqlTypeName()).append(" NOT NULL, ");
        }
        ddlBuilder.append("nonPk VARCHAR ");

        // primary key constraint declaration
        ddlBuilder.append("CONSTRAINT pk PRIMARY KEY (");
        if(isMultiTenant) {
            ddlBuilder.append("tenantId, ");
        }
        ddlBuilder.append("pk1, pk2, pk3, pk4, pk5) ) ");

        // modifier declarations
        if(saltBuckets != 0) {
            ddlBuilder.append("SALT_BUCKETS = ").append(saltBuckets);
        }
        if(saltBuckets != 0 && isMultiTenant) {
            ddlBuilder.append(", ");
        }
        if(isMultiTenant) {
            ddlBuilder.append("MULTI_TENANT=true");
        }

        return ddlBuilder.toString();
    }

    /**
     * Creates a table with the given properties and returns its name. If the table is multi-tenant,
     * also creates a tenant view for that table and returns the name of the view instead.
     * @param baseConn  a non-tenant specific connection. Used to create the base tables
     * @param conn  a tenant-specific connection, if necessary. Otherwise ignored.
     * @param isMultiTenant  whether or not this table should be multi-tenant
     * @param pkType  the data type of the primary key columns
     * @param saltBuckets  the number of salt buckets if the table is salted, otherwise 0
     * @return  the table or view name that should be used to access the created table
     */
    private static String initializeAndGetTable(Connection baseConn, Connection conn, boolean isMultiTenant, PDataType pkType, int saltBuckets) throws SQLException {
        String tableName = getTableName(isMultiTenant, pkType, saltBuckets);
        String tableDDL = createTableDDL(tableName, pkType, saltBuckets, isMultiTenant);
        baseConn.createStatement().execute(tableDDL);

        // if requested, create a tenant specific view and return the view name instead
        if(isMultiTenant) {
            String viewName = tableName + "_view";
            String viewDDL = "CREATE VIEW " + viewName + " AS SELECT * FROM " + tableName;
            conn.createStatement().execute(viewDDL);
            return viewName;
        }
        else {
            return tableName;
        }
    }

    private static String getTableName(boolean isMultiTenant, PDataType pkType, int saltBuckets) {
        return prefix+"init_in_test_" + pkType.getSqlTypeName() + saltBuckets + (isMultiTenant ?
            "_multi" :
            "_single");
    }

    private static final String TENANT_ID = "ABC";
    private static final String TENANT_URL = getUrl() + ";" + PhoenixRuntime.TENANT_ID_ATTRIB + '=' + TENANT_ID;

    // the different combinations to check each test against
    private static final List<Boolean> TENANCIES = Arrays.asList(false, true);
    private static final List<? extends PDataType> INTEGER_TYPES = Arrays.asList(PInteger.INSTANCE);
    private static final List<Integer> SALT_BUCKET_NUMBERS = Arrays.asList(0, 4);

    private static final List<String> HINTS = Arrays.asList("/*+ SKIP_SCAN */", "/*+ RANGE_SCAN */");

    private  void initializeTables() throws Exception {
        buildSchema(tableName, viewName1, true);
        buildSchema(tableName2, viewName2, false);
        for (boolean isMultiTenant : TENANCIES) {
            Connection baseConn = DriverManager.getConnection(getUrl());
            Connection conn = isMultiTenant ? DriverManager.getConnection(TENANT_URL) : baseConn;

            try {
                // test each combination of types and salting
                for (PDataType pkType : INTEGER_TYPES) {
                    for (int saltBuckets : SALT_BUCKET_NUMBERS) {
                        // use a different table with a unique name for each variation
                        String tableName =
                            initializeAndGetTable(baseConn, conn, isMultiTenant, pkType,
                                saltBuckets);

                        // upsert the given data
                        for (String upsertBody : DEFAULT_UPSERT_BODIES) {
                            conn.createStatement()
                                .execute("UPSERT INTO " + tableName + " " + upsertBody);
                        }
                        conn.commit();
                    }
                }
            }

            // clean up the connections used
            finally {
                baseConn.close();
                if (!conn.isClosed()) {
                    conn.close();
                }

            }
        }
    }

    /**
     * Tests the given where clause against the given upserts by comparing against the list of
     * expected result strings.
     * @param whereClause  the where clause to test. Should only refer to the pks upserted.
     * @param expecteds  a complete list of all of the expected result row names
     */
    private void testWithIntegerTypesWithVariedSaltingAndTenancy(String whereClause,
        List<String> expecteds) throws SQLException {
        // test single and multitenant tables
        for(boolean isMultiTenant : TENANCIES) {
            Connection baseConn = DriverManager.getConnection(getUrl());
            Connection conn = isMultiTenant ? DriverManager.getConnection(TENANT_URL)
                    : baseConn;

            try {
                // test each combination of types and salting
                for(PDataType pkType : INTEGER_TYPES) {
                    for(int saltBuckets : SALT_BUCKET_NUMBERS) {
                        // use a different table with a unique name for each variation
                        String tableName = getTableName(isMultiTenant, pkType, saltBuckets);

                        for(String hint : HINTS) {
                            String context = "where: " + whereClause + ", type: " + pkType + ", salt buckets: "
                                    + saltBuckets + ", multitenant: " + isMultiTenant + ", hint: " + hint + "";

                            // perform the query
                            String sql = "SELECT " + hint + " nonPk FROM " + tableName + " " + whereClause;
                            ResultSet rs = conn.createStatement().executeQuery(sql);
                            for (String expected : expecteds) {
                                assertTrue("did not include result '" + expected + "' (" + context + ")", rs.next());
                                assertEquals(context, expected, rs.getString(1));
                            }
                            assertFalse(context, rs.next());
                        }
                    }
                }
            }
            // clean up the connections used
            finally {
                baseConn.close();
                if(!conn.isClosed()) {
                    conn.close();
                }
            }
        }
    }

    List<List<Object>> DEFAULT_UPSERTS = Arrays.asList(Arrays.<Object>asList(1, 2, 4, 5, 6, "row1"),
            Arrays.<Object>asList(2, 3, 4, 5, 6, "row2"),
            Arrays.<Object>asList(2, 3, 6, 4, 5, "row3"),
            Arrays.<Object>asList(6, 5, 4, 3, 2, "row4"));

    List<String> DEFAULT_UPSERT_BODIES = Lists.transform(DEFAULT_UPSERTS, new Function<List<Object>, String>() {
        @Override
        public String apply(List<Object> input) {
            List<Object> pks = input.subList(0, 5);
            Object nonPk = input.get(5);

            return "(pk1, pk2, pk3, pk4, pk5, nonPk) VALUES ( "
                    + Joiner.on(", ").join(pks) + ", '" + nonPk + "')";
        }
    });

    // test variations used:
    // 1. queries with no results
    // 2. queries with fully qualified row keys
    // 3. queries with partiall qualified row keys, starting from the beginning
    // 4. queries with partially qualified row keys, but not the beginning
    // 5. queries with partially qualified row keys with a "hole slot" in the middle

    @Test
    public void testPlainRVCNoResults() throws Exception {
        String whereClause = "WHERE (pk1, pk2, pk3, pk4, pk5) IN ((1, 2, 3, 4, 5), (1, 2, 4, 5, 3))";
        List<String> expecteds = Collections.<String>emptyList();

        testWithIntegerTypesWithVariedSaltingAndTenancy(whereClause, expecteds);
    }

    @Test
    public void testPlainRVCFullyQualified() throws Exception {
        String whereClause = "WHERE (pk1, pk2, pk3, pk4, pk5) IN ((1, 2, 3, 4, 5), (1, 2, 4, 5, 6))";
        List<String> expecteds = singletonList("row1");

        testWithIntegerTypesWithVariedSaltingAndTenancy(whereClause, expecteds);
    }

    @Test
    public void testPlainRVCPartiallyQualifiedBegin() throws Exception {
        String whereClause = "WHERE (pk1, pk2, pk3, pk4) IN ((2, 3, 4, 5), (1, 2, 4, 5))";
        List<String> expecteds = Arrays.asList("row1", "row2");

        testWithIntegerTypesWithVariedSaltingAndTenancy( whereClause, expecteds);
    }

    @Test
    public void testPlainRVCPartiallyQualifiedEnd() throws Exception {
        String whereClause = "WHERE (pk2, pk3, pk4, pk5) IN ((2, 3, 4, 5), (2, 4, 5, 6))";
        List<String> expecteds = singletonList("row1");

        testWithIntegerTypesWithVariedSaltingAndTenancy( whereClause, expecteds);
    }

    @Test
    public void testPlainRVCSlotHole() throws Exception {
        String whereClause = "WHERE (pk1, pk2, pk4, pk5) IN ((1, 2, 4, 5), (6, 5, 3, 2))";
        List<String> expecteds = singletonList("row4");

        testWithIntegerTypesWithVariedSaltingAndTenancy( whereClause, expecteds);
    }

    @Test
    public void testLeadingPKWithTrailingRVCNoResults() throws Exception {
        String whereClause = "WHERE pk1 != 2 AND (pk3, pk4, pk5) IN ((6, 4, 5), (5, 6, 4))";
        List<String> expecteds = Collections.<String>emptyList();

        testWithIntegerTypesWithVariedSaltingAndTenancy( whereClause, expecteds);
    }

    @Test
    public void testLeadingPKWithTrailingRVCFullyQualified() throws Exception {
        String whereClause = "WHERE pk1 = 2 AND (pk2, pk3, pk4, pk5) IN ((2, 4, 5, 6), (3, 4, 5, 6))";
        List<String> expecteds = singletonList("row2");

        testWithIntegerTypesWithVariedSaltingAndTenancy(whereClause, expecteds);
    }

    @Test
    public void testLeadingPKWithTrailingRVCPartiallyQualifiedBegin() throws Exception {
        String whereClause = "WHERE pk1 = 2 AND (pk2, pk3) IN ((3, 6), (5, 4))";
        List<String> expecteds = singletonList("row3");

        testWithIntegerTypesWithVariedSaltingAndTenancy(whereClause, expecteds);
    }

    @Test
    public void testLeadingPKWithTrailingRVCPartiallyQualifiedEnd() throws Exception {
        String whereClause = "WHERE pk2 = 2 AND (pk3, pk4, pk5) IN ((4, 5, 6), (5, 6, 4))";
        List<String> expecteds = singletonList("row1");

        testWithIntegerTypesWithVariedSaltingAndTenancy( whereClause, expecteds);
    }

    @Test
    public void testLeadingPKWithTrailingRVCSlotHole() throws Exception {
        String whereClause = "WHERE pk1 = 2 AND (pk3, pk4, pk5) IN ((4, 5, 6), (5, 6, 4))";
        List<String> expecteds = singletonList("row2");

        testWithIntegerTypesWithVariedSaltingAndTenancy( whereClause, expecteds);
    }

    @Test
    public void testLeadingRVCWithTrailingPKNoResults() throws Exception {
        String whereClause = "WHERE (pk1, pk2, pk3) IN ((2, 3, 4), (2, 3, 6)) AND pk4 = 3";
        List<String> expecteds = Collections.<String>emptyList();

        testWithIntegerTypesWithVariedSaltingAndTenancy( whereClause, expecteds);
    }

    @Test
    public void testLeadingRVCWithTrailingPKFullyQualified() throws Exception {
        String whereClause = "WHERE (pk1, pk2, pk3, pk4) IN ((1, 2, 4, 5), (2, 3, 4, 5)) AND pk5 = 6";
        List<String> expecteds = Arrays.asList("row1", "row2");

        testWithIntegerTypesWithVariedSaltingAndTenancy(whereClause, expecteds);
    }

    @Test
    public void testLeadingRVCWithTrailingPKPartiallyQualifiedBegin() throws Exception {
        String whereClause = "WHERE (pk1, pk2, pk3) IN ((2, 3, 4), (2, 3, 6)) AND pk4 = 4";
        List<String> expecteds = singletonList("row3");

        testWithIntegerTypesWithVariedSaltingAndTenancy(whereClause, expecteds);
    }

    @Test
    public void testLeadingRVCWithTrailingPKPartiallyQualifiedEnd() throws Exception {
        String whereClause = "WHERE (pk2, pk3, pk4) IN ((3, 4, 5), (3, 6, 4)) AND pk5 = 5";
        List<String> expecteds = singletonList("row3");

        testWithIntegerTypesWithVariedSaltingAndTenancy( whereClause, expecteds);
    }

    @Test
    public void testLeadingRVCWithTrailingPKSlotHole() throws Exception {
        String whereClause = "WHERE (pk1, pk2, pk3) IN ((2, 3, 4), (2, 3, 6)) AND pk5 = 5";
        List<String> expecteds = singletonList("row3");

        testWithIntegerTypesWithVariedSaltingAndTenancy( whereClause, expecteds);
    }

    @Test
    public void testOverlappingRVCAndPKNoResults() throws Exception {
        String whereClause = "WHERE (pk1, pk2) IN ((1, 2), (2, 3)) AND pk2 = 4";
        List<String> expecteds = Collections.<String>emptyList();

        testWithIntegerTypesWithVariedSaltingAndTenancy( whereClause, expecteds);
    }

    @Test
    public void testOverlappingRVCAndPKFullyQualified() throws Exception {
        String whereClause = "WHERE (pk1, pk2, pk3, pk4, pk5) IN ((1, 2, 4, 5, 6), (2, 3, 4, 5, 6)) AND pk1 = 2";
        List<String> expecteds = singletonList("row2");

        testWithIntegerTypesWithVariedSaltingAndTenancy(whereClause, expecteds);
    }

    @Test
    public void testOverlappingRVCAndPKPartiallyQualifiedBegin() throws Exception {
        String whereClause = "WHERE (pk1, pk2, pk3) IN ((1, 2, 4), (2, 3, 6)) AND pk3 = 4";
        List<String> expecteds = singletonList("row1");

        testWithIntegerTypesWithVariedSaltingAndTenancy( whereClause, expecteds);
    }

    @Test
    public void testOverlappingRVCAndPKPartiallyQualifiedEnd() throws Exception {
        String whereClause = "WHERE (pk3, pk4, pk5) IN ((4, 5, 6), (4, 3, 2)) AND pk5 = 2";
        List<String> expecteds = singletonList("row4");

        testWithIntegerTypesWithVariedSaltingAndTenancy(whereClause, expecteds);
    }

    @Test
    public void testOverlappingRVCAndRVCNoResults() throws Exception {
        String whereClause = "WHERE (pk1, pk2) IN ((1, 2), (2, 3)) AND (pk2, pk3) IN ((4, 4), (4, 6))";
        List<String> expecteds = Collections.<String>emptyList();

        testWithIntegerTypesWithVariedSaltingAndTenancy(whereClause, expecteds);
    }

    @Test
    public void testOverlappingRVCAndRVCFullyQualified() throws Exception {
        String whereClause = "WHERE (pk1, pk2, pk3) IN ((2, 3, 6), (2, 3, 4)) AND (pk3, pk4, pk5) IN ((4, 5, 6), (4, 3, 2))";
        List<String> expecteds = singletonList("row2");

        testWithIntegerTypesWithVariedSaltingAndTenancy( whereClause, expecteds);
    }

    @Test public void testOverlappingRVCWithMiddleColumn() throws Exception {
        String whereClause =
            "WHERE pk2=3 and (pk1, pk2, pk3, pk4) IN ((2, 3, 6, 6), (2, 3, 4, 5)) ";
        List<String> expecteds = singletonList("row2");

        testWithIntegerTypesWithVariedSaltingAndTenancy( whereClause,
            expecteds);
    }

    @Test public void testOverlappingRVCWithMultipleMiddleColumn() throws Exception {
        String whereClause =
            "WHERE (pk2,pk3) in ((3,4)) and (pk1, pk2, pk3, pk4) IN ((2, 3, 6, 6), (2, 3, 4, 5)) ";
        List<String> expecteds = singletonList("row2");

        testWithIntegerTypesWithVariedSaltingAndTenancy( whereClause,
            expecteds);
    }

    @Test
    public void testOverlappingRVCAndRVCPartiallyQualifiedBegin() throws Exception {
        String whereClause = "WHERE (pk1, pk2) IN ((1, 2), (2, 3)) AND (pk2, pk3) IN ((3, 4), (3, 6))";
        List<String> expecteds = Arrays.asList("row2", "row3");

        testWithIntegerTypesWithVariedSaltingAndTenancy( whereClause, expecteds);
    }

    @Test
    public void testOverlappingRVCAndRVCPartiallyQualifiedEnd() throws Exception {
        String whereClause = "WHERE (pk3, pk4) IN ((4, 5), (4, 3)) AND (pk4, pk5) IN ((3, 2), (4, 5))";
        List<String> expecteds = singletonList("row4");

        testWithIntegerTypesWithVariedSaltingAndTenancy(whereClause, expecteds);
    }

    @Test
    public void testWithFixedLengthDescPK() throws Exception {
        testWithFixedLengthPK(SortOrder.DESC);
    }

    @Test
    public void testWithFixedLengthAscPK() throws Exception {
        testWithFixedLengthPK(SortOrder.ASC);
    }

    @Test
    public void testWithFixedLengthKV() throws Exception {
        Properties props = PropertiesUtil.deepCopy(TEST_PROPERTIES);
        Connection conn = DriverManager.getConnection(getUrl(), props);
        String tableName = generateUniqueName();
        conn.createStatement().execute("CREATE TABLE " + tableName + " ( id INTEGER PRIMARY KEY, k CHAR(3))");

        conn.createStatement().execute("upsert into " + tableName + " values (1, 'aa')");
        conn.createStatement().execute("upsert into " + tableName + " values (2, 'bb')");
        conn.commit();

        ResultSet rs = conn.createStatement().executeQuery("select k from " + tableName + " WHERE k IN ('aa','bb')");
        assertTrue(rs.next());
        assertEquals("aa", rs.getString(1));
        assertTrue(rs.next());
        assertEquals("bb", rs.getString(1));
        assertFalse(rs.next());

        conn.close();
    }

    private void testWithFixedLengthPK(SortOrder sortOrder) throws Exception {
        Properties props = PropertiesUtil.deepCopy(TEST_PROPERTIES);
        Connection conn = DriverManager.getConnection(getUrl(), props);
        String tableName = generateUniqueName();
        conn.createStatement().execute("CREATE TABLE " + tableName + " ( k CHAR(3) PRIMARY KEY " + (sortOrder == SortOrder.DESC ? "DESC" : "") + ")");

        conn.createStatement().execute("upsert into " + tableName + " (k) values ('aa')");
        conn.createStatement().execute("upsert into " + tableName + " (k) values ('bb')");
        conn.commit();

        ResultSet rs = conn.createStatement().executeQuery("select k from " + tableName + " WHERE k IN ('aa','bb')");
        assertTrue(rs.next());
        assertEquals(sortOrder == SortOrder.ASC ? "aa" : "bb", rs.getString(1));
        assertTrue(rs.next());
        assertEquals(sortOrder == SortOrder.ASC ? "bb" : "aa", rs.getString(1));
        assertFalse(rs.next());

        conn.close();
    }

    @Test
    public void testInListExpressionWithNull() throws Exception {
        try (Connection conn = DriverManager.getConnection(getUrl())) {
            Statement stmt = conn.createStatement();
            ResultSet rs = stmt.executeQuery("SELECT COUNT(*) FROM SYSTEM.CATALOG WHERE " +
                    "TENANT_ID IN ('', 'FOO')");
            ResultSet rs2 = stmt.executeQuery("SELECT COUNT(*) FROM SYSTEM.CATALOG WHERE " +
                    "TENANT_ID = '' OR TENANT_ID = 'FOO'");
            assertTrue(rs.next());
            assertTrue(rs2.next());
            assertEquals(rs.getInt(1), rs2.getInt(1));
            assertEquals(0, rs.getInt(1));
        }
    }

    @Test(expected = TypeMismatchException.class)
    public void testInListExpressionWithNotValidElements() throws Exception {
        try (Connection conn = DriverManager.getConnection(getUrl())) {
            Statement stmt = conn.createStatement();
            ResultSet rs = stmt.executeQuery("SELECT COUNT(*) FROM SYSTEM.CATALOG WHERE " +
                    "TENANT_ID IN (4, 8)");
            assertTrue(rs.next());
            assertEquals(0, rs.getInt(1));
        }
    }

    @Test(expected = SQLException.class)
    public void testInListExpressionWithNoElements() throws Exception {
        try (Connection conn = DriverManager.getConnection(getUrl())) {
            Statement stmt = conn.createStatement();
            ResultSet rs = stmt.executeQuery("SELECT COUNT(*) FROM SYSTEM.CATALOG WHERE " +
                    "TENANT_ID IN ()");
            assertTrue(rs.next());
            assertEquals(0, rs.getInt(1));
        }
    }

    @Test
    public void testInListExpressionWithNullAndWrongTypedData() throws Exception {
        try (Connection conn = DriverManager.getConnection(getUrl())) {
            Statement stmt = conn.createStatement();
            ResultSet rs = stmt.executeQuery("SELECT COUNT(*) FROM SYSTEM.CATALOG WHERE " +
                    "TENANT_ID IN ('', 4)");
            assertTrue(rs.next());
            assertEquals(0, rs.getInt(1));
        }
    }

    @Test
    public void testInListExpressionWithDesc() throws Exception {
        String fullTableName = generateUniqueName();
        String fullViewName = generateUniqueName();
        String tenantView = generateUniqueName();
        // create base table and global view using global connection
        try (Connection conn = DriverManager.getConnection(getUrl())) {
            Statement stmt = conn.createStatement();
            stmt.execute("CREATE TABLE " + fullTableName + "(\n" +
                    "    TENANT_ID CHAR(15) NOT NULL,\n" +
                    "    KEY_PREFIX CHAR(3) NOT NULL,\n" +
                    "    CREATED_DATE DATE,\n" +
                    "    CREATED_BY CHAR(15),\n" +
                    "    SYSTEM_MODSTAMP DATE\n" +
                    "    CONSTRAINT PK PRIMARY KEY (\n" +
                    "       TENANT_ID," +
                    "       KEY_PREFIX" +
                    ")) MULTI_TENANT=TRUE");

            stmt.execute("CREATE VIEW " + fullViewName + "(\n" +
                    "    MODEL VARCHAR NOT NULL,\n" +
                    "    MILEAGE  BIGINT NOT NULL,\n" +
                    "    MILES_DRIVEN BIGINT NOT NULL,\n" +
                    "    MAKE VARCHAR,\n" +
                    "    CONSTRAINT PKVIEW PRIMARY KEY\n" +
                    "    (\n" +
                    "    MODEL, MILEAGE DESC, MILES_DRIVEN\n" +
                    ")) AS SELECT * FROM " + fullTableName + " WHERE KEY_PREFIX = '0CY'");

        }

        // create and use a tenant specific view to write data
        try (Connection viewConn = DriverManager.getConnection(TENANT_SPECIFIC_URL1) ) {
            Statement stmt = viewConn.createStatement();
            stmt.execute("CREATE VIEW IF NOT EXISTS " + tenantView + " AS SELECT * FROM " + fullViewName );
            viewConn.createStatement().execute("UPSERT INTO " + tenantView + "(CREATED_BY, CREATED_DATE, SYSTEM_MODSTAMP, MODEL, MILEAGE, MILES_DRIVEN, MAKE) VALUES ('005xx000001Sv6o', 1532458254819, 1532458254819, 'a5', 23, 10000, 'AUDI')");
            viewConn.createStatement().execute("UPSERT INTO " + tenantView + "(CREATED_BY, CREATED_DATE, SYSTEM_MODSTAMP, MODEL, MILEAGE, MILES_DRIVEN, MAKE) VALUES ('005xx000001Sv6o', 1532458254819, 1532458254819, 'a4', 27, 30000, 'AUDI')");
            viewConn.createStatement().execute("UPSERT INTO " + tenantView + "(CREATED_BY, CREATED_DATE, SYSTEM_MODSTAMP, MODEL, MILEAGE, MILES_DRIVEN, MAKE) VALUES ('005xx000001Sv6o', 1532458254819, 1532458254819, '328i', 32, 40000, 'BMW')");
            viewConn.commit();

            ResultSet rs = stmt.executeQuery("SELECT Make, Model FROM " + tenantView + " WHERE MILEAGE IN (32, 27)");
            assertTrue(rs.next());
            assertEquals("BMW", rs.getString(1));
            assertEquals("328i", rs.getString(2));
            assertTrue(rs.next());
            assertEquals("AUDI", rs.getString(1));
            assertEquals("a4", rs.getString(2));
            assertFalse(rs.next());
        }
    }

    private void buildSchema(String fullTableName, String fullViewName, boolean isDecOrder) throws Exception {
        try (Connection conn = DriverManager.getConnection(getUrl())) {
            conn.setAutoCommit(true);
            try (Statement stmt = conn.createStatement()) {
                stmt.execute("CREATE TABLE " + fullTableName + "(\n" + " TENANT_ID CHAR(15) NOT NULL,\n" + " KEY_PREFIX CHAR(3) NOT NULL, ID5 BIGINT \n" +
                        " CONSTRAINT PK PRIMARY KEY (\n" + " TENANT_ID," + " KEY_PREFIX" + ")) MULTI_TENANT=TRUE");
                if (isDecOrder) {
                    stmt.execute("CREATE VIEW " + fullViewName + "(\n" + " ID1 VARCHAR NOT NULL,\n" + " ID2 VARCHAR NOT NULL,\n" + " ID3 BIGINT, ID4 BIGINT \n" +
                            " CONSTRAINT PKVIEW PRIMARY KEY\n" + " (\n" + " ID1, ID2 DESC\n" + ")) " +
                            "AS SELECT * FROM " + fullTableName + " WHERE KEY_PREFIX = '0CY'");
                    try (Connection viewConn = DriverManager.getConnection(TENANT_SPECIFIC_URL1)) {
                        viewConn.setAutoCommit(true);
                        try (Statement tenantStmt = viewConn.createStatement()) {
                            tenantStmt.execute("CREATE VIEW IF NOT EXISTS " + this.descViewName + " AS SELECT * FROM " + fullViewName);
                        }
                    }
                } else {
                    stmt.execute("CREATE VIEW " + fullViewName + "(\n" + " ID1 VARCHAR NOT NULL,\n" + " ID2 VARCHAR NOT NULL,\n" + " ID3 BIGINT, ID4 BIGINT \n" +
                            " CONSTRAINT PKVIEW PRIMARY KEY\n" + " (ID1, ID2)) " +
                            "AS SELECT * FROM " + fullTableName + " WHERE KEY_PREFIX = '0CY'");
                    try (Connection viewConn = DriverManager.getConnection(TENANT_SPECIFIC_URL1)) {
                        viewConn.setAutoCommit(true);
                        try (Statement tenantStmt = viewConn.createStatement()) {
                            tenantStmt.execute("CREATE VIEW IF NOT EXISTS " + this.ascViewName + " AS SELECT * FROM " + fullViewName);
                        }
                    }
                }
            }
        }
    }

    @Test
    public void testPkDescOrderedTenantViewOnGlobalViewWithRightQueryPlan() throws Exception {
        String tenantView = generateUniqueName();

        try (Connection viewConn = DriverManager.getConnection(TENANT_SPECIFIC_URL1)) {
            viewConn.setAutoCommit(true);
            try (Statement stmt = viewConn.createStatement()) {
                stmt.execute("CREATE VIEW IF NOT EXISTS " + tenantView + " AS SELECT * FROM " + descViewName);
                viewConn.createStatement().execute("UPSERT INTO " + tenantView + "(ID1, ID2) VALUES " +
                        "('foo', '000000000000300')");
                viewConn.createStatement().execute("UPSERT INTO " + tenantView + "(ID1, ID2) VALUES " +
                        "('005xx000001Sv6o', '000000000000500')");
                viewConn.commit();

                try (PreparedStatement preparedStmt = viewConn.prepareStatement(
                        "SELECT * FROM " + tenantView + " WHERE (ID1, ID2) " +
                                "IN (('005xx000001Sv6o', '000000000000500'))")) {
                    QueryPlan queryPlan = PhoenixRuntime.getOptimizedQueryPlan(preparedStmt);
                    assertTrue(queryPlan.getExplainPlan().toString().contains(ExplainTable.POINT_LOOKUP_ON_STRING));
                }

                try (PreparedStatement preparedStmt = viewConn.prepareStatement(
                        "SELECT * FROM " + tenantView + " WHERE (ID2, ID1) " +
                                "IN (('000000000000500', '005xx000001Sv6o'))")) {
                    QueryPlan queryPlan = PhoenixRuntime.getOptimizedQueryPlan(preparedStmt);
                    assertTrue(queryPlan.getExplainPlan().toString().contains(ExplainTable.POINT_LOOKUP_ON_STRING));
                }

                stmt.execute("DELETE FROM " + tenantView + " WHERE (ID2, ID1) IN " +
                        "(('bar', '005xx000001Sv6o'))");

                ResultSet rs = stmt.executeQuery("SELECT ID2 FROM " + tenantView);
                assertTrue(rs.next());
                assertEquals("000000000000500", rs.getString(1));
                stmt.execute("DELETE FROM " + tenantView);
            }
        }
    }

    @Test
    public void testColumnDescOrderedTenantViewOnGlobalViewWithStringValue() throws Exception {
        String tenantView = generateUniqueName();

        try (Connection viewConn = DriverManager.getConnection(TENANT_SPECIFIC_URL1)) {
            viewConn.setAutoCommit(true);
            try (Statement stmt = viewConn.createStatement()) {
                stmt.execute("CREATE VIEW IF NOT EXISTS " + tenantView + " AS SELECT * FROM " + descViewName);
                viewConn.createStatement().execute("UPSERT INTO " + tenantView + "(ID1, ID2) VALUES " +
                        "('foo', '000000000000300')");
                viewConn.createStatement().execute("UPSERT INTO " + tenantView + "(ID1, ID2) VALUES " +
                        "('bar', '000000000000400')");
                viewConn.createStatement().execute("UPSERT INTO " + tenantView + "(ID1, ID2) VALUES " +
                        "('005xx000001Sv6o', '000000000000500')");
                viewConn.commit();

                try (PreparedStatement preparedStmt = viewConn.prepareStatement(
                        "SELECT * FROM " + tenantView + " WHERE (ID1, ID2) " +
                                "IN (('005xx000001Sv6o', '000000000000500')," +
                                "('bar', '000000000000400')," +
                                "('foo', '000000000000300'))")) {
                    QueryPlan queryPlan = PhoenixRuntime.getOptimizedQueryPlan(preparedStmt);
                    assertTrue(queryPlan.getExplainPlan().toString().contains(ExplainTable.POINT_LOOKUP_ON_STRING));
                }

                try (PreparedStatement preparedStmt = viewConn.prepareStatement(
                        "SELECT * FROM " + tenantView + " WHERE (ID2, ID1) IN " +
                                "(('bar', '005xx000001Sv6o')," +
                                "('foo', '005xx000001Sv6o'))")) {
                    QueryPlan queryPlan = PhoenixRuntime.getOptimizedQueryPlan(preparedStmt);
                    assertTrue(queryPlan.getExplainPlan().toString().contains(ExplainTable.POINT_LOOKUP_ON_STRING));
                }

                stmt.execute("DELETE FROM " + tenantView + " WHERE (ID2, ID1) IN " +
                        "(('000000000000400', 'bar')," +
                        "('000000000000300', 'foo'))");

                ResultSet rs = stmt.executeQuery("SELECT ID2 FROM " + tenantView);
                assertTrue(rs.next());
                assertEquals("000000000000500", rs.getString(1));

                rs = stmt.executeQuery("SELECT COUNT(*) FROM " + tenantView);
                assertTrue(rs.next());
                assertEquals(1, rs.getInt(1));
                stmt.execute("DELETE FROM " + tenantView);
            }
        }
    }

    @Test
    public void testInListExpressionWithRightQueryPlanForTenantViewOnGlobalView() throws Exception {
        String tenantView = generateUniqueName();

        try (Connection viewConn = DriverManager.getConnection(TENANT_SPECIFIC_URL1)) {
            viewConn.setAutoCommit(true);
            try (Statement stmt = viewConn.createStatement()) {
                stmt.execute("CREATE VIEW IF NOT EXISTS " + tenantView + " AS SELECT * FROM " + ascViewName);

                viewConn.createStatement().execute("UPSERT INTO " + tenantView + "(ID1, ID2) VALUES " +
                        "('005xx000001Sv6o', '000000000000300')");
                viewConn.createStatement().execute("UPSERT INTO " + tenantView + "(ID1, ID2) VALUES " +
                        "('005xx000001Sv6o', '000000000000400')");
                viewConn.commit();

                try (PreparedStatement preparedStmt = viewConn.prepareStatement(
                        "SELECT * FROM " + tenantView + " WHERE (ID1, ID2) IN " +
                                "(('005xx000001Sv6o', '000000000000500')," +
                                "('005xx000001Sv6o', '000000000000400')," +
                                "('005xx000001Sv6o', '000000000000300'))")) {
                    QueryPlan queryPlan = PhoenixRuntime.getOptimizedQueryPlan(preparedStmt);
                    assertTrue(queryPlan.getExplainPlan().toString().contains(ExplainTable.POINT_LOOKUP_ON_STRING));
                }

                try (PreparedStatement preparedStmt = viewConn.prepareStatement(
                        "SELECT * FROM " + tenantView + " WHERE (ID2, ID1) IN " +
                                "(('000000000000400', '005xx000001Sv6o')," +
                                "('000000000000300', '005xx000001Sv6o'))")) {
                    QueryPlan queryPlan = PhoenixRuntime.getOptimizedQueryPlan(preparedStmt);
                    assertTrue(queryPlan.getExplainPlan().toString().contains(ExplainTable.POINT_LOOKUP_ON_STRING));
                }
                ResultSet rs = stmt.executeQuery("SELECT COUNT(*) FROM " + tenantView + " WHERE (ID2, ID1) IN " +
                        "(('000000000000400', '005xx000001Sv6o')," +
                        "('000000000000300', '005xx000001Sv6o'))");
                assertTrue(rs.next());

                stmt.execute("DELETE FROM " + tenantView + " WHERE (ID2, ID1) IN " +
                        "(('000000000000300', '005xx000001Sv6o'))");

                rs = stmt.executeQuery("SELECT COUNT(*) FROM " + tenantView);
                assertTrue(rs.next());
                assertEquals(1, rs.getInt(1));
            }
        }
    }

    @Test
    public void testInListExpressionGeneratesRightScanForAsc() throws Exception {
        testFullPkListPlan(this.ascViewName);
        testPartialPkListPlan(this.ascViewName);
        testPartialPkPlusNonPkListPlan(this.ascViewName);
        testNonPkListPlan(this.ascViewName);
    }

    @Test
    public void testInListExpressionGeneratesRightScanForDesc() throws Exception {
        testFullPkListPlan(this.descViewName);
        testPartialPkListPlan(this.descViewName);
        testPartialPkPlusNonPkListPlan(this.descViewName);
        testNonPkListPlan(this.descViewName);
    }

    private void testFullPkListPlan(String tenantView) throws Exception {
        Long numberOfRowsToScan = new Long(2);
        try (Connection viewConn = DriverManager.getConnection(TENANT_SPECIFIC_URL1) ) {
            PreparedStatement preparedStmt = viewConn.prepareStatement("SELECT * FROM " + tenantView + " WHERE (ID1, ID2) IN " +
                    "(('005xx000001Sv6o', '000000000000500')," +
                    "('005xx000001Sv6o', '000000000000400'))");
            QueryPlan queryPlan = PhoenixRuntime.getOptimizedQueryPlan(preparedStmt);
            assertEquals(numberOfRowsToScan, queryPlan.getEstimatedRowsToScan());
            assertTrue(queryPlan.getExplainPlan().toString().contains(ExplainTable.POINT_LOOKUP_ON_STRING));

            viewConn.prepareStatement("DELETE FROM " + tenantView + " WHERE (ID1, ID2) IN " +
                    "(('005xx000001Sv6o', '000000000000500')," +
                    "('005xx000001Sv6o', '000000000000400'))");
            queryPlan = PhoenixRuntime.getOptimizedQueryPlan(preparedStmt);
            assertEquals(numberOfRowsToScan, queryPlan.getEstimatedRowsToScan());
            assertTrue(queryPlan.getExplainPlan().toString().contains(ExplainTable.POINT_LOOKUP_ON_STRING));
        }
    }

    private void testPartialPkListPlan(String tenantView) throws Exception {
        try (Connection viewConn = DriverManager.getConnection(TENANT_SPECIFIC_URL1) ) {
            PreparedStatement preparedStmt = viewConn.prepareStatement("SELECT * FROM " + tenantView + " WHERE (ID1) IN " +
                    "(('005xx000001Sv6o')," +
                    "('005xx000001Sv6o'))");
            QueryPlan queryPlan = PhoenixRuntime.getOptimizedQueryPlan(preparedStmt);
            assertTrue(queryPlan.getExplainPlan().toString().contains("CLIENT PARALLEL 1-WAY RANGE SCAN OVER"));

            viewConn.prepareStatement("DELETE FROM " + tenantView + " WHERE (ID1) IN " +
                    "(('005xx000001Sv6o')," +
                    "('005xx000001Sv6o'))");
            queryPlan = PhoenixRuntime.getOptimizedQueryPlan(preparedStmt);
            assertTrue(queryPlan.getExplainPlan().toString().contains("CLIENT PARALLEL 1-WAY RANGE SCAN OVER"));

            preparedStmt = viewConn.prepareStatement("SELECT * FROM " + tenantView + " WHERE (ID2) IN " +
                    "(('000000000000500')," +
                    "('000000000000400'))");
            queryPlan = PhoenixRuntime.getOptimizedQueryPlan(preparedStmt);
            assertTrue(queryPlan.getExplainPlan().toString().contains("CLIENT PARALLEL 1-WAY RANGE SCAN OVER"));

            viewConn.prepareStatement("DELETE FROM " + tenantView + " WHERE (ID2) IN " +
                    "(('000000000000500')," +
                    "('000000000000400'))");
            queryPlan = PhoenixRuntime.getOptimizedQueryPlan(preparedStmt);
            assertTrue(queryPlan.getExplainPlan().toString().contains("CLIENT PARALLEL 1-WAY RANGE SCAN OVER"));
        }
    }

    private void testPartialPkPlusNonPkListPlan(String tenantView) throws Exception {
        try (Connection viewConn = DriverManager.getConnection(TENANT_SPECIFIC_URL1) ) {
            PreparedStatement preparedStmt = viewConn.prepareStatement("SELECT * FROM " + tenantView + " WHERE (ID1, ID3) IN " +
                    "(('005xx000001Sv6o', 1)," +
                    "('005xx000001Sv6o', 2))");
            QueryPlan queryPlan = PhoenixRuntime.getOptimizedQueryPlan(preparedStmt);
            assertTrue(queryPlan.getExplainPlan().toString().contains("CLIENT PARALLEL 1-WAY RANGE SCAN OVER"));

            viewConn.prepareStatement("DELETE FROM " + tenantView + " WHERE (ID1, ID3) IN " +
                    "(('005xx000001Sv6o', 1)," +
                    "('005xx000001Sv6o', 2))");
            queryPlan = PhoenixRuntime.getOptimizedQueryPlan(preparedStmt);
            assertTrue(queryPlan.getExplainPlan().toString().contains("CLIENT PARALLEL 1-WAY RANGE SCAN OVER"));

            preparedStmt = viewConn.prepareStatement("SELECT * FROM " + tenantView + " WHERE (ID2, ID3) IN " +
                    "(('000000000000500', 1)," +
                    "('000000000000400', 2))");
            queryPlan = PhoenixRuntime.getOptimizedQueryPlan(preparedStmt);
            assertTrue(queryPlan.getExplainPlan().toString().contains("CLIENT PARALLEL 1-WAY RANGE SCAN OVER"));

            viewConn.prepareStatement("DELETE FROM " + tenantView + " WHERE (ID2, ID3) IN " +
                    "(('000000000000500', 1)," +
                    "('000000000000400', 2))");
            queryPlan = PhoenixRuntime.getOptimizedQueryPlan(preparedStmt);
            assertTrue(queryPlan.getExplainPlan().toString().contains("CLIENT PARALLEL 1-WAY RANGE SCAN OVER"));
        }
    }

    private void testNonPkListPlan(String tenantView) throws Exception {
        // Tenant connection should generate a range scan because tenant id is the leading PK.
        try (Connection viewConn = DriverManager.getConnection(TENANT_SPECIFIC_URL1) ) {
            PreparedStatement preparedStmt = viewConn.prepareStatement("SELECT * FROM " + tenantView + " WHERE (ID3, ID4) IN " +
                    "((1, 1)," +
                    "(2, 2))");
            QueryPlan queryPlan = PhoenixRuntime.getOptimizedQueryPlan(preparedStmt);
            assertTrue(queryPlan.getExplainPlan().toString().contains("CLIENT PARALLEL 1-WAY RANGE SCAN OVER"));

            viewConn.prepareStatement("DELETE FROM " + tenantView + " WHERE (ID3, ID4) IN " +
                    "((1, 1)," +
                    "(2, 2))");
            queryPlan = PhoenixRuntime.getOptimizedQueryPlan(preparedStmt);
            assertTrue(queryPlan.getExplainPlan().toString().contains("CLIENT PARALLEL 1-WAY RANGE SCAN OVER"));
        }
    }

    private void deleteTenantData(String tenantView) throws SQLException {
        try (Connection tenantConn = DriverManager.getConnection(TENANT_SPECIFIC_URL1)) {
            tenantConn.createStatement().execute("DELETE FROM " + tenantView);
        }
    }

    @Test
    public void testInListExpressionWithRightQueryPlanForNumericalValue() throws Exception {
        String tenantView = generateUniqueName();

        try (Connection viewConn = DriverManager.getConnection(TENANT_SPECIFIC_URL1)) {
            viewConn.setAutoCommit(true);
            try (Statement stmt = viewConn.createStatement()) {
                stmt.execute("CREATE VIEW IF NOT EXISTS " + tenantView + "(DOUBLE1 DOUBLE NOT NULL, INT1 BIGINT NOT NULL " +
                        " CONSTRAINT PKVIEW PRIMARY KEY\n" + " (DOUBLE1, INT1)) " +
                        " AS SELECT * FROM " + tableName + " WHERE KEY_PREFIX = 'ABC'");

                viewConn.createStatement().execute("UPSERT INTO " + tenantView + "(DOUBLE1, INT1) VALUES " +
                        "(12.0, 8)");
                viewConn.createStatement().execute("UPSERT INTO " + tenantView + "(DOUBLE1, INT1) VALUES " +
                        "(13.0, 9)");
                viewConn.commit();

                ResultSet rs = stmt.executeQuery("SELECT COUNT(*) FROM " + tenantView + " WHERE (INT1, DOUBLE1) IN " +
                        "((8, 12.0)," +
                        "(9, 13.0))");
                assertTrue(rs.next());
                assertEquals(2, rs.getInt(1));

                stmt.execute("DELETE FROM " + tenantView + " WHERE (INT1, DOUBLE1) IN " +
                        "((8, 12.0)," +
                        "(9, 13.0))");

                rs = stmt.executeQuery("SELECT COUNT(*) FROM " + tenantView);
                assertTrue(rs.next());
                assertEquals(0, rs.getInt(1));
            }
        }
    }

    @Test
    public void testQueryPlanForPkDescOrderedTenantViewOnGlobalViewForStringValue() throws Exception {
        String tenantView = generateUniqueName();

        try (Connection viewConn = DriverManager.getConnection(TENANT_SPECIFIC_URL1)) {
            viewConn.setAutoCommit(true);
            try (Statement stmt = viewConn.createStatement()) {
                stmt.execute("CREATE VIEW IF NOT EXISTS " + tenantView + " AS SELECT * FROM " + descViewName);
                viewConn.createStatement().execute("UPSERT INTO " + tenantView + "(ID1, ID2) VALUES " +
                        "('foo', '000000000000300')");
                viewConn.createStatement().execute("UPSERT INTO " + tenantView + "(ID1, ID2) VALUES " +
                        "('bar', '000000000000400')");
                viewConn.createStatement().execute("UPSERT INTO " + tenantView + "(ID1, ID2) VALUES " +
                        "('005xx000001Sv6o', '000000000000500')");
                viewConn.commit();

                ResultSet rs = stmt.executeQuery("SELECT COUNT(*) FROM " + tenantView + " WHERE (ID2, ID1) IN " +
                        "(('000000000000400', 'bar')," +
                        "('000000000000300','foo'))");

                assertTrue(rs.next());
                assertEquals(2, rs.getInt(1));

                stmt.execute("DELETE FROM " + tenantView + " WHERE (ID2, ID1) IN " +
                        "(('000000000000400', 'bar')," +
                        "('000000000000300','foo'))");

                rs = stmt.executeQuery("SELECT ID2 FROM " + tenantView);
                assertTrue(rs.next());
                assertEquals("000000000000500", rs.getString(1));

                rs = stmt.executeQuery("SELECT COUNT(*) FROM " + tenantView);
                assertTrue(rs.next());
                assertEquals(1, rs.getInt(1));
                stmt.execute("DELETE FROM " + tenantView);
            }
        }
    }

    @Test
    public void testQueryPlanForTenantViewOnBaseTableWithVarcharValue() throws Exception {
        String tenantView = generateUniqueName();

        try (Connection viewConn = DriverManager.getConnection(TENANT_SPECIFIC_URL1)) {
            viewConn.setAutoCommit(true);
            try (Statement stmt = viewConn.createStatement()) {
                stmt.execute("CREATE VIEW IF NOT EXISTS " + tenantView + "(DOUBLE1 VARCHAR NOT NULL, INT1 VARCHAR NOT NULL " +
                        " CONSTRAINT PKVIEW PRIMARY KEY\n" + " (DOUBLE1, INT1)) " +
                        " AS SELECT * FROM " + tableName + " WHERE KEY_PREFIX = 'ABC'");

                viewConn.createStatement().execute("UPSERT INTO " + tenantView + "(DOUBLE1, INT1) VALUES " +
                        "('12.0', '8')");
                viewConn.createStatement().execute("UPSERT INTO " + tenantView + "(DOUBLE1, INT1) VALUES " +
                        "('13.0', '9')");
                viewConn.commit();

                ResultSet rs = stmt.executeQuery("SELECT COUNT(*) FROM " + tenantView + " WHERE (INT1, DOUBLE1) IN " +
                        "(('8', '12.0')," +
                        "('9', '13.0'))");
                assertTrue(rs.next());
                assertEquals(2, rs.getInt(1));

                stmt.execute("DELETE FROM " + tenantView + " WHERE (INT1, DOUBLE1) IN " +
                        "(('8', '12.0')," +
                        "('9', '13.0'))");

                rs = stmt.executeQuery("SELECT COUNT(*) FROM " + tenantView);
                assertTrue(rs.next());
                assertEquals(0, rs.getInt(1));
            }
        }
    }

    @Test
    public void testQueryPlanForTenantViewOnBaseTableWithNumericalValue() throws Exception {
        String tenantView = generateUniqueName();

        try (Connection viewConn = DriverManager.getConnection(TENANT_SPECIFIC_URL1)) {
            viewConn.setAutoCommit(true);
            try (Statement stmt = viewConn.createStatement()) {
                stmt.execute("CREATE VIEW IF NOT EXISTS " + tenantView +
                        " (ID1 BIGINT NOT NULL, ID2 BIGINT NOT NULL, ID4 BIGINT " +
                        " CONSTRAINT PKVIEW PRIMARY KEY " + " (ID1, ID2)) " +
                        " AS SELECT * FROM " + tableName + " WHERE KEY_PREFIX = 'ABC'");

                viewConn.createStatement().execute("UPSERT INTO " + tenantView + "(ID1, ID2, ID5, ID4) VALUES " +
                        "(12, 8, 7, 6)");
                viewConn.createStatement().execute("UPSERT INTO " + tenantView + "(ID1, ID2, ID5, ID4) VALUES " +
                        "(13, 9, 13, 9)");
                viewConn.commit();

                ResultSet rs = stmt.executeQuery("SELECT ID1, ID2, ID5, ID4 FROM " + tenantView +
                        " WHERE (ID1, ID5) IN " +
                        "((12, 7)," +
                        "(12, 13))");
                assertTrue(rs.next());
                assertEquals(12, rs.getInt(1));
                assertEquals(8, rs.getInt(2));
                assertEquals(7, rs.getInt(3));
                assertEquals(6, rs.getInt(4));
                assertTrue(!rs.next());

                rs = stmt.executeQuery("SELECT ID1, ID2, ID5, ID4 FROM " + tenantView + " WHERE (ID4, ID1) IN " +
                        "((9, 13)," +
                        "(12, 13))");
                assertTrue(rs.next());
                assertEquals(13, rs.getInt(1));
                assertEquals(9, rs.getInt(2));
                assertEquals(13, rs.getInt(3));
                assertEquals(9, rs.getInt(4));
                assertTrue(!rs.next());

                rs = stmt.executeQuery("SELECT ID1, ID2, ID5, ID4 FROM " + tenantView + " WHERE (ID2, ID1) IN " +
                        "((8, 12)," +
                        "(9, 13))");
                assertTrue(rs.next());
                assertEquals(12, rs.getInt(1));
                assertEquals(8, rs.getInt(2));
                assertEquals(7, rs.getInt(3));
                assertEquals(6, rs.getInt(4));
                assertTrue(rs.next());
                assertEquals(13, rs.getInt(1));
                assertEquals(9, rs.getInt(2));
                assertEquals(13, rs.getInt(3));
                assertEquals(9, rs.getInt(4));
                assertTrue(!rs.next());

                stmt.execute("DELETE FROM " + tenantView + " WHERE (ID2, ID1) IN " +
                        "((8, 12)," +
                        "(9, 13))");

                rs = stmt.executeQuery("SELECT COUNT(*) FROM " + tenantView);
                assertTrue(rs.next());
                assertEquals(0, rs.getInt(1));
            }
        }
    }

    @Test
    public void testInListExpressionWithFunction() throws Exception {
        String tenantView = generateUniqueName();

        try (Connection viewConn = DriverManager.getConnection(TENANT_SPECIFIC_URL1)) {
            viewConn.setAutoCommit(true);
            try (Statement stmt = viewConn.createStatement()) {
                stmt.execute("CREATE VIEW IF NOT EXISTS " + tenantView + "(ID1 BIGINT NOT NULL, ID2 BIGINT NOT NULL, ID4 BIGINT " +
                        " CONSTRAINT PKVIEW PRIMARY KEY\n" + " (ID1, ID2)) " +
                        " AS SELECT * FROM " + tableName + " WHERE KEY_PREFIX = 'ABC'");

                viewConn.createStatement().execute("UPSERT INTO " + tenantView + "(ID1, ID2, ID5, ID4) VALUES " +
                        "(12, 8, 7, 6)");
                viewConn.createStatement().execute("UPSERT INTO " + tenantView + "(ID1, ID2, ID5, ID4) VALUES " +
                        "(13, 9, 13, 9)");
                viewConn.commit();

                ResultSet rs = stmt.executeQuery("SELECT ID1, ID2, ID5, ID4 FROM " + tenantView + " WHERE (ID1 + 1, ID5) IN " +
                        "((13, 7)," +
                        "(13, 13))");
                assertTrue(rs.next());
                assertEquals(12, rs.getInt(1));
                assertEquals(8, rs.getInt(2));
                assertEquals(7, rs.getInt(3));
                assertEquals(6, rs.getInt(4));
                assertTrue(!rs.next());

                rs = stmt.executeQuery("SELECT ID1, ID2, ID5, ID4 FROM " + tenantView + " WHERE (ID4 - 1, ID1) IN " +
                        "((8, 13)," +
                        "(11, 13))");
                assertTrue(rs.next());
                assertEquals(13, rs.getInt(1));
                assertEquals(9, rs.getInt(2));
                assertEquals(13, rs.getInt(3));
                assertEquals(9, rs.getInt(4));
                assertTrue(!rs.next());

                rs = stmt.executeQuery("SELECT ID1, ID2, ID5, ID4 FROM " + tenantView + " WHERE (ID2 + 1 , ID1 - 1) IN " +
                        "((9, 11)," +
                        "(10, 12))");
                assertTrue(rs.next());
                assertEquals(12, rs.getInt(1));
                assertEquals(8, rs.getInt(2));
                assertEquals(7, rs.getInt(3));
                assertEquals(6, rs.getInt(4));
                assertTrue(rs.next());
                assertEquals(13, rs.getInt(1));
                assertEquals(9, rs.getInt(2));
                assertEquals(13, rs.getInt(3));
                assertEquals(9, rs.getInt(4));
                assertTrue(!rs.next());

                stmt.execute("DELETE FROM " + tenantView + " WHERE (ID2 -1, ID1 + 1) IN " +
                        "((7, 13)," +
                        "(8, 14))");

                rs = stmt.executeQuery("SELECT COUNT(*) FROM " + tenantView);
                assertTrue(rs.next());
                assertEquals(0, rs.getInt(1));
            }
        }
    }

    @Test
    public void testInListExpressionWithFunctionAndIndex() throws Exception {
        String tenantView = generateUniqueName();
        String tenantIndexView = generateUniqueName();

        try (Connection viewConn = DriverManager.getConnection(TENANT_SPECIFIC_URL1)) {
            viewConn.setAutoCommit(true);
            try (Statement stmt = viewConn.createStatement()) {
                stmt.execute("CREATE VIEW IF NOT EXISTS " + tenantView + "(ID1 BIGINT NOT NULL, ID2 BIGINT NOT NULL, ID4 BIGINT " +
                        " CONSTRAINT PKVIEW PRIMARY KEY\n" + " (ID1, ID2)) " +
                        " AS SELECT * FROM " + tableName + " WHERE KEY_PREFIX = 'ABC'");

                viewConn.createStatement().execute("UPSERT INTO " + tenantView + "(ID1, ID2, ID5, ID4) VALUES " +
                        "(12, 8, 7, 6)");
                viewConn.createStatement().execute("UPSERT INTO " + tenantView + "(ID1, ID2, ID5, ID4) VALUES " +
                        "(13, 9, 13, 9)");
                viewConn.commit();

                stmt.execute("CREATE INDEX " + tenantIndexView + " ON " + tenantView + " (ID5) INCLUDE (ID4, ID1)");

                ResultSet rs = stmt.executeQuery("SELECT ID1, ID2, ID5, ID4 FROM " + tenantView + " WHERE (ID1 + 1, ID5) IN " +
                        "((13, 7)," +
                        "(13, 13))");
                assertTrue(rs.next());
                assertEquals(12, rs.getInt(1));
                assertEquals(8, rs.getInt(2));
                assertEquals(7, rs.getInt(3));
                assertEquals(6, rs.getInt(4));
                assertTrue(!rs.next());

                rs = stmt.executeQuery("SELECT ID1, ID2, ID5, ID4 FROM " + tenantView + " WHERE (ID5 - 1, ID1) IN " +
                        "((6, 13)," +
                        "(12, 13))");
                assertTrue(rs.next());
                assertEquals(13, rs.getInt(1));
                assertEquals(9, rs.getInt(2));
                assertEquals(13, rs.getInt(3));
                assertEquals(9, rs.getInt(4));
                assertTrue(!rs.next());

                rs = stmt.executeQuery("SELECT ID1, ID2, ID5, ID4 FROM " + tenantView + " WHERE (ID5 + 1 , ID1 - 1) IN " +
                        "((8, 11)," +
                        "(14, 12))");
                assertTrue(rs.next());
                assertEquals(12, rs.getInt(1));
                assertEquals(8, rs.getInt(2));
                assertEquals(7, rs.getInt(3));
                assertEquals(6, rs.getInt(4));
                assertTrue(rs.next());
                assertEquals(13, rs.getInt(1));
                assertEquals(9, rs.getInt(2));
                assertEquals(13, rs.getInt(3));
                assertEquals(9, rs.getInt(4));
                assertTrue(!rs.next());

                stmt.execute("DELETE FROM " + tenantView + " WHERE (ID5 -1, ID1 + 1) IN " +
                        "((6, 13)," +
                        "(12, 14))");

                rs = stmt.executeQuery("SELECT COUNT(*) FROM " + tenantView);
                assertTrue(rs.next());
                assertEquals(0, rs.getInt(1));
            }
        }
    }

    @Test
    public void testInListExpressionWithIndex() throws Exception {
        String tenantView = generateUniqueName();
        String tenantIndexView = generateUniqueName();

        try (Connection viewConn = DriverManager.getConnection(TENANT_SPECIFIC_URL1)) {
            viewConn.setAutoCommit(true);
            try (Statement stmt = viewConn.createStatement()) {
                stmt.execute("CREATE VIEW IF NOT EXISTS " + tenantView + "(ID1 BIGINT NOT NULL, ID2 BIGINT NOT NULL, ID4 BIGINT " +
                        " CONSTRAINT PKVIEW PRIMARY KEY\n" + " (ID1, ID2)) " +
                        " AS SELECT * FROM " + tableName + " WHERE KEY_PREFIX = 'ABC'");

                viewConn.createStatement().execute("UPSERT INTO " + tenantView + "(ID1, ID2, ID5, ID4) VALUES " +
                        "(12, 8, 7, 6)");
                viewConn.createStatement().execute("UPSERT INTO " + tenantView + "(ID1, ID2, ID5, ID4) VALUES " +
                        "(13, 9, 13, 9)");
                viewConn.commit();

                stmt.execute("CREATE INDEX " + tenantIndexView + " ON " + tenantView + " (ID5) INCLUDE (ID4)");

                ResultSet rs = stmt.executeQuery("SELECT ID1, ID2, ID5, ID4 FROM " + tenantView + " WHERE (ID5, ID1) IN " +
                        "((7, 12)," +
                        "(7, 13))");
                assertTrue(rs.next());
                assertEquals(12, rs.getInt(1));
                assertEquals(8, rs.getInt(2));
                assertEquals(7, rs.getInt(3));
                assertEquals(6, rs.getInt(4));
                assertTrue(!rs.next());

                rs = stmt.executeQuery("SELECT ID1, ID2, ID5, ID4 FROM " + tenantView + " WHERE (ID2, ID5) IN " +
                        "((8, 13)," +
                        "(9, 13))");
                assertTrue(rs.next());
                assertEquals(13, rs.getInt(1));
                assertEquals(9, rs.getInt(2));
                assertEquals(13, rs.getInt(3));
                assertEquals(9, rs.getInt(4));
                assertTrue(!rs.next());

                rs = stmt.executeQuery("SELECT ID1, ID2, ID5, ID4 FROM " + tenantView + " WHERE (ID5, ID4) IN " +
                        "((7, 6)," +
                        "(13, 9))");
                assertTrue(rs.next());
                assertEquals(12, rs.getInt(1));
                assertEquals(8, rs.getInt(2));
                assertEquals(7, rs.getInt(3));
                assertEquals(6, rs.getInt(4));
                assertTrue(rs.next());
                assertEquals(13, rs.getInt(1));
                assertEquals(9, rs.getInt(2));
                assertEquals(13, rs.getInt(3));
                assertEquals(9, rs.getInt(4));
                assertTrue(!rs.next());

                stmt.execute("DELETE FROM " + tenantView + " WHERE (ID1, ID5) IN " +
                        "((12, 7)," +
                        "(13, 13))");

                rs = stmt.executeQuery("SELECT COUNT(*) FROM " + tenantView);
                assertTrue(rs.next());
                assertEquals(0, rs.getInt(1));
            }
        }
    }

    @Test
    public void testInListExpressionWithGlobalViewAndFunction() throws Exception {
        String tenantView = generateUniqueName();
        String globalView = generateUniqueName();

        try (Connection conn = DriverManager.getConnection(getUrl())) {
            conn.setAutoCommit(true);
            try (Statement stmt = conn.createStatement()) {
                stmt.execute("CREATE VIEW " + globalView + " AS SELECT * FROM " + tableName + " WHERE KEY_PREFIX = 'ABC'");
            }
        }

        try (Connection viewConn = DriverManager.getConnection(TENANT_SPECIFIC_URL1)) {
            viewConn.setAutoCommit(true);
            try (Statement stmt = viewConn.createStatement()) {
                stmt.execute("CREATE VIEW IF NOT EXISTS " + tenantView + "(ID1 BIGINT NOT NULL, ID2 BIGINT NOT NULL, ID4 BIGINT " +
                        " CONSTRAINT PKVIEW PRIMARY KEY\n" + " (ID1, ID2)) " +
                        " AS SELECT * FROM " + globalView);

                viewConn.createStatement().execute("UPSERT INTO " + tenantView + "(ID1, ID2, ID5, ID4) VALUES " +
                        "(12, 8, 7, 6)");
                viewConn.createStatement().execute("UPSERT INTO " + tenantView + "(ID1, ID2, ID5, ID4) VALUES " +
                        "(13, 9, 13, 9)");
                viewConn.commit();

                ResultSet rs = stmt.executeQuery("SELECT ID1, ID2, ID5, ID4 FROM " + tenantView + " WHERE (ID1 + 1, ID5) IN " +
                        "((13, 7)," +
                        "(13, 13))");
                assertTrue(rs.next());
                assertEquals(12, rs.getInt(1));
                assertEquals(8, rs.getInt(2));
                assertEquals(7, rs.getInt(3));
                assertEquals(6, rs.getInt(4));
                assertTrue(!rs.next());

                rs = stmt.executeQuery("SELECT ID1, ID2, ID5, ID4 FROM " + tenantView + " WHERE (ID4 - 1, ID1) IN " +
                        "((8, 13)," +
                        "(11, 13))");
                assertTrue(rs.next());
                assertEquals(13, rs.getInt(1));
                assertEquals(9, rs.getInt(2));
                assertEquals(13, rs.getInt(3));
                assertEquals(9, rs.getInt(4));
                assertTrue(!rs.next());

                rs = stmt.executeQuery("SELECT ID1, ID2, ID5, ID4 FROM " + tenantView + " WHERE (ID2 + 1 , ID1 - 1) IN " +
                        "((9, 11)," +
                        "(10, 12))");
                assertTrue(rs.next());
                assertEquals(12, rs.getInt(1));
                assertEquals(8, rs.getInt(2));
                assertEquals(7, rs.getInt(3));
                assertEquals(6, rs.getInt(4));
                assertTrue(rs.next());
                assertEquals(13, rs.getInt(1));
                assertEquals(9, rs.getInt(2));
                assertEquals(13, rs.getInt(3));
                assertEquals(9, rs.getInt(4));
                assertTrue(!rs.next());

                stmt.execute("DELETE FROM " + tenantView + " WHERE (ID2 -1, ID1 + 1) IN " +
                        "((7, 13)," +
                        "(8, 14))");

                rs = stmt.executeQuery("SELECT COUNT(*) FROM " + tenantView);
                assertTrue(rs.next());
                assertEquals(0, rs.getInt(1));
            }
        }
    }

    @Test
    public void testGlobalViewWithPowerFunction() throws Exception {
        String tenantView = generateUniqueName();
        String globalView = generateUniqueName();

        try (Connection conn = DriverManager.getConnection(getUrl())) {
            conn.setAutoCommit(true);
            try (Statement stmt = conn.createStatement()) {
                stmt.execute("CREATE VIEW " + globalView + " AS SELECT * FROM " + tableName + " WHERE KEY_PREFIX = 'ABC'");
            }
        }

        try (Connection viewConn = DriverManager.getConnection(TENANT_SPECIFIC_URL1)) {
            viewConn.setAutoCommit(true);
            try (Statement stmt = viewConn.createStatement()) {
                stmt.execute("CREATE VIEW IF NOT EXISTS " + tenantView + "(ID1 DOUBLE NOT NULL, ID2 DOUBLE NOT NULL, ID4 BIGINT " +
                        " CONSTRAINT PKVIEW PRIMARY KEY\n" + " (ID1, ID2)) " +
                        " AS SELECT * FROM " + globalView);

                viewConn.createStatement().execute("UPSERT INTO " + tenantView + "(ID1, ID2, ID5, ID4) VALUES " +
                        "(9, 2, 7, 6)");
                viewConn.createStatement().execute("UPSERT INTO " + tenantView + "(ID1, ID2, ID5, ID4) VALUES " +
                        "(2, 9, 13, 9)");
                viewConn.commit();

                ResultSet rs = stmt.executeQuery("SELECT ID1, ID2, ID5, ID4 FROM " + tenantView + " WHERE (POWER(ID2, 2), ID1) IN " +
                        "((4.0, 9)," +
                        "(10, 12))");
                assertTrue(rs.next());
                assertEquals(9, rs.getInt(1));
                assertEquals(2, rs.getInt(2));
                assertEquals(7, rs.getInt(3));
                assertEquals(6, rs.getInt(4));
                assertTrue(!rs.next());

                stmt.execute("DELETE FROM " + tenantView + " WHERE (POWER(ID1,2), ID2) IN " +
                        "((81, 2)," +
                        "(4, 9))");

                rs = stmt.executeQuery("SELECT COUNT(*) FROM " + tenantView);
                assertTrue(rs.next());
                assertEquals(0, rs.getInt(1));
            }
        }
    }

    @Test
    public void testBaseTableAndIndexTableHaveReversePKOrder() throws Exception {
        String view = generateUniqueName();
        String index = generateUniqueName();

        try (Connection conn = DriverManager.getConnection(TENANT_SPECIFIC_URL1)) {
            conn.setAutoCommit(true);
            try (Statement stmt = conn.createStatement()) {
                stmt.execute("CREATE VIEW " + view + " (ID1 BIGINT NOT NULL, ID2 BIGINT NOT NULL, ID4 BIGINT " +
                        "CONSTRAINT PKVIEW PRIMARY KEY (ID1, ID2)) AS SELECT * FROM " + tableName + " WHERE KEY_PREFIX = 'ABC'");

                conn.createStatement().execute("UPSERT INTO " + view + "(ID1, ID2, ID5, ID4) VALUES " +
                        "(9, 2, 7, 6)");
                conn.createStatement().execute("UPSERT INTO " + view + "(ID1, ID2, ID5, ID4) VALUES " +
                        "(2, 9, 13, 9)");
                conn.commit();

                stmt.execute("CREATE INDEX " + index + " ON " + view + " (ID2, ID1) INCLUDE (ID5, ID4)");

                // TESTING for optimized scan
                PreparedStatement preparedStmt = conn.prepareStatement("SELECT * FROM " + view + " WHERE (ID1, ID2) IN " +
                        "((1, 1)," +
                        "(2, 2))");
                QueryPlan queryPlan = PhoenixRuntime.getOptimizedQueryPlan(preparedStmt);
                assertEquals(new Long(2), queryPlan.getEstimatedRowsToScan());
                assertTrue(queryPlan.getExplainPlan().toString().contains(ExplainTable.POINT_LOOKUP_ON_STRING));

                preparedStmt = conn.prepareStatement("SELECT * FROM " + view + " WHERE (ID2, ID1) IN ((1, 1),(2, 2))");
                queryPlan = PhoenixRuntime.getOptimizedQueryPlan(preparedStmt);
                assertEquals(new Long(2), queryPlan.getEstimatedRowsToScan());
                assertTrue(queryPlan.getExplainPlan().toString().contains(ExplainTable.POINT_LOOKUP_ON_STRING));


                preparedStmt = conn.prepareStatement("SELECT * FROM " + view + " WHERE (ID2, ID1) IN ((1, 1),(2, 2))");
                queryPlan = PhoenixRuntime.getOptimizedQueryPlan(preparedStmt);
                assertEquals(new Long(2), queryPlan.getEstimatedRowsToScan());
                assertTrue(queryPlan.getExplainPlan().toString().contains(ExplainTable.POINT_LOOKUP_ON_STRING));


                ResultSet rs = stmt.executeQuery("SELECT ID1, ID2, ID5, ID4 FROM " + view + " WHERE (POWER(ID2, 2), ID1) IN " +
                        "((4.0, 9)," +
                        "(10, 12))");
                assertTrue(rs.next());
                assertEquals(9, rs.getInt(1));
                assertEquals(2, rs.getInt(2));
                assertEquals(7, rs.getInt(3));
                assertEquals(6, rs.getInt(4));
                assertTrue(!rs.next());

                stmt.execute("DELETE FROM " + view + " WHERE (POWER(ID1,2), ID2) IN " +
                        "((81, 2)," +
                        "(4, 9))");

                rs = stmt.executeQuery("SELECT COUNT(*) FROM " + view);
                assertTrue(rs.next());
                assertEquals(0, rs.getInt(1));
            }
        }
    }

    @Test
    public void testDeletionFromTenantViewAndViewIndex() throws Exception {
        String view = generateUniqueName();
        String index = generateUniqueName();

        try (Connection conn = DriverManager.getConnection(TENANT_SPECIFIC_URL1)) {
            conn.setAutoCommit(true);
            try (Statement stmt = conn.createStatement()) {
                stmt.execute("CREATE VIEW " + view + " (ID1 BIGINT NOT NULL, ID2 BIGINT NOT NULL, ID4 BIGINT " +
                        "CONSTRAINT PKVIEW PRIMARY KEY (ID1, ID2)) AS SELECT * FROM " + tableName + " WHERE KEY_PREFIX = 'ABC'");

                conn.createStatement().execute("UPSERT INTO " + view + "(ID1, ID2, ID5, ID4) VALUES " +
                        "(9, 2, 7, 6)");
                conn.createStatement().execute("UPSERT INTO " + view + "(ID1, ID2, ID5, ID4) VALUES " +
                        "(2, 9, 13, 9)");
                conn.commit();

                stmt.execute("CREATE INDEX " + index + " ON " + view + " (ID4, ID2) INCLUDE (ID1, ID5)");

                // TESTING for optimized scan
                PreparedStatement preparedStmt = conn.prepareStatement("SELECT * FROM " + view + " WHERE (ID4, ID2) IN " +
                        "((1, 1)," +
                        "(2, 2))");
                QueryPlan queryPlan = PhoenixRuntime.getOptimizedQueryPlan(preparedStmt);
                assertTrue(queryPlan.getExplainPlan().toString().contains("RANGE SCAN"));

                preparedStmt = conn.prepareStatement("SELECT ID1,ID5 FROM " + view + " WHERE (ID1, ID2) IN " +
                        "((1, 1),(2, 2))");
                queryPlan = PhoenixRuntime.getOptimizedQueryPlan(preparedStmt);
                assertEquals(new Long(2), queryPlan.getEstimatedRowsToScan());
                assertTrue(queryPlan.getExplainPlan().toString().contains(ExplainTable.POINT_LOOKUP_ON_STRING));


                preparedStmt = conn.prepareStatement("SELECT * FROM " + view + " WHERE (ID2, ID1) IN ((1, 1),(2, 2))");
                queryPlan = PhoenixRuntime.getOptimizedQueryPlan(preparedStmt);
                assertEquals(new Long(2), queryPlan.getEstimatedRowsToScan());
                assertTrue(queryPlan.getExplainPlan().toString().contains(ExplainTable.POINT_LOOKUP_ON_STRING));


                ResultSet rs = stmt.executeQuery("SELECT ID1, ID2, ID5, ID4 FROM " + view + " WHERE (POWER(ID2, 2), ID1) IN " +
                        "((4.0, 9)," +
                        "(10, 12))");
                assertTrue(rs.next());
                assertEquals(9, rs.getInt(1));
                assertEquals(2, rs.getInt(2));
                assertEquals(7, rs.getInt(3));
                assertEquals(6, rs.getInt(4));
                assertTrue(!rs.next());

                stmt.execute("DELETE FROM " + view + " WHERE (POWER(ID1,2), ID2) IN " +
                        "((81, 2)," +
                        "(4, 9))");

                rs = stmt.executeQuery("SELECT COUNT(*) FROM " + view);
                assertTrue(rs.next());
                assertEquals(0, rs.getInt(1));
            }
        }
    }

    @Test
    public void testBaseTableAndIndexTableHaveRightScan() throws Exception {
        String index = generateUniqueName();
        String fullTableName = generateUniqueName();

        try (Connection conn = DriverManager.getConnection(getUrl())) {
            conn.setAutoCommit(true);
            try (Statement stmt = conn.createStatement()) {
                stmt.execute("CREATE TABLE " + fullTableName + "(ID1 BIGINT NOT NULL, ID2 BIGINT NOT NULL, " +
                        "VAL1 BIGINT, VAL2 BIGINT CONSTRAINT PK PRIMARY KEY (ID1,ID2))");
                stmt.execute("CREATE INDEX " + index + " ON " + fullTableName + " (ID2, ID1) INCLUDE (VAL2)");
            }

            PreparedStatement preparedStmt = conn.prepareStatement("SELECT VAL2 FROM " + fullTableName +
                    " WHERE (ID2, ID1) IN ((1, 1),(2, 2))");
            QueryPlan queryPlan = PhoenixRuntime.getOptimizedQueryPlan(preparedStmt);
            queryPlan.getTableRef().getTable().getType();
            assertTrue(queryPlan.getExplainPlan().toString().contains(ExplainTable.POINT_LOOKUP_ON_STRING));
        }
    }
}
