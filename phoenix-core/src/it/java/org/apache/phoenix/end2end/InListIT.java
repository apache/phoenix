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
import static org.apache.phoenix.util.TestUtil.TEST_PROPERTIES;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Properties;

import org.apache.phoenix.schema.types.PInteger;
import org.apache.phoenix.schema.types.PDataType;
import org.apache.phoenix.schema.types.PLong;
import org.apache.phoenix.util.PhoenixRuntime;
import org.apache.phoenix.util.PropertiesUtil;
import org.junit.Test;

import com.google.common.base.Function;
import com.google.common.base.Joiner;
import com.google.common.collect.Lists;


public class InListIT extends BaseHBaseManagedTimeIT {

    @Test
    public void testLeadingPKWithTrailingRVC() throws Exception {
        Properties props = PropertiesUtil.deepCopy(TEST_PROPERTIES);
        Connection conn = DriverManager.getConnection(getUrl(), props);
        conn.createStatement().execute("CREATE TABLE in_test "
                + "( col1 VARCHAR NOT NULL,"
                + "  col2 VARCHAR NOT NULL, "
                + "  id VARCHAR NOT NULL,"
                + "  CONSTRAINT pk PRIMARY KEY (col1, col2, id))");

        conn.createStatement().execute("upsert into in_test (col1, col2, id) values ('a', 'b', 'c')");
        conn.createStatement().execute("upsert into in_test (col1, col2, id) values ('a', 'b', 'd')");
        conn.commit();

        ResultSet rs = conn.createStatement().executeQuery("select id from in_test WHERE col1 = 'a' and ((col2, id) IN (('b', 'c'),('b', 'e')))");
        assertTrue(rs.next());
        assertEquals("c", rs.getString(1));
        assertFalse(rs.next());
        
        conn.close();
    }

    @Test
    public void testLeadingPKWithTrailingRVC2() throws Exception {
        Properties props = PropertiesUtil.deepCopy(TEST_PROPERTIES);
        Connection conn = DriverManager.getConnection(getUrl(), props);
        conn.createStatement().execute("CREATE TABLE in_test ( user VARCHAR, tenant_id VARCHAR(5) NOT NULL,tenant_type_id VARCHAR(3) NOT NULL,  id INTEGER NOT NULL CONSTRAINT pk PRIMARY KEY (tenant_id, tenant_type_id, id))");

        conn.createStatement().execute("upsert into in_test (tenant_id, tenant_type_id, id, user) values ('a', 'a', 1, 'BonA')");
        conn.createStatement().execute("upsert into in_test (tenant_id, tenant_type_id, id, user) values ('a', 'a', 2, 'BonB')");
        conn.commit();

        ResultSet rs = conn.createStatement().executeQuery("select id from in_test WHERE tenant_id = 'a' and tenant_type_id = 'a' and ((id, user) IN ((1, 'BonA'),(1, 'BonB')))");
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
            String tableName = "in_test" + pkType.getSqlTypeName() + saltBuckets + (isMultiTenant ? "_multi" : "_single");
            
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
    
    private static final String TENANT_ID = "ABC";
    private static final String TENANT_URL = getUrl() + ";" + PhoenixRuntime.TENANT_ID_ATTRIB + '=' + TENANT_ID;
    
    // the different combinations to check each test against
    private static final List<Boolean> TENANCIES = Arrays.asList(false, true);
    private static final List<? extends PDataType> INTEGER_TYPES = Arrays.asList(PInteger.INSTANCE, PLong.INSTANCE);
    private static final List<Integer> SALT_BUCKET_NUMBERS = Arrays.asList(0, 4);

    private static final List<String> HINTS = Arrays.asList("", "/*+ SKIP_SCAN */", "/*+ RANGE_SCAN */");
    
    /**
     * Tests the given where clause against the given upserts by comparing against the list of
     * expected result strings.
     * @param upsertBodies  list of upsert bodies with the form "(pk1, pk2, ..., nonPk) VALUES (1, 7, ..., "row1")
     *                      excludes the "UPSERT INTO table_name " segment so that table name can vary
     * @param whereClause  the where clause to test. Should only refer to the pks upserted.
     * @param expecteds  a complete list of all of the expected result row names
     */
    private void testWithIntegerTypesWithVariedSaltingAndTenancy(List<String> upsertBodies, String whereClause, List<String> expecteds) throws SQLException {
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
                        String tableName = initializeAndGetTable(baseConn, conn, isMultiTenant, pkType, saltBuckets);

                        // upsert the given data 
                        for(String upsertBody : upsertBodies) {
                            conn.createStatement().execute("UPSERT INTO " + tableName + " " + upsertBody);
                        }
                        conn.commit();

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
        
        testWithIntegerTypesWithVariedSaltingAndTenancy(DEFAULT_UPSERT_BODIES, whereClause, expecteds);
    }
    
    @Test
    public void testPlainRVCFullyQualified() throws Exception {
        String whereClause = "WHERE (pk1, pk2, pk3, pk4, pk5) IN ((1, 2, 3, 4, 5), (1, 2, 4, 5, 6))";
        List<String> expecteds = singletonList("row1");
        
        testWithIntegerTypesWithVariedSaltingAndTenancy(DEFAULT_UPSERT_BODIES, whereClause, expecteds);
    }
    
    @Test
    public void testPlainRVCPartiallyQualifiedBegin() throws Exception {
        String whereClause = "WHERE (pk1, pk2, pk3, pk4) IN ((2, 3, 4, 5), (1, 2, 4, 5))";
        List<String> expecteds = Arrays.asList("row1", "row2");
        
        testWithIntegerTypesWithVariedSaltingAndTenancy(DEFAULT_UPSERT_BODIES, whereClause, expecteds);
    }
    
    @Test
    public void testPlainRVCPartiallyQualifiedEnd() throws Exception {
        String whereClause = "WHERE (pk2, pk3, pk4, pk5) IN ((2, 3, 4, 5), (2, 4, 5, 6))";
        List<String> expecteds = singletonList("row1");
        
        testWithIntegerTypesWithVariedSaltingAndTenancy(DEFAULT_UPSERT_BODIES, whereClause, expecteds);
    }
    
    @Test
    public void testPlainRVCSlotHole() throws Exception {
        String whereClause = "WHERE (pk1, pk2, pk4, pk5) IN ((1, 2, 4, 5), (6, 5, 3, 2))";
        List<String> expecteds = singletonList("row4");
        
        testWithIntegerTypesWithVariedSaltingAndTenancy(DEFAULT_UPSERT_BODIES, whereClause, expecteds);
    }
    
    @Test
    public void testLeadingPKWithTrailingRVCNoResults() throws Exception {
        String whereClause = "WHERE pk1 != 2 AND (pk3, pk4, pk5) IN ((6, 4, 5), (5, 6, 4))";
        List<String> expecteds = Collections.<String>emptyList();
        
        testWithIntegerTypesWithVariedSaltingAndTenancy(DEFAULT_UPSERT_BODIES, whereClause, expecteds);
    }
    
    @Test
    public void testLeadingPKWithTrailingRVCFullyQualified() throws Exception {
        String whereClause = "WHERE pk1 = 2 AND (pk2, pk3, pk4, pk5) IN ((2, 4, 5, 6), (3, 4, 5, 6))";
        List<String> expecteds = singletonList("row2");
        
        testWithIntegerTypesWithVariedSaltingAndTenancy(DEFAULT_UPSERT_BODIES, whereClause, expecteds);
    }
    
    @Test
    public void testLeadingPKWithTrailingRVCPartiallyQualifiedBegin() throws Exception {
        String whereClause = "WHERE pk1 = 2 AND (pk2, pk3) IN ((3, 6), (5, 4))";
        List<String> expecteds = singletonList("row3");
        
        testWithIntegerTypesWithVariedSaltingAndTenancy(DEFAULT_UPSERT_BODIES, whereClause, expecteds);
    }
    
    @Test
    public void testLeadingPKWithTrailingRVCPartiallyQualifiedEnd() throws Exception {
        String whereClause = "WHERE pk2 = 2 AND (pk3, pk4, pk5) IN ((4, 5, 6), (5, 6, 4))";
        List<String> expecteds = singletonList("row1");
        
        testWithIntegerTypesWithVariedSaltingAndTenancy(DEFAULT_UPSERT_BODIES, whereClause, expecteds);
    }
    
    @Test
    public void testLeadingPKWithTrailingRVCSlotHole() throws Exception {
        String whereClause = "WHERE pk1 = 2 AND (pk3, pk4, pk5) IN ((4, 5, 6), (5, 6, 4))";
        List<String> expecteds = singletonList("row2");
        
        testWithIntegerTypesWithVariedSaltingAndTenancy(DEFAULT_UPSERT_BODIES, whereClause, expecteds);
    }
    
    @Test
    public void testLeadingRVCWithTrailingPKNoResults() throws Exception {
        String whereClause = "WHERE (pk1, pk2, pk3) IN ((2, 3, 4), (2, 3, 6)) AND pk4 = 3";
        List<String> expecteds = Collections.<String>emptyList();
        
        testWithIntegerTypesWithVariedSaltingAndTenancy(DEFAULT_UPSERT_BODIES, whereClause, expecteds);
    }
    
    @Test
    public void testLeadingRVCWithTrailingPKFullyQualified() throws Exception {
        String whereClause = "WHERE (pk1, pk2, pk3, pk4) IN ((1, 2, 4, 5), (2, 3, 4, 5)) AND pk5 = 6";
        List<String> expecteds = Arrays.asList("row1", "row2");
        
        testWithIntegerTypesWithVariedSaltingAndTenancy(DEFAULT_UPSERT_BODIES, whereClause, expecteds);
    }
    
    @Test
    public void testLeadingRVCWithTrailingPKPartiallyQualifiedBegin() throws Exception {
        String whereClause = "WHERE (pk1, pk2, pk3) IN ((2, 3, 4), (2, 3, 6)) AND pk4 = 4";
        List<String> expecteds = singletonList("row3");
        
        testWithIntegerTypesWithVariedSaltingAndTenancy(DEFAULT_UPSERT_BODIES, whereClause, expecteds);
    }
    
    @Test
    public void testLeadingRVCWithTrailingPKPartiallyQualifiedEnd() throws Exception {
        String whereClause = "WHERE (pk2, pk3, pk4) IN ((3, 4, 5), (3, 6, 4)) AND pk5 = 5";
        List<String> expecteds = singletonList("row3");
        
        testWithIntegerTypesWithVariedSaltingAndTenancy(DEFAULT_UPSERT_BODIES, whereClause, expecteds);
    }
    
    @Test
    public void testLeadingRVCWithTrailingPKSlotHole() throws Exception {
        String whereClause = "WHERE (pk1, pk2, pk3) IN ((2, 3, 4), (2, 3, 6)) AND pk5 = 5";
        List<String> expecteds = singletonList("row3");
        
        testWithIntegerTypesWithVariedSaltingAndTenancy(DEFAULT_UPSERT_BODIES, whereClause, expecteds);
    }
    
    @Test
    public void testOverlappingRVCAndPKNoResults() throws Exception {
        String whereClause = "WHERE (pk1, pk2) IN ((1, 2), (2, 3)) AND pk2 = 4";
        List<String> expecteds = Collections.<String>emptyList();
        
        testWithIntegerTypesWithVariedSaltingAndTenancy(DEFAULT_UPSERT_BODIES, whereClause, expecteds);
    }
    
    @Test
    public void testOverlappingRVCAndPKFullyQualified() throws Exception {
        String whereClause = "WHERE (pk1, pk2, pk3, pk4, pk5) IN ((1, 2, 4, 5, 6), (2, 3, 4, 5, 6)) AND pk1 = 2";
        List<String> expecteds = singletonList("row2");
        
        testWithIntegerTypesWithVariedSaltingAndTenancy(DEFAULT_UPSERT_BODIES, whereClause, expecteds);
    }
    
    @Test
    public void testOverlappingRVCAndPKPartiallyQualifiedBegin() throws Exception {
        String whereClause = "WHERE (pk1, pk2, pk3) IN ((1, 2, 4), (2, 3, 6)) AND pk3 = 4";
        List<String> expecteds = singletonList("row1");
        
        testWithIntegerTypesWithVariedSaltingAndTenancy(DEFAULT_UPSERT_BODIES, whereClause, expecteds);
    }
    
    @Test
    public void testOverlappingRVCAndPKPartiallyQualifiedEnd() throws Exception {
        String whereClause = "WHERE (pk3, pk4, pk5) IN ((4, 5, 6), (4, 3, 2)) AND pk5 = 2";
        List<String> expecteds = singletonList("row4");
        
        testWithIntegerTypesWithVariedSaltingAndTenancy(DEFAULT_UPSERT_BODIES, whereClause, expecteds);
    }
    
    @Test
    public void testOverlappingRVCAndRVCNoResults() throws Exception {
        String whereClause = "WHERE (pk1, pk2) IN ((1, 2), (2, 3)) AND (pk2, pk3) IN ((4, 4), (4, 6))";
        List<String> expecteds = Collections.<String>emptyList();
        
        testWithIntegerTypesWithVariedSaltingAndTenancy(DEFAULT_UPSERT_BODIES, whereClause, expecteds);
    }
    
    @Test
    public void testOverlappingRVCAndRVCFullyQualified() throws Exception {
        String whereClause = "WHERE (pk1, pk2, pk3) IN ((2, 3, 6), (2, 3, 4)) AND (pk3, pk4, pk5) IN ((4, 5, 6), (4, 3, 2))";
        List<String> expecteds = singletonList("row2");
        
        testWithIntegerTypesWithVariedSaltingAndTenancy(DEFAULT_UPSERT_BODIES, whereClause, expecteds);
    }
    
    @Test
    public void testOverlappingRVCAndRVCPartiallyQualifiedBegin() throws Exception {
        String whereClause = "WHERE (pk1, pk2) IN ((1, 2), (2, 3)) AND (pk2, pk3) IN ((3, 4), (3, 6))";
        List<String> expecteds = Arrays.asList("row2", "row3");
        
        testWithIntegerTypesWithVariedSaltingAndTenancy(DEFAULT_UPSERT_BODIES, whereClause, expecteds);
    }
    
    @Test
    public void testOverlappingRVCAndRVCPartiallyQualifiedEnd() throws Exception {
        String whereClause = "WHERE (pk3, pk4) IN ((4, 5), (4, 3)) AND (pk4, pk5) IN ((3, 2), (4, 5))";
        List<String> expecteds = singletonList("row4");
        
        testWithIntegerTypesWithVariedSaltingAndTenancy(DEFAULT_UPSERT_BODIES, whereClause, expecteds);
    }
}
