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
package org.apache.phoenix.end2end.index;

import static org.apache.phoenix.util.TestUtil.TEST_PROPERTIES;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import java.math.BigDecimal;
import java.sql.Connection;
import java.sql.Date;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.Map;
import java.util.Properties;

import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.phoenix.compile.ColumnResolver;
import org.apache.phoenix.compile.FromCompiler;
import org.apache.phoenix.end2end.BaseHBaseManagedTimeIT;
import org.apache.phoenix.end2end.Shadower;
import org.apache.phoenix.jdbc.PhoenixConnection;
import org.apache.phoenix.parse.NamedTableNode;
import org.apache.phoenix.parse.TableName;
import org.apache.phoenix.query.QueryServices;
import org.apache.phoenix.schema.PTable;
import org.apache.phoenix.util.MetaDataUtil;
import org.apache.phoenix.util.PropertiesUtil;
import org.apache.phoenix.util.QueryUtil;
import org.apache.phoenix.util.ReadOnlyProps;
import org.apache.phoenix.util.TestUtil;
import org.junit.BeforeClass;
import org.junit.Test;

import com.google.common.collect.Maps;
import com.google.common.primitives.Doubles;


public abstract class BaseMutableIndexIT extends BaseHBaseManagedTimeIT {
    
    @BeforeClass
    @Shadower(classBeingShadowed = BaseHBaseManagedTimeIT.class)
    public static void doSetup() throws Exception {
        Map<String,String> props = Maps.newHashMapWithExpectedSize(3);
        // Don't split intra region so we can more easily know that the n-way parallelization is for the explain plan
        // Forces server cache to be used
        props.put(QueryServices.INDEX_MUTATE_BATCH_SIZE_THRESHOLD_ATTRIB, Integer.toString(2));
        props.put(QueryServices.DROP_METADATA_ATTRIB, Boolean.toString(true));
        setUpTestDriver(new ReadOnlyProps(props.entrySet().iterator()));
    }
    
    private final boolean localIndex;
    
    BaseMutableIndexIT(boolean localIndex) {
        this.localIndex = localIndex;
    }
    
    @Test
    public void createIndexOnTableWithSpecifiedDefaultCF() throws Exception {
        String query;
        ResultSet rs;

        Properties props = PropertiesUtil.deepCopy(TEST_PROPERTIES);
        Connection conn = DriverManager.getConnection(getUrl(), props);
        conn.createStatement().execute(
                "CREATE TABLE " + TestUtil.DEFAULT_DATA_TABLE_FULL_NAME + " (k VARCHAR NOT NULL PRIMARY KEY, v1 VARCHAR, v2 VARCHAR) DEFAULT_COLUMN_FAMILY='A'");
        query = "SELECT * FROM " + TestUtil.DEFAULT_DATA_TABLE_FULL_NAME;
        rs = conn.createStatement().executeQuery(query);
        assertFalse(rs.next());

        String options = localIndex ? "SALT_BUCKETS=10, MULTI_TENANT=true, IMMUTABLE_ROWS=true, DISABLE_WAL=true" : "";
        conn.createStatement().execute(
                "CREATE INDEX " + TestUtil.DEFAULT_INDEX_TABLE_NAME + " ON " + TestUtil.DEFAULT_DATA_TABLE_FULL_NAME + " (v1) INCLUDE (v2) " + options);
        query = "SELECT * FROM " + TestUtil.DEFAULT_INDEX_TABLE_FULL_NAME;
        rs = conn.createStatement().executeQuery(query);
        assertFalse(rs.next());
        
        //check options set correctly on index
        TableName indexName = TableName.create(TestUtil.DEFAULT_SCHEMA_NAME, TestUtil.DEFAULT_INDEX_TABLE_NAME);
        NamedTableNode indexNode = NamedTableNode.create(null, indexName, null);
        ColumnResolver resolver = FromCompiler.getResolver(indexNode, conn.unwrap(PhoenixConnection.class));
        PTable indexTable = resolver.getTables().get(0).getTable();
        // Can't set IMMUTABLE_ROWS, MULTI_TENANT or DEFAULT_COLUMN_FAMILY_NAME on an index
        assertNull(indexTable.getDefaultFamilyName());
        assertFalse(indexTable.isMultiTenant());
        assertFalse(indexTable.isImmutableRows());
        if(localIndex) {
            assertEquals(10, indexTable.getBucketNum().intValue());
            assertTrue(indexTable.isWALDisabled());
        }
    }

    @Test
    public void testIndexWithNullableFixedWithCols() throws Exception {
        Properties props = PropertiesUtil.deepCopy(TEST_PROPERTIES);
        Connection conn = DriverManager.getConnection(getUrl(), props);
        conn.setAutoCommit(false);
        try {
            createMultiCFTestTable(TestUtil.DEFAULT_DATA_TABLE_FULL_NAME);
            populateMultiCFTestTable(TestUtil.DEFAULT_DATA_TABLE_FULL_NAME);
            String ddl = null;
            if(localIndex){
                ddl = "CREATE LOCAL INDEX " + TestUtil.DEFAULT_INDEX_TABLE_NAME + " ON " + TestUtil.DEFAULT_DATA_TABLE_FULL_NAME
                        + " (char_col1 ASC, int_col1 ASC)"
                        + " INCLUDE (long_col1, long_col2)";
            } else {
                ddl = "CREATE INDEX " + TestUtil.DEFAULT_INDEX_TABLE_NAME + " ON " + TestUtil.DEFAULT_DATA_TABLE_FULL_NAME
                        + " (char_col1 ASC, int_col1 ASC)"
                        + " INCLUDE (long_col1, long_col2)";
            }
            PreparedStatement stmt = conn.prepareStatement(ddl);
            stmt.execute();
            
            String query = "SELECT d.char_col1, int_col1 from " + TestUtil.DEFAULT_DATA_TABLE_FULL_NAME + " as d";
            ResultSet rs = conn.createStatement().executeQuery("EXPLAIN " + query);
            if (localIndex) {
                assertEquals("CLIENT PARALLEL 1-WAY RANGE SCAN OVER " + MetaDataUtil.getLocalIndexTableName(TestUtil.DEFAULT_DATA_TABLE_FULL_NAME)+" [-32768]\n"
                           + "    SERVER FILTER BY FIRST KEY ONLY\n"
                           + "CLIENT MERGE SORT", QueryUtil.getExplainPlan(rs));
            } else {
                assertEquals("CLIENT PARALLEL 1-WAY FULL SCAN OVER " + TestUtil.DEFAULT_INDEX_TABLE_FULL_NAME + "\n" +
                             "    SERVER FILTER BY FIRST KEY ONLY", QueryUtil.getExplainPlan(rs));
            }
            
            rs = conn.createStatement().executeQuery(query);
            assertTrue(rs.next());
            assertEquals("chara", rs.getString(1));
            assertEquals("chara", rs.getString("char_col1"));
            assertEquals(2, rs.getInt(2));
            assertTrue(rs.next());
            assertEquals("chara", rs.getString(1));
            assertEquals(3, rs.getInt(2));
            assertTrue(rs.next());
            assertEquals("chara", rs.getString(1));
            assertEquals(4, rs.getInt(2));
            assertFalse(rs.next());
        } finally {
            conn.close();
        }
    }

    @Test
    public void testIndexWithNullableDateCol() throws Exception {
        Properties props = PropertiesUtil.deepCopy(TEST_PROPERTIES);
        Connection conn = DriverManager.getConnection(getUrl(), props);
        conn.setAutoCommit(false);
        try {
            Date date = new Date(System.currentTimeMillis());
            
            createMultiCFTestTable(TestUtil.DEFAULT_DATA_TABLE_FULL_NAME);
            populateMultiCFTestTable(TestUtil.DEFAULT_DATA_TABLE_FULL_NAME, date);
            String ddl = null;
            if (localIndex) {
                ddl = "CREATE LOCAL INDEX " + TestUtil.DEFAULT_INDEX_TABLE_NAME + " ON " + TestUtil.DEFAULT_DATA_TABLE_FULL_NAME + " (date_col)";
            } else {
                ddl = "CREATE INDEX " + TestUtil.DEFAULT_INDEX_TABLE_NAME + " ON " + TestUtil.DEFAULT_DATA_TABLE_FULL_NAME + " (date_col)";
            }
            PreparedStatement stmt = conn.prepareStatement(ddl);
            stmt.execute();
            
            String query = "SELECT int_pk from " + TestUtil.DEFAULT_DATA_TABLE_FULL_NAME ;
            ResultSet rs = conn.createStatement().executeQuery("EXPLAIN " + query);
            if (localIndex) {
                assertEquals("CLIENT PARALLEL 1-WAY RANGE SCAN OVER _LOCAL_IDX_" + TestUtil.DEFAULT_DATA_TABLE_FULL_NAME +" [-32768]\n"
                           + "    SERVER FILTER BY FIRST KEY ONLY\n"
                           + "CLIENT MERGE SORT", QueryUtil.getExplainPlan(rs));
            } else {
                assertEquals("CLIENT PARALLEL 1-WAY FULL SCAN OVER " + TestUtil.DEFAULT_INDEX_TABLE_FULL_NAME + "\n"
                        + "    SERVER FILTER BY FIRST KEY ONLY", QueryUtil.getExplainPlan(rs));
            }
            
            rs = conn.createStatement().executeQuery(query);
            assertTrue(rs.next());
            assertEquals(2, rs.getInt(1));
            assertTrue(rs.next());
            assertEquals(1, rs.getInt(1));
            assertTrue(rs.next());
            assertEquals(3, rs.getInt(1));
            assertFalse(rs.next());
            
            query = "SELECT date_col from " + TestUtil.DEFAULT_DATA_TABLE_FULL_NAME + " order by date_col" ;
            rs = conn.createStatement().executeQuery("EXPLAIN " + query);
            if (localIndex) {
                assertEquals("CLIENT PARALLEL 1-WAY RANGE SCAN OVER _LOCAL_IDX_" + TestUtil.DEFAULT_DATA_TABLE_FULL_NAME + " [-32768]\n"
                        + "    SERVER FILTER BY FIRST KEY ONLY\n"
                        + "CLIENT MERGE SORT", QueryUtil.getExplainPlan(rs));
            } else {
                assertEquals("CLIENT PARALLEL 1-WAY FULL SCAN OVER " + TestUtil.DEFAULT_INDEX_TABLE_FULL_NAME + "\n"
                        + "    SERVER FILTER BY FIRST KEY ONLY", QueryUtil.getExplainPlan(rs));
            }
            
            rs = conn.createStatement().executeQuery(query);
            assertTrue(rs.next());
            assertEquals(date, rs.getDate(1));
            assertTrue(rs.next());
            assertEquals(new Date(date.getTime() + TestUtil.MILLIS_IN_DAY), rs.getDate(1));
            assertTrue(rs.next());
            assertEquals(new Date(date.getTime() + 2 * TestUtil.MILLIS_IN_DAY), rs.getDate(1));
            assertFalse(rs.next());
        } finally {
            conn.close();
        }
    }

    @Test
    public void testCoveredColumnUpdates() throws Exception {
        Properties props = PropertiesUtil.deepCopy(TEST_PROPERTIES);
        Connection conn = DriverManager.getConnection(getUrl(), props);
        conn.setAutoCommit(false);
        try {
            createMultiCFTestTable(TestUtil.DEFAULT_DATA_TABLE_FULL_NAME);
            populateMultiCFTestTable(TestUtil.DEFAULT_DATA_TABLE_FULL_NAME);
            String ddl = null;
            if(localIndex) {
                ddl = "CREATE LOCAL INDEX " + TestUtil.DEFAULT_INDEX_TABLE_NAME + " ON " + TestUtil.DEFAULT_DATA_TABLE_FULL_NAME
                        + " (char_col1 ASC, int_col1 ASC)"
                        + " INCLUDE (long_col1, long_col2)";
            } else {
                ddl = "CREATE INDEX " + TestUtil.DEFAULT_INDEX_TABLE_NAME + " ON " + TestUtil.DEFAULT_DATA_TABLE_FULL_NAME
                        + " (char_col1 ASC, int_col1 ASC)"
                        + " INCLUDE (long_col1, long_col2)";
            }

            PreparedStatement stmt = conn.prepareStatement(ddl);
            stmt.execute();
            
            String query = "SELECT char_col1, int_col1, long_col2 from " + TestUtil.DEFAULT_DATA_TABLE_FULL_NAME;
            ResultSet rs = conn.createStatement().executeQuery("EXPLAIN " + query);
            if (localIndex) {
                assertEquals("CLIENT PARALLEL 1-WAY RANGE SCAN OVER _LOCAL_IDX_" + TestUtil.DEFAULT_DATA_TABLE_FULL_NAME +" [-32768]\nCLIENT MERGE SORT", QueryUtil.getExplainPlan(rs));
            } else {
                assertEquals("CLIENT PARALLEL 1-WAY FULL SCAN OVER " + TestUtil.DEFAULT_INDEX_TABLE_FULL_NAME, QueryUtil.getExplainPlan(rs));
            }
            
            rs = conn.createStatement().executeQuery(query);
            assertTrue(rs.next());
            assertEquals("chara", rs.getString(1));
            assertEquals(2, rs.getInt(2));
            assertEquals(3L, rs.getLong(3));
            assertTrue(rs.next());
            assertEquals("chara", rs.getString(1));
            assertEquals(3, rs.getInt(2));
            assertEquals(4L, rs.getLong(3));
            assertTrue(rs.next());
            assertEquals("chara", rs.getString(1));
            assertEquals(4, rs.getInt(2));
            assertEquals(5L, rs.getLong(3));
            assertFalse(rs.next());
            
            stmt = conn.prepareStatement("UPSERT INTO " + TestUtil.DEFAULT_DATA_TABLE_FULL_NAME
                    + "(varchar_pk, char_pk, int_pk, long_pk , decimal_pk, long_col2) SELECT varchar_pk, char_pk, int_pk, long_pk , decimal_pk, long_col2*2 FROM "
                    + TestUtil.DEFAULT_DATA_TABLE_FULL_NAME + " WHERE long_col2=?");
            stmt.setLong(1,4L);
            assertEquals(1,stmt.executeUpdate());
            conn.commit();

            rs = conn.createStatement().executeQuery(query);
            assertTrue(rs.next());
            assertEquals("chara", rs.getString(1));
            assertEquals(2, rs.getInt(2));
            assertEquals(3L, rs.getLong(3));
            assertTrue(rs.next());
            assertEquals("chara", rs.getString(1));
            assertEquals(3, rs.getInt(2));
            assertEquals(8L, rs.getLong(3));
            assertTrue(rs.next());
            assertEquals("chara", rs.getString(1));
            assertEquals(4, rs.getInt(2));
            assertEquals(5L, rs.getLong(3));
            assertFalse(rs.next());
            
            stmt = conn.prepareStatement("UPSERT INTO " + TestUtil.DEFAULT_DATA_TABLE_FULL_NAME
                    + "(varchar_pk, char_pk, int_pk, long_pk , decimal_pk, long_col2) SELECT varchar_pk, char_pk, int_pk, long_pk , decimal_pk, null FROM "
                    + TestUtil.DEFAULT_DATA_TABLE_FULL_NAME + " WHERE long_col2=?");
            stmt.setLong(1,3L);
            assertEquals(1,stmt.executeUpdate());
            conn.commit();
            
            rs = conn.createStatement().executeQuery(query);
            assertTrue(rs.next());
            assertEquals("chara", rs.getString(1));
            assertEquals(2, rs.getInt(2));
            assertEquals(0, rs.getLong(3));
            assertTrue(rs.wasNull());
            assertTrue(rs.next());
            assertEquals("chara", rs.getString(1));
            assertEquals(3, rs.getInt(2));
            assertEquals(8L, rs.getLong(3));
            assertTrue(rs.next());
            assertEquals("chara", rs.getString(1));
            assertEquals(4, rs.getInt(2));
            assertEquals(5L, rs.getLong(3));
            assertFalse(rs.next());
            if(localIndex) {
                query = "SELECT b.* from " + TestUtil.DEFAULT_DATA_TABLE_FULL_NAME + " where int_col1 = 4";
                rs = conn.createStatement().executeQuery("EXPLAIN " + query);
                assertEquals("CLIENT PARALLEL 1-WAY RANGE SCAN OVER _LOCAL_IDX_" + TestUtil.DEFAULT_DATA_TABLE_FULL_NAME +" [-32768]\n" +
                		"    SERVER FILTER BY TO_INTEGER(\"INT_COL1\") = 4\nCLIENT MERGE SORT", QueryUtil.getExplainPlan(rs));
                rs = conn.createStatement().executeQuery(query);
                assertTrue(rs.next());
                assertEquals("varchar_b", rs.getString(1));
                assertEquals("charb", rs.getString(2));
                assertEquals(5, rs.getInt(3));
                assertEquals(5, rs.getLong(4));
                assertFalse(rs.next());
                
            }
        } finally {
            conn.close();
        }
    }
    
    @Test
    public void testSelectAllAndAliasWithIndex() throws Exception {
        String query;
        ResultSet rs;
        
        Properties props = PropertiesUtil.deepCopy(TEST_PROPERTIES);
        Connection conn = DriverManager.getConnection(getUrl(), props);
        conn.setAutoCommit(false);
        conn.createStatement().execute("CREATE TABLE " + TestUtil.DEFAULT_DATA_TABLE_FULL_NAME + " (k VARCHAR NOT NULL PRIMARY KEY, v1 VARCHAR, v2 VARCHAR)");
        query = "SELECT * FROM " + TestUtil.DEFAULT_DATA_TABLE_FULL_NAME;
        rs = conn.createStatement().executeQuery(query);
        assertFalse(rs.next());
        
        if (localIndex) {
            conn.createStatement().execute("CREATE LOCAL INDEX " + TestUtil.DEFAULT_INDEX_TABLE_NAME + " ON " + TestUtil.DEFAULT_DATA_TABLE_FULL_NAME + " (v2 DESC) INCLUDE (v1)");
        } else {
            conn.createStatement().execute("CREATE INDEX " + TestUtil.DEFAULT_INDEX_TABLE_NAME + " ON " + TestUtil.DEFAULT_DATA_TABLE_FULL_NAME + " (v2 DESC) INCLUDE (v1)");
        }
        query = "SELECT * FROM " + TestUtil.DEFAULT_INDEX_TABLE_FULL_NAME;
        rs = conn.createStatement().executeQuery(query);
        assertFalse(rs.next());

        PreparedStatement stmt = conn.prepareStatement("UPSERT INTO " + TestUtil.DEFAULT_DATA_TABLE_FULL_NAME + " VALUES(?,?,?)");
        stmt.setString(1,"a");
        stmt.setString(2, "x");
        stmt.setString(3, "1");
        stmt.execute();
        stmt.setString(1,"b");
        stmt.setString(2, "y");
        stmt.setString(3, "2");
        stmt.execute();
        conn.commit();
        
        query = "SELECT * FROM " + TestUtil.DEFAULT_DATA_TABLE_FULL_NAME;
        rs = conn.createStatement().executeQuery("EXPLAIN " + query);
        if(localIndex){
            assertEquals("CLIENT PARALLEL 1-WAY RANGE SCAN OVER _LOCAL_IDX_" + TestUtil.DEFAULT_DATA_TABLE_FULL_NAME+" [-32768]\nCLIENT MERGE SORT", QueryUtil.getExplainPlan(rs));
        } else {
            assertEquals("CLIENT PARALLEL 1-WAY FULL SCAN OVER " + TestUtil.DEFAULT_INDEX_TABLE_FULL_NAME, QueryUtil.getExplainPlan(rs));
        }

        rs = conn.createStatement().executeQuery(query);
        assertTrue(rs.next());
        assertEquals("b",rs.getString(1));
        assertEquals("y",rs.getString(2));
        assertEquals("2",rs.getString(3));
        assertEquals("b",rs.getString("k"));
        assertEquals("y",rs.getString("v1"));
        assertEquals("2",rs.getString("v2"));
        assertTrue(rs.next());
        assertEquals("a",rs.getString(1));
        assertEquals("x",rs.getString(2));
        assertEquals("1",rs.getString(3));
        assertEquals("a",rs.getString("k"));
        assertEquals("x",rs.getString("v1"));
        assertEquals("1",rs.getString("v2"));
        assertFalse(rs.next());
        
        query = "SELECT v1 as foo FROM " + TestUtil.DEFAULT_DATA_TABLE_FULL_NAME + " WHERE v2 = '1' ORDER BY foo";
        rs = conn.createStatement().executeQuery("EXPLAIN " + query);
        if(localIndex){
            assertEquals("CLIENT PARALLEL 1-WAY RANGE SCAN OVER _LOCAL_IDX_" +TestUtil.DEFAULT_DATA_TABLE_FULL_NAME + " [-32768,~'1']\n" + 
                    "    SERVER SORTED BY [\"V1\"]\n" + 
                    "CLIENT MERGE SORT", QueryUtil.getExplainPlan(rs));
        } else {
            assertEquals("CLIENT PARALLEL 1-WAY RANGE SCAN OVER " +TestUtil.DEFAULT_INDEX_TABLE_FULL_NAME + " [~'1']\n" + 
                    "    SERVER SORTED BY [\"V1\"]\n" + 
                    "CLIENT MERGE SORT", QueryUtil.getExplainPlan(rs));
        }

        rs = conn.createStatement().executeQuery(query);
        assertTrue(rs.next());
        assertEquals("x",rs.getString(1));
        assertEquals("x",rs.getString("foo"));
        assertFalse(rs.next());
    }
    
    @Test
    public void testSelectCF() throws Exception {
        String query;
        ResultSet rs;
        
        Properties props = PropertiesUtil.deepCopy(TEST_PROPERTIES);
        Connection conn = DriverManager.getConnection(getUrl(), props);
        conn.setAutoCommit(false);
        conn.createStatement().execute("CREATE TABLE " + TestUtil.DEFAULT_DATA_TABLE_FULL_NAME + " (k VARCHAR NOT NULL PRIMARY KEY, a.v1 VARCHAR, a.v2 VARCHAR, b.v1 VARCHAR)  ");
        query = "SELECT * FROM " + TestUtil.DEFAULT_DATA_TABLE_FULL_NAME;
        rs = conn.createStatement().executeQuery(query);
        assertFalse(rs.next());
        if(localIndex) {
            conn.createStatement().execute("CREATE LOCAL INDEX " + TestUtil.DEFAULT_INDEX_TABLE_NAME + " ON " + TestUtil.DEFAULT_DATA_TABLE_FULL_NAME + " (v2 DESC) INCLUDE (a.v1)");
        } else {
            conn.createStatement().execute("CREATE INDEX " + TestUtil.DEFAULT_INDEX_TABLE_NAME + " ON " + TestUtil.DEFAULT_DATA_TABLE_FULL_NAME + " (v2 DESC) INCLUDE (a.v1)");
        }
        query = "SELECT * FROM " + TestUtil.DEFAULT_INDEX_TABLE_FULL_NAME;
        rs = conn.createStatement().executeQuery(query);
        assertFalse(rs.next());

        PreparedStatement stmt = conn.prepareStatement("UPSERT INTO " + TestUtil.DEFAULT_DATA_TABLE_FULL_NAME + " VALUES(?,?,?,?)");
        stmt.setString(1,"a");
        stmt.setString(2, "x");
        stmt.setString(3, "1");
        stmt.setString(4, "A");
        stmt.execute();
        stmt.setString(1,"b");
        stmt.setString(2, "y");
        stmt.setString(3, "2");
        stmt.setString(4, "B");
        stmt.execute();
        conn.commit();
        
        query = "SELECT * FROM " + TestUtil.DEFAULT_DATA_TABLE_FULL_NAME;
        rs = conn.createStatement().executeQuery("EXPLAIN " + query);
        assertEquals("CLIENT PARALLEL 1-WAY FULL SCAN OVER " + TestUtil.DEFAULT_DATA_TABLE_FULL_NAME, QueryUtil.getExplainPlan(rs));

        query = "SELECT a.* FROM " + TestUtil.DEFAULT_DATA_TABLE_FULL_NAME;
        rs = conn.createStatement().executeQuery("EXPLAIN " + query);
        if(localIndex) {
            assertEquals("CLIENT PARALLEL 1-WAY RANGE SCAN OVER _LOCAL_IDX_" + TestUtil.DEFAULT_DATA_TABLE_FULL_NAME+" [-32768]\nCLIENT MERGE SORT", QueryUtil.getExplainPlan(rs));
        } else {
            assertEquals("CLIENT PARALLEL 1-WAY FULL SCAN OVER " + TestUtil.DEFAULT_INDEX_TABLE_FULL_NAME, QueryUtil.getExplainPlan(rs));
        }
        rs = conn.createStatement().executeQuery(query);
        assertTrue(rs.next());
        assertEquals("y",rs.getString(1));
        assertEquals("2",rs.getString(2));
        assertEquals("y",rs.getString("v1"));
        assertEquals("2",rs.getString("v2"));
        assertTrue(rs.next());
        assertEquals("x",rs.getString(1));
        assertEquals("1",rs.getString(2));
        assertEquals("x",rs.getString("v1"));
        assertEquals("1",rs.getString("v2"));
        assertFalse(rs.next());
    }
    
    @Test
    public void testCoveredColumns() throws Exception {
        String query;
        ResultSet rs;
        
        Properties props = PropertiesUtil.deepCopy(TEST_PROPERTIES);
        Connection conn = DriverManager.getConnection(getUrl(), props);
        conn.setAutoCommit(false);
        conn.createStatement().execute("CREATE TABLE " + TestUtil.DEFAULT_DATA_TABLE_FULL_NAME + " (k VARCHAR NOT NULL PRIMARY KEY, v1 VARCHAR, v2 VARCHAR)");
        query = "SELECT * FROM " + TestUtil.DEFAULT_DATA_TABLE_FULL_NAME;
        rs = conn.createStatement().executeQuery(query);
        assertFalse(rs.next());
        
        if(localIndex) {
            conn.createStatement().execute("CREATE LOCAL INDEX " + TestUtil.DEFAULT_INDEX_TABLE_NAME + " ON " + TestUtil.DEFAULT_DATA_TABLE_FULL_NAME + " (v1) INCLUDE (v2)");
        } else {
            conn.createStatement().execute("CREATE INDEX " + TestUtil.DEFAULT_INDEX_TABLE_NAME + " ON " + TestUtil.DEFAULT_DATA_TABLE_FULL_NAME + " (v1) INCLUDE (v2)");
        }
        query = "SELECT * FROM " + TestUtil.DEFAULT_INDEX_TABLE_FULL_NAME;
        rs = conn.createStatement().executeQuery(query);
        assertFalse(rs.next());

        PreparedStatement stmt = conn.prepareStatement("UPSERT INTO " + TestUtil.DEFAULT_DATA_TABLE_FULL_NAME + " VALUES(?,?,?)");
        stmt.setString(1,"a");
        stmt.setString(2, "x");
        stmt.setString(3, "1");
        stmt.execute();
        conn.commit();
        
        query = "SELECT * FROM " + TestUtil.DEFAULT_INDEX_TABLE_FULL_NAME;
        rs = conn.createStatement().executeQuery(query);
        assertTrue(rs.next());
        assertEquals("x",rs.getString(1));
        assertEquals("a",rs.getString(2));
        assertEquals("1",rs.getString(3));
        assertFalse(rs.next());

        stmt = conn.prepareStatement("UPSERT INTO " + TestUtil.DEFAULT_DATA_TABLE_FULL_NAME + "(k,v2) VALUES(?,?)");
        stmt.setString(1,"a");
        stmt.setString(2, null);
        stmt.execute();
        conn.commit();
        
        query = "SELECT * FROM " + TestUtil.DEFAULT_INDEX_TABLE_FULL_NAME;
        rs = conn.createStatement().executeQuery(query);
        assertTrue(rs.next());
        assertEquals("x",rs.getString(1));
        assertEquals("a",rs.getString(2));
        assertNull(rs.getString(3));
        assertFalse(rs.next());

        query = "SELECT * FROM " + TestUtil.DEFAULT_DATA_TABLE_FULL_NAME;
        rs = conn.createStatement().executeQuery("EXPLAIN " + query);
        if(localIndex) {
            assertEquals("CLIENT PARALLEL 1-WAY RANGE SCAN OVER _LOCAL_IDX_" + TestUtil.DEFAULT_DATA_TABLE_FULL_NAME+" [-32768]\nCLIENT MERGE SORT", QueryUtil.getExplainPlan(rs));            
        } else {
            assertEquals("CLIENT PARALLEL 1-WAY FULL SCAN OVER " + TestUtil.DEFAULT_INDEX_TABLE_FULL_NAME, QueryUtil.getExplainPlan(rs));
        }

        rs = conn.createStatement().executeQuery(query);
        assertTrue(rs.next());
        assertEquals("a",rs.getString(1));
        assertEquals("x",rs.getString(2));
        assertNull(rs.getString(3));
        assertFalse(rs.next());

        stmt = conn.prepareStatement("UPSERT INTO " + TestUtil.DEFAULT_DATA_TABLE_FULL_NAME + "(k,v2) VALUES(?,?)");
        stmt.setString(1,"a");
        stmt.setString(2,"3");
        stmt.execute();
        conn.commit();
        
        query = "SELECT * FROM " + TestUtil.DEFAULT_DATA_TABLE_FULL_NAME;
        rs = conn.createStatement().executeQuery("EXPLAIN " + query);
        if(localIndex) {
            assertEquals("CLIENT PARALLEL 1-WAY RANGE SCAN OVER _LOCAL_IDX_" + TestUtil.DEFAULT_DATA_TABLE_FULL_NAME + " [-32768]\nCLIENT MERGE SORT", QueryUtil.getExplainPlan(rs));            
        } else {
            assertEquals("CLIENT PARALLEL 1-WAY FULL SCAN OVER " + TestUtil.DEFAULT_INDEX_TABLE_FULL_NAME, QueryUtil.getExplainPlan(rs));
        }
        
        rs = conn.createStatement().executeQuery(query);
        assertTrue(rs.next());
        assertEquals("a",rs.getString(1));
        assertEquals("x",rs.getString(2));
        assertEquals("3",rs.getString(3));
        assertFalse(rs.next());

        stmt = conn.prepareStatement("UPSERT INTO " + TestUtil.DEFAULT_DATA_TABLE_FULL_NAME + "(k,v2) VALUES(?,?)");
        stmt.setString(1,"a");
        stmt.setString(2,"4");
        stmt.execute();
        conn.commit();
        
        query = "SELECT * FROM " + TestUtil.DEFAULT_DATA_TABLE_FULL_NAME;
        rs = conn.createStatement().executeQuery("EXPLAIN " + query);
        if(localIndex) {
            assertEquals("CLIENT PARALLEL 1-WAY RANGE SCAN OVER _LOCAL_IDX_" + TestUtil.DEFAULT_DATA_TABLE_FULL_NAME+" [-32768]\nCLIENT MERGE SORT", QueryUtil.getExplainPlan(rs));            
        } else {
            assertEquals("CLIENT PARALLEL 1-WAY FULL SCAN OVER " + TestUtil.DEFAULT_INDEX_TABLE_FULL_NAME, QueryUtil.getExplainPlan(rs));
        }
        
        rs = conn.createStatement().executeQuery(query);
        assertTrue(rs.next());
        assertEquals("a",rs.getString(1));
        assertEquals("x",rs.getString(2));
        assertEquals("4",rs.getString(3));
        assertFalse(rs.next());
    }

    @Test
    public void testCompoundIndexKey() throws Exception {
        String query;
        ResultSet rs;
        
        Properties props = PropertiesUtil.deepCopy(TEST_PROPERTIES);
        Connection conn = DriverManager.getConnection(getUrl(), props);
        conn.setAutoCommit(false);

        // make sure that the tables are empty, but reachable
        conn.createStatement().execute("CREATE TABLE " + TestUtil.DEFAULT_DATA_TABLE_FULL_NAME + " (k VARCHAR NOT NULL PRIMARY KEY, v1 VARCHAR, v2 VARCHAR)");
        query = "SELECT * FROM " + TestUtil.DEFAULT_DATA_TABLE_FULL_NAME;
        rs = conn.createStatement().executeQuery(query);
        assertFalse(rs.next());
        if(localIndex) {
            conn.createStatement().execute("CREATE LOCAL INDEX " + TestUtil.DEFAULT_INDEX_TABLE_NAME + " ON " + TestUtil.DEFAULT_DATA_TABLE_FULL_NAME + " (v1, v2)");
        } else {
            conn.createStatement().execute("CREATE INDEX " + TestUtil.DEFAULT_INDEX_TABLE_NAME + " ON " + TestUtil.DEFAULT_DATA_TABLE_FULL_NAME + " (v1, v2)");
        }
        query = "SELECT * FROM " + TestUtil.DEFAULT_INDEX_TABLE_FULL_NAME;
        rs = conn.createStatement().executeQuery(query);
        assertFalse(rs.next());

        // load some data into the table
        PreparedStatement stmt = conn.prepareStatement("UPSERT INTO " + TestUtil.DEFAULT_DATA_TABLE_FULL_NAME + " VALUES(?,?,?)");
        stmt.setString(1,"a");
        stmt.setString(2, "x");
        stmt.setString(3, "1");
        stmt.execute();
        conn.commit();
        
        query = "SELECT * FROM " + TestUtil.DEFAULT_INDEX_TABLE_FULL_NAME;
        rs = conn.createStatement().executeQuery(query);
        assertTrue(rs.next());
        assertEquals("x",rs.getString(1));
        assertEquals("1",rs.getString(2));
        assertEquals("a",rs.getString(3));
        assertFalse(rs.next());

        stmt = conn.prepareStatement("UPSERT INTO " + TestUtil.DEFAULT_DATA_TABLE_FULL_NAME + " VALUES(?,?,?)");
        stmt.setString(1,"a");
        stmt.setString(2, "y");
        stmt.setString(3, null);
        stmt.execute();
        conn.commit();
        
        query = "SELECT * FROM " + TestUtil.DEFAULT_INDEX_TABLE_FULL_NAME;
        rs = conn.createStatement().executeQuery(query);
        assertTrue(rs.next());
        assertEquals("y",rs.getString(1));
        assertNull(rs.getString(2));
        assertEquals("a",rs.getString(3));
        assertFalse(rs.next());

        query = "SELECT * FROM " + TestUtil.DEFAULT_DATA_TABLE_FULL_NAME;
        rs = conn.createStatement().executeQuery("EXPLAIN " + query);
        if (localIndex) {
            assertEquals("CLIENT PARALLEL 1-WAY RANGE SCAN OVER _LOCAL_IDX_" + TestUtil.DEFAULT_DATA_TABLE_FULL_NAME+" [-32768]\n"
                    + "    SERVER FILTER BY FIRST KEY ONLY\n"
                    + "CLIENT MERGE SORT", QueryUtil.getExplainPlan(rs));
        } else {
            assertEquals("CLIENT PARALLEL 1-WAY FULL SCAN OVER " + TestUtil.DEFAULT_INDEX_TABLE_FULL_NAME + "\n"
                       + "    SERVER FILTER BY FIRST KEY ONLY", QueryUtil.getExplainPlan(rs));
        }
        //make sure the data table looks like what we expect
        rs = conn.createStatement().executeQuery(query);
        assertTrue(rs.next());
        assertEquals("a",rs.getString(1));
        assertEquals("y",rs.getString(2));
        assertNull(rs.getString(3));
        assertFalse(rs.next());
        
        // Upsert new row with null leading index column
        stmt.setString(1,"b");
        stmt.setString(2, null);
        stmt.setString(3, "3");
        stmt.execute();
        conn.commit();
        
        query = "SELECT * FROM " + TestUtil.DEFAULT_INDEX_TABLE_FULL_NAME;
        rs = conn.createStatement().executeQuery(query);
        assertTrue(rs.next());
        assertEquals(null,rs.getString(1));
        assertEquals("3",rs.getString(2));
        assertEquals("b",rs.getString(3));
        assertTrue(rs.next());
        assertEquals("y",rs.getString(1));
        assertNull(rs.getString(2));
        assertEquals("a",rs.getString(3));
        assertFalse(rs.next());

        // Update row with null leading index column to have a value
        stmt = conn.prepareStatement("UPSERT INTO " + TestUtil.DEFAULT_DATA_TABLE_FULL_NAME + " VALUES(?,?)");
        stmt.setString(1,"b");
        stmt.setString(2, "z");
        stmt.execute();
        conn.commit();
        
        query = "SELECT * FROM " + TestUtil.DEFAULT_INDEX_TABLE_FULL_NAME;
        rs = conn.createStatement().executeQuery(query);
        assertTrue(rs.next());
        assertEquals("y",rs.getString(1));
        assertNull(rs.getString(2));
        assertEquals("a",rs.getString(3));
        assertTrue(rs.next());
        assertEquals("z",rs.getString(1));
        assertEquals("3",rs.getString(2));
        assertEquals("b",rs.getString(3));
        assertFalse(rs.next());

    }
 
    /**
     * There was a case where if there were multiple updates to a single row in the same batch, the
     * index wouldn't be updated correctly as each element of the batch was evaluated with the state
     * previous to the batch, rather than with the rest of the batch. This meant you could do a put
     * and a delete on a row in the same batch and the index result would contain the current + put
     * and current + delete, but not current + put + delete.
     * @throws Exception on failure
     */
    @Test
    public void testMultipleUpdatesToSingleRow() throws Exception {
        String query;
        ResultSet rs;
    
        Properties props = PropertiesUtil.deepCopy(TEST_PROPERTIES);
        Connection conn = DriverManager.getConnection(getUrl(), props);
        conn.setAutoCommit(false);
    
        // make sure that the tables are empty, but reachable
        conn.createStatement().execute(
          "CREATE TABLE " + TestUtil.DEFAULT_DATA_TABLE_FULL_NAME
              + " (k VARCHAR NOT NULL PRIMARY KEY, v1 VARCHAR, v2 VARCHAR)");
        query = "SELECT * FROM " + TestUtil.DEFAULT_DATA_TABLE_FULL_NAME;
        rs = conn.createStatement().executeQuery(query);
        assertFalse(rs.next());

        if(localIndex) {
            conn.createStatement().execute(
                "CREATE LOCAL INDEX " + TestUtil.DEFAULT_INDEX_TABLE_NAME + " ON " + TestUtil.DEFAULT_DATA_TABLE_FULL_NAME + " (v1, v2)");
        } else {
            conn.createStatement().execute(
                "CREATE INDEX " + TestUtil.DEFAULT_INDEX_TABLE_NAME + " ON " + TestUtil.DEFAULT_DATA_TABLE_FULL_NAME + " (v1, v2)");
        }
        query = "SELECT * FROM " + TestUtil.DEFAULT_INDEX_TABLE_FULL_NAME;
        rs = conn.createStatement().executeQuery(query);
        assertFalse(rs.next());
    
        // load some data into the table
        PreparedStatement stmt =
            conn.prepareStatement("UPSERT INTO " + TestUtil.DEFAULT_DATA_TABLE_FULL_NAME + " VALUES(?,?,?)");
        stmt.setString(1, "a");
        stmt.setString(2, "x");
        stmt.setString(3, "1");
        stmt.execute();
        conn.commit();
        
        // make sure the index is working as expected
        query = "SELECT * FROM " + TestUtil.DEFAULT_INDEX_TABLE_FULL_NAME;
        rs = conn.createStatement().executeQuery(query);
        assertTrue(rs.next());
        assertEquals("x", rs.getString(1));
        assertEquals("1", rs.getString(2));
        assertEquals("a", rs.getString(3));
        assertFalse(rs.next());
      
        // do multiple updates to the same row, in the same batch
        stmt = conn.prepareStatement("UPSERT INTO " + TestUtil.DEFAULT_DATA_TABLE_FULL_NAME + "(k, v1) VALUES(?,?)");
        stmt.setString(1, "a");
        stmt.setString(2, "y");
        stmt.execute();
        stmt = conn.prepareStatement("UPSERT INTO " + TestUtil.DEFAULT_DATA_TABLE_FULL_NAME + "(k,v2) VALUES(?,?)");
        stmt.setString(1, "a");
        stmt.setString(2, null);
        stmt.execute();
        conn.commit();
    
        query = "SELECT * FROM " + TestUtil.DEFAULT_INDEX_TABLE_FULL_NAME;
        rs = conn.createStatement().executeQuery(query);
        assertTrue(rs.next());
        assertEquals("y", rs.getString(1));
        assertNull(rs.getString(2));
        assertEquals("a", rs.getString(3));
        assertFalse(rs.next());
    
        query = "SELECT * FROM " + TestUtil.DEFAULT_DATA_TABLE_FULL_NAME;
        rs = conn.createStatement().executeQuery("EXPLAIN " + query);
        if(localIndex) {
            assertEquals("CLIENT PARALLEL 1-WAY RANGE SCAN OVER _LOCAL_IDX_" + TestUtil.DEFAULT_DATA_TABLE_FULL_NAME+" [-32768]\n"
                    + "    SERVER FILTER BY FIRST KEY ONLY\n"
                    + "CLIENT MERGE SORT",
                QueryUtil.getExplainPlan(rs));
        } else {
            assertEquals("CLIENT PARALLEL 1-WAY FULL SCAN OVER " + TestUtil.DEFAULT_INDEX_TABLE_FULL_NAME + "\n"
                    + "    SERVER FILTER BY FIRST KEY ONLY",
                QueryUtil.getExplainPlan(rs));
        }
    
        // check that the data table matches as expected
        rs = conn.createStatement().executeQuery(query);
        assertTrue(rs.next());
        assertEquals("a", rs.getString(1));
        assertEquals("y", rs.getString(2));
        assertNull(rs.getString(3));
        assertFalse(rs.next());
    }

    @Test
    public void testUpsertAfterIndexDrop() throws Exception {
        String query;
        ResultSet rs;
    
        Properties props = PropertiesUtil.deepCopy(TEST_PROPERTIES);
        Connection conn = DriverManager.getConnection(getUrl(), props);
        conn.setAutoCommit(false);
    
        // make sure that the tables are empty, but reachable
        conn.createStatement().execute(
          "CREATE TABLE " + TestUtil.DEFAULT_DATA_TABLE_FULL_NAME
              + " (k VARCHAR NOT NULL PRIMARY KEY, v1 VARCHAR, v2 VARCHAR)");
        query = "SELECT * FROM " + TestUtil.DEFAULT_DATA_TABLE_FULL_NAME;
        rs = conn.createStatement().executeQuery(query);
        assertFalse(rs.next());
    
        conn.createStatement().execute(
          "CREATE " + (localIndex ? "LOCAL " : "") + "INDEX " + TestUtil.DEFAULT_INDEX_TABLE_NAME + " ON " + TestUtil.DEFAULT_DATA_TABLE_FULL_NAME + " (v1, v2)");
        query = "SELECT * FROM " + TestUtil.DEFAULT_INDEX_TABLE_FULL_NAME;
        rs = conn.createStatement().executeQuery(query);
        assertFalse(rs.next());
    
        // load some data into the table
        PreparedStatement stmt =
            conn.prepareStatement("UPSERT INTO " + TestUtil.DEFAULT_DATA_TABLE_FULL_NAME + " VALUES(?,?,?)");
        stmt.setString(1, "a");
        stmt.setString(2, "x");
        stmt.setString(3, "1");
        stmt.execute();
        conn.commit();
        
        // make sure the index is working as expected
        query = "SELECT * FROM " + TestUtil.DEFAULT_INDEX_TABLE_FULL_NAME;
        rs = conn.createStatement().executeQuery(query);
        assertTrue(rs.next());
        assertEquals("x", rs.getString(1));
        assertEquals("1", rs.getString(2));
        assertEquals("a", rs.getString(3));
        assertFalse(rs.next());

        String ddl = "DROP INDEX " + TestUtil.DEFAULT_INDEX_TABLE_NAME + " ON " + TestUtil.DEFAULT_DATA_TABLE_FULL_NAME;
        stmt = conn.prepareStatement(ddl);
        stmt.execute();
        
        stmt = conn.prepareStatement("UPSERT INTO " + TestUtil.DEFAULT_DATA_TABLE_FULL_NAME + "(k, v1) VALUES(?,?)");
        stmt.setString(1, "a");
        stmt.setString(2, "y");
        stmt.execute();
        conn.commit();
    
        query = "SELECT * FROM " + TestUtil.DEFAULT_DATA_TABLE_FULL_NAME;
    
        // check that the data table matches as expected
        rs = conn.createStatement().executeQuery(query);
        assertTrue(rs.next());
        assertEquals("a", rs.getString(1));
        assertEquals("y", rs.getString(2));
        assertFalse(rs.next());
    }
    
    @Test
    public void testMultipleUpdatesAcrossRegions() throws Exception {
        String query;
        ResultSet rs;
    
        Properties props = PropertiesUtil.deepCopy(TEST_PROPERTIES);
        Connection conn = DriverManager.getConnection(getUrl(), props);
        conn.setAutoCommit(false);
    
        // make sure that the tables are empty, but reachable
        conn.createStatement().execute(
          "CREATE TABLE " + TestUtil.DEFAULT_DATA_TABLE_FULL_NAME
              + " (k VARCHAR NOT NULL PRIMARY KEY, v1 VARCHAR, v2 VARCHAR) " + HTableDescriptor.MAX_FILESIZE + "=1, " + HTableDescriptor.MEMSTORE_FLUSHSIZE + "=1 " +
                  "SPLIT ON ('b')");
        query = "SELECT * FROM " + TestUtil.DEFAULT_DATA_TABLE_FULL_NAME;
        rs = conn.createStatement().executeQuery(query);
        assertFalse(rs.next());

        if(localIndex) {
            conn.createStatement().execute(
                "CREATE LOCAL INDEX " + TestUtil.DEFAULT_INDEX_TABLE_NAME + " ON " + TestUtil.DEFAULT_DATA_TABLE_FULL_NAME + " (v1, v2)");
        } else {
            conn.createStatement().execute(
                "CREATE INDEX " + TestUtil.DEFAULT_INDEX_TABLE_NAME + " ON " + TestUtil.DEFAULT_DATA_TABLE_FULL_NAME + " (v1, v2)");
        }
        query = "SELECT * FROM " + TestUtil.DEFAULT_INDEX_TABLE_FULL_NAME;
        rs = conn.createStatement().executeQuery(query);
        assertFalse(rs.next());
    
        // load some data into the table
        PreparedStatement stmt =
            conn.prepareStatement("UPSERT INTO " + TestUtil.DEFAULT_DATA_TABLE_FULL_NAME + " VALUES(?,?,?)");
        stmt.setString(1, "a");
        stmt.setString(2, "x");
        stmt.setString(3, "1");
        stmt.execute();
        stmt.setString(1, "b");
        stmt.setString(2, "y");
        stmt.setString(3, "2");
        stmt.execute();
        stmt.setString(1, "c");
        stmt.setString(2, "z");
        stmt.setString(3, "3");
        stmt.execute();
        conn.commit();
        
        // make sure the index is working as expected
        query = "SELECT * FROM " + TestUtil.DEFAULT_INDEX_TABLE_FULL_NAME;
        rs = conn.createStatement().executeQuery(query);
        assertTrue(rs.next());
        assertEquals("x", rs.getString(1));
        assertEquals("1", rs.getString(2));
        assertEquals("a", rs.getString(3));
        assertTrue(rs.next());
        assertEquals("y", rs.getString(1));
        assertEquals("2", rs.getString(2));
        assertEquals("b", rs.getString(3));
        assertTrue(rs.next());
        assertEquals("z", rs.getString(1));
        assertEquals("3", rs.getString(2));
        assertEquals("c", rs.getString(3));
        assertFalse(rs.next());

        query = "SELECT * FROM " + TestUtil.DEFAULT_DATA_TABLE_FULL_NAME;
        rs = conn.createStatement().executeQuery("EXPLAIN " + query);
        if (localIndex) {
            assertEquals("CLIENT PARALLEL 2-WAY RANGE SCAN OVER _LOCAL_IDX_" + TestUtil.DEFAULT_DATA_TABLE_FULL_NAME+" [-32768]\n"
                       + "    SERVER FILTER BY FIRST KEY ONLY\n"
                       + "CLIENT MERGE SORT",
                QueryUtil.getExplainPlan(rs));
        } else {
            assertEquals("CLIENT PARALLEL 1-WAY FULL SCAN OVER " + TestUtil.DEFAULT_INDEX_TABLE_FULL_NAME + "\n"
                    + "    SERVER FILTER BY FIRST KEY ONLY",
                QueryUtil.getExplainPlan(rs));
        }
    
        // check that the data table matches as expected
        rs = conn.createStatement().executeQuery(query);
        assertTrue(rs.next());
        assertEquals("a", rs.getString(1));
        assertEquals("x", rs.getString(2));
        assertEquals("1", rs.getString(3));
        assertTrue(rs.next());
        assertEquals("b", rs.getString(1));
        assertEquals("y", rs.getString(2));
        assertEquals("2", rs.getString(3));
        assertTrue(rs.next());
        assertEquals("c", rs.getString(1));
        assertEquals("z", rs.getString(2));
        assertEquals("3", rs.getString(3));
        assertFalse(rs.next());
    }
    
    @Test
    public void testIndexWithCaseSensitiveCols() throws Exception {
        String query;
        ResultSet rs;
        
        Properties props = PropertiesUtil.deepCopy(TEST_PROPERTIES);
        Connection conn = DriverManager.getConnection(getUrl(), props);
        conn.setAutoCommit(false);
        try {
            conn.createStatement().execute("CREATE TABLE cs (k VARCHAR NOT NULL PRIMARY KEY, \"V1\" VARCHAR, \"v2\" VARCHAR)");
            query = "SELECT * FROM cs";
            rs = conn.createStatement().executeQuery(query);
            assertFalse(rs.next());
            if (localIndex) {
                conn.createStatement().execute("CREATE LOCAL INDEX ics ON cs (\"v2\") INCLUDE (\"V1\")");
            } else {
                conn.createStatement().execute("CREATE INDEX ics ON cs (\"v2\") INCLUDE (\"V1\")");
            }
            query = "SELECT * FROM ics";
            rs = conn.createStatement().executeQuery(query);
            assertFalse(rs.next());

            PreparedStatement stmt = conn.prepareStatement("UPSERT INTO cs VALUES(?,?,?)");
            stmt.setString(1,"a");
            stmt.setString(2, "x");
            stmt.setString(3, "1");
            stmt.execute();
            stmt.setString(1,"b");
            stmt.setString(2, "y");
            stmt.setString(3, "2");
            stmt.execute();
            conn.commit();

            query = "SELECT * FROM cs WHERE \"v2\" = '1'";
            rs = conn.createStatement().executeQuery("EXPLAIN " + query);
            if(localIndex){
                assertEquals("CLIENT PARALLEL 1-WAY RANGE SCAN OVER _LOCAL_IDX_CS [-32768,'1']\n"
                           + "CLIENT MERGE SORT", QueryUtil.getExplainPlan(rs));
            } else {
                assertEquals("CLIENT PARALLEL 1-WAY RANGE SCAN OVER ICS ['1']", QueryUtil.getExplainPlan(rs));
            }

            rs = conn.createStatement().executeQuery(query);
            assertTrue(rs.next());
            assertEquals("a",rs.getString(1));
            assertEquals("x",rs.getString(2));
            assertEquals("1",rs.getString(3));
            assertEquals("a",rs.getString("k"));
            assertEquals("x",rs.getString("V1"));
            assertEquals("1",rs.getString("v2"));
            assertFalse(rs.next());

            query = "SELECT \"V1\", \"V1\" as foo1, \"v2\" as foo, \"v2\" as \"Foo1\", \"v2\" FROM cs ORDER BY foo";
            rs = conn.createStatement().executeQuery("EXPLAIN " + query);
            if(localIndex){
                assertEquals("CLIENT PARALLEL 1-WAY RANGE SCAN OVER _LOCAL_IDX_CS [-32768]\nCLIENT MERGE SORT",
                    QueryUtil.getExplainPlan(rs));
            } else {
                assertEquals("CLIENT PARALLEL 1-WAY FULL SCAN OVER ICS", QueryUtil.getExplainPlan(rs));
            }

            rs = conn.createStatement().executeQuery(query);
            assertTrue(rs.next());
            assertEquals("x",rs.getString(1));
            assertEquals("x",rs.getString("V1"));
            assertEquals("x",rs.getString(2));
            assertEquals("x",rs.getString("foo1"));
            assertEquals("1",rs.getString(3));
            assertEquals("1",rs.getString("Foo"));
            assertEquals("1",rs.getString(4));
            assertEquals("1",rs.getString("Foo1"));
            assertEquals("1",rs.getString(5));
            assertEquals("1",rs.getString("v2"));
            assertTrue(rs.next());
            assertEquals("y",rs.getString(1));
            assertEquals("y",rs.getString("V1"));
            assertEquals("y",rs.getString(2));
            assertEquals("y",rs.getString("foo1"));
            assertEquals("2",rs.getString(3));
            assertEquals("2",rs.getString("Foo"));
            assertEquals("2",rs.getString(4));
            assertEquals("2",rs.getString("Foo1"));
            assertEquals("2",rs.getString(5));
            assertEquals("2",rs.getString("v2"));
            assertFalse(rs.next());
        } finally {
            conn.close();
        }
    }

    @Test
    public void testInFilterOnIndexedTable() throws Exception {
        Properties props = PropertiesUtil.deepCopy(TEST_PROPERTIES);
        Connection conn = DriverManager.getConnection(getUrl(), props);
        conn.setAutoCommit(false);
        try {
	        String ddl = "CREATE TABLE TEST (PK1 CHAR(2) NOT NULL PRIMARY KEY, CF1.COL1 BIGINT)";
	        conn.createStatement().execute(ddl);
	        if(localIndex) {
	            ddl = "CREATE LOCAL INDEX IDX1 ON TEST (COL1)";
	        } else {
	            ddl = "CREATE INDEX IDX1 ON TEST (COL1)";
	        }
	        conn.createStatement().execute(ddl);
	
	        String query = "SELECT COUNT(COL1) FROM TEST WHERE COL1 IN (1,25,50,75,100)"; 
	        ResultSet rs = conn.createStatement().executeQuery(query);
	        assertTrue(rs.next());
        } finally {
            conn.close();
        }
    }

    @Test
    public void testIndexWithDecimalCol() throws Exception {
        Properties props = PropertiesUtil.deepCopy(TEST_PROPERTIES);
        Connection conn = DriverManager.getConnection(getUrl(), props);
        conn.setAutoCommit(false);
        try {
            Date date = new Date(System.currentTimeMillis());
            
            createMultiCFTestTable(TestUtil.DEFAULT_DATA_TABLE_FULL_NAME);
            populateMultiCFTestTable(TestUtil.DEFAULT_DATA_TABLE_FULL_NAME, date);
            String ddl = null;
            if (localIndex) {
                ddl = "CREATE LOCAL INDEX " + TestUtil.DEFAULT_INDEX_TABLE_NAME + " ON " + TestUtil.DEFAULT_DATA_TABLE_FULL_NAME + " (decimal_pk) INCLUDE (decimal_col1, decimal_col2)";
            } else {
                ddl = "CREATE INDEX " + TestUtil.DEFAULT_INDEX_TABLE_NAME + " ON " + TestUtil.DEFAULT_DATA_TABLE_FULL_NAME + " (decimal_pk) INCLUDE (decimal_col1, decimal_col2)";
            }
            PreparedStatement stmt = conn.prepareStatement(ddl);
            stmt.execute();
            
            String query = "SELECT decimal_pk, decimal_col1, decimal_col2 from " + TestUtil.DEFAULT_DATA_TABLE_FULL_NAME ;
            ResultSet rs = conn.createStatement().executeQuery("EXPLAIN " + query);
            if(localIndex) {
                assertEquals("CLIENT PARALLEL 1-WAY RANGE SCAN OVER _LOCAL_IDX_" + TestUtil.DEFAULT_DATA_TABLE_FULL_NAME+" [-32768]\nCLIENT MERGE SORT", QueryUtil.getExplainPlan(rs));
            } else {
                assertEquals("CLIENT PARALLEL 1-WAY FULL SCAN OVER " + TestUtil.DEFAULT_INDEX_TABLE_FULL_NAME, QueryUtil.getExplainPlan(rs));
            }
            
            rs = conn.createStatement().executeQuery(query);
            assertTrue(rs.next());
            assertEquals(new BigDecimal("1.1"), rs.getBigDecimal(1));
            assertEquals(new BigDecimal("2.1"), rs.getBigDecimal(2));
            assertEquals(new BigDecimal("3.1"), rs.getBigDecimal(3));
            assertTrue(rs.next());
            assertEquals(new BigDecimal("2.2"), rs.getBigDecimal(1));
            assertEquals(new BigDecimal("3.2"), rs.getBigDecimal(2));
            assertEquals(new BigDecimal("4.2"), rs.getBigDecimal(3));
            assertTrue(rs.next());
            assertEquals(new BigDecimal("3.3"), rs.getBigDecimal(1));
            assertEquals(new BigDecimal("4.3"), rs.getBigDecimal(2));
            assertEquals(new BigDecimal("5.3"), rs.getBigDecimal(3));
            assertFalse(rs.next());
        } finally {
            conn.close();
        }
    }
    
    @Test
    public void testUpsertingNullForIndexedColumns() throws Exception {
    	Properties props = PropertiesUtil.deepCopy(TEST_PROPERTIES);
    	Connection conn = DriverManager.getConnection(getUrl(), props);
    	conn.setAutoCommit(false);
    	try {
    		Statement stmt = conn.createStatement();
    		stmt.execute("CREATE TABLE DEMO(v1 VARCHAR PRIMARY KEY, v2 DOUBLE, v3 VARCHAR)");
    		stmt.execute("CREATE " + (localIndex ? "LOCAL " : "") + "INDEX DEMO_idx ON DEMO (v2) INCLUDE(v3)");
    		
    		//create a row with value null for indexed column v2
    		stmt.executeUpdate("upsert into DEMO values('cc1', null, 'abc')");
    		conn.commit();
            
    		//assert values in index table 
    		ResultSet rs = stmt.executeQuery("select * from DEMO_IDX");
    		assertTrue(rs.next());
    		assertEquals(0, Doubles.compare(0, rs.getDouble(1)));
    		assertEquals("cc1", rs.getString(2));
    		assertEquals("abc", rs.getString(3));
    		assertFalse(rs.next());
    		
    		//assert values in data table
    		rs = stmt.executeQuery("select v1, v2, v3 from DEMO");
    		assertTrue(rs.next());
    		assertEquals("cc1", rs.getString(1));
    		assertEquals(0, Doubles.compare(0, rs.getDouble(2)));
    		assertEquals("abc", rs.getString(3));
    		assertFalse(rs.next());
    		
    		//update the previously null value for indexed column v2 to a non-null value 1.23
    		stmt.executeUpdate("upsert into DEMO values('cc1', 1.23, 'abc')");
    		conn.commit();
    		
    		//assert values in index table 
    		rs = stmt.executeQuery("select * from DEMO_IDX");
    		assertTrue(rs.next());
    		assertEquals(0, Doubles.compare(1.23, rs.getDouble(1)));
    		assertEquals("cc1", rs.getString(2));
    		assertEquals("abc", rs.getString(3));
    		assertFalse(rs.next());
    		
    		//assert values in data table
    		rs = stmt.executeQuery("select v1, v2, v3 from DEMO");
    		assertTrue(rs.next());
    		assertEquals("cc1", rs.getString(1));
    		assertEquals(0, Doubles.compare(1.23, rs.getDouble(2)));
    		assertEquals("abc", rs.getString(3));
    		assertFalse(rs.next());
    		
    		//update the value for indexed column v2 back to null
    		stmt.executeUpdate("upsert into DEMO values('cc1', null, 'abc')");
    		conn.commit();
    		
    		//assert values in index table 
    		rs = stmt.executeQuery("select * from DEMO_IDX");
    		assertTrue(rs.next());
    		assertEquals(0, Doubles.compare(0, rs.getDouble(1)));
    		assertEquals("cc1", rs.getString(2));
    		assertEquals("abc", rs.getString(3));
    		assertFalse(rs.next());
    		
    		//assert values in data table
    		rs = stmt.executeQuery("select v1, v2, v3 from DEMO");
    		assertTrue(rs.next());
    		assertEquals("cc1", rs.getString(1));
    		assertEquals(0, Doubles.compare(0, rs.getDouble(2)));
    		assertEquals("abc", rs.getString(3));
    		assertFalse(rs.next());
    	} finally {
    		conn.close();
    	}
    }
}
