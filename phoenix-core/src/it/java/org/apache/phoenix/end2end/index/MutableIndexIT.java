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
import org.apache.phoenix.end2end.BaseHBaseManagedTimeIT;
import org.apache.phoenix.end2end.HBaseManagedTimeTest;
import org.apache.phoenix.end2end.Shadower;
import org.apache.phoenix.query.QueryServices;
import org.apache.phoenix.util.MetaDataUtil;
import org.apache.phoenix.util.QueryUtil;
import org.apache.phoenix.util.ReadOnlyProps;
import org.apache.phoenix.util.TestUtil;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import com.google.common.collect.Maps;
import com.google.common.primitives.Doubles;

@Category(HBaseManagedTimeTest.class)
public class MutableIndexIT extends BaseMutableIndexIT {
    
    @BeforeClass
    @Shadower(classBeingShadowed = BaseHBaseManagedTimeIT.class)
    public static void doSetup() throws Exception {
        Map<String,String> props = Maps.newHashMapWithExpectedSize(1);
        // Don't split intra region so we can more easily know that the n-way parallelization is for the explain plan
        props.put(QueryServices.MAX_INTRA_REGION_PARALLELIZATION_ATTRIB, Integer.toString(1));
        // Forces server cache to be used
        props.put(QueryServices.INDEX_MUTATE_BATCH_SIZE_THRESHOLD_ATTRIB, Integer.toString(2));
        props.put(QueryServices.DROP_METADATA_ATTRIB, Boolean.toString(true));
        // Must update config before starting server
        props.put(QueryServices.DROP_METADATA_ATTRIB, Boolean.toString(true));
        setUpTestDriver(getUrl(), new ReadOnlyProps(props.entrySet().iterator()));
    }

    @Test
    public void testIndexWithNullableFixedWithCols() throws Exception {
        testIndexWithNullableFixedWithCols(false);
    }
    
    @Test
    public void testLocalIndexWithNullableFixedWithCols() throws Exception {
        testIndexWithNullableFixedWithCols(true);
    }

    private void testIndexWithNullableFixedWithCols(boolean localIndex) throws Exception {
        Properties props = new Properties(TEST_PROPERTIES);
        Connection conn = DriverManager.getConnection(getUrl(), props);
        conn.setAutoCommit(false);
        try {
            createTestTable();
            populateTestTable();
            String ddl = null;
            if(localIndex){
                ddl = "CREATE LOCAL INDEX " + INDEX_TABLE_NAME + " ON " + DATA_TABLE_FULL_NAME
                        + " (char_col1 ASC, int_col1 ASC)"
                        + " INCLUDE (long_col1, long_col2)";
            } else {
                ddl = "CREATE INDEX " + INDEX_TABLE_NAME + " ON " + DATA_TABLE_FULL_NAME
                        + " (char_col1 ASC, int_col1 ASC)"
                        + " INCLUDE (long_col1, long_col2)";
            }
            PreparedStatement stmt = conn.prepareStatement(ddl);
            stmt.execute();
            
            String query = "SELECT d.char_col1, int_col1 from " + DATA_TABLE_FULL_NAME + " as d";
            ResultSet rs = conn.createStatement().executeQuery("EXPLAIN " + query);
            if (localIndex) {
                assertEquals("CLIENT PARALLEL 1-WAY FULL SCAN OVER " + MetaDataUtil.getLocalIndexTableName(DATA_TABLE_FULL_NAME)+"\nCLIENT MERGE SORT", QueryUtil.getExplainPlan(rs));
            } else {
                assertEquals("CLIENT PARALLEL 1-WAY FULL SCAN OVER " + INDEX_TABLE_FULL_NAME, QueryUtil.getExplainPlan(rs));
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
        testIndexWithNullableDateCol(false);
    }

    @Test
    public void testLocalIndexWithNullableDateCol() throws Exception {
        testIndexWithNullableDateCol(true);
    }

    private void testIndexWithNullableDateCol(boolean localIndex) throws Exception {
        Properties props = new Properties(TEST_PROPERTIES);
        Connection conn = DriverManager.getConnection(getUrl(), props);
        conn.setAutoCommit(false);
        try {
            Date date = new Date(System.currentTimeMillis());
            
            createTestTable();
            populateTestTable(date);
            String ddl = null;
            if (localIndex) {
                ddl = "CREATE LOCAL INDEX " + INDEX_TABLE_NAME + " ON " + DATA_TABLE_FULL_NAME + " (date_col)";
            } else {
                ddl = "CREATE INDEX " + INDEX_TABLE_NAME + " ON " + DATA_TABLE_FULL_NAME + " (date_col)";
            }
            PreparedStatement stmt = conn.prepareStatement(ddl);
            stmt.execute();
            
            String query = "SELECT int_pk from " + DATA_TABLE_FULL_NAME ;
            ResultSet rs = conn.createStatement().executeQuery("EXPLAIN " + query);
            if (localIndex) {
                assertEquals("CLIENT PARALLEL 1-WAY FULL SCAN OVER _LOCAL_IDX_" + DATA_TABLE_FULL_NAME +"\nCLIENT MERGE SORT", QueryUtil.getExplainPlan(rs));
            } else {
                assertEquals("CLIENT PARALLEL 1-WAY FULL SCAN OVER " + INDEX_TABLE_FULL_NAME, QueryUtil.getExplainPlan(rs));
            }
            
            rs = conn.createStatement().executeQuery(query);
            assertTrue(rs.next());
            assertEquals(2, rs.getInt(1));
            assertTrue(rs.next());
            assertEquals(1, rs.getInt(1));
            assertTrue(rs.next());
            assertEquals(3, rs.getInt(1));
            assertFalse(rs.next());
            
            query = "SELECT date_col from " + DATA_TABLE_FULL_NAME + " order by date_col" ;
            rs = conn.createStatement().executeQuery("EXPLAIN " + query);
            if (localIndex) {
                assertEquals("CLIENT PARALLEL 1-WAY FULL SCAN OVER _LOCAL_IDX_" + DATA_TABLE_FULL_NAME + "\nCLIENT MERGE SORT", QueryUtil.getExplainPlan(rs));
            } else {
                assertEquals("CLIENT PARALLEL 1-WAY FULL SCAN OVER " + INDEX_TABLE_FULL_NAME, QueryUtil.getExplainPlan(rs));
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
        testCoveredColumnUpdates(false);
    }

    @Test
    public void testCoveredColumnUpdatesWithLocalIndex() throws Exception {
        testCoveredColumnUpdates(true);
    }

    private void testCoveredColumnUpdates(boolean localIndex) throws Exception {
        Properties props = new Properties(TEST_PROPERTIES);
        Connection conn = DriverManager.getConnection(getUrl(), props);
        conn.setAutoCommit(false);
        try {
            createTestTable();
            populateTestTable();
            String ddl = null;
            if(localIndex) {
                ddl = "CREATE LOCAL INDEX " + INDEX_TABLE_NAME + " ON " + DATA_TABLE_FULL_NAME
                        + " (char_col1 ASC, int_col1 ASC)"
                        + " INCLUDE (long_col1, long_col2)";
            } else {
                ddl = "CREATE INDEX " + INDEX_TABLE_NAME + " ON " + DATA_TABLE_FULL_NAME
                        + " (char_col1 ASC, int_col1 ASC)"
                        + " INCLUDE (long_col1, long_col2)";
            }

            PreparedStatement stmt = conn.prepareStatement(ddl);
            stmt.execute();
            
            String query = "SELECT char_col1, int_col1, long_col2 from " + DATA_TABLE_FULL_NAME;
            ResultSet rs = conn.createStatement().executeQuery("EXPLAIN " + query);
            if (localIndex) {
                assertEquals("CLIENT PARALLEL 1-WAY FULL SCAN OVER _LOCAL_IDX_" + DATA_TABLE_FULL_NAME +"\nCLIENT MERGE SORT", QueryUtil.getExplainPlan(rs));
            } else {
                assertEquals("CLIENT PARALLEL 1-WAY FULL SCAN OVER " + INDEX_TABLE_FULL_NAME, QueryUtil.getExplainPlan(rs));
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
            
            stmt = conn.prepareStatement("UPSERT INTO " + DATA_TABLE_FULL_NAME
                    + "(varchar_pk, char_pk, int_pk, long_pk , decimal_pk, long_col2) SELECT varchar_pk, char_pk, int_pk, long_pk , decimal_pk, long_col2*2 FROM "
                    + DATA_TABLE_FULL_NAME + " WHERE long_col2=?");
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
            
            stmt = conn.prepareStatement("UPSERT INTO " + DATA_TABLE_FULL_NAME
                    + "(varchar_pk, char_pk, int_pk, long_pk , decimal_pk, long_col2) SELECT varchar_pk, char_pk, int_pk, long_pk , decimal_pk, null FROM "
                    + DATA_TABLE_FULL_NAME + " WHERE long_col2=?");
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
            
        } finally {
            conn.close();
        }
    }
    
    @Test
    public void testSelectAllAndAliasWithIndex() throws Exception {
        testSelectAllAndAliasWithIndex(false);
    }

    @Test
    public void testSelectAllAndAliasWithLocalIndex() throws Exception {
        testSelectAllAndAliasWithIndex(true);
    }

    private void testSelectAllAndAliasWithIndex(boolean localIndex) throws Exception {
        String query;
        ResultSet rs;
        
        Properties props = new Properties(TEST_PROPERTIES);
        Connection conn = DriverManager.getConnection(getUrl(), props);
        conn.setAutoCommit(false);
        conn.createStatement().execute("CREATE TABLE " + DATA_TABLE_FULL_NAME + " (k VARCHAR NOT NULL PRIMARY KEY, v1 VARCHAR, v2 VARCHAR)");
        query = "SELECT * FROM " + DATA_TABLE_FULL_NAME;
        rs = conn.createStatement().executeQuery(query);
        assertFalse(rs.next());
        
        if (localIndex) {
            conn.createStatement().execute("CREATE LOCAL INDEX " + INDEX_TABLE_NAME + " ON " + DATA_TABLE_FULL_NAME + " (v2 DESC) INCLUDE (v1)");
        } else {
            conn.createStatement().execute("CREATE INDEX " + INDEX_TABLE_NAME + " ON " + DATA_TABLE_FULL_NAME + " (v2 DESC) INCLUDE (v1)");
        }
        query = "SELECT * FROM " + INDEX_TABLE_FULL_NAME;
        rs = conn.createStatement().executeQuery(query);
        assertFalse(rs.next());

        PreparedStatement stmt = conn.prepareStatement("UPSERT INTO " + DATA_TABLE_FULL_NAME + " VALUES(?,?,?)");
        stmt.setString(1,"a");
        stmt.setString(2, "x");
        stmt.setString(3, "1");
        stmt.execute();
        stmt.setString(1,"b");
        stmt.setString(2, "y");
        stmt.setString(3, "2");
        stmt.execute();
        conn.commit();
        
        query = "SELECT * FROM " + DATA_TABLE_FULL_NAME;
        rs = conn.createStatement().executeQuery("EXPLAIN " + query);
        if(localIndex){
            assertEquals("CLIENT PARALLEL 1-WAY FULL SCAN OVER _LOCAL_IDX_" + DATA_TABLE_FULL_NAME+"\nCLIENT MERGE SORT", QueryUtil.getExplainPlan(rs));
        } else {
            assertEquals("CLIENT PARALLEL 1-WAY FULL SCAN OVER " + INDEX_TABLE_FULL_NAME, QueryUtil.getExplainPlan(rs));
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
        
        query = "SELECT v1 as foo FROM " + DATA_TABLE_FULL_NAME + " WHERE v2 = '1' ORDER BY foo";
        rs = conn.createStatement().executeQuery("EXPLAIN " + query);
        if(localIndex){
            assertEquals("CLIENT PARALLEL 1-WAY RANGE SCAN OVER _LOCAL_IDX_" +DATA_TABLE_FULL_NAME + " [-32768,~'1']\n" + 
                    "    SERVER SORTED BY [V1]\n" + 
                    "CLIENT MERGE SORT", QueryUtil.getExplainPlan(rs));
        } else {
            assertEquals("CLIENT PARALLEL 1-WAY RANGE SCAN OVER " +INDEX_TABLE_FULL_NAME + " [~'1']\n" + 
                    "    SERVER SORTED BY [V1]\n" + 
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
        testSelectCF(false);
    }

    @Test
    public void testSelectCFWithLocalIndex() throws Exception {
        testSelectCF(true);
    }

    private void testSelectCF(boolean localIndex) throws Exception {
        String query;
        ResultSet rs;
        
        Properties props = new Properties(TEST_PROPERTIES);
        Connection conn = DriverManager.getConnection(getUrl(), props);
        conn.setAutoCommit(false);
        conn.createStatement().execute("CREATE TABLE " + DATA_TABLE_FULL_NAME + " (k VARCHAR NOT NULL PRIMARY KEY, a.v1 VARCHAR, a.v2 VARCHAR, b.v1 VARCHAR)  ");
        query = "SELECT * FROM " + DATA_TABLE_FULL_NAME;
        rs = conn.createStatement().executeQuery(query);
        assertFalse(rs.next());
        if(localIndex) {
            conn.createStatement().execute("CREATE LOCAL INDEX " + INDEX_TABLE_NAME + " ON " + DATA_TABLE_FULL_NAME + " (v2 DESC) INCLUDE (a.v1)");
        } else {
            conn.createStatement().execute("CREATE INDEX " + INDEX_TABLE_NAME + " ON " + DATA_TABLE_FULL_NAME + " (v2 DESC) INCLUDE (a.v1)");
        }
        query = "SELECT * FROM " + INDEX_TABLE_FULL_NAME;
        rs = conn.createStatement().executeQuery(query);
        assertFalse(rs.next());

        PreparedStatement stmt = conn.prepareStatement("UPSERT INTO " + DATA_TABLE_FULL_NAME + " VALUES(?,?,?,?)");
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
        
        query = "SELECT * FROM " + DATA_TABLE_FULL_NAME;
        rs = conn.createStatement().executeQuery("EXPLAIN " + query);
        assertEquals("CLIENT PARALLEL 1-WAY FULL SCAN OVER " + DATA_TABLE_FULL_NAME, QueryUtil.getExplainPlan(rs));

        query = "SELECT a.* FROM " + DATA_TABLE_FULL_NAME;
        rs = conn.createStatement().executeQuery("EXPLAIN " + query);
        if(localIndex) {
            assertEquals("CLIENT PARALLEL 1-WAY FULL SCAN OVER _LOCAL_IDX_" + DATA_TABLE_FULL_NAME+"\nCLIENT MERGE SORT", QueryUtil.getExplainPlan(rs));
        } else {
            assertEquals("CLIENT PARALLEL 1-WAY FULL SCAN OVER " + INDEX_TABLE_FULL_NAME, QueryUtil.getExplainPlan(rs));
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
        testCoveredColumns(false);
    }

    @Test
    public void testCoveredColumnsWithLocalIndex() throws Exception {
        testCoveredColumns(true);
    }

    private void testCoveredColumns(boolean localIndex) throws Exception {
        String query;
        ResultSet rs;
        
        Properties props = new Properties(TEST_PROPERTIES);
        Connection conn = DriverManager.getConnection(getUrl(), props);
        conn.setAutoCommit(false);
        conn.createStatement().execute("CREATE TABLE " + DATA_TABLE_FULL_NAME + " (k VARCHAR NOT NULL PRIMARY KEY, v1 VARCHAR, v2 VARCHAR)");
        query = "SELECT * FROM " + DATA_TABLE_FULL_NAME;
        rs = conn.createStatement().executeQuery(query);
        assertFalse(rs.next());
        
        if(localIndex) {
            conn.createStatement().execute("CREATE LOCAL INDEX " + INDEX_TABLE_NAME + " ON " + DATA_TABLE_FULL_NAME + " (v1) INCLUDE (v2)");
        } else {
            conn.createStatement().execute("CREATE INDEX " + INDEX_TABLE_NAME + " ON " + DATA_TABLE_FULL_NAME + " (v1) INCLUDE (v2)");
        }
        query = "SELECT * FROM " + INDEX_TABLE_FULL_NAME;
        rs = conn.createStatement().executeQuery(query);
        assertFalse(rs.next());

        PreparedStatement stmt = conn.prepareStatement("UPSERT INTO " + DATA_TABLE_FULL_NAME + " VALUES(?,?,?)");
        stmt.setString(1,"a");
        stmt.setString(2, "x");
        stmt.setString(3, "1");
        stmt.execute();
        conn.commit();
        
        query = "SELECT * FROM " + INDEX_TABLE_FULL_NAME;
        rs = conn.createStatement().executeQuery(query);
        assertTrue(rs.next());
        assertEquals("x",rs.getString(1));
        assertEquals("a",rs.getString(2));
        assertEquals("1",rs.getString(3));
        assertFalse(rs.next());

        stmt = conn.prepareStatement("UPSERT INTO " + DATA_TABLE_FULL_NAME + "(k,v2) VALUES(?,?)");
        stmt.setString(1,"a");
        stmt.setString(2, null);
        stmt.execute();
        conn.commit();
        
        query = "SELECT * FROM " + INDEX_TABLE_FULL_NAME;
        rs = conn.createStatement().executeQuery(query);
        assertTrue(rs.next());
        assertEquals("x",rs.getString(1));
        assertEquals("a",rs.getString(2));
        assertNull(rs.getString(3));
        assertFalse(rs.next());

        query = "SELECT * FROM " + DATA_TABLE_FULL_NAME;
        rs = conn.createStatement().executeQuery("EXPLAIN " + query);
        if(localIndex) {
            assertEquals("CLIENT PARALLEL 1-WAY FULL SCAN OVER _LOCAL_IDX_" + DATA_TABLE_FULL_NAME+"\nCLIENT MERGE SORT", QueryUtil.getExplainPlan(rs));            
        } else {
            assertEquals("CLIENT PARALLEL 1-WAY FULL SCAN OVER " + INDEX_TABLE_FULL_NAME, QueryUtil.getExplainPlan(rs));
        }

        rs = conn.createStatement().executeQuery(query);
        assertTrue(rs.next());
        assertEquals("a",rs.getString(1));
        assertEquals("x",rs.getString(2));
        assertNull(rs.getString(3));
        assertFalse(rs.next());

        stmt = conn.prepareStatement("UPSERT INTO " + DATA_TABLE_FULL_NAME + "(k,v2) VALUES(?,?)");
        stmt.setString(1,"a");
        stmt.setString(2,"3");
        stmt.execute();
        conn.commit();
        
        query = "SELECT * FROM " + DATA_TABLE_FULL_NAME;
        rs = conn.createStatement().executeQuery("EXPLAIN " + query);
        if(localIndex) {
            assertEquals("CLIENT PARALLEL 1-WAY FULL SCAN OVER _LOCAL_IDX_" + DATA_TABLE_FULL_NAME + "\nCLIENT MERGE SORT", QueryUtil.getExplainPlan(rs));            
        } else {
            assertEquals("CLIENT PARALLEL 1-WAY FULL SCAN OVER " + INDEX_TABLE_FULL_NAME, QueryUtil.getExplainPlan(rs));
        }
        
        rs = conn.createStatement().executeQuery(query);
        assertTrue(rs.next());
        assertEquals("a",rs.getString(1));
        assertEquals("x",rs.getString(2));
        assertEquals("3",rs.getString(3));
        assertFalse(rs.next());

        stmt = conn.prepareStatement("UPSERT INTO " + DATA_TABLE_FULL_NAME + "(k,v2) VALUES(?,?)");
        stmt.setString(1,"a");
        stmt.setString(2,"4");
        stmt.execute();
        conn.commit();
        
        query = "SELECT * FROM " + DATA_TABLE_FULL_NAME;
        rs = conn.createStatement().executeQuery("EXPLAIN " + query);
        if(localIndex) {
            assertEquals("CLIENT PARALLEL 1-WAY FULL SCAN OVER _LOCAL_IDX_" + DATA_TABLE_FULL_NAME+"\nCLIENT MERGE SORT", QueryUtil.getExplainPlan(rs));            
        } else {
            assertEquals("CLIENT PARALLEL 1-WAY FULL SCAN OVER " + INDEX_TABLE_FULL_NAME, QueryUtil.getExplainPlan(rs));
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
        testCompoundIndexKey(false);
    }

    @Test
    public void testCompoundIndexKeyWithLocalIndex() throws Exception {
        testCompoundIndexKey(true);
    }

    private void testCompoundIndexKey(boolean localIndex) throws Exception {
        String query;
        ResultSet rs;
        
        Properties props = new Properties(TEST_PROPERTIES);
        Connection conn = DriverManager.getConnection(getUrl(), props);
        conn.setAutoCommit(false);

        // make sure that the tables are empty, but reachable
        conn.createStatement().execute("CREATE TABLE " + DATA_TABLE_FULL_NAME + " (k VARCHAR NOT NULL PRIMARY KEY, v1 VARCHAR, v2 VARCHAR)");
        query = "SELECT * FROM " + DATA_TABLE_FULL_NAME;
        rs = conn.createStatement().executeQuery(query);
        assertFalse(rs.next());
        if(localIndex) {
            conn.createStatement().execute("CREATE LOCAL INDEX " + INDEX_TABLE_NAME + " ON " + DATA_TABLE_FULL_NAME + " (v1, v2)");
        } else {
            conn.createStatement().execute("CREATE INDEX " + INDEX_TABLE_NAME + " ON " + DATA_TABLE_FULL_NAME + " (v1, v2)");
        }
        query = "SELECT * FROM " + INDEX_TABLE_FULL_NAME;
        rs = conn.createStatement().executeQuery(query);
        assertFalse(rs.next());

        // load some data into the table
        PreparedStatement stmt = conn.prepareStatement("UPSERT INTO " + DATA_TABLE_FULL_NAME + " VALUES(?,?,?)");
        stmt.setString(1,"a");
        stmt.setString(2, "x");
        stmt.setString(3, "1");
        stmt.execute();
        conn.commit();
        
        query = "SELECT * FROM " + INDEX_TABLE_FULL_NAME;
        rs = conn.createStatement().executeQuery(query);
        assertTrue(rs.next());
        assertEquals("x",rs.getString(1));
        assertEquals("1",rs.getString(2));
        assertEquals("a",rs.getString(3));
        assertFalse(rs.next());

        stmt = conn.prepareStatement("UPSERT INTO " + DATA_TABLE_FULL_NAME + " VALUES(?,?,?)");
        stmt.setString(1,"a");
        stmt.setString(2, "y");
        stmt.setString(3, null);
        stmt.execute();
        conn.commit();
        
        query = "SELECT * FROM " + INDEX_TABLE_FULL_NAME;
        rs = conn.createStatement().executeQuery(query);
        assertTrue(rs.next());
        assertEquals("y",rs.getString(1));
        assertNull(rs.getString(2));
        assertEquals("a",rs.getString(3));
        assertFalse(rs.next());

        query = "SELECT * FROM " + DATA_TABLE_FULL_NAME;
        rs = conn.createStatement().executeQuery("EXPLAIN " + query);
        if (localIndex) {
            assertEquals("CLIENT PARALLEL 1-WAY FULL SCAN OVER _LOCAL_IDX_" + DATA_TABLE_FULL_NAME+"\nCLIENT MERGE SORT", QueryUtil.getExplainPlan(rs));
        } else {
            assertEquals("CLIENT PARALLEL 1-WAY FULL SCAN OVER " + INDEX_TABLE_FULL_NAME, QueryUtil.getExplainPlan(rs));
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
        
        query = "SELECT * FROM " + INDEX_TABLE_FULL_NAME;
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
        stmt = conn.prepareStatement("UPSERT INTO " + DATA_TABLE_FULL_NAME + " VALUES(?,?)");
        stmt.setString(1,"b");
        stmt.setString(2, "z");
        stmt.execute();
        conn.commit();
        
        query = "SELECT * FROM " + INDEX_TABLE_FULL_NAME;
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
 
    @Test
    public void testMultipleUpdatesToSingleRow() throws Exception {
        testMultipleUpdatesToSingleRow(false);
    }

    @Test
    public void testMultipleUpdatesToSingleRowWithLocalIndex() throws Exception {
        testMultipleUpdatesToSingleRow(true);
    }
    
    /**
     * There was a case where if there were multiple updates to a single row in the same batch, the
     * index wouldn't be updated correctly as each element of the batch was evaluated with the state
     * previous to the batch, rather than with the rest of the batch. This meant you could do a put
     * and a delete on a row in the same batch and the index result would contain the current + put
     * and current + delete, but not current + put + delete.
     * @throws Exception on failure
     */
    private void testMultipleUpdatesToSingleRow(boolean localIndex) throws Exception {
        String query;
        ResultSet rs;
    
        Properties props = new Properties(TEST_PROPERTIES);
        Connection conn = DriverManager.getConnection(getUrl(), props);
        conn.setAutoCommit(false);
    
        // make sure that the tables are empty, but reachable
        conn.createStatement().execute(
          "CREATE TABLE " + DATA_TABLE_FULL_NAME
              + " (k VARCHAR NOT NULL PRIMARY KEY, v1 VARCHAR, v2 VARCHAR)");
        query = "SELECT * FROM " + DATA_TABLE_FULL_NAME;
        rs = conn.createStatement().executeQuery(query);
        assertFalse(rs.next());

        if(localIndex) {
            conn.createStatement().execute(
                "CREATE LOCAL INDEX " + INDEX_TABLE_NAME + " ON " + DATA_TABLE_FULL_NAME + " (v1, v2)");
        } else {
            conn.createStatement().execute(
                "CREATE INDEX " + INDEX_TABLE_NAME + " ON " + DATA_TABLE_FULL_NAME + " (v1, v2)");
        }
        query = "SELECT * FROM " + INDEX_TABLE_FULL_NAME;
        rs = conn.createStatement().executeQuery(query);
        assertFalse(rs.next());
    
        // load some data into the table
        PreparedStatement stmt =
            conn.prepareStatement("UPSERT INTO " + DATA_TABLE_FULL_NAME + " VALUES(?,?,?)");
        stmt.setString(1, "a");
        stmt.setString(2, "x");
        stmt.setString(3, "1");
        stmt.execute();
        conn.commit();
        
        // make sure the index is working as expected
        query = "SELECT * FROM " + INDEX_TABLE_FULL_NAME;
        rs = conn.createStatement().executeQuery(query);
        assertTrue(rs.next());
        assertEquals("x", rs.getString(1));
        assertEquals("1", rs.getString(2));
        assertEquals("a", rs.getString(3));
        assertFalse(rs.next());
      
        // do multiple updates to the same row, in the same batch
        stmt = conn.prepareStatement("UPSERT INTO " + DATA_TABLE_FULL_NAME + "(k, v1) VALUES(?,?)");
        stmt.setString(1, "a");
        stmt.setString(2, "y");
        stmt.execute();
        stmt = conn.prepareStatement("UPSERT INTO " + DATA_TABLE_FULL_NAME + "(k,v2) VALUES(?,?)");
        stmt.setString(1, "a");
        stmt.setString(2, null);
        stmt.execute();
        conn.commit();
    
        query = "SELECT * FROM " + INDEX_TABLE_FULL_NAME;
        rs = conn.createStatement().executeQuery(query);
        assertTrue(rs.next());
        assertEquals("y", rs.getString(1));
        assertNull(rs.getString(2));
        assertEquals("a", rs.getString(3));
        assertFalse(rs.next());
    
        query = "SELECT * FROM " + DATA_TABLE_FULL_NAME;
        rs = conn.createStatement().executeQuery("EXPLAIN " + query);
        if(localIndex) {
            assertEquals("CLIENT PARALLEL 1-WAY FULL SCAN OVER _LOCAL_IDX_" + DATA_TABLE_FULL_NAME+"\nCLIENT MERGE SORT",
                QueryUtil.getExplainPlan(rs));
        } else {
            assertEquals("CLIENT PARALLEL 1-WAY FULL SCAN OVER " + INDEX_TABLE_FULL_NAME,
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
    
        Properties props = new Properties(TEST_PROPERTIES);
        Connection conn = DriverManager.getConnection(getUrl(), props);
        conn.setAutoCommit(false);
    
        // make sure that the tables are empty, but reachable
        conn.createStatement().execute(
          "CREATE TABLE " + DATA_TABLE_FULL_NAME
              + " (k VARCHAR NOT NULL PRIMARY KEY, v1 VARCHAR, v2 VARCHAR)");
        query = "SELECT * FROM " + DATA_TABLE_FULL_NAME;
        rs = conn.createStatement().executeQuery(query);
        assertFalse(rs.next());
    
        conn.createStatement().execute(
          "CREATE INDEX " + INDEX_TABLE_NAME + " ON " + DATA_TABLE_FULL_NAME + " (v1, v2)");
        query = "SELECT * FROM " + INDEX_TABLE_FULL_NAME;
        rs = conn.createStatement().executeQuery(query);
        assertFalse(rs.next());
    
        // load some data into the table
        PreparedStatement stmt =
            conn.prepareStatement("UPSERT INTO " + DATA_TABLE_FULL_NAME + " VALUES(?,?,?)");
        stmt.setString(1, "a");
        stmt.setString(2, "x");
        stmt.setString(3, "1");
        stmt.execute();
        conn.commit();
        
        // make sure the index is working as expected
        query = "SELECT * FROM " + INDEX_TABLE_FULL_NAME;
        rs = conn.createStatement().executeQuery(query);
        assertTrue(rs.next());
        assertEquals("x", rs.getString(1));
        assertEquals("1", rs.getString(2));
        assertEquals("a", rs.getString(3));
        assertFalse(rs.next());

        String ddl = "DROP INDEX " + INDEX_TABLE_NAME + " ON " + DATA_TABLE_FULL_NAME;
        stmt = conn.prepareStatement(ddl);
        stmt.execute();
        
        stmt = conn.prepareStatement("UPSERT INTO " + DATA_TABLE_FULL_NAME + "(k, v1) VALUES(?,?)");
        stmt.setString(1, "a");
        stmt.setString(2, "y");
        stmt.execute();
        conn.commit();
    
        query = "SELECT * FROM " + DATA_TABLE_FULL_NAME;
    
        // check that the data table matches as expected
        rs = conn.createStatement().executeQuery(query);
        assertTrue(rs.next());
        assertEquals("a", rs.getString(1));
        assertEquals("y", rs.getString(2));
        assertFalse(rs.next());
    }
    
    @Test
    public void testMultipleUpdatesAcrossRegions() throws Exception {
        testMultipleUpdatesAcrossRegions(false);
    }

    @Test
    public void testMultipleUpdatesAcrossRegionsWithLocalIndex() throws Exception {
        testMultipleUpdatesAcrossRegions(true);
    }

    private void testMultipleUpdatesAcrossRegions(boolean localIndex) throws Exception {
        String query;
        ResultSet rs;
    
        Properties props = new Properties(TEST_PROPERTIES);
        Connection conn = DriverManager.getConnection(getUrl(), props);
        conn.setAutoCommit(false);
    
        // make sure that the tables are empty, but reachable
        conn.createStatement().execute(
          "CREATE TABLE " + DATA_TABLE_FULL_NAME
              + " (k VARCHAR NOT NULL PRIMARY KEY, v1 VARCHAR, v2 VARCHAR) " + HTableDescriptor.MAX_FILESIZE + "=1, " + HTableDescriptor.MEMSTORE_FLUSHSIZE + "=1 " +
                  "SPLIT ON ('b')");
        query = "SELECT * FROM " + DATA_TABLE_FULL_NAME;
        rs = conn.createStatement().executeQuery(query);
        assertFalse(rs.next());

        if(localIndex) {
            conn.createStatement().execute(
                "CREATE LOCAL INDEX " + INDEX_TABLE_NAME + " ON " + DATA_TABLE_FULL_NAME + " (v1, v2)");
        } else {
            conn.createStatement().execute(
                "CREATE INDEX " + INDEX_TABLE_NAME + " ON " + DATA_TABLE_FULL_NAME + " (v1, v2)");
        }
        query = "SELECT * FROM " + INDEX_TABLE_FULL_NAME;
        rs = conn.createStatement().executeQuery(query);
        assertFalse(rs.next());
    
        // load some data into the table
        PreparedStatement stmt =
            conn.prepareStatement("UPSERT INTO " + DATA_TABLE_FULL_NAME + " VALUES(?,?,?)");
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
        query = "SELECT * FROM " + INDEX_TABLE_FULL_NAME;
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

        query = "SELECT * FROM " + DATA_TABLE_FULL_NAME;
        rs = conn.createStatement().executeQuery("EXPLAIN " + query);
        if (localIndex) {
            assertEquals("CLIENT PARALLEL 2-WAY FULL SCAN OVER _LOCAL_IDX_" + DATA_TABLE_FULL_NAME+"\nCLIENT MERGE SORT",
                QueryUtil.getExplainPlan(rs));
        } else {
            assertEquals("CLIENT PARALLEL 1-WAY FULL SCAN OVER " + INDEX_TABLE_FULL_NAME,
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
        testIndexWithCaseSensitiveCols(false);
    }

    @Test
    public void testLocalIndexWithCaseSensitiveCols() throws Exception {
        testIndexWithCaseSensitiveCols(true);
    }

    private void testIndexWithCaseSensitiveCols(boolean localIndex) throws Exception {
        String query;
        ResultSet rs;
        
        Properties props = new Properties(TEST_PROPERTIES);
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
                assertEquals("CLIENT PARALLEL 1-WAY RANGE SCAN OVER _LOCAL_IDX_CS [-32768,'1']\nCLIENT MERGE SORT", QueryUtil.getExplainPlan(rs));
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
                assertEquals("CLIENT PARALLEL 1-WAY FULL SCAN OVER _LOCAL_IDX_CS\nCLIENT MERGE SORT",
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
        testInFilterOnIndexedTable(false);
    }
    
    @Test
    public void testInFilterOnLocalIndexedTable() throws Exception {
        testInFilterOnIndexedTable(true);
    }
    
    private void testInFilterOnIndexedTable(boolean localIndex) throws Exception {
        Properties props = new Properties(TEST_PROPERTIES);
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
        testIndexWithDecimalCol(false);
    }

    @Test
    public void testLocalIndexWithDecimalCol() throws Exception {
        testIndexWithDecimalCol(true);
    }

    private void testIndexWithDecimalCol(boolean localIndex) throws Exception {
        Properties props = new Properties(TEST_PROPERTIES);
        Connection conn = DriverManager.getConnection(getUrl(), props);
        conn.setAutoCommit(false);
        try {
            Date date = new Date(System.currentTimeMillis());
            
            createTestTable();
            populateTestTable(date);
            String ddl = null;
            if (localIndex) {
                ddl = "CREATE LOCAL INDEX " + INDEX_TABLE_NAME + " ON " + DATA_TABLE_FULL_NAME + " (decimal_pk) INCLUDE (decimal_col1, decimal_col2)";
            } else {
                ddl = "CREATE INDEX " + INDEX_TABLE_NAME + " ON " + DATA_TABLE_FULL_NAME + " (decimal_pk) INCLUDE (decimal_col1, decimal_col2)";
            }
            PreparedStatement stmt = conn.prepareStatement(ddl);
            stmt.execute();
            
            String query = "SELECT decimal_pk, decimal_col1, decimal_col2 from " + DATA_TABLE_FULL_NAME ;
            ResultSet rs = conn.createStatement().executeQuery("EXPLAIN " + query);
            if(localIndex) {
                assertEquals("CLIENT PARALLEL 1-WAY FULL SCAN OVER _LOCAL_IDX_" + DATA_TABLE_FULL_NAME+"\nCLIENT MERGE SORT", QueryUtil.getExplainPlan(rs));
            } else {
                assertEquals("CLIENT PARALLEL 1-WAY FULL SCAN OVER " + INDEX_TABLE_FULL_NAME, QueryUtil.getExplainPlan(rs));
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
    	Properties props = new Properties(TEST_PROPERTIES);
    	Connection conn = DriverManager.getConnection(getUrl(), props);
    	conn.setAutoCommit(false);
    	try {
    		Statement stmt = conn.createStatement();
    		stmt.execute("CREATE TABLE DEMO(v1 VARCHAR PRIMARY KEY, v2 DOUBLE, v3 VARCHAR)");
    		stmt.execute("CREATE INDEX DEMO_idx ON DEMO (v2) INCLUDE(v3)");
    		
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
