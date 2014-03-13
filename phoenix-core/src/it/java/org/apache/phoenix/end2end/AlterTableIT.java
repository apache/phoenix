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
import static org.apache.phoenix.util.TestUtil.closeConnection;
import static org.apache.phoenix.util.TestUtil.closeStatement;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.math.BigDecimal;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Properties;

import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.client.HTableInterface;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.phoenix.exception.SQLExceptionCode;
import org.apache.phoenix.jdbc.PhoenixConnection;
import org.apache.phoenix.query.QueryConstants;
import org.apache.phoenix.schema.PTableKey;
import org.apache.phoenix.schema.TableNotFoundException;
import org.apache.phoenix.util.IndexUtil;
import org.apache.phoenix.util.SchemaUtil;
import org.junit.Test;



public class AlterTableIT extends BaseHBaseManagedTimeIT {
    public static final String SCHEMA_NAME = "";
    public static final String DATA_TABLE_NAME = "T";
    public static final String INDEX_TABLE_NAME = "I";
    public static final String DATA_TABLE_FULL_NAME = SchemaUtil.getTableName(SCHEMA_NAME, "T");
    public static final String INDEX_TABLE_FULL_NAME = SchemaUtil.getTableName(SCHEMA_NAME, "I");


    @Test
    public void testAlterTableWithVarBinaryKey() throws Exception {
        Properties props = new Properties(TEST_PROPERTIES);
        Connection conn = DriverManager.getConnection(getUrl(), props);
        conn.setAutoCommit(false);
        
        try {
            String ddl = "CREATE TABLE test_table " +
                    "  (a_string varchar not null, a_binary varbinary not null, col1 integer" +
                    "  CONSTRAINT pk PRIMARY KEY (a_string, a_binary))\n";
            createTestTable(getUrl(), ddl);
            
            ddl = "ALTER TABLE test_table ADD b_string VARCHAR NULL PRIMARY KEY";
            PreparedStatement stmt = conn.prepareStatement(ddl);
            stmt.execute();
            fail("Should have caught bad alter.");
        } catch (SQLException e) {
            assertEquals(SQLExceptionCode.VARBINARY_LAST_PK.getErrorCode(), e.getErrorCode());
        } finally {
            conn.close();
        }
    }

    @Test
    public void testAddVarCharColToPK() throws Exception {
        Properties props = new Properties(TEST_PROPERTIES);
        Connection conn = DriverManager.getConnection(getUrl(), props);
        conn.setAutoCommit(false);
        
        try {
            String ddl = "CREATE TABLE test_table " +
                    "  (a_string varchar not null, col1 integer" +
                    "  CONSTRAINT pk PRIMARY KEY (a_string))\n";
            conn.createStatement().execute(ddl);
            
            String dml = "UPSERT INTO test_table VALUES(?)";
            PreparedStatement stmt = conn.prepareStatement(dml);
            stmt.setString(1, "b");
            stmt.execute();
            stmt.setString(1, "a");
            stmt.execute();
            conn.commit();
            
            String query = "SELECT * FROM test_table";
            ResultSet rs = conn.createStatement().executeQuery(query);
            assertTrue(rs.next());
            assertEquals("a",rs.getString(1));
            assertTrue(rs.next());
            assertEquals("b",rs.getString(1));
            assertFalse(rs.next());
            
            ddl = "ALTER TABLE test_table ADD  b_string VARCHAR  NULL PRIMARY KEY  ";
            conn.createStatement().execute(ddl);
            
            query = "SELECT * FROM test_table WHERE a_string = 'a' AND b_string IS NULL";
            rs = conn.createStatement().executeQuery(query);
            assertTrue(rs.next());
            assertEquals("a",rs.getString(1));
            assertFalse(rs.next());
            
            dml = "UPSERT INTO test_table VALUES(?)";
            stmt = conn.prepareStatement(dml);
            stmt.setString(1, "c");
            stmt.execute();
            conn.commit();
           
            query = "SELECT * FROM test_table WHERE a_string = 'c' AND b_string IS NULL";
            rs = conn.createStatement().executeQuery(query);
            assertTrue(rs.next());
            assertEquals("c",rs.getString(1));
            assertFalse(rs.next());
            
            dml = "UPSERT INTO test_table(a_string,col1) VALUES(?,?)";
            stmt = conn.prepareStatement(dml);
            stmt.setString(1, "a");
            stmt.setInt(2, 5);
            stmt.execute();
            conn.commit();
           
            query = "SELECT a_string,col1 FROM test_table WHERE a_string = 'a' AND b_string IS NULL";
            rs = conn.createStatement().executeQuery(query);
            assertTrue(rs.next());
            assertEquals("a",rs.getString(1));
            assertEquals(5,rs.getInt(2)); // TODO: figure out why this flaps
            assertFalse(rs.next());
            
        } finally {
            conn.close();
        }
    }
    

    
    @Test
    public void testAlterColumnFamilyProperty() throws Exception {

        Properties props = new Properties(TEST_PROPERTIES);
        Connection conn = DriverManager.getConnection(getUrl(), props);
        
        String ddl = "CREATE TABLE test_table " +
                "  (a_string varchar not null, col1 integer" +
                "  CONSTRAINT pk PRIMARY KEY (a_string))\n";
        try {
                conn.createStatement().execute(ddl);
              
                conn.createStatement().execute("ALTER TABLE TEST_TABLE ADD col2 integer IN_MEMORY=true");
                
                HTableInterface htable1 = conn.unwrap(PhoenixConnection.class).getQueryServices().getTable(Bytes.toBytes("TEST_TABLE")); 
                HTableDescriptor htableDesciptor1 = htable1.getTableDescriptor();
                HColumnDescriptor hcolumnDescriptor1 = htableDesciptor1.getFamily(QueryConstants.DEFAULT_COLUMN_FAMILY_BYTES);
                assertNotNull(hcolumnDescriptor1);
               
                try {
                    
                    conn.createStatement().execute("ALTER TABLE TEST_TABLE SET IN_MEMORY=false");
                    fail("Should have caught exception.");
                    
                } catch (SQLException e) {
                    assertTrue(e.getMessage(), e.getMessage().contains("ERROR 1025 (42Y84): Unsupported property set in ALTER TABLE command."));
                } 
        }finally {
            conn.close();
        }
     }
  
    private static void assertIndexExists(Connection conn, boolean exists) throws SQLException {
        ResultSet rs = conn.getMetaData().getIndexInfo(null, SCHEMA_NAME, DATA_TABLE_NAME, false, false);
        assertEquals(exists, rs.next());
    }
    
    @Test
    public void testDropIndexedColumn() throws Exception {
        String query;
        ResultSet rs;
        PreparedStatement stmt;
    
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
        stmt = conn.prepareStatement("UPSERT INTO " + DATA_TABLE_FULL_NAME + " VALUES(?,?,?)");
        stmt.setString(1, "a");
        stmt.setString(2, "x");
        stmt.setString(3, "1");
        stmt.execute();
        conn.commit();
        
        assertIndexExists(conn,true);
        conn.createStatement().execute("ALTER TABLE " + DATA_TABLE_FULL_NAME + " DROP COLUMN v1");
        assertIndexExists(conn,false);
        
        query = "SELECT * FROM " + DATA_TABLE_FULL_NAME;
        rs = conn.createStatement().executeQuery(query);
        assertTrue(rs.next());
        assertEquals("a",rs.getString(1));
        assertEquals("1",rs.getString(2));
        assertFalse(rs.next());
        
        // load some data into the table
        stmt = conn.prepareStatement("UPSERT INTO " + DATA_TABLE_FULL_NAME + " VALUES(?,?)");
        stmt.setString(1, "a");
        stmt.setString(2, "2");
        stmt.execute();
        conn.commit();
        
        query = "SELECT * FROM " + DATA_TABLE_FULL_NAME;
        rs = conn.createStatement().executeQuery(query);
        assertTrue(rs.next());
        assertEquals("a",rs.getString(1));
        assertEquals("2",rs.getString(2));
        assertFalse(rs.next());
    }
    
    @Test
    public void testDropCoveredColumn() throws Exception {
        String query;
        ResultSet rs;
        PreparedStatement stmt;
    
        Properties props = new Properties(TEST_PROPERTIES);
        Connection conn = DriverManager.getConnection(getUrl(), props);
        conn.setAutoCommit(false);
    
        // make sure that the tables are empty, but reachable
        conn.createStatement().execute(
          "CREATE TABLE " + DATA_TABLE_FULL_NAME
              + " (k VARCHAR NOT NULL PRIMARY KEY, v1 VARCHAR, v2 VARCHAR, v3 VARCHAR)");
        query = "SELECT * FROM " + DATA_TABLE_FULL_NAME;
        rs = conn.createStatement().executeQuery(query);
        assertFalse(rs.next());
    
        conn.createStatement().execute(
          "CREATE INDEX " + INDEX_TABLE_NAME + " ON " + DATA_TABLE_FULL_NAME + " (v1) include (v2, v3)");
        query = "SELECT * FROM " + INDEX_TABLE_FULL_NAME;
        rs = conn.createStatement().executeQuery(query);
        assertFalse(rs.next());
    
        // load some data into the table
        stmt = conn.prepareStatement("UPSERT INTO " + DATA_TABLE_FULL_NAME + " VALUES(?,?,?,?)");
        stmt.setString(1, "a");
        stmt.setString(2, "x");
        stmt.setString(3, "1");
        stmt.setString(4, "j");
        stmt.execute();
        conn.commit();
        
        assertIndexExists(conn,true);
        conn.createStatement().execute("ALTER TABLE " + DATA_TABLE_FULL_NAME + " DROP COLUMN v2");
        // TODO: verify meta data that we get back to confirm our column was dropped
        assertIndexExists(conn,true);
        
        query = "SELECT * FROM " + DATA_TABLE_FULL_NAME;
        rs = conn.createStatement().executeQuery(query);
        assertTrue(rs.next());
        assertEquals("a",rs.getString(1));
        assertEquals("x",rs.getString(2));
        assertEquals("j",rs.getString(3));
        assertFalse(rs.next());
        
        // load some data into the table
        stmt = conn.prepareStatement("UPSERT INTO " + DATA_TABLE_FULL_NAME + " VALUES(?,?,?)");
        stmt.setString(1, "a");
        stmt.setString(2, "y");
        stmt.setString(3, "k");
        stmt.execute();
        conn.commit();
        
        query = "SELECT * FROM " + DATA_TABLE_FULL_NAME;
        rs = conn.createStatement().executeQuery(query);
        assertTrue(rs.next());
        assertEquals("a",rs.getString(1));
        assertEquals("y",rs.getString(2));
        assertEquals("k",rs.getString(3));
        assertFalse(rs.next());
    }
    
    @Test
    public void testAddPKColumnToTableWithIndex() throws Exception {
        String query;
        ResultSet rs;
        PreparedStatement stmt;
    
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
          "CREATE INDEX " + INDEX_TABLE_NAME + " ON " + DATA_TABLE_FULL_NAME + " (v1) include (v2)");
        query = "SELECT * FROM " + INDEX_TABLE_FULL_NAME;
        rs = conn.createStatement().executeQuery(query);
        assertFalse(rs.next());
    
        // load some data into the table
        stmt = conn.prepareStatement("UPSERT INTO " + DATA_TABLE_FULL_NAME + " VALUES(?,?,?)");
        stmt.setString(1, "a");
        stmt.setString(2, "x");
        stmt.setString(3, "1");
        stmt.execute();
        conn.commit();
        
        assertIndexExists(conn,true);
        conn.createStatement().execute("ALTER TABLE " + DATA_TABLE_FULL_NAME + " ADD v3 VARCHAR, k2 DECIMAL PRIMARY KEY");
        rs = conn.getMetaData().getPrimaryKeys("", SCHEMA_NAME, DATA_TABLE_NAME);
        assertTrue(rs.next());
        assertEquals("K",rs.getString("COLUMN_NAME"));
        assertEquals(1, rs.getShort("KEY_SEQ"));
        assertTrue(rs.next());
        assertEquals("K2",rs.getString("COLUMN_NAME"));
        assertEquals(2, rs.getShort("KEY_SEQ"));

        rs = conn.getMetaData().getPrimaryKeys("", SCHEMA_NAME, INDEX_TABLE_NAME);
        assertTrue(rs.next());
        assertEquals(QueryConstants.DEFAULT_COLUMN_FAMILY + IndexUtil.INDEX_COLUMN_NAME_SEP + "V1",rs.getString("COLUMN_NAME"));
        assertEquals(1, rs.getShort("KEY_SEQ"));
        assertTrue(rs.next());
        assertEquals(IndexUtil.INDEX_COLUMN_NAME_SEP + "K",rs.getString("COLUMN_NAME"));
        assertEquals(2, rs.getShort("KEY_SEQ"));
        assertTrue(rs.next());
        assertEquals(IndexUtil.INDEX_COLUMN_NAME_SEP + "K2",rs.getString("COLUMN_NAME"));
        assertEquals(3, rs.getShort("KEY_SEQ"));
        
        assertIndexExists(conn,true);
        
        query = "SELECT * FROM " + DATA_TABLE_FULL_NAME;
        rs = conn.createStatement().executeQuery(query);
        assertTrue(rs.next());
        assertEquals("a",rs.getString(1));
        assertEquals("x",rs.getString(2));
        assertEquals("1",rs.getString(3));
        assertNull(rs.getBigDecimal(4));
        assertFalse(rs.next());
        
        // load some data into the table
        stmt = conn.prepareStatement("UPSERT INTO " + DATA_TABLE_FULL_NAME + "(K,K2,V1,V2) VALUES(?,?,?,?)");
        stmt.setString(1, "b");
        stmt.setBigDecimal(2, BigDecimal.valueOf(2));
        stmt.setString(3, "y");
        stmt.setString(4, "2");
        stmt.execute();
        conn.commit();
        
        query = "SELECT k,k2 FROM " + DATA_TABLE_FULL_NAME + " WHERE v1='y'";
        rs = conn.createStatement().executeQuery(query);
        assertTrue(rs.next());
        assertEquals("b",rs.getString(1));
        assertEquals(BigDecimal.valueOf(2),rs.getBigDecimal(2));
        assertFalse(rs.next());
    }
    
    @Test
    public void testSetSaltedTableAsImmutable() throws Exception {
        Properties props = new Properties(TEST_PROPERTIES);
        Connection conn = DriverManager.getConnection(getUrl(), props);
        conn.setAutoCommit(false);
        
        try {
            String ddl = "CREATE TABLE MESSAGES (\n" + 
            		"        SENDER_ID UNSIGNED_LONG NOT NULL,\n" + 
            		"        RECIPIENT_ID UNSIGNED_LONG NOT NULL,\n" + 
            		"        M_TIMESTAMP DATE  NOT NULL,\n" + 
            		"        ROW_ID UNSIGNED_LONG NOT NULL,\n" + 
            		"        IS_READ TINYINT,\n" + 
            		"        IS_DELETED TINYINT,\n" + 
            		"        VISIBILITY TINYINT,\n" + 
            		"        B.SENDER_IP VARCHAR,\n" + 
            		"        B.JSON VARCHAR,\n" + 
            		"        B.M_TEXT VARCHAR\n" + 
            		"        CONSTRAINT ROWKEY PRIMARY KEY\n" + 
            		"(SENDER_ID,RECIPIENT_ID,M_TIMESTAMP DESC,ROW_ID))\n" + 
            		"SALT_BUCKETS=4";
            conn.createStatement().execute(ddl);
            
            ddl = "ALTER TABLE MESSAGES SET IMMUTABLE_ROWS=true";
            conn.createStatement().execute(ddl);
            
            conn.createStatement().executeQuery("select count(*) from messages").next();
            
        } finally {
            conn.close();
        }
    }
    
    
    @Test
    public void testDropColumnFromSaltedTable() throws Exception {
        Properties props = new Properties(TEST_PROPERTIES);
        Connection conn = DriverManager.getConnection(getUrl(), props);
        conn.setAutoCommit(false);
        
        try {
            String ddl = "CREATE TABLE MESSAGES (\n" + 
                    "        SENDER_ID UNSIGNED_LONG NOT NULL,\n" + 
                    "        RECIPIENT_ID UNSIGNED_LONG NOT NULL,\n" + 
                    "        M_TIMESTAMP DATE  NOT NULL,\n" + 
                    "        ROW_ID UNSIGNED_LONG NOT NULL,\n" + 
                    "        IS_READ TINYINT,\n" + 
                    "        IS_DELETED TINYINT,\n" + 
                    "        VISIBILITY TINYINT,\n" + 
                    "        B.SENDER_IP VARCHAR,\n" + 
                    "        B.JSON VARCHAR,\n" + 
                    "        B.M_TEXT VARCHAR\n" + 
                    "        CONSTRAINT ROWKEY PRIMARY KEY\n" + 
                    "(SENDER_ID,RECIPIENT_ID,M_TIMESTAMP DESC,ROW_ID))\n" + 
                    "SALT_BUCKETS=4";
            conn.createStatement().execute(ddl);
            
            ddl = "ALTER TABLE MESSAGES DROP COLUMN B.JSON";
            conn.createStatement().execute(ddl);
            
            conn.createStatement().executeQuery("select count(*) from messages").next();
        } finally {
            conn.close();
        }

    }
    
    
    @Test
    public void testAddVarCols() throws Exception {
        Properties props = new Properties(TEST_PROPERTIES);
        Connection conn = DriverManager.getConnection(getUrl(), props);
        conn.setAutoCommit(false);
        
        try {
            String ddl = "CREATE TABLE test_table " +
                    "  (a_string varchar not null, col1 integer" +
                    "  CONSTRAINT pk PRIMARY KEY (a_string))\n";
            conn.createStatement().execute(ddl);
            
            String dml = "UPSERT INTO test_table VALUES(?)";
            PreparedStatement stmt = conn.prepareStatement(dml);
            stmt.setString(1, "b");
            stmt.execute();
            stmt.setString(1, "a");
            stmt.execute();
            conn.commit();
            
            String query = "SELECT * FROM test_table";
            ResultSet rs = conn.createStatement().executeQuery(query);
            assertTrue(rs.next());
            assertEquals("a",rs.getString(1));
            assertTrue(rs.next());
            assertEquals("b",rs.getString(1));
            assertFalse(rs.next());
            
            
            query = "SELECT * FROM test_table WHERE a_string = 'a' ";
            rs = conn.createStatement().executeQuery(query);
            assertTrue(rs.next());
            assertEquals("a",rs.getString(1));
          
            ddl = "ALTER TABLE test_table ADD  c1.col2 VARCHAR  , c1.col3 integer , c2.col4 integer";
            conn.createStatement().execute(ddl);
            
            ddl = "ALTER TABLE test_table ADD   col5 integer , c1.col2 VARCHAR";
            try {
                conn.createStatement().execute(ddl);
                fail();
            } catch (SQLException e) {
                assertEquals(SQLExceptionCode.COLUMN_EXIST_IN_DEF.getErrorCode(), e.getErrorCode());
            }
            
            query = "SELECT col5 FROM test_table";
            try {
                conn.createStatement().executeQuery(query);
                fail(); 
            } catch(SQLException e) {
                assertTrue(e.getMessage(), e.getMessage().contains("ERROR 504 (42703): Undefined column."));
            }
       
            ddl = "ALTER TABLE test_table ADD IF NOT EXISTS col5 integer , c1.col2 VARCHAR";
            conn.createStatement().execute(ddl);
            
            dml = "UPSERT INTO test_table VALUES(?,?,?,?,?)";
            stmt = conn.prepareStatement(dml);
            stmt.setString(1, "c");
            stmt.setInt(2, 100);
            stmt.setString(3, "d");
            stmt.setInt(4, 101);
            stmt.setInt(5, 102);
            stmt.execute();
            conn.commit();
           
            query = "SELECT * FROM test_table WHERE a_string = 'c' ";
            rs = conn.createStatement().executeQuery(query);
            assertTrue(rs.next());
            assertEquals("c",rs.getString(1));
            assertEquals(100,rs.getInt(2));
            assertEquals("d",rs.getString(3));
            assertEquals(101,rs.getInt(4));
            assertEquals(102,rs.getInt(5));
            assertFalse(rs.next());
            
            ddl = "ALTER TABLE test_table ADD  col5 integer";
            conn.createStatement().execute(ddl);
            
            query = "SELECT c1.* FROM test_table WHERE a_string = 'c' ";
            rs = conn.createStatement().executeQuery(query);
            assertTrue(rs.next());
            assertEquals("d",rs.getString(1));
            assertEquals(101,rs.getInt(2));
            assertFalse(rs.next());
            
            
            dml = "UPSERT INTO test_table(a_string,col1,col5) VALUES(?,?,?)";
            stmt = conn.prepareStatement(dml);
            stmt.setString(1, "e");
            stmt.setInt(2, 200);
            stmt.setInt(3, 201);
            stmt.execute();
            conn.commit();
            
            
            query = "SELECT a_string,col1,col5 FROM test_table WHERE a_string = 'e' ";
            rs = conn.createStatement().executeQuery(query);
            assertTrue(rs.next());
            assertEquals("e",rs.getString(1));
            assertEquals(200,rs.getInt(2));
            assertEquals(201,rs.getInt(3));
            assertFalse(rs.next());
            
          } finally {
            conn.close();
        }
    }

    @Test
    public void testDropVarCols() throws Exception {
        Properties props = new Properties(TEST_PROPERTIES);
        Connection conn = DriverManager.getConnection(getUrl(), props);
        conn.setAutoCommit(false);
        try {
            String ddl = "CREATE TABLE test_table " + "  (a_string varchar not null, col1 integer, cf1.col2 integer"
                    + "  CONSTRAINT pk PRIMARY KEY (a_string))\n";
            conn.createStatement().execute(ddl);

            ddl = "ALTER TABLE test_table DROP COLUMN col1";
            conn.createStatement().execute(ddl);

            ddl = "ALTER TABLE test_table DROP COLUMN cf1.col2";
            conn.createStatement().execute(ddl);
        } finally {
            conn.close();
        }
    }
    
    @Test
    public void testDisallowAddingNotNullableColumnNotPartOfPkForExistingTable() throws Exception {
        Properties props = new Properties(TEST_PROPERTIES);
        Connection conn = null;
        PreparedStatement stmt = null;
        try {
            conn = DriverManager.getConnection(getUrl(), props);
            conn.setAutoCommit(false);
            try {
                String ddl = "CREATE TABLE test_table " + "  (a_string varchar not null, col1 integer, cf1.col2 integer"
                        + "  CONSTRAINT pk PRIMARY KEY (a_string))\n";
                stmt = conn.prepareStatement(ddl);
                stmt.execute();
            } finally {
                closeStatement(stmt);
            }
            try {
                stmt = conn.prepareStatement("ALTER TABLE test_table ADD b_string VARCHAR NOT NULL");
                stmt.execute();
                fail("Should have failed since altering a table by adding a non-nullable column is not allowed.");
            } catch (SQLException e) {
                assertEquals(SQLExceptionCode.CANNOT_ADD_NOT_NULLABLE_COLUMN.getErrorCode(), e.getErrorCode());
            } finally {
                closeStatement(stmt);
            }
        } finally {
            closeConnection(conn);
        }
    }

    private void asssertIsWALDisabled(Connection conn, String fullTableName, boolean expectedValue) throws SQLException {
        PhoenixConnection pconn = conn.unwrap(PhoenixConnection.class);
        assertEquals(expectedValue, pconn.getMetaDataCache().getTable(new PTableKey(pconn.getTenantId(), fullTableName)).isWALDisabled());
    }
    
    @Test
    public void testDisableWAL() throws Exception {
        String fullTableName = "TEST_TABLE";
        String fullIndexName = "I";
        Properties props = new Properties(TEST_PROPERTIES);
        Connection conn = DriverManager.getConnection(getUrl(), props);

        try {
            conn.createStatement()
                    .execute(
                            "CREATE TABLE test_table "
                                    + "  (a_string varchar not null, col1 integer, cf1.col2 integer, col3 integer , cf2.col4 integer "
                                    + "  CONSTRAINT pk PRIMARY KEY (a_string)) immutable_rows=true, disable_wal=true ");

            Connection conn2 = DriverManager.getConnection(getUrl(), props);
            String query = "SELECT * FROM test_table";
            ResultSet rs = conn2.createStatement().executeQuery(query);
            assertFalse(rs.next());
            asssertIsWALDisabled(conn2,fullTableName, true);
            conn2.close();
            asssertIsWALDisabled(conn,fullTableName, true);

            conn.createStatement().execute("CREATE INDEX i ON test_table (col1) include (cf1.col2) SALT_BUCKETS=4");
            conn2 = DriverManager.getConnection(getUrl(), props);
            query = "SELECT * FROM i";
            rs = conn2.createStatement().executeQuery(query);
            asssertIsWALDisabled(conn2,fullIndexName, true);
            assertFalse(rs.next());
            conn2.close();
            asssertIsWALDisabled(conn,fullIndexName, true);
            
            conn.createStatement().execute("DROP TABLE test_table");
        } finally {
            conn.close();
        }
        conn = DriverManager.getConnection(getUrl(), props);

        try {
            conn.createStatement()
                    .execute(
                            "CREATE TABLE test_table "
                                    + "  (a_string varchar not null, col1 integer, cf1.col2 integer, col3 integer , cf2.col4 integer "
                                    + "  CONSTRAINT pk PRIMARY KEY (a_string)) immutable_rows=true");

            Connection conn2 = DriverManager.getConnection(getUrl(), props);
            String query = "SELECT * FROM test_table";
            ResultSet rs = conn2.createStatement().executeQuery(query);
            assertFalse(rs.next());
            asssertIsWALDisabled(conn,fullTableName, false);
            conn2.close();
            asssertIsWALDisabled(conn,fullTableName, false);

            conn.createStatement().execute("CREATE INDEX i ON test_table (col1) include (cf1.col2) SALT_BUCKETS=4");
            conn2 = DriverManager.getConnection(getUrl(), props);
            query = "SELECT * FROM i";
            rs = conn2.createStatement().executeQuery(query);
            asssertIsWALDisabled(conn2,fullIndexName, true);
            assertFalse(rs.next());
            conn2.close();
            asssertIsWALDisabled(conn,fullIndexName, true);
            conn.createStatement().execute("DROP TABLE test_table");
        } finally {
            conn.close();
        }
        conn = DriverManager.getConnection(getUrl(), props);

        try {
            conn.createStatement()
                    .execute(
                            "CREATE TABLE test_table "
                                    + "  (a_string varchar not null, col1 integer, cf1.col2 integer, col3 integer , cf2.col4 integer "
                                    + "  CONSTRAINT pk PRIMARY KEY (a_string))");

            Connection conn2 = DriverManager.getConnection(getUrl(), props);
            String query = "SELECT * FROM test_table";
            ResultSet rs = conn2.createStatement().executeQuery(query);
            assertFalse(rs.next());
            asssertIsWALDisabled(conn2,fullTableName, false);
            conn2.close();
            asssertIsWALDisabled(conn,fullTableName, false);

            conn.createStatement().execute("CREATE INDEX i ON test_table (col1) include (cf1.col2) SALT_BUCKETS=4");
            conn2 = DriverManager.getConnection(getUrl(), props);
            query = "SELECT * FROM i";
            rs = conn2.createStatement().executeQuery(query);
            asssertIsWALDisabled(conn2,fullIndexName, false);
            assertFalse(rs.next());
            conn2.close();
            asssertIsWALDisabled(conn,fullIndexName, false);
            
        } finally {
            conn.close();
        }
    }

    @Test
    public void testDropColumnsWithImutability() throws Exception {

        Properties props = new Properties(TEST_PROPERTIES);
        Connection conn = DriverManager.getConnection(getUrl(), props);
        conn.setAutoCommit(false);

        try {
            conn.createStatement()
                    .execute(
                            "CREATE TABLE test_table "
                                    + "  (a_string varchar not null, col1 integer, cf1.col2 integer, col3 integer , cf2.col4 integer "
                                    + "  CONSTRAINT pk PRIMARY KEY (a_string)) immutable_rows=true , SALT_BUCKETS=3 ");

            String query = "SELECT * FROM test_table";
            ResultSet rs = conn.createStatement().executeQuery(query);
            assertFalse(rs.next());

            conn.createStatement().execute("CREATE INDEX i ON test_table (col1) include (cf1.col2) SALT_BUCKETS=4");
            query = "SELECT * FROM i";
            rs = conn.createStatement().executeQuery(query);
            assertFalse(rs.next());

            String dml = "UPSERT INTO test_table VALUES(?,?,?,?,?)";
            PreparedStatement stmt = conn.prepareStatement(dml);
            stmt.setString(1, "b");
            stmt.setInt(2, 10);
            stmt.setInt(3, 20);
            stmt.setInt(4, 30);
            stmt.setInt(5, 40);
            stmt.execute();
            stmt.setString(1, "a");
            stmt.setInt(2, 101);
            stmt.setInt(3, 201);
            stmt.setInt(4, 301);
            stmt.setInt(5, 401);
            stmt.execute();
            conn.commit();

            query = "SELECT * FROM test_table order by col1";
            rs = conn.createStatement().executeQuery(query);
            assertTrue(rs.next());
            assertEquals("b", rs.getString(1));
            assertTrue(rs.next());
            assertEquals("a", rs.getString(1));
            assertFalse(rs.next());

            String ddl = "ALTER TABLE test_table DROP COLUMN IF EXISTS col2,col3";
            conn.createStatement().execute(ddl);
            
            ddl = "ALTER TABLE test_table DROP COLUMN a_string,col1";
            try{
                conn.createStatement().execute(ddl);
                fail();
            } catch (SQLException e) {
                assertEquals(SQLExceptionCode.CANNOT_DROP_PK.getErrorCode(), e.getErrorCode());
            }
            
            ddl = "ALTER TABLE test_table DROP COLUMN col4,col5";
            try {
                conn.createStatement().execute(ddl);
                fail();
            } catch (SQLException e) {
                assertEquals(SQLExceptionCode.COLUMN_NOT_FOUND.getErrorCode(), e.getErrorCode());
                assertTrue(e.getMessage(), e.getMessage().contains("ERROR 504 (42703): Undefined column. columnName=COL5"));
            } 

            ddl = "ALTER TABLE test_table DROP COLUMN IF EXISTS col1";
            conn.createStatement().execute(ddl);
            
            query = "SELECT * FROM i";
            try {
                rs = conn.createStatement().executeQuery(query);
                fail();
            } catch (TableNotFoundException e) {}
            
            query = "select col4 FROM test_table";
            rs = conn.createStatement().executeQuery(query);
            assertTrue(rs.next());
            assertTrue(rs.next());

            query = "select col2,col3 FROM test_table";
            try {
                rs = conn.createStatement().executeQuery(query);
                fail();
            } catch (SQLException e) {
                assertEquals(SQLExceptionCode.COLUMN_NOT_FOUND.getErrorCode(), e.getErrorCode());
            }
              
        } finally {
            conn.close();
        }
    }
   
 }
