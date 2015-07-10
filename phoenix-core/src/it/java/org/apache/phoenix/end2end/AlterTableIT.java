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

import static org.apache.hadoop.hbase.HColumnDescriptor.DEFAULT_REPLICATION_SCOPE;
import static org.apache.phoenix.exception.SQLExceptionCode.CANNOT_MUTATE_TABLE;
import static org.apache.phoenix.util.TestUtil.TEST_PROPERTIES;
import static org.apache.phoenix.util.TestUtil.closeConnection;
import static org.apache.phoenix.util.TestUtil.closeStatement;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.math.BigDecimal;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.KeepDeletedCells;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.phoenix.compile.QueryPlan;
import org.apache.phoenix.coprocessor.MetaDataProtocol;
import org.apache.phoenix.exception.SQLExceptionCode;
import org.apache.phoenix.jdbc.PhoenixConnection;
import org.apache.phoenix.jdbc.PhoenixDatabaseMetaData;
import org.apache.phoenix.jdbc.PhoenixStatement;
import org.apache.phoenix.query.QueryConstants;
import org.apache.phoenix.schema.PColumn;
import org.apache.phoenix.schema.PTable;
import org.apache.phoenix.schema.PTableKey;
import org.apache.phoenix.schema.PTableType;
import org.apache.phoenix.schema.TableNotFoundException;
import org.apache.phoenix.util.IndexUtil;
import org.apache.phoenix.util.PhoenixRuntime;
import org.apache.phoenix.util.PropertiesUtil;
import org.apache.phoenix.util.ReadOnlyProps;
import org.apache.phoenix.util.SchemaUtil;
import org.junit.BeforeClass;
import org.junit.Test;

import com.google.common.base.Objects;

/**
 *
 * A lot of tests in this class test HBase level properties. As a result,
 * tests need to have non-overlapping table names. The option of
 * disabling and dropping underlying HBase tables at the end of each test
 * to avoid the overlap makes the test class really slow. By having the
 * test class run in its own cluster and having non overlapping table names
 * we don't need to worry about dropping the tables between each test
 * or at the end of test class.
 *
 */
public class AlterTableIT extends BaseOwnClusterHBaseManagedTimeIT {
    public static final String SCHEMA_NAME = "";
    public static final String DATA_TABLE_NAME = "T";
    public static final String INDEX_TABLE_NAME = "I";
    public static final String LOCAL_INDEX_TABLE_NAME = "LI";
    public static final String DATA_TABLE_FULL_NAME = SchemaUtil.getTableName(SCHEMA_NAME, "T");
    public static final String INDEX_TABLE_FULL_NAME = SchemaUtil.getTableName(SCHEMA_NAME, "I");
    public static final String LOCAL_INDEX_TABLE_FULL_NAME = SchemaUtil.getTableName(SCHEMA_NAME, "LI");

    @BeforeClass
    public static void doSetup() throws Exception {
        Map<String, String> props = Collections.emptyMap();
        setUpTestDriver(new ReadOnlyProps(props.entrySet().iterator()));
    }

    @Test
    public void testAlterTableWithVarBinaryKey() throws Exception {
        Properties props = PropertiesUtil.deepCopy(TEST_PROPERTIES);
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
    public void testAddColsIntoSystemTable() throws Exception {
        Properties props = PropertiesUtil.deepCopy(TEST_PROPERTIES);
        props.setProperty(PhoenixRuntime.CURRENT_SCN_ATTRIB,
                Long.toString(MetaDataProtocol.MIN_SYSTEM_TABLE_TIMESTAMP + 1));
        Connection conn = DriverManager.getConnection(getUrl(), props);

      try{
        conn.createStatement().executeUpdate("ALTER TABLE " + PhoenixDatabaseMetaData.SYSTEM_CATALOG +
          " ADD IF NOT EXISTS testNewColumn integer");
        String query = "SELECT testNewColumn FROM " + PhoenixDatabaseMetaData.SYSTEM_CATALOG;
        try {
          conn.createStatement().executeQuery(query);
        } catch(SQLException e) {
          assertFalse("testNewColumn wasn't created successfully:" + e, true);
        }
      } finally {
        conn.close();
      }
    }

    @Test
    public void testDropSystemTable() throws Exception {
        Properties props = PropertiesUtil.deepCopy(TEST_PROPERTIES);
        Connection conn = DriverManager.getConnection(getUrl(), props);

        try {
            try {
                conn.createStatement().executeUpdate(
                        "DROP TABLE " + PhoenixDatabaseMetaData.SYSTEM_CATALOG);
                fail("Should not be allowed to drop a system table");
            } catch (SQLException e) {
                assertEquals(SQLExceptionCode.CANNOT_MUTATE_TABLE.getErrorCode(), e.getErrorCode());
            }
        } finally {
            conn.close();
        }
    }

    @Test
    public void testAddVarCharColToPK() throws Exception {
        Properties props = PropertiesUtil.deepCopy(TEST_PROPERTIES);
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
    public void testSetPropertyAndAddColumnForNewColumnFamily() throws Exception {
        Properties props = PropertiesUtil.deepCopy(TEST_PROPERTIES);
        Connection conn = DriverManager.getConnection(getUrl(), props);
        String ddl = "CREATE TABLE SETPROPNEWCF " +
                "  (a_string varchar not null, col1 integer" +
                "  CONSTRAINT pk PRIMARY KEY (a_string))\n";
        try {
            conn.createStatement().execute(ddl);
            conn.createStatement().execute("ALTER TABLE SETPROPNEWCF ADD CF.col2 integer CF.IN_MEMORY=true");
            try (HBaseAdmin admin = conn.unwrap(PhoenixConnection.class).getQueryServices().getAdmin()) {
                HColumnDescriptor[] columnFamilies = admin.getTableDescriptor(Bytes.toBytes("SETPROPNEWCF")).getColumnFamilies();
                assertEquals(2, columnFamilies.length);
                assertEquals("0", columnFamilies[0].getNameAsString());
                assertFalse(columnFamilies[0].isInMemory());
                assertEquals("CF", columnFamilies[1].getNameAsString());
                assertTrue(columnFamilies[1].isInMemory());
            }
        } finally {
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

        Properties props = PropertiesUtil.deepCopy(TEST_PROPERTIES);
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
        conn.createStatement().execute(
            "CREATE LOCAL INDEX " + LOCAL_INDEX_TABLE_NAME + " ON " + DATA_TABLE_FULL_NAME + " (v1, v2)");

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
        ResultSet rs;
        PreparedStatement stmt;

        Properties props = PropertiesUtil.deepCopy(TEST_PROPERTIES);
        Connection conn = DriverManager.getConnection(getUrl(), props);
        conn.setAutoCommit(false);

        // make sure that the tables are empty, but reachable
        conn.createStatement().execute(
          "CREATE TABLE " + DATA_TABLE_FULL_NAME
              + " (k VARCHAR NOT NULL PRIMARY KEY, v1 VARCHAR, v2 VARCHAR, v3 VARCHAR)");
        String dataTableQuery = "SELECT * FROM " + DATA_TABLE_FULL_NAME;
        rs = conn.createStatement().executeQuery(dataTableQuery);
        assertFalse(rs.next());

        conn.createStatement().execute(
          "CREATE INDEX " + INDEX_TABLE_NAME + " ON " + DATA_TABLE_FULL_NAME + " (v1) include (v2, v3)");
        conn.createStatement().execute(
            "CREATE LOCAL INDEX " + LOCAL_INDEX_TABLE_NAME + " ON " + DATA_TABLE_FULL_NAME + " (v1) include (v2, v3)");
        rs = conn.createStatement().executeQuery(dataTableQuery);
        assertFalse(rs.next());
        String indexTableQuery = "SELECT * FROM " + INDEX_TABLE_NAME;
        rs = conn.createStatement().executeQuery(indexTableQuery);
        assertFalse(rs.next());
        String localIndexTableQuery = "SELECT * FROM " + LOCAL_INDEX_TABLE_FULL_NAME;
        rs = conn.createStatement().executeQuery(localIndexTableQuery);
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
        assertIndexExists(conn,true);

        // verify data table rows
        rs = conn.createStatement().executeQuery(dataTableQuery);
        assertTrue(rs.next());
        assertEquals("a",rs.getString(1));
        assertEquals("x",rs.getString(2));
        assertEquals("j",rs.getString(3));
        assertFalse(rs.next());
        
        // verify index table rows
        rs = conn.createStatement().executeQuery(indexTableQuery);
        assertTrue(rs.next());
        assertEquals("x",rs.getString(1));
        assertEquals("a",rs.getString(2));
        assertEquals("j",rs.getString(3));
        assertFalse(rs.next());
        
        // verify local index table rows
        rs = conn.createStatement().executeQuery(localIndexTableQuery);
        assertTrue(rs.next());
        assertEquals("x",rs.getString(1));
        assertEquals("a",rs.getString(2));
        assertEquals("j",rs.getString(3));
        assertFalse(rs.next());

        // load some data into the table
        stmt = conn.prepareStatement("UPSERT INTO " + DATA_TABLE_FULL_NAME + " VALUES(?,?,?)");
        stmt.setString(1, "a");
        stmt.setString(2, "y");
        stmt.setString(3, "k");
        stmt.execute();
        conn.commit();

        // verify data table rows
        rs = conn.createStatement().executeQuery(dataTableQuery);
        assertTrue(rs.next());
        assertEquals("a",rs.getString(1));
        assertEquals("y",rs.getString(2));
        assertEquals("k",rs.getString(3));
        assertFalse(rs.next());
        
        // verify index table rows
        rs = conn.createStatement().executeQuery(indexTableQuery);
        assertTrue(rs.next());
        assertEquals("y",rs.getString(1));
        assertEquals("a",rs.getString(2));
        assertEquals("k",rs.getString(3));
        assertFalse(rs.next());
        
        // verify local index table rows
        rs = conn.createStatement().executeQuery(localIndexTableQuery);
        assertTrue(rs.next());
        assertEquals("y",rs.getString(1));
        assertEquals("a",rs.getString(2));
        assertEquals("k",rs.getString(3));
        assertFalse(rs.next());
    }

    @Test
    public void testAddPKColumnToTableWithIndex() throws Exception {
        String query;
        ResultSet rs;
        PreparedStatement stmt;

        Properties props = PropertiesUtil.deepCopy(TEST_PROPERTIES);
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
        conn.createStatement().execute("ALTER TABLE " + DATA_TABLE_FULL_NAME + " ADD v3 VARCHAR, k2 DECIMAL PRIMARY KEY, k3 DECIMAL PRIMARY KEY");
        rs = conn.getMetaData().getPrimaryKeys("", SCHEMA_NAME, DATA_TABLE_NAME);
        assertTrue(rs.next());
        assertEquals("K",rs.getString("COLUMN_NAME"));
        assertEquals(1, rs.getShort("KEY_SEQ"));
        assertTrue(rs.next());
        assertEquals("K2",rs.getString("COLUMN_NAME"));
        assertEquals(2, rs.getShort("KEY_SEQ"));
        assertTrue(rs.next());
        assertEquals("K3",rs.getString("COLUMN_NAME"));
        assertEquals(3, rs.getShort("KEY_SEQ"));
        assertFalse(rs.next());

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
        assertTrue(rs.next());
        assertEquals(IndexUtil.INDEX_COLUMN_NAME_SEP + "K3",rs.getString("COLUMN_NAME"));
        assertEquals(4, rs.getShort("KEY_SEQ"));
        assertFalse(rs.next());

        query = "SELECT * FROM " + DATA_TABLE_FULL_NAME;
        rs = conn.createStatement().executeQuery(query);
        assertTrue(rs.next());
        assertEquals("a",rs.getString(1));
        assertEquals("x",rs.getString(2));
        assertEquals("1",rs.getString(3));
        assertNull(rs.getBigDecimal(4));
        assertFalse(rs.next());

        // load some data into the table
        stmt = conn.prepareStatement("UPSERT INTO " + DATA_TABLE_FULL_NAME + "(K,K2,V1,V2,K3) VALUES(?,?,?,?,?)");
        stmt.setString(1, "b");
        stmt.setBigDecimal(2, BigDecimal.valueOf(2));
        stmt.setString(3, "y");
        stmt.setString(4, "2");
        stmt.setBigDecimal(5, BigDecimal.valueOf(3));
        stmt.execute();
        conn.commit();

        query = "SELECT k,k2,k3 FROM " + DATA_TABLE_FULL_NAME + " WHERE v1='y'";
        rs = conn.createStatement().executeQuery(query);
        assertTrue(rs.next());
        assertEquals("b",rs.getString(1));
        assertEquals(BigDecimal.valueOf(2),rs.getBigDecimal(2));
        assertEquals(BigDecimal.valueOf(3),rs.getBigDecimal(3));
        assertFalse(rs.next());
    }

    @Test
    public void testSetSaltedTableAsImmutable() throws Exception {
        Properties props = PropertiesUtil.deepCopy(TEST_PROPERTIES);
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
        Properties props = PropertiesUtil.deepCopy(TEST_PROPERTIES);
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
        Properties props = PropertiesUtil.deepCopy(TEST_PROPERTIES);
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
        Properties props = PropertiesUtil.deepCopy(TEST_PROPERTIES);
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
        Properties props = PropertiesUtil.deepCopy(TEST_PROPERTIES);
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
        Properties props = PropertiesUtil.deepCopy(TEST_PROPERTIES);
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
            asssertIsWALDisabled(conn2,fullIndexName, false);
            assertFalse(rs.next());
            conn2.close();
            asssertIsWALDisabled(conn,fullIndexName, false);

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
            asssertIsWALDisabled(conn2,fullIndexName, false);
            assertFalse(rs.next());
            conn2.close();
            asssertIsWALDisabled(conn,fullIndexName, false);
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

        Properties props = PropertiesUtil.deepCopy(TEST_PROPERTIES);
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

    @Test
    public void alterTableFromDifferentClient() throws Exception {
        Properties props = PropertiesUtil.deepCopy(TEST_PROPERTIES);
        Connection conn3 = DriverManager.getConnection(getUrl(), props);

        // here we insert into the orig schema with one column
        Connection conn1 = DriverManager.getConnection(getUrl(), props);
        conn1.createStatement().execute("create table test_simpletable(id VARCHAR PRIMARY KEY, field1 BIGINT)");
        PreparedStatement stmtInsert1 = conn1.prepareStatement("upsert into test_simpletable (id, field1) values ( ?, ?)");
        stmtInsert1.setString(1, "key1");
        stmtInsert1.setLong(2, 1L);
        stmtInsert1.execute();
        conn1.commit();
        stmtInsert1.close();
        conn1.close();

        // Do the alter through a separate client.
        conn3.createStatement().execute("alter table test_simpletable add field2 BIGINT");

        //Connection conn1 = DriverManager.getConnection(getUrl(), props);
        PreparedStatement pstmt2 = conn1.prepareStatement("upsert into test_simpletable (id, field1, field2) values ( ?, ?, ?)");
        pstmt2.setString(1, "key2");
        pstmt2.setLong(2, 2L);
        pstmt2.setLong(3, 2L);
        pstmt2.execute();
        conn1.commit();
        pstmt2.close();
        conn1.close();
    }

    @Test
    public void testAddColumnsUsingNewConnection() throws Exception {
        Properties props = PropertiesUtil.deepCopy(TEST_PROPERTIES);
        String ddl = "CREATE TABLE T (\n"
                +"ID1 VARCHAR(15) NOT NULL,\n"
                +"ID2 VARCHAR(15) NOT NULL,\n"
                +"CREATED_DATE DATE,\n"
                +"CREATION_TIME BIGINT,\n"
                +"LAST_USED DATE,\n"
                +"CONSTRAINT PK PRIMARY KEY (ID1, ID2))";
        Connection conn1 = DriverManager.getConnection(getUrl(), props);
        conn1.createStatement().execute(ddl);
        ddl = "ALTER TABLE T ADD STRING VARCHAR, STRING_DATA_TYPES VARCHAR";
        conn1.createStatement().execute(ddl);
        ddl = "ALTER TABLE T DROP COLUMN STRING, STRING_DATA_TYPES";
        conn1.createStatement().execute(ddl);
        ddl = "ALTER TABLE T ADD STRING_ARRAY1 VARCHAR[]";
        conn1.createStatement().execute(ddl);
        conn1.close();
    }

    @Test
    public void testAddColumnForNewColumnFamily() throws Exception {
        Properties props = PropertiesUtil.deepCopy(TEST_PROPERTIES);
        String ddl = "CREATE TABLE testAddColumnForNewColumnFamily (\n"
                +"ID1 VARCHAR(15) NOT NULL,\n"
                +"ID2 VARCHAR(15) NOT NULL,\n"
                +"CREATED_DATE DATE,\n"
                +"CREATION_TIME BIGINT,\n"
                +"LAST_USED DATE,\n"
                +"CONSTRAINT PK PRIMARY KEY (ID1, ID2)) SALT_BUCKETS = 8";
        Connection conn1 = DriverManager.getConnection(getUrl(), props);
        conn1.createStatement().execute(ddl);
        ddl = "ALTER TABLE testAddColumnForNewColumnFamily ADD CF.STRING VARCHAR";
        conn1.createStatement().execute(ddl);
        try (HBaseAdmin admin = conn1.unwrap(PhoenixConnection.class).getQueryServices().getAdmin()) {
            HColumnDescriptor[] columnFamilies = admin.getTableDescriptor(Bytes.toBytes("testAddColumnForNewColumnFamily".toUpperCase())).getColumnFamilies();
            assertEquals(2, columnFamilies.length);
            assertEquals("0", columnFamilies[0].getNameAsString());
            assertEquals(HColumnDescriptor.DEFAULT_TTL, columnFamilies[0].getTimeToLive());
            assertEquals("CF", columnFamilies[1].getNameAsString());
            assertEquals(HColumnDescriptor.DEFAULT_TTL, columnFamilies[1].getTimeToLive());
        }
    }

    @Test
    public void testSetHColumnProperties() throws Exception {
        Properties props = PropertiesUtil.deepCopy(TEST_PROPERTIES);
        String ddl = "CREATE TABLE T1 (\n"
                +"ID1 VARCHAR(15) NOT NULL,\n"
                +"ID2 VARCHAR(15) NOT NULL,\n"
                +"CREATED_DATE DATE,\n"
                +"CREATION_TIME BIGINT,\n"
                +"LAST_USED DATE,\n"
                +"CONSTRAINT PK PRIMARY KEY (ID1, ID2)) SALT_BUCKETS = 8";
        Connection conn1 = DriverManager.getConnection(getUrl(), props);
        conn1.createStatement().execute(ddl);
        ddl = "ALTER TABLE T1 SET REPLICATION_SCOPE=1";
        conn1.createStatement().execute(ddl);
        try (HBaseAdmin admin = conn1.unwrap(PhoenixConnection.class).getQueryServices().getAdmin()) {
            HColumnDescriptor[] columnFamilies = admin.getTableDescriptor(Bytes.toBytes("T1")).getColumnFamilies();
            assertEquals(1, columnFamilies.length);
            assertEquals("0", columnFamilies[0].getNameAsString());
            assertEquals(1, columnFamilies[0].getScope());
        }
    }

    @Test
    public void testSetHTableProperties() throws Exception {
        Properties props = PropertiesUtil.deepCopy(TEST_PROPERTIES);
        String ddl = "CREATE TABLE T2 (\n"
                +"ID1 VARCHAR(15) NOT NULL,\n"
                +"ID2 VARCHAR(15) NOT NULL,\n"
                +"CREATED_DATE DATE,\n"
                +"CREATION_TIME BIGINT,\n"
                +"LAST_USED DATE,\n"
                +"CONSTRAINT PK PRIMARY KEY (ID1, ID2)) SALT_BUCKETS = 8";
        Connection conn1 = DriverManager.getConnection(getUrl(), props);
        conn1.createStatement().execute(ddl);
        ddl = "ALTER TABLE T2 SET COMPACTION_ENABLED=FALSE";
        conn1.createStatement().execute(ddl);
        try (HBaseAdmin admin = conn1.unwrap(PhoenixConnection.class).getQueryServices().getAdmin()) {
            HTableDescriptor tableDesc = admin.getTableDescriptor(Bytes.toBytes("T2"));
            assertEquals(1, tableDesc.getColumnFamilies().length);
            assertEquals("0", tableDesc.getColumnFamilies()[0].getNameAsString());
            assertEquals(Boolean.toString(false), tableDesc.getValue(HTableDescriptor.COMPACTION_ENABLED));
        }
    }

    @Test
    public void testSetHTableAndHColumnProperties() throws Exception {
        Properties props = PropertiesUtil.deepCopy(TEST_PROPERTIES);
        String ddl = "CREATE TABLE T3 (\n"
                +"ID1 VARCHAR(15) NOT NULL,\n"
                +"ID2 VARCHAR(15) NOT NULL,\n"
                +"CREATED_DATE DATE,\n"
                +"CREATION_TIME BIGINT,\n"
                +"LAST_USED DATE,\n"
                +"CONSTRAINT PK PRIMARY KEY (ID1, ID2)) SALT_BUCKETS = 8";
        Connection conn1 = DriverManager.getConnection(getUrl(), props);
        conn1.createStatement().execute(ddl);
        ddl = "ALTER TABLE T3 SET COMPACTION_ENABLED = FALSE, REPLICATION_SCOPE = 1";
        conn1.createStatement().execute(ddl);
        try (HBaseAdmin admin = conn1.unwrap(PhoenixConnection.class).getQueryServices().getAdmin()) {
            HTableDescriptor tableDesc = admin.getTableDescriptor(Bytes.toBytes("T3"));
            HColumnDescriptor[] columnFamilies = tableDesc.getColumnFamilies();
            assertEquals(1, columnFamilies.length);
            assertEquals("0", columnFamilies[0].getNameAsString());
            assertEquals(1, columnFamilies[0].getScope());
            assertEquals(false, tableDesc.isCompactionEnabled());
        }
    }

    @Test
    public void testSetHTableHColumnAndPhoenixTableProperties() throws Exception {
        Properties props = PropertiesUtil.deepCopy(TEST_PROPERTIES);
        String ddl = "CREATE TABLE T3 (\n"
                +"ID1 VARCHAR(15) NOT NULL,\n"
                +"ID2 VARCHAR(15) NOT NULL,\n"
                +"CREATED_DATE DATE,\n"
                +"CF1.CREATION_TIME BIGINT,\n"
                +"CF2.LAST_USED DATE,\n"
                +"CONSTRAINT PK PRIMARY KEY (ID1, ID2)) IMMUTABLE_ROWS=true";
        Connection conn = DriverManager.getConnection(getUrl(), props);
        conn.createStatement().execute(ddl);
        assertImmutableRows(conn, "T3", true);
        ddl = "ALTER TABLE T3 SET COMPACTION_ENABLED = FALSE, VERSIONS = 10";
        conn.createStatement().execute(ddl);
        ddl = "ALTER TABLE T3 SET COMPACTION_ENABLED = FALSE, CF1.MIN_VERSIONS = 1, CF2.MIN_VERSIONS = 3, MIN_VERSIONS = 8, IMMUTABLE_ROWS=false, CF1.KEEP_DELETED_CELLS = true, KEEP_DELETED_CELLS = false";
        conn.createStatement().execute(ddl);
        assertImmutableRows(conn, "T3", false);

        try (HBaseAdmin admin = conn.unwrap(PhoenixConnection.class).getQueryServices().getAdmin()) {
            HTableDescriptor tableDesc = admin.getTableDescriptor(Bytes.toBytes("T3"));
            HColumnDescriptor[] columnFamilies = tableDesc.getColumnFamilies();
            assertEquals(3, columnFamilies.length);

            assertEquals("0", columnFamilies[0].getNameAsString());
            assertEquals(8, columnFamilies[0].getMinVersions());
            assertEquals(10, columnFamilies[0].getMaxVersions());
            assertEquals(KeepDeletedCells.FALSE, columnFamilies[0].getKeepDeletedCells());

            assertEquals("CF1", columnFamilies[1].getNameAsString());
            assertEquals(1, columnFamilies[1].getMinVersions());
            assertEquals(10, columnFamilies[1].getMaxVersions());
            assertEquals(KeepDeletedCells.TRUE, columnFamilies[1].getKeepDeletedCells());

            assertEquals("CF2", columnFamilies[2].getNameAsString());
            assertEquals(3, columnFamilies[2].getMinVersions());
            assertEquals(10, columnFamilies[2].getMaxVersions());
            assertEquals(KeepDeletedCells.FALSE, columnFamilies[2].getKeepDeletedCells());

            assertEquals(Boolean.toString(false), tableDesc.getValue(HTableDescriptor.COMPACTION_ENABLED));
        }
    }

    @Test
    public void testSpecifyingColumnFamilyForHTablePropertyFails() throws Exception {
        Properties props = PropertiesUtil.deepCopy(TEST_PROPERTIES);
        String ddl = "CREATE TABLE T4 (\n"
                +"ID1 VARCHAR(15) NOT NULL,\n"
                +"ID2 VARCHAR(15) NOT NULL,\n"
                +"CREATED_DATE DATE,\n"
                +"CREATION_TIME BIGINT,\n"
                +"LAST_USED DATE,\n"
                +"CONSTRAINT PK PRIMARY KEY (ID1, ID2)) SALT_BUCKETS = 8";
        Connection conn1 = DriverManager.getConnection(getUrl(), props);
        conn1.createStatement().execute(ddl);
        ddl = "ALTER TABLE T4 SET CF.COMPACTION_ENABLED = FALSE";
        try {
            conn1.createStatement().execute(ddl);
            fail();
        } catch (SQLException e) {
            assertEquals(SQLExceptionCode.COLUMN_FAMILY_NOT_ALLOWED_TABLE_PROPERTY.getErrorCode(), e.getErrorCode());
        }
    }

    @Test
    public void testSpecifyingColumnFamilyForPhoenixTablePropertyFails() throws Exception {
        Properties props = PropertiesUtil.deepCopy(TEST_PROPERTIES);
        String ddl = "CREATE TABLE T5 (\n"
                +"ID1 VARCHAR(15) NOT NULL,\n"
                +"ID2 VARCHAR(15) NOT NULL,\n"
                +"CREATED_DATE DATE,\n"
                +"CREATION_TIME BIGINT,\n"
                +"LAST_USED DATE,\n"
                +"CONSTRAINT PK PRIMARY KEY (ID1, ID2)) SALT_BUCKETS = 8";
        Connection conn1 = DriverManager.getConnection(getUrl(), props);
        conn1.createStatement().execute(ddl);
        ddl = "ALTER TABLE T5 SET CF.DISABLE_WAL = TRUE";
        try {
            conn1.createStatement().execute(ddl);
            fail();
        } catch (SQLException e) {
            assertEquals(SQLExceptionCode.COLUMN_FAMILY_NOT_ALLOWED_TABLE_PROPERTY.getErrorCode(), e.getErrorCode());
        }
    }

    @Test
    public void testSpecifyingColumnFamilyForTTLFails() throws Exception {
        Properties props = PropertiesUtil.deepCopy(TEST_PROPERTIES);
        String ddl = "CREATE TABLE T6 (\n"
                +"ID1 VARCHAR(15) NOT NULL,\n"
                +"ID2 VARCHAR(15) NOT NULL,\n"
                +"CREATED_DATE DATE,\n"
                +"CREATION_TIME BIGINT,\n"
                +"CF.LAST_USED DATE,\n"
                +"CONSTRAINT PK PRIMARY KEY (ID1, ID2)) SALT_BUCKETS = 8";
        Connection conn1 = DriverManager.getConnection(getUrl(), props);
        conn1.createStatement().execute(ddl);
        ddl = "ALTER TABLE T6 SET CF.TTL = 86400";
        try {
            conn1.createStatement().execute(ddl);
            fail();
        } catch (SQLException e) {
            assertEquals(SQLExceptionCode.COLUMN_FAMILY_NOT_ALLOWED_FOR_TTL.getErrorCode(), e.getErrorCode());
        }
    }

    @Test
    public void testSetPropertyNeedsColumnFamilyToExist() throws Exception {
        Properties props = PropertiesUtil.deepCopy(TEST_PROPERTIES);
        String ddl = "CREATE TABLE T7 (\n"
                +"ID1 VARCHAR(15) NOT NULL,\n"
                +"ID2 VARCHAR(15) NOT NULL,\n"
                +"CREATED_DATE DATE,\n"
                +"CREATION_TIME BIGINT,\n"
                +"LAST_USED DATE,\n"
                +"CONSTRAINT PK PRIMARY KEY (ID1, ID2)) SALT_BUCKETS = 8";
        Connection conn1 = DriverManager.getConnection(getUrl(), props);
        conn1.createStatement().execute(ddl);
        ddl = "ALTER TABLE T7 SET CF.REPLICATION_SCOPE = 1";
        try {
            conn1.createStatement().execute(ddl);
            fail();
        } catch (SQLException e) {
            assertEquals(SQLExceptionCode.COLUMN_FAMILY_NOT_FOUND.getErrorCode(), e.getErrorCode());
        }
    }

    @Test
    public void testSetDefaultColumnFamilyNotAllowed() throws Exception {
        Properties props = PropertiesUtil.deepCopy(TEST_PROPERTIES);
        String ddl = "CREATE TABLE T8 (\n"
                +"ID1 VARCHAR(15) NOT NULL,\n"
                +"ID2 VARCHAR(15) NOT NULL,\n"
                +"CREATED_DATE DATE,\n"
                +"CREATION_TIME BIGINT,\n"
                +"LAST_USED DATE,\n"
                +"CONSTRAINT PK PRIMARY KEY (ID1, ID2)) SALT_BUCKETS = 8";
        Connection conn1 = DriverManager.getConnection(getUrl(), props);
        conn1.createStatement().execute(ddl);
        ddl = "ALTER TABLE T8 SET DEFAULT_COLUMN_FAMILY = 'A'";
        try {
            conn1.createStatement().execute(ddl);
            fail();
        } catch (SQLException e) {
            assertEquals(SQLExceptionCode.DEFAULT_COLUMN_FAMILY_ONLY_ON_CREATE_TABLE.getErrorCode(), e.getErrorCode());
        }
    }

    @Test
    public void testSetHColumnOrHTablePropertiesOnViewsNotAllowed() throws Exception {
        Properties props = PropertiesUtil.deepCopy(TEST_PROPERTIES);
        String ddl = "CREATE TABLE T9 (\n"
                +"ID1 VARCHAR(15) NOT NULL,\n"
                +"ID2 VARCHAR(15) NOT NULL,\n"
                +"CREATED_DATE DATE,\n"
                +"CREATION_TIME BIGINT,\n"
                +"LAST_USED DATE,\n"
                +"CONSTRAINT PK PRIMARY KEY (ID1, ID2)) SALT_BUCKETS = 8";
        Connection conn1 = DriverManager.getConnection(getUrl(), props);
        conn1.createStatement().execute(ddl);
        ddl = "CREATE VIEW v AS SELECT * FROM T9 WHERE CREATION_TIME = 1";
        conn1.createStatement().execute(ddl);
        ddl = "ALTER VIEW v SET REPLICATION_SCOPE = 1";
        try {
            conn1.createStatement().execute(ddl);
            fail();
        } catch (SQLException e) {
            assertEquals(SQLExceptionCode.VIEW_WITH_PROPERTIES.getErrorCode(), e.getErrorCode());
        }
        ddl = "ALTER VIEW v SET COMPACTION_ENABLED = FALSE";
        try {
            conn1.createStatement().execute(ddl);
            fail();
        } catch (SQLException e) {
            assertEquals(SQLExceptionCode.VIEW_WITH_PROPERTIES.getErrorCode(), e.getErrorCode());
        }
    }

    @Test
    public void testSetForSomePhoenixTablePropertiesOnViewsAllowed() throws Exception {
        Properties props = PropertiesUtil.deepCopy(TEST_PROPERTIES);
        String ddl = "CREATE TABLE T10 (\n"
                +"ID1 VARCHAR(15) NOT NULL,\n"
                +"ID2 VARCHAR(15) NOT NULL,\n"
                +"CREATED_DATE DATE,\n"
                +"CREATION_TIME BIGINT,\n"
                +"LAST_USED DATE,\n"
                +"CONSTRAINT PK PRIMARY KEY (ID1, ID2)) SALT_BUCKETS = 8";
        Connection conn1 = DriverManager.getConnection(getUrl(), props);
        conn1.createStatement().execute(ddl);
        ddl = "CREATE VIEW v AS SELECT * FROM T10 WHERE CREATION_TIME = 1";
        conn1.createStatement().execute(ddl);
        ddl = "ALTER VIEW v SET IMMUTABLE_ROWS = TRUE";
        conn1.createStatement().execute(ddl);
        assertImmutableRows(conn1, "V", true);
        ddl = "ALTER VIEW v SET IMMUTABLE_ROWS = FALSE";
        conn1.createStatement().execute(ddl);
        assertImmutableRows(conn1, "V", false);
        ddl = "ALTER VIEW v SET DISABLE_WAL = TRUE";
        try {
            conn1.createStatement().execute(ddl);
            fail();
        } catch (SQLException e) {
            assertEquals(SQLExceptionCode.VIEW_WITH_PROPERTIES.getErrorCode(), e.getErrorCode());
        }
    }

    @Test
    public void testSettingPropertiesWhenTableHasDefaultColFamilySpecified() throws Exception {
        Properties props = PropertiesUtil.deepCopy(TEST_PROPERTIES);
        String ddl = "CREATE TABLE T11 (\n"
                +"ID1 VARCHAR(15) NOT NULL,\n"
                +"ID2 VARCHAR(15) NOT NULL,\n"
                +"CREATED_DATE DATE,\n"
                +"CREATION_TIME BIGINT,\n"
                +"CF.LAST_USED DATE,\n"
                +"CONSTRAINT PK PRIMARY KEY (ID1, ID2)) IMMUTABLE_ROWS=true, DEFAULT_COLUMN_FAMILY = 'XYZ'";
        Connection conn = DriverManager.getConnection(getUrl(), props);
        conn.createStatement().execute(ddl);
        assertImmutableRows(conn, "T11", true);
        ddl = "ALTER TABLE T11 SET COMPACTION_ENABLED = FALSE, CF.REPLICATION_SCOPE=1, IMMUTABLE_ROWS = TRUE, TTL=1000";
        conn.createStatement().execute(ddl);
        assertImmutableRows(conn, "T11", true);
        try (HBaseAdmin admin = conn.unwrap(PhoenixConnection.class).getQueryServices().getAdmin()) {
            HTableDescriptor tableDesc = admin.getTableDescriptor(Bytes.toBytes("T11"));
            HColumnDescriptor[] columnFamilies = tableDesc.getColumnFamilies();
            assertEquals(2, columnFamilies.length);
            assertEquals("CF", columnFamilies[0].getNameAsString());
            assertEquals(1, columnFamilies[0].getScope());
            assertEquals(1000, columnFamilies[0].getTimeToLive());
            assertEquals("XYZ", columnFamilies[1].getNameAsString());
            assertEquals(DEFAULT_REPLICATION_SCOPE, columnFamilies[1].getScope());
            assertEquals(1000, columnFamilies[1].getTimeToLive());
            assertEquals(Boolean.toString(false), tableDesc.getValue(HTableDescriptor.COMPACTION_ENABLED));
        }
    }

    @Test
    public void testNewColumnFamilyInheritsTTLOfEmptyCF() throws Exception {
        Properties props = PropertiesUtil.deepCopy(TEST_PROPERTIES);
        String ddl = "CREATE TABLE NEWCFTTLTEST (\n"
                +"ID1 VARCHAR(15) NOT NULL,\n"
                +"ID2 VARCHAR(15) NOT NULL,\n"
                +"CREATED_DATE DATE,\n"
                +"CREATION_TIME BIGINT,\n"
                +"LAST_USED DATE,\n"
                +"CONSTRAINT PK PRIMARY KEY (ID1, ID2)) SALT_BUCKETS = 8, TTL = 1000";
        Connection conn1 = DriverManager.getConnection(getUrl(), props);
        conn1.createStatement().execute(ddl);
        ddl = "ALTER TABLE NEWCFTTLTEST ADD CF.STRING VARCHAR";
        conn1.createStatement().execute(ddl);
        try (HBaseAdmin admin = conn1.unwrap(PhoenixConnection.class).getQueryServices().getAdmin()) {
            HTableDescriptor tableDesc = admin.getTableDescriptor(Bytes.toBytes("NEWCFTTLTEST"));
            HColumnDescriptor[] columnFamilies = tableDesc.getColumnFamilies();
            assertEquals(2, columnFamilies.length);
            assertEquals("0", columnFamilies[0].getNameAsString());
            assertEquals(1000, columnFamilies[0].getTimeToLive());
            assertEquals("CF", columnFamilies[1].getNameAsString());
            assertEquals(1000, columnFamilies[1].getTimeToLive());
        }
    }

    private static void assertImmutableRows(Connection conn, String fullTableName, boolean expectedValue) throws SQLException {
        PhoenixConnection pconn = conn.unwrap(PhoenixConnection.class);
        assertEquals(expectedValue, pconn.getMetaDataCache().getTable(new PTableKey(pconn.getTenantId(), fullTableName)).isImmutableRows());
    }

    @Test
    public void testSetPropertyAndAddColumnForExistingColumnFamily() throws Exception {
        Properties props = PropertiesUtil.deepCopy(TEST_PROPERTIES);
        Connection conn = DriverManager.getConnection(getUrl(), props);
        String ddl = "CREATE TABLE SETPROPEXISTINGCF " +
                "  (a_string varchar not null, col1 integer, CF.col2 integer" +
                "  CONSTRAINT pk PRIMARY KEY (a_string))\n";
        try {
            conn.createStatement().execute(ddl);
            conn.createStatement().execute("ALTER TABLE SETPROPEXISTINGCF ADD CF.col3 integer CF.IN_MEMORY=true");
            try (HBaseAdmin admin = conn.unwrap(PhoenixConnection.class).getQueryServices().getAdmin()) {
                HColumnDescriptor[] columnFamilies = admin.getTableDescriptor(Bytes.toBytes("SETPROPEXISTINGCF")).getColumnFamilies();
                assertEquals(2, columnFamilies.length);
                assertEquals("0", columnFamilies[0].getNameAsString());
                assertFalse(columnFamilies[0].isInMemory());
                assertEquals("CF", columnFamilies[1].getNameAsString());
                assertTrue(columnFamilies[1].isInMemory());
            }
        } finally {
            conn.close();
        }
    }

    @Test
    public void testSetPropertyAndAddColumnForNewAndExistingColumnFamily() throws Exception {
        Properties props = PropertiesUtil.deepCopy(TEST_PROPERTIES);
        Connection conn = DriverManager.getConnection(getUrl(), props);
        String ddl = "CREATE TABLE SETPROPNEWEXISTCF " +
                "  (a_string varchar not null, col1 integer, CF1.col2 integer" +
                "  CONSTRAINT pk PRIMARY KEY (a_string))\n";
        try {
            conn.createStatement().execute(ddl);
            conn.createStatement().execute("ALTER TABLE SETPROPNEWEXISTCF ADD col4 integer, CF1.col5 integer, CF2.col6 integer IN_MEMORY=true, CF1.REPLICATION_SCOPE=1, CF2.IN_MEMORY=false ");
            try (HBaseAdmin admin = conn.unwrap(PhoenixConnection.class).getQueryServices().getAdmin()) {
                HColumnDescriptor[] columnFamilies = admin.getTableDescriptor(Bytes.toBytes("SETPROPNEWEXISTCF")).getColumnFamilies();
                assertEquals(3, columnFamilies.length);
                assertEquals("0", columnFamilies[0].getNameAsString());
                assertTrue(columnFamilies[0].isInMemory());
                assertEquals(0, columnFamilies[0].getScope());
                assertEquals("CF1", columnFamilies[1].getNameAsString());
                assertTrue(columnFamilies[1].isInMemory());
                assertEquals(1, columnFamilies[1].getScope());
                assertEquals("CF2", columnFamilies[2].getNameAsString());
                assertFalse(columnFamilies[2].isInMemory());
                assertEquals(0, columnFamilies[2].getScope());
            }
        } finally {
            conn.close();
        }
    }

    @Test
    public void testSetPropertyAndAddColumnWhenTableHasExplicitDefaultColumnFamily() throws Exception {
        Properties props = PropertiesUtil.deepCopy(TEST_PROPERTIES);
        Connection conn = DriverManager.getConnection(getUrl(), props);
        String ddl = "CREATE TABLE SETPROPNEWEXISTDEFCOLFAM " +
                "  (a_string varchar not null, col1 integer, CF1.col2 integer" +
                "  CONSTRAINT pk PRIMARY KEY (a_string)) DEFAULT_COLUMN_FAMILY = 'XYZ'\n";
        try {
            conn.createStatement().execute(ddl);
            conn.createStatement().execute("ALTER TABLE SETPROPNEWEXISTDEFCOLFAM ADD col4 integer, CF1.col5 integer, CF2.col6 integer IN_MEMORY=true, CF1.REPLICATION_SCOPE=1, CF2.IN_MEMORY=false, XYZ.REPLICATION_SCOPE=1 ");
            try (HBaseAdmin admin = conn.unwrap(PhoenixConnection.class).getQueryServices().getAdmin()) {
                HColumnDescriptor[] columnFamilies = admin.getTableDescriptor(Bytes.toBytes("SETPROPNEWEXISTDEFCOLFAM")).getColumnFamilies();
                assertEquals(3, columnFamilies.length);
                assertEquals("CF1", columnFamilies[0].getNameAsString());
                assertTrue(columnFamilies[0].isInMemory());
                assertEquals(1, columnFamilies[0].getScope());
                assertEquals("CF2", columnFamilies[1].getNameAsString());
                assertFalse(columnFamilies[1].isInMemory());
                assertEquals(0, columnFamilies[1].getScope());
                assertEquals("XYZ", columnFamilies[2].getNameAsString());
                assertTrue(columnFamilies[2].isInMemory());
                assertEquals(1, columnFamilies[2].getScope());
            }
        } finally {
            conn.close();
        }
    }

    @Test
    public void testSetPropertyAndAddColumnFailsForColumnFamilyNotPresentInAddCol() throws Exception {
    	Properties props = PropertiesUtil.deepCopy(TEST_PROPERTIES);
    	Connection conn = DriverManager.getConnection(getUrl(), props);
    	String ddl = "CREATE TABLE ADDCOLNOTPRESENT " +
    			"  (a_string varchar not null, col1 integer, CF1.col2 integer" +
    			"  CONSTRAINT pk PRIMARY KEY (a_string)) DEFAULT_COLUMN_FAMILY = 'XYZ'\n";
    	try {
    		conn.createStatement().execute(ddl);
    		try {
    			conn.createStatement().execute("ALTER TABLE ADDCOLNOTPRESENT ADD col4 integer CF1.REPLICATION_SCOPE=1, XYZ.IN_MEMORY=true ");
    			fail();
    		} catch(SQLException e) {
    			assertEquals(SQLExceptionCode.CANNOT_SET_PROPERTY_FOR_COLUMN_NOT_ADDED.getErrorCode(), e.getErrorCode());
    		}
    	} finally {
    		conn.close();
    	}
    }

    @Test
    public void testSetPropertyAndAddColumnForDifferentColumnFamilies() throws Exception {
    	Properties props = PropertiesUtil.deepCopy(TEST_PROPERTIES);
    	Connection conn = DriverManager.getConnection(getUrl(), props);
    	String ddl = "CREATE TABLE XYZ.SETPROPDIFFCFSTABLE " +
                "  (a_string varchar not null, col1 integer, CF1.col2 integer, CF2.col3 integer" +
                "  CONSTRAINT pk PRIMARY KEY (a_string)) DEFAULT_COLUMN_FAMILY = 'XYZ'\n";
        try {
            conn.createStatement().execute(ddl);
            conn.createStatement().execute("ALTER TABLE XYZ.SETPROPDIFFCFSTABLE ADD col4 integer, CF1.col5 integer, CF2.col6 integer, CF3.col7 integer CF1.REPLICATION_SCOPE=1, CF1.IN_MEMORY=false, IN_MEMORY=true ");
            try (HBaseAdmin admin = conn.unwrap(PhoenixConnection.class).getQueryServices().getAdmin()) {
                HColumnDescriptor[] columnFamilies = admin.getTableDescriptor(Bytes.toBytes(SchemaUtil.getTableName("XYZ", "SETPROPDIFFCFSTABLE"))).getColumnFamilies();
                assertEquals(4, columnFamilies.length);
                assertEquals("CF1", columnFamilies[0].getNameAsString());
                assertFalse(columnFamilies[0].isInMemory());
                assertEquals(1, columnFamilies[0].getScope());
                assertEquals("CF2", columnFamilies[1].getNameAsString());
                assertTrue(columnFamilies[1].isInMemory());
                assertEquals(0, columnFamilies[1].getScope());
                assertEquals("CF3", columnFamilies[2].getNameAsString());
                assertTrue(columnFamilies[2].isInMemory());
                assertEquals(0, columnFamilies[2].getScope());
                assertEquals("XYZ", columnFamilies[3].getNameAsString());
                assertTrue(columnFamilies[3].isInMemory());
                assertEquals(0, columnFamilies[3].getScope());
            }
    	} finally {
    		conn.close();
    	}
    }

    @Test
    public void testSetPropertyAndAddColumnUsingDefaultColumnFamilySpecifier() throws Exception {
    	Properties props = PropertiesUtil.deepCopy(TEST_PROPERTIES);
    	Connection conn = DriverManager.getConnection(getUrl(), props);
    	String ddl = "CREATE TABLE SETPROPDEFCF " +
    			"  (a_string varchar not null, col1 integer, CF1.col2 integer" +
    			"  CONSTRAINT pk PRIMARY KEY (a_string)) DEFAULT_COLUMN_FAMILY = 'XYZ'\n";
    	try {
            conn.createStatement().execute(ddl);
            conn.createStatement().execute("ALTER TABLE SETPROPDEFCF ADD col4 integer XYZ.REPLICATION_SCOPE=1 ");
            conn.createStatement().execute("ALTER TABLE SETPROPDEFCF ADD XYZ.col5 integer IN_MEMORY=true ");
            try (HBaseAdmin admin = conn.unwrap(PhoenixConnection.class).getQueryServices().getAdmin()) {
                HColumnDescriptor[] columnFamilies = admin.getTableDescriptor(Bytes.toBytes("SETPROPDEFCF")).getColumnFamilies();
                assertEquals(2, columnFamilies.length);
                assertEquals("CF1", columnFamilies[0].getNameAsString());
                assertFalse(columnFamilies[0].isInMemory());
                assertEquals(0, columnFamilies[0].getScope());
                assertEquals("XYZ", columnFamilies[1].getNameAsString());
                assertTrue(columnFamilies[1].isInMemory());
                assertEquals(1, columnFamilies[1].getScope());
            }
        } finally {
            conn.close();
        }
    }

    @Test
    public void testSetPropertyAndAddColumnForDefaultColumnFamily() throws Exception {
    	Properties props = PropertiesUtil.deepCopy(TEST_PROPERTIES);
    	Connection conn = DriverManager.getConnection(getUrl(), props);
    	conn.setAutoCommit(false);
    	String ddl = "CREATE TABLE TT " +
    			"  (a_string varchar not null, col1 integer" +
    			"  CONSTRAINT pk PRIMARY KEY (a_string))\n";
    	try {
    		conn.createStatement().execute(ddl);
    		conn.createStatement().execute("ALTER TABLE TT ADD col2 integer IN_MEMORY=true");
    		try (HBaseAdmin admin = conn.unwrap(PhoenixConnection.class).getQueryServices().getAdmin()) {
    			HColumnDescriptor[] columnFamilies = admin.getTableDescriptor(Bytes.toBytes("TT")).getColumnFamilies();
    			assertEquals(1, columnFamilies.length);
    			assertEquals("0", columnFamilies[0].getNameAsString());
    			assertTrue(columnFamilies[0].isInMemory());
    		}
    	} finally {
    		conn.close();
    	}
    }

    @Test
    public void testAddNewColumnFamilyProperties() throws Exception {
    	Properties props = PropertiesUtil.deepCopy(TEST_PROPERTIES);
    	Connection conn = DriverManager.getConnection(getUrl(), props);
    	conn.setAutoCommit(false);

    	try {
    		conn.createStatement()
    		.execute(
    				"CREATE TABLE mixed_add_table "
    						+ "  (a_string varchar not null, col1 integer, cf1.col2 integer, col3 integer , cf2.col4 integer "
    						+ "  CONSTRAINT pk PRIMARY KEY (a_string)) immutable_rows=true , SALT_BUCKETS=3 ");

    		String ddl = "Alter table mixed_add_table add cf3.col5 integer, cf4.col6 integer in_memory=true";
    		conn.createStatement().execute(ddl);

    		try (HBaseAdmin admin = conn.unwrap(PhoenixConnection.class).getQueryServices().getAdmin()) {
    			HTableDescriptor tableDesc = admin.getTableDescriptor(Bytes.toBytes("MIXED_ADD_TABLE"));
    			assertTrue(tableDesc.isCompactionEnabled());
    			HColumnDescriptor[] columnFamilies = tableDesc.getColumnFamilies();
    			assertEquals(5, columnFamilies.length);
    			assertEquals("0", columnFamilies[0].getNameAsString());
    			assertFalse(columnFamilies[0].isInMemory());
    			assertEquals("CF1", columnFamilies[1].getNameAsString());
    			assertFalse(columnFamilies[1].isInMemory());
    			assertEquals("CF2", columnFamilies[2].getNameAsString());
    			assertFalse(columnFamilies[2].isInMemory());
    			assertEquals("CF3", columnFamilies[3].getNameAsString());
    			assertTrue(columnFamilies[3].isInMemory());
    			assertEquals("CF4", columnFamilies[4].getNameAsString());
    			assertTrue(columnFamilies[4].isInMemory());
    		}
    	} finally {
    		conn.close();
    	}
    }

    @Test
    public void testAddProperyToExistingColumnFamily() throws Exception {
    	Properties props = PropertiesUtil.deepCopy(TEST_PROPERTIES);
    	Connection conn = DriverManager.getConnection(getUrl(), props);
    	conn.setAutoCommit(false);

    	try {
    		conn.createStatement()
    		.execute(
    				"CREATE TABLE exist_table "
    						+ "  (a_string varchar not null, col1 integer, cf1.col2 integer, col3 integer , cf2.col4 integer "
    						+ "  CONSTRAINT pk PRIMARY KEY (a_string)) immutable_rows=true , SALT_BUCKETS=3 ");

    		String ddl = "Alter table exist_table add cf1.col5 integer in_memory=true";
    		conn.createStatement().execute(ddl);

    		try (HBaseAdmin admin = conn.unwrap(PhoenixConnection.class).getQueryServices().getAdmin()) {
    			HTableDescriptor tableDesc = admin.getTableDescriptor(Bytes.toBytes("EXIST_TABLE"));
    			assertTrue(tableDesc.isCompactionEnabled());
    			HColumnDescriptor[] columnFamilies = tableDesc.getColumnFamilies();
    			assertEquals(3, columnFamilies.length);
    			assertEquals("0", columnFamilies[0].getNameAsString());
    			assertFalse(columnFamilies[0].isInMemory());
    			assertEquals("CF1", columnFamilies[1].getNameAsString());
    			assertTrue(columnFamilies[1].isInMemory());
    			assertEquals("CF2", columnFamilies[2].getNameAsString());
    			assertFalse(columnFamilies[2].isInMemory());
    		}
    	} finally {
    		conn.close();
    	}
    }

    @Test
    public void testAddTTLToExistingColumnFamily() throws Exception {
    	Properties props = PropertiesUtil.deepCopy(TEST_PROPERTIES);
    	Connection conn = DriverManager.getConnection(getUrl(), props);
    	conn.setAutoCommit(false);

    	try {
    		String ddl = "CREATE TABLE exist_test (pk char(2) not null primary key, col1 integer, b.col1 integer) SPLIT ON ('EA','EZ')";
    		conn.createStatement().execute(ddl);
    		ddl = "ALTER TABLE exist_test add b.col2 varchar ttl=30";
    		conn.createStatement().execute(ddl);
    		fail();
    	} catch (SQLException e) {
    		assertEquals(SQLExceptionCode.CANNOT_SET_TABLE_PROPERTY_ADD_COLUMN.getErrorCode(), e.getErrorCode());
    	} finally {
    		conn.close();
    	}
    }

    @Test
    public void testSettingTTLWhenAddingColumnNotAllowed() throws Exception {
    	Properties props = PropertiesUtil.deepCopy(TEST_PROPERTIES);
    	Connection conn = DriverManager.getConnection(getUrl(), props);
    	conn.setAutoCommit(false);

    	try {
    		String ddl = "CREATE TABLE ttl_test (pk char(2) not null primary key) TTL=100 SPLIT ON ('EA','EZ')";
    		conn.createStatement().execute(ddl);
    		ddl = "ALTER TABLE ttl_test add col1 varchar ttl=30";
    		conn.createStatement().execute(ddl);
    		fail();
    	} catch (SQLException e) {
    		assertEquals(SQLExceptionCode.CANNOT_SET_TABLE_PROPERTY_ADD_COLUMN.getErrorCode(), e.getErrorCode());
    	}
    	try {
    		String ddl = "ALTER TABLE ttl_test add col1 varchar a.ttl=30";
    		conn.createStatement().execute(ddl);
    		fail();
    	} catch (SQLException e) {
    		assertEquals(SQLExceptionCode.COLUMN_FAMILY_NOT_ALLOWED_FOR_TTL.getErrorCode(), e.getErrorCode());
    	} finally {
    		conn.close();
    	}
    }

    @Test
    public void testSetTTLForTableWithOnlyPKCols() throws Exception {
    	Properties props = PropertiesUtil.deepCopy(TEST_PROPERTIES);
    	Connection conn = DriverManager.getConnection(getUrl(), props);
    	conn.setAutoCommit(false);
    	try {
    	   	String ddl = "create table IF NOT EXISTS ttl_test2 ("
        		    + " id char(1) NOT NULL,"
        		    + " col1 integer NOT NULL,"
        		    + " col2 bigint NOT NULL,"
        		    + " CONSTRAINT NAME_PK PRIMARY KEY (id, col1, col2)"
        		    + " ) TTL=86400, SALT_BUCKETS = 4, DEFAULT_COLUMN_FAMILY='XYZ'";
            conn.createStatement().execute(ddl);
            try (HBaseAdmin admin = conn.unwrap(PhoenixConnection.class).getQueryServices().getAdmin()) {
    			HTableDescriptor tableDesc = admin.getTableDescriptor(Bytes.toBytes("TTL_TEST2"));
    			HColumnDescriptor[] columnFamilies = tableDesc.getColumnFamilies();
    			assertEquals(1, columnFamilies.length);
    			assertEquals("XYZ", columnFamilies[0].getNameAsString());
    			assertEquals(86400, columnFamilies[0].getTimeToLive());
    		}
    		ddl = "ALTER TABLE ttl_test2 SET TTL=30";
    		conn.createStatement().execute(ddl);
    		conn.commit();
    		try (HBaseAdmin admin = conn.unwrap(PhoenixConnection.class).getQueryServices().getAdmin()) {
    			HTableDescriptor tableDesc = admin.getTableDescriptor(Bytes.toBytes("TTL_TEST2"));
    			HColumnDescriptor[] columnFamilies = tableDesc.getColumnFamilies();
    			assertEquals(1, columnFamilies.length);
    			assertEquals(30, columnFamilies[0].getTimeToLive());
    			assertEquals("XYZ", columnFamilies[0].getNameAsString());
    		}
    	} finally {
    		conn.close();
    	}
    }

    @Test
    public void testSetHColumnPropertyForTableWithOnlyPKCols() throws Exception {
    	Properties props = PropertiesUtil.deepCopy(TEST_PROPERTIES);
    	Connection conn = DriverManager.getConnection(getUrl(), props);
    	conn.setAutoCommit(false);
    	try {
    	   	String ddl = "create table IF NOT EXISTS SETHCPROPPKONLY ("
        		    + " id char(1) NOT NULL,"
        		    + " col1 integer NOT NULL,"
        		    + " col2 bigint NOT NULL,"
        		    + " CONSTRAINT NAME_PK PRIMARY KEY (id, col1, col2)"
        		    + " ) TTL=86400, SALT_BUCKETS = 4, DEFAULT_COLUMN_FAMILY='XYZ'";
            conn.createStatement().execute(ddl);
            ddl = "ALTER TABLE SETHCPROPPKONLY SET IN_MEMORY=true";
    		conn.createStatement().execute(ddl);
    		conn.commit();
    		try (HBaseAdmin admin = conn.unwrap(PhoenixConnection.class).getQueryServices().getAdmin()) {
    			HTableDescriptor tableDesc = admin.getTableDescriptor(Bytes.toBytes("SETHCPROPPKONLY"));
    			HColumnDescriptor[] columnFamilies = tableDesc.getColumnFamilies();
    			assertEquals(1, columnFamilies.length);
    			assertEquals(true, columnFamilies[0].isInMemory());
    			assertEquals("XYZ", columnFamilies[0].getNameAsString());
    		}
    	} finally {
    		conn.close();
    	}

    	try {
    	   	String ddl = "create table IF NOT EXISTS SETHCPROPPKONLY2 ("
        		    + " id char(1) NOT NULL,"
        		    + " col1 integer NOT NULL,"
        		    + " col2 bigint NOT NULL,"
        		    + " CONSTRAINT NAME_PK PRIMARY KEY (id, col1, col2)"
        		    + " ) TTL=86400, SALT_BUCKETS = 4";
            conn.createStatement().execute(ddl);
            ddl = "ALTER TABLE SETHCPROPPKONLY2 SET IN_MEMORY=true";
    		conn.createStatement().execute(ddl);
    		conn.commit();
    		try (HBaseAdmin admin = conn.unwrap(PhoenixConnection.class).getQueryServices().getAdmin()) {
    			HTableDescriptor tableDesc = admin.getTableDescriptor(Bytes.toBytes("SETHCPROPPKONLY2"));
    			HColumnDescriptor[] columnFamilies = tableDesc.getColumnFamilies();
    			assertEquals(1, columnFamilies.length);
    			assertEquals(true, columnFamilies[0].isInMemory());
    			assertEquals("0", columnFamilies[0].getNameAsString());
    		}
    	} finally {
    		conn.close();
    	}
    }

    @Test
    public void testSetHColumnPropertyAndAddColumnForDefaultCFForTableWithOnlyPKCols() throws Exception {
    	Properties props = PropertiesUtil.deepCopy(TEST_PROPERTIES);
    	Connection conn = DriverManager.getConnection(getUrl(), props);
    	conn.setAutoCommit(false);
    	try {
    	   	String ddl = "create table IF NOT EXISTS SETHCPROPADDCOLPKONLY ("
        		    + " id char(1) NOT NULL,"
        		    + " col1 integer NOT NULL,"
        		    + " col2 bigint NOT NULL,"
        		    + " CONSTRAINT NAME_PK PRIMARY KEY (id, col1, col2)"
        		    + " ) TTL=86400, SALT_BUCKETS = 4, DEFAULT_COLUMN_FAMILY='XYZ'";
            conn.createStatement().execute(ddl);
            ddl = "ALTER TABLE SETHCPROPADDCOLPKONLY ADD COL3 INTEGER IN_MEMORY=true";
    		conn.createStatement().execute(ddl);
    		conn.commit();
    		try (HBaseAdmin admin = conn.unwrap(PhoenixConnection.class).getQueryServices().getAdmin()) {
    			HTableDescriptor tableDesc = admin.getTableDescriptor(Bytes.toBytes("SETHCPROPADDCOLPKONLY"));
    			HColumnDescriptor[] columnFamilies = tableDesc.getColumnFamilies();
    			assertEquals(1, columnFamilies.length);
    			assertEquals(true, columnFamilies[0].isInMemory());
    			assertEquals("XYZ", columnFamilies[0].getNameAsString());
    		}
    	} finally {
    		conn.close();
    	}
    }

    @Test
    public void testSetHColumnPropertyAndAddColumnForNewCFForTableWithOnlyPKCols() throws Exception {
    	Properties props = PropertiesUtil.deepCopy(TEST_PROPERTIES);
    	Connection conn = DriverManager.getConnection(getUrl(), props);
    	conn.setAutoCommit(false);
    	try {
    	   	String ddl = "create table IF NOT EXISTS SETHCPROPADDNEWCFCOLPKONLY ("
        		    + " id char(1) NOT NULL,"
        		    + " col1 integer NOT NULL,"
        		    + " col2 bigint NOT NULL,"
        		    + " CONSTRAINT NAME_PK PRIMARY KEY (id, col1, col2)"
        		    + " ) TTL=86400, SALT_BUCKETS = 4, DEFAULT_COLUMN_FAMILY='XYZ'";
            conn.createStatement().execute(ddl);
            ddl = "ALTER TABLE SETHCPROPADDNEWCFCOLPKONLY ADD NEWCF.COL3 INTEGER IN_MEMORY=true";
    		conn.createStatement().execute(ddl);
    		conn.commit();
    		try (HBaseAdmin admin = conn.unwrap(PhoenixConnection.class).getQueryServices().getAdmin()) {
    			HTableDescriptor tableDesc = admin.getTableDescriptor(Bytes.toBytes("SETHCPROPADDNEWCFCOLPKONLY"));
    			HColumnDescriptor[] columnFamilies = tableDesc.getColumnFamilies();
    			assertEquals(2, columnFamilies.length);
    			assertEquals("NEWCF", columnFamilies[0].getNameAsString());
    			assertEquals(true, columnFamilies[0].isInMemory());
    			assertEquals("XYZ", columnFamilies[1].getNameAsString());
    			assertEquals(false, columnFamilies[1].isInMemory());
    		}
    	} finally {
    		conn.close();
    	}
    }

    @Test
    public void testTTLAssignmentForNewEmptyCF() throws Exception {
    	Properties props = PropertiesUtil.deepCopy(TEST_PROPERTIES);
    	Connection conn = DriverManager.getConnection(getUrl(), props);
    	conn.setAutoCommit(false);
    	try {
    	   	String ddl = "create table IF NOT EXISTS NEWEMPTYCFTABLE ("
        		    + " id char(1) NOT NULL,"
        		    + " col1 integer NOT NULL,"
        		    + " col2 bigint NOT NULL,"
        		    + " CONSTRAINT NAME_PK PRIMARY KEY (id, col1, col2)"
        		    + " ) TTL=86400, SALT_BUCKETS = 4, DEFAULT_COLUMN_FAMILY='XYZ'";
            conn.createStatement().execute(ddl);
            ddl = "ALTER TABLE NEWEMPTYCFTABLE ADD NEWCF.COL3 INTEGER IN_MEMORY=true";
    		conn.createStatement().execute(ddl);
    		conn.commit();
    		try (HBaseAdmin admin = conn.unwrap(PhoenixConnection.class).getQueryServices().getAdmin()) {
    			HTableDescriptor tableDesc = admin.getTableDescriptor(Bytes.toBytes("NEWEMPTYCFTABLE"));
    			HColumnDescriptor[] columnFamilies = tableDesc.getColumnFamilies();
    			assertEquals(2, columnFamilies.length);
    			assertEquals("NEWCF", columnFamilies[0].getNameAsString());
    			assertEquals(true, columnFamilies[0].isInMemory());
    			assertEquals(86400, columnFamilies[0].getTimeToLive());
    			assertEquals("XYZ", columnFamilies[1].getNameAsString());
    			assertEquals(false, columnFamilies[1].isInMemory());
    			assertEquals(86400, columnFamilies[1].getTimeToLive());
    		}

    		ddl = "ALTER TABLE NEWEMPTYCFTABLE SET TTL=1000";
    		conn.createStatement().execute(ddl);
    		conn.commit();
    		try (HBaseAdmin admin = conn.unwrap(PhoenixConnection.class).getQueryServices().getAdmin()) {
    			HTableDescriptor tableDesc = admin.getTableDescriptor(Bytes.toBytes("NEWEMPTYCFTABLE"));
    			HColumnDescriptor[] columnFamilies = tableDesc.getColumnFamilies();
    			assertEquals(2, columnFamilies.length);
    			assertEquals("NEWCF", columnFamilies[0].getNameAsString());
    			assertEquals(true, columnFamilies[0].isInMemory());
    			assertEquals(1000, columnFamilies[0].getTimeToLive());
    			assertEquals("XYZ", columnFamilies[1].getNameAsString());
    			assertEquals(false, columnFamilies[1].isInMemory());
    			assertEquals(86400, columnFamilies[1].getTimeToLive());
    		}

    		// the new column will be assigned to the column family XYZ. With the a KV column getting added for XYZ,
    		// the column family will start showing up in PTable.getColumnFamilies() after the column is added. Thus
    		// being a new column family for the PTable, it will end up inheriting the TTL of the emptyCF (NEWCF).
    		ddl = "ALTER TABLE NEWEMPTYCFTABLE ADD COL3 INTEGER";
    		conn.createStatement().execute(ddl);
    		conn.commit();
    		try (HBaseAdmin admin = conn.unwrap(PhoenixConnection.class).getQueryServices().getAdmin()) {
    			HTableDescriptor tableDesc = admin.getTableDescriptor(Bytes.toBytes("NEWEMPTYCFTABLE"));
    			HColumnDescriptor[] columnFamilies = tableDesc.getColumnFamilies();
    			assertEquals(2, columnFamilies.length);
    			assertEquals("NEWCF", columnFamilies[0].getNameAsString());
    			assertEquals(true, columnFamilies[0].isInMemory());
    			assertEquals(1000, columnFamilies[0].getTimeToLive());
    			assertEquals("XYZ", columnFamilies[1].getNameAsString());
    			assertEquals(false, columnFamilies[1].isInMemory());
    			assertEquals(1000, columnFamilies[1].getTimeToLive());
    		}
    	} finally {
    		conn.close();
    	}
    }

    @Test
    public void testSettingNotHColumnNorPhoenixPropertyEndsUpAsHTableProperty() throws Exception {
    	Properties props = PropertiesUtil.deepCopy(TEST_PROPERTIES);
    	Connection conn = DriverManager.getConnection(getUrl(), props);
    	try {
    		String ddl = "create table IF NOT EXISTS RANDMONPROPTABLE ("
    				+ " id char(1) NOT NULL,"
    				+ " col1 integer NOT NULL,"
    				+ " col2 bigint NOT NULL,"
    				+ " CONSTRAINT NAME_PK PRIMARY KEY (id, col1, col2)"
    				+ " )";
    		conn.createStatement().execute(ddl);
    		ddl = "ALTER TABLE RANDMONPROPTABLE ADD NEWCF.COL3 INTEGER NEWCF.UNKNOWN_PROP='ABC'";
    		try {
    			conn.createStatement().execute(ddl);
    			fail();
    		} catch (SQLException e) {
    			assertEquals(SQLExceptionCode.CANNOT_SET_TABLE_PROPERTY_ADD_COLUMN.getErrorCode(), e.getErrorCode());
    		}
    		ddl = "ALTER TABLE RANDMONPROPTABLE SET UNKNOWN_PROP='ABC'";
    		conn.createStatement().execute(ddl);
    		try (HBaseAdmin admin = conn.unwrap(PhoenixConnection.class).getQueryServices().getAdmin()) {
    			HTableDescriptor tableDesc = admin.getTableDescriptor(Bytes.toBytes("RANDMONPROPTABLE"));
    			assertEquals("ABC", tableDesc.getValue("UNKNOWN_PROP"));
    		}
    	} finally {
    		conn.close();
    	}
    }

    @Test
    public void testAlterStoreNulls() throws SQLException {
        Properties props = PropertiesUtil.deepCopy(TEST_PROPERTIES);
        Connection conn = DriverManager.getConnection(getUrl(), props);

        Statement stmt = conn.createStatement();
        stmt.execute("CREATE TABLE WITH_NULLS (id SMALLINT PRIMARY KEY, name VARCHAR)");

        ResultSet rs = stmt.executeQuery("SELECT STORE_NULLS FROM SYSTEM.CATALOG " +
                "WHERE table_name = 'WITH_NULLS' AND STORE_NULLS IS NOT NULL");
        assertTrue(rs.next());
        assertFalse(rs.getBoolean(1));
        assertFalse(rs.next());
        rs.close();

        stmt.execute("ALTER TABLE WITH_NULLS SET STORE_NULLS = true");

        rs = stmt.executeQuery("SELECT STORE_NULLS FROM SYSTEM.CATALOG " +
                "WHERE table_name = 'WITH_NULLS' AND STORE_NULLS IS NOT NULL");
        assertTrue(rs.next());
        assertTrue(rs.getBoolean(1));
        assertFalse(rs.next());
        rs.close();
        stmt.close();

    }
    
    @Test
    public void testAddingPkColAndSettingProperties() throws Exception {
        Connection conn = DriverManager.getConnection(getUrl());
        try {       
            String ddl = "create table IF NOT EXISTS testAddingPkColAndSettingProperties ("
                    + " k1 char(1) NOT NULL,"
                    + " k2 integer NOT NULL,"
                    + " col1 bigint,"
                    + " CONSTRAINT NAME_PK PRIMARY KEY (k1, k2)"
                    + " )";
            conn.createStatement().execute(ddl);

            // set HTableProperty when adding a pk column should fail
            ddl = "ALTER TABLE testAddingPkColAndSettingProperties ADD k3 DECIMAL PRIMARY KEY COMPACTION_ENABLED = false";
            try {
                conn.createStatement().execute(ddl);
                fail();
            } catch (SQLException e) {
                assertEquals(SQLExceptionCode.CANNOT_SET_TABLE_PROPERTY_ADD_COLUMN.getErrorCode(), e.getErrorCode());
            }

            // set HColumnProperty when adding only a pk column should fail
            ddl = "ALTER TABLE testAddingPkColAndSettingProperties ADD k3 DECIMAL PRIMARY KEY REPLICATION_SCOPE = 0";
            try {
                conn.createStatement().execute(ddl);
                fail();
            } catch (SQLException e) {
                assertEquals(SQLExceptionCode.SET_UNSUPPORTED_PROP_ON_ALTER_TABLE.getErrorCode(), e.getErrorCode());
            }

            // set phoenix table property when adding a pk column should fail
            ddl = "ALTER TABLE testAddingPkColAndSettingProperties ADD k3 DECIMAL PRIMARY KEY DISABLE_WAL = true";
            try {
                conn.createStatement().execute(ddl);
                fail();
            } catch (SQLException e) {
                assertEquals(SQLExceptionCode.CANNOT_SET_TABLE_PROPERTY_ADD_COLUMN.getErrorCode(), e.getErrorCode());
            }

            // set HColumnProperty property when adding a pk column and other key value columns should work
            ddl = "ALTER TABLE testAddingPkColAndSettingProperties ADD k3 DECIMAL PRIMARY KEY, col2 bigint, CF.col3 bigint IN_MEMORY = true, CF.IN_MEMORY=false, CF.REPLICATION_SCOPE = 1";
            conn.createStatement().execute(ddl);
            String tableName = "testAddingPkColAndSettingProperties".toUpperCase();
            // assert that k3 was added as new pk
            ResultSet rs = conn.getMetaData().getPrimaryKeys("", "", tableName);
            assertTrue(rs.next());
            assertEquals("K1",rs.getString("COLUMN_NAME"));
            assertEquals(1, rs.getShort("KEY_SEQ"));
            assertTrue(rs.next());
            assertEquals("K2",rs.getString("COLUMN_NAME"));
            assertEquals(2, rs.getShort("KEY_SEQ"));
            assertTrue(rs.next());
            assertEquals("K3",rs.getString("COLUMN_NAME"));
            assertEquals(3, rs.getShort("KEY_SEQ"));
            assertFalse(rs.next());

            try (HBaseAdmin admin = conn.unwrap(PhoenixConnection.class).getQueryServices().getAdmin()) {
                HTableDescriptor tableDesc = admin.getTableDescriptor(Bytes.toBytes(tableName));
                HColumnDescriptor[] columnFamilies = tableDesc.getColumnFamilies();
                assertEquals(2, columnFamilies.length);
                assertEquals("0", columnFamilies[0].getNameAsString());
                assertEquals(true, columnFamilies[0].isInMemory());
                assertEquals(0, columnFamilies[0].getScope());
                assertEquals("CF", columnFamilies[1].getNameAsString());
                assertEquals(false, columnFamilies[1].isInMemory());
                assertEquals(1, columnFamilies[1].getScope());
            }

        } finally {
            conn.close();
        }
    }
    
    @Test
    public void testClientCacheUpdatedOnChangingPhoenixTableProperties() throws Exception {
        Connection conn = DriverManager.getConnection(getUrl());
        try {       
            String ddl = "create table IF NOT EXISTS TESTCHANGEPHOENIXPROPS ("
                    + " id char(1) NOT NULL,"
                    + " col1 integer NOT NULL,"
                    + " col2 bigint NOT NULL,"
                    + " CONSTRAINT NAME_PK PRIMARY KEY (id, col1, col2)"
                    + " )";
            conn.createStatement().execute(ddl);
            asssertIsWALDisabled(conn, "TESTCHANGEPHOENIXPROPS", false);
            
            ddl = "ALTER TABLE TESTCHANGEPHOENIXPROPS SET DISABLE_WAL = true";
            conn.createStatement().execute(ddl);
            // check metadata cache is updated with DISABLE_WAL = true
            asssertIsWALDisabled(conn, "TESTCHANGEPHOENIXPROPS", true);
            
            ddl = "ALTER TABLE TESTCHANGEPHOENIXPROPS SET DISABLE_WAL = false";
            conn.createStatement().execute(ddl);
            // check metadata cache is updated with DISABLE_WAL = false
            asssertIsWALDisabled(conn, "TESTCHANGEPHOENIXPROPS", false);
            
            ddl = "ALTER TABLE TESTCHANGEPHOENIXPROPS SET MULTI_TENANT = true";
            conn.createStatement().execute(ddl);
            // check metadata cache is updated with MULTI_TENANT = true
            PTable t = conn.unwrap(PhoenixConnection.class).getMetaDataCache().getTable(new PTableKey(null, "TESTCHANGEPHOENIXPROPS"));
            assertTrue(t.isMultiTenant());
            
            // check table metadata updated server side
            ResultSet rs = conn.createStatement().executeQuery("SELECT DISABLE_WAL, MULTI_TENANT FROM SYSTEM.CATALOG " +
                    "WHERE table_name = 'TESTCHANGEPHOENIXPROPS' AND DISABLE_WAL IS NOT NULL AND MULTI_TENANT IS NOT NULL");
            assertTrue(rs.next());
            assertFalse(rs.getBoolean(1));
            assertTrue(rs.getBoolean(2));
            assertFalse(rs.next());
            rs.close();
        } finally {
            conn.close();
        }
    }
    
    @Test
    public void testAddNewColumnsToBaseTableWithViews() throws Exception {
        Connection conn = DriverManager.getConnection(getUrl());
        try {       
            conn.createStatement().execute("CREATE TABLE IF NOT EXISTS TABLEWITHVIEW ("
                    + " ID char(1) NOT NULL,"
                    + " COL1 integer NOT NULL,"
                    + " COL2 bigint NOT NULL,"
                    + " CONSTRAINT NAME_PK PRIMARY KEY (ID, COL1, COL2)"
                    + " )");
            assertTableDefinition(conn, "TABLEWITHVIEW", PTableType.TABLE, null, 0, 3, QueryConstants.BASE_TABLE_BASE_COLUMN_COUNT, "ID", "COL1", "COL2");
            
            conn.createStatement().execute("CREATE VIEW VIEWOFTABLE ( VIEW_COL1 DECIMAL(10,2), VIEW_COL2 VARCHAR ) AS SELECT * FROM TABLEWITHVIEW");
            assertTableDefinition(conn, "VIEWOFTABLE", PTableType.VIEW, "TABLEWITHVIEW", 0, 5, 3, "ID", "COL1", "COL2", "VIEW_COL1", "VIEW_COL2");
            
            // adding a new pk column and a new regular column
            conn.createStatement().execute("ALTER TABLE TABLEWITHVIEW ADD COL3 varchar(10) PRIMARY KEY, COL4 integer");
            assertTableDefinition(conn, "TABLEWITHVIEW", PTableType.TABLE, null, 1, 5, QueryConstants.BASE_TABLE_BASE_COLUMN_COUNT, "ID", "COL1", "COL2", "COL3", "COL4");
            assertTableDefinition(conn, "VIEWOFTABLE", PTableType.VIEW, "TABLEWITHVIEW", 1, 7, 5, "ID", "COL1", "COL2", "COL3", "COL4", "VIEW_COL1", "VIEW_COL2");
        } finally {
            conn.close();
        }
    }
    
    @Test
    public void testAddExistingViewColumnToBaseTableWithViews() throws Exception {
        Connection conn = DriverManager.getConnection(getUrl());
        try {       
            conn.createStatement().execute("CREATE TABLE IF NOT EXISTS TABLEWITHVIEW ("
                    + " ID char(10) NOT NULL,"
                    + " COL1 integer NOT NULL,"
                    + " COL2 bigint NOT NULL,"
                    + " CONSTRAINT NAME_PK PRIMARY KEY (ID, COL1, COL2)"
                    + " )");
            assertTableDefinition(conn, "TABLEWITHVIEW", PTableType.TABLE, null, 0, 3, QueryConstants.BASE_TABLE_BASE_COLUMN_COUNT, "ID", "COL1", "COL2");
            
            conn.createStatement().execute("CREATE VIEW VIEWOFTABLE ( VIEW_COL1 DECIMAL(10,2), VIEW_COL2 VARCHAR(256), VIEW_COL3 VARCHAR, VIEW_COL4 DECIMAL, VIEW_COL5 DECIMAL(10,2), VIEW_COL6 VARCHAR, CONSTRAINT pk PRIMARY KEY (VIEW_COL5, VIEW_COL6) ) AS SELECT * FROM TABLEWITHVIEW");
            assertTableDefinition(conn, "VIEWOFTABLE", PTableType.VIEW, "TABLEWITHVIEW", 0, 9, 3, "ID", "COL1", "COL2", "VIEW_COL1", "VIEW_COL2", "VIEW_COL3", "VIEW_COL4", "VIEW_COL5", "VIEW_COL6");
            
            // upsert single row into view
            String dml = "UPSERT INTO VIEWOFTABLE VALUES(?,?,?,?,?, ?, ?, ?, ?)";
            PreparedStatement stmt = conn.prepareStatement(dml);
            stmt.setString(1, "view1");
            stmt.setInt(2, 12);
            stmt.setInt(3, 13);
            stmt.setInt(4, 14);
            stmt.setString(5, "view5");
            stmt.setString(6, "view6");
            stmt.setInt(7, 17);
            stmt.setInt(8, 18);
            stmt.setString(9, "view9");
            stmt.execute();
            conn.commit();
            
            try {
            	// should fail because there is already a view column with same name of different type
            	conn.createStatement().execute("ALTER TABLE TABLEWITHVIEW ADD VIEW_COL1 char(10)");
            	fail();
            }
            catch (SQLException e) {
            	assertEquals("Unexpected exception", CANNOT_MUTATE_TABLE.getErrorCode(), e.getErrorCode());
            }           
            
            try {
            	// should fail because there is already a view column with same name with different scale
            	conn.createStatement().execute("ALTER TABLE TABLEWITHVIEW ADD VIEW_COL1 DECIMAL(10,1)");
            	fail();
            }
            catch (SQLException e) {
            	assertEquals("Unexpected exception", CANNOT_MUTATE_TABLE.getErrorCode(), e.getErrorCode());
            } 
            
            try {
            	// should fail because there is already a view column with same name with different length
            	conn.createStatement().execute("ALTER TABLE TABLEWITHVIEW ADD VIEW_COL1 DECIMAL(9,2)");
            	fail();
            }
            catch (SQLException e) {
            	assertEquals("Unexpected exception", CANNOT_MUTATE_TABLE.getErrorCode(), e.getErrorCode());
            } 
            
            try {
            	// should fail because there is already a view column with different length
            	conn.createStatement().execute("ALTER TABLE TABLEWITHVIEW ADD VIEW_COL2 VARCHAR");
            	fail();
            }
            catch (SQLException e) {
            	assertEquals("Unexpected exception", CANNOT_MUTATE_TABLE.getErrorCode(), e.getErrorCode());
            }
            
            // validate that there were no columns added to the table or view
            assertTableDefinition(conn, "TABLEWITHVIEW", PTableType.TABLE, null, 0, 3, QueryConstants.BASE_TABLE_BASE_COLUMN_COUNT, "ID", "COL1", "COL2");
            assertTableDefinition(conn, "VIEWOFTABLE", PTableType.VIEW, "TABLEWITHVIEW", 0, 9, 3, "ID", "COL1", "COL2", "VIEW_COL1", "VIEW_COL2", "VIEW_COL3", "VIEW_COL4", "VIEW_COL5", "VIEW_COL6");
            
            // should succeed 
            conn.createStatement().execute("ALTER TABLE TABLEWITHVIEW ADD VIEW_COL4 DECIMAL, VIEW_COL2 VARCHAR(256)");
            assertTableDefinition(conn, "TABLEWITHVIEW", PTableType.TABLE, null, 1, 5, QueryConstants.BASE_TABLE_BASE_COLUMN_COUNT, "ID", "COL1", "COL2", "VIEW_COL4", "VIEW_COL2");
            assertTableDefinition(conn, "VIEWOFTABLE", PTableType.VIEW, "TABLEWITHVIEW", 1, 9, 5, "ID", "COL1", "COL2", "VIEW_COL1", "VIEW_COL2", "VIEW_COL3", "VIEW_COL4", "VIEW_COL5", "VIEW_COL6");
            
            // query table
            ResultSet rs = stmt.executeQuery("SELECT * FROM TABLEWITHVIEW");
            assertTrue(rs.next());
            assertEquals("view1", rs.getString("ID"));
            assertEquals(12, rs.getInt("COL1"));
            assertEquals(13, rs.getInt("COL2"));
            assertEquals("view5", rs.getString("VIEW_COL2"));
            assertEquals(17, rs.getInt("VIEW_COL4"));
            assertFalse(rs.next());

            // query view
            rs = stmt.executeQuery("SELECT * FROM VIEWOFTABLE");
            assertTrue(rs.next());
            assertEquals("view1", rs.getString("ID"));
            assertEquals(12, rs.getInt("COL1"));
            assertEquals(13, rs.getInt("COL2"));
            assertEquals(14, rs.getInt("VIEW_COL1"));
            assertEquals("view5", rs.getString("VIEW_COL2"));
            assertEquals("view6", rs.getString("VIEW_COL3"));
            assertEquals(17, rs.getInt("VIEW_COL4"));
            assertEquals(18, rs.getInt("VIEW_COL5"));
            assertEquals("view9", rs.getString("VIEW_COL6"));
            assertFalse(rs.next());
        } finally {
            conn.close();
        }
    }
    
    @Test
    public void testAddExistingViewPkColumnToBaseTableWithViews() throws Exception {
        Connection conn = DriverManager.getConnection(getUrl());
        try {       
            conn.createStatement().execute("CREATE TABLE IF NOT EXISTS TABLEWITHVIEW ("
                    + " ID char(10) NOT NULL,"
                    + " COL1 integer NOT NULL,"
                    + " COL2 integer NOT NULL,"
                    + " CONSTRAINT NAME_PK PRIMARY KEY (ID, COL1, COL2)"
                    + " )");
            assertTableDefinition(conn, "TABLEWITHVIEW", PTableType.TABLE, null, 0, 3, QueryConstants.BASE_TABLE_BASE_COLUMN_COUNT, "ID", "COL1", "COL2");
            
            conn.createStatement().execute("CREATE VIEW VIEWOFTABLE ( VIEW_COL1 DECIMAL(10,2), VIEW_COL2 VARCHAR(256) CONSTRAINT pk PRIMARY KEY (VIEW_COL1, VIEW_COL2)) AS SELECT * FROM TABLEWITHVIEW");
            assertTableDefinition(conn, "VIEWOFTABLE", PTableType.VIEW, "TABLEWITHVIEW", 0, 5, 3, "ID", "COL1", "COL2", "VIEW_COL1", "VIEW_COL2");
            
            // upsert single row into view
            String dml = "UPSERT INTO VIEWOFTABLE VALUES(?,?,?,?,?)";
            PreparedStatement stmt = conn.prepareStatement(dml);
            stmt.setString(1, "view1");
            stmt.setInt(2, 12);
            stmt.setInt(3, 13);
            stmt.setInt(4, 14);
            stmt.setString(5, "view5");
            stmt.execute();
            conn.commit();
            
            try {
            	// should fail because there we have to add both VIEW_COL1 and VIEW_COL2 to the pk
            	conn.createStatement().execute("ALTER TABLE TABLEWITHVIEW ADD VIEW_COL2 VARCHAR(256) PRIMARY KEY");
            	fail();
            }
            catch (SQLException e) {
            	assertEquals("Unexpected exception", CANNOT_MUTATE_TABLE.getErrorCode(), e.getErrorCode());
            }
            
            try {
            	// should fail because there we have to add both VIEW_COL1 and VIEW_COL2  to the pk 
            	conn.createStatement().execute("ALTER TABLE TABLEWITHVIEW ADD VIEW_COL1 DECIMAL(10,2) PRIMARY KEY");
            	fail();
            }
            catch (SQLException e) {
            	assertEquals("Unexpected exception", CANNOT_MUTATE_TABLE.getErrorCode(), e.getErrorCode());
            }
            
            try {
            	// should fail because there we have to add both VIEW_COL1 and VIEW_COL2 to the pk
            	conn.createStatement().execute("ALTER TABLE TABLEWITHVIEW ADD VIEW_COL1 DECIMAL(10,2), VIEW_COL2 VARCHAR(256) PRIMARY KEY");
            	fail();
            }
            catch (SQLException e) {
            	assertEquals("Unexpected exception", CANNOT_MUTATE_TABLE.getErrorCode(), e.getErrorCode());
            }
            
            try {
            	// should fail because there we have to add both VIEW_COL1 and VIEW_COL2  to the pk 
            	conn.createStatement().execute("ALTER TABLE TABLEWITHVIEW ADD VIEW_COL1 DECIMAL(10,2) PRIMARY KEY, VIEW_COL2 VARCHAR(256)");
            	fail();
            }
            catch (SQLException e) {
            	assertEquals("Unexpected exception", CANNOT_MUTATE_TABLE.getErrorCode(), e.getErrorCode());
            }
            
            try {
            	// should fail because there we have to add both VIEW_COL1 and VIEW_COL2 to the pk in the right order
            	conn.createStatement().execute("ALTER TABLE TABLEWITHVIEW ADD VIEW_COL2 VARCHAR(256) PRIMARY KEY, VIEW_COL1 DECIMAL(10,2) PRIMARY KEY");
            	fail();
            }
            catch (SQLException e) {
            	assertEquals("Unexpected exception", CANNOT_MUTATE_TABLE.getErrorCode(), e.getErrorCode());
            }
            
            try {
            	// should fail because there we have to add both VIEW_COL1 and VIEW_COL2 with the right sort order
            	conn.createStatement().execute("ALTER TABLE TABLEWITHVIEW ADD VIEW_COL1 DECIMAL(10,2) PRIMARY KEY DESC, VIEW_COL2 VARCHAR(256) PRIMARY KEY");
            	fail();
            }
            catch (SQLException e) {
            	assertEquals("Unexpected exception", CANNOT_MUTATE_TABLE.getErrorCode(), e.getErrorCode());
            }
            
            // add the pk column of the view to the base table
            conn.createStatement().execute("ALTER TABLE TABLEWITHVIEW ADD VIEW_COL1 DECIMAL(10,2) PRIMARY KEY, VIEW_COL2 VARCHAR(256) PRIMARY KEY");
            assertTableDefinition(conn, "TABLEWITHVIEW", PTableType.TABLE, null, 1, 5, QueryConstants.BASE_TABLE_BASE_COLUMN_COUNT, "ID", "COL1", "COL2", "VIEW_COL1", "VIEW_COL2");
            assertTableDefinition(conn, "VIEWOFTABLE", PTableType.VIEW, "TABLEWITHVIEW", 1, 5, 5, "ID", "COL1", "COL2", "VIEW_COL1", "VIEW_COL2");
            
            // query table
            ResultSet rs = stmt.executeQuery("SELECT * FROM TABLEWITHVIEW");
            assertTrue(rs.next());
            assertEquals("view1", rs.getString("ID"));
            assertEquals(12, rs.getInt("COL1"));
            assertEquals(13, rs.getInt("COL2"));
            assertEquals(14, rs.getInt("VIEW_COL1"));
            assertEquals("view5", rs.getString("VIEW_COL2"));
            assertFalse(rs.next());

            // query view
            rs = stmt.executeQuery("SELECT * FROM VIEWOFTABLE");
            assertTrue(rs.next());
            assertEquals("view1", rs.getString("ID"));
            assertEquals(12, rs.getInt("COL1"));
            assertEquals(13, rs.getInt("COL2"));
            assertEquals(14, rs.getInt("VIEW_COL1"));
            assertEquals("view5", rs.getString("VIEW_COL2"));
            assertFalse(rs.next());
        } finally {
            conn.close();
        }
    }
    
    @Test
    public void testAddExistingViewPkColumnToBaseTableWithMultipleViews() throws Exception {
        Connection conn = DriverManager.getConnection(getUrl());
        try {       
            conn.createStatement().execute("CREATE TABLE IF NOT EXISTS TABLEWITHVIEW ("
                    + " ID char(10) NOT NULL,"
                    + " COL1 integer NOT NULL,"
                    + " COL2 integer NOT NULL,"
                    + " CONSTRAINT NAME_PK PRIMARY KEY (ID, COL1, COL2)"
                    + " )");
            assertTableDefinition(conn, "TABLEWITHVIEW", PTableType.TABLE, null, 0, 3, QueryConstants.BASE_TABLE_BASE_COLUMN_COUNT, "ID", "COL1", "COL2");
            
            conn.createStatement().execute("CREATE VIEW VIEWOFTABLE1 ( VIEW_COL1 DECIMAL(10,2), VIEW_COL2 VARCHAR(256) CONSTRAINT pk PRIMARY KEY (VIEW_COL1, VIEW_COL2)) AS SELECT * FROM TABLEWITHVIEW");
            assertTableDefinition(conn, "VIEWOFTABLE1", PTableType.VIEW, "TABLEWITHVIEW", 0, 5, 3, "ID", "COL1", "COL2", "VIEW_COL1", "VIEW_COL2");
            
            conn.createStatement().execute("CREATE VIEW VIEWOFTABLE2 ( VIEW_COL3 VARCHAR(256), VIEW_COL4 DECIMAL(10,2) CONSTRAINT pk PRIMARY KEY (VIEW_COL3, VIEW_COL4)) AS SELECT * FROM TABLEWITHVIEW");
            assertTableDefinition(conn, "VIEWOFTABLE2", PTableType.VIEW, "TABLEWITHVIEW", 0, 5, 3, "ID", "COL1", "COL2", "VIEW_COL3", "VIEW_COL4");
            
            try {
            	// should fail because there are two view with different pk columns
            	conn.createStatement().execute("ALTER TABLE TABLEWITHVIEW ADD VIEW_COL1 DECIMAL PRIMARY KEY, VIEW_COL2 VARCHAR PRIMARY KEY");
            	fail();
            }
            catch (SQLException e) {
            	assertEquals("Unexpected exception", CANNOT_MUTATE_TABLE.getErrorCode(), e.getErrorCode());
            }
            
            try {
            	// should fail because there are two view with different pk columns
            	conn.createStatement().execute("ALTER TABLE TABLEWITHVIEW ADD VIEW_COL3 VARCHAR PRIMARY KEY, VIEW_COL4 DECIMAL PRIMARY KEY");
            	fail();
            }
            catch (SQLException e) {
            	assertEquals("Unexpected exception", CANNOT_MUTATE_TABLE.getErrorCode(), e.getErrorCode());
            }
            
            try {
            	// should fail because slot positions of pks are different
            	conn.createStatement().execute("ALTER TABLE TABLEWITHVIEW ADD VIEW_COL1 DECIMAL PRIMARY KEY, VIEW_COL2 VARCHAR PRIMARY KEY, VIEW_COL3 VARCHAR PRIMARY KEY, VIEW_COL4 DECIMAL PRIMARY KEY");
            	fail();
            }
            catch (SQLException e) {
            	assertEquals("Unexpected exception", CANNOT_MUTATE_TABLE.getErrorCode(), e.getErrorCode());
            }
            
            try {
            	// should fail because slot positions of pks are different
            	conn.createStatement().execute("ALTER TABLE TABLEWITHVIEW ADD VIEW_COL3 VARCHAR PRIMARY KEY, VIEW_COL4 DECIMAL PRIMARY KEY, VIEW_COL1 DECIMAL PRIMARY KEY, VIEW_COL2 VARCHAR PRIMARY KEY");
            	fail();
            }
            catch (SQLException e) {
            	assertEquals("Unexpected exception", CANNOT_MUTATE_TABLE.getErrorCode(), e.getErrorCode());
            }
        } finally {
            conn.close();
        }
    }
    
    @Test
    public void testAddExistingViewPkColumnToBaseTableWithMultipleViewsHavingSamePks() throws Exception {
        Connection conn = DriverManager.getConnection(getUrl());
        try {       
            conn.createStatement().execute("CREATE TABLE IF NOT EXISTS TABLEWITHVIEW ("
                    + " ID char(10) NOT NULL,"
                    + " COL1 integer NOT NULL,"
                    + " COL2 integer NOT NULL,"
                    + " CONSTRAINT NAME_PK PRIMARY KEY (ID, COL1, COL2)"
                    + " )");
            assertTableDefinition(conn, "TABLEWITHVIEW", PTableType.TABLE, null, 0, 3, QueryConstants.BASE_TABLE_BASE_COLUMN_COUNT, "ID", "COL1", "COL2");
            
            conn.createStatement().execute("CREATE VIEW VIEWOFTABLE1 ( VIEW_COL1 DECIMAL(10,2), VIEW_COL2 VARCHAR(256) CONSTRAINT pk PRIMARY KEY (VIEW_COL1, VIEW_COL2)) AS SELECT * FROM TABLEWITHVIEW");
            assertTableDefinition(conn, "VIEWOFTABLE1", PTableType.VIEW, "TABLEWITHVIEW", 0, 5, 3, "ID", "COL1", "COL2", "VIEW_COL1", "VIEW_COL2");
            
            conn.createStatement().execute("CREATE VIEW VIEWOFTABLE2 ( VIEW_COL1 DECIMAL(10,2), VIEW_COL2 VARCHAR(256) CONSTRAINT pk PRIMARY KEY (VIEW_COL1, VIEW_COL2)) AS SELECT * FROM TABLEWITHVIEW");
            assertTableDefinition(conn, "VIEWOFTABLE2", PTableType.VIEW, "TABLEWITHVIEW", 0, 5, 3, "ID", "COL1", "COL2", "VIEW_COL1", "VIEW_COL2");
            
            // upsert single row into both view
            String dml = "UPSERT INTO VIEWOFTABLE1 VALUES(?,?,?,?,?)";
            PreparedStatement stmt = conn.prepareStatement(dml);
            stmt.setString(1, "view1");
            stmt.setInt(2, 12);
            stmt.setInt(3, 13);
            stmt.setInt(4, 14);
            stmt.setString(5, "view5");
            stmt.execute();
            conn.commit();
            dml = "UPSERT INTO VIEWOFTABLE2 VALUES(?,?,?,?,?)";
            stmt = conn.prepareStatement(dml);
            stmt.setString(1, "view1");
            stmt.setInt(2, 12);
            stmt.setInt(3, 13);
            stmt.setInt(4, 14);
            stmt.setString(5, "view5");
            stmt.execute();
            conn.commit();
            
            try {
            	// should fail because the view have two extra columns in their pk
            	conn.createStatement().execute("ALTER TABLE TABLEWITHVIEW ADD VIEW_COL1 DECIMAL(10,2) PRIMARY KEY");
            	fail();
            }
            catch (SQLException e) {
            	assertEquals("Unexpected exception", CANNOT_MUTATE_TABLE.getErrorCode(), e.getErrorCode());
            }
            
            try {
            	// should fail because the view have two extra columns in their pk
            	conn.createStatement().execute("ALTER TABLE TABLEWITHVIEW ADD VIEW_COL2 VARCHAR(256) PRIMARY KEY");
            	fail();
            }
            catch (SQLException e) {
            	assertEquals("Unexpected exception", CANNOT_MUTATE_TABLE.getErrorCode(), e.getErrorCode());
            }
            
            try {
            	// should fail because slot positions of pks are different
            	conn.createStatement().execute("ALTER TABLE TABLEWITHVIEW ADD VIEW_COL2 DECIMAL(10,2) PRIMARY KEY, VIEW_COL1 VARCHAR(256) PRIMARY KEY");
            	fail();
            }
            catch (SQLException e) {
            	assertEquals("Unexpected exception", CANNOT_MUTATE_TABLE.getErrorCode(), e.getErrorCode());
            }
            
            conn.createStatement().execute("ALTER TABLE TABLEWITHVIEW ADD VIEW_COL1 DECIMAL(10,2) PRIMARY KEY, VIEW_COL2 VARCHAR(256) PRIMARY KEY");
            assertTableDefinition(conn, "TABLEWITHVIEW", PTableType.TABLE, null, 1, 5, QueryConstants.BASE_TABLE_BASE_COLUMN_COUNT, "ID", "COL1", "COL2", "VIEW_COL1", "VIEW_COL2");
            assertTableDefinition(conn, "VIEWOFTABLE1", PTableType.VIEW, "TABLEWITHVIEW", 1, 5, 5, "ID", "COL1", "COL2", "VIEW_COL1", "VIEW_COL2");
            assertTableDefinition(conn, "VIEWOFTABLE2", PTableType.VIEW, "TABLEWITHVIEW", 1, 5, 5, "ID", "COL1", "COL2", "VIEW_COL1", "VIEW_COL2");
            
            // query table
            ResultSet rs = stmt.executeQuery("SELECT * FROM TABLEWITHVIEW");
            assertTrue(rs.next());
            assertEquals("view1", rs.getString("ID"));
            assertEquals(12, rs.getInt("COL1"));
            assertEquals(13, rs.getInt("COL2"));
            assertEquals(14, rs.getInt("VIEW_COL1"));
            assertEquals("view5", rs.getString("VIEW_COL2"));
            assertFalse(rs.next());

            // query both views
            rs = stmt.executeQuery("SELECT * FROM VIEWOFTABLE1");
            assertTrue(rs.next());
            assertEquals("view1", rs.getString("ID"));
            assertEquals(12, rs.getInt("COL1"));
            assertEquals(13, rs.getInt("COL2"));
            assertEquals(14, rs.getInt("VIEW_COL1"));
            assertEquals("view5", rs.getString("VIEW_COL2"));
            assertFalse(rs.next());
            rs = stmt.executeQuery("SELECT * FROM VIEWOFTABLE2");
            assertTrue(rs.next());
            assertEquals("view1", rs.getString("ID"));
            assertEquals(12, rs.getInt("COL1"));
            assertEquals(13, rs.getInt("COL2"));
            assertEquals(14, rs.getInt("VIEW_COL1"));
            assertEquals("view5", rs.getString("VIEW_COL2"));
            assertFalse(rs.next());
        } finally {
            conn.close();
        }
    }
    
    private void assertTableDefinition(Connection conn, String tableName, PTableType tableType, String parentTableName, int sequenceNumber, int columnCount, int baseColumnCount, String... columnName) throws Exception {
        PreparedStatement p = conn.prepareStatement("SELECT * FROM SYSTEM.CATALOG WHERE TABLE_NAME=? AND TABLE_TYPE=?");
        p.setString(1, tableName);
        p.setString(2, tableType.getSerializedValue());
        ResultSet rs = p.executeQuery();
        assertTrue(rs.next());
        assertEquals(getSystemCatalogEntriesForTable(conn, tableName, "Mismatch in BaseColumnCount"), baseColumnCount, rs.getInt("BASE_COLUMN_COUNT"));
        assertEquals(getSystemCatalogEntriesForTable(conn, tableName, "Mismatch in columnCount"), columnCount, rs.getInt("COLUMN_COUNT"));
        assertEquals(getSystemCatalogEntriesForTable(conn, tableName, "Mismatch in sequenceNumber"), sequenceNumber, rs.getInt("TABLE_SEQ_NUM"));
        rs.close();

        ResultSet parentTableColumnsRs = null; 
        if (parentTableName != null) {
            parentTableColumnsRs = conn.getMetaData().getColumns(null, null, parentTableName, null);
            parentTableColumnsRs.next();
        }
        
        ResultSet viewColumnsRs = conn.getMetaData().getColumns(null, null, tableName, null);
        for (int i = 0; i < columnName.length; i++) {
            if (columnName[i] != null) {
                assertTrue(viewColumnsRs.next());
                assertEquals(getSystemCatalogEntriesForTable(conn, tableName, "Mismatch in columnName: i=" + i), columnName[i], viewColumnsRs.getString(PhoenixDatabaseMetaData.COLUMN_NAME));
                assertEquals(getSystemCatalogEntriesForTable(conn, tableName, "Mismatch in ordinalPosition: i=" + i), i+1, viewColumnsRs.getInt(PhoenixDatabaseMetaData.ORDINAL_POSITION));
                // validate that all the columns in the base table are present in the view   
                if (parentTableColumnsRs != null && !parentTableColumnsRs.isAfterLast()) {
                    ResultSetMetaData parentTableColumnsMetadata = parentTableColumnsRs.getMetaData();
                    assertEquals(parentTableColumnsMetadata.getColumnCount(), viewColumnsRs.getMetaData().getColumnCount());
                    
                    // if you add a non-pk column that already exists in the view
                    if (!viewColumnsRs.getString(PhoenixDatabaseMetaData.COLUMN_NAME).equals(parentTableColumnsRs.getString(PhoenixDatabaseMetaData.COLUMN_NAME))) {
                    	continue;
                    }
                    
                    for (int columnIndex = 1; columnIndex < parentTableColumnsMetadata.getColumnCount(); columnIndex++) {
                        String viewColumnValue = viewColumnsRs.getString(columnIndex);
                        String parentTableColumnValue = parentTableColumnsRs.getString(columnIndex);
                        if (!Objects.equal(viewColumnValue, parentTableColumnValue)) {
                            if (parentTableColumnsMetadata.getColumnName(columnIndex).equals(PhoenixDatabaseMetaData.TABLE_NAME)) {
                                assertEquals(parentTableName, parentTableColumnValue);
                                assertEquals(tableName, viewColumnValue);
                            } 
                            // its ok if the ordinal positions don't match for non-pk columns
                            else if (!(parentTableColumnsMetadata.getColumnName(columnIndex).equals(PhoenixDatabaseMetaData.ORDINAL_POSITION) && parentTableColumnsRs.getString(PhoenixDatabaseMetaData.COLUMN_FAMILY)!=null)) {
                                fail(parentTableColumnsMetadata.getColumnName(columnIndex) + " of base table " + parentTableColumnValue + " does not match view "+viewColumnValue) ;
                            }
                        }
                    }
                    parentTableColumnsRs.next();
                }
            }
        }
        assertFalse(getSystemCatalogEntriesForTable(conn, tableName, ""), viewColumnsRs.next());
    }
    
    private String getSystemCatalogEntriesForTable(Connection conn, String tableName, String message) throws Exception {
        StringBuilder sb = new StringBuilder(message);
        sb.append("\n\n\n");
        ResultSet rs = conn.createStatement().executeQuery("SELECT * FROM SYSTEM.CATALOG WHERE TABLE_NAME='"+ tableName +"'");
        ResultSetMetaData metaData = rs.getMetaData();
        int rowNum = 0;
        while (rs.next()) {
            sb.append(rowNum++).append("------\n");
            for (int i = 1; i <= metaData.getColumnCount(); i++) {
                sb.append("\t").append(metaData.getColumnLabel(i)).append("=").append(rs.getString(i)).append("\n");
            }
            sb.append("\n");
        }
        rs.close();
        return sb.toString();
    }
    
    @Test
    public void testCacheInvalidatedAfterAddingColumnToBaseTableWithViews() throws Exception {
        String baseTable = "testCacheInvalidatedAfterAddingColumnToBaseTableWithViews";
        String viewName = baseTable + "_view";
        String tenantId = "tenantId";
        try (Connection globalConn = DriverManager.getConnection(getUrl())) {
            String tableDDL = "CREATE TABLE " + baseTable + " (TENANT_ID VARCHAR NOT NULL, PK1 VARCHAR NOT NULL, V1 VARCHAR CONSTRAINT NAME_PK PRIMARY KEY(TENANT_ID, PK1)) MULTI_TENANT = true " ;
            globalConn.createStatement().execute(tableDDL);
            Properties tenantProps = new Properties();
            tenantProps.setProperty(PhoenixRuntime.TENANT_ID_ATTRIB, tenantId);
            // create a tenant specific view
            try (Connection tenantConn =  DriverManager.getConnection(getUrl(), tenantProps)) {
                String viewDDL = "CREATE VIEW " + viewName + " AS SELECT * FROM " + baseTable;
                tenantConn.createStatement().execute(viewDDL);
                
                // Add a column to the base table using global connection
                globalConn.createStatement().execute("ALTER TABLE " + baseTable + " ADD NEW_COL VARCHAR");

                // Check now whether the tenant connection can see the column that was added
                tenantConn.createStatement().execute("SELECT NEW_COL FROM " + viewName);
                tenantConn.createStatement().execute("SELECT NEW_COL FROM " + baseTable);
            }
        }
    }
    
    @Test
    public void testDropColumnOnTableWithViewsNotAllowed() throws Exception {
        String baseTable = "testDropColumnOnTableWithViewsNotAllowed";
        String viewName = baseTable + "_view";
        try (Connection conn = DriverManager.getConnection(getUrl())) {
            String tableDDL = "CREATE TABLE " + baseTable + " (PK1 VARCHAR NOT NULL PRIMARY KEY, V1 VARCHAR, V2 VARCHAR)";
            conn.createStatement().execute(tableDDL);
            
            String viewDDL = "CREATE VIEW " + viewName + " AS SELECT * FROM " + baseTable;
            conn.createStatement().execute(viewDDL);
            
            String dropColumn = "ALTER TABLE " + baseTable + " DROP COLUMN V2";
            try {
                conn.createStatement().execute(dropColumn);
                fail("Dropping column on a base table that has views is not allowed");
            } catch (SQLException e) {  
                assertEquals(CANNOT_MUTATE_TABLE.getErrorCode(), e.getErrorCode());
            }
        }
    }
    
    @Test
    public void testAlteringViewThatHasChildViews() throws Exception {
        String baseTable = "testAlteringViewThatHasChildViews";
        String childView = "childView";
        String grandChildView = "grandChildView";
        try (Connection conn = DriverManager.getConnection(getUrl())) {
            String baseTableDDL =
                    "CREATE TABLE " +  baseTable + " (TENANT_ID VARCHAR NOT NULL, PK2 VARCHAR NOT NULL, V1 VARCHAR, V2 VARCHAR CONSTRAINT NAME_PK PRIMARY KEY(TENANT_ID, PK2))";
            conn.createStatement().execute(baseTableDDL);

            String childViewDDL = "CREATE VIEW " + childView + " AS SELECT * FROM " + baseTable;
            conn.createStatement().execute(childViewDDL);

            String addColumnToChildViewDDL =
                    "ALTER VIEW " + childView + " ADD CHILD_VIEW_COL VARCHAR";
            conn.createStatement().execute(addColumnToChildViewDDL);

            String grandChildViewDDL =
                    "CREATE VIEW " + grandChildView + " AS SELECT * FROM " + childView;
            conn.createStatement().execute(grandChildViewDDL);

            // dropping base table column from child view should succeed
            String dropColumnFromChildView = "ALTER VIEW " + childView + " DROP COLUMN V2";
            conn.createStatement().execute(dropColumnFromChildView);

            // dropping view specific column from child view should succeed
            dropColumnFromChildView = "ALTER VIEW " + childView + " DROP COLUMN CHILD_VIEW_COL";
            conn.createStatement().execute(dropColumnFromChildView);
            
            // Adding column to view that has child views is allowed
            String addColumnToChildView = "ALTER VIEW " + childView + " ADD V5 VARCHAR";
            conn.createStatement().execute(addColumnToChildView);
            // V5 column should be visible now for childView
            conn.createStatement().execute("SELECT V5 FROM " + childView);    
            
            // However, column V5 shouldn't have propagated to grandChildView. Not till PHOENIX-2054 is fixed.
            try {
                conn.createStatement().execute("SELECT V5 FROM " + grandChildView);
            } catch (SQLException e) {
                assertEquals(SQLExceptionCode.COLUMN_NOT_FOUND.getErrorCode(), e.getErrorCode());
            }

            // dropping column from the grand child view, however, should work.
            String dropColumnFromGrandChildView =
                    "ALTER VIEW " + grandChildView + " DROP COLUMN CHILD_VIEW_COL";
            conn.createStatement().execute(dropColumnFromGrandChildView);

            // similarly, dropping column inherited from the base table should work.
            dropColumnFromGrandChildView = "ALTER VIEW " + grandChildView + " DROP COLUMN V2";
            conn.createStatement().execute(dropColumnFromGrandChildView);
        }
    }
    
    @Test
    public void testDivorcedViewsStayDivorced() throws Exception {
        String baseTable = "testDivorcedViewsStayDivorced";
        String viewName = baseTable + "_view";
        try (Connection conn = DriverManager.getConnection(getUrl())) {
            String tableDDL = "CREATE TABLE " + baseTable + " (PK1 VARCHAR NOT NULL PRIMARY KEY, V1 VARCHAR, V2 VARCHAR)";
            conn.createStatement().execute(tableDDL);
            
            String viewDDL = "CREATE VIEW " + viewName + " AS SELECT * FROM " + baseTable;
            conn.createStatement().execute(viewDDL);
            
            // Drop the column inherited from base table to divorce the view
            String dropColumn = "ALTER VIEW " + viewName + " DROP COLUMN V2";
            conn.createStatement().execute(dropColumn);
            
            String alterBaseTable = "ALTER TABLE " + baseTable + " ADD V3 VARCHAR";
            try {
            	conn.createStatement().execute(alterBaseTable);
	            fail();
	        }
	        catch (SQLException e) {
	        	assertEquals("Unexpected exception", CANNOT_MUTATE_TABLE.getErrorCode(), e.getErrorCode());
	        }
            
            // Column V3 shouldn't have propagated to the divorced view.
            String sql = "SELECT V3 FROM " + viewName;
            try {
                conn.createStatement().execute(sql);
            } catch (SQLException e) {
                assertEquals(SQLExceptionCode.COLUMN_NOT_FOUND.getErrorCode(), e.getErrorCode());
            }
        } 
    }
    
    @Test
    public void testAddingColumnToBaseTablePropagatesToEntireViewHierarchy() throws Exception {
        String baseTable = "testViewHierarchy";
        String view1 = "view1";
        String view2 = "view2";
        String view3 = "view3";
        String view4 = "view4";
        /*                                     baseTable
                                 /                  |               \ 
                         view1(tenant1)    view3(tenant2)          view4(global)
                          /
                        view2(tenant1)  
        */
        try (Connection conn = DriverManager.getConnection(getUrl())) {
            String baseTableDDL = "CREATE TABLE " + baseTable + " (TENANT_ID VARCHAR NOT NULL, PK1 VARCHAR NOT NULL, V1 VARCHAR, V2 VARCHAR CONSTRAINT NAME_PK PRIMARY KEY(TENANT_ID, PK1)) MULTI_TENANT = true ";
            conn.createStatement().execute(baseTableDDL);
            
            try (Connection tenant1Conn = getTenantConnection("tenant1")) {
                String view1DDL = "CREATE VIEW " + view1 + " AS SELECT * FROM " + baseTable;
                tenant1Conn.createStatement().execute(view1DDL);
                
                String view2DDL = "CREATE VIEW " + view2 + " AS SELECT * FROM " + view1;
                tenant1Conn.createStatement().execute(view2DDL);
            }
            
            try (Connection tenant2Conn = getTenantConnection("tenant2")) {
                String view3DDL = "CREATE VIEW " + view3 + " AS SELECT * FROM " + baseTable;
                tenant2Conn.createStatement().execute(view3DDL);
            }
            
            String view4DDL = "CREATE VIEW " + view4 + " AS SELECT * FROM " + baseTable;
            conn.createStatement().execute(view4DDL);
            
            String alterBaseTable = "ALTER TABLE " + baseTable + " ADD V3 VARCHAR";
            conn.createStatement().execute(alterBaseTable);
            
            // verify that the column is visible to view4
            conn.createStatement().execute("SELECT V3 FROM " + view4);
            
            // verify that the column is visible to view1 and view2
            try (Connection tenant1Conn = getTenantConnection("tenant1")) {
                tenant1Conn.createStatement().execute("SELECT V3 from " + view1);
                tenant1Conn.createStatement().execute("SELECT V3 from " + view2);
            }
            
            // verify that the column is visible to view3
            try (Connection tenant2Conn = getTenantConnection("tenant2")) {
                tenant2Conn.createStatement().execute("SELECT V3 from " + view3);
            }
            
        }
           
    }
    
    @Test
    public void testChangingPKOfBaseTableChangesPKForAllViews() throws Exception {
        String baseTable = "testChangePKOfBaseTable";
        String view1 = "view1";
        String view2 = "view2";
        String view3 = "view3";
        String view4 = "view4";
        /*                                     baseTable
                                 /                  |               \ 
                         view1(tenant1)    view3(tenant2)          view4(global)
                          /
                        view2(tenant1)  
         */
        Connection tenant1Conn = null, tenant2Conn = null;
        try (Connection globalConn = DriverManager.getConnection(getUrl())) {
            String baseTableDDL = "CREATE TABLE "
                    + baseTable
                    + " (TENANT_ID VARCHAR NOT NULL, PK1 VARCHAR NOT NULL, V1 VARCHAR, V2 VARCHAR CONSTRAINT NAME_PK PRIMARY KEY(TENANT_ID, PK1)) MULTI_TENANT = true ";
            globalConn.createStatement().execute(baseTableDDL);

            tenant1Conn = getTenantConnection("tenant1");
            String view1DDL = "CREATE VIEW " + view1 + " AS SELECT * FROM " + baseTable;
            tenant1Conn.createStatement().execute(view1DDL);

            String view2DDL = "CREATE VIEW " + view2 + " AS SELECT * FROM " + view1;
            tenant1Conn.createStatement().execute(view2DDL);

            tenant2Conn = getTenantConnection("tenant2");
            String view3DDL = "CREATE VIEW " + view3 + " AS SELECT * FROM " + baseTable;
            tenant2Conn.createStatement().execute(view3DDL);

            String view4DDL = "CREATE VIEW " + view4 + " AS SELECT * FROM " + baseTable;
            globalConn.createStatement().execute(view4DDL);

            String alterBaseTable = "ALTER TABLE " + baseTable + " ADD NEW_PK varchar primary key ";
            globalConn.createStatement().execute(alterBaseTable);
            
            // verify that the new column new_pk is now part of the primary key for the entire hierarchy
            
            globalConn.createStatement().execute("SELECT * FROM " + baseTable);
            assertTrue(checkColumnPartOfPk(globalConn.unwrap(PhoenixConnection.class), "NEW_PK", baseTable));
            
            tenant1Conn.createStatement().execute("SELECT * FROM " + view1);
            assertTrue(checkColumnPartOfPk(tenant1Conn.unwrap(PhoenixConnection.class), "NEW_PK", view1));
            
            tenant1Conn.createStatement().execute("SELECT * FROM " + view2);
            assertTrue(checkColumnPartOfPk(tenant1Conn.unwrap(PhoenixConnection.class), "NEW_PK", view2));
            
            tenant2Conn.createStatement().execute("SELECT * FROM " + view3);
            assertTrue(checkColumnPartOfPk(tenant2Conn.unwrap(PhoenixConnection.class), "NEW_PK", view3));
            
            globalConn.createStatement().execute("SELECT * FROM " + view4);
            assertTrue(checkColumnPartOfPk(globalConn.unwrap(PhoenixConnection.class), "NEW_PK", view4));

        } finally {
            if (tenant1Conn != null) {
                try {
                    tenant1Conn.close();
                } catch (Throwable ignore) {}
            }
            if (tenant2Conn != null) {
                try {
                    tenant2Conn.close();
                } catch (Throwable ignore) {}
            }
        }

    }
    
    private boolean checkColumnPartOfPk(PhoenixConnection conn, String columnName, String tableName) throws SQLException {
        String normalizedTableName = SchemaUtil.normalizeIdentifier(tableName);
        PTable table = conn.getMetaDataCache().getTable(new PTableKey(conn.getTenantId(), normalizedTableName));
        List<PColumn> pkCols = table.getPKColumns();
        String normalizedColumnName = SchemaUtil.normalizeIdentifier(columnName);
        for (PColumn pkCol : pkCols) {
            if (pkCol.getName().getString().equals(normalizedColumnName)) {
                return true;
            }
        }
        return false;
    }
    
    private int getIndexOfPkColumn(PhoenixConnection conn, String columnName, String tableName) throws SQLException {
        String normalizedTableName = SchemaUtil.normalizeIdentifier(tableName);
        PTable table = conn.getMetaDataCache().getTable(new PTableKey(conn.getTenantId(), normalizedTableName));
        List<PColumn> pkCols = table.getPKColumns();
        String normalizedColumnName = SchemaUtil.normalizeIdentifier(columnName);
        int i = 0;
        for (PColumn pkCol : pkCols) {
            if (pkCol.getName().getString().equals(normalizedColumnName)) {
                return i;
            }
            i++;
        }
        return -1;
    }
    
    private Connection getTenantConnection(String tenantId) throws Exception {
        Properties tenantProps = new Properties();
        tenantProps.setProperty(PhoenixRuntime.TENANT_ID_ATTRIB, tenantId);
        return DriverManager.getConnection(getUrl(), tenantProps);
    }
    
    @Test
    public void testAddPKColumnToBaseTableWhoseViewsHaveIndices() throws Exception {
        String baseTable = "testAddPKColumnToBaseTableWhoseViewsHaveIndices";
        String view1 = "view1";
        String view2 = "view2";
        String view3 = "view3";
        String tenant1 = "tenant1";
        String tenant2 = "tenant2";
        String view2Index = view2 + "_idx";
        String view3Index = view3 + "_idx";
        /*                          baseTable(mutli-tenant)
                                 /                           \                
                         view1(tenant1)                  view3(tenant2, index) 
                          /
                        view2(tenant1, index)  
         */
        try (Connection globalConn = DriverManager.getConnection(getUrl())) {
            // make sure that the tables are empty, but reachable
            globalConn
            .createStatement()
            .execute(
                    "CREATE TABLE "
                            + baseTable
                            + " (TENANT_ID VARCHAR NOT NULL, K1 varchar not null, V1 VARCHAR, V2 VARCHAR CONSTRAINT NAME_PK PRIMARY KEY(TENANT_ID, K1)) MULTI_TENANT = true ");

        }
        try (Connection tenantConn = getTenantConnection(tenant1)) {
            // create tenant specific view for tenant1 - view1
            tenantConn.createStatement().execute("CREATE VIEW " + view1 + " AS SELECT * FROM " + baseTable);
            PhoenixConnection phxConn = tenantConn.unwrap(PhoenixConnection.class);
            assertEquals(0, getTableSequenceNumber(phxConn, view1));
            assertEquals(2, getMaxKeySequenceNumber(phxConn, view1));

            // create a view - view2 on view - view1
            tenantConn.createStatement().execute("CREATE VIEW " + view2 + " AS SELECT * FROM " + view1);
            assertEquals(0, getTableSequenceNumber(phxConn, view2));
            assertEquals(2, getMaxKeySequenceNumber(phxConn, view2));


            // create an index on view2
            tenantConn.createStatement().execute("CREATE INDEX " + view2Index + " ON " + view2 + " (v1) include (v2)");
            assertEquals(0, getTableSequenceNumber(phxConn, view2Index));
            assertEquals(4, getMaxKeySequenceNumber(phxConn, view2Index));
        }
        try (Connection tenantConn = getTenantConnection(tenant2)) {
            // create tenant specific view for tenant2 - view3
            tenantConn.createStatement().execute("CREATE VIEW " + view3 + " AS SELECT * FROM " + baseTable);
            PhoenixConnection phxConn = tenantConn.unwrap(PhoenixConnection.class);
            assertEquals(0, getTableSequenceNumber(phxConn, view3));
            assertEquals(2, getMaxKeySequenceNumber(phxConn, view3));


            // create an index on view3
            tenantConn.createStatement().execute("CREATE INDEX " + view3Index + " ON " + view3 + " (v1) include (v2)");
            assertEquals(0, getTableSequenceNumber(phxConn, view3Index));
            assertEquals(4, getMaxKeySequenceNumber(phxConn, view3Index));


        }

        // alter the base table by adding 1 non-pk and 2 pk columns
        try (Connection globalConn = DriverManager.getConnection(getUrl())) {
            globalConn.createStatement().execute("ALTER TABLE " + baseTable + " ADD v3 VARCHAR, k2 VARCHAR PRIMARY KEY, k3 VARCHAR PRIMARY KEY");
            assertEquals(4, getMaxKeySequenceNumber(globalConn.unwrap(PhoenixConnection.class), baseTable));

            // Upsert records in the base table
            String upsert = "UPSERT INTO " + baseTable + " (TENANT_ID, K1, K2, K3, V1, V2, V3) VALUES (?, ?, ?, ?, ?, ?, ?)";
            PreparedStatement stmt = globalConn.prepareStatement(upsert);
            stmt.setString(1, tenant1);
            stmt.setString(2, "K1");
            stmt.setString(3, "K2");
            stmt.setString(4, "K3");
            stmt.setString(5, "V1");
            stmt.setString(6, "V2");
            stmt.setString(7, "V3");
            stmt.executeUpdate();
            stmt.setString(1, tenant2);
            stmt.setString(2, "K11");
            stmt.setString(3, "K22");
            stmt.setString(4, "K33");
            stmt.setString(5, "V11");
            stmt.setString(6, "V22");
            stmt.setString(7, "V33");
            stmt.executeUpdate();
            globalConn.commit();
        }

        // Verify now that the sequence number of data table, indexes and views have changed.
        // Also verify that the newly added pk columns show up as pk columns of data table, indexes and views.
        try (Connection tenantConn = getTenantConnection(tenant1)) {

            ResultSet rs = tenantConn.createStatement().executeQuery("SELECT K2, K3, V3 FROM " + view1);
            PhoenixConnection phxConn = tenantConn.unwrap(PhoenixConnection.class);
            assertEquals(2, getIndexOfPkColumn(phxConn, "k2", view1));
            assertEquals(3, getIndexOfPkColumn(phxConn, "k3", view1));
            assertEquals(1, getTableSequenceNumber(phxConn, view1));
            assertEquals(4, getMaxKeySequenceNumber(phxConn, view1));
            verifyNewColumns(rs, "K2", "K3", "V3");


            rs = tenantConn.createStatement().executeQuery("SELECT K2, K3, V3 FROM " + view2);
            assertEquals(2, getIndexOfPkColumn(phxConn, "k2", view2));
            assertEquals(3, getIndexOfPkColumn(phxConn, "k3", view2));
            assertEquals(1, getTableSequenceNumber(phxConn, view2));
            assertEquals(4, getMaxKeySequenceNumber(phxConn, view2));
            verifyNewColumns(rs, "K2", "K3", "V3");

            assertEquals(4, getIndexOfPkColumn(phxConn, IndexUtil.getIndexColumnName(null, "k2"), view2Index));
            assertEquals(5, getIndexOfPkColumn(phxConn, IndexUtil.getIndexColumnName(null, "k3"), view2Index));
            assertEquals(1, getTableSequenceNumber(phxConn, view2Index));
            assertEquals(6, getMaxKeySequenceNumber(phxConn, view2Index));
        }
        try (Connection tenantConn = getTenantConnection(tenant2)) {
            ResultSet rs = tenantConn.createStatement().executeQuery("SELECT K2, K3, V3 FROM " + view3);
            PhoenixConnection phxConn = tenantConn.unwrap(PhoenixConnection.class);
            assertEquals(2, getIndexOfPkColumn(phxConn, "k2", view3));
            assertEquals(3, getIndexOfPkColumn(phxConn, "k3", view3));
            assertEquals(1, getTableSequenceNumber(phxConn, view3));
            verifyNewColumns(rs, "K22", "K33", "V33");

            assertEquals(4, getIndexOfPkColumn(phxConn, IndexUtil.getIndexColumnName(null, "k2"), view3Index));
            assertEquals(5, getIndexOfPkColumn(phxConn, IndexUtil.getIndexColumnName(null, "k3"), view3Index));
            assertEquals(1, getTableSequenceNumber(phxConn, view3Index));
            assertEquals(6, getMaxKeySequenceNumber(phxConn, view3Index));
        }
        // Verify that the index is actually being used when using newly added pk col
        try (Connection tenantConn = getTenantConnection(tenant1)) {
            String upsert = "UPSERT INTO " + view2 + " (K1, K2, K3, V1, V2, V3) VALUES ('key1', 'key2', 'key3', 'value1', 'value2', 'value3')";
            tenantConn.createStatement().executeUpdate(upsert);
            tenantConn.commit();
            Statement stmt = tenantConn.createStatement();
            String sql = "SELECT V2 FROM " + view2 + " WHERE V1 = 'value1' AND K3 = 'key3'";
            QueryPlan plan = stmt.unwrap(PhoenixStatement.class).optimizeQuery(sql);
            assertTrue(plan.getTableRef().getTable().getName().getString().equals(SchemaUtil.normalizeIdentifier(view2Index)));
            ResultSet rs = tenantConn.createStatement().executeQuery(sql);
            verifyNewColumns(rs, "value2");
        }

    }
    
    private static long getTableSequenceNumber(PhoenixConnection conn, String tableName) throws SQLException {
        PTable table = conn.getMetaDataCache().getTable(new PTableKey(conn.getTenantId(), SchemaUtil.normalizeIdentifier(tableName)));
        return table.getSequenceNumber();
    }
    
    private static short getMaxKeySequenceNumber(PhoenixConnection conn, String tableName) throws SQLException {
        PTable table = conn.getMetaDataCache().getTable(new PTableKey(conn.getTenantId(), SchemaUtil.normalizeIdentifier(tableName)));
        return SchemaUtil.getMaxKeySeq(table);
    }
    
    private static void verifyNewColumns(ResultSet rs, String ... values) throws SQLException {
        assertTrue(rs.next());
        int i = 1;
        for (String value : values) {
            assertEquals(value, rs.getString(i++));
        }
        assertFalse(rs.next());
    }
    
}
