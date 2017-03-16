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
import static org.apache.phoenix.jdbc.PhoenixDatabaseMetaData.COLUMN_FAMILY;
import static org.apache.phoenix.jdbc.PhoenixDatabaseMetaData.COLUMN_NAME;
import static org.apache.phoenix.jdbc.PhoenixDatabaseMetaData.COLUMN_QUALIFIER;
import static org.apache.phoenix.jdbc.PhoenixDatabaseMetaData.COLUMN_QUALIFIER_COUNTER;
import static org.apache.phoenix.jdbc.PhoenixDatabaseMetaData.TABLE_NAME;
import static org.apache.phoenix.jdbc.PhoenixDatabaseMetaData.TABLE_SCHEM;
import static org.apache.phoenix.jdbc.PhoenixDatabaseMetaData.TABLE_SEQ_NUM;
import static org.apache.phoenix.query.QueryConstants.DEFAULT_COLUMN_FAMILY;
import static org.apache.phoenix.query.QueryConstants.ENCODED_CQ_COUNTER_INITIAL_VALUE;
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
import java.sql.Statement;
import java.util.Arrays;
import java.util.Collection;
import java.util.Properties;

import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.KeepDeletedCells;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.phoenix.exception.SQLExceptionCode;
import org.apache.phoenix.jdbc.PhoenixConnection;
import org.apache.phoenix.jdbc.PhoenixDatabaseMetaData;
import org.apache.phoenix.query.BaseTest;
import org.apache.phoenix.query.QueryConstants;
import org.apache.phoenix.query.QueryServices;
import org.apache.phoenix.schema.PTable;
import org.apache.phoenix.schema.PTable.EncodedCQCounter;
import org.apache.phoenix.schema.PTable.QualifierEncodingScheme;
import org.apache.phoenix.schema.PTableKey;
import org.apache.phoenix.schema.TableNotFoundException;
import org.apache.phoenix.util.IndexUtil;
import org.apache.phoenix.util.PropertiesUtil;
import org.apache.phoenix.util.SchemaUtil;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;

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
@RunWith(Parameterized.class)
public class AlterTableIT extends ParallelStatsDisabledIT {
    private String schemaName;
    private String dataTableName;
    private String indexTableName;
    private String localIndexTableName;
    private String viewName;
    private String dataTableFullName;
    private String indexTableFullName;
    private String localIndexTableFullName;
    private String tableDDLOptions;
    private final boolean columnEncoded;
    
    public AlterTableIT(boolean columnEncoded) {
        this.columnEncoded = columnEncoded;
        this.tableDDLOptions = columnEncoded ? "" : "COLUMN_ENCODED_BYTES=0";
    }
    
    @Parameters(name="AlterTableIT_columnEncoded={0}") // name is used by failsafe as file name in reports
    public static Collection<Boolean> data() {
        return Arrays.asList( false, true);
    }

    @Before
    public void setupTableNames() throws Exception {
        schemaName = "";
        dataTableName = generateUniqueName();
        indexTableName = "I_" + generateUniqueName();
        localIndexTableName = "LI_" + generateUniqueName();
        dataTableFullName = SchemaUtil.getTableName(schemaName, dataTableName);
        indexTableFullName = SchemaUtil.getTableName(schemaName, indexTableName);
        localIndexTableFullName = SchemaUtil.getTableName(schemaName, localIndexTableName);
        viewName = generateUniqueName();
    }

    @Test
    public void testAlterTableWithVarBinaryKey() throws Exception {
        Properties props = PropertiesUtil.deepCopy(TEST_PROPERTIES);
        Connection conn = DriverManager.getConnection(getUrl(), props);
        conn.setAutoCommit(false);

        try {
            String ddl = "CREATE TABLE  " + dataTableFullName +
                    "  (a_string varchar not null, a_binary varbinary not null, col1 integer" +
                    "  CONSTRAINT pk PRIMARY KEY (a_string, a_binary)) " + tableDDLOptions;
            createTestTable(getUrl(), ddl);

            ddl = "ALTER TABLE " + dataTableFullName + " ADD b_string VARCHAR NULL PRIMARY KEY";
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
            String ddl = "CREATE TABLE " + dataTableFullName +
                    "  (a_string varchar not null, col1 integer" +
                    "  CONSTRAINT pk PRIMARY KEY (a_string)) " + tableDDLOptions;
            conn.createStatement().execute(ddl);

            String dml = "UPSERT INTO " + dataTableFullName + " VALUES(?)";
            PreparedStatement stmt = conn.prepareStatement(dml);
            stmt.setString(1, "b");
            stmt.execute();
            stmt.setString(1, "a");
            stmt.execute();
            conn.commit();

            String query = "SELECT * FROM " + dataTableFullName;
            ResultSet rs = conn.createStatement().executeQuery(query);
            assertTrue(rs.next());
            assertEquals("a",rs.getString(1));
            assertTrue(rs.next());
            assertEquals("b",rs.getString(1));
            assertFalse(rs.next());

            ddl = "ALTER TABLE " + dataTableFullName + " ADD  b_string VARCHAR  NULL PRIMARY KEY  ";
            conn.createStatement().execute(ddl);

            query = "SELECT * FROM " + dataTableFullName + " WHERE a_string = 'a' AND b_string IS NULL";
            rs = conn.createStatement().executeQuery(query);
            assertTrue(rs.next());
            assertEquals("a",rs.getString(1));
            assertFalse(rs.next());

            dml = "UPSERT INTO " + dataTableFullName + " VALUES(?)";
            stmt = conn.prepareStatement(dml);
            stmt.setString(1, "c");
            stmt.execute();
            conn.commit();

            query = "SELECT * FROM " + dataTableFullName + " WHERE a_string = 'c' AND b_string IS NULL";
            rs = conn.createStatement().executeQuery(query);
            assertTrue(rs.next());
            assertEquals("c",rs.getString(1));
            assertFalse(rs.next());

            dml = "UPSERT INTO " + dataTableFullName + "(a_string,col1) VALUES(?,?)";
            stmt = conn.prepareStatement(dml);
            stmt.setString(1, "a");
            stmt.setInt(2, 5);
            stmt.execute();
            conn.commit();

            query = "SELECT a_string,col1 FROM " + dataTableFullName + " WHERE a_string = 'a' AND b_string IS NULL";
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
        String ddl = "CREATE TABLE  " + dataTableFullName +
                "  (a_string varchar not null, col1 integer" +
                "  CONSTRAINT pk PRIMARY KEY (a_string)) " + tableDDLOptions;
        try {
            conn.createStatement().execute(ddl);
            conn.createStatement().execute("ALTER TABLE " + dataTableFullName + " ADD CF.col2 integer CF.IN_MEMORY=true");
            try (HBaseAdmin admin = conn.unwrap(PhoenixConnection.class).getQueryServices().getAdmin()) {
                HColumnDescriptor[] columnFamilies = admin.getTableDescriptor(Bytes.toBytes(dataTableFullName)).getColumnFamilies();
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

    private void assertIndexExists(Connection conn, boolean exists) throws SQLException {
        ResultSet rs = conn.getMetaData().getIndexInfo(null, schemaName, dataTableName, false, false);
        assertEquals(exists, rs.next());
    }

    
    @Test
    public void testDropIndexedColumnImmutableIndex() throws Exception {
        helpTestDropIndexedColumn(true);
    }
    
    @Test
    public void testDropIndexedColumnMutableIndex() throws Exception {
        helpTestDropIndexedColumn(false);
    }
    
    private String generateDDLOptions(String options) {
        StringBuilder sb = new StringBuilder();
        if (!options.isEmpty()) {
            sb.append(options);
        }
        if (!tableDDLOptions.isEmpty()) {
            if (sb.length()!=0)
                sb.append(",");
            sb.append(tableDDLOptions);
        }
        return sb.toString();
    }
    
    private void helpTestDropIndexedColumn(boolean immutable) throws Exception {
        String query;
        ResultSet rs;
        PreparedStatement stmt;

        Properties props = PropertiesUtil.deepCopy(TEST_PROPERTIES);
        Connection conn = DriverManager.getConnection(getUrl(), props);
        conn.setAutoCommit(false);

        // make sure that the tables are empty, but reachable
        conn.createStatement().execute(
          "CREATE TABLE " + dataTableFullName
              + " (k VARCHAR NOT NULL PRIMARY KEY, v1 VARCHAR, v2 VARCHAR) "
              + generateDDLOptions(immutable ? "IMMUTABLE_ROWS = true" : "")
              + (!columnEncoded ? ",IMMUTABLE_STORAGE_SCHEME=" + PTable.ImmutableStorageScheme.ONE_CELL_PER_COLUMN : ""));
        query = "SELECT * FROM " + dataTableFullName;
        rs = conn.createStatement().executeQuery(query);
        assertFalse(rs.next());

        conn.createStatement().execute(
          "CREATE INDEX " + indexTableName + " ON " + dataTableFullName + " (v1, v2)");
        conn.createStatement().execute(
            "CREATE LOCAL INDEX " + localIndexTableName + " ON " + dataTableFullName + " (v1, v2)");

        query = "SELECT * FROM " + indexTableFullName;
        rs = conn.createStatement().executeQuery(query);
        assertFalse(rs.next());

        // load some data into the table
        stmt = conn.prepareStatement("UPSERT INTO " + dataTableFullName + " VALUES(?,?,?)");
        stmt.setString(1, "a");
        stmt.setString(2, "x");
        stmt.setString(3, "1");
        stmt.execute();
        conn.commit();

        assertIndexExists(conn,true);
        conn.createStatement().execute("ALTER TABLE " + dataTableFullName + " DROP COLUMN v1");
        assertIndexExists(conn,false);

        query = "SELECT * FROM " + dataTableFullName;
        rs = conn.createStatement().executeQuery(query);
        assertTrue(rs.next());
        assertEquals("a",rs.getString(1));
        assertEquals("1",rs.getString(2));
        assertFalse(rs.next());

        // load some data into the table
        stmt = conn.prepareStatement("UPSERT INTO " + dataTableFullName + " VALUES(?,?)");
        stmt.setString(1, "a");
        stmt.setString(2, "2");
        stmt.execute();
        conn.commit();

        query = "SELECT * FROM " + dataTableFullName;
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
          "CREATE TABLE " + dataTableFullName
              + " (k VARCHAR NOT NULL PRIMARY KEY, v1 VARCHAR, v2 VARCHAR, v3 VARCHAR) " +  tableDDLOptions);
        String dataTableQuery = "SELECT * FROM " + dataTableFullName;
        rs = conn.createStatement().executeQuery(dataTableQuery);
        assertFalse(rs.next());

        conn.createStatement().execute(
          "CREATE INDEX " + indexTableName + " ON " + dataTableFullName + " (v1) include (v2, v3)");
        conn.createStatement().execute(
            "CREATE LOCAL INDEX " + localIndexTableName + " ON " + dataTableFullName + " (v1) include (v2, v3)");
        rs = conn.createStatement().executeQuery(dataTableQuery);
        assertFalse(rs.next());
        String indexTableQuery = "SELECT * FROM " + indexTableName;
        rs = conn.createStatement().executeQuery(indexTableQuery);
        assertFalse(rs.next());
        String localIndexTableQuery = "SELECT * FROM " + localIndexTableFullName;
        rs = conn.createStatement().executeQuery(localIndexTableQuery);
        assertFalse(rs.next());

        // load some data into the table
        stmt = conn.prepareStatement("UPSERT INTO " + dataTableFullName + " VALUES(?,?,?,?)");
        stmt.setString(1, "a");
        stmt.setString(2, "x");
        stmt.setString(3, "1");
        stmt.setString(4, "j");
        stmt.execute();
        conn.commit();

        assertIndexExists(conn,true);
        conn.createStatement().execute("ALTER TABLE " + dataTableFullName + " DROP COLUMN v2");
        assertIndexExists(conn,true);

        // verify data table rows
        Scan scan = new Scan();
        HTable table = (HTable) conn.unwrap(PhoenixConnection.class).getQueryServices().getTable(Bytes.toBytes(dataTableFullName));
        ResultScanner results = table.getScanner(scan);
        for (Result res : results) {
        	assertNull("Column value was not deleted",res.getValue(QueryConstants.DEFAULT_COLUMN_FAMILY_BYTES, Bytes.toBytes("V2")));
        }
        results.close();
        rs = conn.createStatement().executeQuery(dataTableQuery);
        assertTrue(rs.next());
        assertEquals("a",rs.getString(1));
        assertEquals("x",rs.getString(2));
        assertEquals("j",rs.getString(3));
        assertFalse(rs.next());
        
        // verify index table rows
        scan = new Scan();
        table = (HTable) conn.unwrap(PhoenixConnection.class).getQueryServices().getTable(Bytes.toBytes(indexTableFullName));
        results = table.getScanner(scan);
        for (Result res : results) {
        	assertNull("Column value was not deleted",res.getValue(QueryConstants.DEFAULT_COLUMN_FAMILY_BYTES, Bytes.toBytes("0:V2")));
        }
        results.close();
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
        stmt = conn.prepareStatement("UPSERT INTO " + dataTableFullName + " VALUES(?,?,?)");
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
          "CREATE TABLE " + dataTableFullName
              + " (k VARCHAR NOT NULL PRIMARY KEY, v1 VARCHAR, v2 VARCHAR) " + tableDDLOptions);
        query = "SELECT * FROM " + dataTableFullName;
        rs = conn.createStatement().executeQuery(query);
        assertFalse(rs.next());

        conn.createStatement().execute(
          "CREATE INDEX " + indexTableName + " ON " + dataTableFullName + " (v1) include (v2)");
        query = "SELECT * FROM " + indexTableFullName;
        rs = conn.createStatement().executeQuery(query);
        assertFalse(rs.next());

        // load some data into the table
        stmt = conn.prepareStatement("UPSERT INTO " + dataTableFullName + " VALUES(?,?,?)");
        stmt.setString(1, "a");
        stmt.setString(2, "x");
        stmt.setString(3, "1");
        stmt.execute();
        conn.commit();

        assertIndexExists(conn,true);
        conn.createStatement().execute("ALTER TABLE " + dataTableFullName + " ADD v3 VARCHAR, k2 DECIMAL PRIMARY KEY, k3 DECIMAL PRIMARY KEY");
        rs = conn.getMetaData().getPrimaryKeys("", schemaName, dataTableName);
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

        rs = conn.getMetaData().getPrimaryKeys("", schemaName, indexTableName);
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

        query = "SELECT * FROM " + dataTableFullName;
        rs = conn.createStatement().executeQuery(query);
        assertTrue(rs.next());
        assertEquals("a",rs.getString(1));
        assertEquals("x",rs.getString(2));
        assertEquals("1",rs.getString(3));
        assertNull(rs.getBigDecimal(4));
        assertFalse(rs.next());

        // load some data into the table
        stmt = conn.prepareStatement("UPSERT INTO " + dataTableFullName + "(K,K2,V1,V2,K3) VALUES(?,?,?,?,?)");
        stmt.setString(1, "b");
        stmt.setBigDecimal(2, BigDecimal.valueOf(2));
        stmt.setString(3, "y");
        stmt.setString(4, "2");
        stmt.setBigDecimal(5, BigDecimal.valueOf(3));
        stmt.execute();
        conn.commit();

        query = "SELECT k,k2,k3 FROM " + dataTableFullName + " WHERE v1='y'";
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
            String ddl = "CREATE TABLE " + dataTableFullName + "(\n" +
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
            		generateDDLOptions("SALT_BUCKETS=4");
            conn.createStatement().execute(ddl);

            ddl = "ALTER TABLE " + dataTableFullName + " SET IMMUTABLE_ROWS=true";
            conn.createStatement().execute(ddl);

            conn.createStatement().executeQuery("select count(*) from " + dataTableFullName).next();

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
            String ddl = "CREATE TABLE " + dataTableFullName + " (\n" +
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
                    generateDDLOptions("SALT_BUCKETS=4");
            conn.createStatement().execute(ddl);

            ddl = "ALTER TABLE " + dataTableFullName + " DROP COLUMN B.JSON";
            conn.createStatement().execute(ddl);

            conn.createStatement().executeQuery("select count(*) from " + dataTableFullName).next();
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
            String ddl = "CREATE TABLE " + dataTableFullName +
                    "  (a_string varchar not null, col1 integer" +
                    "  CONSTRAINT pk PRIMARY KEY (a_string)) " + tableDDLOptions;
            conn.createStatement().execute(ddl);

            String dml = "UPSERT INTO " + dataTableFullName + " VALUES(?)";
            PreparedStatement stmt = conn.prepareStatement(dml);
            stmt.setString(1, "b");
            stmt.execute();
            stmt.setString(1, "a");
            stmt.execute();
            conn.commit();

            String query = "SELECT * FROM " + dataTableFullName;
            ResultSet rs = conn.createStatement().executeQuery(query);
            assertTrue(rs.next());
            assertEquals("a",rs.getString(1));
            assertTrue(rs.next());
            assertEquals("b",rs.getString(1));
            assertFalse(rs.next());


            query = "SELECT * FROM " + dataTableFullName + " WHERE a_string = 'a' ";
            rs = conn.createStatement().executeQuery(query);
            assertTrue(rs.next());
            assertEquals("a",rs.getString(1));

            ddl = "ALTER TABLE " + dataTableFullName + " ADD  c1.col2 VARCHAR  , c1.col3 integer , c2.col4 integer";
            conn.createStatement().execute(ddl);

            ddl = "ALTER TABLE " + dataTableFullName + " ADD   col5 integer , c1.col2 VARCHAR";
            try {
                conn.createStatement().execute(ddl);
                fail();
            } catch (SQLException e) {
                assertEquals(SQLExceptionCode.COLUMN_EXIST_IN_DEF.getErrorCode(), e.getErrorCode());
            }

            query = "SELECT col5 FROM " + dataTableFullName;
            try {
                conn.createStatement().executeQuery(query);
                fail();
            } catch(SQLException e) {
                assertTrue(e.getMessage(), e.getMessage().contains("ERROR 504 (42703): Undefined column."));
            }

            ddl = "ALTER TABLE " + dataTableFullName + " ADD IF NOT EXISTS col5 integer , c1.col2 VARCHAR";
            conn.createStatement().execute(ddl);

            dml = "UPSERT INTO " + dataTableFullName + " VALUES(?,?,?,?,?)";
            stmt = conn.prepareStatement(dml);
            stmt.setString(1, "c");
            stmt.setInt(2, 100);
            stmt.setString(3, "d");
            stmt.setInt(4, 101);
            stmt.setInt(5, 102);
            stmt.execute();
            conn.commit();

            query = "SELECT * FROM " + dataTableFullName + " WHERE a_string = 'c' ";
            rs = conn.createStatement().executeQuery(query);
            assertTrue(rs.next());
            assertEquals("c",rs.getString(1));
            assertEquals(100,rs.getInt(2));
            assertEquals("d",rs.getString(3));
            assertEquals(101,rs.getInt(4));
            assertEquals(102,rs.getInt(5));
            assertFalse(rs.next());

            ddl = "ALTER TABLE " + dataTableFullName + " ADD  col5 integer";
            conn.createStatement().execute(ddl);

            query = "SELECT c1.* FROM " + dataTableFullName + " WHERE a_string = 'c' ";
            rs = conn.createStatement().executeQuery(query);
            assertTrue(rs.next());
            assertEquals("d",rs.getString(1));
            assertEquals(101,rs.getInt(2));
            assertFalse(rs.next());


            dml = "UPSERT INTO " + dataTableFullName + "(a_string,col1,col5) VALUES(?,?,?)";
            stmt = conn.prepareStatement(dml);
            stmt.setString(1, "e");
            stmt.setInt(2, 200);
            stmt.setInt(3, 201);
            stmt.execute();
            conn.commit();


            query = "SELECT a_string,col1,col5 FROM " + dataTableFullName + " WHERE a_string = 'e' ";
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
            String ddl = "CREATE TABLE " + dataTableFullName + " " + "  (a_string varchar not null, col1 integer, cf1.col2 integer"
                    + "  CONSTRAINT pk PRIMARY KEY (a_string)) " + tableDDLOptions;
            conn.createStatement().execute(ddl);

            ddl = "ALTER TABLE " + dataTableFullName + " DROP COLUMN col1";
            conn.createStatement().execute(ddl);

            ddl = "ALTER TABLE " + dataTableFullName + " DROP COLUMN cf1.col2";
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
                String ddl = "CREATE TABLE " + dataTableFullName + " " + "  (a_string varchar not null, col1 integer, cf1.col2 integer"
                        + "  CONSTRAINT pk PRIMARY KEY (a_string)) " +  tableDDLOptions;
                stmt = conn.prepareStatement(ddl);
                stmt.execute();
            } finally {
                closeStatement(stmt);
            }
            try {
                stmt = conn.prepareStatement("ALTER TABLE " + dataTableFullName + " ADD b_string VARCHAR NOT NULL");
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
        assertEquals(expectedValue, pconn.getTable(new PTableKey(pconn.getTenantId(), fullTableName)).isWALDisabled());
    }

    @Test
    public void testDisableWAL1() throws Exception {
        Properties props = PropertiesUtil.deepCopy(TEST_PROPERTIES);
        Connection conn = DriverManager.getConnection(getUrl(), props);

        try {
            conn.createStatement()
                    .execute(
                            "CREATE TABLE " + dataTableFullName
                                    + "  (a_string varchar not null, col1 integer, cf1.col2 integer, col3 integer , cf2.col4 integer "
                                    + "  CONSTRAINT pk PRIMARY KEY (a_string)) "
                                    + generateDDLOptions("immutable_rows=true, disable_wal=true"
                                    + (!columnEncoded ? ",IMMUTABLE_STORAGE_SCHEME=" + PTable.ImmutableStorageScheme.ONE_CELL_PER_COLUMN : "")));

            Connection conn2 = DriverManager.getConnection(getUrl(), props);
            String query = "SELECT * FROM " + dataTableFullName;
            ResultSet rs = conn2.createStatement().executeQuery(query);
            assertFalse(rs.next());
            asssertIsWALDisabled(conn2,dataTableFullName, true);
            conn2.close();
            asssertIsWALDisabled(conn,dataTableFullName, true);

            conn.createStatement().execute("CREATE INDEX " + indexTableName + " ON " + dataTableFullName + " (col1) include (cf1.col2) SALT_BUCKETS=4");
            conn2 = DriverManager.getConnection(getUrl(), props);
            query = "SELECT * FROM " + indexTableFullName;
            rs = conn2.createStatement().executeQuery(query);
            asssertIsWALDisabled(conn2,indexTableFullName, false);
            assertFalse(rs.next());
            conn2.close();
            asssertIsWALDisabled(conn,indexTableFullName, false);
        } finally {
            conn.close();
        }
    }
    
    @Test
    public void testDisableWAL2() throws Exception {
        Properties props = PropertiesUtil.deepCopy(TEST_PROPERTIES);
        Connection conn = DriverManager.getConnection(getUrl(), props);
        try {
            conn.createStatement()
                    .execute(
                            "CREATE TABLE " + dataTableFullName
                                    + "  (a_string varchar not null, col1 integer, cf1.col2 integer, col3 integer , cf2.col4 integer "
                                    + "  CONSTRAINT pk PRIMARY KEY (a_string))" 
                                    + generateDDLOptions("immutable_rows=true"
                                    + (!columnEncoded ? ",IMMUTABLE_STORAGE_SCHEME=" + PTable.ImmutableStorageScheme.ONE_CELL_PER_COLUMN : "")));

            Connection conn2 = DriverManager.getConnection(getUrl(), props);
            String query = "SELECT * FROM " + dataTableFullName;
            ResultSet rs = conn2.createStatement().executeQuery(query);
            assertFalse(rs.next());
            asssertIsWALDisabled(conn,dataTableFullName, false);
            conn2.close();
            asssertIsWALDisabled(conn,dataTableFullName, false);

            conn.createStatement().execute("CREATE INDEX " + indexTableName + " ON " + dataTableFullName + " (col1) include (cf1.col2) SALT_BUCKETS=4");
            conn2 = DriverManager.getConnection(getUrl(), props);
            query = "SELECT * FROM " + indexTableFullName;
            rs = conn2.createStatement().executeQuery(query);
            asssertIsWALDisabled(conn2,indexTableFullName, false);
            assertFalse(rs.next());
            conn2.close();
            asssertIsWALDisabled(conn,indexTableFullName, false);
        } finally {
            conn.close();
        }
    }
    
    @Test
    public void testDisableWAL3() throws Exception {
        Properties props = PropertiesUtil.deepCopy(TEST_PROPERTIES);
        Connection conn = DriverManager.getConnection(getUrl(), props);

        try {
            conn.createStatement()
                    .execute(
                            "CREATE TABLE " + dataTableFullName
                                    + "  (a_string varchar not null, col1 integer, cf1.col2 integer, col3 integer , cf2.col4 integer "
                                    + "  CONSTRAINT pk PRIMARY KEY (a_string)) " + tableDDLOptions);

            Connection conn2 = DriverManager.getConnection(getUrl(), props);
            String query = "SELECT * FROM " + dataTableFullName;
            ResultSet rs = conn2.createStatement().executeQuery(query);
            assertFalse(rs.next());
            asssertIsWALDisabled(conn2,dataTableFullName, false);
            conn2.close();
            asssertIsWALDisabled(conn,dataTableFullName, false);

            conn.createStatement().execute(
                    "CREATE INDEX " + indexTableName + " ON " + dataTableFullName
                            + " (col1) include (cf1.col2) SALT_BUCKETS=4");
            conn2 = DriverManager.getConnection(getUrl(), props);
            query = "SELECT * FROM " + indexTableFullName;
            rs = conn2.createStatement().executeQuery(query);
            asssertIsWALDisabled(conn2,indexTableFullName, false);
            assertFalse(rs.next());
            conn2.close();
            asssertIsWALDisabled(conn,indexTableFullName, false);

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
                            "CREATE TABLE " + dataTableFullName
                                    + "  (a_string varchar not null, col1 integer, cf1.col2 integer, col3 integer , cf2.col4 integer "
                                    + "  CONSTRAINT pk PRIMARY KEY (a_string)) " 
                                    + generateDDLOptions("immutable_rows=true , SALT_BUCKETS=3 "
                                    + (!columnEncoded ? ",IMMUTABLE_STORAGE_SCHEME=" + PTable.ImmutableStorageScheme.ONE_CELL_PER_COLUMN : "")));

            String query = "SELECT * FROM " + dataTableFullName;
            ResultSet rs = conn.createStatement().executeQuery(query);
            assertFalse(rs.next());

            conn.createStatement().execute("CREATE INDEX " + indexTableName + " ON " + dataTableFullName + " (col1) include (cf1.col2) SALT_BUCKETS=4");
            query = "SELECT * FROM " + indexTableFullName;
            rs = conn.createStatement().executeQuery(query);
            assertFalse(rs.next());

            String dml = "UPSERT INTO " + dataTableFullName + " VALUES(?,?,?,?,?)";
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

            query = "SELECT * FROM " + dataTableFullName + " order by col1";
            rs = conn.createStatement().executeQuery(query);
            assertTrue(rs.next());
            assertEquals("b", rs.getString(1));
            assertTrue(rs.next());
            assertEquals("a", rs.getString(1));
            assertFalse(rs.next());

            String ddl = "ALTER TABLE " + dataTableFullName + " DROP COLUMN IF EXISTS col2,col3";
            conn.createStatement().execute(ddl);

            ddl = "ALTER TABLE " + dataTableFullName + " DROP COLUMN a_string,col1";
            try{
                conn.createStatement().execute(ddl);
                fail();
            } catch (SQLException e) {
                assertEquals(SQLExceptionCode.CANNOT_DROP_PK.getErrorCode(), e.getErrorCode());
            }

            ddl = "ALTER TABLE " + dataTableFullName + " DROP COLUMN col4,col5";
            try {
                conn.createStatement().execute(ddl);
                fail();
            } catch (SQLException e) {
                assertEquals(SQLExceptionCode.COLUMN_NOT_FOUND.getErrorCode(), e.getErrorCode());
                assertTrue(e.getMessage(), e.getMessage().contains("ERROR 504 (42703): Undefined column. columnName=COL5"));
            }

            ddl = "ALTER TABLE " + dataTableFullName + " DROP COLUMN IF EXISTS col1";
            conn.createStatement().execute(ddl);

            query = "SELECT * FROM " + indexTableFullName;
            try {
                rs = conn.createStatement().executeQuery(query);
                fail();
            } catch (TableNotFoundException e) {}

            query = "select col4 FROM " + dataTableFullName;
            rs = conn.createStatement().executeQuery(query);
            assertTrue(rs.next());
            assertTrue(rs.next());

            query = "select col2,col3 FROM " + dataTableFullName;
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
        conn1.createStatement().execute("create table " + dataTableFullName + "(id VARCHAR PRIMARY KEY, field1 BIGINT) " + tableDDLOptions);
        PreparedStatement stmtInsert1 = conn1.prepareStatement("upsert into " + dataTableFullName + " (id, field1) values ( ?, ?)");
        stmtInsert1.setString(1, "key1");
        stmtInsert1.setLong(2, 1L);
        stmtInsert1.execute();
        conn1.commit();
        stmtInsert1.close();
        conn1.close();

        // Do the alter through a separate client.
        conn3.createStatement().execute("alter table " + dataTableFullName + " add field2 BIGINT");

        //Connection conn1 = DriverManager.getConnection(getUrl(), props);
        PreparedStatement pstmt2 = conn1.prepareStatement("upsert into " + dataTableFullName + " (id, field1, field2) values ( ?, ?, ?)");
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
        String ddl = "CREATE TABLE " + dataTableFullName + " (\n"
                +"ID1 VARCHAR(15) NOT NULL,\n"
                +"ID2 VARCHAR(15) NOT NULL,\n"
                +"CREATED_DATE DATE,\n"
                +"CREATION_TIME BIGINT,\n"
                +"LAST_USED DATE,\n"
                +"CONSTRAINT PK PRIMARY KEY (ID1, ID2)) "  + tableDDLOptions;
        Connection conn1 = DriverManager.getConnection(getUrl(), props);
        conn1.createStatement().execute(ddl);
        ddl = "ALTER TABLE " + dataTableFullName + " ADD STRING VARCHAR, STRING_DATA_TYPES VARCHAR";
        conn1.createStatement().execute(ddl);
        ddl = "ALTER TABLE " + dataTableFullName + " DROP COLUMN STRING, STRING_DATA_TYPES";
        conn1.createStatement().execute(ddl);
        ddl = "ALTER TABLE " + dataTableFullName + " ADD STRING_ARRAY1 VARCHAR[]";
        conn1.createStatement().execute(ddl);
        conn1.close();
    }

    @Test
    public void testAddMultipleColumns() throws Exception {
        Properties props = PropertiesUtil.deepCopy(TEST_PROPERTIES);
        String ddl = "CREATE TABLE " + dataTableFullName + " (\n"
                +"ID VARCHAR(15) PRIMARY KEY,\n"
                +"COL1 BIGINT) " + tableDDLOptions;
        Connection conn1 = DriverManager.getConnection(getUrl(), props);
        conn1.createStatement().execute(ddl);
        conn1.createStatement().execute("CREATE INDEX " + indexTableName + " ON " + dataTableFullName + "(COL1)");
        
        ddl = "ALTER TABLE " + dataTableFullName + " ADD COL2 VARCHAR PRIMARY KEY, COL3 VARCHAR PRIMARY KEY";
        conn1.createStatement().execute(ddl);
        ResultSet rs = conn1.getMetaData().getColumns("", "", dataTableFullName, null);
        assertTrue(rs.next());
        assertEquals("ID",rs.getString(4));
        assertTrue(rs.next());
        assertEquals("COL1",rs.getString(4));
        assertTrue(rs.next());
        assertEquals("COL2",rs.getString(4));
        assertTrue(rs.next());
        assertEquals("COL3",rs.getString(4));
        assertFalse(rs.next());
        
        rs = conn1.createStatement().executeQuery("SELECT COLUMN_COUNT FROM \"SYSTEM\".\"CATALOG\"\n"
                + "WHERE TENANT_ID IS NULL AND\n"
                + "(TABLE_SCHEM, TABLE_NAME) = ('" + schemaName + "','"+ dataTableName + "') AND\n"
                + "COLUMN_FAMILY IS NULL AND COLUMN_NAME IS NULL");
        assertTrue(rs.next());
        assertEquals(4,rs.getInt(1));
        assertFalse(rs.next());

        rs = conn1.createStatement().executeQuery("SELECT COLUMN_COUNT FROM \"SYSTEM\".\"CATALOG\"\n"
                + "WHERE TENANT_ID IS NULL AND\n"
                + "(TABLE_SCHEM, TABLE_NAME) = ('" + schemaName + "','"+ indexTableName + "') AND\n"
                + "COLUMN_FAMILY IS NULL AND COLUMN_NAME IS NULL");
        assertTrue(rs.next());
        assertEquals(4,rs.getInt(1));
        assertFalse(rs.next());
        
        conn1.createStatement().execute("UPSERT INTO " + dataTableFullName + " VALUES ('a',2,'a','b')");
        conn1.createStatement().execute("UPSERT INTO " + dataTableFullName + " VALUES ('b',3,'b','c')");
        conn1.createStatement().execute("UPSERT INTO " + dataTableFullName + " VALUES ('c',4,'c','c')");
        conn1.commit();
        
        rs = conn1.createStatement().executeQuery("SELECT ID,COL1 FROM " + dataTableFullName + " WHERE COL1=3");
        assertTrue(rs.next());
        assertEquals("b",rs.getString(1));
        assertEquals(3,rs.getLong(2));
        assertFalse(rs.next());
        
        conn1.close();
    }


    @Test
    public void testDropMultipleColumns() throws Exception {
        Properties props = PropertiesUtil.deepCopy(TEST_PROPERTIES);
        String ddl = "CREATE TABLE " + dataTableFullName + " (\n"
                + "ID VARCHAR(15) PRIMARY KEY,\n"
                + "COL1 BIGINT,"
                + "COL2 BIGINT,"
                + "COL3 BIGINT,"
                + "COL4 BIGINT) " + tableDDLOptions;
        Connection conn1 = DriverManager.getConnection(getUrl(), props);
        conn1.createStatement().execute(ddl);
        conn1.createStatement().execute("CREATE INDEX " + indexTableName + " ON " + dataTableFullName + "(COL1) INCLUDE (COL2,COL3,COL4)");
        
        ddl = "ALTER TABLE " + dataTableFullName + " DROP COLUMN COL2, COL3";
        conn1.createStatement().execute(ddl);
        ResultSet rs = conn1.getMetaData().getColumns("", "", dataTableFullName, null);
        assertTrue(rs.next());
        assertEquals("ID",rs.getString(4));
        assertTrue(rs.next());
        assertEquals("COL1",rs.getString(4));
        assertTrue(rs.next());
        assertEquals("COL4",rs.getString(4));
        assertFalse(rs.next());
        
        rs = conn1.createStatement().executeQuery("SELECT COLUMN_COUNT FROM \"SYSTEM\".\"CATALOG\"\n"
                + "WHERE TENANT_ID IS NULL AND\n"
                + "(TABLE_SCHEM, TABLE_NAME) = ('" + schemaName + "','"+ dataTableName + "') AND\n"
                + "COLUMN_FAMILY IS NULL AND COLUMN_NAME IS NULL");
        assertTrue(rs.next());
        assertEquals(3,rs.getInt(1));
        assertFalse(rs.next());

        rs = conn1.createStatement().executeQuery("SELECT COLUMN_COUNT FROM \"SYSTEM\".\"CATALOG\"\n"
                + "WHERE TENANT_ID IS NULL AND\n"
                + "(TABLE_SCHEM, TABLE_NAME) = ('" + schemaName + "','"+ indexTableName + "') AND\n"
                + "COLUMN_FAMILY IS NULL AND COLUMN_NAME IS NULL");
        assertTrue(rs.next());
        assertEquals(3,rs.getInt(1));
        assertFalse(rs.next());
        
        conn1.createStatement().execute("UPSERT INTO " + dataTableFullName + " VALUES ('a',2, 20)");
        conn1.createStatement().execute("UPSERT INTO " + dataTableFullName + " VALUES ('b',3, 30)");
        conn1.createStatement().execute("UPSERT INTO " + dataTableFullName + " VALUES ('c',4, 40)");
        conn1.commit();
        
        rs = conn1.createStatement().executeQuery("SELECT ID,COL1,COL4 FROM " + dataTableFullName + " WHERE COL1=3");
        assertTrue(rs.next());
        assertEquals("b",rs.getString(1));
        assertEquals(3,rs.getLong(2));
        assertEquals(30,rs.getLong(3));
        assertFalse(rs.next());
        
        conn1.close();
    }

    @Test
    public void testSetHColumnProperties() throws Exception {
        Properties props = PropertiesUtil.deepCopy(TEST_PROPERTIES);
        String ddl = "CREATE TABLE " + dataTableFullName + " (\n"
                +"ID1 VARCHAR(15) NOT NULL,\n"
                +"ID2 VARCHAR(15) NOT NULL,\n"
                +"CREATED_DATE DATE,\n"
                +"CREATION_TIME BIGINT,\n"
                +"LAST_USED DATE,\n"
                +"CONSTRAINT PK PRIMARY KEY (ID1, ID2)) " + generateDDLOptions("SALT_BUCKETS = 8");
        Connection conn1 = DriverManager.getConnection(getUrl(), props);
        conn1.createStatement().execute(ddl);
        ddl = "ALTER TABLE " + dataTableFullName + " SET REPLICATION_SCOPE=1";
        conn1.createStatement().execute(ddl);
        try (HBaseAdmin admin = conn1.unwrap(PhoenixConnection.class).getQueryServices().getAdmin()) {
            HColumnDescriptor[] columnFamilies = admin.getTableDescriptor(Bytes.toBytes(dataTableFullName))
                    .getColumnFamilies();
            assertEquals(1, columnFamilies.length);
            assertEquals("0", columnFamilies[0].getNameAsString());
            assertEquals(1, columnFamilies[0].getScope());
        }
    }

    @Test
    public void testSetHTableProperties() throws Exception {
        Properties props = PropertiesUtil.deepCopy(TEST_PROPERTIES);
        String ddl = "CREATE TABLE " + dataTableFullName + " (\n"
                +"ID1 VARCHAR(15) NOT NULL,\n"
                +"ID2 VARCHAR(15) NOT NULL,\n"
                +"CREATED_DATE DATE,\n"
                +"CREATION_TIME BIGINT,\n"
                +"LAST_USED DATE,\n"
                +"CONSTRAINT PK PRIMARY KEY (ID1, ID2)) " + generateDDLOptions("SALT_BUCKETS = 8");
        Connection conn1 = DriverManager.getConnection(getUrl(), props);
        conn1.createStatement().execute(ddl);
        ddl = "ALTER TABLE " + dataTableFullName + " SET COMPACTION_ENABLED=FALSE";
        conn1.createStatement().execute(ddl);
        try (HBaseAdmin admin = conn1.unwrap(PhoenixConnection.class).getQueryServices().getAdmin()) {
            HTableDescriptor tableDesc = admin.getTableDescriptor(Bytes.toBytes(dataTableFullName));
            assertEquals(1, tableDesc.getColumnFamilies().length);
            assertEquals("0", tableDesc.getColumnFamilies()[0].getNameAsString());
            assertEquals(Boolean.toString(false), tableDesc.getValue(HTableDescriptor.COMPACTION_ENABLED));
        }
    }

    @Test
    public void testSetHTableAndHColumnProperties() throws Exception {
        Properties props = PropertiesUtil.deepCopy(TEST_PROPERTIES);
        String ddl = "CREATE TABLE " + dataTableFullName + " (\n"
                +"ID1 VARCHAR(15) NOT NULL,\n"
                +"ID2 VARCHAR(15) NOT NULL,\n"
                +"CREATED_DATE DATE,\n"
                +"CREATION_TIME BIGINT,\n"
                +"LAST_USED DATE,\n"
                +"CONSTRAINT PK PRIMARY KEY (ID1, ID2)) " + generateDDLOptions("SALT_BUCKETS = 8");
        Connection conn1 = DriverManager.getConnection(getUrl(), props);
        conn1.createStatement().execute(ddl);
        ddl = "ALTER TABLE " + dataTableFullName + " SET COMPACTION_ENABLED = FALSE, REPLICATION_SCOPE = 1";
        conn1.createStatement().execute(ddl);
        try (HBaseAdmin admin = conn1.unwrap(PhoenixConnection.class).getQueryServices().getAdmin()) {
            HTableDescriptor tableDesc = admin.getTableDescriptor(Bytes.toBytes(dataTableFullName));
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
        String ddl = "CREATE TABLE " + dataTableFullName + " (\n"
                +"ID1 VARCHAR(15) NOT NULL,\n"
                +"ID2 VARCHAR(15) NOT NULL,\n"
                +"CREATED_DATE DATE,\n"
                +"CF1.CREATION_TIME BIGINT,\n"
                +"CF2.LAST_USED DATE,\n"
                +"CONSTRAINT PK PRIMARY KEY (ID1, ID2)) " + generateDDLOptions("IMMUTABLE_ROWS=true"
                + (!columnEncoded ? ",IMMUTABLE_STORAGE_SCHEME=" + PTable.ImmutableStorageScheme.ONE_CELL_PER_COLUMN : ""));
        Connection conn = DriverManager.getConnection(getUrl(), props);
        conn.createStatement().execute(ddl);
        assertImmutableRows(conn, dataTableFullName, true);
        ddl = "ALTER TABLE " + dataTableFullName + " SET COMPACTION_ENABLED = FALSE, VERSIONS = 10";
        conn.createStatement().execute(ddl);
        ddl = "ALTER TABLE " + dataTableFullName + " SET COMPACTION_ENABLED = FALSE, CF1.MIN_VERSIONS = 1, CF2.MIN_VERSIONS = 3, MIN_VERSIONS = 8, CF1.KEEP_DELETED_CELLS = true, KEEP_DELETED_CELLS = false";
        conn.createStatement().execute(ddl);

        try (HBaseAdmin admin = conn.unwrap(PhoenixConnection.class).getQueryServices().getAdmin()) {
            HTableDescriptor tableDesc = admin.getTableDescriptor(Bytes.toBytes(dataTableFullName));
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
        String ddl = "CREATE TABLE " + dataTableFullName + " (\n"
                +"ID1 VARCHAR(15) NOT NULL,\n"
                +"ID2 VARCHAR(15) NOT NULL,\n"
                +"CREATED_DATE DATE,\n"
                +"CREATION_TIME BIGINT,\n"
                +"LAST_USED DATE,\n"
                +"CONSTRAINT PK PRIMARY KEY (ID1, ID2)) " + generateDDLOptions("SALT_BUCKETS = 8");
        Connection conn1 = DriverManager.getConnection(getUrl(), props);
        conn1.createStatement().execute(ddl);
        ddl = "ALTER TABLE " + dataTableFullName + " SET CF.COMPACTION_ENABLED = FALSE";
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
        String ddl = "CREATE TABLE " + dataTableFullName + " (\n"
                +"ID1 VARCHAR(15) NOT NULL,\n"
                +"ID2 VARCHAR(15) NOT NULL,\n"
                +"CREATED_DATE DATE,\n"
                +"CREATION_TIME BIGINT,\n"
                +"LAST_USED DATE,\n"
                +"CONSTRAINT PK PRIMARY KEY (ID1, ID2)) " + generateDDLOptions("SALT_BUCKETS = 8");
        Connection conn1 = DriverManager.getConnection(getUrl(), props);
        conn1.createStatement().execute(ddl);
        ddl = "ALTER TABLE " + dataTableFullName + " SET CF.DISABLE_WAL = TRUE";
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
        String ddl = "CREATE TABLE " + dataTableFullName + " (\n"
                +"ID1 VARCHAR(15) NOT NULL,\n"
                +"ID2 VARCHAR(15) NOT NULL,\n"
                +"CREATED_DATE DATE,\n"
                +"CREATION_TIME BIGINT,\n"
                +"CF.LAST_USED DATE,\n"
                +"CONSTRAINT PK PRIMARY KEY (ID1, ID2)) " + generateDDLOptions("SALT_BUCKETS = 8");
        Connection conn1 = DriverManager.getConnection(getUrl(), props);
        conn1.createStatement().execute(ddl);
        ddl = "ALTER TABLE " + dataTableFullName + " SET CF.TTL = 86400";
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
        String ddl = "CREATE TABLE " + dataTableFullName + " (\n"
                +"ID1 VARCHAR(15) NOT NULL,\n"
                +"ID2 VARCHAR(15) NOT NULL,\n"
                +"CREATED_DATE DATE,\n"
                +"CREATION_TIME BIGINT,\n"
                +"LAST_USED DATE,\n"
                +"CONSTRAINT PK PRIMARY KEY (ID1, ID2)) " + generateDDLOptions("SALT_BUCKETS = 8");
        Connection conn1 = DriverManager.getConnection(getUrl(), props);
        conn1.createStatement().execute(ddl);
        ddl = "ALTER TABLE " + dataTableFullName + " SET CF.REPLICATION_SCOPE = 1";
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
        String ddl = "CREATE TABLE " + dataTableFullName + " (\n"
                +"ID1 VARCHAR(15) NOT NULL,\n"
                +"ID2 VARCHAR(15) NOT NULL,\n"
                +"CREATED_DATE DATE,\n"
                +"CREATION_TIME BIGINT,\n"
                +"LAST_USED DATE,\n"
                +"CONSTRAINT PK PRIMARY KEY (ID1, ID2)) " + generateDDLOptions(" SALT_BUCKETS = 8");
        Connection conn1 = DriverManager.getConnection(getUrl(), props);
        conn1.createStatement().execute(ddl);
        ddl = "ALTER TABLE " + dataTableFullName + " SET DEFAULT_COLUMN_FAMILY = 'A'";
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
        String ddl = "CREATE TABLE " + dataTableFullName + " (\n"
                +"ID1 VARCHAR(15) NOT NULL,\n"
                +"ID2 VARCHAR(15) NOT NULL,\n"
                +"CREATED_DATE DATE,\n"
                +"CREATION_TIME BIGINT,\n"
                +"LAST_USED DATE,\n"
                +"CONSTRAINT PK PRIMARY KEY (ID1, ID2)) " + generateDDLOptions("SALT_BUCKETS = 8");
        Connection conn1 = DriverManager.getConnection(getUrl(), props);
        conn1.createStatement().execute(ddl);
        ddl = "CREATE VIEW " + viewName + "  AS SELECT * FROM " + dataTableFullName + " WHERE CREATION_TIME = 1";
        conn1.createStatement().execute(ddl);
        ddl = "ALTER VIEW " + viewName + " SET REPLICATION_SCOPE = 1";
        try {
            conn1.createStatement().execute(ddl);
            fail();
        } catch (SQLException e) {
            assertEquals(SQLExceptionCode.VIEW_WITH_PROPERTIES.getErrorCode(), e.getErrorCode());
        }
        ddl = "ALTER VIEW " + viewName + " SET COMPACTION_ENABLED = FALSE";
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
        String ddl = "CREATE TABLE " + dataTableFullName + " (\n"
                +"ID1 VARCHAR(15) NOT NULL,\n"
                +"ID2 VARCHAR(15) NOT NULL,\n"
                +"CREATED_DATE DATE,\n"
                +"CREATION_TIME BIGINT,\n"
                +"LAST_USED DATE,\n"
                +"CONSTRAINT PK PRIMARY KEY (ID1, ID2)) " + generateDDLOptions("SALT_BUCKETS = 8");
        Connection conn1 = DriverManager.getConnection(getUrl(), props);
        conn1.createStatement().execute(ddl);
        String viewFullName = SchemaUtil.getTableName(schemaName, generateUniqueName());
        ddl = "CREATE VIEW " + viewFullName + " AS SELECT * FROM " + dataTableFullName + " WHERE CREATION_TIME = 1";
        conn1.createStatement().execute(ddl);
        ddl = "ALTER VIEW " + viewFullName + " SET UPDATE_CACHE_FREQUENCY = 10";
        conn1.createStatement().execute(ddl);
        conn1.createStatement().execute("SELECT * FROM " + viewFullName);
        PhoenixConnection pconn = conn1.unwrap(PhoenixConnection.class);
        assertEquals(10, pconn.getTable(new PTableKey(pconn.getTenantId(), viewFullName)).getUpdateCacheFrequency());
        ddl = "ALTER VIEW " + viewFullName + " SET UPDATE_CACHE_FREQUENCY = 20";
        conn1.createStatement().execute(ddl);
        conn1.createStatement().execute("SELECT * FROM " + viewFullName);
        pconn = conn1.unwrap(PhoenixConnection.class);
        assertEquals(20, pconn.getTable(new PTableKey(pconn.getTenantId(), viewFullName)).getUpdateCacheFrequency());
        assertImmutableRows(conn1, viewFullName, false);
        ddl = "ALTER VIEW " + viewFullName + " SET DISABLE_WAL = TRUE";
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
        String ddl = "CREATE TABLE  " + dataTableFullName + " (\n"
                +"ID1 VARCHAR(15) NOT NULL,\n"
                +"ID2 VARCHAR(15) NOT NULL,\n"
                +"CREATED_DATE DATE,\n"
                +"CREATION_TIME BIGINT,\n"
                +"CF.LAST_USED DATE,\n"
                +"CONSTRAINT PK PRIMARY KEY (ID1, ID2)) " + generateDDLOptions("IMMUTABLE_ROWS=true, DEFAULT_COLUMN_FAMILY = 'XYZ'"
                + (!columnEncoded ? ",IMMUTABLE_STORAGE_SCHEME=" + PTable.ImmutableStorageScheme.ONE_CELL_PER_COLUMN : ""));
        Connection conn = DriverManager.getConnection(getUrl(), props);
        conn.createStatement().execute(ddl);
        assertImmutableRows(conn, dataTableFullName, true);
        ddl = "ALTER TABLE  " + dataTableFullName
                + " SET COMPACTION_ENABLED = FALSE, CF.REPLICATION_SCOPE=1, IMMUTABLE_ROWS = TRUE, TTL=1000";
        conn.createStatement().execute(ddl);
        assertImmutableRows(conn, dataTableFullName, true);
        try (HBaseAdmin admin = conn.unwrap(PhoenixConnection.class).getQueryServices().getAdmin()) {
            HTableDescriptor tableDesc = admin.getTableDescriptor(Bytes.toBytes(dataTableFullName));
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

    private static void assertImmutableRows(Connection conn, String fullTableName, boolean expectedValue) throws SQLException {
        PhoenixConnection pconn = conn.unwrap(PhoenixConnection.class);
        assertEquals(expectedValue, pconn.getTable(new PTableKey(pconn.getTenantId(), fullTableName)).isImmutableRows());
    }

    @Test
    public void testSetPropertyAndAddColumnForExistingColumnFamily() throws Exception {
        Properties props = PropertiesUtil.deepCopy(TEST_PROPERTIES);
        Connection conn = DriverManager.getConnection(getUrl(), props);
        String ddl = "CREATE TABLE " + dataTableFullName
                +
                "  (a_string varchar not null, col1 integer, CF.col2 integer" +
                "  CONSTRAINT pk PRIMARY KEY (a_string)) " + tableDDLOptions;
        try {
            conn.createStatement().execute(ddl);
            conn.createStatement().execute(
                    "ALTER TABLE " + dataTableFullName + " ADD CF.col3 integer CF.IN_MEMORY=true");
            try (HBaseAdmin admin = conn.unwrap(PhoenixConnection.class).getQueryServices().getAdmin()) {
                HColumnDescriptor[] columnFamilies = admin.getTableDescriptor(Bytes.toBytes(dataTableFullName))
                        .getColumnFamilies();
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
        String ddl = "CREATE TABLE " + dataTableFullName + " "
                +
                "  (a_string varchar not null, col1 integer, CF1.col2 integer" +
                "  CONSTRAINT pk PRIMARY KEY (a_string)) " + tableDDLOptions;
        try {
            conn.createStatement().execute(ddl);
            conn.createStatement()
                    .execute(
                            "ALTER TABLE "
                                    + dataTableFullName
                                    + " ADD col4 integer, CF1.col5 integer, CF2.col6 integer IN_MEMORY=true, CF1.REPLICATION_SCOPE=1, CF2.IN_MEMORY=false ");
            try (HBaseAdmin admin = conn.unwrap(PhoenixConnection.class).getQueryServices().getAdmin()) {
                HColumnDescriptor[] columnFamilies = admin.getTableDescriptor(Bytes.toBytes(dataTableFullName))
                        .getColumnFamilies();
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
        String ddl = "CREATE TABLE " + dataTableFullName + " "
                +
                "  (a_string varchar not null, col1 integer, CF1.col2 integer" +
                "  CONSTRAINT pk PRIMARY KEY (a_string)) " + generateDDLOptions("DEFAULT_COLUMN_FAMILY = 'XYZ'");
        try {
            conn.createStatement().execute(ddl);
            conn.createStatement()
                    .execute(
                            "ALTER TABLE "
                                    + dataTableFullName
                                    + " ADD col4 integer, CF1.col5 integer, CF2.col6 integer IN_MEMORY=true, CF1.REPLICATION_SCOPE=1, CF2.IN_MEMORY=false, XYZ.REPLICATION_SCOPE=1 ");
            try (HBaseAdmin admin = conn.unwrap(PhoenixConnection.class).getQueryServices().getAdmin()) {
                HColumnDescriptor[] columnFamilies = admin.getTableDescriptor(Bytes.toBytes(dataTableFullName))
                        .getColumnFamilies();
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
        String ddl = "CREATE TABLE " + dataTableFullName + " "
                +
    			"  (a_string varchar not null, col1 integer, CF1.col2 integer" +
    			"  CONSTRAINT pk PRIMARY KEY (a_string)) "+ generateDDLOptions("DEFAULT_COLUMN_FAMILY = 'XYZ'");
    	try {
    		conn.createStatement().execute(ddl);
    		try {
                conn.createStatement().execute(
                        "ALTER TABLE " + dataTableFullName
                                + " ADD col4 integer CF1.REPLICATION_SCOPE=1, XYZ.IN_MEMORY=true ");
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
        String ddl = "CREATE TABLE " + dataTableFullName
                +
                "  (a_string varchar not null, col1 integer, CF1.col2 integer, CF2.col3 integer" +
                "  CONSTRAINT pk PRIMARY KEY (a_string)) " + generateDDLOptions("DEFAULT_COLUMN_FAMILY = 'XYZ' ");
        try {
            conn.createStatement().execute(ddl);
            conn.createStatement()
                    .execute(
                            "ALTER TABLE "
                                    + dataTableFullName
                                    + " ADD col4 integer, CF1.col5 integer, CF2.col6 integer, CF3.col7 integer CF1.REPLICATION_SCOPE=1, CF1.IN_MEMORY=false, IN_MEMORY=true ");
            try (HBaseAdmin admin = conn.unwrap(PhoenixConnection.class).getQueryServices().getAdmin()) {
                HColumnDescriptor[] columnFamilies = admin.getTableDescriptor(Bytes.toBytes(dataTableFullName))
                        .getColumnFamilies();
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
        String ddl = "CREATE TABLE " + dataTableFullName
                +
    			"  (a_string varchar not null, col1 integer, CF1.col2 integer" +
    			"  CONSTRAINT pk PRIMARY KEY (a_string)) " + generateDDLOptions("DEFAULT_COLUMN_FAMILY = 'XYZ'");
    	try {
            conn.createStatement().execute(ddl);
            conn.createStatement().execute(
                    "ALTER TABLE " + dataTableFullName + " ADD col4 integer XYZ.REPLICATION_SCOPE=1 ");
            conn.createStatement()
                    .execute("ALTER TABLE " + dataTableFullName + " ADD XYZ.col5 integer IN_MEMORY=true ");
            try (HBaseAdmin admin = conn.unwrap(PhoenixConnection.class).getQueryServices().getAdmin()) {
                HColumnDescriptor[] columnFamilies = admin.getTableDescriptor(Bytes.toBytes(dataTableFullName))
                        .getColumnFamilies();
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
        String ddl = "CREATE TABLE " + dataTableFullName +
    			"  (a_string varchar not null, col1 integer" +
    			"  CONSTRAINT pk PRIMARY KEY (a_string)) " + tableDDLOptions;
    	try {
    		conn.createStatement().execute(ddl);
            conn.createStatement().execute("ALTER TABLE " + dataTableFullName + " ADD col2 integer IN_MEMORY=true");
    		try (HBaseAdmin admin = conn.unwrap(PhoenixConnection.class).getQueryServices().getAdmin()) {
                HColumnDescriptor[] columnFamilies = admin.getTableDescriptor(Bytes.toBytes(dataTableFullName))
                        .getColumnFamilies();
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
                            "CREATE TABLE "
                                    + dataTableFullName
    						+ "  (a_string varchar not null, col1 integer, cf1.col2 integer, col3 integer , cf2.col4 integer "
    						+ "  CONSTRAINT pk PRIMARY KEY (a_string)) " + generateDDLOptions("immutable_rows=true , SALT_BUCKETS=3 "
    						+ (!columnEncoded ? ",IMMUTABLE_STORAGE_SCHEME=" + PTable.ImmutableStorageScheme.ONE_CELL_PER_COLUMN : ""))); 

            String ddl = "Alter table " + dataTableFullName + " add cf3.col5 integer, cf4.col6 integer in_memory=true";
    		conn.createStatement().execute(ddl);

    		try (HBaseAdmin admin = conn.unwrap(PhoenixConnection.class).getQueryServices().getAdmin()) {
                HTableDescriptor tableDesc = admin.getTableDescriptor(Bytes.toBytes(dataTableFullName));
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
                            "CREATE TABLE "
                                    + dataTableFullName
    						+ "  (a_string varchar not null, col1 integer, cf1.col2 integer, col3 integer , cf2.col4 integer "
    						+ "  CONSTRAINT pk PRIMARY KEY (a_string)) " + generateDDLOptions("immutable_rows=true , SALT_BUCKETS=3 "
    						+ (!columnEncoded ? ",IMMUTABLE_STORAGE_SCHEME=" + PTable.ImmutableStorageScheme.ONE_CELL_PER_COLUMN : "")));    

            String ddl = "Alter table " + dataTableFullName + " add cf1.col5 integer in_memory=true";
    		conn.createStatement().execute(ddl);

    		try (HBaseAdmin admin = conn.unwrap(PhoenixConnection.class).getQueryServices().getAdmin()) {
                HTableDescriptor tableDesc = admin.getTableDescriptor(Bytes.toBytes(dataTableFullName));
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
            String ddl = "CREATE TABLE " + dataTableFullName
                    + " (pk char(2) not null primary key, col1 integer, b.col1 integer) " + tableDDLOptions + " SPLIT ON ('EA','EZ') ";
    		conn.createStatement().execute(ddl);
            ddl = "ALTER TABLE " + dataTableFullName + " add b.col2 varchar ttl=30";
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
            String ddl = "CREATE TABLE " + dataTableFullName
                    + " (pk char(2) not null primary key) " + generateDDLOptions("TTL=100") + " SPLIT ON ('EA','EZ')";
    		conn.createStatement().execute(ddl);
            ddl = "ALTER TABLE " + dataTableFullName + " add col1 varchar ttl=30";
    		conn.createStatement().execute(ddl);
    		fail();
    	} catch (SQLException e) {
    		assertEquals(SQLExceptionCode.CANNOT_SET_TABLE_PROPERTY_ADD_COLUMN.getErrorCode(), e.getErrorCode());
    	}
    	try {
            String ddl = "ALTER TABLE " + dataTableFullName + " add col1 varchar a.ttl=30";
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
            String ddl = "create table " + dataTableFullName + " ("
        		    + " id char(1) NOT NULL,"
        		    + " col1 integer NOT NULL,"
        		    + " col2 bigint NOT NULL,"
        		    + " CONSTRAINT NAME_PK PRIMARY KEY (id, col1, col2)"
        		    + " ) " + generateDDLOptions("TTL=86400, SALT_BUCKETS = 4, DEFAULT_COLUMN_FAMILY='XYZ'");
            conn.createStatement().execute(ddl);
            try (HBaseAdmin admin = conn.unwrap(PhoenixConnection.class).getQueryServices().getAdmin()) {
                HTableDescriptor tableDesc = admin.getTableDescriptor(Bytes.toBytes(dataTableFullName));
    			HColumnDescriptor[] columnFamilies = tableDesc.getColumnFamilies();
    			assertEquals(1, columnFamilies.length);
    			assertEquals("XYZ", columnFamilies[0].getNameAsString());
    			assertEquals(86400, columnFamilies[0].getTimeToLive());
    		}
            ddl = "ALTER TABLE " + dataTableFullName + " SET TTL=30";
    		conn.createStatement().execute(ddl);
    		conn.commit();
    		try (HBaseAdmin admin = conn.unwrap(PhoenixConnection.class).getQueryServices().getAdmin()) {
                HTableDescriptor tableDesc = admin.getTableDescriptor(Bytes.toBytes(dataTableFullName));
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
    public void testSetHColumnPropertyForTableWithOnlyPKCols1() throws Exception {
    	Properties props = PropertiesUtil.deepCopy(TEST_PROPERTIES);
    	Connection conn = DriverManager.getConnection(getUrl(), props);
    	conn.setAutoCommit(false);
    	try {
            String ddl = "create table " + dataTableFullName + " ("
        		    + " id char(1) NOT NULL,"
        		    + " col1 integer NOT NULL,"
        		    + " col2 bigint NOT NULL,"
        		    + " CONSTRAINT NAME_PK PRIMARY KEY (id, col1, col2)"
        		    + " ) " + generateDDLOptions("TTL=86400, SALT_BUCKETS = 4, DEFAULT_COLUMN_FAMILY='XYZ'");
            conn.createStatement().execute(ddl);
            ddl = "ALTER TABLE " + dataTableFullName + " SET IN_MEMORY=true";
    		conn.createStatement().execute(ddl);
    		conn.commit();
    		try (HBaseAdmin admin = conn.unwrap(PhoenixConnection.class).getQueryServices().getAdmin()) {
                HTableDescriptor tableDesc = admin.getTableDescriptor(Bytes.toBytes(dataTableFullName));
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
    public void testSetHColumnPropertyForTableWithOnlyPKCols2() throws Exception {
        Properties props = PropertiesUtil.deepCopy(TEST_PROPERTIES);
        Connection conn = DriverManager.getConnection(getUrl(), props);
        conn.setAutoCommit(false);
    	try {
            String ddl = "create table " + dataTableFullName + " ("
        		    + " id char(1) NOT NULL,"
        		    + " col1 integer NOT NULL,"
        		    + " col2 bigint NOT NULL,"
        		    + " CONSTRAINT NAME_PK PRIMARY KEY (id, col1, col2)"
        		    + " ) " + generateDDLOptions("TTL=86400, SALT_BUCKETS = 4");
            conn.createStatement().execute(ddl);
            ddl = "ALTER TABLE " + dataTableFullName + " SET IN_MEMORY=true";
    		conn.createStatement().execute(ddl);
    		conn.commit();
    		try (HBaseAdmin admin = conn.unwrap(PhoenixConnection.class).getQueryServices().getAdmin()) {
                HTableDescriptor tableDesc = admin.getTableDescriptor(Bytes.toBytes(dataTableFullName));
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
            String ddl = "create table " + dataTableFullName + " ("
        		    + " id char(1) NOT NULL,"
        		    + " col1 integer NOT NULL,"
        		    + " col2 bigint NOT NULL,"
        		    + " CONSTRAINT NAME_PK PRIMARY KEY (id, col1, col2)"
        		    + " ) " + generateDDLOptions("TTL=86400, SALT_BUCKETS = 4, DEFAULT_COLUMN_FAMILY='XYZ'");
            conn.createStatement().execute(ddl);
            ddl = "ALTER TABLE " + dataTableFullName + " ADD COL3 INTEGER IN_MEMORY=true";
    		conn.createStatement().execute(ddl);
    		conn.commit();
    		try (HBaseAdmin admin = conn.unwrap(PhoenixConnection.class).getQueryServices().getAdmin()) {
                HTableDescriptor tableDesc = admin.getTableDescriptor(Bytes.toBytes(dataTableFullName));
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
            String ddl = "create table " + dataTableFullName + " ("
        		    + " id char(1) NOT NULL,"
        		    + " col1 integer NOT NULL,"
        		    + " col2 bigint NOT NULL,"
        		    + " CONSTRAINT NAME_PK PRIMARY KEY (id, col1, col2)"
        		    + " ) " + generateDDLOptions("TTL=86400, SALT_BUCKETS = 4, DEFAULT_COLUMN_FAMILY='XYZ'");
            conn.createStatement().execute(ddl);
            ddl = "ALTER TABLE " + dataTableFullName + " ADD NEWCF.COL3 INTEGER IN_MEMORY=true";
    		conn.createStatement().execute(ddl);
    		conn.commit();
    		try (HBaseAdmin admin = conn.unwrap(PhoenixConnection.class).getQueryServices().getAdmin()) {
                HTableDescriptor tableDesc = admin.getTableDescriptor(Bytes.toBytes(dataTableFullName));
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
            String ddl = "create table " + dataTableFullName + " ("
        		    + " id char(1) NOT NULL,"
        		    + " col1 integer NOT NULL,"
        		    + " col2 bigint NOT NULL,"
        		    + " CONSTRAINT NAME_PK PRIMARY KEY (id, col1, col2)"
        		    + " ) " + generateDDLOptions("TTL=86400, SALT_BUCKETS = 4, DEFAULT_COLUMN_FAMILY='XYZ'");
            conn.createStatement().execute(ddl);
            ddl = "ALTER TABLE " + dataTableFullName + " ADD NEWCF.COL3 INTEGER IN_MEMORY=true";
    		conn.createStatement().execute(ddl);
    		conn.commit();
    		try (HBaseAdmin admin = conn.unwrap(PhoenixConnection.class).getQueryServices().getAdmin()) {
                HTableDescriptor tableDesc = admin.getTableDescriptor(Bytes.toBytes(dataTableFullName));
    			HColumnDescriptor[] columnFamilies = tableDesc.getColumnFamilies();
    			assertEquals(2, columnFamilies.length);
    			assertEquals("NEWCF", columnFamilies[0].getNameAsString());
    			assertEquals(true, columnFamilies[0].isInMemory());
    			assertEquals(86400, columnFamilies[0].getTimeToLive());
    			assertEquals("XYZ", columnFamilies[1].getNameAsString());
    			assertEquals(false, columnFamilies[1].isInMemory());
    			assertEquals(86400, columnFamilies[1].getTimeToLive());
    		}

            ddl = "ALTER TABLE " + dataTableFullName + " SET TTL=1000";
    		conn.createStatement().execute(ddl);
    		conn.commit();
    		try (HBaseAdmin admin = conn.unwrap(PhoenixConnection.class).getQueryServices().getAdmin()) {
                HTableDescriptor tableDesc = admin.getTableDescriptor(Bytes.toBytes(dataTableFullName));
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
            ddl = "ALTER TABLE " + dataTableFullName + " ADD COL3 INTEGER";
    		conn.createStatement().execute(ddl);
    		conn.commit();
    		try (HBaseAdmin admin = conn.unwrap(PhoenixConnection.class).getQueryServices().getAdmin()) {
                HTableDescriptor tableDesc = admin.getTableDescriptor(Bytes.toBytes(dataTableFullName));
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
            String ddl = "create table " + dataTableFullName + " ("
    				+ " id char(1) NOT NULL,"
    				+ " col1 integer NOT NULL,"
    				+ " col2 bigint NOT NULL,"
    				+ " CONSTRAINT NAME_PK PRIMARY KEY (id, col1, col2)"
    				+ " ) " +  tableDDLOptions;
    		conn.createStatement().execute(ddl);
            ddl = "ALTER TABLE " + dataTableFullName + " ADD NEWCF.COL3 INTEGER NEWCF.UNKNOWN_PROP='ABC'";
    		try {
    			conn.createStatement().execute(ddl);
    			fail();
    		} catch (SQLException e) {
    			assertEquals(SQLExceptionCode.CANNOT_SET_TABLE_PROPERTY_ADD_COLUMN.getErrorCode(), e.getErrorCode());
    		}
            ddl = "ALTER TABLE " + dataTableFullName + " SET UNKNOWN_PROP='ABC'";
    		conn.createStatement().execute(ddl);
    		try (HBaseAdmin admin = conn.unwrap(PhoenixConnection.class).getQueryServices().getAdmin()) {
                HTableDescriptor tableDesc = admin.getTableDescriptor(Bytes.toBytes(dataTableFullName));
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
        stmt.execute("CREATE TABLE " + dataTableFullName + " (id SMALLINT PRIMARY KEY, name VARCHAR) "+tableDDLOptions);

        ResultSet rs = stmt.executeQuery("SELECT STORE_NULLS FROM \"SYSTEM\".\"CATALOG\" " +
 "WHERE table_name = '"
                + dataTableFullName + "' AND STORE_NULLS IS NOT NULL");
        assertTrue(rs.next());
        assertFalse(rs.getBoolean(1));
        assertFalse(rs.next());
        rs.close();

        stmt.execute("ALTER TABLE " + dataTableFullName + " SET STORE_NULLS = true");

        rs = stmt.executeQuery("SELECT STORE_NULLS FROM \"SYSTEM\".\"CATALOG\" " +
 "WHERE table_name = '" + dataTableFullName
                + "' AND STORE_NULLS IS NOT NULL");
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
            String ddl = "create table " + dataTableFullName + " ("
                    + " k1 char(1) NOT NULL,"
                    + " k2 integer NOT NULL,"
                    + " col1 bigint,"
                    + " CONSTRAINT NAME_PK PRIMARY KEY (k1, k2)"
                    + " ) "+tableDDLOptions;
            conn.createStatement().execute(ddl);

            // set HTableProperty when adding a pk column should fail
            ddl = "ALTER TABLE " + dataTableFullName + " ADD k3 DECIMAL PRIMARY KEY COMPACTION_ENABLED = false";
            try {
                conn.createStatement().execute(ddl);
                fail();
            } catch (SQLException e) {
                assertEquals(SQLExceptionCode.CANNOT_SET_TABLE_PROPERTY_ADD_COLUMN.getErrorCode(), e.getErrorCode());
            }

            // set HColumnProperty when adding only a pk column should fail
            ddl = "ALTER TABLE " + dataTableFullName + " ADD k3 DECIMAL PRIMARY KEY REPLICATION_SCOPE = 0";
            try {
                conn.createStatement().execute(ddl);
                fail();
            } catch (SQLException e) {
                assertEquals(SQLExceptionCode.SET_UNSUPPORTED_PROP_ON_ALTER_TABLE.getErrorCode(), e.getErrorCode());
            }

            // set phoenix table property when adding a pk column should fail
            ddl = "ALTER TABLE " + dataTableFullName + " ADD k3 DECIMAL PRIMARY KEY DISABLE_WAL = true";
            try {
                conn.createStatement().execute(ddl);
                fail();
            } catch (SQLException e) {
                assertEquals(SQLExceptionCode.CANNOT_SET_TABLE_PROPERTY_ADD_COLUMN.getErrorCode(), e.getErrorCode());
            }

            // set HColumnProperty property when adding a pk column and other key value columns should work
            ddl = "ALTER TABLE "
                    + dataTableFullName
                    + " ADD k3 DECIMAL PRIMARY KEY, col2 bigint, CF.col3 bigint IN_MEMORY = true, CF.IN_MEMORY=false, CF.REPLICATION_SCOPE = 1";
            conn.createStatement().execute(ddl);
            // assert that k3 was added as new pk
            ResultSet rs = conn.getMetaData().getPrimaryKeys("", schemaName, dataTableName);
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
                HTableDescriptor tableDesc = admin.getTableDescriptor(Bytes.toBytes(dataTableFullName));
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
            String ddl = "create table " + dataTableFullName + " ("
                    + " id char(1) NOT NULL,"
                    + " col1 integer NOT NULL,"
                    + " col2 bigint NOT NULL,"
                    + " CONSTRAINT NAME_PK PRIMARY KEY (id, col1, col2)"
                    + " ) "+tableDDLOptions;
            conn.createStatement().execute(ddl);
            asssertIsWALDisabled(conn, dataTableFullName, false);
            
            ddl = "ALTER TABLE " + dataTableFullName + " SET DISABLE_WAL = true";
            conn.createStatement().execute(ddl);
            // check metadata cache is updated with DISABLE_WAL = true
            asssertIsWALDisabled(conn, dataTableFullName, true);
            
            ddl = "ALTER TABLE " + dataTableFullName + " SET DISABLE_WAL = false";
            conn.createStatement().execute(ddl);
            // check metadata cache is updated with DISABLE_WAL = false
            asssertIsWALDisabled(conn, dataTableFullName, false);
            
            ddl = "ALTER TABLE " + dataTableFullName + " SET MULTI_TENANT = true";
            conn.createStatement().execute(ddl);
            // check metadata cache is updated with MULTI_TENANT = true
            PTable t = conn.unwrap(PhoenixConnection.class).getTable(new PTableKey(null, dataTableFullName));
            assertTrue(t.isMultiTenant());
            
            // check table metadata updated server side
            ResultSet rs = conn.createStatement().executeQuery("SELECT DISABLE_WAL, MULTI_TENANT FROM \"SYSTEM\".\"CATALOG\""
                            + "WHERE table_name = '"
                            + dataTableFullName + "' AND DISABLE_WAL IS NOT NULL AND MULTI_TENANT IS NOT NULL");
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
    public void testDeclaringColumnAsRowTimestamp() throws Exception {
        try (Connection conn = DriverManager.getConnection(getUrl())) {
            conn.createStatement().execute("CREATE TABLE " + dataTableFullName + " (PK1 DATE NOT NULL, PK2 VARCHAR NOT NULL, KV1 VARCHAR CONSTRAINT PK PRIMARY KEY(PK1 ROW_TIMESTAMP, PK2)) " + tableDDLOptions);
            PhoenixConnection phxConn = conn.unwrap(PhoenixConnection.class); 
            PTable table = phxConn.getTable(new PTableKey(phxConn.getTenantId(), dataTableFullName));
            // Assert that the column shows up as row time stamp in the cache.
            assertTrue(table.getColumnForColumnName("PK1").isRowTimestamp());
            assertFalse(table.getColumnForColumnName("PK2").isRowTimestamp());
            assertIsRowTimestampSet(schemaName, dataTableName, "PK1");
            
            String dataTableName2 = BaseTest.generateUniqueName();
            String dataTableFullName2 = SchemaUtil.getTableName(schemaName, dataTableName2);
            conn.createStatement().execute("CREATE TABLE " + dataTableFullName2 + " (PK1 VARCHAR, PK2 DATE PRIMARY KEY ROW_TIMESTAMP, KV1 VARCHAR, KV2 INTEGER)");
            table = phxConn.getTable(new PTableKey(phxConn.getTenantId(), dataTableFullName2));
            // Assert that the column shows up as row time stamp in the cache.
            assertFalse(table.getColumnForColumnName("PK1").isRowTimestamp());
            assertTrue(table.getColumnForColumnName("PK2").isRowTimestamp());
            assertIsRowTimestampSet(schemaName, dataTableName2, "PK2");
            
            // Create an index on a table has a row time stamp pk column. The column should show up as a row time stamp column for the index too. 
            conn.createStatement().execute("CREATE INDEX " + indexTableName + "  ON " + dataTableFullName2 + " (KV1) include (KV2)");
            PTable indexTable = phxConn.getTable(new PTableKey(phxConn.getTenantId(), indexTableFullName));
            String indexColName = IndexUtil.getIndexColumnName(table.getColumnForColumnName("PK2"));
            // Assert that the column shows up as row time stamp in the cache.
            assertTrue(indexTable.getColumnForColumnName(indexColName).isRowTimestamp());
            assertIsRowTimestampSet(schemaName, indexTableName, indexColName);
            String viewTableName2 = dataTableName2 + "_VIEW";
            String viewTableFullName2 = SchemaUtil.getTableName(schemaName, viewTableName2);
            // Creating a view with a row_timestamp column in its pk constraint is not allowed
            try {
                conn.createStatement().execute("CREATE VIEW " + viewTableFullName2 + " (KV3 VARCHAR, KV4 DATE, KV5 INTEGER, CONSTRAINT PK PRIMARY KEY (KV3, KV4 ROW_TIMESTAMP) ) AS SELECT * FROM " + dataTableFullName2);
                fail("Creating a view with a row_timestamp column in its pk constraint is not allowed");
            } catch (SQLException e) {
                assertEquals(SQLExceptionCode.ROWTIMESTAMP_NOT_ALLOWED_ON_VIEW.getErrorCode(), e.getErrorCode());
            }
            
            // Make sure that the base table column declared as row_timestamp is also row_timestamp for view
            conn.createStatement().execute("CREATE VIEW " + viewTableFullName2 + " (KV3 VARCHAR, KV4 VARCHAR, KV5 INTEGER, CONSTRAINT PK PRIMARY KEY (KV3, KV4) ) AS SELECT * FROM " + dataTableFullName2);
            PTable view = phxConn.getTable(new PTableKey(phxConn.getTenantId(), viewTableFullName2));
            assertNotNull(view.getPKColumn("PK2"));
            assertTrue(view.getPKColumn("PK2").isRowTimestamp());
        }
    }
    
    private void assertIsRowTimestampSet(String schemaName, String tableName, String columnName) throws SQLException {
        String sql = "SELECT IS_ROW_TIMESTAMP FROM \"SYSTEM\".\"CATALOG\" WHERE "
                + "(TABLE_SCHEM, TABLE_NAME) = ('" + schemaName + "','"+ tableName + "') AND\n"
                + "COLUMN_FAMILY IS NULL AND COLUMN_NAME = ?";
        try(Connection conn = DriverManager.getConnection(getUrl())) {
            PreparedStatement stmt = conn.prepareStatement(sql);
            stmt.setString(1, columnName);
            ResultSet rs = stmt.executeQuery();
            assertTrue(rs.next());
            assertEquals(true, rs.getBoolean(1));
        }
    }
    
    @Test
    public void testAddingRowTimestampColumnNotAllowedViaAlterTable() throws Exception {
        try (Connection conn = DriverManager.getConnection(getUrl())) {
            conn.createStatement().execute("CREATE TABLE " + dataTableFullName + " (PK1 VARCHAR NOT NULL, PK2 VARCHAR NOT NULL, KV1 VARCHAR CONSTRAINT PK PRIMARY KEY(PK1, PK2)) " + tableDDLOptions);
            // adding a new pk column that is also row_timestamp is not allowed
            try {
                conn.createStatement().execute("ALTER TABLE " + dataTableFullName + " ADD PK3 DATE PRIMARY KEY ROW_TIMESTAMP");
                fail("Altering table to add a PK column as row_timestamp column should fail");
            } catch (SQLException e) {
                assertEquals(SQLExceptionCode.ROWTIMESTAMP_CREATE_ONLY.getErrorCode(), e.getErrorCode());
            }
        }
    }
    
	@Test
	public void testCreatingTxnTableFailsIfTxnsDisabled() throws Exception {
        Properties props = PropertiesUtil.deepCopy(TEST_PROPERTIES);
        props.setProperty(QueryServices.TRANSACTIONS_ENABLED, Boolean.toString(false));
		try (Connection conn = DriverManager.getConnection(getUrl(), props)) {
			// creating a transactional table should fail if transactions are disabled
			try {
				conn.createStatement().execute("CREATE TABLE " + dataTableFullName + "(k INTEGER PRIMARY KEY, v VARCHAR) " + generateDDLOptions("TRANSACTIONAL=true"));
				fail();
			} catch (SQLException e) {
				assertEquals(SQLExceptionCode.CANNOT_CREATE_TXN_TABLE_IF_TXNS_DISABLED.getErrorCode(), e.getErrorCode());
			}
			// altering a table to be transactional  should fail if transactions are disabled
			conn.createStatement().execute("CREATE TABLE " + dataTableFullName + "(k INTEGER PRIMARY KEY, v VARCHAR)");
			try {
				conn.createStatement().execute("ALTER TABLE " + dataTableFullName + " SET TRANSACTIONAL=true");
				fail();
			} catch (SQLException e) {
				assertEquals(SQLExceptionCode.CANNOT_ALTER_TO_BE_TXN_IF_TXNS_DISABLED.getErrorCode(), e.getErrorCode());
			}
		}
	}
	
	@Test
	public void testMetadataForImmutableTable() throws Exception {
	    String schemaName = "XYZ";
	    String baseTableName = generateUniqueName();
	    String viewName = generateUniqueName();
	    String fullTableName = schemaName + "." + baseTableName;
	    String fullViewName = schemaName + "." + viewName;
	    try (Connection conn = DriverManager.getConnection(getUrl())) {
	        PhoenixConnection phxConn = conn.unwrap(PhoenixConnection.class);
	        conn.createStatement().execute("CREATE TABLE IF NOT EXISTS " + fullTableName + " ("
	                + " ID char(1) NOT NULL,"
	                + " COL1 integer NOT NULL,"
	                + " COL2 bigint NOT NULL,"
	                + " KV1 VARCHAR"
	                + " CONSTRAINT NAME_PK PRIMARY KEY (ID, COL1, COL2)"
	                + " ) " + generateDDLOptions("IMMUTABLE_ROWS = true"
	                + (!columnEncoded ? ",IMMUTABLE_STORAGE_SCHEME="+ PTable.ImmutableStorageScheme.ONE_CELL_PER_COLUMN : "")));
	        PTable baseTable = phxConn.getTable(new PTableKey(phxConn.getTenantId(), fullTableName));
	        long initBaseTableSeqNumber = baseTable.getSequenceNumber(); 

	        // assert that the client side cache is updated.
	        EncodedCQCounter cqCounter = baseTable.getEncodedCQCounter();
	        assertEquals( columnEncoded ? (Integer)(ENCODED_CQ_COUNTER_INITIAL_VALUE + 1) : null, cqCounter.getNextQualifier(QueryConstants.DEFAULT_COLUMN_FAMILY));
	        
	        // assert that the server side metadata is updated correctly.
	        assertEncodedCQCounter(DEFAULT_COLUMN_FAMILY, schemaName, baseTableName, ENCODED_CQ_COUNTER_INITIAL_VALUE + 1);
	        assertEncodedCQValue(DEFAULT_COLUMN_FAMILY, "KV1", schemaName, baseTableName, ENCODED_CQ_COUNTER_INITIAL_VALUE);
	        assertSequenceNumber(schemaName, baseTableName, initBaseTableSeqNumber);

	        // now create a view and validate client and server side metadata
	        String viewDDL = "CREATE VIEW " + fullViewName + " ( VIEW_COL1 INTEGER, A.VIEW_COL2 VARCHAR ) AS SELECT * FROM " + fullTableName;
	        conn.createStatement().execute(viewDDL);
	        baseTable = phxConn.getTable(new PTableKey(phxConn.getTenantId(), fullTableName));
	        PTable view = phxConn.getTable(new PTableKey(phxConn.getTenantId(), fullViewName));

	        // verify that the client side cache is updated. Base table's cq counters should be updated.
	        assertEquals( columnEncoded ? (Integer)(ENCODED_CQ_COUNTER_INITIAL_VALUE + 2) : null, baseTable.getEncodedCQCounter().getNextQualifier(DEFAULT_COLUMN_FAMILY));
	        assertEquals( columnEncoded ? (Integer)(ENCODED_CQ_COUNTER_INITIAL_VALUE + 1) : null, baseTable.getEncodedCQCounter().getNextQualifier("A"));
	        assertNull("A view should always have the null cq counter", view.getEncodedCQCounter().getNextQualifier(DEFAULT_COLUMN_FAMILY));
	        
	        // assert that the server side metadata for the base table and the view is also updated correctly.
	        assertEncodedCQCounter(DEFAULT_COLUMN_FAMILY, schemaName, baseTableName, ENCODED_CQ_COUNTER_INITIAL_VALUE + 2);
	        assertEncodedCQCounter("A", schemaName, baseTableName, ENCODED_CQ_COUNTER_INITIAL_VALUE + 1);
	        assertEncodedCQValue(DEFAULT_COLUMN_FAMILY, "VIEW_COL1", schemaName, viewName, ENCODED_CQ_COUNTER_INITIAL_VALUE + 1);
	        assertEncodedCQValue("A", "VIEW_COL2", schemaName, viewName, ENCODED_CQ_COUNTER_INITIAL_VALUE);
	        assertSequenceNumber(schemaName, baseTableName, initBaseTableSeqNumber + (columnEncoded ? 1 : 0));
	        assertSequenceNumber(schemaName, viewName, PTable.INITIAL_SEQ_NUM);
	    }
	}
	
	@Test
	public void testMetadataForMutableTable() throws Exception {
	    String schemaName = "XYZ";
	    String baseTableName = generateUniqueName();
	    String viewName = generateUniqueName();
	    String fullTableName = schemaName + "." + baseTableName;
	    String fullViewName = schemaName + "." + viewName;
	    try (Connection conn = DriverManager.getConnection(getUrl())) {
	        PhoenixConnection phxConn = conn.unwrap(PhoenixConnection.class);
	        conn.createStatement().execute("CREATE TABLE IF NOT EXISTS " + fullTableName + " ("
	                + " ID char(1) NOT NULL,"
	                + " COL1 integer NOT NULL,"
	                + " COL2 bigint NOT NULL,"
	                + " KV1 VARCHAR"
	                + " CONSTRAINT NAME_PK PRIMARY KEY (ID, COL1, COL2)"
	                + " ) " + tableDDLOptions);
	        PTable baseTable = phxConn.getTable(new PTableKey(phxConn.getTenantId(), fullTableName));
	        long initBaseTableSeqNumber = baseTable.getSequenceNumber(); 

	        // assert that the client side cache is updated.
	        EncodedCQCounter cqCounter = baseTable.getEncodedCQCounter();
	        assertEquals( columnEncoded ? (Integer)(ENCODED_CQ_COUNTER_INITIAL_VALUE + 1) : null, cqCounter.getNextQualifier(QueryConstants.DEFAULT_COLUMN_FAMILY));


	        // assert that the server side metadata is updated correctly.
	        assertEncodedCQCounter(DEFAULT_COLUMN_FAMILY, schemaName, baseTableName, ENCODED_CQ_COUNTER_INITIAL_VALUE + 1);
	        assertEncodedCQValue(DEFAULT_COLUMN_FAMILY, "KV1", schemaName, baseTableName, ENCODED_CQ_COUNTER_INITIAL_VALUE);
	        assertSequenceNumber(schemaName, baseTableName, initBaseTableSeqNumber);

	        // now create a view and validate client and server side metadata
	        String viewDDL = "CREATE VIEW " + fullViewName + " ( VIEW_COL1 INTEGER, A.VIEW_COL2 VARCHAR ) AS SELECT * FROM " + fullTableName;
	        conn.createStatement().execute(viewDDL);
	        baseTable = phxConn.getTable(new PTableKey(phxConn.getTenantId(), fullTableName));
	        PTable view = phxConn.getTable(new PTableKey(phxConn.getTenantId(), fullViewName));

	        // verify that the client side cache is updated. Base table's cq counters should be updated.
	        assertEquals(columnEncoded ? (Integer)(ENCODED_CQ_COUNTER_INITIAL_VALUE + 3) : null, baseTable.getEncodedCQCounter().getNextQualifier(DEFAULT_COLUMN_FAMILY));
	        assertNull("A view should always have the null cq counter", view.getEncodedCQCounter().getNextQualifier(DEFAULT_COLUMN_FAMILY));

	        // assert that the server side metadata for the base table and the view is also updated correctly.
	        assertEncodedCQCounter(DEFAULT_COLUMN_FAMILY, schemaName, baseTableName, ENCODED_CQ_COUNTER_INITIAL_VALUE + 3);
	        assertEncodedCQValue(DEFAULT_COLUMN_FAMILY, "VIEW_COL1", schemaName, viewName, ENCODED_CQ_COUNTER_INITIAL_VALUE + 1);
	        assertEncodedCQValue("A", "VIEW_COL2", schemaName, viewName, ENCODED_CQ_COUNTER_INITIAL_VALUE + 2);
	        assertSequenceNumber(schemaName, baseTableName, initBaseTableSeqNumber + (columnEncoded ? 1 : 0));
	        assertSequenceNumber(schemaName, viewName, PTable.INITIAL_SEQ_NUM);
	    }
	}
	
	@Test
    public void testAddingColumnsToTablesAndViews() throws Exception {
        String schemaName = generateUniqueName();
        String baseTableName = generateUniqueName();
        String viewName = generateUniqueName();
        String fullTableName = schemaName + "." + baseTableName;
        String fullViewName = schemaName + "." + viewName;
        Properties props = new Properties();
        props.put(QueryServices.IS_NAMESPACE_MAPPING_ENABLED, Boolean.toString(true));
        try (Connection conn = DriverManager.getConnection(getUrl(), props)) {
            conn.createStatement().execute("CREATE SCHEMA " + schemaName);
            PhoenixConnection phxConn = conn.unwrap(PhoenixConnection.class);
            conn.createStatement().execute("CREATE TABLE " + fullTableName + " ("
                    + " ID char(1) NOT NULL,"
                    + " COL1 integer NOT NULL,"
                    + " COL2 bigint NOT NULL,"
                    + " CONSTRAINT NAME_PK PRIMARY KEY (ID, COL1, COL2)"
                    + " ) " + tableDDLOptions);
            PTable baseTable = phxConn.getTable(new PTableKey(phxConn.getTenantId(), fullTableName));
            long initBaseTableSeqNumber = baseTable.getSequenceNumber(); 

            // Add a column to the base table and see if the client and server metadata is updated correctly
            String alterDDL = "ALTER TABLE " + fullTableName + " ADD COL3 VARCHAR PRIMARY KEY, COL4 INTEGER, COL5 VARCHAR, B.COL6 DECIMAL (10, 2)";
            conn.createStatement().execute(alterDDL);

            // assert that the client side cache is updated.
            baseTable = phxConn.getTable(new PTableKey(phxConn.getTenantId(), fullTableName));
            EncodedCQCounter encodedCqCounter = baseTable.getEncodedCQCounter();
            assertEquals( columnEncoded ?(Integer)(ENCODED_CQ_COUNTER_INITIAL_VALUE + 3) : null, encodedCqCounter.getNextQualifier(DEFAULT_COLUMN_FAMILY));
            
            // assert that the server side metadata is updated correctly.
            assertEncodedCQCounter(DEFAULT_COLUMN_FAMILY, schemaName, baseTableName, ENCODED_CQ_COUNTER_INITIAL_VALUE + 3);
            
            // assert that the server side metadata for columns is updated correctly.
            assertEncodedCQValue(DEFAULT_COLUMN_FAMILY, "COL4", schemaName, baseTableName, ENCODED_CQ_COUNTER_INITIAL_VALUE);
            assertEncodedCQValue(DEFAULT_COLUMN_FAMILY, "COL5", schemaName, baseTableName, ENCODED_CQ_COUNTER_INITIAL_VALUE + 1);
            assertEncodedCQValue("B", "COL6", schemaName, baseTableName, ENCODED_CQ_COUNTER_INITIAL_VALUE + 2);
            long baseTableSeqNumBeforeAddingChildCols = initBaseTableSeqNumber + 1;
            assertSequenceNumber(schemaName, baseTableName, baseTableSeqNumBeforeAddingChildCols);

            // Create a view
            String viewDDL = "CREATE VIEW " + fullViewName + " ( VIEW_COL1 INTEGER, A.VIEW_COL2 VARCHAR ) AS SELECT * FROM " + fullTableName;
            conn.createStatement().execute(viewDDL);
            
            // assert that the server side metadata is updated correctly.
            assertEncodedCQCounter(DEFAULT_COLUMN_FAMILY, schemaName, baseTableName, ENCODED_CQ_COUNTER_INITIAL_VALUE + 5);
            
            // assert that the server side metadata for columns is updated correctly.
            assertEncodedCQValue(DEFAULT_COLUMN_FAMILY, "VIEW_COL1", schemaName, viewName, ENCODED_CQ_COUNTER_INITIAL_VALUE + 3);
            assertEncodedCQValue("A", "VIEW_COL2", schemaName, viewName, ENCODED_CQ_COUNTER_INITIAL_VALUE + 4);
            // for encoded columns creating a view that adds its own columns should increment the base table's sequence number too.
            assertSequenceNumber(schemaName, baseTableName, columnEncoded ? initBaseTableSeqNumber + 2 : baseTableSeqNumBeforeAddingChildCols );

            // Add column to the view
            viewDDL = "ALTER VIEW " + fullViewName + " ADD VIEW_COL3 DECIMAL(10, 2), A.VIEW_COL4 VARCHAR, B.VIEW_COL5 INTEGER";
            conn.createStatement().execute(viewDDL);

            // assert that the client cache for the base table is updated
            baseTable = phxConn.getTable(new PTableKey(phxConn.getTenantId(), fullTableName));
            encodedCqCounter = baseTable.getEncodedCQCounter();
            assertEquals( columnEncoded ? (Integer)(ENCODED_CQ_COUNTER_INITIAL_VALUE + 8) : null, encodedCqCounter.getNextQualifier(DEFAULT_COLUMN_FAMILY));
            
            // assert client cache for view
            PTable view = phxConn.getTable(new PTableKey(phxConn.getTenantId(), fullViewName));
            encodedCqCounter = view.getEncodedCQCounter();
            assertNull("A view should always have the column qualifier counter as null", view.getEncodedCQCounter().getNextQualifier(DEFAULT_COLUMN_FAMILY));
            
            // assert that the server side metadata for the base table and the view is also updated correctly.
            assertEncodedCQCounter(DEFAULT_COLUMN_FAMILY, schemaName, baseTableName, ENCODED_CQ_COUNTER_INITIAL_VALUE + 8);
            assertEncodedCQValue(DEFAULT_COLUMN_FAMILY, "VIEW_COL1", schemaName, viewName, ENCODED_CQ_COUNTER_INITIAL_VALUE + 3);
            assertEncodedCQValue("A", "VIEW_COL2", schemaName, viewName, ENCODED_CQ_COUNTER_INITIAL_VALUE + 4);
            assertEncodedCQValue(DEFAULT_COLUMN_FAMILY, "VIEW_COL3", schemaName, viewName, ENCODED_CQ_COUNTER_INITIAL_VALUE + 5);
            assertEncodedCQValue("A", "VIEW_COL4", schemaName, viewName, ENCODED_CQ_COUNTER_INITIAL_VALUE + 6);
            assertEncodedCQValue("B", "VIEW_COL5", schemaName, viewName, ENCODED_CQ_COUNTER_INITIAL_VALUE + 7);
            // adding a column to the should increment the base table's sequence number too since we update the cq counters for column families.
            assertSequenceNumber(schemaName, baseTableName, columnEncoded ? initBaseTableSeqNumber + 3 : baseTableSeqNumBeforeAddingChildCols );
            assertSequenceNumber(schemaName, viewName, PTable.INITIAL_SEQ_NUM + 1);
            
            // Add column to the base table which doesn't already exist in the view.
            alterDDL = "ALTER TABLE " + fullTableName + " ADD COL10 VARCHAR, A.COL11 INTEGER";
            conn.createStatement().execute(alterDDL);
            baseTable = phxConn.getTable(new PTableKey(phxConn.getTenantId(), fullTableName));
            
            // assert that the client cache for the base table is updated 
            encodedCqCounter = baseTable.getEncodedCQCounter();
            assertEquals( columnEncoded ? (Integer)(ENCODED_CQ_COUNTER_INITIAL_VALUE + 10) : null, encodedCqCounter.getNextQualifier(DEFAULT_COLUMN_FAMILY));
            
            // assert client cache for view
            view = phxConn.getTable(new PTableKey(phxConn.getTenantId(), fullViewName));
            encodedCqCounter = view.getEncodedCQCounter();
            assertNull("A view should always have the column qualifier counter as null", view.getEncodedCQCounter().getNextQualifier(DEFAULT_COLUMN_FAMILY));
            
            // assert that the server side metadata for the base table and the view is also updated correctly.
            assertEncodedCQCounter(DEFAULT_COLUMN_FAMILY, schemaName, baseTableName, (ENCODED_CQ_COUNTER_INITIAL_VALUE + 10));
            assertEncodedCQValue(DEFAULT_COLUMN_FAMILY, "COL10", schemaName, viewName, (ENCODED_CQ_COUNTER_INITIAL_VALUE + 8));
            assertEncodedCQValue("A", "COL11", schemaName, viewName, ENCODED_CQ_COUNTER_INITIAL_VALUE + 9);
            assertSequenceNumber(schemaName, baseTableName, columnEncoded ? initBaseTableSeqNumber + 4 : initBaseTableSeqNumber + 2 );
            assertSequenceNumber(schemaName, viewName, PTable.INITIAL_SEQ_NUM + 2);
        }
    }
	
	private void assertEncodedCQValue(String columnFamily, String columnName, String schemaName, String tableName, int expectedValue) throws Exception {
        String query = "SELECT " + COLUMN_QUALIFIER + " FROM SYSTEM.CATALOG WHERE " + TABLE_SCHEM + " = ? AND " + TABLE_NAME
                + " = ? " + " AND " + COLUMN_FAMILY + " = ?" + " AND " + COLUMN_NAME  + " = ?" + " AND " + COLUMN_QUALIFIER  + " IS NOT NULL";
        try (Connection conn = DriverManager.getConnection(getUrl())) {
            PreparedStatement stmt = conn.prepareStatement(query);
            stmt.setString(1, schemaName);
            stmt.setString(2, tableName);
            stmt.setString(3, columnFamily);
            stmt.setString(4, columnName);
            ResultSet rs = stmt.executeQuery();
            assertTrue(rs.next());
            if (columnEncoded) {
                assertTrue(Bytes.equals(QualifierEncodingScheme.TWO_BYTE_QUALIFIERS.encode(expectedValue), rs.getBytes(1)));
            } else {
                assertTrue(Bytes.equals(columnName.getBytes(), rs.getBytes(1)));
            }
            assertFalse(rs.next());
        }
    }
    
    private void assertEncodedCQCounter(String columnFamily, String schemaName, String tableName, int expectedValue) throws Exception {
        String query = "SELECT " + COLUMN_QUALIFIER_COUNTER + " FROM SYSTEM.CATALOG WHERE " + TABLE_SCHEM + " = ? AND " + TABLE_NAME
                + " = ? " + " AND " + COLUMN_FAMILY + " = ? AND " + COLUMN_QUALIFIER_COUNTER + " IS NOT NULL";
        try (Connection conn = DriverManager.getConnection(getUrl())) {
            PreparedStatement stmt = conn.prepareStatement(query);
            stmt.setString(1, schemaName);
            stmt.setString(2, tableName);
            stmt.setString(3, columnFamily);
            ResultSet rs = stmt.executeQuery();
            if (columnEncoded) {
                assertTrue(rs.next());
                assertEquals(expectedValue, rs.getInt(1));
                assertFalse(rs.next());
            } else {
                assertFalse(rs.next());
            }
        }
    }
    
    private void assertSequenceNumber(String schemaName, String tableName, long expectedSequenceNumber) throws Exception {
        String query = "SELECT " + TABLE_SEQ_NUM + " FROM SYSTEM.CATALOG WHERE " + TABLE_SCHEM + " = ? AND " + TABLE_NAME
                + " = ? AND " +  TABLE_SEQ_NUM + " IS NOT NULL AND " + COLUMN_NAME + " IS NULL AND "
                + COLUMN_FAMILY + " IS NULL ";
        try (Connection conn = DriverManager.getConnection(getUrl())) {
            PreparedStatement stmt = conn.prepareStatement(query);
            stmt.setString(1, schemaName);
            stmt.setString(2, tableName);
            ResultSet rs = stmt.executeQuery();
            assertTrue(rs.next());
            assertEquals(expectedSequenceNumber, rs.getInt(1));
            assertFalse(rs.next());
        }
    }
    
    @Test
    public void testAlterImmutableRowsPropertyForOneCellPerKeyValueColumnStorageScheme() throws Exception {
        Properties props = PropertiesUtil.deepCopy(TEST_PROPERTIES);
        String ddl = "CREATE TABLE " + dataTableFullName + " (\n"
                +"ID VARCHAR(15) NOT NULL,\n"
                +"CREATED_DATE DATE,\n"
                +"CREATION_TIME BIGINT,\n"
                +"CONSTRAINT PK PRIMARY KEY (ID)) " + tableDDLOptions;
        Connection conn = DriverManager.getConnection(getUrl(), props);
        conn.createStatement().execute(ddl);
        assertImmutableRows(conn, dataTableFullName, false);
        ddl = "ALTER TABLE " + dataTableFullName + " SET IMMUTABLE_ROWS = true";
        conn.createStatement().execute(ddl);
        assertImmutableRows(conn, dataTableFullName, true);
    }
    
    @Test
    public void testAlterImmutableRowsPropertyForOneCellPerColumnFamilyStorageScheme() throws Exception {
        Properties props = PropertiesUtil.deepCopy(TEST_PROPERTIES);
        String ddl = "CREATE TABLE " + dataTableFullName + " (\n"
                +"ID VARCHAR(15) NOT NULL,\n"
                +"CREATED_DATE DATE,\n"
                +"CREATION_TIME BIGINT,\n"
                +"CONSTRAINT PK PRIMARY KEY (ID)) " + generateDDLOptions("COLUMN_ENCODED_BYTES=4, IMMUTABLE_ROWS=true"
                + (!columnEncoded ? ",IMMUTABLE_STORAGE_SCHEME=" + PTable.ImmutableStorageScheme.ONE_CELL_PER_COLUMN : ""));
        Connection conn = DriverManager.getConnection(getUrl(), props);
        conn.createStatement().execute(ddl);
        assertImmutableRows(conn, dataTableFullName, true);
        try {
	        ddl = "ALTER TABLE " + dataTableFullName + " SET IMMUTABLE_ROWS = false";
	        conn.createStatement().execute(ddl);
	        if (columnEncoded) {
	            fail();
	        }
        }
        catch(SQLException e) {
        	assertEquals(SQLExceptionCode.CANNOT_ALTER_IMMUTABLE_ROWS_PROPERTY.getErrorCode(), e.getErrorCode());
        }
        assertImmutableRows(conn, dataTableFullName, columnEncoded);
    }
    
}
 
