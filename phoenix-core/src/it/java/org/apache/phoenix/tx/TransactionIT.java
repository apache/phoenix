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
package org.apache.phoenix.tx;

import static org.apache.phoenix.util.TestUtil.INDEX_DATA_SCHEMA;
import static org.apache.phoenix.util.TestUtil.TEST_PROPERTIES;
import static org.apache.phoenix.util.TestUtil.TRANSACTIONAL_DATA_TABLE;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.sql.Connection;
import java.sql.Date;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTableInterface;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.phoenix.coprocessor.PhoenixTransactionalProcessor;
import org.apache.phoenix.end2end.BaseHBaseManagedTimeIT;
import org.apache.phoenix.end2end.Shadower;
import org.apache.phoenix.exception.SQLExceptionCode;
import org.apache.phoenix.jdbc.PhoenixConnection;
import org.apache.phoenix.jdbc.PhoenixDatabaseMetaData;
import org.apache.phoenix.query.QueryConstants;
import org.apache.phoenix.query.QueryServices;
import org.apache.phoenix.query.QueryServicesOptions;
import org.apache.phoenix.schema.PTable;
import org.apache.phoenix.schema.PTableKey;
import org.apache.phoenix.schema.types.PInteger;
import org.apache.phoenix.util.ByteUtil;
import org.apache.phoenix.util.PropertiesUtil;
import org.apache.phoenix.util.ReadOnlyProps;
import org.apache.phoenix.util.TestUtil;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Ignore;
import org.junit.Test;

import co.cask.tephra.TransactionContext;
import co.cask.tephra.TransactionSystemClient;
import co.cask.tephra.TxConstants;
import co.cask.tephra.hbase10cdh.TransactionAwareHTable;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;

public class TransactionIT extends BaseHBaseManagedTimeIT {
    
    private static final String FULL_TABLE_NAME = INDEX_DATA_SCHEMA + QueryConstants.NAME_SEPARATOR + TRANSACTIONAL_DATA_TABLE;
    
    @Before
    public void setUp() throws SQLException {
        ensureTableCreated(getUrl(), TRANSACTIONAL_DATA_TABLE);
    }
    
    @BeforeClass
    @Shadower(classBeingShadowed = BaseHBaseManagedTimeIT.class)
    public static void doSetup() throws Exception {
        Map<String,String> props = Maps.newHashMapWithExpectedSize(1);
        props.put(QueryServices.TRANSACTIONS_ENABLED, Boolean.toString(true));
        setUpTestDriver(new ReadOnlyProps(props.entrySet().iterator()));
    }
        
    @Test
    public void testReadOwnWrites() throws Exception {
        String selectSql = "SELECT * FROM "+FULL_TABLE_NAME;
        try (Connection conn = DriverManager.getConnection(getUrl())) {
            conn.setAutoCommit(false);
            ResultSet rs = conn.createStatement().executeQuery(selectSql);
            assertFalse(rs.next());
            
            String upsert = "UPSERT INTO " + FULL_TABLE_NAME + "(varchar_pk, char_pk, int_pk, long_pk, decimal_pk, date_pk) VALUES(?, ?, ?, ?, ?, ?)";
            PreparedStatement stmt = conn.prepareStatement(upsert);
            // upsert two rows
            TestUtil.setRowKeyColumns(stmt, 1);
            stmt.execute();
            TestUtil.setRowKeyColumns(stmt, 2);
            stmt.execute();
            
            // verify rows can be read even though commit has not been called
            rs = conn.createStatement().executeQuery(selectSql);
            TestUtil.validateRowKeyColumns(rs, 1);
            TestUtil.validateRowKeyColumns(rs, 2);
            assertFalse(rs.next());
            
            conn.commit();
            
            // verify rows can be read after commit
            rs = conn.createStatement().executeQuery(selectSql);
            TestUtil.validateRowKeyColumns(rs, 1);
            TestUtil.validateRowKeyColumns(rs, 2);
            assertFalse(rs.next());
        }
    }
    
    @Test
    public void testTxnClosedCorrecty() throws Exception {
        String selectSql = "SELECT * FROM "+FULL_TABLE_NAME;
        try (Connection conn = DriverManager.getConnection(getUrl())) {
            conn.setAutoCommit(false);
            ResultSet rs = conn.createStatement().executeQuery(selectSql);
            assertFalse(rs.next());
            
            String upsert = "UPSERT INTO " + FULL_TABLE_NAME + "(varchar_pk, char_pk, int_pk, long_pk, decimal_pk, date_pk) VALUES(?, ?, ?, ?, ?, ?)";
            PreparedStatement stmt = conn.prepareStatement(upsert);
            // upsert two rows
            TestUtil.setRowKeyColumns(stmt, 1);
            stmt.execute();
            TestUtil.setRowKeyColumns(stmt, 2);
            stmt.execute();
            
            // verify rows can be read even though commit has not been called
            rs = conn.createStatement().executeQuery(selectSql);
            TestUtil.validateRowKeyColumns(rs, 1);
            TestUtil.validateRowKeyColumns(rs, 2);
            assertFalse(rs.next());
            
            conn.close();
            // wait for any open txns to time out
            Thread.sleep(DEFAULT_TXN_TIMEOUT_SECONDS*1000+10000);
            assertTrue("There should be no invalid transactions", txManager.getInvalidSize()==0);
        }
    }
    
    @Test
    public void testDelete() throws Exception {
        String selectSQL = "SELECT * FROM " + FULL_TABLE_NAME;
        try (Connection conn1 = DriverManager.getConnection(getUrl()); 
                Connection conn2 = DriverManager.getConnection(getUrl())) {
            conn1.setAutoCommit(false);
            ResultSet rs = conn1.createStatement().executeQuery(selectSQL);
            assertFalse(rs.next());
            
            String upsert = "UPSERT INTO " + FULL_TABLE_NAME + "(varchar_pk, char_pk, int_pk, long_pk, decimal_pk, date_pk) VALUES(?, ?, ?, ?, ?, ?)";
            PreparedStatement stmt = conn1.prepareStatement(upsert);
            // upsert two rows
            TestUtil.setRowKeyColumns(stmt, 1);
            stmt.execute();
            conn1.commit();
            
            TestUtil.setRowKeyColumns(stmt, 2);
            stmt.execute();
            
            // verify rows can be read even though commit has not been called
            int rowsDeleted = conn1.createStatement().executeUpdate("DELETE FROM " + FULL_TABLE_NAME);
            assertEquals(2, rowsDeleted);
            
            // Delete and second upsert not committed yet, so there should be one row.
            rs = conn2.createStatement().executeQuery("SELECT count(*) FROM " + FULL_TABLE_NAME);
            assertTrue(rs.next());
            assertEquals(1, rs.getInt(1));
            
            conn1.commit();
            
            // verify rows are deleted after commit
            rs = conn1.createStatement().executeQuery(selectSQL);
            assertFalse(rs.next());
        }
    }
    
    @Test
    public void testAutoCommitQuerySingleTable() throws Exception {
        try (Connection conn = DriverManager.getConnection(getUrl())) {
            conn.setAutoCommit(true);
            // verify no rows returned
            ResultSet rs = conn.createStatement().executeQuery("SELECT * FROM " + FULL_TABLE_NAME);
            assertFalse(rs.next());
        }
    }
    
    @Test
    public void testAutoCommitQueryMultiTables() throws Exception {
        try (Connection conn = DriverManager.getConnection(getUrl())) {
            conn.setAutoCommit(true);
            // verify no rows returned
            ResultSet rs = conn.createStatement().executeQuery("SELECT * FROM " + FULL_TABLE_NAME + " a JOIN " + FULL_TABLE_NAME + " b ON (a.long_pk = b.int_pk)");
            assertFalse(rs.next());
        } 
    }
    
    @Test
    public void testColConflicts() throws Exception {
        try (Connection conn1 = DriverManager.getConnection(getUrl()); 
                Connection conn2 = DriverManager.getConnection(getUrl())) {
            conn1.setAutoCommit(false);
            conn2.setAutoCommit(false);
            String selectSql = "SELECT * FROM "+FULL_TABLE_NAME;
            conn1.setAutoCommit(false);
            ResultSet rs = conn1.createStatement().executeQuery(selectSql);
            assertFalse(rs.next());
            // upsert row using conn1
            String upsertSql = "UPSERT INTO " + FULL_TABLE_NAME + "(varchar_pk, char_pk, int_pk, long_pk, decimal_pk, date_pk, a.int_col1) VALUES(?, ?, ?, ?, ?, ?, ?)";
            PreparedStatement stmt = conn1.prepareStatement(upsertSql);
            TestUtil.setRowKeyColumns(stmt, 1);
            stmt.setInt(7, 10);
            stmt.execute();
            // upsert row using conn2
            stmt = conn2.prepareStatement(upsertSql);
            TestUtil.setRowKeyColumns(stmt, 1);
            stmt.setInt(7, 11);
            stmt.execute();
            
            conn1.commit();
            //second commit should fail
            try {
                conn2.commit();
                fail();
            }   
            catch (SQLException e) {
                assertEquals(e.getErrorCode(), SQLExceptionCode.TRANSACTION_CONFLICT_EXCEPTION.getErrorCode());
            }
        }
    }
    
    private void testRowConflicts() throws Exception {
        try (Connection conn1 = DriverManager.getConnection(getUrl()); 
                Connection conn2 = DriverManager.getConnection(getUrl())) {
            conn1.setAutoCommit(false);
            conn2.setAutoCommit(false);
            String selectSql = "SELECT * FROM "+FULL_TABLE_NAME;
            conn1.setAutoCommit(false);
            ResultSet rs = conn1.createStatement().executeQuery(selectSql);
            boolean immutableRows = conn1.unwrap(PhoenixConnection.class).getTable(new PTableKey(null, FULL_TABLE_NAME)).isImmutableRows();
            assertFalse(rs.next());
            // upsert row using conn1
            String upsertSql = "UPSERT INTO " + FULL_TABLE_NAME + "(varchar_pk, char_pk, int_pk, long_pk, decimal_pk, date_pk, a.int_col1) VALUES(?, ?, ?, ?, ?, ?, ?)";
            PreparedStatement stmt = conn1.prepareStatement(upsertSql);
            TestUtil.setRowKeyColumns(stmt, 1);
            stmt.setInt(7, 10);
            stmt.execute();
            // upsert row using conn2
            upsertSql = "UPSERT INTO " + FULL_TABLE_NAME + "(varchar_pk, char_pk, int_pk, long_pk, decimal_pk, date_pk, b.int_col2) VALUES(?, ?, ?, ?, ?, ?, ?)";
            stmt = conn2.prepareStatement(upsertSql);
            TestUtil.setRowKeyColumns(stmt, 1);
            stmt.setInt(7, 11);
            stmt.execute();
            
            conn1.commit();
            //second commit should fail
            try {
                conn2.commit();
                if (!immutableRows) fail();
            }   
            catch (SQLException e) {
                if (immutableRows) fail();
                assertEquals(e.getErrorCode(), SQLExceptionCode.TRANSACTION_CONFLICT_EXCEPTION.getErrorCode());
            }
        }
    }
    
    @Test
    public void testRowConflictDetected() throws Exception {
        testRowConflicts();
    }
    
    @Test
    public void testNoConflictDetectionForImmutableRows() throws Exception {
        Connection conn = DriverManager.getConnection(getUrl());
        conn.createStatement().execute("ALTER TABLE " + FULL_TABLE_NAME + " SET IMMUTABLE_ROWS=true");
        testRowConflicts();
    }
    
    @Test
    public void testNonTxToTxTable() throws Exception {
        Connection conn = DriverManager.getConnection(getUrl());
        conn.createStatement().execute("CREATE TABLE NON_TX_TABLE(k INTEGER PRIMARY KEY, v VARCHAR)");
        conn.createStatement().execute("UPSERT INTO NON_TX_TABLE VALUES (1)");
        conn.createStatement().execute("UPSERT INTO NON_TX_TABLE VALUES (2, 'a')");
        conn.createStatement().execute("UPSERT INTO NON_TX_TABLE VALUES (3, 'b')");
        conn.commit();
        
        conn.createStatement().execute("CREATE INDEX IDX ON NON_TX_TABLE(v)");
        // Reset empty column value to an empty value like it is pre-transactions
        HTableInterface htable = conn.unwrap(PhoenixConnection.class).getQueryServices().getTable(Bytes.toBytes("NON_TX_TABLE"));
        List<Put>puts = Lists.newArrayList(new Put(PInteger.INSTANCE.toBytes(1)), new Put(PInteger.INSTANCE.toBytes(2)), new Put(PInteger.INSTANCE.toBytes(3)));
        for (Put put : puts) {
            put.add(QueryConstants.DEFAULT_COLUMN_FAMILY_BYTES, QueryConstants.EMPTY_COLUMN_BYTES, ByteUtil.EMPTY_BYTE_ARRAY);
        }
        htable.put(puts);
        
        conn.createStatement().execute("ALTER TABLE NON_TX_TABLE SET TRANSACTIONAL=true");
        
        htable = conn.unwrap(PhoenixConnection.class).getQueryServices().getTable(Bytes.toBytes("NON_TX_TABLE"));
        assertTrue(htable.getTableDescriptor().getCoprocessors().contains(PhoenixTransactionalProcessor.class.getName()));
        htable = conn.unwrap(PhoenixConnection.class).getQueryServices().getTable(Bytes.toBytes("IDX"));
        assertTrue(htable.getTableDescriptor().getCoprocessors().contains(PhoenixTransactionalProcessor.class.getName()));

        conn.createStatement().execute("UPSERT INTO NON_TX_TABLE VALUES (4, 'c')");
        ResultSet rs = conn.createStatement().executeQuery("SELECT /*+ NO_INDEX */ k FROM NON_TX_TABLE WHERE v IS NULL");
        assertTrue(conn.unwrap(PhoenixConnection.class).getTable(new PTableKey(null, "NON_TX_TABLE")).isTransactional());
        assertTrue(rs.next());
        assertEquals(1,rs.getInt(1));
        assertFalse(rs.next());
        conn.commit();
        
        conn.createStatement().execute("UPSERT INTO NON_TX_TABLE VALUES (5, 'd')");
        rs = conn.createStatement().executeQuery("SELECT k FROM NON_TX_TABLE");
        assertTrue(conn.unwrap(PhoenixConnection.class).getTable(new PTableKey(null, "IDX")).isTransactional());
        assertTrue(rs.next());
        assertEquals(1,rs.getInt(1));
        assertTrue(rs.next());
        assertEquals(2,rs.getInt(1));
        assertTrue(rs.next());
        assertEquals(3,rs.getInt(1));
        assertTrue(rs.next());
        assertEquals(4,rs.getInt(1));
        assertTrue(rs.next());
        assertEquals(5,rs.getInt(1));
        assertFalse(rs.next());
        conn.rollback();
        
        rs = conn.createStatement().executeQuery("SELECT k FROM NON_TX_TABLE");
        assertTrue(rs.next());
        assertEquals(1,rs.getInt(1));
        assertTrue(rs.next());
        assertEquals(2,rs.getInt(1));
        assertTrue(rs.next());
        assertEquals(3,rs.getInt(1));
        assertTrue(rs.next());
        assertEquals(4,rs.getInt(1));
        assertFalse(rs.next());
    }
    
    @Ignore
    @Test
    public void testNonTxToTxTableFailure() throws Exception {
        Connection conn = DriverManager.getConnection(getUrl());
        // Put table in SYSTEM schema to prevent attempts to update the cache after we disable SYSTEM.CATALOG
        conn.createStatement().execute("CREATE TABLE SYSTEM.NON_TX_TABLE(k INTEGER PRIMARY KEY, v VARCHAR)");
        conn.createStatement().execute("UPSERT INTO SYSTEM.NON_TX_TABLE VALUES (1)");
        conn.commit();
        // Reset empty column value to an empty value like it is pre-transactions
        HTableInterface htable = conn.unwrap(PhoenixConnection.class).getQueryServices().getTable(Bytes.toBytes("SYSTEM.NON_TX_TABLE"));
        Put put = new Put(PInteger.INSTANCE.toBytes(1));
        put.add(QueryConstants.DEFAULT_COLUMN_FAMILY_BYTES, QueryConstants.EMPTY_COLUMN_BYTES, ByteUtil.EMPTY_BYTE_ARRAY);
        htable.put(put);
        
        HBaseAdmin admin = conn.unwrap(PhoenixConnection.class).getQueryServices().getAdmin();
        admin.disableTable(PhoenixDatabaseMetaData.SYSTEM_CATALOG_NAME);
        try {
            // This will succeed initially in updating the HBase metadata, but then will fail when
            // the SYSTEM.CATALOG table is attempted to be updated, exercising the code to restore
            // the coprocessors back to the non transactional ones.
            conn.createStatement().execute("ALTER TABLE SYSTEM.NON_TX_TABLE SET TRANSACTIONAL=true");
            fail();
        } catch (SQLException e) {
            assertTrue(e.getMessage().contains(PhoenixDatabaseMetaData.SYSTEM_CATALOG_NAME + " is disabled"));
        } finally {
            admin.enableTable(PhoenixDatabaseMetaData.SYSTEM_CATALOG_NAME);
            admin.close();
        }
        
        ResultSet rs = conn.createStatement().executeQuery("SELECT k FROM SYSTEM.NON_TX_TABLE WHERE v IS NULL");
        assertTrue(rs.next());
        assertEquals(1,rs.getInt(1));
        assertFalse(rs.next());
        
        htable = conn.unwrap(PhoenixConnection.class).getQueryServices().getTable(Bytes.toBytes("SYSTEM.NON_TX_TABLE"));
        assertFalse(htable.getTableDescriptor().getCoprocessors().contains(PhoenixTransactionalProcessor.class.getName()));
        assertEquals(1,conn.unwrap(PhoenixConnection.class).getQueryServices().
                getTableDescriptor(Bytes.toBytes("SYSTEM.NON_TX_TABLE")).
                getFamily(QueryConstants.DEFAULT_COLUMN_FAMILY_BYTES).getMaxVersions());
    }
    
    @Test
    public void testProperties() throws Exception {
        Connection conn = DriverManager.getConnection(getUrl());
        conn.createStatement().execute("CREATE TABLE NON_TX_TABLE1(k INTEGER PRIMARY KEY, a.v VARCHAR, b.v VARCHAR, c.v VARCHAR) TTL=1000");
        conn.createStatement().execute("CREATE INDEX idx1 ON NON_TX_TABLE1(a.v, b.v) TTL=1000");
        conn.createStatement().execute("CREATE INDEX idx2 ON NON_TX_TABLE1(c.v) INCLUDE (a.v, b.v) TTL=1000");

        conn.createStatement().execute("ALTER TABLE NON_TX_TABLE1 SET TRANSACTIONAL=true");

        HTableDescriptor desc = conn.unwrap(PhoenixConnection.class).getQueryServices().getTableDescriptor(Bytes.toBytes("NON_TX_TABLE1"));
        for (HColumnDescriptor colDesc : desc.getFamilies()) {
            assertEquals(QueryServicesOptions.DEFAULT_MAX_VERSIONS_TRANSACTIONAL, colDesc.getMaxVersions());
            assertEquals(1000, colDesc.getTimeToLive());
            assertEquals(1000, Integer.parseInt(colDesc.getValue(TxConstants.PROPERTY_TTL)));
        }

        desc = conn.unwrap(PhoenixConnection.class).getQueryServices().getTableDescriptor(Bytes.toBytes("IDX1"));
        for (HColumnDescriptor colDesc : desc.getFamilies()) {
            assertEquals(QueryServicesOptions.DEFAULT_MAX_VERSIONS_TRANSACTIONAL, colDesc.getMaxVersions());
            assertEquals(1000, colDesc.getTimeToLive());
            assertEquals(1000, Integer.parseInt(colDesc.getValue(TxConstants.PROPERTY_TTL)));
        }
        
        desc = conn.unwrap(PhoenixConnection.class).getQueryServices().getTableDescriptor(Bytes.toBytes("IDX2"));
        for (HColumnDescriptor colDesc : desc.getFamilies()) {
            assertEquals(QueryServicesOptions.DEFAULT_MAX_VERSIONS_TRANSACTIONAL, colDesc.getMaxVersions());
            assertEquals(1000, colDesc.getTimeToLive());
            assertEquals(1000, Integer.parseInt(colDesc.getValue(TxConstants.PROPERTY_TTL)));
        }
        
        conn.createStatement().execute("CREATE TABLE NON_TX_TABLE2(k INTEGER PRIMARY KEY, a.v VARCHAR, b.v VARCHAR, c.v VARCHAR)");
        conn.createStatement().execute("ALTER TABLE NON_TX_TABLE2 SET TRANSACTIONAL=true, VERSIONS=10");
        desc = conn.unwrap(PhoenixConnection.class).getQueryServices().getTableDescriptor(Bytes.toBytes("NON_TX_TABLE2"));
        for (HColumnDescriptor colDesc : desc.getFamilies()) {
            assertEquals(10, colDesc.getMaxVersions());
            assertEquals(HColumnDescriptor.DEFAULT_TTL, colDesc.getTimeToLive());
            assertEquals(null, colDesc.getValue(TxConstants.PROPERTY_TTL));
        }
        conn.createStatement().execute("ALTER TABLE NON_TX_TABLE2 SET TTL=1000");
        desc = conn.unwrap(PhoenixConnection.class).getQueryServices().getTableDescriptor(Bytes.toBytes("NON_TX_TABLE2"));
        for (HColumnDescriptor colDesc : desc.getFamilies()) {
            assertEquals(10, colDesc.getMaxVersions());
            assertEquals(1000, colDesc.getTimeToLive());
            assertEquals(1000, Integer.parseInt(colDesc.getValue(TxConstants.PROPERTY_TTL)));
        }

        conn.createStatement().execute("CREATE TABLE NON_TX_TABLE3(k INTEGER PRIMARY KEY, a.v VARCHAR, b.v VARCHAR, c.v VARCHAR)");
        conn.createStatement().execute("ALTER TABLE NON_TX_TABLE3 SET TRANSACTIONAL=true, b.VERSIONS=10, c.VERSIONS=20");
        desc = conn.unwrap(PhoenixConnection.class).getQueryServices().getTableDescriptor(Bytes.toBytes("NON_TX_TABLE3"));
        assertEquals(QueryServicesOptions.DEFAULT_MAX_VERSIONS_TRANSACTIONAL, desc.getFamily(Bytes.toBytes("A")).getMaxVersions());
        assertEquals(10, desc.getFamily(Bytes.toBytes("B")).getMaxVersions());
        assertEquals(20, desc.getFamily(Bytes.toBytes("C")).getMaxVersions());

        conn.createStatement().execute("CREATE TABLE NON_TX_TABLE4(k INTEGER PRIMARY KEY, a.v VARCHAR, b.v VARCHAR, c.v VARCHAR)");
        try {
            conn.createStatement().execute("ALTER TABLE NON_TX_TABLE4 SET TRANSACTIONAL=true, VERSIONS=1");
            fail();
        } catch (SQLException e) {
            assertEquals(SQLExceptionCode.TX_MAX_VERSIONS_MUST_BE_GREATER_THAN_ONE.getErrorCode(), e.getErrorCode());
        }

        try {
            conn.createStatement().execute("ALTER TABLE NON_TX_TABLE4 SET TRANSACTIONAL=true, b.VERSIONS=1");
            fail();
        } catch (SQLException e) {
            assertEquals(SQLExceptionCode.TX_MAX_VERSIONS_MUST_BE_GREATER_THAN_ONE.getErrorCode(), e.getErrorCode());
        }
        
        conn.createStatement().execute("CREATE TABLE TX_TABLE1(k INTEGER PRIMARY KEY, v VARCHAR) TTL=1000, TRANSACTIONAL=true");
        desc = conn.unwrap(PhoenixConnection.class).getQueryServices().getTableDescriptor(Bytes.toBytes("TX_TABLE1"));
        for (HColumnDescriptor colDesc : desc.getFamilies()) {
            assertEquals(QueryServicesOptions.DEFAULT_MAX_VERSIONS_TRANSACTIONAL, colDesc.getMaxVersions());
            assertEquals(HColumnDescriptor.DEFAULT_TTL, colDesc.getTimeToLive());
            assertEquals(1000, Integer.parseInt(colDesc.getValue(TxConstants.PROPERTY_TTL)));
        }
    }
    
    @Test
    public void testCreateTableToBeTransactional() throws Exception {
        Properties props = PropertiesUtil.deepCopy(TEST_PROPERTIES);
        Connection conn = DriverManager.getConnection(getUrl(), props);
        String ddl = "CREATE TABLE TEST_TRANSACTIONAL_TABLE (k varchar primary key) transactional=true";
        conn.createStatement().execute(ddl);
        PhoenixConnection pconn = conn.unwrap(PhoenixConnection.class);
        PTable table = pconn.getTable(new PTableKey(null, "TEST_TRANSACTIONAL_TABLE"));
        HTableInterface htable = pconn.getQueryServices().getTable(Bytes.toBytes("TEST_TRANSACTIONAL_TABLE"));
        assertTrue(table.isTransactional());
        assertTrue(htable.getTableDescriptor().getCoprocessors().contains(PhoenixTransactionalProcessor.class.getName()));
        
        try {
            ddl = "ALTER TABLE TEST_TRANSACTIONAL_TABLE SET transactional=false";
            conn.createStatement().execute(ddl);
            fail();
        } catch (SQLException e) {
            assertEquals(SQLExceptionCode.TX_MAY_NOT_SWITCH_TO_NON_TX.getErrorCode(), e.getErrorCode());
        }

        HBaseAdmin admin = pconn.getQueryServices().getAdmin();
        HTableDescriptor desc = new HTableDescriptor(TableName.valueOf("TXN_TEST_EXISTING"));
        desc.addFamily(new HColumnDescriptor(QueryConstants.DEFAULT_COLUMN_FAMILY_BYTES));
        admin.createTable(desc);
        ddl = "CREATE TABLE TXN_TEST_EXISTING (k varchar primary key) transactional=true";
        conn.createStatement().execute(ddl);
        assertEquals(Boolean.TRUE.toString(), admin.getTableDescriptor(TableName.valueOf("TXN_TEST_EXISTING")).getValue(TxConstants.READ_NON_TX_DATA));
        
        // Should be ok, as HBase metadata should match existing metadata.
        ddl = "CREATE TABLE IF NOT EXISTS TEST_TRANSACTIONAL_TABLE (k varchar primary key)"; 
        try {
            conn.createStatement().execute(ddl);
            fail();
        } catch (SQLException e) {
            assertEquals(SQLExceptionCode.TX_MAY_NOT_SWITCH_TO_NON_TX.getErrorCode(), e.getErrorCode());
        }
        ddl += " transactional=true";
        conn.createStatement().execute(ddl);
        table = pconn.getTable(new PTableKey(null, "TEST_TRANSACTIONAL_TABLE"));
        htable = pconn.getQueryServices().getTable(Bytes.toBytes("TEST_TRANSACTIONAL_TABLE"));
        assertTrue(table.isTransactional());
        assertTrue(htable.getTableDescriptor().getCoprocessors().contains(PhoenixTransactionalProcessor.class.getName()));
    }

    public void testCurrentDate() throws Exception {
        String selectSql = "SELECT current_date() FROM "+FULL_TABLE_NAME;
        try (Connection conn = DriverManager.getConnection(getUrl())) {
            conn.setAutoCommit(false);
            ResultSet rs = conn.createStatement().executeQuery(selectSql);
            assertFalse(rs.next());
            
            String upsert = "UPSERT INTO " + FULL_TABLE_NAME + "(varchar_pk, char_pk, int_pk, long_pk, decimal_pk, date_pk) VALUES(?, ?, ?, ?, ?, ?)";
            PreparedStatement stmt = conn.prepareStatement(upsert);
            // upsert two rows
            TestUtil.setRowKeyColumns(stmt, 1);
            stmt.execute();
            conn.commit();
            
            rs = conn.createStatement().executeQuery(selectSql);
            assertTrue(rs.next());
            Date date1 = rs.getDate(1);
            assertFalse(rs.next());
            
            Thread.sleep(1000);
            
            rs = conn.createStatement().executeQuery(selectSql);
            assertTrue(rs.next());
            Date date2 = rs.getDate(1);
            assertFalse(rs.next());
            assertTrue("current_date() should change while executing multiple statements", date2.getTime() > date1.getTime());
        }
    }
    
    @Test
    public void testReCreateTxnTableAfterDroppingExistingNonTxnTable() throws SQLException {
        Properties props = PropertiesUtil.deepCopy(TEST_PROPERTIES);
        Connection conn = DriverManager.getConnection(getUrl(), props);
        conn.setAutoCommit(false);
        Statement stmt = conn.createStatement();
        stmt.execute("CREATE TABLE DEMO(k VARCHAR PRIMARY KEY, v1 VARCHAR, v2 VARCHAR)");
        stmt.execute("DROP TABLE DEMO");
        stmt.execute("CREATE TABLE DEMO(k VARCHAR PRIMARY KEY, v1 VARCHAR, v2 VARCHAR) TRANSACTIONAL=true");
        stmt.execute("CREATE INDEX DEMO_IDX ON DEMO (v1) INCLUDE(v2)");
        assertTrue(conn.unwrap(PhoenixConnection.class).getTable(new PTableKey(null, "DEMO")).isTransactional());
        assertTrue(conn.unwrap(PhoenixConnection.class).getTable(new PTableKey(null, "DEMO_IDX")).isTransactional());
    }
    
    @Test
    public void testRowTimestampDisabled() throws SQLException {
        Properties props = PropertiesUtil.deepCopy(TEST_PROPERTIES);
        try (Connection conn = DriverManager.getConnection(getUrl(), props)) {
            conn.setAutoCommit(false);
            Statement stmt = conn.createStatement();
            try {
                stmt.execute("CREATE TABLE DEMO(k VARCHAR, v VARCHAR, d DATE NOT NULL, CONSTRAINT PK PRIMARY KEY(k,d ROW_TIMESTAMP)) TRANSACTIONAL=true");
                fail();
            }
            catch(SQLException e) {
                assertEquals(SQLExceptionCode.CANNOT_CREATE_TXN_TABLE_WITH_ROW_TIMESTAMP.getErrorCode(), e.getErrorCode());
            }
            stmt.execute("CREATE TABLE DEMO(k VARCHAR, v VARCHAR, d DATE NOT NULL, CONSTRAINT PK PRIMARY KEY(k,d ROW_TIMESTAMP))");
            try {
                stmt.execute("ALTER TABLE DEMO SET TRANSACTIONAL=true");
                fail();
            }
            catch(SQLException e) {
                assertEquals(SQLExceptionCode.CANNOT_ALTER_TO_BE_TXN_WITH_ROW_TIMESTAMP.getErrorCode(), e.getErrorCode());
            }
        }
    }
    
    @Test
    public void testExternalTxContext() throws Exception {
        ResultSet rs;
        Properties props = PropertiesUtil.deepCopy(TEST_PROPERTIES);
        Connection conn = DriverManager.getConnection(getUrl(), props);
        conn.setAutoCommit(false);
        PhoenixConnection pconn = conn.unwrap(PhoenixConnection.class);
        
        TransactionSystemClient txServiceClient = pconn.getQueryServices().getTransactionSystemClient();

        String fullTableName = "T";
        Statement stmt = conn.createStatement();
        stmt.execute("CREATE TABLE " + fullTableName + "(K VARCHAR PRIMARY KEY, V1 VARCHAR, V2 VARCHAR) TRANSACTIONAL=true");
        HTableInterface htable = pconn.getQueryServices().getTable(Bytes.toBytes(fullTableName));
        stmt.executeUpdate("upsert into " + fullTableName + " values('x', 'a', 'a')");
        conn.commit();

        try (Connection newConn = DriverManager.getConnection(getUrl(), props)) {
            rs = newConn.createStatement().executeQuery("select count(*) from " + fullTableName);
            assertTrue(rs.next());
            assertEquals(1,rs.getInt(1));
        }

        // Use HBase level Tephra APIs to start a new transaction
        TransactionAwareHTable txAware = new TransactionAwareHTable(htable, TxConstants.ConflictDetection.ROW);
        TransactionContext txContext = new TransactionContext(txServiceClient, txAware);
        txContext.start();
        
        // Use HBase APIs to add a new row
        Put put = new Put(Bytes.toBytes("z"));
        put.addColumn(QueryConstants.DEFAULT_COLUMN_FAMILY_BYTES, QueryConstants.EMPTY_COLUMN_BYTES, QueryConstants.EMPTY_COLUMN_VALUE_BYTES);
        put.addColumn(QueryConstants.DEFAULT_COLUMN_FAMILY_BYTES, Bytes.toBytes("V1"), Bytes.toBytes("b"));
        txAware.put(put);
        
        // Use Phoenix APIs to add new row (sharing the transaction context)
        pconn.setTransactionContext(txContext);
        conn.createStatement().executeUpdate("upsert into " + fullTableName + " values('y', 'c', 'c')");

        // New connection should not see data as it hasn't been committed yet
        try (Connection newConn = DriverManager.getConnection(getUrl(), props)) {
            rs = newConn.createStatement().executeQuery("select count(*) from " + fullTableName);
            assertTrue(rs.next());
            assertEquals(1,rs.getInt(1));
        }
        
        // Use new connection to create a row with a conflict
        Connection connWithConflict = DriverManager.getConnection(getUrl(), props);
        connWithConflict.createStatement().execute("upsert into " + fullTableName + " values('z', 'd', 'd')");
        
        // Existing connection should see data even though it hasn't been committed yet
        rs = conn.createStatement().executeQuery("select count(*) from " + fullTableName);
        assertTrue(rs.next());
        assertEquals(3,rs.getInt(1));
        
        // Use Tephra APIs directly to finish (i.e. commit) the transaction
        txContext.finish();
        
        // Confirm that attempt to commit row with conflict fails
        try {
            connWithConflict.commit();
            fail();
        } catch (SQLException e) {
            assertEquals(SQLExceptionCode.TRANSACTION_CONFLICT_EXCEPTION.getErrorCode(), e.getErrorCode());
        } finally {
            connWithConflict.close();
        }
        
        // New connection should now see data as it has been committed
        try (Connection newConn = DriverManager.getConnection(getUrl(), props)) {
            rs = newConn.createStatement().executeQuery("select count(*) from " + fullTableName);
            assertTrue(rs.next());
            assertEquals(3,rs.getInt(1));
        }
        
        // Repeat the same as above, but this time abort the transaction
        txContext = new TransactionContext(txServiceClient, txAware);
        txContext.start();
        
        // Use HBase APIs to add a new row
        put = new Put(Bytes.toBytes("j"));
        put.addColumn(QueryConstants.DEFAULT_COLUMN_FAMILY_BYTES, QueryConstants.EMPTY_COLUMN_BYTES, QueryConstants.EMPTY_COLUMN_VALUE_BYTES);
        put.addColumn(QueryConstants.DEFAULT_COLUMN_FAMILY_BYTES, Bytes.toBytes("V1"), Bytes.toBytes("e"));
        txAware.put(put);
        
        // Use Phoenix APIs to add new row (sharing the transaction context)
        pconn.setTransactionContext(txContext);
        conn.createStatement().executeUpdate("upsert into " + fullTableName + " values('k', 'f', 'f')");
        
        // Existing connection should see data even though it hasn't been committed yet
        rs = conn.createStatement().executeQuery("select count(*) from " + fullTableName);
        assertTrue(rs.next());
        assertEquals(5,rs.getInt(1));

        connWithConflict.createStatement().execute("upsert into " + fullTableName + " values('k', 'g', 'g')");
        rs = connWithConflict.createStatement().executeQuery("select count(*) from " + fullTableName);
        assertTrue(rs.next());
        assertEquals(4,rs.getInt(1));

        // Use Tephra APIs directly to abort (i.e. rollback) the transaction
        txContext.abort();
        
        rs = conn.createStatement().executeQuery("select count(*) from " + fullTableName);
        assertTrue(rs.next());
        assertEquals(3,rs.getInt(1));

        // Should succeed since conflicting row was aborted
        connWithConflict.commit();

        // New connection should now see data as it has been committed
        try (Connection newConn = DriverManager.getConnection(getUrl(), props)) {
            rs = newConn.createStatement().executeQuery("select count(*) from " + fullTableName);
            assertTrue(rs.next());
            assertEquals(4,rs.getInt(1));
        }
        
        // Even using HBase APIs directly, we shouldn't find 'j' since a delete marker would have been
        // written to hide it.
        Result result = htable.get(new Get(Bytes.toBytes("j")));
        assertTrue(result.isEmpty());
    }

    @Test
    public void testCheckpointAndRollback() throws Exception {
        Properties props = PropertiesUtil.deepCopy(TEST_PROPERTIES);
        Connection conn = DriverManager.getConnection(getUrl(), props);
        conn.setAutoCommit(false);
        try {
            String fullTableName = "T";
            Statement stmt = conn.createStatement();
            stmt.execute("CREATE TABLE " + fullTableName + "(k VARCHAR PRIMARY KEY, v1 VARCHAR, v2 VARCHAR) TRANSACTIONAL=true");
            stmt.executeUpdate("upsert into " + fullTableName + " values('x', 'a', 'a')");
            conn.commit();
            
            stmt.executeUpdate("upsert into " + fullTableName + "(k,v1) SELECT k,v1||'a' FROM " + fullTableName);
            ResultSet rs = stmt.executeQuery("select k, v1, v2 from " + fullTableName);
            assertTrue(rs.next());
            assertEquals("x", rs.getString(1));
            assertEquals("aa", rs.getString(2));
            assertEquals("a", rs.getString(3));
            assertFalse(rs.next());
            
            stmt.executeUpdate("upsert into " + fullTableName + "(k,v1) SELECT k,v1||'a' FROM " + fullTableName);
            
            rs = stmt.executeQuery("select k, v1, v2 from " + fullTableName);
            assertTrue(rs.next());
            assertEquals("x", rs.getString(1));
            assertEquals("aaa", rs.getString(2));
            assertEquals("a", rs.getString(3));
            assertFalse(rs.next());
            
            conn.rollback();
            
            //assert original row exists in fullTableName1
            rs = stmt.executeQuery("select k, v1, v2 from " + fullTableName);
            assertTrue(rs.next());
            assertEquals("x", rs.getString(1));
            assertEquals("a", rs.getString(2));
            assertEquals("a", rs.getString(3));
            assertFalse(rs.next());
            
        } finally {
            conn.close();
        }
    }
    
    @Test
    public void testInflightUpdateNotSeen() throws Exception {
        String selectSQL = "SELECT * FROM " + FULL_TABLE_NAME;
        try (Connection conn1 = DriverManager.getConnection(getUrl()); 
                Connection conn2 = DriverManager.getConnection(getUrl())) {
            conn1.setAutoCommit(false);
            conn2.setAutoCommit(true);
            ResultSet rs = conn1.createStatement().executeQuery(selectSQL);
            assertFalse(rs.next());
            
            String upsert = "UPSERT INTO " + FULL_TABLE_NAME + "(varchar_pk, char_pk, int_pk, long_pk, decimal_pk, date_pk) VALUES(?, ?, ?, ?, ?, ?)";
            PreparedStatement stmt = conn1.prepareStatement(upsert);
            // upsert two rows
            TestUtil.setRowKeyColumns(stmt, 1);
            stmt.execute();
            conn1.commit();
            
            TestUtil.setRowKeyColumns(stmt, 2);
            stmt.execute();
            
            rs = conn1.createStatement().executeQuery("SELECT count(*) FROM " + FULL_TABLE_NAME + " WHERE int_col1 IS NULL");
            assertTrue(rs.next());
            assertEquals(2, rs.getInt(1));
            
            upsert = "UPSERT INTO " + FULL_TABLE_NAME + "(varchar_pk, char_pk, int_pk, long_pk, decimal_pk, date_pk, int_col1) VALUES(?, ?, ?, ?, ?, ?, 1)";
            stmt = conn1.prepareStatement(upsert);
            TestUtil.setRowKeyColumns(stmt, 1);
            stmt.execute();
            
            rs = conn1.createStatement().executeQuery("SELECT int_col1 FROM " + FULL_TABLE_NAME + " WHERE int_col1 = 1");
            assertTrue(rs.next());
            assertEquals(1, rs.getInt(1));
            assertFalse(rs.next());
            
            rs = conn2.createStatement().executeQuery("SELECT count(*) FROM " + FULL_TABLE_NAME + " WHERE int_col1 = 1");
            assertTrue(rs.next());
            assertEquals(0, rs.getInt(1));
            rs = conn2.createStatement().executeQuery("SELECT * FROM " + FULL_TABLE_NAME + " WHERE int_col1 = 1");
            assertFalse(rs.next());
            
            conn1.commit();
            
            rs = conn2.createStatement().executeQuery("SELECT count(*) FROM " + FULL_TABLE_NAME + " WHERE int_col1 = 1");
            assertTrue(rs.next());
            assertEquals(1, rs.getInt(1));
            rs = conn2.createStatement().executeQuery("SELECT * FROM " + FULL_TABLE_NAME + " WHERE int_col1 = 1");
            assertTrue(rs.next());
            assertFalse(rs.next());
        }
    }
    
    @Test
    public void testInflightDeleteNotSeen() throws Exception {
        String selectSQL = "SELECT * FROM " + FULL_TABLE_NAME;
        try (Connection conn1 = DriverManager.getConnection(getUrl()); 
                Connection conn2 = DriverManager.getConnection(getUrl())) {
            conn1.setAutoCommit(false);
            conn2.setAutoCommit(true);
            ResultSet rs = conn1.createStatement().executeQuery(selectSQL);
            assertFalse(rs.next());
            
            String upsert = "UPSERT INTO " + FULL_TABLE_NAME + "(varchar_pk, char_pk, int_pk, long_pk, decimal_pk, date_pk) VALUES(?, ?, ?, ?, ?, ?)";
            PreparedStatement stmt = conn1.prepareStatement(upsert);
            // upsert two rows
            TestUtil.setRowKeyColumns(stmt, 1);
            stmt.execute();
            TestUtil.setRowKeyColumns(stmt, 2);
            stmt.execute();
            
            conn1.commit();
            
            rs = conn1.createStatement().executeQuery("SELECT count(*) FROM " + FULL_TABLE_NAME);
            assertTrue(rs.next());
            assertEquals(2, rs.getInt(1));
            
            String delete = "DELETE FROM " + FULL_TABLE_NAME + " WHERE varchar_pk = 'varchar1'";
            stmt = conn1.prepareStatement(delete);
            int count = stmt.executeUpdate();
            assertEquals(1,count);
            
            rs = conn1.createStatement().executeQuery("SELECT count(*) FROM " + FULL_TABLE_NAME);
            assertTrue(rs.next());
            assertEquals(1, rs.getInt(1));
            assertFalse(rs.next());
            
            rs = conn2.createStatement().executeQuery("SELECT count(*) FROM " + FULL_TABLE_NAME);
            assertTrue(rs.next());
            assertEquals(2, rs.getInt(1));
            assertFalse(rs.next());
            
            conn1.commit();
            
            rs = conn2.createStatement().executeQuery("SELECT count(*) FROM " + FULL_TABLE_NAME);
            assertTrue(rs.next());
            assertEquals(1, rs.getInt(1));
            assertFalse(rs.next());
        }
    }

    @Test
    public void testParallelUpsertSelect() throws Exception {
        Properties props = PropertiesUtil.deepCopy(TEST_PROPERTIES);
        props.setProperty(QueryServices.MUTATE_BATCH_SIZE_ATTRIB, Integer.toString(3));
        props.setProperty(QueryServices.SCAN_CACHE_SIZE_ATTRIB, Integer.toString(3));
        props.setProperty(QueryServices.SCAN_RESULT_CHUNK_SIZE, Integer.toString(3));
        Connection conn = DriverManager.getConnection(getUrl(), props);
        conn.setAutoCommit(false);
        conn.createStatement().execute("CREATE SEQUENCE S1");
        conn.createStatement().execute("CREATE TABLE SALTEDT1 (pk INTEGER PRIMARY KEY, val INTEGER) SALT_BUCKETS=4,TRANSACTIONAL=true");
        conn.createStatement().execute("CREATE TABLE T2 (pk INTEGER PRIMARY KEY, val INTEGER) TRANSACTIONAL=true");

        for (int i = 0; i < 100; i++) {
            conn.createStatement().execute("UPSERT INTO SALTEDT1 VALUES (NEXT VALUE FOR S1, " + (i%10) + ")");
        }
        conn.commit();
        conn.setAutoCommit(true);
        int upsertCount = conn.createStatement().executeUpdate("UPSERT INTO T2 SELECT pk, val FROM SALTEDT1");
        assertEquals(100,upsertCount);
        conn.close();
    }
}
