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
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTableInterface;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.util.Bytes;
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
import org.junit.Test;

import co.cask.tephra.TxConstants;
import co.cask.tephra.hbase11.coprocessor.TransactionProcessor;

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
        assertTrue(htable.getTableDescriptor().getCoprocessors().contains(TransactionProcessor.class.getName()));
        htable = conn.unwrap(PhoenixConnection.class).getQueryServices().getTable(Bytes.toBytes("IDX"));
        assertTrue(htable.getTableDescriptor().getCoprocessors().contains(TransactionProcessor.class.getName()));

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
        assertFalse(htable.getTableDescriptor().getCoprocessors().contains(TransactionProcessor.class.getName()));
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
        assertTrue(htable.getTableDescriptor().getCoprocessors().contains(TransactionProcessor.class.getName()));
        
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
        assertTrue(htable.getTableDescriptor().getCoprocessors().contains(TransactionProcessor.class.getName()));
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
}
