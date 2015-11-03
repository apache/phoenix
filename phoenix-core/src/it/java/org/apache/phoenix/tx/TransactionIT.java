/*
 * Licensed to the Apache Software Foundation (ASF) under one or more contributor license agreements. See the NOTICE
 * file distributed with this work for additional information regarding copyright ownership. The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with the
 * License. You may obtain a copy of the License at http://www.apache.org/licenses/LICENSE-2.0 Unless required by
 * applicable law or agreed to in writing, software distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the specific language
 * governing permissions and limitations under the License.
 */
package org.apache.phoenix.tx;

import static org.apache.phoenix.util.TestUtil.INDEX_DATA_SCHEMA;
import static org.apache.phoenix.util.TestUtil.TRANSACTIONAL_DATA_TABLE;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.List;

import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTableInterface;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.phoenix.end2end.BaseHBaseManagedTimeIT;
import org.apache.phoenix.exception.SQLExceptionCode;
import org.apache.phoenix.jdbc.PhoenixConnection;
import org.apache.phoenix.jdbc.PhoenixDatabaseMetaData;
import org.apache.phoenix.query.QueryConstants;
import org.apache.phoenix.query.QueryServicesOptions;
import org.apache.phoenix.schema.PTableKey;
import org.apache.phoenix.schema.types.PInteger;
import org.apache.phoenix.util.ByteUtil;
import org.apache.phoenix.util.TestUtil;
import org.junit.Before;
import org.junit.Test;

import com.google.common.collect.Lists;

import co.cask.tephra.hbase98.coprocessor.TransactionProcessor;

public class TransactionIT extends BaseHBaseManagedTimeIT {
	
	private static final String FULL_TABLE_NAME = INDEX_DATA_SCHEMA + QueryConstants.NAME_SEPARATOR + TRANSACTIONAL_DATA_TABLE;
	
    @Before
    public void setUp() throws SQLException {
        ensureTableCreated(getUrl(), TRANSACTIONAL_DATA_TABLE);
    }
		
	@Test
	public void testReadOwnWrites() throws Exception {
		String selectSql = "SELECT * FROM "+FULL_TABLE_NAME;
		Connection conn = DriverManager.getConnection(getUrl());
		try {
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
        finally {
        	conn.close();
        }
	}
	
    @Test
    public void testDelete() throws Exception {
        String selectSQL = "SELECT * FROM " + FULL_TABLE_NAME;
        Connection conn1 = DriverManager.getConnection(getUrl());
        Connection conn2 = DriverManager.getConnection(getUrl());
        try {
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
        finally {
            conn1.close();
        }
    }
    
	@Test
	public void testAutoCommitQuerySingleTable() throws Exception {
		Connection conn = DriverManager.getConnection(getUrl());
		try {
			conn.setAutoCommit(true);
			// verify no rows returned
			ResultSet rs = conn.createStatement().executeQuery("SELECT * FROM " + FULL_TABLE_NAME);
			assertFalse(rs.next());
		} finally {
			conn.close();
		}
	}
	
    @Test
    public void testAutoCommitQueryMultiTables() throws Exception {
        Connection conn = DriverManager.getConnection(getUrl());
        try {
            conn.setAutoCommit(true);
            // verify no rows returned
            ResultSet rs = conn.createStatement().executeQuery("SELECT * FROM " + FULL_TABLE_NAME + " a JOIN " + FULL_TABLE_NAME + " b ON (a.long_pk = b.int_pk)");
            assertFalse(rs.next());
        } finally {
            conn.close();
        }
    }
    
	@Test
	public void testColConflicts() throws Exception {
		Connection conn1 = DriverManager.getConnection(getUrl());
		Connection conn2 = DriverManager.getConnection(getUrl());
		try {
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
        finally {
        	conn1.close();
        }
	}
	
	private void testRowConflicts() throws Exception {
		Connection conn1 = DriverManager.getConnection(getUrl());
		Connection conn2 = DriverManager.getConnection(getUrl());
		try {
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
        finally {
        	conn1.close();
        	conn2.close();
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
    public void testMaxVersions() throws Exception {
        Connection conn = DriverManager.getConnection(getUrl());
        conn.createStatement().execute("CREATE TABLE NON_TX_TABLE1(k INTEGER PRIMARY KEY, a.v VARCHAR, b.v VARCHAR, c.v VARCHAR)");
        conn.createStatement().execute("CREATE INDEX idx1 ON NON_TX_TABLE1(a.v, b.v)");
        conn.createStatement().execute("CREATE INDEX idx2 ON NON_TX_TABLE1(c.v) INCLUDE (a.v, b.v)");

        conn.createStatement().execute("ALTER TABLE NON_TX_TABLE1 SET TRANSACTIONAL=true");

        HTableDescriptor desc = conn.unwrap(PhoenixConnection.class).getQueryServices().getTableDescriptor(Bytes.toBytes("NON_TX_TABLE1"));
        for (HColumnDescriptor colDesc : desc.getFamilies()) {
            assertEquals(QueryServicesOptions.DEFAULT_MAX_VERSIONS_TRANSACTIONAL, colDesc.getMaxVersions());
        }

        desc = conn.unwrap(PhoenixConnection.class).getQueryServices().getTableDescriptor(Bytes.toBytes("IDX1"));
        for (HColumnDescriptor colDesc : desc.getFamilies()) {
            assertEquals(QueryServicesOptions.DEFAULT_MAX_VERSIONS_TRANSACTIONAL, colDesc.getMaxVersions());
        }
        
        desc = conn.unwrap(PhoenixConnection.class).getQueryServices().getTableDescriptor(Bytes.toBytes("IDX2"));
        for (HColumnDescriptor colDesc : desc.getFamilies()) {
            assertEquals(QueryServicesOptions.DEFAULT_MAX_VERSIONS_TRANSACTIONAL, colDesc.getMaxVersions());
        }
        
        conn.createStatement().execute("CREATE TABLE NON_TX_TABLE2(k INTEGER PRIMARY KEY, a.v VARCHAR, b.v VARCHAR, c.v VARCHAR)");
        conn.createStatement().execute("ALTER TABLE NON_TX_TABLE2 SET TRANSACTIONAL=true, VERSIONS=10");
        desc = conn.unwrap(PhoenixConnection.class).getQueryServices().getTableDescriptor(Bytes.toBytes("NON_TX_TABLE2"));
        for (HColumnDescriptor colDesc : desc.getFamilies()) {
            assertEquals(10, colDesc.getMaxVersions());
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
    }
        

}
