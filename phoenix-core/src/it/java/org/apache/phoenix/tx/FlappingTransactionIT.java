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
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Arrays;
import java.util.Collection;
import java.util.Properties;

import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.phoenix.end2end.ParallelStatsDisabledIT;
import org.apache.phoenix.end2end.ParallelStatsDisabledTest;
import org.apache.phoenix.exception.SQLExceptionCode;
import org.apache.phoenix.jdbc.PhoenixConnection;
import org.apache.phoenix.query.QueryConstants;
import org.apache.phoenix.transaction.PhoenixTransactionContext;
import org.apache.phoenix.transaction.PhoenixTransactionProvider;
import org.apache.phoenix.transaction.TransactionFactory;
import org.apache.phoenix.util.PropertiesUtil;
import org.apache.phoenix.util.TestUtil;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;

/**
 * 
 * Transaction related tests that flap when run in parallel.
 * TODO: review with Tephra community
 *
 */
@Category(ParallelStatsDisabledTest.class)
@RunWith(Parameterized.class)
public class FlappingTransactionIT extends ParallelStatsDisabledIT {
    private final String txProvider;
    
    public FlappingTransactionIT(String provider) {
        txProvider = provider;
    }

    @Parameters(name="FlappingTransactionIT_transactionProvider={0}") // name is used by failsafe as file name in reports
    public static synchronized Collection<Object[]> data() {
        return TestUtil.filterTxParamData(Arrays.asList(new Object[][] {
            {"TEPHRA"}, {"OMID"}
           }),0);
    }
    
    @Test
    public void testDelete() throws Exception {
        String transTableName = generateUniqueName();
        String fullTableName = INDEX_DATA_SCHEMA + QueryConstants.NAME_SEPARATOR + transTableName;
        String selectSQL = "SELECT * FROM " + fullTableName;
        try (Connection conn = DriverManager.getConnection(getUrl());
                Connection conn1 = DriverManager.getConnection(getUrl()); 
                Connection conn2 = DriverManager.getConnection(getUrl())) {
            TestUtil.createTransactionalTable(conn, fullTableName, "TRANSACTION_PROVIDER='"+txProvider+"'");
            conn1.setAutoCommit(false);
            ResultSet rs = conn1.createStatement().executeQuery(selectSQL);
            assertFalse(rs.next());
            
            String upsert = "UPSERT INTO " + fullTableName + "(varchar_pk, char_pk, int_pk, long_pk, decimal_pk, date_pk) VALUES(?, ?, ?, ?, ?, ?)";
            PreparedStatement stmt = conn1.prepareStatement(upsert);
            // upsert two rows
            TestUtil.setRowKeyColumns(stmt, 1);
            stmt.execute();
            conn1.commit();
            
            TestUtil.setRowKeyColumns(stmt, 2);
            stmt.execute();
            
            // verify rows can be read even though commit has not been called
            int rowsDeleted = conn1.createStatement().executeUpdate("DELETE FROM " + fullTableName);
            assertEquals(2, rowsDeleted);
            
            // Delete and second upsert not committed yet, so there should be one row.
            rs = conn2.createStatement().executeQuery("SELECT count(*) FROM " + fullTableName);
            assertTrue(rs.next());
            assertEquals(1, rs.getInt(1));
            
            conn1.commit();
            
            // verify rows are deleted after commit
            rs = conn1.createStatement().executeQuery(selectSQL);
            assertFalse(rs.next());
        }
    }
        
    @Test
    public void testInflightUpdateNotSeen() throws Exception {
        String transTableName = generateUniqueName();
        String fullTableName = INDEX_DATA_SCHEMA + QueryConstants.NAME_SEPARATOR + transTableName;
        String selectSQL = "SELECT * FROM " + fullTableName;
        try (Connection conn = DriverManager.getConnection(getUrl());
                Connection conn1 = DriverManager.getConnection(getUrl()); 
                Connection conn2 = DriverManager.getConnection(getUrl())) {
            TestUtil.createTransactionalTable(conn, fullTableName, "TRANSACTION_PROVIDER='"+txProvider+"'");
            conn1.setAutoCommit(false);
            conn2.setAutoCommit(true);
            ResultSet rs = conn1.createStatement().executeQuery(selectSQL);
            assertFalse(rs.next());
            
            String upsert = "UPSERT INTO " + fullTableName + "(varchar_pk, char_pk, int_pk, long_pk, decimal_pk, date_pk) VALUES(?, ?, ?, ?, ?, ?)";
            PreparedStatement stmt = conn1.prepareStatement(upsert);
            // upsert two rows
            TestUtil.setRowKeyColumns(stmt, 1);
            stmt.execute();
            conn1.commit();
            
            TestUtil.setRowKeyColumns(stmt, 2);
            stmt.execute();
            
            rs = conn1.createStatement().executeQuery("SELECT count(*) FROM " + fullTableName + " WHERE int_col1 IS NULL");
            assertTrue(rs.next());
            assertEquals(2, rs.getInt(1));
            
            upsert = "UPSERT INTO " + fullTableName + "(varchar_pk, char_pk, int_pk, long_pk, decimal_pk, date_pk, int_col1) VALUES(?, ?, ?, ?, ?, ?, 1)";
            stmt = conn1.prepareStatement(upsert);
            TestUtil.setRowKeyColumns(stmt, 1);
            stmt.execute();
            
            rs = conn1.createStatement().executeQuery("SELECT int_col1 FROM " + fullTableName + " WHERE int_col1 = 1");
            assertTrue(rs.next());
            assertEquals(1, rs.getInt(1));
            assertFalse(rs.next());
            
            rs = conn2.createStatement().executeQuery("SELECT count(*) FROM " + fullTableName + " WHERE int_col1 = 1");
            assertTrue(rs.next());
            assertEquals(0, rs.getInt(1));
            // this succeeds rs = conn2.createStatement().executeQuery("SELECT int_col1 FROM " + fullTableName + " WHERE int_col1 = 1");
            // this fails rs = conn2.createStatement().executeQuery("SELECT * FROM " + fullTableName + " WHERE int_col1 = 1");
            rs = conn2.createStatement().executeQuery("SELECT * FROM " + fullTableName + " WHERE int_col1 = 1");
            boolean hasNext = rs.next();
            assertFalse(hasNext);
            
            conn1.commit();
            
            rs = conn2.createStatement().executeQuery("SELECT count(*) FROM " + fullTableName + " WHERE int_col1 = 1");
            assertTrue(rs.next());
            assertEquals(1, rs.getInt(1));
            rs = conn2.createStatement().executeQuery("SELECT * FROM " + fullTableName + " WHERE int_col1 = 1");
            assertTrue(rs.next());
            assertFalse(rs.next());
        }
    }
    
    @Test
    public void testInflightDeleteNotSeen() throws Exception {
        String transTableName = generateUniqueName();
        String fullTableName = INDEX_DATA_SCHEMA + QueryConstants.NAME_SEPARATOR + transTableName;
        String selectSQL = "SELECT * FROM " + fullTableName;
        try (Connection conn = DriverManager.getConnection(getUrl());
                Connection conn1 = DriverManager.getConnection(getUrl()); 
                Connection conn2 = DriverManager.getConnection(getUrl())) {
            TestUtil.createTransactionalTable(conn, fullTableName, "TRANSACTION_PROVIDER='"+txProvider+"'");
            conn1.setAutoCommit(false);
            conn2.setAutoCommit(true);
            ResultSet rs = conn1.createStatement().executeQuery(selectSQL);
            assertFalse(rs.next());
            
            String upsert = "UPSERT INTO " + fullTableName + "(varchar_pk, char_pk, int_pk, long_pk, decimal_pk, date_pk) VALUES(?, ?, ?, ?, ?, ?)";
            PreparedStatement stmt = conn1.prepareStatement(upsert);
            // upsert two rows
            TestUtil.setRowKeyColumns(stmt, 1);
            stmt.execute();
            TestUtil.setRowKeyColumns(stmt, 2);
            stmt.execute();
            
            conn1.commit();
            
            rs = conn1.createStatement().executeQuery("SELECT count(*) FROM " + fullTableName);
            assertTrue(rs.next());
            assertEquals(2, rs.getInt(1));
            
            String delete = "DELETE FROM " + fullTableName + " WHERE varchar_pk = 'varchar1'";
            stmt = conn1.prepareStatement(delete);
            int count = stmt.executeUpdate();
            assertEquals(1,count);
            
            rs = conn1.createStatement().executeQuery("SELECT count(*) FROM " + fullTableName);
            assertTrue(rs.next());
            assertEquals(1, rs.getInt(1));
            assertFalse(rs.next());
            
            rs = conn2.createStatement().executeQuery("SELECT count(*) FROM " + fullTableName);
            assertTrue(rs.next());
            assertEquals(2, rs.getInt(1));
            assertFalse(rs.next());
            
            conn1.commit();
            
            rs = conn2.createStatement().executeQuery("SELECT count(*) FROM " + fullTableName);
            assertTrue(rs.next());
            assertEquals(1, rs.getInt(1));
            assertFalse(rs.next());
        }
    }

    @Test
    public void testExternalTxContext() throws Exception {
        ResultSet rs;
        Properties props = PropertiesUtil.deepCopy(TEST_PROPERTIES);
        Connection conn = DriverManager.getConnection(getUrl(), props);
        conn.setAutoCommit(false);
        String fullTableName = generateUniqueName();
        PhoenixConnection pconn = conn.unwrap(PhoenixConnection.class);
        
        Statement stmt = conn.createStatement();
        stmt.execute("CREATE TABLE " + fullTableName + "(K VARCHAR PRIMARY KEY, V1 VARCHAR, V2 VARCHAR) TRANSACTIONAL=true,TRANSACTION_PROVIDER='"+txProvider+"'");
        Table htable = pconn.getQueryServices().getTable(Bytes.toBytes(fullTableName));
        stmt.executeUpdate("upsert into " + fullTableName + " values('x', 'a', 'a')");
        conn.commit();

        try (Connection newConn = DriverManager.getConnection(getUrl(), props)) {
            rs = newConn.createStatement().executeQuery("select count(*) from " + fullTableName);
            assertTrue(rs.next());
            assertEquals(1,rs.getInt(1));
        }

        PhoenixTransactionProvider provider = TransactionFactory.Provider.valueOf(txProvider).getTransactionProvider();
        PhoenixTransactionContext txContext = provider.getTransactionContext(pconn);
        // FIXME: must begin txn before getting transactional table
        // Either set txn on all existing OmidTransactionTable or throw exception
        // when attempting to get OmidTransactionTable if a txn is not in progress.
        txContext.begin(); 
        Table txTable = txContext.getTransactionalTable(htable, false);

        // Use HBase APIs to add a new row
        Put put = new Put(Bytes.toBytes("z"));
        put.addColumn(QueryConstants.DEFAULT_COLUMN_FAMILY_BYTES, QueryConstants.EMPTY_COLUMN_BYTES, QueryConstants.EMPTY_COLUMN_VALUE_BYTES);
        put.addColumn(QueryConstants.DEFAULT_COLUMN_FAMILY_BYTES, Bytes.toBytes("V1"), Bytes.toBytes("b"));
        txTable.put(put);
        
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
        
        // Use TM APIs directly to finish (i.e. commit) the transaction
        txContext.commit();
        
        // Confirm that attempt to commit row with conflict fails
        try {
            connWithConflict.commit();
            fail();
        } catch (SQLException e) {
            assertEquals(SQLExceptionCode.TRANSACTION_CONFLICT_EXCEPTION.getErrorCode(), e.getErrorCode());
        }
        
        // New connection should now see data as it has been committed
        try (Connection newConn = DriverManager.getConnection(getUrl(), props)) {
            rs = newConn.createStatement().executeQuery("select count(*) from " + fullTableName);
            assertTrue(rs.next());
            assertEquals(3,rs.getInt(1));
        }
        
        // Repeat the same as above, but this time abort the transaction
        txContext = provider.getTransactionContext(pconn);
        txContext.begin();
        txTable = txContext.getTransactionalTable(htable, false);

        
        // Use HBase APIs to add a new row
        put = new Put(Bytes.toBytes("j"));
        put.addColumn(QueryConstants.DEFAULT_COLUMN_FAMILY_BYTES, QueryConstants.EMPTY_COLUMN_BYTES, QueryConstants.EMPTY_COLUMN_VALUE_BYTES);
        put.addColumn(QueryConstants.DEFAULT_COLUMN_FAMILY_BYTES, Bytes.toBytes("V1"), Bytes.toBytes("e"));
        txTable.put(put);
        
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

        // Use TM APIs directly to abort (i.e. rollback) the transaction
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

}
