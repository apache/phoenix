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
import java.sql.Date;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Properties;

import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.ColumnFamilyDescriptorBuilder;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.client.TableDescriptor;
import org.apache.hadoop.hbase.client.TableDescriptorBuilder;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.phoenix.coprocessor.TephraTransactionalProcessor;
import org.apache.phoenix.end2end.ParallelStatsDisabledIT;
import org.apache.phoenix.end2end.ParallelStatsDisabledTest;
import org.apache.phoenix.exception.SQLExceptionCode;
import org.apache.phoenix.jdbc.PhoenixConnection;
import org.apache.phoenix.jdbc.PhoenixDatabaseMetaData;
import org.apache.phoenix.query.QueryConstants;
import org.apache.phoenix.query.QueryServices;
import org.apache.phoenix.schema.PTable;
import org.apache.phoenix.schema.PTableImpl;
import org.apache.phoenix.schema.PTableKey;
import org.apache.phoenix.schema.types.PInteger;
import org.apache.phoenix.transaction.PhoenixTransactionContext;
import org.apache.phoenix.transaction.PhoenixTransactionProvider;
import org.apache.phoenix.transaction.PhoenixTransactionProvider.Feature;
import org.apache.phoenix.transaction.TransactionFactory;
import org.apache.phoenix.util.ByteUtil;
import org.apache.phoenix.util.PropertiesUtil;
import org.apache.phoenix.util.TestUtil;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;

import org.apache.phoenix.thirdparty.com.google.common.collect.Lists;

@Category(ParallelStatsDisabledTest.class)
@RunWith(Parameterized.class)
public class ParameterizedTransactionIT extends ParallelStatsDisabledIT {
    
    private final String tableDDLOptions;
    private final String tableDDLOptionsWithoutProvider;
    private final PhoenixTransactionProvider transactionProvider;

    public ParameterizedTransactionIT(Boolean mutable, Boolean columnEncoded, String transactionProvider) {
        StringBuilder optionBuilder = new StringBuilder();
        optionBuilder.append("TRANSACTION_PROVIDER='"+transactionProvider+"',");
        this.transactionProvider = TransactionFactory.Provider.valueOf(transactionProvider).getTransactionProvider();
        StringBuilder optionBuilder2 = new StringBuilder();
        if (!columnEncoded) {
            optionBuilder2.append("COLUMN_ENCODED_BYTES=0,");
        }
        if (!mutable) {
            optionBuilder2.append("IMMUTABLE_ROWS=true,");
            if (!columnEncoded) {
                optionBuilder2.append("IMMUTABLE_STORAGE_SCHEME="+PTableImpl.ImmutableStorageScheme.ONE_CELL_PER_COLUMN +",");
            }
        }
        if (optionBuilder2.length() > 0) {
            optionBuilder2.setLength(optionBuilder2.length()-1);
            optionBuilder.append(optionBuilder2);
        } else {
            optionBuilder.setLength(optionBuilder.length()-1);
        }
        this.tableDDLOptions = optionBuilder.toString();
        this.tableDDLOptionsWithoutProvider = optionBuilder2.toString();
    }
    
    @Parameters(name="ParameterizedTransactionIT_mutable={0},columnEncoded={1},transactionProvider={2}") // name is used by failsafe as file name in reports
    public static synchronized Collection<Object[]> data() {
        return TestUtil.filterTxParamData(Arrays.asList(new Object[][] {     
                 {false, false, "TEPHRA" }, {false, true, "TEPHRA" }, {true, false, "TEPHRA" }, { true, true, "TEPHRA" },
                 {false, false, "OMID" }, {true, false, "OMID" },
           }), 2);
    }
    
    @Test
    public void testReadOwnWrites() throws Exception {
        String transTableName = generateUniqueName();
        String fullTableName = INDEX_DATA_SCHEMA + QueryConstants.NAME_SEPARATOR + transTableName;
        String selectSql = "SELECT * FROM "+ fullTableName;
        try (Connection conn = DriverManager.getConnection(getUrl())) {
            conn.createStatement().execute("create table " + fullTableName + TestUtil.TEST_TABLE_SCHEMA + tableDDLOptions + (tableDDLOptions.length() > 0 ? "," : "") + "TRANSACTIONAL=true");
            conn.setAutoCommit(false);
            ResultSet rs = conn.createStatement().executeQuery(selectSql);
            assertFalse(rs.next());
            
            String upsert = "UPSERT INTO " + fullTableName + "(varchar_pk, char_pk, int_pk, long_pk, decimal_pk, date_pk) VALUES(?, ?, ?, ?, ?, ?)";
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
    
    // @Test - disabled, doesn't test anything new. TODO: Fix or remove.
    public void testTxnClosedCorrecty() throws Exception {
        String transTableName = generateUniqueName();
        String fullTableName = INDEX_DATA_SCHEMA + QueryConstants.NAME_SEPARATOR + transTableName;
        String selectSql = "SELECT * FROM "+fullTableName;
        try (Connection conn = DriverManager.getConnection(getUrl())) {
            String ddl = "create table " + fullTableName + TestUtil.TEST_TABLE_SCHEMA + tableDDLOptions + (tableDDLOptions.length() > 0 ? "," : "") + "TRANSACTIONAL=true";
            conn.createStatement().execute(ddl);
            conn.setAutoCommit(false);
            ResultSet rs = conn.createStatement().executeQuery(selectSql);
            assertFalse(rs.next());
            
            String upsert = "UPSERT INTO " + fullTableName + "(varchar_pk, char_pk, int_pk, long_pk, decimal_pk, date_pk) VALUES(?, ?, ?, ?, ?, ?)";
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
            // Long currentTx = rs.unwrap(PhoenixResultSet.class).getCurrentRow().getValue(0).getTimestamp();
            assertFalse(rs.next());
            
            conn.close();
            // start new connection
            // conn.createStatement().executeQuery(selectSql);
            // assertFalse("This transaction should not be on the invalid transactions",
            // txManager.getCurrentState().getInvalid().contains(currentTx));
        }
    }
    
    @Test
    public void testAutoCommitQuery() throws Exception {
        String transTableName = generateUniqueName();
        String fullTableName = INDEX_DATA_SCHEMA + QueryConstants.NAME_SEPARATOR + transTableName;
        try (Connection conn = DriverManager.getConnection(getUrl())) {
            conn.createStatement().execute("create table " + fullTableName + TestUtil.TEST_TABLE_SCHEMA + tableDDLOptions + (tableDDLOptions.length() > 0 ? "," : "") + "TRANSACTIONAL=true");
            conn.setAutoCommit(true);
            // verify no rows returned with single table
            ResultSet rs = conn.createStatement().executeQuery("SELECT * FROM " + fullTableName);
            assertFalse(rs.next());

            // verify no rows returned with multiple tables
            rs = conn.createStatement().executeQuery("SELECT * FROM " + fullTableName + " x JOIN " + fullTableName + " y ON (x.long_pk = y.int_pk)");
            assertFalse(rs.next());
        } 
    }
    
    @Test
    public void testSelfJoin() throws Exception {
        String t1 = generateUniqueName();
        String t2 = generateUniqueName();
        try (Connection conn = DriverManager.getConnection(getUrl())) {
            conn.createStatement().execute("create table " + t1 + " (varchar_pk VARCHAR NOT NULL primary key, a.varchar_col1 VARCHAR, b.varchar_col2 VARCHAR)" + tableDDLOptions + (tableDDLOptions.length() > 0 ? "," : "") + "TRANSACTIONAL=true");
            conn.createStatement().execute("create table " + t2 + " (varchar_pk VARCHAR NOT NULL primary key, a.varchar_col1 VARCHAR, b.varchar_col1 VARCHAR)" + tableDDLOptions + (tableDDLOptions.length() > 0 ? "," : "") + "TRANSACTIONAL=true");
            // verify no rows returned
            ResultSet rs = conn.createStatement().executeQuery("SELECT * FROM " + t1 + " x JOIN " + t1 + " y ON (x.varchar_pk = y.a.varchar_col1)");
            assertFalse(rs.next());
            rs = conn.createStatement().executeQuery("SELECT * FROM " + t2 + " x JOIN " + t2 + " y ON (x.varchar_pk = y.a.varchar_col1)");
            assertFalse(rs.next());
        } 
    }
    
    private void testRowConflicts(String fullTableName) throws Exception {
        try (Connection conn1 = DriverManager.getConnection(getUrl());
                Connection conn2 = DriverManager.getConnection(getUrl())) {
            conn1.setAutoCommit(false);
            conn2.setAutoCommit(false);
            String selectSql = "SELECT * FROM "+fullTableName;
            conn1.setAutoCommit(false);
            ResultSet rs = conn1.createStatement().executeQuery(selectSql);
            boolean immutableRows = conn1.unwrap(PhoenixConnection.class).getTable(new PTableKey(null, fullTableName)).isImmutableRows();
            assertFalse(rs.next());
            // upsert row using conn1
            String upsertSql = "UPSERT INTO " + fullTableName + "(varchar_pk, char_pk, int_pk, long_pk, decimal_pk, date_pk, a.int_col1) VALUES(?, ?, ?, ?, ?, ?, ?)";
            PreparedStatement stmt = conn1.prepareStatement(upsertSql);
            TestUtil.setRowKeyColumns(stmt, 1);
            stmt.setInt(7, 10);
            stmt.execute();
            // upsert row using conn2
            upsertSql = "UPSERT INTO " + fullTableName + "(varchar_pk, char_pk, int_pk, long_pk, decimal_pk, date_pk, b.int_col2) VALUES(?, ?, ?, ?, ?, ?, ?)";
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
        String transTableName = generateUniqueName();
        String fullTableName = INDEX_DATA_SCHEMA + QueryConstants.NAME_SEPARATOR + transTableName;
        Connection conn = DriverManager.getConnection(getUrl());
        conn.createStatement().execute("create table " + fullTableName + TestUtil.TEST_TABLE_SCHEMA + tableDDLOptions + (tableDDLOptions.length() > 0 ? "," : "") + "TRANSACTIONAL=true");
        testRowConflicts(fullTableName);
    }
    
    @Test
    public void testNoConflictDetectionForImmutableRows() throws Exception {
        if (tableDDLOptions.contains("IMMUTABLE_ROWS=true")) {
            // only need to test this for immutable rows
            String transTableName = generateUniqueName();
            String fullTableName = INDEX_DATA_SCHEMA + QueryConstants.NAME_SEPARATOR + transTableName;
            Connection conn = DriverManager.getConnection(getUrl());
            conn.createStatement().execute("create table " + fullTableName + TestUtil.TEST_TABLE_SCHEMA + tableDDLOptions + (tableDDLOptions.length() > 0 ? "," : "") + "TRANSACTIONAL=true");
            testRowConflicts(fullTableName);
        }
    }
    
    @Test
    public void testNonTxToTxTable() throws Exception {
        String nonTxTableName = generateUniqueName();

        Connection conn = DriverManager.getConnection(getUrl());
        conn.createStatement().execute("CREATE TABLE " + nonTxTableName + "(k INTEGER PRIMARY KEY, v VARCHAR)" + tableDDLOptionsWithoutProvider);
        conn.createStatement().execute("UPSERT INTO " + nonTxTableName + " VALUES (1)");
        conn.createStatement().execute("UPSERT INTO " + nonTxTableName + " VALUES (2, 'a')");
        conn.createStatement().execute("UPSERT INTO " + nonTxTableName + " VALUES (3, 'b')");
        conn.commit();
        
        String index = generateUniqueName();
        conn.createStatement().execute("CREATE INDEX " + index + " ON " + nonTxTableName + "(v)");
        // Reset empty column value to an empty value like it is pre-transactions
        Table htable = conn.unwrap(PhoenixConnection.class).getQueryServices().getTable(Bytes.toBytes( nonTxTableName));
        List<Put>puts = Lists.newArrayList(new Put(PInteger.INSTANCE.toBytes(1)), new Put(PInteger.INSTANCE.toBytes(2)), new Put(PInteger.INSTANCE.toBytes(3)));
        for (Put put : puts) {
            put.addColumn(QueryConstants.DEFAULT_COLUMN_FAMILY_BYTES, QueryConstants.EMPTY_COLUMN_BYTES, ByteUtil.EMPTY_BYTE_ARRAY);
        }
        htable.put(puts);
        
        try {
            conn.createStatement().execute("ALTER TABLE " + nonTxTableName + " SET TRANSACTIONAL=true,TRANSACTION_PROVIDER='" + transactionProvider + "'");
            if (transactionProvider.isUnsupported(Feature.ALTER_NONTX_TO_TX)) {
                fail();
            }
        } catch (SQLException e) {
            if (transactionProvider.isUnsupported(Feature.ALTER_NONTX_TO_TX)) {
                assertEquals(SQLExceptionCode.CANNOT_ALTER_TABLE_FROM_NON_TXN_TO_TXNL.getErrorCode(), e.getErrorCode());
                return;
            } else {
                throw e;
            }
        }
        
        htable = conn.unwrap(PhoenixConnection.class).getQueryServices().getTable(Bytes.toBytes( nonTxTableName));
        assertTrue(htable.getDescriptor().getCoprocessors().contains(TephraTransactionalProcessor.class.getName()));
        htable = conn.unwrap(PhoenixConnection.class).getQueryServices().getTable(Bytes.toBytes(index));
        assertTrue(htable.getDescriptor().getCoprocessors().contains(TephraTransactionalProcessor.class.getName()));

        conn.createStatement().execute("UPSERT INTO " + nonTxTableName + " VALUES (4, 'c')");
        ResultSet rs = conn.createStatement().executeQuery("SELECT /*+ NO_INDEX */ k FROM " + nonTxTableName + " WHERE v IS NULL");
        assertTrue(conn.unwrap(PhoenixConnection.class).getTable(new PTableKey(null,  nonTxTableName)).isTransactional());
        assertTrue(rs.next());
        assertEquals(1,rs.getInt(1));
        assertFalse(rs.next());
        conn.commit();
        
        conn.createStatement().execute("UPSERT INTO " + nonTxTableName + " VALUES (5, 'd')");
        rs = conn.createStatement().executeQuery("SELECT k FROM " + nonTxTableName);
        assertTrue(conn.unwrap(PhoenixConnection.class).getTable(new PTableKey(null, index)).isTransactional());
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
        
        rs = conn.createStatement().executeQuery("SELECT k FROM " + nonTxTableName);
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
        if (tableDDLOptions.contains("COLUMN_ENCODED_BYTES")) {
            // no need to test this with all variations of column encoding
            return;
        }

        String nonTxTableName = generateUniqueName();

        Connection conn = DriverManager.getConnection(getUrl());
        // Put table in SYSTEM schema to prevent attempts to update the cache after we disable SYSTEM.CATALOG
        conn.createStatement().execute("CREATE TABLE \"SYSTEM\"." + nonTxTableName + "(k INTEGER PRIMARY KEY, v VARCHAR)" + tableDDLOptionsWithoutProvider);
        conn.createStatement().execute("UPSERT INTO \"SYSTEM\"." + nonTxTableName + " VALUES (1)");
        conn.commit();
        // Reset empty column value to an empty value like it is pre-transactions
        Table htable = conn.unwrap(PhoenixConnection.class).getQueryServices().getTable(Bytes.toBytes("SYSTEM." + nonTxTableName));
        Put put = new Put(PInteger.INSTANCE.toBytes(1));
        put.addColumn(QueryConstants.DEFAULT_COLUMN_FAMILY_BYTES, QueryConstants.EMPTY_COLUMN_BYTES, ByteUtil.EMPTY_BYTE_ARRAY);
        htable.put(put);
        
        Admin admin = conn.unwrap(PhoenixConnection.class).getQueryServices().getAdmin();
        admin.disableTable(TableName.valueOf(PhoenixDatabaseMetaData.SYSTEM_CATALOG_NAME));
        try {
            // This will succeed initially in updating the HBase metadata, but then will fail when
            // the SYSTEM.CATALOG table is attempted to be updated, exercising the code to restore
            // the coprocessors back to the non transactional ones.
            conn.createStatement().execute("ALTER TABLE \"SYSTEM\"." + nonTxTableName + " SET TRANSACTIONAL=true,TRANSACTION_PROVIDER='" + transactionProvider + "'");
            fail();
        } catch (SQLException e) {
            if (transactionProvider.isUnsupported(Feature.ALTER_NONTX_TO_TX)) {
                assertEquals(SQLExceptionCode.CANNOT_ALTER_TABLE_FROM_NON_TXN_TO_TXNL.getErrorCode(), e.getErrorCode());
            } else {
                assertTrue(e.getMessage().contains(PhoenixDatabaseMetaData.SYSTEM_CATALOG_NAME + " is disabled"));
            }
        } finally {
            admin.enableTable(TableName.valueOf(PhoenixDatabaseMetaData.SYSTEM_CATALOG_NAME));
            admin.close();
        }
        
        ResultSet rs = conn.createStatement().executeQuery("SELECT k FROM \"SYSTEM\"." + nonTxTableName + " WHERE v IS NULL");
        assertTrue(rs.next());
        assertEquals(1,rs.getInt(1));
        assertFalse(rs.next());
        
        htable = conn.unwrap(PhoenixConnection.class).getQueryServices().getTable(Bytes.toBytes("SYSTEM." + nonTxTableName));
        assertFalse(htable.getDescriptor().getCoprocessors().contains(transactionProvider.getCoprocessorClassName()));
        assertEquals(1,conn.unwrap(PhoenixConnection.class).getQueryServices().
                getTableDescriptor(Bytes.toBytes("SYSTEM." + nonTxTableName)).
                getColumnFamily(QueryConstants.DEFAULT_COLUMN_FAMILY_BYTES).getMaxVersions());
    }
    
    @Test
    public void testCreateTableToBeTransactional() throws Exception {
        if (tableDDLOptions.contains("COLUMN_ENCODED_BYTES")) {
            // no need to test this with all variations of column encoding
            return;
        }

        Properties props = PropertiesUtil.deepCopy(TEST_PROPERTIES);
        Connection conn = DriverManager.getConnection(getUrl(), props);
        String t1 = generateUniqueName();
        String t2 = generateUniqueName();
        String ddl = "CREATE TABLE " + t1 + " (k varchar primary key)" + tableDDLOptions + (tableDDLOptions.length() > 0 ? "," : "") + "TRANSACTIONAL=true";
        conn.createStatement().execute(ddl);
        PhoenixConnection pconn = conn.unwrap(PhoenixConnection.class);
        PTable table = pconn.getTable(new PTableKey(null, t1));
        Table htable = pconn.getQueryServices().getTable(Bytes.toBytes(t1));
        assertTrue(table.isTransactional());
        assertTrue(htable.getDescriptor().getCoprocessors().contains(transactionProvider.getCoprocessorClassName()));
        
        try {
            ddl = "ALTER TABLE " + t1 + " SET transactional=false";
            conn.createStatement().execute(ddl);
            fail();
        } catch (SQLException e) {
            assertEquals(SQLExceptionCode.TX_MAY_NOT_SWITCH_TO_NON_TX.getErrorCode(), e.getErrorCode());
        }

        Admin admin = pconn.getQueryServices().getAdmin();
        admin.createTable(TableDescriptorBuilder.newBuilder(TableName.valueOf(t2))
                .addColumnFamily(ColumnFamilyDescriptorBuilder.of(QueryConstants.DEFAULT_COLUMN_FAMILY_BYTES)).build());
        try {
            ddl = "CREATE TABLE " + t2 + " (k varchar primary key) transactional=true,transaction_provider='" + transactionProvider + "'";
            conn.createStatement().execute(ddl);
            if (transactionProvider.isUnsupported(Feature.ALTER_NONTX_TO_TX)) {
                fail();
            }
        } catch (SQLException e) {
            if (transactionProvider.isUnsupported(Feature.ALTER_NONTX_TO_TX)) {
                assertEquals(SQLExceptionCode.CANNOT_ALTER_TABLE_FROM_NON_TXN_TO_TXNL.getErrorCode(), e.getErrorCode());
                return;
            }
            throw e;
        }

        TableDescriptor tableDescriptor = admin.getDescriptor(TableName.valueOf(t2));
        String str = tableDescriptor.getValue(PhoenixTransactionContext.READ_NON_TX_DATA);
        assertEquals(Boolean.TRUE.toString(), str);
        
        // Should be ok, as HBase metadata should match existing metadata.
        ddl = "CREATE TABLE IF NOT EXISTS " + t1 + " (k varchar primary key)"; 
        try {
            conn.createStatement().execute(ddl);
            fail();
        } catch (SQLException e) {
            assertEquals(SQLExceptionCode.TX_MAY_NOT_SWITCH_TO_NON_TX.getErrorCode(), e.getErrorCode());
        }
        ddl += " transactional=true,transaction_provider='" + transactionProvider + "'";
        conn.createStatement().execute(ddl);
        table = pconn.getTable(new PTableKey(null, t1));
        htable = pconn.getQueryServices().getTable(Bytes.toBytes(t1));
        assertTrue(table.isTransactional());
        assertTrue(htable.getDescriptor().getCoprocessors().contains(transactionProvider.getCoprocessorClassName()));
    }

    @Test
    public void testCurrentDate() throws Exception {
        String transTableName = generateUniqueName();
        String fullTableName = INDEX_DATA_SCHEMA + QueryConstants.NAME_SEPARATOR + transTableName;
        String selectSql = "SELECT current_date() FROM "+fullTableName;
        try (Connection conn = DriverManager.getConnection(getUrl())) {
            conn.createStatement().execute("create table " + fullTableName + TestUtil.TEST_TABLE_SCHEMA + tableDDLOptions + (tableDDLOptions.length() > 0 ? "," : "") + "TRANSACTIONAL=true");
            conn.setAutoCommit(false);
            ResultSet rs = conn.createStatement().executeQuery(selectSql);
            assertFalse(rs.next());
            
            String upsert = "UPSERT INTO " + fullTableName + "(varchar_pk, char_pk, int_pk, long_pk, decimal_pk, date_pk) VALUES(?, ?, ?, ?, ?, ?)";
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
    public void testParallelUpsertSelect() throws Exception {
        Properties props = PropertiesUtil.deepCopy(TEST_PROPERTIES);
        props.setProperty(QueryServices.MUTATE_BATCH_SIZE_BYTES_ATTRIB, Integer.toString(512));
        props.setProperty(QueryServices.SCAN_CACHE_SIZE_ATTRIB, Integer.toString(3));
        props.setProperty(QueryServices.SCAN_RESULT_CHUNK_SIZE, Integer.toString(3));
        Connection conn = DriverManager.getConnection(getUrl(), props);
        conn.setAutoCommit(false);
        String fullTableName1 = generateUniqueName();
        String fullTableName2 = generateUniqueName();
        String sequenceName = "S_" + generateUniqueName();
        conn.createStatement().execute("CREATE SEQUENCE " + sequenceName);
        conn.createStatement().execute("CREATE TABLE " + fullTableName1 + " (pk INTEGER PRIMARY KEY, val INTEGER) SALT_BUCKETS=4, TRANSACTIONAL=true"
                + (tableDDLOptions.length() > 0 ? "," : "")  + tableDDLOptions);
        conn.createStatement().execute("CREATE TABLE " + fullTableName2 + " (pk INTEGER PRIMARY KEY, val INTEGER)" );

        for (int i = 0; i < 100; i++) {
            conn.createStatement().execute("UPSERT INTO " + fullTableName1 + " VALUES (NEXT VALUE FOR " + sequenceName + ", " + (i%10) + ")");
        }
        conn.commit();
        conn.setAutoCommit(true);
        int upsertCount = conn.createStatement().executeUpdate("UPSERT INTO " + fullTableName2 + " SELECT pk, val FROM " + fullTableName1);
        assertEquals(100,upsertCount);
        conn.close();
    }

    @Test
    public void testInflightPartialEval() throws SQLException {

        try (Connection conn = DriverManager.getConnection(getUrl())) {
            String transactTableName = generateUniqueName();
            Statement stmt = conn.createStatement();
            stmt.execute("CREATE TABLE " + transactTableName + " (k VARCHAR PRIMARY KEY, v1 VARCHAR, v2 VARCHAR) " + tableDDLOptions + (tableDDLOptions.length() > 0 ? "," : "") + "TRANSACTIONAL=true");

            
            try (Connection conn1 = DriverManager.getConnection(getUrl()); Connection conn2 = DriverManager.getConnection(getUrl())) {
                conn1.createStatement().execute("UPSERT INTO " + transactTableName + " VALUES ('a','b','x')");
                // Select to force uncommitted data to be written
                ResultSet rs = conn1.createStatement().executeQuery("SELECT * FROM " + transactTableName);
                assertTrue(rs.next());
                assertEquals("a", rs.getString(1));
                assertEquals("b", rs.getString(2));
                assertFalse(rs.next());
                
                conn2.createStatement().execute("UPSERT INTO " + transactTableName + " VALUES ('a','c','x')");
                // Select to force uncommitted data to be written
                rs = conn2.createStatement().executeQuery("SELECT * FROM " + transactTableName );
                assertTrue(rs.next());
                assertEquals("a", rs.getString(1));
                assertEquals("c", rs.getString(2));
                assertFalse(rs.next());
                
                // If the AndExpression were to see the uncommitted row from conn2, the filter would
                // filter the row out early and no longer continue to evaluate other cells due to
                // the way partial evaluation holds state.
                rs = conn1.createStatement().executeQuery("SELECT * FROM " +  transactTableName + " WHERE v1 != 'c' AND v2 = 'x'");
                assertTrue(rs.next());
                assertEquals("a", rs.getString(1));
                assertEquals("b", rs.getString(2));
                assertFalse(rs.next());
                
                // Same as above for conn1 data
                rs = conn2.createStatement().executeQuery("SELECT * FROM " + transactTableName + " WHERE v1 != 'b' AND v2 = 'x'");
                assertTrue(rs.next());
                assertEquals("a", rs.getString(1));
                assertEquals("c", rs.getString(2));
                assertFalse(rs.next());
            }

        }
    }
    
}
