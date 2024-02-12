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
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.IOException;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Arrays;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.hadoop.hbase.DoNotRetryIOException;
import org.apache.hadoop.hbase.HBaseIOException;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.TableNotFoundException;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.ColumnFamilyDescriptor;
import org.apache.hadoop.hbase.client.ColumnFamilyDescriptorBuilder;
import org.apache.hadoop.hbase.client.Mutation;
import org.apache.hadoop.hbase.client.TableDescriptor;
import org.apache.hadoop.hbase.coprocessor.ObserverContext;
import org.apache.hadoop.hbase.coprocessor.RegionCoprocessorEnvironment;
import org.apache.hadoop.hbase.coprocessor.SimpleRegionObserver;
import org.apache.hadoop.hbase.regionserver.MiniBatchOperationInProgress;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.phoenix.end2end.ParallelStatsDisabledIT;
import org.apache.phoenix.end2end.ParallelStatsDisabledTest;
import org.apache.phoenix.exception.SQLExceptionCode;
import org.apache.phoenix.jdbc.PhoenixConnection;
import org.apache.phoenix.jdbc.PhoenixDatabaseMetaData;
import org.apache.phoenix.query.QueryConstants;
import org.apache.phoenix.query.QueryServices;
import org.apache.phoenix.query.QueryServicesOptions;
import org.apache.phoenix.schema.PTable;
import org.apache.phoenix.schema.PTableKey;
import org.apache.phoenix.transaction.PhoenixTransactionContext;
import org.apache.phoenix.transaction.PhoenixTransactionProvider;
import org.apache.phoenix.transaction.PhoenixTransactionProvider.Feature;
import org.apache.phoenix.transaction.TransactionFactory;
import org.apache.phoenix.util.EnvironmentEdgeManager;
import org.apache.phoenix.util.PhoenixRuntime;
import org.apache.phoenix.util.PropertiesUtil;
import org.apache.phoenix.util.StringUtil;
import org.apache.phoenix.util.TestUtil;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;

import org.apache.phoenix.thirdparty.com.google.common.collect.Lists;

@Category(ParallelStatsDisabledTest.class)
@RunWith(Parameterized.class)
public class TransactionIT  extends ParallelStatsDisabledIT {
    private final String txProvider;
    private final String tableDDLOptions;
    
    public TransactionIT(String provider) {
        txProvider = provider;
        tableDDLOptions = PhoenixDatabaseMetaData.TRANSACTION_PROVIDER + "='" + provider + "'";
    }

    // name is used by failsafe as file name in reports
    @Parameters(name="TransactionIT_provider={0}")
    public static synchronized Collection<Object[]> data() {
        return Arrays.asList(new Object[][] { { "OMID" } });
    }

    @Test
    public void testFailureToRollbackAfterDelete() throws Exception {
        Properties props = PropertiesUtil.deepCopy(TEST_PROPERTIES);
        try (Connection conn = DriverManager.getConnection(getUrl(), props)) {
            String tableName = generateUniqueName();
            conn.createStatement().execute(
                    "CREATE TABLE " + tableName + " (k VARCHAR NOT NULL PRIMARY KEY) TRANSACTIONAL=true,TRANSACTION_PROVIDER='" + txProvider + "'");
            conn.createStatement().execute("UPSERT INTO " + tableName + " VALUES('a')");
            conn.createStatement().execute("UPSERT INTO " + tableName + " VALUES('b')");
            conn.commit();
            // Delete a in new transaction
            conn.createStatement().execute("DELETE FROM " + tableName + " WHERE k='a'");
            // Forces data to be written to HBase, but not yet committed
            conn.createStatement().executeQuery("SELECT * FROM " + tableName).next();
            // Upsert another row so commit below will fail the write (and fail subsequent attempt t o abort)
            conn.createStatement().execute("UPSERT INTO " + tableName + " VALUES('c')");
            TestUtil.addCoprocessor(conn, tableName, WriteFailingRegionObserver.class);
            try {
                conn.commit();
                fail();
            } catch (SQLException e) {
            }
            // Delete of a shouldn't be visible since commit failed, so all rows should be present
            ResultSet rs = conn.createStatement().executeQuery("SELECT * FROM " + tableName);
            assertTrue(rs.next());
            assertEquals("a", rs.getString(1));
            assertTrue(rs.next());
            assertEquals("b", rs.getString(1));
            assertFalse(rs.next());
        }
    }
    
    public static class WriteFailingRegionObserver extends SimpleRegionObserver {
        @Override
        public void preBatchMutate(ObserverContext<RegionCoprocessorEnvironment> c, MiniBatchOperationInProgress<Mutation> miniBatchOp) throws HBaseIOException {
            throw new DoNotRetryIOException();
        }
    }
    
    @Test
    public void testUpsertSelectDoesntSeeUpsertedData() throws Exception {
        Properties props = PropertiesUtil.deepCopy(TEST_PROPERTIES);
        props.setProperty(QueryServices.MUTATE_BATCH_SIZE_BYTES_ATTRIB, Integer.toString(512));
        props.setProperty(QueryServices.SCAN_CACHE_SIZE_ATTRIB, Integer.toString(3));
        props.setProperty(QueryServices.SCAN_RESULT_CHUNK_SIZE, Integer.toString(3));
        try (Connection conn = DriverManager.getConnection(getUrl(), props);
             Connection otherConn = DriverManager.getConnection(getUrl());
             Admin admin = driver
                        .getConnectionQueryServices(getUrl(), TestUtil.TEST_PROPERTIES)
                        .getAdmin();) {
            conn.setAutoCommit(true);
            otherConn.setAutoCommit(true);
            String tableName = generateUniqueName();
            conn.createStatement().execute("CREATE SEQUENCE " + tableName + "_seq CACHE 1000");
            conn.createStatement().execute("CREATE TABLE " + tableName
                    + " (pk INTEGER PRIMARY KEY, val INTEGER) UPDATE_CACHE_FREQUENCY=3600000, TRANSACTIONAL=true,"
                    + "TRANSACTION_PROVIDER='" + txProvider + "'");

            conn.createStatement().executeUpdate("UPSERT INTO " + tableName + " VALUES (NEXT VALUE FOR "
                    + tableName + "_seq,1)");
            PreparedStatement stmt = conn.prepareStatement("UPSERT INTO " + tableName
                    + " SELECT NEXT VALUE FOR " + tableName + "_seq, val FROM " + tableName);
            PreparedStatement query = otherConn
                    .prepareStatement("SELECT COUNT(*) FROM " + tableName);
            for (int i = 0; i < 12; i++) {
                try {
                    admin.split(TableName.valueOf(tableName));
                } catch (IOException ignore) {
                    // we don't care if the split sometime cannot be executed
                }
                int upsertCount = stmt.executeUpdate();
                assertEquals((int) Math.pow(2, i), upsertCount);
                ResultSet rs = query.executeQuery();
                assertTrue(rs.next());
                assertEquals((int) Math.pow(2, i + 1), rs.getLong(1));
                rs.close();
            }
        }
    }

    @Test
    public void testWithMixOfTxProviders() throws Exception {
        // No sense in running the test with every providers, so just run it with the default one
        if (!TransactionFactory.Provider.valueOf(txProvider).equals(TransactionFactory.Provider.getDefault())) {
            return;
        }
        List<String> tableNames = Lists.newArrayList();
        Properties props = PropertiesUtil.deepCopy(TEST_PROPERTIES);
        try (Connection conn = DriverManager.getConnection(getUrl(), props)) {
            for (TransactionFactory.Provider provider : TransactionFactory.Provider.available()) {
              String tableName = generateUniqueName();
              tableNames.add(tableName);
              conn.createStatement().execute(
                  "CREATE TABLE " + tableName + " (k VARCHAR NOT NULL PRIMARY KEY, v1 VARCHAR) TRANSACTIONAL=true,TRANSACTION_PROVIDER='" + provider + "'");
            }
            if (tableNames.size() < 2) {
                return;
            }
            Iterator<String> iterator = tableNames.iterator();
            String tableName1 = iterator.next();
            conn.createStatement().execute("UPSERT INTO " + tableName1 + " VALUES('a')");
            String tableName2 = iterator.next();
            try {
                conn.createStatement().execute("UPSERT INTO " + tableName2 + " VALUES('a')");
                fail();
            } catch (SQLException e) {
                assertEquals(SQLExceptionCode.CANNOT_MIX_TXN_PROVIDERS.getErrorCode(), e.getErrorCode());
            }
            
            conn.rollback();
            conn.setAutoCommit(true);
            for (String tableName : tableNames) {
                conn.createStatement().execute("UPSERT INTO " + tableName + " VALUES('a')");
            }
            for (String tableName : tableNames) {
                ResultSet rs = conn.createStatement().executeQuery("SELECT * FROM " + tableName);
                assertTrue(rs.next());
                assertEquals("a", rs.getString(1));
                assertFalse(rs.next());
            }
        }
    }
    
    @Test
    public void testPreventLocalIndexCreation() throws Exception {
        Properties props = PropertiesUtil.deepCopy(TEST_PROPERTIES);
        try (Connection conn = DriverManager.getConnection(getUrl(), props)) {
            for (TransactionFactory.Provider provider : TransactionFactory.Provider.available()) {
                if (provider.getTransactionProvider().isUnsupported(PhoenixTransactionProvider.Feature.ALLOW_LOCAL_INDEX)) {
                    String tableName = generateUniqueName();
                    conn.createStatement().execute(
                            "CREATE TABLE " + tableName + " (k VARCHAR NOT NULL PRIMARY KEY, v1 VARCHAR) TRANSACTIONAL=true,TRANSACTION_PROVIDER='" + provider + "'");
                    String indexName = generateUniqueName();
                    try {
                        conn.createStatement().execute("CREATE LOCAL INDEX " + indexName + "_IDX ON " + tableName + " (v1) INCLUDE(v2)");
                        fail();
                    } catch (SQLException e) {
                        assertEquals(SQLExceptionCode.CANNOT_CREATE_LOCAL_INDEX_FOR_TXN_TABLE.getErrorCode(), e.getErrorCode());
                    }
                }
            }
        }
    }

    @Test
    public void testQueryWithSCN() throws Exception {
        String tableName = generateUniqueName();
        Properties props = PropertiesUtil.deepCopy(TEST_PROPERTIES);
        try (Connection conn = DriverManager.getConnection(getUrl(), props);) {
            conn.createStatement().execute(
                    "CREATE TABLE " + tableName + " (k VARCHAR NOT NULL PRIMARY KEY, v1 VARCHAR) TRANSACTIONAL=true," + tableDDLOptions);
        }
        props.put(PhoenixRuntime.CURRENT_SCN_ATTRIB, Long.toString(EnvironmentEdgeManager.currentTimeMillis()));
        try (Connection conn = DriverManager.getConnection(getUrl(), props);) {
            try {
            	conn.createStatement().executeQuery("SELECT * FROM " + tableName);
                fail();
            } catch (SQLException e) {
                assertEquals("Unexpected Exception",
                        SQLExceptionCode.CANNOT_START_TRANSACTION_WITH_SCN_SET
                                .getErrorCode(), e.getErrorCode());
            }
        }
    }

    @Test
    public void testReCreateTxnTableAfterDroppingExistingNonTxnTable() throws Exception {
        String tableName = generateUniqueName();
        Properties props = PropertiesUtil.deepCopy(TEST_PROPERTIES);
        Connection conn = DriverManager.getConnection(getUrl(), props);
        conn.setAutoCommit(false);
        Statement stmt = conn.createStatement();
        stmt.execute("CREATE TABLE " + tableName + "(k VARCHAR PRIMARY KEY, v1 VARCHAR, v2 VARCHAR)");
        stmt.execute("DROP TABLE " + tableName);
        // Must drop metadata as Omid does not allow creating a transactional table from a non transactional one
        Admin admin = conn.unwrap(PhoenixConnection.class).getQueryServices().getAdmin();
        admin.disableTable(TableName.valueOf(tableName));
        admin.deleteTable(TableName.valueOf(tableName));
        stmt.execute("CREATE TABLE " + tableName + "(k VARCHAR PRIMARY KEY, v1 VARCHAR, v2 VARCHAR) TRANSACTIONAL=true," + tableDDLOptions);
        stmt.execute("CREATE INDEX " + tableName + "_IDX ON " + tableName + " (v1) INCLUDE(v2)");
        assertTrue(conn.unwrap(PhoenixConnection.class).getTable(new PTableKey(null, tableName)).isTransactional());
        assertTrue(conn.unwrap(PhoenixConnection.class).getTable(new PTableKey(null,  tableName + "_IDX")).isTransactional());
    }
    
    @Test
    public void testRowTimestampDisabled() throws SQLException {
        String tableName = generateUniqueName();
        Properties props = PropertiesUtil.deepCopy(TEST_PROPERTIES);
        try (Connection conn = DriverManager.getConnection(getUrl(), props)) {
            conn.setAutoCommit(false);
            Statement stmt = conn.createStatement();
            try {
                stmt.execute("CREATE TABLE " + tableName + "(k VARCHAR, v VARCHAR, d DATE NOT NULL, CONSTRAINT PK PRIMARY KEY(k,d ROW_TIMESTAMP)) TRANSACTIONAL=true," + tableDDLOptions);
                fail();
            }
            catch(SQLException e) {
                assertEquals(SQLExceptionCode.CANNOT_CREATE_TXN_TABLE_WITH_ROW_TIMESTAMP.getErrorCode(), e.getErrorCode());
            }
            stmt.execute("CREATE TABLE " + tableName + "(k VARCHAR, v VARCHAR, d DATE NOT NULL, CONSTRAINT PK PRIMARY KEY(k,d ROW_TIMESTAMP))");
            try {
                stmt.execute("ALTER TABLE " + tableName + " SET TRANSACTIONAL=true");
                fail();
            }
            catch(SQLException e) {
                assertEquals(SQLExceptionCode.CANNOT_ALTER_TO_BE_TXN_WITH_ROW_TIMESTAMP.getErrorCode(), e.getErrorCode());
            }
        }
    }
    
    @Test
    public void testTransactionalTableMetadata() throws SQLException {

        try (Connection conn = DriverManager.getConnection(getUrl())) {
            String transactTableName = generateUniqueName();
            Statement stmt = conn.createStatement();
            stmt.execute("CREATE TABLE " + transactTableName + " (k VARCHAR PRIMARY KEY, v1 VARCHAR, v2 VARCHAR) " +
                "TRANSACTIONAL=true," + tableDDLOptions);
            conn.commit();

            DatabaseMetaData dbmd = conn.getMetaData();
            ResultSet rs = dbmd.getTables(null, null, StringUtil.escapeLike(transactTableName), null);
            assertTrue(rs.next());
            assertEquals("Transactional table was not marked as transactional in JDBC API.",
                "true", rs.getString(PhoenixDatabaseMetaData.TRANSACTIONAL));
            assertEquals(txProvider, rs.getString(PhoenixDatabaseMetaData.TRANSACTION_PROVIDER));
            
            // Ensure round-trip-ability of TRANSACTION_PROVIDER
            PTable table = conn.unwrap(PhoenixConnection.class).getTableNoCache(transactTableName);
            assertEquals(txProvider, table.getTransactionProvider().name());

            String nonTransactTableName = generateUniqueName();
            Statement stmt2 = conn.createStatement();
            stmt2.execute("CREATE TABLE " + nonTransactTableName + "(k VARCHAR PRIMARY KEY, v1 VARCHAR, v2 VARCHAR) ");
            conn.commit();

            ResultSet rs2 = dbmd.getTables(null, null, StringUtil.escapeLike(nonTransactTableName), null);
            assertTrue(rs2.next());
            assertEquals("Non-transactional table was marked as transactional in JDBC API.",
                "false", rs2.getString(PhoenixDatabaseMetaData.TRANSACTIONAL));
            assertNull(rs2.getString(PhoenixDatabaseMetaData.TRANSACTION_PROVIDER));
            
            try {
                stmt.execute("CREATE TABLE " + transactTableName + " (k VARCHAR PRIMARY KEY, v1 VARCHAR, v2 VARCHAR) " +
                        "TRANSACTION_PROVIDER=foo");
                fail();
            } catch (SQLException e) {
                assertEquals(SQLExceptionCode.UNKNOWN_TRANSACTION_PROVIDER.getErrorCode(), e.getErrorCode());
            }
        }
    }
    
    @Test
    public void testOnDupKeyForTransactionalTable() throws Exception {
        // TODO: we should support having a transactional table defined for a connectionless connection
        try (Connection conn = DriverManager.getConnection(getUrl())) {
            String transactTableName = generateUniqueName();
            conn.createStatement().execute("CREATE TABLE " + transactTableName + " (k integer not null primary key, v bigint) TRANSACTIONAL=true," + tableDDLOptions);
            conn.createStatement().execute("UPSERT INTO " + transactTableName + " VALUES(0,0) ON DUPLICATE KEY UPDATE v = v + 1");
            fail();
        } catch (SQLException e) {
            assertEquals(SQLExceptionCode.CANNOT_USE_ON_DUP_KEY_FOR_TRANSACTIONAL.getErrorCode(), e.getErrorCode());
        }
    }
    
    @Test
    public void testProperties() throws Exception {
        String nonTxTableName = generateUniqueName();
        String txTableName = generateUniqueName();
        String idx1 = generateUniqueName();
        String idx2 = generateUniqueName();

        Connection conn = DriverManager.getConnection(getUrl());
        conn.createStatement().execute("CREATE TABLE " + nonTxTableName + "1(k INTEGER PRIMARY KEY, a.v VARCHAR, b.v VARCHAR, c.v VARCHAR) TTL=1000");
        conn.createStatement().execute("CREATE INDEX " + idx1 + " ON " + nonTxTableName + "1(a.v, b.v)");
        conn.createStatement().execute("CREATE INDEX " + idx2 + " ON " + nonTxTableName + "1(c.v) INCLUDE (a.v, b.v)");

        try {
            conn.createStatement().execute("ALTER TABLE " + nonTxTableName + "1 SET TRANSACTIONAL=true," + tableDDLOptions);
            if (TransactionFactory.Provider.valueOf(txProvider).getTransactionProvider().isUnsupported(Feature.ALTER_NONTX_TO_TX)) {
                fail(txProvider + " should not allow converting a non transactional table to be transactional");
            }
        } catch (SQLException e) { // Should fail for Omid
            if (!TransactionFactory.Provider.valueOf(txProvider).getTransactionProvider().isUnsupported(Feature.ALTER_NONTX_TO_TX)) {
                throw e;
            }
            assertEquals(SQLExceptionCode.CANNOT_ALTER_TABLE_FROM_NON_TXN_TO_TXNL.getErrorCode(), e.getErrorCode());
            // FIXME: should verify Omid table properties too, but the checks below won't be valid for Omid
            return;
        }

        TableDescriptor desc = conn.unwrap(PhoenixConnection.class).getQueryServices().getTableDescriptor(Bytes.toBytes(nonTxTableName + "1"));
        for (ColumnFamilyDescriptor colDesc : desc.getColumnFamilies()) {
            assertEquals(QueryServicesOptions.DEFAULT_MAX_VERSIONS_TRANSACTIONAL, colDesc.getMaxVersions());
            assertEquals(1000, colDesc.getTimeToLive());
            byte[] propertyTTL = colDesc.getValue(PhoenixTransactionContext.PROPERTY_TTL_BYTES);
            assertEquals(1000, Integer.parseInt(Bytes.toString(propertyTTL)));
        }

        desc = conn.unwrap(PhoenixConnection.class).getQueryServices().getTableDescriptor(Bytes.toBytes(idx1));
        for (ColumnFamilyDescriptor colDesc : desc.getColumnFamilies()) {
            assertEquals(QueryServicesOptions.DEFAULT_MAX_VERSIONS_TRANSACTIONAL, colDesc.getMaxVersions());
            assertEquals(1000, colDesc.getTimeToLive());
            byte[] propertyTTL = colDesc.getValue(PhoenixTransactionContext.PROPERTY_TTL_BYTES);
            assertEquals(1000, Integer.parseInt(Bytes.toString(propertyTTL)));
        }
        
        desc = conn.unwrap(PhoenixConnection.class).getQueryServices().getTableDescriptor(Bytes.toBytes(idx2));
        for (ColumnFamilyDescriptor colDesc : desc.getColumnFamilies()) {
            assertEquals(QueryServicesOptions.DEFAULT_MAX_VERSIONS_TRANSACTIONAL, colDesc.getMaxVersions());
            assertEquals(1000, colDesc.getTimeToLive());
            byte[] propertyTTL = colDesc.getValue(PhoenixTransactionContext.PROPERTY_TTL_BYTES);
            assertEquals(1000, Integer.parseInt(Bytes.toString(propertyTTL)));
        }
        
        conn.createStatement().execute("CREATE TABLE " + nonTxTableName + "2(k INTEGER PRIMARY KEY, a.v VARCHAR, b.v VARCHAR, c.v VARCHAR)");
        conn.createStatement().execute("ALTER TABLE " + nonTxTableName + "2 SET TRANSACTIONAL=true, VERSIONS=10, " + tableDDLOptions);
        desc = conn.unwrap(PhoenixConnection.class).getQueryServices().getTableDescriptor(Bytes.toBytes( nonTxTableName + "2"));
        for (ColumnFamilyDescriptor colDesc : desc.getColumnFamilies()) {
            assertEquals(10, colDesc.getMaxVersions());
            assertEquals(ColumnFamilyDescriptorBuilder.DEFAULT_TTL, colDesc.getTimeToLive());
            assertEquals(null, colDesc.getValue(PhoenixTransactionContext.PROPERTY_TTL_BYTES));
        }
        conn.createStatement().execute("ALTER TABLE " + nonTxTableName + "2 SET TTL=1000");
        desc = conn.unwrap(PhoenixConnection.class).getQueryServices().getTableDescriptor(Bytes.toBytes( nonTxTableName + "2"));
        for (ColumnFamilyDescriptor colDesc : desc.getColumnFamilies()) {
            assertEquals(10, colDesc.getMaxVersions());
            assertEquals(1000, colDesc.getTimeToLive());
            byte[] propertyTTL = colDesc.getValue(PhoenixTransactionContext.PROPERTY_TTL_BYTES);
            assertEquals(1000, Integer.parseInt(Bytes.toString(propertyTTL)));
        }

        conn.createStatement().execute("CREATE TABLE " + nonTxTableName + "3(k INTEGER PRIMARY KEY, a.v VARCHAR, b.v VARCHAR, c.v VARCHAR)");
        conn.createStatement().execute("ALTER TABLE " + nonTxTableName + "3 SET TRANSACTIONAL=true, b.VERSIONS=10, c.VERSIONS=20," + tableDDLOptions);
        desc = conn.unwrap(PhoenixConnection.class).getQueryServices().getTableDescriptor(Bytes.toBytes( nonTxTableName + "3"));
        assertEquals(QueryServicesOptions.DEFAULT_MAX_VERSIONS_TRANSACTIONAL, desc.getColumnFamily(Bytes.toBytes("A")).getMaxVersions());
        assertEquals(10, desc.getColumnFamily(Bytes.toBytes("B")).getMaxVersions());
        assertEquals(20, desc.getColumnFamily(Bytes.toBytes("C")).getMaxVersions());

        conn.createStatement().execute("CREATE TABLE " + nonTxTableName + "4(k INTEGER PRIMARY KEY, a.v VARCHAR, b.v VARCHAR, c.v VARCHAR)");
        try {
            conn.createStatement().execute("ALTER TABLE " + nonTxTableName + "4 SET TRANSACTIONAL=true, VERSIONS=1," + tableDDLOptions);
            fail();
        } catch (SQLException e) {
            assertEquals(SQLExceptionCode.TX_MAX_VERSIONS_MUST_BE_GREATER_THAN_ONE.getErrorCode(), e.getErrorCode());
        }

        try {
            conn.createStatement().execute("ALTER TABLE " + nonTxTableName + "4 SET TRANSACTIONAL=true, b.VERSIONS=1," + tableDDLOptions);
            fail();
        } catch (SQLException e) {
            assertEquals(SQLExceptionCode.TX_MAX_VERSIONS_MUST_BE_GREATER_THAN_ONE.getErrorCode(), e.getErrorCode());
        }
        
        conn.createStatement().execute("CREATE TABLE " + txTableName + "(k INTEGER PRIMARY KEY, v VARCHAR) TTL=1000, TRANSACTIONAL=true," + tableDDLOptions);
        desc = conn.unwrap(PhoenixConnection.class).getQueryServices().getTableDescriptor(Bytes.toBytes(txTableName));
        for (ColumnFamilyDescriptor colDesc : desc.getColumnFamilies()) {
            assertEquals(QueryServicesOptions.DEFAULT_MAX_VERSIONS_TRANSACTIONAL, colDesc.getMaxVersions());
            assertEquals(ColumnFamilyDescriptorBuilder.DEFAULT_TTL, colDesc.getTimeToLive());
            byte[] propertyTTL = colDesc.getValue(PhoenixTransactionContext.PROPERTY_TTL_BYTES);
            assertEquals(1000, Integer.parseInt(Bytes.toString(propertyTTL)));
        }
    }
    
    @Test
    public void testColConflicts() throws Exception {
        String transTableName = generateUniqueName();
        String fullTableName = INDEX_DATA_SCHEMA + QueryConstants.NAME_SEPARATOR + transTableName;
        try (Connection conn1 = DriverManager.getConnection(getUrl()); 
                Connection conn2 = DriverManager.getConnection(getUrl())) {
            TestUtil.createTransactionalTable(conn1, fullTableName, tableDDLOptions);
            conn1.setAutoCommit(false);
            conn2.setAutoCommit(false);
            String selectSql = "SELECT * FROM "+fullTableName;
            conn1.setAutoCommit(false);
            ResultSet rs = conn1.createStatement().executeQuery(selectSql);
            assertFalse(rs.next());
            // upsert row using conn1
            String upsertSql = "UPSERT INTO " + fullTableName + "(varchar_pk, char_pk, int_pk, long_pk, decimal_pk, date_pk, a.int_col1) VALUES(?, ?, ?, ?, ?, ?, ?)";
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
    
    @Test
    public void testCheckpointAndRollback() throws Exception {
        Properties props = PropertiesUtil.deepCopy(TEST_PROPERTIES);
        Connection conn = DriverManager.getConnection(getUrl(), props);
        String fullTableName = generateUniqueName();
        conn.setAutoCommit(false);
        try {
            Statement stmt = conn.createStatement();
            stmt.execute("CREATE TABLE " + fullTableName + "(k VARCHAR PRIMARY KEY, v1 VARCHAR, v2 VARCHAR) TRANSACTIONAL=true," + tableDDLOptions);
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
    
    private static void assertTTL(Admin admin, String tableName, int ttl) throws Exception {
        TableDescriptor tableDesc = admin.getDescriptor(TableName.valueOf(tableName));
        for (ColumnFamilyDescriptor colDesc : tableDesc.getColumnFamilies()) {
            assertEquals(ColumnFamilyDescriptorBuilder.DEFAULT_TTL,colDesc.getTimeToLive());
        }
    }
    
    @Test
    public void testSetTTL() throws Exception {
        Properties props = PropertiesUtil.deepCopy(TEST_PROPERTIES);
        TransactionFactory.Provider txProvider = TransactionFactory.Provider.valueOf(this.txProvider);
        try (Connection conn = DriverManager.getConnection(getUrl(), props); 
             Admin admin = conn.unwrap(PhoenixConnection.class).getQueryServices().getAdmin()) {
            String tableName = generateUniqueName();
            try {
                conn.createStatement().execute("CREATE TABLE " + tableName + 
                        "(K VARCHAR PRIMARY KEY) TRANSACTIONAL=true,TRANSACTION_PROVIDER='" + txProvider + "',TTL=100");
                if (txProvider.getTransactionProvider().isUnsupported(Feature.SET_TTL)) {
                    fail();
                }
                assertTTL(admin, tableName, 100);
            } catch (SQLException e) {
                if (txProvider.getTransactionProvider().isUnsupported(Feature.SET_TTL)) {
                    assertEquals(SQLExceptionCode.TTL_UNSUPPORTED_FOR_TXN_TABLE.getErrorCode(), e.getErrorCode());
                } else {
                    throw e;
                }
            }
            tableName = generateUniqueName();
            conn.createStatement().execute("CREATE TABLE " + tableName + 
                    "(K VARCHAR PRIMARY KEY) TRANSACTIONAL=true,TRANSACTION_PROVIDER='" + txProvider + "'");
            try {
                conn.createStatement().execute("ALTER TABLE " + tableName + " SET TTL=" + 200);
                if (txProvider.getTransactionProvider().isUnsupported(Feature.SET_TTL)) {
                    fail();
                }
                assertTTL(admin, tableName, 200);
            } catch (SQLException e) {
                if (txProvider.getTransactionProvider().isUnsupported(Feature.SET_TTL)) {
                    assertEquals(SQLExceptionCode.TTL_UNSUPPORTED_FOR_TXN_TABLE.getErrorCode(), e.getErrorCode());
                } else {
                    throw e;
                }
            }
        }
    }

    private class ParallelQuery implements Runnable {
        PreparedStatement query;
        CountDownLatch started = new CountDownLatch(1);
        AtomicBoolean done = new AtomicBoolean(false);
        ConcurrentHashMap<Long, Long> failCounts = new ConcurrentHashMap<>();

        public ParallelQuery(PreparedStatement query) {
            this.query = query;
        }

        @Override
        public void run() {
            try {
                started.countDown();
                while(!done.get()) {
                    ResultSet rs = query.executeQuery();
                    assertTrue(rs.next());
                    long count = rs.getLong(1);
                    rs.close();
                    if (count != 0 && count != (int)Math.pow(2, 12)) {
                        failCounts.put(count, count);
                    }
                }
            } catch (SQLException x) {
                throw new RuntimeException(x);
            }
        }
    }

    @Test
    public void testParallelConnectionOnlySeesCommittedData() throws Exception {
        Properties props = PropertiesUtil.deepCopy(TEST_PROPERTIES);
        props.setProperty(QueryServices.MUTATE_BATCH_SIZE_BYTES_ATTRIB, Integer.toString(512));
        props.setProperty(QueryServices.SCAN_CACHE_SIZE_ATTRIB, Integer.toString(3));
        props.setProperty(QueryServices.SCAN_RESULT_CHUNK_SIZE, Integer.toString(3));
        try (Connection conn = DriverManager.getConnection(getUrl(), props);
             Connection otherConn = DriverManager.getConnection(getUrl());
             Admin admin = driver
                        .getConnectionQueryServices(getUrl(), TestUtil.TEST_PROPERTIES)
                        .getAdmin();) {
            conn.setAutoCommit(false);
            otherConn.setAutoCommit(true);
            String tableName = generateUniqueName();
            conn.createStatement().execute("CREATE SEQUENCE " + tableName + "_seq CACHE 1000");
            conn.createStatement().execute("CREATE TABLE " + tableName
                    + " (pk INTEGER PRIMARY KEY, val INTEGER) UPDATE_CACHE_FREQUENCY=3600000, TRANSACTIONAL=true,TRANSACTION_PROVIDER='"
                    + txProvider + "'");

            PreparedStatement stmt = conn.prepareStatement("UPSERT INTO " + tableName
                    + " SELECT NEXT VALUE FOR " + tableName + "_seq, val FROM " + tableName);
            PreparedStatement seed = conn.prepareStatement("UPSERT INTO " + tableName
                    + " VALUES (NEXT VALUE FOR " + tableName + "_seq,1)");

            PreparedStatement query = conn.prepareStatement("SELECT COUNT(*) FROM " + tableName);
            PreparedStatement otherQuery = otherConn
                    .prepareStatement("SELECT COUNT(*) FROM " + tableName);

            // seed
            seed.executeUpdate();
            for (int i = 0; i < 12; i++) {
                try {
                    admin.split(TableName.valueOf(tableName));
                } catch (IOException ignore) {
                    // we don't care if the split sometime cannot be executed
                }
                int upsertCount = stmt.executeUpdate();
                assertEquals((int) Math.pow(2, i), upsertCount);

                // read-own-writes, this forces uncommitted data to the server
                ResultSet rs = query.executeQuery();
                assertTrue(rs.next());
                assertEquals((int) Math.pow(2, i + 1), rs.getLong(1));
                rs.close();

                rs = otherQuery.executeQuery();
                assertTrue(rs.next());
                assertEquals(0, rs.getLong(1));
                rs.close();
            }

            ParallelQuery q = new ParallelQuery(otherQuery);
            Thread t = new Thread(q);
            t.start();
            q.started.await();

            conn.commit();

            q.done.set(true);
            t.join();

            assertTrue(
                    "Expected 0 or 4096 but got these intermediary counts " + q.failCounts.keySet(),
                    q.failCounts.isEmpty());
            ResultSet rs = otherQuery.executeQuery();
            assertTrue(rs.next());
            assertEquals((int) Math.pow(2, 12), rs.getLong(1));
            rs.close();
        }
    }

}
