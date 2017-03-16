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
import java.sql.DatabaseMetaData;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Properties;

import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.phoenix.end2end.ParallelStatsDisabledIT;
import org.apache.phoenix.exception.SQLExceptionCode;
import org.apache.phoenix.jdbc.PhoenixConnection;
import org.apache.phoenix.jdbc.PhoenixDatabaseMetaData;
import org.apache.phoenix.query.QueryConstants;
import org.apache.phoenix.query.QueryServicesOptions;
import org.apache.phoenix.schema.PTableKey;
import org.apache.phoenix.util.PropertiesUtil;
import org.apache.phoenix.util.StringUtil;
import org.apache.phoenix.util.TestUtil;
import org.apache.tephra.TxConstants;
import org.junit.Test;

public class TransactionIT  extends ParallelStatsDisabledIT {

    @Test
    public void testReCreateTxnTableAfterDroppingExistingNonTxnTable() throws SQLException {
        String tableName = generateUniqueName();
        Properties props = PropertiesUtil.deepCopy(TEST_PROPERTIES);
        Connection conn = DriverManager.getConnection(getUrl(), props);
        conn.setAutoCommit(false);
        Statement stmt = conn.createStatement();
        stmt.execute("CREATE TABLE " + tableName + "(k VARCHAR PRIMARY KEY, v1 VARCHAR, v2 VARCHAR)");
        stmt.execute("DROP TABLE " + tableName);
        stmt.execute("CREATE TABLE " + tableName + "(k VARCHAR PRIMARY KEY, v1 VARCHAR, v2 VARCHAR) TRANSACTIONAL=true");
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
                stmt.execute("CREATE TABLE " + tableName + "(k VARCHAR, v VARCHAR, d DATE NOT NULL, CONSTRAINT PK PRIMARY KEY(k,d ROW_TIMESTAMP)) TRANSACTIONAL=true");
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
                "TRANSACTIONAL=true");
            conn.commit();

            DatabaseMetaData dbmd = conn.getMetaData();
            ResultSet rs = dbmd.getTables(null, null, StringUtil.escapeLike(transactTableName), null);
            assertTrue(rs.next());
            assertEquals("Transactional table was not marked as transactional in JDBC API.",
                "true", rs.getString(PhoenixDatabaseMetaData.TRANSACTIONAL));

            String nonTransactTableName = generateUniqueName();
            Statement stmt2 = conn.createStatement();
            stmt2.execute("CREATE TABLE " + nonTransactTableName + "(k VARCHAR PRIMARY KEY, v1 VARCHAR, v2 VARCHAR) ");
            conn.commit();

            ResultSet rs2 = dbmd.getTables(null, null, StringUtil.escapeLike(nonTransactTableName), null);
            assertTrue(rs2.next());
            assertEquals("Non-transactional table was marked as transactional in JDBC API.",
                "false", rs2.getString(PhoenixDatabaseMetaData.TRANSACTIONAL));
        }
    }
    
    @Test
    public void testOnDupKeyForTransactionalTable() throws Exception {
        // TODO: we should support having a transactional table defined for a connectionless connection
        try (Connection conn = DriverManager.getConnection(getUrl())) {
            String transactTableName = generateUniqueName();
            conn.createStatement().execute("CREATE TABLE " + transactTableName + " (k integer not null primary key, v bigint) TRANSACTIONAL=true");
            conn.createStatement().execute("UPSERT INTO " + transactTableName + " VALUES(0,0) ON DUPLICATE KEY UPDATE v = v + 1");
            fail();
        } catch (SQLException e) {
            assertEquals(SQLExceptionCode.CANNOT_USE_ON_DUP_KEY_FOR_TRANSACTIONAL.getErrorCode(), e.getErrorCode());
        }
    }
    
    @Test
    public void testProperties() throws Exception {
        String nonTxTableName = generateUniqueName();

        Connection conn = DriverManager.getConnection(getUrl());
        conn.createStatement().execute("CREATE TABLE " + nonTxTableName + "1(k INTEGER PRIMARY KEY, a.v VARCHAR, b.v VARCHAR, c.v VARCHAR) TTL=1000");
        conn.createStatement().execute("CREATE INDEX idx1 ON " + nonTxTableName + "1(a.v, b.v) TTL=1000");
        conn.createStatement().execute("CREATE INDEX idx2 ON " + nonTxTableName + "1(c.v) INCLUDE (a.v, b.v) TTL=1000");

        conn.createStatement().execute("ALTER TABLE " + nonTxTableName + "1 SET TRANSACTIONAL=true");

        HTableDescriptor desc = conn.unwrap(PhoenixConnection.class).getQueryServices().getTableDescriptor(Bytes.toBytes(nonTxTableName + "1"));
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
        
        conn.createStatement().execute("CREATE TABLE " + nonTxTableName + "2(k INTEGER PRIMARY KEY, a.v VARCHAR, b.v VARCHAR, c.v VARCHAR)");
        conn.createStatement().execute("ALTER TABLE " + nonTxTableName + "2 SET TRANSACTIONAL=true, VERSIONS=10");
        desc = conn.unwrap(PhoenixConnection.class).getQueryServices().getTableDescriptor(Bytes.toBytes( nonTxTableName + "2"));
        for (HColumnDescriptor colDesc : desc.getFamilies()) {
            assertEquals(10, colDesc.getMaxVersions());
            assertEquals(HColumnDescriptor.DEFAULT_TTL, colDesc.getTimeToLive());
            assertEquals(null, colDesc.getValue(TxConstants.PROPERTY_TTL));
        }
        conn.createStatement().execute("ALTER TABLE " + nonTxTableName + "2 SET TTL=1000");
        desc = conn.unwrap(PhoenixConnection.class).getQueryServices().getTableDescriptor(Bytes.toBytes( nonTxTableName + "2"));
        for (HColumnDescriptor colDesc : desc.getFamilies()) {
            assertEquals(10, colDesc.getMaxVersions());
            assertEquals(1000, colDesc.getTimeToLive());
            assertEquals(1000, Integer.parseInt(colDesc.getValue(TxConstants.PROPERTY_TTL)));
        }

        conn.createStatement().execute("CREATE TABLE " + nonTxTableName + "3(k INTEGER PRIMARY KEY, a.v VARCHAR, b.v VARCHAR, c.v VARCHAR)");
        conn.createStatement().execute("ALTER TABLE " + nonTxTableName + "3 SET TRANSACTIONAL=true, b.VERSIONS=10, c.VERSIONS=20");
        desc = conn.unwrap(PhoenixConnection.class).getQueryServices().getTableDescriptor(Bytes.toBytes( nonTxTableName + "3"));
        assertEquals(QueryServicesOptions.DEFAULT_MAX_VERSIONS_TRANSACTIONAL, desc.getFamily(Bytes.toBytes("A")).getMaxVersions());
        assertEquals(10, desc.getFamily(Bytes.toBytes("B")).getMaxVersions());
        assertEquals(20, desc.getFamily(Bytes.toBytes("C")).getMaxVersions());

        conn.createStatement().execute("CREATE TABLE " + nonTxTableName + "4(k INTEGER PRIMARY KEY, a.v VARCHAR, b.v VARCHAR, c.v VARCHAR)");
        try {
            conn.createStatement().execute("ALTER TABLE " + nonTxTableName + "4 SET TRANSACTIONAL=true, VERSIONS=1");
            fail();
        } catch (SQLException e) {
            assertEquals(SQLExceptionCode.TX_MAX_VERSIONS_MUST_BE_GREATER_THAN_ONE.getErrorCode(), e.getErrorCode());
        }

        try {
            conn.createStatement().execute("ALTER TABLE " + nonTxTableName + "4 SET TRANSACTIONAL=true, b.VERSIONS=1");
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
    public void testColConflicts() throws Exception {
        String transTableName = generateUniqueName();
        String fullTableName = INDEX_DATA_SCHEMA + QueryConstants.NAME_SEPARATOR + transTableName;
        try (Connection conn1 = DriverManager.getConnection(getUrl()); 
                Connection conn2 = DriverManager.getConnection(getUrl())) {
            TestUtil.createTransactionalTable(conn1, fullTableName);
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
}