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
import static org.apache.phoenix.util.TestUtil.createTransactionalTable;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;

import javax.annotation.concurrent.NotThreadSafe;

import org.apache.phoenix.end2end.ParallelStatsDisabledIT;
import org.apache.phoenix.query.QueryConstants;
import org.apache.phoenix.util.TestUtil;
import org.junit.Test;

/**
 * 
 * Transaction related tests that flap when run in parallel.
 * TODO: review with Tephra community
 *
 */
@NotThreadSafe // Prevents test methods from running in parallel
public class NotThreadSafeTransactionIT extends ParallelStatsDisabledIT {
    @Test
    public void testDelete() throws Exception {
        String transTableName = generateUniqueName();
        String fullTableName = INDEX_DATA_SCHEMA + QueryConstants.NAME_SEPARATOR + transTableName;
        String selectSQL = "SELECT * FROM " + fullTableName;
        try (Connection conn = DriverManager.getConnection(getUrl());
                Connection conn1 = DriverManager.getConnection(getUrl()); 
                Connection conn2 = DriverManager.getConnection(getUrl())) {
            TestUtil.createTransactionalTable(conn, fullTableName);
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
            createTransactionalTable(conn, fullTableName);
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
            rs = conn2.createStatement().executeQuery("SELECT * FROM " + fullTableName + " WHERE int_col1 = 1");
            assertFalse(rs.next());
            
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
            createTransactionalTable(conn, fullTableName);
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

}