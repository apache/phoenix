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
package org.apache.phoenix.transactions;

import static org.apache.phoenix.util.TestUtil.TEST_PROPERTIES;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertTrue;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.Properties;

import org.apache.phoenix.end2end.BaseHBaseManagedTimeIT;
import org.apache.phoenix.execute.MutationState;
import org.apache.phoenix.jdbc.PhoenixConnection;
import org.apache.phoenix.query.QueryServices;
import org.apache.phoenix.util.PropertiesUtil;
import org.junit.Test;

import co.cask.tephra.Transaction.VisibilityLevel;

public class TxCheckpointIT extends BaseHBaseManagedTimeIT {

    
    @Test
    public void testUpsertSelectDoesntSeeUpsertedData() throws Exception {
        Properties props = PropertiesUtil.deepCopy(TEST_PROPERTIES);
        props.setProperty(QueryServices.MUTATE_BATCH_SIZE_ATTRIB, Integer.toString(3));
        props.setProperty(QueryServices.SCAN_CACHE_SIZE_ATTRIB, Integer.toString(3));
        props.setProperty(QueryServices.SCAN_RESULT_CHUNK_SIZE, Integer.toString(3));
        Connection conn = DriverManager.getConnection(getUrl(), props);
        conn.setAutoCommit(true);
        conn.createStatement().execute("CREATE SEQUENCE keys");
        conn.createStatement().execute("CREATE TABLE txfoo (pk INTEGER PRIMARY KEY, val INTEGER) TRANSACTIONAL=true");

        conn.createStatement().execute("UPSERT INTO txfoo VALUES (NEXT VALUE FOR keys,1)");
        for (int i=0; i<6; i++) {
            Statement stmt = conn.createStatement();
            int upsertCount = stmt.executeUpdate("UPSERT INTO txfoo SELECT NEXT VALUE FOR keys, val FROM txfoo");
            assertEquals((int)Math.pow(2, i), upsertCount);
        }
        conn.close();
    }
    
    @Test
    public void testCheckpointForUpsertSelect() throws Exception {
        ResultSet rs;
        Properties props = PropertiesUtil.deepCopy(TEST_PROPERTIES);
        Connection conn = DriverManager.getConnection(getUrl(), props);
        conn.createStatement().execute("create table tx1 (id bigint not null primary key) TRANSACTIONAL=true");
        conn.createStatement().execute("create table tx2 (id bigint not null primary key) TRANSACTIONAL=true");

        conn.createStatement().execute("upsert into tx1 values (1)");
        conn.createStatement().execute("upsert into tx1 values (2)");
        conn.createStatement().execute("upsert into tx1 values (3)");
        conn.commit();

        MutationState state = conn.unwrap(PhoenixConnection.class).getMutationState();
        state.startTransaction();
        long wp = state.getWritePointer();
        conn.createStatement().execute("upsert into tx1 select max(id)+1 from tx1");
        assertEquals(VisibilityLevel.SNAPSHOT_EXCLUDE_CURRENT, state.getVisibilityLevel());
        assertEquals(wp, state.getWritePointer()); // Make sure write ptr didn't move
        rs = conn.createStatement().executeQuery("select max(id) from tx1");
        
        assertTrue(rs.next());
        assertEquals(4,rs.getLong(1));
        assertFalse(rs.next());
        
        conn.createStatement().execute("upsert into tx1 select max(id)+1 from tx1");
        assertEquals(VisibilityLevel.SNAPSHOT_EXCLUDE_CURRENT, state.getVisibilityLevel());
        assertNotEquals(wp, state.getWritePointer()); // Make sure write ptr moves
        wp = state.getWritePointer();
        
        conn.createStatement().execute("upsert into tx1 select id from tx2");
        assertEquals(VisibilityLevel.SNAPSHOT, state.getVisibilityLevel());
        // Write ptr shouldn't move b/c we're not reading from a table with uncommitted data
        assertEquals(wp, state.getWritePointer()); 
        
        rs = conn.createStatement().executeQuery("select max(id) from tx1");
        
        assertTrue(rs.next());
        assertEquals(5,rs.getLong(1));
        assertFalse(rs.next());
        
        conn.rollback();
        
        rs = conn.createStatement().executeQuery("select max(id) from tx1");
        
        assertTrue(rs.next());
        assertEquals(3,rs.getLong(1));
        assertFalse(rs.next());

        wp = state.getWritePointer();
        conn.createStatement().execute("upsert into tx1 select max(id)+1 from tx1");
        assertEquals(VisibilityLevel.SNAPSHOT_EXCLUDE_CURRENT, state.getVisibilityLevel());
        assertEquals(wp, state.getWritePointer()); // Make sure write ptr didn't move
        rs = conn.createStatement().executeQuery("select max(id) from tx1");
        
        assertTrue(rs.next());
        assertEquals(4,rs.getLong(1));
        assertFalse(rs.next());
        
        conn.createStatement().execute("upsert into tx1 select max(id)+1 from tx1");
        assertEquals(VisibilityLevel.SNAPSHOT_EXCLUDE_CURRENT, state.getVisibilityLevel());
        assertNotEquals(wp, state.getWritePointer()); // Make sure write ptr moves
        rs = conn.createStatement().executeQuery("select max(id) from tx1");
        
        assertTrue(rs.next());
        assertEquals(5,rs.getLong(1));
        assertFalse(rs.next());
        
        conn.commit();
        
        rs = conn.createStatement().executeQuery("select max(id) from tx1");
        
        assertTrue(rs.next());
        assertEquals(5,rs.getLong(1));
        assertFalse(rs.next());
    }  

    @Test
    public void testCheckpointForDelete() throws Exception {
        ResultSet rs;
        Properties props = PropertiesUtil.deepCopy(TEST_PROPERTIES);
        Connection conn = DriverManager.getConnection(getUrl(), props);
        conn.createStatement().execute("create table tx3 (id1 bigint primary key, fk1 integer) TRANSACTIONAL=true");
        conn.createStatement().execute("create table tx4 (id2 bigint primary key, fk2 integer) TRANSACTIONAL=true");

        conn.createStatement().execute("upsert into tx3 values (1, 3)");
        conn.createStatement().execute("upsert into tx3 values (2, 2)");
        conn.createStatement().execute("upsert into tx3 values (3, 1)");
        conn.createStatement().execute("upsert into tx4 values (1, 1)");
        conn.commit();

        MutationState state = conn.unwrap(PhoenixConnection.class).getMutationState();
        state.startTransaction();
        long wp = state.getWritePointer();
        conn.createStatement().execute("delete from tx3 where id1=fk1");
        assertEquals(VisibilityLevel.SNAPSHOT, state.getVisibilityLevel());
        assertEquals(wp, state.getWritePointer()); // Make sure write ptr didn't move

        rs = conn.createStatement().executeQuery("select id1 from tx3");
        assertTrue(rs.next());
        assertEquals(1,rs.getLong(1));
        assertTrue(rs.next());
        assertEquals(3,rs.getLong(1));
        assertFalse(rs.next());

        conn.createStatement().execute("delete from tx3 where id1 in (select fk1 from tx3 join tx4 on (fk2=id1))");
        assertEquals(VisibilityLevel.SNAPSHOT_EXCLUDE_CURRENT, state.getVisibilityLevel());
        assertNotEquals(wp, state.getWritePointer()); // Make sure write ptr moved

        rs = conn.createStatement().executeQuery("select id1 from tx3");
        assertTrue(rs.next());
        assertEquals(1,rs.getLong(1));
        assertFalse(rs.next());

        /*
         * TODO: file Tephra JIRA, as this fails with an NPE because the
         * ActionChange has a null family since we're issuing row deletes.
         * See this code in TransactionAwareHTable.transactionalizeAction(Delete)
         * and try modifying addToChangeSet(deleteRow, null, null);
         * to modifying addToChangeSet(deleteRow, family, null);
            } else {
              for (Map.Entry<byte [], List<Cell>> familyEntry : familyToDelete.entrySet()) {
                byte[] family = familyEntry.getKey();
                List<Cell> entries = familyEntry.getValue();
                boolean isFamilyDelete = false;
                if (entries.size() == 1) {
                  Cell cell = entries.get(0);
                  isFamilyDelete = CellUtil.isDeleteFamily(cell);
                }
                if (isFamilyDelete) {
                  if (conflictLevel == TxConstants.ConflictDetection.ROW ||
                      conflictLevel == TxConstants.ConflictDetection.NONE) {
                    // no need to identify individual columns deleted
                    txDelete.deleteFamily(family);
                    addToChangeSet(deleteRow, null, null);
         */
//        conn.rollback();
//        rs = conn.createStatement().executeQuery("select id1 from tx3");
//        assertTrue(rs.next());
//        assertEquals(1,rs.getLong(1));
//        assertTrue(rs.next());
//        assertEquals(2,rs.getLong(1));
//        assertTrue(rs.next());
//        assertEquals(3,rs.getLong(1));
//        assertFalse(rs.next());

    }  
}
