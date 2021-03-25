/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you maynot use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicablelaw or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.phoenix.execute;

import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Mutation;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.Pair;
import org.apache.phoenix.exception.SQLExceptionCode;
import org.apache.phoenix.execute.MutationState.MultiRowMutationState;
import org.apache.phoenix.execute.MutationState.RowMutationState;
import org.apache.phoenix.hbase.index.util.ImmutableBytesPtr;
import org.apache.phoenix.jdbc.PhoenixConnection;
import org.apache.phoenix.query.QueryServices;
import org.apache.phoenix.schema.TableRef;
import org.apache.phoenix.schema.types.PUnsignedInt;
import org.apache.phoenix.schema.types.PVarchar;
import org.apache.phoenix.thirdparty.com.google.common.collect.ImmutableList;
import org.apache.phoenix.util.PhoenixRuntime;
import org.apache.phoenix.util.PropertiesUtil;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import static org.apache.phoenix.execute.MutationState.joinSortedIntArrays;
import static org.apache.phoenix.query.BaseTest.generateUniqueName;
import static org.apache.phoenix.util.TestUtil.TEST_PROPERTIES;
import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.when;


public class MutationStateTest {

    @Test
    public void testJoinIntArrays() {
        // simple case
        int[] a = new int[] {1};
        int[] b = new int[] {2};
        int[] result = joinSortedIntArrays(a, b);
        
        assertEquals(2, result.length);
        assertArrayEquals(new int[] {1,2}, result);
        
        // empty arrays
        a = new int[0];
        b = new int[0];
        result = joinSortedIntArrays(a, b);
        
        assertEquals(0, result.length);
        assertArrayEquals(new int[] {}, result);
        
        // dupes between arrays
        a = new int[] {1,2,3};
        b = new int[] {1,2,4};
        result = joinSortedIntArrays(a, b);
        
        assertEquals(4, result.length);
        assertArrayEquals(new int[] {1,2,3,4}, result);
        
        // dupes within arrays
        a = new int[] {1,2,2,3};
        b = new int[] {1,2,4};
        result = joinSortedIntArrays(a, b);
        
        assertEquals(4, result.length);
        assertArrayEquals(new int[] {1,2,3,4}, result);
    }

    private static String getUrl() {
        return PhoenixRuntime.JDBC_PROTOCOL + PhoenixRuntime.JDBC_PROTOCOL_SEPARATOR + PhoenixRuntime.CONNECTIONLESS;
    }

    @Test
    public void testToMutationsOverMultipleTables() throws Exception {
        Connection conn = null;
        try {
            conn=DriverManager.getConnection(getUrl());
            conn.createStatement().execute(
                    "create table MUTATION_TEST1"+
                            "( id1 UNSIGNED_INT not null primary key,"+
                    "appId1 VARCHAR)");
            conn.createStatement().execute(
                    "create table MUTATION_TEST2"+
                            "( id2 UNSIGNED_INT not null primary key,"+
                    "appId2 VARCHAR)");

            conn.createStatement().execute("upsert into MUTATION_TEST1(id1,appId1) values(111,'app1')");
            conn.createStatement().execute("upsert into MUTATION_TEST2(id2,appId2) values(222,'app2')");


            Iterator<Pair<byte[],List<Cell>>> dataTableNameAndMutationKeyValuesIter =
                    PhoenixRuntime.getUncommittedDataIterator(conn);


            assertTrue(dataTableNameAndMutationKeyValuesIter.hasNext());
            Pair<byte[],List<Cell>> pair=dataTableNameAndMutationKeyValuesIter.next();
            String tableName1=Bytes.toString(pair.getFirst());
            List<Cell> keyValues1=pair.getSecond();

            assertTrue(dataTableNameAndMutationKeyValuesIter.hasNext());
            pair=dataTableNameAndMutationKeyValuesIter.next();
            String tableName2=Bytes.toString(pair.getFirst());
            List<Cell> keyValues2=pair.getSecond();

            if("MUTATION_TEST1".equals(tableName1)) {
                assertTable(tableName1, keyValues1, tableName2, keyValues2);
            }
            else {
                assertTable(tableName2, keyValues2, tableName1, keyValues1);
            }
            assertTrue(!dataTableNameAndMutationKeyValuesIter.hasNext());
        }
        finally {
            if(conn!=null) {
                conn.close();
            }
        }
    }

    private void assertTable(String tableName1,List<Cell> keyValues1,String tableName2,List<Cell> keyValues2) {
        assertTrue("MUTATION_TEST1".equals(tableName1));
        assertTrue(Bytes.equals(PUnsignedInt.INSTANCE.toBytes(111),CellUtil.cloneRow(keyValues1.get(0))));
        assertTrue("app1".equals(PVarchar.INSTANCE.toObject(CellUtil.cloneValue(keyValues1.get(1)))));

        assertTrue("MUTATION_TEST2".equals(tableName2));
        assertTrue(Bytes.equals(PUnsignedInt.INSTANCE.toBytes(222),CellUtil.cloneRow(keyValues2.get(0))));
        assertTrue("app2".equals(PVarchar.INSTANCE.toObject(CellUtil.cloneValue(keyValues2.get(1)))));

    }

    @Test
    public void testGetMutationBatchList() {
        byte[] r1 = Bytes.toBytes(1);
        byte[] r2 = Bytes.toBytes(2);
        byte[] r3 = Bytes.toBytes(3);
        byte[] r4 = Bytes.toBytes(4);
        // one put and one delete as a group
        {
            List<Mutation> list = ImmutableList.of(new Put(r1), new Put(r2), new Delete(r2));
            List<List<Mutation>> batchLists = MutationState.getMutationBatchList(2, 10, list);
            assertTrue(batchLists.size() == 2);
            assertEquals(batchLists.get(0).size(), 1);
            assertEquals(batchLists.get(1).size(), 2);
        }

        {
            List<Mutation> list = ImmutableList.of(new Put(r1), new Delete(r1), new Put(r2));
            List<List<Mutation>> batchLists = MutationState.getMutationBatchList(2, 10, list);
            assertTrue(batchLists.size() == 2);
            assertEquals(batchLists.get(0).size(), 2);
            assertEquals(batchLists.get(1).size(), 1);
        }

        {
            List<Mutation> list = ImmutableList.of(new Put(r3), new Put(r1), new Delete(r1), new Put(r2), new Put(r4), new Delete(r4));
            List<List<Mutation>> batchLists = MutationState.getMutationBatchList(2, 10, list);
            assertTrue(batchLists.size() == 4);
            assertEquals(batchLists.get(0).size(), 1);
            assertEquals(batchLists.get(1).size(), 2);
            assertEquals(batchLists.get(2).size(), 1);
            assertEquals(batchLists.get(3).size(), 2);
        }

    }

    @Rule
    public ExpectedException exceptionRule = ExpectedException.none();

    @Test
    public void testPendingMutationsOnDDL() throws Exception {
        Properties props = PropertiesUtil.deepCopy(TEST_PROPERTIES);
        props.setProperty(QueryServices.PENDING_MUTATIONS_DDL_THROW_ATTRIB, "true");
        try (Connection conn = DriverManager.getConnection(getUrl(), props);
                PhoenixConnection pConnSpy = spy((PhoenixConnection) conn)) {
            MutationState mutationState = mock(MutationState.class);
            when(mutationState.getNumRows()).thenReturn(1);

            // Create a connection with mutation state and mock it
            doReturn(mutationState).when(pConnSpy).getMutationState();
            exceptionRule.expect(SQLException.class);
            exceptionRule.expectMessage(
                SQLExceptionCode.CANNOT_PERFORM_DDL_WITH_PENDING_MUTATIONS.getMessage());

            pConnSpy.createStatement().execute("create table MUTATION_TEST1"
                    + "( id1 UNSIGNED_INT not null primary key," + "appId1 VARCHAR)");
        }

    }

    @Test
    public void testOnDupAndUpsertInSameCommitBatch() throws Exception {
        String dataTable1 = generateUniqueName();
        String dataTable2 = generateUniqueName();
        try (Connection conn = DriverManager.getConnection(getUrl())) {
            conn.createStatement().execute(String.format(
                "create table %s (id1 UNSIGNED_INT not null primary key, appId1 VARCHAR)", dataTable1));
            conn.createStatement().execute(String.format(
                "create table %s (id2 UNSIGNED_INT not null primary key, appId2 VARCHAR)", dataTable2));

            conn.createStatement().execute(String.format(
                "upsert into %s(id1,appId1) values(111,'app1')", dataTable1));
            conn.createStatement().execute(String.format(
                "upsert into %s(id1,appId1) values(111, 'app1') ON DUPLICATE KEY UPDATE appId1 = null", dataTable1));
            conn.createStatement().execute(String.format(
                "upsert into %s(id2,appId2) values(222,'app2')", dataTable2));
            conn.createStatement().execute(String.format(
                "upsert into %s(id2,appId2) values(222,'app2') ON DUPLICATE KEY UPDATE appId2 = null", dataTable2));

            final PhoenixConnection pconn = conn.unwrap(PhoenixConnection.class);
            MutationState state = pconn.getMutationState();
            assertEquals(2, state.getNumRows());

            int actualPairs = 0;
            Iterator<Pair<byte[], List<Mutation>>> mutations = state.toMutations();
            while (mutations.hasNext()) {
                Pair<byte[], List<Mutation>> nextTable = mutations.next();
                ++actualPairs;
                assertEquals(1, nextTable.getSecond().size());
            }
            // we have 2 tables and each table has 2 mutation batches
            // so we should get 4 <table name, [mutations]> pairs
            assertEquals(4, actualPairs);

            List<Map<TableRef, MultiRowMutationState>> commitBatches = state.createCommitBatches();
            assertEquals(2, commitBatches.size());
            // first commit batch should only contain regular upserts
            verifyCommitBatch(commitBatches.get(0), false, 2, 1);
            verifyCommitBatch(commitBatches.get(1), true, 2, 1);
        }
    }

    private void verifyCommitBatch(Map<TableRef, MultiRowMutationState> commitBatch, boolean conditional,
        int numberOfBatches, int rowsPerBatch) {
        // one for each table
        assertEquals(numberOfBatches, commitBatch.size());
        for (Map.Entry<TableRef, MultiRowMutationState> entry : commitBatch.entrySet()) {
            TableRef tableRef = entry.getKey();
            MultiRowMutationState batch = entry.getValue();
            assertEquals(rowsPerBatch, batch.size());
            for (Map.Entry<ImmutableBytesPtr, RowMutationState> row : batch.entrySet()) {
                ImmutableBytesPtr key = row.getKey();
                RowMutationState rowMutationState = row.getValue();
                if (conditional == true) {
                    assertNotNull(rowMutationState.getOnDupKeyBytes());
                } else {
                    assertNull(rowMutationState.getOnDupKeyBytes());
                }
            }
        }
    }
}
