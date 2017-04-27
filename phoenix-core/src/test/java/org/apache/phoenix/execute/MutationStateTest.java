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

import static org.apache.phoenix.execute.MutationState.joinSortedIntArrays;
import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.sql.Connection;
import java.sql.DriverManager;
import java.util.Iterator;
import java.util.List;

import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.Pair;
import org.apache.phoenix.schema.types.PUnsignedInt;
import org.apache.phoenix.schema.types.PVarchar;
import org.apache.phoenix.util.PhoenixRuntime;
import org.junit.Test;

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


            Iterator<Pair<byte[],List<KeyValue>>> dataTableNameAndMutationKeyValuesIter =
                    PhoenixRuntime.getUncommittedDataIterator(conn);


            assertTrue(dataTableNameAndMutationKeyValuesIter.hasNext());
            Pair<byte[],List<KeyValue>> pair=dataTableNameAndMutationKeyValuesIter.next();
            String tableName1=Bytes.toString(pair.getFirst());
            List<KeyValue> keyValues1=pair.getSecond();

            assertTrue(dataTableNameAndMutationKeyValuesIter.hasNext());
            pair=dataTableNameAndMutationKeyValuesIter.next();
            String tableName2=Bytes.toString(pair.getFirst());
            List<KeyValue> keyValues2=pair.getSecond();

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

    private void assertTable(String tableName1,List<KeyValue> keyValues1,String tableName2,List<KeyValue> keyValues2) {
        assertTrue("MUTATION_TEST1".equals(tableName1));
        assertTrue(Bytes.equals(PUnsignedInt.INSTANCE.toBytes(111),CellUtil.cloneRow(keyValues1.get(0))));
        assertTrue("app1".equals(PVarchar.INSTANCE.toObject(CellUtil.cloneValue(keyValues1.get(1)))));

        assertTrue("MUTATION_TEST2".equals(tableName2));
        assertTrue(Bytes.equals(PUnsignedInt.INSTANCE.toBytes(222),CellUtil.cloneRow(keyValues2.get(0))));
        assertTrue("app2".equals(PVarchar.INSTANCE.toObject(CellUtil.cloneValue(keyValues2.get(1)))));

    }
}
