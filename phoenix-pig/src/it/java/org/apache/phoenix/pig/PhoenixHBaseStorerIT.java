/*
 * Copyright 2010 The Apache Software Foundation
 *
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 *distributed with this work for additional information
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
package org.apache.phoenix.pig;

import static org.apache.phoenix.util.PhoenixRuntime.JDBC_PROTOCOL_SEPARATOR;
import static org.apache.phoenix.util.TestUtil.LOCALHOST;
import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.Collection;

import org.apache.hadoop.hbase.util.Bytes;
import org.apache.phoenix.end2end.BaseHBaseManagedTimeIT;
import org.apache.pig.ExecType;
import org.apache.pig.PigServer;
import org.apache.pig.backend.executionengine.ExecJob.JOB_STATUS;
import org.apache.pig.builtin.mock.Storage;
import org.apache.pig.builtin.mock.Storage.Data;
import org.apache.pig.data.DataByteArray;
import org.apache.pig.data.Tuple;
import org.apache.pig.data.TupleFactory;
import org.joda.time.DateTime;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import com.google.common.collect.Lists;


public class PhoenixHBaseStorerIT extends BaseHBaseManagedTimeIT {

    private static TupleFactory tupleFactory;
    private static Connection conn;
    private static PigServer pigServer;
    private static String zkQuorum;
    
    @BeforeClass
    public static void setUpBeforeClass() throws Exception {
        conn = DriverManager.getConnection(getUrl());
        zkQuorum = LOCALHOST + JDBC_PROTOCOL_SEPARATOR + getZKClientPort(getTestClusterConfig());
        // Pig variables
        tupleFactory = TupleFactory.getInstance();
    }
    
    @Before
    public void setUp() throws Exception {
        pigServer = new PigServer(ExecType.LOCAL, getTestClusterConfig());
    }
    
    @After
    public void tearDown() throws Exception {
         pigServer.shutdown();
    }

    @AfterClass
    public static void tearDownAfterClass() throws Exception {
        conn.close();
    }

    /**
     * Basic test - writes data to a Phoenix table and compares the data written
     * to expected
     * 
     * @throws Exception
     */
    @Test
    public void testStorer() throws Exception {
        final String tableName = "TABLE1";
        final Statement stmt = conn.createStatement();

        stmt.execute("CREATE TABLE " + tableName +
                 " (ID INTEGER NOT NULL PRIMARY KEY, NAME VARCHAR)");

        final Data data = Storage.resetData(pigServer);
        final Collection<Tuple> list = Lists.newArrayList();

        // Create input dataset
        int rows = 100;
        for (int i = 0; i < rows; i++) {
            Tuple t = tupleFactory.newTuple();
            t.append(i);
            t.append("a" + i);
            list.add(t);
        }
        data.set("in", "id:int, name:chararray", list);

        pigServer.setBatchOn();
        pigServer.registerQuery("A = LOAD 'in' USING mock.Storage();");

        pigServer.registerQuery("Store A into 'hbase://" + tableName
                               + "' using " + PhoenixHBaseStorage.class.getName() + "('"
                                + zkQuorum + "', '-batchSize 1000');");

         // Now run the Pig script
        if (pigServer.executeBatch().get(0).getStatus() != JOB_STATUS.COMPLETED) {
            throw new RuntimeException("Job failed", pigServer.executeBatch()
                    .get(0).getException());
        }

        // Compare data in Phoenix table to the expected
        final ResultSet rs = stmt
                .executeQuery("SELECT id, name FROM table1 ORDER BY id");

        for (int i = 0; i < rows; i++) {
            assertTrue(rs.next());
            assertEquals(i, rs.getInt(1));
            assertEquals("a" +  i, rs.getString(2));
        }
    }
    
    /**
     * Basic test - writes specific columns data to a Phoenix table and compares the data written
     * to expected
     * 
     * @throws Exception
     */
    @Test
    public void testStorerForSpecificColumns() throws Exception {
        final String tableName = "TABLE2";
        final Statement stmt = conn.createStatement();

        stmt.execute("CREATE TABLE " + tableName +
                 " (ID INTEGER NOT NULL PRIMARY KEY, NAME VARCHAR, AGE INTEGER)");

        final Data data = Storage.resetData(pigServer);
        final Collection<Tuple> list = Lists.newArrayList();

        // Create input dataset
        int rows = 100;
        for (int i = 0; i < rows; i++) {
            Tuple t = tupleFactory.newTuple();
            t.append(i);
            t.append("a" + i);
            t.append(i * 2);
            list.add(t);
        }
        data.set("in", "id:int, name:chararray,age:int", list);

        pigServer.setBatchOn();
        pigServer.registerQuery("A = LOAD 'in' USING mock.Storage();");
        pigServer.registerQuery("B = FOREACH A GENERATE id,name;");
        pigServer.registerQuery("Store B into 'hbase://" + tableName + "/ID,NAME"
                               + "' using " + PhoenixHBaseStorage.class.getName() + "('"
                                + zkQuorum + "', '-batchSize 1000');");

         // Now run the Pig script
        if (pigServer.executeBatch().get(0).getStatus() != JOB_STATUS.COMPLETED) {
            throw new RuntimeException("Job failed", pigServer.executeBatch()
                    .get(0).getException());
        }

        // Compare data in Phoenix table to the expected
        final ResultSet rs = stmt
                .executeQuery("SELECT id, name,age FROM " + tableName + " ORDER BY id");

        for (int i = 0; i < rows; i++) {
            assertTrue(rs.next());
            assertEquals(i, rs.getInt(1));
            assertEquals("a" +  i, rs.getString(2));
            assertEquals(0, rs.getInt(3));
        }
    }
    
    /**
     * Test storage of DataByteArray columns to Phoenix
     * Maps the DataByteArray with the target PhoenixDataType and persists in HBase. 
    * @throws Exception
     */
    @Test
    public void testStoreWithBinaryDataTypes() throws Exception {
     
    	final String tableName = "TABLE3";
        final Statement stmt = conn.createStatement();

        stmt.execute("CREATE TABLE " + tableName +
                " (col1 BIGINT NOT NULL, col2 INTEGER , col3 FLOAT, col4 DOUBLE , col5 TINYINT , " +
                "  col6 BOOLEAN , col7 VARBINARY CONSTRAINT my_pk PRIMARY KEY (col1))");

        final Data data = Storage.resetData(pigServer);
        final Collection<Tuple> list = Lists.newArrayList();

        int rows = 10;
        for (int i = 1; i <= rows; i++) {
            Tuple t = tupleFactory.newTuple();
            t.append(i);
            t.append(new DataByteArray(Bytes.toBytes(i * 5)));
            t.append(new DataByteArray(Bytes.toBytes(i * 10.0F)));
            t.append(new DataByteArray(Bytes.toBytes(i * 15.0D)));
            t.append(new DataByteArray(Bytes.toBytes(i)));
            t.append(new DataByteArray(Bytes.toBytes( i % 2 == 0)));
            t.append(new DataByteArray(Bytes.toBytes(i)));
            list.add(t);
        }
        data.set("in", "col1:int,col2:bytearray,col3:bytearray,col4:bytearray,col5:bytearray,col6:bytearray,col7:bytearray ", list);

        pigServer.setBatchOn();
        pigServer.registerQuery("A = LOAD 'in' USING mock.Storage();");

        pigServer.registerQuery("Store A into 'hbase://" + tableName
                               + "' using " + PhoenixHBaseStorage.class.getName() + "('"
                                + zkQuorum + "', '-batchSize 1000');");

        if (pigServer.executeBatch().get(0).getStatus() != JOB_STATUS.COMPLETED) {
            throw new RuntimeException("Job failed", pigServer.executeBatch()
                    .get(0).getException());
        }

        final ResultSet rs = stmt
                .executeQuery(String.format("SELECT col1 , col2 , col3 , col4 , col5 , col6, col7  FROM %s ORDER BY col1" , tableName));

        int count = 0;
        for (int i = 1; i <= rows; i++) {
            assertTrue(rs.next());
            assertEquals(i, rs.getInt(1));
            assertEquals(i * 5, rs.getInt(2));
            assertEquals(i * 10.0F, rs.getFloat(3),0.0);
            assertEquals(i * 15.0D, rs.getInt(4),0.0);
            assertEquals(i,rs.getInt(5));
            assertEquals(i % 2 == 0, rs.getBoolean(6));
            assertArrayEquals(Bytes.toBytes(i), rs.getBytes(7));
            count++;
        }
        assertEquals(rows, count);
     }
    
    @Test
    public void testStoreWithDateTime() throws Exception {
     
    	final String tableName = "TABLE4";
        final Statement stmt = conn.createStatement();

        stmt.execute("CREATE TABLE " + tableName +
                " (col1 BIGINT NOT NULL, col2 DATE , col3 TIME, " +
                " col4 TIMESTAMP CONSTRAINT my_pk PRIMARY KEY (col1))");

        long now = System.currentTimeMillis();
        final DateTime dt = new DateTime(now);
        
        final Data data = Storage.resetData(pigServer);
        final Collection<Tuple> list = Lists.newArrayList();
        Tuple t = tupleFactory.newTuple();
        
        t.append(1);
        t.append(dt);
        t.append(dt);
        t.append(dt);
       
        list.add(t);
        
        data.set("in", "col1:int,col2:datetime,col3:datetime,col4:datetime", list);

        pigServer.setBatchOn();
        pigServer.registerQuery("A = LOAD 'in' USING mock.Storage();");

        pigServer.registerQuery("Store A into 'hbase://" + tableName
                               + "' using " + PhoenixHBaseStorage.class.getName() + "('"
                                + zkQuorum + "', '-batchSize 1000');");

        if (pigServer.executeBatch().get(0).getStatus() != JOB_STATUS.COMPLETED) {
            throw new RuntimeException("Job failed", pigServer.executeBatch()
                    .get(0).getException());
        }

        final ResultSet rs = stmt
                .executeQuery(String.format("SELECT col1 , col2 , col3 , col4 FROM %s " , tableName));

        assertTrue(rs.next());
        assertEquals(1, rs.getInt(1));
        assertEquals(now, rs.getDate(2).getTime());
        assertEquals(now, rs.getTime(3).getTime());
        assertEquals(now, rs.getTimestamp(4).getTime());
     
    }
}
