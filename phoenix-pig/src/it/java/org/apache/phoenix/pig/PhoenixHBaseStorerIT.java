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
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.Collection;

import org.apache.phoenix.end2end.BaseHBaseManagedTimeIT;
import org.apache.phoenix.end2end.HBaseManagedTimeTest;
import org.apache.pig.ExecType;
import org.apache.pig.PigServer;
import org.apache.pig.backend.executionengine.ExecJob.JOB_STATUS;
import org.apache.pig.builtin.mock.Storage;
import org.apache.pig.builtin.mock.Storage.Data;
import org.apache.pig.data.Tuple;
import org.apache.pig.data.TupleFactory;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import com.google.common.collect.Lists;

@Category(HBaseManagedTimeTest.class)
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
}
