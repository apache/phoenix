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
package org.apache.phoenix.mapreduce;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.phoenix.jdbc.PhoenixDriver;
import org.apache.phoenix.util.ConfigUtil;
import org.apache.phoenix.util.PhoenixRuntime;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.PrintWriter;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class CsvBulkLoadToolIT {

    // We use HBaseTestUtil because we need to start up a MapReduce cluster as well
    private static HBaseTestingUtility hbaseTestUtil;
    private static String zkQuorum;
    private static Connection conn;

    @BeforeClass
    public static void setUp() throws Exception {
        hbaseTestUtil = new HBaseTestingUtility();
        Configuration conf = hbaseTestUtil.getConfiguration();
        ConfigUtil.setReplicationConfigIfAbsent(conf);
        hbaseTestUtil.startMiniCluster();
        hbaseTestUtil.startMiniMapReduceCluster();

        Class.forName(PhoenixDriver.class.getName());
        zkQuorum = "localhost:" + hbaseTestUtil.getZkCluster().getClientPort();
        conn = DriverManager.getConnection(PhoenixRuntime.JDBC_PROTOCOL
                + PhoenixRuntime.JDBC_PROTOCOL_SEPARATOR + zkQuorum);
    }

    @AfterClass
    public static void tearDownAfterClass() throws Exception {
        conn.close();
        PhoenixDriver.INSTANCE.close();
        hbaseTestUtil.shutdownMiniMapReduceCluster();
        hbaseTestUtil.shutdownMiniCluster();
    }

    @Test
    public void testBasicImport() throws Exception {

        Statement stmt = conn.createStatement();
        stmt.execute("CREATE TABLE TABLE1 (ID INTEGER NOT NULL PRIMARY KEY, NAME VARCHAR)");

        FileSystem fs = FileSystem.get(hbaseTestUtil.getConfiguration());
        FSDataOutputStream outputStream = fs.create(new Path("/tmp/input1.csv"));
        PrintWriter printWriter = new PrintWriter(outputStream);
        printWriter.println("1,Name 1");
        printWriter.println("2,Name 2");
        printWriter.close();

        CsvBulkLoadTool csvBulkLoadTool = new CsvBulkLoadTool();
        csvBulkLoadTool.setConf(hbaseTestUtil.getConfiguration());
        int exitCode = csvBulkLoadTool.run(new String[] {
                "--input", "/tmp/input1.csv",
                "--table", "table1",
                "--zookeeper", zkQuorum});
        assertEquals(0, exitCode);

        ResultSet rs = stmt.executeQuery("SELECT id, name FROM table1 ORDER BY id");
        assertTrue(rs.next());
        assertEquals(1, rs.getInt(1));
        assertEquals("Name 1", rs.getString(2));
        assertTrue(rs.next());
        assertEquals(2, rs.getInt(1));
        assertEquals("Name 2", rs.getString(2));
        assertFalse(rs.next());

        rs.close();
        stmt.close();
    }

    @Test
    public void testFullOptionImport() throws Exception {

        Statement stmt = conn.createStatement();
        stmt.execute("CREATE TABLE TABLE2 (ID INTEGER NOT NULL PRIMARY KEY, " +
                "NAME VARCHAR, NAMES VARCHAR ARRAY)");

        FileSystem fs = FileSystem.get(hbaseTestUtil.getConfiguration());
        FSDataOutputStream outputStream = fs.create(new Path("/tmp/input2.csv"));
        PrintWriter printWriter = new PrintWriter(outputStream);
        printWriter.println("1|Name 1a;Name 1b");
        printWriter.println("2|Name 2a;Name 2b");
        printWriter.close();

        CsvBulkLoadTool csvBulkLoadTool = new CsvBulkLoadTool();
        csvBulkLoadTool.setConf(hbaseTestUtil.getConfiguration());
        int exitCode = csvBulkLoadTool.run(new String[] {
                "--input", "/tmp/input2.csv",
                "--table", "table2",
                "--zookeeper", zkQuorum,
                "--delimiter", "|",
                "--array-delimiter", ";",
                "--import-columns", "ID,NAMES"});
        assertEquals(0, exitCode);

        ResultSet rs = stmt.executeQuery("SELECT id, names FROM table2 ORDER BY id");
        assertTrue(rs.next());
        assertEquals(1, rs.getInt(1));
        assertArrayEquals(new Object[] { "Name 1a", "Name 1b" }, (Object[]) rs.getArray(2).getArray());
        assertTrue(rs.next());
        assertEquals(2, rs.getInt(1));
        assertArrayEquals(new Object[] { "Name 2a", "Name 2b" }, (Object[]) rs.getArray(2).getArray());
        assertFalse(rs.next());

        rs.close();
        stmt.close();
    }
}
