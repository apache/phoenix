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

import static org.apache.phoenix.query.BaseTest.setUpConfigForMiniCluster;
import static org.apache.phoenix.query.QueryServices.DATE_FORMAT_ATTRIB;
import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.io.PrintWriter;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.phoenix.end2end.NeedsOwnMiniClusterTest;
import org.apache.phoenix.jdbc.PhoenixDriver;
import org.apache.phoenix.util.DateUtil;
import org.apache.phoenix.util.PhoenixRuntime;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;

@Category(NeedsOwnMiniClusterTest.class)
public class CsvBulkLoadToolIT {

    // We use HBaseTestUtil because we need to start up a MapReduce cluster as well
    private static HBaseTestingUtility hbaseTestUtil;
    private static String zkQuorum;
    private static Connection conn;

    @BeforeClass
    public static void setUp() throws Exception {
        hbaseTestUtil = new HBaseTestingUtility();
        Configuration conf = hbaseTestUtil.getConfiguration();
        setUpConfigForMiniCluster(conf);
        hbaseTestUtil.startMiniCluster();
        hbaseTestUtil.startMiniMapReduceCluster();

        Class.forName(PhoenixDriver.class.getName());
        zkQuorum = "localhost:" + hbaseTestUtil.getZkCluster().getClientPort();
        conn = DriverManager.getConnection(PhoenixRuntime.JDBC_PROTOCOL
                + PhoenixRuntime.JDBC_PROTOCOL_SEPARATOR + zkQuorum);
    }

    @AfterClass
    public static void tearDownAfterClass() throws Exception {
        try {
            conn.close();
        } finally {
            try {
                PhoenixDriver.INSTANCE.close();
            } finally {
                try {
                    DriverManager.deregisterDriver(PhoenixDriver.INSTANCE);
                } finally {                    
                    try {
                        hbaseTestUtil.shutdownMiniMapReduceCluster();
                    } finally {
                        hbaseTestUtil.shutdownMiniCluster();
                    }
                }
            }
        }
    }

    @Test
    public void testBasicImport() throws Exception {

        Statement stmt = conn.createStatement();
        stmt.execute("CREATE TABLE TABLE1 (ID INTEGER NOT NULL PRIMARY KEY, NAME VARCHAR, T DATE)");

        FileSystem fs = FileSystem.get(hbaseTestUtil.getConfiguration());
        FSDataOutputStream outputStream = fs.create(new Path("/tmp/input1.csv"));
        PrintWriter printWriter = new PrintWriter(outputStream);
        printWriter.println("1,Name 1,1970/01/01");
        printWriter.println("2,Name 2,1970/01/02");
        printWriter.close();

        CsvBulkLoadTool csvBulkLoadTool = new CsvBulkLoadTool();
        csvBulkLoadTool.setConf(new Configuration(hbaseTestUtil.getConfiguration()));
        csvBulkLoadTool.getConf().set(DATE_FORMAT_ATTRIB,"yyyy/MM/dd");
        int exitCode = csvBulkLoadTool.run(new String[] {
                "--input", "/tmp/input1.csv",
                "--table", "table1",
                "--zookeeper", zkQuorum});
        assertEquals(0, exitCode);

        ResultSet rs = stmt.executeQuery("SELECT id, name, t FROM table1 ORDER BY id");
        assertTrue(rs.next());
        assertEquals(1, rs.getInt(1));
        assertEquals("Name 1", rs.getString(2));
        assertEquals(DateUtil.parseDate("1970-01-01"), rs.getDate(3));
        assertTrue(rs.next());
        assertEquals(2, rs.getInt(1));
        assertEquals("Name 2", rs.getString(2));
        assertEquals(DateUtil.parseDate("1970-01-02"), rs.getDate(3));
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

    @Test
    public void testImportWithIndex() throws Exception {

        Statement stmt = conn.createStatement();
        stmt.execute("CREATE TABLE TABLE3 (ID INTEGER NOT NULL PRIMARY KEY, " +
            "FIRST_NAME VARCHAR, LAST_NAME VARCHAR)");
        String ddl = "CREATE INDEX TABLE3_IDX ON TABLE3 "
                + " (FIRST_NAME ASC)"
                + " INCLUDE (LAST_NAME)";
        stmt.execute(ddl);
        
        FileSystem fs = FileSystem.get(hbaseTestUtil.getConfiguration());
        FSDataOutputStream outputStream = fs.create(new Path("/tmp/input3.csv"));
        PrintWriter printWriter = new PrintWriter(outputStream);
        printWriter.println("1,FirstName 1,LastName 1");
        printWriter.println("2,FirstName 2,LastName 2");
        printWriter.close();

        CsvBulkLoadTool csvBulkLoadTool = new CsvBulkLoadTool();
        csvBulkLoadTool.setConf(hbaseTestUtil.getConfiguration());
        int exitCode = csvBulkLoadTool.run(new String[] {
                "--input", "/tmp/input3.csv",
                "--table", "table3",
                "--zookeeper", zkQuorum});
        assertEquals(0, exitCode);

        ResultSet rs = stmt.executeQuery("SELECT id, FIRST_NAME FROM TABLE3 where first_name='FirstName 2'");
        assertTrue(rs.next());
        assertEquals(2, rs.getInt(1));
        assertEquals("FirstName 2", rs.getString(2));

        rs.close();
        stmt.close();
    }

    @Test
    public void testImportWithLocalIndex() throws Exception {

        Statement stmt = conn.createStatement();
        stmt.execute("CREATE TABLE TABLE6 (ID INTEGER NOT NULL PRIMARY KEY, " +
                "FIRST_NAME VARCHAR, LAST_NAME VARCHAR)");
        String ddl = "CREATE LOCAL INDEX TABLE6_IDX ON TABLE6 "
                + " (FIRST_NAME ASC)";
        stmt.execute(ddl);

        FileSystem fs = FileSystem.get(hbaseTestUtil.getConfiguration());
        FSDataOutputStream outputStream = fs.create(new Path("/tmp/input3.csv"));
        PrintWriter printWriter = new PrintWriter(outputStream);
        printWriter.println("1,FirstName 1,LastName 1");
        printWriter.println("2,FirstName 2,LastName 2");
        printWriter.close();

        CsvBulkLoadTool csvBulkLoadTool = new CsvBulkLoadTool();
        csvBulkLoadTool.setConf(hbaseTestUtil.getConfiguration());
        int exitCode = csvBulkLoadTool.run(new String[] {
                "--input", "/tmp/input3.csv",
                "--table", "table6",
                "--zookeeper", zkQuorum});
        assertEquals(0, exitCode);

        ResultSet rs = stmt.executeQuery("SELECT id, FIRST_NAME FROM TABLE6 where first_name='FirstName 2'");
        assertTrue(rs.next());
        assertEquals(2, rs.getInt(1));
        assertEquals("FirstName 2", rs.getString(2));

        rs.close();
        stmt.close();
    }

    @Test
    public void testImportOneIndexTable() throws Exception {
        testImportOneIndexTable("TABLE4", false);
    }

    @Test
    public void testImportOneLocalIndexTable() throws Exception {
        testImportOneIndexTable("TABLE5", true);
    }

    public void testImportOneIndexTable(String tableName, boolean localIndex) throws Exception {

        String indexTableName = String.format("%s_IDX", tableName);
        Statement stmt = conn.createStatement();
        stmt.execute("CREATE TABLE " + tableName + "(ID INTEGER NOT NULL PRIMARY KEY, "
                + "FIRST_NAME VARCHAR, LAST_NAME VARCHAR)");
        String ddl =
                "CREATE " + (localIndex ? "LOCAL" : "") + " INDEX " + indexTableName + " ON "
                        + tableName + "(FIRST_NAME ASC)";
        stmt.execute(ddl);

        FileSystem fs = FileSystem.get(hbaseTestUtil.getConfiguration());
        FSDataOutputStream outputStream = fs.create(new Path("/tmp/input4.csv"));
        PrintWriter printWriter = new PrintWriter(outputStream);
        printWriter.println("1,FirstName 1,LastName 1");
        printWriter.println("2,FirstName 2,LastName 2");
        printWriter.close();

        CsvBulkLoadTool csvBulkLoadTool = new CsvBulkLoadTool();
        csvBulkLoadTool.setConf(hbaseTestUtil.getConfiguration());
        int exitCode = csvBulkLoadTool.run(new String[] {
                "--input", "/tmp/input4.csv",
                "--table", tableName,
                "--index-table", indexTableName,
                "--zookeeper", zkQuorum });
        assertEquals(0, exitCode);

        ResultSet rs = stmt.executeQuery("SELECT * FROM " + tableName);
        assertFalse(rs.next());
        rs = stmt.executeQuery("SELECT FIRST_NAME FROM " + tableName + " where FIRST_NAME='FirstName 1'");
        assertTrue(rs.next());
        assertEquals("FirstName 1", rs.getString(1));

        rs.close();
        stmt.close();
    }
    
}
