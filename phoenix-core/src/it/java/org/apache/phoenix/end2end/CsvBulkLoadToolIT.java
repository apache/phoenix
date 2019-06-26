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
package org.apache.phoenix.end2end;

import static org.apache.phoenix.query.QueryServices.DATE_FORMAT_ATTRIB;
import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.PrintWriter;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapred.FileAlreadyExistsException;
import org.apache.phoenix.jdbc.PhoenixConnection;
import org.apache.phoenix.mapreduce.CsvBulkLoadTool;
import org.apache.phoenix.schema.ColumnNotFoundException;
import org.apache.phoenix.schema.PTable;
import org.apache.phoenix.schema.PTableKey;
import org.apache.phoenix.util.DateUtil;
import org.apache.phoenix.util.PhoenixRuntime;
import org.apache.phoenix.util.ReadOnlyProps;
import org.apache.phoenix.util.TestUtil;
import org.junit.BeforeClass;
import org.junit.Test;

public class CsvBulkLoadToolIT extends BaseOwnClusterIT {

    private static Connection conn;
    private static String zkQuorum;

    @BeforeClass
    public static void doSetup() throws Exception {
        setUpTestDriver(ReadOnlyProps.EMPTY_PROPS);
        zkQuorum = TestUtil.LOCALHOST + PhoenixRuntime.JDBC_PROTOCOL_SEPARATOR + getUtility().getZkCluster().getClientPort();
        conn = DriverManager.getConnection(getUrl());
    }

    @Test
    public void testBasicImport() throws Exception {

        Statement stmt = conn.createStatement();
        stmt.execute("CREATE TABLE S.TABLE1 (ID INTEGER NOT NULL PRIMARY KEY, NAME VARCHAR, T DATE) SPLIT ON (1,2)");

        FileSystem fs = FileSystem.get(getUtility().getConfiguration());
        FSDataOutputStream outputStream = fs.create(new Path("/tmp/input1.csv"));
        PrintWriter printWriter = new PrintWriter(outputStream);
        printWriter.println("1,Name 1,1970/01/01");
        printWriter.println("2,Name 2,1970/01/02");
        printWriter.close();

        CsvBulkLoadTool csvBulkLoadTool = new CsvBulkLoadTool();
        csvBulkLoadTool.setConf(new Configuration(getUtility().getConfiguration()));
        csvBulkLoadTool.getConf().set(DATE_FORMAT_ATTRIB,"yyyy/MM/dd");
        int exitCode = csvBulkLoadTool.run(new String[] {
                "--input", "/tmp/input1.csv",
                "--table", "table1",
                "--schema", "s",
                "--zookeeper", zkQuorum});
        assertEquals(0, exitCode);

        ResultSet rs = stmt.executeQuery("SELECT id, name, t FROM s.table1 ORDER BY id");
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
    public void testImportWithRowTimestamp() throws Exception {

        Statement stmt = conn.createStatement();
        stmt.execute("CREATE TABLE S.TABLE9 (ID INTEGER NOT NULL , NAME VARCHAR, T DATE NOT NULL," +
                " " +
                "CONSTRAINT PK PRIMARY KEY (ID, T ROW_TIMESTAMP))");

        FileSystem fs = FileSystem.get(getUtility().getConfiguration());
        FSDataOutputStream outputStream = fs.create(new Path("/tmp/input1.csv"));
        PrintWriter printWriter = new PrintWriter(outputStream);
        printWriter.println("1,Name 1,1970/01/01");
        printWriter.println("2,Name 2,1971/01/01");
        printWriter.println("3,Name 2,1972/01/01");
        printWriter.close();

        CsvBulkLoadTool csvBulkLoadTool = new CsvBulkLoadTool();
        csvBulkLoadTool.setConf(new Configuration(getUtility().getConfiguration()));
        csvBulkLoadTool.getConf().set(DATE_FORMAT_ATTRIB,"yyyy/MM/dd");
        int exitCode = csvBulkLoadTool.run(new String[] {
                "--input", "/tmp/input1.csv",
                "--table", "table9",
                "--schema", "s",
                "--zookeeper", zkQuorum});
        assertEquals(0, exitCode);

        ResultSet rs = stmt.executeQuery("SELECT id, name, t FROM s.table9 WHERE T < to_date" +
                "('1972-01-01') AND T > to_date('1970-01-01') ORDER BY id");
        assertTrue(rs.next());
        assertEquals(2, rs.getInt(1));
        assertEquals("Name 2", rs.getString(2));
        assertEquals(DateUtil.parseDate("1971-01-01"), rs.getDate(3));
        assertFalse(rs.next());

        rs.close();
        stmt.close();
    }


    @Test
    public void testImportWithTabs() throws Exception {

        Statement stmt = conn.createStatement();
        stmt.execute("CREATE TABLE TABLE8 (ID INTEGER NOT NULL PRIMARY KEY, " +
                "NAME1 VARCHAR, NAME2 VARCHAR)");

        FileSystem fs = FileSystem.get(getUtility().getConfiguration());
        FSDataOutputStream outputStream = fs.create(new Path("/tmp/input8.csv"));
        PrintWriter printWriter = new PrintWriter(outputStream);
        printWriter.println("1\tName 1a\tName 2a");
        printWriter.println("2\tName 2a\tName 2b");
        printWriter.close();

        CsvBulkLoadTool csvBulkLoadTool = new CsvBulkLoadTool();
        csvBulkLoadTool.setConf(getUtility().getConfiguration());
        int exitCode = csvBulkLoadTool.run(new String[] {
                "--input", "/tmp/input8.csv",
                "--table", "table8",
                "--zookeeper", zkQuorum,
                "--delimiter", "\\t"});
        assertEquals(0, exitCode);

        ResultSet rs = stmt.executeQuery("SELECT id, name1, name2 FROM table8 ORDER BY id");
        assertTrue(rs.next());
        assertEquals(1, rs.getInt(1));
        assertEquals("Name 1a", rs.getString(2));

        rs.close();
        stmt.close();
    }

    @Test
    public void testFullOptionImport() throws Exception {

        Statement stmt = conn.createStatement();
        stmt.execute("CREATE TABLE TABLE2 (ID INTEGER NOT NULL PRIMARY KEY, " +
                "NAME VARCHAR, NAMES VARCHAR ARRAY)");

        FileSystem fs = FileSystem.get(getUtility().getConfiguration());
        FSDataOutputStream outputStream = fs.create(new Path("/tmp/input2.csv"));
        PrintWriter printWriter = new PrintWriter(outputStream);
        printWriter.println("1|Name 1a;Name 1b");
        printWriter.println("2|Name 2a;Name 2b");
        printWriter.close();

        CsvBulkLoadTool csvBulkLoadTool = new CsvBulkLoadTool();
        csvBulkLoadTool.setConf(getUtility().getConfiguration());
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
    public void testMultipleInputFiles() throws Exception {

        Statement stmt = conn.createStatement();
        stmt.execute("CREATE TABLE TABLE7 (ID INTEGER NOT NULL PRIMARY KEY, NAME VARCHAR, T DATE) SPLIT ON (1,2)");

        FileSystem fs = FileSystem.get(getUtility().getConfiguration());
        FSDataOutputStream outputStream = fs.create(new Path("/tmp/input1.csv"));
        PrintWriter printWriter = new PrintWriter(outputStream);
        printWriter.println("1,Name 1,1970/01/01");
        printWriter.close();
        outputStream = fs.create(new Path("/tmp/input2.csv"));
        printWriter = new PrintWriter(outputStream);
        printWriter.println("2,Name 2,1970/01/02");
        printWriter.close();

        CsvBulkLoadTool csvBulkLoadTool = new CsvBulkLoadTool();
        csvBulkLoadTool.setConf(new Configuration(getUtility().getConfiguration()));
        csvBulkLoadTool.getConf().set(DATE_FORMAT_ATTRIB,"yyyy/MM/dd");
        int exitCode = csvBulkLoadTool.run(new String[] {
            "--input", "/tmp/input1.csv,/tmp/input2.csv",
            "--table", "table7",
            "--zookeeper", zkQuorum});
        assertEquals(0, exitCode);

        ResultSet rs = stmt.executeQuery("SELECT id, name, t FROM table7 ORDER BY id");
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
    public void testImportWithIndex() throws Exception {

        Statement stmt = conn.createStatement();
        stmt.execute("CREATE TABLE TABLE3 (ID INTEGER NOT NULL PRIMARY KEY, " +
            "FIRST_NAME VARCHAR, LAST_NAME VARCHAR)");
        String ddl = "CREATE INDEX TABLE3_IDX ON TABLE3 "
                + " (FIRST_NAME ASC)"
                + " INCLUDE (LAST_NAME)";
        stmt.execute(ddl);
        
        FileSystem fs = FileSystem.get(getUtility().getConfiguration());
        FSDataOutputStream outputStream = fs.create(new Path("/tmp/input3.csv"));
        PrintWriter printWriter = new PrintWriter(outputStream);
        printWriter.println("1,FirstName 1,LastName 1");
        printWriter.println("2,FirstName 2,LastName 2");
        printWriter.close();

        CsvBulkLoadTool csvBulkLoadTool = new CsvBulkLoadTool();
        csvBulkLoadTool.setConf(getUtility().getConfiguration());
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
                "FIRST_NAME VARCHAR, LAST_NAME VARCHAR) SPLIt ON (1,2)");
        String ddl = "CREATE LOCAL INDEX TABLE6_IDX ON TABLE6 "
                + " (FIRST_NAME ASC)";
        stmt.execute(ddl);
        ddl = "CREATE LOCAL INDEX TABLE6_IDX2 ON TABLE6 " + " (LAST_NAME ASC)";
        stmt.execute(ddl);

        FileSystem fs = FileSystem.get(getUtility().getConfiguration());
        FSDataOutputStream outputStream = fs.create(new Path("/tmp/input3.csv"));
        PrintWriter printWriter = new PrintWriter(outputStream);
        printWriter.println("1,FirstName 1,LastName 1");
        printWriter.println("2,FirstName 2,LastName 2");
        printWriter.close();

        CsvBulkLoadTool csvBulkLoadTool = new CsvBulkLoadTool();
        csvBulkLoadTool.setConf(getUtility().getConfiguration());
        int exitCode = csvBulkLoadTool.run(new String[] {
                "--input", "/tmp/input3.csv",
                "--table", "table6",
                "--zookeeper", zkQuorum});
        assertEquals(0, exitCode);

        ResultSet rs = stmt.executeQuery("SELECT id, FIRST_NAME FROM TABLE6 where first_name='FirstName 2'");
        assertTrue(rs.next());
        assertEquals(2, rs.getInt(1));
        assertEquals("FirstName 2", rs.getString(2));

        rs = stmt.executeQuery("SELECT LAST_NAME FROM TABLE6  where last_name='LastName 1'");
        assertTrue(rs.next());
        assertEquals("LastName 1", rs.getString(1));

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

        FileSystem fs = FileSystem.get(getUtility().getConfiguration());
        FSDataOutputStream outputStream = fs.create(new Path("/tmp/input4.csv"));
        PrintWriter printWriter = new PrintWriter(outputStream);
        printWriter.println("1,FirstName 1,LastName 1");
        printWriter.println("2,FirstName 2,LastName 2");
        printWriter.close();

        CsvBulkLoadTool csvBulkLoadTool = new CsvBulkLoadTool();
        csvBulkLoadTool.setConf(getUtility().getConfiguration());
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
    
    @Test
    public void testInvalidArguments() {
        String tableName = "TABLE8";
        CsvBulkLoadTool csvBulkLoadTool = new CsvBulkLoadTool();
        csvBulkLoadTool.setConf(getUtility().getConfiguration());
        try {
            csvBulkLoadTool.run(new String[] {
                "--input", "/tmp/input4.csv",
                "--table", tableName,
                "--zookeeper", zkQuorum });
            fail(String.format("Table %s not created, hence should fail",tableName));
        } catch (Exception ex) {
            assertTrue(ex instanceof IllegalArgumentException); 
            assertTrue(ex.getMessage().contains(String.format("Table %s not found", tableName)));
        }
    }
    
    @Test
    public void testAlreadyExistsOutputPath() {
        String tableName = "TABLE9";
        String outputPath = "/tmp/output/tabl9";
        try {
            Statement stmt = conn.createStatement();
            stmt.execute("CREATE TABLE " + tableName + "(ID INTEGER NOT NULL PRIMARY KEY, "
                    + "FIRST_NAME VARCHAR, LAST_NAME VARCHAR)");
            
            FileSystem fs = FileSystem.get(getUtility().getConfiguration());
            fs.create(new Path(outputPath));
            FSDataOutputStream outputStream = fs.create(new Path("/tmp/input9.csv"));
            PrintWriter printWriter = new PrintWriter(outputStream);
            printWriter.println("1,FirstName 1,LastName 1");
            printWriter.println("2,FirstName 2,LastName 2");
            printWriter.close();
            
            CsvBulkLoadTool csvBulkLoadTool = new CsvBulkLoadTool();
            csvBulkLoadTool.setConf(getUtility().getConfiguration());
            csvBulkLoadTool.run(new String[] {
                "--input", "/tmp/input9.csv",
                "--output", outputPath,
                "--table", tableName,
                "--zookeeper", zkQuorum });
            
            fail(String.format("Output path %s already exists. hence, should fail",outputPath));
        } catch (Exception ex) {
            assertTrue(ex instanceof FileAlreadyExistsException); 
        }
    }

    @Test
    public void testImportInImmutableTable() throws Exception {
        Statement stmt = conn.createStatement();
        stmt.execute("CREATE IMMUTABLE TABLE S.TABLE10 (ID INTEGER NOT NULL PRIMARY KEY, NAME VARCHAR, T DATE, CF1.T2 DATE, CF2.T3 DATE) ");

        FileSystem fs = FileSystem.get(getUtility().getConfiguration());
        FSDataOutputStream outputStream = fs.create(new Path("/tmp/input10.csv"));
        PrintWriter printWriter = new PrintWriter(outputStream);
        printWriter.println("1,Name 1,1970/01/01,1970/02/01,1970/03/01");
        printWriter.println("2,Name 2,1970/01/02,1970/02/02,1970/03/02");
        printWriter.close();
        CsvBulkLoadTool csvBulkLoadTool = new CsvBulkLoadTool();
        csvBulkLoadTool.setConf(new Configuration(getUtility().getConfiguration()));
        csvBulkLoadTool.getConf().set(DATE_FORMAT_ATTRIB, "yyyy/MM/dd");
        int exitCode = csvBulkLoadTool.run(new String[] { "--input", "/tmp/input10.csv", "--table", "table10",
                "--schema", "s", "--zookeeper", zkQuorum });
        assertEquals(0, exitCode);
        ResultSet rs = stmt.executeQuery("SELECT id, name, t, CF1.T2, CF2.T3 FROM s.table10 ORDER BY id");
        assertTrue(rs.next());
        assertEquals(1, rs.getInt(1));
        assertEquals("Name 1", rs.getString(2));
        assertEquals(DateUtil.parseDate("1970-01-01"), rs.getDate(3));
        assertEquals(DateUtil.parseDate("1970-02-01"), rs.getDate(4));
        assertEquals(DateUtil.parseDate("1970-03-01"), rs.getDate(5));
        assertTrue(rs.next());
        assertEquals(2, rs.getInt(1));
        assertEquals("Name 2", rs.getString(2));
        assertEquals(DateUtil.parseDate("1970-01-02"), rs.getDate(3));
        assertEquals(DateUtil.parseDate("1970-02-02"), rs.getDate(4));
        assertEquals(DateUtil.parseDate("1970-03-02"), rs.getDate(5));
        assertFalse(rs.next());

        rs.close();
        stmt.close();
    }

    /**
     * This test case validates the import using CsvBulkLoadTool in
     * SingleCellArrayWithOffsets table.
     * PHOENIX-4872
     */

    @Test
    public void testImportInSingleCellArrayWithOffsetsTable() throws Exception {
        Statement stmt = conn.createStatement();
        stmt.execute("CREATE IMMUTABLE TABLE S.TABLE12 (ID INTEGER NOT NULL PRIMARY KEY," +
                " CF0.NAME VARCHAR, CF0.T DATE, CF1.T2 DATE, CF2.T3 DATE) " +
                "IMMUTABLE_STORAGE_SCHEME=SINGLE_CELL_ARRAY_WITH_OFFSETS");
        PhoenixConnection phxConn = conn.unwrap(PhoenixConnection.class);
        PTable table = phxConn.getTable(new PTableKey(null, "S.TABLE12"));

        assertEquals(PTable.ImmutableStorageScheme.SINGLE_CELL_ARRAY_WITH_OFFSETS,
                table.getImmutableStorageScheme());

        FileSystem fs = FileSystem.get(getUtility().getConfiguration());
        FSDataOutputStream outputStream = fs.create(new Path("/tmp/inputSCAWO.csv"));
        PrintWriter printWriter = new PrintWriter(outputStream);
        printWriter.println("1,Name 1,1970/01/01,1970/02/01,1970/03/01");
        printWriter.println("2,Name 2,1970/01/02,1970/02/02,1970/03/02");
        printWriter.println("3,Name 1,1970/01/01,1970/02/03,1970/03/01");
        printWriter.println("4,Name 2,1970/01/02,1970/02/04,1970/03/02");
        printWriter.println("5,Name 1,1970/01/01,1970/02/05,1970/03/01");
        printWriter.println("6,Name 2,1970/01/02,1970/02/06,1970/03/02");
        printWriter.close();

        CsvBulkLoadTool csvBulkLoadTool = new CsvBulkLoadTool();
        csvBulkLoadTool.setConf(new Configuration(getUtility().getConfiguration()));
        csvBulkLoadTool.getConf().set(DATE_FORMAT_ATTRIB,"yyyy/MM/dd");
        int exitCode = csvBulkLoadTool.run(new String[] {
                "--input", "/tmp/inputSCAWO.csv",
                "--table", "table12",
                "--schema", "s",
                "--zookeeper", zkQuorum});
        assertEquals(0, exitCode);

        ResultSet rs = stmt.executeQuery("SELECT COUNT(1) FROM S.TABLE12");
        assertTrue(rs.next());
        assertEquals(6, rs.getInt(1));

        rs.close();
        stmt.close();

    }

    @Test
    public void testIgnoreCsvHeader() throws Exception {
        try (Statement stmt = conn.createStatement()) {
            stmt.execute("CREATE TABLE S.TABLE13 (ID INTEGER NOT NULL PRIMARY KEY, NAME VARCHAR)");

            final Configuration conf = new Configuration(getUtility().getConfiguration());
            FileSystem fs = FileSystem.get(getUtility().getConfiguration());
            FSDataOutputStream outputStream = fs.create(new Path("/tmp/input13.csv"));
            try (PrintWriter printWriter = new PrintWriter(outputStream)) {
                printWriter.println("id,name");
                printWriter.println("1,Name 1");
                printWriter.println("2,Name 2");
                printWriter.println("3,Name 3");
            }

            CsvBulkLoadTool csvBulkLoadTool = new CsvBulkLoadTool();
            csvBulkLoadTool.setConf(conf);
            int exitCode = csvBulkLoadTool.run(new String[] {
                    "--input", "/tmp/input13.csv",
                    "--table", "table13",
                    "--schema", "s",
                    "--zookeeper", zkQuorum,
                    "--skip-header"});
            assertEquals(0, exitCode);

            try (ResultSet rs = stmt.executeQuery("SELECT COUNT(1) FROM S.TABLE13")) {
                assertTrue(rs.next());
                assertEquals(3, rs.getInt(1));
                assertFalse(rs.next());
            }
        }
    }

    @Test
    public void testHeaderAndSkipHeaderAbsent() throws Exception {
        try (Statement stmt = conn.createStatement()) {
            stmt.execute("CREATE TABLE S.TABLE14 (\"id\" INTEGER NOT NULL PRIMARY KEY, \"name\" VARCHAR, \"type\" VARCHAR)");

            final Configuration conf = new Configuration(getUtility().getConfiguration());
            FileSystem fs = FileSystem.get(getUtility().getConfiguration());
            FSDataOutputStream outputStream1 = fs.create(new Path("/tmp/input14.csv"));
            try (PrintWriter printWriter = new PrintWriter(outputStream1)) {
                printWriter.println("id,name");
                printWriter.println("1,Name 1");
                printWriter.println("2,Name 2");
                printWriter.println("3,Name 3");
            }
            CsvBulkLoadTool csvBulkLoadTool = new CsvBulkLoadTool();
            csvBulkLoadTool.setConf(conf);

            // If the --parse-header or --skip-header is not passed, the upsert executor
            // should fail as the total number of columns in CSV is less than that defined
            // in table
            int exitCode = csvBulkLoadTool.run(new String[]{
                    "--input", "/tmp/input14.csv",
                    "--table", "table14",
                    "--schema", "s",
                    "--zookeeper", zkQuorum});
            assertEquals(-1, exitCode);
            try (ResultSet rs = stmt.executeQuery("SELECT COUNT(1) FROM S.TABLE14")) {
                assertTrue(rs.next());
                assertEquals(0, rs.getInt(1));
                assertFalse(rs.next());
            }
        }
    }

    @Test
    public void testParseCsvHeaderAsInputColumns() throws Exception {
        try (Statement stmt = conn.createStatement()) {
            stmt.execute("CREATE TABLE S.TABLE15 (\"id\" INTEGER NOT NULL PRIMARY KEY, \"name\" VARCHAR, \"type\" VARCHAR, \"category\" VARCHAR)");

            final Configuration conf = new Configuration(getUtility().getConfiguration());
            FileSystem fs = FileSystem.get(getUtility().getConfiguration());
            FSDataOutputStream outputStream1 = fs.create(new Path("/tmp/input15.csv"));
            try (PrintWriter printWriter = new PrintWriter(outputStream1)) {
                printWriter.println("id,name");
                printWriter.println("1,Name 1");
                printWriter.println("2,Name 2");
                printWriter.println("3,Name 3");
            }
            CsvBulkLoadTool csvBulkLoadTool = new CsvBulkLoadTool();
            csvBulkLoadTool.setConf(conf);

            // Should ingest data successfully if --parse-header arg is passed
            int exitCode = csvBulkLoadTool.run(new String[] {
                    "--input", "/tmp/input15.csv",
                    "--table", "table15",
                    "--schema", "s",
                    "--parse-header",
                    "--zookeeper", zkQuorum});
            assertEquals(0, exitCode);

            try (ResultSet rs = stmt.executeQuery("SELECT COUNT(1) FROM S.TABLE15")) {
                assertTrue(rs.next());
                assertEquals(3, rs.getInt(1));
                assertFalse(rs.next());
            }
        }
    }

    @Test
    public void testParseHeaderMultipleFiles() throws Exception {
        try (Statement stmt = conn.createStatement()) {
            stmt.execute("CREATE TABLE S.TABLE16 (\"id\" INTEGER NOT NULL PRIMARY KEY, \"name\" VARCHAR, \"type\" VARCHAR)");

            final Configuration conf = new Configuration(getUtility().getConfiguration());
            FileSystem fs = FileSystem.get(getUtility().getConfiguration());
            FSDataOutputStream outputStream1 = fs.create(new Path("/tmp/input16-1.csv"));
            FSDataOutputStream outputStream2 = fs.create(new Path("/tmp/input16-2.csv"));
            try (PrintWriter printWriter = new PrintWriter(outputStream1)) {
                printWriter.println("id,name");
                printWriter.println("1,Name 1");
                printWriter.println("2,Name 2");
                printWriter.println("3,Name 3");
            }
            try (PrintWriter printWriter = new PrintWriter(outputStream2)) {
                printWriter.println("id,name");
                printWriter.println("4,Name 4");
                printWriter.println("2,Name 5");
                printWriter.println("5,Name 6");
            }
            CsvBulkLoadTool csvBulkLoadTool = new CsvBulkLoadTool();
            csvBulkLoadTool.setConf(conf);

            // Should pass if there are multiple input paths provided having same headers
            int exitCode = csvBulkLoadTool.run(new String[] {
                    "--input", "/tmp/input16-1.csv,/tmp/input16-2.csv",
                    "--table", "table16",
                    "--schema", "s",
                    "--parse-header",
                    "--zookeeper", zkQuorum});
            assertEquals(0, exitCode);

            try (ResultSet rs = stmt.executeQuery("SELECT COUNT(1) FROM S.TABLE16")) {
                assertTrue(rs.next());
                assertEquals(5, rs.getInt(1));
                assertFalse(rs.next());
            }
        }
    }

    @Test
    public void testParseHeaderMultipleFilesFailure() throws Exception {
        try (Statement stmt = conn.createStatement()) {
            stmt.execute("CREATE TABLE S.TABLE17 (\"id\" INTEGER NOT NULL PRIMARY KEY, \"name\" VARCHAR, \"type\" VARCHAR)");

            final Configuration conf = new Configuration(getUtility().getConfiguration());
            FileSystem fs = FileSystem.get(getUtility().getConfiguration());
            FSDataOutputStream outputStream1 = fs.create(new Path("/tmp/input17-1.csv"));
            FSDataOutputStream outputStream2 = fs.create(new Path("/tmp/input17-2.csv"));
            try (PrintWriter printWriter = new PrintWriter(outputStream1)) {
                printWriter.println("id,name");
                printWriter.println("1,Name 1");
                printWriter.println("2,Name 2");
                printWriter.println("3,Name 3");
            }
            try (PrintWriter printWriter = new PrintWriter(outputStream2)) {
                printWriter.println("id,name,type");
                printWriter.println("1,Name 1,Type 1");
                printWriter.println("2,Name 2,Type 2");
                printWriter.println("4,Name 4,Type 4");
            }
            CsvBulkLoadTool csvBulkLoadTool = new CsvBulkLoadTool();
            csvBulkLoadTool.setConf(conf);

            // Should fail if there are multitple input paths having different headers in
            // either of them.  Here, input17-1 and input17-2 have different headers
            try {
                csvBulkLoadTool.run(new String[] {
                        "--input", "/tmp/input17-1.csv,/tmp/input17-2.csv",
                        "--table", "table17",
                        "--schema", "s",
                        "--parse-header",
                        "--zookeeper", zkQuorum});
                fail("Random error message");
            } catch (Exception ex) {
                assertTrue(ex instanceof IllegalArgumentException);
                assertTrue(ex.getMessage().contains(
                        "Headers in provided input files are different. Headers must be unique for all input files"
                ));
            }
        }
    }

    @Test
    public void testParseCsvHeaderWithoutHeaderOption() throws Exception {
        try (Statement stmt = conn.createStatement()) {
            stmt.execute("CREATE TABLE S.TABLE18 (\"id\" INTEGER NOT NULL PRIMARY KEY, \"name\" VARCHAR, \"type\" VARCHAR)");

            final Configuration conf = new Configuration(getUtility().getConfiguration());
            FileSystem fs = FileSystem.get(getUtility().getConfiguration());
            FSDataOutputStream outputStream1 = fs.create(new Path("/tmp/input18.csv"));
            try (PrintWriter printWriter = new PrintWriter(outputStream1)) {
                printWriter.println("4,Name 4");
                printWriter.println("2,Name 5");
                printWriter.println("5,Name 6");
            }
            CsvBulkLoadTool csvBulkLoadTool = new CsvBulkLoadTool();
            csvBulkLoadTool.setConf(conf);

            // Should fail if the input file does not contain the header and --parse-header
            // option is also passed.
            try {
                csvBulkLoadTool.run(new String[] {
                        "--input", "/tmp/input18.csv",
                        "--table", "table18",
                        "--schema", "s",
                        "--parse-header",
                        "--zookeeper", zkQuorum});
                fail("Random error message");
            } catch (Exception ex) {
                assertTrue(ex instanceof ColumnNotFoundException);
                assertTrue(ex.getMessage().contains(
                        "Undefined column. columnName=S.TABLE18.4"
                ));
            }
        }
    }

    @Test
    public void testBothParseHeaderSkipHeaderPassed() throws Exception {
        try (Statement stmt = conn.createStatement()) {
            stmt.execute("CREATE TABLE S.TABLE19 (\"id\" INTEGER NOT NULL PRIMARY KEY, \"name\" VARCHAR, \"type\" VARCHAR)");

            final Configuration conf = new Configuration(getUtility().getConfiguration());
            FileSystem fs = FileSystem.get(getUtility().getConfiguration());
            FSDataOutputStream outputStream1 = fs.create(new Path("/tmp/input19.csv"));
            try (PrintWriter printWriter = new PrintWriter(outputStream1)) {
                printWriter.println("id,name");
                printWriter.println("1,Name 1");
                printWriter.println("2,Name 2");
                printWriter.println("3,Name 3");
            }
            CsvBulkLoadTool csvBulkLoadTool = new CsvBulkLoadTool();
            csvBulkLoadTool.setConf(conf);

            // Should fail if both parse-header and skip-header is passed.
            try {
                csvBulkLoadTool.run(new String[] {
                        "--input", "/tmp/input19.csv",
                        "--table", "table19",
                        "--schema", "s",
                        "--parse-header",
                        "--skip-header",
                        "--zookeeper", zkQuorum});
                fail("Random error message");
            } catch (Exception ex) {
                System.out.println(ex.getMessage());
                assertTrue(ex instanceof IllegalArgumentException);
                assertTrue(ex.getMessage().contains(
                        "parse-header and skip-header cannot be used together."
                ));
            }
        }
    }

    @Test
    public void testParseHeaderWithColumnFamily() throws Exception {
        try (Statement stmt = conn.createStatement()) {
            stmt.execute("CREATE TABLE S.TABLE20 (\"id\" INTEGER NOT NULL PRIMARY KEY, \"cf1\".\"name\" VARCHAR, \"cf2\".\"type\" VARCHAR, \"cf1\".\"category\" VARCHAR)");

            final Configuration conf = new Configuration(getUtility().getConfiguration());
            FileSystem fs = FileSystem.get(getUtility().getConfiguration());
            FSDataOutputStream outputStream1 = fs.create(new Path("/tmp/input20.csv"));
            try (PrintWriter printWriter = new PrintWriter(outputStream1)) {
                printWriter.println("id,cf1.name,cf2.type,cf1.category");
                printWriter.println("1,Name 1,Type1,Category1");
                printWriter.println("2,Name 2,Type2,Category2");
                printWriter.println("3,Name 3,Type3,Category3");
            }
            CsvBulkLoadTool csvBulkLoadTool = new CsvBulkLoadTool();
            csvBulkLoadTool.setConf(conf);

            // Should ingest data successfully if --parse-header arg is passed
            int exitCode = csvBulkLoadTool.run(new String[] {
                    "--input", "/tmp/input20.csv",
                    "--table", "table20",
                    "--schema", "s",
                    "--parse-header",
                    "--zookeeper", zkQuorum});
            assertEquals(0, exitCode);

            try (ResultSet rs = stmt.executeQuery("SELECT COUNT(1) FROM S.TABLE20")) {
                assertTrue(rs.next());
                assertEquals(3, rs.getInt(1));
                assertFalse(rs.next());
            }

            try (ResultSet rs = stmt.executeQuery("SELECT \"id\",\"cf1\".\"name\", \"cf2\".\"type\", \"cf1\".\"category\" FROM S.TABLE20")) {
                assertTrue(rs.next());
                assertEquals(1, rs.getInt(1));
                assertEquals("Name 1", rs.getString(2));
                assertEquals("Type1", rs.getString(3));
                assertEquals("Category1", rs.getString(4));
                assertTrue(rs.next());
                assertEquals(2, rs.getInt(1));
                assertEquals("Name 2", rs.getString(2));
                assertEquals("Type2", rs.getString(3));
                assertEquals("Category2", rs.getString(4));
                assertTrue(rs.next());
                assertEquals(3, rs.getInt(1));
                assertEquals("Name 3", rs.getString(2));
                assertEquals("Type3", rs.getString(3));
                assertEquals("Category3", rs.getString(4));
            }
        }
    }
}
