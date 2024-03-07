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
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.phoenix.end2end.index.IndexTestUtil;
import org.apache.phoenix.jdbc.PhoenixConnection;
import org.apache.phoenix.mapreduce.CsvBulkLoadTool;
import org.apache.phoenix.query.QueryConstants;
import org.apache.phoenix.query.QueryServices;
import org.apache.phoenix.schema.PTable;
import org.apache.phoenix.schema.PTableKey;
import org.apache.phoenix.thirdparty.com.google.common.collect.Maps;
import org.apache.phoenix.util.DateUtil;
import org.apache.phoenix.util.PhoenixRuntime;
import org.apache.phoenix.util.QueryUtil;
import org.apache.phoenix.util.ReadOnlyProps;
import org.apache.phoenix.util.SchemaUtil;
import org.apache.phoenix.util.TestUtil;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;

@Category(NeedsOwnMiniClusterTest.class)
public class CsvBulkLoadToolIT extends BaseOwnClusterIT {

    private static Connection conn;
    private static String zkQuorum;

    @BeforeClass
    public static synchronized void doSetup() throws Exception {
        Map<String, String> clientProps = Maps.newHashMapWithExpectedSize(1);
        clientProps.put(QueryServices.INDEX_REGION_OBSERVER_ENABLED_ATTRIB, Boolean.FALSE.toString());
        setUpTestDriver(ReadOnlyProps.EMPTY_PROPS, new ReadOnlyProps(clientProps.entrySet().iterator()));
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
    public void testImportWithGlobalIndex() throws Exception {

        Statement stmt = conn.createStatement();
        stmt.execute("CREATE TABLE S.TABLE1 (ID INTEGER NOT NULL PRIMARY KEY, NAME VARCHAR, T DATE) SPLIT ON (1,2)");
        stmt.execute("CREATE INDEX glob_idx ON S.TABLE1(ID, T)");
        conn.commit();

        FileSystem fs = FileSystem.get(getUtility().getConfiguration());
        FSDataOutputStream outputStream = fs.create(new Path("/tmp/input1.csv"));
        PrintWriter printWriter = new PrintWriter(outputStream);
        printWriter.println("1,Name 1,1970/01/01");
        printWriter.println("2,Name 2,1970/01/02");
        printWriter.close();

        fs = FileSystem.get(getUtility().getConfiguration());
        outputStream = fs.create(new Path("/tmp/input2.csv"));
        printWriter = new PrintWriter(outputStream);
        printWriter.println("3,Name 3,1970/01/03");
        printWriter.println("4,Name 4,1970/01/04");
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

        csvBulkLoadTool = new CsvBulkLoadTool();
        csvBulkLoadTool.setConf(new Configuration(getUtility().getConfiguration()));
        csvBulkLoadTool.getConf().set(DATE_FORMAT_ATTRIB,"yyyy/MM/dd");
        try {
            exitCode = csvBulkLoadTool.run(new String[] {
                    "--input", "/tmp/input2.csv",
                    "--table", "table1",
                    "--schema", "s",
                    "--zookeeper", zkQuorum});
            assertTrue("Bulk loading error should have happened earlier", exitCode != 0);
        } catch (Exception e) {
            fail("Tools should return non-zero exit codes on failure"
                    + " instead of throwing an exception");
        }

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

        csvBulkLoadTool = new CsvBulkLoadTool();
        csvBulkLoadTool.setConf(new Configuration(getUtility().getConfiguration()));
        csvBulkLoadTool.getConf().set(DATE_FORMAT_ATTRIB,"yyyy/MM/dd");
        exitCode = csvBulkLoadTool.run(new String[] {
                "--input", "/tmp/input2.csv",
                "--table", "table1",
                "--schema", "s",
                "--zookeeper", zkQuorum,
                "--corruptindexes"});
        assertEquals(0, exitCode);

        rs = stmt.executeQuery("SELECT id, name, t FROM s.table1 ORDER BY id");
        assertTrue(rs.next());
        assertEquals(1, rs.getInt(1));
        assertEquals("Name 1", rs.getString(2));
        assertEquals(DateUtil.parseDate("1970-01-01"), rs.getDate(3));
        assertTrue(rs.next());
        assertEquals(2, rs.getInt(1));
        assertEquals("Name 2", rs.getString(2));
        assertEquals(DateUtil.parseDate("1970-01-02"), rs.getDate(3));
        assertTrue(rs.next());
        assertEquals(3, rs.getInt(1));
        assertEquals("Name 3", rs.getString(2));
        assertEquals(DateUtil.parseDate("1970-01-03"), rs.getDate(3));
        assertTrue(rs.next());
        assertEquals(4, rs.getInt(1));
        assertEquals("Name 4", rs.getString(2));
        assertEquals(DateUtil.parseDate("1970-01-04"), rs.getDate(3));
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
    public void testImportWithTabsAndEmptyQuotes() throws Exception {

        Statement stmt = conn.createStatement();
        stmt.execute("CREATE TABLE TABLE8 (ID INTEGER NOT NULL PRIMARY KEY, " +
                "NAME1 VARCHAR, NAME2 VARCHAR)");

        FileSystem fs = FileSystem.get(getUtility().getConfiguration());
        FSDataOutputStream outputStream = fs.create(new Path("/tmp/input8.csv"));
        PrintWriter printWriter = new PrintWriter(outputStream);
        printWriter.println("1\t\"\\t123\tName 2a");
        printWriter.println("2\tName 2a\tName 2b");
        printWriter.close();

        CsvBulkLoadTool csvBulkLoadTool = new CsvBulkLoadTool();
        csvBulkLoadTool.setConf(getUtility().getConfiguration());
        int exitCode = csvBulkLoadTool.run(new String[] {
                "--input", "/tmp/input8.csv",
                "--table", "table8",
                "--zookeeper", zkQuorum,
                "-q", "",
                "-e", "",
                "--delimiter", "\\t"
                });
        assertEquals(0, exitCode);

        ResultSet rs = stmt.executeQuery("SELECT id, name1, name2 FROM table8 ORDER BY id");
        assertTrue(rs.next());
        assertEquals(1, rs.getInt(1));
        assertEquals("\"\\t123", rs.getString(2));
        assertEquals("Name 2a", rs.getString(3));

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

        IndexTestUtil.assertRowsForEmptyColValue(conn, "TABLE3_IDX", QueryConstants.VERIFIED_BYTES);
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

        if (!localIndex) {
            IndexTestUtil.assertRowsForEmptyColValue(conn, indexTableName,
                QueryConstants.VERIFIED_BYTES);
        }
    }

    @Test
    public void testImportWithDifferentPhysicalName() throws Exception {
        String schemaName = "S_" + generateUniqueName();
        String tableName = "TBL_" + generateUniqueName();
        String indexTableName = String.format("%s_IDX", tableName);
        String fullTableName = SchemaUtil.getTableName(schemaName, tableName);
        String fullIndexTableName = SchemaUtil.getTableName(schemaName, indexTableName);
        Statement stmt = conn.createStatement();
        stmt.execute("CREATE TABLE " + fullTableName + "(ID INTEGER NOT NULL PRIMARY KEY, "
                + "FIRST_NAME VARCHAR, LAST_NAME VARCHAR)");
        String ddl = "CREATE  INDEX " + indexTableName + " ON " + fullTableName + "(FIRST_NAME ASC)";
        stmt.execute(ddl);
        String newTableName = LogicalTableNameIT.NEW_TABLE_PREFIX + generateUniqueName();
        String fullNewTableName = SchemaUtil.getTableName(schemaName, newTableName);
        try (Admin admin = conn.unwrap(PhoenixConnection.class).getQueryServices().getAdmin()) {
            String snapshotName = new StringBuilder(tableName).append("-Snapshot").toString();
            admin.snapshot(snapshotName, TableName.valueOf(fullTableName));
            admin.cloneSnapshot(snapshotName, TableName.valueOf(fullNewTableName));
        }
        LogicalTableNameIT.renameAndDropPhysicalTable(conn, "NULL", schemaName, tableName, newTableName, false);

        String csvName = "/tmp/input_logical_name.csv";
        FileSystem fs = FileSystem.get(getUtility().getConfiguration());
        FSDataOutputStream outputStream = fs.create(new Path(csvName));
        PrintWriter printWriter = new PrintWriter(outputStream);
        printWriter.println("1,FirstName 1,LastName 1");
        printWriter.println("2,FirstName 2,LastName 2");
        printWriter.close();

        CsvBulkLoadTool csvBulkLoadTool = new CsvBulkLoadTool();
        csvBulkLoadTool.setConf(getUtility().getConfiguration());
        int
                exitCode =
                csvBulkLoadTool
                        .run(new String[] { "--input", csvName, "--table", tableName,
                                "--schema", schemaName,
                                "--zookeeper", zkQuorum });
        assertEquals(0, exitCode);

        ResultSet rs = stmt.executeQuery("SELECT /*+ NO_INDEX */ id, FIRST_NAME FROM " + fullTableName + " where first_name='FirstName 2'");
        assertTrue(rs.next());
        assertEquals(2, rs.getInt(1));
        assertEquals("FirstName 2", rs.getString(2));
        String selectFromIndex = "SELECT FIRST_NAME FROM " + fullTableName + " where FIRST_NAME='FirstName 1'";
        rs = stmt.executeQuery("EXPLAIN " + selectFromIndex);
        assertTrue(QueryUtil.getExplainPlan(rs).contains(indexTableName));
        rs = stmt.executeQuery(selectFromIndex);
        assertTrue(rs.next());
        assertEquals("FirstName 1", rs.getString(1));

        String csvNameForIndex = "/tmp/input_logical_name_index.csv";
        // Now just run the tool on the index table and check that the index has extra row.
        outputStream = fs.create(new Path(csvNameForIndex));
        printWriter = new PrintWriter(outputStream);
        printWriter.println("3,FirstName 3,LastName 3");
        printWriter.close();
        exitCode = csvBulkLoadTool
                        .run(new String[] { "--input", csvNameForIndex, "--table", tableName,
                                "--schema", schemaName,
                                "--index-table", indexTableName, "--zookeeper", zkQuorum,
                                "--corruptindexes"});
        assertEquals(0, exitCode);
        selectFromIndex = "SELECT FIRST_NAME FROM " + fullTableName + " where FIRST_NAME='FirstName 3'";
        rs = stmt.executeQuery("EXPLAIN " + selectFromIndex);
        assertTrue(QueryUtil.getExplainPlan(rs).contains(indexTableName));
        rs = stmt.executeQuery(selectFromIndex);
        assertTrue(rs.next());
        assertEquals("FirstName 3", rs.getString(1));
        rs.close();
        stmt.close();

        IndexTestUtil.assertRowsForEmptyColValue(conn, fullIndexTableName,
            QueryConstants.VERIFIED_BYTES);
    }

    @Test
    public void testInvalidArguments() {
        String tableName = "TABLE8";
        CsvBulkLoadTool csvBulkLoadTool = new CsvBulkLoadTool();
        csvBulkLoadTool.setConf(getUtility().getConfiguration());
        try {
            int exitCode = csvBulkLoadTool.run(new String[] {
                "--input", "/tmp/input4.csv",
                "--table", tableName,
                "--zookeeper", zkQuorum });
            assertTrue(String.format("Table %s not created, hence should fail", tableName),
                exitCode != 0);
        } catch (Exception ex) {
            fail("Tools should return non-zero exit codes on failure"
                    + " instead of throwing an exception");        }
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
            int exitCode = csvBulkLoadTool.run(new String[] {
                "--input", "/tmp/input9.csv",
                "--output", outputPath,
                "--table", tableName,
                "--zookeeper", zkQuorum });

            assertTrue(
                String.format("Output path %s already exists. hence, should fail", outputPath),
                exitCode != 0);
        } catch (Exception ex) {
            fail("Tools should return non-zero exit codes when fail,"
                + " instead of throwing an exception");
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
    public void testImportWithUpperCaseSchemaNameAndLowerCaseTableName() throws Exception {
        Statement stmt = conn.createStatement();
        stmt.execute("CREATE TABLE S.\"t\" (ID INTEGER NOT NULL PRIMARY KEY, NAME VARCHAR, " +
                              "T DATE) SPLIT ON (1,2)");
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
                "--table", "\"t\"",
                "--schema", "S",
                "--zookeeper", zkQuorum});
        assertEquals(0, exitCode);
        ResultSet rs = stmt.executeQuery("SELECT id, name, t FROM S.\"t\" ORDER BY id");
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
    public void testImportWithLowerCaseSchemaNameAndUpperCaseTableName() throws Exception {
        Statement stmt = conn.createStatement();
        stmt.execute("CREATE TABLE \"s\".T (ID INTEGER NOT NULL PRIMARY KEY, NAME VARCHAR, " +
                               "T DATE) SPLIT ON (1,2)");
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
                "--table", "T",
                "--schema", "\"s\"",
                "--zookeeper", zkQuorum});
        assertEquals(0, exitCode);
        ResultSet rs = stmt.executeQuery("SELECT id, name, t FROM \"s\".T ORDER BY id");
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
}
