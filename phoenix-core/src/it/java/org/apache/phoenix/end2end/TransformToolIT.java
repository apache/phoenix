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

import org.apache.phoenix.thirdparty.com.google.common.collect.Lists;
import org.apache.phoenix.thirdparty.com.google.common.collect.Maps;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.phoenix.end2end.index.SingleCellIndexIT;
import org.apache.phoenix.jdbc.PhoenixConnection;
import org.apache.phoenix.mapreduce.transform.TransformTool;
import org.apache.phoenix.query.QueryServices;
import org.apache.phoenix.query.QueryServicesOptions;
import org.apache.phoenix.schema.PTable;
import org.apache.phoenix.schema.transform.SystemTransformRecord;
import org.apache.phoenix.schema.transform.Transform;
import org.apache.phoenix.util.PhoenixRuntime;
import org.apache.phoenix.util.PropertiesUtil;
import org.apache.phoenix.util.ReadOnlyProps;
import org.apache.phoenix.util.SchemaUtil;
import org.junit.BeforeClass;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.UUID;

import static org.apache.phoenix.util.TestUtil.TEST_PROPERTIES;
import static org.apache.phoenix.util.TestUtil.getRowCount;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

public class TransformToolIT extends ParallelStatsDisabledIT{
    private static final Logger LOGGER = LoggerFactory.getLogger(TransformToolIT.class);
    private final String tableDDLOptions;
    // TODO test with immutable
    private boolean mutable = true;

    public TransformToolIT() {
        StringBuilder optionBuilder = new StringBuilder();
        optionBuilder.append(" IMMUTABLE_STORAGE_SCHEME=ONE_CELL_PER_COLUMN, COLUMN_ENCODED_BYTES=NONE, TTL=18000 ");
        if (!mutable) {
            optionBuilder.append(", IMMUTABLE_ROWS=true ");
        }
        tableDDLOptions = optionBuilder.toString();
    }

    @BeforeClass
    public static synchronized void setup() throws Exception {
        Map<String, String> serverProps = Maps.newHashMapWithExpectedSize(2);
        serverProps.put(QueryServices.STATS_GUIDEPOST_WIDTH_BYTES_ATTRIB, Long.toString(20));
        serverProps.put(QueryServices.MAX_SERVER_METADATA_CACHE_TIME_TO_LIVE_MS_ATTRIB, Long.toString(5));
        serverProps.put(QueryServices.EXTRA_JDBC_ARGUMENTS_ATTRIB,
                QueryServicesOptions.DEFAULT_EXTRA_JDBC_ARGUMENTS);
        serverProps.put(QueryServices.INDEX_REBUILD_PAGE_SIZE_IN_ROWS, Long.toString(8));
        Map<String, String> clientProps = Maps.newHashMapWithExpectedSize(2);
        setUpTestDriver(new ReadOnlyProps(serverProps.entrySet().iterator()),
                new ReadOnlyProps(clientProps.entrySet().iterator()));
    }

    private void createTableAndUpsertRows(Connection conn, String dataTableFullName, int numOfRows) throws SQLException {
        String stmString1 =
                "CREATE TABLE IF NOT EXISTS " + dataTableFullName
                        + " (ID INTEGER NOT NULL PRIMARY KEY, NAME VARCHAR, ZIP INTEGER) "
                        + tableDDLOptions;
        conn.createStatement().execute(stmString1);
        String upsertQuery = String.format("UPSERT INTO %s VALUES(?, ?, ?)", dataTableFullName);
        PreparedStatement stmt1 = conn.prepareStatement(upsertQuery);

        // insert rows
        for (int i = 1; i <= numOfRows; i++) {
            IndexToolIT.upsertRow(stmt1, i);
        }
        conn.commit();
    }
    @Test
    public void testTransformTable() throws Exception {
        String schemaName = generateUniqueName();
        String dataTableName = generateUniqueName();
        String dataTableFullName = SchemaUtil.getTableName(schemaName, dataTableName);
        String newTableFullName = dataTableFullName + "_1";
        Properties props = PropertiesUtil.deepCopy(TEST_PROPERTIES);
        try (Connection conn = DriverManager.getConnection(getUrl(), props)) {
            conn.setAutoCommit(true);
            int numOfRows = 2;
            createTableAndUpsertRows(conn, dataTableFullName, numOfRows);

            conn.createStatement().execute("ALTER TABLE " + dataTableFullName +
                    " SET IMMUTABLE_STORAGE_SCHEME=SINGLE_CELL_ARRAY_WITH_OFFSETS, COLUMN_ENCODED_BYTES=2");

            SystemTransformRecord record = Transform.getTransformRecord(schemaName, dataTableName, null, null, conn.unwrap(PhoenixConnection.class));
            assertNotNull(record);

            List<String> args = getArgList(schemaName, dataTableName, null,
                    null, null, null, false, false, false, false);
            runTransformTool(args.toArray(new String[0]), 0);
            record = Transform.getTransformRecord(schemaName, dataTableName, null, null, conn.unwrap(PhoenixConnection.class));
            assertEquals(PTable.TransformStatus.COMPLETED.name(), record.getTransformStatus());
            assertEquals(getRowCount(conn, dataTableFullName), getRowCount(conn,newTableFullName));

            // Test that the PhysicalTableName is updated.
            PTable oldTable = PhoenixRuntime.getTable(conn, dataTableFullName);
            assertEquals(dataTableName+"_1", oldTable.getPhysicalName(true).getString());

            String sql = "SELECT ID, NAME, ZIP FROM %s ";
            ResultSet rs1 = conn.createStatement().executeQuery(String.format(sql, dataTableFullName));
            ResultSet rs2 = conn.createStatement().executeQuery(String.format(sql, newTableFullName));
            for (int i=0; i < numOfRows; i++) {
                assertTrue(rs1.next());
                assertTrue(rs2.next());
                assertEquals(rs1.getString(1), rs2.getString(1));
                assertEquals(rs1.getString(2), rs2.getString(2));
                assertEquals(rs1.getInt(3), rs2.getInt(3));
            }
            assertFalse(rs1.next());
            assertFalse(rs2.next());
        }
    }

    @Test
    public void testAbortTransform() throws Exception {
        String schemaName = generateUniqueName();
        String dataTableName = generateUniqueName();
        String dataTableFullName = SchemaUtil.getTableName(schemaName, dataTableName);
        Properties props = PropertiesUtil.deepCopy(TEST_PROPERTIES);
        try (Connection conn = DriverManager.getConnection(getUrl(), props)) {
            conn.setAutoCommit(true);
            createTableAndUpsertRows(conn, dataTableFullName, 2);

            conn.createStatement().execute("ALTER TABLE " + dataTableFullName +
                    " SET IMMUTABLE_STORAGE_SCHEME=SINGLE_CELL_ARRAY_WITH_OFFSETS, COLUMN_ENCODED_BYTES=2");

            SystemTransformRecord record = Transform.getTransformRecord(schemaName, dataTableName, null, null,conn.unwrap(PhoenixConnection.class));
            assertNotNull(record);

            List<String> args = getArgList(schemaName, dataTableName, null,
                    null, null, null, true, false, false, false);

            runTransformTool(args.toArray(new String[0]), 0);
            record = Transform.getTransformRecord(schemaName, dataTableName, null, null, conn.unwrap(PhoenixConnection.class));
            assertNull(record);
        }
    }

    private void pauseTableTransform(String schemaName, String dataTableName, Connection conn) throws Exception {
        String dataTableFullName = SchemaUtil.getTableName(schemaName, dataTableName);

        createTableAndUpsertRows(conn, dataTableFullName, 2);

        conn.createStatement().execute("ALTER TABLE " + dataTableFullName +
                " SET IMMUTABLE_STORAGE_SCHEME=SINGLE_CELL_ARRAY_WITH_OFFSETS, COLUMN_ENCODED_BYTES=2");

        SystemTransformRecord record = Transform.getTransformRecord(schemaName, dataTableName, null, null, conn.unwrap(PhoenixConnection.class));
        assertNotNull(record);

        List<String> args = getArgList(schemaName, dataTableName, null,
                null, null, null, false, true, false, false);

        runTransformTool(args.toArray(new String[0]), 0);
        record = Transform.getTransformRecord(schemaName, dataTableName, null, null, conn.unwrap(PhoenixConnection.class));
        assertEquals(PTable.TransformStatus.PAUSED.name(), record.getTransformStatus());
    }

    @Test
    public void testPauseTransform() throws Exception {
        String schemaName = generateUniqueName();
        String dataTableName = generateUniqueName();
        Properties props = PropertiesUtil.deepCopy(TEST_PROPERTIES);
        try (Connection conn = DriverManager.getConnection(getUrl(), props)) {
            conn.setAutoCommit(true);
            pauseTableTransform(schemaName, dataTableName, conn);
        }
    }

    @Test
    public void testResumeTransform() throws Exception {
        String schemaName = generateUniqueName();
        String dataTableName = generateUniqueName();
        Properties props = PropertiesUtil.deepCopy(TEST_PROPERTIES);
        try (Connection conn = DriverManager.getConnection(getUrl(), props)) {
            conn.setAutoCommit(true);
            pauseTableTransform(schemaName, dataTableName, conn);
            List<String> args = getArgList(schemaName, dataTableName, null,
                    null, null, null, false, false, true, false);

            runTransformTool(args.toArray(new String[0]), 0);
            SystemTransformRecord record = Transform.getTransformRecord(schemaName, dataTableName, null, null, conn.unwrap(PhoenixConnection.class));
            assertEquals(PTable.TransformStatus.COMPLETED.name(), record.getTransformStatus());
        }
    }

    /**
     * Test presplitting an index table
     */
    @Test
    public void testSplitTable() throws Exception {
        String schemaName = generateUniqueName();
        String dataTableName = generateUniqueName();
        String dataTableFullName = SchemaUtil.getTableName(schemaName, dataTableName);
        final TableName dataTN = TableName.valueOf(dataTableFullName);
        final TableName newDataTN = TableName.valueOf(dataTableFullName + "_1");
        try (Connection conn =
                     DriverManager.getConnection(getUrl(), PropertiesUtil.deepCopy(TEST_PROPERTIES));
             HBaseAdmin admin = conn.unwrap(PhoenixConnection.class).getQueryServices().getAdmin()) {
            conn.setAutoCommit(true);
            String dataDDL =
                    "CREATE TABLE " + dataTableFullName + "(\n"
                            + "ID VARCHAR NOT NULL PRIMARY KEY,\n"
                            + "\"info\".CAR_NUM VARCHAR(18) NULL,\n"
                            + "\"test\".CAR_NUM VARCHAR(18) NULL,\n"
                            + "\"info\".CAP_DATE VARCHAR NULL,\n" + "\"info\".ORG_ID BIGINT NULL,\n"
                            + "\"info\".ORG_NAME VARCHAR(255) NULL\n" + ") IMMUTABLE_STORAGE_SCHEME=ONE_CELL_PER_COLUMN, COLUMN_ENCODED_BYTES = 0";
            conn.createStatement().execute(dataDDL);

            String[] idPrefixes = new String[] {"1", "2", "3", "4"};

            // split the data table, as the tool splits the new table to have the same # of regions
            // doesn't really matter what the split points are, we just want a target # of regions
            int numSplits = idPrefixes.length;
            int targetNumRegions = numSplits + 1;
            byte[][] splitPoints = new byte[numSplits][];
            for (String prefix : idPrefixes) {
                splitPoints[--numSplits] = Bytes.toBytes(prefix);
            }
            HTableDescriptor dataTD = admin.getTableDescriptor(dataTN);
            admin.disableTable(dataTN);
            admin.deleteTable(dataTN);
            admin.createTable(dataTD, splitPoints);
            assertEquals(targetNumRegions, admin.getTableRegions(dataTN).size());

            // insert data
            int idCounter = 1;
            try (PreparedStatement ps = conn.prepareStatement("UPSERT INTO " + dataTableFullName
                    + "(ID,\"info\".CAR_NUM,\"test\".CAR_NUM,CAP_DATE,ORG_ID,ORG_NAME) VALUES(?,?,?,'2021-01-01 00:00:00',11,'orgname1')")){
                for (String carNum : idPrefixes) {
                    for (int i = 0; i < 100; i++) {
                        ps.setString(1, idCounter++ + "");
                        ps.setString(2, carNum + "_" + i);
                        ps.setString(3, "test-" + carNum + "_ " + i);
                        ps.addBatch();
                    }
                }
                ps.executeBatch();
                conn.commit();
            }
            SingleCellIndexIT.assertMetadata(conn, PTable.ImmutableStorageScheme.ONE_CELL_PER_COLUMN, PTable.QualifierEncodingScheme.NON_ENCODED_QUALIFIERS, dataTableFullName);

            conn.createStatement().execute("ALTER TABLE " + dataTableFullName +
                    " SET IMMUTABLE_STORAGE_SCHEME=SINGLE_CELL_ARRAY_WITH_OFFSETS, COLUMN_ENCODED_BYTES=2");

            List<String> args = getArgList(schemaName, dataTableName, null,
                    null, null, null, false, false, false, false);
            // split if data table more than 3 regions
            args.add("--autosplit=3");

            args.add("-op");
            args.add("/tmp/" + UUID.randomUUID().toString());

            runTransformTool(args.toArray(new String[0]), 0);

            SingleCellIndexIT.assertMetadata(conn, PTable.ImmutableStorageScheme.SINGLE_CELL_ARRAY_WITH_OFFSETS, PTable.QualifierEncodingScheme.TWO_BYTE_QUALIFIERS, newDataTN.getNameAsString());
            assertEquals(targetNumRegions, admin.getTableRegions(newDataTN).size());
            assertEquals(getRowCount(conn, dataTableFullName), getRowCount(conn, dataTableFullName + "_1"));
        }
    }

    @Test
    public void testDataAfterTransformingMultiColFamilyTable() throws Exception {
        String schemaName = generateUniqueName();
        String dataTableName = generateUniqueName();
        String dataTableFullName = SchemaUtil.getTableName(schemaName, dataTableName);
        final TableName dataTN = TableName.valueOf(dataTableFullName);
        final TableName newDataTN = TableName.valueOf(dataTableFullName + "_1");
        try (Connection conn =
                     DriverManager.getConnection(getUrl(), PropertiesUtil.deepCopy(TEST_PROPERTIES));
             HBaseAdmin admin = conn.unwrap(PhoenixConnection.class).getQueryServices().getAdmin()) {
            conn.setAutoCommit(true);
            String dataDDL =
                    "CREATE TABLE " + dataTableFullName + "(\n"
                            + "ID VARCHAR(5) NOT NULL PRIMARY KEY,\n"
                            + "\"info\".CAR_NUM VARCHAR(18) NULL,\n"
                            + "\"test\".CAR_NUM VARCHAR(18) NULL,\n"
                            + "\"info\".CAP_DATE VARCHAR NULL,\n" + "\"info\".ORG_ID BIGINT NULL,\n"
                            + "\"test\".ORG_NAME VARCHAR(255) NULL\n" + ") IMMUTABLE_STORAGE_SCHEME=ONE_CELL_PER_COLUMN, COLUMN_ENCODED_BYTES = 0";
            conn.createStatement().execute(dataDDL);

            // insert data
            int idCounter = 1;
            try (PreparedStatement ps = conn.prepareStatement("UPSERT INTO " + dataTableFullName
                    + "(ID,\"info\".CAR_NUM,\"test\".CAR_NUM,CAP_DATE,ORG_ID,ORG_NAME) VALUES(?,?,?,'2021-01-01 00:00:00',11,'orgname1')")) {
                for (int i = 0; i < 5; i++) {
                    ps.setString(1, idCounter++ + "");
                    ps.setString(2, "info-" + i);
                    ps.setString(3, "test-" + i);
                    ps.addBatch();
                }
                ps.executeBatch();
                conn.commit();
            }
            SingleCellIndexIT.assertMetadata(conn, PTable.ImmutableStorageScheme.ONE_CELL_PER_COLUMN, PTable.QualifierEncodingScheme.NON_ENCODED_QUALIFIERS, dataTableFullName);

            conn.createStatement().execute("ALTER TABLE " + dataTableFullName +
                    " SET IMMUTABLE_STORAGE_SCHEME=SINGLE_CELL_ARRAY_WITH_OFFSETS, COLUMN_ENCODED_BYTES=2");

            List<String> args = getArgList(schemaName, dataTableName, null,
                    null, null, null, false, false, false, false);
            runTransformTool(args.toArray(new String[0]), 0);

            SingleCellIndexIT.assertMetadata(conn, PTable.ImmutableStorageScheme.SINGLE_CELL_ARRAY_WITH_OFFSETS, PTable.QualifierEncodingScheme.TWO_BYTE_QUALIFIERS, newDataTN.getNameAsString());
            assertEquals(getRowCount(conn, dataTableFullName), getRowCount(conn, dataTableFullName + "_1"));

            String select = "SELECT ID,\"info\".CAR_NUM,\"test\".CAR_NUM,CAP_DATE,ORG_ID,ORG_NAME FROM ";
            ResultSet resultSetNew = conn.createStatement().executeQuery(select + newDataTN.getNameAsString());
            ResultSet resultSetOld = conn.createStatement().executeQuery(select + dataTableFullName);
            for (int i=0; i < idCounter-1; i++) {
                assertTrue(resultSetNew.next());
                assertTrue(resultSetOld.next());
                assertEquals(resultSetOld.getString(1), resultSetNew.getString(1));
                assertEquals(resultSetOld.getString(2), resultSetNew.getString(2));
                assertEquals(resultSetOld.getString(3), resultSetNew.getString(3));
                assertEquals(resultSetOld.getString(4), resultSetNew.getString(4));
                assertEquals(resultSetOld.getString(5), resultSetNew.getString(5));
                assertEquals(resultSetOld.getString(6), resultSetNew.getString(6));
            }
            assertFalse(resultSetNew.next());
            assertFalse(resultSetOld.next());
        }
    }

    @Test
    public void testTransformIndex() throws Exception {
        String schemaName = generateUniqueName();
        String dataTableName = generateUniqueName();
        String dataTableFullName = SchemaUtil.getTableName(schemaName, dataTableName);
        String indexTableName = "I_" + generateUniqueName();
        String indexTableFullName = SchemaUtil.getTableName(schemaName, indexTableName);

        Properties props = PropertiesUtil.deepCopy(TEST_PROPERTIES);
        try (Connection conn = DriverManager.getConnection(getUrl(), props)) {
            conn.setAutoCommit(true);
            int numOfRows = 2;
            createTableAndUpsertRows(conn, dataTableFullName, numOfRows);
            conn.createStatement().execute("CREATE INDEX " + indexTableName + " ON " + dataTableFullName + " (NAME) INCLUDE (ZIP)");
            SingleCellIndexIT.assertMetadata(conn, PTable.ImmutableStorageScheme.ONE_CELL_PER_COLUMN, PTable.QualifierEncodingScheme.NON_ENCODED_QUALIFIERS, indexTableFullName);
            conn.createStatement().execute("ALTER INDEX " + indexTableName + " ON " + dataTableFullName +
                    " ACTIVE IMMUTABLE_STORAGE_SCHEME=SINGLE_CELL_ARRAY_WITH_OFFSETS, COLUMN_ENCODED_BYTES=2");

            SystemTransformRecord record = Transform.getTransformRecord(schemaName, indexTableName, dataTableFullName, null, conn.unwrap(PhoenixConnection.class));
            assertNotNull(record);

            List<String> args = getArgList(schemaName, dataTableName, indexTableName,
                    null, null, null, false, false, false, false);

            runTransformTool(args.toArray(new String[0]), 0);
            record = Transform.getTransformRecord(schemaName, indexTableName, dataTableFullName, null, conn.unwrap(PhoenixConnection.class));
            assertEquals(PTable.TransformStatus.COMPLETED.name(), record.getTransformStatus());
            assertEquals(getRowCount(conn, indexTableFullName), getRowCount(conn, indexTableFullName + "_1"));

            // Test that the PhysicalTableName is updated.
            PTable oldTable = PhoenixRuntime.getTable(conn, indexTableFullName);
            assertEquals(indexTableName+"_1", oldTable.getPhysicalName(true).getString());

            String sql = "SELECT \":ID\", \"0:NAME\", \"0:ZIP\" FROM %s ORDER BY \":ID\"";
            ResultSet rs1 = conn.createStatement().executeQuery(String.format(sql, indexTableFullName));
            ResultSet rs2 = conn.createStatement().executeQuery(String.format(sql, indexTableFullName + "_1"));
            for (int i=0; i < numOfRows; i++) {
                assertTrue(rs1.next());
                assertTrue(rs2.next());
                assertEquals(rs1.getString(1), rs2.getString(1));
                assertEquals(rs1.getString(2), rs2.getString(2));
                assertEquals(rs1.getInt(3), rs2.getInt(3));
            }
            assertFalse(rs1.next());
            assertFalse(rs2.next());
        }
    }

    public static List<String> getArgList(String schemaName, String dataTable, String indxTable, String tenantId,
                                          Long startTime, Long endTime,
                                          boolean shouldAbort, boolean shouldPause, boolean shouldResume, boolean isPartial) {
        List<String> args = Lists.newArrayList();
        if (schemaName != null) {
            args.add("--schema=" + schemaName);
        }
        // Work around CLI-254. The long-form arg parsing doesn't strip off double-quotes
        args.add("--data-table=" + dataTable);
        if (indxTable != null) {
            args.add("--index-table=" + indxTable);
        }

        args.add("-op");
        args.add("/tmp/" + UUID.randomUUID().toString());
        // Need to run this job in foreground for the test to be deterministic
        args.add("-runfg");

        if (tenantId != null) {
            args.add("-tenant");
            args.add(tenantId);
        }
        if(startTime != null) {
            args.add("-st");
            args.add(String.valueOf(startTime));
        }
        if(endTime != null) {
            args.add("-et");
            args.add(String.valueOf(endTime));
        }
        if (shouldAbort) {
            args.add("-abort");
        }
        if (shouldPause) {
            args.add("-pause");
        }
        if (shouldResume) {
            args.add("-resume");
        }
        if (isPartial) {
            args.add("-pt");
        }
        return args;
    }


    public static String [] getArgValues(String schemaName,
                                         String dataTable, String indexTable, String tenantId,
                                         Long startTime, Long endTime) {
        List<String> args = getArgList(schemaName, dataTable, indexTable,
                tenantId, startTime, endTime, false, false, false, false);
        args.add("-op");
        args.add("/tmp/" + UUID.randomUUID().toString());
        return args.toArray(new String[0]);
    }

    public static TransformTool runTransformTool(int expectedStatus, String schemaName, String dataTableName, String indexTableName, String tenantId,
                                                 String... additionalArgs) throws Exception {
        final String[] cmdArgs = getArgValues(schemaName, dataTableName,
                indexTableName, tenantId, 0L, 0L);
        List<String> cmdArgList = new ArrayList<>(Arrays.asList(cmdArgs));
        cmdArgList.add("-op");
        cmdArgList.add("/tmp/" + UUID.randomUUID().toString());

        cmdArgList.addAll(Arrays.asList(additionalArgs));
        return runTransformTool(cmdArgList.toArray(new String[cmdArgList.size()]), expectedStatus);
    }

    public static TransformTool runTransformTool(String[] cmdArgs, int expectedStatus) throws Exception {
        TransformTool tt = new TransformTool();
        Configuration conf = new Configuration(getUtility().getConfiguration());
        tt.setConf(conf);

        LOGGER.info("Running TransformTool with {}", Arrays.toString(cmdArgs), new Exception("Stack Trace"));
        int status = tt.run(cmdArgs);

        assertEquals(expectedStatus, status);
        return tt;
    }

}