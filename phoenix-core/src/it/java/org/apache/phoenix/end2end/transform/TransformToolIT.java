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
package org.apache.phoenix.end2end.transform;

import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.TableDescriptor;
import org.apache.phoenix.coprocessor.tasks.TransformMonitorTask;
import org.apache.phoenix.exception.SQLExceptionCode;
import org.apache.phoenix.mapreduce.util.PhoenixConfigurationUtilHelper;
import org.apache.phoenix.thirdparty.com.google.common.collect.Lists;
import org.apache.phoenix.thirdparty.com.google.common.collect.Maps;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.phoenix.end2end.IndexToolIT;
import org.apache.phoenix.end2end.ParallelStatsDisabledIT;
import org.apache.phoenix.end2end.index.SingleCellIndexIT;
import org.apache.phoenix.hbase.index.IndexRegionObserver;
import org.apache.phoenix.jdbc.PhoenixConnection;
import org.apache.phoenix.mapreduce.index.IndexTool;
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
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.UUID;

import static org.apache.phoenix.end2end.index.ImmutableIndexExtendedIT.getRowCountForEmptyColValue;
import static org.apache.phoenix.mapreduce.PhoenixJobCounters.INPUT_RECORDS;
import static org.apache.phoenix.mapreduce.index.PhoenixIndexToolJobCounters.AFTER_REBUILD_VALID_INDEX_ROW_COUNT;
import static org.apache.phoenix.mapreduce.index.PhoenixIndexToolJobCounters.BEFORE_REBUILD_MISSING_INDEX_ROW_COUNT;
import static org.apache.phoenix.mapreduce.index.PhoenixIndexToolJobCounters.BEFORE_REBUILD_UNVERIFIED_INDEX_ROW_COUNT;
import static org.apache.phoenix.mapreduce.index.PhoenixIndexToolJobCounters.BEFORE_REBUILD_VALID_INDEX_ROW_COUNT;
import static org.apache.phoenix.mapreduce.index.PhoenixIndexToolJobCounters.BEFORE_REPAIR_EXTRA_UNVERIFIED_INDEX_ROW_COUNT;
import static org.apache.phoenix.mapreduce.index.PhoenixIndexToolJobCounters.REBUILT_INDEX_ROW_COUNT;
import static org.apache.phoenix.mapreduce.index.PhoenixIndexToolJobCounters.SCANNED_DATA_ROW_COUNT;
import static org.apache.phoenix.query.QueryConstants.UNVERIFIED_BYTES;
import static org.apache.phoenix.query.QueryConstants.VERIFIED_BYTES;
import static org.apache.phoenix.util.TestUtil.TEST_PROPERTIES;
import static org.apache.phoenix.util.TestUtil.getRowCount;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

@RunWith(Parameterized.class)
public class TransformToolIT extends ParallelStatsDisabledIT {
    private static final Logger LOGGER = LoggerFactory.getLogger(TransformToolIT.class);
    private final String tableDDLOptions;

    @Parameterized.Parameters(
            name = "mutable={0}")
    public static synchronized Collection<Object[]> data() {
        List<Object[]> list = Lists.newArrayListWithExpectedSize(2);
        boolean[] Booleans = new boolean[]{true, false};
        for (boolean mutable : Booleans) {
            list.add(new Object[]{mutable});
        }
        return list;
    }

    public TransformToolIT(boolean mutable) {
        StringBuilder optionBuilder = new StringBuilder();
        optionBuilder.append(" IMMUTABLE_STORAGE_SCHEME=ONE_CELL_PER_COLUMN, COLUMN_ENCODED_BYTES=NONE ");
        if (!mutable) {
            optionBuilder.append(", IMMUTABLE_ROWS=true ");
        }
        tableDDLOptions = optionBuilder.toString();
    }

    @BeforeClass
    public static synchronized void setup() throws Exception {
        TransformMonitorTask.disableTransformMonitorTask(true);
        Map<String, String> serverProps = Maps.newHashMapWithExpectedSize(2);
        serverProps.put(QueryServices.STATS_GUIDEPOST_WIDTH_BYTES_ATTRIB, Long.toString(20));
        serverProps.put(QueryServices.MAX_SERVER_METADATA_CACHE_TIME_TO_LIVE_MS_ATTRIB, Long.toString(5));
        serverProps.put(QueryServices.EXTRA_JDBC_ARGUMENTS_ATTRIB,
                QueryServicesOptions.DEFAULT_EXTRA_JDBC_ARGUMENTS);
        serverProps.put(QueryServices.INDEX_REBUILD_PAGE_SIZE_IN_ROWS, Long.toString(8));
        serverProps.put(QueryServices.TRANSACTIONS_ENABLED, Boolean.TRUE.toString());
        serverProps.put(PhoenixConfigurationUtilHelper.TRANSFORM_MONITOR_ENABLED, Boolean.FALSE.toString());
        Map<String, String> clientProps = Maps.newHashMapWithExpectedSize(2);
        clientProps.put(PhoenixConfigurationUtilHelper.TRANSFORM_MONITOR_ENABLED, Boolean.FALSE.toString());
        clientProps.put("hbase.procedure.remote.dispatcher.delay.msec", "0");
        setUpTestDriver(new ReadOnlyProps(serverProps.entrySet().iterator()),
                new ReadOnlyProps(clientProps.entrySet().iterator()));
    }

    @AfterClass
    public static synchronized void cleanup() {
        TransformMonitorTask.disableTransformMonitorTask(false);
    }

    private void createTableAndUpsertRows(Connection conn, String dataTableFullName, int numOfRows) throws SQLException {
        createTableAndUpsertRows(conn, dataTableFullName, numOfRows, tableDDLOptions);
    }

    public static void createTableAndUpsertRows(Connection conn, String dataTableFullName, int numOfRows, String tableOptions) throws SQLException {
        createTableAndUpsertRows(conn, dataTableFullName, numOfRows, "", tableOptions);
    }

    public static void createTableAndUpsertRows(Connection conn, String dataTableFullName, int numOfRows, String constantVal, String tableOptions) throws SQLException {
        String stmString1 =
                "CREATE TABLE IF NOT EXISTS " + dataTableFullName
                        + " (ID INTEGER NOT NULL PRIMARY KEY, NAME VARCHAR, ZIP INTEGER, DATA VARCHAR) "
                        + tableOptions;
        conn.createStatement().execute(stmString1);
        upsertRows(conn, dataTableFullName, 1, numOfRows, constantVal);
        conn.commit();
    }

    public static void upsertRows(Connection conn, String dataTableFullName, int startIdx, int numOfRows) throws SQLException {
        upsertRows(conn, dataTableFullName, startIdx, numOfRows, "");
    }
    public static void upsertRows(Connection conn, String dataTableFullName, int startIdx, int numOfRows, String constantVal) throws SQLException {
        String upsertQuery = String.format("UPSERT INTO %s VALUES(?, ?, ?, ?)", dataTableFullName);
        PreparedStatement stmt = conn.prepareStatement(upsertQuery);

        // insert rows
        for (int i = startIdx; i < startIdx+numOfRows; i++) {
            stmt.setInt(1, i);
            stmt.setString(2, "uname" + String.valueOf(i));
            stmt.setInt(3, 95050 + i);
            stmt.setString(4, constantVal);
            stmt.executeUpdate();
        }
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
            createTableAndUpsertRows(conn, dataTableFullName, numOfRows, tableDDLOptions);

            conn.createStatement().execute("ALTER TABLE " + dataTableFullName +
                    " SET IMMUTABLE_STORAGE_SCHEME=SINGLE_CELL_ARRAY_WITH_OFFSETS, COLUMN_ENCODED_BYTES=2");

            SystemTransformRecord record = Transform.getTransformRecord(schemaName, dataTableName, null, null, conn.unwrap(PhoenixConnection.class));
            assertNotNull(record);

            List<String> args = getArgList(schemaName, dataTableName, null,
                    null, null, null, false, false, false, false,false);
            runTransformTool(args.toArray(new String[0]), 0);
            record = Transform.getTransformRecord(schemaName, dataTableName, null, null, conn.unwrap(PhoenixConnection.class));
            assertTransformStatusOrPartial(PTable.TransformStatus.PENDING_CUTOVER, record);
            assertEquals(getRowCount(conn, dataTableFullName), getRowCount(conn,newTableFullName));

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
            createTableAndUpsertRows(conn, dataTableFullName, 2, tableDDLOptions);

            conn.createStatement().execute("ALTER TABLE " + dataTableFullName +
                    " SET IMMUTABLE_STORAGE_SCHEME=SINGLE_CELL_ARRAY_WITH_OFFSETS, COLUMN_ENCODED_BYTES=2");

            SystemTransformRecord record = Transform.getTransformRecord(schemaName, dataTableName, null, null,conn.unwrap(PhoenixConnection.class));
            assertNotNull(record);

            List<String> args = getArgList(schemaName, dataTableName, null,
                    null, null, null, true, false, false, false, false);

            runTransformTool(args.toArray(new String[0]), 0);
            record = Transform.getTransformRecord(schemaName, dataTableName, null, null, conn.unwrap(PhoenixConnection.class));
            assertNull(record);
        }
    }

    public static void pauseTableTransform(String schemaName, String dataTableName, Connection conn, String tableDDLOptions) throws Exception {
        String dataTableFullName = SchemaUtil.getTableName(schemaName, dataTableName);

        createTableAndUpsertRows(conn, dataTableFullName, 2, tableDDLOptions);

        conn.createStatement().execute("ALTER TABLE " + dataTableFullName +
                " SET IMMUTABLE_STORAGE_SCHEME=SINGLE_CELL_ARRAY_WITH_OFFSETS, COLUMN_ENCODED_BYTES=2");

        SystemTransformRecord record = Transform.getTransformRecord(schemaName, dataTableName, null, null, conn.unwrap(PhoenixConnection.class));
        assertNotNull(record);

        List<String> args = getArgList(schemaName, dataTableName, null,
                null, null, null, false, true, false, false, false);

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
            pauseTableTransform(schemaName, dataTableName, conn, tableDDLOptions);
        }
    }

    @Test
    public void testResumeTransform() throws Exception {
        String schemaName = generateUniqueName();
        String dataTableName = generateUniqueName();
        Properties props = PropertiesUtil.deepCopy(TEST_PROPERTIES);
        try (Connection conn = DriverManager.getConnection(getUrl(), props)) {
            conn.setAutoCommit(true);
            pauseTableTransform(schemaName, dataTableName, conn, tableDDLOptions);
            List<String> args = getArgList(schemaName, dataTableName, null,
                    null, null, null, false, false, true, false, false);

            runTransformTool(args.toArray(new String[0]), 0);
            SystemTransformRecord record = Transform.getTransformRecord(schemaName, dataTableName, null, null, conn.unwrap(PhoenixConnection.class));
            assertTransformStatusOrPartial(PTable.TransformStatus.PENDING_CUTOVER, record);
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
             Admin admin = conn.unwrap(PhoenixConnection.class).getQueryServices().getAdmin()) {
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
            TableDescriptor dataTD = admin.getDescriptor(dataTN);
            admin.disableTable(dataTN);
            admin.deleteTable(dataTN);
            admin.createTable(dataTD, splitPoints);
            assertEquals(targetNumRegions, admin.getRegions(dataTN).size());

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
                    null, null, null, false, false, false, false, false);
            // split if data table more than 3 regions
            args.add("--autosplit=3");

            args.add("-op");
            args.add("/tmp/" + UUID.randomUUID().toString());

            runTransformTool(args.toArray(new String[0]), 0);

            SingleCellIndexIT.assertMetadata(conn, PTable.ImmutableStorageScheme.SINGLE_CELL_ARRAY_WITH_OFFSETS, PTable.QualifierEncodingScheme.TWO_BYTE_QUALIFIERS, newDataTN.getNameAsString());
            assertEquals(targetNumRegions, admin.getRegions(newDataTN).size());
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
             Admin admin = conn.unwrap(PhoenixConnection.class).getQueryServices().getAdmin()) {
            conn.setAutoCommit(true);
            String dataDDL =
                    "CREATE TABLE " + dataTableFullName + "(\n"
                            + "ID CHAR(5) NOT NULL PRIMARY KEY,\n"
                            + "\"info\".CAR_NUM VARCHAR(18) NULL,\n"
                            + "\"test\".CAR_NUM VARCHAR(18) NULL,\n"
                            + "\"info\".CAP_DATE VARCHAR NULL,\n" + "\"info\".ORG_ID BIGINT NULL,\n"
                            + "\"test\".ORG_NAME VARCHAR(255) NULL\n" + ") IMMUTABLE_STORAGE_SCHEME=ONE_CELL_PER_COLUMN, COLUMN_ENCODED_BYTES=NONE ";
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
                    null, null, null, false, false, false, false, false);
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
            createTableAndUpsertRows(conn, dataTableFullName, numOfRows, tableDDLOptions);
            conn.createStatement().execute("CREATE INDEX " + indexTableName + " ON " + dataTableFullName + " (NAME) INCLUDE (ZIP)");
            SingleCellIndexIT.assertMetadata(conn, PTable.ImmutableStorageScheme.ONE_CELL_PER_COLUMN, PTable.QualifierEncodingScheme.NON_ENCODED_QUALIFIERS, indexTableFullName);
            conn.createStatement().execute("ALTER INDEX " + indexTableName + " ON " + dataTableFullName +
                    " ACTIVE IMMUTABLE_STORAGE_SCHEME=SINGLE_CELL_ARRAY_WITH_OFFSETS, COLUMN_ENCODED_BYTES=2");

            SystemTransformRecord record = Transform.getTransformRecord(schemaName, indexTableName, dataTableFullName, null, conn.unwrap(PhoenixConnection.class));
            assertNotNull(record);

            List<String> args = getArgList(schemaName, dataTableName, indexTableName,
                    null, null, null, false, false, false, false, false);

            runTransformTool(args.toArray(new String[0]), 0);
            record = Transform.getTransformRecord(schemaName, indexTableName, dataTableFullName, null, conn.unwrap(PhoenixConnection.class));
            assertTransformStatusOrPartial(PTable.TransformStatus.PENDING_CUTOVER, record);
            assertEquals(getRowCount(conn, indexTableFullName), getRowCount(conn, indexTableFullName + "_1"));

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

    @Test
    public void testTransformMutationReadRepair() throws Exception {
        String schemaName = generateUniqueName();
        String dataTableName = generateUniqueName();
        String dataTableFullName = SchemaUtil.getTableName(schemaName, dataTableName);

        Properties props = PropertiesUtil.deepCopy(TEST_PROPERTIES);
        try (Connection conn = DriverManager.getConnection(getUrl(), props)) {
            conn.setAutoCommit(true);
            int numOfRows = 0;
            createTableAndUpsertRows(conn, dataTableFullName, numOfRows, tableDDLOptions);
            SingleCellIndexIT.assertMetadata(conn, PTable.ImmutableStorageScheme.ONE_CELL_PER_COLUMN, PTable.QualifierEncodingScheme.NON_ENCODED_QUALIFIERS, dataTableFullName);

            conn.createStatement().execute("ALTER TABLE " + dataTableFullName +
                    " SET IMMUTABLE_STORAGE_SCHEME=SINGLE_CELL_ARRAY_WITH_OFFSETS, COLUMN_ENCODED_BYTES=2");
            SystemTransformRecord record = Transform.getTransformRecord(schemaName, dataTableName, null, null, conn.unwrap(PhoenixConnection.class));
            assertNotNull(record);
            assertMetadata(conn, PTable.ImmutableStorageScheme.SINGLE_CELL_ARRAY_WITH_OFFSETS, PTable.QualifierEncodingScheme.TWO_BYTE_QUALIFIERS, record.getNewPhysicalTableName());

            String upsertQuery = String.format("UPSERT INTO %s VALUES(?, ?, ?)", dataTableFullName);
            PreparedStatement stmt1 = conn.prepareStatement(upsertQuery);
            IndexToolIT.upsertRow(stmt1, 1);

            IndexRegionObserver.setFailPostIndexUpdatesForTesting(true);
            IndexToolIT.upsertRow(stmt1, 2);

            assertEquals(1, getRowCountForEmptyColValue(conn, record.getNewPhysicalTableName(), UNVERIFIED_BYTES));
            assertEquals(1, getRowCountForEmptyColValue(conn, record.getNewPhysicalTableName(), VERIFIED_BYTES));

            IndexRegionObserver.setFailPostIndexUpdatesForTesting(false);

            Transform.doCutover(conn.unwrap(PhoenixConnection.class), record);
            Transform.updateTransformRecord(conn.unwrap(PhoenixConnection.class), record, PTable.TransformStatus.COMPLETED);

            assertEquals(1, getRowCountForEmptyColValue(conn, record.getNewPhysicalTableName(), UNVERIFIED_BYTES));
            assertEquals(1, getRowCountForEmptyColValue(conn, record.getNewPhysicalTableName(), VERIFIED_BYTES));

            // Now do read repair
            String select = "SELECT * FROM " + dataTableFullName;
            ResultSet rs = conn.createStatement().executeQuery(select);
            assertTrue(rs.next());
            assertTrue(rs.next());
            assertFalse(rs.next());

            assertEquals(0, getRowCountForEmptyColValue(conn, record.getNewPhysicalTableName(), UNVERIFIED_BYTES));
            assertEquals(2, getRowCountForEmptyColValue(conn, record.getNewPhysicalTableName(), VERIFIED_BYTES));
        } finally {
            IndexRegionObserver.setFailPreIndexUpdatesForTesting(false);
            IndexRegionObserver.setFailDataTableUpdatesForTesting(false);
            IndexRegionObserver.setFailPostIndexUpdatesForTesting(false);
        }
    }

    @Test
    public void testTransformIndexReadRepair() throws Exception {
        String schemaName = generateUniqueName();
        String dataTableName = generateUniqueName();
        String dataTableFullName = SchemaUtil.getTableName(schemaName, dataTableName);
        String indexTableName = "IDX_" + generateUniqueName();
        String indexTableFullName = SchemaUtil.getTableName(schemaName, indexTableName);

        Properties props = PropertiesUtil.deepCopy(TEST_PROPERTIES);
        try (Connection conn = DriverManager.getConnection(getUrl(), props)) {
            conn.setAutoCommit(true);
            int numOfRows = 0;
            createTableAndUpsertRows(conn, dataTableFullName, numOfRows, tableDDLOptions);
            SingleCellIndexIT.assertMetadata(conn, PTable.ImmutableStorageScheme.ONE_CELL_PER_COLUMN, PTable.QualifierEncodingScheme.NON_ENCODED_QUALIFIERS, dataTableFullName);

            conn.createStatement().execute("CREATE INDEX " + indexTableName + " ON " + dataTableFullName + " (NAME) INCLUDE (ZIP)");
            SingleCellIndexIT.assertMetadata(conn, PTable.ImmutableStorageScheme.ONE_CELL_PER_COLUMN, PTable.QualifierEncodingScheme.NON_ENCODED_QUALIFIERS, indexTableFullName);
            conn.createStatement().execute("ALTER INDEX " + indexTableName + " ON " + dataTableFullName +
                    " ACTIVE IMMUTABLE_STORAGE_SCHEME=SINGLE_CELL_ARRAY_WITH_OFFSETS, COLUMN_ENCODED_BYTES=2");

            SystemTransformRecord record = Transform.getTransformRecord(schemaName, indexTableName, dataTableFullName, null, conn.unwrap(PhoenixConnection.class));
            assertNotNull(record);
            assertMetadata(conn, PTable.ImmutableStorageScheme.SINGLE_CELL_ARRAY_WITH_OFFSETS, PTable.QualifierEncodingScheme.TWO_BYTE_QUALIFIERS, record.getNewPhysicalTableName());

            String upsertQuery = String.format("UPSERT INTO %s VALUES(?, ?, ?)", dataTableFullName);
            PreparedStatement stmt1 = conn.prepareStatement(upsertQuery);
            IndexToolIT.upsertRow(stmt1, 1);

            IndexRegionObserver.setFailPostIndexUpdatesForTesting(true);
            IndexToolIT.upsertRow(stmt1, 2);

            assertEquals(1, getRowCountForEmptyColValue(conn, record.getNewPhysicalTableName(), UNVERIFIED_BYTES));
            assertEquals(1, getRowCountForEmptyColValue(conn, record.getNewPhysicalTableName(), VERIFIED_BYTES));

            IndexRegionObserver.setFailPostIndexUpdatesForTesting(false);

            Transform.doCutover(conn.unwrap(PhoenixConnection.class), record);

            assertEquals(1, getRowCountForEmptyColValue(conn, record.getNewPhysicalTableName(), UNVERIFIED_BYTES));
            assertEquals(1, getRowCountForEmptyColValue(conn, record.getNewPhysicalTableName(), VERIFIED_BYTES));

            // Now do read repair
            String select = "SELECT NAME, ZIP FROM " + dataTableFullName;
            ResultSet rs = conn.createStatement().executeQuery(select);
            assertTrue(rs.next());
            assertTrue(rs.next());
            assertFalse(rs.next());

            assertEquals(0, getRowCountForEmptyColValue(conn, record.getNewPhysicalTableName(), UNVERIFIED_BYTES));
            assertEquals(2, getRowCountForEmptyColValue(conn, record.getNewPhysicalTableName(), VERIFIED_BYTES));
        } finally {
            IndexRegionObserver.setFailPreIndexUpdatesForTesting(false);
            IndexRegionObserver.setFailDataTableUpdatesForTesting(false);
            IndexRegionObserver.setFailPostIndexUpdatesForTesting(false);
        }
    }

    @Test
    public void testTransformMutationFailureRepair() throws Exception {
        String schemaName = generateUniqueName();
        String dataTableName = generateUniqueName();
        String dataTableFullName = SchemaUtil.getTableName(schemaName, dataTableName);
        String parentViewName = "VWP_" + generateUniqueName();
        String viewName = "VW_" + generateUniqueName();
        String viewIdxName = "VWIDX_" + generateUniqueName();

        Properties props = PropertiesUtil.deepCopy(TEST_PROPERTIES);
        try (Connection conn = DriverManager.getConnection(getUrl(), props)) {
            conn.setAutoCommit(true);
            int numOfRows = 0;
            createTableAndUpsertRows(conn, dataTableFullName, numOfRows, tableDDLOptions);
            String createParentViewSql = "CREATE VIEW " + parentViewName + " ( PARENT_VIEW_COL1 VARCHAR ) AS SELECT * FROM " + dataTableFullName;
            conn.createStatement().execute(createParentViewSql);

            String createViewSql = "CREATE VIEW " + viewName + " ( VIEW_COL1 INTEGER, VIEW_COL2 VARCHAR ) AS SELECT * FROM " + parentViewName;
            conn.createStatement().execute(createViewSql);

            String createViewIdxSql = "CREATE INDEX " + viewIdxName + " ON " + viewName + " (VIEW_COL1) include (VIEW_COL2) ";
            conn.createStatement().execute(createViewIdxSql);

            SingleCellIndexIT.assertMetadata(conn, PTable.ImmutableStorageScheme.ONE_CELL_PER_COLUMN, PTable.QualifierEncodingScheme.NON_ENCODED_QUALIFIERS, dataTableFullName);

            conn.createStatement().execute("ALTER TABLE " + dataTableFullName +
                    " SET IMMUTABLE_STORAGE_SCHEME=SINGLE_CELL_ARRAY_WITH_OFFSETS, COLUMN_ENCODED_BYTES=2");
            SystemTransformRecord record = Transform.getTransformRecord(schemaName, dataTableName, null, null, conn.unwrap(PhoenixConnection.class));
            assertNotNull(record);
            assertMetadata(conn, PTable.ImmutableStorageScheme.SINGLE_CELL_ARRAY_WITH_OFFSETS, PTable.QualifierEncodingScheme.TWO_BYTE_QUALIFIERS, record.getNewPhysicalTableName());
            IndexRegionObserver.setFailPreIndexUpdatesForTesting(true);
            String upsertQuery = String.format("UPSERT INTO %s VALUES(?, ?, ?)", viewName);
            PreparedStatement stmt1 = conn.prepareStatement(upsertQuery);
            try {
                IndexToolIT.upsertRow(stmt1, 1);
                fail("Transform table upsert should have failed");
            } catch (Exception e) {
            }
            assertEquals(0, getRowCount(conn, record.getNewPhysicalTableName()));
            assertEquals(0, getRowCount(conn, dataTableFullName));

            IndexRegionObserver.setFailPreIndexUpdatesForTesting(false);
            IndexRegionObserver.setFailPostIndexUpdatesForTesting(true);
            IndexToolIT.upsertRow(stmt1, 2);
            IndexToolIT.upsertRow(stmt1, 3);

            assertEquals(2, getRowCount(conn, record.getNewPhysicalTableName()));
            assertEquals(2, getRowCount(conn,dataTableFullName));

            IndexRegionObserver.setFailPostIndexUpdatesForTesting(false);
            IndexRegionObserver.setFailDataTableUpdatesForTesting(true);
            try {
                IndexToolIT.upsertRow(stmt1, 4);
                fail("Data table upsert should have failed");
            } catch (Exception e) {
            }
            try {
                IndexToolIT.upsertRow(stmt1, 5);
                fail("Data table upsert should have failed");
            } catch (Exception e) {
            }
            assertEquals(4, getRowCount(conn, record.getNewPhysicalTableName()));
            assertEquals(2, getRowCount(conn,dataTableFullName));

            IndexRegionObserver.setFailDataTableUpdatesForTesting(false);

            List<String> args = getArgList(schemaName, dataTableName, null,
                    null, null, null, false, false, false, false, true);
            runTransformTool(args.toArray(new String[0]), 0);

            assertEquals(2, getRowCount(conn.unwrap(PhoenixConnection.class).getQueryServices()
                    .getTable(Bytes.toBytes(dataTableFullName)), false));
            assertEquals(0, getRowCountForEmptyColValue(conn, record.getNewPhysicalTableName(), UNVERIFIED_BYTES));
            assertEquals(2, getRowCountForEmptyColValue(conn, record.getNewPhysicalTableName(), VERIFIED_BYTES));
        } finally {
            IndexRegionObserver.setFailPreIndexUpdatesForTesting(false);
            IndexRegionObserver.setFailDataTableUpdatesForTesting(false);
            IndexRegionObserver.setFailPostIndexUpdatesForTesting(false);
        }
    }

    @Test
    public void testTransformFailedForTransactionalTable() throws Exception {
        testTransactionalTableCannotTransform("OMID");
    }

    private void testTransactionalTableCannotTransform(String provider) throws Exception{
        String tableOptions = tableDDLOptions + " ,TRANSACTIONAL=true,TRANSACTION_PROVIDER='" + provider + "'";

        String schemaName = generateUniqueName();
        String dataTableName = generateUniqueName();
        String dataTableFullName = SchemaUtil.getTableName(schemaName, dataTableName);

        Properties props = PropertiesUtil.deepCopy(TEST_PROPERTIES);
        try (Connection conn = DriverManager.getConnection(getUrl(), props)) {
            conn.setAutoCommit(true);
            int numOfRows = 1;
            createTableAndUpsertRows(conn, dataTableFullName, numOfRows, tableOptions);
            SingleCellIndexIT.assertMetadata(conn, PTable.ImmutableStorageScheme.ONE_CELL_PER_COLUMN, PTable.QualifierEncodingScheme.NON_ENCODED_QUALIFIERS, dataTableFullName);

            try {
                conn.createStatement().execute("ALTER TABLE " + dataTableFullName +
                        " SET IMMUTABLE_STORAGE_SCHEME=SINGLE_CELL_ARRAY_WITH_OFFSETS, COLUMN_ENCODED_BYTES=2");
            } catch (SQLException ex) {
                assertEquals(SQLExceptionCode.CANNOT_TRANSFORM_TRANSACTIONAL_TABLE.getErrorCode(), ex.getErrorCode());
            }
            // Even when we run TransformTool again, verified bit is not cleared but the empty column stays as is
            List<String> args = getArgList(schemaName, dataTableName, null,
                    null, null, null, false, false, false, false, false);

            runTransformTool(args.toArray(new String[0]), -1);
        }
    }

    @Test
    public void testTransformVerify() throws Exception {
        String schemaName = generateUniqueName();
        String dataTableName = generateUniqueName();
        String dataTableFullName = SchemaUtil.getTableName(schemaName, dataTableName);

        Properties props = PropertiesUtil.deepCopy(TEST_PROPERTIES);
        try (Connection conn = DriverManager.getConnection(getUrl(), props)) {
            conn.setAutoCommit(true);
            int numOfRows = 2;
            createTableAndUpsertRows(conn, dataTableFullName, numOfRows, tableDDLOptions);
            SingleCellIndexIT.assertMetadata(conn, PTable.ImmutableStorageScheme.ONE_CELL_PER_COLUMN, PTable.QualifierEncodingScheme.NON_ENCODED_QUALIFIERS, dataTableFullName);

            conn.createStatement().execute("ALTER TABLE " + dataTableFullName +
                    " SET IMMUTABLE_STORAGE_SCHEME=SINGLE_CELL_ARRAY_WITH_OFFSETS, COLUMN_ENCODED_BYTES=2");
            SystemTransformRecord record = Transform.getTransformRecord(schemaName, dataTableName, null, null, conn.unwrap(PhoenixConnection.class));
            assertNotNull(record);
            assertMetadata(conn, PTable.ImmutableStorageScheme.SINGLE_CELL_ARRAY_WITH_OFFSETS, PTable.QualifierEncodingScheme.TWO_BYTE_QUALIFIERS, record.getNewPhysicalTableName());
            IndexRegionObserver.setFailPreIndexUpdatesForTesting(true);
            String upsertQuery = String.format("UPSERT INTO %s VALUES(?, ?, ?)", dataTableFullName);
            PreparedStatement stmt1 = conn.prepareStatement(upsertQuery);
            try {
                IndexToolIT.upsertRow(stmt1, numOfRows+1);
                fail("New table upsert should have failed");
            } catch (Exception e) {
            }
            // We didn't run transform tool yet. So new table would have 0.
            assertEquals(0, getRowCount(conn, record.getNewPhysicalTableName()));
            assertEquals(numOfRows, getRowCount(conn, dataTableFullName));

            IndexRegionObserver.setFailPreIndexUpdatesForTesting(false);
            IndexRegionObserver.setFailPostIndexUpdatesForTesting(true);
            IndexToolIT.upsertRow(stmt1, ++numOfRows);
            IndexToolIT.upsertRow(stmt1, ++numOfRows);

            // 2 missing in the new table since it is the initial rows
            assertEquals(numOfRows-2, getRowCount(conn, record.getNewPhysicalTableName()));
            assertEquals(numOfRows, getRowCount(conn,dataTableFullName));

            IndexRegionObserver.setFailPostIndexUpdatesForTesting(false);
            IndexRegionObserver.setFailDataTableUpdatesForTesting(true);
            try {
                IndexToolIT.upsertRow(stmt1, numOfRows+1);
                fail("Data table upsert should have failed");
            } catch (Exception e) {
            }
            //  Three unverified row in new table, original 2 rows missing since transform tool did not run.
            //  One unverified row is not in the data table. 2 unverified row is in the data table.
            assertEquals(numOfRows-1, getRowCount(conn, record.getNewPhysicalTableName()));
            assertEquals(numOfRows, getRowCount(conn,dataTableFullName));

            IndexRegionObserver.setFailDataTableUpdatesForTesting(false);

            List<String> args = getArgList(schemaName, dataTableName, null,
                    null, null, null, false, false, false, false, false);
            args.add("-v");
            args.add(IndexTool.IndexVerifyType.ONLY.getValue());
            // Run only validation and check
            TransformTool transformTool = runTransformTool(args.toArray(new String[0]), 0);
            assertEquals(numOfRows, transformTool.getJob().getCounters().findCounter(INPUT_RECORDS).getValue());
            assertEquals(numOfRows, transformTool.getJob().getCounters().findCounter(SCANNED_DATA_ROW_COUNT).getValue());
            assertEquals(0, transformTool.getJob().getCounters().findCounter(REBUILT_INDEX_ROW_COUNT).getValue());
            assertEquals(numOfRows-2, transformTool.getJob().getCounters().findCounter(BEFORE_REBUILD_VALID_INDEX_ROW_COUNT).getValue());
            assertEquals(2, transformTool.getJob().getCounters().findCounter(BEFORE_REBUILD_MISSING_INDEX_ROW_COUNT).getValue());
            assertEquals(2, transformTool.getJob().getCounters().findCounter(BEFORE_REBUILD_UNVERIFIED_INDEX_ROW_COUNT).getValue());

            args = getArgList(schemaName, dataTableName, null,
                    null, null, null, false, false, false, false, false);
            args.add("-v");
            args.add(IndexTool.IndexVerifyType.AFTER.getValue());
            // Run after validation and check that 2 missing (initial) rows are built.
            transformTool = runTransformTool(args.toArray(new String[0]), 0);
            assertEquals(numOfRows, transformTool.getJob().getCounters().findCounter(INPUT_RECORDS).getValue());
            assertEquals(numOfRows, transformTool.getJob().getCounters().findCounter(SCANNED_DATA_ROW_COUNT).getValue());
            assertEquals(numOfRows, transformTool.getJob().getCounters().findCounter(REBUILT_INDEX_ROW_COUNT).getValue());
            assertEquals(numOfRows, transformTool.getJob().getCounters().findCounter(AFTER_REBUILD_VALID_INDEX_ROW_COUNT).getValue());

            int numOfRows2= 2;
            String dataTableName2 = generateUniqueName();
            String dataTableFullName2 = SchemaUtil.getTableName(schemaName, dataTableName2);
            createTableAndUpsertRows(conn, dataTableFullName2, numOfRows2, tableDDLOptions);
            conn.createStatement().execute("ALTER TABLE " + dataTableFullName2 +
                    " SET IMMUTABLE_STORAGE_SCHEME=SINGLE_CELL_ARRAY_WITH_OFFSETS, COLUMN_ENCODED_BYTES=2");
            record = Transform.getTransformRecord(schemaName, dataTableName2, null, null, conn.unwrap(PhoenixConnection.class));
            assertNotNull(record);

            //  Original 2 rows missing since transform tool did not run
            assertEquals(0, getRowCount(conn, record.getNewPhysicalTableName()));
            assertEquals(numOfRows2, getRowCount(conn,dataTableFullName2));

            args = getArgList(schemaName, dataTableName2, null,
                    null, null, null, false, false, false, false, false);
            args.add("-v");
            args.add(IndexTool.IndexVerifyType.BEFORE.getValue());
            // Run before validation and check.
            transformTool = runTransformTool(args.toArray(new String[0]), 0);
            assertEquals(numOfRows2, transformTool.getJob().getCounters().findCounter(INPUT_RECORDS).getValue());
            assertEquals(numOfRows2, transformTool.getJob().getCounters().findCounter(SCANNED_DATA_ROW_COUNT).getValue());
            assertEquals(numOfRows2, transformTool.getJob().getCounters().findCounter(REBUILT_INDEX_ROW_COUNT).getValue());
            assertEquals(numOfRows2-2, transformTool.getJob().getCounters().findCounter(BEFORE_REBUILD_VALID_INDEX_ROW_COUNT).getValue());
            assertEquals(numOfRows2, transformTool.getJob().getCounters().findCounter(BEFORE_REBUILD_MISSING_INDEX_ROW_COUNT).getValue());
            assertEquals(0, transformTool.getJob().getCounters().findCounter(BEFORE_REBUILD_UNVERIFIED_INDEX_ROW_COUNT).getValue());
        } finally {
            IndexRegionObserver.setFailPreIndexUpdatesForTesting(false);
            IndexRegionObserver.setFailDataTableUpdatesForTesting(false);
            IndexRegionObserver.setFailPostIndexUpdatesForTesting(false);
        }
    }

    @Test
    public void testTransformVerify_shouldFixUnverified() throws Exception {
        String schemaName = generateUniqueName();
        String dataTableName = generateUniqueName();
        String dataTableFullName = SchemaUtil.getTableName(schemaName, dataTableName);

        Properties props = PropertiesUtil.deepCopy(TEST_PROPERTIES);
        try (Connection conn = DriverManager.getConnection(getUrl(), props)) {
            conn.setAutoCommit(true);
            int numOfRows = 1;
            int numOfRowsInNewTbl = 0;
            createTableAndUpsertRows(conn, dataTableFullName, numOfRows, tableDDLOptions);
            SingleCellIndexIT.assertMetadata(conn, PTable.ImmutableStorageScheme.ONE_CELL_PER_COLUMN, PTable.QualifierEncodingScheme.NON_ENCODED_QUALIFIERS, dataTableFullName);

            conn.createStatement().execute("ALTER TABLE " + dataTableFullName +
                    " SET IMMUTABLE_STORAGE_SCHEME=SINGLE_CELL_ARRAY_WITH_OFFSETS, COLUMN_ENCODED_BYTES=2");
            SystemTransformRecord record = Transform.getTransformRecord(schemaName, dataTableName, null, null, conn.unwrap(PhoenixConnection.class));
            assertNotNull(record);
            assertMetadata(conn, PTable.ImmutableStorageScheme.SINGLE_CELL_ARRAY_WITH_OFFSETS, PTable.QualifierEncodingScheme.TWO_BYTE_QUALIFIERS, record.getNewPhysicalTableName());
            String upsertQuery = String.format("UPSERT INTO %s VALUES(?, ?, ?)", dataTableFullName);
            PreparedStatement stmt1 = conn.prepareStatement(upsertQuery);
            // We didn't run transform tool yet. So new table would have 0.
            assertEquals(0, getRowCount(conn, record.getNewPhysicalTableName()));
            assertEquals(numOfRows, getRowCount(conn, dataTableFullName));

            IndexRegionObserver.setFailPostIndexUpdatesForTesting(true);
            IndexToolIT.upsertRow(stmt1, ++numOfRows);
            numOfRowsInNewTbl++;
            IndexToolIT.upsertRow(stmt1, ++numOfRows);
            numOfRowsInNewTbl++;

            // 1 missing in the new table since it is the initial rows
            assertEquals(numOfRows-1, getRowCount(conn, record.getNewPhysicalTableName()));
            assertEquals(numOfRows, getRowCount(conn,dataTableFullName));

            IndexRegionObserver.setFailPostIndexUpdatesForTesting(false);
            IndexRegionObserver.setFailDataTableUpdatesForTesting(true);
            try {
                numOfRowsInNewTbl++;
                IndexToolIT.upsertRow(stmt1, numOfRows+1);
                fail("Data table upsert should have failed");
            } catch (Exception e) {
            }
            try {
                numOfRowsInNewTbl++;
                IndexToolIT.upsertRow(stmt1, numOfRows+2);
                fail("Data table upsert should have failed");
            } catch (Exception e) {
            }
            //  Four unverified row in new table, original 1 row is missing since transform tool did not run.
            //  Two of the unverified rows is not in the data table.
            assertEquals(numOfRowsInNewTbl, getRowCount(conn, record.getNewPhysicalTableName()));
            assertEquals(numOfRows, getRowCount(conn,dataTableFullName));

            IndexRegionObserver.setFailDataTableUpdatesForTesting(false);
            List<String> args = getArgList(schemaName, dataTableName, null,
                    null, null, null, false, false, false, false, true);
            args.add("-v");
            args.add(IndexTool.IndexVerifyType.BEFORE.getValue());
            TransformTool transformTool = runTransformTool(args.toArray(new String[0]), 0);
            // Run after validation and check that 2 missing (initial) rows are built but the unverified one is not built yet since we didn't build with -fixunverified option.
            assertEquals(numOfRowsInNewTbl, transformTool.getJob().getCounters().findCounter(INPUT_RECORDS).getValue());
            assertEquals(2, transformTool.getJob().getCounters().findCounter(BEFORE_REPAIR_EXTRA_UNVERIFIED_INDEX_ROW_COUNT).getValue());
        } finally {
            IndexRegionObserver.setFailPreIndexUpdatesForTesting(false);
            IndexRegionObserver.setFailDataTableUpdatesForTesting(false);
            IndexRegionObserver.setFailPostIndexUpdatesForTesting(false);
        }
    }

    @Test
    public void testTransformVerify_VerifyOnlyShouldNotChangeTransformState() throws Exception {
        String schemaName = generateUniqueName();
        String dataTableName = generateUniqueName();
        String dataTableFullName = SchemaUtil.getTableName(schemaName, dataTableName);
        String indexTableName = "IDX_" + generateUniqueName();
        String indexTableFullName = SchemaUtil.getTableName(schemaName, indexTableName);

        Properties props = PropertiesUtil.deepCopy(TEST_PROPERTIES);
        try (Connection conn = DriverManager.getConnection(getUrl(), props)) {
            conn.setAutoCommit(true);
            int numOfRows = 1;
            createTableAndUpsertRows(conn, dataTableFullName, numOfRows, tableDDLOptions);
            SingleCellIndexIT.assertMetadata(conn, PTable.ImmutableStorageScheme.ONE_CELL_PER_COLUMN, PTable.QualifierEncodingScheme.NON_ENCODED_QUALIFIERS, dataTableFullName);

            conn.createStatement().execute("CREATE INDEX " + indexTableName + " ON " + dataTableFullName + " (NAME) INCLUDE (ZIP)");
            SingleCellIndexIT.assertMetadata(conn, PTable.ImmutableStorageScheme.ONE_CELL_PER_COLUMN, PTable.QualifierEncodingScheme.NON_ENCODED_QUALIFIERS, indexTableFullName);
            conn.createStatement().execute("ALTER INDEX " + indexTableName + " ON " + dataTableFullName +
                    " ACTIVE IMMUTABLE_STORAGE_SCHEME=SINGLE_CELL_ARRAY_WITH_OFFSETS, COLUMN_ENCODED_BYTES=2");

            SystemTransformRecord record = Transform.getTransformRecord(schemaName, indexTableName, dataTableFullName, null, conn.unwrap(PhoenixConnection.class));
            assertNotNull(record);

            List<String> args = getArgList(schemaName, dataTableName, indexTableName,
                    null, null, null, false, false, false, false, false);
            args.add("-v");
            args.add(IndexTool.IndexVerifyType.ONLY.getValue());
            TransformTool transformTool = runTransformTool(args.toArray(new String[0]), 0);
            // No change
            assertEquals(PTable.TransformStatus.CREATED.toString(), record.getTransformStatus());

            // Now test data table
            conn.createStatement().execute("ALTER TABLE " + dataTableFullName +
                    " SET IMMUTABLE_STORAGE_SCHEME=SINGLE_CELL_ARRAY_WITH_OFFSETS, COLUMN_ENCODED_BYTES=2");
            record = Transform.getTransformRecord(schemaName, dataTableName, null, null, conn.unwrap(PhoenixConnection.class));
            assertNotNull(record);
            args = getArgList(schemaName, dataTableName, null,
                    null, null, null, false, false, false, false, false);
            args.add("-v");
            args.add(IndexTool.IndexVerifyType.ONLY.getValue());
            runTransformTool(args.toArray(new String[0]), 0);
            assertEquals(PTable.TransformStatus.CREATED.toString(), record.getTransformStatus());
            assertEquals(0, getRowCount(conn, record.getNewPhysicalTableName()));
        }
    }

    @Test
    public void testTransformVerify_ForceCutover() throws Exception {
        String schemaName = generateUniqueName();
        String dataTableName = generateUniqueName();
        String dataTableFullName = SchemaUtil.getTableName(schemaName, dataTableName);
        String indexTableName = "IDX_" + generateUniqueName();
        String indexTableFullName = SchemaUtil.getTableName(schemaName, indexTableName);

        Properties props = PropertiesUtil.deepCopy(TEST_PROPERTIES);
        try (PhoenixConnection conn = (PhoenixConnection) DriverManager.getConnection(getUrl(),
                props)) {
            conn.setAutoCommit(true);
            int numOfRows = 1;
            createTableAndUpsertRows(conn, dataTableFullName, numOfRows, tableDDLOptions);
            SingleCellIndexIT.assertMetadata(conn, PTable.ImmutableStorageScheme.ONE_CELL_PER_COLUMN, PTable.QualifierEncodingScheme.NON_ENCODED_QUALIFIERS, dataTableFullName);

            conn.createStatement().execute("CREATE INDEX " + indexTableName + " ON " + dataTableFullName + " (NAME) INCLUDE (ZIP)");
            SingleCellIndexIT.assertMetadata(conn, PTable.ImmutableStorageScheme.ONE_CELL_PER_COLUMN, PTable.QualifierEncodingScheme.NON_ENCODED_QUALIFIERS, indexTableFullName);
            conn.createStatement().execute("ALTER INDEX " + indexTableName + " ON " + dataTableFullName +
                    " ACTIVE IMMUTABLE_STORAGE_SCHEME=SINGLE_CELL_ARRAY_WITH_OFFSETS, COLUMN_ENCODED_BYTES=2");

            List<String> args = getArgList(schemaName, dataTableName, indexTableName,
                    null, null, null, false, false, false, false, false);
            args.add("-fco");
            runTransformTool(args.toArray(new String[0]), 0);

            SystemTransformRecord record = Transform.getTransformRecord(schemaName, indexTableName, dataTableFullName, null, conn.unwrap(PhoenixConnection.class));
            assertNotNull(record);
            assertTransformStatusOrPartial(PTable.TransformStatus.COMPLETED, record);
            PTable pOldIndexTable = conn.getTableNoCache(indexTableFullName);
            assertEquals(SchemaUtil.getTableNameFromFullName(record.getNewPhysicalTableName()),
                    pOldIndexTable.getPhysicalName(true).getString());

            // Now test data table
            conn.createStatement().execute("ALTER TABLE " + dataTableFullName +
                    " SET IMMUTABLE_STORAGE_SCHEME=SINGLE_CELL_ARRAY_WITH_OFFSETS, COLUMN_ENCODED_BYTES=2");
            args = getArgList(schemaName, dataTableName, null,
                    null, null, null, false, false, false, false, false);
            args.add("-fco");
            runTransformTool(args.toArray(new String[0]), 0);

            record = Transform.getTransformRecord(schemaName, dataTableName, null, null, conn.unwrap(PhoenixConnection.class));
            assertNotNull(record);
            assertTransformStatusOrPartial(PTable.TransformStatus.COMPLETED, record);
            PTable pOldTable = conn.getTableNoCache(dataTableFullName);
            assertEquals(SchemaUtil.getTableNameFromFullName(record.getNewPhysicalTableName()),
                    pOldTable.getPhysicalName(true).getString());
        }
    }

    @Test
    public void testTransformForGlobalViews() throws Exception {
        String schemaName = generateUniqueName();
        String dataTableName = generateUniqueName();
        String dataTableFullName = SchemaUtil.getTableName(schemaName, dataTableName);
        String view1Name = "VW1_" + generateUniqueName();
        String view2Name = "VW2_" + generateUniqueName();
        String upsertQuery = "UPSERT INTO %s VALUES(?, ?, ?, ?, ?, ?)";

        Properties props = PropertiesUtil.deepCopy(TEST_PROPERTIES);
        try (Connection conn = DriverManager.getConnection(getUrl(), props)) {
            conn.setAutoCommit(true);
            int numOfRows = 0;
            createTableAndUpsertRows(conn, dataTableFullName, numOfRows, tableDDLOptions);
            SingleCellIndexIT.assertMetadata(conn, PTable.ImmutableStorageScheme.ONE_CELL_PER_COLUMN, PTable.QualifierEncodingScheme.NON_ENCODED_QUALIFIERS, dataTableFullName);

            String createViewSql = "CREATE VIEW " + view1Name + " ( VIEW_COL11 INTEGER, VIEW_COL12 VARCHAR ) AS SELECT * FROM "
                    + dataTableFullName + " where ID=1";
            conn.createStatement().execute(createViewSql);

            createViewSql = "CREATE VIEW " + view2Name + " ( VIEW_COL21 INTEGER, VIEW_COL22 VARCHAR ) AS SELECT * FROM "
                    + dataTableFullName + " where ID=11";
            conn.createStatement().execute(createViewSql);

            PreparedStatement stmt1 = conn.prepareStatement(String.format(upsertQuery, view1Name));
            stmt1.setInt(1, 1);
            stmt1.setString(2, "uname1");
            stmt1.setInt(3, 95051);
            stmt1.setString(4, "");
            stmt1.setInt(5, 101);
            stmt1.setString(6, "viewCol12");
            stmt1.executeUpdate();
            conn.commit();

            stmt1 = conn.prepareStatement(String.format(upsertQuery, view2Name));
            stmt1.setInt(1, 11);
            stmt1.setString(2, "uname11");
            stmt1.setInt(3, 950511);
            stmt1.setString(4, "");
            stmt1.setInt(5, 111);
            stmt1.setString(6, "viewCol22");
            stmt1.executeUpdate();
            conn.commit();

            conn.createStatement().execute("ALTER TABLE " + dataTableFullName +
                    " SET IMMUTABLE_STORAGE_SCHEME=SINGLE_CELL_ARRAY_WITH_OFFSETS, COLUMN_ENCODED_BYTES=2");
            SystemTransformRecord record = Transform.getTransformRecord(schemaName, dataTableName, null, null, conn.unwrap(PhoenixConnection.class));
            assertNotNull(record);
            assertMetadata(conn, PTable.ImmutableStorageScheme.SINGLE_CELL_ARRAY_WITH_OFFSETS, PTable.QualifierEncodingScheme.TWO_BYTE_QUALIFIERS, record.getNewPhysicalTableName());

            List<String> args = getArgList(schemaName, dataTableName, null,
                    null, null, null, false, false, false, false, false);
            runTransformTool(args.toArray(new String[0]), 0);
            Transform.doCutover(conn.unwrap(PhoenixConnection.class), record);
            Transform.updateTransformRecord(conn.unwrap(PhoenixConnection.class), record, PTable.TransformStatus.COMPLETED);
            try (Admin admin = conn.unwrap(PhoenixConnection.class).getQueryServices().getAdmin()) {
                admin.disableTable(TableName.valueOf(dataTableFullName));
                admin.truncateTable(TableName.valueOf(dataTableFullName), true);
            }

            String sql = "SELECT VIEW_COL11, VIEW_COL12 FROM %s ";
            ResultSet rs1 = conn.createStatement().executeQuery(String.format(sql, view1Name));
            assertTrue(rs1.next());
            assertEquals(101, rs1.getInt(1));
            assertEquals("viewCol12", rs1.getString(2));

            sql = "SELECT VIEW_COL21, VIEW_COL22 FROM %s ";
            rs1 = conn.createStatement().executeQuery(String.format(sql, view2Name));
            assertTrue(rs1.next());
            assertEquals(111, rs1.getInt(1));
            assertEquals("viewCol22", rs1.getString(2));
        }
    }

    @Test
    public void testTransformForTenantViews() throws Exception {
        String schemaName = generateUniqueName();
        String dataTableName = generateUniqueName();
        String dataTableFullName = SchemaUtil.getTableName(schemaName, dataTableName);
        String view1Name = "VW1_" + generateUniqueName();
        String view2Name = "VW2_" + generateUniqueName();
        String upsertQuery = "UPSERT INTO %s VALUES(?, ?, ?, ?, ?, ?)";

        Properties props = PropertiesUtil.deepCopy(TEST_PROPERTIES);
        try (Connection conn = DriverManager.getConnection(getUrl(), props)) {
            conn.setAutoCommit(true);
            int numOfRows = 0;
            createTableAndUpsertRows(conn, dataTableFullName, numOfRows, tableDDLOptions);
            SingleCellIndexIT.assertMetadata(conn, PTable.ImmutableStorageScheme.ONE_CELL_PER_COLUMN, PTable.QualifierEncodingScheme.NON_ENCODED_QUALIFIERS, dataTableFullName);
        }

        try (Connection tenantConn1 = getTenantConnection("tenant1")) {
            String createViewSql = "CREATE VIEW " + view1Name + " ( VIEW_COL11 INTEGER, VIEW_COL12 VARCHAR ) AS SELECT * FROM "
                    + dataTableFullName + " where ID=1";
            tenantConn1.createStatement().execute(createViewSql);
        }

        try (Connection tenantConn2 = getTenantConnection("tenant2")) {
            String createViewSql = "CREATE VIEW " + view2Name + " ( VIEW_COL21 INTEGER, VIEW_COL22 VARCHAR ) AS SELECT * FROM "
                    + dataTableFullName + " where ID=11";
            tenantConn2.createStatement().execute(createViewSql);
        }

        try (Connection tenantConn1 = getTenantConnection("tenant1")) {
            PreparedStatement stmt1 = tenantConn1.prepareStatement(String.format(upsertQuery, view1Name));
            stmt1.setInt(1, 1);
            stmt1.setString(2, "uname1");
            stmt1.setInt(3, 95051);
            stmt1.setString(4, "");
            stmt1.setInt(5, 101);
            stmt1.setString(6, "viewCol12");
            stmt1.executeUpdate();
            tenantConn1.commit();
        }

        try (Connection tenantConn2 = getTenantConnection("tenant2")) {
            PreparedStatement stmt1 = tenantConn2.prepareStatement(String.format(upsertQuery, view2Name));
            stmt1.setInt(1, 11);
            stmt1.setString(2, "uname11");
            stmt1.setInt(3, 950511);
            stmt1.setString(4, "");
            stmt1.setInt(5, 111);
            stmt1.setString(6, "viewCol22");
            stmt1.executeUpdate();
            tenantConn2.commit();
        }

        try (Connection conn = DriverManager.getConnection(getUrl(), props)) {
            conn.createStatement().execute("ALTER TABLE " + dataTableFullName +
                    " SET IMMUTABLE_STORAGE_SCHEME=SINGLE_CELL_ARRAY_WITH_OFFSETS, COLUMN_ENCODED_BYTES=2");
            SystemTransformRecord record = Transform.getTransformRecord(schemaName, dataTableName, null, null, conn.unwrap(PhoenixConnection.class));
            assertNotNull(record);
            assertMetadata(conn, PTable.ImmutableStorageScheme.SINGLE_CELL_ARRAY_WITH_OFFSETS, PTable.QualifierEncodingScheme.TWO_BYTE_QUALIFIERS, record.getNewPhysicalTableName());

            List<String> args = getArgList(schemaName, dataTableName, null,
                    null, null, null, false, false, false, false, false);
            runTransformTool(args.toArray(new String[0]), 0);
            Transform.doCutover(conn.unwrap(PhoenixConnection.class), record);
            Transform.updateTransformRecord(conn.unwrap(PhoenixConnection.class), record, PTable.TransformStatus.COMPLETED);
            try (Admin admin = conn.unwrap(PhoenixConnection.class).getQueryServices().getAdmin()) {
                admin.disableTable(TableName.valueOf(dataTableFullName));
                admin.truncateTable(TableName.valueOf(dataTableFullName), true);
            }
        }

        try (Connection tenantConn1 = getTenantConnection("tenant1")) {
            String sql = "SELECT VIEW_COL11, VIEW_COL12 FROM %s ";
            ResultSet rs1 = tenantConn1.createStatement().executeQuery(String.format(sql, view1Name));
            assertTrue(rs1.next());
            assertEquals(101, rs1.getInt(1));
            assertEquals("viewCol12", rs1.getString(2));
        }

        try (Connection tenantConn2 = getTenantConnection("tenant2")) {
            String sql = "SELECT VIEW_COL21, VIEW_COL22 FROM %s ";
            ResultSet rs1 = tenantConn2.createStatement().executeQuery(String.format(sql, view2Name));
            assertTrue(rs1.next());
            assertEquals(111, rs1.getInt(1));
            assertEquals("viewCol22", rs1.getString(2));
        }
    }


    public static Connection getTenantConnection(String tenant) throws SQLException {
        Properties props = new Properties();
        props.setProperty(PhoenixRuntime.TENANT_ID_ATTRIB, tenant);
        return DriverManager.getConnection(getUrl(), props);
    }

    public static void assertTransformStatusOrPartial(PTable.TransformStatus expectedStatus, SystemTransformRecord systemTransformRecord) {
        if (systemTransformRecord.getTransformStatus().equals(expectedStatus.name())) {
            return;
        }

        assertEquals(true, systemTransformRecord.getTransformType().toString().contains("PARTIAL"));
    }

    public static List<String> getArgList(String schemaName, String dataTable, String indxTable, String tenantId,
                                          Long startTime, Long endTime,
                                          boolean shouldAbort, boolean shouldPause, boolean shouldResume, boolean isPartial,
                                          boolean shouldFixUnverified) {
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
        if (shouldFixUnverified) {
            args.add("-fu");
        }
        return args;
    }


    public static String [] getArgValues(String schemaName,
                                         String dataTable, String indexTable, String tenantId,
                                         Long startTime, Long endTime) {
        List<String> args = getArgList(schemaName, dataTable, indexTable,
                tenantId, startTime, endTime, false, false, false, false, false);
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