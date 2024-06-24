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

import org.apache.phoenix.coprocessorclient.BaseScannerRegionObserverConstants;
import org.apache.phoenix.thirdparty.com.google.common.collect.Maps;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.mapreduce.Counters;
import org.apache.hadoop.mapreduce.Job;
import org.apache.phoenix.mapreduce.index.IndexScrutinyMapper;
import org.apache.phoenix.mapreduce.index.IndexScrutinyTableOutput;
import org.apache.phoenix.mapreduce.index.IndexScrutinyTool;
import org.apache.phoenix.query.QueryServices;
import org.apache.phoenix.schema.TableNotFoundException;
import org.apache.phoenix.util.EnvironmentEdgeManager;
import org.apache.phoenix.util.ManualEnvironmentEdge;
import org.apache.phoenix.util.MetaDataUtil;
import org.apache.phoenix.util.PropertiesUtil;
import org.apache.phoenix.util.ReadOnlyProps;
import org.apache.phoenix.util.SchemaUtil;
import org.apache.phoenix.util.TestUtil;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.List;
import java.util.Map;

import static org.apache.phoenix.mapreduce.index.PhoenixScrutinyJobCounters.INVALID_ROW_COUNT;
import static org.apache.phoenix.mapreduce.index.PhoenixScrutinyJobCounters.BEYOND_MAX_LOOKBACK_COUNT;
import static org.apache.phoenix.mapreduce.index.PhoenixScrutinyJobCounters.VALID_ROW_COUNT;
import static org.apache.phoenix.util.TestUtil.TEST_PROPERTIES;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

public class IndexScrutinyWithMaxLookbackIT extends IndexScrutinyToolBaseIT {

    private static PreparedStatement upsertDataStmt;
    private static String dataTableFullName;
    private static String schema;
    private static String dataTableName;
    private static String indexTableName;
    private static String indexTableFullName;
    private static String viewName;
    private static boolean isViewIndex;
    private static ManualEnvironmentEdge testClock;
    public static final String UPSERT_DATA = "UPSERT INTO %s VALUES (?, ?, ?)";
    public static final String DELETE_SCRUTINY_METADATA = "DELETE FROM "
        + IndexScrutinyTableOutput.OUTPUT_METADATA_TABLE_NAME;
    public static final String DELETE_SCRUTINY_OUTPUT = "DELETE FROM " +
        IndexScrutinyTableOutput.OUTPUT_TABLE_NAME;
    public static final int MAX_LOOKBACK = 6;
    private long scrutinyTs;


    @BeforeClass
    public static synchronized void doSetup() throws Exception {
        Map<String, String> props = Maps.newHashMapWithExpectedSize(2);
        props.put(QueryServices.GLOBAL_INDEX_ROW_AGE_THRESHOLD_TO_DELETE_MS_ATTRIB, Long.toString(0));
        props.put(BaseScannerRegionObserverConstants.PHOENIX_MAX_LOOKBACK_AGE_CONF_KEY,
            Integer.toString(MAX_LOOKBACK));
        props.put("hbase.procedure.remote.dispatcher.delay.msec", "0");
        setUpTestDriver(new ReadOnlyProps(props.entrySet().iterator()));
    }

    @Before
    public void setupTest() throws SQLException {
        try(Connection conn = DriverManager.getConnection(getUrl(),
            PropertiesUtil.deepCopy(TEST_PROPERTIES))){
            conn.createStatement().execute(DELETE_SCRUTINY_METADATA);
            conn.createStatement().execute(DELETE_SCRUTINY_OUTPUT);
            conn.commit();
        } catch (TableNotFoundException tnfe){
            //This will happen the first time
        }
    }

    @Test
    public void testScrutinyOnRowsBeyondMaxLookBack() throws Exception {
        setupTables();
        try {
            upsertDataAndScrutinize(dataTableName, dataTableFullName, testClock);
            assertBeyondMaxLookbackOutput(dataTableFullName, indexTableFullName);
        } finally {
            EnvironmentEdgeManager.reset();
        }

    }

    @Test
    public void testScrutinyOnRowsBeyondMaxLookback_viewIndex() throws Exception {
        schema = "S"+generateUniqueName();
        dataTableName = "T"+generateUniqueName();
        dataTableFullName = SchemaUtil.getTableName(schema,dataTableName);
        indexTableName = "VI"+generateUniqueName();
        isViewIndex = true;
        viewName = "V"+generateUniqueName();
        String viewFullName = SchemaUtil.getTableName(schema,viewName);
        String indexFullName = SchemaUtil.getTableName(schema, indexTableName);
        String dataTableDDL = "CREATE TABLE %s (ID INTEGER NOT NULL PRIMARY KEY, NAME VARCHAR, "
            + "ZIP INTEGER) COLUMN_ENCODED_BYTES = 0, VERSIONS = 1 ";
        String viewDDL = "CREATE VIEW %s AS SELECT * FROM %s";
        String indexTableDDL = "CREATE INDEX %s ON %s (NAME) INCLUDE (ZIP) VERSIONS = 1";
        testClock = new ManualEnvironmentEdge();

        try (Connection conn =
                 DriverManager.getConnection(getUrl(), PropertiesUtil.deepCopy(TEST_PROPERTIES))) {
            conn.createStatement().execute(String.format(dataTableDDL, dataTableFullName));
            conn.createStatement().execute(String.format(viewDDL, viewFullName, dataTableFullName));
            conn.createStatement().execute(String.format(indexTableDDL, indexTableName,
                viewFullName));
            conn.commit();
        }
        upsertDataAndScrutinize(viewName, viewFullName, testClock);
        assertBeyondMaxLookbackOutput(viewFullName, indexFullName);
    }

    private void assertBeyondMaxLookbackOutput(String dataTableName, String indexTableName)
        throws Exception {
        try (Connection conn =
                 DriverManager.getConnection(getUrl(), PropertiesUtil.deepCopy(TEST_PROPERTIES))) {
            ResultSet metadataRS = IndexScrutinyTableOutput.queryAllLatestMetadata(conn, dataTableName,
                    indexTableName);
            assertTrue("No results from scrutiny metadata query!", metadataRS.next());
            assertEquals(1, metadataRS.getLong("BEYOND_MAX_LOOKBACK_COUNT"));
            String beyondLookbackOutputSql =
                metadataRS.getString("INVALID_ROWS_QUERY_BEYOND_MAX_LOOKBACK");
            assertNotNull(beyondLookbackOutputSql);
            ResultSet outputRS = conn.createStatement().executeQuery(beyondLookbackOutputSql);
            assertTrue("No results from scrutiny beyond max lookback output query!",
                outputRS.next());
            assertTrue("Beyond max lookback flag not set",
                outputRS.getBoolean("BEYOND_MAX_LOOKBACK"));
            assertFalse("Too many rows output from scrutiny beyond max lookback output query!",
                outputRS.next());
        }
    }

    @Test
    public void testScrutinyOnDeletedRowsBeyondMaxLookBack() throws Exception {
        setupTables();
        try {
            upsertDataThenDeleteAndScrutinize(dataTableName, dataTableFullName, testClock);
            assertBeyondMaxLookbackOutput(dataTableFullName, indexTableFullName);
        } finally {
            EnvironmentEdgeManager.reset();
        }
    }

    private void setupTables() throws SQLException {
        schema = "S" + generateUniqueName();
        dataTableName = "T" + generateUniqueName();
        indexTableName = "I" + generateUniqueName();
        dataTableFullName = SchemaUtil.getTableName(schema, dataTableName);
        indexTableFullName = SchemaUtil.getTableName(schema, indexTableName);
        isViewIndex = false;
        String dataTableDDL = "CREATE TABLE %s (ID INTEGER NOT NULL PRIMARY KEY, NAME VARCHAR, "
            + "ZIP INTEGER) COLUMN_ENCODED_BYTES=0, VERSIONS=1";
        String indexTableDDL = "CREATE INDEX %s ON %s (NAME) INCLUDE (ZIP)";
        testClock = new ManualEnvironmentEdge();

        try (Connection conn =
                 DriverManager.getConnection(getUrl(), PropertiesUtil.deepCopy(TEST_PROPERTIES))) {
            conn.createStatement().execute(String.format(dataTableDDL, dataTableFullName));
            conn.createStatement().execute(String.format(indexTableDDL, indexTableName,
                dataTableFullName));
            conn.commit();
        }
    }

    private void upsertDataAndScrutinize(String tableName, String tableFullName,
                                         ManualEnvironmentEdge testClock)
        throws Exception {
        try(Connection conn =
                DriverManager.getConnection(getUrl(), PropertiesUtil.deepCopy(TEST_PROPERTIES))) {
            // insert two rows
            populateTable(tableFullName, conn);
            long afterInsertSCN = EnvironmentEdgeManager.currentTimeMillis() + 1;
            testClock.setValue(afterInsertSCN);
            EnvironmentEdgeManager.injectEdge(testClock);
            //move forward to the time we want to scrutinize, which is less than max lookback age
            //for the initial inserts
            testClock.incrementValue(MAX_LOOKBACK /2  * 1000);
            scrutinyTs = EnvironmentEdgeManager.currentTimeMillis();
            updateIndexRows(conn);
            //now go past max lookback age for the initial 2 inserts
            testClock.incrementValue(MAX_LOOKBACK /2  * 1000);
            List<Job> completedJobs = runScrutiny(schema, tableName, indexTableName, scrutinyTs);
            Job job = completedJobs.get(0);
            assertTrue(job.isSuccessful());
            assertCounters(job.getCounters());
        }
    }

    private void populateTable(String tableFullName, Connection conn) throws SQLException {
        upsertDataStmt = getUpsertDataStmt(tableFullName, conn);

        NonParameterizedIndexScrutinyToolIT.upsertRow(upsertDataStmt, 1, "name-1", 98051);
        NonParameterizedIndexScrutinyToolIT.upsertRow(upsertDataStmt, 2, "name-2", 98052);
        conn.commit();
    }

    private void updateIndexRows(Connection conn) throws SQLException {
        String tableName = isViewIndex ?
            SchemaUtil.getTableName(schema, viewName) : dataTableFullName;
        PreparedStatement stmt = getUpsertDataStmt(tableName, conn);
        NonParameterizedIndexScrutinyToolIT.upsertRow(stmt, 1, "name-1", 38139);
        conn.commit();
    }

    private void upsertDataThenDeleteAndScrutinize(String tableName, String tableFullName,
                                                   ManualEnvironmentEdge testClock)
        throws Exception {
        try(Connection conn =
                DriverManager.getConnection(getUrl(), PropertiesUtil.deepCopy(TEST_PROPERTIES))) {
            populateTable(tableFullName, conn);
            long afterInsertSCN = EnvironmentEdgeManager.currentTimeMillis() + 1;
            testClock.setValue(afterInsertSCN);
            EnvironmentEdgeManager.injectEdge(testClock);
            deleteIndexRows(conn);
            //move forward to the time we want to scrutinize, which is less than max lookback age
            //for the initial inserts
            testClock.incrementValue(MAX_LOOKBACK /2  * 1000);
            scrutinyTs = EnvironmentEdgeManager.currentTimeMillis();
            //now go past max lookback age for the initial 2 inserts
            testClock.incrementValue(MAX_LOOKBACK /2  * 1000);

            List<Job> completedJobs = runScrutiny(schema, tableName, indexTableName, scrutinyTs);
            Job job = completedJobs.get(0);
            assertTrue(job.isSuccessful());
            assertCounters(job.getCounters());
        }
    }

    private void deleteIndexRows(Connection conn) throws SQLException {
        String deleteSql = "DELETE FROM " + SchemaUtil.getTableName(schema, indexTableName) + " " +
            "LIMIT 1";
        conn.createStatement().execute(deleteSql);
        conn.commit();
    }

    private static PreparedStatement getUpsertDataStmt(String tableFullName, Connection conn) throws SQLException {
        return conn.prepareStatement(String.format(UPSERT_DATA, tableFullName));
    }

    private void assertCounters(Counters counters) {
        assertEquals(1, getCounterValue(counters, VALID_ROW_COUNT));
        assertEquals(1, getCounterValue(counters, BEYOND_MAX_LOOKBACK_COUNT));
        assertEquals(0, getCounterValue(counters, INVALID_ROW_COUNT));
    }

    private List<Job> runScrutiny(String schemaName, String dataTableName, String indexTableName,
                                  Long scrutinyTs)
        throws Exception {
        return runScrutiny(schemaName, dataTableName, indexTableName, null, null, scrutinyTs);
    }

    private List<Job> runScrutiny(String schemaName, String dataTableName, String indexTableName,
                                  Long batchSize, IndexScrutinyTool.SourceTable sourceTable,
                                  Long scrutinyTs) throws Exception {
        final String[]
            cmdArgs =
            getArgValues(schemaName, dataTableName, indexTableName, batchSize, sourceTable,
                true, IndexScrutinyTool.OutputFormat.TABLE,
                null, null, scrutinyTs);
        return runScrutiny(MaxLookbackIndexScrutinyMapper.class, cmdArgs);
    }

    private static class MaxLookbackIndexScrutinyMapper extends IndexScrutinyMapper {
        @Override
        public void postSetup(){
            try {
                String tableToCompact;
                if (isViewIndex){
                    String physicalDataTableName =
                        SchemaUtil.getPhysicalHBaseTableName(schema, dataTableName, false).getString();
                    tableToCompact = MetaDataUtil.getViewIndexPhysicalName(physicalDataTableName);
                } else {
                    tableToCompact = SchemaUtil.getPhysicalHBaseTableName(schema, indexTableName, false).getString();
                }
                TableName indexTable = TableName.valueOf(tableToCompact);
                testClock.incrementValue(1);
                getUtility().getAdmin().flush(indexTable);
                TestUtil.majorCompact(getUtility(), indexTable);
            } catch (Exception e){
                throw new RuntimeException(e);
            }

        }
    }
}