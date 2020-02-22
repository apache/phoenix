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

import com.google.common.collect.Maps;
import org.apache.hadoop.hbase.regionserver.ScanInfoUtil;
import org.apache.hadoop.mapreduce.Counters;
import org.apache.hadoop.mapreduce.Job;
import org.apache.phoenix.mapreduce.index.IndexScrutinyMapperForTest;
import org.apache.phoenix.mapreduce.index.IndexScrutinyTool;
import org.apache.phoenix.query.QueryServices;
import org.apache.phoenix.util.EnvironmentEdgeManager;
import org.apache.phoenix.util.PropertiesUtil;
import org.apache.phoenix.util.ReadOnlyProps;
import org.apache.phoenix.util.SchemaUtil;
import org.junit.BeforeClass;
import org.junit.Test;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.util.List;
import java.util.Map;

import static org.apache.phoenix.mapreduce.index.PhoenixScrutinyJobCounters.INVALID_ROW_COUNT;
import static org.apache.phoenix.mapreduce.index.PhoenixScrutinyJobCounters.BEYOND_MAX_LOOKBACK;
import static org.apache.phoenix.mapreduce.index.PhoenixScrutinyJobCounters.VALID_ROW_COUNT;
import static org.apache.phoenix.util.TestUtil.TEST_PROPERTIES;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class IndexScrutinyWithMaxLookBackIT extends IndexScrutinyToolBaseIT {

    @BeforeClass
    public static synchronized void doSetup() throws Exception {
        Map<String, String> props = Maps.newHashMapWithExpectedSize(1);
        props.put(QueryServices.GLOBAL_INDEX_ROW_AGE_THRESHOLD_TO_DELETE_MS_ATTRIB, Long.toString(0));
        props.put(ScanInfoUtil.PHOENIX_MAX_LOOKBACK_AGE_CONF_KEY, Integer.toString(IndexScrutinyMapperForTest.MAX_LOOKBACK));
        setUpTestDriver(new ReadOnlyProps(props.entrySet().iterator()));
    }

    @Test
    public void testScrutinyOnRowsBeyondMaxLookBack() throws Exception {
        String schema = generateUniqueName();
        String dataTableName = generateUniqueName();
        String indexTableName = generateUniqueName();
        String dataTableFullName = SchemaUtil.getTableName(schema, dataTableName);

        String dataTableDDL = "CREATE TABLE %s (ID INTEGER NOT NULL PRIMARY KEY, NAME VARCHAR, "
                + "ZIP INTEGER) COLUMN_ENCODED_BYTES=0";
        String indexTableDDL = "CREATE INDEX %s ON %s (NAME) INCLUDE (ZIP)";
        String upsertData = "UPSERT INTO %s VALUES (?, ?, ?)";
        IndexScrutinyMapperForTest.ScrutinyTestClock
                testClock = new IndexScrutinyMapperForTest.ScrutinyTestClock(0);

        try (Connection conn =
                DriverManager.getConnection(getUrl(), PropertiesUtil.deepCopy(TEST_PROPERTIES))) {
            conn.setAutoCommit(true);
            conn.createStatement().execute(String.format(dataTableDDL, dataTableFullName));
            conn.createStatement().execute(String.format(indexTableDDL, indexTableName,
                    dataTableFullName));
            // insert two rows
            PreparedStatement
                    upsertDataStmt = conn.prepareStatement(String.format(upsertData,
                    dataTableFullName));

            EnvironmentEdgeManager.injectEdge(testClock);
            NonParameterizedIndexScrutinyToolIT.upsertRow(upsertDataStmt, 1, "name-1", 98051);
            NonParameterizedIndexScrutinyToolIT.upsertRow(upsertDataStmt, 2, "name-2", 98052);

            List<Job> completedJobs = runScrutiny(schema, dataTableName, indexTableName);
            Job job = completedJobs.get(0);
            assertTrue(job.isSuccessful());
            assertCounters(job.getCounters());
        }
    }

    @Test
    public void testScrutinyOnRowsBeyondMaxLookBack_viewIndex() throws Exception {
        String schemaName = "S"+generateUniqueName();
        String dataTableName = "T"+generateUniqueName();
        String dataTableFullName = SchemaUtil.getTableName(schemaName,dataTableName);
        String viewIndexName = "VI"+generateUniqueName();
        String viewName = "V"+generateUniqueName();
        String viewFullName = SchemaUtil.getTableName(schemaName,viewName);
        String dataTableDDL = "CREATE TABLE %s (ID INTEGER NOT NULL PRIMARY KEY, NAME VARCHAR, "
                + "ZIP INTEGER) ";
        String viewDDL = "CREATE VIEW %s AS SELECT * FROM %s";
        String indexTableDDL = "CREATE INDEX %s ON %s (NAME) INCLUDE (ZIP)";
        String upsertData = "UPSERT INTO %s VALUES (?, ?, ?)";
        IndexScrutinyMapperForTest.ScrutinyTestClock
                testClock = new IndexScrutinyMapperForTest.ScrutinyTestClock(0);

        try (Connection conn =
                DriverManager.getConnection(getUrl(), PropertiesUtil.deepCopy(TEST_PROPERTIES))) {
            conn.createStatement().execute(String.format(dataTableDDL, dataTableFullName));
            conn.createStatement().execute(String.format(viewDDL, viewFullName, dataTableFullName));
            conn.createStatement().execute(String.format(indexTableDDL, viewIndexName,
                    viewFullName));
            // insert two rows
            PreparedStatement
                    upsertDataStmt = conn.prepareStatement(String.format(upsertData,
                    viewFullName));

            EnvironmentEdgeManager.injectEdge(testClock);
            NonParameterizedIndexScrutinyToolIT.upsertRow(upsertDataStmt, 1, "name-1", 98051);
            NonParameterizedIndexScrutinyToolIT.upsertRow(upsertDataStmt, 2, "name-2", 98052);
            conn.commit();

            List<Job> completedJobs = runScrutiny(schemaName, viewName, viewIndexName);
            Job job = completedJobs.get(0);
            assertTrue(job.isSuccessful());
            assertCounters(job.getCounters());
        }
    }

    private void assertCounters(Counters counters) {
        assertEquals(1, getCounterValue(counters, BEYOND_MAX_LOOKBACK));
        assertEquals(1, getCounterValue(counters, VALID_ROW_COUNT));
        assertEquals(0, getCounterValue(counters, INVALID_ROW_COUNT));
    }

    private List<Job> runScrutiny(String schemaName, String dataTableName, String indexTableName)
            throws Exception {
        return runScrutiny(schemaName, dataTableName, indexTableName, null, null);
    }

    private List<Job> runScrutiny(String schemaName, String dataTableName, String indexTableName,
            Long batchSize, IndexScrutinyTool.SourceTable sourceTable) throws Exception {
        final String[]
                cmdArgs =
                getArgValues(schemaName, dataTableName, indexTableName, batchSize, sourceTable,
                        false, null, null, null, Long.MAX_VALUE);
        return runScrutiny(cmdArgs);
    }
}
