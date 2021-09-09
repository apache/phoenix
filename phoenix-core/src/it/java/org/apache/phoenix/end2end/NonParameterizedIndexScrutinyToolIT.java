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

import org.apache.hadoop.mapreduce.Counters;
import org.apache.hadoop.mapreduce.Job;
import org.apache.phoenix.mapreduce.index.IndexScrutinyMapperForTest;
import org.apache.phoenix.mapreduce.index.IndexScrutinyTool;
import org.apache.phoenix.util.EnvironmentEdgeManager;
import org.apache.phoenix.util.PropertiesUtil;
import org.apache.phoenix.util.SchemaUtil;
import org.junit.Test;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.List;

import static org.apache.phoenix.mapreduce.index.IndexScrutinyMapperForTest.TEST_TABLE_TTL;
import static org.apache.phoenix.mapreduce.index.PhoenixScrutinyJobCounters.EXPIRED_ROW_COUNT;
import static org.apache.phoenix.mapreduce.index.PhoenixScrutinyJobCounters.INVALID_ROW_COUNT;
import static org.apache.phoenix.mapreduce.index.PhoenixScrutinyJobCounters.VALID_ROW_COUNT;
import static org.apache.phoenix.util.TestUtil.TEST_PROPERTIES;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

//FIXME this test is never run by maven, as it has no @Category
public class NonParameterizedIndexScrutinyToolIT extends IndexScrutinyToolBaseIT {

    @Test
    public void testScrutinyOnArrayTypes() throws Exception {
        String dataTableName = generateUniqueName();
        String indexTableName = generateUniqueName();
        String dataTableDDL = "CREATE TABLE %s (ID INTEGER NOT NULL PRIMARY KEY, NAME VARCHAR, "
                + "VB VARBINARY)";
        String indexTableDDL = "CREATE INDEX %s ON %s (NAME) INCLUDE (VB)";
        String upsertData = "UPSERT INTO %s VALUES (?, ?, ?)";
        String upsertIndex = "UPSERT INTO %s (\"0:NAME\", \":ID\", \"0:VB\") values (?,?,?)";

        try (Connection conn =
                DriverManager.getConnection(getUrl(), PropertiesUtil.deepCopy(TEST_PROPERTIES))) {
            conn.createStatement().execute(String.format(dataTableDDL, dataTableName));
            conn.createStatement().execute(String.format(indexTableDDL, indexTableName,
                    dataTableName));
            // insert two rows
            PreparedStatement upsertDataStmt = conn.prepareStatement(String.format(upsertData,
                    dataTableName));
            upsertRow(upsertDataStmt, 1, "name-1", new byte[] {127, 0, 0, 1});
            upsertRow(upsertDataStmt, 2, "name-2", new byte[] {127, 1, 0, 5});
            conn.commit();

            List<Job> completedJobs = runScrutiny(null, dataTableName, indexTableName);
            Job job = completedJobs.get(0);
            assertTrue(job.isSuccessful());
            Counters counters = job.getCounters();
            assertEquals(2, getCounterValue(counters, VALID_ROW_COUNT));
            assertEquals(0, getCounterValue(counters, INVALID_ROW_COUNT));

            // Now insert a different varbinary row
            upsertRow(upsertDataStmt, 3, "name-3", new byte[] {1, 1, 1, 1});
            conn.commit();

            PreparedStatement upsertIndexStmt = conn.prepareStatement(String.format(upsertIndex,
                    indexTableName));
            upsertIndexStmt.setString(1, "name-3");
            upsertIndexStmt.setInt(2, 3);
            upsertIndexStmt.setBytes(3, new byte[] {0, 0, 0, 1});
            upsertIndexStmt.executeUpdate();
            conn.commit();

            completedJobs = runScrutiny(null, dataTableName, indexTableName);
            job = completedJobs.get(0);
            assertTrue(job.isSuccessful());
            counters = job.getCounters();
            assertEquals(2, getCounterValue(counters, VALID_ROW_COUNT));
            assertEquals(1, getCounterValue(counters, INVALID_ROW_COUNT));

            // Have source null
            upsertRow(upsertDataStmt, 4, "name-4", null);
            conn.commit();

            upsertIndexStmt.setString(1, "name-4");
            upsertIndexStmt.setInt(2, 4);
            upsertIndexStmt.setBytes(3, new byte[] {0, 0, 1, 1});
            upsertIndexStmt.executeUpdate();
            conn.commit();

            completedJobs = runScrutiny(null, dataTableName, indexTableName);
            job = completedJobs.get(0);
            assertTrue(job.isSuccessful());
            counters = job.getCounters();
            assertEquals(2, getCounterValue(counters, VALID_ROW_COUNT));
            assertEquals(2, getCounterValue(counters, INVALID_ROW_COUNT));

            // Have target null
            upsertRow(upsertDataStmt, 5, "name-5", new byte[] {0, 1, 1, 1});
            conn.commit();

            upsertIndexStmt.setString(1, "name-5");
            upsertIndexStmt.setInt(2, 5);
            upsertIndexStmt.setBytes(3, null);
            upsertIndexStmt.executeUpdate();
            conn.commit();

            completedJobs = runScrutiny(null, dataTableName, indexTableName);
            job = completedJobs.get(0);
            assertTrue(job.isSuccessful());
            counters = job.getCounters();
            assertEquals(2, getCounterValue(counters, VALID_ROW_COUNT));
            assertEquals(3, getCounterValue(counters, INVALID_ROW_COUNT));

            // Have both of them null
            upsertRow(upsertDataStmt, 6, "name-6", null);
            conn.commit();

            upsertIndexStmt.setString(1, "name-6");
            upsertIndexStmt.setInt(2, 6);
            upsertIndexStmt.setBytes(3, null);
            upsertIndexStmt.executeUpdate();
            conn.commit();

            completedJobs = runScrutiny(null, dataTableName, indexTableName);
            job = completedJobs.get(0);
            assertTrue(job.isSuccessful());
            counters = job.getCounters();
            assertEquals(3, getCounterValue(counters, VALID_ROW_COUNT));
            assertEquals(3, getCounterValue(counters, INVALID_ROW_COUNT));
        }
    }

    @Test
    public void testScrutinyOnRowsNearExpiry() throws Exception {
        String schema = generateUniqueName();
        String dataTableName = generateUniqueName();
        String indexTableName = generateUniqueName();
        String dataTableFullName = SchemaUtil.getTableName(schema, dataTableName);
        String dataTableDDL = "CREATE TABLE %s (ID INTEGER NOT NULL PRIMARY KEY, NAME VARCHAR, "
                + "ZIP INTEGER) TTL="+TEST_TABLE_TTL;
        String indexTableDDL = "CREATE INDEX %s ON %s (NAME) INCLUDE (ZIP)";
        String upsertData = "UPSERT INTO %s VALUES (?, ?, ?)";
        IndexScrutinyMapperForTest.ScrutinyTestClock
                testClock = new IndexScrutinyMapperForTest.ScrutinyTestClock(0);

        try (Connection conn =
                DriverManager.getConnection(getUrl(), PropertiesUtil.deepCopy(TEST_PROPERTIES))) {
            conn.createStatement().execute(String.format(dataTableDDL, dataTableFullName));
            conn.createStatement().execute(String.format(indexTableDDL, indexTableName,
                    dataTableFullName));
            // insert two rows
            PreparedStatement
                    upsertDataStmt = conn.prepareStatement(String.format(upsertData,
                    dataTableFullName));

            EnvironmentEdgeManager.injectEdge(testClock);
            upsertRow(upsertDataStmt, 1, "name-1", 98051);
            upsertRow(upsertDataStmt, 2, "name-2", 98052);
            conn.commit();

            List<Job> completedJobs = runScrutiny(schema, dataTableName, indexTableName);
            Job job = completedJobs.get(0);
            assertTrue(job.isSuccessful());
            Counters counters = job.getCounters();
            assertEquals(2, getCounterValue(counters, EXPIRED_ROW_COUNT));
            assertEquals(0, getCounterValue(counters, VALID_ROW_COUNT));
            assertEquals(0, getCounterValue(counters, INVALID_ROW_COUNT));
        } finally {
            EnvironmentEdgeManager.reset();
        }
    }

    @Test
    public void testScrutinyOnRowsNearExpiry_viewIndex() throws Exception {
        String schemaName = "S"+generateUniqueName();
        String dataTableName = "T"+generateUniqueName();
        String dataTableFullName = SchemaUtil.getTableName(schemaName,dataTableName);
        String viewIndexName = "VI"+generateUniqueName();
        String viewName = "V"+generateUniqueName();
        String viewFullName = SchemaUtil.getTableName(schemaName,viewName);
        String dataTableDDL = "CREATE TABLE %s (ID INTEGER NOT NULL PRIMARY KEY, NAME VARCHAR, "
                + "ZIP INTEGER) TTL="+TEST_TABLE_TTL;
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
            upsertRow(upsertDataStmt, 1, "name-1", 98051);
            upsertRow(upsertDataStmt, 2, "name-2", 98052);
            conn.commit();

            List<Job> completedJobs = runScrutiny(schemaName, viewName, viewIndexName);
            Job job = completedJobs.get(0);
            assertTrue(job.isSuccessful());
            Counters counters = job.getCounters();
            assertEquals(2, getCounterValue(counters, EXPIRED_ROW_COUNT));
            assertEquals(0, getCounterValue(counters, VALID_ROW_COUNT));
            assertEquals(0, getCounterValue(counters, INVALID_ROW_COUNT));
        } finally {
            EnvironmentEdgeManager.reset();
        }
    }

    public static void upsertRow(PreparedStatement stmt, int id, String name, byte[] val) throws
            SQLException {
        int index = 1;
        // insert row
        stmt.setInt(index++, id);
        stmt.setString(index++, name);
        stmt.setBytes(index++, val);
        stmt.executeUpdate();
    }

    public static void upsertRow(PreparedStatement stmt, int id, String name, int val)
            throws SQLException {
        int index = 1;
        // insert row
        stmt.setInt(index++, id);
        stmt.setString(index++, name);
        stmt.setInt(index++, val);
        stmt.executeUpdate();
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
        return runScrutiny(IndexScrutinyMapperForTest.class, cmdArgs);
    }
}
