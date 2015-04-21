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

import static org.apache.phoenix.monitoring.PhoenixMetrics.CountMetric.FAILED_QUERY;
import static org.apache.phoenix.monitoring.PhoenixMetrics.CountMetric.MUTATION_COUNT;
import static org.apache.phoenix.monitoring.PhoenixMetrics.CountMetric.NUM_SPOOL_FILE;
import static org.apache.phoenix.monitoring.PhoenixMetrics.CountMetric.QUERY_COUNT;
import static org.apache.phoenix.monitoring.PhoenixMetrics.CountMetric.QUERY_TIMEOUT;
import static org.apache.phoenix.monitoring.PhoenixMetrics.CountMetric.REJECTED_TASK_COUNT;
import static org.apache.phoenix.monitoring.PhoenixMetrics.SizeMetric.MUTATION_BATCH_SIZE;
import static org.apache.phoenix.monitoring.PhoenixMetrics.SizeMetric.MUTATION_BYTES;
import static org.apache.phoenix.monitoring.PhoenixMetrics.SizeMetric.MUTATION_COMMIT_TIME;
import static org.apache.phoenix.monitoring.PhoenixMetrics.SizeMetric.PARALLEL_SCANS;
import static org.apache.phoenix.monitoring.PhoenixMetrics.SizeMetric.QUERY_TIME;
import static org.apache.phoenix.monitoring.PhoenixMetrics.SizeMetric.SCAN_BYTES;
import static org.apache.phoenix.monitoring.PhoenixMetrics.SizeMetric.TASK_END_TO_END_TIME;
import static org.apache.phoenix.monitoring.PhoenixMetrics.SizeMetric.TASK_EXECUTION_TIME;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;

import org.apache.phoenix.monitoring.Metric;
import org.apache.phoenix.util.PhoenixRuntime;
import org.junit.Test;

public class PhoenixMetricsIT extends BaseHBaseManagedTimeIT {
    
    @Test
    public void testResetPhoenixMetrics() {
        resetMetrics();
        for (Metric m : PhoenixRuntime.getInternalPhoenixMetrics()) {
            assertEquals(0, m.getTotalSum());
            assertEquals(0, m.getNumberOfSamples());
        }
    }
    
    @Test
    public void testPhoenixMetricsForQueries() throws Exception {
        createTableAndInsertValues("T", true);
        resetMetrics(); // we want to count metrics related only to the below query
        Connection conn = DriverManager.getConnection(getUrl());
        String query = "SELECT * FROM T";
        ResultSet rs = conn.createStatement().executeQuery(query);
        while (rs.next()) {
            rs.getString(1);
            rs.getString(2);
        }
        assertEquals(1, PARALLEL_SCANS.getMetric().getTotalSum());
        assertEquals(1, QUERY_COUNT.getMetric().getTotalSum());
        assertEquals(0, REJECTED_TASK_COUNT.getMetric().getTotalSum());
        assertEquals(0, QUERY_TIMEOUT.getMetric().getTotalSum());
        assertEquals(0, FAILED_QUERY.getMetric().getTotalSum());
        assertEquals(0, NUM_SPOOL_FILE.getMetric().getTotalSum());
        assertEquals(0, MUTATION_BATCH_SIZE.getMetric().getTotalSum());
        assertEquals(0, MUTATION_BYTES.getMetric().getTotalSum());
        assertEquals(0, MUTATION_COMMIT_TIME.getMetric().getTotalSum());
        
        assertTrue(SCAN_BYTES.getMetric().getTotalSum() > 0);
        assertTrue(QUERY_TIME.getMetric().getTotalSum() > 0);
        assertTrue(TASK_END_TO_END_TIME.getMetric().getTotalSum() > 0);
        assertTrue(TASK_EXECUTION_TIME.getMetric().getTotalSum() > 0);
    }
    
    @Test
    public void testPhoenixMetricsForMutations() throws Exception {
        createTableAndInsertValues("T", true);
        assertEquals(10, MUTATION_BATCH_SIZE.getMetric().getTotalSum());
        assertEquals(10, MUTATION_COUNT.getMetric().getTotalSum());
        assertTrue(MUTATION_BYTES.getMetric().getTotalSum() > 0);
        assertTrue(MUTATION_COMMIT_TIME.getMetric().getTotalSum() > 0);
        assertEquals(0, PARALLEL_SCANS.getMetric().getTotalSum());
        assertEquals(0, QUERY_COUNT.getMetric().getTotalSum());
        assertEquals(0, REJECTED_TASK_COUNT.getMetric().getTotalSum());
        assertEquals(0, QUERY_TIMEOUT.getMetric().getTotalSum());
        assertEquals(0, FAILED_QUERY.getMetric().getTotalSum());
        assertEquals(0, NUM_SPOOL_FILE.getMetric().getTotalSum());
    }
    
    
    @Test
    public void testPhoenixMetricsForUpsertSelect() throws Exception { 
        createTableAndInsertValues("T", true);
        resetMetrics();
        String ddl = "CREATE TABLE T2 (K VARCHAR NOT NULL PRIMARY KEY, V VARCHAR)";
        Connection conn = DriverManager.getConnection(getUrl());
        conn.createStatement().execute(ddl);
        resetMetrics();
        String dml = "UPSERT INTO T2 (K, V) SELECT K, V FROM T";
        conn.createStatement().executeUpdate(dml);
        conn.commit();
        assertEquals(10, MUTATION_BATCH_SIZE.getMetric().getTotalSum());
        assertEquals(1, MUTATION_COUNT.getMetric().getTotalSum());
        assertEquals(1, PARALLEL_SCANS.getMetric().getTotalSum());
        assertEquals(0, QUERY_TIME.getMetric().getTotalSum());
        assertTrue(SCAN_BYTES.getMetric().getTotalSum() > 0);
        assertTrue(MUTATION_BYTES.getMetric().getTotalSum() > 0);
        assertTrue(MUTATION_COMMIT_TIME.getMetric().getTotalSum() > 0);
        assertEquals(0, QUERY_COUNT.getMetric().getTotalSum());
        assertEquals(0, REJECTED_TASK_COUNT.getMetric().getTotalSum());
        assertEquals(0, QUERY_TIMEOUT.getMetric().getTotalSum());
        assertEquals(0, FAILED_QUERY.getMetric().getTotalSum());
        assertEquals(0, NUM_SPOOL_FILE.getMetric().getTotalSum());
    }
    
    private static void resetMetrics() {
        for (Metric m : PhoenixRuntime.getInternalPhoenixMetrics()) {
            m.reset();
        }
    }
    
    private static void createTableAndInsertValues(String tableName, boolean resetMetricsAfterTableCreate) throws Exception {
        String ddl = "CREATE TABLE " + tableName + " (K VARCHAR NOT NULL PRIMARY KEY, V VARCHAR)";
        Connection conn = DriverManager.getConnection(getUrl());
        conn.createStatement().execute(ddl);
        if (resetMetricsAfterTableCreate) {
            resetMetrics();
        }
        // executing 10 upserts/mutations.
        String dml = "UPSERT INTO " + tableName + " VALUES (?, ?)";
        PreparedStatement stmt = conn.prepareStatement(dml);
        for (int i = 1; i <= 10; i++) {
            stmt.setString(1, "key" + i);
            stmt.setString(2, "value" + i);
            stmt.executeUpdate();
        }
        conn.commit();
    }
    
    
    
}
