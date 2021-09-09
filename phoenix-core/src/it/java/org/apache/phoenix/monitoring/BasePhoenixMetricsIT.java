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
package org.apache.phoenix.monitoring;

import org.apache.phoenix.thirdparty.com.google.common.collect.Lists;
import org.apache.phoenix.thirdparty.com.google.common.collect.Maps;
import org.apache.phoenix.jdbc.PhoenixDriver;
import org.apache.phoenix.query.BaseTest;
import org.apache.phoenix.query.QueryServices;
import org.apache.phoenix.util.ReadOnlyProps;
import org.junit.BeforeClass;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

import static org.apache.phoenix.monitoring.MetricType.SCAN_BYTES;
import static org.apache.phoenix.monitoring.MetricType.TASK_EXECUTED_COUNTER;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public abstract class BasePhoenixMetricsIT extends BaseTest {

    static final int MAX_RETRIES = 5;

    static final List<MetricType> mutationMetricsToSkip =
    Lists.newArrayList(MetricType.MUTATION_COMMIT_TIME);
    static final List<MetricType> readMetricsToSkip =
    Lists.newArrayList(MetricType.TASK_QUEUE_WAIT_TIME,
            MetricType.TASK_EXECUTION_TIME, MetricType.TASK_END_TO_END_TIME,
            MetricType.COUNT_MILLS_BETWEEN_NEXTS);
    static final String CUSTOM_URL_STRING = "SESSION";
    static final AtomicInteger numConnections = new AtomicInteger(0);

    @BeforeClass
    public static synchronized void doSetup() throws Exception {
        Map<String, String> props = Maps.newHashMapWithExpectedSize(3);
        // Disable system task handling
        props.put(QueryServices.TASK_HANDLING_INITIAL_DELAY_MS_ATTRIB, Long.toString(Long.MAX_VALUE));
        // Phoenix Global client metrics are enabled by default
        // Enable request metric collection at the driver level
        props.put(QueryServices.COLLECT_REQUEST_LEVEL_METRICS, String.valueOf(true));
        // disable renewing leases as this will force spooling to happen.
        props.put(QueryServices.RENEW_LEASE_ENABLED, String.valueOf(false));
        setUpTestDriver(new ReadOnlyProps(props.entrySet().iterator()));
        // need the non-test driver for some tests that check number of hconnections, etc.
        DriverManager.registerDriver(PhoenixDriver.INSTANCE);

    }

    Connection insertRowsInTable(String tableName, long numRows) throws SQLException {
        String dml = "UPSERT INTO " + tableName + " VALUES (?, ?)";
        Connection conn = DriverManager.getConnection(getUrl());
        PreparedStatement stmt = conn.prepareStatement(dml);
        for (int i = 1; i <= numRows; i++) {
            stmt.setString(1, "key" + i);
            stmt.setString(2, "value" + i);
            stmt.executeUpdate();
        }
        conn.commit();
        return conn;
    }

    void assertReadMetricsForMutatingSql(String tableName, long tableSaltBuckets,
                                                 Map<String, Map<MetricType, Long>> readMetrics) {
        assertTrue("No read metrics present when there should have been!", readMetrics.size() > 0);
        int numTables = 0;
        for (Map.Entry<String, Map<MetricType, Long>> entry : readMetrics.entrySet()) {
            String t = entry.getKey();
            assertEquals("Table name didn't match for read metrics", tableName, t);
            numTables++;
            Map<MetricType, Long> p = entry.getValue();
            assertTrue("No read metrics present when there should have been", p.size() > 0);
            for (Map.Entry<MetricType, Long> metric : p.entrySet()) {
                MetricType metricType = metric.getKey();
                long metricValue = metric.getValue();
                if (metricType.equals(TASK_EXECUTED_COUNTER)) {
                    assertEquals(tableSaltBuckets, metricValue);
                } else if (metricType.equals(SCAN_BYTES)) {
                    assertTrue("Scan bytes read should be greater than zero", metricValue > 0);
                }
            }
        }
        assertEquals("There should have been read metrics only for one table: " + tableName, 1, numTables);
    }

    void assertMutationMetrics(String tableName, int numRows, Map<String, Map<MetricType, Long>> mutationMetrics) {
        assertTrue("No mutation metrics present when there should have been", mutationMetrics.size() > 0);
        for (Map.Entry<String, Map<MetricType, Long>> entry : mutationMetrics.entrySet()) {
            String t = entry.getKey();
            assertEquals("Table name didn't match for mutation metrics", tableName, t);
            Map<MetricType, Long> p = entry.getValue();
            assertEquals("There should have been five metrics", 5, p.size());
            for (Map.Entry<MetricType, Long> metric : p.entrySet()) {
                MetricType metricType = metric.getKey();
                long metricValue = metric.getValue();
                if (metricType.equals(MetricType.MUTATION_BATCH_SIZE)) {
                    assertEquals("Mutation batch sizes didn't match!", numRows, metricValue);
                } else if (metricType.equals(MetricType.MUTATION_COMMIT_TIME)) {
                    assertTrue("Mutation commit time should be greater than zero", metricValue > 0);
                } else if (metricType.equals(MetricType.MUTATION_BYTES)) {
                    assertTrue("Mutation bytes size should be greater than zero", metricValue > 0);
                } else if (metricType.equals(MetricType.MUTATION_BATCH_FAILED_SIZE)) {
                    assertEquals("Zero failed mutations expected", 0, metricValue);
                } else if (metricType.equals(MetricType.INDEX_COMMIT_FAILURE_SIZE)) {
                    assertEquals("Zero failed phase 3 mutations expected", 0, metricValue);
                }
            }
        }
    }


}
