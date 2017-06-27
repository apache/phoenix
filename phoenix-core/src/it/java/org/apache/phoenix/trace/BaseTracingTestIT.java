/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.phoenix.trace;

import static org.apache.phoenix.util.PhoenixRuntime.ANNOTATION_ATTRIB_PREFIX;
import static org.apache.phoenix.util.TestUtil.TEST_PROPERTIES;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.Collections;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.htrace.Span;
import org.apache.htrace.Trace;
import org.apache.htrace.impl.MilliSpan;
import org.apache.phoenix.end2end.ParallelStatsDisabledIT;
import org.apache.phoenix.trace.util.Tracing;
import org.apache.phoenix.trace.util.Tracing.Frequency;
import org.apache.phoenix.util.PhoenixRuntime;
import org.apache.phoenix.util.PropertiesUtil;
import org.junit.After;
import org.junit.Before;

/**
 * Base test for tracing tests - helps manage getting tracing/non-tracing connections, as well as
 * any supporting utils.
 */

public class BaseTracingTestIT extends ParallelStatsDisabledIT {

    private static final Log LOG = LogFactory.getLog(BaseTracingTestIT.class);

    protected CountDownLatch latch;
    protected int defaultTracingThreadPoolForTest = 1;
    protected int defaultTracingBatchSizeForTest = 1;
    protected String tracingTableName;
    protected TraceSpanReceiver traceSpanReceiver = null;
    protected TestTraceWriter testTraceWriter = null;

    @Before
    public void setup() {
        tracingTableName = "TRACING_" + generateUniqueName();
        traceSpanReceiver = new TraceSpanReceiver();
        Trace.addReceiver(traceSpanReceiver);
        testTraceWriter =
                new TestTraceWriter(tracingTableName, defaultTracingThreadPoolForTest,
                        defaultTracingBatchSizeForTest);
    }

    @After
    public void cleanUp() {
        Trace.removeReceiver(traceSpanReceiver);
        if (testTraceWriter != null) testTraceWriter.stop();
    }

    public static Connection getConnectionWithoutTracing() throws SQLException {
        Properties props = PropertiesUtil.deepCopy(TEST_PROPERTIES);
        return getConnectionWithoutTracing(props);
    }

    public static Connection getConnectionWithoutTracing(Properties props) throws SQLException {
        Connection conn = getConnectionWithTracingFrequency(props, Frequency.NEVER);
        return conn;
    }

    public static Connection getTracingConnection() throws Exception {
        return getTracingConnection(Collections.<String, String> emptyMap(), null);
    }

    public static Connection getTracingConnection(Map<String, String> customAnnotations,
            String tenantId) throws Exception {
        Properties props = PropertiesUtil.deepCopy(TEST_PROPERTIES);
        for (Map.Entry<String, String> annot : customAnnotations.entrySet()) {
            props.put(ANNOTATION_ATTRIB_PREFIX + annot.getKey(), annot.getValue());
        }
        if (tenantId != null) {
            props.put(PhoenixRuntime.TENANT_ID_ATTRIB, tenantId);
        }
        return getConnectionWithTracingFrequency(props, Tracing.Frequency.ALWAYS);
    }

    public static Connection getConnectionWithTracingFrequency(Properties props,
            Tracing.Frequency frequency) throws SQLException {
        Tracing.setSampling(props, frequency);
        return DriverManager.getConnection(getUrl(), props);
    }

    protected Span createNewSpan(long traceid, long parentid, long spanid, String description,
            long startTime, long endTime, String processid, String... tags) {

        Span span =
                new MilliSpan.Builder().description(description).traceId(traceid)
                        .parents(new long[] { parentid }).spanId(spanid).processId(processid)
                        .begin(startTime).end(endTime).build();

        int tagCount = 0;
        for (String annotation : tags) {
            span.addKVAnnotation((Integer.toString(tagCount++)).getBytes(), annotation.getBytes());
        }
        return span;
    }

    private static class CountDownConnection extends DelegateConnection {
        private CountDownLatch commit;

        public CountDownConnection(Connection conn, CountDownLatch commit) {
            super(conn);
            this.commit = commit;
        }

        @Override
        public void commit() throws SQLException {
            super.commit();
            commit.countDown();
        }

    }

    protected class TestTraceWriter extends TraceWriter {

        public TestTraceWriter(String tableName, int numThreads, int batchSize) {
            super(tableName, numThreads, batchSize);
        }

        @Override
        protected Connection getConnection(String tableName) {
            try {
                Connection connection =
                        new CountDownConnection(getConnectionWithoutTracing(), latch);
                if (!traceTableExists(connection, tableName)) {
                    createTable(connection, tableName);
                }
                return connection;
            } catch (SQLException e) {
                LOG.error("New connection failed for tracing Table: " + tableName, e);
                return null;
            }
        }

        @Override
        protected TraceSpanReceiver getTraceSpanReceiver() {
            return traceSpanReceiver;
        }

        public void stop() {
            if (executor == null) return;
            try {
                executor.shutdownNow();
                executor.awaitTermination(5, TimeUnit.SECONDS);
            } catch (InterruptedException e) {
                LOG.error("Failed to stop the thread. ", e);
            }
        }

    }

}
