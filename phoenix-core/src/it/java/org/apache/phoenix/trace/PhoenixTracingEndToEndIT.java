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
package org.apache.phoenix.trace;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Collection;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.phoenix.coprocessor.BaseScannerRegionObserver;
import org.apache.phoenix.end2end.HBaseManagedTimeTest;
import org.apache.phoenix.metrics.Metrics;
import org.apache.phoenix.metrics.TracingTestCompat;
import org.apache.phoenix.query.QueryServicesOptions;
import org.apache.phoenix.trace.Hadoop1TracingTestEnabler.Hadoop1Disabled;
import org.apache.phoenix.trace.TraceReader.SpanInfo;
import org.apache.phoenix.trace.TraceReader.TraceHolder;
import org.cloudera.htrace.Sampler;
import org.cloudera.htrace.Span;
import org.cloudera.htrace.SpanReceiver;
import org.cloudera.htrace.Trace;
import org.cloudera.htrace.TraceScope;
import org.junit.After;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

/**
 * Test that the logging sink stores the expected metrics/stats
 */
@RunWith(Hadoop1TracingTestEnabler.class)
@Hadoop1Disabled("tracing")
@Category(HBaseManagedTimeTest.class)
public class PhoenixTracingEndToEndIT extends BaseTracingTestIT {

    private static final Log LOG = LogFactory.getLog(PhoenixTracingEndToEndIT.class);
    private static final int MAX_RETRIES = 10;
    private final String table = "ENABLED_FOR_LOGGING";
    private final String index = "ENABALED_FOR_LOGGING_INDEX";

    private static DisableableMetricsWriter sink;

    @BeforeClass
    public static void setupMetrics() throws Exception {
        if (shouldEarlyExitForHadoop1Test()) {
            return;
        }
        PhoenixTableMetricsWriter pWriter = new PhoenixTableMetricsWriter();
        Connection conn = getConnectionWithoutTracing();
        pWriter.initForTesting(conn);
        sink = new DisableableMetricsWriter(pWriter);

        TracingTestCompat.registerSink(sink);
    }

    @After
    public void cleanup() {
        sink.disable();
        sink.clear();
        sink.enable();

        // LISTENABLE.clearListeners();
    }

    private static void waitForCommit(CountDownLatch latch) throws SQLException {
        Connection conn = new CountDownConnection(getConnectionWithoutTracing(), latch);
        replaceWriterConnection(conn);
    }

    private static void replaceWriterConnection(Connection conn) throws SQLException {
        // disable the writer
        sink.disable();

        // swap the connection for one that listens
        sink.getDelegate().initForTesting(conn);

        // enable the writer
        sink.enable();
    }

    /**
     * Simple test that we can correctly write spans to the phoenix table
     * @throws Exception on failure
     */
    @Test
    public void testWriteSpans() throws Exception {
        // get a receiver for the spans
        SpanReceiver receiver = TracingCompat.newTraceMetricSource();
        // which also needs to a source for the metrics system
        Metrics.getManager().registerSource("testWriteSpans-source", "source for testWriteSpans",
            receiver);

        // watch our sink so we know when commits happen
        CountDownLatch latch = new CountDownLatch(1);
        waitForCommit(latch);

        // write some spans
        TraceScope trace = Trace.startSpan("Start write test", Sampler.ALWAYS);
        Span span = trace.getSpan();

        // add a child with some annotations
        Span child = span.child("child 1");
        child.addTimelineAnnotation("timeline annotation");
        TracingCompat.addAnnotation(child, "test annotation", 10);
        child.stop();

        // sleep a little bit to get some time difference
        Thread.sleep(100);

        trace.close();

        // pass the trace on
        receiver.receiveSpan(span);

        // wait for the tracer to actually do the write
        latch.await();

        // look for the writes to make sure they were made
        Connection conn = getConnectionWithoutTracing();
        checkStoredTraces(conn, new TraceChecker() {
            @Override
            public boolean foundTrace(TraceHolder trace, SpanInfo info) {
                if (info.description.equals("child 1")) {
                    assertEquals("Not all annotations present", 1, info.annotationCount);
                    assertEquals("Not all tags present", 1, info.tagCount);
                    boolean found = false;
                    for (String annotation : info.annotations) {
                        if (annotation.startsWith("test annotation")) {
                            found = true;
                        }
                    }
                    assertTrue("Missing the annotations in span: " + info, found);
                    found = false;
                    for (String tag : info.tags) {
                        if (tag.endsWith("timeline annotation")) {
                            found = true;
                        }
                    }
                    assertTrue("Missing the tags in span: " + info, found);
                    return true;
                }
                return false;
            }
        });
    }

    /**
     * Test that span will actually go into the this sink and be written on both side of the wire,
     * through the indexing code.
     * @throws Exception
     */
    @Test
    public void testClientServerIndexingTracing() throws Exception {
        // one call for client side, one call for server side
        final CountDownLatch updated = new CountDownLatch(2);
        waitForCommit(updated);

        // separate connection so we don't create extra traces
        Connection conn = getConnectionWithoutTracing();
        createTestTable(conn, true);

        // trace the requests we send
        Connection traceable = getTracingConnection();
        LOG.debug("Doing dummy the writes to the tracked table");
        String insert = "UPSERT INTO " + table + " VALUES (?, ?)";
        PreparedStatement stmt = traceable.prepareStatement(insert);
        stmt.setString(1, "key1");
        stmt.setLong(2, 1);
        // this first trace just does a simple open/close of the span. Its not doing anything
        // terribly interesting because we aren't auto-committing on the connection, so it just
        // updates the mutation state and returns.
        stmt.execute();
        stmt.setString(1, "key2");
        stmt.setLong(2, 2);
        stmt.execute();
        traceable.commit();

        // wait for the latch to countdown, as the metrics system is time-based
        LOG.debug("Waiting for latch to complete!");
        updated.await(200, TimeUnit.SECONDS);// should be way more than GC pauses

        // read the traces back out

        /* Expected:
         * 1. Single element trace - for first PreparedStatement#execute span
         * 2. Two element trace for second PreparedStatement#execute span
         *  a. execute call
         *  b. metadata lookup*
         * 3. Commit trace.
         *  a. Committing to tables
         *    i. Committing to single table
         *    ii. hbase batch write*
         *    i.I. span on server
         *    i.II. building index updates
         *    i.III. waiting for latch
         * where '*' is a generically named thread (e.g phoenix-1-thread-X)
         */
        boolean indexingCompleted = checkStoredTraces(conn, new TraceChecker() {
            @Override
            public boolean foundTrace(TraceHolder trace, SpanInfo span) {
                String traceInfo = trace.toString();
                // skip logging traces that are just traces about tracing
                if (traceInfo.contains(QueryServicesOptions.DEFAULT_TRACING_STATS_TABLE_NAME)) {
                    return false;
                }
                if (traceInfo.contains("Completing index")) {
                    return true;
                }
                return false;
            }
        });

        assertTrue("Never found indexing updates", indexingCompleted);
    }

    private void createTestTable(Connection conn, boolean withIndex) throws SQLException {
        // create a dummy table
        String ddl =
                "create table if not exists " + table + "(" + "k varchar not null, " + "c1 bigint"
                        + " CONSTRAINT pk PRIMARY KEY (k))";
        conn.createStatement().execute(ddl);

        // early exit if we don't need to create an index
        if (!withIndex) {
            return;
        }
        // create an index on the table - we know indexing has some basic tracing
        ddl = "CREATE INDEX IF NOT EXISTS " + index + " on " + table + " (c1)";
        conn.createStatement().execute(ddl);
        conn.commit();
    }

    @Test
    public void testScanTracing() throws Exception {
        // separate connections to minimize amount of traces that are generated
        Connection traceable = getTracingConnection();
        Connection conn = getConnectionWithoutTracing();

        // one call for client side, one call for server side
        CountDownLatch updated = new CountDownLatch(2);
        waitForCommit(updated);

        // create a dummy table
        createTestTable(conn, false);

        // update the table, but don't trace these, to simplify the traces we read
        LOG.debug("Doing dummy the writes to the tracked table");
        String insert = "UPSERT INTO " + table + " VALUES (?, ?)";
        PreparedStatement stmt = conn.prepareStatement(insert);
        stmt.setString(1, "key1");
        stmt.setLong(2, 1);
        stmt.execute();
        conn.commit();
        conn.rollback();

        // setup for next set of updates
        stmt.setString(1, "key2");
        stmt.setLong(2, 2);
        stmt.execute();
        conn.commit();
        conn.rollback();

        // do a scan of the table
        String read = "SELECT * FROM " + table;
        ResultSet results = traceable.createStatement().executeQuery(read);
        assertTrue("Didn't get first result", results.next());
        assertTrue("Didn't get second result", results.next());
        results.close();

        assertTrue("Get expected updates to trace table", updated.await(200, TimeUnit.SECONDS));
        // don't trace reads either
        boolean tracingComplete = checkStoredTraces(conn, new TraceChecker(){

            @Override
            public boolean foundTrace(TraceHolder currentTrace) {
                String traceInfo = currentTrace.toString();
                return traceInfo.contains("Parallel scanner");
            }
        });
        assertTrue("Didn't find the parallel scanner in the tracing", tracingComplete);
    }

    @Test
    public void testScanTracingOnServer() throws Exception {
        // separate connections to minimize amount of traces that are generated
        Connection traceable = getTracingConnection();
        Connection conn = getConnectionWithoutTracing();

        // one call for client side, one call for server side
        CountDownLatch updated = new CountDownLatch(2);
        waitForCommit(updated);

        // create a dummy table
        createTestTable(conn, false);

        // update the table, but don't trace these, to simplify the traces we read
        LOG.debug("Doing dummy the writes to the tracked table");
        String insert = "UPSERT INTO " + table + " VALUES (?, ?)";
        PreparedStatement stmt = conn.prepareStatement(insert);
        stmt.setString(1, "key1");
        stmt.setLong(2, 1);
        stmt.execute();
        conn.commit();
        conn.rollback();

        // setup for next set of updates
        stmt.setString(1, "key2");
        stmt.setLong(2, 2);
        stmt.execute();
        conn.commit();
        conn.rollback();

        // do a scan of the table
        String read = "SELECT COUNT(*) FROM " + table;
        ResultSet results = traceable.createStatement().executeQuery(read);
        assertTrue("Didn't get count result", results.next());
        // make sure we got the expected count
        assertEquals("Didn't get the expected number of row", 2, results.getInt(1));
        results.close();

        assertTrue("Get expected updates to trace table", updated.await(200, TimeUnit.SECONDS));
        // don't trace reads either
        boolean found = checkStoredTraces(conn, new TraceChecker() {
            @Override
            public boolean foundTrace(TraceHolder trace) {
                String traceInfo = trace.toString();
                return traceInfo.contains(BaseScannerRegionObserver.SCANNER_OPENED_TRACE_INFO);
            }
        });
        assertTrue("Didn't find the parallel scanner in the tracing", found);
    }

    private boolean checkStoredTraces(Connection conn, TraceChecker checker) throws Exception {
        TraceReader reader = new TraceReader(conn);
        int retries = 0;
        boolean found = false;
        outer: while (retries < MAX_RETRIES) {
            Collection<TraceHolder> traces = reader.readAll(100);
            for (TraceHolder trace : traces) {
                LOG.info("Got trace: " + trace);
                found = checker.foundTrace(trace);
                if (found) {
                    break outer;
                }
                for (SpanInfo span : trace.spans) {
                    found = checker.foundTrace(trace, span);
                    if (found) {
                        break outer;
                    }
                }
            }
            LOG.info("======  Waiting for tracing updates to be propagated ========");
            Thread.sleep(1000);
            retries++;
        }
        return found;
    }

    private abstract class TraceChecker {
        public boolean foundTrace(TraceHolder currentTrace) {
            return false;
        }

        public boolean foundTrace(TraceHolder currentTrace, SpanInfo currentSpan) {
            return false;
        }
    }

    private static class CountDownConnection extends DelegatingConnection {
        private CountDownLatch commit;

        @SuppressWarnings("unchecked")
        public CountDownConnection(Connection conn, CountDownLatch commit) {
            super(conn);
            this.commit = commit;
        }

        @Override
        public void commit() throws SQLException {
            commit.countDown();
            super.commit();
        }

    }
}