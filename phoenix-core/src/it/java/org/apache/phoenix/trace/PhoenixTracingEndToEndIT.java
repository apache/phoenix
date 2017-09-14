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

import static org.apache.phoenix.util.PhoenixRuntime.TENANT_ID_ATTRIB;
import static org.apache.phoenix.util.TestUtil.TEST_PROPERTIES;
import static org.junit.Assert.*;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.*;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.htrace.*;
import org.apache.htrace.impl.ProbabilitySampler;
import org.apache.phoenix.coprocessor.BaseScannerRegionObserver;
import org.apache.phoenix.jdbc.PhoenixConnection;
import org.apache.phoenix.trace.TraceReader.SpanInfo;
import org.apache.phoenix.trace.TraceReader.TraceHolder;
import org.junit.Before;
import org.junit.Test;

import com.google.common.collect.ImmutableMap;

/**
 * Test that the logging sink stores the expected metrics/stats
 */

public class PhoenixTracingEndToEndIT extends BaseTracingTestIT {

    private static final Log LOG = LogFactory.getLog(PhoenixTracingEndToEndIT.class);
    private static final int MAX_RETRIES = 10;
    private String enabledForLoggingTable;
    private String enableForLoggingIndex;

    @Before
    public void setupMetrics() throws Exception {
        enabledForLoggingTable = "ENABLED_FOR_LOGGING_" + generateUniqueName();
        enableForLoggingIndex = "ENABALED_FOR_LOGGING_INDEX_" + generateUniqueName();
    }

    /**
     * Simple test that we can correctly write spans to the phoenix table
     * @throws Exception on failure
     */
    @Test
    public void testWriteSpans() throws Exception {

        LOG.info("testWriteSpans TableName: " + tracingTableName);
        // watch our sink so we know when commits happen
        latch = new CountDownLatch(1);

        testTraceWriter.start();

        // write some spans
        TraceScope trace = Trace.startSpan("Start write test", Sampler.ALWAYS);
        Span span = trace.getSpan();

        // add a child with some annotations
        Span child = span.child("child 1");
        child.addTimelineAnnotation("timeline annotation");
        TracingUtils.addAnnotation(child, "test annotation", 10);
        child.stop();

        // sleep a little bit to get some time difference
        Thread.sleep(100);

        trace.close();

        // pass the trace on
        Tracer.getInstance().deliver(span);

        // wait for the tracer to actually do the write
        assertTrue("Sink not flushed. commit() not called on the connection", latch.await(60, TimeUnit.SECONDS));

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

        LOG.info("testClientServerIndexingTracing TableName: " + tracingTableName);
        // one call for client side, one call for server side
        latch = new CountDownLatch(2);
        testTraceWriter.start();

        // separate connection so we don't create extra traces
        Connection conn = getConnectionWithoutTracing();
        createTestTable(conn, true);

        // trace the requests we send
        Connection traceable = getTracingConnection();
        LOG.debug("Doing dummy the writes to the tracked table");
        String insert = "UPSERT INTO " + enabledForLoggingTable + " VALUES (?, ?)";
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
        latch.await(200, TimeUnit.SECONDS);// should be way more than GC pauses

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
                if (traceInfo.contains(tracingTableName)) {
                    return false;
                }
                return traceInfo.contains("Completing index");
            }
        });

        assertTrue("Never found indexing updates", indexingCompleted);
    }

    private void createTestTable(Connection conn, boolean withIndex) throws SQLException {
        // create a dummy table
        String ddl =
                "create table if not exists " + enabledForLoggingTable + "(" + "k varchar not null, " + "c1 bigint"
                        + " CONSTRAINT pk PRIMARY KEY (k))";
        conn.createStatement().execute(ddl);

        // early exit if we don't need to create an index
        if (!withIndex) {
            return;
        }
        // create an index on the table - we know indexing has some basic tracing
        ddl = "CREATE INDEX IF NOT EXISTS " + enableForLoggingIndex + " on " + enabledForLoggingTable + " (c1)";
        conn.createStatement().execute(ddl);
    }

    @Test
    public void testScanTracing() throws Exception {

        LOG.info("testScanTracing TableName: " + tracingTableName);

        // separate connections to minimize amount of traces that are generated
        Connection traceable = getTracingConnection();
        Connection conn = getConnectionWithoutTracing();

        // one call for client side, one call for server side
        latch = new CountDownLatch(2);
        testTraceWriter.start();

        // create a dummy table
        createTestTable(conn, false);

        // update the table, but don't trace these, to simplify the traces we read
        LOG.debug("Doing dummy the writes to the tracked table");
        String insert = "UPSERT INTO " + enabledForLoggingTable + " VALUES (?, ?)";
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
        String read = "SELECT * FROM " + enabledForLoggingTable;
        ResultSet results = traceable.createStatement().executeQuery(read);
        assertTrue("Didn't get first result", results.next());
        assertTrue("Didn't get second result", results.next());
        results.close();

        assertTrue("Get expected updates to trace table", latch.await(200, TimeUnit.SECONDS));
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

        LOG.info("testScanTracingOnServer TableName: " + tracingTableName);

        // separate connections to minimize amount of traces that are generated
        Connection traceable = getTracingConnection();
        Connection conn = getConnectionWithoutTracing();

        // one call for client side, one call for server side
        latch = new CountDownLatch(5);
        testTraceWriter.start();

        // create a dummy table
        createTestTable(conn, false);

        // update the table, but don't trace these, to simplify the traces we read
        LOG.debug("Doing dummy the writes to the tracked table");
        String insert = "UPSERT INTO " + enabledForLoggingTable + " VALUES (?, ?)";
        PreparedStatement stmt = conn.prepareStatement(insert);
        stmt.setString(1, "key1");
        stmt.setLong(2, 1);
        stmt.execute();
        conn.commit();

        // setup for next set of updates
        stmt.setString(1, "key2");
        stmt.setLong(2, 2);
        stmt.execute();
        conn.commit();

        // do a scan of the table
        String read = "SELECT COUNT(*) FROM " + enabledForLoggingTable;
        ResultSet results = traceable.createStatement().executeQuery(read);
        assertTrue("Didn't get count result", results.next());
        // make sure we got the expected count
        assertEquals("Didn't get the expected number of row", 2, results.getInt(1));
        results.close();

        assertTrue("Didn't get expected updates to trace table", latch.await(60, TimeUnit.SECONDS));

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

    @Test
    public void testCustomAnnotationTracing() throws Exception {

        LOG.info("testCustomAnnotationTracing TableName: " + tracingTableName);

    	final String customAnnotationKey = "myannot";
    	final String customAnnotationValue = "a1";
    	final String tenantId = "tenant1";
        // separate connections to minimize amount of traces that are generated
        Connection traceable = getTracingConnection(ImmutableMap.of(customAnnotationKey, customAnnotationValue), tenantId);
        Connection conn = getConnectionWithoutTracing();

        // one call for client side, one call for server side
        latch = new CountDownLatch(2);
        testTraceWriter.start();

        // create a dummy table
        createTestTable(conn, false);

        // update the table, but don't trace these, to simplify the traces we read
        LOG.debug("Doing dummy the writes to the tracked table");
        String insert = "UPSERT INTO " + enabledForLoggingTable + " VALUES (?, ?)";
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
        String read = "SELECT * FROM " + enabledForLoggingTable;
        ResultSet results = traceable.createStatement().executeQuery(read);
        assertTrue("Didn't get first result", results.next());
        assertTrue("Didn't get second result", results.next());
        results.close();

        assertTrue("Get expected updates to trace table", latch.await(200, TimeUnit.SECONDS));

        assertAnnotationPresent(customAnnotationKey, customAnnotationValue, conn);
        assertAnnotationPresent(TENANT_ID_ATTRIB, tenantId, conn);
        // CurrentSCN is also added as an annotation. Not tested here because it screws up test setup.
    }

    @Test
    public void testTraceOnOrOff() throws Exception {
        Connection conn1 = getConnectionWithoutTracing(); //DriverManager.getConnection(getUrl());
        try{
            Statement statement = conn1.createStatement();
            ResultSet  rs = statement.executeQuery("TRACE ON");
            assertTrue(rs.next());
            PhoenixConnection pconn = (PhoenixConnection) conn1;
            long traceId = pconn.getTraceScope().getSpan().getTraceId();
            assertEquals(traceId, rs.getLong(1));
            assertEquals(traceId, rs.getLong("trace_id"));
            assertFalse(rs.next());
            assertEquals(Sampler.ALWAYS, pconn.getSampler());

            rs = statement.executeQuery("TRACE OFF");
            assertTrue(rs.next());
            assertEquals(traceId, rs.getLong(1));
            assertEquals(traceId, rs.getLong("trace_id"));
            assertFalse(rs.next());
            assertEquals(Sampler.NEVER, pconn.getSampler());

            rs = statement.executeQuery("TRACE OFF");
            assertFalse(rs.next());

            rs = statement.executeQuery("TRACE ON  WITH SAMPLING 0.5");
            rs.next();
            assertTrue(((PhoenixConnection) conn1).getSampler() instanceof ProbabilitySampler);

            rs = statement.executeQuery("TRACE ON  WITH SAMPLING 1.0");
            assertTrue(rs.next());
            traceId = pconn.getTraceScope().getSpan()
            .getTraceId();
            assertEquals(traceId, rs.getLong(1));
            assertEquals(traceId, rs.getLong("trace_id"));
            assertFalse(rs.next());
            assertEquals(Sampler.ALWAYS, pconn.getSampler());

            rs = statement.executeQuery("TRACE ON  WITH SAMPLING 0.5");
            rs.next();
            assertTrue(((PhoenixConnection) conn1).getSampler() instanceof ProbabilitySampler);

            rs = statement.executeQuery("TRACE ON WITH SAMPLING 0.0");
            rs.next();
            assertEquals(Sampler.NEVER, pconn.getSampler());

            rs = statement.executeQuery("TRACE OFF");
            assertFalse(rs.next());

       } finally {
            conn1.close();
        }
    }

    @Test
    public void testSingleSpan() throws Exception {

        LOG.info("testSingleSpan TableName: " + tracingTableName);

        Properties props = new Properties(TEST_PROPERTIES);
        Connection conn = DriverManager.getConnection(getUrl(), props);
        latch = new CountDownLatch(1);
        testTraceWriter.start();

        // create a simple metrics record
        long traceid = 987654;
        Span span = createNewSpan(traceid, Span.ROOT_SPAN_ID, 10, "root", 12, 13, "Some process", "test annotation for a span");

        Tracer.getInstance().deliver(span);
        assertTrue("Updates not written in table", latch.await(60, TimeUnit.SECONDS));

        // start a reader
        validateTraces(Collections.singletonList(span), conn, traceid, tracingTableName);
    }

    /**
     * Test multiple spans, within the same trace. Some spans are independent of the parent span,
     * some are child spans
     * @throws Exception on failure
     */
    @Test
    public void testMultipleSpans() throws Exception {

        LOG.info("testMultipleSpans TableName: " + tracingTableName);

        Connection conn = getConnectionWithoutTracing();
        latch = new CountDownLatch(4);
        testTraceWriter.start();

        // create a simple metrics record
        long traceid = 12345;
        List<Span> spans = new ArrayList<Span>();

        Span span =
                createNewSpan(traceid, Span.ROOT_SPAN_ID, 7777, "root", 10, 30,
                        "root process", "root-span tag");
        spans.add(span);

        // then create a child record
        span =
                createNewSpan(traceid, 7777, 6666, "c1", 11, 15, "c1 process",
                        "first child");
        spans.add(span);

        // create a different child
        span =
                createNewSpan(traceid, 7777, 5555, "c2", 11, 18, "c2 process",
                        "second child");
        spans.add(span);

        // create a child of the second child
        span =
                createNewSpan(traceid, 5555, 4444, "c3", 12, 16, "c3 process",
                        "third child");
        spans.add(span);

        for(Span span1 : spans)
            Tracer.getInstance().deliver(span1);

        assertTrue("Updates not written in table", latch.await(100, TimeUnit.SECONDS));

        // start a reader
        validateTraces(spans, conn, traceid, tracingTableName);
    }

    private void validateTraces(List<Span> spans, Connection conn, long traceid, String tableName)
            throws Exception {
        TraceReader reader = new TraceReader(conn, tableName);
        Collection<TraceHolder> traces = reader.readAll(1);
        assertEquals("Got an unexpected number of traces!", 1, traces.size());
        // make sure the trace matches what we wrote
        TraceHolder trace = traces.iterator().next();
        assertEquals("Got an unexpected traceid", traceid, trace.traceid);
        assertEquals("Got an unexpected number of spans", spans.size(), trace.spans.size());

        validateTrace(spans, trace);
    }

    /**
     * @param spans
     * @param trace
     */
    private void validateTrace(List<Span> spans, TraceHolder trace) {
        // drop each span into a sorted list so we get the expected ordering
        Iterator<SpanInfo> spanIter = trace.spans.iterator();
        for (Span span : spans) {
            SpanInfo spanInfo = spanIter.next();
            LOG.info("Checking span:\n" + spanInfo);

            long parentId = span.getParentId();
            if(parentId == Span.ROOT_SPAN_ID) {
                assertNull("Got a parent, but it was a root span!", spanInfo.parent);
            } else {
                assertEquals("Got an unexpected parent span id", parentId, spanInfo.parent.id);
            }

            assertEquals("Got an unexpected start time", span.getStartTimeMillis(), spanInfo.start);
            assertEquals("Got an unexpected end time", span.getStopTimeMillis(), spanInfo.end);

            int annotationCount = 0;
            for(Map.Entry<byte[], byte[]> entry : span.getKVAnnotations().entrySet()) {
                int count = annotationCount++;
                assertEquals("Didn't get expected annotation", count + " - " + Bytes.toString(entry.getValue()),
                        spanInfo.annotations.get(count));
            }
            assertEquals("Didn't get expected number of annotations", annotationCount,
                    spanInfo.annotationCount);
        }
    }

    private void assertAnnotationPresent(final String annotationKey, final String annotationValue, Connection conn) throws Exception {
        boolean tracingComplete = checkStoredTraces(conn, new TraceChecker(){
            @Override
            public boolean foundTrace(TraceHolder currentTrace) {
                return currentTrace.toString().contains(annotationKey + " - " + annotationValue);
            }
        });

        assertTrue("Didn't find the custom annotation in the tracing", tracingComplete);
    }

    private boolean checkStoredTraces(Connection conn, TraceChecker checker) throws Exception {
        TraceReader reader = new TraceReader(conn, tracingTableName);
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

}
