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

import static org.apache.phoenix.util.PhoenixRuntime.ANNOTATION_ATTRIB_PREFIX;
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
import java.util.stream.Collectors;

import io.opentelemetry.api.GlobalOpenTelemetry;
import io.opentelemetry.api.OpenTelemetry;
import io.opentelemetry.api.baggage.Baggage;
import io.opentelemetry.api.baggage.BaggageBuilder;
import io.opentelemetry.api.common.AttributeKey;
import io.opentelemetry.api.common.Attributes;
import io.opentelemetry.api.trace.Span;
import io.opentelemetry.context.Context;
import io.opentelemetry.context.Scope;
import io.opentelemetry.sdk.OpenTelemetrySdk;
import io.opentelemetry.sdk.testing.exporter.InMemorySpanExporter;
import io.opentelemetry.sdk.trace.SdkTracerProvider;
import io.opentelemetry.sdk.trace.data.SpanData;
import io.opentelemetry.sdk.trace.export.SimpleSpanProcessor;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.phoenix.compat.hbase.coprocessor.CompatBaseScannerRegionObserver;
import org.apache.phoenix.coprocessor.BaseScannerRegionObserver;
import org.apache.phoenix.end2end.ParallelStatsDisabledTest;
import org.apache.phoenix.jdbc.PhoenixConnection;
import org.apache.phoenix.thirdparty.com.google.common.collect.Maps;
import org.apache.phoenix.trace.TraceReader.SpanInfo;
import org.apache.phoenix.trace.TraceReader.TraceHolder;
import org.apache.phoenix.util.ReadOnlyProps;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Ignore;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.phoenix.thirdparty.com.google.common.collect.ImmutableMap;

/**
 * Test that the logging sink stores the expected metrics/stats
 */
@Category(ParallelStatsDisabledTest.class)
@Ignore("Will need to revisit for new HDFS/HBase/HTrace, broken on 5.x")
public class PhoenixTracingEndToEndIT extends BaseTracingTestIT {

    private static final Logger LOGGER = LoggerFactory.getLogger(PhoenixTracingEndToEndIT.class);
    private static final int MAX_RETRIES = 10;
    private String enabledForLoggingTable;
    private String enableForLoggingIndex;

    private static InMemorySpanExporter testExporter = InMemorySpanExporter.create();

    @BeforeClass
    public static void doSetup() throws Exception {
        GlobalOpenTelemetry.resetForTest();
        SdkTracerProvider sdkTracerProvider = SdkTracerProvider.builder()
            .addSpanProcessor(SimpleSpanProcessor.create(testExporter))
            .build();
        OpenTelemetry openTelemetry = OpenTelemetrySdk.builder()
            .setTracerProvider(sdkTracerProvider)
            .buildAndRegisterGlobal();
        Map<String, String> props = Maps.newHashMapWithExpectedSize(1);
        props.put(
            CompatBaseScannerRegionObserver.PHOENIX_MAX_LOOKBACK_AGE_CONF_KEY, Integer.toString(60*60)); // An hour
        setUpTestDriver(new ReadOnlyProps(props.entrySet().iterator()));
    }

    @Before
    public void setupMetrics() throws Exception {
        enabledForLoggingTable = "ENABLED_FOR_LOGGING_" + generateUniqueName();
        enableForLoggingIndex = "ENABALED_FOR_LOGGING_INDEX_" + generateUniqueName();
    }

    /**
     * Simple test that we can correctly write spans to the phoenix table
     * @throws Exception on failure
     */
    @Ignore
    // this is to test traces are being in stored in phoenix table
    // in this PR we are not making changes to support phoenix as a
    // store for traces
    public void testWriteSpans() throws Exception {

        LOGGER.info("testWriteSpans TableName: " + tracingTableName);
        // watch our sink so we know when commits happen
        latch = new CountDownLatch(1);

        testTraceWriter.start();

        // write some spans
        Span span = TraceUtil.getGlobalTracer().spanBuilder("Start write test").startSpan();

        // add a child with some annotations
        Span child = TraceUtil.getGlobalTracer().spanBuilder("child 1").setParent(Context.current().with(span)).startSpan();
        child.addEvent("timeline annotation");
        child.setAttribute("test annotation", 10);
        child.end();

        // sleep a little bit to get some time difference
        Thread.sleep(100);


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
        testExporter.reset();
        LOGGER.info("testClientServerIndexingTracing TableName: " + tracingTableName);

        // separate connection so we don't create extra traces
        Connection conn = getConnectionWithoutTracing();
        createTestTable(conn, true);
        // trace the requests we send
        Connection traceable = getTracingConnection();
        LOGGER.debug("Doing dummy the writes to the tracked table");
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
        List<SpanData> spans = testExporter.getFinishedSpanItems();
        boolean indexingCompleted = checkStoredTraces(spans, "Completing post index");
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
        testExporter.reset();
        LOGGER.info("testScanTracing TableName: " + tracingTableName);

        // separate connections to minimize amount of traces that are generated
        Connection traceable = getTracingConnection();
        Connection conn = getConnectionWithoutTracing();

        // create a dummy table
        createTestTable(conn, false);

        // update the table, but don't trace these, to simplify the traces we read
        LOGGER.debug("Doing dummy the writes to the tracked table");
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
        
        // don't trace reads either
        boolean tracingComplete = checkStoredTraces(testExporter.getFinishedSpanItems(), "Parallel scanner");
        assertTrue("No spans found for current table: " + enabledForLoggingTable,
            testExporter.getFinishedSpanItems().stream().filter(spanData -> spanData.getName()
                    .contains("Parallel scans for table: " + enabledForLoggingTable))
                .collect(Collectors.toList()).size() > 0);
        assertTrue("Didn't find the parallel scanner in the tracing", tracingComplete);
    }

    @Test
    public void testScanTracingOnServer() throws Exception {
        testExporter.reset();
        LOGGER.info("testScanTracingOnServer TableName: " + tracingTableName);

        // separate connections to minimize amount of traces that are generated
        Connection traceable = getTracingConnection();
        Connection conn = getConnectionWithoutTracing();

        // one call for client side, one call for server side
        latch = new CountDownLatch(5);
        testTraceWriter.start();

        // create a dummy table
        createTestTable(conn, false);

        // update the table, but don't trace these, to simplify the traces we read
        LOGGER.debug("Doing dummy the writes to the tracked table");
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
        testExporter.reset();
        // do a scan of the table
        Span selectSpan =
            TraceUtil.getGlobalTracer().spanBuilder("select span for " + enabledForLoggingTable)
                .startSpan();
        try (Scope scope = selectSpan.makeCurrent()) {
            String read = "SELECT COUNT(*) FROM " + enabledForLoggingTable;
            ResultSet results = traceable.createStatement().executeQuery(read);
            assertTrue("Didn't get count result", results.next());
            // make sure we got the expected count
            assertEquals("Didn't get the expected number of row", 2, results.getInt(1));
            results.close();
        } finally {
            selectSpan.end();
        }
        Optional<SpanData> optionalSpanData = testExporter.getFinishedSpanItems().stream()
            .filter(spanData -> spanData.getName().contains(enabledForLoggingTable)).findFirst();
        Set<String> parentSpansSet = testExporter.getFinishedSpanItems().stream()
            .filter(spanData -> spanData.getName().contains(enabledForLoggingTable))
            .collect(Collectors.toList()).stream().map(spanData -> spanData.getTraceId())
            .collect(Collectors.toSet());
        List<SpanData> spanDataList = null;
        if (optionalSpanData.isPresent()) {
            spanDataList = testExporter.getFinishedSpanItems().stream()
                .filter(spanData -> parentSpansSet.contains(spanData.getTraceId()))
                .collect(Collectors.toList());
        }
        assertNotNull("Span exist for current table: " + enabledForLoggingTable, spanDataList);
        // don't trace reads either
        boolean found = checkStoredTraces(testExporter.getFinishedSpanItems(),
            BaseScannerRegionObserver.SCANNER_OPENED_TRACE_INFO);
        assertTrue("Didn't find the parallel scanner in the tracing", found);
    }

    @Test
    public void testCustomAnnotationTracing() throws Exception {
        testExporter.reset();
        LOGGER.info("testCustomAnnotationTracing TableName: " + tracingTableName);

    	final String customAnnotationKey = "myannot";
    	final String customAnnotationValue = "a1";
    	final String tenantId = "tenant1";
        // separate connections to minimize amount of traces that are generated
        Connection traceable = getTracingConnection(ImmutableMap.of(customAnnotationKey, customAnnotationValue), tenantId);
        Connection conn = getConnectionWithoutTracing();


        // create a dummy table
        createTestTable(conn, false);

        // update the table, but don't trace these, to simplify the traces we read
        LOGGER.debug("Doing dummy the writes to the tracked table");
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
        Span span = TraceUtil.getGlobalTracer().spanBuilder("select span").setAttribute(ANNOTATION_ATTRIB_PREFIX + customAnnotationKey, customAnnotationValue).startSpan();
        try(Scope scope = span.makeCurrent()){
            String read = "SELECT * FROM " + enabledForLoggingTable;
            ResultSet results = traceable.createStatement().executeQuery(read);
            assertTrue("Didn't get first result", results.next());
            assertTrue("Didn't get second result", results.next());
            results.close();
        } finally {
            span.end();
        }
        List<SpanData> completedSpans = testExporter.getFinishedSpanItems();
        Optional<SpanData> optionalSpanData = completedSpans.stream().filter(spanData -> spanData.getName().contains("select span")).findFirst();
        assertTrue(optionalSpanData.isPresent());
        SpanData spanData = optionalSpanData.get();
        Attributes attributes =  spanData.getAttributes();

        assertNotNull("Attribute value for key:" + ANNOTATION_ATTRIB_PREFIX + customAnnotationKey, attributes.get(AttributeKey.stringKey(ANNOTATION_ATTRIB_PREFIX + customAnnotationKey)));
        assertTrue(attributes.get(AttributeKey.stringKey(ANNOTATION_ATTRIB_PREFIX + customAnnotationKey)).equals(customAnnotationValue));
        // CurrentSCN is also added as an annotation. Not tested here because it screws up test setup.
    }

    @Test
    public void testTraceOnOrOff() throws Exception {
        Connection conn1 = getConnectionWithoutTracing(); //DriverManager.getConnection(getUrl());
        try{
            /*Statement statement = conn1.createStatement();
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
            */

       } finally {
            conn1.close();
        }
    }

    @Test
    public void testSingleSpan() throws Exception {

        LOGGER.info("testSingleSpan TableName: " + tracingTableName);

        Properties props = new Properties(TEST_PROPERTIES);
        Connection conn = DriverManager.getConnection(getUrl(), props);
        latch = new CountDownLatch(1);
        testTraceWriter.start();

        /*// create a simple metrics record
        long traceid = 987654;
        Span span = createNewSpan(traceid, Span.ROOT_SPAN_ID, 10, "root", 12, 13, "Some process", "test annotation for a span");

        Tracer.getInstance().deliver(span);
        assertTrue("Updates not written in table", latch.await(60, TimeUnit.SECONDS));

        // start a reader
        validateTraces(Collections.singletonList(span), conn, traceid, tracingTableName);
         */
    }

    /**
     * Test multiple spans, within the same trace. Some spans are independent of the parent span,
     * some are child spans
     * @throws Exception on failure
     */
    @Test
    public void testMultipleSpans() throws Exception {
        /*
        LOGGER.info("testMultipleSpans TableName: " + tracingTableName);

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
        validateTraces(spans, conn, traceid, tracingTableName);*/
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
        /*for (Span span : spans) {
            SpanInfo spanInfo = spanIter.next();
            LOGGER.info("Checking span:\n" + spanInfo);

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
        }*/
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

    private boolean checkStoredTraces(List<SpanData> spans, String spanString) {
        return spans.stream().filter(spanData -> spanData.getName().contains(spanString)).findFirst().isPresent();
    }

    private boolean checkStoredTraces(Connection conn, TraceChecker checker) throws Exception {
        TraceReader reader = new TraceReader(conn, tracingTableName);
        int retries = 0;
        boolean found = false;
        outer: while (retries < MAX_RETRIES) {
            Collection<TraceHolder> traces = reader.readAll(100);
            for (TraceHolder trace : traces) {
                LOGGER.info("Got trace: " + trace);
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
            LOGGER.info("======  Waiting for tracing updates to be propagated ========");
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
