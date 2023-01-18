/**
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
import static org.junit.Assert.fail;

import java.sql.Connection;
import java.util.Collection;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import io.opentelemetry.api.GlobalOpenTelemetry;
import io.opentelemetry.api.OpenTelemetry;
import io.opentelemetry.sdk.OpenTelemetrySdk;
import io.opentelemetry.sdk.testing.exporter.InMemorySpanExporter;
import io.opentelemetry.sdk.trace.SdkTracerProvider;
import io.opentelemetry.sdk.trace.export.SimpleSpanProcessor;
import io.opentelemetry.api.trace.Span;
import org.apache.phoenix.end2end.ParallelStatsDisabledTest;
import org.apache.phoenix.query.QueryServicesOptions;
import org.apache.phoenix.trace.TraceReader.SpanInfo;
import org.apache.phoenix.trace.TraceReader.TraceHolder;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;

/**
 * Test that the logging sink stores the expected metrics/stats
 */
@Category(ParallelStatsDisabledTest.class)
public class PhoenixTableMetricsWriterIT extends BaseTracingTestIT {

    private final InMemorySpanExporter testExporter = InMemorySpanExporter.create();
    private OpenTelemetry openTelemetry = null;

    @Before
    public void setup() {
        GlobalOpenTelemetry.resetForTest();
        SdkTracerProvider sdkTracerProvider = SdkTracerProvider.builder()
            .addSpanProcessor(SimpleSpanProcessor.create(testExporter))
            .build();
        openTelemetry = OpenTelemetrySdk.builder()
            .setTracerProvider(sdkTracerProvider)
            .buildAndRegisterGlobal();
    }

    /**
     * IT should create the target table if it hasn't been created yet, but not fail if the table
     * has already been created
     * @throws Exception on failure
     */
    @Test
    public void testCreatesTable() throws Exception {

        Connection conn = getConnectionWithoutTracing();

        // check for existence of the tracing table
        try {
            String ddl = "CREATE TABLE " + QueryServicesOptions.DEFAULT_TRACING_STATS_TABLE_NAME;
            conn.createStatement().execute(ddl);
            fail("Table " + QueryServicesOptions.DEFAULT_TRACING_STATS_TABLE_NAME
                    + " was not created by the metrics sink");
        } catch (Exception e) {
            // expected
        }
    }

    /**
     * Simple metrics writing and reading check, that uses the standard wrapping in the
     * {@link TraceWriter}
     * @throws Exception on failure
     */
    @Test
    public void writeMetrics() throws Exception {

        Connection conn = getConnectionWithoutTracing();
        latch = new CountDownLatch(1);
        testTraceWriter.start();

        // create a simple metrics record
        long traceid = 987654;
        String description = "Some generic trace";
        long spanid = 10;
        long parentid = 11;
        long startTime = 12;
        long endTime = 13;
        String processid = "Some process";
        String annotation = "test annotation for a span";

        Span span = createNewSpan(traceid, parentid, spanid, description, startTime, endTime,
            processid, annotation);

        // make sure we only get expected stat entry (matcing the trace id), otherwise we could the
        // stats for the update as well
        TraceReader reader = new TraceReader(conn, tracingTableName);
        Collection<TraceHolder> traces = reader.readAll(10);
        assertEquals("Wrong number of traces in the tracing table", 1, traces.size());

        // validate trace
        TraceHolder trace = traces.iterator().next();
        // we are just going to get an orphan span b/c we don't send in a parent
        assertEquals("Didn't get expected orphaned spans!" + trace.orphans, 1, trace.orphans.size());

        assertEquals(traceid, trace.traceid);
        SpanInfo spanInfo = trace.orphans.get(0);
        assertEquals(description, spanInfo.description);
        assertEquals(parentid, spanInfo.getParentIdForTesting());
        assertEquals(startTime, spanInfo.start);
        assertEquals(endTime, spanInfo.end);
        assertEquals("Wrong number of tags", 0, spanInfo.tagCount);
        assertEquals("Wrong number of annotations", 1, spanInfo.annotationCount);
    }

    @After
    public void tearDown(){
        if(testExporter != null){
            testExporter.close();
        }
        GlobalOpenTelemetry.resetForTest();
    }

}
