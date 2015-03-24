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

import static org.apache.phoenix.util.TestUtil.TEST_PROPERTIES;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

import java.sql.Connection;
import java.sql.DriverManager;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Properties;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.metrics2.AbstractMetric;
import org.apache.hadoop.metrics2.MetricsRecord;
import org.apache.hadoop.metrics2.MetricsTag;
import org.apache.phoenix.metrics.MetricInfo;
import org.apache.phoenix.trace.TraceReader.SpanInfo;
import org.apache.phoenix.trace.TraceReader.TraceHolder;
import org.apache.htrace.Span;
import org.junit.Test;

/**
 * Test that the {@link TraceReader} will correctly read traces written by the
 * {@link org.apache.phoenix.trace.PhoenixMetricsSink}
 */

public class PhoenixTraceReaderIT extends BaseTracingTestIT {

    private static final Log LOG = LogFactory.getLog(PhoenixTraceReaderIT.class);

    @Test
    public void singleSpan() throws Exception {
        PhoenixMetricsSink sink = new PhoenixMetricsSink();
        Properties props = new Properties(TEST_PROPERTIES);
        Connection conn = DriverManager.getConnection(getUrl(), props);
        sink.initForTesting(conn);

        // create a simple metrics record
        long traceid = 987654;
        MetricsRecord record =
                createAndFlush(sink, traceid, Span.ROOT_SPAN_ID, 10, "root", 12, 13,
                    "host-name.value", "test annotation for a span");

        // start a reader
        validateTraces(Collections.singletonList(record), conn, traceid);
    }

    private MetricsRecord createAndFlush(PhoenixMetricsSink sink, long traceid,
            long parentid, long spanid, String desc, long startTime, long endTime, String hostname,
            String... tags) {
        MetricsRecord record =
                createRecord(traceid, parentid, spanid, desc, startTime, endTime, hostname, tags);
        sink.putMetrics(record);
        sink.flush();
        return record;
    }

    /**
     * Test multiple spans, within the same trace. Some spans are independent of the parent span,
     * some are child spans
     * @throws Exception on failure
     */
    @Test
    public void testMultipleSpans() throws Exception {
        // hook up a phoenix sink
        PhoenixMetricsSink sink = new PhoenixMetricsSink();
        Connection conn = getConnectionWithoutTracing();
        sink.initForTesting(conn);

        // create a simple metrics record
        long traceid = 12345;
        List<MetricsRecord> records = new ArrayList<MetricsRecord>();
        MetricsRecord record =
                createAndFlush(sink, traceid, Span.ROOT_SPAN_ID, 7777, "root", 10, 30,
                    "hostname.value", "root-span tag");
        records.add(record);

        // then create a child record
        record =
                createAndFlush(sink, traceid, 7777, 6666, "c1", 11, 15, "hostname.value",
                    "first child");
        records.add(record);

        // create a different child
        record =
                createAndFlush(sink, traceid, 7777, 5555, "c2", 11, 18, "hostname.value",
                    "second child");
        records.add(record);

        // create a child of the second child
        record =
                createAndFlush(sink, traceid, 5555, 4444, "c3", 12, 16, "hostname.value",
                    "third child");
        records.add(record);

        // flush all the values to the table
        sink.flush();

        // start a reader
        validateTraces(records, conn, traceid);
    }

    private void validateTraces(List<MetricsRecord> records, Connection conn, long traceid)
            throws Exception {
        TraceReader reader = new TraceReader(conn);
        Collection<TraceHolder> traces = reader.readAll(1);
        assertEquals("Got an unexpected number of traces!", 1, traces.size());
        // make sure the trace matches what we wrote
        TraceHolder trace = traces.iterator().next();
        assertEquals("Got an unexpected traceid", traceid, trace.traceid);
        assertEquals("Got an unexpected number of spans", records.size(), trace.spans.size());

        validateTrace(records, trace);
    }

    /**
     * @param records
     * @param trace
     */
    private void validateTrace(List<MetricsRecord> records, TraceHolder trace) {
        // drop each span into a sorted list so we get the expected ordering
        Iterator<SpanInfo> spanIter = trace.spans.iterator();
        for (MetricsRecord record : records) {
            SpanInfo spanInfo = spanIter.next();
            LOG.info("Checking span:\n" + spanInfo);
            Iterator<AbstractMetric> metricIter = record.metrics().iterator();
            assertEquals("Got an unexpected span id", metricIter.next().value(), spanInfo.id);
            long parentId = (Long) metricIter.next().value();
            if (parentId == Span.ROOT_SPAN_ID) {
                assertNull("Got a parent, but it was a root span!", spanInfo.parent);
            } else {
                assertEquals("Got an unexpected parent span id", parentId, spanInfo.parent.id);
            }
            assertEquals("Got an unexpected start time", metricIter.next().value(), spanInfo.start);
            assertEquals("Got an unexpected end time", metricIter.next().value(), spanInfo.end);

            Iterator<MetricsTag> tags = record.tags().iterator();

            int annotationCount = 0;
            while (tags.hasNext()) {
                // hostname is a tag, so we differentiate it
                MetricsTag tag = tags.next();
                if (tag.name().equals(MetricInfo.HOSTNAME.traceName)) {
                    assertEquals("Didn't store correct hostname value", tag.value(),
                        spanInfo.hostname);
                } else {
                    int count = annotationCount++;
                    assertEquals("Didn't get expected annotation", count + " - " + tag.value(),
                        spanInfo.annotations.get(count));
                }
            }
            assertEquals("Didn't get expected number of annotations", annotationCount,
                spanInfo.annotationCount);
        }
    }
}