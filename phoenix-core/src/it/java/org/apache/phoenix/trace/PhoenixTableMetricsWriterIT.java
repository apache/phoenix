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

import org.apache.hadoop.metrics2.MetricsRecord;
import org.apache.phoenix.query.QueryServicesOptions;
import org.apache.phoenix.trace.TraceReader.SpanInfo;
import org.apache.phoenix.trace.TraceReader.TraceHolder;
import org.junit.Test;

import java.sql.Connection;
import java.util.Collection;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

/**
 * Test that the logging sink stores the expected metrics/stats
 */
public class PhoenixTableMetricsWriterIT extends BaseTracingTestIT {

    /**
     * IT should create the target table if it hasn't been created yet, but not fail if the table
     * has already been created
     * @throws Exception on failure
     */
    @Test
    public void testCreatesTable() throws Exception {
        PhoenixMetricsSink sink = new PhoenixMetricsSink();
        Connection conn = getConnectionWithoutTracing();
        sink.initForTesting(conn);

        // check for existence of the tracing table
        try {
            String ddl = "CREATE TABLE " + QueryServicesOptions.DEFAULT_TRACING_STATS_TABLE_NAME;
            conn.createStatement().execute(ddl);
            fail("Table " + QueryServicesOptions.DEFAULT_TRACING_STATS_TABLE_NAME
                    + " was not created by the metrics sink");
        } catch (Exception e) {
            // expected
        }

        // initialize sink again, which should attempt to create the table, but not fail
        try {
            sink.initForTesting(conn);
        } catch (Exception e) {
            fail("Initialization shouldn't fail if table already exists!");
        }
    }

    /**
     * Simple metrics writing and reading check, that uses the standard wrapping in the
     * {@link PhoenixMetricsSink}
     * @throws Exception on failure
     */
    @Test
    public void writeMetrics() throws Exception {
        // hook up a phoenix sink
        PhoenixMetricsSink sink = new PhoenixMetricsSink();
        Connection conn = getConnectionWithoutTracing();
        sink.initForTesting(conn);

        // create a simple metrics record
        long traceid = 987654;
        String description = "Some generic trace";
        long spanid = 10;
        long parentid = 11;
        long startTime = 12;
        long endTime = 13;
        String annotation = "test annotation for a span";
        String hostnameValue = "host-name.value";
       MetricsRecord record =
                createRecord(traceid, parentid, spanid, description, startTime, endTime,
                    hostnameValue, annotation);

        // actually write the record to the table
        sink.putMetrics(record);
        sink.flush();

        // make sure we only get expected stat entry (matcing the trace id), otherwise we could the
        // stats for the update as well
        TraceReader reader = new TraceReader(conn);
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
        assertEquals(hostnameValue, spanInfo.hostname);
        assertEquals("Wrong number of tags", 0, spanInfo.tagCount);
        assertEquals("Wrong number of annotations", 1, spanInfo.annotationCount);
    }
}