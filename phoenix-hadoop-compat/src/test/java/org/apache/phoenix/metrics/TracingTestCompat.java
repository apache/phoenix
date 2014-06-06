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
package org.apache.phoenix.metrics;

import org.apache.hadoop.hbase.CompatibilityFactory;
import org.apache.phoenix.trace.TestableMetricsWriter;

/**
 * Utility class for testing tracing
 */
public class TracingTestCompat {

    private TracingTestCompat() {
        assert false;
    }

    public static TestableMetricsWriter newTraceMetricSink() {
        return CompatibilityFactory.getInstance(TestableMetricsWriter.class);
    }

    /**
     * Register the sink with the metrics system, so we don't need to specify it in the conf
     * @param sink
     */
    public static void registerSink(MetricsWriter sink) {
        TestableMetricsWriter writer = newTraceMetricSink();
        writer.setWriterForTesting(sink);
        Metrics.getManager().register("phoenix", "test sink gets logged", writer);
    }
}