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

import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.Pair;
import org.apache.htrace.Span;

/**
 * Utilities for tracing
 */
public class TracingUtils {
    public static final String METRIC_SOURCE_KEY = "phoenix.";

    /** Set context to enable filtering */
    public static final String METRICS_CONTEXT = "tracing";

    /** Marker metric to ensure that we register the tracing mbeans */
    public static final String METRICS_MARKER_CONTEXT = "marker";

    public static void addAnnotation(Span span, String message, int value) {
        span.addKVAnnotation(message.getBytes(), Bytes.toBytes(Integer.toString(value)));
    }

    public static Pair<String, String> readAnnotation(byte[] key, byte[] value) {
        return new Pair<String, String>(new String(key), Bytes.toString(value));
    }

    /**
     * @see #getTraceMetricName(String)
     */
    public static final String getTraceMetricName(long traceId) {
        return getTraceMetricName(Long.toString(traceId));
    }

    /**
     * @param traceId unique id of the trace
     * @return the name of the metric record that should be generated for a given trace
     */
    public static final String getTraceMetricName(String traceId) {
        return METRIC_SOURCE_KEY + traceId;
    }
}
