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
package org.apache.phoenix.trace.util;

import java.util.concurrent.Callable;

import io.opentelemetry.api.trace.Span;
import io.opentelemetry.api.trace.Tracer;
import org.apache.phoenix.call.CallRunner;
import org.apache.phoenix.call.CallWrapper;
import org.apache.phoenix.jdbc.PhoenixConnection;
import org.apache.phoenix.query.QueryServicesOptions;
import org.apache.phoenix.trace.TraceUtil;
import org.apache.phoenix.trace.TraceWriter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.phoenix.thirdparty.com.google.common.base.Preconditions;

/**
 * Helper class to manage using the {@link Tracer} within Phoenix
 */
public class Tracing {

    private static final Logger LOGGER = LoggerFactory.getLogger(Tracing.class);

    private static final String SEPARATOR = ".";
    // Constants for tracing across the wire
    public static final String TRACE_ID_ATTRIBUTE_KEY = "phoenix.trace.traceid";
    public static final String SPAN_ID_ATTRIBUTE_KEY = "phoenix.trace.spanid";

    // Constants for passing into the metrics system
    private static final String TRACE_METRIC_PREFIX = "phoenix.trace.instance";



    /**
     * Wrap the callable in a TraceCallable, if tracing.
     * @param callable to call
     * @param description description of the operation being run. If <tt>null</tt> uses the current
     *            thread name
     * @return The callable provided, wrapped if tracing, 'callable' if not.
     */
    public static <V> Callable<V> wrap(Callable<V> callable, String description) {
        return new TraceUtil.TraceCallable<V>(callable, description);
    }


    /**
     * Helper to automatically start and complete tracing on the given method, used in conjuction
     * with {@link CallRunner#run}.
     * <p>
     *
     * Ensures that the trace is closed, even if there is an exception from the
     * {@link org.apache.phoenix.call.CallRunner.CallableThrowable}.
     * <p>
     * Generally, this should wrap a long-running operation.
     * @param conn connection
     * @param desc description of the operation being run
     * @return the value returned from the call
     */
    public static CallWrapper withTracing(PhoenixConnection conn, String desc) {
        return new TracingWrapper(conn, desc);
    }


    private static class TracingWrapper implements CallWrapper {
        private Span span;
        private final PhoenixConnection conn;
        private final String desc;

        public TracingWrapper(PhoenixConnection conn, String desc){
            this.conn = conn;
            this.desc = desc;
        }

        @Override
        public void before() {
            span = TraceUtil.getGlobalTracer().spanBuilder("Executing " + desc).startSpan();
        }

        @Override
        public void after() {
            if (span != null) {
                span.end();
            }
        }
    }

    /**
     * Track if the tracing system has been initialized for phoenix
     */
    private static boolean initialized = false;

    /**
     * Add the phoenix span receiver so we can log the traces. We have a single trace source for the
     * whole JVM
     */
    public synchronized static void addTraceMetricsSource() {
        try {
            QueryServicesOptions options = QueryServicesOptions.withDefaults();
            if (!initialized && options.isTracingEnabled()) {
                TraceWriter traceWriter = new TraceWriter(options.getTableName(), options.getTracingThreadPoolSize(), options.getTracingBatchSize());
                traceWriter.start();
            }
        } catch (RuntimeException e) {
            LOGGER.warn("Tracing will outputs will not be written to any metrics sink! No "
                    + "TraceMetricsSink found on the classpath", e);
        } catch (IllegalAccessError e) {
            // This is an issue when we have a class incompatibility error, such as when running
            // within SquirrelSQL which uses an older incompatible version of commons-collections.
            // Seeing as this only results in disabling tracing, we swallow this exception and just
            // continue on without tracing.
            LOGGER.warn("Class incompatibility while initializing metrics, metrics will be disabled", e);
        }
        initialized = true;
    }

    public static boolean isTraceOn(String traceOption) {
        Preconditions.checkArgument(traceOption != null);
        if(traceOption.equalsIgnoreCase("ON")) return true;
        if(traceOption.equalsIgnoreCase("OFF")) return false;
        else {
            throw new IllegalArgumentException("Unknown tracing option: " + traceOption);
        }
    }

    /**
     * Check whether tracing is generally enabled.
     * @return true If tracing is enabled, false otherwise
     */
    public static boolean isTracing() {
        //TODO: Currently setting it to false will investigate
        return false;
    }
}
