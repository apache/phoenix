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

import static org.apache.phoenix.util.StringUtil.toBytes;

import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.Callable;

import javax.annotation.Nullable;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.htrace.HTraceConfiguration;
import org.apache.phoenix.call.CallRunner;
import org.apache.phoenix.call.CallWrapper;
import org.apache.phoenix.jdbc.PhoenixConnection;
import org.apache.phoenix.parse.TraceStatement;
import org.apache.phoenix.query.QueryServices;
import org.apache.phoenix.query.QueryServicesOptions;
import org.apache.phoenix.trace.TraceMetricSource;
import org.apache.htrace.Sampler;
import org.apache.htrace.Span;
import org.apache.htrace.Trace;
import org.apache.htrace.TraceScope;
import org.apache.htrace.Tracer;
import org.apache.htrace.impl.ProbabilitySampler;
import org.apache.htrace.wrappers.TraceCallable;
import org.apache.htrace.wrappers.TraceRunnable;

import com.google.common.base.Function;
import com.google.common.base.Preconditions;
import com.sun.istack.NotNull;

/**
 * Helper class to manage using the {@link Tracer} within Phoenix
 */
public class Tracing {

    private static final Log LOG = LogFactory.getLog(Tracing.class);

    private static final String SEPARATOR = ".";
    // Constants for tracing across the wire
    public static final String TRACE_ID_ATTRIBUTE_KEY = "phoenix.trace.traceid";
    public static final String SPAN_ID_ATTRIBUTE_KEY = "phoenix.trace.spanid";

    // Constants for passing into the metrics system
    private static final String TRACE_METRIC_PREFIX = "phoenix.trace.instance";

    /**
     * Manage the types of frequencies that we support. By default, we never turn on tracing.
     */
    public static enum Frequency {
        NEVER("never", CREATE_NEVER), // default
        ALWAYS("always", CREATE_ALWAYS), PROBABILITY("probability", CREATE_PROBABILITY);

        String key;
        Function<ConfigurationAdapter, Sampler<?>> builder;

        private Frequency(String key, Function<ConfigurationAdapter, Sampler<?>> builder) {
            this.key = key;
            this.builder = builder;
        }

        public String getKey() {
            return key;
        }

        static Frequency getSampler(String key) {
            for (Frequency type : Frequency.values()) {
                if (type.key.equals(key)) {
                    return type;
                }
            }
            return NEVER;
        }
    }

    private static Function<ConfigurationAdapter, Sampler<?>> CREATE_ALWAYS =
            new Function<ConfigurationAdapter, Sampler<?>>() {
                @Override
                public Sampler<?> apply(ConfigurationAdapter arg0) {
                    return Sampler.ALWAYS;
                }
            };

    private static Function<ConfigurationAdapter, Sampler<?>> CREATE_NEVER =
            new Function<ConfigurationAdapter, Sampler<?>>() {
                @Override
                public Sampler<?> apply(ConfigurationAdapter arg0) {
                    return Sampler.NEVER;
                }
            };

    private static Function<ConfigurationAdapter, Sampler<?>> CREATE_PROBABILITY =
            new Function<ConfigurationAdapter, Sampler<?>>() {
                @Override
                public Sampler<?> apply(ConfigurationAdapter conf) {
                    // get the connection properties for the probability information
                    Map<String, String> items = new HashMap<String, String>();
                    items.put(ProbabilitySampler.SAMPLER_FRACTION_CONF_KEY,
                            conf.get(QueryServices.TRACING_PROBABILITY_THRESHOLD_ATTRIB, Double.toString(QueryServicesOptions.DEFAULT_TRACING_PROBABILITY_THRESHOLD)));
                    return new ProbabilitySampler(HTraceConfiguration.fromMap(items));
                }
            };

    public static Sampler<?> getConfiguredSampler(PhoenixConnection connection) {
        String tracelevel = connection.getQueryServices().getProps().get(QueryServices.TRACING_FREQ_ATTRIB, QueryServicesOptions.DEFAULT_TRACING_FREQ);
        return getSampler(tracelevel, new ConfigurationAdapter.ConnectionConfigurationAdapter(
                connection));
    }

    public static Sampler<?> getConfiguredSampler(Configuration conf) {
        String tracelevel = conf.get(QueryServices.TRACING_FREQ_ATTRIB, QueryServicesOptions.DEFAULT_TRACING_FREQ);
        return getSampler(tracelevel, new ConfigurationAdapter.HadoopConfigConfigurationAdapter(
                conf));
    }

    public static Sampler<?> getConfiguredSampler(TraceStatement traceStatement) {
      double samplingRate = traceStatement.getSamplingRate();
      if (samplingRate >= 1.0) {
          return Sampler.ALWAYS;
      } else if (samplingRate < 1.0 && samplingRate > 0.0) {
          Map<String, String> items = new HashMap<String, String>();
          items.put(ProbabilitySampler.SAMPLER_FRACTION_CONF_KEY, Double.toString(samplingRate));
          return new ProbabilitySampler(HTraceConfiguration.fromMap(items));
      } else {
          return Sampler.NEVER;
      }
    }

    private static Sampler<?> getSampler(String traceLevel, ConfigurationAdapter conf) {
        return Frequency.getSampler(traceLevel).builder.apply(conf);
    }

    public static void setSampling(Properties props, Frequency freq) {
        props.setProperty(QueryServices.TRACING_FREQ_ATTRIB, freq.key);
    }

    /**
     * Start a span with the currently configured sampling frequency. Creates a new 'current' span
     * on this thread - the previous 'current' span will be replaced with this newly created span.
     * <p>
     * Hands back the direct span as you shouldn't be detaching the span - use {@link TraceRunnable}
     * instead to detach a span from this operation.
     * @param connection from which to read parameters
     * @param string description of the span to start
     * @return the underlying span.
     */
    public static TraceScope startNewSpan(PhoenixConnection connection, String string) {
        Sampler<?> sampler = connection.getSampler();
        TraceScope scope = Trace.startSpan(string, sampler);
        addCustomAnnotationsToSpan(scope.getSpan(), connection);
        return scope;
    }

    public static String getSpanName(Span span) {
        return Tracing.TRACE_METRIC_PREFIX + span.getTraceId() + SEPARATOR + span.getParentId()
                + SEPARATOR + span.getSpanId();
    }

    public static Span child(Span s, String d) {
        if (s == null) {
            return NullSpan.INSTANCE;
        }
        return s.child(d);
    }

    /**
     * Wrap the callable in a TraceCallable, if tracing.
     * @param callable to call
     * @param description description of the operation being run. If <tt>null</tt> uses the current
     *            thread name
     * @return The callable provided, wrapped if tracing, 'callable' if not.
     */
    public static <V> Callable<V> wrap(Callable<V> callable, String description) {
        if (Trace.isTracing()) {
            return new TraceCallable<V>(Trace.currentSpan(), callable, description);
        }
        return callable;
    }

    /**
     * Helper to automatically start and complete tracing on the given method, used in conjuction
     * with {@link CallRunner#run}.
     * <p>
     * This will always attempt start a new span (which will always start, unless the
     * {@link Sampler} says it shouldn't be traced). If you are just looking for flexible tracing
     * that only turns on if the current thread/query is already tracing, use
     * {@link #wrap(Callable, String)} or {@link Trace#wrap(Callable)}.
     * <p>
     * Ensures that the trace is closed, even if there is an exception from the
     * {@link org.apache.phoenix.call.CallRunner.CallableThrowable}.
     * <p>
     * Generally, this should wrap a long-running operation.
     * @param conn connection from which to determine if we are tracing, ala
     *            {@link #startNewSpan(PhoenixConnection, String)}
     * @param desc description of the operation being run
     * @return the value returned from the call
     */
    public static CallWrapper withTracing(PhoenixConnection conn, String desc) {
        return new TracingWrapper(conn, desc);
    }

    private static void addCustomAnnotationsToSpan(@Nullable Span span, @NotNull PhoenixConnection conn) {
        Preconditions.checkNotNull(conn);

        if (span == null) {
        	return;
        }
		Map<String, String> annotations = conn.getCustomTracingAnnotations();
		// copy over the annotations as bytes
		for (Map.Entry<String, String> annotation : annotations.entrySet()) {
			span.addKVAnnotation(toBytes(annotation.getKey()), toBytes(annotation.getValue()));
        }
    }

    private static class TracingWrapper implements CallWrapper {
        private TraceScope scope;
        private final PhoenixConnection conn;
        private final String desc;

        public TracingWrapper(PhoenixConnection conn, String desc){
            this.conn = conn;
            this.desc = desc;
        }

        @Override
        public void before() {
            scope = Tracing.startNewSpan(conn, "Executing " + desc);
        }

        @Override
        public void after() {
            scope.close();
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
            if (!initialized) {
                Trace.addReceiver(new TraceMetricSource());
            }
        } catch (RuntimeException e) {
            LOG.warn("Tracing will outputs will not be written to any metrics sink! No "
                    + "TraceMetricsSink found on the classpath", e);
        } catch (IllegalAccessError e) {
            // This is an issue when we have a class incompatibility error, such as when running
            // within SquirrelSQL which uses an older incompatible version of commons-collections.
            // Seeing as this only results in disabling tracing, we swallow this exception and just
            // continue on without tracing.
            LOG.warn("Class incompatibility while initializing metrics, metrics will be disabled", e);
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

}
