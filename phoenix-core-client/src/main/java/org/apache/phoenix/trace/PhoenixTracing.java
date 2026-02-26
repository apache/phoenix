/*
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

import io.opentelemetry.api.GlobalOpenTelemetry;
import io.opentelemetry.api.trace.Span;
import io.opentelemetry.api.trace.SpanKind;
import io.opentelemetry.api.trace.StatusCode;
import io.opentelemetry.api.trace.Tracer;
import io.opentelemetry.context.Context;
import io.opentelemetry.context.Scope;
import java.util.concurrent.Callable;
import java.util.concurrent.CompletableFuture;
import java.util.function.Supplier;
import org.apache.phoenix.call.CallWrapper;

/**
 * Central tracing facade for Apache Phoenix using OpenTelemetry.
 * <p>
 * This class follows the same pattern as HBase's {@code TraceUtil} (HBASE-22120). It provides a
 * thin facade over the OpenTelemetry API for creating and managing spans throughout Phoenix.
 * <p>
 * All methods are safe to call even when no OpenTelemetry SDK is configured (e.g., when the Java
 * Agent is not attached). In that case, all calls are no-ops with zero overhead, because the
 * OpenTelemetry API returns no-op implementations by default.
 * <p>
 * <b>Architecture:</b>
 * <ul>
 * <li>Compile-time: Phoenix depends only on {@code opentelemetry-api} (no SDK bundled)</li>
 * <li>Runtime: The OpenTelemetry Java Agent provides the SDK implementation. Since Phoenix runs
 * inside HBase's JVM (as coprocessors), it uses HBase's agent (shipped in
 * {@code lib/trace/}).</li>
 * <li>Backend: Operators configure the export destination (Jaeger, Tempo, Zipkin, etc.) via
 * environment variables.</li>
 * </ul>
 * <p>
 * <b>Usage examples:</b>
 *
 * <pre>
 * // Simple span creation with try-with-resources
 * Span span = PhoenixTracing.createSpan("phoenix.query.compile");
 * try (Scope ignored = span.makeCurrent()) {
 *     // do work
 *     span.setStatus(StatusCode.OK);
 * } catch (Exception e) {
 *     PhoenixTracing.setError(span, e);
 *     throw e;
 * } finally {
 *     span.end();
 * }
 *
 * // Using the trace() convenience method
 * PhoenixTracing.trace(() -&gt; {
 *     compileQuery(sql);
 * }, "phoenix.query.compile");
 *
 * // Wrapping a callable for thread pool execution
 * Callable&lt;Result&gt; wrapped = PhoenixTracing.wrap(myCallable);
 * executor.submit(wrapped);
 * </pre>
 *
 * @see <a href="https://issues.apache.org/jira/browse/HBASE-22120">HBASE-22120</a>
 */
public final class PhoenixTracing {

    private static final String INSTRUMENTATION_NAME = "org.apache.phoenix";

    private PhoenixTracing() {
    }

    /**
     * Returns the global tracer for Phoenix. The tracer is obtained from
     * {@link GlobalOpenTelemetry} on each call (the OTel SDK caches it internally, so there is no
     * performance penalty). This avoids issues with eager initialization when the SDK has not been
     * configured yet.
     */
    public static Tracer getTracer() {
        return GlobalOpenTelemetry.getTracer(INSTRUMENTATION_NAME);
    }

    /**
     * Create a {@link SpanKind#INTERNAL} span. This is the default for most Phoenix operations
     * (query compilation, mutation processing, index maintenance, etc.).
     */
    public static Span createSpan(String name) {
        return createSpan(name, SpanKind.INTERNAL);
    }

    /**
     * Create a span with the given {@code kind}. Note that OpenTelemetry expects at most one
     * {@link SpanKind#CLIENT} span and one {@link SpanKind#SERVER} span per traced request, so use
     * this with caution for kinds other than {@link SpanKind#INTERNAL}.
     */
    private static Span createSpan(String name, SpanKind kind) {
        return getTracer().spanBuilder(name).setSpanKind(kind).startSpan();
    }

    /**
     * Create a {@link SpanKind#CLIENT} span. Use this for outgoing RPC calls from Phoenix client
     * to HBase (e.g., scan requests, mutation commits).
     */
    public static Span createClientSpan(String name) {
        return createSpan(name, SpanKind.CLIENT);
    }

    /**
     * Create a {@link SpanKind#SERVER} span from a remote context. Use this for operations that
     * originate from a remote call, such as coprocessor endpoints. The parent context is typically
     * extracted from RPC headers.
     */
    public static Span createRemoteSpan(String name, Context ctx) {
        return getTracer().spanBuilder(name).setParent(ctx).setSpanKind(SpanKind.SERVER)
                .startSpan();
    }

    /**
     * Check if the current span is recording. This can be used to avoid expensive attribute
     * computation when tracing is not active.
     *
     * @return {@code true} if the current span is recording, {@code false} if tracing is off or no
     *         SDK is configured.
     */
    public static boolean isRecording() {
        return Span.current().isRecording();
    }

    /**
     * Record an exception on the given span and set its status to {@link StatusCode#ERROR}.
     */
    public static void setError(Span span, Throwable error) {
        span.recordException(error);
        span.setStatus(StatusCode.ERROR);
    }

    /**
     * Trace the execution of a synchronous operation that may throw. Creates a span, runs the
     * operation, and automatically handles success/error status and span lifecycle.
     *
     * <pre>
     * PhoenixTracing.trace(() -&gt; {
     *     doSomeWork();
     * }, "phoenix.operation.name");
     * </pre>
     */
    public static <T extends Throwable> void trace(ThrowingRunnable<T> runnable, String spanName)
            throws T {
        trace(runnable, () -> createSpan(spanName));
    }

    /**
     * Trace the execution of a synchronous operation that may throw, using a custom span supplier.
     * This allows callers to customize the span (e.g., set attributes before starting).
     */
    public static <T extends Throwable> void trace(ThrowingRunnable<T> runnable,
            Supplier<Span> spanSupplier) throws T {
        Span span = spanSupplier.get();
        try (Scope ignored = span.makeCurrent()) {
            runnable.run();
            span.setStatus(StatusCode.OK);
        } catch (Throwable e) {
            setError(span, e);
            throw e;
        } finally {
            span.end();
        }
    }

    /**
     * Trace the execution of a synchronous operation that returns a value and may throw.
     *
     * <pre>
     * QueryPlan plan = PhoenixTracing.trace(() -&gt; {
     *     return compiler.compile(sql);
     * }, "phoenix.query.compile");
     * </pre>
     */
    public static <R, T extends Throwable> R trace(ThrowingCallable<R, T> callable,
            String spanName) throws T {
        return trace(callable, () -> createSpan(spanName));
    }

    /**
     * Trace the execution of a synchronous operation that returns a value and may throw, using a
     * custom span supplier.
     */
    public static <R, T extends Throwable> R trace(ThrowingCallable<R, T> callable,
            Supplier<Span> spanSupplier) throws T {
        Span span = spanSupplier.get();
        try (Scope ignored = span.makeCurrent()) {
            R result = callable.call();
            span.setStatus(StatusCode.OK);
            return result;
        } catch (Throwable e) {
            setError(span, e);
            throw e;
        } finally {
            span.end();
        }
    }

    /**
     * Trace an asynchronous operation that returns a {@link CompletableFuture}. The span is ended
     * when the future completes (either successfully or with an error).
     *
     * <pre>
     * CompletableFuture&lt;Result&gt; future = PhoenixTracing.tracedFuture(() -&gt; {
     *     return asyncOperation();
     * }, "phoenix.async.operation");
     * </pre>
     */
    public static <T> CompletableFuture<T> tracedFuture(Supplier<CompletableFuture<T>> action,
            String spanName) {
        return tracedFuture(action, () -> createSpan(spanName));
    }

    /**
     * Trace an asynchronous operation that returns a {@link CompletableFuture}, using a custom span
     * supplier.
     */
    public static <T> CompletableFuture<T> tracedFuture(Supplier<CompletableFuture<T>> action,
            Supplier<Span> spanSupplier) {
        Span span = spanSupplier.get();
        try (Scope ignored = span.makeCurrent()) {
            CompletableFuture<T> future = action.get();
            endSpan(future, span);
            return future;
        }
    }

    /**
     * Wrap the provided {@link Runnable} in a traced {@link Runnable}. The returned runnable will
     * create a span, execute the original runnable, and end the span.
     */
    public static Runnable tracedRunnable(Runnable runnable, String spanName) {
        return tracedRunnable(runnable, () -> createSpan(spanName));
    }

    /**
     * Wrap the provided {@link Runnable} in a traced {@link Runnable}, using a custom span
     * supplier.
     */
    public static Runnable tracedRunnable(Runnable runnable, Supplier<Span> spanSupplier) {
        return () -> {
            Span span = spanSupplier.get();
            try (Scope ignored = span.makeCurrent()) {
                runnable.run();
                span.setStatus(StatusCode.OK);
            } finally {
                span.end();
            }
        };
    }

    /**
     * Wrap a {@link Callable} with the current OpenTelemetry context. This ensures that the trace
     * context is propagated when the callable is executed in a different thread (e.g., in a thread
     * pool for parallel scans).
     *
     * <pre>
     * Callable&lt;Result&gt; wrapped = PhoenixTracing.wrap(myCallable);
     * executor.submit(wrapped); // trace context is preserved
     * </pre>
     */
    public static <V> Callable<V> wrap(Callable<V> callable) {
        return Context.current().wrap(callable);
    }

    /**
     * Wrap a {@link Runnable} with the current OpenTelemetry context. This ensures that the trace
     * context is propagated when the runnable is executed in a different thread.
     */
    public static Runnable wrap(Runnable runnable) {
        return Context.current().wrap(runnable);
    }

    /**
     * Finish the span when the given future completes. Sets {@link StatusCode#OK} on success, or
     * records the exception and sets {@link StatusCode#ERROR} on failure.
     */
    private static void endSpan(CompletableFuture<?> future, Span span) {
        future.whenComplete((resp, error) -> {
            if (error != null) {
                setError(span, error);
            } else {
                span.setStatus(StatusCode.OK);
            }
            span.end();
        });
    }

    /**
     * Create a {@link CallWrapper} that wraps a call with an OpenTelemetry span. The span is
     * started in {@code before()} and ended in {@code after()}. This is used with
     * {@link org.apache.phoenix.call.CallRunner#run} to trace operations like commit and rollback.
     */
    public static CallWrapper withTracing(String spanName) {
        return new CallWrapper() {
            private Span span;
            private Scope scope;

            @Override
            public void before() {
                span = createSpan(spanName);
                scope = span.makeCurrent();
            }

            @Override
            public void after() {
                // Note: after() is called from CallRunner's finally block, so we don't
                // know if the operation succeeded or failed. We don't set StatusCode.OK
                // here because the span may represent a failed operation. If the caller
                // needs to record errors, they should do so on Span.current() before
                // the exception propagates.
                try {
                    if (scope != null) {
                        scope.close();
                    }
                } finally {
                    if (span != null) {
                        span.end();
                    }
                }
            }
        };
    }

    /**
     * A {@link Runnable}-like interface that may throw a checked exception.
     *
     * @param <T> the type of {@link Throwable} that can be thrown
     */
    @FunctionalInterface
    public interface ThrowingRunnable<T extends Throwable> {
        void run() throws T;
    }

    /**
     * A {@link Callable}-like interface that may throw a checked exception.
     *
     * @param <R> the result type
     * @param <T> the type of {@link Throwable} that can be thrown
     */
    @FunctionalInterface
    public interface ThrowingCallable<R, T extends Throwable> {
        R call() throws T;
    }
}
