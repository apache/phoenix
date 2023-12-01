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

import static org.apache.phoenix.trace.PhoenixSemanticAttributes.DB_CONNECTION_STRING;
import static org.apache.phoenix.trace.PhoenixSemanticAttributes.DB_NAME;
import static org.apache.phoenix.trace.PhoenixSemanticAttributes.DB_SYSTEM;
import static org.apache.phoenix.trace.PhoenixSemanticAttributes.DB_SYSTEM_VALUE;
import static org.apache.phoenix.trace.PhoenixSemanticAttributes.DB_USER;

import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.function.BiConsumer;
import java.util.function.Supplier;

import org.apache.hadoop.security.UserGroupInformation;
import org.apache.phoenix.jdbc.PhoenixConnection;
import org.apache.phoenix.thirdparty.com.google.common.base.Preconditions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.opentelemetry.api.GlobalOpenTelemetry;
import io.opentelemetry.api.common.AttributeKey;
import io.opentelemetry.api.common.AttributeType;
import io.opentelemetry.api.trace.Span;
import io.opentelemetry.api.trace.SpanBuilder;
import io.opentelemetry.api.trace.SpanKind;
import io.opentelemetry.api.trace.StatusCode;
import io.opentelemetry.api.trace.Tracer;
import io.opentelemetry.context.Context;
import io.opentelemetry.context.Scope;

// This is copied from org.apache.hadoop.hbase.trace.TraceUtil
// We shouldn't use the HBase class directly, as it is @InterfaceAudience.Private
public final class TraceUtil {

    private static final Logger LOGGER = LoggerFactory.getLogger(TraceUtil.class);

    // We could use Boolean, but we're trying to mimic to the Opentracing hint behaviour
    private static final AttributeKey<Long> SAMPLING_PRIORITY_ATTRIBUTE_KEY =
            AttributeKey.longKey("sampling.priority");

    private TraceUtil() {
    }

    public static Tracer getGlobalTracer() {
        // TODO use a real version
        return GlobalOpenTelemetry.getTracer("org.apache.phoenix", "0.0.1");
    }

    private static SpanBuilder createPhoenixSpanBuilder(String name) {
        return getGlobalTracer()
                .spanBuilder(name)
                .setAttribute(DB_SYSTEM, DB_SYSTEM_VALUE);
    }

    private static SpanBuilder setFromConnection(SpanBuilder builder, PhoenixConnection conn) {
        builder.setAttribute(DB_CONNECTION_STRING, conn.getURL());
        try {
            builder.setAttribute(DB_USER, UserGroupInformation.getCurrentUser().getShortUserName());
        } catch (Exception e) {
            // Ignore
        }
        try {
            builder.setAttribute(DB_NAME, conn.getSchema());
        } catch (Exception e) {
            // Ignore
        }
        return builder;
    }

    /**
     * Create a span with the given {@code kind}. Notice that, OpenTelemetry only expects one
     * {@link SpanKind#CLIENT} span and one {@link SpanKind#SERVER} span for a traced request, so
     * use this with caution when you want to create spans with kind other than
     * {@link SpanKind#INTERNAL}.
     */
    public static Span createSpan(PhoenixConnection conn, String name, SpanKind kind, boolean hinted) {
        SpanBuilder builder = createPhoenixSpanBuilder(name);
        builder = setFromConnection(builder, conn);
        builder.setSpanKind(kind);
        if (hinted) {
            // Only has an effect if PhoenixHintableSampler is used  
            builder.setAttribute(SAMPLING_PRIORITY_ATTRIBUTE_KEY, 1L);
        }
        //FIXME only for debugging. Maybe add a property for runtime ?
        StringBuilder sb = new StringBuilder();
        for (StackTraceElement st : Thread.currentThread().getStackTrace()) {
            sb.append(st.toString() + System.lineSeparator());
        }
        builder.setAttribute("create stack trace", sb.toString());
        return builder.startSpan();
    }

    public static Span createSpan(PhoenixConnection conn, String name, boolean hinted) {
        return createSpan(conn, name, SpanKind.INTERNAL, hinted);
    }

    public static Span createSpan(PhoenixConnection conn, String name) {
        return createSpan(conn, name, SpanKind.INTERNAL, false);
    }

    
    /**
     * Create a span without the information taken from Connection.
     * This is to be used in the server side code.
     * 
     * @param name
     * @param kind
     * @return
     */
    private static Span createServerSideSpan(String name, SpanKind kind) {
        SpanBuilder builder = createPhoenixSpanBuilder(name);
        builder.setSpanKind(kind);
        //FIXME only for debugging. Maybe add a property for runtime ?
        StringBuilder sb = new StringBuilder();
        for (StackTraceElement st : Thread.currentThread().getStackTrace()) {
            sb.append(st.toString() + System.lineSeparator());
        }
        builder.setAttribute("create stack trace", sb.toString());
        return builder.startSpan();
    }

    public static Span createServerSideSpan(String name) {
        return createServerSideSpan(name, SpanKind.INTERNAL);
    }
    
    /**
     * Create a span which parent is from remote, i.e, passed through rpc.
     * </p>
     * We will set the kind of the returned span to {@link SpanKind#SERVER}, as this should be the
     * top most span at server side.
     */
//    public static Span createRemoteSpan(String name, Context ctx) {
//        return getGlobalTracer().spanBuilder(name).setParent(ctx).setSpanKind(SpanKind.SERVER)
//                .startSpan();
//    }

    /**
     * Create a span with {@link SpanKind#CLIENT}.
     */
//    public static Span createClientSpan(String name) {
//        return createSpan(name, SpanKind.CLIENT);
//    }

    /**
     * Trace an asynchronous operation for a table.
     */
//    public static <T> CompletableFuture<T> tracedFuture(Supplier<CompletableFuture<T>> action,
//            Supplier<Span> spanSupplier) {
//        Span span = spanSupplier.get();
//        try (Scope ignored = span.makeCurrent()) {
//            CompletableFuture<T> future = action.get();
//            endSpan(future, span);
//            return future;
//        }
//    }

    /**
     * Trace an asynchronous operation.
     */
//    public static <T> CompletableFuture<T> tracedFuture(Supplier<CompletableFuture<T>> action,
//            String spanName) {
//        Span span = createSpan(spanName);
//        try (Scope ignored = span.makeCurrent()) {
//            CompletableFuture<T> future = action.get();
//            endSpan(future, span);
//            return future;
//        }
//    }

    /**
     * Trace an asynchronous operation, and finish the create {@link Span} when all the given
     * {@code futures} are completed.
     */
//    public static <T> List<CompletableFuture<T>> tracedFutures(
//            Supplier<List<CompletableFuture<T>>> action, Supplier<Span> spanSupplier) {
//        Span span = spanSupplier.get();
//        try (Scope ignored = span.makeCurrent()) {
//            List<CompletableFuture<T>> futures = action.get();
//            endSpan(CompletableFuture.allOf(futures.toArray(new CompletableFuture[0])), span);
//            return futures;
//        }
//    }

    public static void setError(Span span, Throwable error) {
        span.recordException(error);
        span.setStatus(StatusCode.ERROR);
    }

    /**
     * Finish the {@code span} when the given {@code future} is completed.
     */
//    private static void endSpan(CompletableFuture<?> future, Span span) {
//        addListener(future, (resp, error) -> {
//            if (error != null) {
//                setError(span, error);
//            } else {
//                span.setStatus(StatusCode.OK);
//            }
//            span.end();
//        });
//    }

    /**
     * Wrap the provided {@code runnable} in a {@link Runnable} that is traced.
     */
//    public static Runnable tracedRunnable(final Runnable runnable, final String spanName) {
//        return tracedRunnable(runnable, () -> createSpan(spanName));
//    }

    /**
     * Wrap the provided {@code runnable} in a {@link Runnable} that is traced.
     */
//    public static Runnable tracedRunnable(final Runnable runnable,
//            final Supplier<Span> spanSupplier) {
//        // N.B. This method name follows the convention of this class, i.e., tracedFuture, rather
//        // than
//        // the convention of the OpenTelemetry classes, i.e., Context#wrap.
//        return () -> {
//            final Span span = spanSupplier.get();
//            try (final Scope ignored = span.makeCurrent()) {
//                runnable.run();
//                span.setStatus(StatusCode.OK);
//            } finally {
//                span.end();
//            }
//        };
//    }

    /**
     * A {@link Runnable} that may also throw.
     * @param <T> the type of {@link Throwable} that can be produced.
     */
//    @FunctionalInterface
//    public interface ThrowingRunnable<T extends Throwable> {
//        void run() throws T;
//    }

    /**
     * Trace the execution of {@code runnable}.
     */
//    public static <T extends Throwable> void trace(final ThrowingRunnable<T> runnable,
//            final String spanName) throws T {
//        trace(runnable, () -> createSpan(spanName));
//    }

    /**
     * Trace the execution of {@code runnable}.
     */
//    public static <T extends Throwable> void trace(final ThrowingRunnable<T> runnable,
//            final Supplier<Span> spanSupplier) throws T {
//        Span span = spanSupplier.get();
//        try (Scope ignored = span.makeCurrent()) {
//            runnable.run();
//            span.setStatus(StatusCode.OK);
//        } catch (Throwable e) {
//            setError(span, e);
//            throw e;
//        } finally {
//            span.end();
//        }
//    }

    /**
     * A {@link Callable} that may also throw.
     * @param <R> the result type of method call.
     * @param <T> the type of {@link Throwable} that can be produced.
     */
//    @FunctionalInterface
//    public interface ThrowingCallable<R, T extends Throwable> {
//        R call() throws T;
//    }

//    public static <R, T extends Throwable> R trace(final ThrowingCallable<R, T> callable,
//            final String spanName) throws T {
//        return trace(callable, () -> createSpan(spanName));
//    }

//    public static <R, T extends Throwable> R trace(final ThrowingCallable<R, T> callable,
//            final Supplier<Span> spanSupplier) throws T {
//        Span span = spanSupplier.get();
//        try (Scope ignored = span.makeCurrent()) {
//            final R ret = callable.call();
//            span.setStatus(StatusCode.OK);
//            return ret;
//        } catch (Throwable e) {
//            setError(span, e);
//            throw e;
//        } finally {
//            span.end();
//        }
//    }

    public static <R> Callable<R> wrap(final Callable<R> callable, String spanName) {
        //FIXME not necessarily server side, we just don't have the PhoenixConnection object here
        return wrap(callable, () -> createServerSideSpan(spanName));
    }

    public static <R> Callable<R> wrap(final Callable<R> callable, final Supplier<Span> spanSupplier) {
        return new Callable<R>() {
            @Override
            public R call() throws Exception {
                Span span = spanSupplier.get();
                try (Scope ignored = span.makeCurrent()) {
                    final R ret = callable.call();
                    span.setStatus(StatusCode.OK);
                    return ret;
                } catch (Throwable e) {
                    setError(span, e);
                    throw e;
                } finally {
                    span.end();
                }
            }
        };
    }

    public static boolean isTraceOn(String traceOption) {
        Preconditions.checkArgument(traceOption != null);
        if(traceOption.equalsIgnoreCase("ON")) return true;
        if(traceOption.equalsIgnoreCase("OFF")) return false;
        else {
            throw new IllegalArgumentException("Unknown tracing option: " + traceOption);
        }
    }

    // These are copied from org.apache.hadoop.hbase.util.FutureUtils to avoid dependence on 
    // HBase IA.Private classes

    /**
     * This is method is used when you just want to add a listener to the given future. We will call
     * {@link CompletableFuture#whenComplete(BiConsumer)} to register the {@code action} to the
     * {@code future}. Ignoring the return value of a Future is considered as a bad practice as it
     * may suppress exceptions thrown from the code that completes the future, and this method will
     * catch all the exception thrown from the {@code action} to catch possible code bugs.
     * <p/>
     * And the error prone check will always report FutureReturnValueIgnored because every method in
     * the {@link CompletableFuture} class will return a new {@link CompletableFuture}, so you
     * always have one future that has not been checked. So we introduce this method and add a
     * suppress warnings annotation here.
     */
    @SuppressWarnings("FutureReturnValueIgnored")
    private static <T> void addListener(CompletableFuture<T> future,
            BiConsumer<? super T, ? super Throwable> action) {
        future.whenComplete((resp, error) -> {
            try {
                // See this post on stack overflow(shorten since the url is too long),
                // https://s.apache.org/completionexception
                // For a chain of CompleableFuture, only the first child CompletableFuture can get
                // the
                // original exception, others will get a CompletionException, which wraps the
                // original
                // exception. So here we unwrap it before passing it to the callback action.
                action.accept(resp, unwrapCompletionException(error));
            } catch (Throwable t) {
                LOGGER.error("Unexpected error caught when processing CompletableFuture", t);
            }
        });
    }

    /**
     * Get the cause of the {@link Throwable} if it is a {@link CompletionException}.
     */
    public static Throwable unwrapCompletionException(Throwable error) {
        if (error instanceof CompletionException) {
            Throwable cause = error.getCause();
            if (cause != null) {
                return cause;
            }
        }
        return error;
    }
}
