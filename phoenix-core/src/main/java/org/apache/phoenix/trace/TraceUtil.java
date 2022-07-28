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

import io.opentelemetry.api.GlobalOpenTelemetry;
import io.opentelemetry.api.trace.Span;
import io.opentelemetry.api.trace.Tracer;
import io.opentelemetry.context.Context;
import io.opentelemetry.context.Scope;
import org.apache.phoenix.jdbc.PhoenixConnection;


import java.util.concurrent.Callable;

public final class TraceUtil {

  private static final String INSTRUMENTATION_NAME = "io.opentelemetry.contrib.phoenix";

  private TraceUtil() {
  }

  public static Tracer getGlobalTracer() {
    return GlobalOpenTelemetry.getTracer(INSTRUMENTATION_NAME);
  }

  public static Object withTracing(PhoenixConnection connection, String toString) {
    return null;
  }

  public static class TraceCallable<V> implements Callable<V> {
    private final Callable<V> impl;
    private final String description;
    private final Span parentSpan;

    public TraceCallable(Callable<V> impl,
        String description, Span parentSpan) {
      this.impl = impl;
      if (description == null){
        this.description = Thread.currentThread().getName();
      }
      else {
        this.description = description;
      }
      this.parentSpan = parentSpan;
    }

    @Override
    public V call() throws Exception {
      Span span =
          getGlobalTracer().spanBuilder(description).setParent(Context.current().with(parentSpan))
              .startSpan();
      try (Scope scope = span.makeCurrent()) {
        return impl.call();
      } finally {
        span.end();
      }
    }

    public Callable<V> getImpl() {
      return impl;
    }
  }
}
