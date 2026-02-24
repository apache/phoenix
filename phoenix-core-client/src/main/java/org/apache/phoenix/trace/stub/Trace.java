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
package org.apache.phoenix.trace.stub;

import java.util.concurrent.Callable;

/**
 * Stub class for HTrace Trace. This is a no-op replacement for
 * {@code org.apache.htrace.Trace} to remove the HTrace dependency.
 * All methods are no-ops that return null/false as appropriate.
 */
public class Trace {

  private Trace() {
  }

  public static TraceScope startSpan(String description) {
    return new TraceScope(null);
  }

  public static TraceScope startSpan(String description, Sampler<?> sampler) {
    return new TraceScope(null);
  }

  public static TraceScope startSpan(String description, Span parent) {
    return new TraceScope(null);
  }

  public static boolean isTracing() {
    return false;
  }

  public static Span currentSpan() {
    return null;
  }

  public static void addReceiver(SpanReceiver receiver) {
    // no-op
  }

  public static void removeReceiver(SpanReceiver receiver) {
    // no-op
  }

  public static Span continueSpan(Span span) {
    return span;
  }

  public static <V> Callable<V> wrap(Callable<V> callable) {
    return callable;
  }
}
