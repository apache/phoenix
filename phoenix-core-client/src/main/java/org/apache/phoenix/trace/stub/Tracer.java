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

/**
 * Stub class for HTrace Tracer. This is a no-op replacement for
 * {@code org.apache.htrace.Tracer} to remove the HTrace dependency.
 */
public class Tracer {

  private static final Tracer INSTANCE = new Tracer();

  private Tracer() {
  }

  public static Tracer getInstance() {
    return INSTANCE;
  }

  /**
   * Deliver a span. In the original HTrace, this would send the span
   * to all registered SpanReceivers. In this stub, it is a no-op.
   */
  public void deliver(Span span) {
    // no-op
  }
}
