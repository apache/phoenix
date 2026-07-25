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

import io.opentelemetry.api.trace.Span;
import io.opentelemetry.api.trace.StatusCode;
import java.sql.SQLException;
import org.apache.phoenix.iterate.DelegateResultIterator;
import org.apache.phoenix.iterate.ResultIterator;
import org.apache.phoenix.schema.tuple.Tuple;

/**
 * A result iterator that manages an OpenTelemetry span lifecycle. The span is ended when the
 * iterator is closed. Events are added to the span as results are iterated.
 * <p>
 * The span is deliberately not made current.
 * <p>
 * makeCurrent() swaps this thread's Context and returns a Scope that has to restore it on the same
 * thread, but an iterator is closed by whichever thread finishes with it.
 * <p>
 * Nothing is therefore parented to this span. Making it a parent again means attaching a Context
 * around each next() call on the calling thread.
 */
public class TracingIterator extends DelegateResultIterator {

  private final Span span;
  private boolean started;

  /**
   * @param span     the OpenTelemetry span to manage
   * @param iterator delegate iterator
   */
  public TracingIterator(Span span, ResultIterator iterator) {
    super(iterator);
    this.span = span;
  }

  @Override
  public void close() throws SQLException {
    try {
      super.close();
      span.setStatus(StatusCode.OK);
    } finally {
      span.end();
    }
  }

  @Override
  public Tuple next() throws SQLException {
    if (!started) {
      span.addEvent("First request completed");
      started = true;
    }
    return super.next();
  }

  @Override
  public String toString() {
    return "TracingIterator [span=" + span + ", started=" + started + "]";
  }
}
