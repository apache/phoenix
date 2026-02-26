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
import io.opentelemetry.context.Scope;
import java.sql.SQLException;
import org.apache.phoenix.iterate.DelegateResultIterator;
import org.apache.phoenix.iterate.ResultIterator;
import org.apache.phoenix.schema.tuple.Tuple;

/**
 * A result iterator that manages an OpenTelemetry span lifecycle. The span is ended when the
 * iterator is closed. Events are added to the span as results are iterated.
 */
public class TracingIterator extends DelegateResultIterator {

    private final Span span;
    private final Scope scope;
    private boolean started;

    /**
     * @param span     the OpenTelemetry span to manage
     * @param scope    the scope that makes the span current
     * @param iterator delegate iterator
     */
    public TracingIterator(Span span, Scope scope, ResultIterator iterator) {
        super(iterator);
        this.span = span;
        this.scope = scope;
    }

    @Override
    public void close() throws SQLException {
        try {
            span.setStatus(StatusCode.OK);
        } finally {
            try {
                scope.close();
            } finally {
                span.end();
            }
        }
        super.close();
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
