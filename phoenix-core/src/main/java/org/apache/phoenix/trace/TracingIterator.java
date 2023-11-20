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

import java.sql.SQLException;
import java.util.List;

import org.apache.phoenix.iterate.DelegateResultIterator;
import org.apache.phoenix.iterate.ResultIterator;
import org.apache.phoenix.schema.tuple.Tuple;
import org.apache.phoenix.thirdparty.com.google.common.collect.Lists;

import io.opentelemetry.api.trace.Span;
import io.opentelemetry.api.trace.StatusCode;
import io.opentelemetry.context.Scope;

/**
 * A simple iterator that closes the Span when the iterator is closed.
 */
public class TracingIterator extends DelegateResultIterator {

    private final Span span;
    private boolean started;

    /**
     * @param scope a scope with a non-null span
     * @param iterator delegate
     */
    public TracingIterator(ResultIterator iterator) {
        super(iterator);
        //FIXME this is not sever side, we just can't access the PhoenixConnection from here 
        span = TraceUtil.createServerSideSpan("Creating Iterator for scan: " + iterator);
        span.setAttribute("plan", String.join("\n", getPlanSteps(iterator)));
        //FIXME where to set the span attributes ?
    }

    private List<String> getPlanSteps(ResultIterator iterator) {
        List<String> planSteps = Lists.newArrayListWithExpectedSize(5);
        iterator.explain(planSteps);
        return planSteps;
    }

    @Override
    public void close() throws SQLException {
        // We start spans in the parent first, so we must close parent last.
        try (Scope ignored = span.makeCurrent()) {
            super.close();
        } catch (Exception e) {
            TraceUtil.setError(span, e);
            throw e;
        } finally {
            span.end();
        }
    }

    @Override
    public Tuple next() throws SQLException {
        // FIXME How much of a performance hit is this ?
        try (Scope ignored = span.makeCurrent()) {
            if (!started) {
                span.addEvent("First request completed");
                //TODO should we set OK even if next() is never called ? Does it matter ?
                span.setStatus(StatusCode.OK);
                started = true;
            }
            try {
                return super.next();
            } catch (SQLException e) {
                TraceUtil.setError(span, e);
                throw e;
            }
        }
    }

	@Override
	public String toString() {
		return "TracingIterator [span=" + span + ", started=" + started + "]";
	}
}