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

import org.apache.phoenix.iterate.DelegateResultIterator;
import org.apache.phoenix.iterate.ResultIterator;
import org.apache.phoenix.schema.tuple.Tuple;
import org.apache.htrace.TraceScope;

/**
 * A simple iterator that closes the trace scope when the iterator is closed.
 */
public class TracingIterator extends DelegateResultIterator {

    private TraceScope scope;
    private boolean started;

    /**
     * @param scope a scope with a non-null span
     * @param iterator delegate
     */
    public TracingIterator(TraceScope scope, ResultIterator iterator) {
        super(iterator);
        this.scope = scope;
    }

    @Override
    public void close() throws SQLException {
        scope.close();
        super.close();
    }

    @Override
    public Tuple next() throws SQLException {
        if (!started) {
            scope.getSpan().addTimelineAnnotation("First request completed");
            started = true;
        }
        return super.next();
    }

	@Override
	public String toString() {
		return "TracingIterator [scope=" + scope + ", started=" + started + "]";
	}
}