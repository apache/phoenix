/**
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

import java.io.IOException;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;

import com.google.common.annotations.VisibleForTesting;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.htrace.Span;
import org.apache.htrace.SpanReceiver;
import org.apache.htrace.impl.MilliSpan;
import org.apache.phoenix.metrics.MetricInfo;
import org.apache.phoenix.query.QueryServicesOptions;

/**
 * Sink for request traces ({@link SpanReceiver}) that pushes writes to {@link TraceWriter} in a
 * format that we can more easily consume.
 * <p>
 * <p>
 * Rather than write directly to a phoenix table, we drop it into the metrics queue so we can more
 * cleanly handle it asynchronously.Currently, {@link MilliSpan} submits the span in a synchronized
 * block to all the receivers, which could have a lot of overhead if we are submitting to multiple
 * receivers.
 * <p>
 * The format of the generated metrics is this:
 * <ol>
 * <li>All Metrics from the same span have the same trace id (allowing correlation in the sink)</li>
 * <li>The description of the metric describes what it contains. For instance,
 * <ul>
 * <li>{@link MetricInfo#PARENT} is the id of the parent of this span. (Root span is
 * {@link Span#ROOT_SPAN_ID}).</li>
 * <li>{@link MetricInfo#START} is the start time of the span</li>
 * <li>{@link MetricInfo#END} is the end time of the span</li>
 * </ul>
 * </li>
 * </ol>
 * <p>
 * <i>So why even submit to {@link TraceWriter} if we only have a single source?</i>
 * <p>
 * This allows us to make the updates in batches. We might have spans that finish before other spans
 * (for instance in the same parent). By batching the updates we can lessen the overhead on the
 * client, which is also busy doing 'real' work. <br>
 * This class is custom implementation of metrics queue and handles batch writes to the Phoenix Table
 * via another thread. Batch size and number of threads are configurable.
 * <p>
 */
public class TraceSpanReceiver implements SpanReceiver {

    private static final Log LOG = LogFactory.getLog(TraceSpanReceiver.class);

    private static final int CAPACITY = QueryServicesOptions.withDefaults().getTracingTraceBufferSize();

    private BlockingQueue<Span> spanQueue = null;

    public TraceSpanReceiver() {
        this.spanQueue = new ArrayBlockingQueue<Span>(CAPACITY);
    }

    @Override
    public void receiveSpan(Span span) {
        if (span.getTraceId() != 0 && spanQueue.offer(span)) {
            if (LOG.isTraceEnabled()) {
                LOG.trace("Span buffered to queue " + span.toJson());
            }
        } else if (span.getTraceId() != 0 && LOG.isDebugEnabled()) {
                LOG.debug("Span NOT buffered due to overflow in queue " + span.toJson());
        }
    }

    @Override
    public void close() throws IOException {
        // noop
    }

    boolean isSpanAvailable() {
        return spanQueue.isEmpty();
    }

    Span getSpan() {
        return spanQueue.poll();
    }

    int getNumSpans() {
        return spanQueue.size();
    }
}
