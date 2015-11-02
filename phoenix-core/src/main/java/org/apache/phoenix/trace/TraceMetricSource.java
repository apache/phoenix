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

import org.apache.hadoop.hbase.util.Pair;
import org.apache.hadoop.metrics2.*;
import org.apache.hadoop.metrics2.lib.Interns;
import org.apache.phoenix.metrics.MetricInfo;
import org.apache.phoenix.metrics.Metrics;
import org.apache.htrace.HTraceConfiguration;
import org.apache.htrace.Span;
import org.apache.htrace.SpanReceiver;
import org.apache.htrace.TimelineAnnotation;
import org.apache.htrace.impl.MilliSpan;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import static org.apache.phoenix.metrics.MetricInfo.*;

/**
 * Sink for request traces ({@link SpanReceiver}) that pushes writes to {@link MetricsSource} in a
 * format that we can more easily consume.
 * <p>
 * <p>
 * Rather than write directly to a phoenix table, we drop it into the metrics queue so we can more
 * cleanly handle it asyncrhonously.Currently, {@link MilliSpan} submits the span in a synchronized
 * block to all the receivers, which could have a lot of overhead if we are submitting to multiple
 * receivers.
 * <p>
 * The format of the generated metrics is this:
 * <ol>
 *   <li>All Metrics from the same span have the same name (allowing correlation in the sink)</li>
 *   <li>The description of the metric describes what it contains. For instance,
 *   <ul>
 *     <li>{@link MetricInfo#PARENT} is the id of the parent of this span. (Root span is
 *     {@link Span#ROOT_SPAN_ID}).</li>
 *     <li>{@value MetricInfo#START} is the start time of the span</li>
 *     <li>{@value MetricInfo#END} is the end time of the span</li>
 *   </ul></li>
 *   <li>Each span's messages are contained in a {@link MetricsTag} with the same name as above and a
 *   generic counter for the number of messages (to differentiate messages and provide timeline
 *   ordering).</li>
 * </ol>
 * <p>
 * <i>So why even submit to metrics2 framework if we only have a single source?</i>
 * <p>
 * This allows us to make the updates in batches. We might have spans that finish before other spans
 * (for instance in the same parent). By batching the updates we can lessen the overhead on the
 * client, which is also busy doing 'real' work. <br>
 * We could make our own queue and manage batching and filtering and dropping extra metrics, but
 * that starts to get complicated fast (its not as easy as it sounds) so we use metrics2 to abstract
 * out that pipeline and also provides us flexibility to dump metrics to other sources.
 * <p>
 * This is a somewhat rough implementation - we do excessive locking for correctness,
 * rather than trying to make it fast, for the moment.
 */
public class TraceMetricSource implements SpanReceiver, MetricsSource {

  private static final String EMPTY_STRING = "";

  private static final String CONTEXT = "tracing";

  private List<Metric> spans = new ArrayList<Metric>();

  public TraceMetricSource() {

    MetricsSystem manager = Metrics.initialize();

    // Register this instance.
    // For right now, we ignore the MBean registration issues that show up in DEBUG logs. Basically,
    // we need a Jmx MBean compliant name. We'll get to a better name when we want that later
    manager.register(CONTEXT, "Phoenix call tracing", this);
  }

  @Override
  public void receiveSpan(Span span) {
    Metric builder = new Metric(span);
    // add all the metrics for the span
    builder.addCounter(Interns.info(SPAN.traceName, EMPTY_STRING), span.getSpanId());
    builder.addCounter(Interns.info(PARENT.traceName, EMPTY_STRING), span.getParentId());
    builder.addCounter(Interns.info(START.traceName, EMPTY_STRING), span.getStartTimeMillis());
    builder.addCounter(Interns.info(END.traceName, EMPTY_STRING), span.getStopTimeMillis());
    // add the tags to the span. They were written in order received so we mark them as such
    for (TimelineAnnotation ta : span.getTimelineAnnotations()) {
      builder.add(new MetricsTag(Interns.info(TAG.traceName, Long.toString(ta.getTime())), ta
          .getMessage()));
    }

    // add the annotations. We assume they are serialized as strings and integers, but that can
    // change in the future
    Map<byte[], byte[]> annotations = span.getKVAnnotations();
    for (Entry<byte[], byte[]> annotation : annotations.entrySet()) {
      Pair<String, String> val =
          TracingUtils.readAnnotation(annotation.getKey(), annotation.getValue());
      builder.add(new MetricsTag(Interns.info(ANNOTATION.traceName, val.getFirst()), val
          .getSecond()));
    }

    // add the span to the list we care about
    synchronized (this) {
      spans.add(builder);
    }
  }

  @Override
  public void getMetrics(MetricsCollector collector, boolean all) {
    // add a marker record so we know how many spans are used
    // this is also necessary to ensure that we register the metrics source as an MBean (avoiding a
    // runtime warning)
    MetricsRecordBuilder marker = collector.addRecord(TracingUtils.METRICS_MARKER_CONTEXT);
    marker.add(new MetricsTag(new MetricsInfoImpl("stat", "num spans"), Integer
        .toString(spans.size())));

    // actually convert the known spans into metric records as well
    synchronized (this) {
      for (Metric span : spans) {
        MetricsRecordBuilder builder = collector.addRecord(new MetricsInfoImpl(TracingUtils
            .getTraceMetricName(span.id), span.desc));
        builder.setContext(TracingUtils.METRICS_CONTEXT);
        for (Pair<MetricsInfo, Long> metric : span.counters) {
          builder.addCounter(metric.getFirst(), metric.getSecond());
        }
        for (MetricsTag tag : span.tags) {
          builder.add(tag);
        }
      }
      // reset the spans so we don't keep a big chunk of memory around
      spans = new ArrayList<Metric>();
    }
  }

  @Override
  public void close() throws IOException {
    // noop
  }

  private static class Metric {

    List<Pair<MetricsInfo, Long>> counters = new ArrayList<Pair<MetricsInfo, Long>>();
    List<MetricsTag> tags = new ArrayList<MetricsTag>();
    private String id;
    private String desc;

    public Metric(Span span) {
      this.id = Long.toString(span.getTraceId());
      this.desc = span.getDescription();
    }

    /**
     * @param metricsInfoImpl
     * @param startTimeMillis
     */
    public void addCounter(MetricsInfo metricsInfoImpl, long startTimeMillis) {
      counters.add(new Pair<MetricsInfo, Long>(metricsInfoImpl, startTimeMillis));
    }

    /**
     * @param metricsTag
     */
    public void add(MetricsTag metricsTag) {
      tags.add(metricsTag);
    }
  }
}