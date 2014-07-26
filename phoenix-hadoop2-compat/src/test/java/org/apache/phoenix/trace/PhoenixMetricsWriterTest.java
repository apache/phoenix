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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.util.List;

import org.apache.hadoop.metrics2.AbstractMetric;
import org.apache.hadoop.metrics2.MetricsInfo;
import org.apache.hadoop.metrics2.MetricsRecord;
import org.apache.hadoop.metrics2.MetricsTag;
import org.apache.hadoop.metrics2.impl.ExposedMetricCounterLong;
import org.apache.hadoop.metrics2.impl.ExposedMetricsRecordImpl;
import org.apache.hadoop.metrics2.lib.ExposedMetricsInfoImpl;
import org.apache.phoenix.metrics.MetricInfo;
import org.apache.phoenix.metrics.MetricsWriter;
import org.apache.phoenix.metrics.PhoenixAbstractMetric;
import org.apache.phoenix.metrics.PhoenixMetricTag;
import org.apache.phoenix.metrics.PhoenixMetricsRecord;
import org.junit.Test;
import org.mockito.Mockito;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;

import com.google.common.collect.Lists;

/**
 * Test that we correctly convert between hadoop2 metrics2 and generic phoenix metrics.
 */
public class PhoenixMetricsWriterTest {

  @Test
  public void testTranslation() throws Exception {
    // hook up a sink we can test
    MetricsWriter mockSink = Mockito.mock(MetricsWriter.class);

    // writer that will translate to the sink (specific to hadoop version used)
    PhoenixMetricsWriter writer = new PhoenixMetricsWriter();
    writer.setWriterForTesting(mockSink);

    // create a simple metrics record
    final long traceid = 987654;
    MetricsInfo info = new ExposedMetricsInfoImpl(TracingCompat.getTraceMetricName(traceid),
        "Some generic trace");
    // setup some metrics for the span
    long spanid = 10;
    AbstractMetric span = new ExposedMetricCounterLong(new ExposedMetricsInfoImpl(
        MetricInfo.SPAN.traceName, ""), spanid);
    long parentid = 11;
    AbstractMetric parent = new ExposedMetricCounterLong(new ExposedMetricsInfoImpl(
        MetricInfo.PARENT.traceName, ""), parentid);
    long startTime = 12;
    AbstractMetric start = new ExposedMetricCounterLong(new ExposedMetricsInfoImpl(
        MetricInfo.START.traceName, ""), startTime);
    long endTime = 13;
    AbstractMetric end = new ExposedMetricCounterLong(new ExposedMetricsInfoImpl(
        MetricInfo.END.traceName, ""), endTime);
    final List<AbstractMetric> metrics = Lists.newArrayList(span, parent, start, end);

    // create an annotation as well
    String annotation = "test annotation for a span";
    MetricsTag tag = new MetricsTag(
        new ExposedMetricsInfoImpl(MetricInfo.ANNOTATION.traceName, "0"), annotation);
    String hostnameValue = "host-name.value";
    MetricsTag hostname = new MetricsTag(new ExposedMetricsInfoImpl(MetricInfo.HOSTNAME.traceName,
        ""), hostnameValue);
    final List<MetricsTag> tags = Lists.newArrayList(hostname, tag);

    MetricsRecord record = new ExposedMetricsRecordImpl(info, System.currentTimeMillis(), tags,
        metrics);

    // setup the mocking/validation for the sink
    Mockito.doAnswer(new Answer<Void>() {

      @Override
      public Void answer(InvocationOnMock invocation) throws Throwable {
        PhoenixMetricsRecord record = (PhoenixMetricsRecord) invocation.getArguments()[0];
        //validate that we got the right fields in the record
        assertEquals("phoenix.987654", record.name());
        assertEquals("Some generic trace", record.description());
        int count = 0;
        for (PhoenixAbstractMetric metric : record.metrics()) {
          count++;
          //find the matching metric in the list
          boolean found = false;
          for(AbstractMetric expected : metrics){
            if(expected.name().equals(metric.getName())){
              found = true;
              // make sure the rest of the info matches
              assertEquals("Metric value mismatch", expected.value(), metric.value());
            }
          }
          assertTrue("Didn't find an expected metric to match "+metric, found);
        }
        assertEquals("Number of metrics is received is wrong", metrics.size(), count);

        count = 0;
        for (PhoenixMetricTag tag : record.tags()) {
          count++;
          // find the matching metric in the list
          boolean found = false;
          for (MetricsTag expected : tags) {
            if (expected.name().equals(tag.name())) {
              found = true;
              // make sure the rest of the info matches
              assertEquals("Tag value mismatch", expected.value(), tag.value());
              assertEquals("Tag description mismatch", expected.description(), tag.description());
            }
          }
          assertTrue("Didn't find an expected metric to match " + tag, found);
        }
        assertEquals("Number of tags is received is wrong", tags.size(), count);
        return null;
      }

    }).when(mockSink).addMetrics(Mockito.any(PhoenixMetricsRecord.class));

    // actually do the update
    writer.putMetrics(record);
    writer.flush();

    Mockito.verify(mockSink).addMetrics(Mockito.any(PhoenixMetricsRecord.class));
    Mockito.verify(mockSink).flush();
  }
}