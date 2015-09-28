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

import static org.junit.Assert.assertTrue;

import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.metrics2.MetricsCollector;
import org.apache.hadoop.metrics2.MetricsRecordBuilder;
import org.apache.hadoop.metrics2.MetricsTag;
import org.apache.hadoop.metrics2.lib.DefaultMetricsSystem;
import org.apache.htrace.Span;
import org.apache.htrace.impl.MilliSpan;
import org.junit.BeforeClass;
import org.junit.Test;
import org.mockito.Mockito;

/**
 * Test that the @{link TraceMetricSource} correctly handles different kinds of traces
 */
public class TraceMetricsSourceTest {

  @BeforeClass
  public static void setup() throws Exception{
    DefaultMetricsSystem.setMiniClusterMode(true);
  }

  /**
   * For PHOENIX-1126, Phoenix originally assumed all the annotation values were integers,
   * but HBase writes some strings as well, so we need to be able to handle that too
   */
  @Test
  public void testNonIntegerAnnotations(){
    Span span = getSpan();
    // make sure its less than the length of an integer
    byte[] value = Bytes.toBytes("a");
    byte[] someInt = Bytes.toBytes(1);
    assertTrue(someInt.length >value.length);

    // an annotation that is not an integer
    span.addKVAnnotation(Bytes.toBytes("key"), value);

    // Create the sink and write the span
    TraceMetricSource source = new TraceMetricSource();
    source.receiveSpan(span);
  }

  @Test
  public void testIntegerAnnotations(){
    Span span = getSpan();

    // add annotation through the phoenix interfaces
    TracingUtils.addAnnotation(span, "message", 10);

    TraceMetricSource source = new TraceMetricSource();
    source.receiveSpan(span);
  }

  /**
   * If the source does not write any metrics when there are no spans, i.e. when initialized,
   * then the metrics system will discard the source, so it needs to always emit some metrics.
   */
  @Test
  public void testWritesInfoWhenNoSpans(){
    TraceMetricSource source = new TraceMetricSource();
    MetricsCollector collector = Mockito.mock(MetricsCollector.class);
    MetricsRecordBuilder builder = Mockito.mock(MetricsRecordBuilder.class);
    Mockito.when(collector.addRecord(Mockito.anyString())).thenReturn(builder);

    source.getMetrics(collector, true);

    // verify that we add a record and that the record has some info
    Mockito.verify(collector).addRecord(Mockito.anyString());
    Mockito.verify(builder).add(Mockito.any(MetricsTag.class));
  }

  private Span getSpan(){
    return new MilliSpan("test span", 0, 1 , 2, "pid");
  }
}