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
import org.apache.htrace.Span;
import org.apache.htrace.Trace;
import org.apache.htrace.Tracer;
import org.apache.htrace.impl.MilliSpan;
import org.junit.BeforeClass;
import org.junit.Test;

/**
 * Test that the @{link TraceSpanReceiver} correctly handles different kinds of traces
 */
public class TraceSpanReceiverTest {

  @BeforeClass
  public static void setup() throws Exception{
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
    assertTrue(someInt.length > value.length);

    // an annotation that is not an integer
    span.addKVAnnotation(Bytes.toBytes("key"), value);

    // Create the sink and write the span
    TraceSpanReceiver source = new TraceSpanReceiver();
    Trace.addReceiver(source);

    Tracer.getInstance().deliver(span);

    assertTrue(source.getNumSpans() == 1);
  }

  @Test
  public void testIntegerAnnotations(){
    Span span = getSpan();

    // add annotation through the phoenix interfaces
    TracingUtils.addAnnotation(span, "message", 10);

    TraceSpanReceiver source = new TraceSpanReceiver();
    Trace.addReceiver(source);

    Tracer.getInstance().deliver(span);
    assertTrue(source.getNumSpans() == 1);
  }

  private Span getSpan(){
    // Spans with Trace Id as 0 will be rejected (See PHOENIX-3767 for details)
    return new MilliSpan("test span", 1, 1 , 2, "pid");
  }
}
