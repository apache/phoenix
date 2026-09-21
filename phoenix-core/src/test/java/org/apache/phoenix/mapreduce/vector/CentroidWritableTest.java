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
package org.apache.phoenix.mapreduce.vector;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

import org.junit.Test;

public class CentroidWritableTest {

  @Test
  public void testAggregationAndCentroidComputation() {
    CentroidWritable cw = new CentroidWritable(0, 3);
    cw.addVector(new float[] { 1.0f, 2.0f, 3.0f }, 0.5);
    cw.addVector(new float[] { 3.0f, 4.0f, 5.0f }, 0.5);

    assertEquals(2, cw.getCount());
    assertEquals(1.0, cw.getDistance(), 1e-6);

    float[] centroid = cw.computeCentroid("L2");
    assertNotNull(centroid);
    assertArrayEquals(new float[] { 2.0f, 3.0f, 4.0f }, centroid, 1e-6f);

    float[] cosineCentroid = cw.computeCentroid("COSINE");
    assertNotNull(cosineCentroid);
    double norm = Math.sqrt(cosineCentroid[0] * cosineCentroid[0]
      + cosineCentroid[1] * cosineCentroid[1] + cosineCentroid[2] * cosineCentroid[2]);
    assertEquals(1.0, norm, 1e-6);
  }

  @Test
  public void testCombinerMergeAndWorstFit() {
    CentroidWritable cw1 = new CentroidWritable(1, new double[] { 2.0, 4.0 }, 1, 1.0);
    CentroidWritable cw2 = new CentroidWritable(1, new double[] { 4.0, 6.0 }, 1, 2.0);

    cw1.add(cw2);
    assertEquals(2, cw1.getCount());
    assertEquals(3.0, cw1.getDistance(), 1e-6);
    assertArrayEquals(new double[] { 6.0, 10.0 }, cw1.getPartialSum(), 1e-6);

    // Centroid key -1 represents worst-fit candidates, where merging retains the candidate with
    // maximum distance
    CentroidWritable wf1 = new CentroidWritable(-1, new double[] { 1.0, 1.0 }, 1, 5.0);
    CentroidWritable wf2 = new CentroidWritable(-1, new double[] { 2.0, 2.0 }, 1, 15.0);

    wf1.add(wf2);
    assertEquals(15.0, wf1.getDistance(), 1e-6);
    assertArrayEquals(new double[] { 2.0, 2.0 }, wf1.getPartialSum(), 1e-6);
  }

  @Test
  public void testWritableRoundTrip() throws Exception {
    CentroidWritable[] testCases =
      new CentroidWritable[] { new CentroidWritable(0, new double[] { 1.0, 2.5, -3.2 }, 15L, 4.75),
        new CentroidWritable(42, new double[] { 0.0, 100.1, -50.5, 3.14159 }, 500L, 123.456),
        new CentroidWritable(-1, new double[] { 5.5, 6.6, 7.7, 8.8 }, 1L, 99.9), // worst-fit
        new CentroidWritable(3, new double[0], 0L, 0.0) // empty partialSum
      };

    for (CentroidWritable original : testCases) {
      org.apache.hadoop.io.DataOutputBuffer dob = new org.apache.hadoop.io.DataOutputBuffer();
      original.write(dob);

      org.apache.hadoop.io.DataInputBuffer dib = new org.apache.hadoop.io.DataInputBuffer();
      dib.reset(dob.getData(), dob.getLength());

      CentroidWritable deserialized = new CentroidWritable();
      deserialized.readFields(dib);

      assertEquals(original.getCentroidId(), deserialized.getCentroidId());
      assertEquals(original.getCount(), deserialized.getCount());
      assertEquals(original.getDistance(), deserialized.getDistance(), 1e-6);
      assertArrayEquals(original.getPartialSum(), deserialized.getPartialSum(), 1e-6);
      assertEquals(original, deserialized);
    }
  }
}
