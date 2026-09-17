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
package org.apache.phoenix.expression.function;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.util.Arrays;
import java.util.Random;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.phoenix.expression.Expression;
import org.apache.phoenix.expression.LiteralExpression;
import org.apache.phoenix.schema.types.PDouble;
import org.apache.phoenix.schema.types.PVectorFloat;
import org.junit.Test;

public class VectorDistanceUtilTest {

  private static final double DELTA = 1e-5;

  /**
   * Validates distance upper bound pruning and boundary conditions.
   */
  @Test
  public void testEarlyTerminationCorrectness() {
    byte[] b1 = PVectorFloat.INSTANCE.toBytes(new float[] { 3.0f, 0.0f });
    byte[] b2 = PVectorFloat.INSTANCE.toBytes(new float[] { 0.0f, 4.0f });

    assertEquals(Double.MAX_VALUE, VectorDistanceUtil.l2DistanceWithBound(b1, 0, b2, 0, 2, 1.0),
      0.0);
    assertEquals(5.0, VectorDistanceUtil.l2DistanceWithBound(b1, 0, b2, 0, 2, 10.0), DELTA);

    byte[] sqB1 = PVectorFloat.INSTANCE.toBytes(new float[] { 1.0f, 2.0f });
    byte[] sqB2 = PVectorFloat.INSTANCE.toBytes(new float[] { 0.0f, 0.0f });
    assertEquals(Double.MAX_VALUE,
      VectorDistanceUtil.l2DistanceSquaredWithBound(sqB1, 0, sqB2, 0, 2, 1.0), 0.0);
    assertEquals(5.0, VectorDistanceUtil.l2DistanceSquaredWithBound(sqB1, 0, sqB2, 0, 2, 10.0),
      DELTA);

    assertEquals(Double.MAX_VALUE,
      VectorDistanceUtil.l2DistanceSquaredWithBound(b1, 0, b2, 0, 2, 1.0), 0.0);
    assertEquals(25.0, VectorDistanceUtil.l2DistanceSquaredWithBound(b1, 0, b2, 0, 2, 30.0), DELTA);

    // Inner product and cosine evaluate across full vectors before bound filtering
    assertEquals(Double.MAX_VALUE,
      VectorDistanceUtil.innerProductDistanceWithBound(b1, 0, b2, 0, 2, -1.0), 0.0);
    assertEquals(0.0,
      VectorDistanceUtil.innerProductDistanceWithBound(b1, 0, b2, 0, 2, Double.MAX_VALUE), DELTA);
    assertEquals(Double.MAX_VALUE, VectorDistanceUtil.cosineDistanceWithBound(b1, 0, b2, 0, 2, 0.5),
      0.0);
    assertEquals(1.0, VectorDistanceUtil.cosineDistanceWithBound(b1, 0, b2, 0, 2, 1.5), DELTA);
  }

  /**
   * Validates early termination in bounded scalar L2 kernels using truncated operand buffers.
   */
  @Test
  public void testEarlyTerminationStopsReading() {
    int dim = 128;
    int readable = dim / 4;
    float[] v1 = new float[readable];
    Arrays.fill(v1, (float) (5.0 / Math.sqrt(dim)));
    float[] v2 = new float[readable];
    // Running sum exceeds threshold within truncated buffer window
    assertEquals(Double.MAX_VALUE, ScalarDistanceKernel.l2DistanceSquared(v1, v2, dim, 1.0), 0.0);
    byte[] b1 = PVectorFloat.INSTANCE.toBytes(v1);
    byte[] b2 = PVectorFloat.INSTANCE.toBytes(v2);
    assertEquals(Double.MAX_VALUE, ScalarDistanceKernel.l2DistanceSquared(b1, 0, b2, 0, dim, 1.0),
      0.0);
  }

  /**
   * Validates calculation consistency between scalar reference kernels and query-time entry points.
   */
  @Test
  public void testScalarAndPackedAgree() {
    Random rng = new Random(3);
    for (int dim : new int[] { 1, 5, 64, 300 }) {
      float[] a = new float[dim];
      float[] b = new float[dim];
      for (int i = 0; i < dim; i++) {
        a[i] = (rng.nextFloat() - 0.5f) * 20.0f;
        b[i] = (rng.nextFloat() - 0.5f) * 20.0f;
      }
      byte[] pa = PVectorFloat.INSTANCE.toBytes(a);
      byte[] pb = PVectorFloat.INSTANCE.toBytes(b);
      assertClose(VectorDistanceUtil.scalarL2DistanceSquared(a, b, dim),
        VectorDistanceUtil.l2DistanceSquaredWithBound(pa, 0, pb, 0, dim, Double.MAX_VALUE));
      assertClose(VectorDistanceUtil.scalarInnerProductDistance(a, b, dim),
        VectorDistanceUtil.innerProductDistanceWithBound(pa, 0, pb, 0, dim, Double.MAX_VALUE));
      assertClose(VectorDistanceUtil.scalarCosineDistance(a, b, dim),
        VectorDistanceUtil.cosineDistanceWithBound(pa, 0, pb, 0, dim, Double.MAX_VALUE));
    }
  }

  private static void assertClose(double expected, double actual) {
    assertEquals(expected, actual, DELTA * Math.max(1.0, Math.abs(expected)));
  }

  /**
   * Validates bounded distance function evaluation during top-N scan execution.
   */
  @Test
  public void testDistanceFunctionEvaluateWithBound() throws Exception {
    Expression child1 =
      LiteralExpression.newConstant(new float[] { 3.0f, 0.0f }, PVectorFloat.INSTANCE, 2, null);
    Expression child2 =
      LiteralExpression.newConstant(new float[] { 0.0f, 4.0f }, PVectorFloat.INSTANCE, 2, null);
    L2DistanceFunction func = new L2DistanceFunction(Arrays.asList(child1, child2));

    ImmutableBytesWritable ptr = new ImmutableBytesWritable();
    func.setDistanceUpperBound(10.0);
    assertTrue(func.evaluate(null, ptr));
    assertEquals(5.0, ((Number) PDouble.INSTANCE.toObject(ptr)).doubleValue(), DELTA);

    func.setDistanceUpperBound(1.0);
    assertTrue(func.evaluate(null, ptr));
    assertEquals(Double.MAX_VALUE, ((Number) PDouble.INSTANCE.toObject(ptr)).doubleValue(), 0.0);
  }
}
