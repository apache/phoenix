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
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.phoenix.expression.Expression;
import org.apache.phoenix.expression.LiteralExpression;
import org.apache.phoenix.schema.types.PDouble;
import org.apache.phoenix.schema.types.PVectorFloat;
import org.junit.Test;

public class VectorDistanceUtilTest {

  private static final double DELTA = 1e-5;

  private static float[] generateRandomVector(int dim, Random rng) {
    float[] v = new float[dim];
    for (int i = 0; i < dim; i++) {
      v[i] = (rng.nextFloat() - 0.5f) * 20.0f;
    }
    return v;
  }

  /**
   * Tests early-termination boundary conditions when the distance upper bound is exceeded or met.
   */
  @Test
  public void testEarlyTerminationCorrectness() {
    float[] v1 = new float[] { 3.0f, 0.0f };
    float[] v2 = new float[] { 0.0f, 4.0f };
    byte[] b1 = PVectorFloat.INSTANCE.toBytes(v1);
    byte[] b2 = PVectorFloat.INSTANCE.toBytes(v2);

    // Verify L2 distance bounded pruning
    double distBound1 = VectorDistanceUtil.l2DistanceWithBound(b1, 0, b2, 0, 2, 1.0);
    assertEquals(Double.MAX_VALUE, distBound1, 0.0);

    double distBound10 = VectorDistanceUtil.l2DistanceWithBound(b1, 0, b2, 0, 2, 10.0);
    assertEquals(5.0, distBound10, DELTA);

    // Verify squared L2 distance bounded pruning
    float[] sqV1 = new float[] { 1.0f, 2.0f };
    float[] sqV2 = new float[] { 0.0f, 0.0f };
    byte[] sqB1 = PVectorFloat.INSTANCE.toBytes(sqV1);
    byte[] sqB2 = PVectorFloat.INSTANCE.toBytes(sqV2);

    double sqBound1 = VectorDistanceUtil.l2DistanceSquaredWithBound(sqB1, 0, sqB2, 0, 2, 1.0);
    assertEquals(Double.MAX_VALUE, sqBound1, 0.0);

    double sqBound10 = VectorDistanceUtil.l2DistanceSquaredWithBound(sqB1, 0, sqB2, 0, 2, 10.0);
    assertEquals(5.0, sqBound10, DELTA);

    double sq25Bound1 = VectorDistanceUtil.l2DistanceSquaredWithBound(b1, 0, b2, 0, 2, 1.0);
    assertEquals(Double.MAX_VALUE, sq25Bound1, 0.0);
    double sq25Bound30 = VectorDistanceUtil.l2DistanceSquaredWithBound(b1, 0, b2, 0, 2, 30.0);
    assertEquals(25.0, sq25Bound30, DELTA);
  }

  /**
   * Tests early-termination pruning efficiency by verifying that element evaluation terminates
   * before scanning the full vector dimension when the bound is exceeded.
   */
  @Test
  public void testEarlyTerminationEfficiency() {
    int dim = 128;
    float diff = (float) (5.0 / Math.sqrt(dim));
    float[] v1 = new float[dim];
    float[] v2 = new float[dim];
    for (int i = 0; i < dim; i++) {
      v1[i] = diff;
      v2[i] = 0.0f;
    }

    double trueDist = VectorDistanceUtil.scalarL2Distance(v1, v2, dim);
    assertEquals(5.0, trueDist, DELTA);

    AtomicInteger scalarElementsProcessed = new AtomicInteger();
    double scalarResult = VectorDistanceUtil.l2DistanceSquaredWithBound(v1, v2, dim, 1.0,
      count -> scalarElementsProcessed.set(count));

    assertEquals(Double.MAX_VALUE, scalarResult, 0.0);
    int scalarCount = scalarElementsProcessed.get();
    assertTrue("Expected fewer than 25% of elements processed, but got " + scalarCount + " of "
      + dim + " (" + (scalarCount * 100.0 / dim) + "%)", scalarCount < (dim * 0.25));

    byte[] b1 = PVectorFloat.INSTANCE.toBytes(v1);
    byte[] b2 = PVectorFloat.INSTANCE.toBytes(v2);
    AtomicInteger bytesProcessed = new AtomicInteger();
    double bytesResult = VectorDistanceUtil.l2DistanceSquaredWithBound(b1, 0, b2, 0, dim, 1.0,
      count -> bytesProcessed.set(count));
    assertEquals(Double.MAX_VALUE, bytesResult, 0.0);
    assertTrue(bytesProcessed.get() < (dim * 0.25));
  }

  /**
   * Tests DistanceFunction evaluation with an explicit distance upper bound.
   */
  @Test
  public void testDistanceFunctionEvaluateWithBound() throws Exception {
    float[] v1 = new float[] { 3.0f, 0.0f };
    float[] v2 = new float[] { 0.0f, 4.0f };
    Expression child1 = LiteralExpression.newConstant(v1, PVectorFloat.INSTANCE, 2, null);
    Expression child2 = LiteralExpression.newConstant(v2, PVectorFloat.INSTANCE, 2, null);

    L2DistanceFunction func = new L2DistanceFunction(Arrays.asList(child1, child2));

    ImmutableBytesWritable ptr = new ImmutableBytesWritable();
    assertTrue(func.evaluate(null, ptr, 10.0));
    double val1 = ((Number) PDouble.INSTANCE.toObject(ptr)).doubleValue();
    assertEquals(5.0, val1, DELTA);

    assertTrue(func.evaluate(null, ptr, 1.0));
    double val2 = ((Number) PDouble.INSTANCE.toObject(ptr)).doubleValue();
    assertEquals(Double.MAX_VALUE, val2, 0.0);
  }
}
