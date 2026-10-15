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
import java.util.List;
import java.util.Random;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.phoenix.expression.BaseTerminalExpression;
import org.apache.phoenix.expression.Expression;
import org.apache.phoenix.expression.LiteralExpression;
import org.apache.phoenix.expression.visitor.ExpressionVisitor;
import org.apache.phoenix.schema.SortOrder;
import org.apache.phoenix.schema.tuple.Tuple;
import org.apache.phoenix.schema.types.PDataType;
import org.apache.phoenix.schema.types.PDouble;
import org.apache.phoenix.schema.types.PVectorDouble;
import org.apache.phoenix.schema.types.PVectorFloat;
import org.junit.Test;

public class VectorDistanceUtilTest {

  private static final double DELTA = 1e-5;

  /**
   * Checks that each bounded method returns Double.MAX_VALUE above the bound and the exact distance
   * at or below it.
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

    // Inner product and cosine compute the full distance and then apply the bound
    assertEquals(Double.MAX_VALUE,
      VectorDistanceUtil.innerProductDistanceWithBound(b1, 0, b2, 0, 2, -1.0), 0.0);
    assertEquals(0.0,
      VectorDistanceUtil.innerProductDistanceWithBound(b1, 0, b2, 0, 2, Double.MAX_VALUE), DELTA);
    assertEquals(Double.MAX_VALUE, VectorDistanceUtil.cosineDistanceWithBound(b1, 0, b2, 0, 2, 0.5),
      0.0);
    assertEquals(1.0, VectorDistanceUtil.cosineDistanceWithBound(b1, 0, b2, 0, 2, 1.5), DELTA);
  }

  /**
   * Checks that the bounded scalar L2 kernels stop early. The operands are shorter than the given
   * dimension, so a kernel that does not stop early fails with an exception.
   */
  @Test
  public void testEarlyTerminationStopsReading() {
    int dim = 128;
    int readable = dim / 4;
    float[] v1 = new float[readable];
    Arrays.fill(v1, (float) (5.0 / Math.sqrt(dim)));
    float[] v2 = new float[readable];
    // The partial sum passes the bound before the end of the short operands
    assertEquals(Double.MAX_VALUE, ScalarDistanceKernel.l2DistanceSquared(v1, v2, dim, 1.0), 0.0);
    byte[] b1 = PVectorFloat.INSTANCE.toBytes(v1);
    byte[] b2 = PVectorFloat.INSTANCE.toBytes(v2);
    assertEquals(Double.MAX_VALUE, ScalarDistanceKernel.l2DistanceSquared(b1, 0, b2, 0, dim, 1.0),
      0.0);
  }

  /**
   * Checks that the scalar reference methods and the query time methods give the same distances.
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
   * Checks that a distance function applies the upper bound that a top-N scan sets on it.
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

  /**
   * A vector operand is a cell value at a random offset inside a larger buffer. Each distance
   * function must read only the operand bytes, for float, double, and mixed precision operands in
   * each sort order. The result must agree with a double precision reference from this test.
   */
  @Test
  public void testDistanceFunctionsReadOperandsAtOffsets() {
    Random rng = new Random(17);
    SortOrder[] orders = { SortOrder.ASC, SortOrder.DESC };
    for (int dim : new int[] { 1, 7, 64, 129 }) {
      for (int trial = 0; trial < 12; trial++) {
        float[] a = new float[dim];
        float[] b = new float[dim];
        for (int i = 0; i < dim; i++) {
          a[i] = (rng.nextFloat() - 0.5f) * 20.0f;
          b[i] = (rng.nextFloat() - 0.5f) * 20.0f;
        }
        boolean aIsDouble = trial % 3 == 1;
        boolean bIsDouble = trial % 3 != 0;
        SortOrder aOrder = orders[(trial / 3) % 2];
        SortOrder bOrder = orders[(trial / 6) % 2];
        List<Expression> args =
          Arrays.asList(new OffsetVectorExpression(rng, a, aIsDouble, aOrder, 1 + trial % 5),
            new OffsetVectorExpression(rng, b, bIsDouble, bOrder, 3 + trial % 7));

        double sumSq = 0.0;
        double dot = 0.0;
        double normA = 0.0;
        double normB = 0.0;
        double terms = 0.0;
        for (int i = 0; i < dim; i++) {
          double diff = (double) a[i] - b[i];
          sumSq += diff * diff;
          dot += (double) a[i] * b[i];
          normA += (double) a[i] * a[i];
          normB += (double) b[i] * b[i];
          terms += Math.abs((double) a[i] * b[i]);
        }
        String ctx = "dim=" + dim + " trial=" + trial;
        assertNear(ctx, sumSq, evaluate(new L2DistanceSquaredFunction(args)), sumSq);
        assertNear(ctx, Math.sqrt(sumSq), evaluate(new L2DistanceFunction(args)), Math.sqrt(sumSq));
        assertNear(ctx, -dot, evaluate(new InnerProductDistanceFunction(args)), terms);
        assertNear(ctx, 1.0 - dot / (Math.sqrt(normA) * Math.sqrt(normB)),
          evaluate(new CosineDistanceFunction(args)), terms / Math.sqrt(normA * normB));
      }
    }
  }

  private static double evaluate(DistanceFunction function) {
    ImmutableBytesWritable ptr = new ImmutableBytesWritable();
    assertTrue(function.evaluate(null, ptr));
    return ((Number) PDouble.INSTANCE.toObject(ptr)).doubleValue();
  }

  private static void assertNear(String ctx, double expected, double actual, double magnitude) {
    assertEquals(ctx, expected, actual, 1e-4 * Math.max(1.0, magnitude));
  }

  /**
   * A vector operand encoded between random bytes. A distance function must not read those bytes.
   */
  private static class OffsetVectorExpression extends BaseTerminalExpression {
    private final byte[] buf;
    private final int offset;
    private final int length;
    private final boolean isDouble;
    private final SortOrder sortOrder;
    private final int dim;

    OffsetVectorExpression(Random rng, float[] v, boolean isDouble, SortOrder sortOrder,
      int offset) {
      this.isDouble = isDouble;
      this.sortOrder = sortOrder;
      this.offset = offset;
      this.dim = v.length;
      this.length = v.length * (isDouble ? Bytes.SIZEOF_DOUBLE : Bytes.SIZEOF_FLOAT);
      this.buf = new byte[offset + length + 16];
      rng.nextBytes(buf);
      byte[] encoded = isDouble
        ? PVectorDouble.INSTANCE.toBytes(toDoubles(v), sortOrder)
        : PVectorFloat.INSTANCE.toBytes(v, sortOrder);
      System.arraycopy(encoded, 0, buf, offset, length);
    }

    private static double[] toDoubles(float[] v) {
      double[] d = new double[v.length];
      for (int i = 0; i < v.length; i++) {
        d[i] = v[i];
      }
      return d;
    }

    @Override
    public boolean evaluate(Tuple tuple, ImmutableBytesWritable ptr) {
      ptr.set(buf, offset, length);
      return true;
    }

    @Override
    public PDataType getDataType() {
      return isDouble ? PVectorDouble.INSTANCE : PVectorFloat.INSTANCE;
    }

    @Override
    public Integer getMaxLength() {
      return dim;
    }

    @Override
    public SortOrder getSortOrder() {
      return sortOrder;
    }

    @Override
    public <T> T accept(ExpressionVisitor<T> visitor) {
      return null;
    }
  }
}
