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

import java.util.Random;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.phoenix.schema.types.PVectorFloat;
import org.junit.Test;

/**
 * Parity tests validating Panama Vector API distance kernel outputs against scalar reference
 * implementations across dimensions and bound thresholds under the {@code java21} test profile.
 */
public class PanamaDistanceKernelParityTest {

  private static final int[] DIMS = { 1, 3, 7, 8, 15, 16, 64, 100, 128, 129, 768, 1536 };
  private static final double TOLERANCE = 1e-5;
  /** Value offsets within a cell are arbitrary, so the kernels must load from any offset. */
  private static final int[] OFFSETS = { 0, 1, 3, 4 };

  private static float[] random(Random rng, int dim) {
    float[] v = new float[dim];
    for (int i = 0; i < dim; i++) {
      v[i] = (rng.nextFloat() - 0.5f) * 20.0f;
    }
    return v;
  }

  /** Packs {@code v} at {@code off} between junk bytes, which a kernel must never read. */
  private static byte[] pack(Random rng, float[] v, int off) {
    byte[] buf = new byte[off + v.length * Bytes.SIZEOF_FLOAT + 64];
    rng.nextBytes(buf);
    PVectorFloat.writeElements(v, buf, off);
    return buf;
  }

  private static void assertClose(String what, double expected, double actual) {
    assertClose(what, expected, actual, Math.abs(expected));
  }

  private static void assertClose(String what, double expected, double actual, double magnitude) {
    assertEquals(what, expected, actual, TOLERANCE * Math.max(1.0, magnitude));
  }

  /**
   * Single precision rounding in a dot product scales with the sum of term magnitudes, not with the
   * result, which cancellation can make arbitrarily small.
   */
  private static double termMagnitude(float[] a, float[] b) {
    double sum = 0.0;
    for (int i = 0; i < a.length; i++) {
      sum += Math.abs((double) a[i] * b[i]);
    }
    return sum;
  }

  @Test
  public void testSimdKernelsAreActive() {
    assertTrue("Panama kernels should be active under the java21 profile",
      VectorDistanceUtil.isSimdEnabled());
  }

  @Test
  public void testKernelParity() {
    Random rng = new Random(42);
    for (int dim : DIMS) {
      for (int trial = 0; trial < 50; trial++) {
        float[] a = random(rng, dim);
        float[] b = random(rng, dim);
        int aOff = OFFSETS[trial % OFFSETS.length];
        int bOff = OFFSETS[(trial / OFFSETS.length) % OFFSETS.length];
        byte[] pa = pack(rng, a, aOff);
        byte[] pb = pack(rng, b, bOff);
        String ctx = "dim=" + dim + " trial=" + trial + " aOff=" + aOff + " bOff=" + bOff;
        assertClose("l2sq " + ctx,
          ScalarDistanceKernel.l2DistanceSquared(a, b, dim, Double.MAX_VALUE),
          PanamaDistanceKernel.l2DistanceSquared(pa, aOff, pb, bOff, dim, Double.MAX_VALUE));
        assertClose("dot " + ctx, ScalarDistanceKernel.dotProduct(a, b, dim),
          PanamaDistanceKernel.dotProduct(pa, aOff, pb, bOff, dim), termMagnitude(a, b));
        assertClose("cosine " + ctx, ScalarDistanceKernel.cosineDistance(a, b, dim),
          PanamaDistanceKernel.cosineDistance(pa, aOff, pb, bOff, dim));
      }
    }
  }

  @Test
  public void testPackedParity() {
    Random rng = new Random(7);
    for (int dim : DIMS) {
      float[] a = random(rng, dim);
      float[] b = random(rng, dim);
      byte[] pa = PVectorFloat.INSTANCE.toBytes(a);
      byte[] pb = PVectorFloat.INSTANCE.toBytes(b);
      String ctx = "dim=" + dim;
      assertClose("l2sq " + ctx, ScalarDistanceKernel.l2DistanceSquared(a, b, dim, Double.MAX_VALUE),
        VectorDistanceUtil.l2DistanceSquaredWithBound(pa, 0, pb, 0, dim, Double.MAX_VALUE));
      assertClose("l2 " + ctx,
        Math.sqrt(ScalarDistanceKernel.l2DistanceSquared(a, b, dim, Double.MAX_VALUE)),
        VectorDistanceUtil.l2DistanceWithBound(pa, 0, pb, 0, dim, Double.MAX_VALUE));
      assertClose("ip " + ctx, -ScalarDistanceKernel.dotProduct(a, b, dim),
        VectorDistanceUtil.innerProductDistanceWithBound(pa, 0, pb, 0, dim, Double.MAX_VALUE),
        termMagnitude(a, b));
      assertClose("cosine " + ctx, ScalarDistanceKernel.cosineDistance(a, b, dim),
        VectorDistanceUtil.cosineDistanceWithBound(pa, 0, pb, 0, dim, Double.MAX_VALUE));
    }
  }

  @Test
  public void testBoundedL2Semantics() {
    Random rng = new Random(11);
    for (int dim : DIMS) {
      float[] a = random(rng, dim);
      float[] b = random(rng, dim);
      byte[] pa = pack(rng, a, 1);
      byte[] pb = pack(rng, b, 0);
      double exact = ScalarDistanceKernel.l2DistanceSquared(a, b, dim, Double.MAX_VALUE);
      assertEquals(Double.MAX_VALUE,
        PanamaDistanceKernel.l2DistanceSquared(pa, 1, pb, 0, dim, exact * 0.5), 0.0);
      assertClose("dim=" + dim, exact,
        PanamaDistanceKernel.l2DistanceSquared(pa, 1, pb, 0, dim, exact * 1.5));
    }
  }
}
