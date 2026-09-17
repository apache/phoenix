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

import org.apache.hadoop.hbase.util.Bytes;

/**
 * Java 21 multi-release implementation of {@code DistanceKernel}. Dispatches to
 * {@link PanamaDistanceKernel} when {@code jdk.incubator.vector} is available, falling back to
 * {@link ScalarDistanceKernel}.
 * <p>
 * Decodes packed big-endian float vectors into thread local float buffers prior to SIMD execution
 * to satisfy Vector API buffer alignment and endianness requirements.
 */
final class DistanceKernel {

  private static final boolean SIMD = isPanamaAvailable();

  private static final ThreadLocal<float[][]> SCRATCH =
    ThreadLocal.withInitial(() -> new float[][] { new float[0], new float[0] });

  private DistanceKernel() {
  }

  static boolean isSimd() {
    return SIMD;
  }

  private static boolean isPanamaAvailable() {
    try {
      Class.forName("jdk.incubator.vector.FloatVector");
      return PanamaDistanceKernel.dotProduct(new float[] { 1f }, new float[] { 1f }, 1) == 1.0;
    } catch (Throwable t) {
      return false;
    }
  }

  private static float[] decode(int slot, byte[] buf, int off, int dim) {
    float[][] scratch = SCRATCH.get();
    float[] out = scratch[slot];
    if (out.length < dim) {
      out = new float[dim];
      scratch[slot] = out;
    }
    for (int i = 0; i < dim; i++) {
      out[i] = Bytes.toFloat(buf, off + i * Bytes.SIZEOF_FLOAT);
    }
    return out;
  }

  static double l2DistanceSquared(byte[] a, int aOff, byte[] b, int bOff, int dim, double bound) {
    if (!SIMD) {
      return ScalarDistanceKernel.l2DistanceSquared(a, aOff, b, bOff, dim, bound);
    }
    return PanamaDistanceKernel.l2DistanceSquared(decode(0, a, aOff, dim), decode(1, b, bOff, dim),
      dim, bound);
  }

  static double dotProduct(byte[] a, int aOff, byte[] b, int bOff, int dim) {
    if (!SIMD) {
      return ScalarDistanceKernel.dotProduct(a, aOff, b, bOff, dim);
    }
    return PanamaDistanceKernel.dotProduct(decode(0, a, aOff, dim), decode(1, b, bOff, dim), dim);
  }

  static double cosineDistance(byte[] a, int aOff, byte[] b, int bOff, int dim) {
    if (!SIMD) {
      return ScalarDistanceKernel.cosineDistance(a, aOff, b, bOff, dim);
    }
    return PanamaDistanceKernel.cosineDistance(decode(0, a, aOff, dim), decode(1, b, bOff, dim),
      dim);
  }
}
