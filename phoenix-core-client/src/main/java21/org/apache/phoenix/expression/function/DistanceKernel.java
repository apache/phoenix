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

import org.apache.phoenix.schema.types.PVectorFloat;

/**
 * Java 21 multi-release implementation of {@code DistanceKernel}. Dispatches to
 * {@link PanamaDistanceKernel} when {@code jdk.incubator.vector} is available, falling back to
 * {@link ScalarDistanceKernel}. Both kernels operate directly on packed vector bytes.
 */
final class DistanceKernel {

  private static final boolean SIMD = isPanamaAvailable();

  private DistanceKernel() {
  }

  static boolean isSimd() {
    return SIMD;
  }

  private static boolean isPanamaAvailable() {
    try {
      Class.forName("jdk.incubator.vector.FloatVector");
      byte[] one = PVectorFloat.INSTANCE.toBytes(new float[] { 1f });
      return PanamaDistanceKernel.dotProduct(one, 0, one, 0, 1) == 1.0;
    } catch (Throwable t) {
      return false;
    }
  }

  static double l2DistanceSquared(byte[] a, int aOff, byte[] b, int bOff, int dim, double bound) {
    return SIMD
      ? PanamaDistanceKernel.l2DistanceSquared(a, aOff, b, bOff, dim, bound)
      : ScalarDistanceKernel.l2DistanceSquared(a, aOff, b, bOff, dim, bound);
  }

  static double dotProduct(byte[] a, int aOff, byte[] b, int bOff, int dim) {
    return SIMD
      ? PanamaDistanceKernel.dotProduct(a, aOff, b, bOff, dim)
      : ScalarDistanceKernel.dotProduct(a, aOff, b, bOff, dim);
  }

  static double cosineDistance(byte[] a, int aOff, byte[] b, int bOff, int dim) {
    return SIMD
      ? PanamaDistanceKernel.cosineDistance(a, aOff, b, bOff, dim)
      : ScalarDistanceKernel.cosineDistance(a, aOff, b, bOff, dim);
  }
}
