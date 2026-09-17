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

/**
 * Utility methods for vector distance computations over packed byte buffers and array structures.
 * <p>
 * Bounded query time methods delegate to {@link DistanceKernel}, supporting SIMD acceleration and
 * early termination when candidate distances exceed the provided threshold. Pass
 * {@link Double#MAX_VALUE} for unbounded evaluation.
 * <p>
 * Unbounded scalar methods bypass SIMD execution paths to guarantee deterministic floating point
 * results across platforms during centroid training and assignment.
 */
public final class VectorDistanceUtil {

  private VectorDistanceUtil() {
  }

  /** Indicates whether SIMD distance kernels are active in the current JVM runtime. */
  public static boolean isSimdEnabled() {
    return DistanceKernel.isSimd();
  }

  public static double scalarL2DistanceSquared(float[] a, float[] b, int dim) {
    return ScalarDistanceKernel.l2DistanceSquared(a, b, dim, Double.MAX_VALUE);
  }

  public static double scalarInnerProductDistance(float[] a, float[] b, int dim) {
    return -ScalarDistanceKernel.dotProduct(a, b, dim);
  }

  public static double scalarCosineDistance(float[] a, float[] b, int dim) {
    return ScalarDistanceKernel.cosineDistance(a, b, dim);
  }

  public static double l2DistanceSquaredWithBound(byte[] a, int aOff, byte[] b, int bOff, int dim,
    double bound) {
    return DistanceKernel.l2DistanceSquared(a, aOff, b, bOff, dim, bound);
  }

  public static double l2DistanceWithBound(byte[] a, int aOff, byte[] b, int bOff, int dim,
    double bound) {
    double boundSq = bound == Double.MAX_VALUE ? Double.MAX_VALUE : bound * bound;
    double sq = DistanceKernel.l2DistanceSquared(a, aOff, b, bOff, dim, boundSq);
    return sq == Double.MAX_VALUE ? Double.MAX_VALUE : applyBound(Math.sqrt(sq), bound);
  }

  public static double innerProductDistanceWithBound(byte[] a, int aOff, byte[] b, int bOff,
    int dim, double bound) {
    return applyBound(-DistanceKernel.dotProduct(a, aOff, b, bOff, dim), bound);
  }

  public static double cosineDistanceWithBound(byte[] a, int aOff, byte[] b, int bOff, int dim,
    double bound) {
    return applyBound(DistanceKernel.cosineDistance(a, aOff, b, bOff, dim), bound);
  }

  static double applyBound(double distance, double bound) {
    return distance > bound ? Double.MAX_VALUE : distance;
  }
}
