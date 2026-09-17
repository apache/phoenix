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
 * Vector distance methods for packed float bytes and {@code float[]} arrays.
 * <p>
 * The bounded methods are for query time. They call {@link DistanceKernel}, which can use SIMD. If
 * the distance is more than the bound, they return {@link Double#MAX_VALUE}, and the L2 methods can
 * stop early. Use {@link Double#MAX_VALUE} as the bound for no limit.
 * <p>
 * The unbounded scalar methods never use SIMD, so their results are the same on all platforms.
 * Centroid training and centroid assignment use these methods for this reason.
 */
public final class VectorDistanceUtil {

  private VectorDistanceUtil() {
  }

  /** Returns true if the query time distance kernels use SIMD in this JVM. */
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
