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
 * Baseline distance kernels for query time vector distance evaluation over packed float vectors.
 * Delegates to {@link ScalarDistanceKernel}. A multi-release Java 21 variant overlays this class to
 * dispatch to Panama Vector API implementations when available.
 */
final class DistanceKernel {

  private DistanceKernel() {
  }

  /** Indicates whether query time distances are computed using SIMD kernels. */
  static boolean isSimd() {
    return false;
  }

  static double l2DistanceSquared(byte[] a, int aOff, byte[] b, int bOff, int dim, double bound) {
    return ScalarDistanceKernel.l2DistanceSquared(a, aOff, b, bOff, dim, bound);
  }

  static double dotProduct(byte[] a, int aOff, byte[] b, int bOff, int dim) {
    return ScalarDistanceKernel.dotProduct(a, aOff, b, bOff, dim);
  }

  static double cosineDistance(byte[] a, int aOff, byte[] b, int bOff, int dim) {
    return ScalarDistanceKernel.cosineDistance(a, aOff, b, bOff, dim);
  }
}
