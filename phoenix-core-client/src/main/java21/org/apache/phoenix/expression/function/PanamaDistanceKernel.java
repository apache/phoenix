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

import jdk.incubator.vector.FloatVector;
import jdk.incubator.vector.VectorOperators;
import jdk.incubator.vector.VectorSpecies;

/**
 * SIMD distance kernels leveraging the Panama Vector API over {@code float[]} operands.
 * <p>
 * Accumulates intermediate lane sums in single precision across fixed size blocks of
 * {@link #BLOCK_STRIDES} vector strides before reducing to double precision. Block level reduction
 * bounds floating point accumulation drift relative to {@link ScalarDistanceKernel} while enabling
 * periodic upper bound pruning without per-stride reduction overhead.
 */
final class PanamaDistanceKernel {

  private static final VectorSpecies<Float> SPECIES = FloatVector.SPECIES_PREFERRED;
  static final int BLOCK_STRIDES = 8;

  private PanamaDistanceKernel() {
  }

  static double l2DistanceSquared(float[] a, float[] b, int dim, double bound) {
    int step = SPECIES.length();
    int upper = SPECIES.loopBound(dim);
    int blockLen = step * BLOCK_STRIDES;
    double total = 0.0;
    int i = 0;
    while (i < upper) {
      int blockEnd = Math.min(upper, i + blockLen);
      FloatVector acc = FloatVector.zero(SPECIES);
      for (; i < blockEnd; i += step) {
        FloatVector diff =
          FloatVector.fromArray(SPECIES, a, i).sub(FloatVector.fromArray(SPECIES, b, i));
        acc = diff.fma(diff, acc);
      }
      total += acc.reduceLanes(VectorOperators.ADD);
      if (total > bound) {
        return Double.MAX_VALUE;
      }
    }
    for (; i < dim; i++) {
      double diff = a[i] - b[i];
      total += diff * diff;
      if (total > bound) {
        return Double.MAX_VALUE;
      }
    }
    return total;
  }

  static double dotProduct(float[] a, float[] b, int dim) {
    int step = SPECIES.length();
    int upper = SPECIES.loopBound(dim);
    int blockLen = step * BLOCK_STRIDES;
    double total = 0.0;
    int i = 0;
    while (i < upper) {
      int blockEnd = Math.min(upper, i + blockLen);
      FloatVector acc = FloatVector.zero(SPECIES);
      for (; i < blockEnd; i += step) {
        acc = FloatVector.fromArray(SPECIES, a, i).fma(FloatVector.fromArray(SPECIES, b, i), acc);
      }
      total += acc.reduceLanes(VectorOperators.ADD);
    }
    for (; i < dim; i++) {
      total += (double) a[i] * b[i];
    }
    return total;
  }

  static double cosineDistance(float[] a, float[] b, int dim) {
    int step = SPECIES.length();
    int upper = SPECIES.loopBound(dim);
    int blockLen = step * BLOCK_STRIDES;
    double dot = 0.0;
    double normA = 0.0;
    double normB = 0.0;
    int i = 0;
    while (i < upper) {
      int blockEnd = Math.min(upper, i + blockLen);
      FloatVector vDot = FloatVector.zero(SPECIES);
      FloatVector vNormA = FloatVector.zero(SPECIES);
      FloatVector vNormB = FloatVector.zero(SPECIES);
      for (; i < blockEnd; i += step) {
        FloatVector va = FloatVector.fromArray(SPECIES, a, i);
        FloatVector vb = FloatVector.fromArray(SPECIES, b, i);
        vDot = va.fma(vb, vDot);
        vNormA = va.fma(va, vNormA);
        vNormB = vb.fma(vb, vNormB);
      }
      dot += vDot.reduceLanes(VectorOperators.ADD);
      normA += vNormA.reduceLanes(VectorOperators.ADD);
      normB += vNormB.reduceLanes(VectorOperators.ADD);
    }
    for (; i < dim; i++) {
      double ai = a[i];
      double bi = b[i];
      dot += ai * bi;
      normA += ai * ai;
      normB += bi * bi;
    }
    return ScalarDistanceKernel.cosineDistance(dot, normA, normB);
  }
}
