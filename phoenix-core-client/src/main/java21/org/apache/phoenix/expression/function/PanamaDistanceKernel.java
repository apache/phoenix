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
 * Hardware-accelerated vector distance computation kernels utilizing the Panama Vector API
 * ({@code jdk.incubator.vector}). Packaged within Java 21 multi-release versions and dynamically
 * loaded when vector incubator support is present at runtime.
 *
 * <p>Vector operations execute across preferred SIMD lane widths with scalar tail loops handling
 * remaining elements.</p>
 */
final class PanamaDistanceKernel {

  private static final VectorSpecies<Float> SPECIES = FloatVector.SPECIES_PREFERRED;

  private PanamaDistanceKernel() {
  }

  static double l2DistanceSquared(float[] a, float[] b, int dim) {
    int i = 0;
    int upperBound = SPECIES.loopBound(dim);
    FloatVector sum = FloatVector.zero(SPECIES);
    for (; i < upperBound; i += SPECIES.length()) {
      FloatVector va = FloatVector.fromArray(SPECIES, a, i);
      FloatVector vb = FloatVector.fromArray(SPECIES, b, i);
      FloatVector diff = va.sub(vb);
      sum = diff.fma(diff, sum);
    }
    double result = sum.reduceLanes(VectorOperators.ADD);
    for (; i < dim; i++) {
      double diff = a[i] - b[i];
      result += diff * diff;
    }
    return result;
  }

  static double dotProduct(float[] a, float[] b, int dim) {
    int i = 0;
    int upperBound = SPECIES.loopBound(dim);
    FloatVector sum = FloatVector.zero(SPECIES);
    for (; i < upperBound; i += SPECIES.length()) {
      FloatVector va = FloatVector.fromArray(SPECIES, a, i);
      FloatVector vb = FloatVector.fromArray(SPECIES, b, i);
      sum = va.fma(vb, sum);
    }
    double result = sum.reduceLanes(VectorOperators.ADD);
    for (; i < dim; i++) {
      result += (double) a[i] * b[i];
    }
    return result;
  }

  static double cosineDistance(float[] a, float[] b, int dim) {
    int i = 0;
    int upperBound = SPECIES.loopBound(dim);
    FloatVector vDot = FloatVector.zero(SPECIES);
    FloatVector vNormA = FloatVector.zero(SPECIES);
    FloatVector vNormB = FloatVector.zero(SPECIES);
    for (; i < upperBound; i += SPECIES.length()) {
      FloatVector va = FloatVector.fromArray(SPECIES, a, i);
      FloatVector vb = FloatVector.fromArray(SPECIES, b, i);
      vDot = va.fma(vb, vDot);
      vNormA = va.fma(va, vNormA);
      vNormB = vb.fma(vb, vNormB);
    }
    double dot = vDot.reduceLanes(VectorOperators.ADD);
    double normA = vNormA.reduceLanes(VectorOperators.ADD);
    double normB = vNormB.reduceLanes(VectorOperators.ADD);
    for (; i < dim; i++) {
      float ai = a[i];
      float bi = b[i];
      dot += (double) ai * bi;
      normA += (double) ai * ai;
      normB += (double) bi * bi;
    }
    double denom = Math.sqrt(normA) * Math.sqrt(normB);
    if (denom == 0.0) {
      return 1.0;
    }
    double cosineSim = dot / denom;
    if (cosineSim > 1.0) {
      cosineSim = 1.0;
    } else if (cosineSim < -1.0) {
      cosineSim = -1.0;
    }
    return 1.0 - cosineSim;
  }
}
