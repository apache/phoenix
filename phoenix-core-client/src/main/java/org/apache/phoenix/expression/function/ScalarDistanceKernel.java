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
 * Reference scalar distance kernels for packed float bytes and {@code float[]} arrays. The kernels
 * add the terms in double precision and in element order, so the results are the same on all
 * platforms.
 */
final class ScalarDistanceKernel {

  private ScalarDistanceKernel() {
  }

  private static float read(byte[] buf, int off, int i) {
    return Bytes.toFloat(buf, off + i * Bytes.SIZEOF_FLOAT);
  }

  /**
   * Computes the squared Euclidean distance. If the partial sum becomes more than {@code bound},
   * the method stops and returns {@link Double#MAX_VALUE}.
   */
  static double l2DistanceSquared(byte[] a, int aOff, byte[] b, int bOff, int dim, double bound) {
    double sum = 0.0;
    for (int i = 0; i < dim; i++) {
      double diff = read(a, aOff, i) - read(b, bOff, i);
      sum += diff * diff;
      if (sum > bound) {
        return Double.MAX_VALUE;
      }
    }
    return sum;
  }

  static double l2DistanceSquared(float[] a, float[] b, int dim, double bound) {
    double sum = 0.0;
    for (int i = 0; i < dim; i++) {
      double diff = a[i] - b[i];
      sum += diff * diff;
      if (sum > bound) {
        return Double.MAX_VALUE;
      }
    }
    return sum;
  }

  static double dotProduct(byte[] a, int aOff, byte[] b, int bOff, int dim) {
    double dot = 0.0;
    for (int i = 0; i < dim; i++) {
      dot += (double) read(a, aOff, i) * read(b, bOff, i);
    }
    return dot;
  }

  static double dotProduct(float[] a, float[] b, int dim) {
    double dot = 0.0;
    for (int i = 0; i < dim; i++) {
      dot += (double) a[i] * b[i];
    }
    return dot;
  }

  static double cosineDistance(byte[] a, int aOff, byte[] b, int bOff, int dim) {
    double dot = 0.0;
    double normA = 0.0;
    double normB = 0.0;
    for (int i = 0; i < dim; i++) {
      double ai = read(a, aOff, i);
      double bi = read(b, bOff, i);
      dot += ai * bi;
      normA += ai * ai;
      normB += bi * bi;
    }
    return cosineDistance(dot, normA, normB);
  }

  static double cosineDistance(float[] a, float[] b, int dim) {
    double dot = 0.0;
    double normA = 0.0;
    double normB = 0.0;
    for (int i = 0; i < dim; i++) {
      double ai = a[i];
      double bi = b[i];
      dot += ai * bi;
      normA += ai * ai;
      normB += bi * bi;
    }
    return cosineDistance(dot, normA, normB);
  }

  /**
   * Computes the cosine distance from a dot product and two squared norms. If an operand has zero
   * magnitude, the result is 1.0, which is the distance for orthogonal vectors. The similarity is
   * clamped to the range -1 to 1, so rounding errors cannot move the result out of 0 to 2.
   */
  static double cosineDistance(double dot, double normA, double normB) {
    double denom = Math.sqrt(normA) * Math.sqrt(normB);
    if (denom == 0.0) {
      return 1.0;
    }
    double similarity = Math.max(-1.0, Math.min(1.0, dot / denom));
    return 1.0 - similarity;
  }
}
