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

import jdk.incubator.vector.ByteVector;
import jdk.incubator.vector.FloatVector;
import jdk.incubator.vector.VectorOperators;
import jdk.incubator.vector.VectorSpecies;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.phoenix.schema.types.PVectorFloat;

/**
 * SIMD distance kernels that use the Panama Vector API on packed little-endian float vectors.
 * <p>
 * The kernels load operands directly from their {@code byte[]} encodings, at any offset, as byte
 * vectors. Then they reinterpret the bytes as float lanes. The Vector API defines this
 * reinterpretation as little-endian on all platforms, which agrees with the {@link PVectorFloat}
 * encoding. Because of this, the kernels do not decode or copy the operands.
 * <p>
 * Each kernel adds lane sums in single precision across a block of {@link #BLOCK_STRIDES} vector
 * strides. Then it adds the block result to a double-precision total. This limits the rounding
 * error relative to {@link ScalarDistanceKernel}. It also lets the L2 kernel compare the total with
 * the upper bound after each block and stop early if the total is larger. The cost of a reduction
 * occurs once for each block, not once for each stride.
 */
final class PanamaDistanceKernel {

  private static final VectorSpecies<Float> SPECIES = FloatVector.SPECIES_PREFERRED;
  private static final VectorSpecies<Byte> BYTE_SPECIES =
    VectorSpecies.of(byte.class, SPECIES.vectorShape());
  static final int BLOCK_STRIDES = 8;

  private PanamaDistanceKernel() {
  }

  private static FloatVector load(byte[] buf, int off, int i) {
    return ByteVector.fromArray(BYTE_SPECIES, buf, off + i * Bytes.SIZEOF_FLOAT)
      .reinterpretAsFloats();
  }

  static double l2DistanceSquared(byte[] a, int aOff, byte[] b, int bOff, int dim, double bound) {
    int step = SPECIES.length();
    int upper = SPECIES.loopBound(dim);
    int blockLen = step * BLOCK_STRIDES;
    double total = 0.0;
    int i = 0;
    while (i < upper) {
      int blockEnd = Math.min(upper, i + blockLen);
      FloatVector acc = FloatVector.zero(SPECIES);
      for (; i < blockEnd; i += step) {
        FloatVector diff = load(a, aOff, i).sub(load(b, bOff, i));
        acc = diff.fma(diff, acc);
      }
      total += acc.reduceLanes(VectorOperators.ADD);
      if (total > bound) {
        return Double.MAX_VALUE;
      }
    }
    for (; i < dim; i++) {
      double diff = PVectorFloat.readElement(a, aOff, i) - PVectorFloat.readElement(b, bOff, i);
      total += diff * diff;
      if (total > bound) {
        return Double.MAX_VALUE;
      }
    }
    return total;
  }

  static double dotProduct(byte[] a, int aOff, byte[] b, int bOff, int dim) {
    int step = SPECIES.length();
    int upper = SPECIES.loopBound(dim);
    int blockLen = step * BLOCK_STRIDES;
    double total = 0.0;
    int i = 0;
    while (i < upper) {
      int blockEnd = Math.min(upper, i + blockLen);
      FloatVector acc = FloatVector.zero(SPECIES);
      for (; i < blockEnd; i += step) {
        acc = load(a, aOff, i).fma(load(b, bOff, i), acc);
      }
      total += acc.reduceLanes(VectorOperators.ADD);
    }
    for (; i < dim; i++) {
      total += (double) PVectorFloat.readElement(a, aOff, i) * PVectorFloat.readElement(b, bOff, i);
    }
    return total;
  }

  static double cosineDistance(byte[] a, int aOff, byte[] b, int bOff, int dim) {
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
        FloatVector va = load(a, aOff, i);
        FloatVector vb = load(b, bOff, i);
        vDot = va.fma(vb, vDot);
        vNormA = va.fma(va, vNormA);
        vNormB = vb.fma(vb, vNormB);
      }
      dot += vDot.reduceLanes(VectorOperators.ADD);
      normA += vNormA.reduceLanes(VectorOperators.ADD);
      normB += vNormB.reduceLanes(VectorOperators.ADD);
    }
    for (; i < dim; i++) {
      double ai = PVectorFloat.readElement(a, aOff, i);
      double bi = PVectorFloat.readElement(b, bOff, i);
      dot += ai * bi;
      normA += ai * ai;
      normB += bi * bi;
    }
    return ScalarDistanceKernel.cosineDistance(dot, normA, normB);
  }
}
