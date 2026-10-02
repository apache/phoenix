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
import org.apache.phoenix.schema.SortOrder;
import org.apache.phoenix.schema.types.PVectorFloat;

/**
 * High-performance vector distance computation utilities supporting early-termination pruning with
 * upper distance bounds. The scalar implementations read directly from packed byte buffers and are
 * JIT-vectorizable.
 */
public final class VectorDistanceUtil {

  @FunctionalInterface
  public interface ElementCallback {
    void onElementsProcessed(int count);
  }

  private VectorDistanceUtil() {
  }

  // Packed byte[] <-> float[] conversion utilities

  /**
   * Converts packed big-endian IEEE 754 float bytes into a native-endian {@code float[]} array.
   */
  public static float[] toFloatArray(byte[] buf, int offset, int dim) {
    float[] out = new float[dim];
    for (int i = 0; i < dim; i++) {
      out[i] = Bytes.toFloat(buf, offset + i * Bytes.SIZEOF_FLOAT);
    }
    return out;
  }

  /**
   * Converts packed big-endian IEEE 754 float bytes with sort order into a {@code float[]} array.
   */
  public static float[] toFloatArray(byte[] buf, int offset, int dim, SortOrder sortOrder) {
    return PVectorFloat.readElements(buf, offset, dim * Bytes.SIZEOF_FLOAT, sortOrder);
  }

  // Scalar distance implementations

  public static double scalarL2DistanceSquared(byte[] a, int aOff, byte[] b, int bOff, int dim) {
    return l2DistanceSquaredWithBound(a, aOff, b, bOff, dim, Double.MAX_VALUE);
  }

  public static double scalarL2DistanceSquared(float[] a, float[] b, int dim) {
    return l2DistanceSquaredWithBound(a, b, dim, Double.MAX_VALUE);
  }

  public static double scalarL2Distance(byte[] a, int aOff, byte[] b, int bOff, int dim) {
    return l2DistanceWithBound(a, aOff, b, bOff, dim, Double.MAX_VALUE);
  }

  public static double scalarL2Distance(float[] a, float[] b, int dim) {
    return l2DistanceWithBound(a, b, dim, Double.MAX_VALUE);
  }

  public static double scalarDotProduct(byte[] a, int aOff, byte[] b, int bOff, int dim) {
    return dotProductWithBound(a, aOff, b, bOff, dim, Double.MAX_VALUE);
  }

  public static double scalarDotProduct(float[] a, float[] b, int dim) {
    return dotProductWithBound(a, b, dim, Double.MAX_VALUE);
  }

  public static double scalarInnerProductDistance(byte[] a, int aOff, byte[] b, int bOff, int dim) {
    return innerProductDistanceWithBound(a, aOff, b, bOff, dim, Double.MAX_VALUE);
  }

  public static double scalarInnerProductDistance(float[] a, float[] b, int dim) {
    return innerProductDistanceWithBound(a, b, dim, Double.MAX_VALUE);
  }

  public static double scalarCosineDistance(byte[] a, int aOff, byte[] b, int bOff, int dim) {
    return cosineDistanceWithBound(a, aOff, b, bOff, dim, Double.MAX_VALUE);
  }

  public static double scalarCosineDistance(float[] a, float[] b, int dim) {
    return cosineDistanceWithBound(a, b, dim, Double.MAX_VALUE);
  }

  // Early termination bounded scalar implementations

  public static double l2DistanceSquaredWithBound(byte[] a, int aOff, byte[] b, int bOff, int dim,
    double bound) {
    return l2DistanceSquaredWithBound(a, aOff, b, bOff, dim, bound, null);
  }

  public static double l2DistanceSquaredWithBound(byte[] a, int aOff, byte[] b, int bOff, int dim,
    double bound, ElementCallback callback) {
    double sum = 0.0;
    for (int i = 0; i < dim; i++) {
      float ai = Bytes.toFloat(a, aOff + i * Bytes.SIZEOF_FLOAT);
      float bi = Bytes.toFloat(b, bOff + i * Bytes.SIZEOF_FLOAT);
      double diff = ai - bi;
      sum += diff * diff;
      if (sum > bound) {
        if (callback != null) {
          callback.onElementsProcessed(i + 1);
        }
        return Double.MAX_VALUE;
      }
    }
    if (callback != null) {
      callback.onElementsProcessed(dim);
    }
    return sum;
  }

  public static double l2DistanceSquaredWithBound(float[] a, float[] b, int dim, double bound) {
    return l2DistanceSquaredWithBound(a, b, dim, bound, null);
  }

  public static double l2DistanceSquaredWithBound(float[] a, float[] b, int dim, double bound,
    ElementCallback callback) {
    double sum = 0.0;
    for (int i = 0; i < dim; i++) {
      double diff = a[i] - b[i];
      sum += diff * diff;
      if (sum > bound) {
        if (callback != null) {
          callback.onElementsProcessed(i + 1);
        }
        return Double.MAX_VALUE;
      }
    }
    if (callback != null) {
      callback.onElementsProcessed(dim);
    }
    return sum;
  }

  public static double l2DistanceWithBound(byte[] a, int aOff, byte[] b, int bOff, int dim,
    double bound) {
    return l2DistanceWithBound(a, aOff, b, bOff, dim, bound, null);
  }

  public static double l2DistanceWithBound(byte[] a, int aOff, byte[] b, int bOff, int dim,
    double bound, ElementCallback callback) {
    double boundSq = (bound == Double.MAX_VALUE) ? Double.MAX_VALUE : (bound * bound);
    double sq = l2DistanceSquaredWithBound(a, aOff, b, bOff, dim, boundSq, callback);
    if (sq == Double.MAX_VALUE) {
      return Double.MAX_VALUE;
    }
    double dist = Math.sqrt(sq);
    return (dist > bound) ? Double.MAX_VALUE : dist;
  }

  public static double l2DistanceWithBound(float[] a, float[] b, int dim, double bound) {
    return l2DistanceWithBound(a, b, dim, bound, null);
  }

  public static double l2DistanceWithBound(float[] a, float[] b, int dim, double bound,
    ElementCallback callback) {
    double boundSq = (bound == Double.MAX_VALUE) ? Double.MAX_VALUE : (bound * bound);
    double sq = l2DistanceSquaredWithBound(a, b, dim, boundSq, callback);
    if (sq == Double.MAX_VALUE) {
      return Double.MAX_VALUE;
    }
    double dist = Math.sqrt(sq);
    return (dist > bound) ? Double.MAX_VALUE : dist;
  }

  public static double dotProductWithBound(byte[] a, int aOff, byte[] b, int bOff, int dim,
    double bound) {
    double dot = 0.0;
    for (int i = 0; i < dim; i++) {
      float ai = Bytes.toFloat(a, aOff + i * Bytes.SIZEOF_FLOAT);
      float bi = Bytes.toFloat(b, bOff + i * Bytes.SIZEOF_FLOAT);
      dot += (double) ai * bi;
    }
    return dot;
  }

  public static double dotProductWithBound(float[] a, float[] b, int dim, double bound) {
    double dot = 0.0;
    for (int i = 0; i < dim; i++) {
      dot += (double) a[i] * b[i];
    }
    return dot;
  }

  public static double innerProductDistanceWithBound(byte[] a, int aOff, byte[] b, int bOff,
    int dim, double bound) {
    double dot = dotProductWithBound(a, aOff, b, bOff, dim, bound);
    double dist = -dot;
    return (dist > bound) ? Double.MAX_VALUE : dist;
  }

  public static double innerProductDistanceWithBound(float[] a, float[] b, int dim, double bound) {
    double dot = dotProductWithBound(a, b, dim, bound);
    double dist = -dot;
    return (dist > bound) ? Double.MAX_VALUE : dist;
  }

  public static double cosineDistanceWithBound(byte[] a, int aOff, byte[] b, int bOff, int dim,
    double bound) {
    double dot = 0.0;
    double normA = 0.0;
    double normB = 0.0;
    for (int i = 0; i < dim; i++) {
      float ai = Bytes.toFloat(a, aOff + i * Bytes.SIZEOF_FLOAT);
      float bi = Bytes.toFloat(b, bOff + i * Bytes.SIZEOF_FLOAT);
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
    double dist = 1.0 - cosineSim;
    return (dist > bound) ? Double.MAX_VALUE : dist;
  }

  public static double cosineDistanceWithBound(float[] a, float[] b, int dim, double bound) {
    double dot = 0.0;
    double normA = 0.0;
    double normB = 0.0;
    for (int i = 0; i < dim; i++) {
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
    double dist = 1.0 - cosineSim;
    return (dist > bound) ? Double.MAX_VALUE : dist;
  }
}
