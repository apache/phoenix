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
package org.apache.phoenix.index.vector;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.Arrays;
import java.util.Objects;

/**
 * Encapsulates cluster size distribution metrics for k-means clustering. Tracks min, max, mean,
 * 95th percentile, standard deviation, and coefficient of variation (CV) across cluster sizes, and
 * provides binary serialization for persistence to metadata catalogs.
 */
public class ClusterSkewMetrics {

  private static final int SERIALIZATION_VERSION = 1;

  private final int min;
  private final int max;
  private final double mean;
  private final double p95;
  private final double stdDev;
  private final double coefficientOfVariation;
  private final int[] clusterSizes;
  private final int totalVectors;
  private final int k;

  public ClusterSkewMetrics(int min, int max, double mean, double p95, double stdDev,
    double coefficientOfVariation, int[] clusterSizes, int totalVectors, int k) {
    this.min = min;
    this.max = max;
    this.mean = mean;
    this.p95 = p95;
    this.stdDev = stdDev;
    this.coefficientOfVariation = coefficientOfVariation;
    this.clusterSizes = clusterSizes != null ? clusterSizes.clone() : new int[0];
    this.totalVectors = totalVectors;
    this.k = k;
  }

  /**
   * Computes skew metrics from an array of cluster sizes.
   * @param clusterSizes non-null, non-empty array of vector counts per cluster
   * @return computed {@link ClusterSkewMetrics}
   */
  public static ClusterSkewMetrics compute(int[] clusterSizes) {
    if (clusterSizes == null || clusterSizes.length == 0) {
      throw new IllegalArgumentException("clusterSizes must not be null or empty");
    }

    int k = clusterSizes.length;
    int min = Integer.MAX_VALUE;
    int max = Integer.MIN_VALUE;
    long total = 0;

    for (int size : clusterSizes) {
      if (size < min) {
        min = size;
      }
      if (size > max) {
        max = size;
      }
      total += size;
    }

    int totalVectors = (int) total;
    double mean = (double) total / k;

    double sumSqDiff = 0.0;
    for (int size : clusterSizes) {
      double diff = size - mean;
      sumSqDiff += diff * diff;
    }
    double variance = sumSqDiff / k;
    double stdDev = Math.sqrt(variance);
    double cv = (mean == 0.0) ? 0.0 : (stdDev / mean);

    int[] sorted = clusterSizes.clone();
    Arrays.sort(sorted);
    double p95;
    if (k == 1) {
      p95 = sorted[0];
    } else {
      double rank = 0.95 * (k - 1);
      int lower = (int) Math.floor(rank);
      int upper = (int) Math.ceil(rank);
      if (lower == upper) {
        p95 = sorted[lower];
      } else {
        p95 = sorted[lower] + (rank - lower) * (sorted[upper] - sorted[lower]);
      }
    }

    return new ClusterSkewMetrics(min, max, mean, p95, stdDev, cv, clusterSizes, totalVectors, k);
  }

  /**
   * Serializes these metrics to a binary byte array.
   */
  public byte[] toBytes() {
    try (ByteArrayOutputStream baos = new ByteArrayOutputStream();
      DataOutputStream dos = new DataOutputStream(baos)) {
      dos.writeInt(SERIALIZATION_VERSION);
      dos.writeInt(k);
      dos.writeInt(totalVectors);
      dos.writeInt(min);
      dos.writeInt(max);
      dos.writeDouble(mean);
      dos.writeDouble(p95);
      dos.writeDouble(stdDev);
      dos.writeDouble(coefficientOfVariation);
      dos.writeInt(clusterSizes.length);
      for (int size : clusterSizes) {
        dos.writeInt(size);
      }
      dos.flush();
      return baos.toByteArray();
    } catch (IOException e) {
      throw new RuntimeException("Failed to serialize ClusterSkewMetrics", e);
    }
  }

  /**
   * Deserializes metrics from a binary byte array.
   */
  public static ClusterSkewMetrics fromBytes(byte[] bytes) {
    if (bytes == null || bytes.length == 0) {
      throw new IllegalArgumentException("Byte array must not be null or empty");
    }
    try (ByteArrayInputStream bais = new ByteArrayInputStream(bytes);
      DataInputStream dis = new DataInputStream(bais)) {
      int version = dis.readInt();
      if (version != SERIALIZATION_VERSION) {
        throw new IllegalArgumentException("Unsupported ClusterSkewMetrics version: " + version);
      }
      int k = dis.readInt();
      int totalVectors = dis.readInt();
      int min = dis.readInt();
      int max = dis.readInt();
      double mean = dis.readDouble();
      double p95 = dis.readDouble();
      double stdDev = dis.readDouble();
      double cv = dis.readDouble();
      int sizesLen = dis.readInt();
      int[] sizes = new int[sizesLen];
      for (int i = 0; i < sizesLen; i++) {
        sizes[i] = dis.readInt();
      }
      return new ClusterSkewMetrics(min, max, mean, p95, stdDev, cv, sizes, totalVectors, k);
    } catch (IOException e) {
      throw new RuntimeException("Failed to deserialize ClusterSkewMetrics", e);
    }
  }

  /**
   * Coefficient of variation threshold above which cluster sizes are considered severely skewed.
   */
  public static final double SEVERE_SKEW_CV_THRESHOLD = 1.0;

  /**
   * Returns true if the coefficient of variation exceeds {@link #SEVERE_SKEW_CV_THRESHOLD}
   * (indicating severe cluster skew).
   */
  public boolean isSevereSkew() {
    return coefficientOfVariation > SEVERE_SKEW_CV_THRESHOLD;
  }

  public int getMin() {
    return min;
  }

  public int getMax() {
    return max;
  }

  public double getMean() {
    return mean;
  }

  public double getP95() {
    return p95;
  }

  public double getStdDev() {
    return stdDev;
  }

  public double getCoefficientOfVariation() {
    return coefficientOfVariation;
  }

  public double getCv() {
    return coefficientOfVariation;
  }

  public int[] getClusterSizes() {
    return clusterSizes.clone();
  }

  public int getTotalVectors() {
    return totalVectors;
  }

  public int getK() {
    return k;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (!(o instanceof ClusterSkewMetrics)) {
      return false;
    }
    ClusterSkewMetrics that = (ClusterSkewMetrics) o;
    return min == that.min && max == that.max && Double.compare(that.mean, mean) == 0
      && Double.compare(that.p95, p95) == 0 && Double.compare(that.stdDev, stdDev) == 0
      && Double.compare(that.coefficientOfVariation, coefficientOfVariation) == 0
      && totalVectors == that.totalVectors && k == that.k
      && Arrays.equals(clusterSizes, that.clusterSizes);
  }

  @Override
  public int hashCode() {
    int result = Objects.hash(min, max, mean, p95, stdDev, coefficientOfVariation, totalVectors, k);
    result = 31 * result + Arrays.hashCode(clusterSizes);
    return result;
  }

  @Override
  public String toString() {
    return "ClusterSkewMetrics{" + "k=" + k + ", totalVectors=" + totalVectors + ", min=" + min
      + ", max=" + max + ", mean=" + mean + ", p95=" + p95 + ", stdDev=" + stdDev + ", cv="
      + coefficientOfVariation + ", severeSkew=" + isSevereSkew() + '}';
  }
}
