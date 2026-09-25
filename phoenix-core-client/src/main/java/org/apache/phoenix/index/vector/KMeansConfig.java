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

import java.util.Locale;
import java.util.Objects;

/**
 * Configuration options for k-means centroid training.
 */
public class KMeansConfig {

  public static final int DEFAULT_MAX_ITERATIONS = 100;
  public static final double DEFAULT_CONVERGENCE_THRESHOLD = 1e-4;
  public static final String DEFAULT_DISTANCE_METRIC = "L2";
  public static final boolean DEFAULT_ENABLE_SPLIT_HEURISTIC = true;
  public static final double DEFAULT_SPLIT_THRESHOLD_MULTIPLIER = 2.0;
  public static final int DEFAULT_SPLIT_REBALANCE_ITERATIONS = 5;
  public static final int DEFAULT_SAMPLE_SIZE = 10000;
  public static final int DEFAULT_PARALLEL_INIT_THRESHOLD = 1024;

  private final int maxIterations;
  private final double convergenceThreshold;
  private final String distanceMetric;
  private final boolean enableSplitHeuristic;
  private final double splitThresholdMultiplier;
  private final int splitRebalanceIterations;
  private final Long randomSeed;
  private final int sampleSize;
  private final int parallelInitThreshold;

  private KMeansConfig(Builder builder) {
    this.maxIterations = builder.maxIterations;
    this.convergenceThreshold = builder.convergenceThreshold;
    this.distanceMetric = builder.distanceMetric;
    this.enableSplitHeuristic = builder.enableSplitHeuristic;
    this.splitThresholdMultiplier = builder.splitThresholdMultiplier;
    this.splitRebalanceIterations = builder.splitRebalanceIterations;
    this.randomSeed = builder.randomSeed;
    this.sampleSize = builder.sampleSize;
    this.parallelInitThreshold = builder.parallelInitThreshold;
  }

  public static Builder newBuilder() {
    return new Builder();
  }

  public static Builder builder() {
    return new Builder();
  }

  public static KMeansConfig defaultConfig() {
    return new Builder().build();
  }

  public int getMaxIterations() {
    return maxIterations;
  }

  public double getConvergenceThreshold() {
    return convergenceThreshold;
  }

  public String getDistanceMetric() {
    return distanceMetric;
  }

  public boolean isEnableSplitHeuristic() {
    return enableSplitHeuristic;
  }

  public boolean getEnableSplitHeuristic() {
    return enableSplitHeuristic;
  }

  public double getSplitThresholdMultiplier() {
    return splitThresholdMultiplier;
  }

  public int getSplitRebalanceIterations() {
    return splitRebalanceIterations;
  }

  public Long getRandomSeed() {
    return randomSeed;
  }

  public int getSampleSize() {
    return sampleSize;
  }

  public int getParallelInitThreshold() {
    return parallelInitThreshold;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (!(o instanceof KMeansConfig)) {
      return false;
    }
    KMeansConfig that = (KMeansConfig) o;
    return maxIterations == that.maxIterations
      && Double.compare(that.convergenceThreshold, convergenceThreshold) == 0
      && enableSplitHeuristic == that.enableSplitHeuristic
      && Double.compare(that.splitThresholdMultiplier, splitThresholdMultiplier) == 0
      && splitRebalanceIterations == that.splitRebalanceIterations && sampleSize == that.sampleSize
      && parallelInitThreshold == that.parallelInitThreshold
      && Objects.equals(distanceMetric, that.distanceMetric)
      && Objects.equals(randomSeed, that.randomSeed);
  }

  @Override
  public int hashCode() {
    return Objects.hash(maxIterations, convergenceThreshold, distanceMetric, enableSplitHeuristic,
      splitThresholdMultiplier, splitRebalanceIterations, randomSeed, sampleSize,
      parallelInitThreshold);
  }

  @Override
  public String toString() {
    return "KMeansConfig{" + "maxIterations=" + maxIterations + ", convergenceThreshold="
      + convergenceThreshold + ", distanceMetric='" + distanceMetric + '\''
      + ", enableSplitHeuristic=" + enableSplitHeuristic + ", splitThresholdMultiplier="
      + splitThresholdMultiplier + ", splitRebalanceIterations=" + splitRebalanceIterations
      + ", randomSeed=" + randomSeed + ", sampleSize=" + sampleSize + ", parallelInitThreshold="
      + parallelInitThreshold + '}';
  }

  public static class Builder {
    private int maxIterations = DEFAULT_MAX_ITERATIONS;
    private double convergenceThreshold = DEFAULT_CONVERGENCE_THRESHOLD;
    private String distanceMetric = DEFAULT_DISTANCE_METRIC;
    private boolean enableSplitHeuristic = DEFAULT_ENABLE_SPLIT_HEURISTIC;
    private double splitThresholdMultiplier = DEFAULT_SPLIT_THRESHOLD_MULTIPLIER;
    private int splitRebalanceIterations = DEFAULT_SPLIT_REBALANCE_ITERATIONS;
    private Long randomSeed = null;
    private int sampleSize = DEFAULT_SAMPLE_SIZE;
    private int parallelInitThreshold = DEFAULT_PARALLEL_INIT_THRESHOLD;

    public Builder maxIterations(int maxIterations) {
      if (maxIterations <= 0) {
        throw new IllegalArgumentException("maxIterations must be > 0: " + maxIterations);
      }
      this.maxIterations = maxIterations;
      return this;
    }

    public Builder convergenceThreshold(double convergenceThreshold) {
      if (convergenceThreshold <= 0.0) {
        throw new IllegalArgumentException(
          "convergenceThreshold must be > 0: " + convergenceThreshold);
      }
      this.convergenceThreshold = convergenceThreshold;
      return this;
    }

    public Builder distanceMetric(String distanceMetric) {
      if (distanceMetric == null) {
        throw new IllegalArgumentException("distanceMetric must not be null");
      }
      String upper = distanceMetric.trim().toUpperCase(Locale.ROOT);
      if (!"L2".equals(upper) && !"COSINE".equals(upper) && !"INNER_PRODUCT".equals(upper)) {
        throw new IllegalArgumentException("Invalid distance metric: " + distanceMetric
          + ". Supported metrics: L2, COSINE, INNER_PRODUCT");
      }
      this.distanceMetric = upper;
      return this;
    }

    public Builder enableSplitHeuristic(boolean enableSplitHeuristic) {
      this.enableSplitHeuristic = enableSplitHeuristic;
      return this;
    }

    public Builder splitThresholdMultiplier(double splitThresholdMultiplier) {
      if (splitThresholdMultiplier <= 0.0) {
        throw new IllegalArgumentException(
          "splitThresholdMultiplier must be > 0: " + splitThresholdMultiplier);
      }
      this.splitThresholdMultiplier = splitThresholdMultiplier;
      return this;
    }

    public Builder splitRebalanceIterations(int splitRebalanceIterations) {
      if (splitRebalanceIterations < 0) {
        throw new IllegalArgumentException(
          "splitRebalanceIterations must be >= 0: " + splitRebalanceIterations);
      }
      this.splitRebalanceIterations = splitRebalanceIterations;
      return this;
    }

    public Builder randomSeed(Long randomSeed) {
      this.randomSeed = randomSeed;
      return this;
    }

    public Builder randomSeed(long randomSeed) {
      this.randomSeed = randomSeed;
      return this;
    }

    public Builder sampleSize(int sampleSize) {
      if (sampleSize <= 0) {
        throw new IllegalArgumentException("sampleSize must be > 0: " + sampleSize);
      }
      this.sampleSize = sampleSize;
      return this;
    }

    public Builder parallelInitThreshold(int parallelInitThreshold) {
      if (parallelInitThreshold <= 0) {
        throw new IllegalArgumentException(
          "parallelInitThreshold must be > 0: " + parallelInitThreshold);
      }
      this.parallelInitThreshold = parallelInitThreshold;
      return this;
    }

    public KMeansConfig build() {
      return new KMeansConfig(this);
    }
  }
}
