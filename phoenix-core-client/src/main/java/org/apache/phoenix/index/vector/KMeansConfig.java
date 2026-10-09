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

import java.util.Objects;
import org.apache.phoenix.optimize.DistanceMetric;

public final class KMeansConfig {

  public static final int DEFAULT_MAX_ITERATIONS = 100;
  public static final double DEFAULT_CONVERGENCE_THRESHOLD = 1e-4;
  public static final double DEFAULT_SPLIT_THRESHOLD_MULTIPLIER = 2.0;
  public static final int DEFAULT_SPLIT_REBALANCE_ITERATIONS = 5;

  private final int maxIterations;
  private final double convergenceThreshold;
  private final DistanceMetric distanceMetric;
  private final boolean enableSplitHeuristic;
  private final double splitThresholdMultiplier;
  private final int splitRebalanceIterations;
  private final Long randomSeed;

  private KMeansConfig(Builder builder) {
    this.maxIterations = builder.maxIterations;
    this.convergenceThreshold = builder.convergenceThreshold;
    this.distanceMetric = builder.distanceMetric;
    this.enableSplitHeuristic = builder.enableSplitHeuristic;
    this.splitThresholdMultiplier = builder.splitThresholdMultiplier;
    this.splitRebalanceIterations = builder.splitRebalanceIterations;
    this.randomSeed = builder.randomSeed;
  }

  public static Builder newBuilder() {
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

  public DistanceMetric getDistanceMetric() {
    return distanceMetric;
  }

  public boolean isEnableSplitHeuristic() {
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

  public static final class Builder {
    private int maxIterations = DEFAULT_MAX_ITERATIONS;
    private double convergenceThreshold = DEFAULT_CONVERGENCE_THRESHOLD;
    private DistanceMetric distanceMetric = DistanceMetric.L2;
    private boolean enableSplitHeuristic = true;
    private double splitThresholdMultiplier = DEFAULT_SPLIT_THRESHOLD_MULTIPLIER;
    private int splitRebalanceIterations = DEFAULT_SPLIT_REBALANCE_ITERATIONS;
    private Long randomSeed;

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

    public Builder distanceMetric(DistanceMetric distanceMetric) {
      this.distanceMetric = Objects.requireNonNull(distanceMetric, "distanceMetric");
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

    public Builder randomSeed(long randomSeed) {
      this.randomSeed = randomSeed;
      return this;
    }

    public KMeansConfig build() {
      return new KMeansConfig(this);
    }
  }
}
