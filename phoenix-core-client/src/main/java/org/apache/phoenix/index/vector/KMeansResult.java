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

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import org.apache.phoenix.optimize.DistanceMetric;

/**
 * Result of k-means clustering containing trained centroids, sample assignments, and skew
 * diagnostics.
 */
public final class KMeansResult {

  private final List<float[]> centroids;
  private final int requestedK;
  private final int iterations;
  private final boolean converged;
  private final double finalDistortion;
  private final int[] clusterSizes;
  private final int[] assignments;
  private final ClusterSkewMetrics skewMetrics;
  private final ClusterSkewMetrics preSplitSkewMetrics;
  private final ClusterSkewMetrics postSplitSkewMetrics;
  private final int dimension;
  private final DistanceMetric distanceMetric;
  private final List<Double> distortionHistory;

  KMeansResult(List<float[]> centroids, int requestedK, int iterations, boolean converged,
    double finalDistortion, int[] clusterSizes, int[] assignments, ClusterSkewMetrics skewMetrics,
    ClusterSkewMetrics preSplitSkewMetrics, ClusterSkewMetrics postSplitSkewMetrics, int dimension,
    DistanceMetric distanceMetric, List<Double> distortionHistory) {
    this.centroids = Collections.unmodifiableList(new ArrayList<>(centroids));
    this.requestedK = requestedK;
    this.iterations = iterations;
    this.converged = converged;
    this.finalDistortion = finalDistortion;
    this.clusterSizes = clusterSizes.clone();
    this.assignments = assignments.clone();
    this.skewMetrics = skewMetrics;
    this.preSplitSkewMetrics = preSplitSkewMetrics;
    this.postSplitSkewMetrics = postSplitSkewMetrics;
    this.dimension = dimension;
    this.distanceMetric = distanceMetric;
    this.distortionHistory = Collections.unmodifiableList(new ArrayList<>(distortionHistory));
  }

  public List<float[]> getCentroids() {
    return centroids;
  }

  /**
   * Returns the effective centroid count, including any sub-centroids produced by cluster
   * splitting.
   */
  public int getEffectiveK() {
    return centroids.size();
  }

  public int getRequestedK() {
    return requestedK;
  }

  public int getIterations() {
    return iterations;
  }

  public boolean isConverged() {
    return converged;
  }

  public double getFinalDistortion() {
    return finalDistortion;
  }

  /** Returns sample vector counts per centroid index. */
  public int[] getClusterSizes() {
    return clusterSizes.clone();
  }

  /** Returns centroid ID assignments for each sample vector. */
  public int[] getAssignments() {
    return assignments.clone();
  }

  /** Returns skew metrics for the final cluster size distribution. */
  public ClusterSkewMetrics getSkewMetrics() {
    return skewMetrics;
  }

  public ClusterSkewMetrics getPreSplitSkewMetrics() {
    return preSplitSkewMetrics;
  }

  /** Returns post-split skew metrics, or null if no clusters were split. */
  public ClusterSkewMetrics getPostSplitSkewMetrics() {
    return postSplitSkewMetrics;
  }

  public boolean hasSplit() {
    return postSplitSkewMetrics != null;
  }

  public int getDimension() {
    return dimension;
  }

  public DistanceMetric getDistanceMetric() {
    return distanceMetric;
  }

  public List<Double> getDistortionHistory() {
    return distortionHistory;
  }
}
