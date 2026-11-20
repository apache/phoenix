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
 * Result of k-means training: the trained centroids, the cluster of each sample vector, and the
 * skew metrics before and after a cluster split.
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
   * Returns the number of trained centroids. A cluster split can make this number larger than the
   * requested count.
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

  /** Returns the number of sample vectors in each cluster, by position in the centroid list. */
  public int[] getClusterSizes() {
    return clusterSizes.clone();
  }

  /** Returns the position in the centroid list of the cluster of each sample vector. */
  public int[] getAssignments() {
    return assignments.clone();
  }

  /** Returns the skew metrics of the final cluster sizes, after a split if one occurred. */
  public ClusterSkewMetrics getSkewMetrics() {
    return skewMetrics;
  }

  public ClusterSkewMetrics getPreSplitSkewMetrics() {
    return preSplitSkewMetrics;
  }

  /** Returns the skew metrics after a cluster split, or null if no cluster was split. */
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
