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

import java.util.AbstractList;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import org.apache.phoenix.schema.types.PVectorFloat;

/**
 * Encapsulates the results of a k-means centroid training run. Extends {@link AbstractList} so that
 * it can be passed directly to APIs expecting {@code List<float[]>}, while also exposing detailed
 * clustering diagnostics, iteration count, distortion, and skew metrics.
 */
public class KMeansResult extends AbstractList<float[]> {

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
  private final boolean hasSplit;
  private final int dimension;
  private final String distanceMetric;
  private final List<Double> distortionHistory;

  public KMeansResult(List<float[]> centroids, int requestedK, int iterations, boolean converged,
    double finalDistortion, int[] clusterSizes, int[] assignments, ClusterSkewMetrics skewMetrics,
    ClusterSkewMetrics preSplitSkewMetrics, ClusterSkewMetrics postSplitSkewMetrics,
    boolean hasSplit, int dimension, String distanceMetric, List<Double> distortionHistory) {
    this.centroids =
      new ArrayList<>(Objects.requireNonNull(centroids, "centroids must not be null"));
    this.requestedK = requestedK;
    this.iterations = iterations;
    this.converged = converged;
    this.finalDistortion = finalDistortion;
    this.clusterSizes = clusterSizes != null ? clusterSizes.clone() : new int[0];
    this.assignments = assignments != null ? assignments.clone() : new int[0];
    this.skewMetrics = skewMetrics;
    this.preSplitSkewMetrics = preSplitSkewMetrics;
    this.postSplitSkewMetrics = postSplitSkewMetrics;
    this.hasSplit = hasSplit;
    this.dimension = dimension;
    this.distanceMetric = distanceMetric;
    this.distortionHistory = distortionHistory != null
      ? new ArrayList<>(distortionHistory)
      : Collections.<Double> emptyList();
  }

  @Override
  public float[] get(int index) {
    return centroids.get(index);
  }

  @Override
  public int size() {
    return centroids.size();
  }

  public List<float[]> getCentroids() {
    return Collections.unmodifiableList(centroids);
  }

  public List<byte[]> getCentroidsAsBytes() {
    List<byte[]> byteList = new ArrayList<>(centroids.size());
    for (float[] centroid : centroids) {
      byteList.add(PVectorFloat.INSTANCE.toBytes(centroid));
    }
    return Collections.unmodifiableList(byteList);
  }

  public List<byte[]> getPackedCentroids() {
    return getCentroidsAsBytes();
  }

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

  public int[] getClusterSizes() {
    return clusterSizes.clone();
  }

  public int[] getAssignments() {
    return assignments.clone();
  }

  public ClusterSkewMetrics getSkewMetrics() {
    return skewMetrics;
  }

  public ClusterSkewMetrics getPreSplitSkewMetrics() {
    return preSplitSkewMetrics;
  }

  public ClusterSkewMetrics getPostSplitSkewMetrics() {
    return postSplitSkewMetrics;
  }

  public boolean hasSplit() {
    return hasSplit;
  }

  public int getDimension() {
    return dimension;
  }

  public String getDistanceMetric() {
    return distanceMetric;
  }

  public List<Double> getDistortionHistory() {
    return Collections.unmodifiableList(distortionHistory);
  }
}
