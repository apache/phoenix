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

/**
 * Metadata and drift metrics of a centroid generation. The summary row of the generation in
 * {@code SYSTEM.VECTOR_CENTROID} ({@code CENTROID_ID = -1}) keeps them. A null field is not set,
 * and a partial update does not change the stored column value of that field.
 */
public final class GenerationSummary {

  /** The generation came from the first training of the index, or a rebuild promoted it. */
  public static final String ACTIVE = "A";
  /** The generation is the target of a rebuild migration that is not complete. */
  public static final String BUILDING = "B";

  private final String rebuildState;
  private final String triggerReason;
  private final Integer requestedLists;
  private final ClusterSkewMetrics skewMetrics;
  private final Long lastRebuildTime;
  private final Long lastScorecardUpdate;

  public GenerationSummary(String rebuildState, String triggerReason, Integer requestedLists,
    ClusterSkewMetrics skewMetrics, Long lastRebuildTime, Long lastScorecardUpdate) {
    this.rebuildState = rebuildState;
    this.triggerReason = triggerReason;
    this.requestedLists = requestedLists;
    this.skewMetrics = skewMetrics;
    this.lastRebuildTime = lastRebuildTime;
    this.lastScorecardUpdate = lastScorecardUpdate;
  }

  /** Returns the rebuild state: {@link #ACTIVE}, {@link #BUILDING}, or null if not set. */
  public String getRebuildState() {
    return rebuildState;
  }

  /**
   * Returns why the generation was made. For the active generation, an assessment that finds drift
   * replaces it with the reasons for that drift.
   */
  public String getTriggerReason() {
    return triggerReason;
  }

  /**
   * Returns the list count that the training of this generation requested. Training can split large
   * clusters, so the generation can have more centroids. A rebuild requests this count again, so
   * that the count does not grow with each rebuild.
   */
  public Integer getRequestedLists() {
    return requestedLists;
  }

  /** Returns the cluster size skew metrics of the training sample. */
  public ClusterSkewMetrics getSkewMetrics() {
    return skewMetrics;
  }

  /** Returns the time at which a rebuild promoted this generation to active, or null. */
  public Long getLastRebuildTime() {
    return lastRebuildTime;
  }

  /** Returns the time of the latest scorecard reconciliation, or null. */
  public Long getLastScorecardUpdate() {
    return lastScorecardUpdate;
  }

  @Override
  public String toString() {
    return "GenerationSummary{rebuildState=" + rebuildState + ", triggerReason=" + triggerReason
      + ", requestedLists=" + requestedLists + ", skewMetrics=" + skewMetrics + ", lastRebuildTime="
      + lastRebuildTime + ", lastScorecardUpdate=" + lastScorecardUpdate + "}";
  }
}
