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
 * Metadata and drift metrics for a centroid generation, persisted in the sentinel row
 * ({@code CENTROID_ID = -1}) of {@code SYSTEM.VECTOR_CENTROID}. Null fields indicate unset values
 * and preserve existing column values during partial updates.
 */
public final class GenerationSummary {

  /** Generation is active for index maintenance and queries. */
  public static final String ACTIVE = "A";
  /** Generation is undergoing background rebuild migration. */
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

  /** Rebuild lifecycle state ({@link #ACTIVE}, {@link #BUILDING}, or null). */
  public String getRebuildState() {
    return rebuildState;
  }

  /** Reason triggering initial creation or subsequent rebuild recommendation. */
  public String getTriggerReason() {
    return triggerReason;
  }

  /**
   * Configured target IVF list count before split heuristic adjustments.
   */
  public Integer getRequestedLists() {
    return requestedLists;
  }

  /** Training sample cluster skew distribution metrics. */
  public ClusterSkewMetrics getSkewMetrics() {
    return skewMetrics;
  }

  /** Timestamp when this generation was promoted to active. */
  public Long getLastRebuildTime() {
    return lastRebuildTime;
  }

  /** Timestamp of the most recent scorecard reconciliation. */
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
