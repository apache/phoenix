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

/** Cluster size and reassignment count of one centroid in the drift scorecard, or deltas. */
public final class ScorecardRow {
  private final int centroidId;
  private final long clusterSize;
  private final long reassignCount;

  public ScorecardRow(int centroidId, long clusterSize, long reassignCount) {
    this.centroidId = centroidId;
    this.clusterSize = clusterSize;
    this.reassignCount = reassignCount;
  }

  public int getCentroidId() {
    return centroidId;
  }

  public long getClusterSize() {
    return clusterSize;
  }

  public long getReassignCount() {
    return reassignCount;
  }

  @Override
  public String toString() {
    return "ScorecardRow{centroidId=" + centroidId + ", clusterSize=" + clusterSize
      + ", reassignCount=" + reassignCount + "}";
  }
}
