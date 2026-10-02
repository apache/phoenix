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

/**
 * Represents a single centroid's drift scorecard metrics stored in {@code SYSTEM.VECTOR_CENTROID}
 * for non-negative centroid IDs ({@code CENTROID_ID >= 0}).
 */
public class ScorecardRow {

  private final String indexName;
  private final long generationId;
  private final int centroidId;
  private final Long clusterSize;
  private final Long reassignCount;
  private final Long lastScorecardUpdate;

  public ScorecardRow(String indexName, long generationId, int centroidId, Long clusterSize,
    Long reassignCount, Long lastScorecardUpdate) {
    this.indexName = indexName;
    this.generationId = generationId;
    this.centroidId = centroidId;
    this.clusterSize = clusterSize;
    this.reassignCount = reassignCount;
    this.lastScorecardUpdate = lastScorecardUpdate;
  }

  public String getIndexName() {
    return indexName;
  }

  public long getGenerationId() {
    return generationId;
  }

  public int getCentroidId() {
    return centroidId;
  }

  public Long getClusterSize() {
    return clusterSize;
  }

  public Long getReassignCount() {
    return reassignCount;
  }

  public Long getLastScorecardUpdate() {
    return lastScorecardUpdate;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (!(o instanceof ScorecardRow)) {
      return false;
    }
    ScorecardRow that = (ScorecardRow) o;
    return generationId == that.generationId && centroidId == that.centroidId
      && Objects.equals(indexName, that.indexName) && Objects.equals(clusterSize, that.clusterSize)
      && Objects.equals(reassignCount, that.reassignCount)
      && Objects.equals(lastScorecardUpdate, that.lastScorecardUpdate);
  }

  @Override
  public int hashCode() {
    return Objects.hash(indexName, generationId, centroidId, clusterSize, reassignCount,
      lastScorecardUpdate);
  }

  @Override
  public String toString() {
    return "ScorecardRow{" + "indexName='" + indexName + '\'' + ", generationId=" + generationId
      + ", centroidId=" + centroidId + ", clusterSize=" + clusterSize + ", reassignCount="
      + reassignCount + ", lastScorecardUpdate=" + lastScorecardUpdate + '}';
  }

  public static class Builder {
    private String indexName;
    private long generationId;
    private int centroidId;
    private Long clusterSize;
    private Long reassignCount;
    private Long lastScorecardUpdate;

    public Builder setIndexName(String indexName) {
      this.indexName = indexName;
      return this;
    }

    public Builder setGenerationId(long generationId) {
      this.generationId = generationId;
      return this;
    }

    public Builder setCentroidId(int centroidId) {
      this.centroidId = centroidId;
      return this;
    }

    public Builder setClusterSize(Long clusterSize) {
      this.clusterSize = clusterSize;
      return this;
    }

    public Builder setReassignCount(Long reassignCount) {
      this.reassignCount = reassignCount;
      return this;
    }

    public Builder setLastScorecardUpdate(Long lastScorecardUpdate) {
      this.lastScorecardUpdate = lastScorecardUpdate;
      return this;
    }

    public ScorecardRow build() {
      return new ScorecardRow(indexName, generationId, centroidId, clusterSize, reassignCount,
        lastScorecardUpdate);
    }
  }
}
