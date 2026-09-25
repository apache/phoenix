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
 * Represents the generation-level summary metadata stored in the sentinel row
 * ({@code CENTROID_ID = -1}) of {@code SYSTEM.VECTOR_CENTROID}.
 */
public class GenerationSummary {

  private final String indexName;
  private final long generationId;
  private final ClusterSkewMetrics skewMetrics;
  private final String skewMetricsDecodeError;
  private final String rebuildState;
  private final String triggerReason;
  private final Long lastRebuildTime;
  private final Long lastScorecardUpdate;

  private GenerationSummary(String indexName, long generationId, ClusterSkewMetrics skewMetrics,
    String skewMetricsDecodeError, String rebuildState, String triggerReason, Long lastRebuildTime,
    Long lastScorecardUpdate) {
    this.indexName = indexName;
    this.generationId = generationId;
    this.skewMetrics = skewMetrics;
    this.skewMetricsDecodeError = skewMetricsDecodeError;
    this.rebuildState = rebuildState;
    this.triggerReason = triggerReason;
    this.lastRebuildTime = lastRebuildTime;
    this.lastScorecardUpdate = lastScorecardUpdate;
  }

  public String getIndexName() {
    return indexName;
  }

  public long getGenerationId() {
    return generationId;
  }

  /**
   * Returns the training skew metrics recorded for this generation, or {@code null} if none were
   * recorded or the stored blob could not be decoded. The decode happens once, where the row is
   * read; check {@link #getSkewMetricsDecodeError()} to tell an undecodable blob from an absent one
   * rather than re-decoding here and discarding the error again.
   */
  public ClusterSkewMetrics getSkewMetrics() {
    return skewMetrics;
  }

  /**
   * Returns a description of why the stored {@code SKEW_METRICS} blob could not be decoded, or
   * {@code null} when it decoded or when the generation carries no metrics at all.
   */
  public String getSkewMetricsDecodeError() {
    return skewMetricsDecodeError;
  }

  /**
   * Returns the encoded metrics to write back, or {@code null} when this summary carries none.
   * Re-encoded from the decoded object, so a summary built from an undecodable blob returns
   * {@code null} and leaves the stored bytes untouched on the next partial write.
   */
  public byte[] getSkewMetricsBytes() {
    return skewMetrics != null ? skewMetrics.toBytes() : null;
  }

  public String getRebuildState() {
    return rebuildState;
  }

  public String getTriggerReason() {
    return triggerReason;
  }

  public Long getLastRebuildTime() {
    return lastRebuildTime;
  }

  public Long getLastScorecardUpdate() {
    return lastScorecardUpdate;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (!(o instanceof GenerationSummary)) {
      return false;
    }
    GenerationSummary that = (GenerationSummary) o;
    return generationId == that.generationId && Objects.equals(indexName, that.indexName)
      && Objects.equals(skewMetrics, that.skewMetrics)
      && Objects.equals(skewMetricsDecodeError, that.skewMetricsDecodeError)
      && Objects.equals(rebuildState, that.rebuildState)
      && Objects.equals(triggerReason, that.triggerReason)
      && Objects.equals(lastRebuildTime, that.lastRebuildTime)
      && Objects.equals(lastScorecardUpdate, that.lastScorecardUpdate);
  }

  @Override
  public int hashCode() {
    return Objects.hash(indexName, generationId, skewMetrics, skewMetricsDecodeError, rebuildState,
      triggerReason, lastRebuildTime, lastScorecardUpdate);
  }

  @Override
  public String toString() {
    return "GenerationSummary{" + "indexName='" + indexName + '\'' + ", generationId="
      + generationId + ", skewMetrics=" + skewMetrics + ", skewMetricsDecodeError='"
      + skewMetricsDecodeError + '\'' + ", rebuildState='" + rebuildState + '\''
      + ", triggerReason='" + triggerReason + '\'' + ", lastRebuildTime=" + lastRebuildTime
      + ", lastScorecardUpdate=" + lastScorecardUpdate + '}';
  }

  public static class Builder {
    private String indexName;
    private long generationId;
    private ClusterSkewMetrics skewMetrics;
    private String skewMetricsDecodeError;
    private String rebuildState;
    private String triggerReason;
    private Long lastRebuildTime;
    private Long lastScorecardUpdate;

    public Builder setIndexName(String indexName) {
      this.indexName = indexName;
      return this;
    }

    public Builder setGenerationId(long generationId) {
      this.generationId = generationId;
      return this;
    }

    public Builder setSkewMetrics(ClusterSkewMetrics skewMetrics) {
      this.skewMetrics = skewMetrics;
      return this;
    }

    /**
     * Decodes the serialized {@code SKEW_METRICS} payload, capturing decode failures to preserve
     * diagnostics for invalid or incompatible payloads.
     */
    public Builder setSkewMetricsBytes(byte[] skewMetricsBytes) {
      if (skewMetricsBytes == null) {
        this.skewMetrics = null;
        this.skewMetricsDecodeError = null;
        return this;
      }
      try {
        this.skewMetrics = ClusterSkewMetrics.fromBytes(skewMetricsBytes);
        this.skewMetricsDecodeError = null;
      } catch (Exception e) {
        this.skewMetrics = null;
        this.skewMetricsDecodeError = skewMetricsBytes.length + " stored bytes did not decode: "
          + e.getClass().getSimpleName() + ": " + e.getMessage();
      }
      return this;
    }

    /** Records that the stored metrics blob could not be decoded. */
    public Builder setSkewMetricsDecodeError(String skewMetricsDecodeError) {
      this.skewMetricsDecodeError = skewMetricsDecodeError;
      return this;
    }

    public Builder setRebuildState(String rebuildState) {
      this.rebuildState = rebuildState;
      return this;
    }

    public Builder setTriggerReason(String triggerReason) {
      this.triggerReason = triggerReason;
      return this;
    }

    public Builder setLastRebuildTime(Long lastRebuildTime) {
      this.lastRebuildTime = lastRebuildTime;
      return this;
    }

    public Builder setLastScorecardUpdate(Long lastScorecardUpdate) {
      this.lastScorecardUpdate = lastScorecardUpdate;
      return this;
    }

    public GenerationSummary build() {
      return new GenerationSummary(indexName, generationId, skewMetrics, skewMetricsDecodeError,
        rebuildState, triggerReason, lastRebuildTime, lastScorecardUpdate);
    }
  }
}
