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
package org.apache.phoenix.mapreduce;

import java.sql.Timestamp;
import java.util.Arrays;
import java.util.Objects;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.util.Bytes;

import org.apache.phoenix.thirdparty.com.google.common.annotations.VisibleForTesting;

/**
 * Data model class representing required row in the PHOENIX_SYNC_TABLE_CHECKPOINT table
 */
public class PhoenixSyncTableOutputRow {

  public enum Type {
    CHUNK,
    MAPPER_REGION
  }

  public enum Status {
    VERIFIED,
    MISMATCHED
  }

  private String tableName;
  private String targetCluster;
  private Type type;
  private Long fromTime;
  private Long toTime;
  private Boolean isDryRun;
  private byte[] startRowKey;
  private byte[] endRowKey;
  private Boolean isFirstRegion;
  private Timestamp executionStartTime;
  private Timestamp executionEndTime;
  private Status status;
  private String counters;

  @Override
  public String toString() {
    return String.format("SyncOutputRow[table=%s, target=%s, type=%s, start=%s, end=%s, status=%s]",
      tableName, targetCluster, type, Bytes.toStringBinary(startRowKey),
      Bytes.toStringBinary(endRowKey), status);
  }

  @Override
  @VisibleForTesting
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    PhoenixSyncTableOutputRow that = (PhoenixSyncTableOutputRow) o;
    return Objects.equals(tableName, that.tableName)
      && Objects.equals(targetCluster, that.targetCluster) && type == that.type
      && Objects.equals(fromTime, that.fromTime) && Objects.equals(toTime, that.toTime)
      && Objects.equals(isDryRun, that.isDryRun) && Arrays.equals(startRowKey, that.startRowKey)
      && Arrays.equals(endRowKey, that.endRowKey)
      && Objects.equals(isFirstRegion, that.isFirstRegion)
      && Objects.equals(executionStartTime, that.executionStartTime)
      && Objects.equals(executionEndTime, that.executionEndTime) && status == that.status
      && Objects.equals(counters, that.counters);
  }

  @Override
  public int hashCode() {
    int result = Objects.hash(tableName, targetCluster, type, fromTime, toTime, isDryRun,
      isFirstRegion, executionStartTime, executionEndTime, status, counters);
    result = 31 * result + Arrays.hashCode(startRowKey);
    result = 31 * result + Arrays.hashCode(endRowKey);
    return result;
  }

  @VisibleForTesting
  public String getTableName() {
    return tableName;
  }

  @VisibleForTesting
  public String getTargetCluster() {
    return targetCluster;
  }

  @VisibleForTesting
  public Type getType() {
    return type;
  }

  @VisibleForTesting
  public Long getFromTime() {
    return fromTime;
  }

  @VisibleForTesting
  public Long getToTime() {
    return toTime;
  }

  public byte[] getStartRowKey() {
    return startRowKey != null ? Arrays.copyOf(startRowKey, startRowKey.length) : null;
  }

  public byte[] getEndRowKey() {
    return endRowKey != null ? Arrays.copyOf(endRowKey, endRowKey.length) : null;
  }

  @VisibleForTesting
  public Timestamp getExecutionStartTime() {
    return executionStartTime;
  }

  @VisibleForTesting
  public Timestamp getExecutionEndTime() {
    return executionEndTime;
  }

  @VisibleForTesting
  public Status getStatus() {
    return status;
  }

  @VisibleForTesting
  public String getCounters() {
    return counters;
  }

  @VisibleForTesting
  public long getSourceRowsProcessed() {
    return parseCounterValue(PhoenixSyncTableMapper.SyncCounters.SOURCE_ROWS_PROCESSED.name());
  }

  @VisibleForTesting
  public long getTargetRowsProcessed() {
    return parseCounterValue(PhoenixSyncTableMapper.SyncCounters.TARGET_ROWS_PROCESSED.name());
  }

  @VisibleForTesting
  private long parseCounterValue(String counterName) {
    if (counters == null || counters.isEmpty()) {
      return 0;
    }

    String[] pairs = counters.split(",");
    for (String pair : pairs) {
      String[] keyValue = pair.split("=");
      if (keyValue.length == 2 && keyValue[0].trim().equals(counterName)) {
        return Long.parseLong(keyValue[1].trim());
      }
    }
    return 0;
  }

  /**
   * Builder for PhoenixSyncTableOutputRow
   */
  public static class Builder {
    private final PhoenixSyncTableOutputRow row;

    public Builder() {
      this.row = new PhoenixSyncTableOutputRow();
    }

    @VisibleForTesting
    public Builder setTableName(String tableName) {
      row.tableName = tableName;
      return this;
    }

    @VisibleForTesting
    public Builder setTargetCluster(String targetCluster) {
      row.targetCluster = targetCluster;
      return this;
    }

    @VisibleForTesting
    public Builder setType(Type type) {
      row.type = type;
      return this;
    }

    @VisibleForTesting
    public Builder setFromTime(Long fromTime) {
      row.fromTime = fromTime;
      return this;
    }

    @VisibleForTesting
    public Builder setToTime(Long toTime) {
      row.toTime = toTime;
      return this;
    }

    @VisibleForTesting
    public Builder setIsDryRun(Boolean isDryRun) {
      row.isDryRun = isDryRun;
      return this;
    }

    public Builder setStartRowKey(byte[] startRowKey) {
      row.startRowKey = startRowKey != null ? Arrays.copyOf(startRowKey, startRowKey.length) : null;
      return this;
    }

    public Builder setEndRowKey(byte[] endRowKey) {
      row.endRowKey = (endRowKey == null || endRowKey.length == 0)
        ? HConstants.EMPTY_END_ROW
        : Arrays.copyOf(endRowKey, endRowKey.length);
      return this;
    }

    @VisibleForTesting
    public Builder setIsFirstRegion(Boolean isFirstRegion) {
      row.isFirstRegion = isFirstRegion;
      return this;
    }

    @VisibleForTesting
    public Builder setExecutionStartTime(Timestamp executionStartTime) {
      row.executionStartTime = executionStartTime;
      return this;
    }

    @VisibleForTesting
    public Builder setExecutionEndTime(Timestamp executionEndTime) {
      row.executionEndTime = executionEndTime;
      return this;
    }

    @VisibleForTesting
    public Builder setStatus(Status status) {
      row.status = status;
      return this;
    }

    @VisibleForTesting
    public Builder setCounters(String counters) {
      row.counters = counters;
      return this;
    }

    public PhoenixSyncTableOutputRow build() {
      return row;
    }
  }
}
