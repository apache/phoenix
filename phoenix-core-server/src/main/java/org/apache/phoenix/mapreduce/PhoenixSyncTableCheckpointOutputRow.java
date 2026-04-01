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
public class PhoenixSyncTableCheckpointOutputRow {

  public enum Type {
    CHUNK,
    REGION
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
  private String tenantId;
  private Boolean isDryRun;
  private byte[] startRowKey;
  private byte[] endRowKey;
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
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    PhoenixSyncTableCheckpointOutputRow that = (PhoenixSyncTableCheckpointOutputRow) o;
    return Objects.equals(tableName, that.tableName)
      && Objects.equals(targetCluster, that.targetCluster) && type == that.type
      && Objects.equals(fromTime, that.fromTime) && Objects.equals(toTime, that.toTime)
      && Objects.equals(tenantId, that.tenantId) && Objects.equals(isDryRun, that.isDryRun)
      && Arrays.equals(startRowKey, that.startRowKey) && Arrays.equals(endRowKey, that.endRowKey)
      && Objects.equals(executionStartTime, that.executionStartTime)
      && Objects.equals(executionEndTime, that.executionEndTime) && status == that.status
      && Objects.equals(counters, that.counters);
  }

  @Override
  public int hashCode() {
    int result = Objects.hash(tableName, targetCluster, type, fromTime, toTime, tenantId, isDryRun,
      executionStartTime, executionEndTime, status, counters);
    result = 31 * result + Arrays.hashCode(startRowKey);
    result = 31 * result + Arrays.hashCode(endRowKey);
    return result;
  }

  public String getTableName() {
    return tableName;
  }

  public String getTargetCluster() {
    return targetCluster;
  }

  public Type getType() {
    return type;
  }

  public Long getFromTime() {
    return fromTime;
  }

  public Long getToTime() {
    return toTime;
  }

  public String getTenantId() {
    return tenantId;
  }

  public Boolean getIsDryRun() {
    return isDryRun;
  }

  public byte[] getStartRowKey() {
    return startRowKey != null ? Arrays.copyOf(startRowKey, startRowKey.length) : null;
  }

  public byte[] getEndRowKey() {
    return endRowKey != null ? Arrays.copyOf(endRowKey, endRowKey.length) : null;
  }

  public Timestamp getExecutionStartTime() {
    return executionStartTime;
  }

  public Timestamp getExecutionEndTime() {
    return executionEndTime;
  }

  public Status getStatus() {
    return status;
  }

  public String getCounters() {
    return counters;
  }

  @VisibleForTesting
  public long getSourceRowsProcessed() {
    return CounterFormatter.parseSourceRows(counters);
  }

  @VisibleForTesting
  public long getTargetRowsProcessed() {
    return CounterFormatter.parseTargetRows(counters);
  }

  /**
   * Utility class for formatting and parsing counter strings. Encapsulates the counter format
   * contract to ensure consistency between formatting (in mapper) and parsing (in tests).
   */
  public static class CounterFormatter {
    private static final String FORMAT_CHUNK = "%s=%d,%s=%d";
    private static final String FORMAT_MAPPER = "%s=%d,%s=%d,%s=%d,%s=%d";

    /**
     * Formats chunk counters as comma-separated key=value pairs.
     * @param sourceRows Source rows processed
     * @param targetRows Target rows processed
     * @return Formatted string: "SOURCE_ROWS_PROCESSED=123,TARGET_ROWS_PROCESSED=456"
     */
    public static String formatChunk(long sourceRows, long targetRows) {
      return String.format(FORMAT_CHUNK,
        PhoenixSyncTableMapper.SyncCounters.SOURCE_ROWS_PROCESSED.name(), sourceRows,
        PhoenixSyncTableMapper.SyncCounters.TARGET_ROWS_PROCESSED.name(), targetRows);
    }

    /**
     * Formats mapper counters as comma-separated key=value pairs.
     * @param chunksVerified   Chunks verified count
     * @param chunksMismatched Chunks mismatched count
     * @param sourceRows       Source rows processed
     * @param targetRows       Target rows processed
     * @return Formatted string with all mapper counters
     */
    public static String formatMapper(long chunksVerified, long chunksMismatched, long sourceRows,
      long targetRows) {
      return String.format(FORMAT_MAPPER,
        PhoenixSyncTableMapper.SyncCounters.CHUNKS_VERIFIED.name(), chunksVerified,
        PhoenixSyncTableMapper.SyncCounters.CHUNKS_MISMATCHED.name(), chunksMismatched,
        PhoenixSyncTableMapper.SyncCounters.SOURCE_ROWS_PROCESSED.name(), sourceRows,
        PhoenixSyncTableMapper.SyncCounters.TARGET_ROWS_PROCESSED.name(), targetRows);
    }

    /**
     * Parses SOURCE_ROWS_PROCESSED value from counter string.
     * @param counters Counter string in format "KEY1=val1,KEY2=val2,..."
     * @return Source rows processed, or 0 if not found
     */
    public static long parseSourceRows(String counters) {
      return parseCounterValue(counters,
        PhoenixSyncTableMapper.SyncCounters.SOURCE_ROWS_PROCESSED.name());
    }

    /**
     * Parses TARGET_ROWS_PROCESSED value from counter string.
     * @param counters Counter string in format "KEY1=val1,KEY2=val2,..."
     * @return Target rows processed, or 0 if not found
     */
    public static long parseTargetRows(String counters) {
      return parseCounterValue(counters,
        PhoenixSyncTableMapper.SyncCounters.TARGET_ROWS_PROCESSED.name());
    }

    /**
     * Parses a specific counter value from the comma-separated counter string.
     * @param counters    Counter string in format "KEY1=val1,KEY2=val2,..."
     * @param counterName Name of the counter to extract
     * @return Counter value, or 0 if not found
     */
    private static long parseCounterValue(String counters, String counterName) {
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
  }

  /**
   * Builder for PhoenixSyncTableCheckpointOutputRow
   */
  public static class Builder {
    private final PhoenixSyncTableCheckpointOutputRow row;

    public Builder() {
      this.row = new PhoenixSyncTableCheckpointOutputRow();
    }

    public Builder setTableName(String tableName) {
      row.tableName = tableName;
      return this;
    }

    public Builder setTargetCluster(String targetCluster) {
      row.targetCluster = targetCluster;
      return this;
    }

    public Builder setType(Type type) {
      row.type = type;
      return this;
    }

    public Builder setFromTime(Long fromTime) {
      row.fromTime = fromTime;
      return this;
    }

    public Builder setToTime(Long toTime) {
      row.toTime = toTime;
      return this;
    }

    public Builder setTenantId(String tenantId) {
      row.tenantId = tenantId;
      return this;
    }

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

    public Builder setExecutionStartTime(Timestamp executionStartTime) {
      row.executionStartTime = executionStartTime;
      return this;
    }

    public Builder setExecutionEndTime(Timestamp executionEndTime) {
      row.executionEndTime = executionEndTime;
      return this;
    }

    public Builder setStatus(Status status) {
      row.status = status;
      return this;
    }

    public Builder setCounters(String counters) {
      row.counters = counters;
      return this;
    }

    public PhoenixSyncTableCheckpointOutputRow build() {
      return row;
    }
  }
}
