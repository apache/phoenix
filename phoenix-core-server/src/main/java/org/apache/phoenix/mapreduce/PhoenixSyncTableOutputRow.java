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

import java.util.Arrays;
import org.apache.hadoop.hbase.util.Bytes;

/**
 * Data model class representing a row in the PHOENIX_SYNC_TABLE_CHECKPOINT table
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

  private byte[] startRowKey;
  private byte[] endRowKey;

  @Override
  public String toString() {
    return String.format("SyncOutputRow[start=%s, end=%s]", Bytes.toStringBinary(startRowKey),
      Bytes.toStringBinary(endRowKey));
  }

  public byte[] getStartRowKey() {
    return startRowKey;
  }

  public byte[] getEndRowKey() {
    return endRowKey;
  }

  /**
   * Builder for PhoenixSyncTableOutputRow
   */
  public static class Builder {
    private final PhoenixSyncTableOutputRow row;

    public Builder() {
      this.row = new PhoenixSyncTableOutputRow();
    }

    public Builder setStartRowKey(byte[] startRowKey) {
      row.startRowKey = startRowKey != null ? Arrays.copyOf(startRowKey, startRowKey.length) : null;
      return this;
    }

    public Builder setEndRowKey(byte[] endRowKey) {
      row.endRowKey = endRowKey != null ? Arrays.copyOf(endRowKey, endRowKey.length) : null;
      return this;
    }

    public PhoenixSyncTableOutputRow build() {
      if (row.startRowKey == null) {
        throw new IllegalStateException("Start row key is required");
      }
      return row;
    }
  }
}
