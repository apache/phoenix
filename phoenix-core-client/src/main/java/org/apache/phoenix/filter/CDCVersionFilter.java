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
package org.apache.phoenix.filter;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.exceptions.DeserializationException;
import org.apache.hadoop.hbase.filter.FilterBase;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.Writables;
import org.apache.hadoop.io.Writable;
import org.apache.phoenix.hbase.index.util.ImmutableBytesPtr;

/**
 * Filter for CDC data table scans that prunes redundant cell versions. For each row, given a set of
 * change timestamps (from the CDC batch), this filter includes only:
 * <ul>
 * <li>Cells at a change timestamp (the change itself)</li>
 * <li>The first cell below each change timestamp per column (the pre-image)</li>
 * <li>All DeleteFamily markers (needed for CDC deletion tracking)</li>
 * </ul>
 * All other cell versions are skipped, reducing network I/O, memory, and processing overhead.
 * <p>
 * Within each column (family:qualifier), HBase delivers cells in timestamp-descending order. The
 * filter maintains a pointer into the sorted change timestamps and tracks whether a pre-image is
 * still needed, advancing through the timestamps as cells arrive.
 */
public class CDCVersionFilter extends FilterBase implements Writable {

  private Map<ImmutableBytesPtr, long[]> timestampMap;

  // Per-row state
  private long[] currentTimestamps;
  private byte[] currentRowKey;

  // Per-column state
  private byte[] prevFamily;
  private byte[] prevQualifier;
  private int tsIdx;
  private boolean needPreImage;

  public CDCVersionFilter() {
  }

  /**
   * @param timestampMap mapping of data row key to sorted (descending) array of change timestamps
   *                     for that row
   */
  public CDCVersionFilter(Map<ImmutableBytesPtr, long[]> timestampMap) {
    this.timestampMap = timestampMap;
  }

  @Override
  public void reset() throws IOException {
    currentTimestamps = null;
    currentRowKey = null;
    resetColumnState();
  }

  private void resetColumnState() {
    prevFamily = null;
    prevQualifier = null;
    tsIdx = 0;
    needPreImage = false;
  }

  private boolean isNewRow(Cell cell) {
    if (currentRowKey == null) {
      return true;
    }
    return !Bytes.equals(currentRowKey, 0, currentRowKey.length, cell.getRowArray(),
      cell.getRowOffset(), cell.getRowLength());
  }

  private void onNewRow(Cell cell) {
    currentRowKey = CellUtil.cloneRow(cell);
    ImmutableBytesPtr rowKeyPtr = new ImmutableBytesPtr(currentRowKey);
    currentTimestamps = timestampMap != null ? timestampMap.get(rowKeyPtr) : null;
    resetColumnState();
  }

  private boolean isNewColumn(Cell cell) {
    if (prevFamily == null) {
      return true;
    }
    if (
      !Bytes.equals(prevFamily, 0, prevFamily.length, cell.getFamilyArray(), cell.getFamilyOffset(),
        cell.getFamilyLength())
    ) {
      return true;
    }
    return !Bytes.equals(prevQualifier, 0, prevQualifier.length, cell.getQualifierArray(),
      cell.getQualifierOffset(), cell.getQualifierLength());
  }

  private void trackColumn(Cell cell) {
    prevFamily = CellUtil.cloneFamily(cell);
    prevQualifier = CellUtil.cloneQualifier(cell);
  }

  // No @Override for HBase 3 compatibility
  public ReturnCode filterKeyValue(Cell v) throws IOException {
    return filterCell(v);
  }

  @Override
  public ReturnCode filterCell(Cell cell) throws IOException {
    if (isNewRow(cell)) {
      onNewRow(cell);
    }

    if (currentTimestamps == null || currentTimestamps.length == 0) {
      return ReturnCode.INCLUDE;
    }

    Cell.Type type = cell.getType();
    if (type == Cell.Type.DeleteFamily || type == Cell.Type.DeleteFamilyVersion) {
      return ReturnCode.INCLUDE;
    }

    if (isNewColumn(cell)) {
      trackColumn(cell);
      tsIdx = 0;
      needPreImage = false;
    }

    long cellTs = cell.getTimestamp();

    // Advance past change timestamps that are above this cell's timestamp.
    // These timestamps had no cell for this column, but we still need a pre-image
    // below them (the first cell we encounter serves as pre-image for all of them).
    while (tsIdx < currentTimestamps.length && currentTimestamps[tsIdx] > cellTs) {
      needPreImage = true;
      tsIdx++;
    }

    if (tsIdx < currentTimestamps.length && cellTs == currentTimestamps[tsIdx]) {
      needPreImage = true;
      tsIdx++;
      return ReturnCode.INCLUDE;
    }

    if (needPreImage) {
      needPreImage = false;
      return ReturnCode.INCLUDE;
    }

    if (tsIdx >= currentTimestamps.length) {
      return ReturnCode.NEXT_COL;
    }

    return ReturnCode.SKIP;
  }

  @Override
  public byte[] toByteArray() throws IOException {
    return Writables.getBytes(this);
  }

  public static CDCVersionFilter parseFrom(byte[] pbBytes) throws DeserializationException {
    try {
      return (CDCVersionFilter) Writables.getWritable(pbBytes, new CDCVersionFilter());
    } catch (IOException e) {
      throw new DeserializationException(e);
    }
  }

  @Override
  public void write(DataOutput out) throws IOException {
    if (timestampMap == null) {
      out.writeInt(0);
      return;
    }
    out.writeInt(timestampMap.size());
    for (Map.Entry<ImmutableBytesPtr, long[]> entry : timestampMap.entrySet()) {
      ImmutableBytesPtr key = entry.getKey();
      long[] timestamps = entry.getValue();
      out.writeInt(key.getLength());
      out.write(key.get(), key.getOffset(), key.getLength());
      out.writeInt(timestamps.length);
      for (long ts : timestamps) {
        out.writeLong(ts);
      }
    }
  }

  @Override
  public void readFields(DataInput in) throws IOException {
    int numRows = in.readInt();
    if (numRows < 0) {
      throw new IOException("Invalid CDCVersionFilter row count: " + numRows);
    }
    timestampMap = new HashMap<>();
    for (int i = 0; i < numRows; i++) {
      int keyLen = in.readInt();
      if (keyLen < 0) {
        throw new IOException("Invalid CDCVersionFilter row key length: " + keyLen);
      }
      byte[] keyBytes = new byte[keyLen];
      in.readFully(keyBytes);
      int numTs = in.readInt();
      if (numTs < 0) {
        throw new IOException("Invalid CDCVersionFilter timestamp count: " + numTs);
      }
      long[] timestamps = new long[numTs];
      for (int j = 0; j < numTs; j++) {
        timestamps[j] = in.readLong();
      }
      timestampMap.put(new ImmutableBytesPtr(keyBytes), timestamps);
    }
  }
}
