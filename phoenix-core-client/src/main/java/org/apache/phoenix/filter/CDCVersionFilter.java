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
 * Filter for CDC data table scans that prunes redundant cell versions. For each data row the filter
 * is given the {@code [min, max]} band of change timestamps for that row (the oldest and newest CDC
 * change seen in the current scan). Per column (family:qualifier), with cells delivered in
 * timestamp-descending order, it includes only:
 * <ul>
 * <li>cells whose timestamp is within {@code [min, max]} (the change cells), and</li>
 * <li>the first cell strictly below {@code min} (the pre-image for the oldest change), and</li>
 * <li>all DeleteFamily / DeleteFamilyVersion markers (needed for CDC deletion tracking).</li>
 * </ul>
 * Cells newer than {@code max}, and all but the first cell older than {@code min}, are skipped,
 * reducing network I/O, memory, and processing overhead on the data table scan.
 */
public class CDCVersionFilter extends FilterBase implements Writable {

  private Map<ImmutableBytesPtr, long[]> rangeMap;

  // Per-row state
  private long[] currentRange;
  private byte[] currentRowKey;

  // Per-column state
  private byte[] prevFamily;
  private byte[] prevQualifier;

  private boolean belowMinEmitted;

  public CDCVersionFilter() {
  }

  /**
   * @param rangeMap mapping of data row key to a 2-element {@code [min, max]} array giving the
   *                 oldest and newest change timestamp for that row
   */
  public CDCVersionFilter(Map<ImmutableBytesPtr, long[]> rangeMap) {
    this.rangeMap = rangeMap;
  }

  @Override
  public void reset() throws IOException {
    currentRange = null;
    currentRowKey = null;
    resetColumnState();
  }

  private void resetColumnState() {
    prevFamily = null;
    prevQualifier = null;
    belowMinEmitted = false;
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
    currentRange = rangeMap != null ? rangeMap.get(rowKeyPtr) : null;
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

    if (currentRange == null) {
      return ReturnCode.INCLUDE;
    }

    Cell.Type type = cell.getType();
    if (type == Cell.Type.DeleteFamily || type == Cell.Type.DeleteFamilyVersion) {
      return ReturnCode.INCLUDE;
    }

    if (isNewColumn(cell)) {
      trackColumn(cell);
      belowMinEmitted = false;
    }

    long cellTs = cell.getTimestamp();
    long minChangeTs = currentRange[0];
    long maxChangeTs = currentRange[1];

    if (cellTs > maxChangeTs) {
      return ReturnCode.SKIP;
    }
    if (cellTs >= minChangeTs) {
      return ReturnCode.INCLUDE;
    }
    if (!belowMinEmitted) {
      belowMinEmitted = true;
      return ReturnCode.INCLUDE;
    }
    return ReturnCode.NEXT_COL;
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
    if (rangeMap == null) {
      out.writeInt(0);
      return;
    }
    out.writeInt(rangeMap.size());
    for (Map.Entry<ImmutableBytesPtr, long[]> entry : rangeMap.entrySet()) {
      ImmutableBytesPtr key = entry.getKey();
      long[] range = entry.getValue();
      out.writeInt(key.getLength());
      out.write(key.get(), key.getOffset(), key.getLength());
      out.writeLong(range[0]);
      out.writeLong(range[1]);
    }
  }

  @Override
  public void readFields(DataInput in) throws IOException {
    int numRows = in.readInt();
    if (numRows < 0) {
      throw new IOException("Invalid CDCVersionFilter row count: " + numRows);
    }
    rangeMap = new HashMap<>();
    for (int i = 0; i < numRows; i++) {
      int keyLen = in.readInt();
      if (keyLen < 0) {
        throw new IOException("Invalid CDCVersionFilter row key length: " + keyLen);
      }
      byte[] keyBytes = new byte[keyLen];
      in.readFully(keyBytes);
      long minChangeTs = in.readLong();
      long maxChangeTs = in.readLong();
      rangeMap.put(new ImmutableBytesPtr(keyBytes), new long[] { minChangeTs, maxChangeTs });
    }
  }
}
