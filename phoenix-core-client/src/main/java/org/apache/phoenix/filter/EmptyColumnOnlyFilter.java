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
import java.util.List;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.exceptions.DeserializationException;
import org.apache.hadoop.hbase.filter.FilterBase;
import org.apache.hadoop.hbase.util.Writables;
import org.apache.hadoop.io.Writable;
import org.apache.phoenix.util.ScanUtil;

import org.apache.hbase.thirdparty.com.google.common.base.Preconditions;

/**
 * This filter returns only the empty column cell if it exists. If an empty column cell does not
 * exist, then it returns the first cell, that is, behaves like FirstKeyOnlyFilter
 */
public class EmptyColumnOnlyFilter extends FilterBase implements Writable {
  private byte[] emptyCF;
  private byte[] emptyCQ;
  private boolean found = false;
  private boolean first = true;
  private Cell emptyColumnCell = null;

  public EmptyColumnOnlyFilter() {
  }

  public EmptyColumnOnlyFilter(byte[] emptyCF, byte[] emptyCQ) {
    Preconditions.checkArgument(emptyCF != null, "Column family must not be null");
    Preconditions.checkArgument(emptyCQ != null, "Column qualifier must not be null");
    this.emptyCF = emptyCF;
    this.emptyCQ = emptyCQ;
  }

  @Override
  public void reset() throws IOException {
    found = false;
    first = true;
    emptyColumnCell = null;
  }

  // No @Override for HBase 3 compatibility
  public ReturnCode filterKeyValue(final Cell c) throws IOException {
    return filterCell(c);
  }

  @Override
  public ReturnCode filterCell(final Cell cell) throws IOException {
    if (found) {
      return ReturnCode.NEXT_ROW;
    }
    if (ScanUtil.isEmptyColumn(cell, emptyCF, emptyCQ)) {
      found = true;
      emptyColumnCell = cell;
      return ReturnCode.INCLUDE;
    }
    if (first) {
      first = false;
      return ReturnCode.INCLUDE;
    }
    return ReturnCode.NEXT_COL;
  }

  @Override
  public void filterRowCells(List<Cell> kvs) throws IOException {
    if (kvs.size() > 2) {
        throw new IOException("EmptyColumnOnlyFilter got unexpected cells: " + kvs.size());
    } else if (kvs.size() == 2) {
      // remove the first cell and only return the empty column cell
      kvs.remove(0);
    } else if (kvs.size() == 1) {
      // we only have 1 cell, check if it is the empty column cell or not
      // since the empty column cell could have been excluded by another filter like the
      // DistinctPrefixFilter.
      Cell cell = kvs.get(0);
      if (found && !ScanUtil.isEmptyColumn(cell, emptyCF, emptyCQ)) {
        // we found the empty cell, but it was not included so replace the existing cell
        // with the empty column cell
        kvs.remove(0);
        kvs.add(emptyColumnCell);
      }
    }
  }

  public static EmptyColumnOnlyFilter parseFrom(final byte[] pbBytes)
    throws DeserializationException {
    try {
      return (EmptyColumnOnlyFilter) Writables.getWritable(pbBytes, new EmptyColumnOnlyFilter());
    } catch (IOException e) {
      throw new DeserializationException(e);
    }
  }

  @Override
  public void write(DataOutput out) throws IOException {
    out.writeInt(emptyCF.length);
    out.write(emptyCF);
    out.writeInt(emptyCQ.length);
    out.write(emptyCQ);
  }

  @Override
  public void readFields(DataInput in) throws IOException {
    int length = in.readInt();
    emptyCF = new byte[length];
    in.readFully(emptyCF, 0, length);
    length = in.readInt();
    emptyCQ = new byte[length];
    in.readFully(emptyCQ, 0, length);
  }

  @Override
  public byte[] toByteArray() throws IOException {
    return Writables.getBytes(this);
  }
}
