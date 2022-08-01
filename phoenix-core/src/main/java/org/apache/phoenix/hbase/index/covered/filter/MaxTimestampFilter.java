/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.phoenix.hbase.index.covered.filter;

import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.filter.FilterBase;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.phoenix.util.PhoenixKeyValueUtil;

/**
 * Inclusive filter on the maximum timestamp allowed. Excludes all elements greater than (but not
 * equal to) the given timestamp, so given ts = 5, a {@link KeyValue} with ts 6 is excluded, but not
 * one with ts = 5.
 */
public class MaxTimestampFilter extends FilterBase {

  private long ts;

  public MaxTimestampFilter(long maxTime) {
    this.ts = maxTime;
  }

  @Override
  public Cell getNextCellHint(Cell currentKV) {
    // this might be a little excessive right now - better safe than sorry though, so we don't mess
    // with other filters too much.
    KeyValue kv = null;
    try {
        kv = PhoenixKeyValueUtil.maybeCopyCell(currentKV).clone();
    } catch (CloneNotSupportedException e) {
        // the exception should not happen at all
        throw new IllegalArgumentException(e);
    }
    kv.setTimestamp(ts);
    return kv;
  }

  @Override
  public ReturnCode filterKeyValue(Cell v) {
    return this.filterCell(v);
  }

  @Override
  public ReturnCode filterCell(Cell v) {
    long timestamp = v.getTimestamp();
    if (timestamp > ts) {
      return ReturnCode.SEEK_NEXT_USING_HINT;
    }
    return ReturnCode.INCLUDE;
  }
}