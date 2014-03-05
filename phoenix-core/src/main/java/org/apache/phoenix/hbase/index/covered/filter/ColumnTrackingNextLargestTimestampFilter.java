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

import org.apache.phoenix.hbase.index.covered.update.ColumnTracker;

/**
 * Similar to the {@link MaxTimestampFilter}, but also updates the 'next largest' timestamp seen
 * that is not skipped by the below criteria. Note that it isn't as quick as the
 * {@link MaxTimestampFilter} as we can't just seek ahead to a key with the matching timestamp, but
 * have to iterate each kv until we find the right one with an allowed timestamp.
 * <p>
 * Inclusively filter on the maximum timestamp allowed. Excludes all elements greater than (but not
 * equal to) the given timestamp, so given ts = 5, a {@link KeyValue} with ts 6 is excluded, but not
 * one with ts = 5.
 * <p>
 * This filter generally doesn't make sense on its own - it should follow a per-column filter and
 * possible a per-delete filter to only track the most recent (but not exposed to the user)
 * timestamp.
 */
public class ColumnTrackingNextLargestTimestampFilter extends FilterBase {

  private long ts;
  private ColumnTracker column;

  public ColumnTrackingNextLargestTimestampFilter(long maxTime, ColumnTracker toTrack) {
    this.ts = maxTime;
    this.column = toTrack;
  }

  @Override
  public ReturnCode filterKeyValue(Cell v) {
    long timestamp = v.getTimestamp();
    if (timestamp > ts) {
      this.column.setTs(timestamp);
      return ReturnCode.SKIP;
    }
    return ReturnCode.INCLUDE;
  }

}