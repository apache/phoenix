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

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.db.DBInputFormat;
import org.apache.hadoop.mapreduce.lib.db.DBWritable;

/**
 * A minimal RecordReader that returns one dummy record per {@link org.apache.phoenix.query.KeyRange
 * KeyRange} carried by a {@link PhoenixInputSplit}.
 * <p>
 * Use this when your mapper:
 * <ul>
 * <li>Doesn't need actual row data from the RecordReader</li>
 * <li>Only needs split/region boundaries (accessible via {@code context.getInputSplit()})</li>
 * <li>Delegates all work to a server-side coprocessor</li>
 * </ul>
 * <p>
 * This avoids the overhead of scanning and returning all rows when the mapper only needs to be
 * triggered per region. The standard {@link PhoenixRecordReader} iterates through all rows, calling
 * {@code map()} for each row - which is wasteful when the mapper ignores the row data entirely.
 * <p>
 * <b>How it works:</b>
 * <ul>
 * <li>{@link #initialize(InputSplit, TaskAttemptContext)} reads the {@link PhoenixInputSplit}'s key
 * ranges to learn how many records to emit (one per range)</li>
 * <li>{@link #nextKeyValue()} returns {@code true} once per range, then {@code false}</li>
 * <li>This triggers {@code map()} once per range; for a coalesced split with N regions the mapper
 * runs {@code map()} N times, giving the framework per-range visibility</li>
 * <li>{@link #getProgress()} returns the fraction of ranges already consumed, so YARN sees real
 * mapper progress instead of a 0% to 100% jump at the end</li>
 * </ul>
 * @see PhoenixSyncTableInputFormat
 * @see PhoenixRecordReader
 */
public class PhoenixNoOpPerRangeRecordReader extends RecordReader<NullWritable, DBWritable> {

  private int totalRanges = 1;
  private int consumedRanges = 0;

  /**
   * Initialize the RecordReader. Reads the number of key ranges from the {@link PhoenixInputSplit}
   * so subsequent {@link #nextKeyValue()} calls emit one record per range.
   * @param split   The InputSplit containing region boundaries
   * @param context The task context
   */
  @Override
  public void initialize(InputSplit split, TaskAttemptContext context) {
    if (split instanceof PhoenixInputSplit) {
      int rangeCount = ((PhoenixInputSplit) split).getKeyRanges().size();
      if (rangeCount > 0) {
        this.totalRanges = rangeCount;
      }
    }
  }

  /**
   * Returns true once per key range in the split, then false.
   * @return true while ranges remain unprocessed; false once all ranges have been emitted, which
   *         makes the Mapper task exit calling map method
   */
  @Override
  public boolean nextKeyValue() {
    if (consumedRanges < totalRanges) {
      consumedRanges++;
      return true;
    }
    return false;
  }

  /**
   * Returns a NullWritable key (mapper ignores this).
   * @return NullWritable singleton
   */
  @Override
  public NullWritable getCurrentKey() {
    return NullWritable.get();
  }

  /**
   * Returns a NullDBWritable value (mapper ignores this). The mapper extracts what it needs from
   * the InputSplit, not from this value.
   * @return A new NullDBWritable instance
   */
  @Override
  public DBWritable getCurrentValue() {
    return new DBInputFormat.NullDBWritable();
  }

  /**
   * Returns the fraction of ranges already consumed, so YARN reports real mapper progress as each
   * range completes (important for coalesced splits where a single mapper covers many regions).
   * @return progress in [0.0, 1.0]
   */
  @Override
  public float getProgress() {
    if (totalRanges == 0) {
      return 1.0f;
    }
    return ((float) consumedRanges) / totalRanges;
  }

  /**
   * Close the RecordReader. Nothing to close since we hold no resources.
   */
  @Override
  public void close() {
    // Nothing to close
  }
}
