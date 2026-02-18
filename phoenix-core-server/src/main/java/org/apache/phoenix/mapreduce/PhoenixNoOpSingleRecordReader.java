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
 * A minimal RecordReader that returns exactly one dummy record per InputSplit.
 * <p>
 * Use this when your mapper:
 * <ul>
 * <li>Doesn't need actual row data from the RecordReader</li>
 * <li>Only needs split/region boundaries (accessible via {@code context.getInputSplit()})</li>
 * <li>Delegates all work to a server-side coprocessor</li>
 * </ul>
 * <p>
 * This avoids the overhead of scanning and returning all rows when the mapper only needs to be
 * triggered once per region/split. The standard {@link PhoenixRecordReader} iterates through all
 * rows, calling {@code map()} for each row - which is wasteful when the mapper ignores the row data
 * entirely.
 * <p>
 * <b>How it works:</b>
 * <ul>
 * <li>{@link #nextKeyValue()} returns {@code true} exactly once, then {@code false}</li>
 * <li>This triggers {@code map()} exactly once per InputSplit (region)</li>
 * <li>The mapper extracts region boundaries from the InputSplit, not from records</li>
 * </ul>
 * @see PhoenixSyncTableInputFormat
 * @see PhoenixRecordReader
 */
public class PhoenixNoOpSingleRecordReader extends RecordReader<NullWritable, DBWritable> {

  private boolean hasRecord = true;

  /**
   * Initialize the RecordReader. No initialization is needed since we return a single dummy record.
   * @param split   The InputSplit containing region boundaries
   * @param context The task context
   */
  @Override
  public void initialize(InputSplit split, TaskAttemptContext context) {
    // No initialization needed
  }

  /**
   * Returns true exactly once to trigger a single map() call per split.
   * @return true on first call, false on subsequent calls which makes Mapper task to exit calling
   *         map method
   */
  @Override
  public boolean nextKeyValue() {
    if (hasRecord) {
      hasRecord = false;
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
   * Returns progress: 0.0 before the record is consumed, 1.0 after.
   * @return 0.0f if record not yet consumed, 1.0f otherwise
   */
  @Override
  public float getProgress() {
    return hasRecord ? 0.0f : 1.0f;
  }

  /**
   * Close the RecordReader. Nothing to close since we hold no resources.
   */
  @Override
  public void close() {
    // Nothing to close
  }
}
