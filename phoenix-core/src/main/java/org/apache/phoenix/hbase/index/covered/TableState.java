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

package org.apache.phoenix.hbase.index.covered;

import java.io.IOException;
import java.util.Collection;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.Mutation;
import org.apache.hadoop.hbase.coprocessor.RegionCoprocessorEnvironment;
import org.apache.hadoop.hbase.util.Pair;

import org.apache.phoenix.hbase.index.covered.update.ColumnReference;
import org.apache.phoenix.hbase.index.covered.update.IndexedColumnGroup;
import org.apache.phoenix.hbase.index.scanner.Scanner;

/**
 * Interface for the current state of the table. This is generally going to be as of a timestamp - a
 * view on the current state of the HBase table - so you don't have to worry about exposing too much
 * information.
 */
public interface TableState {

  // use this to get batch ids/ptable stuff
  /**
   * WARNING: messing with this can affect the indexing plumbing. Use with caution :)
   * @return get the current environment in which this table lives.
   */
  public RegionCoprocessorEnvironment getEnvironment();

  /**
   * @return the current timestamp up-to-which we are releasing table state.
   */
  public long getCurrentTimestamp();

  /**
   * Set the current timestamp up to which the table should allow access to the underlying table.
   * This overrides the timestamp view provided by the indexer - use with care!
   * @param timestamp timestamp up to which the table should allow access.
   */
  public void setCurrentTimestamp(long timestamp);

  /**
   * @return the attributes attached to the current update (e.g. {@link Mutation}).
   */
  public Map<String, byte[]> getUpdateAttributes();

  /**
   * Get a scanner on the columns that are needed by the index.
   * <p>
   * The returned scanner is already pre-seeked to the first {@link KeyValue} that matches the given
   * columns with a timestamp earlier than the timestamp to which the table is currently set (the
   * current state of the table for which we need to build an update).
   * <p>
   * If none of the passed columns matches any of the columns in the pending update (as determined
   * by {@link ColumnReference#matchesFamily(byte[])} and
   * {@link ColumnReference#matchesQualifier(byte[])}, then an empty scanner will be returned. This
   * is because it doesn't make sense to build index updates when there is no change in the table
   * state for any of the columns you are indexing.
   * <p>
   * <i>NOTE:</i> This method should <b>not</b> be used during
   * {@link IndexCodec#getIndexDeletes(TableState)} as the pending update will not yet have been
   * applied - you are merely attempting to cleanup the current state and therefore do <i>not</i>
   * need to track the indexed columns.
   * <p>
   * As a side-effect, we update a timestamp for the next-most-recent timestamp for the columns you
   * request - you will never see a column with the timestamp we are tracking, but the next oldest
   * timestamp for that column.
   * @param indexedColumns the columns to that will be indexed
   * @return an iterator over the columns and the {@link IndexUpdate} that should be passed back to
   *         the builder. Even if no update is necessary for the requested columns, you still need
   *         to return the {@link IndexUpdate}, just don't set the update for the
   *         {@link IndexUpdate}.
   * @throws IOException
   */
  Pair<Scanner, IndexUpdate> getIndexedColumnsTableState(
      Collection<? extends ColumnReference> indexedColumns) throws IOException;

  /**
   * @return the row key for the current row for which we are building an index update.
   */
  byte[] getCurrentRowKey();

  /**
   * Get the 'hint' for the columns that were indexed last time for the same set of keyvalues.
   * Generally, this will only be used when fixing up a 'back in time' put or delete as we need to
   * fix up all the indexes that reference the changed columns.
   * @return the hint the index columns that were queried on the last iteration for the changed
   *         column
   */
  List<? extends IndexedColumnGroup> getIndexColumnHints();

  /**
   * Can be used to help the codec to determine which columns it should attempt to index.
   * @return the keyvalues in the pending update to the table.
   */
  Collection<KeyValue> getPendingUpdate();
}