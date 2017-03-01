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
import org.apache.phoenix.hbase.index.ValueGetter;
import org.apache.phoenix.hbase.index.covered.update.ColumnReference;
import org.apache.phoenix.hbase.index.covered.update.IndexedColumnGroup;

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
   * @return the attributes attached to the current update (e.g. {@link Mutation}).
   */
  public Map<String, byte[]> getUpdateAttributes();

  /**
   * Get a getter interface for the state of the index row
   * @param indexedColumns list of indexed columns.
 * @param ignoreNewerMutations ignore mutations newer than m when determining current state. Useful
   *        when replaying mutation state for partial index rebuild where writes succeeded to the data
   *        table, but not to the index table.
   */
  Pair<ValueGetter, IndexUpdate> getIndexUpdateState(
      Collection<? extends ColumnReference> indexedColumns, boolean ignoreNewerMutations, boolean returnNullScannerIfRowNotFound) throws IOException;

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