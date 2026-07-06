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
package org.apache.phoenix.hbase.index.covered.data;

import java.io.IOException;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.DoNotRetryIOException;
import org.apache.hadoop.hbase.client.Mutation;
import org.apache.phoenix.hbase.index.IndexRegionObserver.RowTsKey;
import org.apache.phoenix.hbase.index.covered.update.ColumnReference;
import org.apache.phoenix.hbase.index.util.ImmutableBytesPtr;
import org.apache.phoenix.util.IndexUtil;

import org.apache.phoenix.thirdparty.com.google.common.base.Preconditions;

/**
 * Standby-side {@link LocalHBaseState} that serves the local-index builder's prior-row-state lookup
 * from the per-batch pre-image the active shipped, instead of scanning the data-table region (which
 * {@link CachedLocalTable} does on the active).
 * <p>
 * On the standby the replication reader can concatenate several active-side batches into one replay
 * batch, so a row recurs at several active timestamps and the region does not yet hold the
 * intermediate state each batch saw. The active shipped one pre-image per row per batch, which is
 * exactly the prior row state that batch built its local index against. We therefore key the lookup
 * by {@code (row, ts)} — recovered from the mutation's row and max cell timestamp — and return that
 * group's own pre-image cells. Each group's build is thus an independent reproduction of the
 * corresponding active {@code preBatchMutate}; no chaining across groups is needed.
 * <p>
 * A {@code null} cell list is the "active saw an empty row" sentinel and is the documented
 * {@link LocalHBaseState#getCurrentRowState} return for "no prior row". A key that is absent
 * entirely (never populated) is a contract violation and throws, so the sentinel stays unambiguous.
 */
public class PreImageLocalTable implements LocalHBaseState {

  /** Distinct marker for "key absent", so a stored null (empty-row sentinel) is not mistaken. */
  private static final List<Cell> ABSENT = Collections.emptyList();

  private final Map<RowTsKey, List<Cell>> preImageCellsByRowTs;

  public PreImageLocalTable(Map<RowTsKey, List<Cell>> preImageCellsByRowTs) {
    this.preImageCellsByRowTs =
      Preconditions.checkNotNull(preImageCellsByRowTs, "preImageCellsByRowTs must not be null");
  }

  /**
   * {@inheritDoc}
   * <p>
   * {@code toCover} and {@code ignoreNewerMutations} are ignored: we already hold the exact
   * per-group prior-row snapshot the active built against, so there is nothing to narrow by column
   * or filter by timestamp. The {@code (row, ts)} key is derived from the mutation the same way
   * {@code IndexRegionObserver.buildReplicatedRowGroups} keys the map that populated this table.
   */
  @Override
  public List<Cell> getCurrentRowState(Mutation mutation,
    Collection<? extends ColumnReference> toCover, boolean ignoreNewerMutations)
    throws IOException {
    RowTsKey key =
      new RowTsKey(new ImmutableBytesPtr(mutation.getRow()), IndexUtil.getMaxTimestamp(mutation));
    // A stored null is the "active saw an empty row" sentinel, so a null value cannot be told from
    // an absent key via get() alone; look up once against a distinct ABSENT marker instead. A true
    // miss means the (row, ts) derivation drifted from the populating side
    // (buildReplayLocalIndexInputs); fail loud rather than return null, which the builder would
    // read
    // as the empty-row sentinel and silently regenerate the index against an empty prior state.
    List<Cell> cells = preImageCellsByRowTs.getOrDefault(key, ABSENT);
    if (cells == ABSENT) {
      throw new DoNotRetryIOException(
        "No pre-image for replayed local-index row; (row, ts) key not populated: " + key);
    }
    return cells;
  }
}
