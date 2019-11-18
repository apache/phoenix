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
package org.apache.phoenix.hbase.index.builder;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import com.google.common.collect.ListMultimap;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.Stoppable;
import org.apache.hadoop.hbase.client.Increment;
import org.apache.hadoop.hbase.client.Mutation;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.coprocessor.RegionCoprocessorEnvironment;
import org.apache.hadoop.hbase.regionserver.MiniBatchOperationInProgress;
import org.apache.hadoop.hbase.regionserver.Region;
import org.apache.hadoop.hbase.regionserver.RegionScanner;
import org.apache.hadoop.hbase.util.Pair;
import org.apache.phoenix.compile.ScanRanges;
import org.apache.phoenix.coprocessor.BaseScannerRegionObserver;
import org.apache.phoenix.coprocessor.BaseScannerRegionObserver.ReplayWrite;
import org.apache.phoenix.filter.SkipScanFilter;
import org.apache.phoenix.hbase.index.Indexer;
import org.apache.phoenix.hbase.index.covered.IndexMetaData;
import org.apache.phoenix.hbase.index.table.HTableInterfaceReference;
import org.apache.phoenix.hbase.index.util.ImmutableBytesPtr;
import org.apache.phoenix.hbase.index.covered.data.CachedLocalTable;
import org.apache.phoenix.hbase.index.covered.update.ColumnReference;
import org.apache.phoenix.hbase.index.util.IndexManagementUtil;
import org.apache.phoenix.index.IndexMaintainer;
import org.apache.phoenix.index.PhoenixIndexMetaData;
import org.apache.phoenix.query.KeyRange;
import org.apache.phoenix.schema.types.PVarbinary;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.Sets;

import java.util.HashMap;

/**
 * Manage the building of index updates from primary table updates.
 */
public class IndexBuildManager implements Stoppable {

  private static final Logger LOGGER = LoggerFactory.getLogger(IndexBuildManager.class);
  private final IndexBuilder delegate;
  private boolean stopped;
  private RegionCoprocessorEnvironment regionCoprocessorEnvironment;

  /**
   * @param env environment in which <tt>this</tt> is running. Used to setup the
   *          {@link IndexBuilder} and executor
   * @throws IOException if an {@link IndexBuilder} cannot be correctly steup
   */
  public IndexBuildManager(RegionCoprocessorEnvironment env) throws IOException {
    // Prevent deadlock by using single thread for all reads so that we know
    // we can get the ReentrantRWLock. See PHOENIX-2671 for more details.
    this.delegate = getIndexBuilder(env);
    this.regionCoprocessorEnvironment = env;
  }
  
  private static IndexBuilder getIndexBuilder(RegionCoprocessorEnvironment e) throws IOException {
    Configuration conf = e.getConfiguration();
    Class<? extends IndexBuilder> builderClass =
        conf.getClass(Indexer.INDEX_BUILDER_CONF_KEY, null, IndexBuilder.class);
    try {
      IndexBuilder builder = builderClass.newInstance();
      builder.setup(e);
      return builder;
    } catch (InstantiationException e1) {
      throw new IOException("Couldn't instantiate index builder:" + builderClass
          + ", disabling indexing on table " + e.getRegion().getTableDesc().getNameAsString());
    } catch (IllegalAccessException e1) {
      throw new IOException("Couldn't instantiate index builder:" + builderClass
          + ", disabling indexing on table " + e.getRegion().getTableDesc().getNameAsString());
    }
  }

  public IndexMetaData getIndexMetaData(MiniBatchOperationInProgress<Mutation> miniBatchOp) throws IOException {
      return this.delegate.getIndexMetaData(miniBatchOp);
  }

  public void getIndexUpdates(ListMultimap<HTableInterfaceReference, Pair<Mutation, byte[]>> indexUpdates,
      MiniBatchOperationInProgress<Mutation> miniBatchOp,
      Collection<? extends Mutation> mutations,
      IndexMetaData indexMetaData) throws Throwable {
    // notify the delegate that we have started processing a batch
    this.delegate.batchStarted(miniBatchOp, indexMetaData);
    CachedLocalTable cachedLocalTable =
            preScanAllRequiredRows(
                    mutations,
                    (PhoenixIndexMetaData)indexMetaData,
                    this.regionCoprocessorEnvironment);
    // Avoid the Object overhead of the executor when it's not actually parallelizing anything.
    for (Mutation m : mutations) {
      Collection<Pair<Mutation, byte[]>> updates = delegate.getIndexUpdate(m, indexMetaData, cachedLocalTable);
      for (Pair<Mutation, byte[]> update : updates) {
        indexUpdates.put(new HTableInterfaceReference(new ImmutableBytesPtr(update.getSecond())), new Pair<>(update.getFirst(), m.getRow()));
      }
    }
  }

  @VisibleForTesting
  public static CachedLocalTable preScanAllRequiredRows(
          Collection<? extends Mutation> dataTableMutationsWithSameRowKeyAndTimestamp,
          final PhoenixIndexMetaData indexMetaData,
          RegionCoprocessorEnvironment regionCoprocessorEnvironment) throws IOException {
      List<IndexMaintainer> indexTableMaintainers = indexMetaData.getIndexMaintainers();
      Set<KeyRange> keys = new HashSet<KeyRange>(dataTableMutationsWithSameRowKeyAndTimestamp.size());
      for (Mutation mutation : dataTableMutationsWithSameRowKeyAndTimestamp) {
          keys.add(PVarbinary.INSTANCE.getKeyRange(mutation.getRow()));
      }

      Set<ColumnReference> getterColumnReferences = Sets.newHashSet();
      for (IndexMaintainer indexTableMaintainer : indexTableMaintainers)
      {
          getterColumnReferences.addAll(
                  indexTableMaintainer.getAllColumns());

      }

      getterColumnReferences.add(new ColumnReference(
              indexTableMaintainers.get(0).getDataEmptyKeyValueCF(),
              indexTableMaintainers.get(0).getEmptyKeyValueQualifier()));

      Scan scan = IndexManagementUtil.newLocalStateScan(
              Collections.singletonList(getterColumnReferences));
      ScanRanges scanRanges = ScanRanges.createPointLookup(new ArrayList<KeyRange>(keys));
      scanRanges.initializeScan(scan);
      scan.setFilter(new SkipScanFilter(scanRanges.getSkipScanFilter(), true));

      if(indexMetaData.getReplayWrite() != null) {
          long timestamp = getMaxTimestamp(dataTableMutationsWithSameRowKeyAndTimestamp);
          scan.setTimeRange(0, timestamp);
      }

      Region region = regionCoprocessorEnvironment.getRegion();
      HashMap<ImmutableBytesPtr, List<Cell>> rowKeyPtrToCells =
              new HashMap<ImmutableBytesPtr, List<Cell>>();
      try (RegionScanner scanner = region.getScanner(scan)) {
          boolean more = true;
          while(more)
          {
              List<Cell> cells = new ArrayList<Cell>();
              more = scanner.next(cells);
              if (cells.isEmpty()) {
                  continue;
              }
              Cell cell = cells.get(0);
              byte[] rowKey = CellUtil.cloneRow(cell);
              rowKeyPtrToCells.put(new ImmutableBytesPtr(rowKey), cells);
          }
      }

      return new CachedLocalTable(rowKeyPtrToCells);
  }

  private static long getMaxTimestamp(Collection<? extends Mutation> dataTableMutationsWithSameRowKeyAndTimestamp){
      long maxTimestamp = Long.MIN_VALUE;
      for(Mutation mutation : dataTableMutationsWithSameRowKeyAndTimestamp) {
          long timestamp = mutation.getFamilyCellMap().values().iterator().next().get(0).getTimestamp();
          if(timestamp > maxTimestamp) {
              maxTimestamp = timestamp;
          }
      }
      return maxTimestamp;
  }

  public Collection<Pair<Mutation, byte[]>> getIndexUpdate(
          MiniBatchOperationInProgress<Mutation> miniBatchOp,
          Collection<? extends Mutation> mutations) throws Throwable {
    // notify the delegate that we have started processing a batch
    final IndexMetaData indexMetaData = this.delegate.getIndexMetaData(miniBatchOp);
    this.delegate.batchStarted(miniBatchOp, indexMetaData);

    CachedLocalTable cachedLocalTable =
            preScanAllRequiredRows(
                    mutations,
                    (PhoenixIndexMetaData)indexMetaData,
                    this.regionCoprocessorEnvironment);
    // Avoid the Object overhead of the executor when it's not actually parallelizing anything.
    ArrayList<Pair<Mutation, byte[]>> results = new ArrayList<>(mutations.size());
    for (Mutation m : mutations) {
      Collection<Pair<Mutation, byte[]>> updates = delegate.getIndexUpdate(m, indexMetaData, cachedLocalTable);
      if (PhoenixIndexMetaData.isIndexRebuild(m.getAttributesMap())) {
        for (Pair<Mutation, byte[]> update : updates) {
          update.getFirst().setAttribute(BaseScannerRegionObserver.REPLAY_WRITES,
                  BaseScannerRegionObserver.REPLAY_INDEX_REBUILD_WRITES);
        }
      }
      results.addAll(updates);
    }
    return results;
  }

  public Collection<Pair<Mutation, byte[]>> getIndexUpdateForFilteredRows(
      Collection<KeyValue> filtered, IndexMetaData indexMetaData) throws IOException {
    // this is run async, so we can take our time here
    return delegate.getIndexUpdateForFilteredRows(filtered, indexMetaData);
  }

  public void batchCompleted(MiniBatchOperationInProgress<Mutation> miniBatchOp) {
    delegate.batchCompleted(miniBatchOp);
  }

  public void batchStarted(MiniBatchOperationInProgress<Mutation> miniBatchOp, IndexMetaData indexMetaData)
      throws IOException {
    delegate.batchStarted(miniBatchOp, indexMetaData);
  }

  public boolean isEnabled(Mutation m) {
    return delegate.isEnabled(m);
  }

  public boolean isAtomicOp(Mutation m) {
    return delegate.isAtomicOp(m);
  }

  public List<Mutation> executeAtomicOp(Increment inc) throws IOException {
      return delegate.executeAtomicOp(inc);
  }
  
  @Override
  public void stop(String why) {
    if (stopped) {
      return;
    }
    this.stopped = true;
    this.delegate.stop(why);
  }

  @Override
  public boolean isStopped() {
    return this.stopped;
  }

  public IndexBuilder getBuilderForTesting() {
    return this.delegate;
  }

  public ReplayWrite getReplayWrite(Mutation m) throws IOException {
    return this.delegate.getReplayWrite(m);
  }
}
