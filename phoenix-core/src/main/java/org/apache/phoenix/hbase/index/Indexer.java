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
package org.apache.phoenix.hbase.index;

import static org.apache.phoenix.hbase.index.util.IndexManagementUtil.rethrowIndexingException;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CoprocessorEnvironment;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Durability;
import org.apache.hadoop.hbase.client.Mutation;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.coprocessor.BaseRegionObserver;
import org.apache.hadoop.hbase.coprocessor.ObserverContext;
import org.apache.hadoop.hbase.coprocessor.RegionCoprocessorEnvironment;
import org.apache.hadoop.hbase.regionserver.InternalScanner;
import org.apache.hadoop.hbase.regionserver.KeyValueScanner;
import org.apache.hadoop.hbase.regionserver.MiniBatchOperationInProgress;
import org.apache.hadoop.hbase.regionserver.ScanType;
import org.apache.hadoop.hbase.regionserver.Store;
import org.apache.hadoop.hbase.regionserver.wal.HLogKey;
import org.apache.hadoop.hbase.regionserver.wal.WALEdit;
import org.apache.hadoop.hbase.util.Pair;
import org.apache.phoenix.hbase.index.builder.IndexBuildManager;
import org.apache.phoenix.hbase.index.builder.IndexBuilder;
import org.apache.phoenix.hbase.index.table.HTableInterfaceReference;
import org.apache.phoenix.hbase.index.util.ImmutableBytesPtr;
import org.apache.phoenix.hbase.index.util.IndexManagementUtil;
import org.apache.phoenix.hbase.index.util.VersionUtil;
import org.apache.phoenix.hbase.index.wal.IndexedKeyValue;
import org.apache.phoenix.hbase.index.write.IndexFailurePolicy;
import org.apache.phoenix.hbase.index.write.IndexWriter;
import org.apache.phoenix.hbase.index.write.recovery.PerRegionIndexWriteCache;
import org.apache.phoenix.hbase.index.write.recovery.StoreFailuresInCachePolicy;
import org.apache.phoenix.hbase.index.write.recovery.TrackingParallelWriterIndexCommitter;
import org.apache.phoenix.trace.TracingUtils;
import org.apache.phoenix.trace.util.NullSpan;
import org.apache.htrace.Span;
import org.apache.htrace.Trace;
import org.apache.htrace.TraceScope;

import com.google.common.collect.Multimap;

/**
 * Do all the work of managing index updates from a single coprocessor. All Puts/Delets are passed
 * to an {@link IndexBuilder} to determine the actual updates to make.
 * <p>
 * If the WAL is enabled, these updates are then added to the WALEdit and attempted to be written to
 * the WAL after the WALEdit has been saved. If any of the index updates fail, this server is
 * immediately terminated and we rely on WAL replay to attempt the index updates again (see
 * {@link #preWALRestore(ObserverContext, HRegionInfo, HLogKey, WALEdit)}).
 * <p>
 * If the WAL is disabled, the updates are attempted immediately. No consistency guarantees are made
 * if the WAL is disabled - some or none of the index updates may be successful. All updates in a
 * single batch must have the same durability level - either everything gets written to the WAL or
 * nothing does. Currently, we do not support mixed-durability updates within a single batch. If you
 * want to have different durability levels, you only need to split the updates into two different
 * batches.
 */
public class Indexer extends BaseRegionObserver {

  private static final Log LOG = LogFactory.getLog(Indexer.class);

  protected IndexWriter writer;
  protected IndexBuildManager builder;

  /** Configuration key for the {@link IndexBuilder} to use */
  public static final String INDEX_BUILDER_CONF_KEY = "index.builder";

  /**
   * Configuration key for if the indexer should check the version of HBase is running. Generally,
   * you only want to ignore this for testing or for custom versions of HBase.
   */
  public static final String CHECK_VERSION_CONF_KEY = "com.saleforce.hbase.index.checkversion";

  private static final String INDEX_RECOVERY_FAILURE_POLICY_KEY = "org.apache.hadoop.hbase.index.recovery.failurepolicy";

  /**
   * cache the failed updates to the various regions. Used for making the WAL recovery mechanisms
   * more robust in the face of recoverying index regions that were on the same server as the
   * primary table region
   */
  private PerRegionIndexWriteCache failedIndexEdits = new PerRegionIndexWriteCache();

  /**
   * IndexWriter for writing the recovered index edits. Separate from the main indexer since we need
   * different write/failure policies
   */
  private IndexWriter recoveryWriter;

  private boolean stopped;
  private boolean disabled;

  public static final String RecoveryFailurePolicyKeyForTesting = INDEX_RECOVERY_FAILURE_POLICY_KEY;

    public static final int INDEXING_SUPPORTED_MAJOR_VERSION = VersionUtil
            .encodeMaxPatchVersion(0, 94);
    public static final int INDEXING_SUPPORTED__MIN_MAJOR_VERSION = VersionUtil
            .encodeVersion("0.94.0");
    private static final int INDEX_WAL_COMPRESSION_MINIMUM_SUPPORTED_VERSION = VersionUtil
            .encodeVersion("0.94.9");

  @Override
  public void start(CoprocessorEnvironment e) throws IOException {
      try {
        final RegionCoprocessorEnvironment env = (RegionCoprocessorEnvironment) e;
        String serverName = env.getRegionServerServices().getServerName().getServerName();
        if (env.getConfiguration().getBoolean(CHECK_VERSION_CONF_KEY, true)) {
          // make sure the right version <-> combinations are allowed.
          String errormsg = Indexer.validateVersion(env.getHBaseVersion(), env.getConfiguration());
          if (errormsg != null) {
            IOException ioe = new IOException(errormsg);
            env.getRegionServerServices().abort(errormsg, ioe);
            throw ioe;
          }
        }
    
        this.builder = new IndexBuildManager(env);

        // setup the actual index writer
        this.writer = new IndexWriter(env, serverName + "-index-writer");
    
        // setup the recovery writer that does retries on the failed edits
        TrackingParallelWriterIndexCommitter recoveryCommmiter =
            new TrackingParallelWriterIndexCommitter();
    
        try {
          // get the specified failure policy. We only ever override it in tests, but we need to do it
          // here
          Class<? extends IndexFailurePolicy> policyClass =
              env.getConfiguration().getClass(INDEX_RECOVERY_FAILURE_POLICY_KEY,
                StoreFailuresInCachePolicy.class, IndexFailurePolicy.class);
          IndexFailurePolicy policy =
              policyClass.getConstructor(PerRegionIndexWriteCache.class).newInstance(failedIndexEdits);
          LOG.debug("Setting up recovery writter with committer: " + recoveryCommmiter.getClass()
              + " and failure policy: " + policy.getClass());
          recoveryWriter =
              new IndexWriter(recoveryCommmiter, policy, env, serverName + "-recovery-writer");
        } catch (Exception ex) {
          throw new IOException("Could not instantiate recovery failure policy!", ex);
        }
      } catch (NoSuchMethodError ex) {
          disabled = true;
          super.start(e);
          LOG.error("Must be too early a version of HBase. Disabled coprocessor ", ex);
      }
  }

  @Override
  public void stop(CoprocessorEnvironment e) throws IOException {
    if (this.stopped) {
      return;
    }
    if (this.disabled) {
        super.stop(e);
        return;
      }
    this.stopped = true;
    String msg = "Indexer is being stopped";
    this.builder.stop(msg);
    this.writer.stop(msg);
    this.recoveryWriter.stop(msg);
  }

  @Override
  public void preBatchMutate(ObserverContext<RegionCoprocessorEnvironment> c,
      MiniBatchOperationInProgress<Mutation> miniBatchOp) throws IOException {
      if (this.disabled) {
          super.preBatchMutate(c, miniBatchOp);
          return;
      }
      try {
          preBatchMutateWithExceptions(c, miniBatchOp);
          return;
      } catch (Throwable t) {
          rethrowIndexingException(t);
      }
      throw new RuntimeException(
        "Somehow didn't return an index update but also didn't propagate the failure to the client!");
  }

  public void preBatchMutateWithExceptions(ObserverContext<RegionCoprocessorEnvironment> c,
          MiniBatchOperationInProgress<Mutation> miniBatchOp) throws Throwable {

      // first group all the updates for a single row into a single update to be processed
      Map<ImmutableBytesPtr, MultiMutation> mutations =
              new HashMap<ImmutableBytesPtr, MultiMutation>();

      Durability defaultDurability = Durability.SYNC_WAL;
      if(c.getEnvironment().getRegion() != null) {
          defaultDurability = c.getEnvironment().getRegion().getTableDesc().getDurability();
          defaultDurability = (defaultDurability == Durability.USE_DEFAULT) ? 
                  Durability.SYNC_WAL : defaultDurability;
      }
      Durability durability = Durability.SKIP_WAL;
      for (int i = 0; i < miniBatchOp.size(); i++) {
          Mutation m = miniBatchOp.getOperation(i);
          // skip this mutation if we aren't enabling indexing
          // unfortunately, we really should ask if the raw mutation (rather than the combined mutation)
          // should be indexed, which means we need to expose another method on the builder. Such is the
          // way optimization go though.
          if (!this.builder.isEnabled(m)) {
              continue;
          }

          Durability effectiveDurablity = (m.getDurability() == Durability.USE_DEFAULT) ? 
                  defaultDurability : m.getDurability();
          if (effectiveDurablity.ordinal() > durability.ordinal()) {
              durability = effectiveDurablity;
          }

          // add the mutation to the batch set
          ImmutableBytesPtr row = new ImmutableBytesPtr(m.getRow());
          MultiMutation stored = mutations.get(row);
          // we haven't seen this row before, so add it
          if (stored == null) {
              stored = new MultiMutation(row);
              mutations.put(row, stored);
          }
          stored.addAll(m);
      }

      // early exit if it turns out we don't have any edits
      if (mutations.entrySet().size() == 0) {
          return;
      }

      // dump all the index updates into a single WAL. They will get combined in the end anyways, so
      // don't worry which one we get
      WALEdit edit = miniBatchOp.getWalEdit(0);
      if (edit == null) {
          edit = new WALEdit();
          miniBatchOp.setWalEdit(0, edit);
      }

      // get the current span, or just use a null-span to avoid a bunch of if statements
      try (TraceScope scope = Trace.startSpan("Starting to build index updates")) {
          Span current = scope.getSpan();
          if (current == null) {
              current = NullSpan.INSTANCE;
          }

          // get the index updates for all elements in this batch
          Collection<Pair<Mutation, byte[]>> indexUpdates =
                  this.builder.getIndexUpdate(miniBatchOp, mutations.values());

          current.addTimelineAnnotation("Built index updates, doing preStep");
          TracingUtils.addAnnotation(current, "index update count", indexUpdates.size());

          // write them, either to WAL or the index tables
          doPre(indexUpdates, edit, durability);
      }
  }

  private class MultiMutation extends Mutation {

    private ImmutableBytesPtr rowKey;

    public MultiMutation(ImmutableBytesPtr rowkey) {
      this.rowKey = rowkey;
    }

    /**
     * @param stored
     */
    public void addAll(Mutation stored) {
      // add all the kvs
      for (Entry<byte[], List<Cell>> kvs : stored.getFamilyCellMap().entrySet()) {
        byte[] family = kvs.getKey();
        List<Cell> list = getKeyValueList(family, kvs.getValue().size());
        list.addAll(kvs.getValue());
        familyMap.put(family, list);
      }

      // add all the attributes, not overriding already stored ones
      for (Entry<String, byte[]> attrib : stored.getAttributesMap().entrySet()) {
        if (this.getAttribute(attrib.getKey()) == null) {
          this.setAttribute(attrib.getKey(), attrib.getValue());
        }
      }
    }

    private List<Cell> getKeyValueList(byte[] family, int hint) {
      List<Cell> list = familyMap.get(family);
      if (list == null) {
        list = new ArrayList<Cell>(hint);
      }
      return list;
    }

    @Override
    public byte[] getRow(){
      return this.rowKey.copyBytesIfNecessary();
    }

    @Override
    public int hashCode() {
      return this.rowKey.hashCode();
    }

    @Override
    public boolean equals(Object o) {
      return o == null ? false : o.hashCode() == this.hashCode();
    }
  }

  /**
   * Add the index updates to the WAL, or write to the index table, if the WAL has been disabled
   * @return <tt>true</tt> if the WAL has been updated.
   * @throws IOException
   */
  private boolean doPre(Collection<Pair<Mutation, byte[]>> indexUpdates, final WALEdit edit,
      final Durability durability) throws IOException {
    // no index updates, so we are done
    if (indexUpdates == null || indexUpdates.size() == 0) {
      return false;
    }

    // if writing to wal is disabled, we never see the WALEdit updates down the way, so do the index
    // update right away
    if (durability == Durability.SKIP_WAL) {
      try {
        this.writer.write(indexUpdates);
        return false;
      } catch (Throwable e) {
        LOG.error("Failed to update index with entries:" + indexUpdates, e);
        IndexManagementUtil.rethrowIndexingException(e);
      }
    }

    // we have all the WAL durability, so we just update the WAL entry and move on
    for (Pair<Mutation, byte[]> entry : indexUpdates) {
      edit.add(new IndexedKeyValue(entry.getSecond(), entry.getFirst()));
    }

    return true;
  }

  @Override
  public void postPut(ObserverContext<RegionCoprocessorEnvironment> e, Put put, WALEdit edit,
      final Durability durability) throws IOException {
      if (this.disabled) {
      super.postPut(e, put, edit, durability);
          return;
        }
    doPost(edit, put, durability);
  }

  @Override
  public void postDelete(ObserverContext<RegionCoprocessorEnvironment> e, Delete delete,
      WALEdit edit, final Durability durability) throws IOException {
      if (this.disabled) {
      super.postDelete(e, delete, edit, durability);
          return;
        }
    doPost(edit, delete, durability);
  }

  @Override
  public void postBatchMutate(ObserverContext<RegionCoprocessorEnvironment> c,
      MiniBatchOperationInProgress<Mutation> miniBatchOp) throws IOException {
      if (this.disabled) {
          super.postBatchMutate(c, miniBatchOp);
          return;
        }
    this.builder.batchCompleted(miniBatchOp);

    //each batch operation, only the first one will have anything useful, so we can just grab that
    Mutation mutation = miniBatchOp.getOperation(0);
    WALEdit edit = miniBatchOp.getWalEdit(0);
    doPost(edit, mutation, mutation.getDurability());
  }

  private void doPost(WALEdit edit, Mutation m, final Durability durability) throws IOException {
    try {
      doPostWithExceptions(edit, m, durability);
      return;
    } catch (Throwable e) {
      rethrowIndexingException(e);
    }
    throw new RuntimeException(
        "Somehow didn't complete the index update, but didn't return succesfully either!");
  }

  private void doPostWithExceptions(WALEdit edit, Mutation m, final Durability durability)
          throws Exception {
      //short circuit, if we don't need to do any work
      if (durability == Durability.SKIP_WAL || !this.builder.isEnabled(m)) {
          // already did the index update in prePut, so we are done
          return;
      }

      // get the current span, or just use a null-span to avoid a bunch of if statements
      try (TraceScope scope = Trace.startSpan("Completing index writes")) {
          Span current = scope.getSpan();
          if (current == null) {
              current = NullSpan.INSTANCE;
          }

          // there is a little bit of excess here- we iterate all the non-indexed kvs for this check first
          // and then do it again later when getting out the index updates. This should be pretty minor
          // though, compared to the rest of the runtime
          IndexedKeyValue ikv = getFirstIndexedKeyValue(edit);

          /*
           * early exit - we have nothing to write, so we don't need to do anything else. NOTE: we don't
           * release the WAL Rolling lock (INDEX_UPDATE_LOCK) since we never take it in doPre if there are
           * no index updates.
           */
          if (ikv == null) {
              return;
          }

          /*
           * only write the update if we haven't already seen this batch. We only want to write the batch
           * once (this hook gets called with the same WALEdit for each Put/Delete in a batch, which can
           * lead to writing all the index updates for each Put/Delete).
           */
          if (!ikv.getBatchFinished()) {
              Collection<Pair<Mutation, byte[]>> indexUpdates = extractIndexUpdate(edit);

              // the WAL edit is kept in memory and we already specified the factory when we created the
              // references originally - therefore, we just pass in a null factory here and use the ones
              // already specified on each reference
              try {
                  current.addTimelineAnnotation("Actually doing index update for first time");
                  writer.writeAndKillYourselfOnFailure(indexUpdates);
              } finally {
                  // With a custom kill policy, we may throw instead of kill the server.
                  // Without doing this in a finally block (at least with the mini cluster),
                  // the region server never goes down.

                  // mark the batch as having been written. In the single-update case, this never gets check
                  // again, but in the batch case, we will check it again (see above).
                  ikv.markBatchFinished();
              }
          }
      }
  }

  /**
   * Search the {@link WALEdit} for the first {@link IndexedKeyValue} present
   * @param edit {@link WALEdit}
   * @return the first {@link IndexedKeyValue} in the {@link WALEdit} or <tt>null</tt> if not
   *         present
   */
  private IndexedKeyValue getFirstIndexedKeyValue(WALEdit edit) {
    for (Cell kv : edit.getCells()) {
      if (kv instanceof IndexedKeyValue) {
        return (IndexedKeyValue) kv;
      }
    }
    return null;
  }

  /**
   * Extract the index updates from the WAL Edit
   * @param edit to search for index updates
   * @return the mutations to apply to the index tables
   */
  private Collection<Pair<Mutation, byte[]>> extractIndexUpdate(WALEdit edit) {
    Collection<Pair<Mutation, byte[]>> indexUpdates = new ArrayList<Pair<Mutation, byte[]>>();
    for (Cell kv : edit.getCells()) {
      if (kv instanceof IndexedKeyValue) {
        IndexedKeyValue ikv = (IndexedKeyValue) kv;
        indexUpdates.add(new Pair<Mutation, byte[]>(ikv.getMutation(), ikv.getIndexTable()));
      }
    }

    return indexUpdates;
  }

  @Override
  public void postOpen(final ObserverContext<RegionCoprocessorEnvironment> c) {
    Multimap<HTableInterfaceReference, Mutation> updates = failedIndexEdits.getEdits(c.getEnvironment().getRegion());
    
    if (this.disabled) {
        super.postOpen(c);
        return;
      }
    LOG.info("Found some outstanding index updates that didn't succeed during"
        + " WAL replay - attempting to replay now.");
    //if we have no pending edits to complete, then we are done
    if (updates == null || updates.size() == 0) {
      return;
    }
    
    // do the usual writer stuff, killing the server again, if we can't manage to make the index
    // writes succeed again
    try {
        writer.writeAndKillYourselfOnFailure(updates);
    } catch (IOException e) {
        LOG.error("Exception thrown instead of killing server during index writing", e);
    }
  }

  @Override
  public void preWALRestore(ObserverContext<RegionCoprocessorEnvironment> env, HRegionInfo info,
      HLogKey logKey, WALEdit logEdit) throws IOException {
      if (this.disabled) {
          super.preWALRestore(env, info, logKey, logEdit);
          return;
        }
    // TODO check the regions in transition. If the server on which the region lives is this one,
    // then we should rety that write later in postOpen.
    // we might be able to get even smarter here and pre-split the edits that are server-local
    // into their own recovered.edits file. This then lets us do a straightforward recovery of each
    // region (and more efficiently as we aren't writing quite as hectically from this one place).

    /*
     * Basically, we let the index regions recover for a little while long before retrying in the
     * hopes they come up before the primary table finishes.
     */
    Collection<Pair<Mutation, byte[]>> indexUpdates = extractIndexUpdate(logEdit);
    recoveryWriter.write(indexUpdates);
  }

  /**
   * Create a custom {@link InternalScanner} for a compaction that tracks the versions of rows that
   * are removed so we can clean then up from the the index table(s).
   * <p>
   * This is not yet implemented - its not clear if we should even mess around with the Index table
   * for these rows as those points still existed. TODO: v2 of indexing
   */
  @Override
  public InternalScanner preCompactScannerOpen(ObserverContext<RegionCoprocessorEnvironment> c,
      Store store, List<? extends KeyValueScanner> scanners, ScanType scanType, long earliestPutTs,
      InternalScanner s) throws IOException {
    return super.preCompactScannerOpen(c, store, scanners, scanType, earliestPutTs, s);
  }

  /**
   * Exposed for testing!
   * @return the currently instantiated index builder
   */
  public IndexBuilder getBuilderForTesting() {
    return this.builder.getBuilderForTesting();
  }

    /**
     * Validate that the version and configuration parameters are supported
     * @param hbaseVersion current version of HBase on which <tt>this</tt> coprocessor is installed
     * @param conf configuration to check for allowed parameters (e.g. WAL Compression only if >=
     *            0.94.9)
     * @return <tt>null</tt> if the version is supported, the error message to display otherwise
     */
    public static String validateVersion(String hbaseVersion, Configuration conf) {
        int encodedVersion = VersionUtil.encodeVersion(hbaseVersion);
        // above 0.94 everything should be supported
        if (encodedVersion > INDEXING_SUPPORTED_MAJOR_VERSION) {
            return null;
        }
        // check to see if its at least 0.94
        if (encodedVersion < INDEXING_SUPPORTED__MIN_MAJOR_VERSION) {
            return "Indexing not supported for versions older than 0.94.X";
        }
        // if less than 0.94.9, we need to check if WAL Compression is enabled
        if (encodedVersion < INDEX_WAL_COMPRESSION_MINIMUM_SUPPORTED_VERSION) {
            if (conf.getBoolean(HConstants.ENABLE_WAL_COMPRESSION, false)) {
                return "Indexing not supported with WAL Compression for versions of HBase older than 0.94.9 - found version:"
                        + hbaseVersion;
            }
        }
        return null;
    }

  /**
   * Enable indexing on the given table
   * @param desc {@link HTableDescriptor} for the table on which indexing should be enabled
 * @param builder class to use when building the index for this table
 * @param properties map of custom configuration options to make available to your
   *          {@link IndexBuilder} on the server-side
 * @param priority TODO
   * @throws IOException the Indexer coprocessor cannot be added
   */
  public static void enableIndexing(HTableDescriptor desc, Class<? extends IndexBuilder> builder,
      Map<String, String> properties, int priority) throws IOException {
    if (properties == null) {
      properties = new HashMap<String, String>();
    }
    properties.put(Indexer.INDEX_BUILDER_CONF_KEY, builder.getName());
    desc.addCoprocessor(Indexer.class.getName(), null, priority, properties);
  }
}

