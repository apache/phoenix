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

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock.ReadLock;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Coprocessor;
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
import org.apache.hadoop.hbase.regionserver.wal.HLog;
import org.apache.hadoop.hbase.regionserver.wal.HLogKey;
import org.apache.hadoop.hbase.regionserver.wal.WALEdit;
import org.apache.hadoop.hbase.util.Pair;

import com.google.common.collect.Multimap;
import org.apache.phoenix.hbase.index.builder.IndexBuildManager;
import org.apache.phoenix.hbase.index.builder.IndexBuilder;
import org.apache.phoenix.hbase.index.builder.IndexBuildingFailureException;
import org.apache.phoenix.hbase.index.table.HTableInterfaceReference;
import org.apache.phoenix.hbase.index.util.ImmutableBytesPtr;
import org.apache.phoenix.hbase.index.util.IndexManagementUtil;
import org.apache.phoenix.hbase.index.wal.IndexedKeyValue;
import org.apache.phoenix.hbase.index.write.IndexFailurePolicy;
import org.apache.phoenix.hbase.index.write.IndexWriter;
import org.apache.phoenix.hbase.index.write.recovery.PerRegionIndexWriteCache;
import org.apache.phoenix.hbase.index.write.recovery.StoreFailuresInCachePolicy;
import org.apache.phoenix.hbase.index.write.recovery.TrackingParallelWriterIndexCommitter;
import org.apache.phoenix.util.MetaDataUtil;

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

  /** WAL on this server */
  private HLog log;
  protected IndexWriter writer;
  protected IndexBuildManager builder;

  /** Configuration key for the {@link IndexBuilder} to use */
  public static final String INDEX_BUILDER_CONF_KEY = "index.builder";

  // Setup out locking on the index edits/WAL so we can be sure that we don't lose a roll a WAL edit
  // before an edit is applied to the index tables
  private static final ReentrantReadWriteLock INDEX_READ_WRITE_LOCK = new ReentrantReadWriteLock(
      true);
  public static final ReadLock INDEX_UPDATE_LOCK = INDEX_READ_WRITE_LOCK.readLock();

  /**
   * Configuration key for if the indexer should check the version of HBase is running. Generally,
   * you only want to ignore this for testing or for custom versions of HBase.
   */
  public static final String CHECK_VERSION_CONF_KEY = "com.saleforce.hbase.index.checkversion";

  private static final String INDEX_RECOVERY_FAILURE_POLICY_KEY = "org.apache.hadoop.hbase.index.recovery.failurepolicy";

  /**
   * Marker {@link KeyValue} to indicate that we are doing a batch operation. Needed because the
   * coprocessor framework throws away the WALEdit from the prePut/preDelete hooks when checking a
   * batch if there were no {@link KeyValue}s attached to the {@link WALEdit}. When you get down to
   * the preBatch hook, there won't be any WALEdits to which to add the index updates.
   */
  private static KeyValue BATCH_MARKER = new KeyValue();

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

    public static final int INDEXING_SUPPORTED_MAJOR_VERSION = MetaDataUtil
            .encodeMaxPatchVersion(0, 94);
    public static final int INDEXING_SUPPORTED__MIN_MAJOR_VERSION = MetaDataUtil
            .encodeVersion("0.94.0");
    private static final int INDEX_WAL_COMPRESSION_MINIMUM_SUPPORTED_VERSION = MetaDataUtil
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
    
        // get a reference to the WAL
        log = env.getRegionServerServices().getWAL();
        // add a synchronizer so we don't archive a WAL that we need
        log.registerWALActionsListener(new IndexLogRollSynchronizer(INDEX_READ_WRITE_LOCK.writeLock()));
    
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
  public void prePut(final ObserverContext<RegionCoprocessorEnvironment> c, final Put put,
      final WALEdit edit, final boolean writeToWAL) throws IOException {
      if (this.disabled) {
          super.prePut(c, put, edit, writeToWAL);
          return;
        }
    // just have to add a batch marker to the WALEdit so we get the edit again in the batch
    // processing step. We let it throw an exception here because something terrible has happened.
    edit.add(BATCH_MARKER);
  }

  @Override
  public void preDelete(ObserverContext<RegionCoprocessorEnvironment> e, Delete delete,
      WALEdit edit, boolean writeToWAL) throws IOException {
      if (this.disabled) {
          super.preDelete(e, delete, edit, writeToWAL);
          return;
        }
    try {
      preDeleteWithExceptions(e, delete, edit, writeToWAL);
      return;
    } catch (Throwable t) {
      rethrowIndexingException(t);
    }
    throw new RuntimeException(
        "Somehow didn't return an index update but also didn't propagate the failure to the client!");
  }

  public void preDeleteWithExceptions(ObserverContext<RegionCoprocessorEnvironment> e,
      Delete delete, WALEdit edit, boolean writeToWAL) throws Exception {
    // if we are making the update as part of a batch, we need to add in a batch marker so the WAL
    // is retained
    if (this.builder.getBatchId(delete) != null) {
      edit.add(BATCH_MARKER);
      return;
    }

    // get the mapping for index column -> target index table
    Collection<Pair<Mutation, byte[]>> indexUpdates = this.builder.getIndexUpdate(delete);

    if (doPre(indexUpdates, edit, writeToWAL)) {
      takeUpdateLock("delete");
    }
  }

  @Override
  public void preBatchMutate(ObserverContext<RegionCoprocessorEnvironment> c,
      MiniBatchOperationInProgress<Pair<Mutation, Integer>> miniBatchOp) throws IOException {
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

  @SuppressWarnings("deprecation")
  public void preBatchMutateWithExceptions(ObserverContext<RegionCoprocessorEnvironment> c,
      MiniBatchOperationInProgress<Pair<Mutation, Integer>> miniBatchOp) throws Throwable {

    // first group all the updates for a single row into a single update to be processed
    Map<ImmutableBytesPtr, MultiMutation> mutations =
        new HashMap<ImmutableBytesPtr, MultiMutation>();
    boolean durable = false;
    for (int i = 0; i < miniBatchOp.size(); i++) {
      // remove the batch keyvalue marker - its added for all puts
      WALEdit edit = miniBatchOp.getWalEdit(i);
      // we don't have a WALEdit for immutable index cases, which still see this path
      // we could check is indexing is enable for the mutation in prePut and then just skip this
      // after checking here, but this saves us the checking again.
      if (edit != null) {
        KeyValue kv = edit.getKeyValues().remove(0);
        assert kv == BATCH_MARKER : "Expected batch marker from the WALEdit, but got: " + kv;
      }
      Pair<Mutation, Integer> op = miniBatchOp.getOperation(i);
      Mutation m = op.getFirst();
      // skip this mutation if we aren't enabling indexing
      // unfortunately, we really should ask if the raw mutation (rather than the combined mutation)
      // should be indexed, which means we need to expose another method on the builder. Such is the
      // way optimization go though.
      if (!this.builder.isEnabled(m)) {
        continue;
      }
      
      // figure out if this is batch is durable or not
      if(!durable){
        durable = m.getDurability() != Durability.SKIP_WAL;
      }

      // add the mutation to the batch set
      ImmutableBytesPtr row = new ImmutableBytesPtr(m.getRow());
      MultiMutation stored = mutations.get(row);
      // we haven't seen this row before, so add it
      if (stored == null) {
        stored = new MultiMutation(row, m.getWriteToWAL());
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

    // get the index updates for all elements in this batch
    Collection<Pair<Mutation, byte[]>> indexUpdates =
        this.builder.getIndexUpdate(miniBatchOp, mutations.values());
    // write them
    if (doPre(indexUpdates, edit, durable)) {
      takeUpdateLock("batch mutation");
    }
  }

  private void takeUpdateLock(String opDesc) throws IndexBuildingFailureException {
    boolean interrupted = false;
    // lock the log, so we are sure that index write gets atomically committed
    LOG.debug("Taking INDEX_UPDATE readlock for " + opDesc);
    // wait for the update lock
    while (!this.stopped) {
      try {
        INDEX_UPDATE_LOCK.lockInterruptibly();
        LOG.debug("Got the INDEX_UPDATE readlock for " + opDesc);
        // unlock the lock so the server can shutdown, if we find that we have stopped since getting
        // the lock
        if (this.stopped) {
          INDEX_UPDATE_LOCK.unlock();
          throw new IndexBuildingFailureException(
              "Found server stop after obtaining the update lock, killing update attempt");
        }
        break;
      } catch (InterruptedException e) {
        LOG.info("Interrupted while waiting for update lock. Ignoring unless stopped");
        interrupted = true;
      }
    }
    if (interrupted) {
      Thread.currentThread().interrupt();
    }
  }

  private class MultiMutation extends Mutation {

    private ImmutableBytesPtr rowKey;

    public MultiMutation(ImmutableBytesPtr rowkey, boolean writeToWal) {
      this.rowKey = rowkey;
      this.writeToWAL = writeToWal;
    }

    /**
     * @param stored
     */
    @SuppressWarnings("deprecation")
    public void addAll(Mutation stored) {
      // add all the kvs
      for (Entry<byte[], List<KeyValue>> kvs : stored.getFamilyMap().entrySet()) {
        byte[] family = kvs.getKey();
        List<KeyValue> list = getKeyValueList(family, kvs.getValue().size());
        list.addAll(kvs.getValue());
        familyMap.put(family, list);
      }

      // add all the attributes, not overriding already stored ones
      for (Entry<String, byte[]> attrib : stored.getAttributesMap().entrySet()) {
        if (this.getAttribute(attrib.getKey()) == null) {
          this.setAttribute(attrib.getKey(), attrib.getValue());
        }
      }
      if (stored.getWriteToWAL()) {
        this.writeToWAL = true;
      }
    }

    private List<KeyValue> getKeyValueList(byte[] family, int hint) {
      List<KeyValue> list = familyMap.get(family);
      if (list == null) {
        list = new ArrayList<KeyValue>(hint);
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

    @Override
    public void readFields(DataInput arg0) throws IOException {
      throw new UnsupportedOperationException("MultiMutations cannot be read/written");
    }

    @Override
    public void write(DataOutput arg0) throws IOException {
      throw new UnsupportedOperationException("MultiMutations cannot be read/written");
    }
  }

  /**
   * Add the index updates to the WAL, or write to the index table, if the WAL has been disabled
   * @return <tt>true</tt> if the WAL has been updated.
   * @throws IOException
   */
  private boolean doPre(Collection<Pair<Mutation, byte[]>> indexUpdates, final WALEdit edit,
      final boolean writeToWAL) throws IOException {
    // no index updates, so we are done
    if (indexUpdates == null || indexUpdates.size() == 0) {
      return false;
    }

    // if writing to wal is disabled, we never see the WALEdit updates down the way, so do the index
    // update right away
    if (!writeToWAL) {
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
      boolean writeToWAL) throws IOException {
      if (this.disabled) {
          super.postPut(e, put, edit, writeToWAL);
          return;
        }
    doPost(edit, put, writeToWAL);
  }

  @Override
  public void postDelete(ObserverContext<RegionCoprocessorEnvironment> e, Delete delete,
      WALEdit edit, boolean writeToWAL) throws IOException {
      if (this.disabled) {
          super.postDelete(e, delete, edit, writeToWAL);
          return;
        }
    doPost(edit,delete, writeToWAL);
  }

  @Override
  public void postBatchMutate(ObserverContext<RegionCoprocessorEnvironment> c,
      MiniBatchOperationInProgress<Pair<Mutation, Integer>> miniBatchOp) throws IOException {
      if (this.disabled) {
          super.postBatchMutate(c, miniBatchOp);
          return;
        }
    this.builder.batchCompleted(miniBatchOp);
    // noop for the rest of the indexer - its handled by the first call to put/delete
  }

  private void doPost(WALEdit edit, Mutation m, boolean writeToWAL) throws IOException {
    try {
      doPostWithExceptions(edit, m, writeToWAL);
      return;
    } catch (Throwable e) {
      rethrowIndexingException(e);
    }
    throw new RuntimeException(
        "Somehow didn't complete the index update, but didn't return succesfully either!");
  }

  private void doPostWithExceptions(WALEdit edit, Mutation m, boolean writeToWAL) throws Exception {
    //short circuit, if we don't need to do any work
    if (!writeToWAL || !this.builder.isEnabled(m)) {
      // already did the index update in prePut, so we are done
      return;
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
          writer.writeAndKillYourselfOnFailure(indexUpdates);
      } finally {
        // With a custom kill policy, we may throw instead of kill the server.
        // Without doing this in a finally block (at least with the mini cluster),
        // the region server never goes down.

        // mark the batch as having been written. In the single-update case, this never gets check
        // again, but in the batch case, we will check it again (see above).
        ikv.markBatchFinished();
      
        // release the lock on the index, we wrote everything properly
        // we took the lock for each Put/Delete, so we have to release it a matching number of times
        // batch cases only take the lock once, so we need to make sure we don't over-release the
        // lock.
        LOG.debug("Releasing INDEX_UPDATE readlock");
        INDEX_UPDATE_LOCK.unlock();
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
    for (KeyValue kv : edit.getKeyValues()) {
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
    for (KeyValue kv : edit.getKeyValues()) {
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
    recoveryWriter.writeAndKillYourselfOnFailure(indexUpdates);
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
        int encodedVersion = MetaDataUtil.encodeVersion(hbaseVersion);
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
   * @throws IOException the Indexer coprocessor cannot be added
   */
  public static void enableIndexing(HTableDescriptor desc, Class<? extends IndexBuilder> builder,
      Map<String, String> properties) throws IOException {
    if (properties == null) {
      properties = new HashMap<String, String>();
    }
    properties.put(Indexer.INDEX_BUILDER_CONF_KEY, builder.getName());
    desc.addCoprocessor(Indexer.class.getName(), null, Coprocessor.PRIORITY_USER, properties);
  }
}