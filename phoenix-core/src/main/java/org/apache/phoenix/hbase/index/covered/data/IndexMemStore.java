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
package org.apache.phoenix.hbase.index.covered.data;

import java.io.IOException;
import java.util.Comparator;
import java.util.Iterator;
import java.util.SortedSet;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.KeyValue.KVComparator;
import org.apache.hadoop.hbase.KeyValueUtil;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.regionserver.IndexKeyValueSkipListSet;
import org.apache.hadoop.hbase.regionserver.KeyValueScanner;
import org.apache.hadoop.hbase.regionserver.MemStore;
import org.apache.hadoop.hbase.regionserver.NonLazyKeyValueScanner;
import org.apache.hadoop.hbase.util.Bytes;

import org.apache.phoenix.hbase.index.covered.KeyValueStore;
import org.apache.phoenix.hbase.index.covered.LocalTableState;

/**
 * Like the HBase {@link MemStore}, but without all that extra work around maintaining snapshots and
 * sizing (for right now). We still support the concurrent access (in case indexes are built in
 * parallel).
 * <p>
 * 
 We basically wrap a KeyValueSkipListSet, just like a regular MemStore, except we are:
 * <ol>
 *  <li>not dealing with
 *    <ul>
 *      <li>space considerations</li>
 *      <li>a snapshot set</li>
 *    </ul>
 *  </li>
 *  <li>ignoring memstore timestamps in favor of deciding when we want to overwrite keys based on how
 *    we obtain them</li>
 *   <li>ignoring time range updates (so 
 *    {@link KeyValueScanner#shouldUseScanner(Scan, SortedSet, long)} isn't supported from 
 *    {@link #getScanner()}).</li>
 * </ol>
 * <p>
 * We can ignore the memstore timestamps because we know that anything we get from the local region
 * is going to be MVCC visible - so it should just go in. However, we also want overwrite any
 * existing state with our pending write that we are indexing, so that needs to clobber the KVs we
 * get from the HRegion. This got really messy with a regular memstore as each KV from the MemStore
 * frequently has a higher MemStoreTS, but we can't just up the pending KVs' MemStoreTs because a
 * memstore relies on the MVCC readpoint, which generally is less than {@link Long#MAX_VALUE}.
 * <p>
 * By realizing that we don't need the snapshot or space requirements, we can go much faster than
 * the previous implementation. Further, by being smart about how we manage the KVs, we can drop the
 * extra object creation we were doing to wrap the pending KVs (which we did previously to ensure
 * they sorted before the ones we got from the HRegion). We overwrite {@link KeyValue}s when we add
 * them from external sources {@link #add(KeyValue, boolean)}, but then don't overwrite existing
 * keyvalues when read them from the underlying table (because pending keyvalues should always
 * overwrite current ones) - this logic is all contained in LocalTableState.
 * @see LocalTableState
 */
public class IndexMemStore implements KeyValueStore {

  private static final Log LOG = LogFactory.getLog(IndexMemStore.class);
  private IndexKeyValueSkipListSet kvset;
  private Comparator<KeyValue> comparator;

  /**
   * Compare two {@link KeyValue}s based only on their row keys. Similar to the standard
   * {@link KeyValue#COMPARATOR}, but doesn't take into consideration the memstore timestamps. We
   * instead manage which KeyValue to retain based on how its loaded here
   */
  public static final Comparator<KeyValue> COMPARATOR = new Comparator<KeyValue>() {

    private final KVComparator rawcomparator = new KVComparator();

    @Override
    public int compare(final KeyValue left, final KeyValue right) {
      return rawcomparator.compareFlatKey(left.getRowArray(), left.getOffset() + KeyValue.ROW_OFFSET,
        left.getKeyLength(), right.getRowArray(), right.getOffset() + KeyValue.ROW_OFFSET,
        right.getKeyLength());
    }
  };

  public IndexMemStore() {
    this(COMPARATOR);
  }

  /**
   * Create a store with the given comparator. This comparator is used to determine both sort order
   * <b>as well as equality of {@link KeyValue}s</b>.
   * <p>
   * Exposed for subclassing/testing.
   * @param comparator to use
   */
  IndexMemStore(Comparator<KeyValue> comparator) {
    this.comparator = comparator;
    this.kvset = IndexKeyValueSkipListSet.create(comparator);
  }

  @Override
  public void add(KeyValue kv, boolean overwrite) {
    if (LOG.isDebugEnabled()) {
      LOG.info("Inserting: " + toString(kv));
    }
    // if overwriting, we will always update
    if (!overwrite) {
      // null if there was no previous value, so we added the kv
      kvset.putIfAbsent(kv);
    } else {
      kvset.add(kv);
    }

    if (LOG.isTraceEnabled()) {
      dump();
    }
  }

  private void dump() {
    LOG.trace("Current kv state:\n");
    for (KeyValue kv : this.kvset) {
      LOG.trace("KV: " + toString(kv));
    }
    LOG.trace("========== END MemStore Dump ==================\n");
  }

  private String toString(KeyValue kv) {
    return kv.toString() + "/value=" + 
        Bytes.toString(kv.getValueArray(), kv.getValueOffset(), kv.getValueLength());
  }

  @Override
  public void rollback(KeyValue kv) {
    if (LOG.isDebugEnabled()) {
      LOG.debug("Rolling back: " + toString(kv));
    }
    // If the key is in the store, delete it
    this.kvset.remove(kv);
    if (LOG.isTraceEnabled()) {
      dump();
    }
  }

  @Override
  public KeyValueScanner getScanner() {
    return new MemStoreScanner();
  }
  
  /*
   * MemStoreScanner implements the KeyValueScanner. It lets the caller scan the contents of a
   * memstore -- both current map and snapshot. This behaves as if it were a real scanner but does
   * not maintain position.
   */
  // This class is adapted from org.apache.hadoop.hbase.MemStore.MemStoreScanner, HBase 0.94.12
  // It does basically the same thing as the MemStoreScanner, but it only keeps track of a single
  // set, rather than a primary and a secondary set of KeyValues.
  protected class MemStoreScanner extends NonLazyKeyValueScanner {
    // Next row information for the set
    private KeyValue nextRow = null;

    // last iterated KVs for kvset and snapshot (to restore iterator state after reseek)
    private KeyValue kvsetItRow = null;

    // iterator based scanning.
    private Iterator<KeyValue> kvsetIt;

    // The kvset at the time of creating this scanner
    volatile IndexKeyValueSkipListSet kvsetAtCreation;

    MemStoreScanner() {
      super();
      kvsetAtCreation = kvset;
    }

    private KeyValue getNext(Iterator<KeyValue> it) {
      // in the original implementation we cared about the current thread's readpoint from MVCC.
      // However, we don't need to worry here because everything the index can see, is also visible
      // to the client (or is the pending primary table update, so it will be once the index is
      // written, so it might as well be).
      KeyValue v = null;
      try {
        while (it.hasNext()) {
          v = it.next();
          return v;
        }

        return null;
      } finally {
        if (v != null) {
          kvsetItRow = v;
        }
      }
    }

    /**
     * Set the scanner at the seek key. Must be called only once: there is no thread safety between
     * the scanner and the memStore.
     * @param key seek value
     * @return false if the key is null or if there is no data
     */
    @Override
    public synchronized boolean seek(Cell key) {
      if (key == null) {
        close();
        return false;
      }

      // kvset and snapshot will never be null.
      // if tailSet can't find anything, SortedSet is empty (not null).
      kvsetIt = kvsetAtCreation.tailSet(KeyValueUtil.ensureKeyValue(key)).iterator();
      kvsetItRow = null;

      return seekInSubLists();
    }

    /**
     * (Re)initialize the iterators after a seek or a reseek.
     */
    private synchronized boolean seekInSubLists() {
      nextRow = getNext(kvsetIt);
      return nextRow != null;
    }

    /**
     * Move forward on the sub-lists set previously by seek.
     * @param key seek value (should be non-null)
     * @return true if there is at least one KV to read, false otherwise
     */
    @Override
    public synchronized boolean reseek(Cell key) {
      /*
       * See HBASE-4195 & HBASE-3855 & HBASE-6591 for the background on this implementation. This
       * code is executed concurrently with flush and puts, without locks. Two points must be known
       * when working on this code: 1) It's not possible to use the 'kvTail' and 'snapshot'
       * variables, as they are modified during a flush. 2) The ideal implementation for performance
       * would use the sub skip list implicitly pointed by the iterators 'kvsetIt' and 'snapshotIt'.
       * Unfortunately the Java API does not offer a method to get it. So we remember the last keys
       * we iterated to and restore the reseeked set to at least that point.
       */

      KeyValue kv = KeyValueUtil.ensureKeyValue(key);
      kvsetIt = kvsetAtCreation.tailSet(getHighest(kv, kvsetItRow)).iterator();
      return seekInSubLists();
    }

    /*
     * Returns the higher of the two key values, or null if they are both null. This uses
     * comparator.compare() to compare the KeyValue using the memstore comparator.
     */
    private KeyValue getHighest(KeyValue first, KeyValue second) {
      if (first == null && second == null) {
        return null;
      }
      if (first != null && second != null) {
        int compare = comparator.compare(first, second);
        return (compare > 0 ? first : second);
      }
      return (first != null ? first : second);
    }

    @Override
    public synchronized Cell peek() {
      // DebugPrint.println(" MS@" + hashCode() + " peek = " + getLowest());
      return nextRow;
    }

    @Override
    public synchronized Cell next() {
      if (nextRow == null) {
        return null;
      }

      final Cell ret = nextRow;

      // Advance the iterators
      nextRow = getNext(kvsetIt);

      return ret;
    }

    @Override
    public synchronized void close() {
      this.nextRow = null;
      this.kvsetIt = null;
      this.kvsetItRow = null;
    }

    /**
     * MemStoreScanner returns max value as sequence id because it will always have the latest data
     * among all files.
     */
    @Override
    public long getSequenceID() {
      return Long.MAX_VALUE;
    }
    
    @Override
    public boolean shouldUseScanner(Scan scan, SortedSet<byte[]> columns, long oldestUnexpiredTS) {
      throw new UnsupportedOperationException(this.getClass().getName()
          + " doesn't support checking to see if it should use a scanner!");
    }

    @Override
    public boolean backwardSeek(Cell arg0) throws IOException {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean seekToLastRow() throws IOException {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean seekToPreviousRow(Cell arg0) throws IOException {
        throw new UnsupportedOperationException();
    }
  }
}