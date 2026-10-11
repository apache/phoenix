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
package org.apache.phoenix.hbase.index.vector;

import io.github.jbellis.jvector.graph.GraphIndexBuilder;
import io.github.jbellis.jvector.graph.GraphSearcher;
import io.github.jbellis.jvector.graph.ListRandomAccessVectorValues;
import io.github.jbellis.jvector.graph.RandomAccessVectorValues;
import io.github.jbellis.jvector.graph.SearchResult;
import io.github.jbellis.jvector.util.Bits;
import io.github.jbellis.jvector.vector.VectorSimilarityFunction;
import io.github.jbellis.jvector.vector.VectorizationProvider;
import io.github.jbellis.jvector.vector.types.VectorFloat;
import io.github.jbellis.jvector.vector.types.VectorTypeSupport;
import java.io.IOException;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.PriorityBlockingQueue;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.function.LongSupplier;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.DoNotRetryIOException;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.RegionTooBusyException;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.ColumnFamilyDescriptor;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.coprocessor.RegionCoprocessorEnvironment;
import org.apache.hadoop.hbase.exceptions.RequestTooBigException;
import org.apache.hadoop.hbase.filter.KeyOnlyFilter;
import org.apache.hadoop.hbase.regionserver.Region;
import org.apache.hadoop.hbase.regionserver.RegionScanner;
import org.apache.hadoop.hbase.regionserver.ScannerContext;
import org.apache.hadoop.hbase.regionserver.Store;
import org.apache.hadoop.hbase.regionserver.StoreFile;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.phoenix.coprocessor.DelegateRegionScanner;
import org.apache.phoenix.coprocessor.ServerScanUtil;
import org.apache.phoenix.hbase.index.covered.update.ColumnReference;
import org.apache.phoenix.hbase.index.hnsw.HnswOffheapAllocator;
import org.apache.phoenix.hbase.index.hnsw.HnswSegment;
import org.apache.phoenix.hbase.index.hnsw.HnswSegmentStore;
import org.apache.phoenix.hbase.index.util.ImmutableBytesPtr;
import org.apache.phoenix.index.IndexMaintainer;
import org.apache.phoenix.index.vector.VectorIndexTrainer;
import org.apache.phoenix.jdbc.PhoenixConnection;
import org.apache.phoenix.query.QueryServices;
import org.apache.phoenix.query.QueryServicesOptions;
import org.apache.phoenix.schema.CompiledConditionalTTLExpression;
import org.apache.phoenix.schema.PIndexState;
import org.apache.phoenix.schema.PTable;
import org.apache.phoenix.schema.PTable.IndexType;
import org.apache.phoenix.schema.PTableType;
import org.apache.phoenix.schema.VectorIndexType;
import org.apache.phoenix.util.EnvironmentEdgeManager;
import org.apache.phoenix.util.IndexUtil;
import org.apache.phoenix.util.QueryUtil;
import org.apache.phoenix.util.ScanUtil;
import org.apache.phoenix.util.SchemaUtil;
import org.apache.phoenix.util.ServerUtil;
import org.apache.phoenix.util.ServerUtil.ConnectionType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.phoenix.thirdparty.com.google.common.annotations.VisibleForTesting;

/**
 * Manages the HNSW graph of one vector index for one region of the data table.
 * <p>
 * The index of the region is a set of immutable segments and an in-memory mutable graph. A full
 * rebuild scans the region and writes a base {@link HnswSegment}. A change of a row goes into the
 * mutable graph, and masks the older entries of the row in the segments. A search reads the
 * segments and the in-memory graphs.
 * <p>
 * A flush starts when the mutable graph holds {@link #FLUSH_THRESHOLD} entries. A flush writes the
 * buffered changes as a delta segment on the base of the region. A newer delta masks the older
 * entries of its rows in its stack. A flush queues a full rebuild instead when the deltas reach
 * their limits, or when the region has no exact base. The full rebuild replaces the stack. If the
 * flushes are slower than the writes, the manager requests a split of the region, and then refuses
 * writes at a limit.
 * <p>
 * The region opens the manager when a write or a scan first names the index. The manager then
 * replays the rows that changed at or after the replay start of the newest segments. This recovers
 * the changes that the segments do not hold.
 * <p>
 * <b>Segments of the region.</b> The segments are cells of the hidden family {@code V#} of the
 * region. The manager reads and writes them through {@link HnswSegmentStore.RegionStore}, with
 * region-local operations and no RPC. Discovery reads all the segments of the index in the family
 * of the region, in all anchor rows. It ignores the segments of another index definition. The
 * region searches the newest bases that cover its range, clipped to the range, and their deltas. A
 * region retires only the segments that its own family holds. It retires a base when newer bases
 * cover the part of its range that is in the region. A delta follows its base.
 * <p>
 * <b>Uncovered regions.</b> The bases of a region can leave a part of its range without coverage.
 * This occurs in the upper daughter of a split, in a region of an active index after a TRUNCATE,
 * and in a region whose segments were lost or purged. Until a covering base lands, such a region
 * answers HNSW scans without candidate restriction. The scan reads the range, and the server ranks
 * its rows with the exact top-N, so the results stay exact. The rebuild of an uncovered region runs
 * before the rebuilds of covered regions.
 * <p>
 * <b>Why split daughters do not inherit.</b> A daughter of a split finds only the segments whose
 * anchor rows are in its range, through its reference to the {@code V#} file of the parent. A
 * region writes its segments at its own anchor row, which is below all split rows. Thus usually
 * only the lower daughter finds the parent segments. A merged region holds the segments of its
 * predecessors at their anchor rows until its rebuild retires them. If it splits before then,
 * either daughter can find segments of a predecessor. A daughter searches the segments that it
 * finds, clipped to its range, until its rebuild replaces them. The writer fence and the rules for
 * lost payloads below apply to them.
 * <p>
 * The upper daughter of a region with only its own segments inherits nothing. The parent cells
 * cannot move to the anchor row of the upper daughter, for two reasons. Both reasons apply to HBase
 * 2.5, 2.6 and 2.7:
 * <ul>
 * <li>A MOB reference cannot change rows. {@code MobFile.readCell} finds a payload by the key of
 * the reference cell itself, and does not check the key of the cell that it finds. A reference that
 * a reader moves to another row resolves to an empty value, or to the payload of another row.</li>
 * <li>The life of a MOB file follows the region that wrote it, not the references to it. Since
 * HBASE-26969, the MOB cleaner archives a MOB file when the directory of its writer region is gone
 * and the file is older than the minimum age. The cleaner does not use {@code MOB_FILE_REFS}.</li>
 * </ul>
 * Thus the upper daughter reads its full range for each HNSW scan until its first build. For the
 * second reason, an inherited segment is also usable only for a limited time. HBase archives the
 * payloads that a daughter or a merged region inherits some time after it removes the parent
 * region. The writer fence makes the region rebuild at open, and the rebuild usually replaces them
 * before then. If not, a load finds the payload lost. The region then leaves the segment out and
 * rebuilds, and reads without candidate restriction if it is uncovered.
 * <p>
 * A later change can remove the window of the upper daughter in one of these ways:
 * <ul>
 * <li>Move only the small cells, and keep the original row in a private tag. Resolve payloads by
 * the original key, and keep user MOB compactions away from the moved cells.</li>
 * <li>Keep the references of the daughters until the build of each daughter retires the parent
 * segments.</li>
 * <li>Change HBase, so that the MOB cleaner honors {@code MOB_FILE_REFS} and
 * {@code MobFile.readCell} checks the key of the cell. Phoenix cannot depend on this change.</li>
 * </ul>
 * <p>
 * <b>Writer fence.</b> Each segment records the encoded name of the region that wrote it. A region
 * trusts only the segments that it wrote itself. HBase keeps the MOB files of a segment only while
 * the region that wrote it exists. If a region finds segments of another region, it searches them,
 * clipped to its range, and rebuilds at once. This rule also applies to segments that cover the
 * range of the region exactly. Such segments occur in these cases:
 * <ul>
 * <li>after a merge and a split, before the rebuild</li>
 * <li>after a split and a merge back to the same range</li>
 * <li>in a cloned table</li>
 * </ul>
 * A region keeps its encoded name when it moves, when its server restarts, and after a snapshot
 * restore. Thus it continues to trust its segments in these cases.
 */
public final class HnswIndexManager implements VectorIndexManager {
  private static final Logger LOG = LoggerFactory.getLogger(HnswIndexManager.class);
  private static final VectorTypeSupport VTS =
    VectorizationProvider.getInstance().getVectorTypeSupport();
  /**
   * The number of mutable graph entries at which a flush starts. The entries are the graph nodes,
   * one for each vector change, and the deleted row keys.
   */
  public static final int FLUSH_THRESHOLD = 10_000;
  // The manager refuses writes while the mutable graph holds this many times FLUSH_THRESHOLD
  // entries or more
  static final int BLOCK_MULTIPLIER = 4;
  // While a full rebuild is in the queue, a delta flush starts only when the mutable graph holds
  // this many times FLUSH_THRESHOLD entries. The full rebuild replaces the stack, so these deltas
  // only keep the graph below the BLOCK_MULTIPLIER limit. A full rebuild that starts first swaps
  // the graph instead
  static final int QUEUED_FLUSH_MULTIPLIER = 2;
  /**
   * The largest number of deltas on a base while no full rebuild is in the queue. A flush that
   * needs one more delta queues a full rebuild. Each delta adds a segment that each search reads.
   */
  private static final int MAX_DELTAS = 4;
  /**
   * A flush queues a full rebuild when the changes reach this fraction of the rows in the base. The
   * changes are the row changes in the deltas and in the mutable graph.
   */
  private static final double REBUILD_RATIO = 0.25;
  /** The time to wait before the manager tries a failed flush, rebuild or split request again. */
  public static final long RETRY_DELAY_MS = 60_000;
  // The number of rows that a full rebuild scans between two checks for a closed manager
  private static final int CLOSE_CHECK_ROWS = 1000;
  // One full rebuild runs at a time on a server, to limit the memory that rebuilds use. The
  // rebuilds of uncovered regions run first, and the others in the order of their requests
  private static final ThreadPoolExecutor REBUILDS =
    new ThreadPoolExecutor(1, 1, 0L, TimeUnit.MILLISECONDS, new PriorityBlockingQueue<>(), r -> {
      Thread t = new Thread(r, "hnsw-rebuild");
      t.setDaemon(true);
      return t;
    });
  // Gives each rebuild request a sequence number, so that the queue keeps the order of the requests
  private static final AtomicLong REBUILD_SEQUENCE = new AtomicLong();
  // Delta flushes have their own thread, so they do not wait behind the full rebuilds of other
  // regions
  private static final ExecutorService DELTAS = Executors.newSingleThreadExecutor(r -> {
    Thread t = new Thread(r, "hnsw-delta");
    t.setDaemon(true);
    return t;
  });
  private static final ScheduledExecutorService RETRIES =
    Executors.newSingleThreadScheduledExecutor(r -> {
      Thread t = new Thread(r, "hnsw-rebuild-retry");
      t.setDaemon(true);
      return t;
    });
  // Split requests to the master run on a different thread, so that writes and flushes do not wait
  // for them. One request runs at a time, so that two requests for one region do not conflict
  private static final ExecutorService SPLITS = Executors.newSingleThreadExecutor(r -> {
    Thread t = new Thread(r, "hnsw-split");
    t.setDaemon(true);
    return t;
  });
  /**
   * The longest time that a split request waits for the master. A request that waits longer fails.
   */
  public static final long SPLIT_TIMEOUT_MS = 300_000;

  private final RegionCoprocessorEnvironment env;
  private final Region region;
  private final String indexName;
  private final byte[] startKey;
  private final byte[] endKey;
  private final PTable.VectorIndex vectorIndex;
  private final IndexMaintainer maintainer;
  // The store of the segment cells of the region. The segments of this index have this index key
  // and this definition digest
  private final HnswSegmentStore.RegionStore store;
  private final byte[] indexKey;
  private final byte[] definition;
  private final VectorSimilarityFunction similarity;
  private final HnswOffheapAllocator allocator;
  // Whether rows of the data table expire. A major compaction purges the expired rows, so the
  // region rebuilds after it
  private final boolean ttl;
  // The column families of the indexed columns. A vector change and a row delete write cells or
  // delete markers to these families
  private final Set<ImmutableBytesPtr> families = new LinkedHashSet<>();
  private final Catalog catalog;
  // A time no later than the start time of the oldest write batch of the region that has not
  // applied its graph changes
  private final LongSupplier oldestBatch;
  // The number of mutable graph entries at which the manager asks the master to split the region
  private final long splitTrigger;
  // Whether the manager can request a split. A request clears it. A swap that replaces the mutable
  // graph sets it again. The end of the retry delay after a failed request also sets it again
  private final AtomicBoolean splitArmed = new AtomicBoolean(true);

  /** Reads SYSTEM.CATALOG before a segment write, so that a stale manager writes no segment. */
  @FunctionalInterface
  interface Catalog {
    /**
     * Returns false if the index was dropped, or created again with a different definition or on a
     * different table. The manager is then stale. For a full rebuild, this method also sets the
     * Phoenix TTL attributes of the data table on the region scan. It also adds the columns that
     * TTL masking reads. A delta flush passes null, because it does not scan the region.
     */
    boolean isCurrent(Scan rebuildScan) throws IOException;
  }

  /** A segment that a search reads, with the mask of its replaced and deleted entries. */
  private static final class Source {
    final HnswSegment segment;
    final HnswSegment.Descriptor descriptor;
    // The number of row changes that the segment holds, which are its vectors and its tombstones.
    // REBUILD_RATIO counts them
    final int changes;
    // The ordinals of entries that a newer change of the same row replaces or deletes
    final Set<Integer> mask = ConcurrentHashMap.newKeySet();
    // Whether the segment covers rows outside this region, for example a segment of a split parent
    // or of a merged predecessor. A search of the segment then accepts only the keys of the region
    final boolean wider;

    Source(HnswSegment segment, HnswSegment.Descriptor descriptor, boolean wider) {
      this.segment = segment;
      this.descriptor = descriptor;
      this.changes = segment.size() + segment.getTombstones().length;
      this.wider = wider;
    }
  }

  /**
   * The state that a search reads: the segments and the in-memory graphs. A change that replaces a
   * graph or the segments installs a new state.
   */
  private static final class State {
    final List<Source> sources;
    final MutableGraph mutable;
    // The graph that a flush writes, or null, and the keys that changed after the swap. A search
    // skips these keys in that graph, and the cutover masks them in the new segment
    final MutableGraph flushing;
    final Set<ImmutableBytesPtr> changedSinceSwap;
    final long swapTime;
    // The replay start of the segment that the flush writes. It is the time of the swap, or the
    // start time of an older write batch that had not applied its graph changes at the swap
    final long replayFrom;
    // Whether the bases of the sources cover all of the range of the region. A scan that searches a
    // state without coverage reads the range without candidate restriction
    final boolean covered;

    State(List<Source> sources, MutableGraph mutable, MutableGraph flushing,
      Set<ImmutableBytesPtr> changedSinceSwap, long swapTime, long replayFrom, boolean covered) {
      this.sources = sources;
      this.mutable = mutable;
      this.flushing = flushing;
      this.changedSinceSwap = changedSinceSwap;
      this.swapTime = swapTime;
      this.replayFrom = replayFrom;
      this.covered = covered;
    }
  }

  private volatile State state;
  // Graph changes hold the read lock and run in parallel. A state change that replaces the mutable
  // graph or the segments holds the write lock. Thus no swap occurs during a graph change
  private final ReentrantReadWriteLock stateLock = new ReentrantReadWriteLock();

  /** The phase of the full rebuild task of the manager. */
  private enum Full {
    NONE,
    // The task is in the queue of the rebuild thread, for example behind the rebuilds of other
    // regions. It swaps the mutable graph only when it starts. Until then, the region continues to
    // write deltas, unless a repair is pending
    QUEUED,
    // The task started, and waits for the end of the delta flush task that runs. No delta flush
    // starts in this phase
    STARTING,
    // The task swapped the graphs, and builds and writes the base segment
    RUNNING
  }

  /**
   * A queued full rebuild. The rebuild thread runs the rebuilds of uncovered regions first, and the
   * other rebuilds in the order of their requests.
   */
  static final class Rebuild implements Runnable, Comparable<Rebuild> {
    private final Runnable task;
    private final boolean uncovered;
    private final long sequence = REBUILD_SEQUENCE.getAndIncrement();

    Rebuild(Runnable task, boolean uncovered) {
      this.task = task;
      this.uncovered = uncovered;
    }

    @Override
    public void run() {
      task.run();
    }

    @Override
    public int compareTo(Rebuild other) {
      if (uncovered != other.uncovered) {
        return uncovered ? -1 : 1;
      }
      return Long.compare(sequence, other.sequence);
    }
  }

  private Full full = Full.NONE;
  // The task of the full rebuild in the queue of the rebuild thread, while full is QUEUED
  private Rebuild queued;
  // The number of the delta flush task that is queued or runs, or 0
  private long deltaTask;
  // Whether that task runs. A full rebuild that starts waits for the end of a delta flush task that
  // runs. It takes over the graph of a delta flush task in the queue, and that task then does
  // nothing
  private boolean deltaRunning;
  // The number of the last delta flush task that the manager submitted
  private long deltaTasks;
  // Whether a request for a full rebuild came while a full rebuild runs. The running rebuild
  // swapped before the request, so the request cannot join it. The end of the running rebuild
  // queues it
  private boolean rebuildPending;
  // Whether a full rebuild must swap before the next delta flush. A change that fails to apply sets
  // this flag. A major compaction that purges expired rows also sets it. Segments that the region
  // cannot trust at open also set it. A delta would move the replay start of the region past the
  // change or the compaction. A region that opens after that delta would then trust segments that
  // do not hold the change
  private boolean repair;
  // The swap time of the full rebuild that runs
  private long fullSwapTime;
  // The base time of the delta flush that owns the graph of the flush. It is null if a full rebuild
  // owns that graph
  private Long deltaBase;
  // The newest swap time or build request time. At open, it is the newest segment time of the
  // region. A swap takes a time later than all earlier swaps and requests. Thus swap times are
  // unique, and a full rebuild that starts after a build request swaps at a later time. Another
  // server can have written the segments, and its clock can be ahead
  private long clock;
  // Whether a major compaction ended while a flush held a graph. That flush swapped before the
  // compaction, so a flush that swaps after the compaction must follow. The next swap clears it
  private boolean compactionPending;
  private volatile boolean closed;

  /**
   * Opens the manager of an index for the region of the environment. The manager opens the current
   * segments, and replays the row changes that the segments do not hold.
   * @param env         the region coprocessor environment
   * @param indexName   the full name of the index
   * @param oldestBatch a time no later than the start time of the oldest write batch of the region
   *                    that has not applied its graph changes, or Long.MAX_VALUE
   * @return the open manager
   * @throws DoNotRetryIOException if the index does not exist, or is not an HNSW index of the data
   *                               table of the region
   * @throws IOException           if the manager cannot read the index, the data table or the
   *                               segments
   */
  static HnswIndexManager open(RegionCoprocessorEnvironment env, String indexName,
    LongSupplier oldestBatch) throws IOException {
    try (PhoenixConnection conn =
      QueryUtil.getConnectionOnServer(env.getConfiguration()).unwrap(PhoenixConnection.class)) {
      PTable index = conn.getTableNoCache(indexName);
      PTable data = dataTableOf(conn, index);
      checkIndexOf(index, data, env.getRegion().getRegionInfo().getTable());
      String definition = definition(index);
      return new HnswIndexManager(env, index, index.getIndexMaintainer(data, conn),
        index.getIndexState() == PIndexState.ACTIVE, hasTtl(data, env.getRegion()),
        scan -> isCurrent(env, indexName, definition, scan), oldestBatch);
    } catch (org.apache.phoenix.schema.TableNotFoundException e) {
      throw new DoNotRetryIOException("Cannot open HNSW index " + indexName, e);
    } catch (SQLException e) {
      throw new IOException("Cannot open HNSW index " + indexName, e);
    }
  }

  // Returns the data table of an index, or null if the table is not an index
  private static PTable dataTableOf(PhoenixConnection conn, PTable index) throws SQLException {
    return index.getType() == PTableType.INDEX
      ? conn.getTableNoCache(index.getParentName().getString())
      : null;
  }

  /**
   * Fails unless {@code index} is an HNSW index of {@code data}, the data table that {@code table}
   * stores. The manager must check the index, because a scan supplies the index name, and the
   * manager writes the segments of the index to the region.
   */
  static void checkIndexOf(PTable index, PTable data, TableName table)
    throws DoNotRetryIOException {
    if (
      index.getType() != PTableType.INDEX || index.getIndexType() != IndexType.VECTOR_LOCAL
        || index.getVectorIndexType() != VectorIndexType.HNSW
    ) {
      throw new DoNotRetryIOException(index.getName() + " is not an HNSW index");
    }
    if (data == null || !TableName.valueOf(data.getPhysicalName().getBytes()).equals(table)) {
      throw new DoNotRetryIOException(
        "HNSW index " + index.getName() + " is not an index of table " + table);
    }
  }

  /**
   * Returns the definition of an index: its parameters and its vector expression. Each segment
   * stores the digest of the definition of its index. Thus the segments of an index that was
   * dropped and created again with the same name apply only if the definition did not change.
   */
  public static String definition(PTable index) {
    PTable.VectorIndex vi = index.getVectorIndex();
    return vi.getDistanceMetric() + "," + vi.getDimension() + "," + vi.getHnswM() + ","
      + vi.getHnswEfConstruction() + "," + vi.getHnswAlpha() + "," + vi.getQuantizationType() + ","
      + vi.getPqSegments() + ","
      + VectorIndexTrainer.getIndexedVectorColumn(index).getExpressionStr();
  }

  // Whether rows of the data table expire. A literal TTL is on the column families, and a
  // conditional TTL is in SYSTEM.CATALOG
  private static boolean hasTtl(PTable data, Region region) {
    if (data.hasConditionalTTL()) {
      return true;
    }
    for (ColumnFamilyDescriptor cf : region.getTableDescriptor().getColumnFamilies()) {
      if (cf.getTimeToLive() != HConstants.FOREVER) {
        return true;
      }
    }
    return false;
  }

  // Reads the index and the data table again before a segment write. The manager compiles the TTL
  // expression for each rebuild, as a client compiles it for each query
  private static boolean isCurrent(RegionCoprocessorEnvironment env, String indexName,
    String definition, Scan rebuildScan) throws IOException {
    try (PhoenixConnection conn =
      QueryUtil.getConnectionOnServer(env.getConfiguration()).unwrap(PhoenixConnection.class)) {
      PTable index;
      PTable data;
      try {
        index = conn.getTableNoCache(indexName);
        data = dataTableOf(conn, index);
      } catch (org.apache.phoenix.schema.TableNotFoundException e) {
        return false;
      }
      if (!isCurrent(index, data, env.getRegion().getRegionInfo().getTable(), definition)) {
        return false;
      }
      if (rebuildScan != null) {
        ScanUtil.setScanAttributesForPhoenixTTL(rebuildScan, data, conn);
        // TTLRegionScanner finds an expired row by its empty column. It evaluates a conditional TTL
        // over the columns that the expression reads
        rebuildScan.addColumn(SchemaUtil.getEmptyColumnFamily(data),
          SchemaUtil.getEmptyColumnQualifier(data));
        if (data.hasConditionalTTL()) {
          for (ColumnReference ref : ((CompiledConditionalTTLExpression) data
            .getCompiledTTLExpression(conn)).getColumnsReferenced()) {
            rebuildScan.addColumn(ref.getFamily(), ref.getQualifier());
          }
        }
      }
      return true;
    } catch (SQLException e) {
      throw new IOException("Cannot read HNSW index " + indexName, e);
    }
  }

  /**
   * Returns true if a manager that opened with {@code definition} can serve {@code index}. The
   * index must be an HNSW index of {@code data}, the data table that {@code table} stores, with the
   * same definition. An index created again with the same name can be a different kind of index, or
   * an index of a different table.
   */
  static boolean isCurrent(PTable index, PTable data, TableName table, String definition) {
    try {
      checkIndexOf(index, data, table);
    } catch (DoNotRetryIOException e) {
      return false;
    }
    return definition.equals(definition(index));
  }

  /**
   * Deletes the segment cells of a dropped index from the region, in all anchor rows, and returns
   * the number of deleted cells. The caller first checks the purge with {@link #checkPurge}, and
   * closes the manager of the index in the region.
   * @param env       the coprocessor environment of the region
   * @param indexName the name of the index without view prefixes, the key of its segments
   */
  public static long purge(RegionCoprocessorEnvironment env, String indexName) throws IOException {
    return new HnswSegmentStore.RegionStore(env).purge(Bytes.toBytes(indexName));
  }

  /**
   * Fails if the catalog has an HNSW index of the given name on the table of the region, so that a
   * purge never deletes the segments of a live index. A purge of a dropped index, or of a name that
   * is now another kind of index or an index of another table, can continue.
   * @throws DoNotRetryIOException if the index is an HNSW index of the table of the region
   */
  public static void checkPurge(RegionCoprocessorEnvironment env, String indexName)
    throws IOException {
    try (PhoenixConnection conn =
      QueryUtil.getConnectionOnServer(env.getConfiguration()).unwrap(PhoenixConnection.class)) {
      PTable index;
      PTable data;
      try {
        index = conn.getTableNoCache(indexName);
        data = dataTableOf(conn, index);
      } catch (org.apache.phoenix.schema.TableNotFoundException e) {
        return;
      }
      TableName table = env.getRegion().getRegionInfo().getTable();
      try {
        checkIndexOf(index, data, table);
      } catch (DoNotRetryIOException e) {
        return;
      }
      throw new DoNotRetryIOException(
        "HNSW index " + indexName + " of table " + table + " exists; its segments are not purged");
    } catch (SQLException e) {
      throw new IOException("Cannot read HNSW index " + indexName, e);
    }
  }

  @VisibleForTesting
  HnswIndexManager(RegionCoprocessorEnvironment env, PTable index, IndexMaintainer maintainer,
    boolean active, boolean ttl, Catalog catalog, LongSupplier oldestBatch) throws IOException {
    this.env = env;
    this.region = env.getRegion();
    this.indexName = index.getName().getString();
    this.startKey = region.getRegionInfo().getStartKey();
    this.endKey = region.getRegionInfo().getEndKey();
    this.vectorIndex = index.getVectorIndex();
    this.maintainer = maintainer;
    this.store = new HnswSegmentStore.RegionStore(env);
    this.indexKey = Bytes.toBytes(indexName);
    this.definition = HnswSegment.definitionDigest(definition(index));
    this.similarity = HnswSegment.similarityFunction(vectorIndex.getDistanceMetric());
    this.allocator = HnswOffheapAllocator.get(env.getConfiguration());
    this.ttl = ttl;
    for (ColumnReference ref : maintainer.getAllColumnsForDataTable()) {
      families.add(new ImmutableBytesPtr(ref.getFamily()));
    }
    this.catalog = catalog;
    this.oldestBatch = oldestBatch;
    int splitMultiplier = env.getConfiguration().getInt(QueryServices.HNSW_SPLIT_MULTIPLIER_ATTRIB,
      QueryServicesOptions.DEFAULT_HNSW_SPLIT_MULTIPLIER);
    // A multiplier of 0 or less turns the split requests off
    this.splitTrigger =
      splitMultiplier > 0 ? (long) splitMultiplier * FLUSH_THRESHOLD : Long.MAX_VALUE;

    List<HnswSegment.Descriptor> current;
    List<Source> sources;
    List<HnswSegment.Descriptor> unreadable = new ArrayList<>();
    Set<ImmutableBytesPtr> missing = new HashSet<>();
    do {
      unreadable.clear();
      current = listCurrentSegments();
      sources = openSources(current, Collections.emptyMap(), unreadable, missing);
    } while (sources == null);
    for (HnswSegment.Descriptor d : current) {
      clock = Math.max(clock, d.time);
    }
    List<HnswSegment.Descriptor> readable = new ArrayList<>(current);
    readable.removeAll(unreadable);
    for (HnswSegment.Descriptor d : unreadable) {
      // The replay starts after the changes of a lost delta, so the stack of the delta misses
      // them. Thus the base of that stack does not count toward the coverage of the region
      if (d.isDelta()) {
        readable.removeIf(r -> stackId(r).equals(stackId(d)));
      }
    }
    this.state = new State(sources, new MutableGraph(vectorIndex, similarity), null, null, 0, 0,
      HnswSegment.covered(startKey, endKey, readable));
    try {
      applyStackMasks(sources);
      long replayStart = current.isEmpty() ? Long.MAX_VALUE : replayStartTime(current);
      if (!current.isEmpty()) {
        replay(replayStart);
      }
      LOG.info("Opened HNSW index {} for region {} with {} segments", indexName,
        region.getRegionInfo().getEncodedName(), sources.size());
      // Rebuild immediately if the region cannot read a segment. Also rebuild if the region has no
      // exact base stack that it can trust. A region of a BUILDING index without segments waits for
      // IndexTool
      if (
        !unreadable.isEmpty() || !trusted(current, replayStart) && (active || !current.isEmpty())
      ) {
        repair();
      }
    } catch (IOException | RuntimeException e) {
      close();
      throw e;
    }
  }

  // Returns true if the region can keep its exact base stack after the replay, and false if it must
  // rebuild. The region trusts only the segments that it wrote itself, the base and each delta.
  // HBase can archive the MOB files of a segment after the region that wrote it is gone, also while
  // other regions use them. The region searches the exact stack of another region until its rebuild
  // replaces it
  private boolean trusted(List<HnswSegment.Descriptor> current, long replayStart)
    throws IOException {
    if (exactBase(current, startKey, endKey) == null) {
      return false;
    }
    byte[] name = region.getRegionInfo().getEncodedNameAsBytes();
    for (HnswSegment.Descriptor d : current) {
      if (!Bytes.equals(name, d.writer)) {
        LOG.info("HNSW index {} region {} found segments of region {}; rebuilding", indexName,
          region.getRegionInfo().getEncodedName(), Bytes.toStringBinary(d.writer));
        return false;
      }
    }
    // A major compaction after the replay start can purge the delete markers of rows that a segment
    // still holds. Only the memory of the closed manager knew that these rows were deleted. Each
    // store of a family that replay reads compacts on its own schedule. The file that a major
    // compaction writes gives the end time of the compaction
    for (ImmutableBytesPtr f : families) {
      Store store = region.getStore(f.copyBytesIfNecessary());
      if (store == null) {
        continue;
      }
      for (StoreFile file : store.getStorefiles()) {
        if (file.isMajorCompactionResult() && file.getModificationTimestamp() > replayStart) {
          LOG.info("HNSW index {} region {} had a major compaction after its segments; rebuilding",
            indexName, region.getRegionInfo().getEncodedName());
          return false;
        }
      }
    }
    return true;
  }

  private static final class StackId {
    final ImmutableBytesPtr startKey;
    final long baseTime;

    StackId(byte[] startKey, long baseTime) {
      this.startKey = new ImmutableBytesPtr(startKey);
      this.baseTime = baseTime;
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) {
        return true;
      }
      if (!(o instanceof StackId)) {
        return false;
      }
      StackId other = (StackId) o;
      return baseTime == other.baseTime && startKey.equals(other.startKey);
    }

    @Override
    public int hashCode() {
      return 31 * startKey.hashCode() + Long.hashCode(baseTime);
    }
  }

  private static StackId stackId(HnswSegment.Descriptor d) {
    return new StackId(d.startKey, d.isDelta() ? d.baseTime : d.time);
  }

  /** Masks each segment entry that a newer delta of the same stack replaces or deletes. */
  private static void applyStackMasks(List<Source> sources) {
    List<HnswSegment.Descriptor> descriptors = new ArrayList<>(sources.size());
    List<HnswSegment> segments = new ArrayList<>(sources.size());
    for (Source source : sources) {
      descriptors.add(source.descriptor);
      segments.add(source.segment);
    }
    List<Set<Integer>> masks = stackMasks(descriptors, segments);
    for (int i = 0; i < sources.size(); i++) {
      sources.get(i).mask.addAll(masks.get(i));
    }
  }

  /**
   * Returns the masked ordinals of each segment. A delta masks the older entries of its rows in its
   * stack. The rows of a delta are its entries and its tombstones. The segments are in newest first
   * order, and each descriptor has the same position as its segment. The fsck tools use the same
   * rule.
   */
  public static List<Set<Integer>> stackMasks(List<HnswSegment.Descriptor> descriptors,
    List<HnswSegment> segments) {
    Map<StackId, Set<ImmutableBytesPtr>> newerKeys = new HashMap<>();
    List<Set<Integer>> masks = new ArrayList<>(segments.size());
    for (int s = 0; s < segments.size(); s++) {
      HnswSegment.Descriptor descriptor = descriptors.get(s);
      HnswSegment segment = segments.get(s);
      Set<ImmutableBytesPtr> newer =
        newerKeys.computeIfAbsent(stackId(descriptor), k -> new HashSet<>());
      Set<Integer> mask = new HashSet<>();
      for (ImmutableBytesPtr key : newer) {
        int ordinal = segment.ordinalOf(key);
        if (ordinal >= 0) {
          mask.add(ordinal);
        }
      }
      if (descriptor.isDelta()) {
        for (int i = 0; i < segment.size(); i++) {
          newer.add(new ImmutableBytesPtr(segment.getKey(i)));
        }
        for (byte[] tombstone : segment.getTombstones()) {
          newer.add(new ImmutableBytesPtr(tombstone));
        }
      }
      masks.add(mask);
    }
    return masks;
  }

  /**
   * Returns the replay start of the region: the oldest replay start of the newest segments of the
   * stacks. A stack holds all changes before the replay start of its newest segment.
   */
  static long replayStartTime(List<HnswSegment.Descriptor> current) {
    Map<StackId, HnswSegment.Descriptor> newestPerStack = new HashMap<>();
    for (HnswSegment.Descriptor d : current) {
      newestPerStack.merge(stackId(d), d, (a, b) -> a.time >= b.time ? a : b);
    }
    long start = Long.MAX_VALUE;
    for (HnswSegment.Descriptor d : newestPerStack.values()) {
      start = Math.min(start, d.replayFrom);
    }
    return start;
  }

  /**
   * Returns the current segments of a region of the range {@code [startKey, endKey)}, newest first.
   * These are the bases that overlap the range and that newer bases do not cover within the range.
   * The deltas of these bases are also current.
   */
  static List<HnswSegment.Descriptor> currentSegments(List<HnswSegment.Descriptor> all,
    byte[] startKey, byte[] endKey) {
    List<HnswSegment.Descriptor> overlappingBases = new ArrayList<>();
    List<HnswSegment.Descriptor> overlappingDeltas = new ArrayList<>();
    for (HnswSegment.Descriptor d : all) {
      if (d.overlaps(startKey, endKey)) {
        if (d.isDelta()) {
          overlappingDeltas.add(d);
        } else {
          overlappingBases.add(d);
        }
      }
    }
    List<HnswSegment.Descriptor> currentBases = new ArrayList<>();
    for (HnswSegment.Descriptor b : overlappingBases) {
      List<HnswSegment.Descriptor> newerBases = new ArrayList<>();
      for (HnswSegment.Descriptor n : overlappingBases) {
        if (n.time > b.time) {
          newerBases.add(n);
        }
      }
      byte[] from = Bytes.compareTo(b.startKey, startKey) > 0 ? b.startKey : startKey;
      byte[] to =
        endKey.length == 0 || (b.endKey.length > 0 && Bytes.compareTo(b.endKey, endKey) < 0)
          ? b.endKey
          : endKey;
      if (!HnswSegment.covered(from, to, newerBases)) {
        currentBases.add(b);
      }
    }
    List<HnswSegment.Descriptor> current = new ArrayList<>(currentBases);
    for (HnswSegment.Descriptor d : overlappingDeltas) {
      for (HnswSegment.Descriptor b : currentBases) {
        if (d.baseTime == b.time && Bytes.equals(d.startKey, b.startKey)) {
          current.add(d);
          break;
        }
      }
    }
    current.sort((a, b) -> Long.compare(b.time, a.time));
    return current;
  }

  // Lists the current segments of the index in the family of the region, with the definition of
  // the index. The region does not search the segments of another definition, for example of an
  // index that was dropped and created again with the same name. A cutover retires them
  private List<HnswSegment.Descriptor> listCurrentSegments() throws IOException {
    List<HnswSegment.Descriptor> fenced = new ArrayList<>();
    for (HnswSegment.Descriptor d : store.list(indexKey)) {
      if (Bytes.equals(d.definition, definition)) {
        fenced.add(d);
      }
    }
    return currentSegments(fenced, startKey, endKey);
  }

  /**
   * Returns the sources of the given segments, newest first. The method uses the sources that are
   * open already, and opens the others. A base that holds no vectors gets no source. A delta always
   * gets a source, because its tombstones mask older entries. Returns null if the region retired a
   * segment after the listing. The caller then lists again. The method leaves out a segment whose
   * payload or tombstones are lost or corrupt, and adds it to {@code unreadable}. A full rebuild
   * then replaces it. A segment is also unreadable if a new listing still returns it after a load
   * found no payload cell. Thus the caller does not list again without end.
   * @param missing the identities of the segments that a load found without a payload cell. The
   *                caller keeps this set across listings.
   */
  private List<Source> openSources(List<HnswSegment.Descriptor> current,
    Map<ImmutableBytesPtr, Source> open, List<HnswSegment.Descriptor> unreadable,
    Set<ImmutableBytesPtr> missing) throws IOException {
    List<Source> sources = new ArrayList<>(current.size());
    List<Source> opened = new ArrayList<>();
    try {
      for (HnswSegment.Descriptor d : current) {
        ImmutableBytesPtr identity = new ImmutableBytesPtr(d.identity);
        Source source = open.get(identity);
        if (source == null && (d.count > 0 || d.isDelta())) {
          if (missing.contains(identity)) {
            LOG.error("HNSW segment {} of index {} for region {} has no payload; rebuilding", d,
              indexName, region.getRegionInfo().getEncodedName());
            unreadable.add(d);
            continue;
          }
          HnswSegment segment;
          try {
            segment = HnswSegment.open(store, d, allocator, vectorIndex.getDistanceMetric());
          } catch (HnswSegment.NotFoundException e) {
            missing.add(identity);
            throw e;
          } catch (DoNotRetryIOException | RuntimeException e) {
            LOG.error("Cannot read HNSW segment {} of index {} for region {}; rebuilding", d,
              indexName, region.getRegionInfo().getEncodedName(), e);
            unreadable.add(d);
            continue;
          }
          source = new Source(segment, d, !d.covers(startKey, endKey));
          opened.add(source);
        }
        if (source != null) {
          sources.add(source);
        }
      }
      return sources;
    } catch (IOException | RuntimeException e) {
      for (Source source : opened) {
        source.segment.close();
      }
      if (e instanceof HnswSegment.NotFoundException) {
        return null;
      }
      throw e;
    }
  }

  /**
   * Decides how a search continues if it does not find a segment that it reads. A cutover of the
   * region installs its new sources before it closes and retires the old ones, so the search runs
   * again on the new sources. Otherwise the payload cell of the segment is missing, and the region
   * leaves the segment out and rebuilds. Returns true if the search can run again. Returns false if
   * the manager closed, or if the search must fail.
   */
  private boolean replaceRetired(List<Source> searched, Source retired) {
    synchronized (this) {
      if (closed) {
        return false;
      }
      if (state.sources != searched) {
        // A cutover or another change installed new sources. The caller searches them
        return true;
      }
    }
    leaveOut(searched, retired);
    return false;
  }

  /**
   * Leaves out a source whose payload is lost, missing or corrupt, and rebuilds the region. Until a
   * covering base lands, the region is uncovered, so its HNSW scans read the range without
   * candidate restriction. Does nothing if a cutover or another change installed new sources.
   */
  private void leaveOut(List<Source> searched, Source lost) {
    synchronized (this) {
      if (closed || state.sources != searched) {
        return;
      }
      stateLock.writeLock().lock();
      try {
        State s = state;
        List<Source> sources = new ArrayList<>(s.sources);
        sources.remove(lost);
        state = new State(sources, s.mutable, s.flushing, s.changedSinceSwap, s.swapTime,
          s.replayFrom, false);
      } finally {
        stateLock.writeLock().unlock();
      }
      LOG.error("HNSW segment {} of index {} for region {} cannot be read; rebuilding",
        lost.descriptor, indexName, region.getRegionInfo().getEncodedName());
      repair();
      // If the region queued its rebuild while it was covered, queue the rebuild again as
      // uncovered, so that it runs before the rebuilds of covered regions. If the rebuild thread
      // already took the task from its queue, the task runs next
      if (full == Full.QUEUED && !queued.uncovered && REBUILDS.remove(queued)) {
        queueFull();
      }
    }
    lost.segment.close();
  }

  /**
   * Applies a committed row change to the mutable graph. The batch calls this method while it holds
   * its row locks, so the method does not throw. If a change fails to apply, the region rebuilds
   * from its rows, which hold the change. This rebuild follows any full rebuild that runs.
   */
  @Override
  public void onMutation(Put currentDataRowState, Put nextDataRowState, float[] nextVector) {
    try {
      applyMutation(currentDataRowState, nextDataRowState, nextVector);
    } catch (RuntimeException e) {
      Put row = nextDataRowState != null ? nextDataRowState : currentDataRowState;
      LOG.error("Cannot apply the change of row {} to HNSW index {} for region {}; rebuilding",
        row == null ? null : Bytes.toStringBinary(row.getRow()), indexName,
        region.getRegionInfo().getEncodedName(), e);
      repair();
    }
  }

  // Requests a full rebuild that swaps before the next delta flush. A full rebuild that runs now
  // can have read the rows before the change or the compaction. Its segment can then miss the
  // change or the compaction, so another full rebuild follows it
  private synchronized void repair() {
    repair = true;
    rebuildPending |= full == Full.RUNNING;
    flush(true);
  }

  private void applyMutation(Put currentDataRowState, Put nextDataRowState, float[] next) {
    float[] current = currentDataRowState == null
      ? null
      : maintainer.getVectorAsFloats(new IndexUtil.SimpleValueGetter(currentDataRowState),
        HConstants.LATEST_TIMESTAMP, false);
    Put row = nextDataRowState != null ? nextDataRowState : currentDataRowState;
    ImmutableBytesPtr key = new ImmutableBytesPtr(row.getRow());
    if (Arrays.equals(next, current)) {
      // A strict conditional TTL reads an expired row as absent and masks its columns. A row
      // without a current state can then still have its old vector in the graph
      if (next == null && currentDataRowState == null && indexed(state, key)) {
        apply(key, null);
      }
      return;
    }
    apply(key, next);
  }

  // Returns true if a search of the given state can return the key
  private static boolean indexed(State s, ImmutableBytesPtr key) {
    if (
      s.mutable.live().containsKey(key)
        || (s.flushing != null && s.flushing.live().containsKey(key))
    ) {
      return true;
    }
    for (Source source : s.sources) {
      int ordinal = source.segment.ordinalOf(key);
      if (ordinal >= 0 && !source.mask.contains(ordinal)) {
        return true;
      }
    }
    return false;
  }

  /**
   * Refuses a write with a retryable {@link RegionTooBusyException} while the mutable graph holds
   * BLOCK_MULTIPLIER times FLUSH_THRESHOLD entries or more. This bounds the heap use while a flush
   * is queued, runs or waits for its retry. The limit is for each manager, so the heap use of a
   * server grows with its number of regions and HNSW indexes.
   */
  @Override
  public void checkResources() throws RegionTooBusyException {
    if (state.mutable.size() >= BLOCK_MULTIPLIER * FLUSH_THRESHOLD) {
      throw new RegionTooBusyException("Over HNSW mutable graph limit, index=" + indexName
        + ", region=" + region.getRegionInfo().getEncodedName());
    }
  }

  // Applies a vector change or a delete to the mutable graph, and masks the older entries of the
  // row in the segments. Changes to different rows run in parallel. Row locks order the changes to
  // one row
  private void apply(ImmutableBytesPtr key, float[] vector) {
    State s;
    stateLock.readLock().lock();
    try {
      s = state;
      // Add the new version before the masks hide the older ones. A search reads the sources oldest
      // first, so it finds one of the versions. A search that overlaps a swap runs again
      if (vector != null) {
        s.mutable.upsert(key, vector);
      } else {
        s.mutable.delete(key);
      }
      if (s.changedSinceSwap != null) {
        s.changedSinceSwap.add(key);
      }
      for (Source source : s.sources) {
        int ordinal = source.segment.ordinalOf(key);
        if (ordinal >= 0) {
          source.mask.add(ordinal);
        }
      }
    } finally {
      stateLock.readLock().unlock();
    }
    int size = s.mutable.size();
    // If the mutable graph gets to the split trigger, the flushes are slower than the writes. The
    // manager then requests a split of the region, once until a swap replaces the mutable graph
    if (size >= splitTrigger && splitArmed.compareAndSet(true, false)) {
      requestSplit();
    }
    // A flush that holds a graph starts the next flush when it ends
    if (s.flushing == null && size >= FLUSH_THRESHOLD) {
      flush(false);
    }
  }

  /**
   * Asks the master to split the region. The request runs on the split thread. HBase selects the
   * split point. HBase refuses the split if the split switch, the table descriptor or the region
   * split policy does not allow it. After a failed or refused request, the manager can request
   * again after {@link #RETRY_DELAY_MS}.
   */
  private void requestSplit() {
    SPLITS.execute(() -> {
      if (closed) {
        return;
      }
      String name = region.getRegionInfo().getEncodedName();
      try (Admin admin = ServerUtil.ConnectionFactory
        .getConnection(ConnectionType.DEFAULT_SERVER_CONNECTION, env).getAdmin()) {
        admin.splitRegionAsync(region.getRegionInfo().getRegionName()).get(SPLIT_TIMEOUT_MS,
          TimeUnit.MILLISECONDS);
        LOG.info("Split region {}: its writes outpace the flushes of HNSW index {}", name,
          indexName);
      } catch (Exception e) {
        if (e instanceof InterruptedException) {
          Thread.currentThread().interrupt();
        }
        LOG.info("Cannot split region {} for HNSW index {}; will request again: {}", name,
          indexName, e.toString());
        LOG.debug("Split request of region {} failed", name, e);
        RETRIES.schedule(() -> splitArmed.set(true), RETRY_DELAY_MS, TimeUnit.MILLISECONDS);
      }
    });
  }

  /**
   * Returns a raw, key-only scan for the rows that changed at or after the given time. Such a row
   * has a cell or a delete marker of that time or later in a family of the indexed columns. A
   * vector change and a row delete write cells or delete markers to these families. Replay reads
   * these rows again, and the fsck tooling counts them as held in memory.
   */
  public static Scan changedRowsScan(IndexMaintainer maintainer, long since) throws IOException {
    Scan scan = new Scan().setRaw(true).setFilter(new KeyOnlyFilter()).setCacheBlocks(false);
    for (ColumnReference ref : maintainer.getAllColumnsForDataTable()) {
      scan.addFamily(ref.getFamily());
    }
    scan.setTimeRange(Math.max(0, since), HConstants.LATEST_TIMESTAMP);
    return scan;
  }

  // Reads again each row that changed at or after the given time, and applies its current vector or
  // its delete to the mutable graph. This recovers the changes that no segment holds
  private void replay(long since) throws IOException {
    Set<ImmutableBytesPtr> changed = new HashSet<>();
    try (RegionScanner scanner = region.getScanner(changedRowsScan(maintainer, since))) {
      List<Cell> cells = new ArrayList<>();
      boolean more;
      do {
        more = scanner.next(cells);
        if (!cells.isEmpty()) {
          changed.add(new ImmutableBytesPtr(CellUtil.cloneRow(cells.get(0))));
          cells.clear();
        }
      } while (more);
    }
    for (ImmutableBytesPtr key : changed) {
      apply(key, vectorOf(maintainer, region.get(new Get(key.copyBytesIfNecessary()))));
    }
    LOG.info("Replayed {} rows changed since {} into HNSW index {} for region {}", changed.size(),
      since, indexName, region.getRegionInfo().getEncodedName());
  }

  /**
   * Returns the indexed vector of a data row that was read with the columns of the maintainer, or
   * null if the row has no vector.
   */
  public static float[] vectorOf(IndexMaintainer maintainer, Result row) throws IOException {
    if (row.isEmpty()) {
      return null;
    }
    Put put = new Put(row.getRow());
    for (Cell cell : row.rawCells()) {
      put.add(cell);
    }
    return maintainer.getVectorAsFloats(new IndexUtil.SimpleValueGetter(put),
      HConstants.LATEST_TIMESTAMP, false);
  }

  // Returns the dimension of the indexed vectors
  int dimension() {
    return vectorIndex.getDimension();
  }

  /** Returns true if the index reads columns of the given column family. */
  public boolean indexes(byte[] family) {
    return families.contains(new ImmutableBytesPtr(family));
  }

  /**
   * Updates the segments after a major compaction of an indexed family. The compaction purges the
   * delete markers of rows that only the buffered changes delete. This method writes these changes
   * to a segment, and moves the replay start of the region past the compaction. It writes a segment
   * also when the manager buffers no change, to move the replay start. On a table with a TTL, the
   * region rebuilds instead, so that the expired rows leave the segments. A region whose replay
   * start is before a major compaction rebuilds when it opens.
   */
  public void flush() {
    if (ttl) {
      repair();
    } else {
      flush(false, true);
    }
  }

  /**
   * Requests a full rebuild for a build scan, and returns the server time at which the region
   * accepted the request. A full rebuild that starts later swaps at a later time. A base segment
   * newer than the returned time then holds every row committed before the request. If a full
   * rebuild runs, it swapped earlier, so a second full rebuild follows it. A repeated request of
   * the same build can find a running full rebuild or an exact base that is newer than
   * {@code after}. The request then returns {@code after}, and requests no rebuild.
   * @param after the time that an earlier request of the same build returned, or Long.MAX_VALUE
   */
  public synchronized long rebuild(long after) {
    // The time of the exact base of the region, or null. A rebuild request needs only the base, so
    // the delta limits do not apply
    Long base = deltaBase(state.sources, 0, startKey, endKey, true);
    if (full == Full.RUNNING && fullSwapTime > after || base != null && base > after) {
      return after;
    }
    clock = Math.max(clock, EnvironmentEdgeManager.currentTimeMillis());
    long accepted = clock;
    rebuildPending |= full == Full.RUNNING;
    flush(true);
    return accepted;
  }

  /**
   * Returns a scanner that answers a build scan with one cell at the start key of the region. The
   * cell holds the time at which the region accepted the request. A purge scan gets the same answer
   * with the number of deleted cells.
   */
  public static RegionScanner acceptedScanner(RegionScanner scanner, long accepted) {
    return new DelegateRegionScanner(scanner) {
      private Cell cell =
        new KeyValue(scanner.getRegionInfo().getStartKey(), HConstants.EMPTY_BYTE_ARRAY,
          HConstants.EMPTY_BYTE_ARRAY, accepted, Bytes.toBytes(accepted));

      @Override
      public boolean next(List<Cell> result) {
        return nextRaw(result);
      }

      @Override
      public boolean next(List<Cell> result, ScannerContext context) {
        return nextRaw(result);
      }

      @Override
      public boolean nextRaw(List<Cell> result, ScannerContext context) {
        return nextRaw(result);
      }

      @Override
      public boolean nextRaw(List<Cell> result) {
        if (cell != null) {
          result.add(cell);
          cell = null;
        }
        return false;
      }
    };
  }

  private void flush(boolean force) {
    flush(force, false);
  }

  // Queues a full rebuild, or swaps the mutable graph and submits a delta flush. A full rebuild
  // swaps when its task starts. A flush for a compaction writes a segment also when the manager
  // buffers no change. It follows a flush that swapped before the compaction
  private synchronized void flush(boolean force, boolean compaction) {
    State s = state;
    if (closed) {
      return;
    }
    if (force) {
      if (full == Full.NONE) {
        queueFull();
      }
      // A queued full rebuild swaps after the request. A request during a running full rebuild
      // joins it, unless the caller set rebuildPending
      return;
    }
    if (s.flushing != null || full == Full.STARTING) {
      // A flush holds a graph, or a full rebuild waits to swap. The end of that task starts the
      // next flush
      compactionPending |= compaction;
      return;
    }
    if (repair) {
      // The full rebuild of a repair is in the queue, or follows the full rebuild that runs. It
      // swaps after the compaction, and its region scan holds the buffered changes
      return;
    }
    if (!compaction && s.mutable.isEmpty() && !hasMasks(s)) {
      return;
    }
    if (
      full == Full.NONE
        && deltaBase(s.sources, s.mutable.changes(), startKey, endKey, false) == null
    ) {
      queueFull();
    }
    if (
      full == Full.QUEUED
        && (compaction || s.mutable.size() < QUEUED_FLUSH_MULTIPLIER * FLUSH_THRESHOLD)
    ) {
      // The queued full rebuild swaps after the compaction, so a compaction needs no delta. Below
      // the QUEUED_FLUSH_MULTIPLIER limit, the graph waits for the swap of that rebuild
      return;
    }
    // Until a queued full rebuild starts, a region with an exact base continues to write deltas.
    // Its writes then do not wait for the full rebuilds of other regions
    Long base = deltaBase(s.sources, s.mutable.changes(), startKey, endKey, full == Full.QUEUED);
    if (base == null) {
      return;
    }
    deltaBase = base;
    swap(s);
    submitDelta();
  }

  // Queues a full rebuild task. The task runs before the rebuilds of covered regions if this region
  // is not covered. The caller holds the monitor
  private void queueFull() {
    full = Full.QUEUED;
    queued = new Rebuild(() -> rebuildSegment(0), !state.covered);
    REBUILDS.execute(queued);
  }

  // Submits a delta flush task for the graph of the swap. The caller holds the monitor
  private void submitDelta() {
    long task = ++deltaTasks;
    deltaTask = task;
    DELTAS.submit(() -> {
      synchronized (this) {
        if (deltaTask != task) {
          // A full rebuild that started took over the graph of this task
          return;
        }
        deltaRunning = true;
      }
      rebuildSegment(task);
    });
  }

  // Swaps the graphs and takes a new swap time. If no flush holds a graph, the mutable graph
  // becomes the graph of the flush, and a new mutable graph takes the writes. Otherwise both graphs
  // stay, and only the swap time changes. The new swap time gives the segment a new identity, so it
  // shares no cells with a failed segment. The time is also later than all earlier build requests.
  // The caller holds the monitor
  private void swap(State s) {
    stateLock.writeLock().lock();
    try {
      long now = EnvironmentEdgeManager.currentTimeMillis();
      clock = Math.max(now, clock + 1);
      state = s.flushing == null
        ? new State(s.sources, new MutableGraph(vectorIndex, similarity), s.mutable,
          ConcurrentHashMap.newKeySet(), clock, replayFrom(now), s.covered)
        : new State(s.sources, s.mutable, s.flushing, s.changedSinceSwap, clock, replayFrom(now),
          s.covered);
    } finally {
      stateLock.writeLock().unlock();
    }
    if (s.flushing == null) {
      splitArmed.set(true);
    }
    // The swap follows every compaction that requested a flush
    compactionPending = false;
  }

  // Starts a queued full rebuild. It waits for the end of a delta flush task that runs, and then
  // swaps. It takes over the graph of a delta flush task that is in the queue or that failed.
  // Returns false if the manager closed. The caller holds the monitor
  private boolean startFull() throws InterruptedException {
    full = Full.STARTING;
    while (deltaRunning && !closed) {
      wait();
    }
    if (closed) {
      return false;
    }
    deltaTask = 0;
    deltaBase = null;
    // The swap follows the change or the compaction of a repair
    repair = false;
    swap(state);
    fullSwapTime = state.swapTime;
    full = Full.RUNNING;
    return true;
  }

  // Returns the replay start of the segment that a swap at the given time writes. A batch takes its
  // start time before its commit, and changes the graph after the commit. As a result, a batch that
  // started before the swap can change the new mutable graph. A region that opens must then replay
  // the rows of the batch
  private long replayFrom(long swapTime) {
    return Math.min(swapTime, oldestBatch.getAsLong());
  }

  // Tries a failed flush again. A failed delta flush runs again, unless a full rebuild that starts
  // takes over its graph. A failed full rebuild goes into the queue again. If another full rebuild
  // is in the queue or started, that rebuild takes over the graph instead
  private synchronized void retryFlush() {
    if (closed || deltaTask != 0 || state.flushing == null) {
      return;
    }
    if (deltaBase != null && full != Full.STARTING) {
      submitDelta();
    } else if (deltaBase == null && full == Full.NONE) {
      queueFull();
    }
  }

  private static boolean hasMasks(State s) {
    for (Source source : s.sources) {
      if (!source.mask.isEmpty()) {
        return true;
      }
    }
    return false;
  }

  // Runs the delta flush task with the given number. Task number 0 is the full rebuild task, which
  // swaps when it starts
  private void rebuildSegment(long task) {
    boolean fullTask = task == 0;
    Long base = null;
    try {
      State s;
      synchronized (this) {
        if (closed || fullTask && !startFull()) {
          return;
        }
        s = state;
        base = deltaBase;
      }
      Scan scan = base != null ? null : new Scan().setCacheBlocks(false);
      if (!catalog.isCurrent(scan)) {
        // The index was dropped, or created again with another definition. A write or a scan that
        // names the index opens a new manager
        LOG.info("HNSW index {} was dropped or created again; closing its manager for region {}",
          indexName, region.getRegionInfo().getEncodedName());
        close();
        return;
      }
      if (base != null) {
        writeDelta(s, base);
      } else {
        writeFull(s, scan);
      }
    } catch (Throwable t) {
      if (t instanceof InterruptedException) {
        Thread.currentThread().interrupt();
      }
      if (scheduleRetry(t, RETRIES, this::retryFlush)) {
        LOG.error("HNSW index {} {} failed for region {}; will retry", indexName,
          base != null ? "delta flush" : "rebuild", region.getRegionInfo().getEncodedName(), t);
      } else {
        LOG.error(
          "HNSW index {} {} failed for region {}; a retry fails the same way, so it is not"
            + " retried until a rebuild is requested",
          indexName, base != null ? "delta flush" : "rebuild",
          region.getRegionInfo().getEncodedName(), t);
      }
    } finally {
      synchronized (this) {
        if (fullTask) {
          full = Full.NONE;
        } else {
          deltaRunning = false;
          // Clear the task number only if no newer delta flush task is in the queue. Such a task
          // swapped after the cutover of this task
          if (deltaTask == task) {
            deltaTask = 0;
          }
          // Wake a full rebuild that waits for the end of this task
          notifyAll();
        }
        // Queue a full rebuild that a caller requested during this task. Run the flush of a
        // compaction that ended during the task. Start the next flush if the mutable graph is at
        // the flush threshold. No write starts that flush, because an apply starts no flush while
        // a flush holds a graph, and checkResources can refuse all writes
        if (rebuildPending && full == Full.NONE) {
          rebuildPending = false;
          flush(true);
        }
        if (compactionPending) {
          flush(false, true);
        }
        if (state.mutable.size() >= FLUSH_THRESHOLD) {
          flush(false);
        }
      }
    }
  }

  /**
   * Schedules {@code retry} after a failed flush or rebuild, unless a retry fails the same way. A
   * retry fails the same way if the segment is too large to write or to load, or if an argument is
   * not valid. Returns true if the retry is scheduled.
   */
  static boolean scheduleRetry(Throwable t, ScheduledExecutorService retries, Runnable retry) {
    if (
      t instanceof HnswSegment.TooLargeException || t instanceof RequestTooBigException
        || t instanceof IllegalArgumentException
    ) {
      return false;
    }
    retries.schedule(retry, RETRY_DELAY_MS, TimeUnit.MILLISECONDS);
    return true;
  }

  // Scans the region, and writes and installs a new base segment. The scan starts after the swap,
  // and each graph change follows the commit of its row. As a result, the scan holds every change
  // that the graphs held at the swap. The manager does not hold its monitor during the write,
  // because the write waits for region row locks
  private void writeFull(State s, Scan scan) throws IOException {
    Map<ImmutableBytesPtr, float[]> vectors = scanRegion(scan);
    if (vectors == null || closed) {
      return;
    }
    List<VectorFloat<?>> values = new ArrayList<>(vectors.size());
    byte[][] keys = new byte[vectors.size()][];
    int i = 0;
    for (Map.Entry<ImmutableBytesPtr, float[]> e : vectors.entrySet()) {
      keys[i++] = e.getKey().copyBytesIfNecessary();
      values.add(VTS.createFloatVector(e.getValue()));
    }
    byte[] payload = keys.length > 0
      ? HnswSegment.build(vectorIndex,
        new ListRandomAccessVectorValues(values, vectorIndex.getDimension()), keys)
      : null;
    // A closed manager writes no segment, because its region can now be open on a different server
    if (closed) {
      return;
    }
    HnswSegment.Descriptor written = new HnswSegment.Descriptor(indexKey, startKey, s.swapTime,
      endKey, keys.length, null, s.replayFrom, definition);
    // Load the segment into off-heap memory before the write. Thus the region never writes a
    // segment that this server cannot load
    HnswSegment segment = payload != null
      ? HnswSegment.of(store, written, allocator, vectorIndex.getDistanceMetric(), payload,
        new byte[0][])
      : null;
    try {
      store.write(written, payload, new byte[0][]);
    } catch (IOException | RuntimeException e) {
      if (segment != null) {
        segment.close();
      }
      throw e;
    }
    if (!cutover(segment, written)) {
      retireAfterClose(written);
      return;
    }
    retireSupersededSegments();
    LOG.info("Rebuilt HNSW index {} for region {} with {} vectors", indexName,
      region.getRegionInfo().getEncodedName(), keys.length);
  }

  // Writes the changes in the graph of the flush as a delta segment on the given base. Then it
  // installs the delta. The manager does not hold its monitor during the write, because the write
  // waits for region row locks
  private void writeDelta(State s, long baseTime) throws IOException {
    Map<ImmutableBytesPtr, float[]> live = s.flushing.live();
    List<VectorFloat<?>> values = new ArrayList<>(live.size());
    byte[][] keys = new byte[live.size()][];
    int i = 0;
    for (Map.Entry<ImmutableBytesPtr, float[]> e : live.entrySet()) {
      keys[i++] = e.getKey().copyBytesIfNecessary();
      values.add(VTS.createFloatVector(e.getValue()));
    }
    byte[] payload = keys.length > 0
      ? HnswSegment.build(vectorIndex,
        new ListRandomAccessVectorValues(values, vectorIndex.getDimension()), keys)
      : null;

    Set<ImmutableBytesPtr> deleted = s.flushing.deleted();
    byte[][] deletedKeys = new byte[deleted.size()][];
    int d = 0;
    for (ImmutableBytesPtr del : deleted) {
      deletedKeys[d++] = del.copyBytesIfNecessary();
    }
    if (closed) {
      return;
    }
    HnswSegment.Descriptor written = new HnswSegment.Descriptor(indexKey, startKey, s.swapTime,
      endKey, keys.length, baseTime, s.replayFrom, definition);
    // Load the segment before the write, as for a base. Thus the region never writes a delta that
    // this server cannot load
    HnswSegment segment = HnswSegment.of(store, written, allocator, vectorIndex.getDistanceMetric(),
      payload, deletedKeys);
    try {
      store.write(written, payload, deletedKeys);
    } catch (IOException | RuntimeException e) {
      segment.close();
      throw e;
    }
    if (!cutover(segment, written)) {
      retireAfterClose(written);
      return;
    }
    LOG.info(
      "Wrote HNSW delta for index {} region {} stacked on base {} with {} vectors and {} tombstones",
      indexName, region.getRegionInfo().getEncodedName(), baseTime, keys.length,
      deletedKeys.length);
  }

  // Deletes the cells of a segment that the manager wrote while it closed, unless the region
  // closes. A purge can close the manager during the write, and the purge scan can come before the
  // write. The purge then misses these cells, so the manager deletes them. Thus no cells of a
  // dropped index stay
  private void retireAfterClose(HnswSegment.Descriptor written) throws IOException {
    if (!region.isClosing() && !region.isClosed()) {
      store.retire(Collections.singletonList(written));
    }
  }

  // Returns the full-precision vectors of the rows of the region that have not expired, or null if
  // the manager closes during the scan. The region-local scanner applies Phoenix TTL masking, as a
  // client scan does
  private Map<ImmutableBytesPtr, float[]> scanRegion(Scan scan) throws IOException {
    Map<ImmutableBytesPtr, float[]> vectors = new LinkedHashMap<>();
    for (ColumnReference ref : maintainer.getAllColumnsForDataTable()) {
      scan.addColumn(ref.getFamily(), ref.getQualifier());
    }
    try (RegionScanner scanner = ServerScanUtil.openRegionScanner(env, region, scan)) {
      List<Cell> cells = new ArrayList<>();
      boolean more;
      int rows = 0;
      do {
        if (++rows % CLOSE_CHECK_ROWS == 0 && closed) {
          return null;
        }
        more = scanner.next(cells);
        if (!cells.isEmpty()) {
          float[] vector = vectorOf(maintainer, Result.create(cells));
          if (vector != null) {
            vectors.put(new ImmutableBytesPtr(CellUtil.cloneRow(cells.get(0))), vector);
          }
          cells.clear();
        }
      } while (more);
    }
    return vectors;
  }

  // Installs a written segment and releases the graph of the flush. A base replaces all the
  // segments, and a delta goes on top of them. The cutover masks the entries of the new segment for
  // rows that changed after the swap. Returns false if the manager closed
  private synchronized boolean cutover(HnswSegment segment, HnswSegment.Descriptor written) {
    State s = state;
    boolean isDelta = written.isDelta();
    if (closed) {
      if (segment != null) {
        segment.close();
      }
      return false;
    }
    List<Source> sources = new ArrayList<>(isDelta ? s.sources.size() + 1 : 1);
    stateLock.writeLock().lock();
    try {
      if (segment != null) {
        Source source = new Source(segment, written, false);
        for (ImmutableBytesPtr key : s.changedSinceSwap) {
          int ordinal = segment.ordinalOf(key);
          if (ordinal >= 0) {
            source.mask.add(ordinal);
          }
        }
        sources.add(source);
      }
      if (isDelta) {
        sources.addAll(s.sources);
      }
      // A new base covers the range of the region exactly, so the new state is covered. A delta
      // does not change the coverage
      state = new State(sources, s.mutable, null, null, 0, 0, !isDelta || s.covered);
    } finally {
      stateLock.writeLock().unlock();
    }
    if (!isDelta) {
      for (Source old : s.sources) {
        old.segment.close();
      }
    }
    return true;
  }

  /**
   * Returns the base time on which a flush can stack a delta, or null if a full rebuild is
   * necessary. A full rebuild is necessary when the deltas are over their limits. It is also
   * necessary when the stack does not cover exactly the key range of the region, as after a split
   * or a merge. While a full rebuild is in the queue, the limits do not apply, because the rebuild
   * replaces the stack.
   * <p>
   * A stack holds at most {@link #MAX_DELTAS} deltas when no rebuild is in the queue. Thus the
   * manager requests a full rebuild at least once every MAX_DELTAS + 1 flushes, about every 50,000
   * graph entries. For a base of up to (MAX_DELTAS + 1) * FLUSH_THRESHOLD / REBUILD_RATIO = 200,000
   * rows, REBUILD_RATIO decides. The amortized rebuild cost per change is then O(log N). For a
   * larger base, MAX_DELTAS decides. The amortized rebuild cost per change is then O(N log N /
   * 50,000), which grows with N.
   */
  static Long deltaBase(List<Source> sources, int flushChanges, byte[] startKey, byte[] endKey,
    boolean rebuildQueued) {
    List<HnswSegment.Descriptor> stack = new ArrayList<>(sources.size());
    int changes = flushChanges;
    for (Source source : sources) {
      stack.add(source.descriptor);
      if (source.descriptor.isDelta()) {
        changes += source.changes;
      }
    }
    HnswSegment.Descriptor base = exactBase(stack, startKey, endKey);
    return base != null
      && (rebuildQueued || stack.size() <= MAX_DELTAS && changes < REBUILD_RATIO * base.count)
        ? base.time
        : null;
  }

  /**
   * Returns the base if the given segments are one stack that covers exactly the given key range.
   * The base and each delta must cover exactly that range. Otherwise returns null.
   */
  static HnswSegment.Descriptor exactBase(List<HnswSegment.Descriptor> segments, byte[] startKey,
    byte[] endKey) {
    HnswSegment.Descriptor base = null;
    for (HnswSegment.Descriptor d : segments) {
      if (!d.covers(startKey, endKey) || (!d.isDelta() && base != null)) {
        return null;
      }
      if (!d.isDelta()) {
        base = d;
      }
    }
    for (HnswSegment.Descriptor d : segments) {
      if (base == null || (d != base && d.baseTime != base.time)) {
        return null;
      }
    }
    return base;
  }

  /**
   * Retires the superseded segments of the index in the family of the region. Newer bases of the
   * index supersede a base when they cover its part of the range of the region. The region retires
   * a delta with its base, or when the listing has no base for it. Each region retires only its own
   * cells. Thus the lower daughter of a split retires the parent segments that it holds. A merged
   * region retires the segments of its predecessors.
   */
  private void retireSupersededSegments() throws IOException {
    List<HnswSegment.Descriptor> toRetire =
      segmentsToRetire(store.list(indexKey), startKey, endKey, definition);
    if (!toRetire.isEmpty()) {
      store.retire(toRetire);
    }
  }

  /**
   * Returns the segments that a region of the range {@code [startKey, endKey)} retires. These are
   * the bases of its family that newer bases of the given definition cover within the range. They
   * also include the deltas of these bases, and the deltas whose base is not in the listing. A
   * segment of another definition never covers a segment. An example is a segment of an index that
   * a user dropped and created again. A newer base of the definition supersedes it.
   * @param all        the segments of the index in the family of the region
   * @param startKey   the start key of the region
   * @param endKey     the end key of the region
   * @param definition the definition digest of the index
   */
  public static List<HnswSegment.Descriptor> segmentsToRetire(List<HnswSegment.Descriptor> all,
    byte[] startKey, byte[] endKey, byte[] definition) {
    List<HnswSegment.Descriptor> bases = new ArrayList<>();
    List<HnswSegment.Descriptor> deltas = new ArrayList<>();
    Set<StackId> listedBases = new HashSet<>();

    for (HnswSegment.Descriptor d : all) {
      if (d.isDelta()) {
        deltas.add(d);
      } else {
        bases.add(d);
        listedBases.add(new StackId(d.startKey, d.time));
      }
    }

    Set<StackId> retiredBases = new HashSet<>();
    List<HnswSegment.Descriptor> toRetire = new ArrayList<>();

    for (HnswSegment.Descriptor b : bases) {
      if (b.overlaps(startKey, endKey)) {
        List<HnswSegment.Descriptor> newerBases = new ArrayList<>();
        for (HnswSegment.Descriptor n : bases) {
          if (n.time > b.time && Bytes.equals(n.definition, definition)) {
            newerBases.add(n);
          }
        }
        byte[] from = Bytes.compareTo(b.startKey, startKey) > 0 ? b.startKey : startKey;
        byte[] to =
          endKey.length == 0 || (b.endKey.length > 0 && Bytes.compareTo(b.endKey, endKey) < 0)
            ? b.endKey
            : endKey;
        if (HnswSegment.covered(from, to, newerBases)) {
          toRetire.add(b);
          retiredBases.add(new StackId(b.startKey, b.time));
        }
      }
    }

    for (HnswSegment.Descriptor d : deltas) {
      if (d.overlaps(startKey, endKey)) {
        StackId baseId = new StackId(d.startKey, d.baseTime);
        if (retiredBases.contains(baseId) || !listedBases.contains(baseId)) {
          toRetire.add(d);
        }
      }
    }

    return toRetire;
  }

  /**
   * Finds the approximate nearest neighbors of the query vector in this region.
   * @param query the query vector
   * @param topK  the maximum number of row keys to return. This is also the size of the candidate
   *              list of the graph search.
   * @return the data table row keys of the nearest neighbors, nearest first
   * @throws IOException if the search fails
   */
  public List<byte[]> search(float[] query, int topK) throws IOException {
    return search(query, topK, false);
  }

  /**
   * Finds the candidate rows of an HNSW scan, as {@link #search(float[], int)} does. Returns null
   * if the state that the search reads does not cover the region, also after the search runs again
   * on a new state. The scan then reads the range of the region without candidate restriction. The
   * coverage and the candidates thus come from the same state.
   */
  List<byte[]> candidates(float[] query, int topK) throws IOException {
    return search(query, topK, true);
  }

  // Searches the current state. If covering is true, returns null for a state without coverage
  private List<byte[]> search(float[] query, int topK, boolean covering) throws IOException {
    State s = state;
    if (covering && !s.covered) {
      return null;
    }
    VectorFloat<?> q = VTS.createFloatVector(query);
    // Search the sources oldest first: the segments, the graph of the flush and the mutable graph.
    // A segment search skips the masked ordinals, and the keys outside the region if the segment is
    // wider. The graph of the flush skips the keys that changed after the swap. A newer source
    // replaces the score of a key. An apply adds a new version before it masks the older ones, so
    // the search finds a row that changes during the search. A swap is the exception. The new state
    // shares the segment masks, so an apply to it can mask a row that this search finds in none of
    // its sources. The search then runs again on the new state
    Map<ImmutableBytesPtr, Float> scores = new HashMap<>();
    for (int i = s.sources.size() - 1; i >= 0; i--) {
      Source source = s.sources.get(i);
      HnswSegment segment = source.segment;
      SearchResult result;
      try {
        result = segment.search(q, topK, topK, ordinal -> !source.mask.contains(ordinal)
          && (!source.wider || region.getRegionInfo().containsRow(segment.getKey(ordinal))));
      } catch (HnswSegment.NotFoundException e) {
        // If a cutover closed the segment, search its replacements. Otherwise the manager closed,
        // or the region left the segment out. The scan then fails with a plain IOException, because
        // a client retries only such a failure. The retry reads without candidate restriction
        if (!replaceRetired(s.sources, source)) {
          throw new IOException("HNSW segment " + source.descriptor + " cannot be read", e);
        }
        return search(query, topK, covering);
      } catch (HnswSegment.TooLargeException e) {
        throw e;
      } catch (DoNotRetryIOException e) {
        // The reload of an evicted segment found its payload lost or corrupt, for example after the
        // MOB cleaner archived a payload that the region inherited. The region leaves the segment
        // out and rebuilds, and the retry of the scan reads without candidate restriction
        leaveOut(s.sources, source);
        throw new IOException("HNSW segment " + source.descriptor + " cannot be read", e);
      }
      for (SearchResult.NodeScore ns : result.getNodes()) {
        scores.put(new ImmutableBytesPtr(segment.getKey(ns.node)), ns.score);
      }
    }
    if (s.flushing != null) {
      s.flushing.search(q, topK, scores, s.changedSinceSwap);
    }
    s.mutable.search(q, topK, scores, null);
    if (state.mutable != s.mutable) {
      return search(query, topK, covering);
    }
    List<Map.Entry<ImmutableBytesPtr, Float>> ranked = new ArrayList<>(scores.entrySet());
    ranked.sort((a, b) -> Float.compare(b.getValue(), a.getValue()));
    List<byte[]> keys = new ArrayList<>(Math.min(topK, ranked.size()));
    for (int i = 0; i < ranked.size() && i < topK; i++) {
      keys.add(ranked.get(i).getKey().copyBytesIfNecessary());
    }
    return keys;
  }

  /**
   * Returns an upper bound of the number of row keys that the index holds for this region. The
   * count includes the masked segment ordinals, because a count without them costs more. Thus the
   * count can be larger than the number of live keys.
   */
  public int count() {
    State s = state;
    long count = s.mutable.live().size();
    if (s.flushing != null) {
      count += s.flushing.live().size();
    }
    for (Source source : s.sources) {
      if (source.wider) {
        for (int ordinal = 0; ordinal < source.segment.size(); ordinal++) {
          if (region.getRegionInfo().containsRow(source.segment.getKey(ordinal))) {
            count++;
          }
        }
      } else {
        count += source.segment.size();
      }
    }
    return (int) Math.min(Integer.MAX_VALUE, count);
  }

  /**
   * Returns true if the bases that the region searches cover all of its range, possibly with wider
   * segments that the region clips. Otherwise an HNSW scan of the region must read its range
   * without candidate restriction, because the graph does not hold every row of the range.
   */
  public boolean covered() {
    return state.covered;
  }

  @Override
  public boolean isClosed() {
    return closed;
  }

  @Override
  public void close() {
    List<Source> sources;
    synchronized (this) {
      closed = true;
      sources = state.sources;
      // Wake a full rebuild that waits in startFull, so that it ends
      notifyAll();
    }
    for (Source source : sources) {
      source.segment.close();
    }
  }

  /**
   * An in-memory HNSW graph that holds the buffered changes of the region until a flush writes
   * them. Changes to different rows run in parallel. Row locks order the changes to one row.
   * <p>
   * The nodes of replaced and deleted versions stay in the JVector graph as routing nodes. A search
   * accepts only the current node of each key. The manager does not mark old nodes deleted in
   * JVector. JVector links a new node only to unmarked nodes, and keeps a marked node as entry
   * point. If the manager marked old nodes, a node that is added while no unmarked node is
   * reachable would get no links. Every node counts toward the flush threshold, so a flush bounds
   * the number of old nodes.
   */
  static final class MutableGraph {
    private final PTable.VectorIndex vectorIndex;
    private final VectorSimilarityFunction similarity;
    // The buffered changes: the newest vector of each changed row, and the deleted rows
    private final Map<ImmutableBytesPtr, float[]> live = new ConcurrentHashMap<>();
    private final Set<ImmutableBytesPtr> deleted = ConcurrentHashMap.newKeySet();
    private final Map<Integer, VectorFloat<?>> vectors = new ConcurrentHashMap<>();
    // The key of each node, including the nodes of replaced and deleted versions
    private final Map<Integer, ImmutableBytesPtr> keys = new ConcurrentHashMap<>();
    // The node of the current version of each key
    private final Map<ImmutableBytesPtr, Integer> ordinals = new ConcurrentHashMap<>();
    private final AtomicInteger next = new AtomicInteger();
    private final RandomAccessVectorValues ravv = new RandomAccessVectorValues() {
      @Override
      public int size() {
        return next.get();
      }

      @Override
      public int dimension() {
        return vectorIndex.getDimension();
      }

      @Override
      public VectorFloat<?> getVector(int ordinal) {
        return vectors.get(ordinal);
      }

      @Override
      public boolean isValueShared() {
        return false;
      }

      @Override
      public RandomAccessVectorValues copy() {
        return this;
      }
    };
    private final GraphIndexBuilder builder;

    MutableGraph(PTable.VectorIndex vectorIndex, VectorSimilarityFunction similarity) {
      this.vectorIndex = vectorIndex;
      this.similarity = similarity;
      this.builder = HnswSegment.graphBuilder(vectorIndex, ravv, similarity, false);
    }

    Map<ImmutableBytesPtr, float[]> live() {
      return live;
    }

    Set<ImmutableBytesPtr> deleted() {
      return deleted;
    }

    // The number of changed rows: the rows with a new vector and the deleted rows
    int changes() {
      return live.size() + deleted.size();
    }

    // The number of entries: the graph nodes, one for each vector change, and the deleted keys. The
    // flush threshold, the split trigger and the write limit use this count
    int size() {
      return next.get() + deleted.size();
    }

    boolean isEmpty() {
      return live.isEmpty() && deleted.isEmpty();
    }

    // Adds the node of the new version before the node becomes current for the key. As a result, a
    // search finds one of the two versions
    void upsert(ImmutableBytesPtr key, float[] vector) {
      VectorFloat<?> v = VTS.createFloatVector(vector);
      int ordinal = next.getAndIncrement();
      vectors.put(ordinal, v);
      keys.put(ordinal, key);
      builder.addGraphNode(ordinal, v);
      live.put(key, vector);
      deleted.remove(key);
      ordinals.put(key, ordinal);
    }

    void delete(ImmutableBytesPtr key) {
      live.remove(key);
      deleted.add(key);
      ordinals.remove(key);
    }

    // Adds the scores of the nearest current nodes to scores. The search skips the keys in excluded
    void search(VectorFloat<?> query, int topK, Map<ImmutableBytesPtr, Float> scores,
      Set<ImmutableBytesPtr> excluded) {
      if (ordinals.isEmpty()) {
        return;
      }
      // The keys of reached nodes that are not current. The search sees the graph as it was at the
      // start. It can then miss the new node of a key that changes during the search
      Set<ImmutableBytesPtr> replaced = new HashSet<>();
      Bits bits = ordinal -> {
        ImmutableBytesPtr key = keys.get(ordinal);
        if (key == null || (excluded != null && excluded.contains(key))) {
          return false;
        }
        Integer current = ordinals.get(key);
        if (current != null && current == ordinal) {
          return true;
        }
        replaced.add(key);
        return false;
      };
      SearchResult result =
        GraphSearcher.search(query, topK, ravv, similarity, builder.getGraph(), bits);
      Map<ImmutableBytesPtr, Float> found = new HashMap<>();
      for (SearchResult.NodeScore ns : result.getNodes()) {
        found.putIfAbsent(keys.get(ns.node), ns.score);
      }
      // Score the current node of a key that the search reached only through an older node
      for (ImmutableBytesPtr key : replaced) {
        Integer current = ordinals.get(key);
        if (current != null && !found.containsKey(key)) {
          found.put(key, similarity.compare(query, vectors.get(current)));
        }
      }
      scores.putAll(found);
    }
  }
}
