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
package org.apache.phoenix.mapreduce.index.fsck.hnsw;

import java.io.IOException;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.BitSet;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapper;
import org.apache.hadoop.hbase.mapreduce.TableSplit;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.NullWritable;
import org.apache.phoenix.coprocessor.IndexToolVerificationResult;
import org.apache.phoenix.hbase.index.hnsw.HnswSegment;
import org.apache.phoenix.hbase.index.table.HTableFactory;
import org.apache.phoenix.hbase.index.util.ImmutableBytesPtr;
import org.apache.phoenix.jdbc.PhoenixConnection;
import org.apache.phoenix.mapreduce.index.IndexTool;
import org.apache.phoenix.mapreduce.index.IndexVerificationOutputRepository;
import org.apache.phoenix.mapreduce.index.IndexVerificationOutputRepository.IndexVerificationErrorType;
import org.apache.phoenix.mapreduce.index.IndexVerificationResultRepository;
import org.apache.phoenix.mapreduce.index.fsck.Retry;
import org.apache.phoenix.mapreduce.util.ConnectionUtil;
import org.apache.phoenix.schema.PTable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A map task that verifies the current HNSW segments of one data region against the data rows of
 * the region. As the region does, the task uses the segments of the index definition that the
 * region holds in its {@code V#} family.
 * <p>
 * The verification has two phases:
 * <ul>
 * <li><b>Data phase ({@code map}):</b> Each data row with a vector must have a live entry in the
 * current segments. The vector of the entry must match the row within the tolerance.</li>
 * <li><b>Segment phase ({@code cleanup}):</b> Each live entry in the key range of the region must
 * match a data row with a vector. An entry that no data row matched is an orphan.</li>
 * </ul>
 * A row that changed at or after the replay start of the region is backlog, not an error. Such a
 * row can be only in the in-memory graph of the region until the next flush. A row that changes
 * while the task scans the region is also backlog. The data phase checks the cell timestamps of the
 * row. The segment phase lists the rows changed since the replay start again after the scan. A
 * region in overflow drops the changes of its rows until the swap of its repair rebuild. No segment
 * moves the replay start past these changes, so these rows are backlog too.
 * <p>
 * A region that its segments do not cover reads its rows without the index, until a rebuild writes
 * a base that covers it. An example is the upper daughter of a split before its first build. A row
 * of such a region that no segment holds is thus not missing, and counts as uncovered. The scans of
 * the data rows read only the data columns of the vector, so they never read the family {@code V#}.
 * <p>
 * The task writes each error to {@code PHOENIX_INDEX_TOOL}, and the counts of the region to
 * {@code PHOENIX_INDEX_TOOL_RESULT}. The results use the physical name of the index, which is the
 * data table. A region whose segments cannot be read gets neither.
 */
public class HnswVerifyMapper extends TableMapper<NullWritable, NullWritable> {
  private static final Logger LOGGER = LoggerFactory.getLogger(HnswVerifyMapper.class);
  /** The configuration key of the name of the index to verify. */
  public static final String INDEX_NAME = "phoenix.hnsw.verify.index";
  /** The configuration key of the name of the data table. */
  public static final String DATA_TABLE_NAME = "phoenix.hnsw.verify.data.table";
  /**
   * The configuration key of the time of the run. The output rows and the result rows of the run
   * have this time in their keys.
   */
  public static final String SCAN_MAX_TS = "phoenix.hnsw.verify.scan.max.ts";

  /** The counters of the verification job. */
  public enum Counters {
    /** Data rows with a vector in regions whose segments the task can read. */
    ROWS,
    /** Data rows with live entries whose vectors match the row. */
    VALID,
    /** Data rows with a vector and no live entry. The count does not include backlog rows. */
    MISSING,
    /**
     * Data rows with a live entry whose vector differs from the row by more than the tolerance. The
     * count does not include backlog rows.
     */
    INVALID,
    /**
     * Live entries in the key range of the region that no data row with a vector matched. The count
     * does not include backlog rows.
     */
    ORPHAN,
    /**
     * Data rows and segment entries that fail a check, of rows that changed at or after the replay
     * start. The region can hold the current versions of these rows only in memory.
     */
    BACKLOG,
    /** Regions whose segments cannot be read or decoded. The task does not verify their rows. */
    UNREADABLE,
    /** Regions not verified because a segment payload is too large for the heap of the task. */
    UNCHECKED,
    /**
     * Data rows with a vector and no live entry, in a region that its segments do not cover. The
     * region reads these rows without the index. The count does not include backlog rows.
     */
    UNCOVERED
  }

  private Connection connection;
  private HnswIndexReader reader;
  private HnswRegionSegments region;
  // True if the segments cover the region. A region that is not covered reads without the index
  private boolean covered;
  // Rows changed at or after the replay start, which the region can hold only in memory
  private Set<ImmutableBytesPtr> changed = Collections.emptySet();
  private long replayStart = Long.MAX_VALUE;
  private final Map<HnswRegionSegments.Source, BitSet> seen = new HashMap<>();
  private long scanMaxTs;
  private byte[] dataTableName;
  private byte[] indexTableName;
  private TableSplit split;
  private IndexVerificationOutputRepository output;
  private final IndexToolVerificationResult.PhaseResult phase =
    new IndexToolVerificationResult.PhaseResult();
  private long rows;

  @Override
  protected void setup(Context context) throws IOException {
    Configuration conf = context.getConfiguration();
    split = (TableSplit) context.getInputSplit();
    scanMaxTs = conf.getLong(SCAN_MAX_TS, 0);
    try {
      connection = ConnectionUtil.getInputConnection(conf);
      PhoenixConnection pconn = connection.unwrap(PhoenixConnection.class);
      PTable data = pconn.getTableNoCache(conf.get(DATA_TABLE_NAME));
      PTable index = pconn.getTableNoCache(conf.get(INDEX_NAME));
      dataTableName = data.getPhysicalName().getBytes();
      indexTableName = index.getPhysicalName().getBytes();
      reader = new HnswIndexReader(new HnswIndexContext(pconn, data, index));
      try {
        region = HnswRegionSegments.open(reader, split.getStartRow(), split.getEndRow());
      } catch (HnswIndexReader.PayloadTooLargeException e) {
        // The size guard of this JVM does not show that the segments are bad
        LOGGER.warn("Cannot check the HNSW segments of region {}", split.getEncodedRegionName(), e);
        context.getCounter(Counters.UNCHECKED).increment(1);
      } catch (IOException | RuntimeException e) {
        if (Retry.isInterrupt(e)) {
          throw e;
        }
        // JVector reports an undecodable graph as a RuntimeException
        LOGGER.warn("Cannot read the HNSW segments of region {}", split.getEncodedRegionName(), e);
        context.getCounter(Counters.UNREADABLE).increment(1);
      }
      covered = region != null && region.covered();
      if (region != null && !region.getCurrent().isEmpty()) {
        replayStart = region.replayStart();
        changed = reader.changedSince(split.getStartRow(), split.getEndRow(), replayStart);
      }
      output = new IndexVerificationOutputRepository(indexTableName, tables(pconn),
        IndexTool.IndexDisableLoggingType.NONE);
    } catch (SQLException e) {
      throw new IOException(e);
    }
  }

  // The repository records the index name in the ITName column only when a factory gives it the
  // tables
  private static HTableFactory tables(PhoenixConnection pconn) {
    return new HTableFactory() {
      @Override
      public Table getTable(ImmutableBytesPtr name) throws IOException {
        try {
          return pconn.getQueryServices().getTable(name.copyBytesIfNecessary());
        } catch (SQLException e) {
          throw new IOException(e);
        }
      }

      @Override
      public Table getTable(ImmutableBytesPtr name, ExecutorService pool) throws IOException {
        return getTable(name);
      }

      @Override
      public org.apache.hadoop.hbase.client.Connection getConnection() {
        // The repository only gets tables
        throw new UnsupportedOperationException();
      }

      @Override
      public void shutdown() {
      }
    };
  }

  /** Returns true if a cell of the row was written at or after {@code since}. */
  static boolean changedSince(Result row, long since) {
    for (Cell cell : row.rawCells()) {
      if (cell.getTimestamp() >= since) {
        return true;
      }
    }
    return false;
  }

  @Override
  protected void map(ImmutableBytesWritable key, Result row, Context context) throws IOException {
    float[] vector = reader.vectorOf(row);
    if (vector == null || region == null) {
      return;
    }
    rows++;
    context.getCounter(Counters.ROWS).increment(1);
    byte[] rowKey = row.getRow();
    List<HnswRegionSegments.Entry> entries = region.lookup(rowKey);
    byte[] divergent = null;
    for (HnswRegionSegments.Entry entry : entries) {
      seen.computeIfAbsent(entry.getSource(), s -> new BitSet()).set(entry.getOrdinal());
      if (entry.getSource().diverges(entry.getOrdinal(), vector)) {
        divergent = entry.getSource().getDescriptor().identity;
      }
    }
    if (entries.isEmpty() || divergent != null) {
      // A write after setup listed the changed rows gives the row a cell with the time of the write
      if (changed.contains(new ImmutableBytesPtr(rowKey)) || changedSince(row, replayStart)) {
        context.getCounter(Counters.BACKLOG).increment(1);
      } else if (entries.isEmpty() && !covered) {
        // The region reads the row without the index until its build, so queries find the row
        context.getCounter(Counters.UNCOVERED).increment(1);
      } else if (entries.isEmpty()) {
        context.getCounter(Counters.MISSING).increment(1);
        phase.setMissingIndexRowCount(phase.getMissingIndexRowCount() + 1);
        log(rowKey, baseHolding(rowKey), "Missing from the HNSW segments",
          IndexVerificationErrorType.MISSING_ROW);
      } else {
        context.getCounter(Counters.INVALID).increment(1);
        phase.setInvalidIndexRowCount(phase.getInvalidIndexRowCount() + 1);
        log(rowKey, divergent, "Vector differs from the HNSW segment's",
          IndexVerificationErrorType.INVALID_ROW);
      }
    } else {
      context.getCounter(Counters.VALID).increment(1);
      phase.setValidIndexRowCount(phase.getValidIndexRowCount() + 1);
    }
  }

  // Returns the identity of the newest current base segment whose range holds the row, or an empty
  // array. The output row of a missing row names this segment
  private byte[] baseHolding(byte[] rowKey) {
    for (HnswSegment.Descriptor d : region.getCurrent()) {
      if (
        !d.isDelta() && Bytes.compareTo(rowKey, d.startKey) >= 0
          && (d.endKey.length == 0 || Bytes.compareTo(rowKey, d.endKey) < 0)
      ) {
        return d.identity;
      }
    }
    return new byte[0];
  }

  // Writes a row failure to the output table. The identity of a segment ends with the segment time
  private void log(byte[] dataRowKey, byte[] segment, String message,
    IndexVerificationErrorType type) throws IOException {
    long segmentTime = segment.length >= Bytes.SIZEOF_LONG
      ? Bytes.toLong(segment, segment.length - Bytes.SIZEOF_LONG)
      : 0;
    output.logToIndexToolOutputTable(dataRowKey, segment, scanMaxTs, segmentTime, message, null,
      null, scanMaxTs, dataTableName, true, type);
  }

  @Override
  protected void cleanup(Context context) throws IOException {
    if (region == null) {
      close();
      return;
    }
    try {
      if (!region.getCurrent().isEmpty()) {
        // List the changed rows again, so that a row deleted during the data scan is backlog
        changed = reader.changedSince(split.getStartRow(), split.getEndRow(), replayStart);
      }
      for (HnswRegionSegments.Source source : region.getSources()) {
        BitSet held = seen.getOrDefault(source, new BitSet());
        int[] range = region.ordinalRange(source);
        for (int ordinal = range[0]; ordinal < range[1]; ordinal++) {
          if (held.get(ordinal) || !region.isLive(source, ordinal)) {
            continue;
          }
          byte[] rowKey = source.getSegment().getKey(ordinal);
          // An entry of a row that TTL purged is an orphan too: only a rebuild removes it
          if (changed.contains(new ImmutableBytesPtr(rowKey))) {
            context.getCounter(Counters.BACKLOG).increment(1);
          } else {
            context.getCounter(Counters.ORPHAN).increment(1);
            phase.setExtraVerifiedIndexRowCount(phase.getExtraVerifiedIndexRowCount() + 1);
            log(rowKey, source.getDescriptor().identity, "Not a data row with a vector",
              IndexVerificationErrorType.EXTRA_ROW);
          }
        }
      }
      IndexToolVerificationResult result =
        new IndexToolVerificationResult(split.getStartRow(), split.getEndRow(), scanMaxTs);
      result.setScannedDataRowCount(rows);
      result.setBefore(phase);
      try (IndexVerificationResultRepository results =
        new IndexVerificationResultRepository(connection, indexTableName)) {
        results.logToIndexToolResultTable(result, IndexTool.IndexVerifyType.ONLY,
          Bytes.toBytes(split.getEncodedRegionName()));
      }
    } catch (SQLException e) {
      throw new IOException(e);
    } finally {
      close();
    }
  }

  private void close() throws IOException {
    try {
      if (region != null) {
        region.close();
      }
      reader.close();
      output.close();
      connection.close();
    } catch (SQLException e) {
      throw new IOException(e);
    }
  }
}
