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
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapper;
import org.apache.hadoop.hbase.mapreduce.TableSplit;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.NullWritable;
import org.apache.phoenix.coprocessor.IndexToolVerificationResult;
import org.apache.phoenix.hbase.index.hnsw.HnswSegment;
import org.apache.phoenix.hbase.index.util.ImmutableBytesPtr;
import org.apache.phoenix.jdbc.PhoenixConnection;
import org.apache.phoenix.mapreduce.index.IndexTool;
import org.apache.phoenix.mapreduce.index.IndexVerificationOutputRepository;
import org.apache.phoenix.mapreduce.index.IndexVerificationOutputRepository.IndexVerificationErrorType;
import org.apache.phoenix.mapreduce.index.IndexVerificationResultRepository;
import org.apache.phoenix.mapreduce.util.ConnectionUtil;
import org.apache.phoenix.schema.LiteralTTLExpression;
import org.apache.phoenix.schema.PTable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * MapReduce mapper that validates consistency between an HBase data table region and its associated
 * HNSW index segments.
 * <p>
 * Verification proceeds in two phases:
 * <ul>
 * <li><b>Data phase ({@code map}):</b> Evaluates base table rows containing vector data against
 * active segment indexes to ensure vector presence and parity within allowable divergence
 * tolerance.</li>
 * <li><b>Segment phase ({@code cleanup}):</b> Scans segment ordinals within the region boundary to
 * identify orphaned or unreferenced index entries not shadowed by newer segments in the stack.</li>
 * </ul>
 * Mutations replayed during region startup are treated as in-flight memstore backlog rather than
 * index corruption. Verification results and discrepancy details are logged to
 * {@code PHOENIX_INDEX_TOOL} and {@code PHOENIX_INDEX_TOOL_RESULT}.
 */
public class HnswVerifyMapper extends TableMapper<NullWritable, NullWritable> {
  private static final Logger LOGGER = LoggerFactory.getLogger(HnswVerifyMapper.class);
  /** Target index table name for verification. */
  public static final String INDEX_NAME = "phoenix.hnsw.verify.index";
  /** Source data table name for verification. */
  public static final String DATA_TABLE_NAME = "phoenix.hnsw.verify.data.table";
  /** Verification run timestamp identifying output result rows. */
  public static final String SCAN_MAX_TS = "phoenix.hnsw.verify.scan.max.ts";

  /** Verification metric counters. */
  public enum Counters {
    /** Total data table rows scanned containing vector data. */
    ROWS,
    /** Rows verified with matching segment vectors. */
    VALID,
    /** Data table rows missing from segment indexes. */
    MISSING,
    /** Data table rows whose vector values diverge beyond tolerance. */
    INVALID,
    /** Segment ordinals referencing non-existent data rows. */
    ORPHAN,
    /** Segment ordinals referencing data rows purged via TTL expiration. */
    EXPIRED,
    /** Rows modified after segment creation serving from memstore replay. */
    BACKLOG,
    /** Regions skipped due to unreadable or corrupt segments. */
    UNREADABLE
  }

  private Connection connection;
  private HnswIndexReader reader;
  private HnswRegionSegments region;
  private Set<ImmutableBytesPtr> changed;
  private final Map<HnswRegionSegments.Source, BitSet> seen = new HashMap<>();
  private boolean ttl;
  private long scanMaxTs;
  private byte[] dataTableName;
  private TableSplit split;
  private Table outputTable;
  private Table indexTable;
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
      ttl = !LiteralTTLExpression.TTL_EXPRESSION_NOT_DEFINED.equals(data.getTTLExpression());
      reader = new HnswIndexReader(new HnswIndexContext(pconn, data, index));
      try {
        region = HnswRegionSegments.open(reader, reader.listSegments(), split.getStartRow(),
          split.getEndRow());
      } catch (IOException e) {
        LOGGER.warn("Cannot read the HNSW segments of region {}", split.getEncodedRegionName(), e);
        context.getCounter(Counters.UNREADABLE).increment(1);
      }
      changed = region == null || region.getCurrent().isEmpty()
        ? Collections.emptySet()
        : reader.changedSince(split.getStartRow(), split.getEndRow(), region.replayStart());
      outputTable = pconn.getQueryServices()
        .getTable(IndexVerificationOutputRepository.getOutputTableNameBytes());
      indexTable = pconn.getQueryServices().getTable(index.getPhysicalName().getBytes());
      output = new IndexVerificationOutputRepository(outputTable, indexTable,
        IndexTool.IndexDisableLoggingType.NONE);
    } catch (SQLException e) {
      throw new IOException(e);
    }
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
        divergent = entry.getSource().getDescriptor().rowKey;
      }
    }
    if (entries.isEmpty() || divergent != null) {
      if (changed.contains(new ImmutableBytesPtr(rowKey))) {
        context.getCounter(Counters.BACKLOG).increment(1);
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

  // Locates the newest base segment containing the specified row key
  private byte[] baseHolding(byte[] rowKey) {
    for (HnswSegment.Descriptor d : region.getCurrent()) {
      if (
        !d.isDelta() && Bytes.compareTo(rowKey, d.startKey) >= 0
          && (d.endKey.length == 0 || Bytes.compareTo(rowKey, d.endKey) < 0)
      ) {
        return d.rowKey;
      }
    }
    return new byte[0];
  }

  private void log(byte[] dataRowKey, byte[] segmentRowKey, String message,
    IndexVerificationErrorType type) throws IOException {
    long segmentTime = segmentRowKey.length >= Bytes.SIZEOF_LONG
      ? Bytes.toLong(segmentRowKey, segmentRowKey.length - Bytes.SIZEOF_LONG)
      : 0;
    output.logToIndexToolOutputTable(dataRowKey, segmentRowKey, scanMaxTs, segmentTime, message,
      null, null, scanMaxTs, dataTableName, true, type);
  }

  @Override
  protected void cleanup(Context context) throws IOException {
    if (region == null) {
      close();
      return;
    }
    try {
      for (HnswRegionSegments.Source source : region.getSources()) {
        BitSet held = seen.getOrDefault(source, new BitSet());
        int[] range = region.ordinalRange(source);
        for (int ordinal = range[0]; ordinal < range[1]; ordinal++) {
          if (held.get(ordinal) || !region.isLive(source, ordinal)) {
            continue;
          }
          byte[] rowKey = source.getSegment().getKey(ordinal);
          if (changed.contains(new ImmutableBytesPtr(rowKey))) {
            context.getCounter(Counters.BACKLOG).increment(1);
          } else if (ttl) {
            context.getCounter(Counters.EXPIRED).increment(1);
            phase.setExpiredIndexRowCount(phase.getExpiredIndexRowCount() + 1);
          } else {
            context.getCounter(Counters.ORPHAN).increment(1);
            phase.setExtraVerifiedIndexRowCount(phase.getExtraVerifiedIndexRowCount() + 1);
            log(rowKey, source.getDescriptor().rowKey, "Not a data row with a vector",
              IndexVerificationErrorType.EXTRA_ROW);
          }
        }
      }
      IndexToolVerificationResult result =
        new IndexToolVerificationResult(split.getStartRow(), split.getEndRow(), scanMaxTs);
      result.setScannedDataRowCount(rows);
      result.setBefore(phase);
      try (IndexVerificationResultRepository results =
        new IndexVerificationResultRepository(connection, indexTable.getName().getName())) {
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
      outputTable.close();
      indexTable.close();
      connection.close();
    } catch (SQLException e) {
      throw new IOException(e);
    }
  }
}
