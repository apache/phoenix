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
package org.apache.phoenix.replication;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Mutation;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.wal.WALEdit;
import org.apache.phoenix.hbase.index.IndexRegionObserver;
import org.apache.phoenix.hbase.index.util.ImmutableBytesPtr;
import org.apache.phoenix.index.PhoenixIndexCodec;

/**
 * Groups a flat cell stream into Put/Delete mutations, mirroring the algorithm HBase's
 * ReplicationSink uses to reconstruct mutations from a WALEdit. A new mutation is started whenever
 * the row key or the cell type differs from the immediately preceding cell; consecutive cells
 * sharing both are collected into one mutation. Because the full cell type participates in the
 * boundary, distinct delete subtypes (e.g. DeleteColumn vs. DeleteFamily) also split into separate
 * mutations. There is no precondition on the input ordering: any cell stream produces valid
 * mutations. Ordering only affects how the cells are partitioned into Mutation objects (a row that
 * recurs non-consecutively yields a separate mutation per run), not correctness -- cell order is
 * preserved, so replaying the resulting mutations in order reproduces the effect of applying the
 * input cells in order.
 * <p>
 * Stateless and thread-safe: a non-instantiable holder of static methods that operate solely on
 * their arguments.
 */
public final class MutationCellGrouper {

  private MutationCellGrouper() {
  }

  private static boolean isNewRowOrType(Cell previousCell, Cell cell) {
    return previousCell == null || previousCell.getType() != cell.getType()
      || !CellUtil.matchingRows(previousCell, cell);
  }

  /**
   * Flatten a mutation's family cell map into a single ordered cell list, preserving family
   * iteration order (typically TreeMap-ordered).
   */
  public static List<Cell> flattenCells(Mutation mutation) {
    List<Cell> body = new ArrayList<>();
    for (List<Cell> familyCells : mutation.getFamilyCellMap().values()) {
      body.addAll(familyCells);
    }
    return body;
  }

  /**
   * Extract the well-known replication attributes
   * ({@link ReplicationLogGroup#REPLICATION_ATTR_KEYS}) from the mutation, copied verbatim. Returns
   * an empty (mutable) map if the mutation has no attributes or none match.
   * <p>
   * {@link PhoenixIndexCodec#INDEX_UUID} is deliberately NOT part of this envelope: whether the
   * standby regenerates indexes is decided by the active from its resolved index maintainers, not
   * from the client-set UUID attribute. The active stamps an empty UUID onto the returned map only
   * for indexed tables (see {@code IndexRegionObserver}).
   */
  public static Map<String, byte[]> extractReplicationAttributes(Mutation mutation) {
    Map<String, byte[]> envelope = new HashMap<>();
    Map<String, byte[]> mutationAttrs = mutation.getAttributesMap();
    if (mutationAttrs == null || mutationAttrs.isEmpty()) {
      return envelope;
    }
    for (String key : ReplicationLogGroup.REPLICATION_ATTR_KEYS) {
      byte[] v = mutationAttrs.get(key);
      if (v != null) {
        envelope.put(key, v);
      }
    }
    return envelope;
  }

  /**
   * Build the flat cell stream the active ships for a batch of replicated mutations: each
   * mutation's data cells in family order, followed by one METAFAMILY pre-image cell carrying
   * {@code preImage}'s row state. This is the inverse of {@link #reconstructMutations}; replaying
   * its output through that method yields back the mutations with their
   * {@link IndexRegionObserver#PRE_IMAGE} attribute attached. A {@code null} {@code preImage}
   * encodes the empty-row sentinel. The pre-image cell is keyed by each mutation's own row,
   * mirroring the active's per-row pre-image capture.
   */
  public static List<Cell> buildReplicatedCells(List<Mutation> mutations, Put preImage)
    throws IOException {
    List<Cell> cells = new ArrayList<>();
    for (Mutation m : mutations) {
      cells.addAll(flattenCells(m));
      cells.add(IndexRegionObserver.buildPreImageCell(m.getRow(), preImage));
    }
    return cells;
  }

  /**
   * Stamp an empty {@link PhoenixIndexCodec#INDEX_UUID} onto a replication attribute envelope. An
   * empty UUID forces the standby down the server-PTable resolution path, which rebuilds index
   * maintainers from the schema/table/tenant attributes in the same envelope. Callers apply this
   * only for indexed tables (a non-indexed table needs no regeneration, and an empty UUID there
   * would fail on the standby with INDEX_METADATA_NOT_FOUND).
   */
  public static void stampIndexAttribute(Map<String, byte[]> attrs) {
    attrs.put(PhoenixIndexCodec.INDEX_UUID, HConstants.EMPTY_BYTE_ARRAY);
  }

  /**
   * Walk the record body, peeling off METAFAMILY pre-image cells (one per row) into a row-keyed
   * bucket and grouping the remaining data cells into Put/Delete mutations. Each result mutation is
   * stamped with the replication attributes and the generic
   * {@link IndexRegionObserver#REPLICATED_MUTATION} marker. When a pre-image entry exists for its
   * row, the pre-image bytes are also attached as {@link IndexRegionObserver#PRE_IMAGE}.
   */
  public static List<Mutation> reconstructMutations(Iterable<Cell> cells,
    Map<String, byte[]> replicationAttrs) throws IOException {
    Map<ImmutableBytesPtr, byte[]> preImages = new HashMap<>();
    List<Cell> dataCells = new ArrayList<>();
    for (Cell c : cells) {
      if (
        CellUtil.matchingFamily(c, WALEdit.METAFAMILY)
          && CellUtil.matchingQualifier(c, IndexRegionObserver.PRE_IMAGE_WAL_QUALIFIER)
      ) {
        preImages.put(new ImmutableBytesPtr(CellUtil.cloneRow(c)), CellUtil.cloneValue(c));
      } else {
        dataCells.add(c);
      }
    }
    List<Mutation> mutations = splitCellsIntoMutations(dataCells);
    for (Mutation m : mutations) {
      if (replicationAttrs != null) {
        for (Map.Entry<String, byte[]> e : replicationAttrs.entrySet()) {
          m.setAttribute(e.getKey(), e.getValue());
        }
      }
      m.setAttribute(IndexRegionObserver.REPLICATED_MUTATION, HConstants.EMPTY_BYTE_ARRAY);
      byte[] preImageBytes = preImages.get(new ImmutableBytesPtr(m.getRow()));
      if (preImageBytes != null) {
        m.setAttribute(IndexRegionObserver.PRE_IMAGE, preImageBytes);
      }
    }
    return mutations;
  }

  /** Group a cell stream into Put/Delete mutations using the row+type boundary algorithm. */
  public static List<Mutation> splitCellsIntoMutations(Iterable<Cell> cells) throws IOException {
    List<Mutation> result = new ArrayList<>();
    Cell previousCell = null;
    Mutation current = null;
    for (Cell cell : cells) {
      if (isNewRowOrType(previousCell, cell)) {
        if (current != null) {
          result.add(current);
        }
        if (CellUtil.isDelete(cell)) {
          current = new Delete(cell.getRowArray(), cell.getRowOffset(), cell.getRowLength());
        } else {
          current = new Put(cell.getRowArray(), cell.getRowOffset(), cell.getRowLength());
        }
      }
      if (CellUtil.isDelete(cell)) {
        ((Delete) current).add(cell);
      } else {
        ((Put) current).add(cell);
      }
      previousCell = cell;
    }
    if (current != null) {
      result.add(current);
    }
    return result;
  }

}
