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

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import java.sql.Connection;
import java.sql.DriverManager;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellBuilderFactory;
import org.apache.hadoop.hbase.CellBuilderType;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Mutation;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.Pair;
import org.apache.hadoop.hbase.wal.WALEdit;
import org.apache.phoenix.execute.MutationState;
import org.apache.phoenix.hbase.index.IndexRegionObserver;
import org.apache.phoenix.index.PhoenixIndexCodec;
import org.apache.phoenix.jdbc.PhoenixConnection;
import org.apache.phoenix.query.BaseConnectionlessQueryTest;
import org.junit.Test;

/**
 * Unit tests for {@link MutationCellGrouper}. Real Phoenix UPSERT/DELETE statements are used to
 * obtain mutations with the same cell shapes the production write path produces, so the tests
 * exercise the row+type boundary algorithm against representative inputs (Put, DeleteFamily,
 * DeleteColumn) without manually constructing cells.
 */
public class MutationCellGrouperTest extends BaseConnectionlessQueryTest {

  private static List<Mutation> getMutations(PhoenixConnection pconn) throws Exception {
    List<Mutation> all = new ArrayList<>();
    Iterator<Pair<byte[], List<Mutation>>> it = pconn.getMutationState().toMutations();
    while (it.hasNext()) {
      all.addAll(it.next().getSecond());
    }
    return all;
  }

  private static List<Cell> flatten(List<Mutation> mutations) {
    List<Cell> cells = new ArrayList<>();
    for (Mutation m : mutations) {
      cells.addAll(MutationCellGrouper.flattenCells(m));
    }
    return cells;
  }

  // ---------- splitCellsIntoMutations + flattenCells round-trip ----------

  @Test
  public void testUpsertSingleRowRoundTrip() throws Exception {
    try (Connection conn = DriverManager.getConnection(getUrl())) {
      conn.setAutoCommit(false);
      conn.createStatement()
        .execute("CREATE TABLE t (k integer not null primary key, a varchar, b varchar)");
      conn.createStatement().execute("UPSERT INTO t VALUES(1, 'aa', 'bb')");
      List<Mutation> source = getMutations(conn.unwrap(PhoenixConnection.class));
      assertEquals(1, source.size());
      assertTrue(source.get(0) instanceof Put);

      List<Mutation> regrouped = MutationCellGrouper.splitCellsIntoMutations(flatten(source));

      assertEquals(1, regrouped.size());
      assertTrue(regrouped.get(0) instanceof Put);
      assertArrayEquals(source.get(0).getRow(), regrouped.get(0).getRow());
    }
  }

  @Test
  public void testUpsertMultipleRowsRoundTrip() throws Exception {
    try (Connection conn = DriverManager.getConnection(getUrl())) {
      conn.setAutoCommit(false);
      conn.createStatement().execute("CREATE TABLE t (k integer not null primary key, a varchar)");
      conn.createStatement().execute("UPSERT INTO t VALUES(1, 'aa')");
      conn.createStatement().execute("UPSERT INTO t VALUES(2, 'bb')");
      conn.createStatement().execute("UPSERT INTO t VALUES(3, 'cc')");
      List<Mutation> source = getMutations(conn.unwrap(PhoenixConnection.class));
      assertEquals(3, source.size());

      List<Mutation> regrouped = MutationCellGrouper.splitCellsIntoMutations(flatten(source));

      assertEquals(3, regrouped.size());
      for (Mutation m : regrouped) {
        assertTrue(m instanceof Put);
      }
    }
  }

  @Test
  public void testDeleteRowRoundTrip() throws Exception {
    try (Connection conn = DriverManager.getConnection(getUrl())) {
      conn.setAutoCommit(false);
      conn.createStatement().execute("CREATE TABLE t (k integer not null primary key, a varchar)");
      conn.createStatement().execute("DELETE FROM t WHERE k = 7");
      List<Mutation> source = getMutations(conn.unwrap(PhoenixConnection.class));
      assertEquals(1, source.size());
      assertTrue(source.get(0) instanceof Delete);

      List<Mutation> regrouped = MutationCellGrouper.splitCellsIntoMutations(flatten(source));

      assertEquals(1, regrouped.size());
      assertTrue(regrouped.get(0) instanceof Delete);
      assertArrayEquals(source.get(0).getRow(), regrouped.get(0).getRow());
    }
  }

  @Test
  public void testMixedUpsertAndDeleteAcrossRows() throws Exception {
    try (Connection conn = DriverManager.getConnection(getUrl())) {
      conn.setAutoCommit(false);
      conn.createStatement().execute("CREATE TABLE t (k integer not null primary key, a varchar)");
      conn.createStatement().execute("UPSERT INTO t VALUES(1, 'aa')");
      conn.createStatement().execute("DELETE FROM t WHERE k = 2");
      conn.createStatement().execute("UPSERT INTO t VALUES(3, 'cc')");
      List<Mutation> source = getMutations(conn.unwrap(PhoenixConnection.class));
      assertEquals(3, source.size());

      // Assert source shape against row key (Phoenix's MutationState may reorder mutations).
      Map<String, Class<?>> sourceShape = new HashMap<>();
      for (Mutation m : source) {
        sourceShape.put(Bytes.toStringBinary(m.getRow()), m.getClass());
      }

      List<Mutation> regrouped = MutationCellGrouper.splitCellsIntoMutations(flatten(source));

      assertEquals(3, regrouped.size());
      Map<String, Class<?>> regroupedShape = new HashMap<>();
      for (Mutation m : regrouped) {
        regroupedShape.put(Bytes.toStringBinary(m.getRow()), m.getClass());
      }
      assertEquals(sourceShape, regroupedShape);
    }
  }

  /**
   * UPSERT that sets a column to NULL produces both Put cells (for the non-null columns and the
   * empty value cell) AND a DeleteColumn cell (for the explicit NULL). The grouper splits a mixed
   * Put/DeleteColumn cell run for one row into one Put + one Delete, which is the correct
   * primary-side shape that the standby applies to the same row in batch order.
   */
  @Test
  public void testUpsertWithNullProducesPutAndDeleteForSameRow() throws Exception {
    try (Connection conn = DriverManager.getConnection(getUrl())) {
      conn.setAutoCommit(false);
      conn.createStatement().execute(
        "CREATE TABLE t (k integer not null primary key, a varchar, b varchar, c varchar)");
      conn.createStatement().execute("UPSERT INTO t VALUES(1, 'aa', NULL, 'cc')");
      List<Mutation> source = getMutations(conn.unwrap(PhoenixConnection.class));
      // Phoenix may produce one Put (with mixed cells) or a Put + a Delete pair; either way
      // the row+type boundary algorithm must reproduce the same per-row cell sets after split.
      Map<String, List<Cell>> sourceByRow = cellsByRow(source);
      List<Cell> flattenedSource = flatten(source);

      List<Mutation> regrouped = MutationCellGrouper.splitCellsIntoMutations(flattenedSource);

      Map<String, List<Cell>> regroupedByRow = cellsByRow(regrouped);
      assertEquals("regrouped row key set must match source", sourceByRow.keySet(),
        regroupedByRow.keySet());
      for (String rowKey : sourceByRow.keySet()) {
        assertEquals("cell count for row " + rowKey + " must round-trip",
          sourceByRow.get(rowKey).size(), regroupedByRow.get(rowKey).size());
      }

      // The point of this test: the mixed Put/DeleteColumn run for the row must split into a Put
      // (non-null columns + empty value cell) AND a Delete (the explicit NULL) -- a single merged
      // Put with the same total cell count would pass the per-row count check above but be wrong.
      String rowKey = sourceByRow.keySet().iterator().next();
      List<Mutation> putsForRow = new ArrayList<>();
      List<Mutation> deletesForRow = new ArrayList<>();
      for (Mutation m : regrouped) {
        if (!Bytes.toStringBinary(m.getRow()).equals(rowKey)) {
          continue;
        }
        (m instanceof Delete ? deletesForRow : putsForRow).add(m);
      }
      assertEquals("row must split into exactly one Put", 1, putsForRow.size());
      assertEquals("row must split into exactly one Delete", 1, deletesForRow.size());
      // Every cell in each split mutation must carry the matching type (no Put cell leaked into the
      // Delete, or vice versa), and the two together must account for all the row's cells.
      int putCells = MutationCellGrouper.flattenCells(putsForRow.get(0)).size();
      int deleteCells = MutationCellGrouper.flattenCells(deletesForRow.get(0)).size();
      for (Cell c : MutationCellGrouper.flattenCells(putsForRow.get(0))) {
        assertTrue("Put must hold only non-delete cells", !CellUtil.isDelete(c));
      }
      for (Cell c : MutationCellGrouper.flattenCells(deletesForRow.get(0))) {
        assertTrue("Delete must hold only delete cells", CellUtil.isDelete(c));
      }
      assertEquals("Put + Delete cells must account for all the row's cells",
        sourceByRow.get(rowKey).size(), putCells + deleteCells);
    }
  }

  private static Map<String, List<Cell>> cellsByRow(List<Mutation> mutations) {
    Map<String, List<Cell>> byRow = new HashMap<>();
    for (Mutation m : mutations) {
      String key = Bytes.toStringBinary(m.getRow());
      byRow.computeIfAbsent(key, k -> new ArrayList<>())
        .addAll(MutationCellGrouper.flattenCells(m));
    }
    return byRow;
  }

  @Test
  public void testEmptyInputProducesEmptyOutput() throws Exception {
    List<Mutation> regrouped =
      MutationCellGrouper.splitCellsIntoMutations(Collections.<Cell> emptyList());
    assertTrue(regrouped.isEmpty());
  }

  // ---------- reconstructMutations: pre-image peeling + REPLICATED_MUTATION/PRE_IMAGE ----------

  private static Cell preImageCell(byte[] row, byte[] value) {
    return CellBuilderFactory.create(CellBuilderType.SHALLOW_COPY).setRow(row)
      .setFamily(WALEdit.METAFAMILY).setQualifier(IndexRegionObserver.PRE_IMAGE_WAL_QUALIFIER)
      .setTimestamp(HConstants.LATEST_TIMESTAMP).setType(Cell.Type.Put).setValue(value).build();
  }

  @Test
  public void testReconstructPeelsPreImageCellAndStampsAttribute() throws Exception {
    try (Connection conn = DriverManager.getConnection(getUrl())) {
      conn.setAutoCommit(false);
      conn.createStatement().execute("CREATE TABLE t (k integer not null primary key, a varchar)");
      conn.createStatement().execute("UPSERT INTO t VALUES(1, 'aa')");
      List<Mutation> source = getMutations(conn.unwrap(PhoenixConnection.class));
      assertEquals(1, source.size());
      byte[] row = source.get(0).getRow();
      List<Cell> cells = flatten(source);
      // Synthesize a pre-image carrier (just opaque bytes here — production carries PB Put).
      byte[] preImageBytes = Bytes.toBytes("OPAQUE_PB_PUT");
      cells.add(preImageCell(row, preImageBytes));

      List<Mutation> regrouped =
        MutationCellGrouper.reconstructMutations(cells, Collections.<String, byte[]> emptyMap());

      assertEquals("pre-image cell must not produce its own mutation", 1, regrouped.size());
      assertTrue(regrouped.get(0) instanceof Put);
      assertArrayEquals(row, regrouped.get(0).getRow());
      assertArrayEquals(preImageBytes,
        regrouped.get(0).getAttribute(IndexRegionObserver.PRE_IMAGE));
      assertNotNull("REPLICATED_MUTATION marker must be stamped on every reconstructed mutation",
        regrouped.get(0).getAttribute(IndexRegionObserver.REPLICATED_MUTATION));
    }
  }

  @Test
  public void testReconstructEmptyPreImageSentinel() throws Exception {
    try (Connection conn = DriverManager.getConnection(getUrl())) {
      conn.setAutoCommit(false);
      conn.createStatement().execute("CREATE TABLE t (k integer not null primary key, a varchar)");
      conn.createStatement().execute("UPSERT INTO t VALUES(1, 'aa')");
      List<Mutation> source = getMutations(conn.unwrap(PhoenixConnection.class));
      byte[] row = source.get(0).getRow();
      List<Cell> cells = flatten(source);
      cells.add(preImageCell(row, HConstants.EMPTY_BYTE_ARRAY));

      List<Mutation> regrouped =
        MutationCellGrouper.reconstructMutations(cells, Collections.<String, byte[]> emptyMap());

      assertEquals(1, regrouped.size());
      byte[] attr = regrouped.get(0).getAttribute(IndexRegionObserver.PRE_IMAGE);
      assertNotNull(attr);
      assertEquals("empty value sentinel preserved", 0, attr.length);
      assertNotNull("REPLICATED_MUTATION marker must be stamped on every reconstructed mutation",
        regrouped.get(0).getAttribute(IndexRegionObserver.REPLICATED_MUTATION));
    }
  }

  @Test
  public void testReconstructStampsReplicationAttributesOnEveryMutation() throws Exception {
    try (Connection conn = DriverManager.getConnection(getUrl())) {
      conn.setAutoCommit(false);
      conn.createStatement().execute("CREATE TABLE t (k integer not null primary key, a varchar)");
      conn.createStatement().execute("UPSERT INTO t VALUES(1, 'aa')");
      conn.createStatement().execute("UPSERT INTO t VALUES(2, 'bb')");
      List<Mutation> source = getMutations(conn.unwrap(PhoenixConnection.class));
      assertEquals(2, source.size());

      Map<String, byte[]> attrs = new HashMap<>();
      attrs.put(PhoenixIndexCodec.INDEX_UUID, Bytes.toBytes("uuid-1"));
      attrs.put(MutationState.MutationMetadataType.SCHEMA_NAME.toString(), Bytes.toBytes("S"));
      attrs.put(MutationState.MutationMetadataType.LOGICAL_TABLE_NAME.toString(),
        Bytes.toBytes("T"));

      List<Mutation> regrouped = MutationCellGrouper.reconstructMutations(flatten(source), attrs);

      assertEquals(2, regrouped.size());
      for (Mutation m : regrouped) {
        for (Map.Entry<String, byte[]> e : attrs.entrySet()) {
          assertArrayEquals("attribute " + e.getKey() + " must be on every mutation", e.getValue(),
            m.getAttribute(e.getKey()));
        }
        assertNotNull("REPLICATED_MUTATION marker must be on every reconstructed mutation",
          m.getAttribute(IndexRegionObserver.REPLICATED_MUTATION));
        assertNull("PRE_IMAGE must not be set when no pre-image cell is present",
          m.getAttribute(IndexRegionObserver.PRE_IMAGE));
      }
    }
  }

  @Test
  public void testReconstructAttachesPreImageOnlyToMatchingRow() throws Exception {
    try (Connection conn = DriverManager.getConnection(getUrl())) {
      conn.setAutoCommit(false);
      conn.createStatement().execute("CREATE TABLE t (k integer not null primary key, a varchar)");
      conn.createStatement().execute("UPSERT INTO t VALUES(1, 'aa')");
      conn.createStatement().execute("UPSERT INTO t VALUES(2, 'bb')");
      List<Mutation> source = getMutations(conn.unwrap(PhoenixConnection.class));
      byte[] row1 = source.get(0).getRow();
      byte[] row2 = source.get(1).getRow();
      List<Cell> cells = flatten(source);
      // Only row1 has a pre-image.
      byte[] row1PreImage = Bytes.toBytes("PRE-1");
      cells.add(preImageCell(row1, row1PreImage));

      List<Mutation> regrouped =
        MutationCellGrouper.reconstructMutations(cells, Collections.<String, byte[]> emptyMap());

      assertEquals(2, regrouped.size());
      for (Mutation m : regrouped) {
        assertNotNull("REPLICATED_MUTATION marker must be on every reconstructed mutation",
          m.getAttribute(IndexRegionObserver.REPLICATED_MUTATION));
        if (Bytes.equals(m.getRow(), row1)) {
          assertArrayEquals(row1PreImage, m.getAttribute(IndexRegionObserver.PRE_IMAGE));
        } else if (Bytes.equals(m.getRow(), row2)) {
          assertNull(m.getAttribute(IndexRegionObserver.PRE_IMAGE));
        }
      }
    }
  }

  // ---------- extractReplicationAttributes ----------

  @Test
  public void testExtractReplicationAttributesFiltersToWellKnownKeys() throws Exception {
    Put p = new Put(Bytes.toBytes("r"));
    p.setAttribute(PhoenixIndexCodec.INDEX_UUID, Bytes.toBytes("uuid"));
    p.setAttribute(MutationState.MutationMetadataType.SCHEMA_NAME.toString(), Bytes.toBytes("S"));
    p.setAttribute("UNRELATED_ATTRIBUTE", Bytes.toBytes("ignore me"));
    p.setAttribute(IndexRegionObserver.REPLICATED_MUTATION, Bytes.toBytes("must-not-leak"));
    p.setAttribute(IndexRegionObserver.PRE_IMAGE, Bytes.toBytes("also-must-not-leak"));

    Map<String, byte[]> extracted = MutationCellGrouper.extractReplicationAttributes(p);

    assertArrayEquals(
      "INDEX_UUID must be normalized to empty so the standby resolves index "
        + "maintainers from its own PTable rather than the active's server-cache key",
      HConstants.EMPTY_BYTE_ARRAY, extracted.get(PhoenixIndexCodec.INDEX_UUID));
    assertArrayEquals(Bytes.toBytes("S"),
      extracted.get(MutationState.MutationMetadataType.SCHEMA_NAME.toString()));
    assertNull("non-replication attribute must be filtered out",
      extracted.get("UNRELATED_ATTRIBUTE"));
    assertNull("REPLICATED_MUTATION is reader-synthesized, must not appear in record attributes",
      extracted.get(IndexRegionObserver.REPLICATED_MUTATION));
    assertNull("PRE_IMAGE is reader-synthesized, must not appear in record attributes",
      extracted.get(IndexRegionObserver.PRE_IMAGE));
  }

  @Test
  public void testExtractReplicationAttributesEmptyForBareMutation() throws Exception {
    Put p = new Put(Bytes.toBytes("r"));
    Map<String, byte[]> extracted = MutationCellGrouper.extractReplicationAttributes(p);
    assertTrue(extracted.isEmpty());
  }

  // ---------- contiguity stress (pre-image cell never breaks groupable runs) ----------

  @Test
  public void testPreImageCellInterleavedDoesNotCreateExtraMutation() throws Exception {
    try (Connection conn = DriverManager.getConnection(getUrl())) {
      conn.setAutoCommit(false);
      conn.createStatement().execute("CREATE TABLE t (k integer not null primary key, a varchar)");
      conn.createStatement().execute("UPSERT INTO t VALUES(1, 'aa')");
      conn.createStatement().execute("UPSERT INTO t VALUES(2, 'bb')");
      List<Mutation> source = getMutations(conn.unwrap(PhoenixConnection.class));
      byte[] row1 = source.get(0).getRow();
      byte[] row2 = source.get(1).getRow();
      // Build a stream that interleaves pre-image cells between row buckets:
      // [row1.data..., row1.preImage, row2.data..., row2.preImage]
      List<Cell> cells = new ArrayList<>();
      cells.addAll(MutationCellGrouper.flattenCells(source.get(0)));
      cells.add(preImageCell(row1, Bytes.toBytes("PRE-1")));
      cells.addAll(MutationCellGrouper.flattenCells(source.get(1)));
      cells.add(preImageCell(row2, Bytes.toBytes("PRE-2")));

      List<Mutation> regrouped =
        MutationCellGrouper.reconstructMutations(cells, Collections.<String, byte[]> emptyMap());

      assertEquals("pre-image cells must be peeled before grouping", 2, regrouped.size());
      for (Mutation m : regrouped) {
        assertTrue(m instanceof Put);
        assertNotNull(m.getAttribute(IndexRegionObserver.REPLICATED_MUTATION));
        assertNotNull(m.getAttribute(IndexRegionObserver.PRE_IMAGE));
      }
    }
  }

  /**
   * Guard against a regression where a pre-image cell that follows a row's last cell of a different
   * type might be misgrouped. The test: a Delete row's cells (Type.DeleteFamily) followed by a
   * Type.Put pre-image cell. Without peeling, this would split into a Delete plus a stray Put. With
   * peeling it must split into exactly one Delete carrying the pre-image attribute.
   */
  @Test
  public void testPreImageCellAfterDeleteCellsIsPeeled() throws Exception {
    try (Connection conn = DriverManager.getConnection(getUrl())) {
      conn.setAutoCommit(false);
      conn.createStatement().execute("CREATE TABLE t (k integer not null primary key, a varchar)");
      conn.createStatement().execute("DELETE FROM t WHERE k = 5");
      List<Mutation> source = getMutations(conn.unwrap(PhoenixConnection.class));
      assertEquals(1, source.size());
      assertTrue(source.get(0) instanceof Delete);
      byte[] row = source.get(0).getRow();

      List<Cell> cells = new ArrayList<>();
      cells.addAll(MutationCellGrouper.flattenCells(source.get(0)));
      cells.add(preImageCell(row, Bytes.toBytes("PRE-DEL")));

      List<Mutation> regrouped =
        MutationCellGrouper.reconstructMutations(cells, Collections.<String, byte[]> emptyMap());

      assertEquals(1, regrouped.size());
      assertTrue(regrouped.get(0) instanceof Delete);
      assertArrayEquals(Bytes.toBytes("PRE-DEL"),
        regrouped.get(0).getAttribute(IndexRegionObserver.PRE_IMAGE));
      assertNotNull(regrouped.get(0).getAttribute(IndexRegionObserver.REPLICATED_MUTATION));
    }
  }
}
