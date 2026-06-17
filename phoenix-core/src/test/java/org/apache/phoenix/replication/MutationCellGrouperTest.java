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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellBuilderFactory;
import org.apache.hadoop.hbase.CellBuilderType;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Mutation;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.Test;

/**
 * Unit tests for {@link MutationCellGrouper#splitCellsIntoMutations}, the replay-side inverse of
 * per-batch cell coalescing. This algorithm is the correctness lynchpin of PHOENIX-7931: a
 * regression here would silently merge or split mutations on the standby with no exception, so the
 * row+type boundary behavior is pinned here at the unit level rather than only through the
 * heavyweight cross-cluster IT.
 */
public class MutationCellGrouperTest {

  private static final byte[] FAMILY = Bytes.toBytes("cf");
  private static final byte[] QUALIFIER = Bytes.toBytes("q");
  private static final long TS = 100L;

  private static Cell putCell(String row, String value) {
    return CellBuilderFactory.create(CellBuilderType.DEEP_COPY).setRow(Bytes.toBytes(row))
      .setFamily(FAMILY).setQualifier(QUALIFIER).setTimestamp(TS).setType(Cell.Type.Put)
      .setValue(Bytes.toBytes(value)).build();
  }

  private static Cell deleteColumnCell(String row) {
    return CellBuilderFactory.create(CellBuilderType.DEEP_COPY).setRow(Bytes.toBytes(row))
      .setFamily(FAMILY).setQualifier(QUALIFIER).setTimestamp(TS).setType(Cell.Type.DeleteColumn)
      .build();
  }

  private static Cell deleteFamilyCell(String row) {
    return CellBuilderFactory.create(CellBuilderType.DEEP_COPY).setRow(Bytes.toBytes(row))
      .setFamily(FAMILY).setQualifier(QUALIFIER).setTimestamp(TS).setType(Cell.Type.DeleteFamily)
      .build();
  }

  private static String rowOf(Mutation m) {
    return Bytes.toString(m.getRow());
  }

  @Test
  public void testEmptyInputYieldsEmptyList() throws Exception {
    List<Mutation> result =
      MutationCellGrouper.splitCellsIntoMutations(Collections.<Cell> emptyList());
    assertTrue("Empty input should produce no mutations", result.isEmpty());
  }

  @Test
  public void testSinglePutCell() throws Exception {
    Cell cell = putCell("row1", "v1");
    List<Mutation> result =
      MutationCellGrouper.splitCellsIntoMutations(Collections.singletonList(cell));
    assertEquals(1, result.size());
    assertTrue("Expected a Put", result.get(0) instanceof Put);
    assertEquals("row1", rowOf(result.get(0)));
    assertEquals(1, result.get(0).size());
  }

  @Test
  public void testSingleDeleteCell() throws Exception {
    Cell cell = deleteColumnCell("row1");
    List<Mutation> result =
      MutationCellGrouper.splitCellsIntoMutations(Collections.singletonList(cell));
    assertEquals(1, result.size());
    assertTrue("Expected a Delete", result.get(0) instanceof Delete);
    assertEquals("row1", rowOf(result.get(0)));
  }

  @Test
  public void testContiguousCellsSameRowAndTypeFormOneMutation() throws Exception {
    List<Cell> cells = new ArrayList<>();
    cells.add(putCell("row1", "v1"));
    cells.add(putCell("row1", "v2"));
    cells.add(putCell("row1", "v3"));
    List<Mutation> result = MutationCellGrouper.splitCellsIntoMutations(cells);
    assertEquals("Same row + same type cells must coalesce into one Put", 1, result.size());
    assertTrue(result.get(0) instanceof Put);
    assertEquals(3, result.get(0).size());
  }

  @Test
  public void testRowChangeStartsNewMutation() throws Exception {
    List<Cell> cells = new ArrayList<>();
    cells.add(putCell("row1", "v1"));
    cells.add(putCell("row2", "v2"));
    cells.add(putCell("row3", "v3"));
    List<Mutation> result = MutationCellGrouper.splitCellsIntoMutations(cells);
    assertEquals("Each distinct row should yield its own Put", 3, result.size());
    assertEquals("row1", rowOf(result.get(0)));
    assertEquals("row2", rowOf(result.get(1)));
    assertEquals("row3", rowOf(result.get(2)));
  }

  /**
   * The case that justifies the class: a single row whose Put cells precede its Delete cells (e.g.
   * an index row that is rewritten then partially deleted within one server-side batch) must split
   * on the put-vs-delete boundary into a separate Put and Delete, never merge into one mutation.
   */
  @Test
  public void testPutThenDeleteSameRowSplitsOnTypeBoundary() throws Exception {
    List<Cell> cells = new ArrayList<>();
    cells.add(putCell("row1", "v1"));
    cells.add(deleteColumnCell("row1"));
    List<Mutation> result = MutationCellGrouper.splitCellsIntoMutations(cells);
    assertEquals("Put and Delete on the same row must be two mutations", 2, result.size());
    assertTrue("First should be the Put", result.get(0) instanceof Put);
    assertTrue("Second should be the Delete", result.get(1) instanceof Delete);
    assertEquals("row1", rowOf(result.get(0)));
    assertEquals("row1", rowOf(result.get(1)));
  }

  @Test
  public void testPutDeletePutOnThreeRowsYieldsThreeMutations() throws Exception {
    List<Cell> cells = new ArrayList<>();
    cells.add(putCell("rowA", "v1"));
    cells.add(deleteColumnCell("rowB"));
    cells.add(putCell("rowC", "v3"));
    List<Mutation> result = MutationCellGrouper.splitCellsIntoMutations(cells);
    assertEquals(3, result.size());
    assertTrue(result.get(0) instanceof Put);
    assertTrue(result.get(1) instanceof Delete);
    assertTrue(result.get(2) instanceof Put);
    assertEquals("rowA", rowOf(result.get(0)));
    assertEquals("rowB", rowOf(result.get(1)));
    assertEquals("rowC", rowOf(result.get(2)));
  }

  /**
   * Documents that the boundary is keyed on the exact cell type, not merely put-vs-delete: adjacent
   * DeleteColumn and DeleteFamily cells on the same row split into two separate Delete mutations.
   * This mirrors HBase's ReplicationSink and is relied on so a future change does not silently
   * coalesce distinct delete subtypes.
   */
  @Test
  public void testAdjacentDeleteSubtypesSameRowSplit() throws Exception {
    List<Cell> cells = new ArrayList<>();
    cells.add(deleteColumnCell("row1"));
    cells.add(deleteFamilyCell("row1"));
    List<Mutation> result = MutationCellGrouper.splitCellsIntoMutations(cells);
    assertEquals("Distinct delete subtypes must split into separate Deletes", 2, result.size());
    assertTrue(result.get(0) instanceof Delete);
    assertTrue(result.get(1) instanceof Delete);
  }

  /**
   * The grouper keys boundaries off the immediately preceding cell only, so a row that recurs
   * non-contiguously produces a separate mutation per contiguous run. This encodes the documented
   * "global row ordering is not required" contract: rowA appearing twice with rowB between yields
   * two rowA mutations, not a merged one.
   */
  @Test
  public void testNonContiguousSameRowYieldsSeparateMutations() throws Exception {
    List<Cell> cells = new ArrayList<>();
    cells.add(putCell("rowA", "v1"));
    cells.add(putCell("rowB", "v2"));
    cells.add(putCell("rowA", "v3"));
    List<Mutation> result = MutationCellGrouper.splitCellsIntoMutations(cells);
    assertEquals(3, result.size());
    assertEquals("rowA", rowOf(result.get(0)));
    assertEquals("rowB", rowOf(result.get(1)));
    assertEquals("rowA", rowOf(result.get(2)));
  }

  @Test
  public void testCellsArePreservedInResultMutations() throws Exception {
    Cell put1 = putCell("row1", "v1");
    Cell put2 = putCell("row1", "v2");
    List<Cell> cells = new ArrayList<>();
    cells.add(put1);
    cells.add(put2);
    List<Mutation> result = MutationCellGrouper.splitCellsIntoMutations(cells);
    assertEquals(1, result.size());
    List<Cell> grouped = result.get(0).getFamilyCellMap().get(FAMILY);
    assertEquals(2, grouped.size());
    assertTrue(
      CellUtil.equals(put1, grouped.get(0)) && CellUtil.matchingValue(put1, grouped.get(0)));
    assertTrue(
      CellUtil.equals(put2, grouped.get(1)) && CellUtil.matchingValue(put2, grouped.get(1)));
  }
}
