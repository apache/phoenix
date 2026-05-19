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
package org.apache.phoenix.compile.keyspace;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import org.apache.hadoop.hbase.CompareOperator;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.phoenix.compile.keyspace.KeySpaceExpressionVisitor.Result;
import org.apache.phoenix.expression.AndExpression;
import org.apache.phoenix.expression.ComparisonExpression;
import org.apache.phoenix.expression.Expression;
import org.apache.phoenix.expression.IsNullExpression;
import org.apache.phoenix.expression.LiteralExpression;
import org.apache.phoenix.expression.OrExpression;
import org.apache.phoenix.expression.RowKeyColumnExpression;
import org.apache.phoenix.expression.RowValueConstructorExpression;
import org.apache.phoenix.schema.PColumn;
import org.apache.phoenix.schema.PDatum;
import org.apache.phoenix.schema.PTable;
import org.apache.phoenix.schema.RowKeyValueAccessor;
import org.apache.phoenix.schema.SortOrder;
import org.apache.phoenix.schema.types.PDataType;
import org.apache.phoenix.schema.types.PVarchar;
import org.junit.Test;

public class KeySpaceExpressionVisitorTest {

  /** Minimal PDatum for tests. */
  private static PDatum datum(PDataType type) {
    return new PDatum() {
      @Override
      public boolean isNullable() {
        return true;
      }

      @Override
      public PDataType getDataType() {
        return type;
      }

      @Override
      public Integer getMaxLength() {
        return null;
      }

      @Override
      public Integer getScale() {
        return null;
      }

      @Override
      public SortOrder getSortOrder() {
        return SortOrder.ASC;
      }
    };
  }

  /** Build a PTable mock whose PK columns all use the given type. */
  private static PTable tableWith(int nPk, PDataType type) {
    PTable t = mock(PTable.class);
    List<PColumn> cols = new ArrayList<>(nPk);
    for (int i = 0; i < nPk; i++) {
      PColumn c = mock(PColumn.class);
      when(c.getDataType()).thenReturn(type);
      when(c.getMaxLength()).thenReturn(null);
      when(c.getSortOrder()).thenReturn(SortOrder.ASC);
      cols.add(c);
    }
    when(t.getPKColumns()).thenReturn(cols);
    return t;
  }

  private static RowKeyColumnExpression col(int position, int nPk) {
    List<PDatum> pks = new ArrayList<>(nPk);
    for (int i = 0; i < nPk; i++) {
      pks.add(datum(PVarchar.INSTANCE));
    }
    return new RowKeyColumnExpression(datum(PVarchar.INSTANCE),
      new RowKeyValueAccessor(pks, position));
  }

  private static Expression lit(String s) throws Exception {
    return LiteralExpression.newConstant(s, PVarchar.INSTANCE);
  }

  private static Expression cmp(CompareOperator op, Expression lhs, Expression rhs) {
    return new ComparisonExpression(Arrays.asList(lhs, rhs), op);
  }

  private static KeySpaceList visit(PTable table, Expression e) {
    KeySpaceExpressionVisitor v = new KeySpaceExpressionVisitor(table);
    Result r = e.accept(v);
    return r.list;
  }

  @Test
  public void scalarEqualityProducesPointKeySpace() throws Exception {
    PTable t = tableWith(2, PVarchar.INSTANCE);
    Expression e = cmp(CompareOperator.EQUAL, col(0, 2), lit("a"));
    KeySpaceList list = visit(t, e);
    assertEquals(1, list.size());
    KeySpace ks = list.spaces().get(0);
    assertTrue(ks.get(0).isSingleKey());
    assertEquals("a".getBytes()[0], ks.get(0).getLowerRange()[0]);
    // Dim 1 stays everything.
    assertTrue(ks.get(1) == org.apache.phoenix.query.KeyRange.EVERYTHING_RANGE);
  }

  @Test
  public void scalarGreaterProducesOpenLowerRange() throws Exception {
    PTable t = tableWith(2, PVarchar.INSTANCE);
    Expression e = cmp(CompareOperator.GREATER, col(0, 2), lit("m"));
    KeySpaceList list = visit(t, e);
    assertEquals(1, list.size());
    KeySpace ks = list.spaces().get(0);
    assertFalse(ks.get(0).isLowerInclusive());
    assertTrue(ks.get(0).upperUnbound());
  }

  @Test
  public void andIntersectsPerDim() throws Exception {
    PTable t = tableWith(3, PVarchar.INSTANCE);
    Expression e = AndExpression.create(new ArrayList<>(Arrays.asList(
      cmp(CompareOperator.EQUAL, col(0, 3), lit("a")),
      cmp(CompareOperator.EQUAL, col(1, 3), lit("b")))));
    KeySpaceList list = visit(t, e);
    assertEquals(1, list.size());
    KeySpace ks = list.spaces().get(0);
    assertTrue(ks.get(0).isSingleKey());
    assertTrue(ks.get(1).isSingleKey());
    assertTrue(ks.get(2) == org.apache.phoenix.query.KeyRange.EVERYTHING_RANGE);
  }

  @Test
  public void orOnSamePkYieldsMultipleSpaces() throws Exception {
    PTable t = tableWith(2, PVarchar.INSTANCE);
    Expression e = new OrExpression(Arrays.asList(
      cmp(CompareOperator.EQUAL, col(0, 2), lit("a")),
      cmp(CompareOperator.EQUAL, col(0, 2), lit("b"))));
    KeySpaceList list = visit(t, e);
    assertEquals(2, list.size());
  }

  @Test
  public void orMergesAdjacentRanges() throws Exception {
    PTable t = tableWith(2, PVarchar.INSTANCE);
    // pk0 < "m" OR pk0 >= "m" => everything on dim0
    Expression e = new OrExpression(Arrays.asList(
      cmp(CompareOperator.LESS, col(0, 2), lit("m")),
      cmp(CompareOperator.GREATER_OR_EQUAL, col(0, 2), lit("m"))));
    KeySpaceList list = visit(t, e);
    // Should merge into one space with dim0 == EVERYTHING (or a single contiguous range).
    assertEquals(1, list.size());
    assertTrue(list.spaces().get(0).get(0).lowerUnbound());
    assertTrue(list.spaces().get(0).get(0).upperUnbound());
  }

  @Test
  public void degenerateAndOnSamePkYieldsUnsatisfiable() throws Exception {
    PTable t = tableWith(3, PVarchar.INSTANCE);
    // pk2 = 'x' AND pk2 = 'y' — PHOENIX-6669 shape, on non-leading PK.
    Expression e = AndExpression.create(new ArrayList<>(Arrays.asList(
      cmp(CompareOperator.EQUAL, col(2, 3), lit("x")),
      cmp(CompareOperator.EQUAL, col(2, 3), lit("y")))));
    KeySpaceList list = visit(t, e);
    assertTrue("degenerate AND must collapse to UNSAT", list.isUnsatisfiable());
  }

  @Test
  public void isNullProducesIsNullRange() throws Exception {
    PTable t = tableWith(2, PVarchar.INSTANCE);
    Expression e = IsNullExpression.create(col(0, 2), false, new ImmutableBytesWritable());
    KeySpaceList list = visit(t, e);
    assertEquals(1, list.size());
    assertTrue(list.spaces().get(0).get(0) == org.apache.phoenix.query.KeyRange.IS_NULL_RANGE);
  }

  @Test
  public void isNotNullProducesIsNotNullRange() throws Exception {
    PTable t = tableWith(2, PVarchar.INSTANCE);
    Expression e = IsNullExpression.create(col(0, 2), true, new ImmutableBytesWritable());
    KeySpaceList list = visit(t, e);
    assertEquals(1, list.size());
    assertTrue(list.spaces().get(0).get(0) == org.apache.phoenix.query.KeyRange.IS_NOT_NULL_RANGE);
  }

  @Test
  public void nonPkComparisonIsEverything() throws Exception {
    PTable t = tableWith(2, PVarchar.INSTANCE);
    // pk position 5 is out of range; visitor must treat as unknown → everything.
    // Build a row-key accessor with 6 positions so construction succeeds.
    List<PDatum> pks = new ArrayList<>();
    for (int i = 0; i < 6; i++) {
      pks.add(datum(PVarchar.INSTANCE));
    }
    RowKeyColumnExpression outOfRange =
      new RowKeyColumnExpression(datum(PVarchar.INSTANCE), new RowKeyValueAccessor(pks, 5));
    Expression e = cmp(CompareOperator.EQUAL, outOfRange, lit("a"));
    KeySpaceList list = visit(t, e);
    assertTrue(list.isEverything());
  }

  @Test
  public void consumedSetPopulatedForScalarComparison() throws Exception {
    PTable t = tableWith(2, PVarchar.INSTANCE);
    ComparisonExpression cmpNode = new ComparisonExpression(
      Arrays.<Expression>asList(col(0, 2), lit("a")), CompareOperator.EQUAL);
    KeySpaceExpressionVisitor v = new KeySpaceExpressionVisitor(t);
    Result r = cmpNode.accept(v);
    assertTrue(r.consumed().contains(cmpNode));
  }

  @Test
  public void rvcInListYieldsPerDimEqualitySpaces() throws Exception {
    PTable t = tableWith(2, PVarchar.INSTANCE);
    // (pk0, pk1) IN (('a','1'), ('b','2')) — the visitor emits one KeySpace per row value,
    // each with per-dim point-equality ranges. This matches the design's N-dim key-space
    // model: each PK column is a distinct dimension.
    RowValueConstructorExpression lhs =
      new RowValueConstructorExpression(Arrays.<Expression>asList(col(0, 2), col(1, 2)), false);
    RowValueConstructorExpression row1 = new RowValueConstructorExpression(
      Arrays.<Expression>asList(lit("a"), lit("1")), true);
    RowValueConstructorExpression row2 = new RowValueConstructorExpression(
      Arrays.<Expression>asList(lit("b"), lit("2")), true);
    org.apache.phoenix.expression.InListExpression in =
      new org.apache.phoenix.expression.InListExpression(
        Arrays.<Expression>asList(lhs, row1, row2), true);
    KeySpaceList list = visit(t, in);
    assertEquals(2, list.size());
    for (KeySpace ks : list.spaces()) {
      assertTrue(ks.get(0).isSingleKey());
      assertTrue(ks.get(1).isSingleKey());
    }
  }

  @Test
  public void emptyTreeViaAndWithNoRelevantChildrenIsEverything() throws Exception {
    PTable t = tableWith(2, PVarchar.INSTANCE);
    // Use a scalar comparison on a non-existent PK slot via position 5, which maps to the
    // "non-PK" path above. Wrap in a single-child AND to exercise visitLeave(AndExpression).
    List<PDatum> pks = new ArrayList<>();
    for (int i = 0; i < 6; i++) {
      pks.add(datum(PVarchar.INSTANCE));
    }
    RowKeyColumnExpression outOfRange =
      new RowKeyColumnExpression(datum(PVarchar.INSTANCE), new RowKeyValueAccessor(pks, 5));
    Expression inner = cmp(CompareOperator.EQUAL, outOfRange, lit("a"));
    Expression andNode = AndExpression.create(new ArrayList<>(Collections.singletonList(inner)));
    KeySpaceList list = visit(t, andNode);
    assertTrue(list.isEverything());
  }
}
