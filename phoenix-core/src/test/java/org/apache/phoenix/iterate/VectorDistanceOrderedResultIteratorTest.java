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
package org.apache.phoenix.iterate;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashSet;
import java.util.List;
import java.util.Random;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.RegionInfo;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.phoenix.expression.Expression;
import org.apache.phoenix.expression.KeyValueColumnExpression;
import org.apache.phoenix.expression.LiteralExpression;
import org.apache.phoenix.expression.OrderByExpression;
import org.apache.phoenix.expression.function.L2DistanceFunction;
import org.apache.phoenix.schema.PDatum;
import org.apache.phoenix.schema.SortOrder;
import org.apache.phoenix.schema.tuple.MultiKeyValueTuple;
import org.apache.phoenix.schema.tuple.SingleKeyValueTuple;
import org.apache.phoenix.schema.tuple.Tuple;
import org.apache.phoenix.schema.types.PDataType;
import org.apache.phoenix.schema.types.PDouble;
import org.apache.phoenix.schema.types.PVectorFloat;
import org.junit.Test;
import org.mockito.Mockito;

/**
 * Unit tests for {@link VectorDistanceOrderedResultIterator}, which keeps the top N rows by
 * distance in one bounded pass.
 */
public class VectorDistanceOrderedResultIteratorTest {

  private static final byte[] CF = Bytes.toBytes("0");
  private static final byte[] CQ = Bytes.toBytes("V");
  private static final int DIM = 8;

  private static final PDatum VECTOR = new PDatum() {
    @Override
    public boolean isNullable() {
      return true;
    }

    @Override
    public PDataType getDataType() {
      return PVectorFloat.INSTANCE;
    }

    @Override
    public Integer getMaxLength() {
      return DIM;
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

  /** Counts the rows for which the distance upper bound stopped the calculation early. */
  private static class CountingL2DistanceFunction extends L2DistanceFunction {
    int pruned;

    CountingL2DistanceFunction(List<Expression> children) {
      super(children);
    }

    @Override
    public boolean evaluate(Tuple tuple, ImmutableBytesWritable ptr) {
      boolean evaluated = super.evaluate(tuple, ptr);
      if (
        evaluated && ptr.getLength() > 0 && getDistanceUpperBound() != Double.MAX_VALUE
          && PDouble.INSTANCE.getCodec().decodeDouble(ptr.get(), ptr.getOffset(), SortOrder.ASC)
              == Double.MAX_VALUE
      ) {
        pruned++;
      }
      return evaluated;
    }
  }

  private static List<OrderByExpression> orderBy(float[] query) throws Exception {
    return orderBy(query, true);
  }

  private static List<OrderByExpression> orderBy(float[] query, boolean nullsLast)
    throws Exception {
    Expression column = new KeyValueColumnExpression(VECTOR, CF, CQ);
    Expression literal = LiteralExpression.newConstant(query, PVectorFloat.INSTANCE, DIM, null);
    return Collections.singletonList(OrderByExpression.createByCheckIfOrderByReverse(
      new CountingL2DistanceFunction(Arrays.asList(column, literal)), nullsLast, true, false));
  }

  private static int pruned(List<OrderByExpression> orderBy) {
    return ((CountingL2DistanceFunction) orderBy.get(0).getExpression()).pruned;
  }

  private static List<Tuple> rows(float[][] vectors) {
    List<Tuple> rows = new ArrayList<>();
    for (int i = 0; i < vectors.length; i++) {
      rows.add(new SingleKeyValueTuple(new KeyValue(Bytes.toBytes(String.format("r%04d", i)), CF,
        CQ, PVectorFloat.INSTANCE.toBytes(vectors[i]))));
    }
    return rows;
  }

  /** Makes a row with no vector cell. The distance of this row is null. */
  private static Tuple nullRow(String key) {
    return new MultiKeyValueTuple(Collections.<Cell> singletonList(
      new KeyValue(Bytes.toBytes(key), CF, Bytes.toBytes("OTHER"), Bytes.toBytes("x"))));
  }

  private static List<String> keys(List<Tuple> rows, List<OrderByExpression> orderBy, int limit)
    throws Exception {
    VectorDistanceOrderedResultIterator iterator =
      new VectorDistanceOrderedResultIterator(new MaterializedResultIterator(rows), orderBy, true,
        Long.MAX_VALUE, limit, 0, Long.MAX_VALUE, new Scan(), Mockito.mock(RegionInfo.class));
    List<String> keys = new ArrayList<>();
    for (Tuple t = iterator.next(); t != null; t = iterator.next()) {
      keys.add(Bytes.toString(t.getValue(0).getRowArray(), t.getValue(0).getRowOffset(),
        t.getValue(0).getRowLength()));
    }
    return keys;
  }

  private static float[] axis(int d, float length) {
    float[] v = new float[DIM];
    v[d] = length;
    return v;
  }

  private static double l2(float[] a, float[] b) {
    double sum = 0;
    for (int i = 0; i < a.length; i++) {
      double d = a[i] - b[i];
      sum += d * d;
    }
    return Math.sqrt(sum);
  }

  @Test
  public void testBoundedPassReturnsExactTopK() throws Exception {
    Random rng = new Random(7);
    float[][] vectors = new float[500][DIM];
    for (float[] v : vectors) {
      for (int d = 0; d < DIM; d++) {
        v[d] = rng.nextFloat() * 10;
      }
    }
    float[] query = vectors[123].clone();
    query[0] += 0.01f;
    List<Integer> expected = new ArrayList<>();
    for (int i = 0; i < vectors.length; i++) {
      expected.add(i);
    }
    expected.sort(Comparator.comparingDouble(i -> l2(vectors[i], query)));
    expected = expected.subList(0, 10);

    List<OrderByExpression> orderBy = orderBy(query);
    List<Integer> actual = new ArrayList<>();
    for (String key : keys(rows(vectors), orderBy, 10)) {
      actual.add(Integer.parseInt(key.substring(1)));
    }
    assertEquals(expected, actual);
    // After the pool is full, most rows cannot enter it, so the bound stops their calculation early
    assertTrue("pruned " + pruned(orderBy), pruned(orderBy) > 250);
  }

  @Test
  public void testNullDistancesFollowNullOrdering() throws Exception {
    float[][] vectors = { axis(0, 3f), axis(1, 1f), axis(2, 2f) };
    List<Tuple> rows = new ArrayList<>();
    rows.add(nullRow("n0"));
    rows.addAll(rows(vectors));
    rows.add(nullRow("n1"));
    float[] origin = new float[DIM];
    // A null distance does not enter the pool and is not pruned. The NULLS order sets its position
    assertEquals(Arrays.asList("r0001", "r0002"), keys(rows, orderBy(origin, true), 2));
    List<String> nullsLast = keys(rows, orderBy(origin, true), 10);
    assertEquals(Arrays.asList("r0001", "r0002", "r0000"), nullsLast.subList(0, 3));
    assertEquals(new HashSet<>(Arrays.asList("n0", "n1")), new HashSet<>(nullsLast.subList(3, 5)));
    List<String> nullsFirst = keys(rows, orderBy(origin, false), 3);
    assertEquals(new HashSet<>(Arrays.asList("n0", "n1")), new HashSet<>(nullsFirst.subList(0, 2)));
    assertEquals("r0001", nullsFirst.get(2));
  }

  @Test
  public void testTiesAtTheBoundAreRetained() throws Exception {
    // After the pool holds {1, 2}, the bound is 2. Later rows at exactly 2 must not be pruned
    float[][] vectors = { axis(0, 1f), axis(1, 2f), axis(2, 3f), axis(3, 2f), axis(4, 2f) };
    List<OrderByExpression> orderBy = orderBy(new float[DIM]);
    List<String> keys = keys(rows(vectors), orderBy, 2);
    assertEquals(2, keys.size());
    assertEquals("r0000", keys.get(0));
    assertTrue(keys.toString(), Arrays.asList("r0001", "r0003", "r0004").contains(keys.get(1)));
    assertEquals("only the row at 3 exceeds the bound", 1, pruned(orderBy));
  }

  @Test
  public void testLimitZeroReturnsNoRows() throws Exception {
    float[][] vectors = { axis(0, 1f), axis(1, 2f) };
    assertEquals(Collections.emptyList(), keys(rows(vectors), orderBy(new float[DIM]), 0));
  }

  @Test
  public void testLimitBeyondRowCountReturnsEveryRowUnpruned() throws Exception {
    float[][] vectors = { axis(0, 3f), axis(1, 1f), axis(2, 2f) };
    List<OrderByExpression> orderBy = orderBy(new float[DIM]);
    assertEquals(Arrays.asList("r0001", "r0002", "r0000"), keys(rows(vectors), orderBy, 3));
    assertEquals(0, pruned(orderBy));
    assertEquals(Arrays.asList("r0001", "r0002", "r0000"),
      keys(rows(vectors), orderBy(new float[DIM]), 10));
  }

  @Test
  public void testAppliesOnlyToSingleAscendingDistance() throws Exception {
    List<OrderByExpression> distance = orderBy(new float[DIM]);
    assertTrue(VectorDistanceOrderedResultIterator.appliesTo(distance));
    OrderByExpression descending = OrderByExpression
      .createByCheckIfOrderByReverse(distance.get(0).getExpression(), false, false, false);
    assertFalse(
      VectorDistanceOrderedResultIterator.appliesTo(Collections.singletonList(descending)));
    List<OrderByExpression> two = new ArrayList<>(distance);
    two.add(distance.get(0));
    assertFalse(VectorDistanceOrderedResultIterator.appliesTo(two));
  }
}
