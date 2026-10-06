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
import java.util.List;
import java.util.Random;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.RegionInfo;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.phoenix.expression.Expression;
import org.apache.phoenix.expression.KeyValueColumnExpression;
import org.apache.phoenix.expression.LiteralExpression;
import org.apache.phoenix.expression.OrderByExpression;
import org.apache.phoenix.expression.function.L2DistanceFunction;
import org.apache.phoenix.schema.PDatum;
import org.apache.phoenix.schema.SortOrder;
import org.apache.phoenix.schema.tuple.SingleKeyValueTuple;
import org.apache.phoenix.schema.tuple.Tuple;
import org.apache.phoenix.schema.types.PDataType;
import org.apache.phoenix.schema.types.PVectorFloat;
import org.junit.Test;
import org.mockito.Mockito;

/**
 * Unit tests for bounded single pass top-N distance ordering via
 * {@link VectorDistanceOrderedResultIterator}.
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

  private static List<OrderByExpression> orderBy(float[] query) throws Exception {
    Expression column = new KeyValueColumnExpression(VECTOR, CF, CQ);
    Expression literal = LiteralExpression.newConstant(query, PVectorFloat.INSTANCE, DIM, null);
    return Collections.singletonList(OrderByExpression.createByCheckIfOrderByReverse(
      new L2DistanceFunction(Arrays.asList(column, literal)), false, true, false));
  }

  private static List<Tuple> rows(float[][] vectors) {
    List<Tuple> rows = new ArrayList<>();
    for (int i = 0; i < vectors.length; i++) {
      rows.add(new SingleKeyValueTuple(new KeyValue(Bytes.toBytes(String.format("r%04d", i)), CF,
        CQ, PVectorFloat.INSTANCE.toBytes(vectors[i]))));
    }
    return rows;
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

    VectorDistanceOrderedResultIterator iterator = new VectorDistanceOrderedResultIterator(
      new MaterializedResultIterator(rows(vectors)), orderBy(query), true, Long.MAX_VALUE, 10, 0,
      Long.MAX_VALUE, new Scan(), Mockito.mock(RegionInfo.class));
    List<Integer> actual = new ArrayList<>();
    for (Tuple t = iterator.next(); t != null; t = iterator.next()) {
      actual.add(Integer.parseInt(Bytes.toString(t.getValue(0).getRowArray(),
        t.getValue(0).getRowOffset() + 1, t.getValue(0).getRowLength() - 1)));
    }
    assertEquals(expected, actual);
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
