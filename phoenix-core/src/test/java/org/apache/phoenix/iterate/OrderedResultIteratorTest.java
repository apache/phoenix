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

import static org.junit.Assert.fail;

import java.sql.SQLException;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import org.apache.hadoop.hbase.client.RegionInfo;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.regionserver.RegionScanner;
import org.apache.phoenix.execute.ScanPlan;
import org.apache.phoenix.expression.Expression;
import org.apache.phoenix.expression.LiteralExpression;
import org.apache.phoenix.expression.OrderByExpression;
import org.apache.phoenix.hbase.index.util.VersionUtil;
import org.apache.phoenix.util.ScanUtil;
import org.junit.Test;
import org.mockito.Mockito;

/**
 * Test class for {@link OrderedResultIterator}.
 */
public class OrderedResultIteratorTest {

  @Test
  public void testNullIteratorOnClose() throws SQLException {
    ResultIterator delegate = ResultIterator.EMPTY_ITERATOR;
    List<OrderByExpression> orderByExpressions = Collections.singletonList(null);
    int thresholdBytes = Integer.MAX_VALUE;
    boolean spoolingEnabled = true;
    OrderedResultIterator iterator =
      new OrderedResultIterator(delegate, orderByExpressions, spoolingEnabled, thresholdBytes);
    // Should not throw an exception
    iterator.close();
  }

  @Test
  public void testSpoolingBackwardCompatibility() {
    RegionScanner s = Mockito.mock(RegionScanner.class);
    RegionInfo regionInfo = Mockito.mock(RegionInfo.class);
    Mockito.when(s.getRegionInfo()).thenReturn(regionInfo);
    Scan scan = new Scan();
    Expression exp = LiteralExpression.newConstant(Boolean.TRUE);
    OrderByExpression ex =
      OrderByExpression.createByCheckIfOrderByReverse(exp, false, false, false);
    ScanPlan.serializeScanRegionObserverIntoScan(scan, 0, Arrays.asList(ex), 100);
    // Check 5.1.0 & Check > 5.1.0
    ScanUtil.setClientVersion(scan, VersionUtil.encodeVersion("5.1.0"));
    NonAggregateRegionScannerFactory.deserializeFromScan(scan, s, false, 100);

    ScanUtil.setClientVersion(scan, VersionUtil.encodeVersion("5.2.0"));
    NonAggregateRegionScannerFactory.deserializeFromScan(scan, s, false, 100);
    // Check 4.15.0 Check > 4.15.0
    ScanUtil.setClientVersion(scan, VersionUtil.encodeVersion("4.15.0"));
    NonAggregateRegionScannerFactory.deserializeFromScan(scan, s, false, 100);
    ScanUtil.setClientVersion(scan, VersionUtil.encodeVersion("4.15.1"));
    NonAggregateRegionScannerFactory.deserializeFromScan(scan, s, false, 100);

    // Check < 5.1
    ScanUtil.setClientVersion(scan, VersionUtil.encodeVersion("5.0.0"));
    try {
      NonAggregateRegionScannerFactory.deserializeFromScan(scan, s, false, 100);
      fail("Deserialize should fail for 5.0.0 since we didn't serialize thresholdBytes");
    } catch (IllegalArgumentException e) {
    }
    // Check < 4.15
    ScanUtil.setClientVersion(scan, VersionUtil.encodeVersion("4.14.0"));
    try {
      NonAggregateRegionScannerFactory.deserializeFromScan(scan, s, false, 100);
      fail("Deserialize should fail for 4.14.0 since we didn't serialize thresholdBytes");
    } catch (IllegalArgumentException e) {
    }

  }

  @Test
  public void testDeserializeFromScanTwoPhaseFlagActivation() throws Exception {
    RegionScanner s = Mockito.mock(RegionScanner.class);
    RegionInfo regionInfo = Mockito.mock(RegionInfo.class);
    Mockito.when(s.getRegionInfo()).thenReturn(regionInfo);
    Scan scan = new Scan();

    Expression v1 = LiteralExpression.newConstant(new float[] { 1.0f, 0.0f },
      org.apache.phoenix.schema.types.PVectorFloat.INSTANCE);
    Expression v2 = LiteralExpression.newConstant(new float[] { 0.0f, 1.0f },
      org.apache.phoenix.schema.types.PVectorFloat.INSTANCE);
    org.apache.phoenix.expression.function.L2DistanceFunction func =
      new org.apache.phoenix.expression.function.L2DistanceFunction(Arrays.asList(v1, v2));
    OrderByExpression ob =
      OrderByExpression.createByCheckIfOrderByReverse(func, false, false, false);

    ScanUtil.setClientVersion(scan, VersionUtil.encodeVersion("5.4.0"));
    ScanPlan.serializeScanRegionObserverIntoScan(scan, 10, Arrays.asList(ob), 100);
    scan.setAttribute(
      org.apache.phoenix.coprocessorclient.BaseScannerRegionObserverConstants.VECTOR_OVERSAMPLE_FACTOR,
      org.apache.hadoop.hbase.util.Bytes.toBytes(3.0));

    OrderedResultIterator iterator = NonAggregateRegionScannerFactory
      .deserializeFromScan(scan, s, false, 20 * 1024 * 1024).getIterator();
    org.junit.Assert.assertNotNull(iterator);
    org.junit.Assert.assertTrue(iterator.isTwoPhaseVectorScoring());
    org.junit.Assert.assertEquals(3.0, iterator.getOversampleFactor(), 0.001);
    org.junit.Assert.assertEquals(Integer.valueOf(30), iterator.getCoarseLimit());
  }

  @Test
  public void testDeserializeFromScanNoOversampleIsSinglePhase() throws Exception {
    RegionScanner s = Mockito.mock(RegionScanner.class);
    RegionInfo regionInfo = Mockito.mock(RegionInfo.class);
    Mockito.when(s.getRegionInfo()).thenReturn(regionInfo);
    Scan scan = new Scan();

    Expression v1 = LiteralExpression.newConstant(new float[] { 1.0f, 0.0f },
      org.apache.phoenix.schema.types.PVectorFloat.INSTANCE);
    Expression v2 = LiteralExpression.newConstant(new float[] { 0.0f, 1.0f },
      org.apache.phoenix.schema.types.PVectorFloat.INSTANCE);
    org.apache.phoenix.expression.function.L2DistanceFunction func =
      new org.apache.phoenix.expression.function.L2DistanceFunction(Arrays.asList(v1, v2));
    OrderByExpression ob =
      OrderByExpression.createByCheckIfOrderByReverse(func, false, false, false);

    ScanUtil.setClientVersion(scan, VersionUtil.encodeVersion("5.4.0"));
    ScanPlan.serializeScanRegionObserverIntoScan(scan, 10, Arrays.asList(ob), 100);

    OrderedResultIterator iterator =
      NonAggregateRegionScannerFactory.deserializeFromScan(scan, s, false, 100).getIterator();
    org.junit.Assert.assertNotNull(iterator);
    org.junit.Assert.assertFalse(iterator.isTwoPhaseVectorScoring());
    org.junit.Assert.assertEquals(1.0, iterator.getOversampleFactor(), 0.001);
  }

  @Test
  public void testDeserializeFromScanNonDistanceFallsBackToSinglePhase() throws Exception {
    RegionScanner s = Mockito.mock(RegionScanner.class);
    RegionInfo regionInfo = Mockito.mock(RegionInfo.class);
    Mockito.when(s.getRegionInfo()).thenReturn(regionInfo);
    Scan scan = new Scan();

    Expression exp = LiteralExpression.newConstant(Boolean.TRUE);
    OrderByExpression ob =
      OrderByExpression.createByCheckIfOrderByReverse(exp, false, false, false);

    ScanUtil.setClientVersion(scan, VersionUtil.encodeVersion("5.4.0"));
    ScanPlan.serializeScanRegionObserverIntoScan(scan, 10, Arrays.asList(ob), 100);
    scan.setAttribute(
      org.apache.phoenix.coprocessorclient.BaseScannerRegionObserverConstants.VECTOR_OVERSAMPLE_FACTOR,
      org.apache.hadoop.hbase.util.Bytes.toBytes(3.0));

    OrderedResultIterator iterator =
      NonAggregateRegionScannerFactory.deserializeFromScan(scan, s, false, 100).getIterator();
    org.junit.Assert.assertNotNull(iterator);
    org.junit.Assert.assertFalse(iterator.isTwoPhaseVectorScoring());
  }

  private static org.apache.phoenix.schema.PDatum createDatum(final int dim) {
    return new org.apache.phoenix.schema.PDatum() {
      @Override
      public boolean isNullable() {
        return true;
      }

      @Override
      public org.apache.phoenix.schema.types.PDataType getDataType() {
        return org.apache.phoenix.schema.types.PVectorFloat.INSTANCE;
      }

      @Override
      public Integer getMaxLength() {
        return dim;
      }

      @Override
      public Integer getScale() {
        return null;
      }

      @Override
      public org.apache.phoenix.schema.SortOrder getSortOrder() {
        return org.apache.phoenix.schema.SortOrder.getDefault();
      }
    };
  }

  @Test
  public void testTwoPhaseCoarseRescoreOrderAndRecallEquivalence() throws Exception {
    int dim = 4;
    byte[] cf = org.apache.hadoop.hbase.util.Bytes.toBytes("0");
    byte[] cq = org.apache.hadoop.hbase.util.Bytes.toBytes("V");
    org.apache.phoenix.expression.KeyValueColumnExpression colExpr =
      new org.apache.phoenix.expression.KeyValueColumnExpression(createDatum(dim), cf, cq);

    float[] queryVec = new float[] { 0.5f, 0.5f, 0.5f, 0.5f };
    LiteralExpression queryExpr = LiteralExpression.newConstant(queryVec,
      org.apache.phoenix.schema.types.PVectorFloat.INSTANCE);
    org.apache.phoenix.expression.function.L2DistanceFunction distFunc =
      new org.apache.phoenix.expression.function.L2DistanceFunction(
        Arrays.asList(colExpr, queryExpr));
    OrderByExpression ob =
      OrderByExpression.createByCheckIfOrderByReverse(distFunc, false, true, false);

    java.util.Random rng = new java.util.Random(12345);
    List<org.apache.phoenix.schema.tuple.Tuple> tuples = new java.util.ArrayList<>();
    for (int i = 0; i < 50; i++) {
      float[] v = new float[dim];
      for (int d = 0; d < dim; d++) {
        v[d] = rng.nextFloat();
      }
      byte[] rowKey = org.apache.hadoop.hbase.util.Bytes.toBytes(String.format("row_%03d", i));
      org.apache.hadoop.hbase.Cell cell = org.apache.phoenix.util.PhoenixKeyValueUtil.newKeyValue(
        rowKey, cf, cq, 0L, org.apache.phoenix.schema.types.PVectorFloat.INSTANCE.toBytes(v));
      tuples.add(new org.apache.phoenix.schema.tuple.SingleKeyValueTuple(cell));
    }

    int limit = 5;
    // Single-phase
    OrderedResultIterator singlePhase = new OrderedResultIterator(
      new MaterializedResultIterator(tuples), Arrays.asList(ob), limit, false, 1.0);
    List<String> singleResults = new java.util.ArrayList<>();
    for (org.apache.phoenix.schema.tuple.Tuple t = singlePhase.next(); t != null; t =
      singlePhase.next()) {
      org.apache.hadoop.hbase.io.ImmutableBytesWritable key =
        new org.apache.hadoop.hbase.io.ImmutableBytesWritable();
      t.getKey(key);
      singleResults.add(
        org.apache.hadoop.hbase.util.Bytes.toString(key.get(), key.getOffset(), key.getLength()));
    }

    // Two-phase
    OrderedResultIterator twoPhase = new OrderedResultIterator(
      new MaterializedResultIterator(tuples), Arrays.asList(ob), limit, true, 3.0);
    List<String> twoResults = new java.util.ArrayList<>();
    for (org.apache.phoenix.schema.tuple.Tuple t = twoPhase.next(); t != null; t =
      twoPhase.next()) {
      org.apache.hadoop.hbase.io.ImmutableBytesWritable key =
        new org.apache.hadoop.hbase.io.ImmutableBytesWritable();
      t.getKey(key);
      twoResults.add(
        org.apache.hadoop.hbase.util.Bytes.toString(key.get(), key.getOffset(), key.getLength()));
    }

    org.junit.Assert.assertEquals(5, singleResults.size());
    org.junit.Assert.assertEquals(singleResults, twoResults);
  }

  @Test
  public void testTwoPhaseCoarsePhaseCandidateMetrics() throws Exception {
    int dim = 128;
    byte[] cf = org.apache.hadoop.hbase.util.Bytes.toBytes("0");
    byte[] cq = org.apache.hadoop.hbase.util.Bytes.toBytes("V");
    org.apache.phoenix.expression.KeyValueColumnExpression colExpr =
      new org.apache.phoenix.expression.KeyValueColumnExpression(createDatum(dim), cf, cq);

    float[] queryVec = new float[dim];
    java.util.Arrays.fill(queryVec, 0.0f);
    LiteralExpression queryExpr = LiteralExpression.newConstant(queryVec,
      org.apache.phoenix.schema.types.PVectorFloat.INSTANCE);
    org.apache.phoenix.expression.function.L2DistanceFunction distFunc =
      new org.apache.phoenix.expression.function.L2DistanceFunction(
        Arrays.asList(colExpr, queryExpr));
    OrderByExpression ob =
      OrderByExpression.createByCheckIfOrderByReverse(distFunc, false, true, false);

    java.util.Random rng = new java.util.Random(999);
    List<org.apache.phoenix.schema.tuple.Tuple> tuples = new java.util.ArrayList<>();
    for (int i = 0; i < 500; i++) {
      float[] v = new float[dim];
      for (int d = 0; d < dim; d++) {
        v[d] = rng.nextFloat() * 10.0f;
      }
      byte[] rowKey = org.apache.hadoop.hbase.util.Bytes.toBytes(String.format("row_%04d", i));
      org.apache.hadoop.hbase.Cell cell = org.apache.phoenix.util.PhoenixKeyValueUtil.newKeyValue(
        rowKey, cf, cq, 0L, org.apache.phoenix.schema.types.PVectorFloat.INSTANCE.toBytes(v));
      tuples.add(new org.apache.phoenix.schema.tuple.SingleKeyValueTuple(cell));
    }

    int limit = 10;
    OrderedResultIterator twoPhase = new OrderedResultIterator(
      new MaterializedResultIterator(tuples), Arrays.asList(ob), limit, true, 3.0);

    int count = 0;
    for (org.apache.phoenix.schema.tuple.Tuple t = twoPhase.next(); t != null; t =
      twoPhase.next()) {
      count++;
    }
    org.junit.Assert.assertEquals(limit, count);
    org.junit.Assert.assertEquals(500, twoPhase.getCoarsePhaseCandidatesConsidered());
    org.junit.Assert.assertEquals(Integer.valueOf(30), twoPhase.getCoarseLimit());
  }
}
