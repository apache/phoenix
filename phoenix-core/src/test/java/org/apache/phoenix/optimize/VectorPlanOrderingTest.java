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
package org.apache.phoenix.optimize;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.phoenix.compile.QueryPlan;
import org.apache.phoenix.compile.ScanRanges;
import org.apache.phoenix.compile.StatementContext;
import org.apache.phoenix.execute.HashJoinPlan;
import org.apache.phoenix.execute.HashJoinPlan.SubPlan;
import org.apache.phoenix.execute.VectorIndexScanPlan;
import org.apache.phoenix.jdbc.PhoenixConnection;
import org.apache.phoenix.query.ConnectionQueryServices;
import org.apache.phoenix.query.KeyRange;
import org.apache.phoenix.schema.PNameFactory;
import org.apache.phoenix.schema.PTable;
import org.apache.phoenix.schema.PTableType;
import org.apache.phoenix.schema.TableRef;
import org.apache.phoenix.schema.stats.GuidePostsInfo;
import org.junit.Test;

/** Unit tests for the precedence rules that order vector index plans in the query optimizer. */
public class VectorPlanOrderingTest {

  private static final int NO_HINT = 1;
  private static final int DATA_HINTED = -1;

  private static VectorIndexScanPlan vectorScan(boolean joinsEveryCandidate) {
    VectorIndexScanPlan plan = mock(VectorIndexScanPlan.class);
    StatementContext context = mock(StatementContext.class);
    when(context.isUncoveredIndex()).thenReturn(joinsEveryCandidate);
    when(plan.getContext()).thenReturn(context);
    return plan;
  }

  private static QueryPlan deferredProjection() {
    HashJoinPlan plan = mock(HashJoinPlan.class);
    SubPlan subPlan = mock(SubPlan.class);
    QueryPlan scan = vectorScan(false);
    when(subPlan.getInnerPlan()).thenReturn(scan);
    when(plan.getSubPlans()).thenReturn(new SubPlan[] { subPlan });
    return plan;
  }

  @Test
  public void testPlansWithoutVectorIndexAreTied() {
    // Plans that do not read a vector index compare as equal, so the default order stays.
    assertEquals(0,
      QueryOptimizer.compareVectorPlans(mock(QueryPlan.class), mock(QueryPlan.class), NO_HINT));
    assertEquals(0,
      QueryOptimizer.compareVectorPlans(mock(QueryPlan.class), mock(QueryPlan.class), DATA_HINTED));
  }

  @Test
  public void testVectorIndexPlanPrecedesOtherPlansUnlessDataHinted() {
    QueryPlan vector = vectorScan(false);
    QueryPlan other = mock(QueryPlan.class);
    assertTrue(QueryOptimizer.compareVectorPlans(vector, other, NO_HINT) < 0);
    assertTrue(QueryOptimizer.compareVectorPlans(other, vector, NO_HINT) > 0);
    assertTrue(QueryOptimizer.compareVectorPlans(vector, other, DATA_HINTED) > 0);
    assertTrue(QueryOptimizer.compareVectorPlans(other, vector, DATA_HINTED) < 0);
  }

  @Test
  public void testVectorPlansOrderedByDataTableReads() {
    QueryPlan covered = vectorScan(false);
    QueryPlan deferred = deferredProjection();
    QueryPlan joined = vectorScan(true);
    assertEquals(0, VectorSearchUtil.getLookupRank(covered));
    assertEquals(1, VectorSearchUtil.getLookupRank(deferred));
    assertEquals(2, VectorSearchUtil.getLookupRank(joined));
    List<QueryPlan> plans = new ArrayList<>(Arrays.asList(joined, deferred, covered));
    plans.sort((p1, p2) -> QueryOptimizer.compareVectorPlans(p1, p2, NO_HINT));
    assertEquals(Arrays.asList(covered, deferred, joined), plans);
  }

  @Test
  public void testOrderingIsTransitiveUnderDataHint() {
    // Under the data table hint, vector index plans sort after all other candidates. The order
    // must stay transitive, without cycles.
    QueryPlan vector = vectorScan(false);
    QueryPlan index = mock(QueryPlan.class);
    QueryPlan data = mock(QueryPlan.class);
    assertTrue(QueryOptimizer.compareVectorPlans(index, vector, DATA_HINTED) < 0);
    assertTrue(QueryOptimizer.compareVectorPlans(data, vector, DATA_HINTED) < 0);
    assertEquals(0, QueryOptimizer.compareVectorPlans(index, data, DATA_HINTED));
  }

  private static QueryPlan planOver(PTableType type, Long estimatedRows) throws Exception {
    return planOver(type, estimatedRows, Collections.emptyList(), null);
  }

  private static final KeyRange POINT = KeyRange.getKeyRange(Bytes.toBytes("u1"));

  /**
   * Returns a mock plan whose key ranges bind the leading primary key columns, one column for each
   * slot of {@code bound}. The table statistics count {@code tableRows} rows. If {@code tableRows}
   * is null, the table has no statistics.
   */
  private static QueryPlan planOver(PTableType type, Long estimatedRows, List<List<KeyRange>> bound,
    Long tableRows) throws Exception {
    QueryPlan plan = mock(QueryPlan.class);
    PTable table = mock(PTable.class);
    when(table.getType()).thenReturn(type);
    when(table.getPhysicalName()).thenReturn(PNameFactory.newName("T"));
    TableRef tableRef = mock(TableRef.class);
    when(tableRef.getTable()).thenReturn(table);
    when(plan.getTableRef()).thenReturn(tableRef);
    when(plan.getEstimatedRowsToScan()).thenReturn(estimatedRows);
    ScanRanges ranges = mock(ScanRanges.class);
    when(ranges.getBoundPkColumnCount()).thenReturn(bound.size());
    when(ranges.getRanges()).thenReturn(bound);
    when(ranges.getSlotSpans()).thenReturn(new int[bound.size()]);
    ConnectionQueryServices services = mock(ConnectionQueryServices.class);
    when(services.getTableStats(any())).thenReturn(tableRows == null
      ? GuidePostsInfo.NO_GUIDEPOST
      : new GuidePostsInfo(Arrays.asList(1L, 1L), new ImmutableBytesWritable(new byte[0]),
        Arrays.asList(tableRows / 2, tableRows - tableRows / 2), 0, 2, Arrays.asList(0L, 0L)));
    PhoenixConnection connection = mock(PhoenixConnection.class);
    when(connection.getQueryServices()).thenReturn(services);
    StatementContext context = mock(StatementContext.class);
    when(context.getScanRanges()).thenReturn(ranges);
    when(context.getConnection()).thenReturn(connection);
    when(plan.getContext()).thenReturn(context);
    return plan;
  }

  @Test
  public void testSelectiveRegularIndexIsEvaluatedFirst() throws Exception {
    QueryPlan data = planOver(PTableType.TABLE, 10_000L);
    QueryPlan selective = planOver(PTableType.INDEX, 10L);
    QueryPlan unselective = planOver(PTableType.INDEX, 9_000L);
    QueryPlan vector = vectorScan(false);
    List<QueryPlan> plans = Arrays.asList(data, selective, unselective, vector);
    Set<QueryPlan> filterFirst = VectorSearchUtil.getFilterFirstPlans(plans, data);
    assertEquals(Collections.singleton(selective), filterFirst);

    List<QueryPlan> ordered = new ArrayList<>(Arrays.asList(unselective, vector, data, selective));
    ordered.sort((p1, p2) -> QueryOptimizer.compareVectorPlans(p1, p2, NO_HINT, filterFirst));
    assertEquals(selective, ordered.get(0));
    assertEquals(vector, ordered.get(1));
    // An explicit data table hint removes the filter first precedence of the secondary index
    assertEquals(0, QueryOptimizer.compareVectorPlans(selective, data, DATA_HINTED, filterFirst));
    assertTrue(QueryOptimizer.compareVectorPlans(selective, vector, DATA_HINTED, filterFirst) < 0);
  }

  @Test
  public void testNoFilterFirstWithoutStatisticsOrVectorPlan() throws Exception {
    QueryPlan noStats = planOver(PTableType.TABLE, null);
    QueryPlan index = planOver(PTableType.INDEX, 1L);
    assertTrue(VectorSearchUtil
      .getFilterFirstPlans(Arrays.asList(noStats, index, vectorScan(false)), noStats).isEmpty());
    QueryPlan data = planOver(PTableType.TABLE, 10_000L);
    assertTrue(VectorSearchUtil.getFilterFirstPlans(Arrays.asList(data, index), data).isEmpty());
  }

  @Test
  public void testDataPlanBindingPrimaryKeyIsEvaluatedFirst() throws Exception {
    // WHERE USER_ID = ? on PK (USER_ID, DOC_ID): the exact range scan ranks before the vector
    // plan. The vector plan probes the posting lists of all users and filters USER_ID after that.
    QueryPlan vector = vectorScan(false);
    QueryPlan bound = planOver(PTableType.TABLE, null, Arrays.asList(Arrays.asList(POINT)), null);
    Set<QueryPlan> filterFirst =
      VectorSearchUtil.getFilterFirstPlans(Arrays.asList(bound, vector), bound);
    assertEquals(Collections.singleton(bound), filterFirst);
    assertTrue(QueryOptimizer.compareVectorPlans(bound, vector, NO_HINT, filterFirst) < 0);
    assertTrue(QueryOptimizer.compareVectorPlans(bound, vector, DATA_HINTED, filterFirst) < 0);

    // With statistics, the bound range must hold no more than the filter first fraction
    QueryPlan narrow = planOver(PTableType.TABLE, 40L, Arrays.asList(Arrays.asList(POINT)), 1_000L);
    assertEquals(Collections.singleton(narrow),
      VectorSearchUtil.getFilterFirstPlans(Arrays.asList(narrow, vector), narrow));
    QueryPlan wide = planOver(PTableType.TABLE, 600L, Arrays.asList(Arrays.asList(POINT)), 1_000L);
    assertTrue(VectorSearchUtil.getFilterFirstPlans(Arrays.asList(wide, vector), wide).isEmpty());

    // Without statistics, an IN list qualifies because it binds point keys. A range such as
    // USER_ID > ? does not qualify, because it can span most of the table. With statistics, the
    // same range qualifies if it is narrow.
    QueryPlan inList = planOver(PTableType.TABLE, null,
      Arrays.asList(Arrays.asList(POINT, KeyRange.getKeyRange(Bytes.toBytes("u2")))), null);
    assertEquals(Collections.singleton(inList),
      VectorSearchUtil.getFilterFirstPlans(Arrays.asList(inList, vector), inList));
    List<List<KeyRange>> range = Arrays.asList(
      Arrays.asList(KeyRange.getKeyRange(Bytes.toBytes("u1"), false, KeyRange.UNBOUND, false)));
    QueryPlan unestimated = planOver(PTableType.TABLE, null, range, null);
    assertTrue(VectorSearchUtil.getFilterFirstPlans(Arrays.asList(unestimated, vector), unestimated)
      .isEmpty());
    QueryPlan narrowRange = planOver(PTableType.TABLE, 40L, range, 1_000L);
    assertEquals(Collections.singleton(narrowRange),
      VectorSearchUtil.getFilterFirstPlans(Arrays.asList(narrowRange, vector), narrowRange));
    // Equality on the first column after the prefix qualifies, whatever binds the next column
    QueryPlan pointThenRange =
      planOver(PTableType.TABLE, null, Arrays.asList(Arrays.asList(POINT), range.get(0)), null);
    assertEquals(Collections.singleton(pointThenRange),
      VectorSearchUtil.getFilterFirstPlans(Arrays.asList(pointThenRange, vector), pointThenRange));

    // A salt byte that is bound to all buckets does not make a selective key range
    QueryPlan salted = planOver(PTableType.TABLE, null, Arrays.asList(Arrays.asList(POINT)), null);
    when(salted.getContext().getScanRanges().isSalted()).thenReturn(true);
    assertTrue(
      VectorSearchUtil.getFilterFirstPlans(Arrays.asList(salted, vector), salted).isEmpty());
  }
}
