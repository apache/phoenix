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
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import org.apache.phoenix.compile.QueryPlan;
import org.apache.phoenix.compile.StatementContext;
import org.apache.phoenix.execute.HashJoinPlan;
import org.apache.phoenix.execute.HashJoinPlan.SubPlan;
import org.apache.phoenix.execute.VectorIndexScanPlan;
import org.apache.phoenix.schema.PTable;
import org.apache.phoenix.schema.PTableType;
import org.apache.phoenix.schema.TableRef;
import org.junit.Test;

/** Unit tests for vector index plan comparison and precedence rules in query optimization. */
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
    // Non-vector plans return neutral comparison to preserve default ordering
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
    // Under data table hint, vector index plans sort after all other candidates consistently
    // without cyclic dependencies.
    QueryPlan vector = vectorScan(false);
    QueryPlan index = mock(QueryPlan.class);
    QueryPlan data = mock(QueryPlan.class);
    assertTrue(QueryOptimizer.compareVectorPlans(index, vector, DATA_HINTED) < 0);
    assertTrue(QueryOptimizer.compareVectorPlans(data, vector, DATA_HINTED) < 0);
    assertEquals(0, QueryOptimizer.compareVectorPlans(index, data, DATA_HINTED));
  }

  private static QueryPlan planOver(PTableType type, Long estimatedRows) throws Exception {
    QueryPlan plan = mock(QueryPlan.class);
    PTable table = mock(PTable.class);
    when(table.getType()).thenReturn(type);
    TableRef tableRef = mock(TableRef.class);
    when(tableRef.getTable()).thenReturn(table);
    when(plan.getTableRef()).thenReturn(tableRef);
    when(plan.getEstimatedRowsToScan()).thenReturn(estimatedRows);
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
    // Explicit data table hint overrides secondary index filter first precedence
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
}
