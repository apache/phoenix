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

import java.sql.SQLException;
import java.util.Collections;
import java.util.List;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.phoenix.compile.ExplainPlan;
import org.apache.phoenix.compile.ExplainPlanAttributes;
import org.apache.phoenix.compile.ExplainPlanAttributes.ExplainPlanAttributesBuilder;
import org.apache.phoenix.compile.QueryPlan;
import org.apache.phoenix.compile.StatementContext;
import org.apache.phoenix.monitoring.OverAllQueryMetrics;
import org.apache.phoenix.monitoring.ReadMetricQueue;
import org.apache.phoenix.query.KeyRange;
import org.apache.phoenix.util.ClientUtil;

import org.apache.phoenix.thirdparty.com.google.common.collect.Lists;

/**
 * Create a union ResultIterators
 */
public class UnionResultIterators implements ResultIterators {
  private final List<KeyRange> splits;
  private final List<List<Scan>> scans;
  private final List<PeekingResultIterator> iterators;
  private final List<ReadMetricQueue> readMetricsList;
  private final List<OverAllQueryMetrics> overAllQueryMetricsList;
  private boolean closed;
  private final StatementContext parentStmtCtx;
  private final List<QueryPlan> plans;

  public UnionResultIterators(List<QueryPlan> plans, StatementContext parentStmtCtx)
    throws SQLException {
    this.parentStmtCtx = parentStmtCtx;
    this.plans = plans;
    int nPlans = plans.size();
    iterators = Lists.newArrayListWithExpectedSize(nPlans);
    splits = Lists.newArrayListWithExpectedSize(nPlans * 30);
    scans = Lists.newArrayListWithExpectedSize(nPlans * 10);
    readMetricsList = Lists.newArrayListWithCapacity(nPlans);
    overAllQueryMetricsList = Lists.newArrayListWithCapacity(nPlans);
    for (QueryPlan plan : plans) {
      parentStmtCtx.addSubStatementContext(plan.getContext());
      readMetricsList.add(plan.getContext().getReadMetricsQueue());
      overAllQueryMetricsList.add(plan.getContext().getOverallQueryMetrics());
      iterators.add(LookAheadResultIterator.wrap(plan.iterator()));
      splits.addAll(plan.getSplits());
      scans.addAll(plan.getScans());
    }
  }

  @Override
  public List<KeyRange> getSplits() {
    if (splits == null) return Collections.emptyList();
    else return splits;
  }

  @Override
  public void close() throws SQLException {
    if (!closed) {
      closed = true;
      SQLException toThrow = null;
      try {
        if (iterators != null) {
          for (int index = 0; index < iterators.size(); index++) {
            PeekingResultIterator iterator = iterators.get(index);
            try {
              iterator.close();
            } catch (Exception e) {
              if (toThrow == null) {
                toThrow = ClientUtil.parseServerException(e);
              } else {
                toThrow.setNextException(ClientUtil.parseServerException(e));
              }
            }
          }
        }
      } catch (Exception e) {
        toThrow = ClientUtil.parseServerException(e);
      } finally {
        setMetricsInParentContext();
        if (toThrow != null) {
          throw toThrow;
        }
      }
    }
  }

  private void setMetricsInParentContext() {
    ReadMetricQueue parentCtxReadMetrics = parentStmtCtx.getReadMetricsQueue();
    for (ReadMetricQueue readMetrics : readMetricsList) {
      parentCtxReadMetrics.combineReadMetrics(readMetrics, true);
    }
    OverAllQueryMetrics parentCtxQueryMetrics = parentStmtCtx.getOverallQueryMetrics();
    for (OverAllQueryMetrics metric : overAllQueryMetricsList) {
      parentCtxQueryMetrics.combine(metric);
      metric.reset();
    }
  }

  @Override
  public List<List<Scan>> getScans() {
    if (scans == null) return Collections.emptyList();
    else return scans;
  }

  @Override
  public int size() {
    return scans.size();
  }

  @Override
  public void explain(List<String> planSteps) {
    try {
      explainBranches(plans, planSteps, null);
    } catch (SQLException e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public List<PeekingResultIterator> getIterators() throws SQLException {
    return iterators;
  }

  @Override
  public void explain(List<String> planSteps,
    ExplainPlanAttributesBuilder explainPlanAttributesBuilder) {
    try {
      explainBranches(plans, planSteps, explainPlanAttributesBuilder);
    } catch (SQLException e) {
      throw new RuntimeException(e);
    }
  }

  /**
   * Single source of truth for composing a union's branches into the explain output. Each branch is
   * rendered from its own {@link QueryPlan#getExplainPlan()} (rather than from its iterator) so the
   * full sub-plan structure of each branch is preserved, and so explaining a union does not trigger
   * sub-plan execution. The branch text steps are indented one level and, when a builder is
   * supplied, the branches' structured attributes are collected onto the builder's {@code subPlans}
   * list.
   */
  public static void explainBranches(List<QueryPlan> plans, List<String> planSteps,
    ExplainPlanAttributesBuilder builder) throws SQLException {
    List<ExplainPlanAttributes> subPlans =
      builder == null ? null : Lists.newArrayListWithExpectedSize(plans.size());
    for (QueryPlan plan : plans) {
      ExplainPlan explainPlan = plan.getExplainPlan();
      for (String step : explainPlan.getPlanSteps()) {
        planSteps.add("    " + step);
      }
      if (subPlans != null) {
        ExplainPlanAttributes attributes = explainPlan.getPlanStepsAsAttributes();
        if (attributes != null && attributes != ExplainPlanAttributes.getDefaultExplainPlan()) {
          subPlans.add(attributes);
        }
      }
    }
    if (subPlans != null && !subPlans.isEmpty()) {
      builder.setSubPlans(subPlans);
    }
  }
}
