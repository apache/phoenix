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
import java.util.List;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.phoenix.compile.ExplainPlanAttributes.ExplainPlanAttributesBuilder;
import org.apache.phoenix.expression.Expression;
import org.apache.phoenix.expression.OrderByExpression;
import org.apache.phoenix.expression.function.DistanceFunction;
import org.apache.phoenix.schema.tuple.Tuple;

/**
 * ResultIterator that does a merge sort on the list of iterators provided, returning the rows
 * ordered by the OrderByExpression. The input iterators must be ordered by the OrderByExpression.
 */
public class MergeSortTopNResultIterator extends MergeSortResultIterator {

  private final int limit;
  private int count = 0;
  private int offsetCount = 0;
  private final List<OrderByExpression> orderByColumns;
  private final ImmutableBytesWritable ptr1 = new ImmutableBytesWritable();
  private final ImmutableBytesWritable ptr2 = new ImmutableBytesWritable();
  private final int offset;

  public MergeSortTopNResultIterator(ResultIterators iterators, Integer limit, Integer offset,
    List<OrderByExpression> orderByColumns) {
    super(iterators);
    this.limit = limit == null ? -1 : limit;
    this.offset = offset == null ? -1 : offset;
    this.orderByColumns = orderByColumns;
  }

  @Override
  protected int compare(Tuple t1, Tuple t2) {
    for (int i = 0; i < orderByColumns.size(); i++) {
      OrderByExpression order = orderByColumns.get(i);
      Expression orderExpr = order.getExpression();
      boolean isNull1 = !orderExpr.evaluate(t1, ptr1) || ptr1.getLength() == 0;
      boolean isNull2 = !orderExpr.evaluate(t2, ptr2) || ptr2.getLength() == 0;
      if (isNull1 && isNull2) {
        continue;
      } else if (isNull1) {
        return order.isNullsLast() ? 1 : -1;
      } else if (isNull2) {
        return order.isNullsLast() ? -1 : 1;
      }
      int cmp = ptr1.compareTo(ptr2);
      if (cmp == 0) {
        continue;
      }
      return order.isAscending() ? cmp : -cmp;
    }
    return 0;
  }

  @Override
  public Tuple peek() throws SQLException {
    while (offsetCount < offset) {
      if (super.next() == null) {
        return null;
      }
      offsetCount++;
    }
    if (limit >= 0 && count >= limit) {
      return null;
    }
    return super.peek();
  }

  @Override
  public Tuple next() throws SQLException {
    while (offsetCount < offset) {
      if (super.next() == null) {
        return null;
      }
      offsetCount++;
    }
    if (limit >= 0 && count++ >= limit) {
      return null;
    }
    return super.next();
  }

  private boolean isVectorSearch() {
    if (limit <= 0 || orderByColumns == null || orderByColumns.size() != 1) {
      return false;
    }
    OrderByExpression orderByExpression = orderByColumns.get(0);
    if (!orderByExpression.isAscending()) {
      return false;
    }
    return orderByExpression.getExpression() instanceof DistanceFunction;
  }

  @Override
  public void explain(List<String> planSteps) {
    resultIterators.explain(planSteps);
    if (isVectorSearch()) {
      planSteps.add("CLIENT MERGE SORT TOP-" + limit);
      if (offset > 0) {
        planSteps.add("CLIENT OFFSET " + offset);
      }
    } else {
      planSteps.add("CLIENT MERGE SORT");
      if (offset > 0) {
        planSteps.add("CLIENT OFFSET " + offset);
      }
      if (limit > 0) {
        planSteps.add("CLIENT LIMIT " + limit);
      }
    }
  }

  @Override
  public void explain(List<String> planSteps,
    ExplainPlanAttributesBuilder explainPlanAttributesBuilder) {
    resultIterators.explain(planSteps, explainPlanAttributesBuilder);
    if (isVectorSearch()) {
      // The top-K limit is part of the merge sort step itself, so no separate CLIENT LIMIT step
      // is emitted.
      String clientSortAlgo = "CLIENT MERGE SORT TOP-" + limit;
      explainPlanAttributesBuilder.setVectorSearch(true);
      explainPlanAttributesBuilder.setClientSortAlgo(clientSortAlgo);
      explainPlanAttributesBuilder.setClientRowLimit(limit);
      planSteps.add(clientSortAlgo);
      explainPlanAttributesBuilder.addClientStep(clientSortAlgo);
      if (offset > 0) {
        explainPlanAttributesBuilder.setClientOffset(offset);
        String step = "CLIENT OFFSET " + offset;
        planSteps.add(step);
        explainPlanAttributesBuilder.addClientStep(step);
      }
    } else {
      explainPlanAttributesBuilder.setClientSortAlgo("CLIENT MERGE SORT");
      planSteps.add("CLIENT MERGE SORT");
      explainPlanAttributesBuilder.addClientStep("CLIENT MERGE SORT");
      if (offset > 0) {
        explainPlanAttributesBuilder.setClientOffset(offset);
        String step = "CLIENT OFFSET " + offset;
        planSteps.add(step);
        explainPlanAttributesBuilder.addClientStep(step);
      }
      if (limit > 0) {
        explainPlanAttributesBuilder.setClientRowLimit(limit);
        String step = "CLIENT LIMIT " + limit;
        planSteps.add(step);
        explainPlanAttributesBuilder.addClientStep(step);
      }
    }
  }

  @Override
  public String toString() {
    return "MergeSortTopNResultIterator [limit=" + limit + ", count=" + count + ", orderByColumns="
      + orderByColumns + ", ptr1=" + ptr1 + ", ptr2=" + ptr2 + ",offset=" + offset + "]";
  }
}
