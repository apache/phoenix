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

import java.util.List;
import org.apache.hadoop.hbase.client.RegionInfo;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.phoenix.expression.Expression;
import org.apache.phoenix.expression.OrderByExpression;
import org.apache.phoenix.expression.function.DistanceFunction;
import org.apache.phoenix.schema.SortOrder;
import org.apache.phoenix.schema.tuple.Tuple;
import org.apache.phoenix.schema.types.PDouble;

import org.apache.phoenix.thirdparty.com.google.common.collect.MinMaxPriorityQueue;

/**
 * Server side top-N result iterator optimizing single ascending distance ordering via bounded
 * evaluation. Tracks the distances of the best {@code limit} candidates and supplies the worst of
 * them as an upper bound to the distance function, enabling early termination during component
 * accumulation. Candidates exceeding the upper bound are pruned from the sort queue. Every
 * candidate that is retained completes its evaluation and is scored exactly, so the region's top-N
 * is exact.
 */
public class VectorDistanceOrderedResultIterator extends OrderedResultIterator {

  private final DistanceFunction distance;
  private final MinMaxPriorityQueue<Double> pool;
  private final int poolSize;

  public VectorDistanceOrderedResultIterator(ResultIterator delegate,
    List<OrderByExpression> orderByExpressions, boolean spoolingEnabled, long thresholdBytes,
    int limit, int estimatedRowSize, long pageSizeMs, Scan scan, RegionInfo regionInfo) {
    super(delegate, orderByExpressions, spoolingEnabled, thresholdBytes, limit, null,
      estimatedRowSize, pageSizeMs, scan, regionInfo);
    this.distance = (DistanceFunction) orderByExpressions.get(0).getExpression();
    this.poolSize = limit;
    this.pool = MinMaxPriorityQueue.<Double> create();
  }

  /** Evaluates whether the ORDER BY clause specifies a single ascending distance expression. */
  public static boolean appliesTo(List<OrderByExpression> orderByExpressions) {
    return orderByExpressions.size() == 1 && orderByExpressions.get(0).isAscending()
      && orderByExpressions.get(0).getExpression() instanceof DistanceFunction;
  }

  @Override
  protected ImmutableBytesWritable[] evaluateSortKeys(Tuple result, List<Expression> expressions) {
    double bound = pool.size() >= poolSize ? pool.peekLast() : Double.MAX_VALUE;
    distance.setDistanceUpperBound(bound);
    ImmutableBytesWritable sortKey = new ImmutableBytesWritable();
    if (!distance.evaluate(result, sortKey) || sortKey.getLength() == 0) {
      return new ImmutableBytesWritable[] { null };
    }
    double d =
      PDouble.INSTANCE.getCodec().decodeDouble(sortKey.get(), sortKey.getOffset(), SortOrder.ASC);
    if (d == Double.MAX_VALUE && bound != Double.MAX_VALUE) {
      // Candidate exceeds current distance upper bound; prune from sort queue
      return null;
    }
    pool.add(d);
    if (pool.size() > poolSize) {
      pool.pollLast();
    }
    return new ImmutableBytesWritable[] { sortKey };
  }
}
