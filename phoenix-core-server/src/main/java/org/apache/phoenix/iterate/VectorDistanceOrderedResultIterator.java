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
 * A server-side top N iterator for an ORDER BY of one ascending distance function. The iterator
 * keeps the distances of the best {@code limit} candidates. It gives the worst of these distances
 * to the distance function as an upper bound, so the function can stop early for a far candidate.
 * The iterator drops a candidate that exceeds the bound. Each candidate that it keeps gets an exact
 * distance, so the top N of the region is exact.
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

  /** Returns true if the ORDER BY has one expression, an ascending distance function. */
  public static boolean appliesTo(List<OrderByExpression> orderByExpressions) {
    return orderByExpressions.size() == 1 && orderByExpressions.get(0).isAscending()
      && orderByExpressions.get(0).getExpression() instanceof DistanceFunction;
  }

  @Override
  protected ImmutableBytesWritable[] evaluateSortKeys(Tuple result, List<Expression> expressions) {
    if (poolSize == 0) {
      // LIMIT 0 keeps no rows, and an empty pool has no worst distance to use as a bound
      return null;
    }
    double bound = pool.size() >= poolSize ? pool.peekLast() : Double.MAX_VALUE;
    distance.setDistanceUpperBound(bound);
    ImmutableBytesWritable sortKey = new ImmutableBytesWritable();
    if (!distance.evaluate(result, sortKey) || sortKey.getLength() == 0) {
      return new ImmutableBytesWritable[] { null };
    }
    double d =
      PDouble.INSTANCE.getCodec().decodeDouble(sortKey.get(), sortKey.getOffset(), SortOrder.ASC);
    if (d == Double.MAX_VALUE && bound != Double.MAX_VALUE) {
      // The candidate exceeds the bound, so it cannot enter the top N
      return null;
    }
    pool.add(d);
    if (pool.size() > poolSize) {
      pool.pollLast();
    }
    return new ImmutableBytesWritable[] { sortKey };
  }
}
