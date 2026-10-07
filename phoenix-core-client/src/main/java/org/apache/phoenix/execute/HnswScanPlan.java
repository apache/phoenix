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
package org.apache.phoenix.execute;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.phoenix.compile.ExplainPlan;
import org.apache.phoenix.compile.OrderByCompiler.OrderBy;
import org.apache.phoenix.compile.RowProjector;
import org.apache.phoenix.compile.StatementContext;
import org.apache.phoenix.coprocessorclient.BaseScannerRegionObserverConstants;
import org.apache.phoenix.iterate.ParallelIteratorFactory;
import org.apache.phoenix.optimize.Cost;
import org.apache.phoenix.parse.FilterableStatement;
import org.apache.phoenix.parse.HintNode.Hint;
import org.apache.phoenix.query.QueryServices;
import org.apache.phoenix.query.QueryServicesOptions;
import org.apache.phoenix.schema.PTable;
import org.apache.phoenix.schema.TableRef;
import org.apache.phoenix.util.CostUtil;

import org.apache.phoenix.thirdparty.com.google.common.base.Optional;

/**
 * Query plan for executing nearest neighbor vector searches using an HNSW index. Scans the data
 * table restricted to candidate rows identified by region local graph searches, with server side
 * ranking by exact distance.
 */
public class HnswScanPlan extends ScanPlan {
  private final PTable index;
  private final int candidates;

  public HnswScanPlan(StatementContext context, FilterableStatement statement, TableRef dataTable,
    RowProjector projector, Integer limit, Integer offset, OrderBy orderBy,
    ParallelIteratorFactory parallelIteratorFactory, boolean allowPageFilter,
    Optional<byte[]> rowOffset, PTable index) throws SQLException {
    super(context, statement, dataTable, projector, limit, offset, orderBy, parallelIteratorFactory,
      allowPageFilter, null, rowOffset);
    this.index = index;
    int efSearch = VectorIndexScanPlan.getHintInt(statement.getHint(), Hint.HNSW_EF_SEARCH,
      context.getConnection().getQueryServices().getProps()
        .getInt(QueryServices.HNSW_EF_SEARCH_ATTRIB, QueryServicesOptions.DEFAULT_HNSW_EF_SEARCH));
    int target = limit + (offset != null ? offset : 0);
    this.candidates = Math.max(efSearch, target);
    float[] query = queryVector(orderBy);
    // Serialized search attribute layout: candidate count, target pass count, and float query
    // vector
    byte[] search = new byte[Bytes.SIZEOF_INT * (2 + query.length)];
    Bytes.putInt(search, 0, candidates);
    Bytes.putInt(search, Bytes.SIZEOF_INT, target);
    for (int i = 0; i < query.length; i++) {
      Bytes.putFloat(search, Bytes.SIZEOF_INT * (2 + i), query[i]);
    }
    context.getScan().setAttribute(BaseScannerRegionObserverConstants.HNSW_SEARCH_INDEX,
      Bytes.toBytes(index.getName().getString()));
    context.getScan().setAttribute(BaseScannerRegionObserverConstants.HNSW_SEARCH_QUERY, search);
  }

  /** Returns the query vector from the ORDER BY clause, or null if absent. */
  public static float[] queryVector(OrderBy orderBy) {
    return VectorIndexScanPlan.getQueryVector(orderBy);
  }

  /** Returns the target HNSW index table. */
  public PTable getIndex() {
    return index;
  }

  @Override
  public Cost getCost() {
    return CostUtil.estimateLookupCost(candidates, getTableRef().getTable());
  }

  @Override
  public ExplainPlan getExplainPlan() throws SQLException {
    ExplainPlan plan = super.getExplainPlan();
    List<String> steps = new ArrayList<>(plan.getPlanSteps());
    steps.add(1,
      "    SERVER HNSW SEARCH " + index.getName().getString() + " (" + candidates + " CANDIDATES)");
    return new ExplainPlan(steps, plan.getPlanStepsAsAttributes());
  }
}
