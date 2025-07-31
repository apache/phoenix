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
import java.util.Collections;
import java.util.List;
import java.util.Set;
import org.apache.hadoop.hbase.HRegionLocation;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.phoenix.compile.ExplainPlan;
import org.apache.phoenix.compile.ExplainPlanAttributes.ExplainPlanAttributesBuilder;
import org.apache.phoenix.compile.OrderByCompiler.OrderBy;
import org.apache.phoenix.compile.RowProjector;
import org.apache.phoenix.compile.StatementContext;
import org.apache.phoenix.execute.visitor.QueryPlanVisitor;
import org.apache.phoenix.iterate.ParallelScanGrouper;
import org.apache.phoenix.iterate.ResultIterator;
import org.apache.phoenix.iterate.SegmentResultIterator;
import org.apache.phoenix.parse.FilterableStatement;
import org.apache.phoenix.query.ConnectionQueryServices;
import org.apache.phoenix.query.QueryServices;
import org.apache.phoenix.query.QueryServicesOptions;
import org.apache.phoenix.schema.TableRef;

import org.apache.phoenix.thirdparty.com.google.common.collect.ImmutableSet;

/**
 * Client-side plan that returns region information for segment instead of executing a server scan.
 */
public class SegmentInfoPlan extends ClientProcessingPlan {

  private final Integer totalSegmentsValue;
  private final Set<TableRef> sourceTables;

  public SegmentInfoPlan(StatementContext context, FilterableStatement statement, TableRef tableRef,
    RowProjector projector, Integer totalSegmentsValue) {
    super(context, statement, tableRef, projector, null, null, null, OrderBy.EMPTY_ORDER_BY, null);
    this.totalSegmentsValue = totalSegmentsValue;
    this.sourceTables = ImmutableSet.of(tableRef);
  }

  @Override
  public Set<TableRef> getSourceRefs() {
    return sourceTables;
  }

  @Override
  public boolean isApplicable() {
    return true;
  }

  @Override
  public ResultIterator iterator(ParallelScanGrouper scanGrouper, Scan scan) throws SQLException {
    ConnectionQueryServices services = context.getConnection().getQueryServices();
    byte[] tableName = table.getTable().getPhysicalName().getBytes();

    int queryTimeout = context.getConnection().getQueryServices().getProps().getInt(
      QueryServices.THREAD_TIMEOUT_MS_ATTRIB, QueryServicesOptions.DEFAULT_THREAD_TIMEOUT_MS);
    int totalSegments = totalSegmentsValue;

    List<HRegionLocation> regions = services.getAllTableRegions(tableName, queryTimeout);

    return new SegmentResultIterator(regions, totalSegments);
  }

  @Override
  public <T> T accept(QueryPlanVisitor<T> visitor) {
    return visitor.defaultReturn(this);
  }

  @Override
  public ExplainPlan getExplainPlan() throws SQLException {
    List<String> planSteps = Collections.singletonList("CLIENT REGION SCAN");
    ExplainPlanAttributesBuilder builder = new ExplainPlanAttributesBuilder();
    return new ExplainPlan(planSteps, builder.build());
  }
}
