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
import java.util.Collections;
import java.util.List;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.phoenix.cache.VectorCentroidCache;
import org.apache.phoenix.cache.VectorCentroidCache.CachedCentroids;
import org.apache.phoenix.compile.ExplainPlan;
import org.apache.phoenix.compile.ExplainPlanAttributes;
import org.apache.phoenix.compile.ExplainPlanAttributes.ExplainPlanAttributesBuilder;
import org.apache.phoenix.compile.OrderByCompiler.OrderBy;
import org.apache.phoenix.compile.QueryPlan;
import org.apache.phoenix.compile.RowProjector;
import org.apache.phoenix.compile.ScanRanges;
import org.apache.phoenix.compile.StatementContext;
import org.apache.phoenix.expression.Expression;
import org.apache.phoenix.expression.function.DistanceFunction;
import org.apache.phoenix.iterate.ParallelIteratorFactory;
import org.apache.phoenix.jdbc.PhoenixConnection;
import org.apache.phoenix.optimize.Cost;
import org.apache.phoenix.optimize.DistanceMetric;
import org.apache.phoenix.parse.FilterableStatement;
import org.apache.phoenix.parse.HintNode;
import org.apache.phoenix.parse.HintNode.Hint;
import org.apache.phoenix.query.KeyRange;
import org.apache.phoenix.query.QueryServices;
import org.apache.phoenix.query.QueryServicesOptions;
import org.apache.phoenix.schema.PTable;
import org.apache.phoenix.schema.SaltingUtil;
import org.apache.phoenix.schema.TableRef;
import org.apache.phoenix.schema.types.PInteger;
import org.apache.phoenix.util.CostUtil;
import org.apache.phoenix.util.ReadOnlyProps;
import org.apache.phoenix.util.ScanUtil;
import org.apache.phoenix.util.SchemaUtil;

import org.apache.phoenix.thirdparty.com.google.common.base.Optional;

/**
 * Scan plan implementation for IVF vector index queries. Maps query vectors to nearest centroid
 * partitions and scopes the scan to corresponding posting list key ranges across salting and tenant
 * prefixes. Region servers score candidates within targeted partitions and return ranked streams
 * for client side top-K merge sort.
 * <p>
 * Falls back to full index scans when partition pruning cannot be applied (such as untrained
 * indexes, unscoped multitenant queries, or conflicting primary key filter constraints).
 */
public class VectorIndexScanPlan extends ScanPlan {

  private final int centroidCount;
  private final int probeCount;
  private final int[] probeCentroids;

  public VectorIndexScanPlan(StatementContext context, FilterableStatement statement,
    TableRef table, RowProjector projector, Integer limit, Integer offset, OrderBy orderBy,
    ParallelIteratorFactory parallelIteratorFactory, boolean allowPageFilter, QueryPlan dataPlan,
    Optional<byte[]> rowOffset) throws SQLException {
    super(context, statement, table, projector, limit, offset, orderBy, parallelIteratorFactory,
      allowPageFilter, dataPlan, rowOffset);
    PTable index = table.getTable();
    PhoenixConnection connection = context.getConnection();
    ReadOnlyProps props = connection.getQueryServices().getProps();
    HintNode hint = statement.getHint();

    float[] queryVector = getQueryVector(orderBy);
    CachedCentroids centroids = null;
    if (queryVector != null && index.getVectorCentroidGeneration() != null) {
      centroids = VectorCentroidCache.getInstance(connection.getQueryServices().getConfiguration())
        .get(connection, index.getName().getString(), index.getVectorCentroidGeneration(),
          DistanceMetric.fromString(index.getVectorDistanceMetric()));
    }
    this.centroidCount = centroids == null ? 0 : centroids.size();
    int[] probes = new int[0];
    if (centroids != null) {
      int requested = getHintInt(hint, Hint.VECTOR_PROBE_COUNT, props.getInt(
        QueryServices.VECTOR_PROBE_COUNT_ATTRIB, QueryServicesOptions.DEFAULT_VECTOR_PROBE_COUNT));
      int count = requested > 0
        ? Math.min(requested, centroidCount)
        : Math.max(1, (int) Math.round(Math.sqrt(centroidCount)));
      int[] nearest = centroids.nearest(queryVector, count);
      ScanRanges ranges = getPostingListRanges(context, index, nearest);
      if (ranges != null) {
        probes = nearest;
        context.setScanRanges(ranges);
        if (ranges.useSkipScanFilter()) {
          ScanUtil.andFilterAtBeginning(context.getScan(), ranges.getSkipScanFilter());
        }
      }
    }
    this.probeCentroids = probes;
    this.probeCount = probes.length;
  }

  /** Extracts the constant query vector from the ORDER BY distance expression if present. */
  static float[] getQueryVector(OrderBy orderBy) {
    if (orderBy == null || orderBy.getOrderByExpressions().size() != 1) {
      return null;
    }
    Expression expression = orderBy.getOrderByExpressions().get(0).getExpression();
    if (!(expression instanceof DistanceFunction)) {
      return null;
    }
    for (Expression child : expression.getChildren()) {
      if (
        child.isStateless() && child.getDataType() != null && child.getDataType().isVectorType()
      ) {
        ImmutableBytesWritable ptr = new ImmutableBytesWritable();
        if (child.evaluate(null, ptr) && ptr.getLength() > 0) {
          return CachedCentroids.decode(ptr.get(), ptr.getOffset(), ptr.getLength(),
            child.getDataType());
        }
      }
    }
    return null;
  }

  /**
   * Constructs scan ranges targeting posting lists of probed centroids across salt and tenant
   * prefixes. Returns null if posting list boundaries cannot be addressed or filter constraints
   * conflict with centroid key prefixes.
   */
  static ScanRanges getPostingListRanges(StatementContext context, PTable index, int[] centroids)
    throws SQLException {
    ScanRanges compiled = context.getScanRanges();
    if (compiled == ScanRanges.NOTHING || index.getViewIndexId() != null) {
      return null;
    }
    boolean isSalted = index.getBucketNum() != null;
    boolean scopeTenant = index.isMultiTenant() && context.getConnection().getTenantId() != null;
    if (index.isMultiTenant() && !scopeTenant) {
      // Unscoped multitenant index keys do not form contiguous posting list ranges
      return null;
    }
    int prefixSlots = (isSalted ? 1 : 0) + (scopeTenant ? 1 : 0);
    if (!compiled.isEverything() && compiled.getBoundPkColumnCount() > prefixSlots) {
      return null;
    }
    List<List<KeyRange>> slots = new ArrayList<>(prefixSlots + 1);
    if (isSalted) {
      slots.add(SaltingUtil.generateAllSaltingRanges(index.getBucketNum()));
    }
    if (scopeTenant) {
      slots.add(Collections
        .singletonList(KeyRange.getKeyRange(ScanUtil.getTenantIdBytes(index.getRowKeySchema(),
          isSalted, context.getConnection().getTenantId(), false))));
    }
    List<KeyRange> centroidRanges = new ArrayList<>(centroids.length);
    for (int centroid : centroids) {
      centroidRanges.add(KeyRange.getKeyRange(PInteger.INSTANCE.toBytes(centroid)));
    }
    centroidRanges.sort(KeyRange.COMPARATOR);
    slots.add(centroidRanges);
    return ScanRanges.create(index.getRowKeySchema(), slots, new int[slots.size()],
      index.getBucketNum(), true, -1);
  }

  static int getHintInt(HintNode hint, Hint name, int defaultValue) {
    String value = hint.getHint(name);
    if (value != null) {
      try {
        return Integer.parseInt(value.substring(1, value.length() - 1).trim());
      } catch (NumberFormatException | IndexOutOfBoundsException e) {
        // fall through to the default
      }
    }
    return defaultValue;
  }

  /**
   * Computes execution cost for vector index scans. When guideposts exist, cost is derived directly
   * from the probed posting list ranges. Without index statistics, cost is estimated by scaling
   * base table statistics by the probed centroid fraction and relative row sizes. For uncovered
   * indexes requiring base table joins, candidate lookup overhead is included.
   */
  @Override
  public Cost getCost() {
    Cost cost = super.getCost();
    Long candidates = null;
    try {
      candidates = getEstimatedRowsToScan();
      if (cost.isUnknown()) {
        Long dataBytes = dataPlan == null ? null : dataPlan.getEstimatedBytesToScan();
        if (dataBytes == null) {
          return Cost.UNKNOWN;
        }
        double fraction = probeCount == 0 ? 1.0 : (double) probeCount / centroidCount;
        double bytes = dataBytes * fraction * SchemaUtil.estimateRowSize(getTableRef().getTable())
          / SchemaUtil.estimateRowSize(dataPlan.getTableRef().getTable());
        cost = new Cost(0, 0, bytes);
        Long dataRows = dataPlan.getEstimatedRowsToScan();
        candidates = dataRows == null ? null : (long) Math.ceil(dataRows * fraction);
        if (limit != null && candidates != null) {
          double outputBytes = Math.min(candidates, limit + (offset == null ? 0 : offset))
            * (double) SchemaUtil.estimateRowSize(getTableRef().getTable());
          cost = cost.plus(CostUtil.estimateOrderByCost(bytes, outputBytes,
            CostUtil.estimateParallelLevel(true, context.getConnection().getQueryServices())));
        }
      }
    } catch (SQLException e) {
      return Cost.UNKNOWN;
    }
    if (context.isUncoveredIndex()) {
      if (candidates == null) {
        return Cost.UNKNOWN;
      }
      cost = cost.plus(CostUtil.estimateLookupCost(candidates, dataPlan.getTableRef().getTable()));
    }
    return cost;
  }

  @Override
  public ExplainPlan getExplainPlan() throws SQLException {
    ExplainPlan explainPlan = super.getExplainPlan();
    if (probeCount == 0) {
      return explainPlan;
    }
    String metric = getTableRef().getTable().getVectorDistanceMetric();
    List<String> steps = new ArrayList<>(explainPlan.getPlanSteps());
    steps.add(0,
      "CLIENT PROBING " + probeCount + " OF " + centroidCount + " CENTROIDS (" + metric + ")");
    ExplainPlanAttributes attributes = explainPlan.getPlanStepsAsAttributes();
    return new ExplainPlan(steps,
      (attributes == null
        ? new ExplainPlanAttributesBuilder()
        : new ExplainPlanAttributesBuilder(attributes)).setVectorProbeCount(probeCount)
          .setVectorCentroidCount(centroidCount).setVectorDistanceMetric(metric).build());
  }

  /** Number of probed centroid posting lists, or 0 for full index scans. */
  public int getProbeCount() {
    return probeCount;
  }

  /** Probed centroid identifiers in order of ascending distance. */
  public int[] getProbeCentroids() {
    return probeCentroids.clone();
  }

  /** Total centroids in the active model generation, or 0 if unprobed. */
  public int getCentroidCount() {
    return centroidCount;
  }
}
