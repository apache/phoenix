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
import org.apache.phoenix.compile.OrderByCompiler.OrderBy;
import org.apache.phoenix.compile.QueryPlan;
import org.apache.phoenix.compile.RowProjector;
import org.apache.phoenix.compile.ScanRanges;
import org.apache.phoenix.compile.StatementContext;
import org.apache.phoenix.expression.Expression;
import org.apache.phoenix.expression.function.DistanceFunction;
import org.apache.phoenix.index.vector.CentroidManager;
import org.apache.phoenix.iterate.ParallelIteratorFactory;
import org.apache.phoenix.jdbc.PhoenixConnection;
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
import org.apache.phoenix.util.ReadOnlyProps;
import org.apache.phoenix.util.ScanUtil;

import org.apache.phoenix.thirdparty.com.google.common.base.Optional;

/**
 * The scan plan for a vector search on an IVF vector index. The plan finds the centroids nearest
 * the query vector and limits the scan to their posting lists, under each salt bucket and tenant
 * prefix. Each region server scores its candidates and returns its top N rows in distance order.
 * The client merges these rows into the final top K.
 * <p>
 * If the probed posting lists hold too few rows, the plan can probe more posting lists in batches.
 * During a migration, the plan probes both the active and the building generation.
 * <p>
 * If the plan cannot limit the scan to posting lists, it keeps the compiled key ranges of the query
 * and probes no centroids. Examples are an untrained index, a view index, a multitenant index
 * without a tenant ID, and a query that binds a key column after the salt and tenant prefix. For
 * the first three examples, the scan usually covers all rows of the index.
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
        .get(connection, CentroidManager.getCentroidIndexName(index.getName().getString()),
          index.getVectorCentroidGeneration(),
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

  /**
   * Returns the constant query vector of the ORDER BY distance function. Returns null if the ORDER
   * BY is not a single distance function with a constant vector argument.
   */
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
   * Returns the scan ranges that cover the posting lists of the given centroids. The ranges repeat
   * for each salt bucket and stay in the tenant of the connection. Returns null if key ranges
   * cannot address the posting lists. This occurs for a view index and for a multitenant index
   * without a tenant. It also occurs if the compiled ranges select nothing or bind key columns
   * after the salt and tenant prefix.
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
      // Without a tenant ID, each posting list spreads over one key range for each tenant
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
        // A repeated hint joins its arguments, as in (4)(8), and the first argument applies
        return Integer.parseInt(value.substring(1, value.indexOf(HintNode.SUFFIX)).trim());
      } catch (NumberFormatException | IndexOutOfBoundsException e) {
        // An argument that is not an integer gives the default value
      }
    }
    return defaultValue;
  }

  @Override
  public ExplainPlan getExplainPlan() throws SQLException {
    ExplainPlan explainPlan = super.getExplainPlan();
    if (probeCount == 0) {
      return explainPlan;
    }
    List<String> steps = new ArrayList<>(explainPlan.getPlanSteps());
    steps.add(0, "CLIENT PROBING " + probeCount + " OF " + centroidCount + " CENTROIDS");
    return new ExplainPlan(steps, explainPlan.getPlanStepsAsAttributes());
  }

  /** Number of probed centroid posting lists, or 0 for full index scans. */
  public int getProbeCount() {
    return probeCount;
  }

  /**
   * Returns a copy of the centroid IDs of the first probe batch, nearest first. During a migration,
   * the rankings of the active and building generations alternate. The array is empty if the plan
   * scans the whole index.
   */
  public int[] getProbeCentroids() {
    return probeCentroids.clone();
  }

  /**
   * Returns the number of centroids in the active generation, plus the building generation during a
   * migration. Returns 0 if the query has no constant query vector or the index has no trained
   * model.
   */
  public int getCentroidCount() {
    return centroidCount;
  }
}
