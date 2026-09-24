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

import java.io.IOException;
import java.sql.Array;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.NavigableSet;
import java.util.Set;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.filter.FilterList;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.Pair;
import org.apache.phoenix.cache.ServerCacheClient.ServerCache;
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
import org.apache.phoenix.coprocessorclient.BaseScannerRegionObserverConstants;
import org.apache.phoenix.execute.visitor.ByteCountVisitor;
import org.apache.phoenix.expression.Expression;
import org.apache.phoenix.expression.LiteralExpression;
import org.apache.phoenix.expression.OrderByExpression;
import org.apache.phoenix.expression.function.CosineDistanceFunction;
import org.apache.phoenix.expression.function.DistanceFunction;
import org.apache.phoenix.expression.function.InnerProductDistanceFunction;
import org.apache.phoenix.expression.function.L2DistanceFunction;
import org.apache.phoenix.expression.function.L2DistanceSquaredFunction;
import org.apache.phoenix.filter.SkipScanFilter;
import org.apache.phoenix.hbase.index.util.ImmutableBytesPtr;
import org.apache.phoenix.index.IndexMaintainer;
import org.apache.phoenix.iterate.BaseResultIterators;
import org.apache.phoenix.iterate.ConcatResultIterator;
import org.apache.phoenix.iterate.LimitingResultIterator;
import org.apache.phoenix.iterate.MergeSortTopNResultIterator;
import org.apache.phoenix.iterate.ParallelIteratorFactory;
import org.apache.phoenix.iterate.ParallelIterators;
import org.apache.phoenix.iterate.ParallelScanGrouper;
import org.apache.phoenix.iterate.PeekingResultIterator;
import org.apache.phoenix.iterate.ResultIterator;
import org.apache.phoenix.iterate.SerialIterators;
import org.apache.phoenix.jdbc.PhoenixConnection;
import org.apache.phoenix.optimize.Cost;
import org.apache.phoenix.optimize.VectorSearchUtil;
import org.apache.phoenix.parse.FilterableStatement;
import org.apache.phoenix.parse.HintNode;
import org.apache.phoenix.parse.SelectStatement;
import org.apache.phoenix.query.KeyRange;
import org.apache.phoenix.query.QueryConstants;
import org.apache.phoenix.query.QueryServices;
import org.apache.phoenix.query.QueryServicesOptions;
import org.apache.phoenix.schema.PColumn;
import org.apache.phoenix.schema.PName;
import org.apache.phoenix.schema.PTable;
import org.apache.phoenix.schema.PTableType;
import org.apache.phoenix.schema.TableRef;
import org.apache.phoenix.schema.tuple.ResultTuple;
import org.apache.phoenix.schema.tuple.Tuple;
import org.apache.phoenix.schema.types.PInteger;
import org.apache.phoenix.schema.types.PVectorDouble;
import org.apache.phoenix.schema.types.PVectorFloat;
import org.apache.phoenix.util.ByteUtil;
import org.apache.phoenix.util.ClientUtil;
import org.apache.phoenix.util.CostUtil;
import org.apache.phoenix.util.IndexUtil;
import org.apache.phoenix.util.ReadOnlyProps;
import org.apache.phoenix.util.ScanUtil;
import org.apache.phoenix.util.SchemaUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.phoenix.thirdparty.com.google.common.base.Optional;

/**
 * Execution plan for IVF vector index scans. Resolves probe count, loads centroid cache, identifies
 * nearest centroids to the query vector, translates selected centroid IDs into multi-range scan
 * keys (handling salting and multi-tenancy), and costs the plan proportional to IVF probe
 * selectivity.
 */
public class VectorIndexScanPlan extends ScanPlan {
  private static final Logger LOGGER = LoggerFactory.getLogger(VectorIndexScanPlan.class);

  /**
   * Policy governing vector query execution when an index rebuild is concurrently in progress.
   */
  public enum RebuildProbePolicy {
    /** Expand probe count to improve recall across mixed-generation index data. */
    EXPAND,
    /** Scan all indexed rows without centroid key range restrictions. */
    EXACT,
    /** Execute queries with default probe count without adjustments for concurrent rebuilds. */
    NONE
  }

  private final CachedCentroids cachedCentroids;
  private final int probeCount;
  private final List<Integer> probeCentroids;
  private final List<KeyRange> keyRanges;
  private final float[] queryVector;
  private final String distanceMetric;
  private final boolean probing;
  private final double oversampleFactor;
  private final Integer coarseLimit;
  private final int maxProbeLimit;
  private final SkipScanFilter centroidSkipScanFilter;
  private final boolean rebuildInProgress;
  private final RebuildProbePolicy rebuildProbePolicy;
  private final int baseProbeCount;
  private boolean filterTimeUncoveredLookup;
  private boolean projectionTimeUncoveredLookup;
  private QueryPlan overrideDataPlan;
  private int lastDeferredLookupCount;
  private int lastProbedBatchCount;
  private int lastProbedCentroidCount;

  public VectorIndexScanPlan(StatementContext context, FilterableStatement statement,
    TableRef table, RowProjector projector, Integer limit, Integer offset, OrderBy orderBy,
    ParallelIteratorFactory parallelIteratorFactory, boolean allowPageFilter, QueryPlan dataPlan,
    Optional<byte[]> rowOffset, CachedCentroids cachedCentroids, float[] queryVector,
    String distanceMetric, Integer explicitProbeCount, Double explicitOversampleFactor,
    Integer explicitMaxProbeLimit) throws SQLException {
    super(context, statement, table, projector, limit, offset, orderBy, parallelIteratorFactory,
      allowPageFilter, dataPlan, rowOffset);

    if (context != null && context.isUncoveredIndex()) {
      this.filterTimeUncoveredLookup = true;
    }

    this.cachedCentroids = resolveCachedCentroids(cachedCentroids, table, context);

    Pair<float[],
      String> extracted = (queryVector == null || distanceMetric == null)
        ? extractQueryVectorAndMetric(orderBy, context)
        : null;
    float[] effectiveQueryVector =
      queryVector != null ? queryVector : (extracted != null ? extracted.getFirst() : null);
    String effectiveMetric = distanceMetric != null
      ? distanceMetric
      : (extracted != null && extracted.getSecond() != null
        ? extracted.getSecond()
        : (table != null && table.getTable() != null
          ? table.getTable().getVectorDistanceMetric()
          : null));

    this.queryVector = effectiveQueryVector != null ? effectiveQueryVector.clone() : null;
    this.distanceMetric = effectiveMetric;

    int centroidCount = this.cachedCentroids != null ? this.cachedCentroids.getCentroidCount() : 0;
    HintNode hintNode = statement != null ? statement.getHint() : null;
    PhoenixConnection connection = context != null ? context.getConnection() : null;

    this.rebuildInProgress =
      this.cachedCentroids != null && this.cachedCentroids.isRebuildInProgress();
    this.rebuildProbePolicy = resolveRebuildProbePolicy(connection);

    if (this.queryVector == null) {
      this.baseProbeCount = 0;
      this.probeCount = 0;
      this.probeCentroids = Collections.emptyList();
      this.keyRanges = Collections.emptyList();
      this.probing = false;
    } else {
      this.baseProbeCount =
        resolveProbeCount(explicitProbeCount, hintNode, connection, centroidCount);
      this.probeCount =
        this.rebuildInProgress && this.rebuildProbePolicy == RebuildProbePolicy.EXPAND
          ? expandProbeCount(this.baseProbeCount, resolveRebuildProbeFactor(connection),
            centroidCount)
          : this.baseProbeCount;
      this.probeCentroids =
        selectProbes(this.cachedCentroids, this.queryVector, this.distanceMetric, this.probeCount);

      if (table != null && table.getTable() != null && table.getTable().getViewIndexId() != null) {
        LOGGER.warn("Vector index {} has a viewIndexId; centroid key ranges will not include it",
          table.getTable().getName());
      }

      Integer saltBuckets =
        (table != null && table.getTable() != null) ? table.getTable().getBucketNum() : null;
      byte[] tenantIdBytes = extractTenantIdBytes(table, connection);

      // Multi-tenant vector index row keys are prefixed with tenant ID ahead of centroid ID.
      // Global connections lacking a tenant ID cannot construct scoped centroid ranges and
      // therefore fall back to scanning the full index.
      boolean tenantScopingRequired =
        table != null && table.getTable() != null && table.getTable().isMultiTenant();
      boolean canScopeCentroidRanges = !tenantScopingRequired || tenantIdBytes != null;

      // When rebuild policy is EXACT, bypass centroid key ranges to scan all indexed rows.

      boolean exactDuringRebuild =
        this.rebuildInProgress && this.rebuildProbePolicy == RebuildProbePolicy.EXACT;

      this.keyRanges = (canScopeCentroidRanges && !exactDuringRebuild)
        ? buildCentroidKeyRanges(this.probeCentroids, saltBuckets, tenantIdBytes)
        : Collections.emptyList();

      this.probing = !this.keyRanges.isEmpty();
    }

    // Configure centroid ranges on the statement context. Initial split and concurrency
    // calculations in the superclass use pre-centroid ranges, while the executable scan
    // is built using these restricted centroid ranges.
    SkipScanFilter appliedCentroidFilter = null;
    if (this.probing && context != null) {
      ScanRanges scanRanges = ScanRanges.createCentroidScanRanges(this.keyRanges);
      context.setScanRanges(scanRanges);
      if (scanRanges.useSkipScanFilter()) {
        // Retain the centroid skip scan filter for replacement during adaptive probe expansion.
        appliedCentroidFilter = scanRanges.getSkipScanFilter();
        ScanUtil.andFilterAtBeginning(context.getScan(), appliedCentroidFilter);
      }
    }
    this.centroidSkipScanFilter = appliedCentroidFilter;

    this.oversampleFactor = resolveOversampleFactor(explicitOversampleFactor, hintNode, connection);
    this.coarseLimit = (limit != null && this.oversampleFactor > 1.0)
      ? (int) Math.ceil(limit * this.oversampleFactor)
      : limit;

    if (
      context != null && context.getScan() != null && this.oversampleFactor > 1.0 && limit != null
    ) {
      context.getScan().setAttribute(BaseScannerRegionObserverConstants.VECTOR_OVERSAMPLE_FACTOR,
        Bytes.toBytes(this.oversampleFactor));
    }

    this.maxProbeLimit = resolveMaxProbeLimit(explicitMaxProbeLimit, hintNode, connection);
    this.lastProbedBatchCount = 0;
    this.lastProbedCentroidCount = 0;
  }

  public VectorIndexScanPlan(StatementContext context, FilterableStatement statement,
    TableRef table, RowProjector projector, Integer limit, Integer offset, OrderBy orderBy,
    ParallelIteratorFactory parallelIteratorFactory, boolean allowPageFilter, QueryPlan dataPlan,
    Optional<byte[]> rowOffset, CachedCentroids cachedCentroids, float[] queryVector,
    String distanceMetric, Integer explicitProbeCount, Double explicitOversampleFactor)
    throws SQLException {
    this(context, statement, table, projector, limit, offset, orderBy, parallelIteratorFactory,
      allowPageFilter, dataPlan, rowOffset, cachedCentroids, queryVector, distanceMetric,
      explicitProbeCount, explicitOversampleFactor, null);
  }

  public VectorIndexScanPlan(StatementContext context, FilterableStatement statement,
    TableRef table, RowProjector projector, Integer limit, Integer offset, OrderBy orderBy,
    ParallelIteratorFactory parallelIteratorFactory, boolean allowPageFilter, QueryPlan dataPlan,
    Optional<byte[]> rowOffset, CachedCentroids cachedCentroids, float[] queryVector,
    String distanceMetric, Integer explicitProbeCount) throws SQLException {
    this(context, statement, table, projector, limit, offset, orderBy, parallelIteratorFactory,
      allowPageFilter, dataPlan, rowOffset, cachedCentroids, queryVector, distanceMetric,
      explicitProbeCount, null);
  }

  public VectorIndexScanPlan(StatementContext context, FilterableStatement statement,
    TableRef table, RowProjector projector, Integer limit, Integer offset, OrderBy orderBy,
    ParallelIteratorFactory parallelIteratorFactory, boolean allowPageFilter, QueryPlan dataPlan,
    Optional<byte[]> rowOffset, CachedCentroids cachedCentroids, byte[] queryVectorBytes,
    String distanceMetric, Integer explicitProbeCount) throws SQLException {
    this(context, statement, table, projector, limit, offset, orderBy, parallelIteratorFactory,
      allowPageFilter, dataPlan, rowOffset, cachedCentroids, decodeVectorBytes(queryVectorBytes),
      distanceMetric, explicitProbeCount);
  }

  public VectorIndexScanPlan(StatementContext context, FilterableStatement statement,
    TableRef table, RowProjector projector, Integer limit, Integer offset, OrderBy orderBy,
    ParallelIteratorFactory parallelIteratorFactory, boolean allowPageFilter, QueryPlan dataPlan,
    Optional<byte[]> rowOffset, float[] queryVector, String distanceMetric,
    Integer explicitProbeCount) throws SQLException {
    this(context, statement, table, projector, limit, offset, orderBy, parallelIteratorFactory,
      allowPageFilter, dataPlan, rowOffset, null, queryVector, distanceMetric, explicitProbeCount);
  }

  public VectorIndexScanPlan(StatementContext context, FilterableStatement statement,
    TableRef table, RowProjector projector, Integer limit, Integer offset, OrderBy orderBy,
    ParallelIteratorFactory parallelIteratorFactory, boolean allowPageFilter, QueryPlan dataPlan,
    Optional<byte[]> rowOffset) throws SQLException {
    this(context, statement, table, projector, limit, offset, orderBy, parallelIteratorFactory,
      allowPageFilter, dataPlan, rowOffset, null, (float[]) null, null, null);
  }

  public static Pair<float[], String> extractQueryVectorAndMetric(OrderBy orderBy,
    StatementContext context) {
    if (orderBy == null || orderBy.getOrderByExpressions() == null) {
      return null;
    }
    for (OrderByExpression obe : orderBy.getOrderByExpressions()) {
      Expression expr = obe.getExpression();
      if (expr instanceof DistanceFunction) {
        DistanceFunction distFunc = (DistanceFunction) expr;
        String metric = null;
        if (
          distFunc instanceof L2DistanceFunction || distFunc instanceof L2DistanceSquaredFunction
        ) {
          metric = "L2";
        } else if (distFunc instanceof CosineDistanceFunction) {
          metric = "COSINE";
        } else if (distFunc instanceof InnerProductDistanceFunction) {
          metric = "INNER_PRODUCT";
        }
        List<Expression> children = distFunc.getChildren();
        if (children != null && children.size() >= 2) {
          Expression qExpr = children.get(0).isStateless()
            ? children.get(0)
            : (children.get(1).isStateless() ? children.get(1) : null);
          if (qExpr != null) {
            float[] qv = extractVectorFromExpression(qExpr);
            if (qv != null) {
              return Pair.newPair(qv, metric);
            }
          }
        }
      }
    }
    return null;
  }

  public static float[] extractVectorFromExpression(Expression expr) {
    if (expr == null) {
      return null;
    }
    if (expr instanceof LiteralExpression) {
      Object val = ((LiteralExpression) expr).getValue();
      float[] v = toFloatArray(val);
      if (v != null) {
        return v;
      }
    }
    ImmutableBytesWritable ptr = new ImmutableBytesWritable();
    try {
      if (expr.evaluate(null, ptr) && ptr.get() != null && ptr.getLength() > 0) {
        if (expr.getDataType() instanceof PVectorFloat) {
          return PVectorFloat.readElements(ptr.get(), ptr.getOffset(), ptr.getLength());
        } else if (expr.getDataType() instanceof PVectorDouble) {
          double[] d = PVectorDouble.readElements(ptr.get(), ptr.getOffset(), ptr.getLength());
          float[] f = new float[d.length];
          for (int i = 0; i < d.length; i++) {
            f[i] = (float) d[i];
          }
          return f;
        } else if (expr.getDataType() != null) {
          Object obj = expr.getDataType().toObject(ptr);
          return toFloatArray(obj);
        }
      }
    } catch (Exception e) {
      LOGGER.warn("Failed to evaluate query vector expression", e);
    }
    return null;
  }

  public static float[] toFloatArray(Object obj) {
    if (obj == null) {
      return null;
    }
    if (obj instanceof float[]) {
      return (float[]) obj;
    }
    if (obj instanceof Float[]) {
      Float[] boxed = (Float[]) obj;
      float[] f = new float[boxed.length];
      for (int i = 0; i < boxed.length; i++) {
        f[i] = boxed[i] != null ? boxed[i] : 0.0f;
      }
      return f;
    }
    if (obj instanceof double[]) {
      double[] d = (double[]) obj;
      float[] f = new float[d.length];
      for (int i = 0; i < d.length; i++) {
        f[i] = (float) d[i];
      }
      return f;
    }
    if (obj instanceof Double[]) {
      Double[] boxed = (Double[]) obj;
      float[] f = new float[boxed.length];
      for (int i = 0; i < boxed.length; i++) {
        f[i] = boxed[i] != null ? boxed[i].floatValue() : 0.0f;
      }
      return f;
    }
    if (obj instanceof Array) {
      try {
        return toFloatArray(((Array) obj).getArray());
      } catch (Exception e) {
        LOGGER.warn("Error getting array from java.sql.Array", e);
      }
    }
    if (obj instanceof Number[]) {
      Number[] numbers = (Number[]) obj;
      float[] f = new float[numbers.length];
      for (int i = 0; i < numbers.length; i++) {
        f[i] = numbers[i] != null ? numbers[i].floatValue() : 0.0f;
      }
      return f;
    }
    return null;
  }

  private static float[] decodeVectorBytes(byte[] bytes) {
    if (bytes == null || bytes.length == 0) {
      return null;
    }
    return PVectorFloat.readElements(bytes, 0, bytes.length);
  }

  private static CachedCentroids resolveCachedCentroids(CachedCentroids explicit, TableRef table,
    StatementContext context) {
    if (explicit != null) {
      return explicit;
    }
    if (table != null && table.getTable() != null) {
      String tableName = table.getTable().getName().getString();
      // Prefer the generation recorded on the resolved index PTable to ensure external rebuilds
      // that update catalog metadata are observed.
      Long generation = table.getTable().getVectorCentroidGeneration();
      try {
        PhoenixConnection connection = context != null ? context.getConnection() : null;
        VectorCentroidCache cache = connection != null
          ? VectorCentroidCache.getInstance(connection.getQueryServices().getConfiguration())
          : VectorCentroidCache.getInstance();
        return generation != null
          ? cache.getCentroids(tableName, generation, connection)
          : cache.getCentroids(tableName, connection);
      } catch (Exception e) {
        LOGGER.warn("Unable to load cached centroids for index table {}", tableName, e);
      }
    }
    return null;
  }

  private static byte[] extractTenantIdBytes(TableRef table, PhoenixConnection connection) {
    if (table == null || table.getTable() == null || connection == null) {
      return null;
    }
    PTable pTable = table.getTable();
    if (!pTable.isMultiTenant() || connection.getTenantId() == null) {
      return null;
    }
    PName tenantId = connection.getTenantId();
    int tenantColIndex = pTable.getBucketNum() != null ? 1 : 0;
    if (tenantColIndex < pTable.getPKColumns().size()) {
      PColumn tenantCol = pTable.getPKColumns().get(tenantColIndex);
      byte[] rawTenantBytes = tenantCol.getDataType().toBytes(tenantId.getString());
      if (!tenantCol.getDataType().isFixedWidth()) {
        return ByteUtil.concat(rawTenantBytes, QueryConstants.SEPARATOR_BYTE_ARRAY);
      } else {
        return rawTenantBytes;
      }
    }
    return null;
  }

  /**
   * Resolves the effective probe count based on explicit parameter, VECTOR_PROBE_COUNT query hint,
   * connection property, query services configuration, or default square-root heuristic clamped to
   * the centroid count.
   */
  public static int resolveProbeCount(Integer explicitProbeCount, HintNode hintNode,
    PhoenixConnection connection, int centroidCount) {
    if (centroidCount <= 0) {
      return 0;
    }

    int candidate = -1;

    if (explicitProbeCount != null && explicitProbeCount > 0) {
      candidate = explicitProbeCount;
    }

    if (candidate <= 0 && hintNode != null && hintNode.hasHint(HintNode.Hint.VECTOR_PROBE_COUNT)) {
      String hintVal = hintNode.getHint(HintNode.Hint.VECTOR_PROBE_COUNT);
      if (hintVal != null) {
        String clean = hintVal.replaceAll("[()=\\s]", "");
        if (!clean.isEmpty()) {
          try {
            int p = Integer.parseInt(clean);
            if (p > 0) {
              candidate = p;
            }
          } catch (NumberFormatException e) {
            LOGGER.warn("Invalid numeric VECTOR_PROBE_COUNT hint: {}", hintVal);
          }
        }
      }
    }

    if (candidate <= 0 && connection != null) {
      String propVal = null;
      try {
        propVal = connection.getClientInfo(QueryServices.VECTOR_PROBE_COUNT_ATTRIB);
        if (propVal == null) {
          propVal = connection.getClientInfo("VECTOR_PROBE_COUNT");
        }
      } catch (Exception e) {
        // ignore
      }
      if (propVal != null && !propVal.trim().isEmpty()) {
        try {
          int p = Integer.parseInt(propVal.trim());
          if (p > 0) {
            candidate = p;
          }
        } catch (NumberFormatException e) {
          LOGGER.warn("Invalid VECTOR_PROBE_COUNT client property: {}", propVal);
        }
      }
      if (candidate <= 0 && connection.getQueryServices() != null) {
        ReadOnlyProps props = connection.getQueryServices().getProps();
        if (props != null) {
          candidate = props.getInt(QueryServices.VECTOR_PROBE_COUNT_ATTRIB, 0);
        }
      }
    }

    if (candidate <= 0) {
      candidate = (int) Math.round(Math.sqrt(centroidCount));
      candidate = Math.max(1, candidate);
    }

    candidate = Math.max(1, Math.min(candidate, centroidCount));
    return candidate;
  }

  /**
   * Scales the base probe count by the configured expansion factor, bounded between the base probe
   * count and the total number of centroids.
   */
  static int expandProbeCount(int baseProbeCount, double factor, int centroidCount) {
    if (baseProbeCount <= 0 || centroidCount <= 0) {
      return baseProbeCount;
    }
    if (!(factor > 1.0)) {
      return Math.min(baseProbeCount, centroidCount);
    }
    long widened = (long) Math.ceil(baseProbeCount * factor);
    return (int) Math.max(baseProbeCount, Math.min(widened, centroidCount));
  }

  /**
   * Resolves the {@link RebuildProbePolicy} from connection client info or query services
   * configuration, defaulting to {@link RebuildProbePolicy#EXPAND}.
   */
  static RebuildProbePolicy resolveRebuildProbePolicy(PhoenixConnection connection) {
    String value =
      readVectorProperty(connection, QueryServices.VECTOR_INDEX_REBUILD_PROBE_POLICY_ATTRIB,
        QueryServicesOptions.DEFAULT_VECTOR_INDEX_REBUILD_PROBE_POLICY);
    try {
      return RebuildProbePolicy.valueOf(value.trim().toUpperCase());
    } catch (IllegalArgumentException e) {
      LOGGER.warn("Unrecognized {} value '{}'; using {}",
        QueryServices.VECTOR_INDEX_REBUILD_PROBE_POLICY_ATTRIB, value,
        QueryServicesOptions.DEFAULT_VECTOR_INDEX_REBUILD_PROBE_POLICY);
      return RebuildProbePolicy
        .valueOf(QueryServicesOptions.DEFAULT_VECTOR_INDEX_REBUILD_PROBE_POLICY);
    }
  }

  /** Resolves the probe count multiplier applied under {@link RebuildProbePolicy#EXPAND}. */
  static double resolveRebuildProbeFactor(PhoenixConnection connection) {
    String value =
      readVectorProperty(connection, QueryServices.VECTOR_INDEX_REBUILD_PROBE_FACTOR_ATTRIB, null);
    if (value != null && !value.trim().isEmpty()) {
      try {
        return Double.parseDouble(value.trim());
      } catch (NumberFormatException e) {
        LOGGER.warn("Invalid {} value: {}", QueryServices.VECTOR_INDEX_REBUILD_PROBE_FACTOR_ATTRIB,
          value);
      }
    }
    return QueryServicesOptions.DEFAULT_VECTOR_INDEX_REBUILD_PROBE_FACTOR;
  }

  /** Resolves a configuration property from connection client info or query services. */
  private static String readVectorProperty(PhoenixConnection connection, String key,
    String defaultValue) {
    if (connection == null) {
      return defaultValue;
    }
    try {
      String clientInfo = connection.getClientInfo(key);
      if (clientInfo != null && !clientInfo.trim().isEmpty()) {
        return clientInfo;
      }
    } catch (Exception e) {
      // fall through to query services props
    }
    if (connection.getQueryServices() != null) {
      ReadOnlyProps props = connection.getQueryServices().getProps();
      if (props != null) {
        String propValue = props.get(key);
        if (propValue != null && !propValue.trim().isEmpty()) {
          return propValue;
        }
      }
    }
    return defaultValue;
  }

  /** Returns whether an index rebuild was in progress when centroids were loaded. */
  public boolean isRebuildInProgress() {
    return rebuildInProgress;
  }

  /** Returns the rebuild probe policy configured for this plan. */
  public RebuildProbePolicy getRebuildProbePolicy() {
    return rebuildProbePolicy;
  }

  /** Returns the unexpanded probe count before rebuild policy adjustments. */
  public int getBaseProbeCount() {
    return baseProbeCount;
  }

  /**
   * Resolves the oversample factor for two-phase vector search with precedence: 1. Explicit
   * parameter (>= 1.0) 2. Query hint OVERSAMPLE (>= 1.0) 3. Connection client info property / query
   * services configuration 4. Default: QueryServicesOptions.DEFAULT_VECTOR_OVERSAMPLE_FACTOR (3.0)
   */
  public static double resolveOversampleFactor(Double explicitOversampleFactor, HintNode hintNode,
    PhoenixConnection connection) {
    double candidate = -1;

    if (explicitOversampleFactor != null && explicitOversampleFactor >= 1.0) {
      candidate = explicitOversampleFactor;
    }

    if (candidate < 1.0 && hintNode != null && hintNode.hasHint(HintNode.Hint.OVERSAMPLE)) {
      String hintVal = hintNode.getHint(HintNode.Hint.OVERSAMPLE);
      if (hintVal != null) {
        String clean = hintVal.replaceAll("[()=\\s]", "");
        if (!clean.isEmpty()) {
          try {
            double f = Double.parseDouble(clean);
            if (f >= 1.0) {
              candidate = f;
            }
          } catch (NumberFormatException e) {
            LOGGER.warn("Invalid numeric OVERSAMPLE hint: {}", hintVal);
          }
        }
      }
    }

    if (candidate < 1.0 && connection != null) {
      String propVal = null;
      try {
        propVal = connection.getClientInfo(QueryServices.VECTOR_OVERSAMPLE_FACTOR_ATTRIB);
        if (propVal == null) {
          propVal = connection.getClientInfo("VECTOR_OVERSAMPLE_FACTOR");
        }
      } catch (Exception e) {
        // ignore
      }
      if (propVal != null && !propVal.trim().isEmpty()) {
        try {
          double f = Double.parseDouble(propVal.trim());
          if (f >= 1.0) {
            candidate = f;
          }
        } catch (NumberFormatException e) {
          LOGGER.warn("Invalid VECTOR_OVERSAMPLE_FACTOR client property: {}", propVal);
        }
      }
      if (candidate < 1.0 && connection.getQueryServices() != null) {
        ReadOnlyProps props = connection.getQueryServices().getProps();
        if (props != null) {
          candidate = props.getDouble(QueryServices.VECTOR_OVERSAMPLE_FACTOR_ATTRIB,
            QueryServicesOptions.DEFAULT_VECTOR_OVERSAMPLE_FACTOR);
        }
      }
    }

    if (candidate < 1.0) {
      candidate = QueryServicesOptions.DEFAULT_VECTOR_OVERSAMPLE_FACTOR;
    }

    return candidate;
  }

  public double getOversampleFactor() {
    return oversampleFactor;
  }

  public Integer getCoarseLimit() {
    return coarseLimit;
  }

  /**
   * Resolves the maximum batch limit for adaptive probe expansion, evaluating explicit parameters,
   * query hints, connection attributes, and query services defaults in order of precedence.
   */
  public static int resolveMaxProbeLimit(Integer explicitMaxProbeLimit, HintNode hintNode,
    PhoenixConnection connection) {
    if (explicitMaxProbeLimit != null && explicitMaxProbeLimit > 0) {
      return explicitMaxProbeLimit;
    }

    if (hintNode != null) {
      String hintVal = null;
      if (hintNode.hasHint(HintNode.Hint.MAX_PROBE_LIMIT)) {
        hintVal = hintNode.getHint(HintNode.Hint.MAX_PROBE_LIMIT);
      }
      if (hintVal != null) {
        String clean = hintVal.replaceAll("[()=\\s]", "");
        if (!clean.isEmpty()) {
          try {
            int p = Integer.parseInt(clean);
            if (p > 0) {
              return p;
            }
          } catch (NumberFormatException e) {
            LOGGER.warn("Invalid numeric MAX_PROBE_LIMIT hint: {}", hintVal);
          }
        }
      }
    }

    if (connection != null) {
      String propVal = null;
      try {
        propVal = connection.getClientInfo(QueryServices.VECTOR_MAX_PROBE_LIMIT_ATTRIB);
        if (propVal == null) {
          propVal = connection.getClientInfo("VECTOR_MAX_PROBE_LIMIT");
        }
        if (propVal == null) {
          propVal = connection.getClientInfo("MAX_PROBE_LIMIT");
        }
        if (propVal == null) {
          propVal = connection.getClientInfo("max_probe_limit");
        }
      } catch (Exception e) {
        // ignore
      }
      if (propVal != null && !propVal.trim().isEmpty()) {
        try {
          int p = Integer.parseInt(propVal.trim());
          if (p > 0) {
            return p;
          }
        } catch (NumberFormatException e) {
          LOGGER.warn("Invalid VECTOR_MAX_PROBE_LIMIT client property: {}", propVal);
        }
      }
      if (connection.getQueryServices() != null) {
        ReadOnlyProps props = connection.getQueryServices().getProps();
        if (props != null) {
          int candidate = props.getInt(QueryServices.VECTOR_MAX_PROBE_LIMIT_ATTRIB,
            QueryServicesOptions.DEFAULT_VECTOR_MAX_PROBE_LIMIT);
          if (candidate > 0) {
            return candidate;
          }
        }
      }
    }

    return QueryServicesOptions.DEFAULT_VECTOR_MAX_PROBE_LIMIT;
  }

  public int getMaxProbeLimit() {
    return maxProbeLimit;
  }

  public int getLastProbedBatchCount() {
    return lastProbedBatchCount;
  }

  public int getLastProbedCentroidCount() {
    return lastProbedCentroidCount;
  }

  /**
   * Selects the top-P probe centroids from the cache based on distance to the query vector. If no
   * query vector is provided, selects the first P centroids.
   */
  public static List<Integer> selectProbes(CachedCentroids cachedCentroids, float[] queryVector,
    String distanceMetric, int probeCount) {
    if (probeCount <= 0 || cachedCentroids == null || cachedCentroids.getCentroidCount() == 0) {
      return Collections.emptyList();
    }
    int effectiveProbeCount = Math.min(probeCount, cachedCentroids.getCentroidCount());
    if (queryVector != null) {
      return cachedCentroids.findNearestCentroids(queryVector, distanceMetric, effectiveProbeCount);
    }
    List<Integer> defaultIds = new ArrayList<>(effectiveProbeCount);
    for (int i = 0; i < effectiveProbeCount; i++) {
      defaultIds.add(i);
    }
    return defaultIds;
  }

  /**
   * Translates selected centroid IDs into multi-range scan keys. Start key: [salt?] [tenant?]
   * [centroid_id (4 bytes big-endian)] (inclusive) End key: [salt?] [tenant?] [centroid_id + 1 (4
   * bytes big-endian)] (exclusive) If salted, replicated across all salt buckets. If multi-tenant,
   * prepended with tenant ID prefix.
   */
  public static List<KeyRange> buildCentroidKeyRanges(List<Integer> centroidIds,
    Integer saltBuckets, byte[] tenantIdBytes) {
    if (centroidIds == null || centroidIds.isEmpty()) {
      return Collections.emptyList();
    }
    List<KeyRange> ranges = new ArrayList<>();
    boolean hasTenant = tenantIdBytes != null && tenantIdBytes.length > 0;
    boolean isSalted = saltBuckets != null && saltBuckets > 0;
    int numBuckets = isSalted ? saltBuckets : 1;

    for (int b = 0; b < numBuckets; b++) {
      for (int cid : centroidIds) {
        byte[] cidBytes = PInteger.INSTANCE.toBytes(cid);
        byte[] nextCidBytes = PInteger.INSTANCE.toBytes(cid + 1);

        byte[] startKey;
        byte[] endKey;

        if (isSalted) {
          byte[] saltByte = new byte[] { (byte) b };
          if (hasTenant) {
            startKey = ByteUtil.concat(saltByte, tenantIdBytes, cidBytes);
            endKey = ByteUtil.concat(saltByte, tenantIdBytes, nextCidBytes);
          } else {
            startKey = ByteUtil.concat(saltByte, cidBytes);
            endKey = ByteUtil.concat(saltByte, nextCidBytes);
          }
        } else {
          if (hasTenant) {
            startKey = ByteUtil.concat(tenantIdBytes, cidBytes);
            endKey = ByteUtil.concat(tenantIdBytes, nextCidBytes);
          } else {
            startKey = cidBytes;
            endKey = nextCidBytes;
          }
        }
        ranges.add(KeyRange.getKeyRange(startKey, true, endKey, false));
      }
    }
    Collections.sort(ranges, KeyRange.COMPARATOR);
    return ranges;
  }

  /**
   * Assembles centroid IDs, salting, and tenant prefix into a ScanRanges object with SkipScanFilter
   * configured for posting lists.
   */
  public static ScanRanges createCentroidScanRanges(List<Integer> centroidIds, Integer saltBuckets,
    byte[] tenantIdBytes) {
    List<KeyRange> ranges = buildCentroidKeyRanges(centroidIds, saltBuckets, tenantIdBytes);
    return ScanRanges.createCentroidScanRanges(ranges);
  }

  @Override
  public Cost getCost() {
    Cost baseCost = super.getCost();
    Cost scanCost = null;
    QueryPlan effectiveDataPlan = getDataPlan();
    PTable indexTable = getTableRef() != null ? getTableRef().getTable() : null;
    PTable dataTable = effectiveDataPlan != null && effectiveDataPlan.getTableRef() != null
      ? effectiveDataPlan.getTableRef().getTable()
      : null;

    int lists = cachedCentroids != null ? cachedCentroids.getCentroidCount() : 0;
    double probeFraction = (probing && lists > 0) ? ((double) probeCount / (double) lists) : 1.0;

    Long derivedRows = null;

    if (baseCost != null && !baseCost.isUnknown()) {
      // When guideposts exist on the vector index, the base scan cost accurately reflects
      // the ranges of probed centroid posting lists.
      scanCost = baseCost;
    } else if (
      effectiveDataPlan != null && effectiveDataPlan.getCost() != null
        && !effectiveDataPlan.getCost().isUnknown() && indexTable != null && dataTable != null
    ) {
      // When index statistics are absent, derive scan volume from data table statistics
      // adjusted for relative row size and centroid probe selectivity.
      try {
        Long dataBytes = effectiveDataPlan.getEstimatedBytesToScan();
        if (dataBytes != null) {
          long indexRowSize = SchemaUtil.estimateRowSize(indexTable);
          long dataRowSize = SchemaUtil.estimateRowSize(dataTable);
          double rowSizeRatio =
            dataRowSize > 0 ? ((double) indexRowSize / (double) dataRowSize) : 1.0;
          double derivedBytes = (double) dataBytes * rowSizeRatio * probeFraction;

          Long dataRows = effectiveDataPlan.getEstimatedRowsToScan();
          if (dataRows != null && dataRows > 0) {
            derivedRows = (long) Math.ceil((double) dataRows * probeFraction);
          } else if (indexRowSize > 0) {
            derivedRows = (long) Math.ceil(derivedBytes / (double) indexRowSize);
          }

          int parallelLevel = CostUtil.estimateParallelLevel(true,
            context != null && context.getConnection() != null
              ? context.getConnection().getQueryServices()
              : null);
          Cost derivedCost = new Cost(0, 0, derivedBytes);
          if (orderBy != null && !orderBy.getOrderByExpressions().isEmpty()) {
            Double outputBytes = this.accept(new ByteCountVisitor());
            if (outputBytes == null) {
              long effectiveLimit = (limit != null ? limit : 0) + (offset != null ? offset : 0);
              double outputRows = effectiveLimit > 0 && derivedRows != null
                ? Math.min(derivedRows.doubleValue(), (double) effectiveLimit)
                : (derivedRows != null ? derivedRows.doubleValue() : 1.0);
              outputBytes = outputRows * (indexRowSize > 0 ? indexRowSize : 1.0);
            }
            Cost orderByCost =
              CostUtil.estimateOrderByCost(derivedBytes, outputBytes, parallelLevel);
            derivedCost = derivedCost.plus(orderByCost);
          }
          scanCost = derivedCost;
        }
      } catch (SQLException e) {
        // Fall through to UNKNOWN
      }
    }

    if (scanCost == null || scanCost.isUnknown()) {
      return Cost.UNKNOWN;
    }

    // Add point lookup overhead to retrieve uncovered columns from the data table.
    if (isProjectionTimeUncoveredLookup() && dataTable != null) {
      long lookupRows = (limit != null ? limit : 0) + (offset != null ? offset : 0);
      Cost lookupCost = CostUtil.estimateVectorLookupCost(lookupRows, dataTable);
      scanCost = scanCost.plus(lookupCost);
    } else if (isFilterTimeUncoveredLookup() && dataTable != null) {
      Long lookupRows = null;
      try {
        lookupRows = getEstimatedRowsToScan();
      } catch (SQLException e) {
        // ignored
      }
      if (lookupRows == null || lookupRows <= 0) {
        if (derivedRows != null && derivedRows > 0) {
          lookupRows = derivedRows;
        } else if (effectiveDataPlan != null) {
          try {
            Long dataRows = effectiveDataPlan.getEstimatedRowsToScan();
            if (dataRows != null && dataRows > 0) {
              lookupRows = (long) Math.ceil((double) dataRows * probeFraction);
            }
          } catch (SQLException e) {
            // ignored
          }
        }
      }
      if (lookupRows != null && lookupRows > 0) {
        Cost lookupCost = CostUtil.estimateVectorLookupCost(lookupRows, dataTable);
        scanCost = scanCost.plus(lookupCost);
      }
    }

    return scanCost;
  }

  @Override
  public ExplainPlan getExplainPlan() throws SQLException {
    ExplainPlan baseExplain = super.getExplainPlan();
    List<String> steps = new ArrayList<>(baseExplain.getPlanSteps());
    ExplainPlanAttributes baseAttributes = baseExplain.getPlanStepsAsAttributes();
    ExplainPlanAttributesBuilder builder = baseAttributes != null
      ? new ExplainPlanAttributesBuilder(baseAttributes)
      : new ExplainPlanAttributesBuilder();

    int lists = cachedCentroids != null ? cachedCentroids.getCentroidCount() : 0;
    String metric = (getTableRef() != null && getTableRef().getTable() != null
      && getTableRef().getTable().getVectorDistanceMetric() != null)
        ? getTableRef().getTable().getVectorDistanceMetric()
        : this.distanceMetric;

    if (probing) {
      String probeLine = "CLIENT PROBING " + probeCount + " OF " + lists + " CENTROIDS"
        + (metric != null && !metric.isEmpty() ? " (" + metric + ")" : "");
      if (rebuildInProgress && rebuildProbePolicy == RebuildProbePolicy.EXPAND) {
        probeLine += " (REBUILD IN PROGRESS: EXPANDED FROM " + baseProbeCount + ")";
      }
      steps.add(0, probeLine);
      builder.setVectorProbeCount(probeCount);
      builder.setVectorCentroidCount(lists);
      builder.setVectorDistanceMetric(metric);
    } else if (rebuildInProgress && rebuildProbePolicy == RebuildProbePolicy.EXACT) {
      steps.add(0,
        "CLIENT EXACT VECTOR EVALUATION OVER " + lists + " CENTROIDS" + " (REBUILD IN PROGRESS)");
      builder.setVectorCentroidCount(lists);
      builder.setVectorDistanceMetric(metric);
    }

    if (isProjectionTimeUncoveredLookup()) {
      PTable indexTable = getTableRef() != null ? getTableRef().getTable() : null;
      QueryPlan effectiveDataPlan = getDataPlan();
      PTable dataTable = effectiveDataPlan != null && effectiveDataPlan.getTableRef() != null
        ? effectiveDataPlan.getTableRef().getTable()
        : null;
      SelectStatement select =
        (effectiveDataPlan != null && effectiveDataPlan.getStatement() instanceof SelectStatement)
          ? (SelectStatement) effectiveDataPlan.getStatement()
          : (statement instanceof SelectStatement ? (SelectStatement) statement : null);
      Set<PColumn> uncoveredCols =
        VectorSearchUtil.getUncoveredProjectionColumns(indexTable, dataTable, select);
      builder.setClientMergeColumns(uncoveredCols);
      if (!uncoveredCols.isEmpty()) {
        String clientMergeLine =
          "CLIENT MERGE " + uncoveredCols.toString() + " FOR TOP-" + limit + " ROWS";
        insertAfterClientMergeSort(steps, clientMergeLine);
        // Keep the ordered client-side pipeline carried by the attributes in step with the plan
        // text, so EXPLAIN FORMAT JSON reports the deferred merge in the same position.
        List<String> clientSteps = baseAttributes == null || baseAttributes.getClientSteps() == null
          ? new ArrayList<>()
          : new ArrayList<>(baseAttributes.getClientSteps());
        insertAfterClientMergeSort(clientSteps, clientMergeLine);
        builder.setClientSteps(clientSteps);
      }
    } else {
      builder.setClientMergeColumns(Collections.emptySet());
    }

    return new ExplainPlan(steps, builder.build());
  }

  /**
   * Inserts {@code line} immediately after the first {@code CLIENT MERGE SORT} entry of
   * {@code lines}, or appends it when no merge sort step is present.
   */
  private static void insertAfterClientMergeSort(List<String> lines, String line) {
    for (int i = 0; i < lines.size(); i++) {
      if (lines.get(i).startsWith("CLIENT MERGE SORT")) {
        lines.add(i + 1, line);
        return;
      }
    }
    lines.add(line);
  }

  public int getProbeCount() {
    return probeCount;
  }

  public List<Integer> getProbeCentroids() {
    return Collections.unmodifiableList(probeCentroids);
  }

  public List<KeyRange> getKeyRanges() {
    return Collections.unmodifiableList(keyRanges);
  }

  /**
   * Returns whether this plan actually narrowed its scan to the selected centroid posting lists.
   * False when no query vector could be resolved, in which case the whole index is scanned.
   */
  public boolean isProbing() {
    return probing;
  }

  public CachedCentroids getCachedCentroids() {
    return cachedCentroids;
  }

  public float[] getQueryVector() {
    return queryVector != null ? queryVector.clone() : null;
  }

  public String getDistanceMetric() {
    return distanceMetric;
  }

  public boolean isFilterTimeUncoveredLookup() {
    return filterTimeUncoveredLookup;
  }

  public void setFilterTimeUncoveredLookup(boolean filterTimeUncoveredLookup) {
    this.filterTimeUncoveredLookup = filterTimeUncoveredLookup;
  }

  public boolean isProjectionTimeUncoveredLookup() {
    return projectionTimeUncoveredLookup;
  }

  public void setProjectionTimeUncoveredLookup(boolean projectionTimeUncoveredLookup) {
    this.projectionTimeUncoveredLookup = projectionTimeUncoveredLookup;
  }

  public void setDataPlan(QueryPlan dataPlan) {
    this.overrideDataPlan = dataPlan;
  }

  public QueryPlan getDataPlan() {
    return overrideDataPlan != null ? overrideDataPlan : this.dataPlan;
  }

  public int getLastDeferredLookupCount() {
    return lastDeferredLookupCount;
  }

  @Override
  public RowProjector getProjector() {
    if (projectionTimeUncoveredLookup && getDataPlan() != null) {
      return getDataPlan().getProjector();
    }
    return super.getProjector();
  }

  /**
   * Removes a specific filter instance by reference identity from a filter hierarchy.
   * @param filter   the root filter or filter list
   * @param toRemove the filter instance to remove
   * @return the resulting filter hierarchy, or null if empty
   */
  public static Filter removeFilter(Filter filter, Filter toRemove) {
    if (filter == null || toRemove == null) {
      return filter;
    }
    if (filter == toRemove) {
      return null;
    }
    if (filter instanceof FilterList) {
      FilterList filterList = (FilterList) filter;
      List<Filter> remaining = new ArrayList<>(filterList.getFilters().size());
      boolean changed = false;
      for (Filter f : filterList.getFilters()) {
        Filter stripped = removeFilter(f, toRemove);
        if (stripped == null) {
          changed = true;
        } else {
          changed |= stripped != f;
          remaining.add(stripped);
        }
      }
      if (!changed) {
        return filter;
      }
      if (remaining.isEmpty()) {
        return null;
      }
      if (remaining.size() == 1) {
        return remaining.get(0);
      }
      return new FilterList(filterList.getOperator(), remaining);
    }
    return filter;
  }

  /**
   * Constructs an iterator for a single probe batch, optionally recording iterator statistics and
   * scan split metadata on the plan.
   */
  protected ResultIterator createBatchIterator(ParallelScanGrouper scanGrouper, Scan batchScan,
    Map<ImmutableBytesPtr, ServerCache> caches, int batchLimit, boolean recordStats)
    throws SQLException {
    batchScan.setAttribute(BaseScannerRegionObserverConstants.NON_AGGREGATE_QUERY,
      QueryConstants.TRUE);
    BaseResultIterators iterators;
    if (isSerial) {
      iterators = new SerialIterators(this, null, null, parallelIteratorFactory, scanGrouper,
        batchScan, caches, dataPlan);
    } else {
      iterators = new ParallelIterators(this, null, parallelIteratorFactory, scanGrouper, batchScan,
        false, caches, dataPlan);
    }
    if (recordStats) {
      recordIteratorStats(iterators);
    }
    if (orderBy != null && !orderBy.getOrderByExpressions().isEmpty()) {
      return new MergeSortTopNResultIterator(iterators, batchLimit, null,
        orderBy.getOrderByExpressions());
    } else {
      return new LimitingResultIterator(new ConcatResultIterator(iterators), batchLimit);
    }
  }

  private class AdaptiveProbeResultIterator implements PeekingResultIterator {
    private final ParallelScanGrouper scanGrouper;
    private final Scan initialScan;
    private final Map<ImmutableBytesPtr, ServerCache> caches;
    private final int effectiveLimit;
    private List<Tuple> resultTuples = null;
    private int cursor = 0;
    private ResultIterator explainIterator = null;

    AdaptiveProbeResultIterator(ParallelScanGrouper scanGrouper, Scan initialScan,
      Map<ImmutableBytesPtr, ServerCache> caches) {
      this.scanGrouper = scanGrouper;
      this.initialScan = initialScan;
      this.caches = caches;
      this.effectiveLimit = (limit != null ? limit : 0) + (offset != null ? offset : 0);
    }

    private void init() throws SQLException {
      if (resultTuples != null) {
        return;
      }
      resultTuples = new ArrayList<>();
      List<Tuple> survivingTuples = new ArrayList<>();

      int totalCentroids = cachedCentroids != null ? cachedCentroids.getCentroidCount() : 0;
      // Centroid ranking is deferred until probe expansion is required.
      List<Integer> allRankedCentroids = null;
      Set<Integer> probedCentroidIds = new LinkedHashSet<>(probeCentroids);

      Integer saltBuckets = (getTableRef() != null && getTableRef().getTable() != null)
        ? getTableRef().getTable().getBucketNum()
        : null;
      PhoenixConnection conn = getContext() != null ? getContext().getConnection() : null;
      byte[] tenantIdBytes = extractTenantIdBytes(getTableRef(), conn);

      int batchesProbed = 0;
      int maxBatches = maxProbeLimit > 0 ? maxProbeLimit : Integer.MAX_VALUE;
      ScanRanges originalScanRanges = getContext() != null ? getContext().getScanRanges() : null;

      try {
        while (true) {
          ResultIterator batchIter;
          if (batchesProbed == 0) {
            batchIter = createBatchIterator(scanGrouper, initialScan, caches, effectiveLimit, true);
          } else {
            if (allRankedCentroids == null) {
              allRankedCentroids =
                selectProbes(cachedCentroids, queryVector, distanceMetric, totalCentroids);
            }
            // Collect unprobed centroids for the next batch in proximity order.
            List<Integer> batchCentroids = new ArrayList<>(probeCount);
            for (int centroidId : allRankedCentroids) {
              if (batchCentroids.size() >= probeCount) {
                break;
              }
              if (probedCentroidIds.add(centroidId)) {
                batchCentroids.add(centroidId);
              }
            }
            if (batchCentroids.isEmpty()) {
              break;
            }
            List<KeyRange> batchKeyRanges =
              buildCentroidKeyRanges(batchCentroids, saltBuckets, tenantIdBytes);
            ScanRanges batchScanRanges = ScanRanges.createCentroidScanRanges(batchKeyRanges);
            getContext().setScanRanges(batchScanRanges);

            Scan batchScan;
            try {
              batchScan = new Scan(initialScan);
            } catch (IOException e) {
              throw ClientUtil.parseServerException(e);
            }
            Filter baseFilter = removeFilter(initialScan.getFilter(), centroidSkipScanFilter);
            batchScan.setFilter(baseFilter);
            batchScanRanges.initializeScan(batchScan);
            if (batchScanRanges.useSkipScanFilter()) {
              ScanUtil.andFilterAtBeginning(batchScan, batchScanRanges.getSkipScanFilter());
            }
            batchIter = createBatchIterator(scanGrouper, batchScan, caches, effectiveLimit, false);
          }

          try {
            Tuple t;
            while ((t = batchIter.next()) != null) {
              survivingTuples.add(t);
            }
          } finally {
            batchIter.close();
          }

          batchesProbed++;
          if (
            survivingTuples.size() >= effectiveLimit || probedCentroidIds.size() >= totalCentroids
              || batchesProbed >= maxBatches
          ) {
            break;
          }
        }
      } finally {
        if (getContext() != null && originalScanRanges != null) {
          getContext().setScanRanges(originalScanRanges);
        }
      }

      lastProbedBatchCount = batchesProbed;
      lastProbedCentroidCount = probedCentroidIds.size();

      if (
        orderBy != null && orderBy.getOrderByExpressions() != null
          && !orderBy.getOrderByExpressions().isEmpty() && survivingTuples.size() > 1
      ) {
        survivingTuples
          .sort(MergeSortTopNResultIterator.newComparator(orderBy.getOrderByExpressions()));
      }

      int start = (offset != null && offset > 0) ? Math.min(offset, survivingTuples.size()) : 0;
      int end = (limit != null && limit > 0)
        ? Math.min(start + limit, survivingTuples.size())
        : survivingTuples.size();
      resultTuples = new ArrayList<>(survivingTuples.subList(start, end));
    }

    @Override
    public Tuple next() throws SQLException {
      init();
      if (cursor < resultTuples.size()) {
        return resultTuples.get(cursor++);
      }
      return null;
    }

    @Override
    public Tuple peek() throws SQLException {
      init();
      if (cursor < resultTuples.size()) {
        return resultTuples.get(cursor);
      }
      return null;
    }

    @Override
    public void close() throws SQLException {
      if (explainIterator != null) {
        explainIterator.close();
      }
    }

    @Override
    public void explain(List<String> planSteps) {
      try {
        if (explainIterator == null) {
          explainIterator =
            createBatchIterator(scanGrouper, initialScan, caches, effectiveLimit, true);
        }
        explainIterator.explain(planSteps);
      } catch (SQLException e) {
        LOGGER.warn("Failed to create explain iterator", e);
      }
    }

    @Override
    public void explain(List<String> planSteps,
      ExplainPlanAttributesBuilder explainPlanAttributesBuilder) {
      try {
        if (explainIterator == null) {
          explainIterator =
            createBatchIterator(scanGrouper, initialScan, caches, effectiveLimit, true);
        }
        explainIterator.explain(planSteps, explainPlanAttributesBuilder);
      } catch (SQLException e) {
        LOGGER.warn("Failed to create explain iterator", e);
      }
    }
  }

  /**
   * Determines whether adaptive probe expansion should be used to satisfy the query limit when a
   * relational filter is present.
   */
  public boolean isAdaptiveProbingApplicable() {
    return this.probing && limit != null && limit > 0 && queryVector != null
      && cachedCentroids != null && cachedCentroids.getCentroidCount() > 0 && statement != null
      && statement.getWhere() != null;
  }

  @Override
  protected ResultIterator newIterator(ParallelScanGrouper scanGrouper, Scan scan,
    Map<ImmutableBytesPtr, ServerCache> caches) throws SQLException {
    ResultIterator inner;
    if (isAdaptiveProbingApplicable()) {
      inner = new AdaptiveProbeResultIterator(scanGrouper, scan, caches);
    } else {
      inner = super.newIterator(scanGrouper, scan, caches);
    }
    if (!projectionTimeUncoveredLookup || getDataPlan() == null) {
      return inner;
    }
    return new DeferredProjectionResultIterator(inner);
  }

  private class DeferredProjectionResultIterator implements ResultIterator {
    private final ResultIterator inner;
    private List<Tuple> projectedTuples = null;
    private int cursor = 0;

    DeferredProjectionResultIterator(ResultIterator inner) {
      this.inner = inner;
    }

    private void init() throws SQLException {
      if (projectedTuples != null) {
        return;
      }
      projectedTuples = new ArrayList<>();
      List<Tuple> indexTuples = new ArrayList<>();
      Tuple tuple;
      while ((tuple = inner.next()) != null) {
        indexTuples.add(tuple);
      }
      QueryPlan effectiveDataPlan = getDataPlan();
      if (indexTuples.isEmpty() || effectiveDataPlan == null) {
        lastDeferredLookupCount = 0;
        return;
      }

      PTable indexTable = getTableRef().getTable();
      PTable dataTable = effectiveDataPlan.getTableRef().getTable();
      PhoenixConnection conn = getContext().getConnection();
      IndexMaintainer maintainer = indexTable.getIndexMaintainer(dataTable, conn);
      byte[][] viewConstants = IndexUtil.getViewConstants(dataTable);

      // Propagate the time range and projected column families from the compiled data plan scan.
      // This maintains snapshot isolation consistency with the index scan and restricts data table
      // gets to the required column families.
      Scan dataScan =
        effectiveDataPlan.getContext() != null ? effectiveDataPlan.getContext().getScan() : null;

      List<Get> gets = new ArrayList<>(indexTuples.size());
      ImmutableBytesWritable ptr = new ImmutableBytesWritable();
      for (Tuple indexTuple : indexTuples) {
        indexTuple.getKey(ptr);
        byte[] dataRowKey = maintainer.buildDataRowKey(ptr, viewConstants);
        Get get = new Get(dataRowKey);
        if (dataScan != null) {
          try {
            get.setTimeRange(dataScan.getTimeRange().getMin(), dataScan.getTimeRange().getMax());
          } catch (IOException e) {
            throw ClientUtil.parseServerException(e);
          }
          Map<byte[], NavigableSet<byte[]>> familyMap = dataScan.getFamilyMap();
          if (familyMap != null && !familyMap.isEmpty()) {
            for (Map.Entry<byte[], NavigableSet<byte[]>> entry : familyMap.entrySet()) {
              NavigableSet<byte[]> quals = entry.getValue();
              if (quals == null || quals.isEmpty()) {
                get.addFamily(entry.getKey());
              } else {
                for (byte[] qual : quals) {
                  get.addColumn(entry.getKey(), qual);
                }
              }
            }
          }
        }
        gets.add(get);
      }

      lastDeferredLookupCount = gets.size();

      TupleProjector tupleProjector = null;
      if (
        effectiveDataPlan.getContext() != null && effectiveDataPlan.getContext().getScan() != null
      ) {
        tupleProjector =
          TupleProjector.deserializeProjectorFromScan(effectiveDataPlan.getContext().getScan());
      }
      if (tupleProjector == null && dataTable.getType() == PTableType.PROJECTED) {
        tupleProjector = new TupleProjector(dataTable);
      }

      byte[] physicalTableName = dataTable.getPhysicalName().getBytes();
      try (Table hTable = conn.getQueryServices().getTable(physicalTableName)) {
        Result[] results = hTable.get(gets);
        for (Result res : results) {
          if (res != null && !res.isEmpty()) {
            Tuple rawTuple = new ResultTuple(res);
            if (tupleProjector != null) {
              projectedTuples.add(tupleProjector.projectResults(rawTuple, true));
            } else {
              projectedTuples.add(rawTuple);
            }
          }
        }
      } catch (IOException e) {
        throw ClientUtil.parseServerException(e);
      }
    }

    @Override
    public Tuple next() throws SQLException {
      init();
      if (cursor < projectedTuples.size()) {
        return projectedTuples.get(cursor++);
      }
      return null;
    }

    @Override
    public void close() throws SQLException {
      inner.close();
    }

    @Override
    public void explain(List<String> planSteps) {
      inner.explain(planSteps);
    }

    @Override
    public void explain(List<String> planSteps,
      ExplainPlanAttributesBuilder explainPlanAttributesBuilder) {
      inner.explain(planSteps, explainPlanAttributesBuilder);
    }
  }
}
