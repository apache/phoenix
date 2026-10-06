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
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.filter.FilterList;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
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
import org.apache.phoenix.expression.Expression;
import org.apache.phoenix.expression.OrderByExpression;
import org.apache.phoenix.expression.function.DistanceFunction;
import org.apache.phoenix.filter.SkipScanFilter;
import org.apache.phoenix.hbase.index.util.ImmutableBytesPtr;
import org.apache.phoenix.iterate.BaseResultIterators;
import org.apache.phoenix.iterate.MergeSortTopNResultIterator;
import org.apache.phoenix.iterate.ParallelIteratorFactory;
import org.apache.phoenix.iterate.ParallelIterators;
import org.apache.phoenix.iterate.ParallelScanGrouper;
import org.apache.phoenix.iterate.ResultIterator;
import org.apache.phoenix.iterate.SerialIterators;
import org.apache.phoenix.jdbc.PhoenixConnection;
import org.apache.phoenix.optimize.Cost;
import org.apache.phoenix.optimize.DistanceMetric;
import org.apache.phoenix.parse.FilterableStatement;
import org.apache.phoenix.parse.HintNode;
import org.apache.phoenix.parse.HintNode.Hint;
import org.apache.phoenix.query.KeyRange;
import org.apache.phoenix.query.QueryConstants;
import org.apache.phoenix.query.QueryServices;
import org.apache.phoenix.query.QueryServicesOptions;
import org.apache.phoenix.schema.PTable;
import org.apache.phoenix.schema.SaltingUtil;
import org.apache.phoenix.schema.TableRef;
import org.apache.phoenix.schema.tuple.Tuple;
import org.apache.phoenix.schema.types.PInteger;
import org.apache.phoenix.util.ClientUtil;
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
  private final int maxProbeBatches;
  private final CachedCentroids centroids;
  private final float[] queryVector;
  // Active posting list skip scan filter, updated across successive adaptive probing batches
  private final SkipScanFilter postingListFilter;

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

    this.queryVector = getQueryVector(orderBy);
    CachedCentroids centroids = null;
    if (queryVector != null && index.getVectorCentroidGeneration() != null) {
      centroids = VectorCentroidCache.getInstance(connection.getQueryServices().getConfiguration())
        .get(connection, index.getName().getString(), index.getVectorCentroidGeneration(),
          DistanceMetric.fromString(index.getVectorDistanceMetric()));
    }
    this.centroids = centroids;
    this.centroidCount = centroids == null ? 0 : centroids.size();
    int[] probes = new int[0];
    SkipScanFilter filter = null;
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
          filter = ranges.getSkipScanFilter();
          ScanUtil.andFilterAtBeginning(context.getScan(), filter);
        }
      }
    }
    this.probeCentroids = probes;
    this.probeCount = probes.length;
    this.postingListFilter = filter;
    this.maxProbeBatches = getHintInt(hint, Hint.MAX_PROBE_LIMIT,
      props.getInt(QueryServices.VECTOR_MAX_PROBE_LIMIT_ATTRIB,
        QueryServicesOptions.DEFAULT_VECTOR_MAX_PROBE_LIMIT));
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
    return buildPostingListRanges(context, index, centroids);
  }

  /**
   * Constructs scan ranges spanning candidate centroid posting lists across tenant and salting key
   * prefixes.
   */
  private static ScanRanges buildPostingListRanges(StatementContext context, PTable index,
    int[] centroids) throws SQLException {
    boolean isSalted = index.getBucketNum() != null;
    boolean scopeTenant = index.isMultiTenant();
    List<List<KeyRange>> slots = new ArrayList<>(3);
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
    String probing =
      "CLIENT PROBING " + probeCount + " OF " + centroidCount + " CENTROIDS (" + metric + ")";
    if (isAdaptive()) {
      int batches = Math.min(maxProbeBatches, (centroidCount + probeCount - 1) / probeCount);
      probing += " EXPANDING UP TO " + batches + " BATCHES";
    }
    steps.add(0, probing);
    ExplainPlanAttributes attributes = explainPlan.getPlanStepsAsAttributes();
    return new ExplainPlan(steps,
      (attributes == null
        ? new ExplainPlanAttributesBuilder()
        : new ExplainPlanAttributesBuilder(attributes)).setVectorProbeCount(probeCount)
          .setVectorCentroidCount(centroidCount).setVectorDistanceMetric(metric).build());
  }

  /**
   * Indicates whether query execution requires adaptive batch expansion over centroid posting lists
   * due to non-exhaustive initial probing and filter selectivity.
   */
  public boolean isAdaptive() {
    return limit != null && probeCount > 0 && probeCount < centroidCount
      && statement.getWhere() != null && maxProbeBatches > 1;
  }

  @Override
  protected ResultIterator newIterator(ParallelScanGrouper scanGrouper, Scan scan,
    Map<ImmutableBytesPtr, ServerCache> caches) throws SQLException {
    if (!isAdaptive()) {
      return super.newIterator(scanGrouper, scan, caches);
    }
    scan.setAttribute(BaseScannerRegionObserverConstants.NON_AGGREGATE_QUERY, QueryConstants.TRUE);
    // Initialize initial probe batch iterators to capture baseline plan statistics and splits
    BaseResultIterators first = newBatch(scanGrouper, scan, caches);
    recordIteratorStats(first);
    return new AdaptiveProbeResultIterator(scanGrouper, scan, caches, first);
  }

  private BaseResultIterators newBatch(ParallelScanGrouper scanGrouper, Scan scan,
    Map<ImmutableBytesPtr, ServerCache> caches) throws SQLException {
    return isSerial
      ? new SerialIterators(this, null, null, parallelIteratorFactory, scanGrouper, scan, caches,
        dataPlan)
      : new ParallelIterators(this, null, parallelIteratorFactory, scanGrouper, scan, false, caches,
        dataPlan);
  }

  /**
   * Replaces the posting list filter within the scan filter tree with the target batch filter,
   * preserving filter structure and position.
   */
  private Filter replacePostingListFilter(Filter filter, SkipScanFilter next) {
    if (filter == postingListFilter) {
      return next;
    }
    if (filter instanceof FilterList) {
      List<Filter> filters = new ArrayList<>(((FilterList) filter).getFilters());
      int i = filters.indexOf(postingListFilter);
      if (i >= 0) {
        if (next == null) {
          filters.remove(i);
        } else {
          filters.set(i, next);
        }
        return new FilterList(((FilterList) filter).getOperator(), filters);
      }
    }
    if (next == null) {
      return filter;
    }
    return filter == null ? next : new FilterList(FilterList.Operator.MUST_PASS_ALL, next, filter);
  }

  /**
   * Result iterator executing adaptive multi-batch scans across successive centroid posting lists,
   * aggregating and globally reranking candidate tuples to satisfy filter and limit criteria.
   */
  private class AdaptiveProbeResultIterator implements ResultIterator {
    private final ParallelScanGrouper scanGrouper;
    private final Scan scan;
    private final Map<ImmutableBytesPtr, ServerCache> caches;
    private final BaseResultIterators first;
    private final int batchLimit;
    private List<Tuple> rows;
    private int next;

    AdaptiveProbeResultIterator(ParallelScanGrouper scanGrouper, Scan scan,
      Map<ImmutableBytesPtr, ServerCache> caches, BaseResultIterators first) {
      this.scanGrouper = scanGrouper;
      this.scan = scan;
      this.caches = caches;
      this.first = first;
      this.batchLimit = limit + (offset == null ? 0 : offset);
    }

    private List<OrderByExpression> orderByExpressions() {
      return orderBy.getOrderByExpressions();
    }

    private void drain(ResultIterator batch, List<Tuple> into) throws SQLException {
      try {
        for (Tuple t = batch.next(); t != null; t = batch.next()) {
          into.add(t);
        }
      } finally {
        batch.close();
      }
    }

    private void probe() throws SQLException {
      List<Tuple> found = new ArrayList<>();
      drain(new MergeSortTopNResultIterator(first, batchLimit, null, orderByExpressions()), found);
      Set<Integer> probed = new HashSet<>();
      for (int c : probeCentroids) {
        probed.add(c);
      }
      int[] ranked = null;
      int batches = 1;
      ScanRanges original = context.getScanRanges();
      try {
        while (
          found.size() < batchLimit && probed.size() < centroidCount && batches < maxProbeBatches
        ) {
          if (ranked == null) {
            ranked = centroids.nearest(queryVector, centroidCount);
          }
          List<Integer> batch = new ArrayList<>(probeCount);
          for (int i = 0; i < ranked.length && batch.size() < probeCount; i++) {
            if (probed.add(ranked[i])) {
              batch.add(ranked[i]);
            }
          }
          int[] ids = new int[batch.size()];
          for (int i = 0; i < ids.length; i++) {
            ids[i] = batch.get(i);
          }
          ScanRanges ranges = buildPostingListRanges(context, getTableRef().getTable(), ids);
          Scan batchScan;
          try {
            batchScan = new Scan(scan);
          } catch (IOException e) {
            throw ClientUtil.parseServerException(e);
          }
          batchScan.setFilter(replacePostingListFilter(scan.getFilter(),
            ranges.useSkipScanFilter() ? ranges.getSkipScanFilter() : null));
          ranges.initializeScan(batchScan);
          // Synchronize context scan ranges for subsequent iterator construction
          context.setScanRanges(ranges);
          drain(new MergeSortTopNResultIterator(newBatch(scanGrouper, batchScan, caches),
            batchLimit, null, orderByExpressions()), found);
          batches++;
        }
      } finally {
        context.setScanRanges(original);
      }
      found.sort(new TupleComparator(orderByExpressions()));
      int from = Math.min(offset == null ? 0 : offset, found.size());
      rows = found.subList(from, Math.min(from + limit, found.size()));
    }

    @Override
    public Tuple next() throws SQLException {
      if (rows == null) {
        probe();
      }
      return next < rows.size() ? rows.get(next++) : null;
    }

    @Override
    public void close() throws SQLException {
      if (rows == null) {
        first.close();
      }
    }

    @Override
    public void explain(List<String> planSteps) {
      new MergeSortTopNResultIterator(first, limit, offset, orderByExpressions())
        .explain(planSteps);
    }

    @Override
    public void explain(List<String> planSteps,
      ExplainPlanAttributesBuilder explainPlanAttributesBuilder) {
      new MergeSortTopNResultIterator(first, limit, offset, orderByExpressions()).explain(planSteps,
        explainPlanAttributesBuilder);
    }
  }

  /** Comparator enforcing client side tuple ordering matching compiled ORDER BY expressions. */
  private static final class TupleComparator implements java.util.Comparator<Tuple> {
    private final List<OrderByExpression> orderBy;
    private final ImmutableBytesWritable ptr1 = new ImmutableBytesWritable();
    private final ImmutableBytesWritable ptr2 = new ImmutableBytesWritable();

    TupleComparator(List<OrderByExpression> orderBy) {
      this.orderBy = orderBy;
    }

    @Override
    public int compare(Tuple t1, Tuple t2) {
      for (OrderByExpression order : orderBy) {
        boolean isNull1 = !order.getExpression().evaluate(t1, ptr1) || ptr1.getLength() == 0;
        boolean isNull2 = !order.getExpression().evaluate(t2, ptr2) || ptr2.getLength() == 0;
        if (isNull1 || isNull2) {
          if (isNull1 != isNull2) {
            return isNull1 == order.isNullsLast() ? 1 : -1;
          }
          continue;
        }
        int c = ptr1.compareTo(ptr2);
        if (c != 0) {
          return order.isAscending() ? c : -c;
        }
      }
      return 0;
    }
  }

  /** Maximum number of probe batches permitted during adaptive centroid expansion. */
  public int getMaxProbeBatches() {
    return maxProbeBatches;
  }

  /** The number of posting lists the plan scans, or 0 when it scans the whole index. */
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
