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
import org.apache.hadoop.hbase.HRegionLocation;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.filter.FilterList;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.util.Bytes;
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
import org.apache.phoenix.index.vector.CentroidManager;
import org.apache.phoenix.iterate.BaseResultIterators;
import org.apache.phoenix.iterate.MergeSortTopNResultIterator;
import org.apache.phoenix.iterate.ParallelIteratorFactory;
import org.apache.phoenix.iterate.ParallelIterators;
import org.apache.phoenix.iterate.ParallelScanGrouper;
import org.apache.phoenix.iterate.ResultIterator;
import org.apache.phoenix.iterate.ResultIterators;
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
import org.apache.phoenix.schema.PColumn;
import org.apache.phoenix.schema.PColumnFamily;
import org.apache.phoenix.schema.PTable;
import org.apache.phoenix.schema.SaltingUtil;
import org.apache.phoenix.schema.TableRef;
import org.apache.phoenix.schema.stats.GuidePostsInfo;
import org.apache.phoenix.schema.stats.GuidePostsKey;
import org.apache.phoenix.schema.tuple.Tuple;
import org.apache.phoenix.schema.types.PInteger;
import org.apache.phoenix.util.ClientUtil;
import org.apache.phoenix.util.CostUtil;
import org.apache.phoenix.util.MetaDataUtil;
import org.apache.phoenix.util.QueryUtil;
import org.apache.phoenix.util.ReadOnlyProps;
import org.apache.phoenix.util.ScanUtil;
import org.apache.phoenix.util.SchemaUtil;

import org.apache.phoenix.thirdparty.com.google.common.base.Optional;

/**
 * The scan plan for a vector search on an IVF vector index. The plan finds the centroids nearest
 * the query vector and limits the scan to their posting lists, under each salt bucket and tenant
 * prefix. Each region server scores its candidates and returns its top N rows in distance order.
 * The client merges these rows into the final top K.
 * <p>
 * If the probed posting lists hold too few rows, the plan can probe more posting lists in batches.
 * Each batch probes twice as many new posting lists as the batch before it. Thus the number of
 * serial batches grows with the logarithm of the probe width that the query needs. The batch limit
 * also sets a budget of posting lists: the limit times the probe count of the first batch. Thus the
 * doubling does not increase the work of a query whose filter passes very few rows. During a
 * migration, the plan probes both the active and the building generation.
 * <p>
 * A probe batch uses positional reads (PREAD) while its estimated bytes per index region scan are
 * at most a bound. The estimate comes from the index statistics. A larger batch, or a batch with no
 * estimate, keeps the default read type, so that a long region scan can switch to a stream read.
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
  private final int maxProbeBatches;
  private final CachedCentroids centroids;
  // Centroids of the building generation during a migration, otherwise null
  private final CachedCentroids buildingCentroids;
  private final float[] queryVector;
  // Skip scan filter for the posting lists of the first probe batch, or null. Each later adaptive
  // batch replaces this filter in its copy of the scan.
  private final SkipScanFilter postingListFilter;
  // Largest estimated bytes per index region scan for which a probe batch uses PREAD, or 0 or less
  // if no batch uses PREAD
  private final long preadMaxBytesPerRegion;
  // Estimated bytes of one posting list part at the last read type check, or null if there is no
  // estimate. An iterator keeps its own copy for its later batches.
  private Long listPartBytes;
  // The scan whose read type an earlier iteration of this plan set, or null
  private Scan boundedReadTypeScan;
  // The read type that the plan set on that scan
  private Scan.ReadType plannedReadType;

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
    CachedCentroids building = null;
    if (queryVector != null && index.getVectorCentroidGeneration() != null) {
      VectorCentroidCache cache =
        VectorCentroidCache.getInstance(connection.getQueryServices().getConfiguration());
      DistanceMetric metric = DistanceMetric.fromString(index.getVectorDistanceMetric());
      String indexName = CentroidManager.getCentroidIndexName(index.getName().getString());
      centroids = cache.get(connection, indexName, index.getVectorCentroidGeneration(), metric);
      // A migration writes the index rows under the building generation, whose centroid IDs are
      // disjoint from the IDs of the active generation. Until promotion, a scan can find a data
      // row under one generation or under both, so the scan must probe both generations. The
      // client returns each data row once.
      if (index.isVectorRebuildInProgress()) {
        building = cache.get(connection, indexName, index.getVectorBuildingGeneration(), metric);
      }
    }
    this.centroids = centroids;
    this.buildingCentroids = building;
    if (building != null) {
      // A data row under both generations can use two of the top N slots of a region. Each
      // region returns 2N rows, so that the client merge can keep N distinct data rows.
      serializeScanRegionObserverIntoScan(context.getScan(),
        (int) Math.min(2L * QueryUtil.getOffsetLimit(limit, offset), Integer.MAX_VALUE),
        orderBy.getOrderByExpressions(), projector.getEstimatedRowByteSize());
    }
    this.centroidCount =
      (centroids == null ? 0 : centroids.size()) + (building == null ? 0 : building.size());
    int[] probes = new int[0];
    SkipScanFilter filter = null;
    if (centroids != null) {
      int requested = getHintInt(hint, Hint.VECTOR_PROBE_COUNT, props.getInt(
        QueryServices.VECTOR_PROBE_COUNT_ATTRIB, QueryServicesOptions.DEFAULT_VECTOR_PROBE_COUNT));
      int count =
        requested > 0 ? requested : Math.max(1, (int) Math.round(Math.sqrt(centroids.size())));
      int[] nearest = nearestCentroids(count);
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
    this.preadMaxBytesPerRegion =
      props.getLongBytes(QueryServices.VECTOR_PREAD_MAX_BYTES_PER_REGION_ATTRIB,
        QueryServicesOptions.DEFAULT_VECTOR_PREAD_MAX_BYTES_PER_REGION);
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
      float[] vector = constantVector(child);
      if (vector != null) {
        return vector;
      }
    }
    return null;
  }

  /** Returns the value of a constant vector expression, or null if the expression is not one. */
  static float[] constantVector(Expression expression) {
    if (
      expression.isStateless() && expression.getDataType() != null
        && expression.getDataType().isVectorType()
    ) {
      ImmutableBytesWritable ptr = new ImmutableBytesWritable();
      if (expression.evaluate(null, ptr) && ptr.getLength() > 0) {
        return CachedCentroids.decode(ptr.get(), ptr.getOffset(), ptr.getLength(),
          expression.getDataType());
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
    return buildPostingListRanges(context, index, centroids);
  }

  /**
   * Makes scan ranges for the posting lists of the given centroids. A salted index gets a slot with
   * all salt buckets. A multitenant index gets a slot with the tenant ID of the connection, so the
   * caller must make sure that the connection has a tenant ID.
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

  /**
   * Returns the {@code n} centroids nearest the query vector in each probed generation, from the
   * nearest to the farthest. During a migration, the result interleaves the rankings of the two
   * generations. A prefix of length 2k then holds the k nearest centroids of each generation, if
   * each generation has at least k centroids.
   */
  private int[] nearestCentroids(int n) {
    int[] active = centroids.nearest(queryVector, Math.min(n, centroids.size()));
    if (buildingCentroids == null) {
      return active;
    }
    return interleave(active,
      buildingCentroids.nearest(queryVector, Math.min(n, buildingCentroids.size())));
  }

  /**
   * Merges two arrays in alternate order: {@code a[0]}, {@code b[0]}, {@code a[1]}, and so on. The
   * other elements of the longer array go at the end.
   */
  public static int[] interleave(int[] a, int[] b) {
    int[] out = new int[a.length + b.length];
    int i = 0;
    int j = 0;
    int k = 0;
    while (i < a.length || j < b.length) {
      if (i < a.length) {
        out[k++] = a[i++];
      }
      if (j < b.length) {
        out[k++] = b[j++];
      }
    }
    return out;
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

  /**
   * Estimates the cost of the vector index scan. If the index has guideposts, the cost comes from
   * the scan ranges of the probed posting lists. Without index statistics, the estimate scales the
   * data table statistics by the probed centroid fraction and by the ratio of the row sizes. If the
   * query has a limit, this estimate also adds the cost of the server top-N sort. For an uncovered
   * index, the cost also includes a data table lookup for each candidate row. Returns
   * {@link Cost#UNKNOWN} if the statistics cannot supply an estimate.
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
    String metric = getTableRef().getTable().getVectorDistanceMetric();
    List<String> steps = new ArrayList<>(explainPlan.getPlanSteps());
    if (probeCount == 0) {
      return explainPlan;
    }
    String probing =
      "CLIENT PROBING " + probeCount + " OF " + centroidCount + " CENTROIDS (" + metric + ")";
    if (buildingCentroids != null) {
      probing += " ACROSS ACTIVE AND BUILDING GENERATIONS";
    }
    if (isAdaptive()) {
      probing += " EXPANDING UP TO " + coveringBatches() + " BATCHES";
    }
    steps.add(0, probing);
    ExplainPlanAttributes attributes = explainPlan.getPlanStepsAsAttributes();
    return new ExplainPlan(steps,
      (attributes == null
        ? new ExplainPlanAttributesBuilder()
        : new ExplainPlanAttributesBuilder(attributes)).setVectorProbeCount(probeCount)
          .setVectorCentroidCount(centroidCount).setVectorDistanceMetric(metric).build());
  }

  // Returns the number of batches that use the full probe budget. This is not more than the batch
  // limit, because the widths double and the budget is the limit times the first width
  private int coveringBatches() {
    int batches = 1;
    long width = probeCount;
    long probed = probeCount;
    while (probed < probeBudget() && batches < maxProbeBatches) {
      width = nextBatchWidth(width, probed);
      probed += width;
      batches++;
    }
    return batches;
  }

  // Returns the maximum number of centroids that one query probes, in all its batches
  private long probeBudget() {
    return Math.min((long) maxProbeBatches * probeCount, centroidCount);
  }

  // Returns the maximum number of new centroids of the batch after a batch of the given width. The
  // batch is smaller if fewer centroids remain
  private long nextBatchWidth(long width, long probed) {
    return Math.min(width * 2, probeBudget() - probed);
  }

  /**
   * Returns true if execution can scan more centroid posting lists when the probed lists hold fewer
   * rows than the limit. A filter can cause this. A tenant scope can also cause this, because all
   * tenants share the centroids. The plan must have a LIMIT, must probe fewer centroids than the
   * index has, and must allow more than one probe batch.
   */
  public boolean isAdaptive() {
    return limit != null && probeCount > 0 && probeCount < centroidCount && maxProbeBatches > 1;
  }

  /**
   * Returns the read type for a probe batch of {@code parts} posting list parts over
   * {@code regions} index regions. A short region scan uses PREAD, which does not reopen its store
   * files for a stream read. A long region scan keeps the default read type, so that the region
   * server can switch it to a stream read. The rule assumes that the probed rows spread evenly over
   * the regions. A region scan reads at least one part. Thus the estimated mean bytes of a region
   * scan are {@code bytesPerPart * max(1, parts / regions)}. A null {@code bytesPerPart} means that
   * there is no estimate. No estimate, or a bound of 0 or less, gives the default read type.
   */
  static Scan.ReadType boundedReadType(long parts, int regions, Long bytesPerPart,
    long maxBytesPerRegion) {
    return maxBytesPerRegion > 0 && bytesPerPart != null
      && bytesPerPart * Math.max(1.0, (double) parts / regions) <= maxBytesPerRegion
        ? Scan.ReadType.PREAD
        : Scan.ReadType.DEFAULT;
  }

  // Returns the number of parts of one posting list. A salted index has one part of each list in
  // each salt bucket. An index without salt has the full list as one part.
  private int listParts() {
    Integer buckets = getTableRef().getTable().getBucketNum();
    return buckets == null ? 1 : buckets;
  }

  /**
   * Returns the read type for a probe batch of {@code width} posting lists over {@code regions}
   * index regions, from the estimate {@code listPartBytes} of one list part.
   */
  private Scan.ReadType batchReadType(long width, int regions, Long listPartBytes) {
    return boundedReadType(width * listParts(), regions, listPartBytes, preadMaxBytesPerRegion);
  }

  /**
   * Returns the estimated bytes of one posting list part, or null if there is no estimate. The
   * estimate is the index size from the statistics divided by the part count of the probed
   * generations. A failed statistics read gives no estimate.
   */
  private Long estimateListPartBytes() {
    Long indexBytes;
    try {
      indexBytes = getIndexBytes(context.getConnection(), getTableRef().getTable());
    } catch (SQLException e) {
      return null;
    }
    long parts = (long) centroidCount * listParts();
    if (indexBytes == null || parts <= 0) {
      return null;
    }
    return indexBytes / parts + (indexBytes % parts == 0 ? 0 : 1);
  }

  /**
   * Returns the estimated bytes of the index from the guideposts in the statistics cache of the
   * connection, or null if there is no estimate. Each index row has a cell in the empty column
   * family, so that family must have guideposts. Another family adds the bytes of its guideposts,
   * if it has some. An empty guidepost gives no bytes, because its byte count is the guidepost
   * width and not the data size. The total can be low, because the statistics can be stale and do
   * not count the bytes after the last guidepost of a region.
   */
  static Long getIndexBytes(PhoenixConnection connection, PTable index) throws SQLException {
    byte[] physicalName = index.getPhysicalName().getBytes();
    byte[] emptyFamily = SchemaUtil.getEmptyColumnFamily(index);
    Long total = getGuidePostBytes(connection, physicalName, emptyFamily);
    if (total == null) {
      return null;
    }
    for (PColumnFamily family : index.getColumnFamilies()) {
      byte[] name = family.getName().getBytes();
      if (!Bytes.equals(name, emptyFamily)) {
        Long bytes = getGuidePostBytes(connection, physicalName, name);
        total += bytes == null ? 0 : bytes;
      }
    }
    return total > 0 ? total : null;
  }

  // Returns the guidepost bytes of one column family, or null if the family has no guideposts
  private static Long getGuidePostBytes(PhoenixConnection connection, byte[] physicalName,
    byte[] family) throws SQLException {
    GuidePostsInfo info =
      connection.getQueryServices().getTableStats(new GuidePostsKey(physicalName, family));
    if (info == null || info.getGuidePostsCount() == 0 || info.isEmptyGuidePost()) {
      return null;
    }
    long total = 0;
    for (long bytes : info.getByteCounts()) {
      total += bytes;
    }
    return total;
  }

  /**
   * Returns the index region count if the bound selects the read type of the probe batches,
   * otherwise 0. The bound does not apply if the caller set a read type, or if the plan probes no
   * posting lists. A read type that an earlier iteration of this plan set is not a caller choice.
   * The bound also does not apply to a scan other than the statement scan. For example, a retry of
   * one region scan keeps the read type of that region scan. The method also updates the estimated
   * bytes of one posting list part. If the region count cannot change the read type of a batch, the
   * method returns 1 and does not look up the regions.
   */
  int getBoundedReadTypeRegions(ParallelScanGrouper scanGrouper, Scan scan) throws SQLException {
    boolean ownReadType = scan == boundedReadTypeScan && scan.getReadType() == plannedReadType;
    if (
      preadMaxBytesPerRegion <= 0 || probeCount == 0 || scan != context.getScan()
        || (!ownReadType && scan.getReadType() != Scan.ReadType.DEFAULT)
    ) {
      return 0;
    }
    listPartBytes = estimateListPartBytes();
    // All batches use the default read type for any region count. The plan still sets it, because
    // an earlier iteration can have set PREAD on the scan.
    if (listPartBytes == null || listPartBytes > preadMaxBytesPerRegion) {
      return 1;
    }
    // All batches use PREAD for any region count
    long widest = isAdaptive() ? probeBudget() : probeCount;
    if ((double) listPartBytes * widest * listParts() <= preadMaxBytesPerRegion) {
      return 1;
    }
    return getProbedRegions(scanGrouper);
  }

  /**
   * Returns the number of index regions that can hold posting lists of the probed generations. An
   * older generation has lower centroid IDs. Thus the count does not include regions that hold only
   * rows of older generations. The regions come from the cached region locations of the index.
   */
  private int getProbedRegions(ParallelScanGrouper scanGrouper) throws SQLException {
    PTable index = getTableRef().getTable();
    int low = centroids.getFirstId();
    int high = low + centroids.size() - 1;
    if (buildingCentroids != null) {
      low = Math.min(low, buildingCentroids.getFirstId());
      high = Math.max(high, buildingCentroids.getFirstId() + buildingCentroids.size() - 1);
    }
    KeyRange probed =
      buildPostingListRanges(context, index, new int[] { low, high }).getScanRange();
    int regions = 0;
    for (HRegionLocation location : scanGrouper.getRegionBoundaries(context,
      index.getPhysicalName().getBytes())) {
      KeyRange region =
        KeyRange.getKeyRange(location.getRegion().getStartKey(), location.getRegion().getEndKey());
      if (probed.intersect(region) != KeyRange.EMPTY_RANGE) {
        regions++;
      }
    }
    return Math.max(1, regions);
  }

  @Override
  protected ResultIterator newIterator(ParallelScanGrouper scanGrouper, Scan scan,
    Map<ImmutableBytesPtr, ServerCache> caches) throws SQLException {
    // Set the read type of the first batch before the batch copies the scan for each region
    int regions = getBoundedReadTypeRegions(scanGrouper, scan);
    if (regions > 0) {
      plannedReadType = batchReadType(probeCount, regions, listPartBytes);
      scan.setReadType(plannedReadType);
      boundedReadTypeScan = scan;
    }
    if (!isAdaptive() && buildingCentroids == null) {
      return super.newIterator(scanGrouper, scan, caches);
    }
    scan.setAttribute(BaseScannerRegionObserverConstants.NON_AGGREGATE_QUERY, QueryConstants.TRUE);
    // Make the first probe batch now, so that the plan records its estimates and splits
    BaseResultIterators first = newBatch(scanGrouper, scan, caches);
    recordIteratorStats(first);
    return new AdaptiveProbeResultIterator(scanGrouper, scan, caches, first, regions,
      listPartBytes);
  }

  /**
   * Returns a copy of {@code scan} for a later probe batch that reads the posting lists of
   * {@code ranges}. If {@code regions} is more than 0, the bound selects the read type of the copy
   * from the batch width and the estimate {@code listPartBytes} of one posting list part. Otherwise
   * the copy keeps the read type of {@code scan}.
   */
  Scan newBatchScan(Scan scan, ScanRanges ranges, int width, int regions, Long listPartBytes)
    throws SQLException {
    Scan batchScan;
    try {
      batchScan = new Scan(scan);
    } catch (IOException e) {
      throw ClientUtil.parseServerException(e);
    }
    batchScan.setFilter(replacePostingListFilter(scan.getFilter(),
      ranges.useSkipScanFilter() ? ranges.getSkipScanFilter() : null));
    ranges.initializeScan(batchScan);
    if (regions > 0) {
      batchScan.setReadType(batchReadType(width, regions, listPartBytes));
    }
    return batchScan;
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
   * Returns the scan filter with {@code next} in place of the posting list filter of the first
   * batch. A null {@code next} removes that filter. The other filters keep their structure and
   * position. The method finds the posting list filter only at the top level or in a top level
   * FilterList. If it does not find the filter, it puts {@code next} first with MUST_PASS_ALL.
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
   * Result iterator that probes posting lists in successive batches until it has enough rows for
   * the limit, or a probe bound stops it. It then sorts all candidates on the client. While a
   * building generation is probed, a plan that is not adaptive also uses this iterator, so that it
   * returns each data row once.
   */
  private class AdaptiveProbeResultIterator implements ResultIterator {
    private final ParallelScanGrouper scanGrouper;
    private final Scan scan;
    private final Map<ImmutableBytesPtr, ServerCache> caches;
    private final BaseResultIterators first;
    private final int batchLimit;
    // Keys of the data rows found so far if a building generation is probed, otherwise null
    private final Set<ImmutableBytesPtr> dataRowKeys;
    private final int centroidPosition;
    // Index region count for the read type bound of each batch, or 0 to keep the read type
    private final int regions;
    // Estimated bytes of one posting list part for the read type bound, or null
    private final Long listPartBytes;
    private List<Tuple> rows;
    private int next;

    AdaptiveProbeResultIterator(ParallelScanGrouper scanGrouper, Scan scan,
      Map<ImmutableBytesPtr, ServerCache> caches, BaseResultIterators first, int regions,
      Long listPartBytes) {
      this.scanGrouper = scanGrouper;
      this.scan = scan;
      this.caches = caches;
      this.first = first;
      this.batchLimit = limit + (offset == null ? 0 : offset);
      this.dataRowKeys = buildingCentroids == null ? null : new HashSet<>();
      this.centroidPosition = getCentroidPosition(getTableRef().getTable());
      this.regions = regions;
      this.listPartBytes = listPartBytes;
    }

    private List<OrderByExpression> orderByExpressions() {
      return orderBy.getOrderByExpressions();
    }

    private void drain(ResultIterators batch, List<Tuple> into) throws SQLException {
      VectorIndexScanPlan.drain(batch, batchLimit, orderByExpressions(), dataRowKeys,
        getTableRef().getTable(), centroidPosition, into);
    }

    private void probe() throws SQLException {
      List<Tuple> found = new ArrayList<>();
      drain(first, found);
      Set<Integer> probed = new HashSet<>();
      for (int c : probeCentroids) {
        probed.add(c);
      }
      int[] ranked = null;
      int batches = 1;
      long width = probeCount;
      ScanRanges original = context.getScanRanges();
      try {
        while (
          isAdaptive() && found.size() < batchLimit && probed.size() < centroidCount
            && probed.size() < probeBudget() && batches < maxProbeBatches
        ) {
          if (ranked == null) {
            ranked = nearestCentroids(centroidCount);
          }
          width = nextBatchWidth(width, probed.size());
          List<Integer> batch = new ArrayList<>((int) width);
          for (int i = 0; i < ranked.length && batch.size() < width; i++) {
            if (probed.add(ranked[i])) {
              batch.add(ranked[i]);
            }
          }
          int[] ids = new int[batch.size()];
          for (int i = 0; i < ids.length; i++) {
            ids[i] = batch.get(i);
          }
          ScanRanges ranges = buildPostingListRanges(context, getTableRef().getTable(), ids);
          Scan batchScan = newBatchScan(scan, ranges, ids.length, regions, listPartBytes);
          // The iterators of this batch read their scan ranges from the statement context
          context.setScanRanges(ranges);
          drain(newBatch(scanGrouper, batchScan, caches), found);
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

  /** Returns the position of the centroid ID column in the primary key of the index. */
  static int getCentroidPosition(PTable index) {
    List<PColumn> pk = index.getPKColumns();
    for (int i = 0; i < pk.size(); i++) {
      if (MetaDataUtil.VECTOR_CENTROID_ID_COLUMN_NAME.equals(pk.get(i).getName().getString())) {
        return i;
      }
    }
    throw new IllegalStateException(index.getName() + " has no centroid id column");
  }

  /**
   * Merges the per-region results of one batch in sort order and adds up to {@code max} rows to
   * {@code into}. If {@code dataRowKeys} is not null, the merge skips a data row that it already
   * found under the other generation. In that case the merge does not stop at {@code max} rows, so
   * that a skipped copy does not use the place of a distinct row. Each region returns at most twice
   * {@code max} rows, which hold at least {@code max} distinct data rows if the region has them.
   */
  static void drain(ResultIterators batch, int max, List<OrderByExpression> orderBy,
    Set<ImmutableBytesPtr> dataRowKeys, PTable index, int centroidPosition, List<Tuple> into)
    throws SQLException {
    ResultIterator merged =
      new MergeSortTopNResultIterator(batch, dataRowKeys == null ? max : null, null, orderBy);
    try {
      Tuple t;
      for (int added = 0; added < max && (t = merged.next()) != null;) {
        // Add a data row only once, also if the scan finds it under both generations
        if (dataRowKeys == null || dataRowKeys.add(getDataRowKey(t, index, centroidPosition))) {
          into.add(t);
          added++;
        }
      }
    } finally {
      merged.close();
    }
  }

  /**
   * Returns the index row key without its salt byte and centroid ID. The result identifies the data
   * row, whichever generation, centroid, and salt byte its index row has.
   */
  static ImmutableBytesPtr getDataRowKey(Tuple tuple, PTable index, int centroidPosition) {
    ImmutableBytesWritable ptr = new ImmutableBytesWritable();
    tuple.getKey(ptr);
    byte[] row = ptr.get();
    int start =
      ptr.getOffset() + (index.getBucketNum() == null ? 0 : SaltingUtil.NUM_SALTING_BYTES);
    int end = ptr.getOffset() + ptr.getLength();
    // Set ptr to the centroid ID field, which has a fixed width
    index.getRowKeySchema().iterator(row, ptr.getOffset(), ptr.getLength(), ptr,
      centroidPosition + 1);
    int prefix = ptr.getOffset() - start;
    int suffix = end - ptr.getOffset() - ptr.getLength();
    byte[] key = new byte[prefix + suffix];
    System.arraycopy(row, start, key, 0, prefix);
    System.arraycopy(row, ptr.getOffset() + ptr.getLength(), key, prefix, suffix);
    return new ImmutableBytesPtr(key);
  }

  /** Orders tuples on the client by the compiled ORDER BY expressions and their null order. */
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

  /** Returns the maximum number of probe batches, including the first, that a query can scan. */
  public int getMaxProbeBatches() {
    return maxProbeBatches;
  }

  /**
   * Returns the number of posting lists that the first probe batch scans, or 0 if the plan scans
   * the whole index.
   */
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
