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
import java.text.Format;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Iterator;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.hadoop.hbase.HRegionLocation;
import org.apache.hadoop.hbase.client.Consistency;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.filter.FirstKeyOnlyFilter;
import org.apache.hadoop.hbase.filter.PageFilter;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.io.TimeRange;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.phoenix.compile.ColumnProjector;
import org.apache.phoenix.compile.ExplainPlanAttributes;
import org.apache.phoenix.compile.ExplainPlanAttributes.ExplainFilter;
import org.apache.phoenix.compile.ExplainPlanAttributes.ExplainPlanAttributesBuilder;
import org.apache.phoenix.compile.GroupByCompiler.GroupBy;
import org.apache.phoenix.compile.OrderByCompiler.OrderBy;
import org.apache.phoenix.compile.RowProjector;
import org.apache.phoenix.compile.ScanRanges;
import org.apache.phoenix.compile.StatementContext;
import org.apache.phoenix.compile.StatementPlan;
import org.apache.phoenix.coprocessorclient.BaseScannerRegionObserverConstants;
import org.apache.phoenix.expression.AndExpression;
import org.apache.phoenix.expression.Expression;
import org.apache.phoenix.expression.function.BsonConditionExpressionFunction;
import org.apache.phoenix.expression.function.JsonExistsFunction;
import org.apache.phoenix.filter.BooleanExpressionFilter;
import org.apache.phoenix.filter.DistinctPrefixFilter;
import org.apache.phoenix.filter.EmptyColumnOnlyFilter;
import org.apache.phoenix.optimize.OptimizerDecision;
import org.apache.phoenix.optimize.OptimizerReasons;
import org.apache.phoenix.optimize.RejectedIndexEntry;
import org.apache.phoenix.parse.HintNode;
import org.apache.phoenix.parse.HintNode.Hint;
import org.apache.phoenix.query.KeyRange;
import org.apache.phoenix.query.KeyRange.Bound;
import org.apache.phoenix.query.QueryConstants;
import org.apache.phoenix.query.QueryServices;
import org.apache.phoenix.query.QueryServicesOptions;
import org.apache.phoenix.schema.PColumn;
import org.apache.phoenix.schema.PTable;
import org.apache.phoenix.schema.PTableType;
import org.apache.phoenix.schema.SortOrder;
import org.apache.phoenix.schema.TableRef;
import org.apache.phoenix.schema.types.PDataType;
import org.apache.phoenix.schema.types.PInteger;
import org.apache.phoenix.util.CDCUtil;
import org.apache.phoenix.util.MetaDataUtil;
import org.apache.phoenix.util.ScanUtil;
import org.apache.phoenix.util.StringUtil;

public abstract class ExplainTable {

  private static final List<KeyRange> EVERYTHING =
    Collections.singletonList(KeyRange.EVERYTHING_RANGE);
  public static final String POINT_LOOKUP_ON_STRING = "POINT LOOKUP ON ";
  public static final String REGION_LOCATIONS = " (region locations = ";

  protected final StatementContext context;
  protected final TableRef tableRef;
  protected final GroupBy groupBy;
  protected final OrderBy orderBy;
  protected final HintNode hint;
  protected final Integer limit;
  protected final Integer offset;

  public ExplainTable(StatementContext context, TableRef table) {
    this(context, table, GroupBy.EMPTY_GROUP_BY, OrderBy.EMPTY_ORDER_BY, HintNode.EMPTY_HINT_NODE,
      null, null);
  }

  public ExplainTable(StatementContext context, TableRef table, GroupBy groupBy, OrderBy orderBy,
    HintNode hintNode, Integer limit, Integer offset) {
    this.context = context;
    this.tableRef = table;
    this.groupBy = groupBy;
    this.orderBy = orderBy;
    this.hint = hintNode;
    this.limit = limit;
    this.offset = offset;
  }

  private String explainSkipScan() {
    StringBuilder buf = new StringBuilder();
    ScanRanges scanRanges = context.getScanRanges();
    if (scanRanges.isPointLookup()) {
      int keyCount = scanRanges.getPointLookupCount();
      buf.append(POINT_LOOKUP_ON_STRING + keyCount + " KEY" + (keyCount > 1 ? "S " : " "));
    } else if (scanRanges.useSkipScanFilter()) {
      buf.append("SKIP SCAN ");
      int count = 1;
      boolean hasRanges = false;
      int nSlots = scanRanges.getBoundSlotCount();
      for (int i = 0; i < nSlots; i++) {
        List<KeyRange> ranges = scanRanges.getRanges().get(i);
        count *= ranges.size();
        for (KeyRange range : ranges) {
          hasRanges |= !range.isSingleKey();
        }
      }
      buf.append("ON ");
      buf.append(count);
      buf.append(hasRanges ? " RANGE" : " KEY");
      buf.append(count > 1 ? "S " : " ");
    } else {
      buf.append("RANGE SCAN ");
    }
    return buf.toString();
  }

  /**
   * Number of region scan splits the plan will hit, used to render the {@code REGIONS PLANNED}
   * per-scan line.
   * @return the split count, or 0 when unknown
   */
  protected int getSplitCount() {
    return 0;
  }

  /**
   * The optimizer's index selection rationale for the plan this scan belongs to, used to render the
   * per-scan {@code INDEX} rule comment and the {@code !INDEX} rejection comments. Returns
   * {@code null} when the plan did not participate in optimizer index selection (DML, DDL, and
   * non-optimizer plans).
   * @return the decision, or {@code null} when unavailable
   */
  protected OptimizerDecision getOptimizerDecision() {
    return null;
  }

  /**
   * The post-compile row projector for the plan this scan belongs to. Returns {@code null} when the
   * plan has no projector.
   */
  protected RowProjector getProjector() {
    return null;
  }

  /**
   * Whether {@code rule} is a default rule whose {@code INDEX} comment is suppressed. The default
   * rules are {@link OptimizerReasons#RULE_DATA_TABLE} (no candidate indexes considered) and
   * {@link OptimizerReasons#RULE_ONLY_CANDIDATE} (a single viable candidate).
   */
  private static boolean isDefaultRule(String rule) {
    return OptimizerReasons.RULE_DATA_TABLE.equals(rule)
      || OptimizerReasons.RULE_ONLY_CANDIDATE.equals(rule);
  }

  /**
   * Logical name used to render a table or index in EXPLAIN output. Shared by both the scan
   * {@code OVER} line's local index decoration and the per scan {@code INDEX} line.
   * @param table the scanned table or index
   * @return the display name with any child-view local-index prefix stripped
   */
  private static String getExplainIndexName(PTable table) {
    String indexName = table.getName().getString();
    if (
      table.getIndexType() == PTable.IndexType.LOCAL && table.getViewIndexId() != null
        && indexName.contains(QueryConstants.CHILD_VIEW_INDEX_NAME_SEPARATOR)
    ) {
      int lastIndexOf = indexName.lastIndexOf(QueryConstants.CHILD_VIEW_INDEX_NAME_SEPARATOR);
      indexName = indexName.substring(lastIndexOf + 1);
    }
    return indexName;
  }

  /**
   * Populate the top-of-plan disclosure attributes on the supplied builder. Should be invoked only
   * for a root plan (i.e. when {@code context.isRoot()}).
   * @param builder  the attributes builder to populate (no-op when null)
   * @param context  the root statement context
   * @param tableRef the plan's primary table reference (may be null)
   */
  public static void populateTopOfPlanAttributes(ExplainPlanAttributesBuilder builder,
    StatementContext context, TableRef tableRef) {
    if (builder == null || context == null) {
      return;
    }
    if (context.getConnection() != null && context.getConnection().getTenantId() != null) {
      builder.setTenantId(context.getConnection().getTenantId().getString());
    }
    PTable table = tableRef == null ? null : tableRef.getTable();
    if (table != null) {
      if (table.getType() == PTableType.VIEW) {
        builder.setViewName(table.getName().getString());
        if (table.getBaseTableLogicalName() != null) {
          builder.setViewBaseName(table.getBaseTableLogicalName().getString());
        }
      }
      if (table.isTransactional() && table.getTransactionProvider() != null) {
        builder.setTxnProvider(table.getTransactionProvider().name());
      }
    }
    String cdcScopes = context.getEncodedCdcIncludeScopes();
    if (cdcScopes == null && table != null && table.getType() == PTableType.CDC) {
      Set<PTable.CDCChangeScope> scopes = table.getCDCIncludeScopes();
      if (scopes != null && !scopes.isEmpty()) {
        cdcScopes = CDCUtil.makeChangeScopeStringFromEnums(scopes);
      }
    }
    if (cdcScopes != null) {
      builder.setCdcScopes(cdcScopes);
    }
    // Aggregate rewrite breadcrumbs across the context and its sub-contexts, deduping while
    // preserving first occurrence order. The derived table flatten count is rendered as a
    // trailing breadcrumb when non-zero.
    Set<String> rewrites = new LinkedHashSet<>();
    int flattenCount = collectAppliedRewrites(context, rewrites);
    if (flattenCount > 0) {
      rewrites.add("DERIVED TABLE FLATTENED " + flattenCount);
    }
    if (!rewrites.isEmpty()) {
      builder.setRewrites(new ArrayList<>(rewrites));
    }
  }

  /**
   * Populate the plan-total estimate attributes on the supplied builder from the given plan. Should
   * be invoked only for a root plan.
   * @param builder the attributes builder to populate (no-op when null)
   * @param plan    the plan to read estimates from (no-op when null)
   */
  public static void populateTopOfPlanEstimates(ExplainPlanAttributesBuilder builder,
    StatementPlan plan) throws SQLException {
    if (builder == null || plan == null) {
      return;
    }
    builder.setEstimatedRows(plan.getEstimatedRowsToScan());
    builder.setEstimatedSizeInBytes(plan.getEstimatedBytesToScan());
    builder.setEstimateInfoTs(plan.getEstimateInfoTimestamp());
  }

  private static int collectAppliedRewrites(StatementContext context, Set<String> out) {
    int flattenCount = context.getDerivedTableFlattenCount();
    out.addAll(context.getAppliedRewrites());
    for (StatementContext sub : context.getSubStatementContexts()) {
      flattenCount += collectAppliedRewrites(sub, out);
    }
    return flattenCount;
  }

  /**
   * Render the top-of-plan disclosure lines from the populated attributes and insert them at the
   * head of {@code planSteps}.
   * @param planSteps the rendered plan step lines to prepend onto (no-op when null)
   * @param attrs     the already-populated root plan attributes (no-op when null)
   */
  public static void renderTopOfPlanText(List<String> planSteps, ExplainPlanAttributes attrs) {
    if (planSteps == null || attrs == null) {
      return;
    }
    List<String> lines = new ArrayList<>();
    if (attrs.getTenantId() != null) {
      lines.add("TENANT '" + attrs.getTenantId() + "'");
    }
    if (attrs.getViewName() != null) {
      StringBuilder viewLine = new StringBuilder("VIEW ").append(attrs.getViewName());
      if (attrs.getViewBaseName() != null) {
        viewLine.append(" OVER ").append(attrs.getViewBaseName());
      }
      lines.add(viewLine.toString());
    }
    if (attrs.getCdcScopes() != null) {
      lines.add("CDC SCOPE " + attrs.getCdcScopes());
    }
    if (attrs.getTxnProvider() != null) {
      lines.add("TXN " + attrs.getTxnProvider());
    }
    if (attrs.getRewrites() != null) {
      for (String rewrite : attrs.getRewrites()) {
        lines.add("REWRITE " + rewrite);
      }
    }
    planSteps.addAll(0, lines);
  }

  protected void explain(String prefix, List<String> planSteps,
    ExplainPlanAttributesBuilder explainPlanAttributesBuilder,
    List<HRegionLocation> regionLocations) {
    StringBuilder buf = new StringBuilder(prefix);
    ScanRanges scanRanges = context.getScanRanges();
    Scan scan = context.getScan();
    boolean verbose = context.isVerbose();

    if (scan.getConsistency() != Consistency.STRONG) {
      buf.append("TIMELINE-CONSISTENCY ");
    }
    if (hint.hasHint(Hint.SMALL)) {
      buf.append(Hint.SMALL).append(" ");
    }
    if (OrderBy.REV_ROW_KEY_ORDER_BY.equals(orderBy)) {
      buf.append("REVERSE ");
    }
    String scanTypeDetails;
    if (scanRanges.isEverything()) {
      scanTypeDetails = "FULL SCAN ";
    } else {
      scanTypeDetails = explainSkipScan();
    }
    buf.append(scanTypeDetails);

    String tableName = tableRef.getTable().getPhysicalName().getString();
    if (tableRef.getTable().getIndexType() == PTable.IndexType.LOCAL) {
      tableName = getExplainIndexName(tableRef.getTable()) + "(" + tableName + ")";
    }
    buf.append("OVER ").append(tableName);

    if (!scanRanges.isPointLookup()) {
      buf.append(appendKeyRanges());
    }
    planSteps.add(buf.toString());
    if (explainPlanAttributesBuilder != null) {
      explainPlanAttributesBuilder.setConsistency(scan.getConsistency());
      if (hint.hasHint(Hint.SMALL)) {
        explainPlanAttributesBuilder.setHint(Hint.SMALL);
      }
      if (OrderBy.REV_ROW_KEY_ORDER_BY.equals(orderBy)) {
        explainPlanAttributesBuilder.setClientSortedBy("REVERSE");
      }
      explainPlanAttributesBuilder.setExplainScanType(scanTypeDetails);
      explainPlanAttributesBuilder.setTableName(tableName);
      if (!scanRanges.isPointLookup()) {
        explainPlanAttributesBuilder.setKeyRanges(appendKeyRanges());
      }
    }

    emitProject(planSteps, explainPlanAttributesBuilder, verbose);

    PTable.IndexType indexType = tableRef.getTable().getIndexType();
    String explainIndexName = getExplainIndexName(tableRef.getTable());
    String indexKind = null;
    if (indexType != null) {
      switch (indexType) {
        case LOCAL:
          indexKind = "LOCAL";
          break;
        case GLOBAL:
          indexKind = "GLOBAL";
          break;
        case UNCOVERED_GLOBAL:
          indexKind = "UNCOVERED GLOBAL";
          break;
        default:
          indexKind = null;
      }
    }
    OptimizerDecision decision = getOptimizerDecision();
    StringBuilder indexLine = new StringBuilder("    INDEX ").append(explainIndexName);
    if (indexKind != null) {
      indexLine.append(" ").append(indexKind);
    }
    if (decision != null) {
      // Disclose the selection rule (unless it is a suppressed default) and, when the chosen plan
      // is a functional index that matched a query expression, the separate "matches <expr>"
      // disclosure. Both may appear together as "/* <rule>, matches <expr> */".
      boolean showRule = !isDefaultRule(decision.getRule());
      String functionalMatch = decision.getFunctionalMatch();
      if (showRule || functionalMatch != null) {
        indexLine.append("  /* ");
        if (showRule) {
          indexLine.append(decision.getRule());
        }
        if (functionalMatch != null) {
          if (showRule) {
            indexLine.append(", ");
          }
          indexLine.append(functionalMatch);
        }
        indexLine.append(" */");
      }
    }
    planSteps.add(indexLine.toString());
    if (verbose && decision != null) {
      for (RejectedIndexEntry rejected : decision.getRejectedIndexes()) {
        planSteps
          .add("    /* !INDEX " + rejected.getName() + " -- " + rejected.getReason() + " */");
      }
    }
    Integer bucketNum = tableRef.getTable().getBucketNum();
    if (bucketNum != null) {
      planSteps.add("    SALT BUCKETS " + bucketNum);
    }
    int splitCount = getSplitCount();
    if (splitCount > 0) {
      planSteps.add("    REGIONS PLANNED " + splitCount);
    }
    if (explainPlanAttributesBuilder != null) {
      explainPlanAttributesBuilder.setIndexName(explainIndexName);
      explainPlanAttributesBuilder.setIndexKind(indexKind);
      if (bucketNum != null) {
        explainPlanAttributesBuilder.setSaltBuckets(bucketNum);
      }
      if (splitCount > 0) {
        explainPlanAttributesBuilder.setRegionsPlanned(splitCount);
      }
    }

    if (context.getScan() != null && tableRef.getTable().getRowTimestampColPos() != -1) {
      TimeRange range = context.getScan().getTimeRange();
      planSteps.add("    ROW TIMESTAMP FILTER [" + range.getMin() + ", " + range.getMax() + ")");
      if (explainPlanAttributesBuilder != null) {
        explainPlanAttributesBuilder.setScanTimeRangeMin(range.getMin());
        explainPlanAttributesBuilder.setScanTimeRangeMax(range.getMax());
      }
    }

    PageFilter pageFilter = null;
    FirstKeyOnlyFilter firstKeyOnlyFilter = null;
    EmptyColumnOnlyFilter emptyColumnOnlyFilter = null;
    BooleanExpressionFilter whereFilter = null;
    DistinctPrefixFilter distinctFilter = null;
    Iterator<Filter> filterIterator = ScanUtil.getFilterIterator(scan);
    if (filterIterator.hasNext()) {
      do {
        Filter filter = filterIterator.next();
        if (filter instanceof FirstKeyOnlyFilter) {
          firstKeyOnlyFilter = (FirstKeyOnlyFilter) filter;
        } else if (filter instanceof EmptyColumnOnlyFilter) {
          emptyColumnOnlyFilter = (EmptyColumnOnlyFilter) filter;
        } else if (filter instanceof PageFilter) {
          pageFilter = (PageFilter) filter;
        } else if (filter instanceof BooleanExpressionFilter) {
          whereFilter = (BooleanExpressionFilter) filter;
        } else if (filter instanceof DistinctPrefixFilter) {
          distinctFilter = (DistinctPrefixFilter) filter;
        }
      } while (filterIterator.hasNext());
    }
    Set<PColumn> dataColumns = context.getDataColumns();
    if (dataColumns != null && !dataColumns.isEmpty()) {
      planSteps.add("    SERVER MERGE " + dataColumns.toString());
      if (explainPlanAttributesBuilder != null) {
        explainPlanAttributesBuilder.setServerMergeColumns(dataColumns);
      }
    }
    String whereFilterStr = null;
    if (whereFilter != null) {
      whereFilterStr = whereFilter.toString();
    } else {
      byte[] expBytes = scan.getAttribute(BaseScannerRegionObserverConstants.INDEX_FILTER_STR);
      if (expBytes == null) {
        // For older clients
        expBytes = scan.getAttribute(BaseScannerRegionObserverConstants.LOCAL_INDEX_FILTER_STR);
      }
      if (expBytes != null) {
        whereFilterStr = Bytes.toString(expBytes);
      }
    }
    if (firstKeyOnlyFilter != null) {
      planSteps.add("    SERVER PROJECTION FILTER BY FIRST KEY ONLY");
      if (explainPlanAttributesBuilder != null) {
        explainPlanAttributesBuilder.setServerFirstKeyOnlyProjection(true);
      }
    }
    if (emptyColumnOnlyFilter != null) {
      planSteps.add("    SERVER PROJECTION FILTER BY EMPTY COLUMN ONLY");
      if (explainPlanAttributesBuilder != null) {
        explainPlanAttributesBuilder.setServerEmptyColumnOnlyProjection(true);
      }
    }
    emitIgnoredHints(planSteps, explainPlanAttributesBuilder, verbose);
    if (whereFilterStr != null) {
      if (verbose) {
        emitServerFilters(planSteps, explainPlanAttributesBuilder,
          whereFilter == null ? null : whereFilter.getExpression(), whereFilterStr);
      } else {
        String serverWhereFilter = "SERVER FILTER BY " + whereFilterStr;
        planSteps.add("    " + serverWhereFilter);
        if (explainPlanAttributesBuilder != null) {
          explainPlanAttributesBuilder.setServerWhereFilter(serverWhereFilter);
        }
      }
    }
    if (distinctFilter != null) {
      String serverDistinctFilter =
        "SERVER DISTINCT PREFIX FILTER OVER " + groupBy.getExpressions().toString();
      planSteps.add("    " + serverDistinctFilter);
      if (explainPlanAttributesBuilder != null) {
        explainPlanAttributesBuilder.setServerDistinctFilter(serverDistinctFilter);
      }
    }
    if (!orderBy.getOrderByExpressions().isEmpty() && groupBy.isEmpty()) { // with GROUP BY, sort
                                                                           // happens client-side
      String orderByExpressions =
        "SERVER" + (limit == null ? "" : " TOP " + limit + " ROW" + (limit == 1 ? "" : "S"))
          + " SORTED BY " + orderBy.getOrderByExpressions().toString();
      planSteps.add("    " + orderByExpressions);
      if (explainPlanAttributesBuilder != null) {
        if (limit != null) {
          explainPlanAttributesBuilder.setServerRowLimit(limit.longValue());
        }
        explainPlanAttributesBuilder.setServerSortedBy(orderBy.getOrderByExpressions().toString());
      }
    } else {
      if (offset != null) {
        planSteps.add("    SERVER OFFSET " + offset);
      }
      Long limit = null;
      if (pageFilter != null) {
        limit = pageFilter.getPageSize();
      } else {
        byte[] limitBytes = scan.getAttribute(BaseScannerRegionObserverConstants.INDEX_LIMIT);
        if (limitBytes != null) {
          limit = Bytes.toLong(limitBytes);
        }
      }
      if (limit != null) {
        planSteps.add("    SERVER " + limit + " ROW LIMIT");
      }
      if (explainPlanAttributesBuilder != null) {
        explainPlanAttributesBuilder.setServerOffset(offset);
        // Populate the attribute whenever a "SERVER n ROW LIMIT" step is emitted, including the
        // uncovered-index/server-merge path where the limit originates from the INDEX_LIMIT scan
        // attribute.
        if (limit != null) {
          explainPlanAttributesBuilder.setServerRowLimit(limit);
        }
      }
    }
    Integer groupByLimit = null;
    byte[] groupByLimitBytes = scan.getAttribute(BaseScannerRegionObserverConstants.GROUP_BY_LIMIT);
    if (groupByLimitBytes != null) {
      groupByLimit = (Integer) PInteger.INSTANCE.toObject(groupByLimitBytes);
    }
    getRegionLocations(planSteps, explainPlanAttributesBuilder, regionLocations);
    groupBy.explain(planSteps, groupByLimit, explainPlanAttributesBuilder);
    emitServerProjection(planSteps, explainPlanAttributesBuilder, "ARRAY",
      Collections.singletonList(BaseScannerRegionObserverConstants.SPECIFIC_ARRAY_INDEX));
    emitServerProjection(planSteps, explainPlanAttributesBuilder, "JSON",
      Arrays.asList(BaseScannerRegionObserverConstants.JSON_VALUE_FUNCTION,
        BaseScannerRegionObserverConstants.JSON_QUERY_FUNCTION));
    emitServerProjection(planSteps, explainPlanAttributesBuilder, "BSON",
      Collections.singletonList(BaseScannerRegionObserverConstants.BSON_VALUE_FUNCTION));
  }

  /**
   * Emit a {@code SERVER <label> PROJECTION <count>} clause plus one indented detail line per
   * server evaluated path expression of the given type. The expressions are read from
   * {@link StatementContext#getServerParsedProjections()}, gathering the buckets named by
   * {@code attributeKeys} in order.
   * @param planSteps                    plan step lines to append to
   * @param explainPlanAttributesBuilder attributes builder to populate, or {@code null}
   * @param label                        the type label ({@code ARRAY}, {@code JSON}, {@code BSON})
   * @param attributeKeys                the scan attribute bucket keys contributing to this type
   */
  private void emitServerProjection(List<String> planSteps,
    ExplainPlanAttributesBuilder explainPlanAttributesBuilder, String label,
    List<String> attributeKeys) {
    Map<String, List<Expression>> serverParsedProjections = context.getServerParsedProjections();
    if (serverParsedProjections == null) {
      return;
    }
    List<String> details = new ArrayList<>();
    for (String attributeKey : attributeKeys) {
      List<Expression> expressions = serverParsedProjections.get(attributeKey);
      if (expressions != null) {
        for (Expression expression : expressions) {
          details.add(expression.toString());
        }
      }
    }
    if (details.isEmpty()) {
      return;
    }
    planSteps.add("    SERVER " + label + " PROJECTION " + details.size());
    for (String detail : details) {
      planSteps.add("        " + detail);
    }
    if (explainPlanAttributesBuilder != null) {
      explainPlanAttributesBuilder.addServerParsedProjection(label, details);
    }
  }

  /** Emit the VERBOSE-only {@code PROJECT <cols>} line and populate {@code serverProject}. */
  private void emitProject(List<String> planSteps,
    ExplainPlanAttributesBuilder explainPlanAttributesBuilder, boolean verbose) {
    if (!verbose) {
      return;
    }
    List<String> columns = projectedColumnNames(getProjector());
    if (columns == null) {
      return;
    }
    planSteps.add("    PROJECT " + String.join(", ", columns));
    if (explainPlanAttributesBuilder != null) {
      explainPlanAttributesBuilder.setServerProject(columns);
    }
  }

  /**
   * Render a {@link RowProjector}'s column list as the display names used by the VERBOSE
   * {@code PROJECT} line, falling back to the projector's expression string when a column projector
   * has no name.
   * @return the list of display names, or {@code null} when the projector is {@code null} or
   *         projects nothing
   */
  public static List<String> projectedColumnNames(RowProjector projector) {
    if (projector == null) {
      return null;
    }
    List<? extends ColumnProjector> columnProjectors = projector.getColumnProjectors();
    if (columnProjectors == null || columnProjectors.isEmpty()) {
      return null;
    }
    List<String> columns = new ArrayList<>(columnProjectors.size());
    for (ColumnProjector columnProjector : columnProjectors) {
      String name = columnProjector.getName();
      if (name == null || name.isEmpty()) {
        name = String.valueOf(columnProjector.getExpression());
      }
      columns.add(name);
    }
    return columns;
  }

  /**
   * Server-side {@code UPSERT SELECT} and {@code DELETE} inner plans are compiled as an aggregating
   * {@code SELECT COUNT(1)} whose count is used solely to report how many rows were touched. That
   * count is an internal compiler choice. Surfacing {@code PROJECT COUNT(1)} on the explain output
   * is misleading because the user asked to upsert/delete a set of columns/rows, not to count them.
   * @param planSteps       the composed mutation plan steps (mutated in place)
   * @param innerAttributes the inner aggregate plan's rendered attributes (source of the count-form
   *                        {@code serverProject})
   * @param builder         the mutation plan's attribute builder (its {@code serverProject} is
   *                        overwritten)
   * @param userProjector   the user-facing projection to surface instead of the count form
   */
  public static void overrideMutationProject(List<String> planSteps,
    ExplainPlanAttributes innerAttributes, ExplainPlanAttributesBuilder builder,
    RowProjector userProjector) {
    if (planSteps == null || innerAttributes == null || builder == null) {
      return;
    }
    List<String> countProject = innerAttributes.getServerProject();
    if (countProject == null || countProject.isEmpty()) {
      return;
    }
    // Only override the internal COUNT(*)/COUNT(1) aggregate projection used for mutation row counts.
    if (countProject.size() != 1 || !countProject.get(0).startsWith("COUNT(")) {
      return;
    }
    List<String> userColumns = projectedColumnNames(userProjector);
    if (userColumns == null || userColumns.isEmpty()) {
      return;
    }
    String oldLine = "    PROJECT " + String.join(", ", countProject);
    int idx = planSteps.indexOf(oldLine);
    if (idx >= 0) {
      planSteps.set(idx, "    PROJECT " + String.join(", ", userColumns));
    }
    builder.setServerProject(userColumns);
  }

  /**
   * Emit the VERBOSE-only ignored-hint comments, one {@code /*- HINT(args) -- reason *}{@code /}
   * line per hint the planner intentionally ignored.
   */
  private void emitIgnoredHints(List<String> planSteps,
    ExplainPlanAttributesBuilder explainPlanAttributesBuilder, boolean verbose) {
    if (!verbose) {
      return;
    }
    Map<Hint, String> ignoredHints = context.getIgnoredHints();
    if (ignoredHints == null || ignoredHints.isEmpty()) {
      return;
    }
    for (Map.Entry<Hint, String> entry : ignoredHints.entrySet()) {
      Hint ignoredHint = entry.getKey();
      String args = hint == null ? null : hint.getHint(ignoredHint);
      String rendered = ignoredHint.name() + (args == null ? "" : args);
      planSteps.add("    /*- " + rendered + " -- " + entry.getValue() + " */");
      if (explainPlanAttributesBuilder != null) {
        explainPlanAttributesBuilder.addIgnoredHint(ignoredHint.name(), entry.getValue());
      }
    }
  }

  /**
   * Emit the VERBOSE-only per-predicate {@code SERVER FILTER BY <expr>  -- <origin>} lines and
   * populate {@code serverFilters}. When the top-level filter is an {@link AndExpression} whose
   * children all carry origin tags, one line is emitted per child. Otherwise a single line carries
   * the comma-separated union of origins.
   */
  private void emitServerFilters(List<String> planSteps,
    ExplainPlanAttributesBuilder explainPlanAttributesBuilder, Expression whereExpression,
    String whereFilterStr) {
    List<ExplainFilter> filters = renderVerboseFilters(context, whereExpression, whereFilterStr,
      "    SERVER FILTER BY", planSteps);
    if (explainPlanAttributesBuilder != null) {
      explainPlanAttributesBuilder.setServerFilters(filters);
    }
  }

  /**
   * Render the VERBOSE per-predicate filter breakdown. Each emitted line is appended to
   * {@code planSteps}.
   * @param context           the statement context carrying the predicate-origin tags.
   * @param filterExpression  the residual filter expression (may be {@code null} when only the
   *                          rendered string is available, e.g. an index-serialized filter).
   * @param combinedFilterStr the combined filter string used for the single-line fallback.
   * @param linePrefix        the full leading token including any indentation, e.g.
   *                          {@code "    SERVER FILTER BY"} or {@code "CLIENT FILTER BY"}.
   * @param planSteps         the plan-steps list to append rendered lines to.
   * @return the structured filters in render order (never {@code null}; never empty).
   */
  public static List<ExplainFilter> renderVerboseFilters(StatementContext context,
    Expression filterExpression, String combinedFilterStr, String linePrefix,
    List<String> planSteps) {
    List<ExplainFilter> filters = new ArrayList<>();
    if (filterExpression instanceof AndExpression) {
      List<Expression> children = filterExpression.getChildren();
      boolean allTagged = !children.isEmpty();
      for (Expression child : children) {
        if (context.getPredicateOrigins(child).isEmpty()) {
          allTagged = false;
          break;
        }
      }
      if (allTagged) {
        for (Expression child : children) {
          filters.add(buildFilter(context, "(" + child + ")", child));
        }
      }
    }
    if (filters.isEmpty()) {
      filters.add(buildCombinedFilter(context, combinedFilterStr, filterExpression));
    }
    for (ExplainFilter filter : filters) {
      StringBuilder line = new StringBuilder(linePrefix).append(" ").append(filter.getExpr());
      String comment = renderOriginComment(filter);
      if (comment != null) {
        line.append("  -- ").append(comment);
      }
      planSteps.add(line.toString());
    }
    return filters;
  }

  private static ExplainFilter buildFilter(StatementContext context, String exprStr,
    Expression expression) {
    List<String> origins =
      expression == null ? null : new ArrayList<>(context.getPredicateOrigins(expression));
    String pathTestSubtag = detectPathTestSubtag(expression);
    return new ExplainFilter(exprStr, origins, pathTestSubtag);
  }

  /**
   * Build the {@link ExplainFilter} for a single combined line. Origin tags can live on the parent
   * expression or its conjunct children, so when we collapse to one line union both sets to avoid
   * dropping the attribution.
   */
  private static ExplainFilter buildCombinedFilter(StatementContext context, String exprStr,
    Expression expression) {
    List<String> origins = expression == null ? null : combinedOrigins(context, expression);
    String pathTestSubtag = detectPathTestSubtag(expression);
    return new ExplainFilter(exprStr, origins, pathTestSubtag);
  }

  /** Union of origin tags on {@code expression} and, when it is an AND, its conjunct children. */
  private static List<String> combinedOrigins(StatementContext context, Expression expression) {
    Set<String> origins = new LinkedHashSet<>(context.getPredicateOrigins(expression));
    if (expression instanceof AndExpression) {
      for (Expression child : expression.getChildren()) {
        origins.addAll(context.getPredicateOrigins(child));
      }
    }
    return new ArrayList<>(origins);
  }

  /** Render the trailing origin comment for a server filter. */
  private static String renderOriginComment(ExplainFilter filter) {
    StringBuilder comment = new StringBuilder();
    List<String> origins = filter.getOrigin();
    if (origins != null && !origins.isEmpty()) {
      comment.append(String.join(", ", origins));
    }
    if (filter.getPathTestSubtag() != null) {
      if (comment.length() > 0) {
        comment.append(" ");
      }
      comment.append("(").append(filter.getPathTestSubtag()).append(")");
    }
    return comment.length() == 0 ? null : comment.toString();
  }

  /** Detect a path test function anywhere in the expression tree and return its sub tag. */
  private static String detectPathTestSubtag(Expression expression) {
    if (expression == null) {
      return null;
    }
    if (expression instanceof JsonExistsFunction) {
      return "JSON EXISTS";
    }
    if (expression instanceof BsonConditionExpressionFunction) {
      return "BSON CONDITION";
    }
    if (expression.getChildren() != null) {
      for (Expression child : expression.getChildren()) {
        String subtag = detectPathTestSubtag(child);
        if (subtag != null) {
          return subtag;
        }
      }
    }
    return null;
  }

  /**
   * Retrieve region locations and set the values in the explain plan output.
   * @param planSteps                    list of plan steps to add explain plan output to.
   * @param explainPlanAttributesBuilder explain plan v2 attributes builder instance.
   * @param regionLocations              region locations.
   */
  private void getRegionLocations(List<String> planSteps,
    ExplainPlanAttributesBuilder explainPlanAttributesBuilder,
    List<HRegionLocation> regionLocations) {
    // Region locations are computed during scan planning, so always record them in the structured
    // attributes. Consumers that read the attributes directly (e.g. the connection activity logger,
    // and JSON output) rely on them being present. Only build and append the (potentially large)
    // text representation to the plan steps when the EXPLAIN statement requested them via the
    // REGIONS option (or the legacy WITH REGIONS alias).
    RegionLocationsExplainInfo regionLocationsInfo =
      populateRegionLocationAttributes(explainPlanAttributesBuilder, regionLocations);
    if (regionLocationsInfo == null || !context.getExplainOptions().isRegions()) {
      return;
    }
    String regionLocationPlan = renderRegionLocationsForExplainPlan(regionLocationsInfo);
    if (regionLocationPlan.length() > 0) {
      planSteps.add(regionLocationPlan);
    }
  }

  /**
   * Deduplicate the region locations by region boundary, trim to the configured max size, and
   * record the result (and the total size) in the structured explain plan attributes. This is
   * always done so that consumers reading the attributes directly have the values available,
   * regardless of whether the text representation is requested.
   * @param explainPlanAttributesBuilder      explain plan v2 attributes builder instance.
   * @param regionLocationsFromResultIterator region locations.
   * @return the deduplicated (and possibly trimmed) region locations along with the total
   *         deduplicated size, or {@code null} when no region locations were available.
   */
  private RegionLocationsExplainInfo populateRegionLocationAttributes(
    ExplainPlanAttributesBuilder explainPlanAttributesBuilder,
    List<HRegionLocation> regionLocationsFromResultIterator) {
    if (regionLocationsFromResultIterator == null) {
      return null;
    }
    Set<String> regionBoundaries = new LinkedHashSet<>();
    List<HRegionLocation> regionLocations = new ArrayList<>();
    for (HRegionLocation regionLocation : regionLocationsFromResultIterator) {
      String regionBoundary = Bytes.toStringBinary(regionLocation.getRegion().getStartKey()) + ":"
        + Bytes.toStringBinary(regionLocation.getRegion().getEndKey());
      if (regionBoundaries.add(regionBoundary)) {
        regionLocations.add(regionLocation);
      }
    }
    int maxLimitRegionLoc = context.getConnection().getQueryServices().getConfiguration().getInt(
      QueryServices.MAX_REGION_LOCATIONS_SIZE_EXPLAIN_PLAN,
      QueryServicesOptions.DEFAULT_MAX_REGION_LOCATIONS_SIZE_EXPLAIN_PLAN);
    int totalSize = regionLocations.size();
    List<HRegionLocation> trimmedRegionLocations = totalSize > maxLimitRegionLoc
      ? regionLocations.subList(0, maxLimitRegionLoc)
      : regionLocations;
    if (explainPlanAttributesBuilder != null) {
      explainPlanAttributesBuilder
        .setRegionLocations(Collections.unmodifiableList(trimmedRegionLocations))
        .setRegionLocationsTotalSize(totalSize);
    }
    return new RegionLocationsExplainInfo(trimmedRegionLocations, totalSize);
  }

  /**
   * Render the region locations text for the explain plan output. If the region locations were
   * trimmed (i.e. the deduplicated total exceeded the configured max limit), the trimmed list is
   * printed followed by the total deduplicated size.
   * @param regionLocationsInfo the deduplicated (and possibly trimmed) region locations along with
   *                            the total deduplicated size.
   * @return region locations to be added to the explain plan output.
   */
  private String
    renderRegionLocationsForExplainPlan(RegionLocationsExplainInfo regionLocationsInfo) {
    List<HRegionLocation> trimmedRegionLocations = regionLocationsInfo.getTrimmedRegionLocations();
    StringBuilder buf = new StringBuilder().append(REGION_LOCATIONS);
    buf.append(trimmedRegionLocations);
    if (trimmedRegionLocations.size() < regionLocationsInfo.getTotalSize()) {
      buf.append("...total size = ");
      buf.append(regionLocationsInfo.getTotalSize());
    }
    buf.append(") ");
    return buf.toString();
  }

  /**
   * Holder for the deduplicated (and possibly trimmed) region locations plus the total deduplicated
   * size, used to keep attribute population separate from text rendering.
   */
  private static final class RegionLocationsExplainInfo {
    private final List<HRegionLocation> trimmedRegionLocations;
    private final int totalSize;

    private RegionLocationsExplainInfo(List<HRegionLocation> trimmedRegionLocations,
      int totalSize) {
      this.trimmedRegionLocations = trimmedRegionLocations;
      this.totalSize = totalSize;
    }

    private List<HRegionLocation> getTrimmedRegionLocations() {
      return trimmedRegionLocations;
    }

    private int getTotalSize() {
      return totalSize;
    }
  }

  @SuppressWarnings("rawtypes")
  private void appendPKColumnValue(StringBuilder buf, byte[] range, Boolean isNull, int slotIndex,
    boolean changeViewIndexId) {
    if (Boolean.TRUE.equals(isNull)) {
      buf.append("null");
      return;
    }
    if (Boolean.FALSE.equals(isNull)) {
      buf.append("not null");
      return;
    }
    if (range.length == 0) {
      buf.append('*');
      return;
    }
    ScanRanges scanRanges = context.getScanRanges();
    PDataType type = scanRanges.getSchema().getField(slotIndex).getDataType();
    SortOrder sortOrder = tableRef.getTable().getPKColumns().get(slotIndex).getSortOrder();
    if (sortOrder == SortOrder.DESC) {
      buf.append('~');
      ImmutableBytesWritable ptr = new ImmutableBytesWritable(range);
      type.coerceBytes(ptr, type, sortOrder, SortOrder.getDefault());
      range = ptr.get();
    }
    if (changeViewIndexId) {
      buf.append(getViewIndexValue(type, range).toString());
    } else {
      Format formatter = context.getConnection().getFormatter(type);
      buf.append(type.toStringLiteral(range, formatter));
    }
  }

  @SuppressWarnings("rawtypes")
  private Long getViewIndexValue(PDataType type, byte[] range) {
    boolean useLongViewIndex = MetaDataUtil.getViewIndexIdDataType().equals(type);
    Object s = type.toObject(range);
    return (useLongViewIndex ? (Long) s : (Short) s) + Short.MAX_VALUE + 2;
  }

  private void appendScanRow(StringBuilder buf, Bound bound) {
    ScanRanges scanRanges = context.getScanRanges();
    Iterator<byte[]> minMaxIterator = Collections.emptyIterator();
    boolean isLocalIndex = ScanUtil.isLocalIndex(context.getScan());
    boolean forceSkipScan = this.hint.hasHint(Hint.SKIP_SCAN);
    int nRanges = forceSkipScan ? scanRanges.getRanges().size() : scanRanges.getBoundSlotCount();
    for (int i = 0, minPos = 0; minPos < nRanges || minMaxIterator.hasNext(); i++) {
      List<KeyRange> ranges = minPos >= nRanges ? EVERYTHING : scanRanges.getRanges().get(minPos++);
      KeyRange range = bound == Bound.LOWER ? ranges.get(0) : ranges.get(ranges.size() - 1);
      byte[] b = range.getRange(bound);
      Boolean isNull = KeyRange.IS_NULL_RANGE == range ? Boolean.TRUE
        : KeyRange.IS_NOT_NULL_RANGE == range ? Boolean.FALSE
        : null;
      if (minMaxIterator.hasNext()) {
        byte[] bMinMax = minMaxIterator.next();
        int cmp = Bytes.compareTo(bMinMax, b) * (bound == Bound.LOWER ? 1 : -1);
        if (cmp > 0) {
          minPos = nRanges;
          b = bMinMax;
          isNull = null;
        } else if (cmp < 0) {
          minMaxIterator = Collections.emptyIterator();
        }
      }
      if (isLocalIndex && i == 0) {
        appendPKColumnValue(buf, b, isNull, i, true);
      } else {
        appendPKColumnValue(buf, b, isNull, i, false);
      }
      buf.append(',');
    }
  }

  private String appendKeyRanges() {
    final StringBuilder buf = new StringBuilder();
    ScanRanges scanRanges = context.getScanRanges();
    if (scanRanges.isDegenerate() || scanRanges.isEverything()) {
      return "";
    }
    buf.append(" [");
    StringBuilder buf1 = new StringBuilder();
    appendScanRow(buf1, Bound.LOWER);
    buf.append(buf1);
    buf.setCharAt(buf.length() - 1, ']');
    StringBuilder buf2 = new StringBuilder();
    appendScanRow(buf2, Bound.UPPER);
    if (!StringUtil.equals(buf1, buf2)) {
      buf.append(" - [");
      buf.append(buf2);
    }
    buf.setCharAt(buf.length() - 1, ']');
    return buf.toString();
  }

}
