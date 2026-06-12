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
package org.apache.phoenix.compile;

import com.fasterxml.jackson.annotation.JsonPropertyOrder;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.hadoop.hbase.HRegionLocation;
import org.apache.hadoop.hbase.client.Consistency;
import org.apache.phoenix.optimize.RejectedIndexEntry;
import org.apache.phoenix.parse.HintNode;
import org.apache.phoenix.parse.HintNode.Hint;
import org.apache.phoenix.parse.UpsertStatement.OnDuplicateKeyType;
import org.apache.phoenix.schema.PColumn;

/**
 * ExplainPlan attributes that contain individual attributes of ExplainPlan that we can assert
 * against. This also makes attribute retrieval easier as an API rather than retrieving list of
 * Strings containing entire plan.
 */
@JsonPropertyOrder({ "tenantId", "viewName", "viewBaseName", "cdcScopes", "txnProvider", "rewrites",
  "estimatedRows", "estimatedSizeInBytes", "estimateInfoTs", "abstractExplainPlan",
  "onDuplicateKeyAction", "serverUpdateSet", "returningRow", "hint", "explainScanType",
  "consistency", "tableName", "keyRanges", "indexName", "indexKind", "indexRule", "indexRejected",
  "saltBuckets", "regionsPlanned", "scanTimeRangeMin", "scanTimeRangeMax", "splitsChunk",
  "useRoundRobinIterator", "samplingRate", "hexStringRVCOffset", "iteratorTypeAndScanSize",
  "scanEstimatedRows", "scanEstimatedSizeInBytes", "serverWhereFilter", "serverDistinctFilter",
  "serverMergeColumns", "serverParsedProjections", "serverFirstKeyOnlyProjection",
  "serverEmptyColumnOnlyProjection", "serverAggregate", "serverGroupByLimit", "serverSortedBy",
  "serverOffset", "serverRowLimit", "clientFilterBy", "clientAggregate", "clientDistinctFilter",
  "clientAfterAggregate", "clientSortAlgo", "clientSortedBy", "clientOffset", "clientRowLimit",
  "clientSequenceCount", "clientCursorName", "clientSteps", "lhsJoinQueryExplainPlan",
  "rhsJoinQueryExplainPlan", "subPlans", "dynamicServerFilter", "afterJoinFilter",
  "joinScannerLimit", "sortMergeSkipMerge", "regionLocations", "regionLocationsTotalSize",
  "numRegionLocationLookups" })
public class ExplainPlanAttributes {

  // Top-of-plan disclosures (populated only on the root plan)
  private final String tenantId;
  private final String viewName;
  private final String viewBaseName;
  private final String cdcScopes;
  private final String txnProvider;
  private final List<String> rewrites;
  // Plan-total estimates.
  private final Long estimatedRows;
  private final Long estimatedSizeInBytes;
  private final Long estimateInfoTs;

  // Plan identity and scan-level metadata
  private final String abstractExplainPlan;
  // Mutation-operator detail (populated only on mutation plans).
  private final OnDuplicateKeyType onDuplicateKeyAction;
  private final List<String> serverUpdateSet;
  private final boolean returningRow;
  private final Hint hint;
  private final String explainScanType;
  private final Consistency consistency;
  private final String tableName;
  private final String keyRanges;
  private final String indexName;
  private final String indexKind;
  private final String indexRule;
  private final List<RejectedIndexEntry> indexRejected;
  private final Integer saltBuckets;
  private final Integer regionsPlanned;
  private final Long scanTimeRangeMin;
  private final Long scanTimeRangeMax;
  private final Integer splitsChunk;
  private final boolean useRoundRobinIterator;
  private final Double samplingRate;
  private final String hexStringRVCOffset;
  private final String iteratorTypeAndScanSize;
  // Per-scan estimates (populated on each plan level from stats).
  private final Long scanEstimatedRows;
  private final Long scanEstimatedSizeInBytes;

  // Server-side operations
  private final String serverWhereFilter;
  private final String serverDistinctFilter;
  private final Set<PColumn> serverMergeColumns;
  private final Map<String, List<String>> serverParsedProjections;
  private final boolean serverFirstKeyOnlyProjection;
  private final boolean serverEmptyColumnOnlyProjection;
  private final String serverAggregate;
  private final Integer serverGroupByLimit;
  private final String serverSortedBy;
  private final Integer serverOffset;
  private final Long serverRowLimit;

  // Client-side operations
  private final String clientFilterBy;
  private final String clientAggregate;
  private final String clientDistinctFilter;
  private final String clientAfterAggregate;
  private final String clientSortAlgo;
  private final String clientSortedBy;
  private final Integer clientOffset;
  private final Integer clientRowLimit;
  private final Integer clientSequenceCount;
  private final String clientCursorName;
  // Ordered client-side pipeline (CLIENT* lines in emission order).
  private final List<String> clientSteps;

  // Join / sub-plan
  private final ExplainPlanAttributes lhsJoinQueryExplainPlan;
  private final ExplainPlanAttributes rhsJoinQueryExplainPlan;
  private final List<ExplainPlanAttributes> subPlans;
  private final String dynamicServerFilter;
  private final String afterJoinFilter;
  private final Long joinScannerLimit;
  private final boolean sortMergeSkipMerge;

  // Region-location metadata
  private final List<HRegionLocation> regionLocations;
  // Total number of distinct region locations before any trimming. Null when not populated.
  private final Integer regionLocationsTotalSize;
  private final int numRegionLocationLookups;

  private static final ExplainPlanAttributes EXPLAIN_PLAN_INSTANCE = new ExplainPlanAttributes();

  private ExplainPlanAttributes() {
    this.tenantId = null;
    this.viewName = null;
    this.viewBaseName = null;
    this.cdcScopes = null;
    this.txnProvider = null;
    this.rewrites = null;
    this.estimatedRows = null;
    this.estimatedSizeInBytes = null;
    this.estimateInfoTs = null;
    this.abstractExplainPlan = null;
    this.onDuplicateKeyAction = null;
    this.serverUpdateSet = null;
    this.returningRow = false;
    this.hint = null;
    this.explainScanType = null;
    this.consistency = null;
    this.tableName = null;
    this.keyRanges = null;
    this.indexName = null;
    this.indexKind = null;
    this.indexRule = null;
    this.indexRejected = null;
    this.saltBuckets = null;
    this.regionsPlanned = null;
    this.scanTimeRangeMin = null;
    this.scanTimeRangeMax = null;
    this.splitsChunk = null;
    this.useRoundRobinIterator = false;
    this.samplingRate = null;
    this.hexStringRVCOffset = null;
    this.iteratorTypeAndScanSize = null;
    this.scanEstimatedRows = null;
    this.scanEstimatedSizeInBytes = null;
    this.serverWhereFilter = null;
    this.serverDistinctFilter = null;
    this.serverMergeColumns = null;
    this.serverParsedProjections = null;
    this.serverFirstKeyOnlyProjection = false;
    this.serverEmptyColumnOnlyProjection = false;
    this.serverAggregate = null;
    this.serverGroupByLimit = null;
    this.serverSortedBy = null;
    this.serverOffset = null;
    this.serverRowLimit = null;
    this.clientFilterBy = null;
    this.clientAggregate = null;
    this.clientDistinctFilter = null;
    this.clientAfterAggregate = null;
    this.clientSortAlgo = null;
    this.clientSortedBy = null;
    this.clientOffset = null;
    this.clientRowLimit = null;
    this.clientSequenceCount = null;
    this.clientCursorName = null;
    this.clientSteps = null;
    this.lhsJoinQueryExplainPlan = null;
    this.rhsJoinQueryExplainPlan = null;
    this.subPlans = null;
    this.dynamicServerFilter = null;
    this.afterJoinFilter = null;
    this.joinScannerLimit = null;
    this.sortMergeSkipMerge = false;
    this.regionLocations = null;
    this.regionLocationsTotalSize = null;
    this.numRegionLocationLookups = 0;
  }

  public ExplainPlanAttributes(String tenantId, String viewName, String viewBaseName,
    String cdcScopes, String txnProvider, List<String> rewrites, Long estimatedRows,
    Long estimatedSizeInBytes, Long estimateInfoTs, String abstractExplainPlan,
    OnDuplicateKeyType onDuplicateKeyAction, List<String> serverUpdateSet, boolean returningRow,
    Hint hint, String explainScanType, Consistency consistency, String tableName, String keyRanges,
    String indexName, String indexKind, String indexRule, List<RejectedIndexEntry> indexRejected,
    Integer saltBuckets, Integer regionsPlanned, Long scanTimeRangeMin, Long scanTimeRangeMax,
    Integer splitsChunk, boolean useRoundRobinIterator, Double samplingRate,
    String hexStringRVCOffset, String iteratorTypeAndScanSize, Long scanEstimatedRows,
    Long scanEstimatedSizeInBytes, String serverWhereFilter, String serverDistinctFilter,
    Set<PColumn> serverMergeColumns, Map<String, List<String>> serverParsedProjections,
    boolean serverFirstKeyOnlyProjection, boolean serverEmptyColumnOnlyProjection,
    String serverAggregate, Integer serverGroupByLimit, String serverSortedBy, Integer serverOffset,
    Long serverRowLimit, String clientFilterBy, String clientAggregate, String clientDistinctFilter,
    String clientAfterAggregate, String clientSortAlgo, String clientSortedBy, Integer clientOffset,
    Integer clientRowLimit, Integer clientSequenceCount, String clientCursorName,
    List<String> clientSteps, ExplainPlanAttributes lhsJoinQueryExplainPlan,
    ExplainPlanAttributes rhsJoinQueryExplainPlan, List<ExplainPlanAttributes> subPlans,
    String dynamicServerFilter, String afterJoinFilter, Long joinScannerLimit,
    boolean sortMergeSkipMerge, List<HRegionLocation> regionLocations,
    Integer regionLocationsTotalSize, int numRegionLocationLookups) {
    this.tenantId = tenantId;
    this.viewName = viewName;
    this.viewBaseName = viewBaseName;
    this.cdcScopes = cdcScopes;
    this.txnProvider = txnProvider;
    this.rewrites = (rewrites == null || rewrites.isEmpty())
      ? null
      : Collections.unmodifiableList(new ArrayList<>(rewrites));
    this.estimatedRows = estimatedRows;
    this.estimatedSizeInBytes = estimatedSizeInBytes;
    this.estimateInfoTs = estimateInfoTs;
    this.abstractExplainPlan = abstractExplainPlan;
    this.onDuplicateKeyAction = onDuplicateKeyAction;
    this.serverUpdateSet = (serverUpdateSet == null || serverUpdateSet.isEmpty())
      ? null
      : Collections.unmodifiableList(new ArrayList<>(serverUpdateSet));
    this.returningRow = returningRow;
    this.hint = hint;
    this.explainScanType = explainScanType;
    this.consistency = consistency;
    this.tableName = tableName;
    this.keyRanges = keyRanges;
    this.indexName = indexName;
    this.indexKind = indexKind;
    this.indexRule = indexRule;
    this.indexRejected = (indexRejected == null || indexRejected.isEmpty())
      ? null
      : Collections.unmodifiableList(new ArrayList<>(indexRejected));
    this.saltBuckets = saltBuckets;
    this.regionsPlanned = regionsPlanned;
    this.scanTimeRangeMin = scanTimeRangeMin;
    this.scanTimeRangeMax = scanTimeRangeMax;
    this.splitsChunk = splitsChunk;
    this.useRoundRobinIterator = useRoundRobinIterator;
    this.samplingRate = samplingRate;
    this.hexStringRVCOffset = hexStringRVCOffset;
    this.iteratorTypeAndScanSize = iteratorTypeAndScanSize;
    this.scanEstimatedRows = scanEstimatedRows;
    this.scanEstimatedSizeInBytes = scanEstimatedSizeInBytes;
    this.serverWhereFilter = serverWhereFilter;
    this.serverDistinctFilter = serverDistinctFilter;
    this.serverMergeColumns = serverMergeColumns;
    this.serverParsedProjections = copyServerParsedProjections(serverParsedProjections);
    this.serverFirstKeyOnlyProjection = serverFirstKeyOnlyProjection;
    this.serverEmptyColumnOnlyProjection = serverEmptyColumnOnlyProjection;
    this.serverAggregate = serverAggregate;
    this.serverGroupByLimit = serverGroupByLimit;
    this.serverSortedBy = serverSortedBy;
    this.serverOffset = serverOffset;
    this.serverRowLimit = serverRowLimit;
    this.clientFilterBy = clientFilterBy;
    this.clientAggregate = clientAggregate;
    this.clientDistinctFilter = clientDistinctFilter;
    this.clientAfterAggregate = clientAfterAggregate;
    this.clientSortAlgo = clientSortAlgo;
    this.clientSortedBy = clientSortedBy;
    this.clientOffset = clientOffset;
    this.clientRowLimit = clientRowLimit;
    this.clientSequenceCount = clientSequenceCount;
    this.clientCursorName = clientCursorName;
    this.clientSteps = (clientSteps == null || clientSteps.isEmpty())
      ? null
      : Collections.unmodifiableList(new ArrayList<>(clientSteps));
    this.lhsJoinQueryExplainPlan = lhsJoinQueryExplainPlan;
    this.rhsJoinQueryExplainPlan = rhsJoinQueryExplainPlan;
    this.subPlans = subPlans;
    this.dynamicServerFilter = dynamicServerFilter;
    this.afterJoinFilter = afterJoinFilter;
    this.joinScannerLimit = joinScannerLimit;
    this.sortMergeSkipMerge = sortMergeSkipMerge;
    this.regionLocations = regionLocations;
    this.regionLocationsTotalSize = regionLocationsTotalSize;
    this.numRegionLocationLookups = numRegionLocationLookups;
  }

  public String getTenantId() {
    return tenantId;
  }

  public String getViewName() {
    return viewName;
  }

  public String getViewBaseName() {
    return viewBaseName;
  }

  public String getCdcScopes() {
    return cdcScopes;
  }

  public String getTxnProvider() {
    return txnProvider;
  }

  public List<String> getRewrites() {
    return rewrites;
  }

  public String getAbstractExplainPlan() {
    return abstractExplainPlan;
  }

  public OnDuplicateKeyType getOnDuplicateKeyAction() {
    return onDuplicateKeyAction;
  }

  public List<String> getServerUpdateSet() {
    return serverUpdateSet;
  }

  public boolean isReturningRow() {
    return returningRow;
  }

  public Hint getHint() {
    return hint;
  }

  public String getExplainScanType() {
    return explainScanType;
  }

  public Consistency getConsistency() {
    return consistency;
  }

  public String getTableName() {
    return tableName;
  }

  public String getKeyRanges() {
    return keyRanges;
  }

  public String getIndexName() {
    return indexName;
  }

  public String getIndexKind() {
    return indexKind;
  }

  public String getIndexRule() {
    return indexRule;
  }

  public List<RejectedIndexEntry> getIndexRejected() {
    return indexRejected;
  }

  public Integer getSaltBuckets() {
    return saltBuckets;
  }

  public Integer getRegionsPlanned() {
    return regionsPlanned;
  }

  public Long getScanTimeRangeMin() {
    return scanTimeRangeMin;
  }

  public Long getScanTimeRangeMax() {
    return scanTimeRangeMax;
  }

  public Integer getSplitsChunk() {
    return splitsChunk;
  }

  public boolean isUseRoundRobinIterator() {
    return useRoundRobinIterator;
  }

  public Double getSamplingRate() {
    return samplingRate;
  }

  public String getHexStringRVCOffset() {
    return hexStringRVCOffset;
  }

  public String getIteratorTypeAndScanSize() {
    return iteratorTypeAndScanSize;
  }

  public Long getScanEstimatedRows() {
    return scanEstimatedRows;
  }

  public Long getScanEstimatedSizeInBytes() {
    return scanEstimatedSizeInBytes;
  }

  public Long getEstimatedRows() {
    return estimatedRows;
  }

  public Long getEstimatedSizeInBytes() {
    return estimatedSizeInBytes;
  }

  public Long getEstimateInfoTs() {
    return estimateInfoTs;
  }

  public String getServerWhereFilter() {
    return serverWhereFilter;
  }

  public String getServerDistinctFilter() {
    return serverDistinctFilter;
  }

  @JsonSerialize(using = ServerMergeColumnsSerializer.class)
  public Set<PColumn> getServerMergeColumns() {
    return serverMergeColumns;
  }

  public Map<String, List<String>> getServerParsedProjections() {
    return serverParsedProjections;
  }

  private static Map<String, List<String>>
    copyServerParsedProjections(Map<String, List<String>> source) {
    if (source == null || source.isEmpty()) {
      return null;
    }
    Map<String, List<String>> copy = new LinkedHashMap<>();
    for (Map.Entry<String, List<String>> entry : source.entrySet()) {
      copy.put(entry.getKey(), Collections.unmodifiableList(new ArrayList<>(entry.getValue())));
    }
    return Collections.unmodifiableMap(copy);
  }

  public boolean isServerFirstKeyOnlyProjection() {
    return serverFirstKeyOnlyProjection;
  }

  public boolean isServerEmptyColumnOnlyProjection() {
    return serverEmptyColumnOnlyProjection;
  }

  public String getServerAggregate() {
    return serverAggregate;
  }

  public Integer getServerGroupByLimit() {
    return serverGroupByLimit;
  }

  public String getServerSortedBy() {
    return serverSortedBy;
  }

  public Integer getServerOffset() {
    return serverOffset;
  }

  public Long getServerRowLimit() {
    return serverRowLimit;
  }

  public String getClientFilterBy() {
    return clientFilterBy;
  }

  public String getClientAggregate() {
    return clientAggregate;
  }

  public String getClientDistinctFilter() {
    return clientDistinctFilter;
  }

  public String getClientAfterAggregate() {
    return clientAfterAggregate;
  }

  public String getClientSortAlgo() {
    return clientSortAlgo;
  }

  public String getClientSortedBy() {
    return clientSortedBy;
  }

  public Integer getClientOffset() {
    return clientOffset;
  }

  public Integer getClientRowLimit() {
    return clientRowLimit;
  }

  public Integer getClientSequenceCount() {
    return clientSequenceCount;
  }

  public String getClientCursorName() {
    return clientCursorName;
  }

  public List<String> getClientSteps() {
    return clientSteps;
  }

  public ExplainPlanAttributes getLhsJoinQueryExplainPlan() {
    return lhsJoinQueryExplainPlan;
  }

  public ExplainPlanAttributes getRhsJoinQueryExplainPlan() {
    return rhsJoinQueryExplainPlan;
  }

  public List<ExplainPlanAttributes> getSubPlans() {
    return subPlans;
  }

  public String getDynamicServerFilter() {
    return dynamicServerFilter;
  }

  public String getAfterJoinFilter() {
    return afterJoinFilter;
  }

  public Long getJoinScannerLimit() {
    return joinScannerLimit;
  }

  public boolean isSortMergeSkipMerge() {
    return sortMergeSkipMerge;
  }

  @JsonSerialize(using = RegionLocationsListSerializer.class)
  public List<HRegionLocation> getRegionLocations() {
    return regionLocations;
  }

  public Integer getRegionLocationsTotalSize() {
    return regionLocationsTotalSize;
  }

  public int getNumRegionLocationLookups() {
    return numRegionLocationLookups;
  }

  public static ExplainPlanAttributes getDefaultExplainPlan() {
    return EXPLAIN_PLAN_INSTANCE;
  }

  public static class ExplainPlanAttributesBuilder {
    private String tenantId;
    private String viewName;
    private String viewBaseName;
    private String cdcScopes;
    private String txnProvider;
    private List<String> rewrites;
    private Long estimatedRows;
    private Long estimatedSizeInBytes;
    private Long estimateInfoTs;
    private String abstractExplainPlan;
    private OnDuplicateKeyType onDuplicateKeyAction;
    private List<String> serverUpdateSet;
    private boolean returningRow;
    private HintNode.Hint hint;
    private String explainScanType;
    private Consistency consistency;
    private String tableName;
    private String keyRanges;
    private String indexName;
    private String indexKind;
    private String indexRule;
    private List<RejectedIndexEntry> indexRejected;
    private Integer saltBuckets;
    private Integer regionsPlanned;
    private Long scanTimeRangeMin;
    private Long scanTimeRangeMax;
    private Integer splitsChunk;
    private boolean useRoundRobinIterator;
    private Double samplingRate;
    private String hexStringRVCOffset;
    private String iteratorTypeAndScanSize;
    private Long scanEstimatedRows;
    private Long scanEstimatedSizeInBytes;
    private String serverWhereFilter;
    private String serverDistinctFilter;
    private Set<PColumn> serverMergeColumns;
    private Map<String, List<String>> serverParsedProjections;
    private boolean serverFirstKeyOnlyProjection;
    private boolean serverEmptyColumnOnlyProjection;
    private String serverAggregate;
    private Integer serverGroupByLimit;
    private String serverSortedBy;
    private Integer serverOffset;
    private Long serverRowLimit;
    private String clientFilterBy;
    private String clientAggregate;
    private String clientDistinctFilter;
    private String clientAfterAggregate;
    private String clientSortAlgo;
    private String clientSortedBy;
    private Integer clientOffset;
    private Integer clientRowLimit;
    private Integer clientSequenceCount;
    private String clientCursorName;
    private List<String> clientSteps;
    private ExplainPlanAttributes lhsJoinQueryExplainPlan;
    private ExplainPlanAttributes rhsJoinQueryExplainPlan;
    private List<ExplainPlanAttributes> subPlans;
    private String dynamicServerFilter;
    private String afterJoinFilter;
    private Long joinScannerLimit;
    private boolean sortMergeSkipMerge;
    private List<HRegionLocation> regionLocations;
    private Integer regionLocationsTotalSize;
    private int numRegionLocationLookups;

    public ExplainPlanAttributesBuilder() {
      // default
    }

    public ExplainPlanAttributesBuilder(ExplainPlanAttributes explainPlanAttributes) {
      this.tenantId = explainPlanAttributes.getTenantId();
      this.viewName = explainPlanAttributes.getViewName();
      this.viewBaseName = explainPlanAttributes.getViewBaseName();
      this.cdcScopes = explainPlanAttributes.getCdcScopes();
      this.txnProvider = explainPlanAttributes.getTxnProvider();
      List<String> srcRewrites = explainPlanAttributes.getRewrites();
      this.rewrites = srcRewrites == null ? null : new ArrayList<>(srcRewrites);
      this.estimatedRows = explainPlanAttributes.getEstimatedRows();
      this.estimatedSizeInBytes = explainPlanAttributes.getEstimatedSizeInBytes();
      this.estimateInfoTs = explainPlanAttributes.getEstimateInfoTs();
      this.abstractExplainPlan = explainPlanAttributes.getAbstractExplainPlan();
      this.onDuplicateKeyAction = explainPlanAttributes.getOnDuplicateKeyAction();
      List<String> srcServerUpdateSet = explainPlanAttributes.getServerUpdateSet();
      this.serverUpdateSet =
        srcServerUpdateSet == null ? null : new ArrayList<>(srcServerUpdateSet);
      this.returningRow = explainPlanAttributes.isReturningRow();
      this.hint = explainPlanAttributes.getHint();
      this.explainScanType = explainPlanAttributes.getExplainScanType();
      this.consistency = explainPlanAttributes.getConsistency();
      this.tableName = explainPlanAttributes.getTableName();
      this.keyRanges = explainPlanAttributes.getKeyRanges();
      this.indexName = explainPlanAttributes.getIndexName();
      this.indexKind = explainPlanAttributes.getIndexKind();
      this.indexRule = explainPlanAttributes.getIndexRule();
      List<RejectedIndexEntry> srcIndexRejected = explainPlanAttributes.getIndexRejected();
      this.indexRejected = srcIndexRejected == null ? null : new ArrayList<>(srcIndexRejected);
      this.saltBuckets = explainPlanAttributes.getSaltBuckets();
      this.regionsPlanned = explainPlanAttributes.getRegionsPlanned();
      this.scanTimeRangeMin = explainPlanAttributes.getScanTimeRangeMin();
      this.scanTimeRangeMax = explainPlanAttributes.getScanTimeRangeMax();
      this.splitsChunk = explainPlanAttributes.getSplitsChunk();
      this.useRoundRobinIterator = explainPlanAttributes.isUseRoundRobinIterator();
      this.samplingRate = explainPlanAttributes.getSamplingRate();
      this.hexStringRVCOffset = explainPlanAttributes.getHexStringRVCOffset();
      this.iteratorTypeAndScanSize = explainPlanAttributes.getIteratorTypeAndScanSize();
      this.scanEstimatedRows = explainPlanAttributes.getScanEstimatedRows();
      this.scanEstimatedSizeInBytes = explainPlanAttributes.getScanEstimatedSizeInBytes();
      this.serverWhereFilter = explainPlanAttributes.getServerWhereFilter();
      this.serverDistinctFilter = explainPlanAttributes.getServerDistinctFilter();
      this.serverMergeColumns = explainPlanAttributes.getServerMergeColumns();
      Map<String, List<String>> srcServerParsedProjections =
        explainPlanAttributes.getServerParsedProjections();
      this.serverParsedProjections =
        srcServerParsedProjections == null ? null : new LinkedHashMap<>(srcServerParsedProjections);
      this.serverFirstKeyOnlyProjection = explainPlanAttributes.isServerFirstKeyOnlyProjection();
      this.serverEmptyColumnOnlyProjection =
        explainPlanAttributes.isServerEmptyColumnOnlyProjection();
      this.serverAggregate = explainPlanAttributes.getServerAggregate();
      this.serverGroupByLimit = explainPlanAttributes.getServerGroupByLimit();
      this.serverSortedBy = explainPlanAttributes.getServerSortedBy();
      this.serverOffset = explainPlanAttributes.getServerOffset();
      this.serverRowLimit = explainPlanAttributes.getServerRowLimit();
      this.clientFilterBy = explainPlanAttributes.getClientFilterBy();
      this.clientAggregate = explainPlanAttributes.getClientAggregate();
      this.clientDistinctFilter = explainPlanAttributes.getClientDistinctFilter();
      this.clientAfterAggregate = explainPlanAttributes.getClientAfterAggregate();
      this.clientSortAlgo = explainPlanAttributes.getClientSortAlgo();
      this.clientSortedBy = explainPlanAttributes.getClientSortedBy();
      this.clientOffset = explainPlanAttributes.getClientOffset();
      this.clientRowLimit = explainPlanAttributes.getClientRowLimit();
      this.clientSequenceCount = explainPlanAttributes.getClientSequenceCount();
      this.clientCursorName = explainPlanAttributes.getClientCursorName();
      List<String> srcClientSteps = explainPlanAttributes.getClientSteps();
      this.clientSteps = srcClientSteps == null ? null : new ArrayList<>(srcClientSteps);
      this.lhsJoinQueryExplainPlan = explainPlanAttributes.getLhsJoinQueryExplainPlan();
      this.rhsJoinQueryExplainPlan = explainPlanAttributes.getRhsJoinQueryExplainPlan();
      this.subPlans = explainPlanAttributes.getSubPlans();
      this.dynamicServerFilter = explainPlanAttributes.getDynamicServerFilter();
      this.afterJoinFilter = explainPlanAttributes.getAfterJoinFilter();
      this.joinScannerLimit = explainPlanAttributes.getJoinScannerLimit();
      this.sortMergeSkipMerge = explainPlanAttributes.isSortMergeSkipMerge();
      this.regionLocations = explainPlanAttributes.getRegionLocations();
      this.regionLocationsTotalSize = explainPlanAttributes.getRegionLocationsTotalSize();
      this.numRegionLocationLookups = explainPlanAttributes.getNumRegionLocationLookups();
    }

    public ExplainPlanAttributesBuilder setTenantId(String tenantId) {
      this.tenantId = tenantId;
      return this;
    }

    public ExplainPlanAttributesBuilder setViewName(String viewName) {
      this.viewName = viewName;
      return this;
    }

    public ExplainPlanAttributesBuilder setViewBaseName(String viewBaseName) {
      this.viewBaseName = viewBaseName;
      return this;
    }

    public ExplainPlanAttributesBuilder setCdcScopes(String cdcScopes) {
      this.cdcScopes = cdcScopes;
      return this;
    }

    public ExplainPlanAttributesBuilder setTxnProvider(String txnProvider) {
      this.txnProvider = txnProvider;
      return this;
    }

    public ExplainPlanAttributesBuilder setRewrites(List<String> rewrites) {
      this.rewrites = rewrites == null ? null : new ArrayList<>(rewrites);
      return this;
    }

    public ExplainPlanAttributesBuilder addRewrite(String rewrite) {
      if (this.rewrites == null) {
        this.rewrites = new ArrayList<>();
      }
      this.rewrites.add(rewrite);
      return this;
    }

    public ExplainPlanAttributesBuilder setEstimatedRows(Long estimatedRows) {
      this.estimatedRows = estimatedRows;
      return this;
    }

    public ExplainPlanAttributesBuilder setEstimatedSizeInBytes(Long estimatedSizeInBytes) {
      this.estimatedSizeInBytes = estimatedSizeInBytes;
      return this;
    }

    public ExplainPlanAttributesBuilder setEstimateInfoTs(Long estimateInfoTs) {
      this.estimateInfoTs = estimateInfoTs;
      return this;
    }

    public ExplainPlanAttributesBuilder setAbstractExplainPlan(String abstractExplainPlan) {
      this.abstractExplainPlan = abstractExplainPlan;
      return this;
    }

    public ExplainPlanAttributesBuilder
      setOnDuplicateKeyAction(OnDuplicateKeyType onDuplicateKeyAction) {
      this.onDuplicateKeyAction = onDuplicateKeyAction;
      return this;
    }

    public ExplainPlanAttributesBuilder setServerUpdateSet(List<String> serverUpdateSet) {
      this.serverUpdateSet = serverUpdateSet == null ? null : new ArrayList<>(serverUpdateSet);
      return this;
    }

    public ExplainPlanAttributesBuilder setReturningRow(boolean returningRow) {
      this.returningRow = returningRow;
      return this;
    }

    public ExplainPlanAttributesBuilder setHint(HintNode.Hint hint) {
      this.hint = hint;
      return this;
    }

    public ExplainPlanAttributesBuilder setExplainScanType(String explainScanType) {
      this.explainScanType = explainScanType;
      return this;
    }

    public ExplainPlanAttributesBuilder setConsistency(Consistency consistency) {
      this.consistency = consistency;
      return this;
    }

    public ExplainPlanAttributesBuilder setTableName(String tableName) {
      this.tableName = tableName;
      return this;
    }

    public ExplainPlanAttributesBuilder setKeyRanges(String keyRanges) {
      this.keyRanges = keyRanges;
      return this;
    }

    public ExplainPlanAttributesBuilder setIndexName(String indexName) {
      this.indexName = indexName;
      return this;
    }

    public ExplainPlanAttributesBuilder setIndexKind(String indexKind) {
      this.indexKind = indexKind;
      return this;
    }

    public ExplainPlanAttributesBuilder setIndexRule(String indexRule) {
      this.indexRule = indexRule;
      return this;
    }

    public ExplainPlanAttributesBuilder setIndexRejected(List<RejectedIndexEntry> indexRejected) {
      this.indexRejected = indexRejected == null ? null : new ArrayList<>(indexRejected);
      return this;
    }

    public ExplainPlanAttributesBuilder setSaltBuckets(Integer saltBuckets) {
      this.saltBuckets = saltBuckets;
      return this;
    }

    public ExplainPlanAttributesBuilder setRegionsPlanned(Integer regionsPlanned) {
      this.regionsPlanned = regionsPlanned;
      return this;
    }

    public ExplainPlanAttributesBuilder setScanTimeRangeMin(Long scanTimeRangeMin) {
      this.scanTimeRangeMin = scanTimeRangeMin;
      return this;
    }

    public ExplainPlanAttributesBuilder setScanTimeRangeMax(Long scanTimeRangeMax) {
      this.scanTimeRangeMax = scanTimeRangeMax;
      return this;
    }

    public ExplainPlanAttributesBuilder setSplitsChunk(Integer splitsChunk) {
      this.splitsChunk = splitsChunk;
      return this;
    }

    public ExplainPlanAttributesBuilder setUseRoundRobinIterator(boolean useRoundRobinIterator) {
      this.useRoundRobinIterator = useRoundRobinIterator;
      return this;
    }

    public ExplainPlanAttributesBuilder setSamplingRate(Double samplingRate) {
      this.samplingRate = samplingRate;
      return this;
    }

    public ExplainPlanAttributesBuilder setHexStringRVCOffset(String hexStringRVCOffset) {
      this.hexStringRVCOffset = hexStringRVCOffset;
      return this;
    }

    public ExplainPlanAttributesBuilder setIteratorTypeAndScanSize(String iteratorTypeAndScanSize) {
      this.iteratorTypeAndScanSize = iteratorTypeAndScanSize;
      return this;
    }

    public ExplainPlanAttributesBuilder setScanEstimatedRows(Long scanEstimatedRows) {
      this.scanEstimatedRows = scanEstimatedRows;
      return this;
    }

    public ExplainPlanAttributesBuilder setScanEstimatedSizeInBytes(Long scanEstimatedSizeInBytes) {
      this.scanEstimatedSizeInBytes = scanEstimatedSizeInBytes;
      return this;
    }

    public ExplainPlanAttributesBuilder setServerWhereFilter(String serverWhereFilter) {
      this.serverWhereFilter = serverWhereFilter;
      return this;
    }

    public ExplainPlanAttributesBuilder setServerDistinctFilter(String serverDistinctFilter) {
      this.serverDistinctFilter = serverDistinctFilter;
      return this;
    }

    public ExplainPlanAttributesBuilder setServerMergeColumns(Set<PColumn> columns) {
      this.serverMergeColumns = columns;
      return this;
    }

    public ExplainPlanAttributesBuilder
      setServerParsedProjections(Map<String, List<String>> serverParsedProjections) {
      this.serverParsedProjections =
        serverParsedProjections == null ? null : new LinkedHashMap<>(serverParsedProjections);
      return this;
    }

    public ExplainPlanAttributesBuilder addServerParsedProjection(String label,
      List<String> details) {
      if (this.serverParsedProjections == null) {
        this.serverParsedProjections = new LinkedHashMap<>();
      }
      this.serverParsedProjections.put(label,
        Collections.unmodifiableList(new ArrayList<>(details)));
      return this;
    }

    public ExplainPlanAttributesBuilder
      setServerFirstKeyOnlyProjection(boolean serverFirstKeyOnlyProjection) {
      this.serverFirstKeyOnlyProjection = serverFirstKeyOnlyProjection;
      return this;
    }

    public ExplainPlanAttributesBuilder
      setServerEmptyColumnOnlyProjection(boolean serverEmptyColumnOnlyProjection) {
      this.serverEmptyColumnOnlyProjection = serverEmptyColumnOnlyProjection;
      return this;
    }

    public ExplainPlanAttributesBuilder setServerAggregate(String serverAggregate) {
      this.serverAggregate = serverAggregate;
      return this;
    }

    public ExplainPlanAttributesBuilder setServerGroupByLimit(Integer serverGroupByLimit) {
      this.serverGroupByLimit = serverGroupByLimit;
      return this;
    }

    public ExplainPlanAttributesBuilder setServerSortedBy(String serverSortedBy) {
      this.serverSortedBy = serverSortedBy;
      return this;
    }

    public ExplainPlanAttributesBuilder setServerOffset(Integer serverOffset) {
      this.serverOffset = serverOffset;
      return this;
    }

    public ExplainPlanAttributesBuilder setServerRowLimit(Long serverRowLimit) {
      this.serverRowLimit = serverRowLimit;
      return this;
    }

    public ExplainPlanAttributesBuilder setClientFilterBy(String clientFilterBy) {
      this.clientFilterBy = clientFilterBy;
      return this;
    }

    public ExplainPlanAttributesBuilder setClientAggregate(String clientAggregate) {
      this.clientAggregate = clientAggregate;
      return this;
    }

    public ExplainPlanAttributesBuilder setClientDistinctFilter(String clientDistinctFilter) {
      this.clientDistinctFilter = clientDistinctFilter;
      return this;
    }

    public ExplainPlanAttributesBuilder setClientAfterAggregate(String clientAfterAggregate) {
      this.clientAfterAggregate = clientAfterAggregate;
      return this;
    }

    public ExplainPlanAttributesBuilder setClientSortAlgo(String clientSortAlgo) {
      this.clientSortAlgo = clientSortAlgo;
      return this;
    }

    public ExplainPlanAttributesBuilder setClientSortedBy(String clientSortedBy) {
      this.clientSortedBy = clientSortedBy;
      return this;
    }

    public ExplainPlanAttributesBuilder setClientOffset(Integer clientOffset) {
      this.clientOffset = clientOffset;
      return this;
    }

    public ExplainPlanAttributesBuilder setClientRowLimit(Integer clientRowLimit) {
      this.clientRowLimit = clientRowLimit;
      return this;
    }

    public ExplainPlanAttributesBuilder setClientSequenceCount(Integer clientSequenceCount) {
      this.clientSequenceCount = clientSequenceCount;
      return this;
    }

    public ExplainPlanAttributesBuilder setClientCursorName(String clientCursorName) {
      this.clientCursorName = clientCursorName;
      return this;
    }

    public ExplainPlanAttributesBuilder setClientSteps(List<String> clientSteps) {
      this.clientSteps = clientSteps == null ? null : new ArrayList<>(clientSteps);
      return this;
    }

    public ExplainPlanAttributesBuilder addClientStep(String step) {
      if (this.clientSteps == null) {
        this.clientSteps = new ArrayList<>();
      }
      this.clientSteps.add(step);
      return this;
    }

    public ExplainPlanAttributesBuilder
      setLhsJoinQueryExplainPlan(ExplainPlanAttributes lhsJoinQueryExplainPlan) {
      this.lhsJoinQueryExplainPlan = lhsJoinQueryExplainPlan;
      return this;
    }

    public ExplainPlanAttributesBuilder
      setRhsJoinQueryExplainPlan(ExplainPlanAttributes rhsJoinQueryExplainPlan) {
      this.rhsJoinQueryExplainPlan = rhsJoinQueryExplainPlan;
      return this;
    }

    public ExplainPlanAttributesBuilder setSubPlans(List<ExplainPlanAttributes> subPlans) {
      this.subPlans = subPlans;
      return this;
    }

    public ExplainPlanAttributesBuilder setDynamicServerFilter(String dynamicServerFilter) {
      this.dynamicServerFilter = dynamicServerFilter;
      return this;
    }

    public ExplainPlanAttributesBuilder setAfterJoinFilter(String afterJoinFilter) {
      this.afterJoinFilter = afterJoinFilter;
      return this;
    }

    public ExplainPlanAttributesBuilder setJoinScannerLimit(Long joinScannerLimit) {
      this.joinScannerLimit = joinScannerLimit;
      return this;
    }

    public ExplainPlanAttributesBuilder setSortMergeSkipMerge(boolean sortMergeSkipMerge) {
      this.sortMergeSkipMerge = sortMergeSkipMerge;
      return this;
    }

    public ExplainPlanAttributesBuilder setRegionLocations(List<HRegionLocation> regionLocations) {
      this.regionLocations = regionLocations;
      return this;
    }

    public ExplainPlanAttributesBuilder
      setRegionLocationsTotalSize(Integer regionLocationsTotalSize) {
      this.regionLocationsTotalSize = regionLocationsTotalSize;
      return this;
    }

    public ExplainPlanAttributesBuilder setNumRegionLocationLookups(int numRegionLocationLookups) {
      this.numRegionLocationLookups = numRegionLocationLookups;
      return this;
    }

    public ExplainPlanAttributes build() {
      return new ExplainPlanAttributes(tenantId, viewName, viewBaseName, cdcScopes, txnProvider,
        rewrites, estimatedRows, estimatedSizeInBytes, estimateInfoTs, abstractExplainPlan,
        onDuplicateKeyAction, serverUpdateSet, returningRow, hint, explainScanType, consistency,
        tableName, keyRanges, indexName, indexKind, indexRule, indexRejected, saltBuckets,
        regionsPlanned, scanTimeRangeMin, scanTimeRangeMax, splitsChunk, useRoundRobinIterator,
        samplingRate, hexStringRVCOffset, iteratorTypeAndScanSize, scanEstimatedRows,
        scanEstimatedSizeInBytes, serverWhereFilter, serverDistinctFilter, serverMergeColumns,
        serverParsedProjections, serverFirstKeyOnlyProjection, serverEmptyColumnOnlyProjection,
        serverAggregate, serverGroupByLimit, serverSortedBy, serverOffset, serverRowLimit,
        clientFilterBy, clientAggregate, clientDistinctFilter, clientAfterAggregate, clientSortAlgo,
        clientSortedBy, clientOffset, clientRowLimit, clientSequenceCount, clientCursorName,
        clientSteps, lhsJoinQueryExplainPlan, rhsJoinQueryExplainPlan, subPlans,
        dynamicServerFilter, afterJoinFilter, joinScannerLimit, sortMergeSkipMerge, regionLocations,
        regionLocationsTotalSize, numRegionLocationLookups);
    }
  }
}
