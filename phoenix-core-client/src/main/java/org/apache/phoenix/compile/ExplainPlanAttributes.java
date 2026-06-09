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
import java.util.List;
import java.util.Set;
import org.apache.hadoop.hbase.HRegionLocation;
import org.apache.hadoop.hbase.client.Consistency;
import org.apache.phoenix.parse.HintNode;
import org.apache.phoenix.parse.HintNode.Hint;
import org.apache.phoenix.schema.PColumn;

/**
 * ExplainPlan attributes that contain individual attributes of ExplainPlan that we can assert
 * against. This also makes attribute retrieval easier as an API rather than retrieving list of
 * Strings containing entire plan.
 */
@JsonPropertyOrder({ "abstractExplainPlan", "hint", "explainScanType", "consistency", "tableName",
  "keyRanges", "indexName", "indexKind", "saltBuckets", "regionsPlanned", "scanTimeRangeMin",
  "scanTimeRangeMax", "splitsChunk", "useRoundRobinIterator", "samplingRate", "hexStringRVCOffset",
  "iteratorTypeAndScanSize", "estimatedRows", "estimatedSizeInBytes", "serverWhereFilter",
  "serverDistinctFilter", "serverMergeColumns", "serverArrayElementProjection", "serverAggregate",
  "serverGroupByLimit", "serverSortedBy", "serverOffset", "serverRowLimit", "clientFilterBy",
  "clientAggregate", "clientDistinctFilter", "clientAfterAggregate", "clientSortAlgo",
  "clientSortedBy", "clientOffset", "clientRowLimit", "clientSequenceCount", "clientCursorName",
  "clientSteps", "lhsJoinQueryExplainPlan", "rhsJoinQueryExplainPlan", "subPlans",
  "dynamicServerFilter", "afterJoinFilter", "joinScannerLimit", "sortMergeSkipMerge",
  "regionLocations", "regionLocationsTotalSize", "numRegionLocationLookups" })
public class ExplainPlanAttributes {

  // Plan identity and scan-level metadata
  private final String abstractExplainPlan;
  private final Hint hint;
  private final String explainScanType;
  private final Consistency consistency;
  private final String tableName;
  private final String keyRanges;
  private final String indexName;
  private final String indexKind;
  private final Integer saltBuckets;
  private final Integer regionsPlanned;
  private final Long scanTimeRangeMin;
  private final Long scanTimeRangeMax;
  private final Integer splitsChunk;
  private final boolean useRoundRobinIterator;
  private final Double samplingRate;
  private final String hexStringRVCOffset;
  private final String iteratorTypeAndScanSize;
  private final Long estimatedRows;
  private final Long estimatedSizeInBytes;

  // Server-side operations
  private final String serverWhereFilter;
  private final String serverDistinctFilter;
  private final Set<PColumn> serverMergeColumns;
  private final boolean serverArrayElementProjection;
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
    this.abstractExplainPlan = null;
    this.hint = null;
    this.explainScanType = null;
    this.consistency = null;
    this.tableName = null;
    this.keyRanges = null;
    this.indexName = null;
    this.indexKind = null;
    this.saltBuckets = null;
    this.regionsPlanned = null;
    this.scanTimeRangeMin = null;
    this.scanTimeRangeMax = null;
    this.splitsChunk = null;
    this.useRoundRobinIterator = false;
    this.samplingRate = null;
    this.hexStringRVCOffset = null;
    this.iteratorTypeAndScanSize = null;
    this.estimatedRows = null;
    this.estimatedSizeInBytes = null;
    this.serverWhereFilter = null;
    this.serverDistinctFilter = null;
    this.serverMergeColumns = null;
    this.serverArrayElementProjection = false;
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

  public ExplainPlanAttributes(String abstractExplainPlan, Hint hint, String explainScanType,
    Consistency consistency, String tableName, String keyRanges, String indexName, String indexKind,
    Integer saltBuckets, Integer regionsPlanned, Long scanTimeRangeMin, Long scanTimeRangeMax,
    Integer splitsChunk, boolean useRoundRobinIterator, Double samplingRate,
    String hexStringRVCOffset, String iteratorTypeAndScanSize, Long estimatedRows,
    Long estimatedSizeInBytes, String serverWhereFilter, String serverDistinctFilter,
    Set<PColumn> serverMergeColumns, boolean serverArrayElementProjection, String serverAggregate,
    Integer serverGroupByLimit, String serverSortedBy, Integer serverOffset, Long serverRowLimit,
    String clientFilterBy, String clientAggregate, String clientDistinctFilter,
    String clientAfterAggregate, String clientSortAlgo, String clientSortedBy, Integer clientOffset,
    Integer clientRowLimit, Integer clientSequenceCount, String clientCursorName,
    List<String> clientSteps, ExplainPlanAttributes lhsJoinQueryExplainPlan,
    ExplainPlanAttributes rhsJoinQueryExplainPlan, List<ExplainPlanAttributes> subPlans,
    String dynamicServerFilter, String afterJoinFilter, Long joinScannerLimit,
    boolean sortMergeSkipMerge, List<HRegionLocation> regionLocations,
    Integer regionLocationsTotalSize, int numRegionLocationLookups) {
    this.abstractExplainPlan = abstractExplainPlan;
    this.hint = hint;
    this.explainScanType = explainScanType;
    this.consistency = consistency;
    this.tableName = tableName;
    this.keyRanges = keyRanges;
    this.indexName = indexName;
    this.indexKind = indexKind;
    this.saltBuckets = saltBuckets;
    this.regionsPlanned = regionsPlanned;
    this.scanTimeRangeMin = scanTimeRangeMin;
    this.scanTimeRangeMax = scanTimeRangeMax;
    this.splitsChunk = splitsChunk;
    this.useRoundRobinIterator = useRoundRobinIterator;
    this.samplingRate = samplingRate;
    this.hexStringRVCOffset = hexStringRVCOffset;
    this.iteratorTypeAndScanSize = iteratorTypeAndScanSize;
    this.estimatedRows = estimatedRows;
    this.estimatedSizeInBytes = estimatedSizeInBytes;
    this.serverWhereFilter = serverWhereFilter;
    this.serverDistinctFilter = serverDistinctFilter;
    this.serverMergeColumns = serverMergeColumns;
    this.serverArrayElementProjection = serverArrayElementProjection;
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

  public String getAbstractExplainPlan() {
    return abstractExplainPlan;
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

  public Long getEstimatedRows() {
    return estimatedRows;
  }

  public Long getEstimatedSizeInBytes() {
    return estimatedSizeInBytes;
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

  public boolean isServerArrayElementProjection() {
    return serverArrayElementProjection;
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
    private String abstractExplainPlan;
    private HintNode.Hint hint;
    private String explainScanType;
    private Consistency consistency;
    private String tableName;
    private String keyRanges;
    private String indexName;
    private String indexKind;
    private Integer saltBuckets;
    private Integer regionsPlanned;
    private Long scanTimeRangeMin;
    private Long scanTimeRangeMax;
    private Integer splitsChunk;
    private boolean useRoundRobinIterator;
    private Double samplingRate;
    private String hexStringRVCOffset;
    private String iteratorTypeAndScanSize;
    private Long estimatedRows;
    private Long estimatedSizeInBytes;
    private String serverWhereFilter;
    private String serverDistinctFilter;
    private Set<PColumn> serverMergeColumns;
    private boolean serverArrayElementProjection;
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
      this.abstractExplainPlan = explainPlanAttributes.getAbstractExplainPlan();
      this.hint = explainPlanAttributes.getHint();
      this.explainScanType = explainPlanAttributes.getExplainScanType();
      this.consistency = explainPlanAttributes.getConsistency();
      this.tableName = explainPlanAttributes.getTableName();
      this.keyRanges = explainPlanAttributes.getKeyRanges();
      this.indexName = explainPlanAttributes.getIndexName();
      this.indexKind = explainPlanAttributes.getIndexKind();
      this.saltBuckets = explainPlanAttributes.getSaltBuckets();
      this.regionsPlanned = explainPlanAttributes.getRegionsPlanned();
      this.scanTimeRangeMin = explainPlanAttributes.getScanTimeRangeMin();
      this.scanTimeRangeMax = explainPlanAttributes.getScanTimeRangeMax();
      this.splitsChunk = explainPlanAttributes.getSplitsChunk();
      this.useRoundRobinIterator = explainPlanAttributes.isUseRoundRobinIterator();
      this.samplingRate = explainPlanAttributes.getSamplingRate();
      this.hexStringRVCOffset = explainPlanAttributes.getHexStringRVCOffset();
      this.iteratorTypeAndScanSize = explainPlanAttributes.getIteratorTypeAndScanSize();
      this.estimatedRows = explainPlanAttributes.getEstimatedRows();
      this.estimatedSizeInBytes = explainPlanAttributes.getEstimatedSizeInBytes();
      this.serverWhereFilter = explainPlanAttributes.getServerWhereFilter();
      this.serverDistinctFilter = explainPlanAttributes.getServerDistinctFilter();
      this.serverMergeColumns = explainPlanAttributes.getServerMergeColumns();
      this.serverArrayElementProjection = explainPlanAttributes.isServerArrayElementProjection();
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

    public ExplainPlanAttributesBuilder setAbstractExplainPlan(String abstractExplainPlan) {
      this.abstractExplainPlan = abstractExplainPlan;
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

    public ExplainPlanAttributesBuilder setEstimatedRows(Long estimatedRows) {
      this.estimatedRows = estimatedRows;
      return this;
    }

    public ExplainPlanAttributesBuilder setEstimatedSizeInBytes(Long estimatedSizeInBytes) {
      this.estimatedSizeInBytes = estimatedSizeInBytes;
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
      setServerArrayElementProjection(boolean serverArrayElementProjection) {
      this.serverArrayElementProjection = serverArrayElementProjection;
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
      return new ExplainPlanAttributes(abstractExplainPlan, hint, explainScanType, consistency,
        tableName, keyRanges, indexName, indexKind, saltBuckets, regionsPlanned, scanTimeRangeMin,
        scanTimeRangeMax, splitsChunk, useRoundRobinIterator, samplingRate, hexStringRVCOffset,
        iteratorTypeAndScanSize, estimatedRows, estimatedSizeInBytes, serverWhereFilter,
        serverDistinctFilter, serverMergeColumns, serverArrayElementProjection, serverAggregate,
        serverGroupByLimit, serverSortedBy, serverOffset, serverRowLimit, clientFilterBy,
        clientAggregate, clientDistinctFilter, clientAfterAggregate, clientSortAlgo, clientSortedBy,
        clientOffset, clientRowLimit, clientSequenceCount, clientCursorName, clientSteps,
        lhsJoinQueryExplainPlan, rhsJoinQueryExplainPlan, subPlans, dynamicServerFilter,
        afterJoinFilter, joinScannerLimit, sortMergeSkipMerge, regionLocations,
        regionLocationsTotalSize, numRegionLocationLookups);
    }
  }
}
