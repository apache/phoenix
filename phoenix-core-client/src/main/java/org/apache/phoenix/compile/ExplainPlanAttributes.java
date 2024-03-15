/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.phoenix.compile;

import java.util.List;
import java.util.Set;

import org.apache.hadoop.hbase.HRegionLocation;
import org.apache.hadoop.hbase.client.Consistency;
import org.apache.phoenix.parse.HintNode;
import org.apache.phoenix.parse.HintNode.Hint;
import org.apache.phoenix.schema.PColumn;

/**
 * ExplainPlan attributes that contain individual attributes of ExplainPlan
 * that we can assert against. This also makes attribute retrieval easier
 * as an API rather than retrieving list of Strings containing entire plan.
 */
public class ExplainPlanAttributes {

    private final String abstractExplainPlan;
    private final Integer splitsChunk;
    private final Long estimatedRows;
    private final Long estimatedSizeInBytes;
    private final String iteratorTypeAndScanSize;
    private final Double samplingRate;
    private final boolean useRoundRobinIterator;
    private final String hexStringRVCOffset;
    private final Consistency consistency;
    private final Hint hint;
    private final String serverSortedBy;
    private final String explainScanType;
    private final String tableName;
    private final String keyRanges;
    private final Long scanTimeRangeMin;
    private final Long scanTimeRangeMax;
    private final String serverWhereFilter;
    private final String serverDistinctFilter;
    private final Integer serverOffset;
    private final Long serverRowLimit;
    private final boolean serverArrayElementProjection;
    private final String serverAggregate;
    private final String clientFilterBy;
    private final String clientAggregate;
    private final String clientSortedBy;
    private final String clientAfterAggregate;
    private final String clientDistinctFilter;
    private final Integer clientOffset;
    private final Integer clientRowLimit;
    private final Integer clientSequenceCount;
    private final String clientCursorName;
    private final String clientSortAlgo;
    // This object represents PlanAttributes object for rhs query
    // to be used only by Join queries. In case of Join query, lhs plan is
    // represented by 'this' object and rhs plan is represented by
    // 'rhsJoinQueryExplainPlan' object (which in turn should
    // have null rhsJoinQueryExplainPlan)
    // For non-Join queries related Plans, rhsJoinQueryExplainPlan will always
    // be null
    private final ExplainPlanAttributes rhsJoinQueryExplainPlan;
    private final Set<PColumn> serverMergeColumns;
    private final List<HRegionLocation> regionLocations;
    private final int numRegionLocationLookups;

    private static final ExplainPlanAttributes EXPLAIN_PLAN_INSTANCE =
        new ExplainPlanAttributes();

    private ExplainPlanAttributes() {
        this.abstractExplainPlan = null;
        this.splitsChunk = null;
        this.estimatedRows = null;
        this.estimatedSizeInBytes = null;
        this.iteratorTypeAndScanSize = null;
        this.samplingRate = null;
        this.useRoundRobinIterator = false;
        this.hexStringRVCOffset = null;
        this.consistency = null;
        this.hint = null;
        this.serverSortedBy = null;
        this.explainScanType = null;
        this.tableName = null;
        this.keyRanges = null;
        this.scanTimeRangeMin = null;
        this.scanTimeRangeMax = null;
        this.serverWhereFilter = null;
        this.serverDistinctFilter = null;
        this.serverOffset = null;
        this.serverRowLimit = null;
        this.serverArrayElementProjection = false;
        this.serverAggregate = null;
        this.clientFilterBy = null;
        this.clientAggregate = null;
        this.clientSortedBy = null;
        this.clientAfterAggregate = null;
        this.clientDistinctFilter = null;
        this.clientOffset = null;
        this.clientRowLimit = null;
        this.clientSequenceCount = null;
        this.clientCursorName = null;
        this.clientSortAlgo = null;
        this.rhsJoinQueryExplainPlan = null;
        this.serverMergeColumns = null;
        this.regionLocations = null;
        this.numRegionLocationLookups = 0;
    }

    public ExplainPlanAttributes(String abstractExplainPlan,
            Integer splitsChunk, Long estimatedRows, Long estimatedSizeInBytes,
            String iteratorTypeAndScanSize, Double samplingRate,
            boolean useRoundRobinIterator,
            String hexStringRVCOffset, Consistency consistency,
            Hint hint, String serverSortedBy, String explainScanType,
            String tableName, String keyRanges, Long scanTimeRangeMin,
            Long scanTimeRangeMax, String serverWhereFilter,
            String serverDistinctFilter,
            Integer serverOffset, Long serverRowLimit,
            boolean serverArrayElementProjection, String serverAggregate,
            String clientFilterBy, String clientAggregate,
            String clientSortedBy,
            String clientAfterAggregate, String clientDistinctFilter,
            Integer clientOffset, Integer clientRowLimit,
            Integer clientSequenceCount, String clientCursorName,
            String clientSortAlgo,
            ExplainPlanAttributes rhsJoinQueryExplainPlan, Set<PColumn> serverMergeColumns,
            List<HRegionLocation> regionLocations, int numRegionLocationLookups) {
        this.abstractExplainPlan = abstractExplainPlan;
        this.splitsChunk = splitsChunk;
        this.estimatedRows = estimatedRows;
        this.estimatedSizeInBytes = estimatedSizeInBytes;
        this.iteratorTypeAndScanSize = iteratorTypeAndScanSize;
        this.samplingRate = samplingRate;
        this.useRoundRobinIterator = useRoundRobinIterator;
        this.hexStringRVCOffset = hexStringRVCOffset;
        this.consistency = consistency;
        this.hint = hint;
        this.serverSortedBy = serverSortedBy;
        this.explainScanType = explainScanType;
        this.tableName = tableName;
        this.keyRanges = keyRanges;
        this.scanTimeRangeMin = scanTimeRangeMin;
        this.scanTimeRangeMax = scanTimeRangeMax;
        this.serverWhereFilter = serverWhereFilter;
        this.serverDistinctFilter = serverDistinctFilter;
        this.serverOffset = serverOffset;
        this.serverRowLimit = serverRowLimit;
        this.serverArrayElementProjection = serverArrayElementProjection;
        this.serverAggregate = serverAggregate;
        this.clientFilterBy = clientFilterBy;
        this.clientAggregate = clientAggregate;
        this.clientSortedBy = clientSortedBy;
        this.clientAfterAggregate = clientAfterAggregate;
        this.clientDistinctFilter = clientDistinctFilter;
        this.clientOffset = clientOffset;
        this.clientRowLimit = clientRowLimit;
        this.clientSequenceCount = clientSequenceCount;
        this.clientCursorName = clientCursorName;
        this.clientSortAlgo = clientSortAlgo;
        this.rhsJoinQueryExplainPlan = rhsJoinQueryExplainPlan;
        this.serverMergeColumns = serverMergeColumns;
        this.regionLocations = regionLocations;
        this.numRegionLocationLookups = numRegionLocationLookups;
    }

    public String getAbstractExplainPlan() {
        return abstractExplainPlan;
    }

    public Integer getSplitsChunk() {
        return splitsChunk;
    }

    public Long getEstimatedRows() {
        return estimatedRows;
    }

    public Long getEstimatedSizeInBytes() {
        return estimatedSizeInBytes;
    }

    public String getIteratorTypeAndScanSize() {
        return iteratorTypeAndScanSize;
    }

    public Double getSamplingRate() {
        return samplingRate;
    }

    public boolean isUseRoundRobinIterator() {
        return useRoundRobinIterator;
    }

    public String getHexStringRVCOffset() {
        return hexStringRVCOffset;
    }

    public Consistency getConsistency() {
        return consistency;
    }

    public Hint getHint() {
        return hint;
    }

    public String getServerSortedBy() {
        return serverSortedBy;
    }

    public String getExplainScanType() {
        return explainScanType;
    }

    public String getTableName() {
        return tableName;
    }

    public String getKeyRanges() {
        return keyRanges;
    }

    public Long getScanTimeRangeMin() {
        return scanTimeRangeMin;
    }

    public Long getScanTimeRangeMax() {
        return scanTimeRangeMax;
    }

    public String getServerWhereFilter() {
        return serverWhereFilter;
    }

    public String getServerDistinctFilter() {
        return serverDistinctFilter;
    }

    public Integer getServerOffset() {
        return serverOffset;
    }

    public Long getServerRowLimit() {
        return serverRowLimit;
    }

    public boolean isServerArrayElementProjection() {
        return serverArrayElementProjection;
    }

    public String getServerAggregate() {
        return serverAggregate;
    }

    public String getClientFilterBy() {
        return clientFilterBy;
    }

    public String getClientAggregate() {
        return clientAggregate;
    }

    public String getClientSortedBy() {
        return clientSortedBy;
    }

    public String getClientAfterAggregate() {
        return clientAfterAggregate;
    }

    public String getClientDistinctFilter() {
        return clientDistinctFilter;
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

    public String getClientSortAlgo() {
        return clientSortAlgo;
    }

    public ExplainPlanAttributes getRhsJoinQueryExplainPlan() {
        return rhsJoinQueryExplainPlan;
    }

    public Set<PColumn> getServerMergeColumns() {
        return serverMergeColumns;
    }

    public List<HRegionLocation> getRegionLocations() {
        return regionLocations;
    }

    public int getNumRegionLocationLookups() {
        return numRegionLocationLookups;
    }

    public static ExplainPlanAttributes getDefaultExplainPlan() {
        return EXPLAIN_PLAN_INSTANCE;
    }

    public static class ExplainPlanAttributesBuilder {
        private String abstractExplainPlan;
        private Integer splitsChunk;
        private Long estimatedRows;
        private Long estimatedSizeInBytes;
        private String iteratorTypeAndScanSize;
        private Double samplingRate;
        private boolean useRoundRobinIterator;
        private String hexStringRVCOffset;
        private Consistency consistency;
        private HintNode.Hint hint;
        private String serverSortedBy;
        private String explainScanType;
        private String tableName;
        private String keyRanges;
        private Long scanTimeRangeMin;
        private Long scanTimeRangeMax;
        private String serverWhereFilter;
        private String serverDistinctFilter;
        private Integer serverOffset;
        private Long serverRowLimit;
        private boolean serverArrayElementProjection;
        private String serverAggregate;
        private String clientFilterBy;
        private String clientAggregate;
        private String clientSortedBy;
        private String clientAfterAggregate;
        private String clientDistinctFilter;
        private Integer clientOffset;
        private Integer clientRowLimit;
        private Integer clientSequenceCount;
        private String clientCursorName;
        private String clientSortAlgo;
        private ExplainPlanAttributes rhsJoinQueryExplainPlan;
        private Set<PColumn> serverMergeColumns;
        private List<HRegionLocation> regionLocations;
        private int numRegionLocationLookups;

        public ExplainPlanAttributesBuilder() {
            // default
        }

        public ExplainPlanAttributesBuilder(
                ExplainPlanAttributes explainPlanAttributes) {
            this.abstractExplainPlan =
                explainPlanAttributes.getAbstractExplainPlan();
            this.splitsChunk = explainPlanAttributes.getSplitsChunk();
            this.estimatedRows = explainPlanAttributes.getEstimatedRows();
            this.estimatedSizeInBytes =
                explainPlanAttributes.getEstimatedSizeInBytes();
            this.iteratorTypeAndScanSize = explainPlanAttributes.getIteratorTypeAndScanSize();
            this.samplingRate = explainPlanAttributes.getSamplingRate();
            this.useRoundRobinIterator =
                explainPlanAttributes.isUseRoundRobinIterator();
            this.hexStringRVCOffset =
                explainPlanAttributes.getHexStringRVCOffset();
            this.consistency = explainPlanAttributes.getConsistency();
            this.hint = explainPlanAttributes.getHint();
            this.serverSortedBy = explainPlanAttributes.getServerSortedBy();
            this.explainScanType = explainPlanAttributes.getExplainScanType();
            this.tableName = explainPlanAttributes.getTableName();
            this.keyRanges = explainPlanAttributes.getKeyRanges();
            this.scanTimeRangeMin = explainPlanAttributes.getScanTimeRangeMin();
            this.scanTimeRangeMax = explainPlanAttributes.getScanTimeRangeMax();
            this.serverWhereFilter =
                explainPlanAttributes.getServerWhereFilter();
            this.serverDistinctFilter =
                explainPlanAttributes.getServerDistinctFilter();
            this.serverOffset = explainPlanAttributes.getServerOffset();
            this.serverRowLimit = explainPlanAttributes.getServerRowLimit();
            this.serverArrayElementProjection =
                explainPlanAttributes.isServerArrayElementProjection();
            this.serverAggregate = explainPlanAttributes.getServerAggregate();
            this.clientFilterBy = explainPlanAttributes.getClientFilterBy();
            this.clientAggregate = explainPlanAttributes.getClientAggregate();
            this.clientSortedBy = explainPlanAttributes.getClientSortedBy();
            this.clientAfterAggregate =
                explainPlanAttributes.getClientAfterAggregate();
            this.clientDistinctFilter =
                explainPlanAttributes.getClientDistinctFilter();
            this.clientOffset = explainPlanAttributes.getClientOffset();
            this.clientRowLimit = explainPlanAttributes.getClientRowLimit();
            this.clientSequenceCount =
                explainPlanAttributes.getClientSequenceCount();
            this.clientCursorName = explainPlanAttributes.getClientCursorName();
            this.clientSortAlgo = explainPlanAttributes.getClientSortAlgo();
            this.rhsJoinQueryExplainPlan =
                explainPlanAttributes.getRhsJoinQueryExplainPlan();
            this.serverMergeColumns = explainPlanAttributes.getServerMergeColumns();
            this.regionLocations = explainPlanAttributes.getRegionLocations();
            this.numRegionLocationLookups = explainPlanAttributes.getNumRegionLocationLookups();
        }

        public ExplainPlanAttributesBuilder setAbstractExplainPlan(
                String abstractExplainPlan) {
            this.abstractExplainPlan = abstractExplainPlan;
            return this;
        }

        public ExplainPlanAttributesBuilder setSplitsChunk(
                Integer splitsChunk) {
            this.splitsChunk = splitsChunk;
            return this;
        }

        public ExplainPlanAttributesBuilder setEstimatedRows(
                Long estimatedRows) {
            this.estimatedRows = estimatedRows;
            return this;
        }

        public ExplainPlanAttributesBuilder setEstimatedSizeInBytes(
                Long estimatedSizeInBytes) {
            this.estimatedSizeInBytes = estimatedSizeInBytes;
            return this;
        }

        public ExplainPlanAttributesBuilder setIteratorTypeAndScanSize(
                String iteratorTypeAndScanSize) {
            this.iteratorTypeAndScanSize = iteratorTypeAndScanSize;
            return this;
        }

        public ExplainPlanAttributesBuilder setSamplingRate(
                Double samplingRate) {
            this.samplingRate = samplingRate;
            return this;
        }

        public ExplainPlanAttributesBuilder setUseRoundRobinIterator(
                boolean useRoundRobinIterator) {
            this.useRoundRobinIterator = useRoundRobinIterator;
            return this;
        }

        public ExplainPlanAttributesBuilder setHexStringRVCOffset(
                String hexStringRVCOffset) {
            this.hexStringRVCOffset = hexStringRVCOffset;
            return this;
        }

        public ExplainPlanAttributesBuilder setConsistency(
                Consistency consistency) {
            this.consistency = consistency;
            return this;
        }

        public ExplainPlanAttributesBuilder setHint(HintNode.Hint hint) {
            this.hint = hint;
            return this;
        }

        public ExplainPlanAttributesBuilder setServerSortedBy(
                String serverSortedBy) {
            this.serverSortedBy = serverSortedBy;
            return this;
        }

        public ExplainPlanAttributesBuilder setExplainScanType(
                String explainScanType) {
            this.explainScanType = explainScanType;
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

        public ExplainPlanAttributesBuilder setScanTimeRangeMin(
                Long scanTimeRangeMin) {
            this.scanTimeRangeMin = scanTimeRangeMin;
            return this;
        }

        public ExplainPlanAttributesBuilder setScanTimeRangeMax(
                Long scanTimeRangeMax) {
            this.scanTimeRangeMax = scanTimeRangeMax;
            return this;
        }

        public ExplainPlanAttributesBuilder setServerWhereFilter(
                String serverWhereFilter) {
            this.serverWhereFilter = serverWhereFilter;
            return this;
        }

        public ExplainPlanAttributesBuilder setServerDistinctFilter(
                String serverDistinctFilter) {
            this.serverDistinctFilter = serverDistinctFilter;
            return this;
        }

        public ExplainPlanAttributesBuilder setServerOffset(
                Integer serverOffset) {
            this.serverOffset = serverOffset;
            return this;
        }

        public ExplainPlanAttributesBuilder setServerRowLimit(
                Long serverRowLimit) {
            this.serverRowLimit = serverRowLimit;
            return this;
        }

        public ExplainPlanAttributesBuilder setServerArrayElementProjection(
                boolean serverArrayElementProjection) {
            this.serverArrayElementProjection = serverArrayElementProjection;
            return this;
        }

        public ExplainPlanAttributesBuilder setServerAggregate(
                String serverAggregate) {
            this.serverAggregate = serverAggregate;
            return this;
        }

        public ExplainPlanAttributesBuilder setClientFilterBy(
                String clientFilterBy) {
            this.clientFilterBy = clientFilterBy;
            return this;
        }

        public ExplainPlanAttributesBuilder setClientAggregate(
                String clientAggregate) {
            this.clientAggregate = clientAggregate;
            return this;
        }

        public ExplainPlanAttributesBuilder setClientSortedBy(
                String clientSortedBy) {
            this.clientSortedBy = clientSortedBy;
            return this;
        }

        public ExplainPlanAttributesBuilder setClientAfterAggregate(
                String clientAfterAggregate) {
            this.clientAfterAggregate = clientAfterAggregate;
            return this;
        }

        public ExplainPlanAttributesBuilder setClientDistinctFilter(
                String clientDistinctFilter) {
            this.clientDistinctFilter = clientDistinctFilter;
            return this;
        }

        public ExplainPlanAttributesBuilder setClientOffset(
                Integer clientOffset) {
            this.clientOffset = clientOffset;
            return this;
        }

        public ExplainPlanAttributesBuilder setClientRowLimit(
                Integer clientRowLimit) {
            this.clientRowLimit = clientRowLimit;
            return this;
        }

        public ExplainPlanAttributesBuilder setClientSequenceCount(
                Integer clientSequenceCount) {
            this.clientSequenceCount = clientSequenceCount;
            return this;
        }

        public ExplainPlanAttributesBuilder setClientCursorName(
                String clientCursorName) {
            this.clientCursorName = clientCursorName;
            return this;
        }

        public ExplainPlanAttributesBuilder setClientSortAlgo(
                String clientSortAlgo) {
            this.clientSortAlgo = clientSortAlgo;
            return this;
        }

        public ExplainPlanAttributesBuilder setRhsJoinQueryExplainPlan(
                ExplainPlanAttributes rhsJoinQueryExplainPlan) {
            this.rhsJoinQueryExplainPlan = rhsJoinQueryExplainPlan;
            return this;
        }

        public ExplainPlanAttributesBuilder setServerMergeColumns(
                Set<PColumn> columns) {
            this.serverMergeColumns = columns;
            return this;
        }

        public ExplainPlanAttributesBuilder setRegionLocations(
                List<HRegionLocation> regionLocations) {
            this.regionLocations = regionLocations;
            return this;
        }

        public ExplainPlanAttributesBuilder setNumRegionLocationLookups(
                int numRegionLocationLookups) {
            this.numRegionLocationLookups = numRegionLocationLookups;
            return this;
        }

        public ExplainPlanAttributes build() {
            return new ExplainPlanAttributes(abstractExplainPlan, splitsChunk,
                estimatedRows, estimatedSizeInBytes, iteratorTypeAndScanSize,
                samplingRate, useRoundRobinIterator, hexStringRVCOffset,
                consistency, hint, serverSortedBy, explainScanType, tableName,
                keyRanges, scanTimeRangeMin, scanTimeRangeMax,
                serverWhereFilter, serverDistinctFilter,
                serverOffset, serverRowLimit,
                serverArrayElementProjection, serverAggregate,
                clientFilterBy, clientAggregate, clientSortedBy,
                clientAfterAggregate, clientDistinctFilter, clientOffset,
                clientRowLimit, clientSequenceCount, clientCursorName,
                clientSortAlgo, rhsJoinQueryExplainPlan, serverMergeColumns,
                regionLocations, numRegionLocationLookups);
        }
    }
}
