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
package org.apache.phoenix.iterate;

import java.io.IOException;
import java.sql.SQLException;
import java.text.Format;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.Set;

import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.HRegionLocation;
import org.apache.hadoop.hbase.client.Consistency;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.filter.FirstKeyOnlyFilter;
import org.apache.hadoop.hbase.filter.PageFilter;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.io.TimeRange;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.phoenix.compile.ExplainPlanAttributes
    .ExplainPlanAttributesBuilder;
import org.apache.phoenix.compile.GroupByCompiler.GroupBy;
import org.apache.phoenix.compile.OrderByCompiler.OrderBy;
import org.apache.phoenix.compile.ScanRanges;
import org.apache.phoenix.compile.StatementContext;
import org.apache.phoenix.coprocessor.BaseScannerRegionObserver;
import org.apache.phoenix.filter.BooleanExpressionFilter;
import org.apache.phoenix.filter.DistinctPrefixFilter;
import org.apache.phoenix.filter.EmptyColumnOnlyFilter;
import org.apache.phoenix.parse.HintNode;
import org.apache.phoenix.parse.HintNode.Hint;
import org.apache.phoenix.query.KeyRange;
import org.apache.phoenix.query.KeyRange.Bound;
import org.apache.phoenix.query.QueryServices;
import org.apache.phoenix.query.QueryServicesOptions;
import org.apache.phoenix.schema.PColumn;
import org.apache.phoenix.schema.RowKeySchema;
import org.apache.phoenix.schema.SortOrder;
import org.apache.phoenix.schema.TableRef;
import org.apache.phoenix.schema.types.PDataType;
import org.apache.phoenix.schema.types.PInteger;
import org.apache.phoenix.util.MetaDataUtil;
import org.apache.phoenix.util.ScanUtil;
import org.apache.phoenix.util.StringUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public abstract class ExplainTable {

    private static final Logger LOGGER = LoggerFactory.getLogger(ExplainTable.class);
    private static final List<KeyRange> EVERYTHING = Collections.singletonList(KeyRange.EVERYTHING_RANGE);
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
        this(context, table, GroupBy.EMPTY_GROUP_BY, OrderBy.EMPTY_ORDER_BY, HintNode.EMPTY_HINT_NODE, null, null);
    }

    public ExplainTable(StatementContext context, TableRef table, GroupBy groupBy, OrderBy orderBy, HintNode hintNode,
            Integer limit, Integer offset) {
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
     * Get regions that represent the given range of start and end key for the given table, and
     * all the regions to the regionLocations list.
     *
     * @param tableName the table name.
     * @param startKey the start rowkey.
     * @param endKey the end rowkey.
     * @param includeEndKey true if end key needs to be included.
     * @param reload true if reload from meta is necessary.
     * @param regionBoundaries set of region boundaries to get the unique list of region locations.
     * @param regionLocations the list of region locations as output.
     * @throws IOException if something goes wrong while creating connection or querying region
     * locations.
     */
    private void getRegionsInRange(final byte[] tableName,
                                   final byte[] startKey,
                                   final byte[] endKey,
                                   final boolean includeEndKey,
                                   final boolean reload,
                                   Set<RegionBoundary> regionBoundaries,
                                   List<HRegionLocation> regionLocations)
            throws IOException, SQLException {
        final boolean endKeyIsEndOfTable = Bytes.equals(endKey, HConstants.EMPTY_END_ROW);
        if ((Bytes.compareTo(startKey, endKey) > 0) && !endKeyIsEndOfTable) {
            throw new IllegalArgumentException(
                    "Invalid range: " + Bytes.toStringBinary(startKey) + " > " +
                            Bytes.toStringBinary(endKey));
        }
        byte[] currentKey = startKey;
        try (Table table = context.getConnection().getQueryServices().getTable(tableName)) {
            do {
                HRegionLocation regionLocation =
                        table.getRegionLocator().getRegionLocation(currentKey, reload);
                RegionBoundary regionBoundary =
                        new RegionBoundary(regionLocation.getRegion().getStartKey(),
                                regionLocation.getRegion().getEndKey());
                if (!regionBoundaries.contains(regionBoundary)) {
                    regionLocations.add(regionLocation);
                    regionBoundaries.add(regionBoundary);
                }
                currentKey = regionLocation.getRegion().getEndKey();
                // condition1 = currentKey != END_ROW_KEY
                // condition2 = endKeyIsEndOfTable == true
                // condition3 = currentKey < endKey
                // condition4 = includeEndKey == true
                // condition5 = currentKey == endKey
                // while (condition1 && (condition2 || condition3 || (condition4 && condition5)))
            } while (!Bytes.equals(currentKey, HConstants.EMPTY_END_ROW)
                    && (endKeyIsEndOfTable || Bytes.compareTo(currentKey, endKey) < 0
                    || (includeEndKey && Bytes.compareTo(currentKey, endKey) == 0)));
        }
    }

    protected void explain(String prefix,
                           List<String> planSteps,
                           ExplainPlanAttributesBuilder explainPlanAttributesBuilder,
                           List<List<Scan>> scansList) {
        StringBuilder buf = new StringBuilder(prefix);
        ScanRanges scanRanges = context.getScanRanges();
        Scan scan = context.getScan();

        if (scan.getConsistency() != Consistency.STRONG){
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
        buf.append("OVER ").append(tableRef.getTable().getPhysicalName().getString());
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
            explainPlanAttributesBuilder.setTableName(tableRef.getTable()
                .getPhysicalName().getString());
            if (!scanRanges.isPointLookup()) {
                explainPlanAttributesBuilder.setKeyRanges(appendKeyRanges());
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
                    firstKeyOnlyFilter = (FirstKeyOnlyFilter)filter;
                } else if (filter instanceof EmptyColumnOnlyFilter) {
                    emptyColumnOnlyFilter = (EmptyColumnOnlyFilter)filter;
                } else if (filter instanceof PageFilter) {
                    pageFilter = (PageFilter)filter;
                } else if (filter instanceof BooleanExpressionFilter) {
                    whereFilter = (BooleanExpressionFilter)filter;
                } else if (filter instanceof DistinctPrefixFilter) {
                    distinctFilter = (DistinctPrefixFilter)filter;
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
            byte[] expBytes = scan.getAttribute(BaseScannerRegionObserver.INDEX_FILTER_STR);
            if (expBytes == null) {
                // For older clients
                expBytes = scan.getAttribute(BaseScannerRegionObserver.LOCAL_INDEX_FILTER_STR);
            }
            if (expBytes != null) {
                whereFilterStr = Bytes.toString(expBytes);
            }
        }
        if (whereFilterStr != null) {
            String serverWhereFilter = "SERVER FILTER BY "
                + (firstKeyOnlyFilter == null ? "" : "FIRST KEY ONLY AND ")
                + (emptyColumnOnlyFilter == null ? "" : "EMPTY COLUMN ONLY AND ")
                + whereFilterStr;
            planSteps.add("    " + serverWhereFilter);
            if (explainPlanAttributesBuilder != null) {
                explainPlanAttributesBuilder.setServerWhereFilter(serverWhereFilter);
            }
        } else if (firstKeyOnlyFilter != null) {
            planSteps.add("    SERVER FILTER BY FIRST KEY ONLY");
            if (explainPlanAttributesBuilder != null) {
                explainPlanAttributesBuilder.setServerWhereFilter(
                    "SERVER FILTER BY FIRST KEY ONLY");
            }
        } else if (emptyColumnOnlyFilter != null) {
            planSteps.add("    SERVER FILTER BY EMPTY COLUMN ONLY");
            if (explainPlanAttributesBuilder != null) {
                explainPlanAttributesBuilder.setServerWhereFilter(
                        "SERVER FILTER BY EMPTY COLUMN ONLY");
            }
        }
        if (distinctFilter != null) {
            String serverDistinctFilter = "SERVER DISTINCT PREFIX FILTER OVER "
                + groupBy.getExpressions().toString();
            planSteps.add("    " + serverDistinctFilter);
            if (explainPlanAttributesBuilder != null) {
                explainPlanAttributesBuilder.setServerDistinctFilter(serverDistinctFilter);
            }
        }
        if (!orderBy.getOrderByExpressions().isEmpty() && groupBy.isEmpty()) { // with GROUP BY, sort happens client-side
            String orderByExpressions = "SERVER"
                + (limit == null ? "" : " TOP " + limit + " ROW" + (limit == 1 ? "" : "S"))
                + " SORTED BY " + orderBy.getOrderByExpressions().toString();
            planSteps.add("    " + orderByExpressions);
            if (explainPlanAttributesBuilder != null) {
                if (limit != null) {
                    explainPlanAttributesBuilder.setServerRowLimit(limit.longValue());
                }
                explainPlanAttributesBuilder.setServerSortedBy(
                    orderBy.getOrderByExpressions().toString());
            }
        } else {
            if (offset != null) {
                planSteps.add("    SERVER OFFSET " + offset);
            }
            Long limit = null;
            if (pageFilter != null) {
                limit = pageFilter.getPageSize();
            } else {
                byte[] limitBytes = scan.getAttribute(BaseScannerRegionObserver.INDEX_LIMIT);
                if (limitBytes != null) {
                    limit = Bytes.toLong(limitBytes);
                }
            }
            if (limit != null) {
                planSteps.add("    SERVER " + limit + " ROW LIMIT");
            }
            if (explainPlanAttributesBuilder != null) {
                explainPlanAttributesBuilder.setServerOffset(offset);
                if (pageFilter != null) {
                    explainPlanAttributesBuilder.setServerRowLimit(
                        pageFilter.getPageSize());
                }
            }
        }
        Integer groupByLimit = null;
        byte[] groupByLimitBytes = scan.getAttribute(BaseScannerRegionObserver.GROUP_BY_LIMIT);
        if (groupByLimitBytes != null) {
            groupByLimit = (Integer) PInteger.INSTANCE.toObject(groupByLimitBytes);
        }
        getRegionLocations(planSteps, explainPlanAttributesBuilder, scansList);
        groupBy.explain(planSteps, groupByLimit, explainPlanAttributesBuilder);
        if (scan.getAttribute(BaseScannerRegionObserver.SPECIFIC_ARRAY_INDEX) != null) {
            planSteps.add("    SERVER ARRAY ELEMENT PROJECTION");
            if (explainPlanAttributesBuilder != null) {
                explainPlanAttributesBuilder.setServerArrayElementProjection(true);
            }
        }
    }

    /**
     * Retrieve region locations and set the values in the explain plan output.
     *
     * @param planSteps list of plan steps to add explain plan output to.
     * @param explainPlanAttributesBuilder explain plan v2 attributes builder instance.
     * @param scansList list of the list of scans, to be used for parallel scans.
     */
    private void getRegionLocations(List<String> planSteps,
                                    ExplainPlanAttributesBuilder explainPlanAttributesBuilder,
                                    List<List<Scan>> scansList) {
        String regionLocationPlan = getRegionLocationsForExplainPlan(explainPlanAttributesBuilder,
                scansList);
        if (regionLocationPlan.length() > 0) {
            planSteps.add(regionLocationPlan);
        }
    }

    /**
     * Retrieve region locations from hbase client and set the values for the explain plan output.
     * If the list of region locations exceed max limit, print only list with the max limit and
     * print num of total list size.
     *
     * @param explainPlanAttributesBuilder explain plan v2 attributes builder instance.
     * @param scansList list of the list of scans, to be used for parallel scans.
     * @return region locations to be added to the explain plan output.
     */
    private String getRegionLocationsForExplainPlan(
            ExplainPlanAttributesBuilder explainPlanAttributesBuilder,
            List<List<Scan>> scansList) {
        try {
            StringBuilder buf = new StringBuilder().append(REGION_LOCATIONS);
            Set<RegionBoundary> regionBoundaries = new HashSet<>();
            List<HRegionLocation> regionLocations = new ArrayList<>();
            for (List<Scan> scans : scansList) {
                for (Scan eachScan : scans) {
                    getRegionsInRange(tableRef.getTable().getPhysicalName().getBytes(),
                            eachScan.getStartRow(),
                            eachScan.getStopRow(),
                            true,
                            false,
                            regionBoundaries,
                            regionLocations);
                }
            }
            int maxLimitRegionLoc = context.getConnection().getQueryServices().getConfiguration()
                    .getInt(QueryServices.MAX_REGION_LOCATIONS_SIZE_EXPLAIN_PLAN,
                            QueryServicesOptions.DEFAULT_MAX_REGION_LOCATIONS_SIZE_EXPLAIN_PLAN);
            if (explainPlanAttributesBuilder != null) {
                explainPlanAttributesBuilder.setRegionLocations(
                        Collections.unmodifiableList(regionLocations));
            }
            if (regionLocations.size() > maxLimitRegionLoc) {
                int originalSize = regionLocations.size();
                List<HRegionLocation> trimmedRegionLocations =
                        regionLocations.subList(0, maxLimitRegionLoc);
                buf.append(trimmedRegionLocations);
                buf.append("...total size = ");
                buf.append(originalSize);
            } else {
                buf.append(regionLocations);
            }
            buf.append(") ");
            return buf.toString();
        } catch (IOException | SQLException | UnsupportedOperationException e) {
            LOGGER.error("Explain table unable to add region locations.", e);
            return "";
        }
    }

    /**
     * Region boundary class with start and end key of the region.
     */
    private static class RegionBoundary {
        private final byte[] startKey;
        private final byte[] endKey;

        RegionBoundary(byte[] startKey, byte[] endKey) {
            this.startKey = startKey;
            this.endKey = endKey;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }
            RegionBoundary that = (RegionBoundary) o;
            return Bytes.compareTo(startKey, that.startKey) == 0
                    && Bytes.compareTo(endKey, that.endKey) == 0;
        }

        @Override
        public int hashCode() {
            return 0;
        }
    }

    private void appendPKColumnValue(StringBuilder buf, byte[] range, Boolean isNull, int slotIndex, boolean changeViewIndexId) {
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

    private Long getViewIndexValue(PDataType type, byte[] range) {
        boolean useLongViewIndex = MetaDataUtil.getViewIndexIdDataType().equals(type);
        Object s = type.toObject(range);
        return (useLongViewIndex ? (Long) s : (Short) s) + Short.MAX_VALUE + 2;
    }

    private static class RowKeyValueIterator implements Iterator<byte[]> {
        private final RowKeySchema schema;
        private ImmutableBytesWritable ptr = new ImmutableBytesWritable();
        private int position = 0;
        private final int maxOffset;
        private byte[] nextValue;
       
        public RowKeyValueIterator(RowKeySchema schema, byte[] rowKey) {
            this.schema = schema;
            this.maxOffset = schema.iterator(rowKey, ptr);
            iterate();
        }
        
        private void iterate() {
            if (schema.next(ptr, position++, maxOffset) == null) {
                nextValue = null;
            } else {
                nextValue = ptr.copyBytes();
            }
        }
        
        @Override
        public boolean hasNext() {
            return nextValue != null;
        }

        @Override
        public byte[] next() {
            if (nextValue == null) {
                throw new NoSuchElementException();
            }
            byte[] value = nextValue;
            iterate();
            return value;
        }

        @Override
        public void remove() {
            throw new UnsupportedOperationException();
        }
        
    }
    
    private void appendScanRow(StringBuilder buf, Bound bound) {
        ScanRanges scanRanges = context.getScanRanges();
        Iterator<byte[]> minMaxIterator = Collections.emptyIterator();
        boolean isLocalIndex = ScanUtil.isLocalIndex(context.getScan());
        boolean forceSkipScan = this.hint.hasHint(Hint.SKIP_SCAN);
        int nRanges = forceSkipScan ? scanRanges.getRanges().size() : scanRanges.getBoundSlotCount();
        for (int i = 0, minPos = 0; minPos < nRanges || minMaxIterator.hasNext(); i++) {
            List<KeyRange> ranges = minPos >= nRanges ? EVERYTHING :  scanRanges.getRanges().get(minPos++);
            KeyRange range = bound == Bound.LOWER ? ranges.get(0) : ranges.get(ranges.size()-1);
            byte[] b = range.getRange(bound);
            Boolean isNull = KeyRange.IS_NULL_RANGE == range ? Boolean.TRUE : KeyRange.IS_NOT_NULL_RANGE == range ? Boolean.FALSE : null;
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
        buf.setCharAt(buf.length()-1, ']');
        StringBuilder buf2 = new StringBuilder();
        appendScanRow(buf2, Bound.UPPER);
        if (!StringUtil.equals(buf1, buf2)) {
            buf.append( " - [");
            buf.append(buf2);
        }
        buf.setCharAt(buf.length()-1, ']');
        return buf.toString();
    }

}
