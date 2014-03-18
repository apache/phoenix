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

import java.text.Format;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;

import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.filter.FilterList;
import org.apache.hadoop.hbase.filter.FirstKeyOnlyFilter;
import org.apache.hadoop.hbase.filter.PageFilter;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.phoenix.compile.GroupByCompiler.GroupBy;
import org.apache.phoenix.compile.ScanRanges;
import org.apache.phoenix.compile.StatementContext;
import org.apache.phoenix.coprocessor.BaseScannerRegionObserver;
import org.apache.phoenix.query.KeyRange;
import org.apache.phoenix.query.KeyRange.Bound;
import org.apache.phoenix.schema.PDataType;
import org.apache.phoenix.schema.RowKeySchema;
import org.apache.phoenix.schema.SortOrder;
import org.apache.phoenix.schema.TableRef;
import org.apache.phoenix.util.StringUtil;

import com.google.common.collect.Iterators;


public abstract class ExplainTable {
    private static final List<KeyRange> EVERYTHING = Collections.singletonList(KeyRange.EVERYTHING_RANGE);
    protected final StatementContext context;
    protected final TableRef tableRef;
    protected final GroupBy groupBy;
   
    public ExplainTable(StatementContext context, TableRef table) {
        this(context,table,GroupBy.EMPTY_GROUP_BY);
    }

    public ExplainTable(StatementContext context, TableRef table, GroupBy groupBy) {
        this.context = context;
        this.tableRef = table;
        this.groupBy = groupBy;
    }

    private boolean explainSkipScan(StringBuilder buf) {
        ScanRanges scanRanges = context.getScanRanges();
        if (scanRanges.isPointLookup()) {
            int keyCount = scanRanges.getPointLookupCount();
            buf.append("POINT LOOKUP ON " + keyCount + " KEY" + (keyCount > 1 ? "S " : " "));
        } else if (scanRanges.useSkipScanFilter()) {
            buf.append("SKIP SCAN ");
            int count = 1;
            boolean hasRanges = false;
            for (List<KeyRange> ranges : scanRanges.getRanges()) {
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
        return scanRanges.useSkipScanFilter();
    }
    
    protected void explain(String prefix, List<String> planSteps) {
        StringBuilder buf = new StringBuilder(prefix);
        ScanRanges scanRanges = context.getScanRanges();
        boolean hasSkipScanFilter = false;
        if (scanRanges.isEverything()) {
            buf.append("FULL SCAN ");
        } else {
            hasSkipScanFilter = explainSkipScan(buf);
        }
        buf.append("OVER " + tableRef.getTable().getPhysicalName().getString());
        if (!scanRanges.isPointLookup()) {
            appendKeyRanges(buf);
        }
        planSteps.add(buf.toString());
        
        Scan scan = context.getScan();
        Filter filter = scan.getFilter();
        PageFilter pageFilter = null;
        if (filter != null) {
            int offset = 0;
            boolean hasFirstKeyOnlyFilter = false;
            String filterDesc = "";
            if (hasSkipScanFilter) {
                if (filter instanceof FilterList) {
                    List<Filter> filterList = ((FilterList) filter).getFilters();
                    if (filterList.get(0) instanceof FirstKeyOnlyFilter) {
                        hasFirstKeyOnlyFilter = true;
                        offset = 1;
                    }
                    if (filterList.size() > offset+1) {
                        filterDesc = filterList.get(offset+1).toString();
                        pageFilter = getPageFilter(filterList);
                    }
                }
            } else if (filter instanceof FilterList) {
                List<Filter> filterList = ((FilterList) filter).getFilters();
                if (filterList.get(0) instanceof FirstKeyOnlyFilter) {
                    hasFirstKeyOnlyFilter = true;
                    offset = 1;
                }
                if (filterList.size() > offset) {
                    filterDesc = filterList.get(offset).toString();
                    pageFilter = getPageFilter(filterList);
                }
            } else {
                if (filter instanceof FirstKeyOnlyFilter) {
                    hasFirstKeyOnlyFilter = true;
                } else {
                    filterDesc = filter.toString();
                }
            }
            if (filterDesc.length() > 0) {
                planSteps.add("    SERVER FILTER BY " + (hasFirstKeyOnlyFilter ? "FIRST KEY ONLY AND " : "") + filterDesc);
            } else if (hasFirstKeyOnlyFilter) {
                planSteps.add("    SERVER FILTER BY FIRST KEY ONLY");
            }
            if (pageFilter != null) {
                planSteps.add("    SERVER " + pageFilter.getPageSize() + " ROW LIMIT");
            }
        }
        Integer groupByLimit = null;
        byte[] groupByLimitBytes = scan.getAttribute(BaseScannerRegionObserver.GROUP_BY_LIMIT);
        if (groupByLimitBytes != null) {
            groupByLimit = (Integer)PDataType.INTEGER.toObject(groupByLimitBytes);
        }
        groupBy.explain(planSteps, groupByLimit);
    }

    private PageFilter getPageFilter(List<Filter> filterList) {
        for (Filter filter : filterList) {
            if (filter instanceof PageFilter) return (PageFilter)filter;
        }
        return null;
    }

    private void appendPKColumnValue(StringBuilder buf, byte[] range, Boolean isNull, int slotIndex) {
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
            range = SortOrder.invert(range, 0, new byte[range.length], 0, range.length);
        }
        Format formatter = context.getConnection().getFormatter(type);
        buf.append(type.toStringLiteral(range, formatter));
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
        KeyRange minMaxRange = context.getMinMaxRange();
        Iterator<byte[]> minMaxIterator = Iterators.emptyIterator();
        if (minMaxRange != null) {
            RowKeySchema schema = tableRef.getTable().getRowKeySchema();
            if (!minMaxRange.isUnbound(bound)) {
                minMaxIterator = new RowKeyValueIterator(schema, minMaxRange.getRange(bound));
            }
        }
        int nRanges = scanRanges.getRanges().size();
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
                    minMaxIterator = Iterators.emptyIterator();
                }
            }
            appendPKColumnValue(buf, b, isNull, i);
            buf.append(',');
        }
    }
    
    private void appendKeyRanges(StringBuilder buf) {
        ScanRanges scanRanges = context.getScanRanges();
        KeyRange minMaxRange = context.getMinMaxRange();
        if (minMaxRange == null && (scanRanges == ScanRanges.EVERYTHING || scanRanges == ScanRanges.NOTHING)) {
            return;
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
    }
}
