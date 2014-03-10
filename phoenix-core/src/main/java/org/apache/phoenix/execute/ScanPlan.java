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
package org.apache.phoenix.execute;


import java.sql.SQLException;
import java.util.List;

import org.apache.phoenix.compile.GroupByCompiler.GroupBy;
import org.apache.phoenix.compile.OrderByCompiler.OrderBy;
import org.apache.phoenix.compile.RowProjector;
import org.apache.phoenix.compile.StatementContext;
import org.apache.phoenix.coprocessor.BaseScannerRegionObserver;
import org.apache.phoenix.coprocessor.ScanRegionObserver;
import org.apache.phoenix.iterate.ConcatResultIterator;
import org.apache.phoenix.iterate.LimitingResultIterator;
import org.apache.phoenix.iterate.MergeSortRowKeyResultIterator;
import org.apache.phoenix.iterate.MergeSortTopNResultIterator;
import org.apache.phoenix.iterate.ParallelIterators;
import org.apache.phoenix.iterate.ParallelIterators.ParallelIteratorFactory;
import org.apache.phoenix.iterate.ResultIterator;
import org.apache.phoenix.iterate.SequenceResultIterator;
import org.apache.phoenix.iterate.SpoolingResultIterator;
import org.apache.phoenix.parse.FilterableStatement;
import org.apache.phoenix.query.KeyRange;
import org.apache.phoenix.query.QueryConstants;
import org.apache.phoenix.query.QueryServices;
import org.apache.phoenix.query.QueryServicesOptions;
import org.apache.phoenix.schema.PTable;
import org.apache.phoenix.schema.SaltingUtil;
import org.apache.phoenix.schema.TableRef;
import org.apache.phoenix.util.ScanUtil;



/**
 * 
 * Query plan for a basic table scan
 *
 * 
 * @since 0.1
 */
public class ScanPlan extends BasicQueryPlan {
    private List<KeyRange> splits;
    
    public ScanPlan(StatementContext context, FilterableStatement statement, TableRef table, RowProjector projector, Integer limit, OrderBy orderBy, ParallelIteratorFactory parallelIteratorFactory) {
        super(context, statement, table, projector, context.getBindManager().getParameterMetaData(), limit, orderBy, null, 
                parallelIteratorFactory != null ? parallelIteratorFactory :
                    new SpoolingResultIterator.SpoolingResultIteratorFactory(context.getConnection().getQueryServices()));
        if (!orderBy.getOrderByExpressions().isEmpty()) { // TopN
            int thresholdBytes = context.getConnection().getQueryServices().getProps().getInt(
                    QueryServices.SPOOL_THRESHOLD_BYTES_ATTRIB, QueryServicesOptions.DEFAULT_SPOOL_THRESHOLD_BYTES);
            ScanRegionObserver.serializeIntoScan(context.getScan(), thresholdBytes, limit == null ? -1 : limit, orderBy.getOrderByExpressions(), projector.getEstimatedRowByteSize());
        }
    }
    
    @Override
    public List<KeyRange> getSplits() {
        return splits;
    }
    
    @Override
    protected ResultIterator newIterator() throws SQLException {
        // Set any scan attributes before creating the scanner, as it will be too late afterwards
        context.getScan().setAttribute(BaseScannerRegionObserver.NON_AGGREGATE_QUERY, QueryConstants.TRUE);
        if (OrderBy.REV_ROW_KEY_ORDER_BY.equals(orderBy)) {
            ScanUtil.setReversed(context.getScan());
        }
        ResultIterator scanner;
        TableRef tableRef = this.getTableRef();
        PTable table = tableRef.getTable();
        boolean isSalted = table.getBucketNum() != null;
        /* If no limit or topN, use parallel iterator so that we get results faster. Otherwise, if
         * limit is provided, run query serially.
         */
        boolean isOrdered = !orderBy.getOrderByExpressions().isEmpty();
        ParallelIterators iterators = new ParallelIterators(context, tableRef, statement, projection, GroupBy.EMPTY_GROUP_BY, isOrdered ? null : limit, parallelIteratorFactory);
        splits = iterators.getSplits();
        if (isOrdered) {
            scanner = new MergeSortTopNResultIterator(iterators, limit, orderBy.getOrderByExpressions());
        } else {
            if (isSalted && 
                    (context.getConnection().getQueryServices().getProps().getBoolean(
                            QueryServices.ROW_KEY_ORDER_SALTED_TABLE_ATTRIB, 
                            QueryServicesOptions.DEFAULT_ROW_KEY_ORDER_SALTED_TABLE) ||
                     orderBy == OrderBy.FWD_ROW_KEY_ORDER_BY ||
                     orderBy == OrderBy.REV_ROW_KEY_ORDER_BY)) { // ORDER BY was optimized out b/c query is in row key order
                scanner = new MergeSortRowKeyResultIterator(iterators, SaltingUtil.NUM_SALTING_BYTES, orderBy == OrderBy.REV_ROW_KEY_ORDER_BY);
            } else {
                scanner = new ConcatResultIterator(iterators);
            }
            if (limit != null) {
                scanner = new LimitingResultIterator(scanner, limit);
            }
        }

        if (context.getSequenceManager().getSequenceCount() > 0) {
            scanner = new SequenceResultIterator(scanner, context.getSequenceManager());
        }
        return scanner;
    }
}