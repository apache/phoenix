/*
 * Copyright 2014 The Apache Software Foundation
 *
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

import java.sql.ParameterMetaData;
import java.sql.SQLException;

import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;

import org.apache.phoenix.compile.ExplainPlan;
import org.apache.phoenix.compile.GroupByCompiler.GroupBy;
import org.apache.phoenix.compile.OrderByCompiler.OrderBy;
import org.apache.phoenix.compile.QueryPlan;
import org.apache.phoenix.compile.RowProjector;
import org.apache.phoenix.compile.ScanRanges;
import org.apache.phoenix.compile.StatementContext;
import org.apache.phoenix.iterate.ParallelIterators.ParallelIteratorFactory;
import org.apache.phoenix.jdbc.PhoenixConnection;
import org.apache.phoenix.parse.FilterableStatement;
import org.apache.phoenix.query.ConnectionQueryServices;
import org.apache.phoenix.query.DegenerateScanner;
import org.apache.phoenix.query.QueryConstants;
import org.apache.phoenix.query.Scanner;
import org.apache.phoenix.schema.PTable;
import org.apache.phoenix.schema.PTableType;
import org.apache.phoenix.schema.TableRef;
import org.apache.phoenix.util.ScanUtil;
import org.apache.phoenix.util.SchemaUtil;



/**
 *
 * Query plan that has no child plans
 *
 * 
 * @since 0.1
 */
public abstract class BasicQueryPlan implements QueryPlan {
    protected final TableRef tableRef;
    protected final StatementContext context;
    protected final FilterableStatement statement;
    protected final RowProjector projection;
    protected final ParameterMetaData paramMetaData;
    protected final Integer limit;
    protected final OrderBy orderBy;
    protected final GroupBy groupBy;
    protected final ParallelIteratorFactory parallelIteratorFactory;

    private Scanner scanner;

    protected BasicQueryPlan(
            StatementContext context, FilterableStatement statement, TableRef table,
            RowProjector projection, ParameterMetaData paramMetaData, Integer limit, OrderBy orderBy,
            GroupBy groupBy, ParallelIteratorFactory parallelIteratorFactory) {
        this.context = context;
        this.statement = statement;
        this.tableRef = table;
        this.projection = projection;
        this.paramMetaData = paramMetaData;
        this.limit = limit;
        this.orderBy = orderBy;
        this.groupBy = groupBy;
        this.parallelIteratorFactory = parallelIteratorFactory;
    }

    @Override
    public GroupBy getGroupBy() {
        return groupBy;
    }

    
    @Override
    public OrderBy getOrderBy() {
        return orderBy;
    }

    @Override
    public TableRef getTableRef() {
        return tableRef;
    }

    @Override
    public Integer getLimit() {
        return limit;
    }

    @Override
    public RowProjector getProjector() {
        return projection;
    }

    private ConnectionQueryServices getConnectionQueryServices(ConnectionQueryServices services) {
        byte[] tenantId = context.getConnection().getTenantId();
        // Get child services associated with tenantId of query.
        ConnectionQueryServices childServices = tenantId == null ? services : services.getChildQueryServices(new ImmutableBytesWritable(tenantId));
        return childServices;
    }

    protected void projectEmptyKeyValue() {
        Scan scan = context.getScan();
        PTable table = tableRef.getTable();
        if (!projection.isProjectEmptyKeyValue() && table.getType() != PTableType.VIEW) {
                scan.addColumn(SchemaUtil.getEmptyColumnFamily(table.getColumnFamilies()), QueryConstants.EMPTY_COLUMN_BYTES);
        }
    }
//    /**
//     * Sets up an id used to do round robin queue processing on the server
//     * @param scan
//     */
//    private void setProducer(Scan scan) {
//        byte[] producer = Bytes.toBytes(UUID.randomUUID().toString());
//        scan.setAttribute(HBaseServer.CALL_QUEUE_PRODUCER_ATTRIB_NAME, producer);
//    }

    @Override
    public final Scanner getScanner() throws SQLException {
        if (scanner != null) {
            return scanner;
        }
        Scan scan = context.getScan();
        // Set producer on scan so HBase server does round robin processing
        //setProducer(scan);
        // Set the time range on the scan so we don't get back rows newer than when the statement was compiled
        // The time stamp comes from the server at compile time when the meta data
        // is resolved.
        // TODO: include time range in explain plan?
        PhoenixConnection connection = context.getConnection();
        Long scn = connection.getSCN();
        ScanUtil.setTimeRange(scan, scn == null ? context.getCurrentTime() : scn);
        ScanUtil.setTenantId(scan, connection.getTenantId());
        scanner = newScanner();
        return scanner;
    }

    abstract protected Scanner newScanner(ConnectionQueryServices services) throws SQLException;

    private Scanner newScanner() throws SQLException {
        ConnectionQueryServices services = getConnectionQueryServices(context.getConnection().getQueryServices());
        if (context.getScanRanges() == ScanRanges.NOTHING) { // is degenerate
            scanner = new DegenerateScanner(tableRef, getProjector());
        } else {
            scanner = newScanner(services);
        }
        return scanner;
    }

    @Override
    public ParameterMetaData getParameterMetaData() {
        return paramMetaData;
    }

    @Override
    public FilterableStatement getStatement() {
        return statement;
    }

    @Override
    public StatementContext getContext() {
        return context;
    }

    @Override
    public ExplainPlan getExplainPlan() throws SQLException {
        if (scanner == null) {
            scanner = newScanner();
        }
        return scanner.getExplainPlan();
    }
}
