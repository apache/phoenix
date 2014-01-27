/*
 * Copyright 2010 The Apache Software Foundation
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
package org.apache.phoenix.compile;

import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.util.Collections;
import java.util.List;

import org.apache.hadoop.hbase.client.Scan;

import org.apache.phoenix.compile.GroupByCompiler.GroupBy;
import org.apache.phoenix.compile.OrderByCompiler.OrderBy;
import org.apache.phoenix.execute.AggregatePlan;
import org.apache.phoenix.execute.DegenerateQueryPlan;
import org.apache.phoenix.execute.ScanPlan;
import org.apache.phoenix.expression.Expression;
import org.apache.phoenix.iterate.ParallelIterators.ParallelIteratorFactory;
import org.apache.phoenix.jdbc.PhoenixConnection;
import org.apache.phoenix.jdbc.PhoenixDatabaseMetaData;
import org.apache.phoenix.jdbc.PhoenixStatement;
import org.apache.phoenix.parse.SelectStatement;
import org.apache.phoenix.query.QueryConstants;
import org.apache.phoenix.schema.AmbiguousColumnException;
import org.apache.phoenix.schema.ColumnNotFoundException;
import org.apache.phoenix.schema.PDatum;
import org.apache.phoenix.schema.PIndexState;
import org.apache.phoenix.schema.PTableType;
import org.apache.phoenix.schema.TableRef;



/**
 * 
 * Class used to build an executable query plan
 *
 * @author jtaylor
 * @since 0.1
 */
public class QueryCompiler {
    /* 
     * Not using Scan.setLoadColumnFamiliesOnDemand(true) because we don't 
     * want to introduce a dependency on 0.94.5 (where this feature was
     * introduced). This will do the same thing. Once we do have a 
     * dependency on 0.94.5 or above, switch this around.
     */
    private static final String LOAD_COLUMN_FAMILIES_ON_DEMAND_ATTR = "_ondemand_";
    private final PhoenixStatement statement;
    private final Scan scan;
    private final List<? extends PDatum> targetColumns;
    private final ParallelIteratorFactory parallelIteratorFactory;

    public QueryCompiler(PhoenixStatement statement) throws SQLException {
        this(statement, Collections.<PDatum>emptyList(), null);
    }
    
    public QueryCompiler(PhoenixStatement statement, List<? extends PDatum> targetColumns, ParallelIteratorFactory parallelIteratorFactory) throws SQLException {
        this.statement = statement;
        this.scan = new Scan();
        this.targetColumns = targetColumns;
        this.parallelIteratorFactory = parallelIteratorFactory;
        if (statement.getConnection().getQueryServices().getLowestClusterHBaseVersion() >= PhoenixDatabaseMetaData.ESSENTIAL_FAMILY_VERSION_THRESHOLD) {
            this.scan.setAttribute(LOAD_COLUMN_FAMILIES_ON_DEMAND_ATTR, QueryConstants.TRUE);
        }
    }

    /**
     * Builds an executable query plan from a parsed SQL statement
     * @param select parsed SQL statement
     * @return executable query plan
     * @throws SQLException if mismatched types are found, bind value do not match binds,
     * or invalid function arguments are encountered.
     * @throws SQLFeatureNotSupportedException if an unsupported construct is encountered
     * @throws TableNotFoundException if table name not found in schema
     * @throws ColumnNotFoundException if column name could not be resolved
     * @throws AmbiguousColumnException if an unaliased column name is ambiguous across multiple tables
     */
    public QueryPlan compile(SelectStatement select) throws SQLException {
        PhoenixConnection connection = statement.getConnection();
        List<Object> binds = statement.getParameters();
        ColumnResolver resolver = FromCompiler.getResolver(select, connection);
        select = StatementNormalizer.normalize(select, resolver);
        TableRef tableRef = resolver.getTables().get(0);
        StatementContext context = new StatementContext(select, connection, resolver, binds, scan);
        // Short circuit out if we're compiling an index query and the index isn't active.
        // We must do this after the ColumnResolver resolves the table, as we may be updating the local
        // cache of the index table and it may now be inactive.
        if (tableRef.getTable().getType() == PTableType.INDEX && tableRef.getTable().getIndexState() != PIndexState.ACTIVE) {
            return new DegenerateQueryPlan(context, select, tableRef);
        }
        Integer limit = LimitCompiler.compile(context, select);

        GroupBy groupBy = GroupByCompiler.compile(context, select);
        // Optimize the HAVING clause by finding any group by expressions that can be moved
        // to the WHERE clause
        select = HavingCompiler.rewrite(context, select, groupBy);
        Expression having = HavingCompiler.compile(context, select, groupBy);
        // Don't pass groupBy when building where clause expression, because we do not want to wrap these
        // expressions as group by key expressions since they're pre, not post filtered.
        WhereCompiler.compile(context, select);
        OrderBy orderBy = OrderByCompiler.compile(context, select, groupBy, limit); 
        RowProjector projector = ProjectionCompiler.compile(context, select, groupBy, targetColumns);
        
        // Final step is to build the query plan
        int maxRows = statement.getMaxRows();
        if (maxRows > 0) {
            if (limit != null) {
                limit = Math.min(limit, maxRows);
            } else {
                limit = maxRows;
            }
        }
        if (select.isAggregate() || select.isDistinct()) {
            return new AggregatePlan(context, select, tableRef, projector, limit, orderBy, parallelIteratorFactory, groupBy, having);
        } else {
            return new ScanPlan(context, select, tableRef, projector, limit, orderBy, parallelIteratorFactory);
        }
    }
}
