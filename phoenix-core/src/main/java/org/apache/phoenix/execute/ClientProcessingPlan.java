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

import org.apache.phoenix.compile.QueryPlan;
import org.apache.phoenix.compile.RowProjector;
import org.apache.phoenix.compile.StatementContext;
import org.apache.phoenix.compile.OrderByCompiler.OrderBy;
import org.apache.phoenix.expression.Expression;
import org.apache.phoenix.parse.FilterableStatement;
import org.apache.phoenix.schema.TableRef;

/**
 * Query plan that does where, order-by limit at client side, which is
 * for derived-table queries that cannot be flattened by SubselectRewriter.
 */
public abstract class ClientProcessingPlan extends DelegateQueryPlan {
    protected final StatementContext context;
    protected final FilterableStatement statement;
    protected final TableRef table;
    protected final RowProjector projector;
    protected final Integer limit;
    protected final Expression where;
    protected final OrderBy orderBy;

    public ClientProcessingPlan(StatementContext context, FilterableStatement statement, TableRef table, 
            RowProjector projector, Integer limit, Expression where, OrderBy orderBy, QueryPlan delegate) {
        super(delegate);
        this.context = context;
        this.statement = statement;
        this.table = table;
        this.projector = projector;
        this.limit = limit;
        this.where = where;
        this.orderBy = orderBy;
    }
    
    @Override
    public StatementContext getContext() {
        return context;
    }

    @Override
    public TableRef getTableRef() {
        return table;
    }

    @Override
    public RowProjector getProjector() {
        return projector;
    }

    @Override
    public Integer getLimit() {
        return limit;
    }

    @Override
    public OrderBy getOrderBy() {
        return orderBy;
    }

    @Override
    public FilterableStatement getStatement() {
        return statement;
    }
}
