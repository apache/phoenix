/*
 * Copyright 2010 The Apache Software Foundation
 *
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 *distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you maynot use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicablelaw or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.phoenix.pig.util;

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.util.Pair;
import org.apache.phoenix.compile.ColumnProjector;
import org.apache.phoenix.compile.QueryPlan;
import org.apache.phoenix.jdbc.PhoenixStatement;
import org.apache.phoenix.mapreduce.util.ConnectionUtil;

import com.google.common.base.Function;
import com.google.common.base.Joiner;
import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;

/**
 * 
 *  A function to parse the select query passed to LOAD into a Pair of <table Name, List<columns>
 *
 */
public class QuerySchemaParserFunction implements Function<String,Pair<String,String>> {

    private static final Log LOG = LogFactory.getLog(QuerySchemaParserFunction.class);
    private final Configuration configuration;
    
    public QuerySchemaParserFunction(Configuration configuration) {
        Preconditions.checkNotNull(configuration);
        this.configuration = configuration;
    }
    
    @Override
    public Pair<String, String> apply(final String selectStatement) {
        Preconditions.checkNotNull(selectStatement);
        Preconditions.checkArgument(!selectStatement.isEmpty(), "Select Query is empty!!");
        Connection connection = null;
        try {
            connection = ConnectionUtil.getInputConnection(this.configuration);
            final Statement  statement = connection.createStatement();
            final PhoenixStatement pstmt = statement.unwrap(PhoenixStatement.class);
            final QueryPlan queryPlan = pstmt.compileQuery(selectStatement);
            isValidStatement(queryPlan);
            final String tableName = queryPlan.getTableRef().getTable().getName().getString();
            final List<? extends ColumnProjector> projectedColumns = queryPlan.getProjector().getColumnProjectors();
            final List<String> columns = Lists.transform(projectedColumns,
                                                            new Function<ColumnProjector,String>() {
                                                                @Override
                                                                public String apply(ColumnProjector column) {
                                                                    return column.getName();
                                                                }
                                                            });
            final String columnsAsStr = Joiner.on(",").join(columns);
            return new Pair<String, String>(tableName, columnsAsStr);
        } catch (SQLException e) {
            LOG.error(String.format(" Error [%s] parsing SELECT query [%s] ",e.getMessage(),selectStatement));
            throw new RuntimeException(e);
        } finally {
            if(connection != null) {
                try {
                    connection.close();
                } catch(SQLException sqle) {
                    LOG.error(" Error closing connection ");
                    throw new RuntimeException(sqle);
                }
            }
        }
    }
    
    /**
     * The method validates the statement passed to the query plan. List of conditions are
     * <ol>
     *   <li>Is a SELECT statement</li>
     *   <li>doesn't contain ORDER BY expression</li>
     *   <li>doesn't contain LIMIT</li>
     *   <li>doesn't contain GROUP BY expression</li>
     *   <li>doesn't contain DISTINCT</li>
     *   <li>doesn't contain AGGREGATE functions</li>
     * </ol>  
     * @param queryPlan
     * @return
     */
    private boolean isValidStatement(final QueryPlan queryPlan) {
        if(queryPlan.getStatement().getOperation() != PhoenixStatement.Operation.QUERY) {
            throw new IllegalArgumentException("Query passed isn't a SELECT statement");
        }
        if(!queryPlan.getOrderBy().getOrderByExpressions().isEmpty() 
                || queryPlan.getLimit() != null 
                || (queryPlan.getGroupBy() != null && !queryPlan.getGroupBy().isEmpty()) 
                || queryPlan.getStatement().isDistinct()
                || queryPlan.getStatement().isAggregate()) {
            throw new IllegalArgumentException("SELECT statement shouldn't contain DISTINCT or ORDER BY or LIMIT or GROUP BY expressions");
        }
        return true;
    }

}
