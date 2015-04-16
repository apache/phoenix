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
import org.apache.phoenix.compile.ColumnProjector;
import org.apache.phoenix.compile.QueryPlan;
import org.apache.phoenix.jdbc.PhoenixStatement;
import org.apache.phoenix.mapreduce.util.ConnectionUtil;
import org.apache.phoenix.util.ColumnInfo;

import com.google.common.base.Function;
import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;

public final class SqlQueryToColumnInfoFunction implements Function<String,List<ColumnInfo>> {
    
    private static final Log LOG = LogFactory.getLog(SqlQueryToColumnInfoFunction.class);
    private final Configuration configuration;

    public SqlQueryToColumnInfoFunction(final Configuration configuration) {
        this.configuration = configuration;
    }

    @Override
    public List<ColumnInfo> apply(String sqlQuery) {
        Preconditions.checkNotNull(sqlQuery);
        Connection connection = null;
        List<ColumnInfo> columnInfos = null;
        try {
            connection = ConnectionUtil.getInputConnection(this.configuration);
            final Statement  statement = connection.createStatement();
            final PhoenixStatement pstmt = statement.unwrap(PhoenixStatement.class);
            final QueryPlan queryPlan = pstmt.compileQuery(sqlQuery);
            final List<? extends ColumnProjector> projectedColumns = queryPlan.getProjector().getColumnProjectors();
            columnInfos = Lists.newArrayListWithCapacity(projectedColumns.size());
            columnInfos = Lists.transform(projectedColumns, new Function<ColumnProjector,ColumnInfo>() {
                @Override
                public ColumnInfo apply(final ColumnProjector columnProjector) {
                    return new ColumnInfo(columnProjector.getName(), columnProjector.getExpression().getDataType().getSqlType());
                }
                
            });
       } catch (SQLException e) {
            LOG.error(String.format(" Error [%s] parsing SELECT query [%s] ",e.getMessage(),sqlQuery));
            throw new RuntimeException(e);
        } finally {
            if(connection != null) {
                try {
                    connection.close();
                } catch(SQLException sqle) {
                    LOG.error("Error closing connection!!");
                    throw new RuntimeException(sqle);
                }
            }
        }
        return columnInfos;
    }

}
