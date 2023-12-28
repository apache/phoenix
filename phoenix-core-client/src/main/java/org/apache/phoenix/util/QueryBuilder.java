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
package org.apache.phoenix.util;

import org.apache.phoenix.thirdparty.com.google.common.base.Preconditions;
import org.apache.phoenix.thirdparty.com.google.common.collect.Lists;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.phoenix.parse.HintNode;

import java.util.Collections;
import java.util.List;

import static org.apache.phoenix.util.SchemaUtil.getEscapedFullColumnName;

public class QueryBuilder {

    private String fullTableName;
    // regular columns that are in the select clause
    private List<String> selectColumns = Collections.emptyList();

    // columns that are required for expressions in the select clause
    private List<String> selectExpressionColumns  = Collections.emptyList();
    // expression string in the select clause (for eg COL1 || COL2)
    private String selectExpression;
    private String whereClause;
    private String orderByClause;
    private String groupByClause;
    private String havingClause;
    private HintNode.Hint hint;
    private boolean escapeCols;
    private boolean distinct;
    private int limit;

    public String getFullTableName() {
        return fullTableName;
    }

    /**
     * @return column names required to evaluate this select statement
     */
    public List<String> getRequiredColumns() {
        List<String> allColumns = Lists.newArrayList(selectColumns);
        if (!CollectionUtils.isEmpty(selectExpressionColumns)) {
            allColumns.addAll(selectExpressionColumns);
        }
        return allColumns;
    }

    public String getWhereClause() {
        return whereClause;
    }

    public HintNode.Hint getHint() {
        return hint;
    }

    public String getOrderByClause() {
        return orderByClause;
    }

    public String getGroupByClause() {
        return groupByClause;
    }

    public QueryBuilder setOrderByClause(String orderByClause) {
        this.orderByClause = orderByClause;
        return this;
    }

    public QueryBuilder setFullTableName(String fullTableName) {
        this.fullTableName = fullTableName;
        return this;
    }

    public QueryBuilder setSelectColumns(List<String> columns) {
        this.selectColumns = columns;
        return this;
    }

    public QueryBuilder setWhereClause(String whereClause) {
        this.whereClause = whereClause;
        return this;
    }

    public QueryBuilder setHint(HintNode.Hint hint) {
        this.hint = hint;
        return this;
    }

    public QueryBuilder setEscapeCols(boolean escapeCols) {
        this.escapeCols = escapeCols;
        return this;
    }

    public QueryBuilder setGroupByClause(String groupByClause) {
        this.groupByClause = groupByClause;
        return this;
    }

    public QueryBuilder setHavingClause(String havingClause) {
        this.havingClause = havingClause;
        return this;
    }

    public List<String> getSelectExpressionColumns() {
        return selectExpressionColumns;
    }

    public QueryBuilder setSelectExpressionColumns(List<String> selectExpressionColumns) {
        this.selectExpressionColumns = selectExpressionColumns;
        return this;
    }

    public String getSelectExpression() {
        return selectExpression;
    }

    public QueryBuilder setSelectExpression(String selectExpression) {
        this.selectExpression = selectExpression;
        return this;
    }

    public QueryBuilder setDistinct(boolean distinct) {
        this.distinct = distinct;
        return this;
    }

    public QueryBuilder setLimit(int limit) {
        this.limit = limit;
        return this;
    }

    public String build() {
        Preconditions.checkNotNull(fullTableName, "Table name cannot be null");
        if (CollectionUtils.isEmpty(selectColumns) && StringUtils.isBlank(selectExpression)) {
            throw new IllegalArgumentException("At least one column or select expression must be provided");
        }
        StringBuilder query = new StringBuilder();
        query.append("SELECT ");

        if (distinct) {
            query.append(" DISTINCT ");
        }

        if (hint != null) {
            final HintNode node = new HintNode(hint.name());
            String hintStr = node.toString();
            query.append(hintStr);
        }

        StringBuilder selectClauseBuilder = new StringBuilder();
        if (StringUtils.isNotBlank(selectExpression)) {
            if (selectClauseBuilder.length()!=0) {
                selectClauseBuilder.append(" , ");
            }
            selectClauseBuilder.append(selectExpression);
        }

        boolean first = true;
        for (String col : selectColumns) {
            if (StringUtils.isNotBlank(col)) {
                if ((first && selectClauseBuilder.length()!=0) || !first) {
                    selectClauseBuilder.append(" , ");
                }
                String fullColumnName = col;
                if (escapeCols) {
                    fullColumnName = getEscapedFullColumnName(col);
                }
                selectClauseBuilder.append(fullColumnName);
                first = false;
            }
        }

        query.append(selectClauseBuilder);
        query.append(" FROM ");
        query.append(fullTableName);
        if (StringUtils.isNotBlank(whereClause)) {
            query.append(" WHERE (").append(whereClause).append(")");
        }
        if (StringUtils.isNotBlank(groupByClause)) {
            query.append(" GROUP BY ").append(groupByClause);
        }
        if (StringUtils.isNotBlank(havingClause)) {
            query.append(" HAVING ").append(havingClause);
        }
        if (StringUtils.isNotBlank(orderByClause)) {
            query.append(" ORDER BY ").append(orderByClause);
        }
        if (limit > 0) {
            query.append(" LIMIT ").append(limit);
        }
        return query.toString();
    }

}
