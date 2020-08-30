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

import org.apache.phoenix.parse.EqualParseNode;
import org.apache.phoenix.parse.ParseNode;
import org.apache.phoenix.parse.ColumnParseNode;
import org.apache.phoenix.parse.InListParseNode;
import org.apache.phoenix.parse.LiteralParseNode;
import org.apache.phoenix.parse.ParseNodeFactory;
import org.apache.phoenix.parse.AndParseNode;
import org.apache.phoenix.parse.TableName;
import org.apache.phoenix.parse.RowValueConstructorParseNode;
import org.apache.phoenix.schema.AmbiguousColumnException;
import org.apache.phoenix.schema.ColumnNotFoundException;
import org.apache.phoenix.schema.PColumn;
import org.apache.phoenix.schema.PTable;

import java.sql.SQLException;
import java.util.*;

/**
 * This class helps extract views with condition to generate a more efficient where clause
 * for example given a table with a composite primary and a view with conditions that are part of parent table's primary key
 * <p>
 * -> CREATE TABLE MY_TABLE(K1 VARCHAR, K2 VARCHAR,K3 VARCHAR, V1 VARCHAR, V2 DATE, CONSTRAINT MY_TABLE_PK PRIMARY KEY(K1,K2,K3))
 * -> CREATE VIEW MY_VIEW(VV VARCHAR) AS SELECT * FROM MY_TABLE WHERE K2 = 'B'
 * <p>
 * We expect the following queries to run as point lookup queries using primary key rather than a range scan:
 * -> SELECT * FROM MY_VIEW WHERE (K1,K2) IN ('A','C')
 */
public class ViewWhereOptimizer {
    protected static final ParseNodeFactory NODE_FACTORY = new ParseNodeFactory();

    /**
     * Tries to return an optimized query condition by using the view condition
     *
     * @param context
     * @param where
     * @param viewWhere
     * @return an optimized query or null if query cannot be optimized
     * @throws SQLException
     */
    public static ParseNode optimizedViewWhereClause(StatementContext context, ParseNode where, ParseNode viewWhere) throws SQLException {
        PTable table = context.getCurrentTable().getTable();
        List<PColumn> pks = table.getPKColumns();
        // Only if it is a composite PK
        if (pks.size() > 1) {
            InListCondition inList = findPartialPrimaryKeyQueryConditions(context, where, pks);
            if (inList != null) {
                TreeMap<ViewPrimaryKeyColumn, ParseNode> viewPKColumns = findPKColumnsInViewCondition(context, viewWhere, pks);
                if (viewPKColumns != null) {
                    return convertConditionsToTupleClause(viewPKColumns, inList);
                }
            }

        }
        return null;
    }

    private static ParseNode convertConditionsToTupleClause(TreeMap<ViewPrimaryKeyColumn, ParseNode> viewPKColumns, InListCondition inList) {
        List<ParseNode> pkColumnNodes = new ArrayList<>();
        List<ParseNode> inListItems = inList.inList.getChildren();
        boolean isListOfLists = !inListItems.get(0).getChildren().isEmpty();
        if (isListOfLists) {
            pkColumnNodes.addAll(inListItems.get(0).getChildren());
        } else {
            pkColumnNodes.add(inListItems.get(0));
        }
        for (ViewPrimaryKeyColumn col : viewPKColumns.keySet()) {
            // If a view condition column is being set explicitly by the query conditions,
            // we should always do an `AND` with the entire view condition and no optimization is possible.
            if (inList.pkColumns.contains(col.pColumn)) {
                return null;
            }
            pkColumnNodes.add(col.order, col.conditionColumn);
        }

        RowValueConstructorParseNode columnNamesInTheInClause = new RowValueConstructorParseNode(pkColumnNodes);
        List<ParseNode> tuplesWithValues = new ArrayList<>();
        tuplesWithValues.add(columnNamesInTheInClause);

        // i = 1 is the values start index
        for (int i = 1; i < inListItems.size(); i++) {
            List<ParseNode> tupleValues = new ArrayList<>();
            if (isListOfLists) {
                tupleValues.addAll(inListItems.get(i).getChildren());
            } else {
                tupleValues.add(inListItems.get(i));
            }
            for (ViewPrimaryKeyColumn col : viewPKColumns.keySet()) {
                tupleValues.add(col.order, viewPKColumns.get(col));
            }
            tuplesWithValues.add(new RowValueConstructorParseNode(tupleValues));
        }
        return NODE_FACTORY.inList(tuplesWithValues, false);
    }

    /**
     * The order of view condition columns matters (sorted by order in the table's primary key)
     * because we use the order to inject the view condition inside a tuple style in clause and use the same order for
     * injecting each column's literal value.
     *
     * @param context
     * @param viewWhere
     * @param pks
     * @return
     */
    private static TreeMap<ViewPrimaryKeyColumn, ParseNode> findPKColumnsInViewCondition(StatementContext context, ParseNode viewWhere, List<PColumn> pks) {
        if (EqualParseNode.class.isInstance(viewWhere)) {
            EqualParseNode viewEqual = ((EqualParseNode) viewWhere);
            PColumn pkCol = findColumn(context, viewEqual.getLHS().toString());
            if (pks.contains(pkCol)) {
                PTable table = context.getCurrentTable().getTable();
                TreeMap<ViewPrimaryKeyColumn, ParseNode> result = new TreeMap<>();
                ColumnParseNode viewColParseNode = new ColumnParseNode(
                        TableName.create(table.getSchemaName().getString(), table.getTableName().getString()),
                        pkCol.getName().getString());
                result.put(new ViewPrimaryKeyColumn(viewColParseNode, pkCol, pks.indexOf(pkCol)), (LiteralParseNode) viewEqual.getRHS());
                return result;
            }
        }
        if (AndParseNode.class.isInstance(viewWhere)) {
            List<ParseNode> children = viewWhere.getChildren();
            TreeMap<ViewPrimaryKeyColumn, ParseNode> result = new TreeMap<>();
            for (ParseNode child : children) {
                Map<ViewPrimaryKeyColumn, ParseNode> childrenMap = findPKColumnsInViewCondition(context, child, pks);
                if (childrenMap == null) return null;
                result.putAll(childrenMap);
            }
            return result;
        }
        return null;
    }

    private static PColumn findColumn(StatementContext context, String columnName) {
        PTable table = context.getCurrentTable().getTable();
        try {
            return table.getColumnForColumnName(columnName);
        } catch (ColumnNotFoundException cnf) {
        } catch (AmbiguousColumnException ace) {
        }
        return null;
    }

    private static InListCondition findPartialPrimaryKeyQueryConditions(StatementContext context, ParseNode where, List<PColumn> pks) {
        if (InListParseNode.class.isInstance(where)) {
            InListParseNode inList = (InListParseNode) where;
            List<ParseNode> inListItems = inList.getChildren();
            List<ParseNode> inColumnNames = inListItems.get(0).getChildren();
            if (inColumnNames.size() < pks.size()) {
                List<PColumn> pkColumns = new ArrayList<>();
                // only if not all PK columns are specified
                int valueIndex = 1; // first item in the in inListItems is the columnNames and after that the values
                for (ParseNode col : inColumnNames) {
                    ColumnParseNode columnParsedNode = (ColumnParseNode) col;
                    PColumn pkColumn = findColumn(context, columnParsedNode.getFullName());
                    if (pkColumn == null || !pks.contains(pkColumn)) {
                        // if column does not exists or is not part of primary key
                        return null;
                    }
                    pkColumns.add(pkColumn);
                }
                return new InListCondition(inList, pkColumns);
            }
        }
        return null;
    }

    private static class InListCondition {
        InListParseNode inList;
        List<PColumn> pkColumns;

        public InListCondition(InListParseNode inList, List<PColumn> pkColumns) {
            this.inList = inList;
            this.pkColumns = pkColumns;
        }
    }

    private static class ViewPrimaryKeyColumn implements Comparable<ViewPrimaryKeyColumn> {
        ParseNode conditionColumn;
        PColumn pColumn;
        int order;

        public ViewPrimaryKeyColumn(ParseNode conditionColumn, PColumn pColumn, int order) {
            this.conditionColumn = conditionColumn;
            this.pColumn = pColumn;
            this.order = order;
        }

        @Override
        public int compareTo(ViewPrimaryKeyColumn o) {
            return this.order - o.order;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            ViewPrimaryKeyColumn column = (ViewPrimaryKeyColumn) o;
            return pColumn.equals(column.pColumn);
        }

        @Override
        public int hashCode() {
            return Objects.hash(pColumn);
        }
    }
}
