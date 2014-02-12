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

import java.sql.SQLException;

import org.apache.phoenix.parse.ColumnParseNode;
import org.apache.phoenix.parse.FamilyWildcardParseNode;
import org.apache.phoenix.parse.ParseNode;
import org.apache.phoenix.parse.ParseNodeFactory;
import org.apache.phoenix.parse.ParseNodeRewriter;
import org.apache.phoenix.parse.SelectStatement;
import org.apache.phoenix.parse.WildcardParseNode;
import org.apache.phoenix.schema.ColumnRef;
import org.apache.phoenix.schema.PDataType;
import org.apache.phoenix.util.IndexUtil;

public class IndexStatementRewriter extends ParseNodeRewriter {
    private static final ParseNodeFactory FACTORY = new ParseNodeFactory();
    
    public IndexStatementRewriter(ColumnResolver dataResolver) {
        super(dataResolver);
    }
    
    /**
     * Rewrite the select statement by translating all data table column references to
     * references to the corresponding index column.
     * @param statement the select statement
     * @return new select statement or the same one if nothing was rewritten.
     * @throws SQLException 
     */
    public static SelectStatement translate(SelectStatement statement, ColumnResolver dataResolver) throws SQLException {
        return rewrite(statement, new IndexStatementRewriter(dataResolver));
    }

    @Override
    public ParseNode visit(ColumnParseNode node) throws SQLException {
        ColumnRef dataColRef = getResolver().resolveColumn(node.getSchemaName(), node.getTableName(), node.getName());
        String indexColName = IndexUtil.getIndexColumnName(dataColRef.getColumn());
        // Same alias as before, but use the index column name instead of the data column name
        ParseNode indexColNode = new ColumnParseNode(null, indexColName, node.toString());
        PDataType indexColType = IndexUtil.getIndexColumnDataType(dataColRef.getColumn());
        PDataType dataColType = dataColRef.getColumn().getDataType();

        // Coerce index column reference back to same type as data column so that
        // expression behave exactly the same. No need to invert, as this will be done
        // automatically as needed. If node is used at the top level, do not convert, as
        // otherwise the wrapper gets in the way in the group by clause. For example,
        // an INTEGER column in a GROUP BY gets doubly wrapped like this:
        //     CAST CAST int_col AS INTEGER AS DECIMAL
        // This is unnecessary and problematic in the case of a null value.
        // TODO: test case for this
        if (!isTopLevel() && indexColType != dataColType) {
            indexColNode = FACTORY.cast(indexColNode, dataColType);
        }
        return indexColNode;
    }

    @Override
    public ParseNode visit(WildcardParseNode node) throws SQLException {
        return WildcardParseNode.REWRITE_INSTANCE;
    }

    @Override
    public ParseNode visit(FamilyWildcardParseNode node) throws SQLException {
        return new FamilyWildcardParseNode(node, true);
    }
    
}
