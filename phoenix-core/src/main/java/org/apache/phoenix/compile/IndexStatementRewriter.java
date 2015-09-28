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
import java.util.Map;

import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.phoenix.parse.ColumnParseNode;
import org.apache.phoenix.parse.FamilyWildcardParseNode;
import org.apache.phoenix.parse.LiteralParseNode;
import org.apache.phoenix.parse.ParseNode;
import org.apache.phoenix.parse.ParseNodeFactory;
import org.apache.phoenix.parse.ParseNodeRewriter;
import org.apache.phoenix.parse.SelectStatement;
import org.apache.phoenix.parse.TableName;
import org.apache.phoenix.parse.TableWildcardParseNode;
import org.apache.phoenix.parse.WildcardParseNode;
import org.apache.phoenix.schema.ColumnRef;
import org.apache.phoenix.schema.PColumn;
import org.apache.phoenix.schema.TableRef;
import org.apache.phoenix.schema.types.PDataType;
import org.apache.phoenix.util.IndexUtil;

public class IndexStatementRewriter extends ParseNodeRewriter {
    private static final ParseNodeFactory FACTORY = new ParseNodeFactory();
    
    private Map<TableRef, TableRef> multiTableRewriteMap;
    private final ImmutableBytesWritable ptr = new ImmutableBytesWritable();
    
    public IndexStatementRewriter(ColumnResolver dataResolver, Map<TableRef, TableRef> multiTableRewriteMap) {
        super(dataResolver);
        this.multiTableRewriteMap = multiTableRewriteMap;
    }
    
    /**
     * Rewrite the parse node by translating all data table column references to
     * references to the corresponding index column.
     * @param node the parse node
     * @param dataResolver the column resolver
     * @return new parse node or the same one if nothing was rewritten.
     * @throws SQLException 
     */
    public static ParseNode translate(ParseNode node, ColumnResolver dataResolver) throws SQLException {
        return rewrite(node, new IndexStatementRewriter(dataResolver, null));
    }
    
    /**
     * Rewrite the select statement by translating all data table column references to
     * references to the corresponding index column.
     * @param statement the select statement
     * @param dataResolver the column resolver
     * @return new select statement or the same one if nothing was rewritten.
     * @throws SQLException 
     */
    public static SelectStatement translate(SelectStatement statement, ColumnResolver dataResolver) throws SQLException {
        return translate(statement, dataResolver, null);
    }
    
    /**
     * Rewrite the select statement containing multiple tables by translating all 
     * data table column references to references to the corresponding index column.
     * @param statement the select statement
     * @param dataResolver the column resolver
     * @param multiTableRewriteMap the data table to index table map
     * @return new select statement or the same one if nothing was rewritten.
     * @throws SQLException 
     */
    public static SelectStatement translate(SelectStatement statement, ColumnResolver dataResolver, Map<TableRef, TableRef> multiTableRewriteMap) throws SQLException {
        return rewrite(statement, new IndexStatementRewriter(dataResolver, multiTableRewriteMap));
    }

    @Override
    public ParseNode visit(ColumnParseNode node) throws SQLException {
        ColumnRef dataColRef = getResolver().resolveColumn(node.getSchemaName(), node.getTableName(), node.getName());
        PColumn dataCol = dataColRef.getColumn();
        TableRef dataTableRef = dataColRef.getTableRef();
        // Rewrite view constants as literals, as they won't be in the schema for
        // an index on the view. Our view may be READ_ONLY yet still have inherited
        // view constants if based on an UPDATABLE view
        if (dataCol.getViewConstant() != null) {
            byte[] viewConstant = dataCol.getViewConstant();
            // Ignore last byte, as it's only there so we can have a way to differentiate null
            // from the absence of a value.
            ptr.set(viewConstant, 0, viewConstant.length-1);
            Object literal = dataCol.getDataType().toObject(ptr);
            return new LiteralParseNode(literal, dataCol.getDataType());
        }
        TableName tName = getReplacedTableName(dataTableRef);
        if (multiTableRewriteMap != null && tName == null)
            return node;

        String indexColName = IndexUtil.getIndexColumnName(dataCol);
        ParseNode indexColNode = new ColumnParseNode(tName, '"' + indexColName + '"', node.getAlias());
        PDataType indexColType = IndexUtil.getIndexColumnDataType(dataCol);
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
            indexColNode = FACTORY.cast(indexColNode, dataColType, null, null);
        }
        return indexColNode;
    }

    @Override
    public ParseNode visit(WildcardParseNode node) throws SQLException {
        return multiTableRewriteMap != null ? node : WildcardParseNode.REWRITE_INSTANCE;
    }

    @Override
    public ParseNode visit(TableWildcardParseNode node) throws SQLException {
        TableName tName = getReplacedTableName(getResolver().resolveTable(node.getTableName().getSchemaName(), node.getTableName().getTableName()));
        return tName == null ? node : TableWildcardParseNode.create(tName, true);
    }

    @Override
    public ParseNode visit(FamilyWildcardParseNode node) throws SQLException {
        return multiTableRewriteMap != null ? node : new FamilyWildcardParseNode(node, true);
    }
    
    private TableName getReplacedTableName(TableRef origRef) {
        if (multiTableRewriteMap == null)
            return null;
        
        TableRef tableRef = multiTableRewriteMap.get(origRef);
        if (tableRef == null)
            return null;
        
        if (origRef.getTableAlias() != null)
            return TableName.create(null, origRef.getTableAlias());
            
        String schemaName = tableRef.getTable().getSchemaName().getString();
        return TableName.create(schemaName.length() == 0 ? null : schemaName, tableRef.getTable().getTableName().getString());
    }
    
}

