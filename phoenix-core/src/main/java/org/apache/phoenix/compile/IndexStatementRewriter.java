package org.apache.phoenix.compile;

import java.sql.SQLException;
import java.util.Map;

import org.apache.phoenix.parse.ColumnParseNode;
import org.apache.phoenix.parse.FamilyWildcardParseNode;
import org.apache.phoenix.parse.ParseNode;
import org.apache.phoenix.parse.ParseNodeFactory;
import org.apache.phoenix.parse.ParseNodeRewriter;
import org.apache.phoenix.parse.SelectStatement;
import org.apache.phoenix.parse.TableName;
import org.apache.phoenix.parse.WildcardParseNode;
import org.apache.phoenix.schema.ColumnRef;
import org.apache.phoenix.schema.PDataType;
import org.apache.phoenix.schema.TableRef;
import org.apache.phoenix.util.IndexUtil;

public class IndexStatementRewriter extends ParseNodeRewriter {
    private static final ParseNodeFactory FACTORY = new ParseNodeFactory();
    
    private Map<TableRef, TableRef> multiTableRewriteMap;
    
    public IndexStatementRewriter(ColumnResolver dataResolver, Map<TableRef, TableRef> multiTableRewriteMap) {
        super(dataResolver);
        this.multiTableRewriteMap = multiTableRewriteMap;
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
        TableName tName = null;
        if (multiTableRewriteMap != null) {
            TableRef origRef = dataColRef.getTableRef();
            TableRef tableRef = multiTableRewriteMap.get(origRef);
            if (tableRef == null)
                return node;
            
            if (origRef.getTableAlias() != null) {
                tName = TableName.create(null, origRef.getTableAlias());
            } else {
                String schemaName = tableRef.getTable().getSchemaName().getString();
                tName = TableName.create(schemaName.length() == 0 ? null : schemaName, tableRef.getTable().getTableName().getString());
            }
        }
        String indexColName = IndexUtil.getIndexColumnName(dataColRef.getColumn());
        // Same alias as before, but use the index column name instead of the data column name
        ParseNode indexColNode = new ColumnParseNode(tName, node.isCaseSensitive() ? '"' + indexColName + '"' : indexColName, node.getAlias());
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
        return multiTableRewriteMap != null ? node : WildcardParseNode.REWRITE_INSTANCE;
    }

    @Override
    public ParseNode visit(FamilyWildcardParseNode node) throws SQLException {
        return multiTableRewriteMap != null ? node : new FamilyWildcardParseNode(node, true);
    }
    
}

