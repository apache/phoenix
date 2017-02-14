package org.apache.phoenix.calcite.parse;

import java.util.List;

import org.apache.calcite.sql.SqlCall;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlLiteral;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.sql.parser.SqlParserPos;

import com.google.common.collect.ImmutableList;

/**
 * Parse tree node for SQL {@code ALTER INDEX} command.
 */
public class SqlAlterIndex extends SqlCall {
    public final SqlOperator operator;
    public final SqlIdentifier indexName;
    public final SqlIdentifier dataTableName;
    public final SqlLiteral ifExists;
    public final SqlIdentifier indexState;
    public final SqlLiteral async;

    /** Creates a ALTER TABLE. */
    public SqlAlterIndex(
            SqlParserPos pos,
            SqlIdentifier indexName,
            SqlIdentifier dataTableName,
            SqlIdentifier indexState,
            SqlLiteral ifExists,
            SqlLiteral async) {
        super(pos);
        this.operator = new SqlDdlOperator("ALTER INDEX", SqlKind.ALTER_INDEX);
        this.indexName = indexName;
        this.dataTableName = dataTableName;
        this.ifExists = ifExists;
        this.indexState = indexState;
        this.async = async;
    }


    @Override
    public SqlOperator getOperator() {
        return this.operator;
    }

    @Override
    public List<SqlNode> getOperandList() {
        return ImmutableList.of(indexName, dataTableName, ifExists, indexState, async);
    }

}
