package org.apache.phoenix.calcite.parse;

import java.util.List;

import org.apache.calcite.sql.SqlCall;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlLiteral;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlNodeList;
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.sql.parser.SqlParserPos;

import com.google.common.collect.ImmutableList;

/**
 * Parse tree node for SQL {@code ALTER TABLE} command.
 */
public class SqlAlterTable extends SqlCall {
    public final SqlOperator operator;
    public final SqlIdentifier tableName;
    public final SqlLiteral isView;
    public final SqlLiteral ifExists;
    public final SqlNodeList columnNames;
    public final SqlLiteral ifNotExists;
    public final SqlNodeList newColumnDefs;
    public final SqlNodeList tableOptions;

    /** Creates a ALTER TABLE. */
    public SqlAlterTable(
            SqlParserPos pos,
            SqlIdentifier tableName,
            SqlLiteral isView,
            SqlLiteral ifExists,
            SqlNodeList columnNames,
            SqlLiteral ifNotExists,
            SqlNodeList newColumnDefs,
            SqlNodeList tableOptions) {
        super(pos);
        this.operator =
                isView.booleanValue() ? new SqlDdlOperator("ALTER VIEW", SqlKind.ALTER_VIEW)
                        : new SqlDdlOperator("ALTER TABLE", SqlKind.ALTER_TABLE);
        this.tableName = tableName;
        this.isView = isView;
        this.ifExists = ifExists;
        this.columnNames = columnNames;
        this.ifNotExists = ifNotExists;
        this.newColumnDefs = newColumnDefs;
        this.tableOptions = tableOptions;
    }


    @Override
    public SqlOperator getOperator() {
        return this.operator;
    }

    @Override
    public List<SqlNode> getOperandList() {
        return ImmutableList.of(tableName, isView, ifExists, columnNames, ifNotExists, newColumnDefs, tableOptions);
    }

}
