package org.apache.phoenix.calcite.parse;

import java.util.List;

import org.apache.calcite.sql.SqlCall;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlLiteral;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.sql.SqlWriter;
import org.apache.calcite.sql.parser.SqlParserPos;

import com.google.common.collect.ImmutableList;

public class SqlDropTable extends SqlCall {
    public final SqlOperator operator;
    
    public final SqlIdentifier tableName;
    public final SqlLiteral ifExists;
    public final SqlLiteral cascade;

    public SqlDropTable(
            SqlParserPos pos,
            boolean isDropTable,
            SqlIdentifier tableName,
            SqlLiteral ifExists,
            SqlLiteral cascade) {
        super(pos);
        if (isDropTable) {
            operator = new SqlDdlOperator("DROP TABLE", SqlKind.DROP_TABLE);
        } else {
            operator = new SqlDdlOperator("DROP VIEW", SqlKind.DROP_VIEW);
        }
        this.tableName = tableName;
        this.ifExists = ifExists;
        this.cascade = cascade;
    }

    @Override
    public SqlOperator getOperator() {
        return operator;
    }

    @Override
    public List<SqlNode> getOperandList() {
        return ImmutableList.of(tableName, ifExists, cascade);
    }

    @Override
    public void unparse(SqlWriter writer, int leftPrec, int rightPrec) {
        // TODO Auto-generated method stub
    }
}
