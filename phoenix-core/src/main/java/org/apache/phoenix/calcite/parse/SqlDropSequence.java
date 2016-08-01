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

public class SqlDropSequence extends SqlCall {
    public static final SqlOperator OPERATOR = new SqlDdlOperator("DROP SEQUENCE", SqlKind.DROP_SEQUENCE);
    
    public final SqlIdentifier sequenceName;
    public final SqlLiteral ifExists;

    public SqlDropSequence(
            SqlParserPos pos,
            SqlIdentifier sequenceName,
            SqlLiteral ifExists) {
        super(pos);
        this.sequenceName = sequenceName;
        this.ifExists = ifExists;
    }

    @Override
    public SqlOperator getOperator() {
        return OPERATOR;
    }

    @Override
    public List<SqlNode> getOperandList() {
        return ImmutableList.of(sequenceName, ifExists);
    }

    @Override
    public void unparse(SqlWriter writer, int leftPrec, int rightPrec) {
        // TODO Auto-generated method stub
    }
}
