package org.apache.calcite.sql;

import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.sql.util.SqlVisitor;
import org.apache.calcite.sql.validate.SqlValidator;
import org.apache.calcite.sql.validate.SqlValidatorScope;
import org.apache.calcite.util.Litmus;

public class SqlTableOptionNode extends SqlNode {
    public final SqlIdentifier key;
    public final SqlLiteral value;

    public SqlTableOptionNode(SqlParserPos pos, SqlIdentifier key, SqlLiteral value) {
        super(pos);
        this.key = key;
        this.value = value;
    }

    @Override
    public void unparse(SqlWriter writer, int leftPrec, int rightPrec) {
        // TODO Auto-generated method stub

    }

    @Override
    public void validate(SqlValidator validator, SqlValidatorScope scope) {
        // TODO Auto-generated method stub

    }

    @Override
    public <R> R accept(SqlVisitor<R> visitor) {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public boolean equalsDeep(SqlNode node, Litmus litmus) {
        // TODO Auto-generated method stub
        return false;
    }

}
