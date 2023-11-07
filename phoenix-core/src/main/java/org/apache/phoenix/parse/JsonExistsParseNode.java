package org.apache.phoenix.parse;

import org.apache.phoenix.compile.StatementContext;
import org.apache.phoenix.expression.Expression;
import org.apache.phoenix.expression.function.FunctionExpression;
import org.apache.phoenix.expression.function.JsonExistsFunction;
import org.apache.phoenix.schema.types.PDataType;
import org.apache.phoenix.schema.types.PJson;

import java.sql.SQLException;
import java.util.List;

public class JsonExistsParseNode extends FunctionParseNode {

    public JsonExistsParseNode(String name, List<ParseNode> children, BuiltInFunctionInfo info) {
        super(name, children, info);
    }

    @Override
    public FunctionExpression create(List<Expression> children, StatementContext context)
            throws SQLException {
        PDataType dataType = children.get(0).getDataType();
        if (!dataType.isCoercibleTo(PJson.INSTANCE)) {
            throw new SQLException(dataType + " type is unsupported for JSON_EXISTS().");
        }
        return new JsonExistsFunction(children);
    }
}
