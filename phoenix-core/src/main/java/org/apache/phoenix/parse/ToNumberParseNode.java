package org.apache.phoenix.parse;

import java.sql.SQLException;
import java.text.Format;
import java.util.List;

import org.apache.phoenix.compile.StatementContext;
import org.apache.phoenix.expression.Expression;
import org.apache.phoenix.expression.LiteralExpression;
import org.apache.phoenix.expression.function.FunctionArgumentType;
import org.apache.phoenix.expression.function.FunctionExpression;
import org.apache.phoenix.expression.function.ToNumberFunction;
import org.apache.phoenix.schema.PDataType;

public class ToNumberParseNode extends FunctionParseNode {

    ToNumberParseNode(String name, List<ParseNode> children,
            BuiltInFunctionInfo info) {
        super(name, children, info);
    }

    @Override
    public FunctionExpression create(List<Expression> children, StatementContext context) throws SQLException {
        PDataType dataType = children.get(0).getDataType();
        String formatString = (String)((LiteralExpression)children.get(1)).getValue(); // either date or number format string
        Format formatter =  null;
        FunctionArgumentType type;
        
        if (dataType.isCoercibleTo(PDataType.TIMESTAMP)) {
            if (formatString == null) {
                formatString = context.getDateFormat();
                formatter = context.getDateFormatter();
            } else {
                formatter = FunctionArgumentType.TEMPORAL.getFormatter(formatString);
            }
            type = FunctionArgumentType.TEMPORAL;
        }
        else if (dataType.isCoercibleTo(PDataType.CHAR)) {
            if (formatString != null) {
                formatter = FunctionArgumentType.CHAR.getFormatter(formatString);
            }
            type = FunctionArgumentType.CHAR;
        }
        else {
            throw new SQLException(dataType + " type is unsupported for TO_NUMBER().  Numeric and temporal types are supported.");
        }
        return new ToNumberFunction(children, type, formatString, formatter);
    }
}
