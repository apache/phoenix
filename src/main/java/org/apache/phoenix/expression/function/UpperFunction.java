package org.apache.phoenix.expression.function;

import java.sql.SQLException;
import java.util.List;

import org.apache.hadoop.hbase.io.ImmutableBytesWritable;

import org.apache.phoenix.expression.Expression;
import org.apache.phoenix.parse.FunctionParseNode;
import org.apache.phoenix.schema.PDataType;
import org.apache.phoenix.schema.tuple.Tuple;

@FunctionParseNode.BuiltInFunction(name=UpperFunction.NAME,  args={
        @FunctionParseNode.Argument(allowedTypes={PDataType.VARCHAR})} )
public class UpperFunction extends ScalarFunction {
    public static final String NAME = "UPPER";

    public UpperFunction() {
    }

    public UpperFunction(List<Expression> children) throws SQLException {
        super(children);
    }

    @Override
    public boolean evaluate(Tuple tuple, ImmutableBytesWritable ptr) {
        if (!getStrExpression().evaluate(tuple, ptr)) {
            return false;
        }

        String sourceStr = (String)PDataType.VARCHAR.toObject(ptr, getStrExpression().getColumnModifier());
        if (sourceStr == null) {
            return true;
        }

        ptr.set(PDataType.VARCHAR.toBytes(sourceStr.toUpperCase()));
        return true;
    }

    @Override
    public PDataType getDataType() {
        return getStrExpression().getDataType();
    }

    @Override
    public boolean isNullable() {
        return getStrExpression().isNullable();
    }

    @Override
    public String getName() {
        return NAME;
    }

    private Expression getStrExpression() {
        return children.get(0);
    }
}
