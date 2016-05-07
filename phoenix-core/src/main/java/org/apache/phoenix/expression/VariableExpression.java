package org.apache.phoenix.expression;

import java.io.DataOutput;
import java.io.IOException;
import java.sql.SQLException;

import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.phoenix.execute.RuntimeContext;

public abstract class VariableExpression extends BaseTerminalExpression {
    protected final String name;
    protected final RuntimeContext runtimeContext;
    
    VariableExpression(String name, RuntimeContext runtimeContext) {
        this.name = name;
        this.runtimeContext = runtimeContext;
    }

    @Override
    public void write(DataOutput output) throws IOException {
        ImmutableBytesWritable ptr = new ImmutableBytesWritable();
        boolean success = evaluate(null, ptr);
        Object value = success ? getDataType().toObject(ptr) : null;
        try {
            LiteralExpression expr = LiteralExpression.newConstant(value, getDataType());
            expr.write(output);
        } catch (SQLException e) {
            throw new IOException(e);
        }
    }
}
