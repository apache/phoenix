package org.apache.phoenix.expression;

import org.apache.hadoop.hbase.io.ImmutableBytesWritable;

import org.apache.phoenix.schema.PDataType;
import org.apache.phoenix.schema.tuple.Tuple;

public class RowKeyExpression extends BaseTerminalExpression {
    public static final RowKeyExpression INSTANCE = new RowKeyExpression();
    
    private RowKeyExpression() {
    }

    @Override
    public boolean evaluate(Tuple tuple, ImmutableBytesWritable ptr) {
        tuple.getKey(ptr);
        return true;
    }

    @Override
    public PDataType getDataType() {
        return PDataType.VARBINARY;
    }

}
