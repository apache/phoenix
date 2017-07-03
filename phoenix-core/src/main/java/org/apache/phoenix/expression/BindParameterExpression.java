package org.apache.phoenix.expression;

import java.sql.SQLException;

import org.apache.calcite.avatica.util.ArrayImpl;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.phoenix.execute.RuntimeContext;
import org.apache.phoenix.expression.visitor.ExpressionVisitor;
import org.apache.phoenix.schema.SortOrder;
import org.apache.phoenix.schema.tuple.Tuple;
import org.apache.phoenix.schema.types.PDataType;
import org.apache.phoenix.schema.types.PhoenixArray;

public class BindParameterExpression extends VariableExpression {
    @SuppressWarnings("rawtypes")
    private final PDataType type;
    private final Integer maxLength;

    public BindParameterExpression(int index,
            @SuppressWarnings("rawtypes") PDataType type, Integer maxLength,
            RuntimeContext runtimeContext) {
        super("?" + index, runtimeContext);
        this.type = type;
        this.maxLength = maxLength;
    }

    @Override
    public boolean evaluate(Tuple tuple, ImmutableBytesWritable ptr) {
        Object value = runtimeContext.getBindParameterValue(name);
        if (value == null) {
            return false;
        }
        if(type.isArrayType()){
            PDataType arrayBaseType = PDataType.arrayBaseType(type);
            try {
                value=new PhoenixArray(arrayBaseType,(Object[])((ArrayImpl)value).getArray());
            } catch (SQLException e) {
                throw new RuntimeException(e);
            }
            if(arrayBaseType.isFixedWidth()){
                value=new PhoenixArray((PhoenixArray)value,maxLength);
            }
        }
        ptr.set(type.toBytes(value));
        type.pad(ptr, maxLength, SortOrder.ASC);
        return true;
    }

    @Override
    public <T> T accept(ExpressionVisitor<T> visitor) {
        return visitor.visit(this);
    }

    @SuppressWarnings("rawtypes")
    @Override
    public PDataType getDataType() {
        return type;
    }

    public Integer getMaxLength() {
        return maxLength;
    }
}
