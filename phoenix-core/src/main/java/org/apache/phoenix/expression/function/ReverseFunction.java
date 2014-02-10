package org.apache.phoenix.expression.function;

import java.sql.SQLException;
import java.util.List;

import org.apache.hadoop.hbase.io.ImmutableBytesWritable;

import org.apache.phoenix.expression.Expression;
import org.apache.phoenix.parse.FunctionParseNode.Argument;
import org.apache.phoenix.parse.FunctionParseNode.BuiltInFunction;
import org.apache.phoenix.schema.SortOrder;
import org.apache.phoenix.schema.PDataType;
import org.apache.phoenix.schema.tuple.Tuple;
import org.apache.phoenix.util.StringUtil;

@BuiltInFunction(name=ReverseFunction.NAME,  args={
        @Argument(allowedTypes={PDataType.VARCHAR})} )
public class ReverseFunction extends ScalarFunction {
    public static final String NAME = "REVERSE";
    
    public ReverseFunction() {
    }

    public ReverseFunction(List<Expression> children) throws SQLException {
        super(children);
    }

    @Override
    public boolean evaluate(Tuple tuple, ImmutableBytesWritable ptr) {
        Expression arg = getChildren().get(0);
        if (!arg.evaluate(tuple, ptr)) {
            return false;
        }

        int targetOffset = ptr.getLength();
        if (targetOffset == 0) {
            return true;
        }

        byte[] source = ptr.get();
        byte[] target = new byte[targetOffset];
        int sourceOffset = ptr.getOffset(); 
        int endOffset = sourceOffset + ptr.getLength();
        SortOrder sortOrder = arg.getSortOrder();
        while (sourceOffset < endOffset) {
            int nBytes = StringUtil.getBytesInChar(source[sourceOffset], sortOrder);
            targetOffset -= nBytes;
            System.arraycopy(source, sourceOffset, target, targetOffset, nBytes);
            sourceOffset += nBytes;
        }
        ptr.set(target);
        return true;
    }

    @Override
    public SortOrder getSortOrder() {
        return getChildren().get(0).getSortOrder();
    }

    @Override
    public PDataType getDataType() {
        return PDataType.VARCHAR;
    }

    @Override
    public String getName() {
        return NAME;
    }

}
