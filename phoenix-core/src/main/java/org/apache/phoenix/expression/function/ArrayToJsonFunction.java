package org.apache.phoenix.expression.function;

import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.phoenix.exception.SQLExceptionCode;
import org.apache.phoenix.exception.SQLExceptionInfo;
import org.apache.phoenix.expression.Expression;
import org.apache.phoenix.parse.FunctionParseNode;
import org.apache.phoenix.schema.IllegalDataException;
import org.apache.phoenix.schema.SortOrder;
import org.apache.phoenix.schema.json.PhoenixJson;
import org.apache.phoenix.schema.tuple.Tuple;
import org.apache.phoenix.schema.types.*;
import org.apache.phoenix.util.ByteUtil;

import java.io.IOException;
import java.sql.SQLException;
import java.util.List;


@FunctionParseNode.BuiltInFunction(name=ArrayToJsonFunction.NAME,  args={
        @FunctionParseNode.Argument(allowedTypes={PVarchar.class})} )
public class ArrayToJsonFunction extends ScalarFunction {
    public static final String NAME = "Array_To_Json";

    public ArrayToJsonFunction() {
    }

    public ArrayToJsonFunction(List<Expression> children) throws SQLException {
        super(children);
    }

    @Override
    public boolean evaluate(Tuple tuple, ImmutableBytesWritable ptr) {
        Expression arrayExpr = getChildren().get(0);

        if (!arrayExpr.evaluate(tuple, ptr)) {
            return false;
        }
        if (ptr.getLength() == 0) {
            return false;
        }


        PDataType baseType = PDataType.fromTypeId(arrayExpr.getDataType()
                .getSqlType()
                - PDataType.ARRAY_TYPE_BASE);
        int length = PArrayDataType.getArrayLength(ptr, baseType, arrayExpr.getMaxLength());
        StringBuilder builder = new StringBuilder("[");
        ImmutableBytesWritable tmp = new ImmutableBytesWritable();
        for(int i=1;i<=length;i++){
            tmp.set(ptr.get());
            PArrayDataType.positionAtArrayElement(tmp, i - 1,baseType, arrayExpr.getMaxLength());
            Object re =baseType.toObject(tmp);
            builder.append(PhoenixJson.DataToJsonValue(baseType, re));
            if(i != length)
            builder.append(",");
        }
        builder.append("]");

        try {
            String str = builder.toString();
            PhoenixJson phoenixJson = PhoenixJson.getInstance(str);
            byte[] json = PJson.INSTANCE.toBytes(phoenixJson);
            ptr.set(json);
        } catch (SQLException sqe) {
            System.out.println(sqe.getMessage());
        }
        return true;
    }

    @Override
    public SortOrder getSortOrder() {
        return getChildren().get(0).getSortOrder();
    }

    @Override
    public PDataType getDataType() {
        return PJson.INSTANCE;
    }

    @Override
    public String getName() {
        return NAME;
    }

}
