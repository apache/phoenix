package org.apache.phoenix.expression.function;

import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.phoenix.compile.KeyPart;
import org.apache.phoenix.exception.SQLExceptionCode;
import org.apache.phoenix.exception.SQLExceptionInfo;
import org.apache.phoenix.expression.Expression;
import org.apache.phoenix.parse.FunctionParseNode.Argument;
import org.apache.phoenix.parse.FunctionParseNode.BuiltInFunction;
import org.apache.phoenix.schema.IllegalDataException;
import org.apache.phoenix.schema.json.PhoenixJson;
import org.apache.phoenix.schema.tuple.Tuple;
import org.apache.phoenix.schema.types.PDataType;
import org.apache.phoenix.schema.types.PJson;
import org.apache.phoenix.schema.types.PVarcharArray;
import org.apache.phoenix.schema.types.PhoenixArray;
import org.apache.phoenix.util.ByteUtil;

import java.io.IOException;
import java.sql.SQLException;
import java.util.List;

@BuiltInFunction(name = ToJsonFunction.NAME, args = {
        @Argument(allowedTypes = { PDataType.class })})
public class ToJsonFunction extends ScalarFunction {
    public static final String NAME = "TO_JSON";

    public ToJsonFunction() {
        super();
    }

    public ToJsonFunction(List<Expression> children) {
        super(children);
    }

    @Override
    public boolean evaluate(Tuple tuple, ImmutableBytesWritable ptr) {

        Expression expression = this.children.get(0);
        if (!expression.evaluate(tuple, ptr)) {
            return false;
        }
        if (ptr.getLength() == 0) {
            return false;
        }
        PDataType baseType = expression.getDataType();
        Object re =baseType.toObject(ptr);
        String jsons = PhoenixJson.DataToJsonValue(baseType, re);
        try {
            PhoenixJson phoenixJson  = PhoenixJson.getInstance(jsons);
            byte[] json = PJson.INSTANCE.toBytes(phoenixJson);
            ptr.set(json);
        } catch (SQLException sqe) {
            System.out.println(sqe.getMessage());
        }

        return true;
    }

    @SuppressWarnings("rawtypes")
    @Override
    public PDataType getDataType() {
        return PJson.INSTANCE;
    }

    @Override
    public String getName() {
        return NAME;
    }

    @Override
    public boolean isNullable() {
        return PJson.INSTANCE.isNullable();
    }

    @Override
    public int getKeyFormationTraversalIndex() {
        return NO_TRAVERSAL;
    }

    @Override
    public KeyPart newKeyPart(KeyPart childPart) {
        return null;
    }

    @Override
    public OrderPreserving preservesOrder() {
        return OrderPreserving.NO;
    }

}
