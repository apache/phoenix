package org.apache.phoenix.expression.function;

import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.phoenix.compile.KeyPart;
import org.apache.phoenix.exception.SQLExceptionCode;
import org.apache.phoenix.exception.SQLExceptionInfo;
import org.apache.phoenix.expression.Expression;
import org.apache.phoenix.parse.FunctionParseNode.Argument;
import org.apache.phoenix.parse.FunctionParseNode.BuiltInFunction;
import org.apache.phoenix.schema.IllegalDataException;
import org.apache.phoenix.schema.json.PhoenixJson;
import org.apache.phoenix.schema.tuple.Tuple;
import org.apache.phoenix.schema.types.*;
import org.apache.phoenix.util.ByteUtil;

import java.sql.SQLException;
import java.util.List;

@BuiltInFunction(name = JsonArrayElementsFunction.NAME, args = {
        @Argument(allowedTypes = { PJson.class })})
public class JsonArrayElementsFunction extends ScalarFunction {
    public static final String NAME = "JSON_ARRAY_ELEMENTS";

    public JsonArrayElementsFunction() {
        super();
    }

    public JsonArrayElementsFunction(List<Expression> children) {
        super(children);
    }

    @Override
    public boolean evaluate(Tuple tuple, ImmutableBytesWritable ptr) {

        Expression jsonExpression = this.children.get(0);
        if (!jsonExpression.evaluate(tuple, ptr)) {
            return false;
        }
        if (ptr.getLength() == 0) {
            return false;
        }
        try {
            PhoenixJson phoenixJson =
                    (PhoenixJson) PJson.INSTANCE.toObject(ptr.get(), ptr.getOffset(),
                            ptr.getLength());
            Object[] elements = phoenixJson.getJsonArrayElements();
            PhoenixArray pa = PArrayDataType.instantiatePhoenixArray(PVarchar.INSTANCE, elements);
            byte[] array = PVarcharArray.INSTANCE.toBytes(pa);
            ptr.set(array);
        }
        catch (SQLException sqe) {
            new IllegalDataException(new SQLExceptionInfo.Builder(SQLExceptionCode.ILLEGAL_DATA)
                    .setRootCause(sqe).build().buildException());
        }

        return true;
    }

    @SuppressWarnings("rawtypes")
    @Override
    public PDataType getDataType() {
        return PVarcharArray.INSTANCE;
    }

    @Override
    public String getName() {
        return NAME;
    }

    @Override
    public boolean isNullable() {
        return PVarcharArray.INSTANCE.isNullable();
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
