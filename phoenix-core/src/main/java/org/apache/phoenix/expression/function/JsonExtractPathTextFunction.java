package org.apache.phoenix.expression.function;

import java.sql.SQLException;
import java.util.List;

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
import org.apache.phoenix.schema.types.PDataType;
import org.apache.phoenix.schema.types.PJsonDataType;
import org.apache.phoenix.schema.types.PVarchar;
import org.apache.phoenix.schema.types.PVarcharArray;
import org.apache.phoenix.schema.types.PhoenixArray;
import org.apache.phoenix.util.ByteUtil;

@BuiltInFunction(name = JsonExtractPathTextFunction.NAME, args = {
        @Argument(allowedTypes = { PJsonDataType.class }),
        @Argument(allowedTypes = { PVarcharArray.class }) })
public class JsonExtractPathTextFunction extends ScalarFunction {
    public static final String NAME = "JSON_EXTRACT_PATH_TEXT";

    public JsonExtractPathTextFunction() {
        super();
    }

    public JsonExtractPathTextFunction(List<Expression> children) {
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
        PhoenixJson phoenixJson =
                (PhoenixJson) PJsonDataType.INSTANCE.toObject(ptr.get(), ptr.getOffset(),
                    ptr.getLength());

        Expression jsonPathArrayExpression = children.get(1);
        if (!jsonPathArrayExpression.evaluate(tuple, ptr)) {
            return false;
        }

        if (ptr.getLength() == 0) {
            return false;
        }

        PhoenixArray phoenixArray = (PhoenixArray) PVarcharArray.INSTANCE.toObject(ptr);
        try {
            String[] jsonPaths = (String[]) phoenixArray.getArray();
            PhoenixJson phoenixJson2 = phoenixJson.getNullablePhoenixJson(jsonPaths);
            if (phoenixJson2 == null) {
                ptr.set(ByteUtil.EMPTY_BYTE_ARRAY);
            } else {
                byte[] json = PVarchar.INSTANCE.toBytes(phoenixJson2.serializeToString());
                ptr.set(json);
            }

        } catch (SQLException sqe) {
            new IllegalDataException(new SQLExceptionInfo.Builder(SQLExceptionCode.ILLEGAL_DATA)
                    .setRootCause(sqe).build().buildException());
        }
        return true;
    }

    @Override
    public boolean isNullable() {
        return PVarchar.INSTANCE.isNullable();
    }

    @SuppressWarnings("rawtypes")
    @Override
    public PDataType getDataType() {
        return PVarchar.INSTANCE;
    }

    @Override
    public String getName() {
        return NAME;
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
