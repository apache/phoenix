package org.apache.phoenix.expression.function;

import java.util.Collections;
import java.util.List;

import org.apache.hadoop.hbase.filter.CompareFilter.CompareOp;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;

import org.apache.phoenix.compile.KeyPart;
import org.apache.phoenix.expression.Expression;
import org.apache.phoenix.query.KeyRange;
import org.apache.phoenix.schema.PColumn;
import org.apache.phoenix.schema.PDataType;
import org.apache.phoenix.util.ByteUtil;

abstract public class PrefixFunction extends ScalarFunction {
    public PrefixFunction() {
    }

    public PrefixFunction(List<Expression> children) {
        super(children);
    }

    @Override
    public int getKeyFormationTraversalIndex() {
        return preservesOrder() == OrderPreserving.NO ? NO_TRAVERSAL : 0;
    }
    
    protected boolean extractNode() {
        return false;
    }

    private static byte[] evaluateExpression(Expression rhs) {
        ImmutableBytesWritable ptr = new ImmutableBytesWritable();
        rhs.evaluate(null, ptr);
        byte[] key = ByteUtil.copyKeyBytesIfNecessary(ptr);
        return key;
    }
    
    @Override
    public KeyPart newKeyPart(final KeyPart childPart) {
        return new KeyPart() {
            private final List<Expression> extractNodes = extractNode() ? Collections.<Expression>singletonList(PrefixFunction.this) : Collections.<Expression>emptyList();

            @Override
            public PColumn getColumn() {
                return childPart.getColumn();
            }

            @Override
            public List<Expression> getExtractNodes() {
                return extractNodes;
            }

            @Override
            public KeyRange getKeyRange(CompareOp op, Expression rhs) {
                byte[] key;
                KeyRange range;
                PDataType type = getColumn().getDataType();
                switch (op) {
                case EQUAL:
                    key = evaluateExpression(rhs);
                    range = type.getKeyRange(key, true, ByteUtil.nextKey(key), false);
                    break;
                case GREATER:
                    key = evaluateExpression(rhs);
                    range = type.getKeyRange(ByteUtil.nextKey(key), true, KeyRange.UNBOUND, false);
                    break;
                case LESS_OR_EQUAL:
                    key = evaluateExpression(rhs);
                    range = type.getKeyRange(KeyRange.UNBOUND, false, ByteUtil.nextKey(key), false);
                    break;
                default:
                    return childPart.getKeyRange(op, rhs);
                }
                Integer length = getColumn().getByteSize();
                return length == null ? range : range.fill(length);
            }
        };
    }


}
