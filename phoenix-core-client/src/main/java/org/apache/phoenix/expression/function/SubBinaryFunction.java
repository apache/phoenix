package org.apache.phoenix.expression.function;

import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.phoenix.expression.Expression;
import org.apache.phoenix.expression.LiteralExpression;
import org.apache.phoenix.parse.FunctionParseNode;
import org.apache.phoenix.schema.SortOrder;
import org.apache.phoenix.schema.tuple.Tuple;
import org.apache.phoenix.schema.types.PBinary;
import org.apache.phoenix.schema.types.PDataType;
import org.apache.phoenix.schema.types.PLong;
import org.apache.phoenix.schema.types.PVarbinary;
import org.apache.phoenix.schema.types.PVarbinaryEncoded;

import java.io.DataInput;
import java.io.IOException;
import java.util.Arrays;
import java.util.List;

/**
 *
 * Implementation of the {@code SUBBINARY(<binary>,<offset>[,<length>]) } built-in function
 * where  {@code <offset> } is the offset from the start of {@code  <binary> }. A positive offset
 * is treated as 1-based, a zero offset is treated as 0-based, and a negative
 * offset starts from the end of the byte array working backwards. The optional
 * {@code <length> } argument is the number of bytes to return. In the absence of the
 * {@code <length> }  argument, the rest of the byte array starting from {@code <offset> } is returned.
 * If {@code <length> }  is less than 1, null is returned.

 */
@FunctionParseNode.BuiltInFunction(name=SubBinaryFunction.NAME,  args={
        @FunctionParseNode.Argument(allowedTypes={PBinary.class,PVarbinary.class, PVarbinaryEncoded.class}),
        @FunctionParseNode.Argument(allowedTypes={PLong.class}), // These are LONG because negative numbers end up as longs
        @FunctionParseNode.Argument(allowedTypes={PLong.class},defaultValue="null")} )
public class SubBinaryFunction extends PrefixFunction {

    public static final String NAME = "SUBBINARY";
    private boolean hasLengthExpression;
    private boolean isOffsetConstant;
    private boolean isLengthConstant;
    private boolean isFixedWidth;
    private Integer maxLength;

    public SubBinaryFunction() {
    }

    public SubBinaryFunction(List<Expression> children) {
        super(children);
        init();
    }

    private void init() {
        isOffsetConstant = getOffsetExpression() instanceof LiteralExpression;
        isLengthConstant = getLengthExpression() instanceof LiteralExpression;
        hasLengthExpression = !isLengthConstant || ((LiteralExpression)getLengthExpression()).getValue() != null;
        isFixedWidth = getBinaryExpression().getDataType().isFixedWidth() && ((hasLengthExpression && isLengthConstant) || (!hasLengthExpression && isOffsetConstant));
        if (hasLengthExpression && isLengthConstant) {
            Integer maxLength = ((Number)((LiteralExpression)getLengthExpression()).getValue()).intValue();
            this.maxLength = maxLength >=0 ? maxLength : 0;
        } else if (isOffsetConstant) {
            Number offsetNumber = (Number)((LiteralExpression)getOffsetExpression()).getValue();
            if (offsetNumber != null) {
                int offset = offsetNumber.intValue();
                PDataType type = getBinaryExpression().getDataType();
                if (type.isFixedWidth()) {
                    if (offset >= 0) {
                        Integer maxLength = getBinaryExpression().getMaxLength();
                        this.maxLength = maxLength - offset + (offset == 0 ? 0 : 1);
                    }
                }
            }
        }
    }

    @Override
    public boolean evaluate(Tuple tuple, ImmutableBytesWritable ptr) {
        Expression offsetExpression = getOffsetExpression();
        if (!offsetExpression.evaluate(tuple, ptr)) {
            return false;
        }
        if (ptr.getLength() == 0) {
            return true;
        }
        int offset = offsetExpression.getDataType().getCodec().decodeInt(ptr, offsetExpression.getSortOrder());
        int length = -1;
        if (hasLengthExpression) {
            Expression lengthExpression = getLengthExpression();
            if (!lengthExpression.evaluate(tuple, ptr)) {
                return false;
            }
            if (ptr.getLength() == 0) {
                return true;
            }
            length = lengthExpression.getDataType().getCodec().decodeInt(ptr, lengthExpression.getSortOrder());
            if (length <= 0) {
                return false;
            }
        }
        if (!getBinaryExpression().evaluate(tuple, ptr)) {
            return false;
        }
        if (ptr.getLength()==0) {
            return true;
        }
        byte[] bytes = new byte[]{};
        int binLength;
        if (getDataType() == PVarbinaryEncoded.INSTANCE) {
            bytes = (byte[]) PVarbinaryEncoded.INSTANCE.toObject(ptr.get(), ptr.getOffset(),
                            ptr.getLength());
            binLength = bytes.length;
        } else {
            binLength = ptr.getLength();
        }
        // Account for 1 versus 0-based offset
        offset = offset - (offset <= 0 ? 0 : 1);
        if (offset < 0) { // Offset < 0 means get from end
            offset = binLength + offset;
        }
        if (offset < 0 || offset >= binLength) {
            return false;
        }
        int maxLength = binLength - offset;
        length = length == -1 ? maxLength : Math.min(length,maxLength);
        if (getDataType() == PVarbinaryEncoded.INSTANCE) {
            byte[] result = Arrays.copyOfRange(bytes, offset, offset + length);
            ptr.set(PVarbinaryEncoded.INSTANCE.toBytes(result));
        }
        else {
            ptr.set(ptr.get(), ptr.getOffset() + offset, length);
        }
        return true;
    }

    @Override
    public String getName() {
        return NAME;
    }

    @Override
    public PDataType getDataType() {
        return getBinaryExpression().getDataType();
    }

    @Override
    public boolean isNullable() {
        return getBinaryExpression().isNullable() || !isFixedWidth || getOffsetExpression().isNullable();
    }

    @Override
    public Integer getMaxLength() {
        return maxLength;
    }

    @Override
    public SortOrder getSortOrder() {
        return getBinaryExpression().getSortOrder();
    }

    @Override
    public void readFields(DataInput input) throws IOException {
        super.readFields(input);
        init();
    }

    @Override
    public OrderPreserving preservesOrder() {
        if (isOffsetConstant) {
            LiteralExpression literal = (LiteralExpression) getOffsetExpression();
            Number offsetNumber = (Number) literal.getValue();
            if (offsetNumber != null) {
                int offset = offsetNumber.intValue();
                if ((offset == 0 || offset == 1) && (!hasLengthExpression || isLengthConstant)) {
                    return OrderPreserving.YES_IF_LAST;
                }
            }
        }
        return OrderPreserving.NO;
    }

    private Expression getBinaryExpression() {
        return children.get(0);
    }

    private Expression getOffsetExpression() {
        return children.get(1);
    }

    private Expression getLengthExpression() {
        return children.get(2);
    }

    @Override
    public String toString() {
        StringBuilder buf = new StringBuilder(getName() + "(");
        if (children.isEmpty())
            return buf.append(")").toString();
        if (hasLengthExpression) {
            buf.append(getBinaryExpression());
            buf.append(", ");
            buf.append(getOffsetExpression());
            buf.append(", ");
            buf.append(getLengthExpression());
        } else {
            buf.append(getBinaryExpression());
            buf.append(", ");
            buf.append(getOffsetExpression());
        }
        buf.append(")");
        return buf.toString();
    }
}
