/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.phoenix.expression.function;

import java.io.DataInput;
import java.io.IOException;
import java.util.List;

import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.phoenix.expression.Expression;
import org.apache.phoenix.expression.LiteralExpression;
import org.apache.phoenix.parse.FunctionParseNode.Argument;
import org.apache.phoenix.parse.FunctionParseNode.BuiltInFunction;
import org.apache.phoenix.schema.types.PChar;
import org.apache.phoenix.schema.types.PDataType;
import org.apache.phoenix.schema.types.PLong;
import org.apache.phoenix.schema.types.PVarchar;
import org.apache.phoenix.schema.SortOrder;
import org.apache.phoenix.schema.tuple.Tuple;
import org.apache.phoenix.util.StringUtil;


/**
 * 
 * Implementation of the SUBSTR(<string>,<offset>[,<length>]) built-in function
 * where <offset> is the offset from the start of <string>. A positive offset
 * is treated as 1-based, a zero offset is treated as 0-based, and a negative
 * offset starts from the end of the string working backwards. The optional
 * <length> argument is the number of characters to return. In the absence of the
 * <length> argument, the rest of the string starting from <offset> is returned.
 * If <length> is less than 1, null is returned.
 *
 * 
 * @since 0.1
 */
@BuiltInFunction(name=SubstrFunction.NAME,  args={
    @Argument(allowedTypes={PVarchar.class}),
    @Argument(allowedTypes={PLong.class}), // These are LONG because negative numbers end up as longs
    @Argument(allowedTypes={PLong.class},defaultValue="null")} )
public class SubstrFunction extends PrefixFunction {
    public static final String NAME = "SUBSTR";
    private boolean hasLengthExpression;
    private boolean isOffsetConstant;
    private boolean isLengthConstant;
    private boolean isFixedWidth;
    private Integer maxLength;

    public SubstrFunction() {
    }

    public SubstrFunction(List<Expression> children) {
        super(children);
        init();
    }

    private void init() {
        isOffsetConstant = getOffsetExpression() instanceof LiteralExpression;
        isLengthConstant = getLengthExpression() instanceof LiteralExpression;
        hasLengthExpression = !isLengthConstant || ((LiteralExpression)getLengthExpression()).getValue() != null;
        isFixedWidth = getStrExpression().getDataType().isFixedWidth() && ((hasLengthExpression && isLengthConstant) || (!hasLengthExpression && isOffsetConstant));
        if (hasLengthExpression && isLengthConstant) {
            Integer maxLength = ((Number)((LiteralExpression)getLengthExpression()).getValue()).intValue();
            this.maxLength = maxLength >= 0 ? maxLength : 0;
        } else if (isOffsetConstant) {
            Number offsetNumber = (Number)((LiteralExpression)getOffsetExpression()).getValue();
            if (offsetNumber != null) {
                int offset = offsetNumber.intValue();
                PDataType type = getStrExpression().getDataType();
                if (type.isFixedWidth()) {
                    if (offset >= 0) {
                        Integer maxLength = getStrExpression().getMaxLength();
                        this.maxLength = maxLength - offset + (offset == 0 ? 0 : 1);
                    } else {
                        this.maxLength = -offset;
                    }
                }
            }
        }
    }

    @Override
    public boolean evaluate(Tuple tuple, ImmutableBytesWritable ptr) {
        Expression offsetExpression = getOffsetExpression();
        if (!offsetExpression.evaluate(tuple,  ptr)) {
            return false;
        }
        int offset = offsetExpression.getDataType().getCodec().decodeInt(ptr, offsetExpression.getSortOrder());
        
        int length = -1;
        if (hasLengthExpression) {
            Expression lengthExpression = getLengthExpression();
            if (!lengthExpression.evaluate(tuple, ptr)) {
                return false;
            }
            length = lengthExpression.getDataType().getCodec().decodeInt(ptr, lengthExpression.getSortOrder());
            if (length <= 0) {
                return false;
            }
        }
        
        if (!getStrExpression().evaluate(tuple, ptr)) {
            return false;
        }
    
        boolean isCharType = getStrExpression().getDataType() == PChar.INSTANCE;
        SortOrder sortOrder = getStrExpression().getSortOrder();
        int strlen = isCharType ? ptr.getLength() : StringUtil.calculateUTF8Length(ptr.get(), ptr.getOffset(), ptr.getLength(), sortOrder);
        
        // Account for 1 versus 0-based offset
        offset = offset - (offset <= 0 ? 0 : 1);
        if (offset < 0) { // Offset < 0 means get from end
            offset = strlen + offset;
        }
        if (offset < 0 || offset >= strlen) {
            return false;
        }
        int maxLength = strlen - offset;
        length = length == -1 ? maxLength : Math.min(length,maxLength);
        
        int byteOffset = isCharType ? offset : StringUtil.getByteLengthForUtf8SubStr(ptr.get(), ptr.getOffset(), offset, sortOrder);
        int byteLength = isCharType ? length : StringUtil.getByteLengthForUtf8SubStr(ptr.get(), ptr.getOffset() + byteOffset, length, sortOrder);
        ptr.set(ptr.get(), ptr.getOffset() + byteOffset, byteLength);
        return true;
    }

    @Override
    public PDataType getDataType() {
        // If fixed width, then return child expression type.
        // If not fixed width, then we don't know how big this will be across the board
        return isFixedWidth ? getStrExpression().getDataType() : PVarchar.INSTANCE;
    }

    @Override
    public boolean isNullable() {
        return getStrExpression().isNullable() || !isFixedWidth || getOffsetExpression().isNullable();
    }

    @Override
    public Integer getMaxLength() {
        return maxLength;
    }
    
    @Override
    public SortOrder getSortOrder() {
        return getStrExpression().getSortOrder();
    }

    @Override
    public void readFields(DataInput input) throws IOException {
        super.readFields(input);
        init();
    }

    private Expression getStrExpression() {
        return children.get(0);
    }

    private Expression getOffsetExpression() {
        return children.get(1);
    }

    private Expression getLengthExpression() {
        return children.get(2);
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

    @Override
    protected boolean extractNode() {
        return true;
    }

    @Override
    public String getName() {
        return NAME;
    }
    
}
