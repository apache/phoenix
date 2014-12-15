/**
 *  Licensed to the Apache Software Foundation (ASF) under one or more
 *  contributor license agreements.  See the NOTICE file distributed with
 *  this work for additional information regarding copyright ownership.
 *  The ASF licenses this file to You under the Apache License, Version 2.0
 *  (the "License"); you may not use this file except in compliance with
 *  the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */
package org.apache.phoenix.expression.function;

import java.util.List;

import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.phoenix.expression.Expression;
import org.apache.phoenix.parse.FunctionParseNode.Argument;
import org.apache.phoenix.parse.FunctionParseNode.BuiltInFunction;
import org.apache.phoenix.schema.types.PChar;
import org.apache.phoenix.schema.types.PInteger;
import org.apache.phoenix.schema.types.PDataType;
import org.apache.phoenix.schema.types.PVarchar;
import org.apache.phoenix.schema.SortOrder;
import org.apache.phoenix.schema.tuple.Tuple;
import org.apache.phoenix.util.StringUtil;

/**
 * Implementation of LPAD(input string, length int [, fill string])
 * 
 * Fills up the input to length (number of characters) by prepending characters in fill (space by default). If the input
 * is already longer than length then it is truncated on the right.
 */
@BuiltInFunction(name = LpadFunction.NAME, args = { @Argument(allowedTypes = { PVarchar.class }),
    @Argument(allowedTypes = { PInteger.class }),
    @Argument(allowedTypes = { PVarchar.class }, defaultValue = "' '") })
public class LpadFunction extends ScalarFunction {
    public static final String NAME = "LPAD";

    public LpadFunction() {
    }

    public LpadFunction(List<Expression> children) {
        super(children);
    }

    /**
     * Helper function to get the utf8 length of CHAR or VARCHAR
     * 
     * @param ptr
     *            points to the string
     * @param sortOrder
     *            sortOrder of the string
     * @param isCharType
     *            whether the string is of char type
     * @return utf8 length of the string
     */
    private int getUTF8Length(ImmutableBytesWritable ptr, SortOrder sortOrder, boolean isCharType) {
        return isCharType ? ptr.getLength() : StringUtil.calculateUTF8Length(ptr.get(), ptr.getOffset(),
            ptr.getLength(), sortOrder);
    }

    /**
     * Helper function to get the byte length of a utf8 encoded string
     * 
     * @param ptr
     *            points to the string
     * @param sortOrder
     *            sortOrder of the string
     * @param isCharType
     *            whether the string is of char type
     * @return byte length of the string
     */
    private int getSubstringByteLength(ImmutableBytesWritable ptr, int length, SortOrder sortOrder, boolean isCharType) {
        return isCharType ? length : StringUtil.getByteLengthForUtf8SubStr(ptr.get(), ptr.getOffset(), length,
            sortOrder);
    }

    /**
     * Left pads a string with with the given fill expression.
     */
    @Override
    public boolean evaluate(Tuple tuple, ImmutableBytesWritable ptr) {
        Expression outputStrLenExpr = getOutputStrLenExpr();
        if (!outputStrLenExpr.evaluate(tuple, ptr)) {
            return false;
        }
        int outputStrLen = outputStrLenExpr.getDataType().getCodec().decodeInt(ptr, outputStrLenExpr.getSortOrder());
        if (outputStrLen < 0) {
            return false;
        }

        Expression strExp = getStrExpr();
        if (!strExp.evaluate(tuple, ptr)) {
            return false;
        }

        boolean isStrCharType = getStrExpr().getDataType() == PChar.INSTANCE;
        boolean isFillCharType = getFillExpr().getDataType() == PChar.INSTANCE;
        SortOrder strSortOrder = getStrExpr().getSortOrder();
        SortOrder fillSortOrder = getFillExpr().getSortOrder();
        int inputStrLen = getUTF8Length(ptr, strSortOrder, isStrCharType);

        if (outputStrLen == inputStrLen) {
            // nothing to do
            return true;
        }
        if (outputStrLen < inputStrLen) {
            // truncate the string from the right
            int subStrByteLength = getSubstringByteLength(ptr, outputStrLen, strSortOrder, isStrCharType);
            ptr.set(ptr.get(), ptr.getOffset(), subStrByteLength);
            return true;
        }

        // left pad the input string with the fill chars
        Expression fillExpr = getFillExpr();
        ImmutableBytesWritable fillPtr = new ImmutableBytesWritable();
        if (!fillExpr.evaluate(tuple, fillPtr)) {
            return false;
        }
        int fillExprLen = fillPtr.getLength();
        if (fillExprLen < 1) {
            // return if fill is empty
            return false;
        }

        // if the padding to be added is not a multiple of the length of the
        // fill string then we need to use part of the fill string to pad
        // LPAD(ab, 5, xy) = xyxab
        // padLen = 3
        // numFillsPrepended = 1
        // numFillCharsPrepended = 1

        // length of the fill string
        int fillLen = getUTF8Length(fillPtr, fillSortOrder, isFillCharType);
        // length of padding to be added
        int padLen = outputStrLen - inputStrLen;
        // number of fill strings to be prepended
        int numFillsPrepended = padLen / fillLen;
        // number of chars from fill string to be prepended
        int numFillCharsPrepended = padLen % fillLen;

        // byte length of the input string
        int strByteLength = getSubstringByteLength(ptr, ptr.getLength(), strSortOrder, isStrCharType);
        // byte length of the fill string
        int fillByteLength = getSubstringByteLength(fillPtr, fillPtr.getLength(), fillSortOrder, isFillCharType);
        // byte length of the full fills to be prepended
        int fullFillsByteLength = numFillsPrepended * fillByteLength;
        // byte length of the chars of fill string to be prepended
        int fillCharsByteLength = getSubstringByteLength(fillPtr, numFillCharsPrepended, fillSortOrder, isFillCharType);
        // byte length of the padded string =
        int strWithPaddingByteLength = fullFillsByteLength + fillCharsByteLength + strByteLength;

        // need to invert the fill string if the sort order of fill and
        // input are different
        boolean invertFill = fillSortOrder != strSortOrder;
        byte[] paddedStr =
            StringUtil.lpad(ptr.get(), ptr.getOffset(), ptr.getLength(), fillPtr.get(), fillPtr.getOffset(),
                fillPtr.getLength(), invertFill, strWithPaddingByteLength);
        ptr.set(paddedStr);
        return true;
    }

    @Override
    public PDataType getDataType() {
        return PVarchar.INSTANCE;
    }

    @Override
    public String getName() {
        return NAME;
    }

    @Override
    public SortOrder getSortOrder() {
        return getStrExpr().getSortOrder();
    }

    private Expression getStrExpr() {
        return children.get(0);
    }

    private Expression getFillExpr() {
        return children.get(2);
    }

    private Expression getOutputStrLenExpr() {
        return children.get(1);
    }

}