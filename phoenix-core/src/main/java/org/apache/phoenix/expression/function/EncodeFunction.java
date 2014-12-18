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
import org.apache.phoenix.schema.IllegalDataException;
import org.apache.phoenix.schema.types.PDataType;
import org.apache.phoenix.schema.types.PLong;
import org.apache.phoenix.schema.types.PVarchar;
import org.apache.phoenix.schema.tuple.Tuple;
import org.apache.phoenix.util.Base62Encoder;

/**
 * Implementation of ENCODE(input number, format encodeformat)
 * 
 * Converts the given base 10 number to a base 62 number and returns a string representing the number.
 */
@BuiltInFunction(name = EncodeFunction.NAME, args = { @Argument(allowedTypes = { PLong.class }),
    @Argument(enumeration = "EncodeFormat") })
public class EncodeFunction extends ScalarFunction {
    public static final String NAME = "ENCODE";
    
    public EncodeFunction() {
    }

    public EncodeFunction(List<Expression> children) {
        super(children);
    }

    @Override
    public boolean evaluate(Tuple tuple, ImmutableBytesWritable ptr) {
        Expression numExpr = getNumExpr();
        if (!numExpr.evaluate(tuple, ptr)) {
            return false;
        }
        long num = numExpr.getDataType().getCodec().decodeLong(ptr, numExpr.getSortOrder());
        
        Expression encodingExpression = getEncodingExpr();
        if (!encodingExpression.evaluate(tuple, ptr)) {
            return false;
        }

        if (ptr.getLength() == 0) {
            throw new IllegalDataException(getMissingEncodeFormatMsg());
        }

        PDataType type = encodingExpression.getDataType();
        String encodingFormat = ((String) type.toObject(ptr)).toUpperCase();
        EncodeFormat format = EncodeFormat.valueOf(encodingFormat);
        switch (format) {
            case BASE62:
                String encodedString = Base62Encoder.toString(num);
                ptr.set(PVarchar.INSTANCE.toBytes(encodedString));
                break;
            default:
                throw new IllegalDataException(getUnsupportedEncodeFormatMsg(encodingFormat));
        }
        return true;
    }
    
    public static String getMissingEncodeFormatMsg() {
        return "Missing Encode Format";
    }
    
    public static String getUnsupportedEncodeFormatMsg(String encodeFormat) {
        return "Unsupported Encode Format : " + encodeFormat;
    }

    @Override
    public PDataType getDataType() {
        return PVarchar.INSTANCE;
    }

    @Override
    public String getName() {
        return NAME;
    }

    private Expression getNumExpr() {
        return children.get(0);
    }

    private Expression getEncodingExpr() {
        return children.get(1);
    }

}
