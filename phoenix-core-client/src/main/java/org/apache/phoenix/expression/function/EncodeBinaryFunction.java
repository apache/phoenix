/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to you under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
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

import java.sql.SQLException;
import java.util.Base64;
import java.util.List;

import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.phoenix.expression.Expression;
import org.apache.phoenix.parse.FunctionParseNode;
import org.apache.phoenix.schema.IllegalDataException;
import org.apache.phoenix.schema.tuple.Tuple;
import org.apache.phoenix.schema.types.PDataType;
import org.apache.phoenix.schema.types.PVarchar;
import org.apache.phoenix.schema.types.PVarbinary;

import static org.apache.hadoop.hbase.util.Bytes.toHex;

/**
 * Encodes binary data into a string to various formats such as Hex, Base64, or HBase binary.
 */
@FunctionParseNode.BuiltInFunction(name = EncodeBinaryFunction.NAME, args = {
    @FunctionParseNode.Argument(allowedTypes = {PVarbinary.class}),
    @FunctionParseNode.Argument(enumeration = "EncodeFormat")})
public class EncodeBinaryFunction extends ScalarFunction {

    public static final String NAME = "ENCODE_BINARY";

    public EncodeBinaryFunction() {
    }

    public EncodeBinaryFunction(List<Expression> children) throws SQLException {
        super(children);
    }

    @Override
    public boolean evaluate(Tuple tuple, ImmutableBytesWritable ptr) {
        Expression expression = getExpression();
        if (!expression.evaluate(tuple, ptr)) {
            return false;
        }
        if (ptr.getLength() == 0) {
            return true;
        }

        byte[] bytesToEncode = ptr.copyBytes();
        Expression encodingExpression = getEncodingExpression();

        if (!encodingExpression.evaluate(tuple, ptr)) {
            return false;
        }

        if (ptr.getLength() == 0) {
            throw new IllegalDataException(getMissingEncodeFormatMsg());
        }

        PDataType type = encodingExpression.getDataType();
        String encodingFormat = ((String) type.toObject(ptr)).toUpperCase();
        EncodeFormat format = EncodeFormat.valueOf(encodingFormat);
        String encodedString;

        switch (format) {
        case HEX:
            encodedString = toHex(bytesToEncode);
            break;
        case BASE64:
            encodedString = Base64.getEncoder().encodeToString(bytesToEncode);
            break;
        case HBASE:
            encodedString = Bytes.toStringBinary(bytesToEncode);
            break;
        default:
            throw new IllegalDataException(getUnsupportedEncodeFormatMsg(encodingFormat));
        }

        ptr.set(PVarchar.INSTANCE.toBytes(encodedString));
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

    private Expression getExpression() {
        return children.get(0);
    }

    private Expression getEncodingExpression() {
        return children.get(1);
    }

}

