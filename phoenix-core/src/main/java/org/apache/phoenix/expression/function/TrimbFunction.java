/*
 * Licensed to the Apache Software Foundation (ASF) under one or more contributor license
 * agreements. See the NOTICE file distributed with this work for additional information regarding
 * copyright ownership. The ASF licenses this file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License. You may obtain a
 * copy of the License at http://www.apache.org/licenses/LICENSE-2.0 Unless required by applicable
 * law or agreed to in writing, software distributed under the License is distributed on an "AS IS"
 * BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License
 * for the specific language governing permissions and limitations under the License.
 */
package org.apache.phoenix.expression.function;

import java.sql.SQLException;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.phoenix.expression.Expression;
import org.apache.phoenix.parse.FunctionParseNode.Argument;
import org.apache.phoenix.parse.FunctionParseNode.BuiltInFunction;
import org.apache.phoenix.schema.tuple.Tuple;
import org.apache.phoenix.schema.types.PBinary;
import org.apache.phoenix.schema.types.PDataType;
import org.apache.phoenix.schema.types.PVarbinary;
import org.apache.phoenix.util.ByteUtil;
import org.apache.phoenix.util.StringUtil;

@BuiltInFunction(name = TrimbFunction.NAME,
        args = { @Argument(allowedTypes = { PBinary.class, PVarbinary.class }),
                @Argument(allowedTypes = { PVarbinary.class }) })
public class TrimbFunction extends ScalarFunction {
    public static final String NAME = "TRIMB";

    public TrimbFunction() {
    }

    public TrimbFunction(List<Expression> children) throws SQLException {
        super(children);
    }

    @Override
    public boolean evaluate(Tuple tuple, ImmutableBytesWritable ptr) {

        //Get trim bytes parameter
        if (!getTrimBytesExpression().evaluate(tuple, ptr)) return false;
        if (ptr.getLength() == 0) return true;
        byte[] trimBytes = (byte[]) getTrimBytesExpression().getDataType()
                        .toObject(ptr, getTrimBytesExpression().getSortOrder());

        //Get data bytes parameter
        if (!getDataBytesExpression().evaluate(tuple, ptr)) return false;
        int length = ptr.getLength();
        if (length == 0) {
            ptr.set(ByteUtil.EMPTY_BYTE_ARRAY);
            return true;
        }

        byte[] dataBytes = (byte[]) getDataBytesExpression().getDataType()
                        .toObject(ptr, getDataBytesExpression().getSortOrder());

        Set<Byte> trimBytesSet = new HashSet<Byte>(trimBytes.length);
        for (byte bVal : trimBytes) {
            trimBytesSet.add(bVal);
        }

        int start = 0;
        int end = length - 1;

        while (start <= end) {
            if (isSingleByteChar(dataBytes[start])) {
                if (trimBytesSet.contains(dataBytes[start])) start++;
                else break;
            } else {
                int nBytes = StringUtil.getBytesInChar(dataBytes[start],
                                getDataBytesExpression().getSortOrder());
                boolean exists = true;
                int startRunner = start;
                while(exists && (startRunner < start + nBytes)) {
                    exists &= trimBytesSet.contains(dataBytes[startRunner]);
                    startRunner++;
                }
                if (exists) start += nBytes;
                else break;
            }
        }

        if (start == end + 1) {
            ptr.set(ByteUtil.EMPTY_BYTE_ARRAY);
            return true;
        }

        while (end > start) {
            if (isSingleByteChar(dataBytes[end])) {
                if (trimBytesSet.contains(dataBytes[end])) end--;
                else break;
            } else {
                int endRunner = end;
                boolean exists = true;
                while (exists && isContinuousByte(dataBytes[endRunner])) {
                    exists &= trimBytesSet.contains(dataBytes[endRunner]);
                    endRunner--;
                }
                exists &= trimBytesSet.contains(dataBytes[endRunner]);
                if (exists) end = endRunner - 1;
                else break;
            }
        }

        ptr.set(dataBytes, start, end - start + 1);
        return true;
    }

    private boolean isSingleByteChar(byte b) {
        return ((b & 0xff) >> 7) == 0x0;
    }

    private boolean isContinuousByte(byte b) {
        return ((b & 0xff) >> 6) == 0x2;
    }

    @Override
    public PDataType getDataType() {
        return getDataBytesExpression().getDataType().isFixedWidth() ? getDataBytesExpression().getDataType()
                : PVarbinary.INSTANCE;
    }

    @Override
    public String getName() {
        return NAME;
    }

    private Expression getDataBytesExpression() {
        return children.get(0);
    }

    private Expression getTrimBytesExpression() {
        return children.get(1);
    }

    @Override
    public OrderPreserving preservesOrder() {
        return OrderPreserving.YES_IF_LAST;
    }

}
