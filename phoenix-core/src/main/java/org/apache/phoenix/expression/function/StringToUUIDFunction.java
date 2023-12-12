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

import java.nio.charset.Charset;
import java.sql.SQLException;
import java.util.List;
import java.util.UUID;

import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.phoenix.expression.Expression;
import org.apache.phoenix.parse.FunctionParseNode.Argument;
import org.apache.phoenix.parse.FunctionParseNode.BuiltInFunction;
import org.apache.phoenix.schema.SortOrder;
import org.apache.phoenix.schema.tuple.Tuple;
import org.apache.phoenix.schema.types.PBinary;
import org.apache.phoenix.schema.types.PDataType;
import org.apache.phoenix.schema.types.PUUID;
import org.apache.phoenix.schema.types.PVarchar;
import org.apache.phoenix.util.UUIDUtil;

/**
 * ScalarFunction {@link ScalarFunction}. Receives a String parameter and returns a UUID.
 * for example:
 * SELECT STR_TO_UUID('be791257-6764-45a5-9719-cc50bf7938e4');
 * inverse function to {@link UUIDToStringFunction}
 * Related to {@link UUIDRandomFunction}.
 */
@BuiltInFunction(name = StringToUUIDFunction.NAME,
        args = { @Argument(allowedTypes = { PVarchar.class }) })
public class StringToUUIDFunction extends ScalarFunction {
    public static final String NAME = "STR_TO_UUID";

    // "00000000-0000-0000-0000-000000000000".length() == 36
    private static final int UUID_TEXTUAL_LENGTH = 36;


    public StringToUUIDFunction() {
    }

    public StringToUUIDFunction(List<Expression> children) throws SQLException {
        super(children);
    }

    private Expression getStringExpression() {
        return children.get(0);
    }

    @Override
    public boolean evaluate(Tuple tuple, ImmutableBytesWritable ptr) {
        if (!getStringExpression().evaluate(tuple, ptr)) {
            return false;
        }

        if (ptr.getLength() != UUID_TEXTUAL_LENGTH) {
            throw new IllegalArgumentException("Invalid UUID string: " + new String(ptr.get(),
                    ptr.getOffset(), ptr.getLength(), Charset.forName("UTF-8")));
        }

        String value;

        // Next is done because, if not,"org.apache.phoenix.expression.function.UUIDFunctionTest",
        // test "testUUIDFuntions" fails when
        // "testStringToUUIDfunction(uuidStr, null, SortOrder.DESC)"
        //  is used (maybe stupid or bad designed test?).
        //
        // But I think that in real world it is not necessary.
        //
        // without check 'ptr.get()[ptr.getOffset() + 8] == '-'' , just only with:
        // 'value = new String(ptr.get(), ptr.getOffset(), 36);' other tests works fine.
        // but placed because not sure (may be removed?).

        if (ptr.get()[ptr.getOffset() + 8] == '-') {
            value =
                    new String(ptr.get(), ptr.getOffset(), UUID_TEXTUAL_LENGTH,
                            Charset.forName("UTF-8"));
        } else {
            byte[] temp =
                    SortOrder.invert(ptr.get(), ptr.getOffset(), new byte[36], 0,
                        UUID_TEXTUAL_LENGTH);
            value = new String(temp, Charset.forName("UTF-8"));
        }

        ptr.set(PBinary.INSTANCE.toBytes(UUIDUtil.getBytesFromUUID(UUID.fromString(value))));
        return true;
    }

    @Override
    public PDataType<?> getDataType() {
        return PUUID.INSTANCE;
    }

    @Override
    public String getName() {
        return NAME;
    }

}
