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
import org.apache.phoenix.schema.tuple.Tuple;
import org.apache.phoenix.schema.types.PDataType;
import org.apache.phoenix.schema.types.PUUID;
import org.apache.phoenix.schema.types.PVarchar;
import org.apache.phoenix.util.UUIDUtil;

/**
 * ScalarFunction {@link ScalarFunction}. Receives a UUID parameter and returns a String.
 * for example:
 * SELECT UUID_TO_STR(UUID_RAND());
 * inverse function to {@link StringToUUIDFunction}.
 * Related to {@link UUIDRandomFunction}.
 */
@BuiltInFunction(name = UUIDToStringFunction.NAME,
        args = { @Argument(allowedTypes = { PUUID.class }) })
public class UUIDToStringFunction extends ScalarFunction {
    public static final String NAME = "UUID_TO_STR";

    public UUIDToStringFunction() {
    }

    public UUIDToStringFunction(List<Expression> children) throws SQLException {
        super(children);
    }

    @Override
    public boolean evaluate(Tuple tuple, ImmutableBytesWritable ptr) {
        Expression childExpr = children.get(0);
        if (!childExpr.evaluate(tuple, ptr)) {
            return false;
        }
        if (ptr.getLength() != UUIDUtil.UUID_BYTES_LENGTH) {
            return false;
        }
        byte[] uuidBytes = ptr.get();

        UUID uuid = UUIDUtil.getUUIDFromBytes(uuidBytes, ptr.getOffset(), childExpr.getSortOrder());

        ptr.set(uuid.toString().getBytes(Charset.forName("UTF-8")));

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

}
