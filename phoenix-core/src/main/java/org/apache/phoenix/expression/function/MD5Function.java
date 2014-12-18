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

import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.sql.SQLException;
import java.util.List;

import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.phoenix.expression.Expression;
import org.apache.phoenix.parse.FunctionParseNode.Argument;
import org.apache.phoenix.parse.FunctionParseNode.BuiltInFunction;
import org.apache.phoenix.schema.types.PBinary;
import org.apache.phoenix.schema.types.PDataType;
import org.apache.phoenix.schema.tuple.Tuple;

@BuiltInFunction(name = MD5Function.NAME, args = { @Argument() })
public class MD5Function extends ScalarFunction {
    public static final String NAME = "MD5";
    public static final Integer LENGTH = 16;

    private final MessageDigest messageDigest;

    public MD5Function() throws SQLException {
        try {
            messageDigest = MessageDigest.getInstance("MD5");
        } catch (NoSuchAlgorithmException e) {
            throw new SQLException(e);
        }
    }

    public MD5Function(List<Expression> children) throws SQLException {
        super(children);
        try {
            messageDigest = MessageDigest.getInstance("MD5");
        } catch (NoSuchAlgorithmException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public boolean evaluate(Tuple tuple, ImmutableBytesWritable ptr) {
        if (!getChildExpression().evaluate(tuple, ptr)) { return false; }

        // Update the digest value
        messageDigest.update(ptr.get(), ptr.getOffset(), ptr.getLength());
        // Get the digest bytes (note this resets the messageDigest as well)
        ptr.set(messageDigest.digest());
        return true;
    }

    @Override
    public PDataType getDataType() {
        return PBinary.INSTANCE;
    }

    @Override
    public Integer getMaxLength() {
        return LENGTH;
    }

    @Override
    public boolean isNullable() {
        return getChildExpression().isNullable();
    }

    @Override
    public String getName() {
        return NAME;
    }

    private Expression getChildExpression() {
        return children.get(0);
    }
}
