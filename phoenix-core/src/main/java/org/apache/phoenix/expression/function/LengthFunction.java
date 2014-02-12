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

import java.io.UnsupportedEncodingException;
import java.sql.SQLException;
import java.util.List;

import org.apache.hadoop.hbase.io.ImmutableBytesWritable;

import org.apache.phoenix.expression.Expression;
import org.apache.phoenix.parse.FunctionParseNode.Argument;
import org.apache.phoenix.parse.FunctionParseNode.BuiltInFunction;
import org.apache.phoenix.schema.PDataType;
import org.apache.phoenix.schema.tuple.Tuple;
import org.apache.phoenix.util.ByteUtil;
import org.apache.phoenix.util.StringUtil;


/**
 * 
 * Implementation of the LENGTH(<string>) build-in function. <string> is the string
 * of characters we want to find the length of. If <string> is NULL or empty, null
 * is returned.
 * 
 * 
 * @since 0.1
 */
@BuiltInFunction(name=LengthFunction.NAME, args={
    @Argument(allowedTypes={PDataType.VARCHAR})} )
public class LengthFunction extends ScalarFunction {
    public static final String NAME = "LENGTH";

    public LengthFunction() { }

    public LengthFunction(List<Expression> children) throws SQLException {
        super(children);
    }

    private Expression getStringExpression() {
        return children.get(0);
    }

    @Override
    public boolean evaluate(Tuple tuple, ImmutableBytesWritable ptr) {
        Expression child = getStringExpression();
        if (!child.evaluate(tuple, ptr)) {
            return false;
        }
        if (ptr.getLength() == 0) {
            ptr.set(ByteUtil.EMPTY_BYTE_ARRAY);
            return true;
        }
        int len;
        if (child.getDataType() == PDataType.CHAR) {
            // Only single-byte characters allowed in CHAR
            len = ptr.getLength();
        } else {
            try {
                len = StringUtil.calculateUTF8Length(ptr.get(), ptr.getOffset(), ptr.getLength(), child.getSortOrder());
            } catch (UnsupportedEncodingException e) {
                return false;
            }
        }
        ptr.set(PDataType.INTEGER.toBytes(len));
        return true;
    }

    @Override
    public PDataType getDataType() {
        return PDataType.INTEGER;
    }

    @Override
    public String getName() {
        return NAME;
    }

}
