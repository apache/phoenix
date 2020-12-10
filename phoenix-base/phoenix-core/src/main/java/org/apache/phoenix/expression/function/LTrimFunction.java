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

import java.sql.SQLException;
import java.util.List;

import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.phoenix.expression.Expression;
import org.apache.phoenix.parse.FunctionParseNode.Argument;
import org.apache.phoenix.parse.FunctionParseNode.BuiltInFunction;
import org.apache.phoenix.schema.types.PDataType;
import org.apache.phoenix.schema.types.PVarchar;
import org.apache.phoenix.schema.SortOrder;
import org.apache.phoenix.schema.tuple.Tuple;
import org.apache.phoenix.util.ByteUtil;
import org.apache.phoenix.util.StringUtil;


/**
 * 
 * Implementation of the LTrim(<string>) build-in function. It removes from the left end of
 * <string> space character and other function bytes in single byte utf8 characters 
 * set.
 * 
 * 
 * @since 0.1
 */
@BuiltInFunction(name=LTrimFunction.NAME, args={
    @Argument(allowedTypes={PVarchar.class})})
public class LTrimFunction extends ScalarFunction {
    public static final String NAME = "LTRIM";

    public LTrimFunction() { }

    public LTrimFunction(List<Expression> children) throws SQLException {
        super(children);
    }

    private Expression getStringExpression() {
        return children.get(0);
    }

    @Override
    public SortOrder getSortOrder() {
        return children.get(0).getSortOrder();
    }    

    @Override
    public boolean evaluate(Tuple tuple, ImmutableBytesWritable ptr) {
        // Starting from the front of the byte, look for all single bytes at the end of the string
        // that is below SPACE_UTF8 (space and control characters) or 0x7f (control chars).
        if (!getStringExpression().evaluate(tuple, ptr)) {
            return false;
        }
        
        if (ptr.getLength() == 0) {
            ptr.set(ByteUtil.EMPTY_BYTE_ARRAY);
            return true;
        }
        byte[] string = ptr.get();
        int offset = ptr.getOffset();
        int length = ptr.getLength();
        
        SortOrder sortOrder = getStringExpression().getSortOrder();
        int i = StringUtil.getFirstNonBlankCharIdxFromStart(string, offset, length, sortOrder);
        if (i == offset + length) {
            ptr.set(ByteUtil.EMPTY_BYTE_ARRAY);
            return true;
        }
        
        ptr.set(string, i, offset + length - i);
        return true;
    }
    
    @Override
    public Integer getMaxLength() {
        return getStringExpression().getMaxLength();
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
