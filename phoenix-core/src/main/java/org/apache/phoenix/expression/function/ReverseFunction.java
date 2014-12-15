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
import org.apache.phoenix.schema.types.PVarchar;
import org.apache.phoenix.schema.SortOrder;
import org.apache.phoenix.schema.types.PDataType;
import org.apache.phoenix.schema.tuple.Tuple;
import org.apache.phoenix.util.StringUtil;

@BuiltInFunction(name=ReverseFunction.NAME,  args={
        @Argument(allowedTypes={PVarchar.class})} )
public class ReverseFunction extends ScalarFunction {
    public static final String NAME = "REVERSE";
    
    public ReverseFunction() {
    }

    public ReverseFunction(List<Expression> children) throws SQLException {
        super(children);
    }

    @Override
    public boolean evaluate(Tuple tuple, ImmutableBytesWritable ptr) {
        Expression arg = getChildren().get(0);
        if (!arg.evaluate(tuple, ptr)) {
            return false;
        }

        int targetOffset = ptr.getLength();
        if (targetOffset == 0) {
            return true;
        }

        byte[] source = ptr.get();
        byte[] target = new byte[targetOffset];
        int sourceOffset = ptr.getOffset(); 
        int endOffset = sourceOffset + ptr.getLength();
        SortOrder sortOrder = arg.getSortOrder();
        while (sourceOffset < endOffset) {
            int nBytes = StringUtil.getBytesInChar(source[sourceOffset], sortOrder);
            targetOffset -= nBytes;
            System.arraycopy(source, sourceOffset, target, targetOffset, nBytes);
            sourceOffset += nBytes;
        }
        ptr.set(target);
        return true;
    }

    @Override
    public SortOrder getSortOrder() {
        return getChildren().get(0).getSortOrder();
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
