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

import java.io.DataInput;
import java.io.IOException;
import java.util.List;

import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.phoenix.expression.Expression;
import org.apache.phoenix.expression.LiteralExpression;
import org.apache.phoenix.parse.FunctionParseNode.Argument;
import org.apache.phoenix.parse.FunctionParseNode.BuiltInFunction;
import org.apache.phoenix.schema.tuple.Tuple;
import org.apache.phoenix.schema.types.PDataType;
import org.apache.phoenix.schema.types.PInteger;
import org.apache.phoenix.schema.types.PVarchar;

@BuiltInFunction(name=InstrFunction.NAME, args={
        @Argument(allowedTypes={ PVarchar.class }),
        @Argument(allowedTypes={ PVarchar.class })})
public class InstrFunction extends ScalarFunction{
    
    public static final String NAME = "INSTR";

    private String literalSourceStr = null;
    private String literalSearchStr = null;
    
    public InstrFunction() { }
    
    public InstrFunction(List<Expression> children) {
        super(children);
        init();
    }
    
    private void init() {
        literalSourceStr = maybeExtractLiteralString(getChildren().get(0));
        literalSearchStr = maybeExtractLiteralString(getChildren().get(1));
    }

    /**
     * Extracts the string-representation of {@code expr} only if {@code expr} is a
     * non-null {@link LiteralExpression}.
     *
     * @param expr An Expression.
     * @return The string value for the expression or null
     */
    private String maybeExtractLiteralString(Expression expr) {
        if (expr instanceof LiteralExpression) {
            // Whether the value is null or non-null, we can give it back right away
            return (String) ((LiteralExpression) expr).getValue();
        }
        return null;
    }
        
    
    @Override
    public boolean evaluate(Tuple tuple, ImmutableBytesWritable ptr) {
        String sourceStr = literalSourceStr;
        if (sourceStr == null) {
            Expression child = getChildren().get(0);

            if (!child.evaluate(tuple, ptr)) {
                return false;
            }

            // We need something non-empty to search against
            if (ptr.getLength() == 0) {
              return true;
            }

            sourceStr = (String) PVarchar.INSTANCE.toObject(ptr, child.getSortOrder());
        }

        String searchStr = literalSearchStr;
        // A literal was not provided, try to evaluate the expression to a literal
        if (searchStr == null){
            Expression child = getChildren().get(1);

            if (!child.evaluate(tuple, ptr)) {
              return false;
            }

            // A null (or zero-length) search string
            if (ptr.getLength() == 0) {
              return true;
            }
            
            searchStr = (String) PVarchar.INSTANCE.toObject(ptr, child.getSortOrder());
        }

        int position = sourceStr.indexOf(searchStr) + 1;
        ptr.set(PInteger.INSTANCE.toBytes(position));
        return true;
    }

    @Override
    public PDataType getDataType() {
        return PInteger.INSTANCE;
    }

    @Override
    public String getName() {
        return NAME;
    }
    
    @Override
    public void readFields(DataInput input) throws IOException {
        super.readFields(input);
        init();
    }
}
