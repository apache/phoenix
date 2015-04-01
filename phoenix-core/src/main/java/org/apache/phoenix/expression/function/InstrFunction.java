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
import org.apache.phoenix.util.ByteUtil;

@BuiltInFunction(name=InstrFunction.NAME, args={
        @Argument(allowedTypes={ PVarchar.class }),
        @Argument(allowedTypes={ PVarchar.class })})
public class InstrFunction extends ScalarFunction{
    
    public static final String NAME = "INSTR";
    
    private String strToSearch = null;
    
    public InstrFunction() { }
    
    public InstrFunction(List<Expression> children) {
        super(children);
        init();
    }
    
    private void init() {
        Expression strToSearchExpression = getChildren().get(1);
        if (strToSearchExpression instanceof LiteralExpression) {
            Object strToSearchValue = ((LiteralExpression) strToSearchExpression).getValue();
            if (strToSearchValue != null) {
                this.strToSearch = strToSearchValue.toString();
            }
        }
    }
        
    
    @Override
    public boolean evaluate(Tuple tuple, ImmutableBytesWritable ptr) {
        Expression child = getChildren().get(0);
        
        if (!child.evaluate(tuple, ptr)) {
            return false;
        }
        
        if (ptr.getLength() == 0) {
            ptr.set(ByteUtil.EMPTY_BYTE_ARRAY);
            return true;
        }
        
        int position;
        //Logic for Empty string search
        if (strToSearch == null){
            position = 0;
            ptr.set(PInteger.INSTANCE.toBytes(position));
            return true;
        }
        
        String sourceStr = (String) PVarchar.INSTANCE.toObject(ptr, getChildren().get(0).getSortOrder());

        position = sourceStr.indexOf(strToSearch);
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
