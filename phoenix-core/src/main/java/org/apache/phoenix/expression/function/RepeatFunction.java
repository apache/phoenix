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

@BuiltInFunction(name=RepeatFunction.NAME, args={
        @Argument(allowedTypes={ PVarchar.class }),
        @Argument(allowedTypes={ PInteger.class })})
public class RepeatFunction extends ScalarFunction{
    
    public static final String NAME = "REPEAT";
    
    private int repeatNbr = 0;
    
    public RepeatFunction() { }
    
    public RepeatFunction(List<Expression> children) {
        super(children);
        init();
    }
    
    private void init() {
        Expression repeatIntExpression = getChildren().get(1);
        if (repeatIntExpression instanceof LiteralExpression) {
            Object repeatNbrValue = ((LiteralExpression) repeatIntExpression).getValue();
            if (repeatNbrValue != null) {
                this.setRepeatNbr((int) repeatNbrValue);
            }
        }
    }
        
    
    @Override
    public boolean evaluate(Tuple tuple, ImmutableBytesWritable ptr) {
        Expression child = getChildren().get(0);
        
        if (!child.evaluate(tuple, ptr)) {
            return false;
        }
        
        if (ptr.getLength() == 0 || repeatNbr < 1) {
            ptr.set(ByteUtil.EMPTY_BYTE_ARRAY);
            return true;
        }
        
        int strLength = ptr.getLength();
        
        String sourceStr = (String) PVarchar.INSTANCE.toObject(ptr, getChildren().get(0).getSortOrder());
        byte[] sourceBytes = sourceStr.getBytes();
        byte[] target = new byte[strLength*repeatNbr];
        int targetOffset = 0;
        
        for (int i =0; i < strLength*repeatNbr; i+=strLength){
            System.arraycopy(sourceBytes, 0, target, targetOffset, strLength);
            targetOffset+=strLength;
        }
        
        ptr.set(target);
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
    
    @Override
    public void readFields(DataInput input) throws IOException {
        super.readFields(input);
        init();
    }
    
    @Override
    public OrderPreserving preservesOrder() {
        Expression repeatIntExpression = getChildren().get(1);
        if (repeatIntExpression instanceof LiteralExpression) {
            Object repeatNbrValue = ((LiteralExpression) repeatIntExpression).getValue();
            if (repeatNbrValue != null && ((int) repeatNbrValue) > 0) {
                return OrderPreserving.YES;
            }
        }
        return OrderPreserving.NO;
        
    }
    
    public int getRepeatNbr() {
        return repeatNbr;
    }

    public void setRepeatNbr(int repeatNbr) {
        this.repeatNbr = repeatNbr;
    }
}
