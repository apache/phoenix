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

import java.util.Arrays;
import java.util.List;

import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.phoenix.expression.Expression;
import org.apache.phoenix.expression.LiteralExpression;
import org.apache.phoenix.parse.FunctionParseNode.Argument;
import org.apache.phoenix.parse.FunctionParseNode.BuiltInFunction;
import org.apache.phoenix.schema.SortOrder;
import org.apache.phoenix.schema.tuple.Tuple;
import org.apache.phoenix.schema.types.PDataType;
import org.apache.phoenix.schema.types.PInteger;
import org.apache.phoenix.schema.types.PVarchar;
import org.apache.phoenix.util.StringUtil;

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
        Expression strToSearchExpression = children.get(1);
        if (strToSearchExpression instanceof LiteralExpression) {
            Object strToSearchValue = ((LiteralExpression) strToSearchExpression).getValue();
            if (strToSearchValue != null) {
                this.strToSearch = strToSearchValue.toString();
            }
        }
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

        int position;
        //Logic for Empty string search
        if (strToSearch == null){
            position = 0;
            ptr.set(PInteger.INSTANCE.toBytes(position));
            return true;
        }
        
        byte [] strToSearchBytes = strToSearch.getBytes();
        ImmutableBytesWritable strImmutableBytes = new ImmutableBytesWritable(strToSearchBytes);
        SortOrder sortOrder = child.getSortOrder();
        byte [] strPattern = strModifiedOnOrder(strToSearchBytes,strImmutableBytes.getOffset(),sortOrder);
        
        ImmutableBytesWritable str = new ImmutableBytesWritable(strPattern);
        
        position = searchString(ptr,str,sortOrder,strPattern);
        
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
    public SortOrder getSortOrder() {
        return getChildren().get(0).getSortOrder();
    }
    
    
    /*Method to return a byte array based on the SortOrder of the string. The returned inverted byte array will
      be used for comparing with the string for DESC SortOrder scenarios
    */
    private byte [] strModifiedOnOrder(byte [] strToSearchBytes, int srcOffset, SortOrder sortOrder){
        if (sortOrder==SortOrder.DESC){
            return SortOrder.invert(strToSearchBytes, srcOffset, strToSearchBytes.length);
        }
        
        return strToSearchBytes;
    }
    //Method is to Search the Pattern in the given String
    private int searchString(ImmutableBytesWritable ptr, ImmutableBytesWritable strbytes,
                             SortOrder sortOrder, byte [] strPattern) {
        
        int textOffset = ptr.getOffset();
        int textLength = ptr.getLength();
        int strLength  = strbytes.getLength();
        byte [] text = ptr.get();
        byte[] target = new byte[strLength];
        int position = 0;
        
        //Logic to compare Byte arrays
        while ((textLength - textOffset) > 0){
            int nBytes = StringUtil.getBytesInChar(text[textOffset], sortOrder);
            
            //Checking for the boundary condition
            if (textLength - textOffset < strLength ){
                return -1;
            }
            
            System.arraycopy(text, textOffset, target, 0, strLength);
            textOffset += nBytes;
            position++;
            if (Arrays.equals(strPattern, target)){
                System.out.println("Position value at the end:"+ position);
                return position - 1;
            }
        }
        
        return -1;
    }
}
