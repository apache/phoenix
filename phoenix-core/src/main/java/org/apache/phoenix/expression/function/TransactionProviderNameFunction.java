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
import org.apache.phoenix.schema.tuple.Tuple;
import org.apache.phoenix.schema.types.PDataType;
import org.apache.phoenix.schema.types.PInteger;
import org.apache.phoenix.schema.types.PTinyint;
import org.apache.phoenix.schema.types.PVarchar;
import org.apache.phoenix.transaction.TransactionFactory;


/**
 * 
 * Function used to get the index state name from the serialized byte value
 * Usage:
 * IndexStateName('a')
 * will return 'ACTIVE'
 * 
 * 
 * @since 2.1
 */
@BuiltInFunction(name=TransactionProviderNameFunction.NAME, args= {
    @Argument(allowedTypes= PInteger.class)} )
public class TransactionProviderNameFunction extends ScalarFunction {
    public static final String NAME = "TransactionProviderName";

    public TransactionProviderNameFunction() {
    }
    
    public TransactionProviderNameFunction(List<Expression> children) throws SQLException {
        super(children);
    }
    
    @Override
    public boolean evaluate(Tuple tuple, ImmutableBytesWritable ptr) {
        Expression child = children.get(0);
        if (!child.evaluate(tuple, ptr)) {
            return false;
        }
        if (ptr.getLength() == 0) {
            return true;
        }
        int code = PTinyint.INSTANCE.getCodec().decodeByte(ptr, child.getSortOrder());
        TransactionFactory.Provider provider = TransactionFactory.Provider.fromCode(code);
        ptr.set(PVarchar.INSTANCE.toBytes(provider.name()));
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
