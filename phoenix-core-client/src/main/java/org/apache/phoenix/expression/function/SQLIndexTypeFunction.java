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
import org.apache.phoenix.schema.PTable.IndexType;
import org.apache.phoenix.schema.types.PUnsignedTinyint;
import org.apache.phoenix.schema.types.PVarchar;
import org.apache.phoenix.schema.tuple.Tuple;


/**
 * 
 * Function used to get the SQL view type name from the serialized view type.
 * Usage:
 * SQLViewType('v') will return 'VIEW' based on
 * {@link java.sql.DatabaseMetaData#getTableTypes()}
 * 
 * 
 * @since 2.2
 */
@BuiltInFunction(name=SQLIndexTypeFunction.NAME, args= {
    @Argument(allowedTypes= PUnsignedTinyint.class)} )
public class SQLIndexTypeFunction extends ScalarFunction {
    public static final String NAME = "SQLIndexType";

    public SQLIndexTypeFunction() {
    }
    
    public SQLIndexTypeFunction(List<Expression> children) throws SQLException {
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
        IndexType viewType = IndexType.fromSerializedValue(ptr.get()[ptr.getOffset()]);
        ptr.set(viewType.getBytes());
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
