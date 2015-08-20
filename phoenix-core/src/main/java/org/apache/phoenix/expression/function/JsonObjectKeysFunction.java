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

import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.phoenix.compile.KeyPart;
import org.apache.phoenix.expression.Expression;
import org.apache.phoenix.parse.FunctionParseNode.Argument;
import org.apache.phoenix.parse.FunctionParseNode.BuiltInFunction;
import org.apache.phoenix.schema.json.PhoenixJson;
import org.apache.phoenix.schema.tuple.Tuple;
import org.apache.phoenix.schema.types.*;
import org.apache.phoenix.util.ByteUtil;

import java.util.List;

@BuiltInFunction(name = JsonObjectKeysFunction.NAME, args = {
        @Argument(allowedTypes = { PJson.class })})
public class JsonObjectKeysFunction extends ScalarFunction {
    public static final String NAME = "JSON_OBJECT_KEYS";

    public JsonObjectKeysFunction() {
        super();
    }

    public JsonObjectKeysFunction(List<Expression> children) {
        super(children);
    }

    @Override
    public boolean evaluate(Tuple tuple, ImmutableBytesWritable ptr) {

        Expression jsonExpression = this.children.get(0);
        if (!jsonExpression.evaluate(tuple, ptr)) {
            return false;
        }else if (ptr.getLength() == 0) {
            return true;
        }
        PhoenixJson phoenixJson =
                (PhoenixJson) PJson.INSTANCE.toObject(ptr.get(), ptr.getOffset(),
                        ptr.getLength());
        Object[] elements = phoenixJson.getJsonObjectKeys();
        if(elements == null || elements.length == 0){
            ptr.set(ByteUtil.EMPTY_BYTE_ARRAY);
        }else{
            PhoenixArray pa = PArrayDataType.instantiatePhoenixArray(PVarchar.INSTANCE, elements);
            byte[] array = PVarcharArray.INSTANCE.toBytes(pa);
            ptr.set(array);
        }

        return true;
    }

    @SuppressWarnings("rawtypes")
    @Override
    public PDataType getDataType() {
        return PVarcharArray.INSTANCE;
    }

    @Override
    public String getName() {
        return NAME;
    }

    @Override
    public boolean isNullable() {
        return PVarcharArray.INSTANCE.isNullable();
    }

    @Override
    public int getKeyFormationTraversalIndex() {
        return NO_TRAVERSAL;
    }

    @Override
    public KeyPart newKeyPart(KeyPart childPart) {
        return null;
    }

    @Override
    public OrderPreserving preservesOrder() {
        return OrderPreserving.NO;
    }

}
