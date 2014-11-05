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
import org.apache.phoenix.expression.CurrentDateTimeFunction;
import org.apache.phoenix.parse.CurrentDateParseNode;
import org.apache.phoenix.parse.FunctionParseNode.BuiltInFunction;
import org.apache.phoenix.schema.PDataType;
import org.apache.phoenix.schema.tuple.Tuple;

import java.util.UUID;


/**
 *
 * Function that returns a random UUID using java.util.UUID.randomUUID().
 * The UUID is formatted into a string format "xxxxxxxx-xxxx-xxxx-xxxx-xxxxxxxxxxxx".
 *
 *
 * @since 0.1
 */
@BuiltInFunction(name= UuidGenFunction.NAME, nodeClass=CurrentDateParseNode.class, args= {} )
public class UuidGenFunction extends CurrentDateTimeFunction {
    public static final String NAME = "UUID_GEN";

    public UuidGenFunction() {
    }

    @Override
    public boolean evaluate(Tuple tuple, ImmutableBytesWritable ptr) {
        String uuidString = UUID.randomUUID().toString();
        ptr.set(PDataType.VARCHAR.toBytes(uuidString));
        return true;
    }

    @Override
    public PDataType getDataType() {
        return PDataType.VARCHAR;
    }

    @Override
    public String getName() {
        return NAME;
    }
}
