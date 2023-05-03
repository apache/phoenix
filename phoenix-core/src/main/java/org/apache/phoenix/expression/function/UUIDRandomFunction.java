/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to you under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
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

import java.util.List;
import java.util.UUID;

import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.phoenix.expression.Determinism;
import org.apache.phoenix.expression.Expression;
import org.apache.phoenix.parse.FunctionParseNode.BuiltInFunction;
import org.apache.phoenix.schema.tuple.Tuple;
import org.apache.phoenix.schema.types.PBinary;
import org.apache.phoenix.schema.types.PDataType;
import org.apache.phoenix.schema.types.PUUID;
import org.apache.phoenix.util.UUIDUtil;

/**
 * ScalarFunction {@link ScalarFunction}. returns  a random UUID.
 * for example:
 * SELECT UUID_RAND();
 * Related to {@link StringToUUIDFunction} and {@link UUIDToStringFunction} .
 */
@BuiltInFunction(name = UUIDRandomFunction.NAME, args = {})
public class UUIDRandomFunction extends ScalarFunction {
    public static final String NAME = "UUID_RAND";

    public UUIDRandomFunction() {
    }

    public UUIDRandomFunction(List<Expression> children) {
        super(children);

    }

    @Override
    public boolean evaluate(Tuple tuple, ImmutableBytesWritable ptr) {
        ptr.set(PBinary.INSTANCE.toBytes(UUIDUtil.getBytesFromUUID(UUID.randomUUID())));
        return true;
    }

    @Override
    public PDataType<?> getDataType() {
        return PUUID.INSTANCE;
    }

    @Override
    public String getName() {
        return NAME;
    }

    @Override
    public Determinism getDeterminism() {
        return Determinism.PER_INVOCATION;
    }

    @Override
    public boolean isStateless() {
        return true;
    }

    // // take the random object onto account
    // @Override
    // public int hashCode() {
    // int hashCode = super.hashCode();
    // return hasSeed ? hashCode : (hashCode + random.hashCode());
    // }
    //
    // // take the random object onto account, as otherwise we'll potentially collapse two
    // // RAND() calls into a single one.
    // @Override
    // public boolean equals(Object obj) {
    // return super.equals(obj) && (hasSeed || random.equals(((UUIDRandomFunction)obj).random));
    // }

    // make sure we do not show the default 'null' parameter
    @Override
    public final String toString() {
        return getName() + "()";

    }

}
