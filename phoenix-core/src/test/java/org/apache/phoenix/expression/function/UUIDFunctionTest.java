/*
 * Licensed to the Apache Software Foundation (ASF) under one or more contributor license
 * agreements. See the NOTICE file distributed with this work for additional information regarding
 * copyright ownership. The ASF licenses this file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License. You may obtain a
 * copy of the License at http://www.apache.org/licenses/LICENSE-2.0 Unless required by applicable
 * law or agreed to in writing, software distributed under the License is distributed on an "AS IS"
 * BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License
 * for the specific language governing permissions and limitations under the License.
 */
package org.apache.phoenix.expression.function;

import static org.junit.Assert.assertTrue;

import java.util.List;
import java.util.UUID;

import org.apache.hadoop.hbase.io.ImmutableBytesWritable;

import org.apache.phoenix.expression.Expression;
import org.apache.phoenix.expression.LiteralExpression;
import org.apache.phoenix.schema.SortOrder;
import org.apache.phoenix.schema.types.PUUID;
import org.apache.phoenix.schema.types.PVarchar;
import org.junit.Test;

import org.apache.phoenix.thirdparty.com.google.common.collect.Lists;

/**
 * "Unit" tests for UUIDFunctions
 */
public class UUIDFunctionTest {

    @Test
    public void testUUIDRandomFunction() throws Exception {
        UUID uuid = getUUIDRandom();
        assertTrue(uuid != null && uuid instanceof UUID);
    }

    @Test
    public void testUUIDFuntions() throws Exception {
        UUID uuid = UUID.randomUUID();
        String uuidStr = testUUIDToStringFunction(uuid, null, SortOrder.ASC);
        UUID returnUUid = testStringToUUIDfunction(uuidStr, null, SortOrder.ASC);
        assertTrue(uuid.equals(returnUUid));

        // maybe this is a stupid or bad designed test; but not sure.
        // if it is not necessary then StringToUUIDFunction.java should be changed
        // and let only 'value = new String(ptr.get(), ptr.getOffset(), UUID_TEXTUAL_LENGTH);'
        // without 'if (ptr.get()[ptr.getOffset() + 8] == '-')' check.
        uuid = UUID.randomUUID();
        uuidStr = testUUIDToStringFunction(uuid, null, SortOrder.DESC);
        returnUUid = testStringToUUIDfunction(uuidStr, null, SortOrder.DESC);
        assertTrue(returnUUid.equals(UUID.fromString(uuidStr)));

    }

    private UUID getUUIDRandom() {
        Expression UUIDRandomFunction = new UUIDRandomFunction();
        ImmutableBytesWritable ptr = new ImmutableBytesWritable();
        boolean ret = UUIDRandomFunction.evaluate(null, ptr);
        UUID result =
                ret ? (UUID) UUIDRandomFunction.getDataType().toObject(ptr,
                    UUIDRandomFunction.getSortOrder()) : null;
        return result;
    }

    private String testUUIDToStringFunction(UUID uuid, String localeIsoCode, SortOrder sortOrder)
            throws Exception {
        LiteralExpression inputUUIDLiteral;
        inputUUIDLiteral = LiteralExpression.newConstant(uuid, PUUID.INSTANCE, sortOrder);

        List<Expression> expressions = Lists.newArrayList((Expression) inputUUIDLiteral);
        Expression UUIDToStringFunction = new UUIDToStringFunction(expressions);
        ImmutableBytesWritable ptr = new ImmutableBytesWritable();
        boolean ret = UUIDToStringFunction.evaluate(null, ptr);
        String result =
                ret ? (String) UUIDToStringFunction.getDataType().toObject(ptr,
                    UUIDToStringFunction.getSortOrder()) : null;
        return result;
    }

    private static UUID testStringToUUIDfunction(String inputStr, String localeIsoCode,
            SortOrder sortOrder) throws Exception {
        LiteralExpression inputStrLiteral;
        inputStrLiteral = LiteralExpression.newConstant(inputStr, PVarchar.INSTANCE, sortOrder);

        List<Expression> expressions = Lists.newArrayList((Expression) inputStrLiteral);
        Expression stringToUUIDFunction = new StringToUUIDFunction(expressions);
        ImmutableBytesWritable ptr = new ImmutableBytesWritable();
        boolean ret = stringToUUIDFunction.evaluate(null, ptr);
        UUID result =
                ret ? (UUID) stringToUUIDFunction.getDataType().toObject(ptr,
                    stringToUUIDFunction.getSortOrder()) : null;
        return result;
    }

}
