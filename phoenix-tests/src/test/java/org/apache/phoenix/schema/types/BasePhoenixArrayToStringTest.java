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
package org.apache.phoenix.schema.types;

import static org.junit.Assert.assertEquals;

import org.junit.Test;

public abstract class BasePhoenixArrayToStringTest {

    @Test
    public void testEmptyArray() {
        helpTestToString(getBaseType(), new Object[] {}, "[]");
    }

    @Test
    public void testSingleObjectArray() {
        helpTestToString(getBaseType(), new Object[] { getElement1() }, "[" + getString1() + "]");
    }

    @Test
    public void testMultipleObjectArray() {
        helpTestToString(getBaseType(),
            new Object[] { getElement1(), getElement2(), getElement3() }, "[" + getString1() + ", "
                    + getString2() + ", " + getString3() + "]");
    }

    @Test
    public void testSingleNullObjectArray() {
        helpTestToString(getBaseType(), new Object[] { null }, "[" + getNullString() + "]");
    }

    @Test
    public void testMultipleNullObjectArray() {
        helpTestToString(getBaseType(), new Object[] { null, null }, "[" + getNullString() + ", "
                + getNullString() + "]");
    }

    @Test
    public void testNormalAndNullObjectArray() {
        helpTestToString(getBaseType(), new Object[] { null, getElement1(), null, getElement2() },
            "[" + getNullString() + ", " + getString1() + ", " + getNullString() + ", "
                    + getString2() + "]");
    }

    protected abstract PDataType getBaseType();

    protected abstract Object getElement1();

    protected abstract String getString1();

    protected abstract Object getElement2();

    protected abstract String getString2();

    protected abstract Object getElement3();

    protected abstract String getString3();

    protected String getNullString() {
        return "null";
    }

    protected void helpTestToString(PDataType type, Object[] array, String expected) {
        PhoenixArray arr = PArrayDataType.instantiatePhoenixArray(type, array);
        boolean isPrimitive = isPrimitive(arr);
        assertEquals("Expected " + getBaseType() + " array to be " + (isPrimitive ? "" : "not ")
                + "primitive.", isPrimitive, !arr.getClass().equals(PhoenixArray.class));
        assertEquals(expected, arr.toString());
    }

    protected boolean isPrimitive(PhoenixArray arr) {
        return arr.isPrimitiveType();
    }

}