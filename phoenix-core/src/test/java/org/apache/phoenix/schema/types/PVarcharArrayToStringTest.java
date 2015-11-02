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

import org.junit.Test;

/**
 * Test for {@link PhoenixArray#toString()}
 */
public class PVarcharArrayToStringTest extends BasePhoenixArrayToStringTest {

    @Test
    public void testUnicodeString() {
        helpTestToString(
            getBaseType(), //
            new String[] { "a" + "\u00ea" + "\u00f1" + "b", "c" + "\u00a0" + "\u00ff" + "d" },
            "['aêñb', 'c ÿd']");
    }

    @Test
    public void testStringWithSeparators() {
        helpTestToString(
            getBaseType(), //
            new String[] { "a,b,c", "d\"e\"f\"", "'g'h'i'" },
            "['a,b,c', 'd\"e\"f\"', '''g''h''i''']");
    }

    @Override
    protected PVarchar getBaseType() {
        return PVarchar.INSTANCE;
    }

    @Override
    protected String getString1() {
        return "'string1'";
    }

    @Override
    protected String getElement1() {
        return "string1";
    }

    @Override
    protected String getString2() {
        return "'string2'";
    }

    @Override
    protected String getElement2() {
        return "string2";
    }

    @Override
    protected String getString3() {
        return "'string3'";
    }

    @Override
    protected String getElement3() {
        return "string3";
    }

}
