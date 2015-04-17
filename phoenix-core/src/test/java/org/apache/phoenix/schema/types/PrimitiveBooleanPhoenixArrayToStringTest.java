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

public class PrimitiveBooleanPhoenixArrayToStringTest extends BasePhoenixArrayToStringTest {

    @Override
    protected PDataType getBaseType() {
        return PBoolean.INSTANCE;
    }

    @Override
    protected String getNullString() {
        // primitive arrays don't use null
        return "false";
    }

    @Override
    protected Object getElement1() {
        return true;
    }

    @Override
    protected String getString1() {
        return "true";
    }

    @Override
    protected Object getElement2() {
        return false;
    }

    @Override
    protected String getString2() {
        return "false";
    }

    @Override
    protected Object getElement3() {
        return getElement1();
    }

    @Override
    protected String getString3() {
        return getString1();
    }

}
