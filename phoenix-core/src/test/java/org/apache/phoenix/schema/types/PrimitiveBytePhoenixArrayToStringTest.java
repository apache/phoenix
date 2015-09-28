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

public class PrimitiveBytePhoenixArrayToStringTest extends BasePrimitiveIntPhoenixArrayToStringTest {

    @Override
    protected PDataType getBaseType() {
        return PTinyint.INSTANCE;
    }

    @Override
    protected Number getElement1() {
        return (byte) 1;
    }

    @Override
    protected Number getElement2() {
        return (byte) 2;
    }

    @Override
    protected Number getElement3() {
        return (byte) 3;
    }
}
