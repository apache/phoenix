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
package org.apache.phoenix.schema;

import org.apache.phoenix.schema.types.PVarchar;

public enum PIndexState {
    BUILDING("b"),
    USABLE("e"),
    UNUSABLE("d"),
    ACTIVE("a"),
    INACTIVE("i"),
    DISABLE("x"),
    REBUILD("r");

    private final String serializedValue;
    private final byte[] serializedBytes;
    private final byte[] nameBytesValue;

    private PIndexState(String value) {
        this.serializedValue = value;
        this.serializedBytes = PVarchar.INSTANCE.toBytes(value);
        this.nameBytesValue = PVarchar.INSTANCE.toBytes(this.toString());
    }

    public String getSerializedValue() {
        return serializedValue;
    }

    public byte[] getSerializedBytes() {
        return serializedBytes;
    }

    public byte[] toBytes() {
        return nameBytesValue;
    }

    private static final PIndexState[] FROM_VALUE;
    private static final int FROM_VALUE_OFFSET;
    static {
        int minChar = Integer.MAX_VALUE;
        int maxChar = Integer.MIN_VALUE;
        for (PIndexState state: PIndexState.values()) {
            char c = state.getSerializedValue().charAt(0);
            if (c < minChar) {
                minChar = c;
            }
            if (c > maxChar) {
                maxChar = c;
            }
        }
        FROM_VALUE_OFFSET = minChar;
        FROM_VALUE = new PIndexState[maxChar - minChar + 1];
        for (PIndexState state: PIndexState.values()) {
            FROM_VALUE[state.getSerializedValue().charAt(0) - minChar] = state;
        }
    }

    public static PIndexState fromSerializedValue(String serializedValue) {
        if (serializedValue.length() == 1) {
            int i = serializedValue.charAt(0) - FROM_VALUE_OFFSET;
            if (i >= 0 && i < FROM_VALUE.length && FROM_VALUE[i] != null) {
                return FROM_VALUE[i];
            }
        }
        throw new IllegalArgumentException("Unable to PIndexState enum for serialized value of '" + serializedValue + "'");
    }

    public static PIndexState fromSerializedValue(byte serializedByte) {
        int i = serializedByte - FROM_VALUE_OFFSET;
        if (i >= 0 && i < FROM_VALUE.length && FROM_VALUE[i] != null) {
            return FROM_VALUE[i];
        }
        throw new IllegalArgumentException("Unable to PIndexState enum for serialized value of '" + (char)serializedByte + "'");
    }
}
