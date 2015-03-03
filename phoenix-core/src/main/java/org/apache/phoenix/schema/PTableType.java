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

import java.util.Map;

import com.google.common.collect.Maps;


public enum PTableType {
    SYSTEM("s", "SYSTEM TABLE"), 
    TABLE("u", "TABLE"),
    VIEW("v", "VIEW"),
    INDEX("i", "INDEX"),
    PROJECTED("p", "PROJECTED"),
    SUBQUERY("q", "SUBQUERY"); 

    private final PName value;
    private final String serializedValue;
    
    private PTableType(String serializedValue, String value) {
        this.serializedValue = serializedValue;
        this.value = PNameFactory.newName(value);
    }
    
    public String getSerializedValue() {
        return serializedValue;
    }
    
    public PName getValue() {
        return value;
    }
    
    @Override
    public String toString() {
        return value.getString();
    }
    
    private static final PTableType[] FROM_SERIALIZED_VALUE;
    private static final int FROM_SERIALIZED_VALUE_OFFSET;
    private static final Map<String,PTableType> FROM_VALUE = Maps.newHashMapWithExpectedSize(PTableType.values().length);
    
    static {
        int minChar = Integer.MAX_VALUE;
        int maxChar = Integer.MIN_VALUE;
        for (PTableType type : PTableType.values()) {
            char c = type.getSerializedValue().charAt(0);
            if (c < minChar) {
                minChar = c;
            }
            if (c > maxChar) {
                maxChar = c;
            }
        }
        FROM_SERIALIZED_VALUE_OFFSET = minChar;
        FROM_SERIALIZED_VALUE = new PTableType[maxChar - minChar + 1];
        for (PTableType type : PTableType.values()) {
            FROM_SERIALIZED_VALUE[type.getSerializedValue().charAt(0) - minChar] = type;
        }
    }
    
    static {
        for (PTableType type : PTableType.values()) {
            if (FROM_VALUE.put(type.getValue().getString(),type) != null) {
                throw new IllegalStateException("Duplicate PTableType value of " + type.getValue().getString() + " is not allowed");
            }
        }
    }
    
    public static PTableType fromValue(String value) {
        PTableType type = FROM_VALUE.get(value);
        if (type == null) {
            throw new IllegalArgumentException("Unable to PTableType enum for value of '" + value + "'");
        }
        return type;
    }
    
    public static PTableType fromSerializedValue(String serializedValue) {
        if (serializedValue.length() == 1) {
            int i = serializedValue.charAt(0) - FROM_SERIALIZED_VALUE_OFFSET;
            if (i >= 0 && i < FROM_SERIALIZED_VALUE.length && FROM_SERIALIZED_VALUE[i] != null) {
                return FROM_SERIALIZED_VALUE[i];
            }
        }
        throw new IllegalArgumentException("Unable to PTableType enum for serialized value of '" + serializedValue + "'");
    }
    
    public static PTableType fromSerializedValue(byte serializedByte) {
        int i = serializedByte - FROM_SERIALIZED_VALUE_OFFSET;
        if (i >= 0 && i < FROM_SERIALIZED_VALUE.length && FROM_SERIALIZED_VALUE[i] != null) {
            return FROM_SERIALIZED_VALUE[i];
        }
        throw new IllegalArgumentException("Unable to PTableType enum for serialized value of '" + (char)serializedByte + "'");
    }
}
