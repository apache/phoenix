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

import com.google.common.base.Joiner;

public enum TimeUnit {
    DAY("day"), 
    HOUR("hour"), 
    MINUTE("minute"), 
    SECOND("second"), 
    MILLISECOND("millisecond");
    
    private String value;
    
    private TimeUnit(String value) {
        this.value = value;
    }
    
    public static final String VALID_VALUES = Joiner.on(", ").join(TimeUnit.values());
    
    public static TimeUnit getTimeUnit(String timeUnit) {
        if(timeUnit == null) {
            throw new IllegalArgumentException("No time unit value specified. Only a time unit value that belongs to one of these : " + VALID_VALUES + " is allowed.");
        }
        for(TimeUnit tu : values()) {
            if(timeUnit.equalsIgnoreCase(tu.value)) {
                return tu;
            }    
        }
        throw new IllegalArgumentException("Invalid value of time unit " + timeUnit + ". Only a time unit value that belongs to one of these : " + VALID_VALUES + " is allowed.");
    }
}
