/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.phoenix.log;

import org.apache.phoenix.schema.types.PDataType;
import org.apache.phoenix.schema.types.PInteger;
import org.apache.phoenix.schema.types.PTimestamp;
import org.apache.phoenix.schema.types.PVarchar;

import java.util.EnumSet;


public enum ActivityLogInfo {

    START_TIME("s", LogLevel.INFO,PTimestamp.INSTANCE),
    OP_TIME("u", LogLevel.INFO,PTimestamp.INSTANCE),
    TENANT_ID("t", LogLevel.INFO,PVarchar.INSTANCE),
    CQS_NAME("p", LogLevel.INFO,PVarchar.INSTANCE),
    REQUEST_ID("r", LogLevel.INFO,PVarchar.INSTANCE),
    TABLE_NAME("n", LogLevel.INFO,PVarchar.INSTANCE),
    OP_NAME("o", LogLevel.INFO,PVarchar.INSTANCE),
    OP_STMTS("#", LogLevel.INFO, PInteger.INSTANCE);

    public final String shortName;
    public final LogLevel logLevel;
    public final PDataType dataType;

    private ActivityLogInfo(String shortName, LogLevel logLevel, PDataType dataType) {
        this.shortName = shortName;
        this.logLevel=logLevel;
        this.dataType=dataType;
    }

    public String getShortName() {
        return shortName;
    }

    public LogLevel getLogLevel() {
        return logLevel;
    }

    public PDataType getDataType() {
        return dataType;
    }
    

}
