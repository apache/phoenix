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

import org.apache.hadoop.hbase.HConstants;
import org.apache.phoenix.exception.SQLExceptionCode;
import org.apache.phoenix.exception.SQLExceptionInfo;

public class SchemaNotFoundException extends MetaDataEntityNotFoundException {
    private static final long serialVersionUID = 1L;
    private static SQLExceptionCode code = SQLExceptionCode.SCHEMA_NOT_FOUND;
    private final String schemaName;
    private final long timestamp;

    public SchemaNotFoundException(SchemaNotFoundException e, long timestamp) {
        this(e.schemaName, timestamp);
    }

    public SchemaNotFoundException(String schemaName) {
        this(schemaName, HConstants.LATEST_TIMESTAMP);
    }

    public SchemaNotFoundException(String schemaName, long timestamp) {
        super(new SQLExceptionInfo.Builder(code).setSchemaName(schemaName).build().toString(), code.getSQLState(),
                code.getErrorCode(), null);
        this.schemaName = schemaName;
        this.timestamp = timestamp;
    }

    public String getSchemaName() {
        return schemaName;
    }

    public long getTimeStamp() {
        return timestamp;
    }
}
