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

import org.apache.phoenix.exception.SQLExceptionCode;
import org.apache.phoenix.exception.SQLExceptionInfo;

import java.sql.SQLException;

/**
 *
 * Exception thrown when MutationState row Column Cell size is bigger than
 * maximum allowed number
 *
 */

public class MaxPhoenixColumnSizeExceededException extends SQLException  {
    private static final long serialVersionUID = 1L;
    private static SQLExceptionCode code = SQLExceptionCode.MAX_HBASE_CLIENT_KEYVALUE_MAXSIZE_EXCEEDED;

    public MaxPhoenixColumnSizeExceededException() {
        super(new SQLExceptionInfo.Builder(code).build().toString(), code.getSQLState(), code.getErrorCode(), null);
    }

    public MaxPhoenixColumnSizeExceededException(String rowkeyAndColumnInfo, int maxMutationCellSizeBytes,
                                                 int mutationCellSizeBytes) {
        super(new SQLExceptionInfo.Builder(code).setMaxPhoenixColumnSizeBytes(maxMutationCellSizeBytes)
                        .setPhoenixColumnSizeBytes(mutationCellSizeBytes).build().toString() + ". " + rowkeyAndColumnInfo,
                code.getSQLState(), code.getErrorCode(), null);
    }
}