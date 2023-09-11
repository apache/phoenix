/*
 *
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

package org.apache.phoenix.exception;

import java.sql.SQLException;
import java.util.List;

/**
 * Invalid region split policy for given table
 */
public class InvalidRegionSplitPolicyException extends SQLException {

    private static final long serialVersionUID = 1L;

    private static final SQLExceptionCode EXCEPTION_CODE =
        SQLExceptionCode.INVALID_REGION_SPLIT_POLICY;
    private static final String ERROR_MSG = "Region split policy for table %s"
        + " is expected to be among: %s , actual split policy: %s";

    public InvalidRegionSplitPolicyException(final String schemaName,
          final String tableName, final List<String> expectedSplitPolicies,
          final String actualSplitPolicy) {
        super(new SQLExceptionInfo.Builder(EXCEPTION_CODE)
                .setSchemaName(schemaName)
                .setTableName(tableName)
                .setMessage(String.format(ERROR_MSG, tableName,
                    expectedSplitPolicies, actualSplitPolicy))
                .build().toString(),
            EXCEPTION_CODE.getSQLState(), EXCEPTION_CODE.getErrorCode(), null);
    }

}