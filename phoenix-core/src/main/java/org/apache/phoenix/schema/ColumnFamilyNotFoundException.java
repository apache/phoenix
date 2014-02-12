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

/**
 * 
 * Exception thrown when a family name could not be found in the schema
 *
 * 
 * @since 0.1
 */
public class ColumnFamilyNotFoundException extends MetaDataEntityNotFoundException {
    private static final long serialVersionUID = 1L;
    private static SQLExceptionCode code = SQLExceptionCode.COLUMN_FAMILY_NOT_FOUND;
    private final String familyName;

    public ColumnFamilyNotFoundException(String familyName) {
        super(new SQLExceptionInfo.Builder(code).setFamilyName(familyName).build().toString(),
                code.getSQLState(), code.getErrorCode(), null);
        this.familyName = familyName;
    }

    public String getFamilyName() {
        return familyName;
    }
}
