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
package org.apache.phoenix.util;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.sql.SQLException;
import java.sql.Types;

import org.apache.phoenix.exception.SQLExceptionCode;
import org.junit.Test;

public class ColumnInfoTest {

    @Test
    public void testToFromStringRoundTrip() {
        ColumnInfo columnInfo = new ColumnInfo("a.myColumn", Types.INTEGER);
        assertEquals(columnInfo, ColumnInfo.fromString(columnInfo.toString()));
    }

    @Test(expected=IllegalArgumentException.class)
    public void testFromString_InvalidString() {
        ColumnInfo.fromString("invalid");
    }

    @Test
    public void testFromString_InvalidDataType() {
        try {
            ColumnInfo.fromString("COLNAME:badType");
        } catch (RuntimeException e) {
            assertTrue(e.getCause() instanceof SQLException);
            SQLException sqlE = (SQLException)e.getCause();
            assertEquals(SQLExceptionCode.ILLEGAL_DATA.getErrorCode(), sqlE.getErrorCode());
        }
    }
    
    @Test
    public void testToFromColonInColumnName() {
        ColumnInfo columnInfo = new ColumnInfo(":myColumn", Types.INTEGER);
        assertEquals(columnInfo, ColumnInfo.fromString(columnInfo.toString()));
    }
}
