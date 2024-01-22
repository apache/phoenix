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

import org.apache.phoenix.exception.SQLExceptionCode;
import org.apache.phoenix.schema.PTable;
import org.junit.Test;

import java.sql.SQLException;
import java.util.Arrays;
import java.util.HashSet;

import static org.apache.phoenix.schema.PTable.CDCChangeScope.*;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class CDCUtilTest {
    @Test
    public void testScopeSetConstruction() throws Exception {
        assertEquals(new HashSet<>(Arrays.asList(PRE)), CDCUtil.makeChangeScopeEnumsFromString(
                "PRE"));
        assertEquals(new HashSet<>(Arrays.asList(PRE)),
                CDCUtil.makeChangeScopeEnumsFromString("PRE,"));
        assertEquals(new HashSet<>(Arrays.asList(PRE)),
                CDCUtil.makeChangeScopeEnumsFromString("PRE, PRE"));
        assertEquals(new HashSet<>(Arrays.asList(CHANGE, PRE, POST)),
                CDCUtil.makeChangeScopeEnumsFromString("POST,PRE,CHANGE"));
        try {
            CDCUtil.makeChangeScopeEnumsFromString("DUMMY");
        } catch (SQLException e) {
            assertEquals(SQLExceptionCode.UNKNOWN_INCLUDE_CHANGE_SCOPE.getErrorCode(),
                    e.getErrorCode());
            assertTrue(e.getMessage().endsWith("DUMMY"));
        }
    }

    @Test
    public void testScopeStringConstruction() throws Exception {
        assertEquals(null, CDCUtil.makeChangeScopeStringFromEnums(null));
        assertEquals("", CDCUtil.makeChangeScopeStringFromEnums(
                new HashSet<PTable.CDCChangeScope>()));
        assertEquals("CHANGE,PRE,POST", CDCUtil.makeChangeScopeStringFromEnums(
                new HashSet<>(Arrays.asList(CHANGE, PRE, POST))));
    }
}
