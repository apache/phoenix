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
package org.apache.phoenix.end2end;

import static org.junit.Assert.*;

import java.sql.*;

import org.apache.phoenix.exception.SQLExceptionCode;
import org.apache.phoenix.expression.function.MathPIFunction;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;

/**
 * End to end tests for {@link MathPIFunction}
 */
@Category(ParallelStatsDisabledTest.class)
public class MathPIFunctionEnd2EndIT extends ParallelStatsDisabledIT {

    @Test
    public void testGetMathPIValue() throws Exception {
        Connection conn  = DriverManager.getConnection(getUrl());
        ResultSet rs = conn.createStatement().executeQuery("SELECT PI()");
        assertTrue(rs.next());
        assertTrue(twoDoubleEquals(rs.getDouble(1), Math.PI));
        assertFalse(rs.next());
    }

    @Test
    public void testMathPIRoundTwoDecimal() throws Exception {
        Connection conn  = DriverManager.getConnection(getUrl());
        ResultSet rs = conn.createStatement().executeQuery("SELECT ROUND(PI(), 2)");
        assertTrue(rs.next());
        assertTrue(twoDoubleEquals(rs.getDouble(1), 3.14));
        assertFalse(rs.next());
    }

    @Test
    public void testMathPIFunctionWithIncorrectFormat() throws Exception {
        Connection conn  = DriverManager.getConnection(getUrl());
        try {
            conn.createStatement().executeQuery("SELECT PI(1)");
        } catch (SQLException e) {
            assertEquals(SQLExceptionCode.FUNCTION_UNDEFINED.getErrorCode(), e.getErrorCode());
        }
    }
}
