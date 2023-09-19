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

import org.apache.phoenix.exception.SQLExceptionCode;
import org.apache.phoenix.query.BaseTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.Properties;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

@Category(ParallelStatsDisabledTest.class)
public class CDCMiscIT extends ParallelStatsDisabledIT {
    @Test
    public void testCreate() throws SQLException {
        Properties props = new Properties();
        Connection conn = DriverManager.getConnection(getUrl(), props);
        String tableName = generateUniqueName();
        conn.createStatement().execute(
                "CREATE TABLE  " + tableName + " ( k INTEGER PRIMARY KEY," + " v1 INTEGER,"
                        + " v2 DATE)");
        String cdcName = generateUniqueName();

        try {
            conn.createStatement().execute("CREATE CDC " + cdcName
                    + " ON NON_EXISTENT_TABLE (PHOENIX_ROW_TIMESTAMP()) INDEX_TYPE=global");
            fail("Expected to fail due to non-existent table");
        } catch (SQLException e) {
            assertEquals(SQLExceptionCode.TABLE_UNDEFINED.getErrorCode(), e.getErrorCode());
        }

        try {
            conn.createStatement().execute("CREATE CDC " + cdcName
                    + " ON " + tableName +"(UNKNOWN_FUNCTION()) INDEX_TYPE=global");
            fail("Expected to fail due to invalid function");
        } catch (SQLException e) {
            assertEquals(SQLExceptionCode.FUNCTION_UNDEFINED.getErrorCode(), e.getErrorCode());
        }

        try {
            conn.createStatement().execute("CREATE CDC " + cdcName
                    + " ON " + tableName +"(NOW()) INDEX_TYPE=global");
            fail("Expected to fail due to non-deterministic function");
        } catch (SQLException e) {
            assertEquals(SQLExceptionCode.NON_DETERMINISTIC_EXPRESSION_NOT_ALLOWED_IN_INDEX.getErrorCode(), e.getErrorCode());
        }

        try {
            conn.createStatement().execute("CREATE CDC " + cdcName
                    + " ON " + tableName +"(ROUND(v1)) INDEX_TYPE=global");
            fail("Expected to fail due to non-timestamp expression in the index PK");
        } catch (SQLException e) {
            assertEquals(SQLExceptionCode.INCORRECT_DATATYPE_FOR_EXPRESSION.getErrorCode(), e.getErrorCode());
        }

        try {
            conn.createStatement().execute("CREATE CDC " + cdcName
                    + " ON " + tableName +"(v1) INDEX_TYPE=global");
            fail("Expected to fail due to non-timestamp column in the index PK");
        } catch (SQLException e) {
            assertEquals(SQLExceptionCode.INCORRECT_DATATYPE_FOR_EXPRESSION.getErrorCode(), e.getErrorCode());
        }

        String cdc_sql = "CREATE CDC " + cdcName
                + " ON " + tableName + "(PHOENIX_ROW_TIMESTAMP()) INDEX_TYPE=global";
        conn.createStatement().execute(cdc_sql);

        try {
            conn.createStatement().execute(cdc_sql);
            fail("Expected to fail due to duplicate index");
        } catch (SQLException e) {
            assertEquals(SQLExceptionCode.TABLE_ALREADY_EXIST.getErrorCode(), e.getErrorCode());
        }

        conn.createStatement().execute("CREATE CDC " + generateUniqueName() + " ON " + tableName +
                "(v2) INDEX_TYPE=global");

        conn.close();
    }
}
