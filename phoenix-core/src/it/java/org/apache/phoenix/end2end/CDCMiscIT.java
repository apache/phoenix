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
import org.apache.phoenix.schema.PTable;
import org.apache.phoenix.schema.TableProperty;
import org.apache.phoenix.util.CDCUtil;
import org.apache.phoenix.util.PhoenixRuntime;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Properties;
import java.util.Set;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

@Category(ParallelStatsDisabledTest.class)
public class CDCMiscIT extends ParallelStatsDisabledIT {
    private void assertCDCState(Connection conn, String cdcName, String expInclude,
                                int idxType) throws SQLException {
        try (ResultSet rs = conn.createStatement().executeQuery("SELECT cdc_include FROM " +
                "system.catalog WHERE table_name = '" + cdcName +
                "' AND column_name IS NULL and column_family IS NULL")) {
            assertEquals(true, rs.next());
            assertEquals(expInclude, rs.getString(1));
        }
        try (ResultSet rs = conn.createStatement().executeQuery("SELECT index_type FROM " +
                "system.catalog WHERE table_name = '" + CDCUtil.getCDCIndexName(cdcName) +
                "' AND column_name IS NULL and column_family IS NULL")) {
                assertEquals(true, rs.next());
            assertEquals(idxType, rs.getInt(1));
        }
    }

    private void assertPTable(String cdcName, Set<PTable.CDCChangeScope> expIncludeScopes)
            throws SQLException {
        Properties props = new Properties();
        Connection conn = DriverManager.getConnection(getUrl(), props);
        PTable table = PhoenixRuntime.getTable(conn, cdcName);
        assertEquals(expIncludeScopes, table.getCDCIncludeScopes());
        assertEquals(expIncludeScopes, TableProperty.INCLUDE.getPTableValue(table));
    }

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
                    + " ON NON_EXISTENT_TABLE (PHOENIX_ROW_TIMESTAMP())");
            fail("Expected to fail due to non-existent table");
        } catch (SQLException e) {
            assertEquals(SQLExceptionCode.TABLE_UNDEFINED.getErrorCode(), e.getErrorCode());
        }

        try {
            conn.createStatement().execute("CREATE CDC " + cdcName
                    + " ON " + tableName +"(UNKNOWN_FUNCTION())");
            fail("Expected to fail due to invalid function");
        } catch (SQLException e) {
            assertEquals(SQLExceptionCode.FUNCTION_UNDEFINED.getErrorCode(), e.getErrorCode());
        }

        try {
            conn.createStatement().execute("CREATE CDC " + cdcName
                    + " ON " + tableName +"(NOW())");
            fail("Expected to fail due to non-deterministic function");
        } catch (SQLException e) {
            assertEquals(SQLExceptionCode.NON_DETERMINISTIC_EXPRESSION_NOT_ALLOWED_IN_INDEX.
                    getErrorCode(), e.getErrorCode());
        }

        try {
            conn.createStatement().execute("CREATE CDC " + cdcName
                    + " ON " + tableName +"(ROUND(v1))");
            fail("Expected to fail due to non-timestamp expression in the index PK");
        } catch (SQLException e) {
            assertEquals(SQLExceptionCode.INCORRECT_DATATYPE_FOR_EXPRESSION.getErrorCode(),
                    e.getErrorCode());
        }

        try {
            conn.createStatement().execute("CREATE CDC " + cdcName
                    + " ON " + tableName +"(v1)");
            fail("Expected to fail due to non-timestamp column in the index PK");
        } catch (SQLException e) {
            assertEquals(SQLExceptionCode.INCORRECT_DATATYPE_FOR_EXPRESSION.getErrorCode(),
                    e.getErrorCode());
        }

        String cdc_sql = "CREATE CDC " + cdcName
                + " ON " + tableName + "(PHOENIX_ROW_TIMESTAMP())";
        conn.createStatement().execute(cdc_sql);
        assertCDCState(conn, cdcName, null, 3);

        try {
            conn.createStatement().execute(cdc_sql);
            fail("Expected to fail due to duplicate index");
        } catch (SQLException e) {
            assertEquals(SQLExceptionCode.TABLE_ALREADY_EXIST.getErrorCode(), e.getErrorCode());
            assertTrue(e.getMessage().endsWith(cdcName));
        }

        cdcName = generateUniqueName();
        conn.createStatement().execute("CREATE CDC " + cdcName + " ON " + tableName +
                "(v2) INCLUDE (post, pre, post) INDEX_TYPE=g");
        assertCDCState(conn, cdcName, "PRE, POST", 3);
        assertPTable(cdcName, new HashSet<>(
                Arrays.asList(PTable.CDCChangeScope.PRE, PTable.CDCChangeScope.POST)));

        cdcName = generateUniqueName();
        conn.createStatement().execute("CREATE CDC " + cdcName + " ON " + tableName +
                "(v2) INCLUDE (pre, post) INDEX_TYPE=l");
        assertCDCState(conn, cdcName, "PRE, POST", 2);
        assertPTable(cdcName, new HashSet<>(
                Arrays.asList(PTable.CDCChangeScope.PRE, PTable.CDCChangeScope.POST)));

        conn.close();
    }
}
