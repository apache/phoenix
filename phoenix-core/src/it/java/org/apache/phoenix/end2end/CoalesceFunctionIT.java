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

import static org.apache.phoenix.util.TestUtil.ROW6;
import static org.apache.phoenix.util.TestUtil.ROW7;
import static org.apache.phoenix.util.TestUtil.TEST_PROPERTIES;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Properties;

import org.apache.phoenix.exception.SQLExceptionCode;
import org.apache.phoenix.util.PropertiesUtil;
import org.junit.Assert;
import org.junit.Test;


public class CoalesceFunctionIT extends BaseHBaseManagedTimeIT {

    @Test
    public void testCoalesce() throws Exception {
        String tenantId = getOrganizationId();
        initATableValues(tenantId, getDefaultSplits(tenantId), getUrl());
        Properties props = PropertiesUtil.deepCopy(TEST_PROPERTIES);
        Connection conn = DriverManager.getConnection(getUrl(), props);
        String query = "SELECT entity_id, a_integer + COALESCE(x_integer,1) FROM ATABLE WHERE organization_id = ? AND a_integer >= 6 AND a_integer <= 7";
        PreparedStatement statement = conn.prepareStatement(query);
        statement.setString(1, tenantId);
        ResultSet rs = statement.executeQuery();

        assertTrue(rs.next());
        assertEquals(ROW6, rs.getString(1));
        assertEquals(7, rs.getInt(2));

        assertTrue(rs.next());
        assertEquals(ROW7, rs.getString(1));
        assertEquals(12, rs.getInt(2));

        assertFalse(rs.next());
        conn.close();
    }

    @Test
    public void coalesceWithSumExplicitLong() throws Exception {
        Connection conn = DriverManager.getConnection(getUrl());

        String ddl = "CREATE TABLE TEST_COALESCE("
                + "    ID BIGINT NOT NULL, "
                + "    COUNT BIGINT "
                + "    CONSTRAINT pk PRIMARY KEY(ID))";
        conn.createStatement().execute(ddl);

        conn.createStatement().execute("UPSERT INTO TEST_COALESCE(ID, COUNT) VALUES(2, null)");
        conn.commit();

        ResultSet rs = conn.createStatement().executeQuery(
                "SELECT "
                + "COALESCE(SUM(COUNT), CAST(0 AS BIGINT)) " //explicitly def long
                + "FROM TEST_COALESCE "
                + "GROUP BY ID");

        assertTrue(rs.next());
        assertEquals(0, rs.getLong(1));
        assertFalse(rs.wasNull());
    }

    @Test
    public void coalesceWithSumImplicitLong() throws Exception {
        Connection conn = DriverManager.getConnection(getUrl());

        String ddl = "CREATE TABLE TEST_COALESCE("
                + "    ID BIGINT NOT NULL, "
                + "    COUNT BIGINT "
                + "    CONSTRAINT pk PRIMARY KEY(ID))";
        conn.createStatement().execute(ddl);

        conn.createStatement().execute("UPSERT INTO TEST_COALESCE(ID, COUNT) VALUES(2, null)");
        conn.commit();

        ResultSet rs = conn.createStatement().executeQuery(
                "SELECT "
                + "COALESCE(SUM(COUNT), 0) " // no long def
                + "FROM TEST_COALESCE "
                + "GROUP BY ID");

        assertTrue(rs.next());
        assertEquals(0, rs.getLong(1));
        assertFalse(rs.wasNull());
    }

    @Test
    public void coalesceWithSecondParamAsExpression() throws Exception {
        Connection conn = DriverManager.getConnection(getUrl());

        String ddl = "CREATE TABLE TEST_COALESCE("
                + "    ID BIGINT NOT NULL, "
                + "    COUNT BIGINT "
                + "    CONSTRAINT pk PRIMARY KEY(ID))";
        conn.createStatement().execute(ddl);

        conn.createStatement().execute("UPSERT INTO TEST_COALESCE(ID, COUNT) VALUES(2, null)");
        conn.commit();

        ResultSet rs = conn.createStatement().executeQuery(
                "SELECT "
                + "COALESCE(SUM(COUNT), SUM(ID)) " // second param as expression
                + "FROM TEST_COALESCE "
                + "GROUP BY ID");

        assertTrue(rs.next());
        assertEquals(2, rs.getLong(1));
        assertFalse(rs.wasNull());
    }

    @Test
    public void nonTypedSecondParameterLong() throws Exception {
        Connection conn = DriverManager.getConnection(getUrl());

        String ddl = "CREATE TABLE TEST_COALESCE("
                + "    ID BIGINT NOT NULL, "
                + "    COUNT BIGINT " //first parameter to coalesce
                + "    CONSTRAINT pk PRIMARY KEY(ID))";
        conn.createStatement().execute(ddl);

        conn.createStatement().execute("UPSERT INTO TEST_COALESCE(ID, COUNT) VALUES(2, null)");
        conn.commit();

        ResultSet rs = conn.createStatement().executeQuery(
                "SELECT "
                + "COALESCE(NTH_VALUE(COUNT, 100) WITHIN GROUP (ORDER BY COUNT DESC), 0) " //second param is int
                + "FROM TEST_COALESCE "
                + "GROUP BY ID");

        assertTrue(rs.next());
        assertEquals(0, rs.getLong(1));
        assertFalse(rs.wasNull());
    }

    @Test
    public void nonTypedSecondParameterUnsignedDataTypes() throws Exception {
        Connection conn = DriverManager.getConnection(getUrl());

        String ddl = "CREATE TABLE TEST_COALESCE ("
                + "    ID BIGINT NOT NULL, "
                + "    COUNT UNSIGNED_INT " //first parameter to coalesce
                + "    CONSTRAINT pk PRIMARY KEY(ID))";
        conn.createStatement().execute(ddl);

        conn.createStatement().execute("UPSERT INTO TEST_COALESCE (ID, COUNT) VALUES(2, null)");
        conn.commit();

        //second param to coalesce is signed int
        ResultSet rs = conn.createStatement().executeQuery(
                "SELECT "
                + " COALESCE(NTH_VALUE(COUNT, 100) WITHIN GROUP (ORDER BY COUNT DESC), 1) "
                + " FROM TEST_COALESCE" 
                + " GROUP BY ID");

        assertTrue(rs.next());
        assertEquals(1, rs.getInt(1));
        assertFalse(rs.wasNull());
    }

    @Test
    public void testWithNthValueAggregationFunction() throws Exception {
        Connection conn = DriverManager.getConnection(getUrl());

        String ddl = "CREATE TABLE TEST_NTH("
                + "    ID BIGINT NOT NULL, "
                + "    DATE TIMESTAMP NOT NULL, "
                + "    COUNT BIGINT "
                + "    CONSTRAINT pk PRIMARY KEY(ID, DATE))";
        conn.createStatement().execute(ddl);

        conn.createStatement().execute("UPSERT INTO TEST_NTH(ID, DATE, COUNT) VALUES(1, CURRENT_TIME(), 1)");
        conn.createStatement().execute("UPSERT INTO TEST_NTH(ID, DATE, COUNT) VALUES(1, CURRENT_TIME(), 2)");
        conn.createStatement().execute("UPSERT INTO TEST_NTH(ID, DATE, COUNT) VALUES(2, CURRENT_TIME(), 1)");
        conn.commit();

        //second param to coalesce is signed int
        ResultSet rs = conn.createStatement().executeQuery(
                "SELECT "
                + " COALESCE("
                + "            NTH_VALUE(COUNT, 2000)" // should evaluate null
                + "            WITHIN GROUP (ORDER BY COUNT DESC),"
                + "       0)"
                + "FROM TEST_NTH "
                + "GROUP BY ID");

        assertTrue(rs.next());
        assertEquals(0, rs.getLong(1));
        assertFalse(rs.wasNull());
    }

    @Test
    public void wrongDataTypeOfSecondParameter() throws Exception {
        Connection conn = DriverManager.getConnection(getUrl());

        String ddl = "CREATE TABLE TEST_COALESCE("
                + "    ID UNSIGNED_INT NOT NULL, "
                + "    COUNT UNSIGNED_INT "
                + "    CONSTRAINT pk PRIMARY KEY(ID))";
        conn.createStatement().execute(ddl);

        conn.createStatement().execute("UPSERT INTO TEST_COALESCE(ID, COUNT) VALUES(2, null)");
        conn.commit();

        try {
            conn.createStatement().executeQuery(
                    "SELECT "
                    + "COALESCE(MIN(COUNT), -1) " // invalid value for UNSIGNED_INT
                    + "FROM TEST_COALESCE "
                    + "GROUP BY ID");

            Assert.fail("CANNOT CONVERT TYPE exception expected");
        } catch (SQLException e) {

        }
    }

    @Test
    public void testImplicitSecondArgCastingException() throws Exception {
        Connection conn = DriverManager.getConnection(getUrl());

        String ddl = "CREATE TABLE TEST_COALESCE("
                + "    ID INTEGER NOT NULL, "
                + "    COUNT UNSIGNED_INT " //first parameter to coalesce
                + "    CONSTRAINT pk PRIMARY KEY(ID))";
        conn.createStatement().execute(ddl);

        conn.createStatement().execute("UPSERT INTO TEST_COALESCE(ID, COUNT) VALUES(-2, null)");
        conn.commit();

        try {
            ResultSet rs = conn.createStatement().executeQuery(
                    "SELECT "
                    + "COALESCE(MIN(COUNT), ID) "
                    + "FROM TEST_COALESCE "
                    + "GROUP BY ID");

            assertTrue(rs.next());
            assertEquals(0, rs.getLong(1));
            fail("Should not cast -2 to UNSIGNED_INT");
        } catch (SQLException e) {
            assertEquals(SQLExceptionCode.ILLEGAL_DATA.getErrorCode(), e.getErrorCode());
        }
    }

    @Test
    public void testImplicitSecondArgCasting() throws Exception {
        Connection conn = DriverManager.getConnection(getUrl());

        String ddl = "CREATE TABLE TEST_COALESCE("
                + "    ID DOUBLE NOT NULL, "
                + "    COUNT INTEGER " //first parameter to coalesce
                + "    CONSTRAINT pk PRIMARY KEY(ID))";
        conn.createStatement().execute(ddl);

        conn.createStatement().execute("UPSERT INTO TEST_COALESCE(ID, COUNT) VALUES(2.0, null)");
        conn.commit();

        ResultSet rs = conn.createStatement().executeQuery(
                "SELECT "
                + "COALESCE(MIN(COUNT), ID) "
                + "FROM TEST_COALESCE "
                + "GROUP BY ID");

        assertTrue(rs.next());
        assertEquals(2, rs.getLong(1));
        assertFalse(rs.wasNull());
    }


}
