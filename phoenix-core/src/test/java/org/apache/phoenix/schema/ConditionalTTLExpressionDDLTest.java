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

import static org.apache.phoenix.util.TestUtil.TEST_PROPERTIES;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;

import org.apache.phoenix.exception.PhoenixParserException;
import org.apache.phoenix.exception.SQLExceptionCode;
import org.apache.phoenix.jdbc.PhoenixConnection;
import org.apache.phoenix.query.BaseConnectionlessQueryTest;
import org.apache.phoenix.util.PropertiesUtil;
import org.junit.Test;

public class ConditionalTTLExpressionDDLTest extends BaseConnectionlessQueryTest {

    private static void assertConditonTTL(Connection conn, String tableName, String ttlExpr) throws SQLException {
        TTLExpression expected = new ConditionTTLExpression(ttlExpr);
        assertConditonTTL(conn, tableName, expected);
    }

    private static void assertConditonTTL(Connection conn, String tableName, TTLExpression expected) throws SQLException {
        PTable table = conn.unwrap(PhoenixConnection.class).getTable(tableName);
        assertEquals(expected, table.getTTL());
    }

    @Test
    public void testBasicExpression() throws SQLException {
        String ddlTemplate = "create table %s (k1 bigint not null, k2 bigint not null, col1 varchar, col2 date " +
                "constraint pk primary key (k1,k2 desc)) TTL = '%s'";
        String quotedValue = "k1 > 5 AND col1 < ''zzzzzz''";
        String ttl = "k1 > 5 AND col1 < 'zzzzzz'";
        String tableName = generateUniqueName();
        String ddl = String.format(ddlTemplate, tableName, quotedValue);
        try (Connection conn = DriverManager.getConnection(getUrl(), PropertiesUtil.deepCopy(TEST_PROPERTIES))) {
            conn.createStatement().execute(ddl);
            assertConditonTTL(conn, tableName, ttl);
        }
    }

    @Test(expected = TypeMismatchException.class)
    public void testNotBooleanExpr() throws SQLException {
        String ddlTemplate = "create table %s (k1 bigint not null, k2 bigint not null, col1 varchar, col2 date " +
                "constraint pk primary key (k1,k2 desc)) TTL = '%s'";
        String ttl = "k1 + 100";
        String tableName = generateUniqueName();
        String ddl = String.format(ddlTemplate, tableName, ttl);
        try (Connection conn = DriverManager.getConnection(getUrl(), PropertiesUtil.deepCopy(TEST_PROPERTIES))) {
            conn.createStatement().execute(ddl);
        }
    }

    @Test(expected = TypeMismatchException.class)
    public void testWrongArgumentValue() throws SQLException {
        String ddlTemplate = "create table %s (k1 bigint not null, k2 bigint not null, col1 varchar, col2 date " +
                "constraint pk primary key (k1,k2 desc)) TTL = '%s'";
        String ttl = "k1 = ''abc''";
        String tableName = generateUniqueName();
        String ddl = String.format(ddlTemplate, tableName, ttl);
        try (Connection conn = DriverManager.getConnection(getUrl(), PropertiesUtil.deepCopy(TEST_PROPERTIES))) {
            conn.createStatement().execute(ddl);
        }
    }

    @Test(expected = PhoenixParserException.class)
    public void testParsingError() throws SQLException {
        String ddlTemplate = "create table %s (k1 bigint not null, k2 bigint not null, col1 varchar, col2 date " +
                "constraint pk primary key (k1,k2 desc)) TTL = '%s'";
        String ttl = "k2 == 23";
        String tableName = generateUniqueName();
        String ddl = String.format(ddlTemplate, tableName, ttl);
        try (Connection conn = DriverManager.getConnection(getUrl(), PropertiesUtil.deepCopy(TEST_PROPERTIES))) {
            conn.createStatement().execute(ddl);
        }
    }

    @Test
    public void testAggregateExpressionNotAllowed() throws SQLException {
        String ddlTemplate = "create table %s (k1 bigint not null, k2 bigint not null, col1 varchar, col2 date " +
                "constraint pk primary key (k1,k2 desc)) TTL = '%s'";
        String ttl = "SUM(k2) > 23";
        String tableName = generateUniqueName();
        String ddl = String.format(ddlTemplate, tableName, ttl);
        try (Connection conn = DriverManager.getConnection(getUrl(), PropertiesUtil.deepCopy(TEST_PROPERTIES))) {
            conn.createStatement().execute(ddl);
            fail();
        } catch (SQLException e) {
            assertEquals(SQLExceptionCode.AGGREGATE_EXPRESSION_NOT_ALLOWED_IN_TTL_EXPRESSION.getErrorCode(),
                    e.getErrorCode());
        } catch (Exception e) {
            fail("Unknown exception " + e);
        }
    }

    @Test
    public void testNullExpression() throws SQLException {
        String ddlTemplate = "create table %s (k1 bigint not null, k2 bigint not null, col1 varchar, col2 date " +
                "constraint pk primary key (k1,k2 desc)) TTL = '%s'";
        String ttl = "col1 is NULL AND col2 < CURRENT_DATE() + 30000";
        String tableName = generateUniqueName();
        String ddl = String.format(ddlTemplate, tableName, ttl);
        try (Connection conn = DriverManager.getConnection(getUrl(), PropertiesUtil.deepCopy(TEST_PROPERTIES))) {
            conn.createStatement().execute(ddl);
            assertConditonTTL(conn, tableName, ttl);
        }
    }

    @Test
    public void testBooleanColumn() throws SQLException {
        String ddlTemplate = "create table %s (k1 bigint not null, k2 bigint not null, expired BOOLEAN " +
                "constraint pk primary key (k1,k2 desc)) TTL = '%s'";
        String ttl = "expired";
        String tableName = generateUniqueName();
        String ddl = String.format(ddlTemplate, tableName, ttl);
        try (Connection conn = DriverManager.getConnection(getUrl(), PropertiesUtil.deepCopy(TEST_PROPERTIES))) {
            conn.createStatement().execute(ddl);
            assertConditonTTL(conn, tableName, ttl);
        }
    }

    @Test
    public void testNot() throws SQLException {
        String ddlTemplate = "create table %s (k1 bigint not null, k2 bigint not null, expired BOOLEAN " +
                "constraint pk primary key (k1,k2 desc)) TTL = '%s'";
        String ttl = "NOT expired";
        String tableName = generateUniqueName();
        String ddl = String.format(ddlTemplate, tableName, ttl);
        try (Connection conn = DriverManager.getConnection(getUrl(), PropertiesUtil.deepCopy(TEST_PROPERTIES))) {
            conn.createStatement().execute(ddl);
            assertConditonTTL(conn, tableName, ttl);
        }
    }

    @Test
    public void testPhoenixRowTimestamp() throws SQLException {
        String ddlTemplate = "create table %s (k1 bigint not null, k2 bigint not null, col1 varchar " +
                "constraint pk primary key (k1,k2 desc)) TTL = '%s'";
        String ttl = "PHOENIX_ROW_TIMESTAMP() < CURRENT_DATE() - 100";
        String tableName = generateUniqueName();
        String ddl = String.format(ddlTemplate, tableName, ttl);
        try (Connection conn = DriverManager.getConnection(getUrl(), PropertiesUtil.deepCopy(TEST_PROPERTIES))) {
            conn.createStatement().execute(ddl);
            assertConditonTTL(conn, tableName, ttl);
        }
    }

    @Test
    public void testBooleanCaseExpression() throws SQLException {
        String ddlTemplate = "create table %s (k1 bigint not null, k2 bigint not null, col1 varchar, status char(1) " +
                "constraint pk primary key (k1,k2 desc)) TTL = '%s'";
        String ttl = "CASE WHEN status = ''E'' THEN TRUE ELSE FALSE END";
        String expectedTTLExpr = "CASE WHEN status = 'E' THEN TRUE ELSE FALSE END";
        String tableName = generateUniqueName();
        String ddl = String.format(ddlTemplate, tableName, ttl);
        try (Connection conn = DriverManager.getConnection(getUrl(), PropertiesUtil.deepCopy(TEST_PROPERTIES))) {
            conn.createStatement().execute(ddl);
            assertConditonTTL(conn, tableName, expectedTTLExpr);
        }
    }

    @Test
    public void testCondTTLOnTopLevelView() throws SQLException {
        String ddlTemplate = "create table %s (k1 bigint not null primary key," +
                "k2 bigint, col1 varchar, status char(1))";
        String tableName = generateUniqueName();
        String viewName = generateUniqueName();
        String viewTemplate = "create view %s (k3 smallint) as select * from %s WHERE k1=7 TTL = '%s'";
        String ttl = "k2 = 34 and k3 = -1";
        String ddl = String.format(ddlTemplate, tableName);
        try (Connection conn = DriverManager.getConnection(getUrl(), PropertiesUtil.deepCopy(TEST_PROPERTIES))) {
            conn.createStatement().execute(ddl);
            ddl = String.format(viewTemplate, viewName, tableName, ttl);
            conn.createStatement().execute(ddl);
            assertConditonTTL(conn, tableName, TTLExpression.TTL_EXPRESSION_NOT_DEFINED);
            assertConditonTTL(conn, viewName, ttl);
        }
    }

    @Test
    public void testCondTTLOnMultiLevelView() throws SQLException {
        String ddlTemplate = "create table %s (k1 bigint not null primary key," +
                "k2 bigint, col1 varchar, status char(1))";
        String tableName = generateUniqueName();
        String parentView = generateUniqueName();
        String childView = generateUniqueName();
        String parentViewTemplate = "create view %s (k3 smallint) as select * from %s WHERE k1=7";
        String childViewTemplate = "create view %s as select * from %s TTL = '%s'";
        String ttl = "k2 = 34 and k3 = -1";
        String ddl = String.format(ddlTemplate, tableName);
        try (Connection conn = DriverManager.getConnection(getUrl(), PropertiesUtil.deepCopy(TEST_PROPERTIES))) {
            conn.createStatement().execute(ddl);
            ddl = String.format(parentViewTemplate, parentView, tableName);
            conn.createStatement().execute(ddl);
            ddl = String.format(childViewTemplate, childView, parentView, ttl);
            conn.createStatement().execute(ddl);
            assertConditonTTL(conn, tableName, TTLExpression.TTL_EXPRESSION_NOT_DEFINED);
            assertConditonTTL(conn, parentView, TTLExpression.TTL_EXPRESSION_NOT_DEFINED);
            assertConditonTTL(conn, childView, ttl);
        }
    }
}
