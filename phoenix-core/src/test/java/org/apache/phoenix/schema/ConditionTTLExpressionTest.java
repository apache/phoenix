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
import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;

import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.filter.FilterList;
import org.apache.phoenix.compile.OrderByCompiler;
import org.apache.phoenix.compile.QueryPlan;
import org.apache.phoenix.exception.PhoenixParserException;
import org.apache.phoenix.exception.SQLExceptionCode;
import org.apache.phoenix.jdbc.PhoenixConnection;
import org.apache.phoenix.jdbc.PhoenixPreparedStatement;
import org.apache.phoenix.jdbc.PhoenixStatement;
import org.apache.phoenix.query.BaseConnectionlessQueryTest;
import org.apache.phoenix.util.PropertiesUtil;
import org.junit.Test;

public class ConditionTTLExpressionTest extends BaseConnectionlessQueryTest {

    private static void assertConditonTTL(Connection conn, String tableName, String ttlExpr) throws SQLException {
        TTLExpression expected = new ConditionTTLExpression(ttlExpr);
        assertTTL(conn, tableName, expected);
    }

    private static void assertTTL(Connection conn, String tableName, TTLExpression expected) throws SQLException {
        PTable table = conn.unwrap(PhoenixConnection.class).getTable(tableName);
        assertEquals(expected, table.getTTL());
    }

    private static void assertScanConditionTTL(Scan scan, Scan scanWithCondTTL) {
        assertEquals(scan.includeStartRow(), scanWithCondTTL.includeStartRow());
        assertArrayEquals(scan.getStartRow(), scanWithCondTTL.getStartRow());
        assertEquals(scan.includeStopRow(), scanWithCondTTL.includeStopRow());
        assertArrayEquals(scan.getStopRow(), scanWithCondTTL.getStopRow());
        Filter filter = scan.getFilter();
        Filter filterCondTTL = scanWithCondTTL.getFilter();
        assertNotNull(filter);
        assertNotNull(filterCondTTL);
        if (filter instanceof FilterList) {
            assertTrue(filterCondTTL instanceof FilterList);
            FilterList filterList = (FilterList) filter;
            FilterList filterListCondTTL = (FilterList) filterCondTTL;
            // ultimately compares the individual filters
            assertEquals(filterList, filterListCondTTL);
        } else {
            assertEquals(filter, filterCondTTL);
        }
    }

    private void compareScanWithCondTTL(Connection conn,
                                        String tableNoTTL,
                                        String tableWithTTL,
                                        String queryTemplate,
                                        String ttl) throws SQLException {
        PhoenixConnection pconn = conn.unwrap(PhoenixConnection.class);
        String query;
        // Modify the query by adding the cond ttl expression explicitly to the WHERE clause
        if (queryTemplate.toUpperCase().contains(" WHERE ")) {
            // append the cond TTL expression to the WHERE clause
            query = String.format(queryTemplate + " AND NOT (%s)", tableNoTTL, ttl);
        } else {
            // add a WHERE clause with negative cond ttl expression
            query = String.format(queryTemplate + " WHERE NOT (%s)", tableNoTTL, ttl);
        }
        PhoenixPreparedStatement pstmt = new PhoenixPreparedStatement(pconn, query);
        QueryPlan plan = pstmt.optimizeQuery();
        Scan scanNoTTL = plan.getContext().getScan();
        // now execute the same query with cond ttl expression implicitly used for masking
        query = String.format(queryTemplate, tableWithTTL);
        pstmt = new PhoenixPreparedStatement(pconn, query);
        plan = pstmt.optimizeQuery();
        Scan scanWithCondTTL = plan.getContext().getScan();
        assertScanConditionTTL(scanNoTTL, scanWithCondTTL);
    }

    @Test
    public void testBasicExpression() throws SQLException {
        String ddlTemplate = "create table %s (k1 bigint not null, k2 bigint not null, col1 varchar, col2 date " +
                "constraint pk primary key (k1,k2 desc))";
        String ddlTemplateWithTTL = ddlTemplate + " TTL = '%s'";
        String tableNoTTL = generateUniqueName();
        String tableWithTTL = generateUniqueName();
        String quotedValue = "k1 > 5 AND col1 < ''zzzzzz''";
        String ttl = "k1 > 5 AND col1 < 'zzzzzz'";
        try (Connection conn = DriverManager.getConnection(getUrl(), PropertiesUtil.deepCopy(TEST_PROPERTIES))) {
            String ddl = String.format(ddlTemplate, tableNoTTL);
            conn.createStatement().execute(ddl);
            ddl = String.format(ddlTemplateWithTTL, tableWithTTL, quotedValue);
            conn.createStatement().execute(ddl);
            assertConditonTTL(conn, tableWithTTL, ttl);
            String queryTemplate = "SELECT k1, k2, col1, col2 from %s where k1 > 3";
            compareScanWithCondTTL(conn, tableNoTTL, tableWithTTL, queryTemplate, ttl);
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
                "constraint pk primary key (k1,k2 desc))";
        String ddlTemplateWithTTL = ddlTemplate + " TTL = '%s'";
        String tableNoTTL = generateUniqueName();
        String tableWithTTL = generateUniqueName();
        String ttl = "col1 is NULL AND col2 < CURRENT_DATE() + 30000";
        try (Connection conn = DriverManager.getConnection(getUrl(), PropertiesUtil.deepCopy(TEST_PROPERTIES))) {
            String ddl = String.format(ddlTemplate, tableNoTTL);
            conn.createStatement().execute(ddl);
            ddl = String.format(ddlTemplateWithTTL, tableWithTTL, ttl);
            conn.createStatement().execute(ddl);
            assertConditonTTL(conn, tableWithTTL, ttl);
            String queryTemplate = "SELECT * from %s";
            compareScanWithCondTTL(conn, tableNoTTL, tableWithTTL, queryTemplate, ttl);
        }
    }

    @Test
    public void testBooleanColumn() throws SQLException {
        String ddlTemplate = "create table %s (k1 bigint not null, k2 bigint not null, val varchar, expired BOOLEAN " +
                "constraint pk primary key (k1,k2 desc))";
        String ddlTemplateWithTTL = ddlTemplate + " TTL = '%s'";
        String tableNoTTL = generateUniqueName();
        String indexNoTTL = "I_" + tableNoTTL;
        String tableWithTTL = generateUniqueName();
        String indexWithTTL = "I_" + tableWithTTL;
        String indexTemplate = "create index %s on %s (val) include (expired)";
        String ttl = "expired";
        try (Connection conn = DriverManager.getConnection(getUrl(), PropertiesUtil.deepCopy(TEST_PROPERTIES))) {
            String ddl = String.format(ddlTemplate, tableNoTTL);
            conn.createStatement().execute(ddl);
            ddl = String.format(ddlTemplateWithTTL, tableWithTTL, ttl);
            conn.createStatement().execute(ddl);
            assertConditonTTL(conn, tableWithTTL, ttl);

            String queryTemplate = "SELECT k1, k2 from %s where (k1,k2) IN ((1,2), (3,4))";
            compareScanWithCondTTL(conn, tableNoTTL, tableWithTTL, queryTemplate, ttl);

            queryTemplate = "SELECT COUNT(*) from %s";
            compareScanWithCondTTL(conn, tableNoTTL, tableWithTTL, queryTemplate, ttl);

            ddl = String.format(indexTemplate, indexNoTTL, tableNoTTL);
            conn.createStatement().execute(ddl);
            ddl = String.format(indexTemplate, indexWithTTL, tableWithTTL);
            conn.createStatement().execute(ddl);
            assertTTL(conn, indexNoTTL, TTLExpression.TTL_EXPRESSION_NOT_DEFINED);
            assertConditonTTL(conn, indexWithTTL, ttl);
            compareScanWithCondTTL(conn, tableNoTTL, tableWithTTL, queryTemplate, ttl);
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
                "constraint pk primary key (k1,k2 desc))";
        String ddlTemplateWithTTL = ddlTemplate + " TTL = '%s'";
        String tableNoTTL = generateUniqueName();
        String tableWithTTL = generateUniqueName();
        String ttl = "PHOENIX_ROW_TIMESTAMP() < CURRENT_DATE() - 100";
        try (Connection conn = DriverManager.getConnection(getUrl(), PropertiesUtil.deepCopy(TEST_PROPERTIES))) {
            String ddl = String.format(ddlTemplate, tableNoTTL);
            conn.createStatement().execute(ddl);
            ddl = String.format(ddlTemplateWithTTL, tableWithTTL, ttl);
            conn.createStatement().execute(ddl);
            assertConditonTTL(conn, tableWithTTL, ttl);
            String queryTemplate = "select * from %s where k1 = 7 AND k2 > 12";
            compareScanWithCondTTL(conn, tableNoTTL, tableWithTTL, queryTemplate, ttl);
            queryTemplate = "select * from %s where k1 = 7 AND k2 > 12 AND col1 = 'abc'";
            compareScanWithCondTTL(conn, tableNoTTL, tableWithTTL, queryTemplate, ttl);
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
        String tableName1 = generateUniqueName();
        String tableName2 = generateUniqueName();
        String viewNameNoTTL = generateUniqueName();
        String viewNameWithTTL = generateUniqueName();
        String viewTemplate = "create view %s (k3 smallint) as select * from %s WHERE k1=7 TTL = '%s'";
        String ttl = "k2 = 34 and k3 = -1";
        try (Connection conn = DriverManager.getConnection(getUrl(), PropertiesUtil.deepCopy(TEST_PROPERTIES))) {
            String ddl = String.format(ddlTemplate, tableName1);
            conn.createStatement().execute(ddl);
            ddl = String.format(viewTemplate, viewNameWithTTL, tableName1, ttl);
            conn.createStatement().execute(ddl);
            assertTTL(conn, tableName1, TTLExpression.TTL_EXPRESSION_NOT_DEFINED);
            assertConditonTTL(conn, viewNameWithTTL, ttl);
            ddl = String.format(ddlTemplate, tableName2);
            conn.createStatement().execute(ddl);
            ddl = String.format(viewTemplate, viewNameNoTTL, tableName2, "NONE");
            conn.createStatement().execute(ddl);
            assertTTL(conn, tableName1, TTLExpression.TTL_EXPRESSION_NOT_DEFINED);
            String queryTemplate = "select * from %s";
            compareScanWithCondTTL(conn, viewNameNoTTL, viewNameWithTTL, queryTemplate, ttl);
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
            assertTTL(conn, tableName, TTLExpression.TTL_EXPRESSION_NOT_DEFINED);
            assertTTL(conn, parentView, TTLExpression.TTL_EXPRESSION_NOT_DEFINED);
            assertConditonTTL(conn, childView, ttl);
        }
    }

    @Test
    public void testRVCUsingPkColsReturnedByPlanShouldUseIndex() throws Exception {
        try (Connection conn = DriverManager.getConnection(getUrl())) {
            conn.createStatement().execute(
                    "CREATE TABLE T (k VARCHAR NOT NULL PRIMARY KEY, v1 CHAR(15), v2 VARCHAR) " +
                            "TTL='v1=''EXPIRED'''");
            conn.createStatement().execute("CREATE INDEX IDX ON T(v1, v2)");
            PhoenixStatement stmt = conn.createStatement().unwrap(PhoenixStatement.class);
            String query = "select * from t where (v1, v2, k) > ('1', '2', '3')";
            QueryPlan plan = stmt.optimizeQuery(query);
            assertEquals("IDX", plan.getTableRef().getTable().getTableName().getString());
            Scan scan = plan.getContext().getScan();
            Filter filter = scan.getFilter();
            assertEquals(filter.toString(), "NOT (TO_CHAR(\"V1\") = 'EXPIRED')");
        }
    }

    @Test
    public void testOrderByOptimizedOut() throws Exception {
        try (Connection conn = DriverManager.getConnection(getUrl())) {
            conn.createStatement().execute(
                    "CREATE TABLE foo (k VARCHAR NOT NULL PRIMARY KEY, v VARCHAR) " +
                            "IMMUTABLE_ROWS=true,TTL='v=''EXPIRED'''");
            PhoenixStatement stmt = conn.createStatement().unwrap(PhoenixStatement.class);
            QueryPlan plan = stmt.optimizeQuery("SELECT * FROM foo ORDER BY k");
            assertEquals(OrderByCompiler.OrderBy.FWD_ROW_KEY_ORDER_BY, plan.getOrderBy());
            Scan scan = plan.getContext().getScan();
            Filter filter = scan.getFilter();
            assertEquals(filter.toString(), "NOT (V = 'EXPIRED')");
        }
    }

    @Test
    public void testTableSelectionWithMultipleIndexes() throws Exception {
        try (Connection conn = DriverManager.getConnection(getUrl())) {
            conn.createStatement().execute(
                    "CREATE TABLE t (k INTEGER NOT NULL PRIMARY KEY, v1 VARCHAR, v2 VARCHAR) " +
                            "IMMUTABLE_ROWS=true,TTL='v2=''EXPIRED'''");
            conn.createStatement().execute("CREATE INDEX idx ON t(v1)");
            PhoenixStatement stmt = conn.createStatement().unwrap(PhoenixStatement.class);
            QueryPlan plan = stmt.optimizeQuery("SELECT v1 FROM t WHERE v1 = 'bar'");
            // T is chosen because TTL expression is on v2 which is not present in index
            assertEquals("T", plan.getTableRef().getTable().getTableName().getString());
            Scan scan = plan.getContext().getScan();
            Filter filter = scan.getFilter();
            assertEquals(filter.toString(), "(V1 = 'bar' AND NOT (V2 = 'EXPIRED'))");
            conn.createStatement().execute("CREATE INDEX idx2 ON t(v1,v2)");
            plan = stmt.optimizeQuery("SELECT v1 FROM t WHERE v1 = 'bar'");
            // Now IDX2 should be chosen
            assertEquals("IDX2", plan.getTableRef().getTable().getTableName().getString());
            scan = plan.getContext().getScan();
            filter = scan.getFilter();
            assertEquals(filter.toString(), "NOT (\"V2\" = 'EXPIRED')");
        }
    }
}
