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

import static org.apache.phoenix.exception.SQLExceptionCode.AGGREGATE_EXPRESSION_NOT_ALLOWED_IN_CONDITIONAL_TTL;
import static org.apache.phoenix.exception.SQLExceptionCode.CANNOT_DROP_COL_REFERENCED_IN_CONDITIONAL_TTL;
import static org.apache.phoenix.exception.SQLExceptionCode.CANNOT_SET_CONDITIONAL_TTL_ON_TABLE_WITH_MULTIPLE_COLUMN_FAMILIES;
import static org.apache.phoenix.schema.LiteralTTLExpression.TTL_EXPRESSION_NOT_DEFINED;
import static org.apache.phoenix.schema.PTableType.INDEX;
import static org.apache.phoenix.util.TestUtil.retainSingleQuotes;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.NavigableSet;

import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.phoenix.thirdparty.com.google.common.collect.Lists;
import org.apache.phoenix.compile.QueryPlan;
import org.apache.phoenix.exception.PhoenixParserException;
import org.apache.phoenix.jdbc.PhoenixConnection;
import org.apache.phoenix.jdbc.PhoenixPreparedStatement;
import org.apache.phoenix.query.BaseConnectionlessQueryTest;
import org.junit.Test;


public class ConditionalTTLExpressionTest extends BaseConnectionlessQueryTest {

    private static void assertConditonTTL(Connection conn,
                                          String tableName,
                                          String ttlExpr) throws SQLException {
        TTLExpression expected = new ConditionalTTLExpression(ttlExpr);
        assertTTL(conn, tableName, expected);
    }

    private static void assertTTL(Connection conn,
                                  String tableName,
                                  TTLExpression expected) throws SQLException {
        PTable table = conn.unwrap(PhoenixConnection.class).getTable(tableName);
        assertEquals(expected, table.getTTLExpression());
    }

    private static void assertMaskingOlderCellsForIndex(Connection conn,
                                                     String tableName,
                                                     boolean expected) throws SQLException {
        PhoenixConnection pconn = conn.unwrap(PhoenixConnection.class);
        PTable table = pconn.getTable(tableName);
        CompiledConditionalTTLExpression compiledCondExpression =
                (CompiledConditionalTTLExpression) table.getCompiledTTLExpression(pconn);
        assertEquals(expected, compiledCondExpression.isMaskingOlderCells());
    }

    private void validateScan(Connection conn,
                              String tableName,
                              String query,
                              String ttl,
                              boolean useIndex,
                              List<String> expectedColsInScan) throws SQLException {
        PhoenixConnection pconn = conn.unwrap(PhoenixConnection.class);
        PhoenixPreparedStatement pstmt = new PhoenixPreparedStatement(pconn, query);
        QueryPlan plan = pstmt.optimizeQuery();
        if (useIndex) {
            assertTrue(plan.getTableRef().getTable().getType() == INDEX);
        }
        plan.iterator(); // create the iterator to initialize the scan
        PTable table = plan.getTableRef().getTable();
        Scan scan = plan.getContext().getScan();
        Map<byte[], NavigableSet<byte[]>> familyMap = scan.getFamilyMap();
        assertTrue(!familyMap.isEmpty());
        // Conditional TTL is only supported for tables with 1 column family
        assertTrue(familyMap.size() == 1);
        byte[] family = scan.getFamilies()[0];
        assertNotNull(family);
        NavigableSet<byte[]> qualifiers = familyMap.get(family);
        if (expectedColsInScan.isEmpty()) {
            assertNull(qualifiers);
        } else {
            for (String col : expectedColsInScan) {
                PColumn column = table.getColumnForColumnName(col.toUpperCase());
                assertTrue(Bytes.equals(family, column.getFamilyName().getBytes()));
                assertTrue(qualifiers.contains(column.getColumnQualifierBytes()));
            }
        }
    }

    @Test
    public void testBasicExpression() throws SQLException {
        String ddlTemplate = "create table %s (k1 bigint not null, k2 bigint not null," +
                "col1 varchar, col2 date constraint pk primary key (k1,k2 desc)) TTL = '%s'";
        String ttl = "k1 > 5 AND col1 < 'zzzzzz'";
        String tableName = generateUniqueName();
        String ddl = String.format(ddlTemplate, tableName, retainSingleQuotes(ttl));
        try (Connection conn = DriverManager.getConnection(getUrl())) {
            conn.createStatement().execute(ddl);
            assertConditonTTL(conn, tableName, ttl);
            String query = String.format("SELECT count(*) from %s where k1 > 3", tableName);
            validateScan(conn, tableName, query, ttl, false, Lists.newArrayList("col1"));
        }
    }

    @Test(expected = TypeMismatchException.class)
    public void testNotBooleanExpr() throws SQLException {
        String ddlTemplate = "create table %s (k1 bigint not null, k2 bigint not null," +
                "col1 varchar, col2 date constraint pk primary key (k1,k2 desc)) TTL = '%s'";
        String ttl = "k1 + 100";
        String tableName = generateUniqueName();
        String ddl = String.format(ddlTemplate, tableName, ttl);
        try (Connection conn = DriverManager.getConnection(getUrl())) {
            conn.createStatement().execute(ddl);
        }
    }

    @Test(expected = TypeMismatchException.class)
    public void testWrongArgumentValue() throws SQLException {
        String ddlTemplate = "create table %s (k1 bigint not null, k2 bigint not null," +
                "col1 varchar, col2 date constraint pk primary key (k1,k2 desc)) TTL = '%s'";
        String ttl = "k1 = ''abc''";
        String tableName = generateUniqueName();
        String ddl = String.format(ddlTemplate, tableName, ttl);
        try (Connection conn = DriverManager.getConnection(getUrl())) {
            conn.createStatement().execute(ddl);
        }
    }

    @Test(expected = PhoenixParserException.class)
    public void testParsingError() throws SQLException {
        String ddlTemplate = "create table %s (k1 bigint not null, k2 bigint not null," +
                "col1 varchar, col2 date constraint pk primary key (k1,k2 desc)) TTL = '%s'";
        String ttl = "k2 == 23";
        String tableName = generateUniqueName();
        String ddl = String.format(ddlTemplate, tableName, ttl);
        try (Connection conn = DriverManager.getConnection(getUrl())) {
            conn.createStatement().execute(ddl);
        }
    }

    @Test
    public void testMultipleColumnFamilyNotAllowed() throws SQLException {
        String ddlTemplate = "create table %s (k1 bigint not null, k2 bigint not null," +
                "A.col1 varchar, col2 date constraint pk primary key (k1,k2 desc)) TTL = '%s'";
        String ttl = "A.col1 = 'expired'";
        String tableName = generateUniqueName();
        String ddl = String.format(ddlTemplate, tableName, retainSingleQuotes(ttl));
        try (Connection conn = DriverManager.getConnection(getUrl())) {
            conn.createStatement().execute(ddl);
            fail();
        } catch (SQLException e) {
            assertEquals(
                CANNOT_SET_CONDITIONAL_TTL_ON_TABLE_WITH_MULTIPLE_COLUMN_FAMILIES.getErrorCode(),
                e.getErrorCode()
            );
        } catch (Exception e) {
            fail("Unknown exception " + e);
        }
    }

    @Test
    public void testSingleNonDefaultColumnFamilyIsAllowed() throws SQLException {
        String ddlTemplate = "create table %s (k1 bigint not null, k2 bigint not null," +
                "A.col1 varchar, A.col2 date constraint pk primary key (k1,k2 desc)) TTL = '%s'";
        String ttl = "col1 = 'expired' AND col2 + 10 > CURRENT_DATE()";
        String tableName = generateUniqueName();
        String ddl = String.format(ddlTemplate, tableName, retainSingleQuotes(ttl));
        try (Connection conn = DriverManager.getConnection(getUrl())) {
            conn.createStatement().execute(ddl);
            assertConditonTTL(conn, tableName, ttl);
            // create global index
            String indexName = "I_" + generateUniqueName();
            ddl = String.format("create index %s on %s (col2) include(col1)",
                    indexName, tableName);
            conn.createStatement().execute(ddl);
            assertConditonTTL(conn, indexName, ttl);
            // create local index
            indexName = "L_" + generateUniqueName();
            ddl = String.format("create local index %s on %s (col2) include(col1)",
                    indexName, tableName);
            conn.createStatement().execute(ddl);
            assertConditonTTL(conn, indexName, ttl);
        }
    }

    @Test
    public void testDefaultColumnFamily() throws SQLException {
        String ddlTemplate = "create table %s (k1 bigint not null, k2 bigint not null," +
                "col1 varchar, col2 date constraint pk primary key (k1,k2 desc)) TTL = '%s'," +
                "DEFAULT_COLUMN_FAMILY='CF'";
        String ttl = "col1 = 'expired' AND CF.col2 + 10 > CURRENT_DATE()";
        String tableName = "T_" + generateUniqueName();
        String ddl = String.format(ddlTemplate, tableName, retainSingleQuotes(ttl));
        try (Connection conn = DriverManager.getConnection(getUrl())) {
            conn.createStatement().execute(ddl);
            assertConditonTTL(conn, tableName, ttl);
            // create view
            String viewName = "GV_" + generateUniqueName();
            ddl = String.format("create view %s (col3 varchar) as select * from %s where k1 = 2",
                    viewName, tableName);
            conn.createStatement().execute(ddl);
            assertConditonTTL(conn, viewName, ttl);
            // create global index
            String indexName = "I_" + generateUniqueName();
            ddl = String.format("create index %s on %s (col2) include(col1)",
                    indexName, tableName);
            conn.createStatement().execute(ddl);
            assertConditonTTL(conn, indexName, ttl);
            // create local index
            indexName = "L_" + generateUniqueName();
            ddl = String.format("create local index %s on %s (col2) include(col1)",
                    indexName, tableName);
            conn.createStatement().execute(ddl);
            assertConditonTTL(conn, indexName, ttl);
        }
    }

    @Test
    public void testMaskingOlderCellsOfIndex() throws SQLException {
        boolean[] mutable = { true, false};
        for (boolean isMutable : mutable) {
            String ddlTemplate = "create table %s (k1 bigint not null, k2 bigint not null," +
                    "col1 varchar, col2 date constraint pk primary key (k1,k2 desc)) TTL = '%s'" +
                    (!isMutable ? ", IMMUTABLE_ROWS=true" : "");
            String ttl = "col1 = 'expired' AND col2 + 10 > CURRENT_DATE()";
            String tableName = "T_" + generateUniqueName();
            String ddl = String.format(ddlTemplate, tableName, retainSingleQuotes(ttl));
            try (Connection conn = DriverManager.getConnection(getUrl())) {
                conn.createStatement().execute(ddl);
                assertMaskingOlderCellsForIndex(conn, tableName, false); // table
                // create view
                String viewName = "GV_" + generateUniqueName();
                ddl = String.format("create view %s (col3 varchar) as " +
                        "select * from %s where k1 = 2", viewName, tableName);
                conn.createStatement().execute(ddl);
                assertMaskingOlderCellsForIndex(conn, viewName, false); // view
                // create global index
                String indexName = "I_" + generateUniqueName();
                ddl = String.format("create index %s on %s (col2) include(col1)",
                        indexName, tableName);
                conn.createStatement().execute(ddl);
                // only mutable global index
                assertMaskingOlderCellsForIndex(conn, indexName, isMutable);
                // create local index
                indexName = "L_" + generateUniqueName();
                ddl = String.format("create local index %s on %s (col2) include(col1)",
                        indexName, tableName);
                conn.createStatement().execute(ddl);
                assertMaskingOlderCellsForIndex(conn, indexName, false); // local index
            }
        }
    }

    @Test
    public void testMultipleColumnFamilyNotAllowedOnAlter() throws SQLException {
        String ddlTemplate = "create table %s (k1 bigint not null, k2 bigint not null," +
                "A.col1 varchar, col2 date constraint pk primary key (k1,k2 desc))";
        String ttl = "A.col1 = 'expired'";
        String tableName = generateUniqueName();
        String ddl = String.format(ddlTemplate, tableName);
        try (Connection conn = DriverManager.getConnection(getUrl())) {
            conn.createStatement().execute(ddl);
            String alterDDL = String.format("alter table %s set TTL = '%s'",
                    tableName, retainSingleQuotes(ttl));
            conn.createStatement().execute(alterDDL);
            fail();
        } catch (SQLException e) {
            assertEquals(
                CANNOT_SET_CONDITIONAL_TTL_ON_TABLE_WITH_MULTIPLE_COLUMN_FAMILIES.getErrorCode(),
                e.getErrorCode()
            );
        } catch (Exception e) {
            fail("Unknown exception " + e);
        }
    }

    @Test
    public void testMultipleColumnFamilyNotAllowedOnAddColumn() throws SQLException {
        String ddlTemplate = "create table %s (k1 bigint not null, k2 bigint not null," +
                "A.col1 varchar, A.col2 date constraint pk primary key (k1,k2 desc)) TTL = '%s'";
        String ttl = "A.col1 = 'expired'";
        String tableName = generateUniqueName();
        String ddl = String.format(ddlTemplate, tableName, retainSingleQuotes(ttl));
        try (Connection conn = DriverManager.getConnection(getUrl())) {
            conn.createStatement().execute(ddl);
            // add a new column in a different column family
            String alterDDL = String.format("alter table %s add col3 varchar", tableName);
            try {
                conn.createStatement().execute(alterDDL);
                fail();
            } catch (SQLException e) {
                assertEquals(CANNOT_SET_CONDITIONAL_TTL_ON_TABLE_WITH_MULTIPLE_COLUMN_FAMILIES
                                .getErrorCode(), e.getErrorCode());
            } catch (Exception e) {
                fail("Unknown exception " + e);
            }
            alterDDL = String.format("alter table %s add A.col3 varchar", tableName);
            conn.createStatement().execute(alterDDL);
        }
    }

    @Test
    public void testMultipleColumnFamilyNotAllowedOnAddColumn2() throws SQLException {
        String ddlTemplate = "create table %s (k1 bigint not null, k2 bigint not null," +
                "col1 varchar, col2 date constraint pk primary key (k1,k2 desc)) TTL = '%s'";
        String ttl = "col1 = 'expired'";
        String tableName = generateUniqueName();
        String ddl = String.format(ddlTemplate, tableName, retainSingleQuotes(ttl));
        try (Connection conn = DriverManager.getConnection(getUrl())) {
            conn.createStatement().execute(ddl);
            // add a new column in a different column family
            String alterDDL = String.format("alter table %s add A.col3 varchar", tableName);
            try {
                conn.createStatement().execute(alterDDL);
                fail();
            } catch (SQLException e) {
                assertEquals(CANNOT_SET_CONDITIONAL_TTL_ON_TABLE_WITH_MULTIPLE_COLUMN_FAMILIES
                                .getErrorCode(), e.getErrorCode());
            } catch (Exception e) {
                fail("Unknown exception " + e);
            }
            alterDDL = String.format("alter table %s add col3 varchar", tableName);
            conn.createStatement().execute(alterDDL);
        }
    }

    @Test
    public void testMultipleColumnFamilyNotAllowedOnAddColumn3() throws SQLException {
        String ddlTemplate = "create table %s (k1 bigint not null, k2 bigint not null," +
                "col1 varchar, col2 date constraint pk primary key (k1,k2 desc)) TTL = '%s'," +
                "DEFAULT_COLUMN_FAMILY='CF'";
        String ttl = "col1 = 'expired' AND CF.col2 + 10 > CURRENT_DATE()";
        String tableName = generateUniqueName();
        String ddl = String.format(ddlTemplate, tableName, retainSingleQuotes(ttl));
        try (Connection conn = DriverManager.getConnection(getUrl())) {
            conn.createStatement().execute(ddl);
            assertConditonTTL(conn, tableName, ttl);
            // add a new column in a different column family
            String alterDDL = String.format("alter table %s add A.col3 varchar", tableName);
            try {
                conn.createStatement().execute(alterDDL);
                fail();
            } catch (SQLException e) {
                assertEquals(CANNOT_SET_CONDITIONAL_TTL_ON_TABLE_WITH_MULTIPLE_COLUMN_FAMILIES
                                .getErrorCode(), e.getErrorCode());
            } catch (Exception e) {
                fail("Unknown exception " + e);
            }
            alterDDL = String.format("alter table %s add col3 varchar", tableName);
            conn.createStatement().execute(alterDDL);
        }
    }

    @Test
    public void testDropColumnNotAllowed() throws SQLException {
        String ddlTemplate = "create table %s (k1 bigint not null, k2 bigint not null," +
                "col1 varchar, col2 date constraint pk primary key (k1,k2 desc)) TTL = '%s'";
        String ttl = "col1 = 'expired' AND col2 + 30 < CURRENT_DATE()";
        String tableName = generateUniqueName();
        String ddl = String.format(ddlTemplate, tableName, retainSingleQuotes(ttl));
        try (Connection conn = DriverManager.getConnection(getUrl())) {
            conn.createStatement().execute(ddl);
            // drop column referenced in TTL expression
            String alterDDL = String.format("alter table %s drop column col2", tableName);
            try {
                conn.createStatement().execute(alterDDL);
                fail();
            } catch (SQLException e) {
                assertEquals(CANNOT_DROP_COL_REFERENCED_IN_CONDITIONAL_TTL.getErrorCode(),
                        e.getErrorCode());
            } catch (Exception e) {
                fail("Unknown exception " + e);
            }
        }
    }

    @Test
    public void testAggregateExpressionNotAllowed() throws SQLException {
        String ddlTemplate = "create table %s (k1 bigint not null, k2 bigint not null," +
                "col1 varchar, col2 date constraint pk primary key (k1,k2 desc)) TTL = '%s'";
        String ttl = "SUM(k2) > 23";
        String tableName = generateUniqueName();
        String ddl = String.format(ddlTemplate, tableName, ttl);
        try (Connection conn = DriverManager.getConnection(getUrl())) {
            conn.createStatement().execute(ddl);
            fail();
        } catch (SQLException e) {
            assertEquals(
                AGGREGATE_EXPRESSION_NOT_ALLOWED_IN_CONDITIONAL_TTL.getErrorCode(),
                e.getErrorCode());
        } catch (Exception e) {
            fail("Unknown exception " + e);
        }
    }

    @Test
    public void testNullExpression() throws SQLException {
        String ddlTemplate = "create table %s (k1 bigint not null, k2 bigint not null," +
                "col1 varchar, col2 date constraint pk primary key (k1,k2 desc)) TTL = '%s'";
        String tableName = generateUniqueName();
        String ttl = "col1 is NULL AND col2 < CURRENT_DATE() + 30000";
        String ddl = String.format(ddlTemplate, tableName, ttl);
        try (Connection conn = DriverManager.getConnection(getUrl())) {
            conn.createStatement().execute(ddl);
            assertConditonTTL(conn, tableName, ttl);
            String query = String.format("SELECT count(*) from %s", tableName);
            validateScan(conn, tableName, query, ttl, false, Lists.newArrayList("col1", "col2"));
        }
    }

    @Test
    public void testBooleanColumn() throws SQLException {
        String ddlTemplate = "create table %s (k1 bigint not null, k2 bigint not null," +
                "val varchar, expired BOOLEAN constraint pk primary key (k1,k2 desc)) TTL = '%s'";
        String tableName = generateUniqueName();
        String indexName = "I_" + tableName;
        String indexTemplate = "create index %s on %s (val) include (expired)";
        String ttl = "expired";
        String query;
        try (Connection conn = DriverManager.getConnection(getUrl())) {
            String ddl = String.format(ddlTemplate, tableName, ttl);
            conn.createStatement().execute(ddl);
            assertConditonTTL(conn, tableName, ttl);

            query = String.format("SELECT k1, k2 from %s where (k1,k2) IN ((1,2), (3,4))",
                    tableName);
            validateScan(conn, tableName, query, ttl, false, Lists.newArrayList("expired"));

            ddl = String.format(indexTemplate, indexName, tableName);
            conn.createStatement().execute(ddl);
            assertConditonTTL(conn, indexName, ttl);

            // validate the scan on index
            query = String.format("SELECT count(*) from %s", tableName);
            validateScan(conn, tableName, query, ttl, true, Lists.newArrayList("0:expired"));
        }
    }

    @Test
    public void testNot() throws SQLException {
        String ddlTemplate = "create table %s (k1 bigint not null, k2 bigint not null," +
                "expired BOOLEAN constraint pk primary key (k1,k2 desc)) TTL = '%s'";
        String ttl = "NOT expired";
        String tableName = generateUniqueName();
        String ddl = String.format(ddlTemplate, tableName, ttl);
        try (Connection conn = DriverManager.getConnection(getUrl())) {
            conn.createStatement().execute(ddl);
            assertConditonTTL(conn, tableName, ttl);
        }
    }

    @Test
    public void testPhoenixRowTimestamp() throws SQLException {
        String ddlTemplate = "create table %s (k1 bigint not null, k2 bigint not null," +
                "col1 varchar, col2 date constraint pk primary key (k1,k2 desc)) TTL = '%s'";
        String tableName = generateUniqueName();
        String ttl = "PHOENIX_ROW_TIMESTAMP() < CURRENT_DATE() - 100";
        String ddl = String.format(ddlTemplate, tableName, ttl);
        String query;
        try (Connection conn = DriverManager.getConnection(getUrl())) {
            conn.createStatement().execute(ddl);
            assertConditonTTL(conn, tableName, ttl);
            query = String.format("select col1 from %s where k1 = 7 AND k2 > 12", tableName);
            validateScan(conn, tableName, query, ttl, false, Lists.newArrayList("col1"));
        }
    }

    @Test
    public void testBooleanCaseExpression() throws SQLException {
        String ddlTemplate = "create table %s (k1 bigint not null, k2 bigint not null," +
                "col1 varchar, status char(1) constraint pk primary key (k1,k2 desc)) TTL = '%s'";
        String ttl = "CASE WHEN status = ''E'' THEN TRUE ELSE FALSE END";
        String expectedTTLExpr = "CASE WHEN status = 'E' THEN TRUE ELSE FALSE END";
        String tableName = generateUniqueName();
        String ddl = String.format(ddlTemplate, tableName, ttl);
        try (Connection conn = DriverManager.getConnection(getUrl())) {
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
        String viewTemplate = "create view %s (k3 smallint) as select * from %s WHERE k1=7 " +
                "TTL = '%s'";
        String ttl = "k2 = 34 and k3 = -1";
        try (Connection conn = DriverManager.getConnection(getUrl())) {
            String ddl = String.format(ddlTemplate, tableName);
            conn.createStatement().execute(ddl);
            ddl = String.format(viewTemplate, viewName, tableName, ttl);
            conn.createStatement().execute(ddl);
            assertTTL(conn, tableName, TTL_EXPRESSION_NOT_DEFINED);
            assertConditonTTL(conn, viewName, ttl);
            String query = String.format("select k3 from %s", viewName);
            validateScan(conn, viewName, query, ttl, false, Lists.newArrayList("k2", "k3"));
        }
    }

    @Test
    public void testCondTTLOnMultiLevelView() throws SQLException {
        String ddlTemplate = "create table %s (k1 bigint not null primary key," +
                "k2 bigint, col1 varchar, status char(1))";
        String tableName = generateUniqueName();
        String parentView = generateUniqueName();
        String childView = generateUniqueName();
        String indexName = generateUniqueName();
        String parentViewTemplate = "create view %s (k3 smallint) as select * from %s WHERE k1=7";
        String childViewTemplate = "create view %s as select * from %s TTL = '%s'";
        String indexOnChildTemplate = "create index %s ON %s (k3) include (col1, k2)";
        String ttl = "k2 = 34 and k3 = -1";
        String ddl = String.format(ddlTemplate, tableName);
        try (Connection conn = DriverManager.getConnection(getUrl())) {
            conn.createStatement().execute(ddl);
            ddl = String.format(parentViewTemplate, parentView, tableName);
            conn.createStatement().execute(ddl);
            ddl = String.format(childViewTemplate, childView, parentView, ttl);
            conn.createStatement().execute(ddl);
            assertTTL(conn, tableName, TTL_EXPRESSION_NOT_DEFINED);
            assertTTL(conn, parentView, TTL_EXPRESSION_NOT_DEFINED);
            assertConditonTTL(conn, childView, ttl);
            // create an index on child view
            ddl = String.format(indexOnChildTemplate, indexName, childView);
            conn.createStatement().execute(ddl);
            assertConditonTTL(conn, indexName, ttl);
        }
    }

    @Test
    public void testInListTTLExpr() throws Exception {
        String ddlTemplate = "create table %s (id varchar not null primary key, " +
                "col1 integer, col2 varchar) TTL = '%s'";
        String tableName = generateUniqueName();
        String ttl = "col2 IN ('expired', 'cancelled')";
        String query;
        try (Connection conn = DriverManager.getConnection(getUrl())) {
            String ddl = String.format(ddlTemplate, tableName, retainSingleQuotes(ttl));
            conn.createStatement().execute(ddl);
            assertConditonTTL(conn, tableName, ttl);
            query = String.format("select col1 from %s where id IN ('abc', 'fff')", tableName);
            validateScan(conn, tableName, query, ttl, false, Lists.newArrayList("col1", "col2"));
        }
    }

    @Test
    public void testPartialIndex() throws Exception {
        String ddlTemplate = "create table %s (id varchar not null primary key, " +
                "col1 integer, col2 integer, col3 double, col4 varchar) TTL = '%s'";
        String tableName = generateUniqueName();
        String indexTemplate = "create index %s on %s (col1) " +
                "include (col2, col3, col4) where col1 > 50";
        String indexName = generateUniqueName();
        String ttl = "col2 > 100 AND col4='expired'";
        String query;
        try (Connection conn = DriverManager.getConnection(getUrl())) {
            String ddl = String.format(ddlTemplate, tableName, retainSingleQuotes(ttl));
            conn.createStatement().execute(ddl);
            assertConditonTTL(conn, tableName, ttl);
            ddl = String.format(indexTemplate, indexName, tableName);
            conn.createStatement().execute(ddl);
            assertConditonTTL(conn, indexName, ttl);
            query = String.format("select col3 from %s where col1 > 60", tableName);
            validateScan(conn, tableName, query, ttl, true,
                    Lists.newArrayList("0:col2", "0:col3", "0:col4"));
        }
    }

    @Test
    public void testUncoveredIndex() throws Exception {
        String ddlTemplate = "create table %s (id varchar not null primary key, " +
                "col1 integer, col2 integer, col3 double, col4 varchar) TTL = '%s'";
        String tableName = generateUniqueName();
        String indexTemplate = "create uncovered index %s on %s (col1) ";
        String indexName = generateUniqueName();
        String ttl = "col2 > 100 AND col4='expired'";
        String query;
        try (Connection conn = DriverManager.getConnection(getUrl())) {
            String ddl = String.format(ddlTemplate, tableName, retainSingleQuotes(ttl));
            conn.createStatement().execute(ddl);
            assertConditonTTL(conn, tableName, ttl);
            ddl = String.format(indexTemplate, indexName, tableName);
            try {
                conn.createStatement().execute(ddl);
                fail("Should have thrown ColumnNotFoundException");
            } catch (SQLException e) {
                assertTrue(e.getCause() instanceof ColumnNotFoundException);
            }
            indexTemplate = "create uncovered index %s on %s (col4, col2) ";
            ddl = String.format(indexTemplate, indexName, tableName);
            conn.createStatement().execute(ddl);
            assertConditonTTL(conn, indexName, ttl);
        }
    }

    @Test
    public void testCreatingIndexWithMissingExprCols() throws Exception {
        String ddlTemplate = "create table %s (id varchar not null primary key, " +
                "col1 integer, col2 integer, col3 double, col4 varchar) TTL = '%s'";
        String tableName = generateUniqueName();
        String indexTemplate = "create index %s on %s (col1) include (col2)";
        String indexName = generateUniqueName();
        String ttl = "col2 > 100 AND col4='expired'";
        try (Connection conn = DriverManager.getConnection(getUrl())) {
            String ddl = String.format(ddlTemplate, tableName, retainSingleQuotes(ttl));
            conn.createStatement().execute(ddl);
            ddl = String.format(indexTemplate, indexName, tableName);
            try {
                conn.createStatement().execute(ddl);
                fail("Should have thrown ColumnNotFoundException");
            } catch (SQLException e) {
                assertTrue(e.getCause() instanceof ColumnNotFoundException);
            }
        }
    }

    @Test
    public void testSettingCondTTLOnTableWithIndexWithMissingExprCols() throws Exception {
        String ddlTemplate = "create table %s (id varchar not null primary key, " +
                "col1 integer, col2 integer, col3 double, col4 varchar)";
        String tableName = generateUniqueName();
        String indexTemplate = "create index %s on %s (col1) include (col2)";
        String indexName = generateUniqueName();
        String ttl = "col2 > 100 AND col4='expired'";
        try (Connection conn = DriverManager.getConnection(getUrl())) {
            String ddl = String.format(ddlTemplate, tableName);
            conn.createStatement().execute(ddl);
            ddl = String.format(indexTemplate, indexName, tableName);
            conn.createStatement().execute(ddl);
            ddl = String.format("alter table %s set TTL = '%s'",
                    tableName, retainSingleQuotes(ttl));
            try {
                conn.createStatement().execute(ddl);
                fail("Should have thrown ColumnNotFoundException");
            } catch (SQLException e) {
                assertTrue(e.getCause() instanceof ColumnNotFoundException);
            }
        }
    }

    @Test
    public void testScanColumns() throws Exception {
        String ddlTemplate = "create table %s (k1 bigint not null, k2 bigint not null," +
                "col1 varchar, col2 date constraint pk primary key (k1,k2 desc)) " +
                "COLUMN_ENCODED_BYTES=0, TTL = '%s'";
        String ttl = "k1 > 5 AND col1 < 'zzzzzz'";
        String tableName = generateUniqueName();
        String ddl = String.format(ddlTemplate, tableName, retainSingleQuotes(ttl));
        try (Connection conn = DriverManager.getConnection(getUrl())) {
            conn.createStatement().execute(ddl);
            assertConditonTTL(conn, tableName, ttl);
            String query = String.format("select * from %s where k1 > 3", tableName);
            // select * so all columns should be read
            validateScan(conn, tableName, query, ttl, false, Collections.EMPTY_LIST);
            // all non pk columns in ttl expression should be added to scan
            query = String.format("select count(*) from %s where k1 > 3", tableName);
            validateScan(conn, tableName, query, ttl, false, Lists.newArrayList("col1"));
            // all non pk columns in ttl expression should be added
            query = String.format("select k1, k2 from %s where k1 > 3", tableName);
            validateScan(conn, tableName, query, ttl, false, Lists.newArrayList("col1"));
            // non pk columns and other non pk columns in ttl expression should be added to scan
            query = String.format("select col1, col2 from %s where k1 > 3", tableName);
            validateScan(conn, tableName, query, ttl, false, Lists.newArrayList("col1", "col2"));
        }
    }
}
