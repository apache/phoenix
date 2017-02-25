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
package org.apache.phoenix.compile;

import static org.apache.phoenix.jdbc.PhoenixDatabaseMetaData.SYSTEM_CATALOG_SCHEMA;
import static org.apache.phoenix.jdbc.PhoenixDatabaseMetaData.SYSTEM_STATS_TABLE;
import static org.apache.phoenix.util.TestUtil.TEST_PROPERTIES;
import static org.apache.phoenix.util.TestUtil.assertDegenerate;
import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.math.BigDecimal;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Properties;

import org.apache.hadoop.hbase.HRegionLocation;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.filter.FirstKeyOnlyFilter;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.phoenix.compile.OrderByCompiler.OrderBy;
import org.apache.phoenix.coprocessor.BaseScannerRegionObserver;
import org.apache.phoenix.exception.SQLExceptionCode;
import org.apache.phoenix.execute.HashJoinPlan;
import org.apache.phoenix.expression.Expression;
import org.apache.phoenix.expression.LiteralExpression;
import org.apache.phoenix.expression.aggregator.Aggregator;
import org.apache.phoenix.expression.aggregator.CountAggregator;
import org.apache.phoenix.expression.aggregator.ServerAggregators;
import org.apache.phoenix.expression.function.TimeUnit;
import org.apache.phoenix.filter.ColumnProjectionFilter;
import org.apache.phoenix.filter.EncodedQualifiersColumnProjectionFilter;
import org.apache.phoenix.jdbc.PhoenixConnection;
import org.apache.phoenix.jdbc.PhoenixDatabaseMetaData;
import org.apache.phoenix.jdbc.PhoenixPreparedStatement;
import org.apache.phoenix.jdbc.PhoenixStatement;
import org.apache.phoenix.query.BaseConnectionlessQueryTest;
import org.apache.phoenix.query.QueryConstants;
import org.apache.phoenix.query.QueryServices;
import org.apache.phoenix.schema.AmbiguousColumnException;
import org.apache.phoenix.schema.ColumnAlreadyExistsException;
import org.apache.phoenix.schema.ColumnNotFoundException;
import org.apache.phoenix.schema.PColumn;
import org.apache.phoenix.schema.PTable;
import org.apache.phoenix.schema.PTableKey;
import org.apache.phoenix.schema.SortOrder;
import org.apache.phoenix.schema.types.PChar;
import org.apache.phoenix.schema.types.PDecimal;
import org.apache.phoenix.schema.types.PInteger;
import org.apache.phoenix.schema.types.PVarchar;
import org.apache.phoenix.util.ByteUtil;
import org.apache.phoenix.util.PhoenixRuntime;
import org.apache.phoenix.util.PropertiesUtil;
import org.apache.phoenix.util.QueryUtil;
import org.apache.phoenix.util.ScanUtil;
import org.apache.phoenix.util.SchemaUtil;
import org.junit.Test;



/**
 * 
 * Tests for compiling a query
 * The compilation stage finds additional errors that can't be found at parse
 * time so this is a good place for negative tests (since the mini-cluster
 * is not necessary enabling the tests to run faster).
 *
 * 
 * @since 0.1
 */
@edu.umd.cs.findbugs.annotations.SuppressWarnings(
        value="RV_RETURN_VALUE_IGNORED",
        justification="Test code.")
public class QueryCompilerTest extends BaseConnectionlessQueryTest {

    @Test
    public void testParameterUnbound() throws Exception {
        try {
            String query = "SELECT a_string, b_string FROM atable WHERE organization_id=? and a_integer = ?";
            Properties props = PropertiesUtil.deepCopy(TEST_PROPERTIES);
            Connection conn = DriverManager.getConnection(getUrl(), props);
            try {
                PreparedStatement statement = conn.prepareStatement(query);
                statement.setString(1, "00D300000000XHP");
                statement.executeQuery();
                fail();
            } finally {
                conn.close();
            }
        } catch (SQLException e) {
            assertTrue(e.getMessage().contains("Parameter 2 is unbound"));
        }
    }

    @Test
    public void testMultiPKDef() throws Exception {
        Connection conn = DriverManager.getConnection(getUrl());
        try {
            String query = "CREATE TABLE foo (pk1 integer not null primary key, pk2 bigint not null primary key)";
            PreparedStatement statement = conn.prepareStatement(query);
            statement.execute();
            fail();
        } catch (SQLException e) {
            assertTrue(e.getMessage(), e.getMessage().contains("ERROR 510 (42889): The table already has a primary key. columnName=PK2"));
        } finally {
            conn.close();
        }
    }

    @Test
    public void testPKDefAndPKConstraint() throws Exception {
        Connection conn = DriverManager.getConnection(getUrl());
        try {
            String query = "CREATE TABLE foo (pk integer not null primary key, col1 decimal, col2 decimal constraint my_pk primary key (col1,col2))";
            PreparedStatement statement = conn.prepareStatement(query);
            statement.execute();
            fail();
        } catch (SQLException e) {
            assertTrue(e.getMessage(), e.getMessage().contains("ERROR 510 (42889): The table already has a primary key. columnName=PK"));
        } finally {
            conn.close();
        }
    }

    @Test
    public void testFamilyNameInPK() throws Exception {
        Connection conn = DriverManager.getConnection(getUrl());
        try {
            String query = "CREATE TABLE foo (a.pk integer not null primary key, col1 decimal, col2 decimal)";
            PreparedStatement statement = conn.prepareStatement(query);
            statement.execute();
            fail();
        } catch (SQLException e) {
            assertEquals(e.getErrorCode(), SQLExceptionCode.PRIMARY_KEY_WITH_FAMILY_NAME.getErrorCode());
        } finally {
            conn.close();
        }
    }

    @Test
    public void testSameColumnNameInPKAndNonPK() throws Exception {
        Connection conn = DriverManager.getConnection(getUrl());
        try {
            String query = "CREATE TABLE t1 (k integer not null primary key, a.k decimal, b.k decimal)";
            conn.createStatement().execute(query);
            PhoenixConnection pconn = conn.unwrap(PhoenixConnection.class);
            PColumn c = pconn.getTable(new PTableKey(pconn.getTenantId(), "T1")).getColumnForColumnName("K");
            assertTrue(SchemaUtil.isPKColumn(c));
        } finally {
            conn.close();
        }
    }

    @Test
    public void testVarBinaryNotLastInMultipartPK() throws Exception {
        Connection conn = DriverManager.getConnection(getUrl());
        // When the VARBINARY key is the last column, it is allowed.
        String query = "CREATE TABLE foo (a_string varchar not null, b_string varchar not null, a_binary varbinary not null, " +
                "col1 decimal, col2 decimal CONSTRAINT pk PRIMARY KEY (a_string, b_string, a_binary))";
        PreparedStatement statement = conn.prepareStatement(query);
        statement.execute();
        try {
            // VARBINARY key is not allowed in the middle of the key.
            query = "CREATE TABLE foo (a_binary varbinary not null, a_string varchar not null, col1 decimal, col2 decimal CONSTRAINT pk PRIMARY KEY (a_binary, a_string))";
            statement = conn.prepareStatement(query);
            statement.execute();
            fail();
        } catch (SQLException e) {
            assertEquals(SQLExceptionCode.VARBINARY_IN_ROW_KEY.getErrorCode(), e.getErrorCode());
        } finally {
            conn.close();
        }
    }

    @Test
    public void testArrayNotLastInMultipartPK() throws Exception {
        Connection conn = DriverManager.getConnection(getUrl());
        // When the VARBINARY key is the last column, it is allowed.
        String query = "CREATE TABLE foo (a_string varchar not null, b_string varchar not null, a_array varchar[] not null, " +
                "col1 decimal, col2 decimal CONSTRAINT pk PRIMARY KEY (a_string, b_string, a_array))";
        PreparedStatement statement = conn.prepareStatement(query);
        statement.execute();
        try {
            // VARBINARY key is not allowed in the middle of the key.
            query = "CREATE TABLE foo (a_array varchar[] not null, a_string varchar not null, col1 decimal, col2 decimal CONSTRAINT pk PRIMARY KEY (a_array, a_string))";
            statement = conn.prepareStatement(query);
            statement.execute();
            fail();
        } catch (SQLException e) {
            assertEquals(SQLExceptionCode.VARBINARY_IN_ROW_KEY.getErrorCode(), e.getErrorCode());
        } finally {
            conn.close();
        }
    }

    @Test
    public void testNoPK() throws Exception {
        Connection conn = DriverManager.getConnection(getUrl());
        try {
            String query = "CREATE TABLE foo (pk integer not null, col1 decimal, col2 decimal)";
            PreparedStatement statement = conn.prepareStatement(query);
            statement.execute();
            fail();
        } catch (SQLException e) {
            assertEquals(SQLExceptionCode.INVALID_NOT_NULL_CONSTRAINT.getErrorCode(), e.getErrorCode());
        } finally {
            conn.close();
        }
    }

    @Test
    public void testUnknownFamilyNameInTableOption() throws Exception {
        Connection conn = DriverManager.getConnection(getUrl());
        try {
            String query = "CREATE TABLE foo (pk integer not null primary key, a.col1 decimal, b.col2 decimal) c.my_property='foo'";
            PreparedStatement statement = conn.prepareStatement(query);
            statement.execute();
            fail();
        } catch (SQLException e) {
            assertTrue(e.getMessage().contains("Properties may not be defined for an unused family name"));
        } finally {
            conn.close();
        }
    }

    @Test
    public void testInvalidGroupedAggregation() throws Exception {
        try {
            // Select non agg column in aggregate query
            String query = "SELECT count(1),a_integer FROM atable WHERE organization_id=? GROUP BY a_string";
            Properties props = PropertiesUtil.deepCopy(TEST_PROPERTIES);
            Connection conn = DriverManager.getConnection(getUrl(), props);
            try {
                PreparedStatement statement = conn.prepareStatement(query);
                statement.setString(1, "00D300000000XHP");
                statement.executeQuery();
                fail();
            } finally {
                conn.close();
            }
        } catch (SQLException e) {
            assertTrue(e.getMessage(), e.getMessage().contains("ERROR 1018 (42Y27): Aggregate may not contain columns not in GROUP BY. A_INTEGER"));
        }
    }

    @Test
    public void testInvalidGroupExpressionAggregation() throws Exception {
        try {
            // Select non agg column in aggregate query
            String query = "SELECT sum(a_integer) + a_integer FROM atable WHERE organization_id=? GROUP BY a_string";
            Properties props = PropertiesUtil.deepCopy(TEST_PROPERTIES);
            Connection conn = DriverManager.getConnection(getUrl(), props);
            try {
                PreparedStatement statement = conn.prepareStatement(query);
                statement.setString(1, "00D300000000XHP");
                statement.executeQuery();
                fail();
            } finally {
                conn.close();
            }
        } catch (SQLException e) {
            assertTrue(e.getMessage(), e.getMessage().contains("ERROR 1018 (42Y27): Aggregate may not contain columns not in GROUP BY. A_INTEGER"));
        }
    }

    @Test
    public void testAggInWhereClause() throws Exception {
        try {
            // Select non agg column in aggregate query
            String query = "SELECT a_integer FROM atable WHERE organization_id=? AND count(1) > 2";
            Properties props = PropertiesUtil.deepCopy(TEST_PROPERTIES);
            Connection conn = DriverManager.getConnection(getUrl(), props);
            try {
                PreparedStatement statement = conn.prepareStatement(query);
                statement.setString(1, "00D300000000XHP");
                statement.executeQuery();
                fail();
            } finally {
                conn.close();
            }
        } catch (SQLException e) {
            assertTrue(e.getMessage(), e.getMessage().contains("ERROR 1017 (42Y26): Aggregate may not be used in WHERE."));
        }
    }

    @Test
    public void testHavingAggregateQuery() throws Exception {
        try {
            // Select non agg column in aggregate query
            String query = "SELECT a_integer FROM atable WHERE organization_id=? HAVING count(1) > 2";
            Properties props = PropertiesUtil.deepCopy(TEST_PROPERTIES);
            Connection conn = DriverManager.getConnection(getUrl(), props);
            try {
                PreparedStatement statement = conn.prepareStatement(query);
                statement.setString(1, "00D300000000XHP");
                statement.executeQuery();
                fail();
            } finally {
                conn.close();
            }
        } catch (SQLException e) {
            assertTrue(e.getMessage(), e.getMessage().contains("ERROR 1018 (42Y27): Aggregate may not contain columns not in GROUP BY. A_INTEGER"));
        }
    }

    @Test
    public void testNonAggInHavingClause() throws Exception {
        try {
            // Select non agg column in aggregate query
            String query = "SELECT a_integer FROM atable WHERE organization_id=? HAVING a_integer = 5";
            Properties props = PropertiesUtil.deepCopy(TEST_PROPERTIES);
            Connection conn = DriverManager.getConnection(getUrl(), props);
            try {
                PreparedStatement statement = conn.prepareStatement(query);
                statement.setString(1, "00D300000000XHP");
                statement.executeQuery();
                fail();
            } finally {
                conn.close();
            }
        } catch (SQLException e) {
            assertTrue(e.getMessage(), e.getMessage().contains("ERROR 1019 (42Y26): Only aggregate maybe used in the HAVING clause."));
        }
    }

    @Test
    public void testTypeMismatchInCase() throws Exception {
        try {
            // Select non agg column in aggregate query
            String query = "SELECT a_integer FROM atable WHERE organization_id=? HAVING CASE WHEN a_integer <= 2 THEN 'foo' WHEN a_integer = 3 THEN 2 WHEN a_integer <= 5 THEN 5 ELSE 5 END  = 5";
            Properties props = PropertiesUtil.deepCopy(TEST_PROPERTIES);
            Connection conn = DriverManager.getConnection(getUrl(), props);
            try {
                PreparedStatement statement = conn.prepareStatement(query);
                statement.setString(1, "00D300000000XHP");
                statement.executeQuery();
                fail();
            } finally {
                conn.close();
            }
        } catch (SQLException e) {
            assertTrue(e.getMessage().contains("Case expressions must have common type"));
        }
    }

    @Test
    public void testNonBooleanWhereExpression() throws Exception {
        try {
            // Select non agg column in aggregate query
            String query = "SELECT a_integer FROM atable WHERE organization_id=? and CASE WHEN a_integer <= 2 THEN 'foo' WHEN a_integer = 3 THEN 'bar' WHEN a_integer <= 5 THEN 'bas' ELSE 'blah' END";
            Properties props = PropertiesUtil.deepCopy(TEST_PROPERTIES);
            Connection conn = DriverManager.getConnection(getUrl(), props);
            try {
                PreparedStatement statement = conn.prepareStatement(query);
                statement.setString(1, "00D300000000XHP");
                statement.executeQuery();
                fail();
            } finally {
                conn.close();
            }
        } catch (SQLException e) {
            assertTrue(e.getMessage().contains("ERROR 203 (22005): Type mismatch. BOOLEAN and VARCHAR for CASE WHEN A_INTEGER <= 2 THEN 'foo'WHEN A_INTEGER = 3 THEN 'bar'WHEN A_INTEGER <= 5 THEN 'bas' ELSE 'blah' END"));
        }
    }

    @Test
    public void testNoSCNInConnectionProps() throws Exception {
        Properties props = new Properties();
        DriverManager.getConnection(getUrl(), props);
    }
    

    @Test
    public void testPercentileWrongQueryWithMixOfAggrAndNonAggrExps() throws Exception {
        String query = "select a_integer, PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY a_integer ASC) from ATABLE";
        try {
            compileQuery(query, Collections.emptyList());
            fail();
        } catch (SQLException e) {
            assertEquals("ERROR 1018 (42Y27): Aggregate may not contain columns not in GROUP BY. A_INTEGER",
                    e.getMessage());
        }
    }

    @Test
    public void testPercentileWrongQuery1() throws Exception {
        String query = "select PERCENTILE_CONT('*') WITHIN GROUP (ORDER BY a_integer ASC) from ATABLE";
        try {
            compileQuery(query, Collections.emptyList());
            fail();
        } catch (SQLException e) {
            assertEquals(
                    "ERROR 203 (22005): Type mismatch. expected: [DECIMAL] but was: VARCHAR at PERCENTILE_CONT argument 3",
                    e.getMessage());
        }
    }

    @Test
    public void testPercentileWrongQuery2() throws Exception {
        String query = "select PERCENTILE_CONT(1.1) WITHIN GROUP (ORDER BY a_integer ASC) from ATABLE";
        try {
            compileQuery(query, Collections.emptyList());
            fail();
        } catch (SQLException e) {
            assertEquals(
                    "ERROR 213 (22003): Value outside range. expected: [0 , 1] but was: 1.1 at PERCENTILE_CONT argument 3",
                    e.getMessage());
        }
    }

    @Test
    public void testPercentileWrongQuery3() throws Exception {
        String query = "select PERCENTILE_CONT(-1) WITHIN GROUP (ORDER BY a_integer ASC) from ATABLE";
        try {
            compileQuery(query, Collections.emptyList());
            fail();
        } catch (Exception e) {
            assertEquals(
                    "ERROR 213 (22003): Value outside range. expected: [0 , 1] but was: -1 at PERCENTILE_CONT argument 3",
                    e.getMessage());
        }
    }

    private Scan compileQuery(String query, List<Object> binds) throws SQLException {
        QueryPlan plan = getQueryPlan(query, binds);
        return plan.getContext().getScan();
    }
    
    private Scan projectQuery(String query) throws SQLException {
        QueryPlan plan = getQueryPlan(query, Collections.emptyList());
        plan.iterator(); // Forces projection
        return plan.getContext().getScan();
    }
    
    private QueryPlan getOptimizedQueryPlan(String query) throws SQLException {
        return getOptimizedQueryPlan(query, Collections.emptyList());
    }

    private QueryPlan getOptimizedQueryPlan(String query, List<Object> binds) throws SQLException {
        Properties props = PropertiesUtil.deepCopy(TEST_PROPERTIES);
        Connection conn = DriverManager.getConnection(getUrl(), props);
        try {
            PhoenixPreparedStatement statement = conn.prepareStatement(query).unwrap(PhoenixPreparedStatement.class);
            for (Object bind : binds) {
                statement.setObject(1, bind);
            }
            QueryPlan plan = statement.optimizeQuery(query);
            return plan;
        } finally {
            conn.close();
        }
    }
    
    private QueryPlan getQueryPlan(String query, List<Object> binds) throws SQLException {
        Properties props = PropertiesUtil.deepCopy(TEST_PROPERTIES);
        Connection conn = DriverManager.getConnection(getUrl(), props);
        try {
            PhoenixPreparedStatement statement = conn.prepareStatement(query).unwrap(PhoenixPreparedStatement.class);
            for (Object bind : binds) {
                statement.setObject(1, bind);
            }
            QueryPlan plan = statement.compileQuery(query);
            return plan;
        } finally {
            conn.close();
        }
    }
    
    @Test
    public void testKeyOrderedGroupByOptimization() throws Exception {
        // Select columns in PK
        String[] queries = new String[] {
            "SELECT count(1) FROM atable GROUP BY organization_id,entity_id",
            "SELECT count(1) FROM atable GROUP BY organization_id,substr(entity_id,1,3),entity_id",
            "SELECT count(1) FROM atable GROUP BY entity_id,organization_id",
            "SELECT count(1) FROM atable GROUP BY substr(entity_id,1,3),organization_id",
            "SELECT count(1) FROM ptsdb GROUP BY host,inst,round(\"DATE\",'HOUR')",
            "SELECT count(1) FROM atable GROUP BY organization_id",
        };
        List<Object> binds = Collections.emptyList();
        for (String query : queries) {
            QueryPlan plan = getQueryPlan(query, binds);
            assertEquals(query, BaseScannerRegionObserver.KEY_ORDERED_GROUP_BY_EXPRESSIONS, plan.getGroupBy().getScanAttribName());
        }
    }

    @Test
    public void testNullInScanKey() throws Exception {
        // Select columns in PK
        String query = "select val from ptsdb where inst is null and host='a'";
        List<Object> binds = Collections.emptyList();
        Scan scan = compileQuery(query, binds);
        // Projects column family with not null column
        assertNull(scan.getFilter());
        assertEquals(1,scan.getFamilyMap().keySet().size());
        assertArrayEquals(Bytes.toBytes(SchemaUtil.normalizeIdentifier(QueryConstants.DEFAULT_COLUMN_FAMILY)), scan.getFamilyMap().keySet().iterator().next());
    }

    @Test
    public void testOnlyNullInScanKey() throws Exception {
        // Select columns in PK
        String query = "select val from ptsdb where inst is null";
        List<Object> binds = Collections.emptyList();
        Scan scan = compileQuery(query, binds);
        // Projects column family with not null column
        assertEquals(1,scan.getFamilyMap().keySet().size());
        assertArrayEquals(Bytes.toBytes(SchemaUtil.normalizeIdentifier(QueryConstants.DEFAULT_COLUMN_FAMILY)), scan.getFamilyMap().keySet().iterator().next());
    }

    @Test
    public void testIsNullOnNotNullable() throws Exception {
        // Select columns in PK
        String query = "select a_string from atable where entity_id is null";
        List<Object> binds = Collections.emptyList();
        Scan scan = compileQuery(query, binds);
        assertDegenerate(scan);
    }

    @Test
    public void testIsNotNullOnNotNullable() throws Exception {
        // Select columns in PK
        String query = "select a_string from atable where entity_id is not null";
        List<Object> binds = Collections.emptyList();
        Scan scan = compileQuery(query, binds);
        assertNull(scan.getFilter());
        assertTrue(scan.getStartRow().length == 0);
        assertTrue(scan.getStopRow().length == 0);
    }

    @Test
    public void testUpsertTypeMismatch() throws Exception {
        try {
            // Select non agg column in aggregate query
            String query = "upsert into ATABLE VALUES (?, ?, ?)";
            Properties props = PropertiesUtil.deepCopy(TEST_PROPERTIES);
            Connection conn = DriverManager.getConnection(getUrl(), props);
            try {
                PreparedStatement statement = conn.prepareStatement(query);
                statement.setString(1, "00D300000000XHP");
                statement.setString(2, "00D300000000XHP");
                statement.setInt(3, 1);
                statement.executeUpdate();
                fail();
            } finally {
                conn.close();
            }
        } catch (SQLException e) { // TODO: use error codes
            assertTrue(e.getMessage().contains("Type mismatch"));
        }
    }

    @Test
    public void testUpsertMultiByteIntoChar() throws Exception {
        try {
            // Select non agg column in aggregate query
            String query = "upsert into ATABLE VALUES (?, ?, ?)";
            Properties props = PropertiesUtil.deepCopy(TEST_PROPERTIES);
            Connection conn = DriverManager.getConnection(getUrl(), props);
            try {
                PreparedStatement statement = conn.prepareStatement(query);
                statement.setString(1, "00D300000000XHP");
                statement.setString(2, "繰り返し曜日マスク");
                statement.setInt(3, 1);
                statement.executeUpdate();
                fail();
            } finally {
                conn.close();
            }
        } catch (SQLException e) {
            assertTrue(e.getMessage(), e.getMessage().contains("ERROR 201 (22000): Illegal data."));
            assertTrue(e.getMessage().contains("CHAR types may only contain single byte characters"));
        }
    }

    @Test
    public void testSelectStarOnGroupBy() throws Exception {
        try {
            // Select non agg column in aggregate query
            String query = "select * from ATABLE group by a_string";
            Properties props = PropertiesUtil.deepCopy(TEST_PROPERTIES);
            Connection conn = DriverManager.getConnection(getUrl(), props);
            try {
                PreparedStatement statement = conn.prepareStatement(query);
                statement.executeQuery();
                fail();
            } finally {
                conn.close();
            }
        } catch (SQLException e) {
            assertTrue(e.getMessage(), e.getMessage().contains("ERROR 1018 (42Y27): Aggregate may not contain columns not in GROUP BY."));
        }
    }

    @Test
    public void testOrderByAggSelectNonAgg() throws Exception {
        try {
            // Order by in select with no limit or group by
            String query = "select a_string from ATABLE order by max(b_string)";
            Properties props = PropertiesUtil.deepCopy(TEST_PROPERTIES);
            Connection conn = DriverManager.getConnection(getUrl(), props);
            try {
                PreparedStatement statement = conn.prepareStatement(query);
                statement.executeQuery();
                fail();
            } finally {
                conn.close();
            }
        } catch (SQLException e) {
            assertTrue(e.getMessage(), e.getMessage().contains("ERROR 1018 (42Y27): Aggregate may not contain columns not in GROUP BY. A_STRING"));
        }
    }

    @Test
    public void testOrderByAggAndNonAgg() throws Exception {
        try {
            // Order by in select with no limit or group by
            String query = "select max(a_string) from ATABLE order by max(b_string),a_string";
            Properties props = PropertiesUtil.deepCopy(TEST_PROPERTIES);
            Connection conn = DriverManager.getConnection(getUrl(), props);
            try {
                PreparedStatement statement = conn.prepareStatement(query);
                statement.executeQuery();
                fail();
            } finally {
                conn.close();
            }
        } catch (SQLException e) {
            assertTrue(e.getMessage(), e.getMessage().contains("ERROR 1018 (42Y27): Aggregate may not contain columns not in GROUP BY. A_STRING"));
        }
    }

    @Test
    public void testOrderByNonAggSelectAgg() throws Exception {
        try {
            // Order by in select with no limit or group by
            String query = "select max(a_string) from ATABLE order by b_string LIMIT 5";
            Properties props = PropertiesUtil.deepCopy(TEST_PROPERTIES);
            Connection conn = DriverManager.getConnection(getUrl(), props);
            try {
                PreparedStatement statement = conn.prepareStatement(query);
                statement.executeQuery();
                fail();
            } finally {
                conn.close();
            }
        } catch (SQLException e) {
            assertTrue(e.getMessage(), e.getMessage().contains("ERROR 1018 (42Y27): Aggregate may not contain columns not in GROUP BY. B_STRING"));
        }
    }

    @Test
    public void testNotKeyOrderedGroupByOptimization() throws Exception {
        // Select columns in PK
        String[] queries = new String[] {
            "SELECT count(1) FROM atable GROUP BY entity_id",
            "SELECT count(1) FROM atable GROUP BY substr(organization_id,2,3)",
            "SELECT count(1) FROM atable GROUP BY substr(entity_id,1,3)",
            "SELECT count(1) FROM atable GROUP BY to_date(organization_id)",
            "SELECT count(1) FROM atable GROUP BY regexp_substr(organization_id, '.*foo.*'),entity_id",
            "SELECT count(1) FROM atable GROUP BY substr(organization_id,1),entity_id",
        };
        List<Object> binds = Collections.emptyList();
        for (String query : queries) {
            QueryPlan plan = getQueryPlan(query, binds);
            assertEquals(plan.getGroupBy().getScanAttribName(), BaseScannerRegionObserver.UNORDERED_GROUP_BY_EXPRESSIONS);
        }
    }
    
    @Test
    public void testFunkyColumnNames() throws Exception {
        // Select columns in PK
        String[] queries = new String[] {
            "SELECT \"foo!\",\"foo.bar-bas\",\"#@$\",\"_blah^\" FROM FUNKY_NAMES",
            "SELECT count(\"foo!\"),\"_blah^\" FROM FUNKY_NAMES WHERE \"foo.bar-bas\"='x' GROUP BY \"#@$\",\"_blah^\"",
        };
        List<Object> binds = Collections.emptyList();
        for (String query : queries) {
            compileQuery(query, binds);
        }
    }
    
    @Test
    public void testCountAggregatorFirst() throws Exception {
        String[] queries = new String[] {
            "SELECT sum(2.5),organization_id FROM atable GROUP BY organization_id,entity_id",
            "SELECT avg(a_integer) FROM atable GROUP BY organization_id,substr(entity_id,1,3),entity_id",
            "SELECT count(a_string) FROM atable GROUP BY substr(organization_id,1),entity_id",
            "SELECT min('foo') FROM atable GROUP BY entity_id,organization_id",
            "SELECT min('foo'),sum(a_integer),avg(2.5),4.5,max(b_string) FROM atable GROUP BY substr(organization_id,1),entity_id",
            "SELECT sum(2.5) FROM atable",
            "SELECT avg(a_integer) FROM atable",
            "SELECT count(a_string) FROM atable",
            "SELECT min('foo') FROM atable LIMIT 5",
            "SELECT min('foo'),sum(a_integer),avg(2.5),4.5,max(b_string) FROM atable",
        };
        List<Object> binds = Collections.emptyList();
        String query = null;
        try {
            for (int i = 0; i < queries.length; i++) {
                query = queries[i];
                Scan scan = compileQuery(query, binds);
                ServerAggregators aggregators = ServerAggregators.deserialize(scan.getAttribute(BaseScannerRegionObserver.AGGREGATORS), null);
                Aggregator aggregator = aggregators.getAggregators()[0];
                assertTrue(aggregator instanceof CountAggregator);
            }
        } catch (Exception e) {
            throw new Exception(query, e);
        }
    }

    @Test
    public void testInvalidArithmetic() throws Exception {
        String[] queries = new String[] { 
                "SELECT entity_id,organization_id FROM atable where A_STRING - 5.5 < 0",
                "SELECT entity_id,organization_id FROM atable where A_DATE - 'transaction' < 0",
                "SELECT entity_id,organization_id FROM atable where A_DATE * 45 < 0",
                "SELECT entity_id,organization_id FROM atable where A_DATE / 45 < 0",
                "SELECT entity_id,organization_id FROM atable where 45 - A_DATE < 0",
                "SELECT entity_id,organization_id FROM atable where A_DATE - to_date('2000-01-01 12:00:00') < to_date('2000-02-01 12:00:00')", // RHS must be number
                "SELECT entity_id,organization_id FROM atable where A_DATE - A_DATE + 1 < A_DATE", // RHS must be number
                "SELECT entity_id,organization_id FROM atable where A_DATE + 2 < 0", // RHS must be date
                "SELECT entity_id,organization_id FROM atable where 45.5 - A_DATE < 0",
                "SELECT entity_id,organization_id FROM atable where 1 + A_DATE + A_DATE < A_DATE",
                "SELECT entity_id,organization_id FROM atable where A_STRING - 45 < 0",
                "SELECT entity_id,organization_id FROM atable where A_STRING / 45 < 0",
                "SELECT entity_id,organization_id FROM atable where A_STRING * 45 < 0",
                "SELECT entity_id,organization_id FROM atable where A_STRING + 45 < 0",
                "SELECT entity_id,organization_id FROM atable where A_STRING - 45 < 0",
                "SELECT entity_id,organization_id FROM atable where A_STRING - 'transaction' < 0", };

        Properties props = PropertiesUtil.deepCopy(TEST_PROPERTIES);
        Connection conn = DriverManager.getConnection(getUrl(), props);
        for (String query : queries) {
            try {
                PreparedStatement statement = conn.prepareStatement(query);
                statement.executeQuery();
                fail(query);
            } catch (SQLException e) {
                if (e.getMessage().contains("ERROR 203 (22005): Type mismatch.")) {
                    continue;
                }
                throw new IllegalStateException("Didn't find type mismatch: " + query, e);
            }
        }
    }
    
    
    @Test
    public void testAmbiguousColumn() throws Exception {
        long ts = nextTimestamp();
        String query = "SELECT * from multi_cf G where RESPONSE_TIME = 2222";
        String url = getUrl() + ";" + PhoenixRuntime.CURRENT_SCN_ATTRIB + "=" + (ts + 5); // Run query at timestamp 5
        Properties props = PropertiesUtil.deepCopy(TEST_PROPERTIES);
        Connection conn = DriverManager.getConnection(url, props);
        try {
            PreparedStatement statement = conn.prepareStatement(query);
            statement.executeQuery();
            fail();
        } catch (AmbiguousColumnException e) { // expected
        } finally {
            conn.close();
        }
    }

    @Test
    public void testTableAliasMatchesCFName() throws Exception {
        long ts = nextTimestamp();
        String query = "SELECT F.RESPONSE_TIME,G.RESPONSE_TIME from multi_cf G where G.RESPONSE_TIME-1 = F.RESPONSE_TIME";
        String url = getUrl() + ";" + PhoenixRuntime.CURRENT_SCN_ATTRIB + "=" + (ts + 5); // Run query at timestamp 5
        Properties props = PropertiesUtil.deepCopy(TEST_PROPERTIES);
        Connection conn = DriverManager.getConnection(url, props);
        try {
            PreparedStatement statement = conn.prepareStatement(query);
            statement.executeQuery();
            fail();
        } catch (AmbiguousColumnException e) { // expected
        } finally {
            conn.close();
        }
    }

    @Test
    public void testCoelesceFunctionTypeMismatch() throws Exception {
        long ts = nextTimestamp();
        String query = "SELECT coalesce(x_integer,'foo') from atable";
        String url = getUrl() + ";" + PhoenixRuntime.CURRENT_SCN_ATTRIB + "=" + (ts + 5); // Run query at timestamp 5
        Properties props = PropertiesUtil.deepCopy(TEST_PROPERTIES);
        Connection conn = DriverManager.getConnection(url, props);
        try {
            PreparedStatement statement = conn.prepareStatement(query);
            statement.executeQuery();
            fail();
        } catch (SQLException e) { // expected
            assertTrue(e.getMessage(), e.getMessage().contains("ERROR 203 (22005): Type mismatch. COALESCE expected INTEGER, but got VARCHAR"));
        } finally {
            conn.close();
        }
    }

    @Test
    public void testOrderByNotInSelectDistinct() throws Exception {
        long ts = nextTimestamp();
        String query = "SELECT distinct a_string,b_string from atable order by x_integer";
        String url = getUrl() + ";" + PhoenixRuntime.CURRENT_SCN_ATTRIB + "=" + (ts + 5); // Run query at timestamp 5
        Properties props = PropertiesUtil.deepCopy(TEST_PROPERTIES);
        Connection conn = DriverManager.getConnection(url, props);
        try {
            PreparedStatement statement = conn.prepareStatement(query);
            statement.executeQuery();
            fail();
        } catch (SQLException e) { // expected
            assertEquals(SQLExceptionCode.ORDER_BY_NOT_IN_SELECT_DISTINCT.getErrorCode(), e.getErrorCode());
        } finally {
            conn.close();
        }
    }

    @Test
    public void testSelectDistinctAndAll() throws Exception {
        long ts = nextTimestamp();
        String query = "SELECT all distinct a_string,b_string from atable order by x_integer";
        String url = getUrl() + ";" + PhoenixRuntime.CURRENT_SCN_ATTRIB + "=" + (ts + 5); // Run query at timestamp 5
        Properties props = PropertiesUtil.deepCopy(TEST_PROPERTIES);
        Connection conn = DriverManager.getConnection(url, props);
        try {
            PreparedStatement statement = conn.prepareStatement(query);
            statement.executeQuery();
            fail();
        } catch (SQLException e) { // expected
            assertEquals(SQLExceptionCode.PARSER_ERROR.getErrorCode(), e.getErrorCode());
        } finally {
            conn.close();
        }
    }

    @Test
    public void testSelectDistinctAndOrderBy() throws Exception {
        long ts = nextTimestamp();
        String query = "select /*+ RANGE_SCAN */ count(distinct organization_id) from atable order by organization_id";
        String query1 = "select count(distinct organization_id) from atable order by organization_id";
        String url = getUrl() + ";" + PhoenixRuntime.CURRENT_SCN_ATTRIB + "=" + (ts + 5); // Run query at timestamp 5
        Properties props = PropertiesUtil.deepCopy(TEST_PROPERTIES);
        Connection conn = DriverManager.getConnection(url, props);
        try {
            PreparedStatement statement = conn.prepareStatement(query);
            statement.executeQuery();
            fail();
        } catch (SQLException e) { // expected
            assertEquals(SQLExceptionCode.AGGREGATE_WITH_NOT_GROUP_BY_COLUMN.getErrorCode(), e.getErrorCode());
        }
        try {
            PreparedStatement statement = conn.prepareStatement(query1);
            statement.executeQuery();
            fail();
        } catch (SQLException e) { // expected
            assertEquals(SQLExceptionCode.AGGREGATE_WITH_NOT_GROUP_BY_COLUMN.getErrorCode(), e.getErrorCode());
        }
        conn.close();
    }

    @Test
    public void testOrderByNotInSelectDistinctAgg() throws Exception {
        long ts = nextTimestamp();
        String query = "SELECT distinct count(1) from atable order by x_integer";
        String url = getUrl() + ";" + PhoenixRuntime.CURRENT_SCN_ATTRIB + "=" + (ts + 5); // Run query at timestamp 5
        Properties props = PropertiesUtil.deepCopy(TEST_PROPERTIES);
        Connection conn = DriverManager.getConnection(url, props);
        try {
            PreparedStatement statement = conn.prepareStatement(query);
            statement.executeQuery();
            fail();
        } catch (SQLException e) { // expected
            assertEquals(SQLExceptionCode.ORDER_BY_NOT_IN_SELECT_DISTINCT.getErrorCode(), e.getErrorCode());
        } finally {
            conn.close();
        }
    }

    @Test
    public void testSelectDistinctWithAggregation() throws Exception {
        long ts = nextTimestamp();
        String query = "SELECT distinct a_string,count(*) from atable";
        String url = getUrl() + ";" + PhoenixRuntime.CURRENT_SCN_ATTRIB + "=" + (ts + 5); // Run query at timestamp 5
        Properties props = PropertiesUtil.deepCopy(TEST_PROPERTIES);
        Connection conn = DriverManager.getConnection(url, props);
        try {
            PreparedStatement statement = conn.prepareStatement(query);
            statement.executeQuery();
            fail();
        } catch (SQLException e) { // expected
            assertEquals(SQLExceptionCode.AGGREGATE_WITH_NOT_GROUP_BY_COLUMN.getErrorCode(), e.getErrorCode());
        } finally {
            conn.close();
        }
    }

    @Test
    public void testAggregateOnColumnsNotInGroupByForImmutableEncodedTable() throws Exception {
        String tableName = generateUniqueName();
        String ddl = "CREATE IMMUTABLE TABLE  " + tableName +
                "  (a_string varchar not null, col1 integer, col2 integer" +
                "  CONSTRAINT pk PRIMARY KEY (a_string))";
        String query = "SELECT col1, max(a_string) from " + tableName + " group by col2";
        try (Connection conn = DriverManager.getConnection(getUrl())) {
            conn.createStatement().execute(ddl);
            try {
                PreparedStatement statement = conn.prepareStatement(query);
                statement.executeQuery();
                fail();
            } catch (SQLException e) { // expected
                assertEquals(SQLExceptionCode.AGGREGATE_WITH_NOT_GROUP_BY_COLUMN.getErrorCode(), e.getErrorCode());
            }
        }
    }

    @Test 
    public void testRegexpSubstrSetScanKeys() throws Exception {
        // First test scan keys are set when the offset is 0 or 1. 
        String query = "SELECT host FROM ptsdb WHERE regexp_substr(inst, '[a-zA-Z]+') = 'abc'";
        List<Object> binds = Collections.emptyList();
        Scan scan = compileQuery(query, binds);
        assertArrayEquals(Bytes.toBytes("abc"), scan.getStartRow());
        assertArrayEquals(ByteUtil.nextKey(Bytes.toBytes("abc")),scan.getStopRow());
        assertTrue(scan.getFilter() != null);

        query = "SELECT host FROM ptsdb WHERE regexp_substr(inst, '[a-zA-Z]+', 0) = 'abc'";
        binds = Collections.emptyList();
        scan = compileQuery(query, binds);
        assertArrayEquals(Bytes.toBytes("abc"), scan.getStartRow());
        assertArrayEquals(ByteUtil.nextKey(Bytes.toBytes("abc")), scan.getStopRow());
        assertTrue(scan.getFilter() != null);

        // Test scan keys are not set when the offset is not 0 or 1.
        query = "SELECT host FROM ptsdb WHERE regexp_substr(inst, '[a-zA-Z]+', 3) = 'abc'";
        binds = Collections.emptyList();
        scan = compileQuery(query, binds);
        assertTrue(scan.getStartRow().length == 0);
        assertTrue(scan.getStopRow().length == 0);
        assertTrue(scan.getFilter() != null);
    }
    
    @Test
    public void testStringConcatExpression() throws Exception {
        long ts = nextTimestamp();
        String query = "SELECT entity_id,a_string FROM atable where 2 || a_integer || ? like '2%'";
        String url = getUrl() + ";" + PhoenixRuntime.CURRENT_SCN_ATTRIB + "=" + (ts + 5); // Run query at timestamp 5
        Properties props = PropertiesUtil.deepCopy(TEST_PROPERTIES);
        Connection conn = DriverManager.getConnection(url, props);
        byte []x=new byte[]{127,127,0,0};//Binary data
        try {
            PreparedStatement statement = conn.prepareStatement(query);
            statement.setBytes(1, x);
            statement.executeQuery();
            fail();
        } catch (SQLException e) { // expected
            assertTrue(e.getMessage().contains("Concatenation does not support"));
        } finally {
            conn.close();
        }
    }
    
    @Test
    public void testDivideByBigDecimalZero() throws Exception {
        long ts = nextTimestamp();
        String query = "SELECT a_integer/x_integer/0.0 FROM atable";
        String url = getUrl() + ";" + PhoenixRuntime.CURRENT_SCN_ATTRIB + "=" + (ts + 5); // Run query at timestamp 5
        Connection conn = DriverManager.getConnection(url);
        try {
            PreparedStatement statement = conn.prepareStatement(query);
            statement.executeQuery();
            fail();
        } catch (SQLException e) { // expected
            assertTrue(e.getMessage().contains("Divide by zero"));
        } finally {
            conn.close();
        }
    }

    @Test
    public void testDivideByIntegerZero() throws Exception {
        long ts = nextTimestamp();
        String query = "SELECT a_integer/0 FROM atable";
        String url = getUrl() + ";" + PhoenixRuntime.CURRENT_SCN_ATTRIB + "=" + (ts + 5); // Run query at timestamp 5
        Connection conn = DriverManager.getConnection(url);
        try {
            PreparedStatement statement = conn.prepareStatement(query);
            statement.executeQuery();
            fail();
        } catch (SQLException e) { // expected
            assertTrue(e.getMessage().contains("Divide by zero"));
        } finally {
            conn.close();
        }
    }

    @Test
    public void testCreateNullableInPKMiddle() throws Exception {
        long ts = nextTimestamp();
        String query = "CREATE TABLE foo(i integer not null, j integer null, k integer not null CONSTRAINT pk PRIMARY KEY(i,j,k))";
        String url = getUrl() + ";" + PhoenixRuntime.CURRENT_SCN_ATTRIB + "=" + (ts + 5); // Run query at timestamp 5
        Connection conn = DriverManager.getConnection(url);
        try {
            PreparedStatement statement = conn.prepareStatement(query);
            statement.execute();
            fail();
        } catch (SQLException e) { // expected
            assertTrue(e.getMessage().contains("PK columns may not be both fixed width and nullable"));
        }
    }

    @Test
    public void testSetSaltBucketOnAlterTable() throws Exception {
        long ts = nextTimestamp();
        String url = getUrl() + ";" + PhoenixRuntime.CURRENT_SCN_ATTRIB + "=" + (ts + 5); // Run query at timestamp 5
        Connection conn = DriverManager.getConnection(url);
        try {
            conn.createStatement().execute("ALTER TABLE atable ADD xyz INTEGER SALT_BUCKETS=4");
            fail();
        } catch (SQLException e) { // expected
            assertEquals(SQLExceptionCode.SALT_ONLY_ON_CREATE_TABLE.getErrorCode(), e.getErrorCode());
        }
        try {
            conn.createStatement().execute("ALTER TABLE atable SET SALT_BUCKETS=4");
            fail();
        } catch (SQLException e) { // expected
            assertEquals(SQLExceptionCode.SALT_ONLY_ON_CREATE_TABLE.getErrorCode(), e.getErrorCode());
        }
    }

    @Test
    public void testSubstrSetScanKey() throws Exception {
        String query = "SELECT inst FROM ptsdb WHERE substr(inst, 0, 3) = 'abc'";
        List<Object> binds = Collections.emptyList();
        Scan scan = compileQuery(query, binds);
        assertArrayEquals(Bytes.toBytes("abc"), scan.getStartRow());
        assertArrayEquals(ByteUtil.nextKey(Bytes.toBytes("abc")), scan.getStopRow());
        assertTrue(scan.getFilter() == null); // Extracted.
    }

    @Test
    public void testRTrimSetScanKey() throws Exception {
        String query = "SELECT inst FROM ptsdb WHERE rtrim(inst) = 'abc'";
        List<Object> binds = Collections.emptyList();
        Scan scan = compileQuery(query, binds);
        assertArrayEquals(Bytes.toBytes("abc"), scan.getStartRow());
        assertArrayEquals(ByteUtil.nextKey(Bytes.toBytes("abc ")), scan.getStopRow());
        assertNotNull(scan.getFilter());
    }
    
    @Test
    public void testCastingIntegerToDecimalInSelect() throws Exception {
        String query = "SELECT CAST (a_integer AS DECIMAL)/2 FROM aTable WHERE 5=a_integer";
        List<Object> binds = Collections.emptyList();
        compileQuery(query, binds);
    }
    
    @Test
    public void testCastingStringToDecimalInSelect() throws Exception {
        String query = "SELECT CAST (b_string AS DECIMAL)/2 FROM aTable WHERE 5=a_integer";
        List<Object> binds = Collections.emptyList();
        try {
            compileQuery(query, binds);
            fail("Compilation should have failed since casting a string to decimal isn't supported");
        } catch (SQLException e) {
            assertTrue(e.getErrorCode() == SQLExceptionCode.TYPE_MISMATCH.getErrorCode());
        }
    }
    
    @Test
    public void testCastingStringToDecimalInWhere() throws Exception {
        String query = "SELECT a_integer FROM aTable WHERE 2.5=CAST (b_string AS DECIMAL)/2 ";
        List<Object> binds = Collections.emptyList();
        try {
            compileQuery(query, binds);
            fail("Compilation should have failed since casting a string to decimal isn't supported");
        } catch (SQLException e) {
            assertTrue(e.getErrorCode() == SQLExceptionCode.TYPE_MISMATCH.getErrorCode());
        }  
    }

    @Test
    public void testCastingWithLengthInSelect() throws Exception {
        String query = "SELECT CAST (b_string AS VARCHAR(10)) FROM aTable";
        List<Object> binds = Collections.emptyList();
        compileQuery(query, binds);
    }

    @Test
    public void testCastingWithLengthInWhere() throws Exception {
        String query = "SELECT b_string FROM aTable WHERE CAST (b_string AS VARCHAR(10)) = 'b'";
        List<Object> binds = Collections.emptyList();
        compileQuery(query, binds);
    }

    @Test
    public void testCastingWithLengthAndScaleInSelect() throws Exception {
        String query = "SELECT CAST (x_decimal AS DECIMAL(10,5)) FROM aTable";
        List<Object> binds = Collections.emptyList();
        compileQuery(query, binds);
    }

    @Test
    public void testUsingNonComparableDataTypesInRowValueConstructorFails() throws Exception {
        String query = "SELECT a_integer, x_integer FROM aTable WHERE (a_integer, x_integer) > (2, 'abc')";
        List<Object> binds = Collections.emptyList();
        try {
            compileQuery(query, binds);
            fail("Compilation should have failed since casting a integer to string isn't supported");
        } catch (SQLException e) {
            assertTrue(e.getErrorCode() == SQLExceptionCode.TYPE_MISMATCH.getErrorCode());
        }
    }
    
    @Test
    public void testUsingNonComparableDataTypesOfColumnRefOnLHSAndRowValueConstructorFails() throws Exception {
        String query = "SELECT a_integer, x_integer FROM aTable WHERE a_integer > ('abc', 2)";
        List<Object> binds = Collections.emptyList();
        try {
            compileQuery(query, binds);
            fail("Compilation should have failed since casting a integer to string isn't supported");
        } catch (SQLException e) {
            assertTrue(e.getErrorCode() == SQLExceptionCode.TYPE_MISMATCH.getErrorCode());
        }
    }
    
    @Test
    public void testUsingNonComparableDataTypesOfLiteralOnLHSAndRowValueConstructorFails() throws Exception {
        String query = "SELECT a_integer, x_integer FROM aTable WHERE 'abc' > (a_integer, x_integer)";
        List<Object> binds = Collections.emptyList();
        try {
            compileQuery(query, binds);
            fail("Compilation should have failed since casting a integer to string isn't supported");
        } catch (SQLException e) {
            assertTrue(e.getErrorCode() == SQLExceptionCode.TYPE_MISMATCH.getErrorCode());
        }
    }
    
    @Test
    public void testUsingNonComparableDataTypesOfColumnRefOnRHSAndRowValueConstructorFails() throws Exception {
        String query = "SELECT a_integer, x_integer FROM aTable WHERE ('abc', 2) < a_integer ";
        List<Object> binds = Collections.emptyList();
        try {
            compileQuery(query, binds);
            fail("Compilation should have failed since casting a integer to string isn't supported");
        } catch (SQLException e) {
            assertTrue(e.getErrorCode() == SQLExceptionCode.TYPE_MISMATCH.getErrorCode());
        }
    }
    
    @Test
    public void testUsingNonComparableDataTypesOfLiteralOnRHSAndRowValueConstructorFails() throws Exception {
        String query = "SELECT a_integer, x_integer FROM aTable WHERE (a_integer, x_integer) < 'abc'";
        List<Object> binds = Collections.emptyList();
        try {
            compileQuery(query, binds);
            fail("Compilation should have failed since casting a integer to string isn't supported");
        } catch (SQLException e) {
            assertTrue(e.getErrorCode() == SQLExceptionCode.TYPE_MISMATCH.getErrorCode());
        }
    }
    
    @Test
    public void testNonConstantInList() throws Exception {
        String query = "SELECT a_integer, x_integer FROM aTable WHERE a_integer IN (x_integer)";
        List<Object> binds = Collections.emptyList();
        try {
            compileQuery(query, binds);
            fail("Compilation should have failed since non constants in IN is not valid");
        } catch (SQLException e) {
            assertTrue(e.getErrorCode() == SQLExceptionCode.VALUE_IN_LIST_NOT_CONSTANT.getErrorCode());
        }
    }
    
    @Test
    public void testKeyValueColumnInPKConstraint() throws Exception {
        String ddl = "CREATE TABLE t (a.k VARCHAR, b.v VARCHAR CONSTRAINT pk PRIMARY KEY(k))";
        Connection conn = DriverManager.getConnection(getUrl());
        try {
            conn.createStatement().execute(ddl);
            fail();
        } catch (SQLException e) {
            assertTrue(e.getErrorCode() == SQLExceptionCode.PRIMARY_KEY_WITH_FAMILY_NAME.getErrorCode());
        }
    }
    
    @Test
    public void testUnknownColumnInPKConstraint() throws Exception {
        String ddl = "CREATE TABLE t (k1 VARCHAR, b.v VARCHAR CONSTRAINT pk PRIMARY KEY(k1, k2))";
        Connection conn = DriverManager.getConnection(getUrl());
        try {
            conn.createStatement().execute(ddl);
            fail();
        } catch (ColumnNotFoundException e) {
            assertEquals("K2",e.getColumnName());
        }
    }
    
    
    @Test
    public void testDuplicatePKColumn() throws Exception {
        String ddl = "CREATE TABLE t (k1 VARCHAR, k1 VARCHAR CONSTRAINT pk PRIMARY KEY(k1))";
        Connection conn = DriverManager.getConnection(getUrl());
        try {
            conn.createStatement().execute(ddl);
            fail();
        } catch (ColumnAlreadyExistsException e) {
            assertEquals("K1",e.getColumnName());
        }
    }
    
    
    @Test
    public void testDuplicateKVColumn() throws Exception {
        String ddl = "CREATE TABLE t (k1 VARCHAR, v1 VARCHAR, v2 VARCHAR, v1 INTEGER CONSTRAINT pk PRIMARY KEY(k1))";
        Connection conn = DriverManager.getConnection(getUrl());
        try {
            conn.createStatement().execute(ddl);
            fail();
        } catch (ColumnAlreadyExistsException e) {
            assertEquals("V1",e.getColumnName());
        }
    }
    
    private void assertImmutableRows(Connection conn, String fullTableName, boolean expectedValue) throws SQLException {
        PhoenixConnection pconn = conn.unwrap(PhoenixConnection.class);
        assertEquals(expectedValue, pconn.getTable(new PTableKey(pconn.getTenantId(), fullTableName)).isImmutableRows());
    }
    
    @Test
    public void testDeleteFromImmutableWithKV() throws Exception {
        String ddl = "CREATE TABLE t (k1 VARCHAR, v1 VARCHAR, v2 VARCHAR CONSTRAINT pk PRIMARY KEY(k1)) immutable_rows=true";
        String indexDDL = "CREATE INDEX i ON t (v1)";
        Connection conn = DriverManager.getConnection(getUrl());
        try {
            conn.createStatement().execute(ddl);
            assertImmutableRows(conn, "T", true);
            conn.createStatement().execute(indexDDL);
            assertImmutableRows(conn, "I", true);
            conn.createStatement().execute("DELETE FROM t WHERE v2 = 'foo'");
            fail();
        } catch (SQLException e) {
            assertEquals(SQLExceptionCode.INVALID_FILTER_ON_IMMUTABLE_ROWS.getErrorCode(), e.getErrorCode());
        }
        // Test with one index having the referenced key value column, but one not having it.
        // Still should fail
        try {
            indexDDL = "CREATE INDEX i2 ON t (v2)";
            conn.createStatement().execute(indexDDL);
            conn.createStatement().execute("DELETE FROM t WHERE v2 = 'foo'");
            fail();
        } catch (SQLException e) {
            assertEquals(SQLExceptionCode.INVALID_FILTER_ON_IMMUTABLE_ROWS.getErrorCode(), e.getErrorCode());
        }
    }
    
    @Test
    public void testInvalidNegativeArrayIndex() throws Exception {
        String query = "SELECT a_double_array[-20] FROM table_with_array";
        Connection conn = DriverManager.getConnection(getUrl());
        try {
            conn.createStatement().execute(query);
            fail();
        } catch (Exception e) {
            
        }
    }
    @Test
    public void testWrongDataTypeInRoundFunction() throws Exception {
        String query = "SELECT ROUND(a_string, 'day', 1) FROM aTable";
        List<Object> binds = Collections.emptyList();
        try {
            compileQuery(query, binds);
            fail("Compilation should have failed since VARCHAR is not a valid data type for ROUND");
        } catch (SQLException e) {
            assertEquals(SQLExceptionCode.TYPE_MISMATCH.getErrorCode(), e.getErrorCode());
        }
    }
    
    @Test
    public void testNonArrayColumnWithIndex() throws Exception {
        String query = "SELECT a_float[1] FROM table_with_array";
        Connection conn = DriverManager.getConnection(getUrl());
        try {
            conn.createStatement().execute(query);
            fail();
        } catch (Exception e) {
        }
    }

    public void testWrongTimeUnitInRoundDateFunction() throws Exception {
        String query = "SELECT ROUND(a_date, 'dayss', 1) FROM aTable";
        List<Object> binds = Collections.emptyList();
        try {
            compileQuery(query, binds);
            fail("Compilation should have failed since dayss is not a valid time unit type");
        } catch (IllegalArgumentException e) {
            assertTrue(e.getMessage().contains(TimeUnit.VALID_VALUES));
        }
    }
    
    @Test
    public void testWrongMultiplierInRoundDateFunction() throws Exception {
        String query = "SELECT ROUND(a_date, 'days', 1.23) FROM aTable";
        List<Object> binds = Collections.emptyList();
        try {
            compileQuery(query, binds);
            fail("Compilation should have failed since multiplier can be an INTEGER only");
        } catch (SQLException e) {
            assertEquals(SQLExceptionCode.TYPE_MISMATCH.getErrorCode(), e.getErrorCode());
        }
    }
    
    @Test
    public void testTypeMismatchForArrayElem() throws Exception {
        String query = "SELECT (a_string,a_date)[1] FROM aTable";
        List<Object> binds = Collections.emptyList();
        try {
            compileQuery(query, binds);
            fail("Compilation should have failed since a row value constructor is not an array");
        } catch (SQLException e) {
            assertEquals(SQLExceptionCode.TYPE_MISMATCH.getErrorCode(), e.getErrorCode());
        }
    }
    
    @Test
    public void testTypeMismatch2ForArrayElem() throws Exception {
        String query = "SELECT ROUND(a_date, 'days', 1.23)[1] FROM aTable";
        List<Object> binds = Collections.emptyList();
        try {
            compileQuery(query, binds);
            fail("Compilation should have failed since ROUND does not return an array");
        } catch (SQLException e) {
            assertEquals(SQLExceptionCode.TYPE_MISMATCH.getErrorCode(), e.getErrorCode());
        }
    }
    
    @Test
    public void testInvalidArraySize() throws Exception {
        Connection conn = DriverManager.getConnection(getUrl());
        try {
            String query = "CREATE TABLE foo (col1 INTEGER[-1] NOT NULL PRIMARY KEY)";
            PreparedStatement statement = conn.prepareStatement(query);
            statement.execute();
            fail();
        } catch (SQLException e) {
                assertEquals(SQLExceptionCode.MISMATCHED_TOKEN.getErrorCode(), e.getErrorCode());
        } finally {
                conn.close();
        }
    }

    @Test
    public void testInvalidArrayElemRefInUpsert() throws Exception {
        Connection conn = DriverManager.getConnection(getUrl());
        conn.createStatement().execute("CREATE TABLE t (k VARCHAR PRIMARY KEY, a INTEGER[10], B INTEGER[10])");
        try {
            conn.createStatement().execute("UPSERT INTO t(k,a[2]) VALUES('A', 5)");
            fail();
        } catch (SQLException e) {
            assertEquals(SQLExceptionCode.PARSER_ERROR.getErrorCode(), e.getErrorCode());
        }
        conn.close();
    }

    @Test
    public void testVarbinaryArrayNotSupported() throws Exception {
        Connection conn = DriverManager.getConnection(getUrl());
        try {
            conn.createStatement().execute("CREATE TABLE t (k VARCHAR PRIMARY KEY, a VARBINARY[10])");
            fail();
        } catch (SQLException e) {
            assertEquals(SQLExceptionCode.VARBINARY_ARRAY_NOT_SUPPORTED.getErrorCode(), e.getErrorCode());
        }
        conn.close();
    }

    @Test
    public void testInvalidNextValueFor() throws Exception {
        Connection conn = DriverManager.getConnection(getUrl());
        conn.createStatement().execute("CREATE SEQUENCE alpha.zeta");
        String[] queries = {
                "SELECT * FROM aTable WHERE a_integer < next value for alpha.zeta",
                "SELECT * FROM aTable GROUP BY a_string,next value for alpha.zeta",
                "SELECT * FROM aTable GROUP BY 1 + next value for alpha.zeta",
                "SELECT * FROM aTable GROUP BY a_integer HAVING a_integer < next value for alpha.zeta",
                "SELECT * FROM aTable WHERE a_integer < 3 GROUP BY a_integer HAVING a_integer < next value for alpha.zeta",
                "SELECT * FROM aTable ORDER BY next value for alpha.zeta",
                "SELECT max(next value for alpha.zeta) FROM aTable",
        };
        for (String query : queries) {
            List<Object> binds = Collections.emptyList();
            try {
                compileQuery(query, binds);
                fail("Compilation should have failed since this is an invalid usage of NEXT VALUE FOR: " + query);
            } catch (SQLException e) {
                assertEquals(query, SQLExceptionCode.INVALID_USE_OF_NEXT_VALUE_FOR.getErrorCode(), e.getErrorCode());
            }
        }
    }

    @Test
    public void testNoCachingHint() throws Exception {
        List<Object> binds = Collections.emptyList();
        Scan scan = compileQuery("select val from ptsdb", binds);
        assertTrue(scan.getCacheBlocks());
        scan = compileQuery("select /*+ NO_CACHE */ val from ptsdb", binds);
        assertFalse(scan.getCacheBlocks());
        scan = compileQuery("select /*+ NO_CACHE */ p1.val from ptsdb p1 inner join ptsdb p2 on p1.inst = p2.inst", binds);
        assertFalse(scan.getCacheBlocks());
    }

    @Test
    public void testExecuteWithNonEmptyBatch() throws Exception {
        Connection conn = DriverManager.getConnection(getUrl());
        try {
            Statement stmt = conn.createStatement();
            stmt.addBatch("SELECT * FROM atable");
            stmt.execute("UPSERT INTO atable VALUES('000000000000000','000000000000000')");
            fail();
        } catch (SQLException e) {
            assertEquals(SQLExceptionCode.EXECUTE_UPDATE_WITH_NON_EMPTY_BATCH.getErrorCode(), e.getErrorCode());
        }
        try {
            Statement stmt = conn.createStatement();
            stmt.addBatch("SELECT * FROM atable");
            stmt.executeUpdate("UPSERT INTO atable VALUES('000000000000000','000000000000000')");
            fail();
        } catch (SQLException e) {
            assertEquals(SQLExceptionCode.EXECUTE_UPDATE_WITH_NON_EMPTY_BATCH.getErrorCode(), e.getErrorCode());
        }
        try {
            PreparedStatement stmt = conn.prepareStatement("UPSERT INTO atable VALUES('000000000000000','000000000000000')");
            stmt.addBatch();
            stmt.execute();
            fail();
        } catch (SQLException e) {
            assertEquals(SQLExceptionCode.EXECUTE_UPDATE_WITH_NON_EMPTY_BATCH.getErrorCode(), e.getErrorCode());
        }
        conn.close();
        try {
            PreparedStatement stmt = conn.prepareStatement("UPSERT INTO atable VALUES('000000000000000','000000000000000')");
            stmt.addBatch();
            stmt.executeUpdate();
            fail();
        } catch (SQLException e) {
            assertEquals(SQLExceptionCode.EXECUTE_UPDATE_WITH_NON_EMPTY_BATCH.getErrorCode(), e.getErrorCode());
        }
        conn.close();
    }
    
    @Test
    public void testInvalidPrimaryKeyDecl() throws Exception {
        String[] queries = {
                "CREATE TABLE t (k varchar null primary key)",
                "CREATE TABLE t (k varchar null, constraint pk primary key (k))",
        };
        Connection conn = DriverManager.getConnection(getUrl());
        for (String query : queries) {
            try {
                conn.createStatement().execute(query);
                fail("Compilation should have failed since this is an invalid PRIMARY KEY declaration: " + query);
            } catch (SQLException e) {
                assertEquals(query, SQLExceptionCode.SINGLE_PK_MAY_NOT_BE_NULL.getErrorCode(), e.getErrorCode());
            }
        }
    }
    
    @Test
    public void testInvalidNullCompositePrimaryKey() throws Exception {
        Connection conn = DriverManager.getConnection(getUrl());
        conn.createStatement().execute("CREATE TABLE t (k1 varchar, k2 varchar, constraint pk primary key(k1,k2))");
        PreparedStatement stmt = conn.prepareStatement("UPSERT INTO t values(?,?)");
        stmt.setString(1, "");
        stmt.setString(2, "");
        try {
            stmt.execute();
            fail();
        } catch (SQLException e) {
            assertTrue(e.getMessage().contains("Primary key may not be null"));
        }
    }

    
    @Test
    public void testGroupByLimitOptimization() throws Exception {
        Connection conn = DriverManager.getConnection(getUrl());
        conn.createStatement().execute("CREATE TABLE t (k1 varchar, k2 varchar, v varchar, constraint pk primary key(k1,k2))");
        ResultSet rs;
        String[] queries = {
                "SELECT DISTINCT v FROM T LIMIT 3",
                "SELECT v FROM T GROUP BY v,k1 LIMIT 3",
                "SELECT count(*) FROM T GROUP BY k1 LIMIT 3",
                "SELECT max(v) FROM T GROUP BY k1,k2 LIMIT 3",
                "SELECT k1,k2 FROM T GROUP BY k1,k2 LIMIT 3",
                "SELECT max(v) FROM T GROUP BY k2,k1 HAVING k1 > 'a' LIMIT 3", // Having optimized out, order of GROUP BY key not important
                };
        String query;
        for (int i = 0; i < queries.length; i++) {
            query = queries[i];
            rs = conn.createStatement().executeQuery("EXPLAIN " + query);
            assertTrue("Expected to find GROUP BY limit optimization in: " + query, QueryUtil.getExplainPlan(rs).contains(" LIMIT 3 GROUPS"));
        }
    }
    
    @Test
    public void testNoGroupByLimitOptimization() throws Exception {
        Connection conn = DriverManager.getConnection(getUrl());
        conn.createStatement().execute("CREATE TABLE t (k1 varchar, k2 varchar, v varchar, constraint pk primary key(k1,k2))");
        ResultSet rs;
        String[] queries = {
//                "SELECT DISTINCT v FROM T ORDER BY v LIMIT 3",
//                "SELECT v FROM T GROUP BY v,k1 ORDER BY v LIMIT 3",
                "SELECT DISTINCT count(*) FROM T GROUP BY k1 LIMIT 3",
                "SELECT count(1) FROM T GROUP BY v,k1 LIMIT 3",
                "SELECT max(v) FROM T GROUP BY k1,k2 HAVING count(k1) > 1 LIMIT 3",
                "SELECT count(v) FROM T GROUP BY to_date(k2),k1 LIMIT 3",
                };
        String query;
        for (int i = 0; i < queries.length; i++) {
            query = queries[i];
            rs = conn.createStatement().executeQuery("EXPLAIN " + query);
            String explainPlan = QueryUtil.getExplainPlan(rs);
            assertFalse("Did not expected to find GROUP BY limit optimization in: " + query, explainPlan.contains(" LIMIT 3 GROUPS"));
        }
    }
    
    @Test
    public void testLocalIndexCreationWithDefaultFamilyOption() throws Exception {
        Connection conn1 = DriverManager.getConnection(getUrl());
        try{
            Statement statement = conn1.createStatement();
            statement.execute("create table example (id integer not null,fn varchar,"
                    + "\"ln\" varchar constraint pk primary key(id)) DEFAULT_COLUMN_FAMILY='F'");
            try {
                statement.execute("create local index my_idx on example (fn) DEFAULT_COLUMN_FAMILY='F'");
                fail();
            } catch (SQLException e) {
                assertEquals(SQLExceptionCode.DEFAULT_COLUMN_FAMILY_ON_SHARED_TABLE.getErrorCode(),e.getErrorCode());
            }
            statement.execute("create local index my_idx on example (fn)");
       } finally {
            conn1.close();
        }
    }

    @Test
    public void testMultiCFProjection() throws Exception {
        Connection conn = DriverManager.getConnection(getUrl());
        String ddl = "CREATE TABLE multiCF (k integer primary key, a.a varchar, b.b varchar)";
        conn.createStatement().execute(ddl);
        String query = "SELECT COUNT(*) FROM multiCF";
        QueryPlan plan = getQueryPlan(query,Collections.emptyList());
        plan.iterator();
        Scan scan = plan.getContext().getScan();
        assertTrue(scan.getFilter() instanceof FirstKeyOnlyFilter);
        assertEquals(1, scan.getFamilyMap().size());
    }
    
    @Test 
    public void testNonDeterministicExpressionIndex() throws Exception {
        String ddl = "CREATE TABLE t (k1 INTEGER PRIMARY KEY)";
        Connection conn = DriverManager.getConnection(getUrl());
        Statement stmt = null;
        try {
            stmt = conn.createStatement();
            stmt.execute(ddl);
            stmt.execute("CREATE INDEX i ON t (RAND())");
            fail();
        } catch (SQLException e) {
            assertEquals(SQLExceptionCode.NON_DETERMINISTIC_EXPRESSION_NOT_ALLOWED_IN_INDEX.getErrorCode(), e.getErrorCode());
        }
        finally {
            stmt.close();
        }
    }
    
    @Test 
    public void testStatelessExpressionIndex() throws Exception {
        String ddl = "CREATE TABLE t (k1 INTEGER PRIMARY KEY)";
        Connection conn = DriverManager.getConnection(getUrl());
        Statement stmt = null;
        try {
            stmt = conn.createStatement();
            stmt.execute(ddl);
            stmt.execute("CREATE INDEX i ON t (2)");
            fail();
        } catch (SQLException e) {
            assertEquals(SQLExceptionCode.STATELESS_EXPRESSION_NOT_ALLOWED_IN_INDEX.getErrorCode(), e.getErrorCode());
        }
        finally {
            stmt.close();
        }
    }
    
    @Test 
    public void testAggregateExpressionIndex() throws Exception {
        String ddl = "CREATE TABLE t (k1 INTEGER PRIMARY KEY)";
        Connection conn = DriverManager.getConnection(getUrl());
        Statement stmt = null;
        try {
            stmt = conn.createStatement();
            stmt.execute(ddl);
            stmt.execute("CREATE INDEX i ON t (SUM(k1))");
            fail();
        } catch (SQLException e) {
            assertEquals(SQLExceptionCode.AGGREGATE_EXPRESSION_NOT_ALLOWED_IN_INDEX.getErrorCode(), e.getErrorCode());
        }
        finally {
            stmt.close();
        }
    }
    
    @Test 
    public void testDescVarbinaryNotSupported() throws Exception {
        Connection conn = DriverManager.getConnection(getUrl());
        try {
            conn.createStatement().execute("CREATE TABLE t (k VARBINARY PRIMARY KEY DESC)");
            fail();
        } catch (SQLException e) {
            assertEquals(SQLExceptionCode.DESC_VARBINARY_NOT_SUPPORTED.getErrorCode(), e.getErrorCode());
        }
        try {
            conn.createStatement().execute("CREATE TABLE t (k1 VARCHAR NOT NULL, k2 VARBINARY, CONSTRAINT pk PRIMARY KEY (k1,k2 DESC))");
            fail();
        } catch (SQLException e) {
            assertEquals(SQLExceptionCode.DESC_VARBINARY_NOT_SUPPORTED.getErrorCode(), e.getErrorCode());
        }
        try {
            conn.createStatement().execute("CREATE TABLE t (k1 VARCHAR PRIMARY KEY)");
            conn.createStatement().execute("ALTER TABLE t ADD k2 VARBINARY PRIMARY KEY DESC");
            fail();
        } catch (SQLException e) {
            assertEquals(SQLExceptionCode.DESC_VARBINARY_NOT_SUPPORTED.getErrorCode(), e.getErrorCode());
        }
        conn.close();
    }
    
    @Test 
    public void testDivideByZeroExpressionIndex() throws Exception {
        String ddl = "CREATE TABLE t (k1 INTEGER PRIMARY KEY)";
        Connection conn = DriverManager.getConnection(getUrl());
        Statement stmt = null;
        try {
            stmt = conn.createStatement();
            stmt.execute(ddl);
            stmt.execute("CREATE INDEX i ON t (k1/0)");
            fail();
        } catch (SQLException e) {
            assertEquals(SQLExceptionCode.DIVIDE_BY_ZERO.getErrorCode(), e.getErrorCode());
        }
        finally {
            stmt.close();
        }
    }
    
    @Test
    public void testRegex() throws Exception {
        Connection conn = DriverManager.getConnection(getUrl());
        Statement stmt = conn.createStatement();
        stmt.execute("CREATE TABLE t (k1 INTEGER PRIMARY KEY, v VARCHAR)");
        
        //character classes
        stmt.executeQuery("select * from T where REGEXP_SUBSTR(v, '[abc]') = 'val'");
        stmt.executeQuery("select * from T where REGEXP_SUBSTR(v, '[^abc]') = 'val'");
        stmt.executeQuery("select * from T where REGEXP_SUBSTR(v, '[a-zA-Z]') = 'val'");
        stmt.executeQuery("select * from T where REGEXP_SUBSTR(v, '[a-d[m-p]]') = 'val'");
        stmt.executeQuery("select * from T where REGEXP_SUBSTR(v, '[a-z&&[def]]') = 'val'");
        stmt.executeQuery("select * from T where REGEXP_SUBSTR(v, '[a-z&&[^bc]]') = 'val'");
        stmt.executeQuery("select * from T where REGEXP_SUBSTR(v, '[a-z&&[^m-p]]') = 'val'");
        
        // predefined character classes
        stmt.executeQuery("select * from T where REGEXP_SUBSTR(v, '.\\\\d\\\\D\\\\s\\\\S\\\\w\\\\W') = 'val'");
    }
    
    private static void assertLiteralEquals(Object o, RowProjector p, int i) {
        assertTrue(i < p.getColumnCount());
        Expression e = p.getColumnProjector(i).getExpression();
        assertTrue(e instanceof LiteralExpression);
        LiteralExpression l = (LiteralExpression)e;
        Object lo = l.getValue();
        assertEquals(o, lo);
    }
    
    @Test
    public void testIntAndLongMinValue() throws Exception {
        BigDecimal oneLessThanMinLong = BigDecimal.valueOf(Long.MIN_VALUE).subtract(BigDecimal.ONE);
        BigDecimal oneMoreThanMaxLong = BigDecimal.valueOf(Long.MAX_VALUE).add(BigDecimal.ONE);
        String query = "SELECT " + 
            Integer.MIN_VALUE + "," + Long.MIN_VALUE + "," + 
            (Integer.MIN_VALUE+1) + "," + (Long.MIN_VALUE+1) + "," + 
            ((long)Integer.MIN_VALUE - 1) + "," + oneLessThanMinLong + "," +
            Integer.MAX_VALUE + "," + Long.MAX_VALUE + "," +
            (Integer.MAX_VALUE - 1) + "," + (Long.MAX_VALUE - 1) + "," +
            ((long)Integer.MAX_VALUE + 1) + "," + oneMoreThanMaxLong +
        " FROM " + "\""+ SYSTEM_CATALOG_SCHEMA + "\".\"" + SYSTEM_STATS_TABLE + "\"" + " LIMIT 1";
        List<Object> binds = Collections.emptyList();
        QueryPlan plan = getQueryPlan(query, binds);
        RowProjector p = plan.getProjector();
        // Negative integers end up as longs once the * -1 occurs
        assertLiteralEquals((long)Integer.MIN_VALUE, p, 0);
        // Min long still stays as long
        assertLiteralEquals(Long.MIN_VALUE, p, 1);
        assertLiteralEquals((long)Integer.MIN_VALUE + 1, p, 2);
        assertLiteralEquals(Long.MIN_VALUE + 1, p, 3);
        assertLiteralEquals((long)Integer.MIN_VALUE - 1, p, 4);
        // Can't fit into long, so becomes BigDecimal
        assertLiteralEquals(oneLessThanMinLong, p, 5);
        // Positive integers stay as ints
        assertLiteralEquals(Integer.MAX_VALUE, p, 6);
        assertLiteralEquals(Long.MAX_VALUE, p, 7);
        assertLiteralEquals(Integer.MAX_VALUE - 1, p, 8);
        assertLiteralEquals(Long.MAX_VALUE - 1, p, 9);
        assertLiteralEquals((long)Integer.MAX_VALUE + 1, p, 10);
        assertLiteralEquals(oneMoreThanMaxLong, p, 11);
    }

    @Test
    public void testMathFunctionOrderByOrderPreservingFwd() throws Exception {
        Connection conn = DriverManager.getConnection(getUrl());
        conn.createStatement().execute("CREATE TABLE t (k1 INTEGER not null, k2 double not null, k3 BIGINT not null, v varchar, constraint pk primary key(k1,k2,k3))");
        /*
         * "SELECT * FROM T ORDER BY k1, k2",
         * "SELECT * FROM T ORDER BY k1, SIGN(k2)",
         * "SELECT * FROM T ORDER BY SIGN(k1), k2",
         */
        List<String> queryList = new ArrayList<String>();
        queryList.add("SELECT * FROM T ORDER BY k1, k2");
        for (String sub : new String[] { "SIGN", "CBRT", "LN", "LOG", "EXP" }) {
            queryList.add(String.format("SELECT * FROM T ORDER BY k1, %s(k2)", sub));
            queryList.add(String.format("SELECT * FROM T ORDER BY %s(k1), k2", sub));
        }
        String[] queries = queryList.toArray(new String[queryList.size()]);
        for (int i = 0; i < queries.length; i++) {
            String query = queries[i];
            QueryPlan plan = conn.createStatement().unwrap(PhoenixStatement.class).compileQuery(query);
            assertTrue(plan.getOrderBy() == OrderBy.FWD_ROW_KEY_ORDER_BY);
        }
        // Negative test
        queryList.clear();
        for (String sub : new String[] { "SIGN", "CBRT", "LN", "LOG", "EXP" }) {
            queryList.add(String.format("SELECT * FROM T WHERE %s(k2)=2.0", sub));
        }
        for (String query : queryList.toArray(new String[queryList.size()])) {
            Scan scan = conn.createStatement().unwrap(PhoenixStatement.class).compileQuery(query).getContext().getScan();
            assertNotNull(scan.getFilter());
            assertTrue(scan.getStartRow().length == 0);
            assertTrue(scan.getStopRow().length == 0);
        }
    }

    @Test
    public void testMathFunctionOrderByOrderPreservingRev() throws Exception {
        Connection conn = DriverManager.getConnection(getUrl());
        conn.createStatement().execute("CREATE TABLE t (k1 INTEGER not null, k2 double not null, k3 BIGINT not null, v varchar, constraint pk primary key(k1,k2 DESC,k3))");
        List<String> queryList = new ArrayList<String>();
        // "SELECT * FROM T ORDER BY k1 DESC, SIGN(k2) DESC, k3 DESC"
        queryList.add("SELECT * FROM T ORDER BY k1 DESC");
        queryList.add("SELECT * FROM T ORDER BY k1 DESC, k2");
        queryList.add("SELECT * FROM T ORDER BY k1 DESC, k2, k3 DESC");
        for (String sub : new String[] { "SIGN", "CBRT", "LN", "LOG", "EXP" }) {
            queryList.add(String.format("SELECT * FROM T ORDER BY k1 DESC, %s(k2) DESC, k3 DESC", sub));
        }
        String[] queries = queryList.toArray(new String[queryList.size()]);
        for (int i = 0; i < queries.length; i++) {
            String query = queries[i];
            QueryPlan plan =
                    conn.createStatement().unwrap(PhoenixStatement.class).compileQuery(query);
            assertTrue(query, plan.getOrderBy() == OrderBy.REV_ROW_KEY_ORDER_BY);
        }
        // Negative test
        queryList.clear();
        for (String sub : new String[] { "SIGN", "CBRT", "LN", "LOG", "EXP" }) {
            queryList.add(String.format("SELECT * FROM T WHERE %s(k2)=2.0", sub));
        }
        for (String query : queryList.toArray(new String[queryList.size()])) {
            Scan scan = conn.createStatement().unwrap(PhoenixStatement.class).compileQuery(query).getContext().getScan();
            assertNotNull(scan.getFilter());
            assertTrue(scan.getStartRow().length == 0);
            assertTrue(scan.getStopRow().length == 0);
        }
    }

    @Test
    public void testOrderByOrderPreservingFwd() throws Exception {
        Connection conn = DriverManager.getConnection(getUrl());
        conn.createStatement().execute("CREATE TABLE t (k1 date not null, k2 date not null, k3 varchar, v varchar, constraint pk primary key(k1,k2,k3))");
        String[] queries = {
                "SELECT * FROM T WHERE k2=CURRENT_DATE() ORDER BY k1, k3",
                "SELECT * FROM T ORDER BY (k1,k2), k3",
                "SELECT * FROM T ORDER BY k1,k2,k3 NULLS FIRST",
                "SELECT * FROM T ORDER BY k1,k2,k3",
                "SELECT * FROM T ORDER BY k1,k2",
                "SELECT * FROM T ORDER BY k1",
                "SELECT * FROM T ORDER BY CAST(k1 AS TIMESTAMP)",
                "SELECT * FROM T ORDER BY (k1,k2,k3)",
                "SELECT * FROM T ORDER BY TRUNC(k1, 'DAY'), CEIL(k2, 'HOUR')",
                "SELECT * FROM T ORDER BY INVERT(k1) DESC",
                "SELECT * FROM T WHERE k1=CURRENT_DATE() ORDER BY k2",
                };
        String query;
        for (int i = 0; i < queries.length; i++) {
            query = queries[i];
            QueryPlan plan = conn.createStatement().unwrap(PhoenixStatement.class).compileQuery(query);
            assertTrue("Expected order by to be compiled out: " + query, plan.getOrderBy() == OrderBy.FWD_ROW_KEY_ORDER_BY);
        }
    }
    
    @Test
    public void testOrderByOrderPreservingRev() throws Exception {
        Connection conn = DriverManager.getConnection(getUrl());
        conn.createStatement().execute("CREATE TABLE t (k1 date not null, k2 date not null, k3 varchar, v varchar, constraint pk primary key(k1,k2 DESC,k3 DESC))");
        String[] queries = {
                "SELECT * FROM T ORDER BY INVERT(k1),k2,k3 nulls last",
                "SELECT * FROM T ORDER BY INVERT(k1),k2",
                "SELECT * FROM T ORDER BY INVERT(k1)",
                 "SELECT * FROM T ORDER BY TRUNC(k1, 'DAY') DESC, CEIL(k2, 'HOUR') DESC",
                "SELECT * FROM T ORDER BY k1 DESC",
                };
        String query;
        for (int i = 0; i < queries.length; i++) {
            query = queries[i];
            QueryPlan plan = conn.createStatement().unwrap(PhoenixStatement.class).compileQuery(query);
            assertTrue("Expected order by to be compiled out: " + query, plan.getOrderBy() == OrderBy.REV_ROW_KEY_ORDER_BY);
        }
    }
    
    @Test
    public void testNotOrderByOrderPreserving() throws Exception {
        Connection conn = DriverManager.getConnection(getUrl());
        conn.createStatement().execute("CREATE TABLE t (k1 date not null, k2 varchar, k3 varchar, v varchar, constraint pk primary key(k1,k2,k3 desc))");
        String[] queries = {
                "SELECT * FROM T ORDER BY k1,k2 NULLS LAST",
                "SELECT * FROM T ORDER BY k1,k2, k3 NULLS LAST",
                "SELECT * FROM T ORDER BY k1,k3",
                "SELECT * FROM T ORDER BY SUBSTR(TO_CHAR(k1),1,4)",
                "SELECT * FROM T ORDER BY k2",
                "SELECT * FROM T ORDER BY INVERT(k1),k3",
                "SELECT * FROM T ORDER BY CASE WHEN k1 = CURRENT_DATE() THEN 0 ELSE 1 END",
                "SELECT * FROM T ORDER BY TO_CHAR(k1)",
                };
        String query;
        for (int i = 0; i < queries.length; i++) {
            query = queries[i];
            QueryPlan plan = conn.createStatement().unwrap(PhoenixStatement.class).compileQuery(query);
            assertFalse("Expected order by not to be compiled out: " + query, plan.getOrderBy().getOrderByExpressions().isEmpty());
        }
    }
    
    @Test
    public void testGroupByOrderPreserving() throws Exception {
        Connection conn = DriverManager.getConnection(getUrl());
        conn.createStatement().execute("CREATE TABLE t (k1 date not null, k2 date not null, k3 date not null, v varchar, constraint pk primary key(k1,k2,k3))");
        String[] queries = {
                "SELECT 1 FROM T GROUP BY k3, (k1,k2)",
                "SELECT 1 FROM T GROUP BY k2,k1,k3",
                "SELECT 1 FROM T GROUP BY k1,k2",
                "SELECT 1 FROM T GROUP BY k1",
                "SELECT 1 FROM T GROUP BY CAST(k1 AS TIMESTAMP)",
                "SELECT 1 FROM T GROUP BY (k1,k2,k3)",
                "SELECT 1 FROM T GROUP BY TRUNC(k2, 'DAY'), CEIL(k1, 'HOUR')",
                };
        String query;
        for (int i = 0; i < queries.length; i++) {
            query = queries[i];
            QueryPlan plan = conn.createStatement().unwrap(PhoenixStatement.class).compileQuery(query);
            assertTrue("Expected group by to be order preserving: " + query, plan.getGroupBy().isOrderPreserving());
        }
    }
    
    @Test
    public void testGroupByOrderPreserving2() throws Exception {
        Connection conn = DriverManager.getConnection(getUrl());
        conn.createStatement().execute("CREATE TABLE T (ORGANIZATION_ID char(15) not null, \n" + 
                "JOURNEY_ID char(15) not null, \n" + 
                "DATASOURCE SMALLINT not null, \n" + 
                "MATCH_STATUS TINYINT not null, \n" + 
                "EXTERNAL_DATASOURCE_KEY varchar(30), \n" + 
                "ENTITY_ID char(15) not null, \n" + 
                "CONSTRAINT PK PRIMARY KEY (\n" + 
                "    ORGANIZATION_ID, \n" + 
                "    JOURNEY_ID, \n" + 
                "    DATASOURCE, \n" + 
                "    MATCH_STATUS,\n" + 
                "    EXTERNAL_DATASOURCE_KEY,\n" + 
                "    ENTITY_ID))");
        String[] queries = {
                "SELECT COUNT(1) As DUP_COUNT\n" + 
                "    FROM T \n" + 
                "   WHERE JOURNEY_ID='07ixx000000004J' AND \n" + 
                "                 DATASOURCE=0 AND MATCH_STATUS <= 1 and \n" + 
                "                 ORGANIZATION_ID='07ixx000000004J' \n" + 
                "    GROUP BY MATCH_STATUS, EXTERNAL_DATASOURCE_KEY \n" + 
                "    HAVING COUNT(1) > 1",
                };
        String query;
        for (int i = 0; i < queries.length; i++) {
            query = queries[i];
            QueryPlan plan = conn.createStatement().unwrap(PhoenixStatement.class).compileQuery(query);
            assertTrue("Expected group by to be order preserving: " + query, plan.getGroupBy().isOrderPreserving());
        }
    }
    
    @Test
    public void testNotGroupByOrderPreserving() throws Exception {
        Connection conn = DriverManager.getConnection(getUrl());
        conn.createStatement().execute("CREATE TABLE t (k1 date not null, k2 date not null, k3 date not null, v varchar, constraint pk primary key(k1,k2,k3))");
        String[] queries = {
                "SELECT 1 FROM T GROUP BY k1,k3",
                "SELECT 1 FROM T GROUP BY k2",
                "SELECT 1 FROM T GROUP BY INVERT(k1),k3",
                "SELECT 1 FROM T GROUP BY CASE WHEN k1 = CURRENT_DATE() THEN 0 ELSE 1 END",
                "SELECT 1 FROM T GROUP BY TO_CHAR(k1)",
                };
        String query;
        for (int i = 0; i < queries.length; i++) {
            query = queries[i];
            QueryPlan plan = conn.createStatement().unwrap(PhoenixStatement.class).compileQuery(query);
            assertFalse("Expected group by not to be order preserving: " + query, plan.getGroupBy().isOrderPreserving());
        }
    }
    
    @Test
    public void testUseRoundRobinIterator() throws Exception {
        Properties props = new Properties();
        props.setProperty(QueryServices.FORCE_ROW_KEY_ORDER_ATTRIB, Boolean.toString(false));
        Connection conn = DriverManager.getConnection(getUrl(), props);
        conn.createStatement().execute("CREATE TABLE t (k1 char(2) not null, k2 varchar not null, k3 integer not null, v varchar, constraint pk primary key(k1,k2,k3))");
        String[] queries = {
                "SELECT 1 FROM T ",
                "SELECT 1 FROM T WHERE V = 'c'",
                "SELECT 1 FROM T WHERE (k1,k2, k3) > ('a', 'ab', 1)",
                };
        String query;
        for (int i = 0; i < queries.length; i++) {
            query = queries[i];
            QueryPlan plan = conn.createStatement().unwrap(PhoenixStatement.class).compileQuery(query);
            assertTrue("Expected plan to use round robin iterator " + query, plan.useRoundRobinIterator());
        }
    }
    
    @Test
    public void testForcingRowKeyOrderNotUseRoundRobinIterator() throws Exception {
        Properties props = new Properties();
        props.setProperty(QueryServices.FORCE_ROW_KEY_ORDER_ATTRIB, Boolean.toString(true));
        Connection conn = DriverManager.getConnection(getUrl(), props);
        testForceRowKeyOrder(conn, false);
        testForceRowKeyOrder(conn, true);
    }

    private void testForceRowKeyOrder(Connection conn, boolean isSalted) throws SQLException {
        String tableName = "tablename" + (isSalted ? "_salt" : "");
        conn.createStatement().execute("CREATE TABLE " + tableName + " (k1 char(2) not null, k2 varchar not null, k3 integer not null, v varchar, constraint pk primary key(k1,k2,k3))");
        String[] queries = {
                "SELECT 1 FROM  " + tableName ,
                "SELECT 1 FROM  " + tableName + "  WHERE V = 'c'",
                "SELECT 1 FROM  " + tableName + "  WHERE (k1, k2, k3) > ('a', 'ab', 1)",
                };
        String query;
        for (int i = 0; i < queries.length; i++) {
            query = queries[i];
            QueryPlan plan = conn.createStatement().unwrap(PhoenixStatement.class).compileQuery(query);
            assertFalse("Expected plan to not use round robin iterator " + query, plan.useRoundRobinIterator());
        }
    }
    
    @Test
    public void testPlanForOrderByOrGroupByNotUseRoundRobin() throws Exception {
        Properties props = new Properties();
        props.setProperty(QueryServices.FORCE_ROW_KEY_ORDER_ATTRIB, Boolean.toString(false));
        Connection conn = DriverManager.getConnection(getUrl(), props);
        testOrderByOrGroupByDoesntUseRoundRobin(conn, true);
        testOrderByOrGroupByDoesntUseRoundRobin(conn, false);
    }

    private void testOrderByOrGroupByDoesntUseRoundRobin(Connection conn, boolean salted) throws SQLException {
        String tableName = "orderbygroupbytable" + (salted ? "_salt" : ""); 
        conn.createStatement().execute("CREATE TABLE " + tableName + " (k1 char(2) not null, k2 varchar not null, k3 integer not null, v varchar, constraint pk primary key(k1,k2,k3))");
        String[] queries = {
                "SELECT 1 FROM  " + tableName + "  ORDER BY K1",
                "SELECT 1 FROM  " + tableName + "  WHERE V = 'c' ORDER BY K1, K2",
                "SELECT 1 FROM  " + tableName + "  WHERE V = 'c' ORDER BY K1, K2, K3",
                "SELECT 1 FROM  " + tableName + "  WHERE V = 'c' ORDER BY K3",
                "SELECT 1 FROM  " + tableName + "  WHERE (k1,k2, k3) > ('a', 'ab', 1) ORDER BY V",
                "SELECT 1 FROM  " + tableName + "  GROUP BY V",
                "SELECT 1 FROM  " + tableName + "  GROUP BY K1, V, K2 ORDER BY V",
                };
        String query;
        for (int i = 0; i < queries.length; i++) {
            query = queries[i];
            QueryPlan plan = conn.createStatement().unwrap(PhoenixStatement.class).compileQuery(query);
            assertFalse("Expected plan to not use round robin iterator " + query, plan.useRoundRobinIterator());
        }
    }
    
    @Test
    public void testSelectColumnsInOneFamily() throws Exception {
        Connection conn = DriverManager.getConnection(getUrl());
        Statement statement = conn.createStatement();
        try {
            // create table with specified column family.
            String create = "CREATE TABLE t (k integer not null primary key, f1.v1 varchar, f1.v2 varchar, f2.v3 varchar, v4 varchar)";
            statement.execute(create);
            // select columns in one family.
            String query = "SELECT f1.*, v4 FROM t";
            ResultSetMetaData rsMeta = statement.executeQuery(query).getMetaData();
            assertEquals("V1", rsMeta.getColumnName(1));
            assertEquals("V2", rsMeta.getColumnName(2));
            assertEquals("V4", rsMeta.getColumnName(3));
        } finally {
            statement.execute("DROP TABLE IF EXISTS t");
            conn.close();
        }
    }

    @Test
    public void testSelectColumnsInOneFamilyWithSchema() throws Exception {
        Connection conn = DriverManager.getConnection(getUrl());
        Statement statement = conn.createStatement();
        try {
            // create table with specified column family.
            String create = "CREATE TABLE s.t (k integer not null primary key, f1.v1 varchar, f1.v2 varchar, f2.v3 varchar, v4 varchar)";
            statement.execute(create);
            // select columns in one family.
            String query = "SELECT f1.*, v4 FROM s.t";
            ResultSetMetaData rsMeta = statement.executeQuery(query).getMetaData();
            assertEquals("V1", rsMeta.getColumnName(1));
            assertEquals("V2", rsMeta.getColumnName(2));
            assertEquals("V4", rsMeta.getColumnName(3));
        } finally {
            statement.execute("DROP TABLE IF EXISTS s.t");
            conn.close();
        }
    }
     
     @Test
     public void testNoFromClauseSelect() throws Exception {
         Connection conn = DriverManager.getConnection(getUrl());
         ResultSet rs = conn.createStatement().executeQuery("SELECT 2 * 3 * 4, 5 + 1");
         assertTrue(rs.next());
         assertEquals(24, rs.getInt(1));
         assertEquals(6, rs.getInt(2));
         assertFalse(rs.next());
         
         String query = 
                 "SELECT 'a' AS col\n" +
                 "UNION ALL\n" +
                 "SELECT 'b' AS col\n" +
                 "UNION ALL\n" +
                 "SELECT 'c' AS col";
         rs = conn.createStatement().executeQuery(query);
         assertTrue(rs.next());
         assertEquals("a", rs.getString(1));
         assertTrue(rs.next());
         assertEquals("b", rs.getString(1));
         assertTrue(rs.next());
         assertEquals("c", rs.getString(1));
         assertFalse(rs.next());
 
         rs = conn.createStatement().executeQuery("SELECT * FROM (" + query + ")");
         assertTrue(rs.next());
         assertEquals("a", rs.getString(1));
         assertTrue(rs.next());
         assertEquals("b", rs.getString(1));
         assertTrue(rs.next());
         assertEquals("c", rs.getString(1));
         assertFalse(rs.next());
     }
     
     
     @Test
     public void testFailNoFromClauseSelect() throws Exception {
         Connection conn = DriverManager.getConnection(getUrl());
         try {
             try {
                 conn.createStatement().executeQuery("SELECT foo, bar");
                 fail("Should have got ColumnNotFoundException");
             } catch (ColumnNotFoundException e) {            
             }
             
             try {
                 conn.createStatement().executeQuery("SELECT *");
                 fail("Should have got SQLException");
             } catch (SQLException e) {
                 assertEquals(SQLExceptionCode.NO_TABLE_SPECIFIED_FOR_WILDCARD_SELECT.getErrorCode(), e.getErrorCode());
             }
             
             try {
                 conn.createStatement().executeQuery("SELECT A.*");
                 fail("Should have got SQLException");
             } catch (SQLException e) {
                 assertEquals(SQLExceptionCode.NO_TABLE_SPECIFIED_FOR_WILDCARD_SELECT.getErrorCode(), e.getErrorCode());
             }
         } finally {
             conn.close();
         }
     }

    @Test
    public void testServerArrayElementProjection1() throws SQLException {
        Connection conn = DriverManager.getConnection(getUrl());
        try {
            conn.createStatement().execute("CREATE TABLE t(a INTEGER PRIMARY KEY, arr INTEGER ARRAY)");
            ResultSet rs = conn.createStatement().executeQuery("EXPLAIN SELECT arr[1] from t");
            assertTrue(QueryUtil.getExplainPlan(rs).contains("    SERVER ARRAY ELEMENT PROJECTION"));
        } finally {
            conn.createStatement().execute("DROP TABLE IF EXISTS t");
            conn.close();
        }
    }

    @Test
    public void testServerArrayElementProjection2() throws SQLException {
        Connection conn = DriverManager.getConnection(getUrl());
        try {
            conn.createStatement().execute("CREATE TABLE t(a INTEGER PRIMARY KEY, arr INTEGER ARRAY)");
            ResultSet rs = conn.createStatement().executeQuery("EXPLAIN SELECT arr, arr[1] from t");
            assertFalse(QueryUtil.getExplainPlan(rs).contains("    SERVER ARRAY ELEMENT PROJECTION"));
        } finally {
            conn.createStatement().execute("DROP TABLE IF EXISTS t");
            conn.close();
        }
    }

    @Test
    public void testServerArrayElementProjection3() throws SQLException {
        Connection conn = DriverManager.getConnection(getUrl());
        try {
            conn.createStatement().execute("CREATE TABLE t(a INTEGER PRIMARY KEY, arr INTEGER ARRAY, arr2 VARCHAR ARRAY)");
            ResultSet rs = conn.createStatement().executeQuery("EXPLAIN SELECT arr, arr[1], arr2[1] from t");
            assertTrue(QueryUtil.getExplainPlan(rs).contains("    SERVER ARRAY ELEMENT PROJECTION"));
        } finally {
            conn.createStatement().execute("DROP TABLE IF EXISTS t");
            conn.close();
        }
    }

    @Test
    public void testServerArrayElementProjection4() throws SQLException {
        Connection conn = DriverManager.getConnection(getUrl());
        try {
            conn.createStatement().execute("CREATE TABLE t (p INTEGER PRIMARY KEY, arr1 INTEGER ARRAY, arr2 INTEGER ARRAY)");
            ResultSet rs = conn.createStatement().executeQuery("EXPLAIN SELECT arr1, arr1[1], ARRAY_APPEND(ARRAY_APPEND(arr1, arr2[2]), arr2[1]), p from t");
            assertTrue(QueryUtil.getExplainPlan(rs).contains("    SERVER ARRAY ELEMENT PROJECTION"));
        } finally {
            conn.createStatement().execute("DROP TABLE IF EXISTS t");
            conn.close();
        }
    }

    @Test
    public void testServerArrayElementProjection5() throws SQLException {
        Connection conn = DriverManager.getConnection(getUrl());
        try {
            conn.createStatement().execute("CREATE TABLE t (p INTEGER PRIMARY KEY, arr1 INTEGER ARRAY, arr2 INTEGER ARRAY)");
            ResultSet rs = conn.createStatement().executeQuery("EXPLAIN SELECT arr1, arr1[1], ARRAY_ELEM(ARRAY_APPEND(arr1, arr2[1]), 1), p, arr2[2] from t");
            assertTrue(QueryUtil.getExplainPlan(rs).contains("    SERVER ARRAY ELEMENT PROJECTION"));
        } finally {
            conn.createStatement().execute("DROP TABLE IF EXISTS t");
            conn.close();
        }
    }

    @Test
    public void testServerArrayElementProjectionWithArrayPrimaryKey() throws SQLException {
        Connection conn = DriverManager.getConnection(getUrl());
        try {
            conn.createStatement().execute("CREATE TABLE t(arr INTEGER ARRAY PRIMARY KEY)");
            ResultSet rs = conn.createStatement().executeQuery("EXPLAIN SELECT arr[1] from t");
            assertFalse(QueryUtil.getExplainPlan(rs).contains("    SERVER ARRAY ELEMENT PROJECTION"));
        } finally {
            conn.createStatement().execute("DROP TABLE IF EXISTS t");
            conn.close();
        }
    }
    
    @Test
    public void testAddingRowTimestampColumn() throws Exception {
        Connection conn = DriverManager.getConnection(getUrl());
        // Column of type VARCHAR cannot be declared as ROW_TIMESTAMP
        try {
            conn.createStatement().execute("CREATE TABLE T1 (PK1 VARCHAR NOT NULL, PK2 VARCHAR NOT NULL, KV1 VARCHAR CONSTRAINT PK PRIMARY KEY(PK1, PK2 ROW_TIMESTAMP)) ");
            fail("Varchar column cannot be added as row_timestamp");
        } catch(SQLException e) {
            assertEquals(SQLExceptionCode.ROWTIMESTAMP_COL_INVALID_TYPE.getErrorCode(), e.getErrorCode());
        }
        // Column of type INTEGER cannot be declared as ROW_TIMESTAMP
        try {
            conn.createStatement().execute("CREATE TABLE T1 (PK1 VARCHAR NOT NULL, PK2 INTEGER NOT NULL, KV1 VARCHAR CONSTRAINT PK PRIMARY KEY(PK1, PK2 ROW_TIMESTAMP)) ");
            fail("Integer column cannot be added as row_timestamp");
        } catch(SQLException e) {
            assertEquals(SQLExceptionCode.ROWTIMESTAMP_COL_INVALID_TYPE.getErrorCode(), e.getErrorCode());
        }
        // Column of type DOUBLE cannot be declared as ROW_TIMESTAMP
        try {
            conn.createStatement().execute("CREATE TABLE T1 (PK1 VARCHAR NOT NULL, PK2 DOUBLE NOT NULL, KV1 VARCHAR CONSTRAINT PK PRIMARY KEY(PK1, PK2 ROW_TIMESTAMP)) ");
            fail("Double column cannot be added as row_timestamp");
        } catch(SQLException e) {
            assertEquals(SQLExceptionCode.ROWTIMESTAMP_COL_INVALID_TYPE.getErrorCode(), e.getErrorCode());
        }
        // Invalid - two columns declared as row_timestamp in pk constraint
        try {
            conn.createStatement().execute("CREATE TABLE T2 (PK1 DATE NOT NULL, PK2 DATE NOT NULL, KV1 VARCHAR CONSTRAINT PK PRIMARY KEY(PK1 ROW_TIMESTAMP , PK2 ROW_TIMESTAMP)) ");
            fail("Creating table with two row_timestamp columns should fail");
        } catch (SQLException e) {
            assertEquals(SQLExceptionCode.ROWTIMESTAMP_ONE_PK_COL_ONLY.getErrorCode(), e.getErrorCode());
        }
        
        // Invalid because only (unsigned)date, time, long, (unsigned)timestamp are valid data types for column to be declared as row_timestamp
        try {
            conn.createStatement().execute("CREATE TABLE T5 (PK1 VARCHAR PRIMARY KEY ROW_TIMESTAMP, PK2 VARCHAR, KV1 VARCHAR)");
            fail("Creating table with a key value column as row_timestamp should fail");
        } catch (SQLException e) {
            assertEquals(SQLExceptionCode.ROWTIMESTAMP_COL_INVALID_TYPE.getErrorCode(), e.getErrorCode());
        }
    }

    @Test
    public void testGroupByVarbinaryOrArray() throws Exception {
        Connection conn = DriverManager.getConnection(getUrl());
        conn.createStatement().execute("CREATE TABLE T1 (PK VARCHAR PRIMARY KEY, c1 VARCHAR, c2 VARBINARY, C3 VARCHAR ARRAY, c4 VARBINARY, C5 VARCHAR ARRAY, C6 BINARY(10)) ");
        try {
            conn.createStatement().executeQuery("SELECT c1 FROM t1 GROUP BY c1,c2,c3");
            fail();
        } catch(SQLException e) {
            assertEquals(SQLExceptionCode.UNSUPPORTED_GROUP_BY_EXPRESSIONS.getErrorCode(), e.getErrorCode());
        }
        try {
            conn.createStatement().executeQuery("SELECT c1 FROM t1 GROUP BY c1,c3,c2");
            fail();
        } catch(SQLException e) {
            assertEquals(SQLExceptionCode.UNSUPPORTED_GROUP_BY_EXPRESSIONS.getErrorCode(), e.getErrorCode());
        }
        try {
            conn.createStatement().executeQuery("SELECT c1 FROM t1 GROUP BY c1,c2,c4");
            fail();
        } catch(SQLException e) {
            assertEquals(SQLExceptionCode.UNSUPPORTED_GROUP_BY_EXPRESSIONS.getErrorCode(), e.getErrorCode());
        }
        try {
            conn.createStatement().executeQuery("SELECT c1 FROM t1 GROUP BY c1,c3,c5");
            fail();
        } catch(SQLException e) {
            assertEquals(SQLExceptionCode.UNSUPPORTED_GROUP_BY_EXPRESSIONS.getErrorCode(), e.getErrorCode());
        }
        try {
            conn.createStatement().executeQuery("SELECT c1 FROM t1 GROUP BY c1,c6,c5");
            fail();
        } catch(SQLException e) {
            assertEquals(SQLExceptionCode.UNSUPPORTED_GROUP_BY_EXPRESSIONS.getErrorCode(), e.getErrorCode());
        }
    }
    
    @Test
    public void testQueryWithSCN() throws Exception {
        Properties props = PropertiesUtil.deepCopy(TEST_PROPERTIES);
        props.put(PhoenixRuntime.CURRENT_SCN_ATTRIB, Long.toString(1000));
        props.put(QueryServices.TRANSACTIONS_ENABLED, Boolean.TRUE.toString());
        try (Connection conn = DriverManager.getConnection(getUrl(), props);) {
            try {
                conn.createStatement().execute(
                                "CREATE TABLE t (k VARCHAR NOT NULL PRIMARY KEY, v1 VARCHAR) TRANSACTIONAL=true");
                fail();
            } catch (SQLException e) {
                assertEquals("Unexpected Exception",
                        SQLExceptionCode.CANNOT_START_TRANSACTION_WITH_SCN_SET
                                .getErrorCode(), e.getErrorCode());
            }
        }
    }

    @Test
    public void testNegativeGuidePostWidth() throws Exception {
        Properties props = PropertiesUtil.deepCopy(TEST_PROPERTIES);
        try (Connection conn = DriverManager.getConnection(getUrl(), props);) {
            try {
                conn.createStatement().execute(
                        "CREATE TABLE t (k VARCHAR NOT NULL PRIMARY KEY, v1 VARCHAR) GUIDE_POSTS_WIDTH = -1");
                fail();
            } catch (SQLException e) {
                assertEquals("Unexpected Exception",
                        SQLExceptionCode.PARSER_ERROR
                                .getErrorCode(), e.getErrorCode());
            }
        }
    }

    private static void assertFamilies(Scan s, String... families) {
        assertEquals(families.length, s.getFamilyMap().size());
        for (String fam : families) {
            byte[] cf = Bytes.toBytes(fam);
            assertTrue("Expected to contain " + fam, s.getFamilyMap().containsKey(cf));
        }
    }
    
    @Test
    public void testProjection() throws SQLException {
        Connection conn = DriverManager.getConnection(getUrl());
        try {
            conn.createStatement().execute("CREATE TABLE t(k INTEGER PRIMARY KEY, a.v1 VARCHAR, b.v2 VARCHAR, c.v3 VARCHAR)");
            assertFamilies(projectQuery("SELECT k FROM t"), "A");
            assertFamilies(projectQuery("SELECT k FROM t WHERE k = 5"), "A");
            assertFamilies(projectQuery("SELECT v2 FROM t WHERE k = 5"), "A", "B");
            assertFamilies(projectQuery("SELECT v2 FROM t WHERE v2 = 'a'"), "B");
            assertFamilies(projectQuery("SELECT v3 FROM t WHERE v2 = 'a'"), "B", "C");
            assertFamilies(projectQuery("SELECT v3 FROM t WHERE v2 = 'a' AND v3 is null"), "A", "B", "C");
        } finally {
            conn.close();
        }
    }
    
    private static boolean hasColumnProjectionFilter(Scan scan) {
        Iterator<Filter> iterator = ScanUtil.getFilterIterator(scan);
        while (iterator.hasNext()) {
            Filter filter = iterator.next();
            if (filter instanceof EncodedQualifiersColumnProjectionFilter) {
                return true;
            }
        }
        return false;
    }
    
    @Test
    public void testColumnProjectionOptimized() throws SQLException {
        Connection conn = DriverManager.getConnection(getUrl());
        try {
            conn.createStatement().execute("CREATE TABLE t(k INTEGER PRIMARY KEY, a.v1 VARCHAR, a.v1b VARCHAR, b.v2 VARCHAR, c.v3 VARCHAR)");
            assertTrue(hasColumnProjectionFilter(projectQuery("SELECT k, v1 FROM t WHERE v2 = 'foo'")));
            assertFalse(hasColumnProjectionFilter(projectQuery("SELECT k, v1 FROM t WHERE v1 = 'foo'")));
            assertFalse(hasColumnProjectionFilter(projectQuery("SELECT v1,v2 FROM t WHERE v1 = 'foo'")));
            assertTrue(hasColumnProjectionFilter(projectQuery("SELECT v1,v2 FROM t WHERE v1 = 'foo' and v2 = 'bar' and v3 = 'bas'")));
            assertFalse(hasColumnProjectionFilter(projectQuery("SELECT a.* FROM t WHERE v1 = 'foo' and v1b = 'bar'")));
        } finally {
            conn.close();
        }
    }
    @Test
    public void testOrderByWithNoProjection() throws SQLException {
        Connection conn = DriverManager.getConnection(getUrl());
        try {
            conn.createStatement().execute("create table x (id integer primary key, A.i1 integer," +
                    " B.i2 integer)");
            Scan scan = projectQuery("select A.i1 from X group by i1 order by avg(B.i2) " +
                    "desc");
            ServerAggregators aggregators = ServerAggregators.deserialize(scan.getAttribute
                    (BaseScannerRegionObserver.AGGREGATORS), null);
            assertEquals(2,aggregators.getAggregatorCount());
        } finally {
            conn.close();
        }
    }

    @Test
    public void testColumnProjectionUnionAll() throws SQLException {
        Connection conn = DriverManager.getConnection(getUrl());
        try {
            conn.createStatement().execute("CREATE TABLE t1(k INTEGER PRIMARY KEY,"+
                    " col1 CHAR(8), col2 VARCHAR(10), col3 decimal(10,2))");
            conn.createStatement().execute("CREATE TABLE t2(k TINYINT PRIMARY KEY," +
                    " col1 CHAR(20), col2 CHAR(30), col3 double)");
            QueryPlan plan = getQueryPlan("SELECT * from t1 union all select * from t2",
                Collections.emptyList());
            RowProjector rowProj = plan.getProjector();
            assertTrue(rowProj.getColumnProjector(0).getExpression().getDataType()
                instanceof PInteger);
            assertTrue(rowProj.getColumnProjector(1).getExpression().getDataType()
                instanceof PChar);
            assertTrue(rowProj.getColumnProjector(1).getExpression().getMaxLength() == 20);
            assertTrue(rowProj.getColumnProjector(2).getExpression().getDataType()
                instanceof PVarchar);
            assertTrue(rowProj.getColumnProjector(2).getExpression().getMaxLength() == 30);
            assertTrue(rowProj.getColumnProjector(3).getExpression().getDataType()
                instanceof PDecimal);
            assertTrue(rowProj.getColumnProjector(3).getExpression().getScale() == 2);
        } finally {
            conn.close();
        }
    }
    
    @Test
    public void testFuncIndexUsage() throws SQLException {
        Connection conn = DriverManager.getConnection(getUrl());
        try {
            conn.createStatement().execute("CREATE TABLE t1(k INTEGER PRIMARY KEY,"+
                    " col1 VARCHAR, col2 VARCHAR)");
            conn.createStatement().execute("CREATE TABLE t2(k INTEGER PRIMARY KEY," +
                    " col1 VARCHAR, col2 VARCHAR)");
            conn.createStatement().execute("CREATE TABLE t3(j INTEGER PRIMARY KEY," +
                    " col3 VARCHAR, col4 VARCHAR)");
            conn.createStatement().execute("CREATE INDEX idx ON t1 (col1 || col2)");
            String query = "SELECT a.k from t1 a where a.col1 || a.col2 = 'foobar'";
            ResultSet rs = conn.createStatement().executeQuery("EXPLAIN "+query);
            String explainPlan = QueryUtil.getExplainPlan(rs);
            assertEquals("CLIENT PARALLEL 1-WAY RANGE SCAN OVER IDX ['foobar']\n" + 
                    "    SERVER FILTER BY FIRST KEY ONLY",explainPlan);
            query = "SELECT k,j from t3 b join t1 a ON k = j where a.col1 || a.col2 = 'foobar'";
            rs = conn.createStatement().executeQuery("EXPLAIN "+query);
            explainPlan = QueryUtil.getExplainPlan(rs);
            assertEquals("CLIENT PARALLEL 1-WAY FULL SCAN OVER T3\n" + 
                    "    SERVER FILTER BY FIRST KEY ONLY\n" + 
                    "    PARALLEL INNER-JOIN TABLE 0\n" + 
                    "        CLIENT PARALLEL 1-WAY RANGE SCAN OVER IDX ['foobar']\n" + 
                    "            SERVER FILTER BY FIRST KEY ONLY\n" + 
                    "    DYNAMIC SERVER FILTER BY B.J IN (\"A.:K\")",explainPlan);
            query = "SELECT a.k,b.k from t2 b join t1 a ON a.k = b.k where a.col1 || a.col2 = 'foobar'";
            rs = conn.createStatement().executeQuery("EXPLAIN "+query);
            explainPlan = QueryUtil.getExplainPlan(rs);
            assertEquals("CLIENT PARALLEL 1-WAY FULL SCAN OVER T2\n" + 
                    "    SERVER FILTER BY FIRST KEY ONLY\n" + 
                    "    PARALLEL INNER-JOIN TABLE 0\n" + 
                    "        CLIENT PARALLEL 1-WAY RANGE SCAN OVER IDX ['foobar']\n" + 
                    "            SERVER FILTER BY FIRST KEY ONLY\n" + 
                    "    DYNAMIC SERVER FILTER BY B.K IN (\"A.:K\")",explainPlan);
        } finally {
            conn.close();
        }
    }
    
    @Test
    public void testSaltTableJoin() throws Exception{

        PhoenixConnection conn = (PhoenixConnection)DriverManager.getConnection(getUrl());
        try {
            conn.createStatement().execute("drop table if exists SALT_TEST2900");

            conn.createStatement().execute(
                "create table SALT_TEST2900"+
                        "("+
                        "id UNSIGNED_INT not null primary key,"+
                        "appId VARCHAR"+
                    ")SALT_BUCKETS=2");



            conn.createStatement().execute("drop table if exists RIGHT_TEST2900 ");
            conn.createStatement().execute(
                "create table RIGHT_TEST2900"+
                        "("+
                        "appId VARCHAR not null primary key,"+
                        "createTime VARCHAR"+
                    ")");

            
            String sql="select * from SALT_TEST2900 a inner join RIGHT_TEST2900 b on a.appId=b.appId where a.id>=3 and a.id<=5";
            HashJoinPlan plan = (HashJoinPlan)getQueryPlan(sql, Collections.emptyList());
            ScanRanges ranges=plan.getContext().getScanRanges();

            List<HRegionLocation> regionLocations=
                    conn.getQueryServices().getAllTableRegions(Bytes.toBytes("SALT_TEST2900"));
            for (HRegionLocation regionLocation : regionLocations) {
                assertTrue(ranges.intersectRegion(regionLocation.getRegionInfo().getStartKey(),
                    regionLocation.getRegionInfo().getEndKey(), false));
            }
        } finally {
            conn.close();
        }
    }
    
    @Test
    public void testStatefulDefault() throws Exception {
        String ddl = "CREATE TABLE table_with_default (" +
                "pk INTEGER PRIMARY KEY, " +
                "datecol DATE DEFAULT CURRENT_DATE())";

        Connection conn = DriverManager.getConnection(getUrl());
        try {
            conn.createStatement().execute(ddl);
            fail();
        } catch (SQLException e) {
            assertEquals(SQLExceptionCode.CANNOT_CREATE_DEFAULT.getErrorCode(), e.getErrorCode());
        }
    }

    @Test
    public void testAlterTableStatefulDefault() throws Exception {
        String ddl = "CREATE TABLE table_with_default (" +
                "pk INTEGER PRIMARY KEY)";
        String ddl2 = "ALTER TABLE table_with_default " +
                "ADD datecol DATE DEFAULT CURRENT_DATE()";

        Connection conn = DriverManager.getConnection(getUrl());
        conn.createStatement().execute(ddl);
        try {
            conn.createStatement().execute(ddl2);
            fail();
        } catch (SQLException e) {
            assertEquals(SQLExceptionCode.CANNOT_CREATE_DEFAULT.getErrorCode(), e.getErrorCode());
        }
    }
    
    @Test
    public void testDefaultTypeMismatch() throws Exception {
        String ddl = "CREATE TABLE table_with_default (" +
                "pk INTEGER PRIMARY KEY, " +
                "v VARCHAR DEFAULT 1)";

        Connection conn = DriverManager.getConnection(getUrl());
        try {
            conn.createStatement().execute(ddl);
            fail();
        } catch (SQLException e) {
            assertEquals(SQLExceptionCode.TYPE_MISMATCH.getErrorCode(), e.getErrorCode());
        }
    }

    @Test
    public void testAlterTableDefaultTypeMismatch() throws Exception {
        String ddl = "CREATE TABLE table_with_default (" +
                "pk INTEGER PRIMARY KEY)";
        String ddl2 = "ALTER TABLE table_with_default " +
                "ADD v CHAR(3) DEFAULT 1";

        Connection conn = DriverManager.getConnection(getUrl());
        conn.createStatement().execute(ddl);
        try {
            conn.createStatement().execute(ddl2);
            fail();
        } catch (SQLException e) {
            assertEquals(SQLExceptionCode.TYPE_MISMATCH.getErrorCode(), e.getErrorCode());
        }
    }
    
    @Test
    public void testDefaultTypeMismatchInView() throws Exception {
        String ddl1 = "CREATE TABLE table_with_default (" +
                "pk INTEGER PRIMARY KEY, " +
                "v VARCHAR DEFAULT 'foo')";
        String ddl2 = "CREATE VIEW my_view(v2 VARCHAR DEFAULT 1) AS SELECT * FROM table_with_default";

        Connection conn = DriverManager.getConnection(getUrl());
        conn.createStatement().execute(ddl1);
        try {
            conn.createStatement().execute(ddl2);
            fail();
        } catch (SQLException e) {
            assertEquals(SQLExceptionCode.TYPE_MISMATCH.getErrorCode(), e.getErrorCode());
        }
    }

    @Test
    public void testDefaultRowTimestamp1() throws Exception {
        String ddl = "CREATE TABLE IF NOT EXISTS table_with_defaults ("
                + "pk1 INTEGER NOT NULL,"
                + "pk2 BIGINT NOT NULL DEFAULT 5,"
                + "CONSTRAINT NAME_PK PRIMARY KEY (pk1, pk2 ROW_TIMESTAMP))";

        Connection conn = DriverManager.getConnection(getUrl());

        try {
            conn.createStatement().execute(ddl);
            fail();
        } catch (SQLException e) {
            assertEquals(
                    SQLExceptionCode.CANNOT_CREATE_DEFAULT_ROWTIMESTAMP.getErrorCode(),
                    e.getErrorCode());
        }
    }

    @Test
    public void testDefaultRowTimestamp2() throws Exception {
        String ddl = "CREATE TABLE table_with_defaults ("
                + "k BIGINT DEFAULT 5 PRIMARY KEY ROW_TIMESTAMP)";

        Connection conn = DriverManager.getConnection(getUrl());

        try {
            conn.createStatement().execute(ddl);
            fail();
        } catch (SQLException e) {
            assertEquals(
                    SQLExceptionCode.CANNOT_CREATE_DEFAULT_ROWTIMESTAMP.getErrorCode(),
                    e.getErrorCode());
        }
    }

    @Test
    public void testDefaultSizeMismatch() throws Exception {
        String ddl = "CREATE TABLE table_with_default (" +
                "pk INTEGER PRIMARY KEY, " +
                "v CHAR(3) DEFAULT 'foobar')";

        Connection conn = DriverManager.getConnection(getUrl());
        try {
            conn.createStatement().execute(ddl);
            fail();
        } catch (SQLException e) {
            assertEquals(SQLExceptionCode.DATA_EXCEEDS_MAX_CAPACITY.getErrorCode(), e.getErrorCode());
        }
    }
    
    @Test
    public void testAlterTableDefaultSizeMismatch() throws Exception {
        String ddl = "CREATE TABLE table_with_default (" +
                "pk INTEGER PRIMARY KEY)";
        String ddl2 = "ALTER TABLE table_with_default " +
                "ADD v CHAR(3) DEFAULT 'foobar'";

        Connection conn = DriverManager.getConnection(getUrl());
        conn.createStatement().execute(ddl);
        try {
            conn.createStatement().execute(ddl2);
            fail();
        } catch (SQLException e) {
            assertEquals(SQLExceptionCode.DATA_EXCEEDS_MAX_CAPACITY.getErrorCode(), e.getErrorCode());
        }
    }
    
    @Test
    public void testNullDefaultRemoved() throws Exception {
        String ddl = "CREATE TABLE table_with_default (" +
                "pk INTEGER PRIMARY KEY, " +
                "v VARCHAR DEFAULT null)";

        Connection conn = DriverManager.getConnection(getUrl());
        conn.createStatement().execute(ddl);
        PTable table = conn.unwrap(PhoenixConnection.class).getMetaDataCache()
                .getTableRef(new PTableKey(null,"TABLE_WITH_DEFAULT")).getTable();
        assertNull(table.getColumnForColumnName("V").getExpressionStr());
    }

    @Test
    public void testNullAlterTableDefaultRemoved() throws Exception {
        String ddl = "CREATE TABLE table_with_default (" +
                "pk INTEGER PRIMARY KEY)";
        String ddl2 = "ALTER TABLE table_with_default " +
                "ADD v CHAR(3) DEFAULT null";

        Connection conn = DriverManager.getConnection(getUrl());
        conn.createStatement().execute(ddl);
        conn.createStatement().execute(ddl2);
        PTable table = conn.unwrap(PhoenixConnection.class).getMetaDataCache()
                .getTableRef(new PTableKey(null,"TABLE_WITH_DEFAULT")).getTable();
        assertNull(table.getColumnForColumnName("V").getExpressionStr());
    }

    @Test
    public void testIndexOnViewWithChildView() throws SQLException {
        try (Connection conn = DriverManager.getConnection(getUrl())) {
            conn.createStatement().execute("CREATE TABLE PLATFORM_ENTITY.GLOBAL_TABLE (\n" + 
                    "    ORGANIZATION_ID CHAR(15) NOT NULL,\n" + 
                    "    KEY_PREFIX CHAR(3) NOT NULL,\n" + 
                    "    CREATED_DATE DATE,\n" + 
                    "    CREATED_BY CHAR(15),\n" + 
                    "    CONSTRAINT PK PRIMARY KEY (\n" + 
                    "        ORGANIZATION_ID,\n" + 
                    "        KEY_PREFIX\n" + 
                    "    )\n" + 
                    ") VERSIONS=1, IMMUTABLE_ROWS=true, MULTI_TENANT=true");
            conn.createStatement().execute("CREATE VIEW PLATFORM_ENTITY.GLOBAL_VIEW  (\n" + 
                    "    INT1 BIGINT NOT NULL,\n" + 
                    "    DOUBLE1 DECIMAL(12, 3),\n" + 
                    "    IS_BOOLEAN BOOLEAN,\n" + 
                    "    TEXT1 VARCHAR,\n" + 
                    "    CONSTRAINT PKVIEW PRIMARY KEY\n" + 
                    "    (\n" + 
                    "        INT1\n" + 
                    "    )\n" + 
                    ")\n" + 
                    "AS SELECT * FROM PLATFORM_ENTITY.GLOBAL_TABLE WHERE KEY_PREFIX = '123'");
            conn.createStatement().execute("CREATE INDEX GLOBAL_INDEX\n" + 
                    "ON PLATFORM_ENTITY.GLOBAL_VIEW (TEXT1 DESC, INT1)\n" + 
                    "INCLUDE (CREATED_BY, DOUBLE1, IS_BOOLEAN, CREATED_DATE)");
            String query = "SELECT DOUBLE1 FROM PLATFORM_ENTITY.GLOBAL_VIEW\n"
                    + "WHERE ORGANIZATION_ID = '00Dxx0000002Col' AND TEXT1='Test' AND INT1=1";
            QueryPlan plan = getOptimizedQueryPlan(query);
            assertEquals("PLATFORM_ENTITY.GLOBAL_VIEW", plan.getContext().getCurrentTable().getTable().getName()
                    .getString());
            query = "SELECT DOUBLE1 FROM PLATFORM_ENTITY.GLOBAL_VIEW\n"
                    + "WHERE ORGANIZATION_ID = '00Dxx0000002Col' AND TEXT1='Test'";
            plan = getOptimizedQueryPlan(query);
            assertEquals("PLATFORM_ENTITY.GLOBAL_INDEX", plan.getContext().getCurrentTable().getTable().getName().getString());
        }
    }

    @Test
    public void testOnDupKeyForImmutableTable() throws Exception {
        Connection conn = DriverManager.getConnection(getUrl());
        try {
            conn.createStatement().execute("CREATE TABLE t1 (k integer not null primary key, v bigint) IMMUTABLE_ROWS=true");
            conn.createStatement().execute("UPSERT INTO t1 VALUES(0,0) ON DUPLICATE KEY UPDATE v = v + 1");
            fail();
        } catch (SQLException e) {
            assertEquals(SQLExceptionCode.CANNOT_USE_ON_DUP_KEY_FOR_IMMUTABLE.getErrorCode(), e.getErrorCode());
        } finally {
            conn.close();
        }
    }

    @Test
    public void testUpdatePKOnDupKey() throws Exception {
        Connection conn = DriverManager.getConnection(getUrl());
        try {
            conn.createStatement().execute("CREATE TABLE t1 (k1 integer not null, k2 integer not null, v bigint, constraint pk primary key (k1,k2))");
            conn.createStatement().execute("UPSERT INTO t1 VALUES(0,0) ON DUPLICATE KEY UPDATE k2 = v + 1");
            fail();
        } catch (SQLException e) {
            assertEquals(SQLExceptionCode.CANNOT_UPDATE_PK_ON_DUP_KEY.getErrorCode(), e.getErrorCode());
        } finally {
            conn.close();
        }
    }

    @Test
    public void testOnDupKeyTypeMismatch() throws Exception {
        Connection conn = DriverManager.getConnection(getUrl());
        try {
            conn.createStatement().execute("CREATE TABLE t1 (k1 integer not null, k2 integer not null, v1 bigint, v2 varchar, constraint pk primary key (k1,k2))");
            conn.createStatement().execute("UPSERT INTO t1 VALUES(0,0) ON DUPLICATE KEY UPDATE v1 = v2 || 'a'");
            fail();
        } catch (SQLException e) {
            assertEquals(SQLExceptionCode.TYPE_MISMATCH.getErrorCode(), e.getErrorCode());
        } finally {
            conn.close();
        }
    }
    
    @Test
    public void testDuplicateColumnOnDupKeyUpdate() throws Exception {
        Connection conn = DriverManager.getConnection(getUrl());
        try {
            conn.createStatement().execute("CREATE TABLE t1 (k1 integer not null, k2 integer not null, v1 bigint, v2 bigint, constraint pk primary key (k1,k2))");
            conn.createStatement().execute("UPSERT INTO t1 VALUES(0,0) ON DUPLICATE KEY UPDATE v1 = v1 + 1, v1 = v2 + 2");
            fail();
        } catch (SQLException e) {
            assertEquals(SQLExceptionCode.DUPLICATE_COLUMN_IN_ON_DUP_KEY.getErrorCode(), e.getErrorCode());
        } finally {
            conn.close();
        }
    }
    
    @Test
    public void testAggregationInOnDupKey() throws Exception {
        Connection conn = DriverManager.getConnection(getUrl());
        conn.createStatement().execute("CREATE TABLE t1 (k1 integer not null, k2 integer not null, v bigint, constraint pk primary key (k1,k2))");
        try {
            conn.createStatement().execute("UPSERT INTO t1 VALUES(0,0) ON DUPLICATE KEY UPDATE v = sum(v)");
            fail();
        } catch (SQLException e) {
            assertEquals(SQLExceptionCode.AGGREGATION_NOT_ALLOWED_IN_ON_DUP_KEY.getErrorCode(), e.getErrorCode());
        } finally {
            conn.close();
        }
    }
    
    @Test
    public void testSequenceInOnDupKey() throws Exception {
        Connection conn = DriverManager.getConnection(getUrl());
        conn.createStatement().execute("CREATE TABLE t1 (k1 integer not null, k2 integer not null, v bigint, constraint pk primary key (k1,k2))");
        conn.createStatement().execute("CREATE SEQUENCE s1");
        try {
            conn.createStatement().execute("UPSERT INTO t1 VALUES(0,0) ON DUPLICATE KEY UPDATE v = next value for s1");
            fail();
        } catch (SQLException e) {
            assertEquals(SQLExceptionCode.INVALID_USE_OF_NEXT_VALUE_FOR.getErrorCode(), e.getErrorCode());
        } finally {
            conn.close();
        }
    }
    
    @Test
    public void testSCNInOnDupKey() throws Exception {
        String url = getUrl() + ";" + PhoenixRuntime.CURRENT_SCN_ATTRIB + "=100";
        Connection conn = DriverManager.getConnection(url);
        conn.createStatement().execute("CREATE TABLE t1 (k1 integer not null, k2 integer not null, v bigint, constraint pk primary key (k1,k2))");
        try {
            conn.createStatement().execute("UPSERT INTO t1 VALUES(0,0) ON DUPLICATE KEY UPDATE v = v + 1");
            fail();
        } catch (SQLException e) {
            assertEquals(SQLExceptionCode.CANNOT_SET_SCN_IN_ON_DUP_KEY.getErrorCode(), e.getErrorCode());
        } finally {
            conn.close();
        }
    }

    @Test
    public void testOrderPreservingGroupBy() throws Exception {
        try (Connection conn= DriverManager.getConnection(getUrl())) {

            conn.createStatement().execute("CREATE TABLE test (\n" + 
                    "            pk1 INTEGER NOT NULL,\n" + 
                    "            pk2 INTEGER NOT NULL,\n" + 
                    "            pk3 INTEGER NOT NULL,\n" + 
                    "            pk4 INTEGER NOT NULL,\n" + 
                    "            v1 INTEGER,\n" + 
                    "            CONSTRAINT pk PRIMARY KEY (\n" + 
                    "               pk1,\n" + 
                    "               pk2,\n" + 
                    "               pk3,\n" + 
                    "               pk4\n" + 
                    "             )\n" + 
                    "         )");
            String[] queries = new String[] {
                    "SELECT pk3 FROM test WHERE pk2 = 1 GROUP BY pk2+1,pk3 ORDER BY pk3",
                    "SELECT pk3 FROM test WHERE pk2 = 1 GROUP BY pk2,pk3 ORDER BY pk3",
                    "SELECT pk3 FROM test WHERE pk1 = 1 and pk2 = 2 GROUP BY pk1+pk2,pk3 ORDER BY pk3",
                    "SELECT pk3 FROM test WHERE pk1 = 1 and pk2 = 2 GROUP BY pk4,CASE WHEN pk1 > pk2 THEN pk1 ELSE pk2 END,pk3 ORDER BY pk4,pk3",
            };
            int index = 0;
            for (String query : queries) {
                QueryPlan plan = getQueryPlan(conn, query);
                assertTrue((index + 1) + ") " + queries[index], plan.getOrderBy().getOrderByExpressions().isEmpty());
                index++;
            }
        }
    }
    
    @Test
    public void testNotOrderPreservingGroupBy() throws Exception {
        try (Connection conn= DriverManager.getConnection(getUrl())) {

            conn.createStatement().execute("CREATE TABLE test (\n" + 
                    "            pk1 INTEGER NOT NULL,\n" + 
                    "            pk2 INTEGER NOT NULL,\n" + 
                    "            pk3 INTEGER NOT NULL,\n" + 
                    "            pk4 INTEGER NOT NULL,\n" + 
                    "            v1 INTEGER,\n" + 
                    "            CONSTRAINT pk PRIMARY KEY (\n" + 
                    "               pk1,\n" + 
                    "               pk2,\n" + 
                    "               pk3,\n" + 
                    "               pk4\n" + 
                    "             )\n" + 
                    "         )");
            String[] queries = new String[] {
                    "SELECT pk3 FROM test WHERE pk1 = 1 and pk2 = 2 GROUP BY pk4,CASE WHEN pk1 > pk2 THEN coalesce(v1,1) ELSE pk2 END,pk3 ORDER BY pk4,pk3",
                    "SELECT pk3 FROM test WHERE pk1 = 1 and pk2 = 2 GROUP BY CASE WHEN pk1 > pk2 THEN v1 WHEN pk1 = pk2 THEN pk1 ELSE pk2 END,pk3 ORDER BY CASE WHEN pk1 > pk2 THEN v1 WHEN pk1 = pk2 THEN pk1 ELSE pk2 END,pk3",
                    "SELECT pk3 FROM test WHERE pk1 = 1 and pk2 = 2 GROUP BY CASE WHEN pk1 > pk2 THEN v1 WHEN pk1 = pk2 THEN pk1 ELSE pk2 END,pk3 ORDER BY CASE WHEN pk1 > pk2 THEN v1 WHEN pk1 = pk2 THEN pk1 ELSE pk2 END,pk3",
                    "SELECT pk3 FROM test GROUP BY pk2,pk3 ORDER BY pk3",
                    "SELECT pk3 FROM test WHERE pk1 = 1 GROUP BY pk1,pk2,pk3 ORDER BY pk3",
                    "SELECT pk3 FROM test WHERE pk1 = 1 GROUP BY RAND()+pk1,pk2,pk3 ORDER BY pk3",
                    "SELECT pk3 FROM test WHERE pk1 = 1 and pk2 = 2 GROUP BY CASE WHEN pk1 > pk2 THEN pk1 ELSE RAND(1) END,pk3 ORDER BY pk3",
            };
            int index = 0;
            for (String query : queries) {
                QueryPlan plan = getQueryPlan(conn, query);
                assertFalse((index + 1) + ") " + queries[index], plan.getOrderBy().getOrderByExpressions().isEmpty());
                index++;
            }
        }
    }
    
    @Test
    public void testGroupByDescColumnBug3451() throws Exception {

        try (Connection conn= DriverManager.getConnection(getUrl())) {

            conn.createStatement().execute("CREATE TABLE IF NOT EXISTS GROUPBYTEST (\n" + 
                    "            ORGANIZATION_ID CHAR(15) NOT NULL,\n" + 
                    "            CONTAINER_ID CHAR(15) NOT NULL,\n" + 
                    "            ENTITY_ID CHAR(15) NOT NULL,\n" + 
                    "            SCORE DOUBLE,\n" + 
                    "            CONSTRAINT TEST_PK PRIMARY KEY (\n" + 
                    "               ORGANIZATION_ID,\n" + 
                    "               CONTAINER_ID,\n" + 
                    "               ENTITY_ID\n" + 
                    "             )\n" + 
                    "         )");
            conn.createStatement().execute("CREATE INDEX SCORE_IDX ON GROUPBYTEST (ORGANIZATION_ID,CONTAINER_ID, SCORE DESC, ENTITY_ID DESC)");
            QueryPlan plan = getQueryPlan(conn, "SELECT DISTINCT entity_id, score\n" + 
                    "    FROM GROUPBYTEST\n" + 
                    "    WHERE organization_id = 'org2'\n" + 
                    "    AND container_id IN ( 'container1','container2','container3' )\n" + 
                    "    ORDER BY score DESC\n" + 
                    "    LIMIT 2");
            assertFalse(plan.getOrderBy().getOrderByExpressions().isEmpty());
            plan = getQueryPlan(conn, "SELECT DISTINCT entity_id, score\n" + 
                    "    FROM GROUPBYTEST\n" + 
                    "    WHERE entity_id = 'entity1'\n" + 
                    "    AND container_id IN ( 'container1','container2','container3' )\n" + 
                    "    ORDER BY score DESC\n" + 
                    "    LIMIT 2");
            assertTrue(plan.getOrderBy().getOrderByExpressions().isEmpty());
        }
    }

    @Test
    public void testGroupByDescColumnBug3452() throws Exception {

       Connection conn=null;
        try {
            conn= DriverManager.getConnection(getUrl());

            String sql="CREATE TABLE GROUPBYDESC3452 ( "+
                "ORGANIZATION_ID VARCHAR,"+
                "CONTAINER_ID VARCHAR,"+
                "ENTITY_ID VARCHAR NOT NULL,"+
                "CONSTRAINT TEST_PK PRIMARY KEY ( "+
                "ORGANIZATION_ID DESC,"+
                "CONTAINER_ID DESC,"+
                "ENTITY_ID"+
                "))";
            conn.createStatement().execute(sql);

            //-----ORGANIZATION_ID

            sql="SELECT CONTAINER_ID,ORGANIZATION_ID FROM GROUPBYDESC3452 group by ORGANIZATION_ID, CONTAINER_ID order by ORGANIZATION_ID ASC NULLS FIRST";
            QueryPlan queryPlan =getQueryPlan(conn, sql);
            assertTrue(queryPlan.getGroupBy().isOrderPreserving());
            assertTrue(queryPlan.getOrderBy().getOrderByExpressions().size()==1);
            assertTrue(queryPlan.getOrderBy().getOrderByExpressions().get(0).toString().equals("ORGANIZATION_ID"));

            sql="SELECT CONTAINER_ID,ORGANIZATION_ID FROM GROUPBYDESC3452 group by ORGANIZATION_ID, CONTAINER_ID order by ORGANIZATION_ID ASC NULLS LAST";
            queryPlan =getQueryPlan(conn, sql);
            assertTrue(queryPlan.getGroupBy().isOrderPreserving());
            assertTrue(queryPlan.getOrderBy()== OrderBy.REV_ROW_KEY_ORDER_BY);

            sql="SELECT CONTAINER_ID,ORGANIZATION_ID FROM GROUPBYDESC3452 group by ORGANIZATION_ID, CONTAINER_ID order by ORGANIZATION_ID DESC NULLS FIRST";
            queryPlan =getQueryPlan(conn, sql);
            assertTrue(queryPlan.getGroupBy().isOrderPreserving());
            assertTrue(queryPlan.getOrderBy()== OrderBy.FWD_ROW_KEY_ORDER_BY);

            sql="SELECT CONTAINER_ID,ORGANIZATION_ID FROM GROUPBYDESC3452 group by ORGANIZATION_ID, CONTAINER_ID order by ORGANIZATION_ID DESC NULLS LAST";
            queryPlan =getQueryPlan(conn, sql);
            assertTrue(queryPlan.getGroupBy().isOrderPreserving());
            assertTrue(queryPlan.getOrderBy().getOrderByExpressions().size()==1);
            assertTrue(queryPlan.getOrderBy().getOrderByExpressions().get(0).toString().equals("ORGANIZATION_ID DESC NULLS LAST"));

            //----CONTAINER_ID

            sql="SELECT CONTAINER_ID,ORGANIZATION_ID FROM GROUPBYDESC3452 group by ORGANIZATION_ID, CONTAINER_ID order by CONTAINER_ID ASC NULLS FIRST";
            queryPlan =getQueryPlan(conn, sql);
            assertTrue(queryPlan.getGroupBy().isOrderPreserving());
            assertTrue(queryPlan.getOrderBy().getOrderByExpressions().size()==1);
            assertTrue(queryPlan.getOrderBy().getOrderByExpressions().get(0).toString().equals("CONTAINER_ID"));

            sql="SELECT CONTAINER_ID,ORGANIZATION_ID FROM GROUPBYDESC3452 group by ORGANIZATION_ID, CONTAINER_ID order by CONTAINER_ID ASC NULLS LAST";
            queryPlan =getQueryPlan(conn, sql);
            assertTrue(queryPlan.getGroupBy().isOrderPreserving());
            assertTrue(queryPlan.getOrderBy().getOrderByExpressions().size()==1);
            assertTrue(queryPlan.getOrderBy().getOrderByExpressions().get(0).toString().equals("CONTAINER_ID NULLS LAST"));

            sql="SELECT CONTAINER_ID,ORGANIZATION_ID FROM GROUPBYDESC3452 group by ORGANIZATION_ID, CONTAINER_ID order by CONTAINER_ID DESC NULLS FIRST";
            queryPlan =getQueryPlan(conn, sql);
            assertTrue(queryPlan.getGroupBy().isOrderPreserving());
            assertTrue(queryPlan.getOrderBy().getOrderByExpressions().size()==1);
            assertTrue(queryPlan.getOrderBy().getOrderByExpressions().get(0).toString().equals("CONTAINER_ID DESC"));

            sql="SELECT CONTAINER_ID,ORGANIZATION_ID FROM GROUPBYDESC3452 group by ORGANIZATION_ID, CONTAINER_ID order by CONTAINER_ID DESC NULLS LAST";
            queryPlan =getQueryPlan(conn, sql);
            assertTrue(queryPlan.getGroupBy().isOrderPreserving());
            assertTrue(queryPlan.getOrderBy().getOrderByExpressions().size()==1);
            assertTrue(queryPlan.getOrderBy().getOrderByExpressions().get(0).toString().equals("CONTAINER_ID DESC NULLS LAST"));

            //-----ORGANIZATION_ID ASC  CONTAINER_ID ASC

            sql="SELECT CONTAINER_ID,ORGANIZATION_ID FROM GROUPBYDESC3452 group by ORGANIZATION_ID, CONTAINER_ID order by ORGANIZATION_ID NULLS FIRST,CONTAINER_ID NULLS FIRST";
            queryPlan =getQueryPlan(conn, sql);
            assertTrue(queryPlan.getGroupBy().isOrderPreserving());
            assertTrue(queryPlan.getOrderBy().getOrderByExpressions().size()==2);
            assertTrue(queryPlan.getOrderBy().getOrderByExpressions().get(0).toString().equals("ORGANIZATION_ID"));
            assertTrue(queryPlan.getOrderBy().getOrderByExpressions().get(1).toString().equals("CONTAINER_ID"));

            sql="SELECT CONTAINER_ID,ORGANIZATION_ID FROM GROUPBYDESC3452 group by ORGANIZATION_ID, CONTAINER_ID order by ORGANIZATION_ID NULLS FIRST,CONTAINER_ID NULLS LAST";
            queryPlan =getQueryPlan(conn, sql);
            assertTrue(queryPlan.getGroupBy().isOrderPreserving());
            assertTrue(queryPlan.getOrderBy().getOrderByExpressions().size()==2);
            assertTrue(queryPlan.getOrderBy().getOrderByExpressions().get(0).toString().equals("ORGANIZATION_ID"));
            assertTrue(queryPlan.getOrderBy().getOrderByExpressions().get(1).toString().equals("CONTAINER_ID NULLS LAST"));

            sql="SELECT CONTAINER_ID,ORGANIZATION_ID FROM GROUPBYDESC3452 group by ORGANIZATION_ID, CONTAINER_ID order by ORGANIZATION_ID NULLS LAST,CONTAINER_ID NULLS FIRST";
            queryPlan =getQueryPlan(conn, sql);
            assertTrue(queryPlan.getGroupBy().isOrderPreserving());
            assertTrue(queryPlan.getOrderBy().getOrderByExpressions().size()==2);
            assertTrue(queryPlan.getOrderBy().getOrderByExpressions().get(0).toString().equals("ORGANIZATION_ID NULLS LAST"));
            assertTrue(queryPlan.getOrderBy().getOrderByExpressions().get(1).toString().equals("CONTAINER_ID"));

            sql="SELECT CONTAINER_ID,ORGANIZATION_ID FROM GROUPBYDESC3452 group by ORGANIZATION_ID, CONTAINER_ID order by ORGANIZATION_ID NULLS LAST,CONTAINER_ID NULLS LAST";
            queryPlan =getQueryPlan(conn, sql);
            assertTrue(queryPlan.getGroupBy().isOrderPreserving());
            assertTrue(queryPlan.getOrderBy() == OrderBy.REV_ROW_KEY_ORDER_BY);

            //-----ORGANIZATION_ID ASC  CONTAINER_ID DESC

            sql="SELECT CONTAINER_ID,ORGANIZATION_ID FROM GROUPBYDESC3452 group by ORGANIZATION_ID, CONTAINER_ID order by ORGANIZATION_ID ASC NULLS FIRST,CONTAINER_ID DESC NULLS FIRST";
            queryPlan =getQueryPlan(conn, sql);
            assertTrue(queryPlan.getGroupBy().isOrderPreserving());
            assertTrue(queryPlan.getOrderBy().getOrderByExpressions().size()==2);
            assertTrue(queryPlan.getOrderBy().getOrderByExpressions().get(0).toString().equals("ORGANIZATION_ID"));
            assertTrue(queryPlan.getOrderBy().getOrderByExpressions().get(1).toString().equals("CONTAINER_ID DESC"));

            sql="SELECT CONTAINER_ID,ORGANIZATION_ID FROM GROUPBYDESC3452 group by ORGANIZATION_ID, CONTAINER_ID order by ORGANIZATION_ID ASC NULLS FIRST,CONTAINER_ID DESC NULLS LAST";
            queryPlan =getQueryPlan(conn, sql);
            assertTrue(queryPlan.getGroupBy().isOrderPreserving());
            assertTrue(queryPlan.getOrderBy().getOrderByExpressions().size()==2);
            assertTrue(queryPlan.getOrderBy().getOrderByExpressions().get(0).toString().equals("ORGANIZATION_ID"));
            assertTrue(queryPlan.getOrderBy().getOrderByExpressions().get(1).toString().equals("CONTAINER_ID DESC NULLS LAST"));

            sql="SELECT CONTAINER_ID,ORGANIZATION_ID FROM GROUPBYDESC3452 group by ORGANIZATION_ID, CONTAINER_ID order by ORGANIZATION_ID ASC NULLS LAST,CONTAINER_ID DESC NULLS FIRST";
            queryPlan =getQueryPlan(conn, sql);
            assertTrue(queryPlan.getGroupBy().isOrderPreserving());
            assertTrue(queryPlan.getOrderBy().getOrderByExpressions().size()==2);
            assertTrue(queryPlan.getOrderBy().getOrderByExpressions().get(0).toString().equals("ORGANIZATION_ID NULLS LAST"));
            assertTrue(queryPlan.getOrderBy().getOrderByExpressions().get(1).toString().equals("CONTAINER_ID DESC"));

            sql="SELECT CONTAINER_ID,ORGANIZATION_ID FROM GROUPBYDESC3452 group by ORGANIZATION_ID, CONTAINER_ID order by ORGANIZATION_ID ASC NULLS LAST,CONTAINER_ID DESC NULLS LAST";
            queryPlan =getQueryPlan(conn, sql);
            assertTrue(queryPlan.getGroupBy().isOrderPreserving());
            assertTrue(queryPlan.getOrderBy().getOrderByExpressions().size()==2);
            assertTrue(queryPlan.getOrderBy().getOrderByExpressions().get(0).toString().equals("ORGANIZATION_ID NULLS LAST"));
            assertTrue(queryPlan.getOrderBy().getOrderByExpressions().get(1).toString().equals("CONTAINER_ID DESC NULLS LAST"));

            //-----ORGANIZATION_ID DESC  CONTAINER_ID ASC

            sql="SELECT CONTAINER_ID,ORGANIZATION_ID FROM GROUPBYDESC3452 group by ORGANIZATION_ID, CONTAINER_ID order by ORGANIZATION_ID DESC NULLS FIRST,CONTAINER_ID ASC NULLS FIRST";
            queryPlan =getQueryPlan(conn, sql);
            assertTrue(queryPlan.getGroupBy().isOrderPreserving());
            assertTrue(queryPlan.getOrderBy().getOrderByExpressions().size()==2);
            assertTrue(queryPlan.getOrderBy().getOrderByExpressions().get(0).toString().equals("ORGANIZATION_ID DESC"));
            assertTrue(queryPlan.getOrderBy().getOrderByExpressions().get(1).toString().equals("CONTAINER_ID"));

            sql="SELECT CONTAINER_ID,ORGANIZATION_ID FROM GROUPBYDESC3452 group by ORGANIZATION_ID, CONTAINER_ID order by ORGANIZATION_ID DESC NULLS FIRST,CONTAINER_ID ASC NULLS LAST";
            queryPlan =getQueryPlan(conn, sql);
            assertTrue(queryPlan.getGroupBy().isOrderPreserving());
            assertTrue(queryPlan.getOrderBy().getOrderByExpressions().size()==2);
            assertTrue(queryPlan.getOrderBy().getOrderByExpressions().get(0).toString().equals("ORGANIZATION_ID DESC"));
            assertTrue(queryPlan.getOrderBy().getOrderByExpressions().get(1).toString().equals("CONTAINER_ID NULLS LAST"));

            sql="SELECT CONTAINER_ID,ORGANIZATION_ID FROM GROUPBYDESC3452 group by ORGANIZATION_ID, CONTAINER_ID order by ORGANIZATION_ID DESC NULLS LAST,CONTAINER_ID ASC NULLS FIRST";
            queryPlan =getQueryPlan(conn, sql);
            assertTrue(queryPlan.getGroupBy().isOrderPreserving());
            assertTrue(queryPlan.getOrderBy().getOrderByExpressions().size()==2);
            assertTrue(queryPlan.getOrderBy().getOrderByExpressions().get(0).toString().equals("ORGANIZATION_ID DESC NULLS LAST"));
            assertTrue(queryPlan.getOrderBy().getOrderByExpressions().get(1).toString().equals("CONTAINER_ID"));

            sql="SELECT CONTAINER_ID,ORGANIZATION_ID FROM GROUPBYDESC3452 group by ORGANIZATION_ID, CONTAINER_ID order by ORGANIZATION_ID DESC NULLS LAST,CONTAINER_ID ASC NULLS LAST";
            queryPlan =getQueryPlan(conn, sql);
            assertTrue(queryPlan.getGroupBy().isOrderPreserving());
            assertTrue(queryPlan.getOrderBy().getOrderByExpressions().size()==2);
            assertTrue(queryPlan.getOrderBy().getOrderByExpressions().get(0).toString().equals("ORGANIZATION_ID DESC NULLS LAST"));
            assertTrue(queryPlan.getOrderBy().getOrderByExpressions().get(1).toString().equals("CONTAINER_ID NULLS LAST"));

            //-----ORGANIZATION_ID DESC  CONTAINER_ID DESC

            sql="SELECT CONTAINER_ID,ORGANIZATION_ID FROM GROUPBYDESC3452 group by ORGANIZATION_ID, CONTAINER_ID order by ORGANIZATION_ID DESC NULLS FIRST,CONTAINER_ID DESC NULLS FIRST";
            queryPlan =getQueryPlan(conn, sql);
            assertTrue(queryPlan.getGroupBy().isOrderPreserving());
            assertTrue(queryPlan.getOrderBy() == OrderBy.FWD_ROW_KEY_ORDER_BY);

            sql="SELECT CONTAINER_ID,ORGANIZATION_ID FROM GROUPBYDESC3452 group by ORGANIZATION_ID, CONTAINER_ID order by ORGANIZATION_ID DESC NULLS FIRST,CONTAINER_ID DESC NULLS LAST";
            queryPlan =getQueryPlan(conn, sql);
            assertTrue(queryPlan.getGroupBy().isOrderPreserving());
            assertTrue(queryPlan.getOrderBy().getOrderByExpressions().size()==2);
            assertTrue(queryPlan.getOrderBy().getOrderByExpressions().get(0).toString().equals("ORGANIZATION_ID DESC"));
            assertTrue(queryPlan.getOrderBy().getOrderByExpressions().get(1).toString().equals("CONTAINER_ID DESC NULLS LAST"));

            sql="SELECT CONTAINER_ID,ORGANIZATION_ID FROM GROUPBYDESC3452 group by ORGANIZATION_ID, CONTAINER_ID order by ORGANIZATION_ID DESC NULLS LAST,CONTAINER_ID DESC NULLS FIRST";
            queryPlan =getQueryPlan(conn, sql);
            assertTrue(queryPlan.getGroupBy().isOrderPreserving());
            assertTrue(queryPlan.getOrderBy().getOrderByExpressions().size()==2);
            assertTrue(queryPlan.getOrderBy().getOrderByExpressions().get(0).toString().equals("ORGANIZATION_ID DESC NULLS LAST"));
            assertTrue(queryPlan.getOrderBy().getOrderByExpressions().get(1).toString().equals("CONTAINER_ID DESC"));

            sql="SELECT CONTAINER_ID,ORGANIZATION_ID FROM GROUPBYDESC3452 group by ORGANIZATION_ID, CONTAINER_ID order by ORGANIZATION_ID DESC NULLS LAST,CONTAINER_ID DESC NULLS LAST";
            queryPlan =getQueryPlan(conn, sql);
            assertTrue(queryPlan.getGroupBy().isOrderPreserving());
            assertTrue(queryPlan.getOrderBy().getOrderByExpressions().size()==2);
            assertTrue(queryPlan.getOrderBy().getOrderByExpressions().get(0).toString().equals("ORGANIZATION_ID DESC NULLS LAST"));
            assertTrue(queryPlan.getOrderBy().getOrderByExpressions().get(1).toString().equals("CONTAINER_ID DESC NULLS LAST"));

            //-----CONTAINER_ID ASC  ORGANIZATION_ID ASC

            sql="SELECT CONTAINER_ID,ORGANIZATION_ID FROM GROUPBYDESC3452 group by ORGANIZATION_ID, CONTAINER_ID order by CONTAINER_ID NULLS FIRST,ORGANIZATION_ID NULLS FIRST";
            queryPlan =getQueryPlan(conn, sql);
            assertTrue(queryPlan.getGroupBy().isOrderPreserving());
            assertTrue(queryPlan.getOrderBy().getOrderByExpressions().size()==2);
            assertTrue(queryPlan.getOrderBy().getOrderByExpressions().get(0).toString().equals("CONTAINER_ID"));
            assertTrue(queryPlan.getOrderBy().getOrderByExpressions().get(1).toString().equals("ORGANIZATION_ID"));

            sql="SELECT CONTAINER_ID,ORGANIZATION_ID FROM GROUPBYDESC3452 group by ORGANIZATION_ID, CONTAINER_ID order by CONTAINER_ID NULLS FIRST,ORGANIZATION_ID NULLS LAST";
            queryPlan =getQueryPlan(conn, sql);
            assertTrue(queryPlan.getGroupBy().isOrderPreserving());
            assertTrue(queryPlan.getOrderBy().getOrderByExpressions().size()==2);
            assertTrue(queryPlan.getOrderBy().getOrderByExpressions().get(0).toString().equals("CONTAINER_ID"));
            assertTrue(queryPlan.getOrderBy().getOrderByExpressions().get(1).toString().equals("ORGANIZATION_ID NULLS LAST"));

            sql="SELECT CONTAINER_ID,ORGANIZATION_ID FROM GROUPBYDESC3452 group by ORGANIZATION_ID, CONTAINER_ID order by CONTAINER_ID NULLS LAST,ORGANIZATION_ID NULLS FIRST";
            queryPlan =getQueryPlan(conn, sql);
            assertTrue(queryPlan.getGroupBy().isOrderPreserving());
            assertTrue(queryPlan.getOrderBy().getOrderByExpressions().size()==2);
            assertTrue(queryPlan.getOrderBy().getOrderByExpressions().get(0).toString().equals("CONTAINER_ID NULLS LAST"));
            assertTrue(queryPlan.getOrderBy().getOrderByExpressions().get(1).toString().equals("ORGANIZATION_ID"));

            sql="SELECT CONTAINER_ID,ORGANIZATION_ID FROM GROUPBYDESC3452 group by ORGANIZATION_ID, CONTAINER_ID order by CONTAINER_ID NULLS LAST,ORGANIZATION_ID NULLS LAST";
            queryPlan =getQueryPlan(conn, sql);
            assertTrue(queryPlan.getGroupBy().isOrderPreserving());
            assertTrue(queryPlan.getOrderBy().getOrderByExpressions().size()==2);
            assertTrue(queryPlan.getOrderBy().getOrderByExpressions().get(0).toString().equals("CONTAINER_ID NULLS LAST"));
            assertTrue(queryPlan.getOrderBy().getOrderByExpressions().get(1).toString().equals("ORGANIZATION_ID NULLS LAST"));

            //-----CONTAINER_ID ASC  ORGANIZATION_ID DESC

            sql="SELECT CONTAINER_ID,ORGANIZATION_ID FROM GROUPBYDESC3452 group by ORGANIZATION_ID, CONTAINER_ID order by CONTAINER_ID ASC NULLS FIRST,ORGANIZATION_ID DESC NULLS FIRST";
            queryPlan =getQueryPlan(conn, sql);
            assertTrue(queryPlan.getGroupBy().isOrderPreserving());
            assertTrue(queryPlan.getOrderBy().getOrderByExpressions().size()==2);
            assertTrue(queryPlan.getOrderBy().getOrderByExpressions().get(0).toString().equals("CONTAINER_ID"));
            assertTrue(queryPlan.getOrderBy().getOrderByExpressions().get(1).toString().equals("ORGANIZATION_ID DESC"));

            sql="SELECT CONTAINER_ID,ORGANIZATION_ID FROM GROUPBYDESC3452 group by ORGANIZATION_ID, CONTAINER_ID order by CONTAINER_ID ASC NULLS FIRST,ORGANIZATION_ID DESC NULLS LAST";
            queryPlan =getQueryPlan(conn, sql);
            assertTrue(queryPlan.getGroupBy().isOrderPreserving());
            assertTrue(queryPlan.getOrderBy().getOrderByExpressions().size()==2);
            assertTrue(queryPlan.getOrderBy().getOrderByExpressions().get(0).toString().equals("CONTAINER_ID"));
            assertTrue(queryPlan.getOrderBy().getOrderByExpressions().get(1).toString().equals("ORGANIZATION_ID DESC NULLS LAST"));

            sql="SELECT CONTAINER_ID,ORGANIZATION_ID FROM GROUPBYDESC3452 group by ORGANIZATION_ID, CONTAINER_ID order by CONTAINER_ID ASC NULLS LAST,ORGANIZATION_ID DESC NULLS FIRST";
            queryPlan =getQueryPlan(conn, sql);
            assertTrue(queryPlan.getGroupBy().isOrderPreserving());
            assertTrue(queryPlan.getOrderBy().getOrderByExpressions().size()==2);
            assertTrue(queryPlan.getOrderBy().getOrderByExpressions().get(0).toString().equals("CONTAINER_ID NULLS LAST"));
            assertTrue(queryPlan.getOrderBy().getOrderByExpressions().get(1).toString().equals("ORGANIZATION_ID DESC"));

            sql="SELECT CONTAINER_ID,ORGANIZATION_ID FROM GROUPBYDESC3452 group by ORGANIZATION_ID, CONTAINER_ID order by CONTAINER_ID ASC NULLS LAST,ORGANIZATION_ID DESC NULLS LAST";
            queryPlan =getQueryPlan(conn, sql);
            assertTrue(queryPlan.getGroupBy().isOrderPreserving());
            assertTrue(queryPlan.getOrderBy().getOrderByExpressions().size()==2);
            assertTrue(queryPlan.getOrderBy().getOrderByExpressions().get(0).toString().equals("CONTAINER_ID NULLS LAST"));
            assertTrue(queryPlan.getOrderBy().getOrderByExpressions().get(1).toString().equals("ORGANIZATION_ID DESC NULLS LAST"));

            //-----CONTAINER_ID DESC  ORGANIZATION_ID ASC

            sql="SELECT CONTAINER_ID,ORGANIZATION_ID FROM GROUPBYDESC3452 group by ORGANIZATION_ID, CONTAINER_ID order by CONTAINER_ID DESC NULLS FIRST,ORGANIZATION_ID ASC NULLS FIRST";
            queryPlan =getQueryPlan(conn, sql);
            assertTrue(queryPlan.getGroupBy().isOrderPreserving());
            assertTrue(queryPlan.getOrderBy().getOrderByExpressions().size()==2);
            assertTrue(queryPlan.getOrderBy().getOrderByExpressions().get(0).toString().equals("CONTAINER_ID DESC"));
            assertTrue(queryPlan.getOrderBy().getOrderByExpressions().get(1).toString().equals("ORGANIZATION_ID"));

            sql="SELECT CONTAINER_ID,ORGANIZATION_ID FROM GROUPBYDESC3452 group by ORGANIZATION_ID, CONTAINER_ID order by CONTAINER_ID DESC NULLS FIRST,ORGANIZATION_ID ASC NULLS LAST";
            queryPlan =getQueryPlan(conn, sql);
            assertTrue(queryPlan.getGroupBy().isOrderPreserving());
            assertTrue(queryPlan.getOrderBy().getOrderByExpressions().size()==2);
            assertTrue(queryPlan.getOrderBy().getOrderByExpressions().get(0).toString().equals("CONTAINER_ID DESC"));
            assertTrue(queryPlan.getOrderBy().getOrderByExpressions().get(1).toString().equals("ORGANIZATION_ID NULLS LAST"));

            sql="SELECT CONTAINER_ID,ORGANIZATION_ID FROM GROUPBYDESC3452 group by ORGANIZATION_ID, CONTAINER_ID order by CONTAINER_ID DESC NULLS LAST,ORGANIZATION_ID ASC NULLS FIRST";
            queryPlan =getQueryPlan(conn, sql);
            assertTrue(queryPlan.getGroupBy().isOrderPreserving());
            assertTrue(queryPlan.getOrderBy().getOrderByExpressions().size()==2);
            assertTrue(queryPlan.getOrderBy().getOrderByExpressions().get(0).toString().equals("CONTAINER_ID DESC NULLS LAST"));
            assertTrue(queryPlan.getOrderBy().getOrderByExpressions().get(1).toString().equals("ORGANIZATION_ID"));

            sql="SELECT CONTAINER_ID,ORGANIZATION_ID FROM GROUPBYDESC3452 group by ORGANIZATION_ID, CONTAINER_ID order by CONTAINER_ID DESC NULLS LAST,ORGANIZATION_ID ASC NULLS LAST";
            queryPlan =getQueryPlan(conn, sql);
            assertTrue(queryPlan.getGroupBy().isOrderPreserving());
            assertTrue(queryPlan.getOrderBy().getOrderByExpressions().size()==2);
            assertTrue(queryPlan.getOrderBy().getOrderByExpressions().get(0).toString().equals("CONTAINER_ID DESC NULLS LAST"));
            assertTrue(queryPlan.getOrderBy().getOrderByExpressions().get(1).toString().equals("ORGANIZATION_ID NULLS LAST"));

            //-----CONTAINER_ID DESC  ORGANIZATION_ID DESC

            sql="SELECT CONTAINER_ID,ORGANIZATION_ID FROM GROUPBYDESC3452 group by ORGANIZATION_ID,CONTAINER_ID order by CONTAINER_ID DESC NULLS FIRST,ORGANIZATION_ID DESC NULLS FIRST";
            queryPlan =getQueryPlan(conn, sql);
            assertTrue(queryPlan.getGroupBy().isOrderPreserving());
            assertTrue(queryPlan.getOrderBy().getOrderByExpressions().size()==2);
            assertTrue(queryPlan.getOrderBy().getOrderByExpressions().get(0).toString().equals("CONTAINER_ID DESC"));
            assertTrue(queryPlan.getOrderBy().getOrderByExpressions().get(1).toString().equals("ORGANIZATION_ID DESC"));

            sql="SELECT CONTAINER_ID,ORGANIZATION_ID FROM GROUPBYDESC3452 group by ORGANIZATION_ID, CONTAINER_ID order by CONTAINER_ID DESC NULLS FIRST,ORGANIZATION_ID DESC NULLS LAST";
            queryPlan =getQueryPlan(conn, sql);
            assertTrue(queryPlan.getGroupBy().isOrderPreserving());
            assertTrue(queryPlan.getOrderBy().getOrderByExpressions().size()==2);
            assertTrue(queryPlan.getOrderBy().getOrderByExpressions().get(0).toString().equals("CONTAINER_ID DESC"));
            assertTrue(queryPlan.getOrderBy().getOrderByExpressions().get(1).toString().equals("ORGANIZATION_ID DESC NULLS LAST"));

            sql="SELECT CONTAINER_ID,ORGANIZATION_ID FROM GROUPBYDESC3452 group by ORGANIZATION_ID, CONTAINER_ID order by CONTAINER_ID DESC NULLS LAST,ORGANIZATION_ID DESC NULLS FIRST";
            queryPlan =getQueryPlan(conn, sql);
            assertTrue(queryPlan.getGroupBy().isOrderPreserving());
            assertTrue(queryPlan.getOrderBy().getOrderByExpressions().size()==2);
            assertTrue(queryPlan.getOrderBy().getOrderByExpressions().get(0).toString().equals("CONTAINER_ID DESC NULLS LAST"));
            assertTrue(queryPlan.getOrderBy().getOrderByExpressions().get(1).toString().equals("ORGANIZATION_ID DESC"));

            sql="SELECT CONTAINER_ID,ORGANIZATION_ID FROM GROUPBYDESC3452 group by ORGANIZATION_ID, CONTAINER_ID order by CONTAINER_ID DESC NULLS LAST,ORGANIZATION_ID DESC NULLS LAST";
            queryPlan =getQueryPlan(conn, sql);
            assertTrue(queryPlan.getGroupBy().isOrderPreserving());
            assertTrue(queryPlan.getOrderBy().getOrderByExpressions().size()==2);
            assertTrue(queryPlan.getOrderBy().getOrderByExpressions().get(0).toString().equals("CONTAINER_ID DESC NULLS LAST"));
            assertTrue(queryPlan.getOrderBy().getOrderByExpressions().get(1).toString().equals("ORGANIZATION_ID DESC NULLS LAST"));

        } finally {
            if(conn!=null) {
                conn.close();
            }
        }
    }

    @Test
    public void testOrderByDescWithNullsLastBug3469() throws Exception {
        Connection conn=null;
        try {
            conn= DriverManager.getConnection(getUrl());

            String sql="CREATE TABLE DESCNULLSLAST3469 ( "+
                "ORGANIZATION_ID VARCHAR,"+
                "CONTAINER_ID VARCHAR,"+
                "ENTITY_ID VARCHAR NOT NULL,"+
                "CONSTRAINT TEST_PK PRIMARY KEY ( "+
                "ORGANIZATION_ID DESC,"+
                "CONTAINER_ID DESC,"+
                "ENTITY_ID"+
                "))";
            conn.createStatement().execute(sql);

            //-----ORGANIZATION_ID

            sql="SELECT CONTAINER_ID,ORGANIZATION_ID FROM DESCNULLSLAST3469 order by ORGANIZATION_ID ASC NULLS FIRST";
            QueryPlan queryPlan =getQueryPlan(conn, sql);
            assertTrue(queryPlan.getOrderBy().getOrderByExpressions().size()==1);
            assertTrue(queryPlan.getOrderBy().getOrderByExpressions().get(0).toString().equals("ORGANIZATION_ID"));

            sql="SELECT CONTAINER_ID,ORGANIZATION_ID FROM DESCNULLSLAST3469 order by ORGANIZATION_ID ASC NULLS LAST";
            queryPlan =getQueryPlan(conn, sql);
            assertTrue(queryPlan.getOrderBy()== OrderBy.REV_ROW_KEY_ORDER_BY);

            sql="SELECT CONTAINER_ID,ORGANIZATION_ID FROM DESCNULLSLAST3469 order by ORGANIZATION_ID DESC NULLS FIRST";
            queryPlan =getQueryPlan(conn, sql);
            assertTrue(queryPlan.getOrderBy()== OrderBy.FWD_ROW_KEY_ORDER_BY);

            sql="SELECT CONTAINER_ID,ORGANIZATION_ID FROM DESCNULLSLAST3469 order by ORGANIZATION_ID DESC NULLS LAST";
            queryPlan =getQueryPlan(conn, sql);
            assertTrue(queryPlan.getOrderBy().getOrderByExpressions().size()==1);
            assertTrue(queryPlan.getOrderBy().getOrderByExpressions().get(0).toString().equals("ORGANIZATION_ID DESC NULLS LAST"));

            //----CONTAINER_ID

            sql="SELECT CONTAINER_ID,ORGANIZATION_ID FROM DESCNULLSLAST3469 order by CONTAINER_ID ASC NULLS FIRST";
            queryPlan =getQueryPlan(conn, sql);
            assertTrue(queryPlan.getOrderBy().getOrderByExpressions().size()==1);
            assertTrue(queryPlan.getOrderBy().getOrderByExpressions().get(0).toString().equals("CONTAINER_ID"));

            sql="SELECT CONTAINER_ID,ORGANIZATION_ID FROM DESCNULLSLAST3469 order by CONTAINER_ID ASC NULLS LAST";
            queryPlan =getQueryPlan(conn, sql);
            assertTrue(queryPlan.getOrderBy().getOrderByExpressions().size()==1);
            assertTrue(queryPlan.getOrderBy().getOrderByExpressions().get(0).toString().equals("CONTAINER_ID NULLS LAST"));

            sql="SELECT CONTAINER_ID,ORGANIZATION_ID FROM DESCNULLSLAST3469 order by CONTAINER_ID DESC NULLS FIRST";
            queryPlan =getQueryPlan(conn, sql);
            assertTrue(queryPlan.getOrderBy().getOrderByExpressions().size()==1);
            assertTrue(queryPlan.getOrderBy().getOrderByExpressions().get(0).toString().equals("CONTAINER_ID DESC"));

            sql="SELECT CONTAINER_ID,ORGANIZATION_ID FROM DESCNULLSLAST3469 order by CONTAINER_ID DESC NULLS LAST";
            queryPlan =getQueryPlan(conn, sql);
            assertTrue(queryPlan.getOrderBy().getOrderByExpressions().size()==1);
            assertTrue(queryPlan.getOrderBy().getOrderByExpressions().get(0).toString().equals("CONTAINER_ID DESC NULLS LAST"));

            //-----ORGANIZATION_ID ASC  CONTAINER_ID ASC

            sql="SELECT CONTAINER_ID,ORGANIZATION_ID FROM DESCNULLSLAST3469 order by ORGANIZATION_ID NULLS FIRST,CONTAINER_ID NULLS FIRST";
            queryPlan =getQueryPlan(conn, sql);
            assertTrue(queryPlan.getOrderBy().getOrderByExpressions().size()==2);
            assertTrue(queryPlan.getOrderBy().getOrderByExpressions().get(0).toString().equals("ORGANIZATION_ID"));
            assertTrue(queryPlan.getOrderBy().getOrderByExpressions().get(1).toString().equals("CONTAINER_ID"));

            sql="SELECT CONTAINER_ID,ORGANIZATION_ID FROM DESCNULLSLAST3469 order by ORGANIZATION_ID NULLS FIRST,CONTAINER_ID NULLS LAST";
            queryPlan =getQueryPlan(conn, sql);
            assertTrue(queryPlan.getOrderBy().getOrderByExpressions().size()==2);
            assertTrue(queryPlan.getOrderBy().getOrderByExpressions().get(0).toString().equals("ORGANIZATION_ID"));
            assertTrue(queryPlan.getOrderBy().getOrderByExpressions().get(1).toString().equals("CONTAINER_ID NULLS LAST"));

            sql="SELECT CONTAINER_ID,ORGANIZATION_ID FROM DESCNULLSLAST3469 order by ORGANIZATION_ID NULLS LAST,CONTAINER_ID NULLS FIRST";
            queryPlan =getQueryPlan(conn, sql);
            assertTrue(queryPlan.getOrderBy().getOrderByExpressions().size()==2);
            assertTrue(queryPlan.getOrderBy().getOrderByExpressions().get(0).toString().equals("ORGANIZATION_ID NULLS LAST"));
            assertTrue(queryPlan.getOrderBy().getOrderByExpressions().get(1).toString().equals("CONTAINER_ID"));

            sql="SELECT CONTAINER_ID,ORGANIZATION_ID FROM DESCNULLSLAST3469 order by ORGANIZATION_ID NULLS LAST,CONTAINER_ID NULLS LAST";
            queryPlan =getQueryPlan(conn, sql);
            assertTrue(queryPlan.getOrderBy() == OrderBy.REV_ROW_KEY_ORDER_BY);

            //-----ORGANIZATION_ID ASC  CONTAINER_ID DESC

            sql="SELECT CONTAINER_ID,ORGANIZATION_ID FROM DESCNULLSLAST3469 order by ORGANIZATION_ID ASC NULLS FIRST,CONTAINER_ID DESC NULLS FIRST";
            queryPlan =getQueryPlan(conn, sql);
            assertTrue(queryPlan.getOrderBy().getOrderByExpressions().size()==2);
            assertTrue(queryPlan.getOrderBy().getOrderByExpressions().get(0).toString().equals("ORGANIZATION_ID"));
            assertTrue(queryPlan.getOrderBy().getOrderByExpressions().get(1).toString().equals("CONTAINER_ID DESC"));

            sql="SELECT CONTAINER_ID,ORGANIZATION_ID FROM DESCNULLSLAST3469 order by ORGANIZATION_ID ASC NULLS FIRST,CONTAINER_ID DESC NULLS LAST";
            queryPlan =getQueryPlan(conn, sql);
            assertTrue(queryPlan.getOrderBy().getOrderByExpressions().size()==2);
            assertTrue(queryPlan.getOrderBy().getOrderByExpressions().get(0).toString().equals("ORGANIZATION_ID"));
            assertTrue(queryPlan.getOrderBy().getOrderByExpressions().get(1).toString().equals("CONTAINER_ID DESC NULLS LAST"));

            sql="SELECT CONTAINER_ID,ORGANIZATION_ID FROM DESCNULLSLAST3469 order by ORGANIZATION_ID ASC NULLS LAST,CONTAINER_ID DESC NULLS FIRST";
            queryPlan =getQueryPlan(conn, sql);
            assertTrue(queryPlan.getOrderBy().getOrderByExpressions().size()==2);
            assertTrue(queryPlan.getOrderBy().getOrderByExpressions().get(0).toString().equals("ORGANIZATION_ID NULLS LAST"));
            assertTrue(queryPlan.getOrderBy().getOrderByExpressions().get(1).toString().equals("CONTAINER_ID DESC"));

            sql="SELECT CONTAINER_ID,ORGANIZATION_ID FROM DESCNULLSLAST3469 order by ORGANIZATION_ID ASC NULLS LAST,CONTAINER_ID DESC NULLS LAST";
            queryPlan =getQueryPlan(conn, sql);
            assertTrue(queryPlan.getOrderBy().getOrderByExpressions().size()==2);
            assertTrue(queryPlan.getOrderBy().getOrderByExpressions().get(0).toString().equals("ORGANIZATION_ID NULLS LAST"));
            assertTrue(queryPlan.getOrderBy().getOrderByExpressions().get(1).toString().equals("CONTAINER_ID DESC NULLS LAST"));

            //-----ORGANIZATION_ID DESC  CONTAINER_ID ASC

            sql="SELECT CONTAINER_ID,ORGANIZATION_ID FROM DESCNULLSLAST3469 order by ORGANIZATION_ID DESC NULLS FIRST,CONTAINER_ID ASC NULLS FIRST";
            queryPlan =getQueryPlan(conn, sql);
            assertTrue(queryPlan.getOrderBy().getOrderByExpressions().size()==2);
            assertTrue(queryPlan.getOrderBy().getOrderByExpressions().get(0).toString().equals("ORGANIZATION_ID DESC"));
            assertTrue(queryPlan.getOrderBy().getOrderByExpressions().get(1).toString().equals("CONTAINER_ID"));

            sql="SELECT CONTAINER_ID,ORGANIZATION_ID FROM DESCNULLSLAST3469 order by ORGANIZATION_ID DESC NULLS FIRST,CONTAINER_ID ASC NULLS LAST";
            queryPlan =getQueryPlan(conn, sql);
            assertTrue(queryPlan.getOrderBy().getOrderByExpressions().size()==2);
            assertTrue(queryPlan.getOrderBy().getOrderByExpressions().get(0).toString().equals("ORGANIZATION_ID DESC"));
            assertTrue(queryPlan.getOrderBy().getOrderByExpressions().get(1).toString().equals("CONTAINER_ID NULLS LAST"));

            sql="SELECT CONTAINER_ID,ORGANIZATION_ID FROM DESCNULLSLAST3469 order by ORGANIZATION_ID DESC NULLS LAST,CONTAINER_ID ASC NULLS FIRST";
            queryPlan =getQueryPlan(conn, sql);
            assertTrue(queryPlan.getOrderBy().getOrderByExpressions().size()==2);
            assertTrue(queryPlan.getOrderBy().getOrderByExpressions().get(0).toString().equals("ORGANIZATION_ID DESC NULLS LAST"));
            assertTrue(queryPlan.getOrderBy().getOrderByExpressions().get(1).toString().equals("CONTAINER_ID"));

            sql="SELECT CONTAINER_ID,ORGANIZATION_ID FROM DESCNULLSLAST3469 order by ORGANIZATION_ID DESC NULLS LAST,CONTAINER_ID ASC NULLS LAST";
            queryPlan =getQueryPlan(conn, sql);
            assertTrue(queryPlan.getOrderBy().getOrderByExpressions().size()==2);
            assertTrue(queryPlan.getOrderBy().getOrderByExpressions().get(0).toString().equals("ORGANIZATION_ID DESC NULLS LAST"));
            assertTrue(queryPlan.getOrderBy().getOrderByExpressions().get(1).toString().equals("CONTAINER_ID NULLS LAST"));

            //-----ORGANIZATION_ID DESC  CONTAINER_ID DESC

            sql="SELECT CONTAINER_ID,ORGANIZATION_ID FROM DESCNULLSLAST3469 order by ORGANIZATION_ID DESC NULLS FIRST,CONTAINER_ID DESC NULLS FIRST";
            queryPlan =getQueryPlan(conn, sql);
            assertTrue(queryPlan.getOrderBy() == OrderBy.FWD_ROW_KEY_ORDER_BY);

            sql="SELECT CONTAINER_ID,ORGANIZATION_ID FROM DESCNULLSLAST3469 order by ORGANIZATION_ID DESC NULLS FIRST,CONTAINER_ID DESC NULLS LAST";
            queryPlan =getQueryPlan(conn, sql);
            assertTrue(queryPlan.getOrderBy().getOrderByExpressions().size()==2);
            assertTrue(queryPlan.getOrderBy().getOrderByExpressions().get(0).toString().equals("ORGANIZATION_ID DESC"));
            assertTrue(queryPlan.getOrderBy().getOrderByExpressions().get(1).toString().equals("CONTAINER_ID DESC NULLS LAST"));

            sql="SELECT CONTAINER_ID,ORGANIZATION_ID FROM DESCNULLSLAST3469 order by ORGANIZATION_ID DESC NULLS LAST,CONTAINER_ID DESC NULLS FIRST";
            queryPlan =getQueryPlan(conn, sql);
            assertTrue(queryPlan.getOrderBy().getOrderByExpressions().size()==2);
            assertTrue(queryPlan.getOrderBy().getOrderByExpressions().get(0).toString().equals("ORGANIZATION_ID DESC NULLS LAST"));
            assertTrue(queryPlan.getOrderBy().getOrderByExpressions().get(1).toString().equals("CONTAINER_ID DESC"));

            sql="SELECT CONTAINER_ID,ORGANIZATION_ID FROM DESCNULLSLAST3469 order by ORGANIZATION_ID DESC NULLS LAST,CONTAINER_ID DESC NULLS LAST";
            queryPlan =getQueryPlan(conn, sql);
            assertTrue(queryPlan.getOrderBy().getOrderByExpressions().size()==2);
            assertTrue(queryPlan.getOrderBy().getOrderByExpressions().get(0).toString().equals("ORGANIZATION_ID DESC NULLS LAST"));
            assertTrue(queryPlan.getOrderBy().getOrderByExpressions().get(1).toString().equals("CONTAINER_ID DESC NULLS LAST"));

            //-----CONTAINER_ID ASC  ORGANIZATION_ID ASC

            sql="SELECT CONTAINER_ID,ORGANIZATION_ID FROM DESCNULLSLAST3469 order by CONTAINER_ID NULLS FIRST,ORGANIZATION_ID NULLS FIRST";
            queryPlan =getQueryPlan(conn, sql);
            assertTrue(queryPlan.getOrderBy().getOrderByExpressions().size()==2);
            assertTrue(queryPlan.getOrderBy().getOrderByExpressions().get(0).toString().equals("CONTAINER_ID"));
            assertTrue(queryPlan.getOrderBy().getOrderByExpressions().get(1).toString().equals("ORGANIZATION_ID"));

            sql="SELECT CONTAINER_ID,ORGANIZATION_ID FROM DESCNULLSLAST3469 order by CONTAINER_ID NULLS FIRST,ORGANIZATION_ID NULLS LAST";
            queryPlan =getQueryPlan(conn, sql);
            assertTrue(queryPlan.getOrderBy().getOrderByExpressions().size()==2);
            assertTrue(queryPlan.getOrderBy().getOrderByExpressions().get(0).toString().equals("CONTAINER_ID"));
            assertTrue(queryPlan.getOrderBy().getOrderByExpressions().get(1).toString().equals("ORGANIZATION_ID NULLS LAST"));

            sql="SELECT CONTAINER_ID,ORGANIZATION_ID FROM DESCNULLSLAST3469 order by CONTAINER_ID NULLS LAST,ORGANIZATION_ID NULLS FIRST";
            queryPlan =getQueryPlan(conn, sql);
            assertTrue(queryPlan.getOrderBy().getOrderByExpressions().size()==2);
            assertTrue(queryPlan.getOrderBy().getOrderByExpressions().get(0).toString().equals("CONTAINER_ID NULLS LAST"));
            assertTrue(queryPlan.getOrderBy().getOrderByExpressions().get(1).toString().equals("ORGANIZATION_ID"));

            sql="SELECT CONTAINER_ID,ORGANIZATION_ID FROM DESCNULLSLAST3469 order by CONTAINER_ID NULLS LAST,ORGANIZATION_ID NULLS LAST";
            queryPlan =getQueryPlan(conn, sql);
            assertTrue(queryPlan.getOrderBy().getOrderByExpressions().size()==2);
            assertTrue(queryPlan.getOrderBy().getOrderByExpressions().get(0).toString().equals("CONTAINER_ID NULLS LAST"));
            assertTrue(queryPlan.getOrderBy().getOrderByExpressions().get(1).toString().equals("ORGANIZATION_ID NULLS LAST"));

            //-----CONTAINER_ID ASC  ORGANIZATION_ID DESC

            sql="SELECT CONTAINER_ID,ORGANIZATION_ID FROM DESCNULLSLAST3469 order by CONTAINER_ID ASC NULLS FIRST,ORGANIZATION_ID DESC NULLS FIRST";
            queryPlan =getQueryPlan(conn, sql);
            assertTrue(queryPlan.getOrderBy().getOrderByExpressions().size()==2);
            assertTrue(queryPlan.getOrderBy().getOrderByExpressions().get(0).toString().equals("CONTAINER_ID"));
            assertTrue(queryPlan.getOrderBy().getOrderByExpressions().get(1).toString().equals("ORGANIZATION_ID DESC"));

            sql="SELECT CONTAINER_ID,ORGANIZATION_ID FROM DESCNULLSLAST3469 order by CONTAINER_ID ASC NULLS FIRST,ORGANIZATION_ID DESC NULLS LAST";
            queryPlan =getQueryPlan(conn, sql);
            assertTrue(queryPlan.getOrderBy().getOrderByExpressions().size()==2);
            assertTrue(queryPlan.getOrderBy().getOrderByExpressions().get(0).toString().equals("CONTAINER_ID"));
            assertTrue(queryPlan.getOrderBy().getOrderByExpressions().get(1).toString().equals("ORGANIZATION_ID DESC NULLS LAST"));

            sql="SELECT CONTAINER_ID,ORGANIZATION_ID FROM DESCNULLSLAST3469 order by CONTAINER_ID ASC NULLS LAST,ORGANIZATION_ID DESC NULLS FIRST";
            queryPlan =getQueryPlan(conn, sql);
            assertTrue(queryPlan.getOrderBy().getOrderByExpressions().size()==2);
            assertTrue(queryPlan.getOrderBy().getOrderByExpressions().get(0).toString().equals("CONTAINER_ID NULLS LAST"));
            assertTrue(queryPlan.getOrderBy().getOrderByExpressions().get(1).toString().equals("ORGANIZATION_ID DESC"));

            sql="SELECT CONTAINER_ID,ORGANIZATION_ID FROM DESCNULLSLAST3469 order by CONTAINER_ID ASC NULLS LAST,ORGANIZATION_ID DESC NULLS LAST";
            queryPlan =getQueryPlan(conn, sql);
            assertTrue(queryPlan.getOrderBy().getOrderByExpressions().size()==2);
            assertTrue(queryPlan.getOrderBy().getOrderByExpressions().get(0).toString().equals("CONTAINER_ID NULLS LAST"));
            assertTrue(queryPlan.getOrderBy().getOrderByExpressions().get(1).toString().equals("ORGANIZATION_ID DESC NULLS LAST"));

            //-----CONTAINER_ID DESC  ORGANIZATION_ID ASC

            sql="SELECT CONTAINER_ID,ORGANIZATION_ID FROM DESCNULLSLAST3469 order by CONTAINER_ID DESC NULLS FIRST,ORGANIZATION_ID ASC NULLS FIRST";
            queryPlan =getQueryPlan(conn, sql);
            assertTrue(queryPlan.getOrderBy().getOrderByExpressions().size()==2);
            assertTrue(queryPlan.getOrderBy().getOrderByExpressions().get(0).toString().equals("CONTAINER_ID DESC"));
            assertTrue(queryPlan.getOrderBy().getOrderByExpressions().get(1).toString().equals("ORGANIZATION_ID"));

            sql="SELECT CONTAINER_ID,ORGANIZATION_ID FROM DESCNULLSLAST3469 order by CONTAINER_ID DESC NULLS FIRST,ORGANIZATION_ID ASC NULLS LAST";
            queryPlan =getQueryPlan(conn, sql);
            assertTrue(queryPlan.getOrderBy().getOrderByExpressions().size()==2);
            assertTrue(queryPlan.getOrderBy().getOrderByExpressions().get(0).toString().equals("CONTAINER_ID DESC"));
            assertTrue(queryPlan.getOrderBy().getOrderByExpressions().get(1).toString().equals("ORGANIZATION_ID NULLS LAST"));

            sql="SELECT CONTAINER_ID,ORGANIZATION_ID FROM DESCNULLSLAST3469 order by CONTAINER_ID DESC NULLS LAST,ORGANIZATION_ID ASC NULLS FIRST";
            queryPlan =getQueryPlan(conn, sql);
            assertTrue(queryPlan.getOrderBy().getOrderByExpressions().size()==2);
            assertTrue(queryPlan.getOrderBy().getOrderByExpressions().get(0).toString().equals("CONTAINER_ID DESC NULLS LAST"));
            assertTrue(queryPlan.getOrderBy().getOrderByExpressions().get(1).toString().equals("ORGANIZATION_ID"));

            sql="SELECT CONTAINER_ID,ORGANIZATION_ID FROM DESCNULLSLAST3469 order by CONTAINER_ID DESC NULLS LAST,ORGANIZATION_ID ASC NULLS LAST";
            queryPlan =getQueryPlan(conn, sql);
            assertTrue(queryPlan.getOrderBy().getOrderByExpressions().size()==2);
            assertTrue(queryPlan.getOrderBy().getOrderByExpressions().get(0).toString().equals("CONTAINER_ID DESC NULLS LAST"));
            assertTrue(queryPlan.getOrderBy().getOrderByExpressions().get(1).toString().equals("ORGANIZATION_ID NULLS LAST"));

            //-----CONTAINER_ID DESC  ORGANIZATION_ID DESC

            sql="SELECT CONTAINER_ID,ORGANIZATION_ID FROM DESCNULLSLAST3469 order by CONTAINER_ID DESC NULLS FIRST,ORGANIZATION_ID DESC NULLS FIRST";
            queryPlan =getQueryPlan(conn, sql);
            assertTrue(queryPlan.getOrderBy().getOrderByExpressions().size()==2);
            assertTrue(queryPlan.getOrderBy().getOrderByExpressions().get(0).toString().equals("CONTAINER_ID DESC"));
            assertTrue(queryPlan.getOrderBy().getOrderByExpressions().get(1).toString().equals("ORGANIZATION_ID DESC"));

            sql="SELECT CONTAINER_ID,ORGANIZATION_ID FROM DESCNULLSLAST3469 order by CONTAINER_ID DESC NULLS FIRST,ORGANIZATION_ID DESC NULLS LAST";
            queryPlan =getQueryPlan(conn, sql);
            assertTrue(queryPlan.getOrderBy().getOrderByExpressions().size()==2);
            assertTrue(queryPlan.getOrderBy().getOrderByExpressions().get(0).toString().equals("CONTAINER_ID DESC"));
            assertTrue(queryPlan.getOrderBy().getOrderByExpressions().get(1).toString().equals("ORGANIZATION_ID DESC NULLS LAST"));

            sql="SELECT CONTAINER_ID,ORGANIZATION_ID FROM DESCNULLSLAST3469 order by CONTAINER_ID DESC NULLS LAST,ORGANIZATION_ID DESC NULLS FIRST";
            queryPlan =getQueryPlan(conn, sql);
            assertTrue(queryPlan.getOrderBy().getOrderByExpressions().size()==2);
            assertTrue(queryPlan.getOrderBy().getOrderByExpressions().get(0).toString().equals("CONTAINER_ID DESC NULLS LAST"));
            assertTrue(queryPlan.getOrderBy().getOrderByExpressions().get(1).toString().equals("ORGANIZATION_ID DESC"));

            sql="SELECT CONTAINER_ID,ORGANIZATION_ID FROM DESCNULLSLAST3469 order by CONTAINER_ID DESC NULLS LAST,ORGANIZATION_ID DESC NULLS LAST";
            queryPlan =getQueryPlan(conn, sql);
            assertTrue(queryPlan.getOrderBy().getOrderByExpressions().size()==2);
            assertTrue(queryPlan.getOrderBy().getOrderByExpressions().get(0).toString().equals("CONTAINER_ID DESC NULLS LAST"));
            assertTrue(queryPlan.getOrderBy().getOrderByExpressions().get(1).toString().equals("ORGANIZATION_ID DESC NULLS LAST"));
        } finally {
            if(conn!=null) {
                conn.close();
            }
        }
    }

    @Test
    public void testOrderByReverseOptimizationBug3491() throws Exception {
        for(boolean salted: new boolean[]{true,false}) {
            boolean[] groupBys=new boolean[]{true,true,true,true,false,false,false,false};
            doTestOrderByReverseOptimizationBug3491(salted,true,true,true,
                    groupBys,
                    new OrderBy[]{
                    OrderBy.REV_ROW_KEY_ORDER_BY,null,null,OrderBy.FWD_ROW_KEY_ORDER_BY,
                    OrderBy.REV_ROW_KEY_ORDER_BY,null,null,OrderBy.FWD_ROW_KEY_ORDER_BY});

            doTestOrderByReverseOptimizationBug3491(salted,true,true,false,
                    groupBys,
                    new OrderBy[]{
                    OrderBy.REV_ROW_KEY_ORDER_BY,null,null,OrderBy.FWD_ROW_KEY_ORDER_BY,
                    null,OrderBy.REV_ROW_KEY_ORDER_BY,OrderBy.FWD_ROW_KEY_ORDER_BY,null});

            doTestOrderByReverseOptimizationBug3491(salted,true,false,true,
                    groupBys,
                    new OrderBy[]{
                    null,OrderBy.REV_ROW_KEY_ORDER_BY,OrderBy.FWD_ROW_KEY_ORDER_BY,null,
                    OrderBy.REV_ROW_KEY_ORDER_BY,null,null,OrderBy.FWD_ROW_KEY_ORDER_BY});

            doTestOrderByReverseOptimizationBug3491(salted,true,false,false,
                    groupBys,
                    new OrderBy[]{
                    null,OrderBy.REV_ROW_KEY_ORDER_BY,OrderBy.FWD_ROW_KEY_ORDER_BY,null,
                    null,OrderBy.REV_ROW_KEY_ORDER_BY,OrderBy.FWD_ROW_KEY_ORDER_BY,null});

            doTestOrderByReverseOptimizationBug3491(salted,false,true,true,
                    groupBys,
                    new OrderBy[]{
                    null,OrderBy.FWD_ROW_KEY_ORDER_BY,OrderBy.REV_ROW_KEY_ORDER_BY,null,
                    null,OrderBy.FWD_ROW_KEY_ORDER_BY,OrderBy.REV_ROW_KEY_ORDER_BY,null});

            doTestOrderByReverseOptimizationBug3491(salted,false,true,false,
                    groupBys,
                    new OrderBy[]{
                    null,OrderBy.FWD_ROW_KEY_ORDER_BY,OrderBy.REV_ROW_KEY_ORDER_BY,null,
                    OrderBy.FWD_ROW_KEY_ORDER_BY,null,null,OrderBy.REV_ROW_KEY_ORDER_BY});

            doTestOrderByReverseOptimizationBug3491(salted,false,false,true,
                    groupBys,
                    new OrderBy[]{
                    OrderBy.FWD_ROW_KEY_ORDER_BY,null,null,OrderBy.REV_ROW_KEY_ORDER_BY,
                    null,OrderBy.FWD_ROW_KEY_ORDER_BY,OrderBy.REV_ROW_KEY_ORDER_BY,null});

            doTestOrderByReverseOptimizationBug3491(salted,false,false,false,
                    groupBys,
                    new OrderBy[]{
                    OrderBy.FWD_ROW_KEY_ORDER_BY,null,null,OrderBy.REV_ROW_KEY_ORDER_BY,
                    OrderBy.FWD_ROW_KEY_ORDER_BY,null,null,OrderBy.REV_ROW_KEY_ORDER_BY});
        }
    }

    private void doTestOrderByReverseOptimizationBug3491(boolean salted,boolean desc1,boolean desc2,boolean desc3,boolean[] groupBys,OrderBy[] orderBys) throws Exception {
        Connection conn = null;
        try {
            conn= DriverManager.getConnection(getUrl());
            String tableName="ORDERBY3491_TEST";
            conn.createStatement().execute("DROP TABLE if exists "+tableName);
            String sql="CREATE TABLE "+tableName+" ( "+
                    "ORGANIZATION_ID INTEGER NOT NULL,"+
                    "CONTAINER_ID INTEGER NOT NULL,"+
                    "SCORE INTEGER NOT NULL,"+
                    "ENTITY_ID INTEGER NOT NULL,"+
                    "CONSTRAINT TEST_PK PRIMARY KEY ( "+
                    "ORGANIZATION_ID" +(desc1 ? " DESC" : "" )+","+
                    "CONTAINER_ID"+(desc2 ? " DESC" : "" )+","+
                    "SCORE"+(desc3 ? " DESC" : "" )+","+
                    "ENTITY_ID"+
                    ")) "+(salted ? "SALT_BUCKETS =4" : "");
            conn.createStatement().execute(sql);


            String[] sqls={
                    //groupBy orderPreserving orderBy asc asc
                    "SELECT ORGANIZATION_ID,CONTAINER_ID FROM "+tableName+" group by ORGANIZATION_ID, CONTAINER_ID ORDER BY ORGANIZATION_ID ASC, CONTAINER_ID ASC",
                    //groupBy orderPreserving orderBy asc desc
                    "SELECT ORGANIZATION_ID,CONTAINER_ID FROM "+tableName+" group by ORGANIZATION_ID, CONTAINER_ID ORDER BY ORGANIZATION_ID ASC, CONTAINER_ID DESC",
                    //groupBy orderPreserving orderBy desc asc
                    "SELECT ORGANIZATION_ID,CONTAINER_ID FROM "+tableName+" group by ORGANIZATION_ID, CONTAINER_ID ORDER BY ORGANIZATION_ID DESC, CONTAINER_ID ASC",
                    //groupBy orderPreserving orderBy desc desc
                    "SELECT ORGANIZATION_ID,CONTAINER_ID FROM "+tableName+" group by ORGANIZATION_ID, CONTAINER_ID ORDER BY ORGANIZATION_ID DESC, CONTAINER_ID DESC",

                    //groupBy not orderPreserving orderBy asc asc
                    "SELECT ORGANIZATION_ID,SCORE FROM "+tableName+" group by ORGANIZATION_ID, SCORE ORDER BY ORGANIZATION_ID ASC, SCORE ASC",
                    //groupBy not orderPreserving orderBy asc desc
                    "SELECT ORGANIZATION_ID,SCORE FROM "+tableName+" group by ORGANIZATION_ID, SCORE ORDER BY ORGANIZATION_ID ASC, SCORE DESC",
                    //groupBy not orderPreserving orderBy desc asc
                    "SELECT ORGANIZATION_ID,SCORE FROM "+tableName+" group by ORGANIZATION_ID, SCORE ORDER BY ORGANIZATION_ID DESC, SCORE ASC",
                    //groupBy not orderPreserving orderBy desc desc
                    "SELECT ORGANIZATION_ID,SCORE FROM "+tableName+" group by ORGANIZATION_ID, SCORE ORDER BY ORGANIZATION_ID DESC, SCORE DESC"
            };

            for(int i=0;i< sqls.length;i++) {
                sql=sqls[i];
                QueryPlan queryPlan=getQueryPlan(conn, sql);
                assertTrue((i+1) + ") " + sql,queryPlan.getGroupBy().isOrderPreserving()== groupBys[i]);
                OrderBy orderBy=queryPlan.getOrderBy();
                if(orderBys[i]!=null) {
                    assertTrue((i+1) + ") " + sql,orderBy == orderBys[i]);
                }
                else {
                    assertTrue((i+1) + ") " + sql,orderBy.getOrderByExpressions().size() > 0);
                }
            }
        } finally {
            if(conn!=null) {
                conn.close();
            }
        }
    }

    @Test
    public void testOrderByReverseOptimizationWithNUllsLastBug3491() throws Exception {
        for(boolean salted: new boolean[]{true,false}) {
            boolean[] groupBys=new boolean[]{
                    //groupBy orderPreserving orderBy asc asc
                    true,true,true,true,
                    //groupBy orderPreserving orderBy asc desc
                    true,true,true,true,
                    //groupBy orderPreserving orderBy desc asc
                    true,true,true,true,
                    //groupBy orderPreserving orderBy desc desc
                    true,true,true,true,

                    //groupBy not orderPreserving orderBy asc asc
                    false,false,false,false,
                    //groupBy not orderPreserving orderBy asc desc
                    false,false,false,false,
                    //groupBy not orderPreserving orderBy desc asc
                    false,false,false,false,
                    //groupBy not orderPreserving orderBy desc desc
                    false,false,false,false,

                    false,false,false,false};
            doTestOrderByReverseOptimizationWithNUllsLastBug3491(salted,true,true,true,
                    groupBys,
                    new OrderBy[]{
                    //groupBy orderPreserving orderBy asc asc
                    null,null,null,OrderBy.REV_ROW_KEY_ORDER_BY,
                    //groupBy orderPreserving orderBy asc desc
                    null,null,null,null,
                    //groupBy orderPreserving orderBy desc asc
                    null,null,null,null,
                    //groupBy orderPreserving orderBy desc desc
                    OrderBy.FWD_ROW_KEY_ORDER_BY,null,null,null,

                    //groupBy not orderPreserving orderBy asc asc
                    null,null,null,OrderBy.REV_ROW_KEY_ORDER_BY,
                    //groupBy not orderPreserving orderBy asc desc
                    null,null,null,null,
                    //groupBy not orderPreserving orderBy desc asc
                    null,null,null,null,
                    //groupBy not orderPreserving orderBy desc desc
                    OrderBy.FWD_ROW_KEY_ORDER_BY,null,null,null,

                    null,OrderBy.REV_ROW_KEY_ORDER_BY,OrderBy.FWD_ROW_KEY_ORDER_BY,null});

            doTestOrderByReverseOptimizationWithNUllsLastBug3491(salted,true,true,false,
                    groupBys,
                    new OrderBy[]{
                    //groupBy orderPreserving orderBy asc asc
                    null,null,null,OrderBy.REV_ROW_KEY_ORDER_BY,
                    //groupBy orderPreserving orderBy asc desc
                    null,null,null,null,
                    //groupBy orderPreserving orderBy desc asc
                    null,null,null,null,
                    //groupBy orderPreserving orderBy desc desc
                    OrderBy.FWD_ROW_KEY_ORDER_BY,null,null,null,

                    //groupBy not orderPreserving orderBy asc asc
                    null,null,null,null,
                    //groupBy not orderPreserving orderBy asc desc
                    null,null,null,OrderBy.REV_ROW_KEY_ORDER_BY,
                    //groupBy not orderPreserving orderBy desc asc
                    OrderBy.FWD_ROW_KEY_ORDER_BY,null,null,null,
                    //groupBy not orderPreserving orderBy desc desc
                    null,null,null,null,

                    OrderBy.FWD_ROW_KEY_ORDER_BY,null,null,OrderBy.REV_ROW_KEY_ORDER_BY});

            doTestOrderByReverseOptimizationWithNUllsLastBug3491(salted,true,false,true,
                    groupBys,
                    new OrderBy[]{
                    //groupBy orderPreserving orderBy asc asc
                    null,null,null,null,
                    //groupBy orderPreserving orderBy asc desc
                    null,null,null,OrderBy.REV_ROW_KEY_ORDER_BY,
                    //groupBy orderPreserving orderBy desc asc
                    OrderBy.FWD_ROW_KEY_ORDER_BY,null,null,null,
                    //groupBy orderPreserving orderBy desc desc
                    null,null,null,null,

                    //groupBy not orderPreserving orderBy asc asc
                    null,null,null,OrderBy.REV_ROW_KEY_ORDER_BY,
                    //groupBy not orderPreserving orderBy asc desc
                    null,null,null,null,
                    //groupBy not orderPreserving orderBy desc asc
                    null,null,null,null,
                    //groupBy not orderPreserving orderBy desc desc
                    OrderBy.FWD_ROW_KEY_ORDER_BY,null,null,null,
                    null,OrderBy.REV_ROW_KEY_ORDER_BY,OrderBy.FWD_ROW_KEY_ORDER_BY,null});

            doTestOrderByReverseOptimizationWithNUllsLastBug3491(salted,true,false,false,
                    groupBys,
                    new OrderBy[]{
                    //groupBy orderPreserving orderBy asc asc
                    null,null,null,null,
                    //groupBy orderPreserving orderBy asc desc
                    null,null,null,OrderBy.REV_ROW_KEY_ORDER_BY,
                    //groupBy orderPreserving orderBy desc asc
                    OrderBy.FWD_ROW_KEY_ORDER_BY,null,null,null,
                    //groupBy orderPreserving orderBy desc desc
                    null,null,null,null,

                    //groupBy not orderPreserving orderBy asc asc
                    null,null,null,null,
                    //groupBy not orderPreserving orderBy asc desc
                    null,null,null,OrderBy.REV_ROW_KEY_ORDER_BY,
                    //groupBy not orderPreserving orderBy desc asc
                    OrderBy.FWD_ROW_KEY_ORDER_BY,null,null,null,
                    //groupBy not orderPreserving orderBy desc desc
                    null,null,null,null,

                    OrderBy.FWD_ROW_KEY_ORDER_BY,null,null,OrderBy.REV_ROW_KEY_ORDER_BY});

            doTestOrderByReverseOptimizationWithNUllsLastBug3491(salted,false,true,true,
                    groupBys,
                    new OrderBy[]{
                    //groupBy orderPreserving orderBy asc asc
                    null,null,null,null,
                    //groupBy orderPreserving orderBy asc desc
                    OrderBy.FWD_ROW_KEY_ORDER_BY,null,null,null,
                    //groupBy orderPreserving orderBy desc asc
                    null,null,null,OrderBy.REV_ROW_KEY_ORDER_BY,
                    //groupBy orderPreserving orderBy desc desc
                    null,null,null,null,

                    //groupBy not orderPreserving orderBy asc asc
                    null,null,null,null,
                    //groupBy not orderPreserving orderBy asc desc
                    OrderBy.FWD_ROW_KEY_ORDER_BY,null,null,null,
                    //groupBy not orderPreserving orderBy desc asc
                    null,null,null,OrderBy.REV_ROW_KEY_ORDER_BY,
                    //groupBy not orderPreserving orderBy desc desc
                    null,null,null,null,

                    null,OrderBy.REV_ROW_KEY_ORDER_BY,OrderBy.FWD_ROW_KEY_ORDER_BY,null});


            doTestOrderByReverseOptimizationWithNUllsLastBug3491(salted,false,true,false,
                    groupBys,
                    new OrderBy[]{
                    //groupBy orderPreserving orderBy asc asc
                    null,null,null,null,
                    //groupBy orderPreserving orderBy asc desc
                    OrderBy.FWD_ROW_KEY_ORDER_BY,null,null,null,
                    //groupBy orderPreserving orderBy desc asc
                    null,null,null,OrderBy.REV_ROW_KEY_ORDER_BY,
                    //groupBy orderPreserving orderBy desc desc
                    null,null,null,null,

                    //groupBy not orderPreserving orderBy asc asc
                    OrderBy.FWD_ROW_KEY_ORDER_BY,null,null,null,
                    //groupBy not orderPreserving orderBy asc desc
                    null,null,null,null,
                    //groupBy not orderPreserving orderBy desc asc
                    null,null,null,null,
                    //groupBy not orderPreserving orderBy desc desc
                    null,null,null,OrderBy.REV_ROW_KEY_ORDER_BY,

                    OrderBy.FWD_ROW_KEY_ORDER_BY,null,null,OrderBy.REV_ROW_KEY_ORDER_BY});

            doTestOrderByReverseOptimizationWithNUllsLastBug3491(salted,false,false,true,
                    groupBys,
                    new OrderBy[]{
                    //groupBy orderPreserving orderBy asc asc
                    OrderBy.FWD_ROW_KEY_ORDER_BY,null,null,null,
                    //groupBy orderPreserving orderBy asc desc
                    null,null,null,null,
                    //groupBy orderPreserving orderBy desc asc
                    null,null,null,null,
                    //groupBy orderPreserving orderBy desc desc
                    null,null,null,OrderBy.REV_ROW_KEY_ORDER_BY,

                    //groupBy not orderPreserving orderBy asc asc
                    null,null,null,null,
                    //groupBy not orderPreserving orderBy asc desc
                    OrderBy.FWD_ROW_KEY_ORDER_BY,null,null,null,
                    //groupBy not orderPreserving orderBy desc asc
                    null,null,null,OrderBy.REV_ROW_KEY_ORDER_BY,
                    //groupBy not orderPreserving orderBy desc desc
                    null,null,null,null,

                    null,OrderBy.REV_ROW_KEY_ORDER_BY,OrderBy.FWD_ROW_KEY_ORDER_BY,null});

            doTestOrderByReverseOptimizationWithNUllsLastBug3491(salted,false,false,false,
                    groupBys,
                    new OrderBy[]{
                    //groupBy orderPreserving orderBy asc asc
                    OrderBy.FWD_ROW_KEY_ORDER_BY,null,null,null,
                    //groupBy orderPreserving orderBy asc desc
                    null,null,null,null,
                    //groupBy orderPreserving orderBy desc asc
                    null,null,null,null,
                    //groupBy orderPreserving orderBy desc desc
                    null,null,null,OrderBy.REV_ROW_KEY_ORDER_BY,

                    //groupBy not orderPreserving orderBy asc asc
                    OrderBy.FWD_ROW_KEY_ORDER_BY,null,null,null,
                    //groupBy not orderPreserving orderBy asc desc
                    null,null,null,null,
                    //groupBy not orderPreserving orderBy desc asc
                    null,null,null,null,
                    //groupBy not orderPreserving orderBy desc desc
                    null,null,null,OrderBy.REV_ROW_KEY_ORDER_BY,

                    OrderBy.FWD_ROW_KEY_ORDER_BY,null,null,OrderBy.REV_ROW_KEY_ORDER_BY});
        }
    }

    private void doTestOrderByReverseOptimizationWithNUllsLastBug3491(boolean salted,boolean desc1,boolean desc2,boolean desc3,boolean[] groupBys,OrderBy[] orderBys) throws Exception {
        Connection conn = null;
        try {
            conn= DriverManager.getConnection(getUrl());
            String tableName="ORDERBY3491_TEST";
            conn.createStatement().execute("DROP TABLE if exists "+tableName);
            String sql="CREATE TABLE "+tableName+" ( "+
                    "ORGANIZATION_ID VARCHAR,"+
                    "CONTAINER_ID VARCHAR,"+
                    "SCORE VARCHAR,"+
                    "ENTITY_ID VARCHAR NOT NULL,"+
                    "CONSTRAINT TEST_PK PRIMARY KEY ( "+
                    "ORGANIZATION_ID" +(desc1 ? " DESC" : "" )+","+
                    "CONTAINER_ID"+(desc2 ? " DESC" : "" )+","+
                    "SCORE"+(desc3 ? " DESC" : "" )+","+
                    "ENTITY_ID"+
                    ")) "+(salted ? "SALT_BUCKETS =4" : "");
            conn.createStatement().execute(sql);

            String[] sqls={
                    //groupBy orderPreserving orderBy asc asc
                    "SELECT ORGANIZATION_ID,CONTAINER_ID FROM "+tableName+" group by ORGANIZATION_ID, CONTAINER_ID ORDER BY ORGANIZATION_ID ASC NULLS FIRST, CONTAINER_ID ASC NULLS FIRST",
                    "SELECT ORGANIZATION_ID,CONTAINER_ID FROM "+tableName+" group by ORGANIZATION_ID, CONTAINER_ID ORDER BY ORGANIZATION_ID ASC NULLS FIRST, CONTAINER_ID ASC NULLS LAST",
                    "SELECT ORGANIZATION_ID,CONTAINER_ID FROM "+tableName+" group by ORGANIZATION_ID, CONTAINER_ID ORDER BY ORGANIZATION_ID ASC NULLS LAST, CONTAINER_ID ASC NULLS FIRST",
                    "SELECT ORGANIZATION_ID,CONTAINER_ID FROM "+tableName+" group by ORGANIZATION_ID, CONTAINER_ID ORDER BY ORGANIZATION_ID ASC NULLS LAST, CONTAINER_ID ASC NULLS LAST",

                    //groupBy orderPreserving orderBy asc desc
                    "SELECT ORGANIZATION_ID,CONTAINER_ID FROM "+tableName+" group by ORGANIZATION_ID, CONTAINER_ID ORDER BY ORGANIZATION_ID ASC NULLS FIRST, CONTAINER_ID DESC NULLS FIRST",
                    "SELECT ORGANIZATION_ID,CONTAINER_ID FROM "+tableName+" group by ORGANIZATION_ID, CONTAINER_ID ORDER BY ORGANIZATION_ID ASC NULLS FIRST, CONTAINER_ID DESC NULLS LAST",
                    "SELECT ORGANIZATION_ID,CONTAINER_ID FROM "+tableName+" group by ORGANIZATION_ID, CONTAINER_ID ORDER BY ORGANIZATION_ID ASC NULLS LAST, CONTAINER_ID DESC NULLS FIRST",
                    "SELECT ORGANIZATION_ID,CONTAINER_ID FROM "+tableName+" group by ORGANIZATION_ID, CONTAINER_ID ORDER BY ORGANIZATION_ID ASC NULLS LAST, CONTAINER_ID DESC NULLS LAST",

                    //groupBy orderPreserving orderBy desc asc
                    "SELECT ORGANIZATION_ID,CONTAINER_ID FROM "+tableName+" group by ORGANIZATION_ID, CONTAINER_ID ORDER BY ORGANIZATION_ID DESC NULLS FIRST, CONTAINER_ID ASC NULLS FIRST",
                    "SELECT ORGANIZATION_ID,CONTAINER_ID FROM "+tableName+" group by ORGANIZATION_ID, CONTAINER_ID ORDER BY ORGANIZATION_ID DESC NULLS FIRST, CONTAINER_ID ASC NULLS LAST",
                    "SELECT ORGANIZATION_ID,CONTAINER_ID FROM "+tableName+" group by ORGANIZATION_ID, CONTAINER_ID ORDER BY ORGANIZATION_ID DESC NULLS LAST, CONTAINER_ID ASC NULLS FIRST",
                    "SELECT ORGANIZATION_ID,CONTAINER_ID FROM "+tableName+" group by ORGANIZATION_ID, CONTAINER_ID ORDER BY ORGANIZATION_ID DESC NULLS LAST, CONTAINER_ID ASC NULLS LAST",

                    //groupBy orderPreserving orderBy desc desc
                    "SELECT ORGANIZATION_ID,CONTAINER_ID FROM "+tableName+" group by ORGANIZATION_ID, CONTAINER_ID ORDER BY ORGANIZATION_ID DESC NULLS FIRST, CONTAINER_ID DESC NULLS FIRST",
                    "SELECT ORGANIZATION_ID,CONTAINER_ID FROM "+tableName+" group by ORGANIZATION_ID, CONTAINER_ID ORDER BY ORGANIZATION_ID DESC NULLS FIRST, CONTAINER_ID DESC NULLS LAST",
                    "SELECT ORGANIZATION_ID,CONTAINER_ID FROM "+tableName+" group by ORGANIZATION_ID, CONTAINER_ID ORDER BY ORGANIZATION_ID DESC NULLS LAST, CONTAINER_ID DESC NULLS FIRST",
                    "SELECT ORGANIZATION_ID,CONTAINER_ID FROM "+tableName+" group by ORGANIZATION_ID, CONTAINER_ID ORDER BY ORGANIZATION_ID DESC NULLS LAST, CONTAINER_ID DESC NULLS LAST",

                    //-----groupBy not orderPreserving

                    //groupBy not orderPreserving orderBy asc asc
                    "SELECT ORGANIZATION_ID,SCORE FROM "+tableName+" group by ORGANIZATION_ID, SCORE ORDER BY ORGANIZATION_ID ASC NULLS FIRST, SCORE ASC NULLS FIRST",
                    "SELECT ORGANIZATION_ID,SCORE FROM "+tableName+" group by ORGANIZATION_ID, SCORE ORDER BY ORGANIZATION_ID ASC NULLS FIRST, SCORE ASC NULLS LAST",
                    "SELECT ORGANIZATION_ID,SCORE FROM "+tableName+" group by ORGANIZATION_ID, SCORE ORDER BY ORGANIZATION_ID ASC NULLS LAST, SCORE ASC NULLS FIRST",
                    "SELECT ORGANIZATION_ID,SCORE FROM "+tableName+" group by ORGANIZATION_ID, SCORE ORDER BY ORGANIZATION_ID ASC NULLS LAST, SCORE ASC NULLS LAST",

                    //groupBy not orderPreserving orderBy asc desc
                    "SELECT ORGANIZATION_ID,SCORE FROM "+tableName+" group by ORGANIZATION_ID, SCORE ORDER BY ORGANIZATION_ID ASC NULLS FIRST, SCORE DESC NULLS FIRST",
                    "SELECT ORGANIZATION_ID,SCORE FROM "+tableName+" group by ORGANIZATION_ID, SCORE ORDER BY ORGANIZATION_ID ASC NULLS FIRST, SCORE DESC NULLS LAST",
                    "SELECT ORGANIZATION_ID,SCORE FROM "+tableName+" group by ORGANIZATION_ID, SCORE ORDER BY ORGANIZATION_ID ASC NULLS LAST, SCORE DESC NULLS FIRST",
                    "SELECT ORGANIZATION_ID,SCORE FROM "+tableName+" group by ORGANIZATION_ID, SCORE ORDER BY ORGANIZATION_ID ASC NULLS LAST, SCORE DESC NULLS LAST",

                    //groupBy not orderPreserving orderBy desc asc
                    "SELECT ORGANIZATION_ID,SCORE FROM "+tableName+" group by ORGANIZATION_ID, SCORE ORDER BY ORGANIZATION_ID DESC NULLS FIRST, SCORE ASC NULLS FIRST",
                    "SELECT ORGANIZATION_ID,SCORE FROM "+tableName+" group by ORGANIZATION_ID, SCORE ORDER BY ORGANIZATION_ID DESC NULLS FIRST, SCORE ASC NULLS LAST",
                    "SELECT ORGANIZATION_ID,SCORE FROM "+tableName+" group by ORGANIZATION_ID, SCORE ORDER BY ORGANIZATION_ID DESC NULLS LAST, SCORE ASC NULLS FIRST",
                    "SELECT ORGANIZATION_ID,SCORE FROM "+tableName+" group by ORGANIZATION_ID, SCORE ORDER BY ORGANIZATION_ID DESC NULLS LAST, SCORE ASC NULLS LAST",

                    //groupBy not orderPreserving orderBy desc desc
                    "SELECT ORGANIZATION_ID,SCORE FROM "+tableName+" group by ORGANIZATION_ID, SCORE ORDER BY ORGANIZATION_ID DESC NULLS FIRST, SCORE DESC NULLS FIRST",
                    "SELECT ORGANIZATION_ID,SCORE FROM "+tableName+" group by ORGANIZATION_ID, SCORE ORDER BY ORGANIZATION_ID DESC NULLS FIRST, SCORE DESC NULLS LAST",
                    "SELECT ORGANIZATION_ID,SCORE FROM "+tableName+" group by ORGANIZATION_ID, SCORE ORDER BY ORGANIZATION_ID DESC NULLS LAST, SCORE DESC NULLS FIRST",
                    "SELECT ORGANIZATION_ID,SCORE FROM "+tableName+" group by ORGANIZATION_ID, SCORE ORDER BY ORGANIZATION_ID DESC NULLS LAST, SCORE DESC NULLS LAST",

                    //-------only one return column----------------------------------
                    "SELECT SCORE FROM "+tableName+" group by SCORE ORDER BY SCORE ASC NULLS FIRST",
                    "SELECT SCORE FROM "+tableName+" group by SCORE ORDER BY SCORE ASC NULLS LAST",
                    "SELECT SCORE FROM "+tableName+" group by SCORE ORDER BY SCORE DESC NULLS FIRST",
                    "SELECT SCORE FROM "+tableName+" group by SCORE ORDER BY SCORE DESC NULLS LAST"
            };

            for(int i=0;i< sqls.length;i++) {
                sql=sqls[i];
                QueryPlan queryPlan=getQueryPlan(conn, sql);
                assertTrue((i+1) + ") " + sql,queryPlan.getGroupBy().isOrderPreserving()== groupBys[i]);
                OrderBy orderBy=queryPlan.getOrderBy();
                if(orderBys[i]!=null) {
                    assertTrue((i+1) + ") " + sql,orderBy == orderBys[i]);
                }
                else {
                    assertTrue((i+1) + ") " + sql,orderBy.getOrderByExpressions().size() > 0);
                }
            }
        } finally {
            if(conn!=null) {
                conn.close();
            }
        }
    }

    @Test
    public void testGroupByCoerceExpressionBug3453() throws Exception {
        Connection conn = null;
        try {
            conn= DriverManager.getConnection(getUrl());
            String tableName="GROUPBY3453_INT";
            String sql="CREATE TABLE "+ tableName +"("+
                    "ENTITY_ID INTEGER NOT NULL,"+
                    "CONTAINER_ID INTEGER NOT NULL,"+
                    "SCORE INTEGER NOT NULL,"+
                    "CONSTRAINT TEST_PK PRIMARY KEY (ENTITY_ID DESC,CONTAINER_ID DESC,SCORE DESC))";
            conn.createStatement().execute(sql);
            sql="select DISTINCT entity_id, score from ( select entity_id, score from "+tableName+" limit 1)";
            QueryPlan queryPlan=getQueryPlan(conn, sql);
            assertTrue(queryPlan.getGroupBy().getExpressions().get(0).getSortOrder()==SortOrder.DESC);
            assertTrue(queryPlan.getGroupBy().getExpressions().get(1).getSortOrder()==SortOrder.DESC);
            assertTrue(queryPlan.getGroupBy().getKeyExpressions().get(0).getSortOrder()==SortOrder.DESC);
            assertTrue(queryPlan.getGroupBy().getKeyExpressions().get(1).getSortOrder()==SortOrder.DESC);

            sql="select DISTINCT entity_id, score from ( select entity_id, score from "+tableName+" limit 3) order by entity_id";
            queryPlan=getQueryPlan(conn, sql);
            assertTrue(queryPlan.getGroupBy().getExpressions().get(0).getSortOrder()==SortOrder.DESC);
            assertTrue(queryPlan.getGroupBy().getExpressions().get(1).getSortOrder()==SortOrder.DESC);
            assertTrue(queryPlan.getGroupBy().getKeyExpressions().get(0).getSortOrder()==SortOrder.DESC);
            assertTrue(queryPlan.getGroupBy().getKeyExpressions().get(1).getSortOrder()==SortOrder.DESC);
            assertTrue(queryPlan.getOrderBy().getOrderByExpressions().get(0).getExpression().getSortOrder()==SortOrder.DESC);

            sql="select DISTINCT entity_id, score from ( select entity_id, score from "+tableName+" limit 3) order by entity_id desc";
            queryPlan=getQueryPlan(conn, sql);
            assertTrue(queryPlan.getGroupBy().getExpressions().get(0).getSortOrder()==SortOrder.DESC);
            assertTrue(queryPlan.getGroupBy().getExpressions().get(1).getSortOrder()==SortOrder.DESC);
            assertTrue(queryPlan.getGroupBy().getKeyExpressions().get(0).getSortOrder()==SortOrder.DESC);
            assertTrue(queryPlan.getGroupBy().getKeyExpressions().get(1).getSortOrder()==SortOrder.DESC);
            assertTrue(queryPlan.getOrderBy()==OrderBy.FWD_ROW_KEY_ORDER_BY);
        } finally {
            if(conn!=null) {
                conn.close();
            }
        }
    }

    private static QueryPlan getQueryPlan(Connection conn,String sql) throws SQLException {
        PhoenixPreparedStatement statement = conn.prepareStatement(sql).unwrap(PhoenixPreparedStatement.class);
        QueryPlan queryPlan = statement.optimizeQuery(sql);
        queryPlan.iterator();
        return queryPlan;
    }
}
