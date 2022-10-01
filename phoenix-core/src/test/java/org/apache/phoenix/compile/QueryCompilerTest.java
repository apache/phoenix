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
import java.sql.SQLFeatureNotSupportedException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Properties;
import java.util.Set;

import org.apache.hadoop.hbase.HRegionLocation;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.filter.FirstKeyOnlyFilter;
import org.apache.hadoop.hbase.filter.PageFilter;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.phoenix.compile.JoinCompiler.JoinTable;
import org.apache.phoenix.compile.JoinCompiler.Table;
import org.apache.phoenix.compile.OrderByCompiler.OrderBy;
import org.apache.phoenix.coprocessor.BaseScannerRegionObserver;
import org.apache.phoenix.exception.SQLExceptionCode;
import org.apache.phoenix.execute.AggregatePlan;
import org.apache.phoenix.execute.ClientAggregatePlan;
import org.apache.phoenix.execute.ClientScanPlan;
import org.apache.phoenix.execute.CursorFetchPlan;
import org.apache.phoenix.execute.HashJoinPlan;
import org.apache.phoenix.execute.HashJoinPlan.HashSubPlan;
import org.apache.phoenix.execute.HashJoinPlan.SubPlan;
import org.apache.phoenix.execute.HashJoinPlan.WhereClauseSubPlan;
import org.apache.phoenix.execute.LiteralResultIterationPlan;
import org.apache.phoenix.execute.ScanPlan;
import org.apache.phoenix.execute.SortMergeJoinPlan;
import org.apache.phoenix.execute.TupleProjectionPlan;
import org.apache.phoenix.execute.TupleProjector;
import org.apache.phoenix.execute.UnionPlan;
import org.apache.phoenix.execute.UnnestArrayPlan;
import org.apache.phoenix.execute.visitor.QueryPlanVisitor;
import org.apache.phoenix.expression.Expression;
import org.apache.phoenix.expression.LiteralExpression;
import org.apache.phoenix.expression.aggregator.Aggregator;
import org.apache.phoenix.expression.aggregator.CountAggregator;
import org.apache.phoenix.expression.aggregator.ServerAggregators;
import org.apache.phoenix.expression.function.TimeUnit;
import org.apache.phoenix.filter.EncodedQualifiersColumnProjectionFilter;
import org.apache.phoenix.jdbc.PhoenixConnection;
import org.apache.phoenix.jdbc.PhoenixPreparedStatement;
import org.apache.phoenix.jdbc.PhoenixStatement;
import org.apache.phoenix.parse.ParseNodeFactory;
import org.apache.phoenix.query.BaseConnectionlessQueryTest;
import org.apache.phoenix.query.QueryConstants;
import org.apache.phoenix.query.QueryServices;
import org.apache.phoenix.schema.AmbiguousColumnException;
import org.apache.phoenix.schema.ColumnAlreadyExistsException;
import org.apache.phoenix.schema.ColumnNotFoundException;
import org.apache.phoenix.schema.PColumn;
import org.apache.phoenix.schema.PTable;
import org.apache.phoenix.schema.PTableKey;
import org.apache.phoenix.schema.PTableType;
import org.apache.phoenix.schema.SortOrder;
import org.apache.phoenix.schema.TableRef;
import org.apache.phoenix.schema.types.PChar;
import org.apache.phoenix.schema.types.PDecimal;
import org.apache.phoenix.schema.types.PInteger;
import org.apache.phoenix.schema.types.PVarchar;
import org.apache.phoenix.thirdparty.com.google.common.collect.Lists;
import org.apache.phoenix.util.ByteUtil;
import org.apache.phoenix.util.EnvironmentEdgeManager;
import org.apache.phoenix.util.PhoenixRuntime;
import org.apache.phoenix.util.PropertiesUtil;
import org.apache.phoenix.util.QueryUtil;
import org.apache.phoenix.util.ScanUtil;
import org.apache.phoenix.util.SchemaUtil;
import org.apache.phoenix.util.TestUtil;
import org.junit.Before;
import org.junit.Ignore;
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

    @Before
    public void setUp() {
        ParseNodeFactory.setTempAliasCounterValue(0);
    }

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
            assertEquals(SQLExceptionCode.KEY_VALUE_NOT_NULL.getErrorCode(), e.getErrorCode());
        } finally {
            conn.close();
        }
    }

    @Test
    public void testImmutableRowsPK() throws Exception {
        Connection conn = DriverManager.getConnection(getUrl());
        try {
            String query = "CREATE IMMUTABLE TABLE foo (pk integer not null, col1 decimal, col2 decimal)";
            PreparedStatement statement = conn.prepareStatement(query);
            statement.execute();
            fail();
        } catch (SQLException e) {
            assertEquals(SQLExceptionCode.PRIMARY_KEY_MISSING.getErrorCode(), e.getErrorCode());
        }
        String query = "CREATE IMMUTABLE TABLE foo (k1 integer not null, k2 decimal not null, col1 decimal not null, constraint pk primary key (k1,k2))";
        PreparedStatement statement = conn.prepareStatement(query);
        statement.execute();
        conn.close();
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
        String value = "繰り返し曜日マスク";
        try {
            // Select non agg column in aggregate query
            String query = "upsert into ATABLE VALUES (?, ?, ?)";
            Properties props = PropertiesUtil.deepCopy(TEST_PROPERTIES);
            Connection conn = DriverManager.getConnection(getUrl(), props);
            try {
                PreparedStatement statement = conn.prepareStatement(query);
                statement.setString(1, "00D300000000XHP");
                statement.setString(2, value);
                statement.setInt(3, 1);
                statement.executeUpdate();
                fail();
            } finally {
                conn.close();
            }
        } catch (SQLException e) {
            assertTrue(e.getMessage(), e.getMessage().contains("ERROR 201 (22000): Illegal data."));
            assertTrue(e.getMessage().contains("CHAR types may only contain single byte characters"));
            assertFalse(e.getMessage().contains(value));
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
                ServerAggregators aggregators = ServerAggregators.deserialize(scan.getAttribute(BaseScannerRegionObserver.AGGREGATORS), null, null);
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
        String query = "SELECT * from multi_cf G where RESPONSE_TIME = 2222";
        Properties props = PropertiesUtil.deepCopy(TEST_PROPERTIES);
        Connection conn = DriverManager.getConnection(getUrl(), props);
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
        String query = "SELECT F.RESPONSE_TIME,G.RESPONSE_TIME from multi_cf G where G.RESPONSE_TIME-1 = F.RESPONSE_TIME";
        Properties props = PropertiesUtil.deepCopy(TEST_PROPERTIES);
        Connection conn = DriverManager.getConnection(getUrl(), props);
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
        String query = "SELECT coalesce(x_integer,'foo') from atable";
        Properties props = PropertiesUtil.deepCopy(TEST_PROPERTIES);
        Connection conn = DriverManager.getConnection(getUrl(), props);
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
        String query = "SELECT distinct a_string,b_string from atable order by x_integer";
        Properties props = PropertiesUtil.deepCopy(TEST_PROPERTIES);
        Connection conn = DriverManager.getConnection(getUrl(), props);
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
        String query = "SELECT all distinct a_string,b_string from atable order by x_integer";
        Properties props = PropertiesUtil.deepCopy(TEST_PROPERTIES);
        Connection conn = DriverManager.getConnection(getUrl(), props);
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
        String query = "select /*+ RANGE_SCAN */ count(distinct organization_id) from atable order by organization_id";
        String query1 = "select count(distinct organization_id) from atable order by organization_id";
        Properties props = PropertiesUtil.deepCopy(TEST_PROPERTIES);
        Connection conn = DriverManager.getConnection(getUrl(), props);
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
        String query = "SELECT distinct count(1) from atable order by x_integer";
        Properties props = PropertiesUtil.deepCopy(TEST_PROPERTIES);
        Connection conn = DriverManager.getConnection(getUrl(), props);
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
        String query = "SELECT distinct a_string,count(*) from atable";
        Properties props = PropertiesUtil.deepCopy(TEST_PROPERTIES);
        Connection conn = DriverManager.getConnection(getUrl(), props);
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
        String query = "SELECT entity_id,a_string FROM atable where 2 || a_integer || ? like '2%'";
        Properties props = PropertiesUtil.deepCopy(TEST_PROPERTIES);
        Connection conn = DriverManager.getConnection(getUrl(), props);
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
        String query = "SELECT a_integer/x_integer/0.0 FROM atable";
        Connection conn = DriverManager.getConnection(getUrl());
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
        String query = "SELECT a_integer/0 FROM atable";
        Connection conn = DriverManager.getConnection(getUrl());
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
        String query = "CREATE TABLE foo(i integer not null, j integer null, k integer not null CONSTRAINT pk PRIMARY KEY(i,j,k))";
        Connection conn = DriverManager.getConnection(getUrl());
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
        Connection conn = DriverManager.getConnection(getUrl());
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
    public void testAlterNotNull() throws Exception {
        Connection conn = DriverManager.getConnection(getUrl());
        try {
            conn.createStatement().execute("ALTER TABLE atable ADD xyz VARCHAR NOT NULL");
            fail();
        } catch (SQLException e) { // expected
            assertEquals(SQLExceptionCode.KEY_VALUE_NOT_NULL.getErrorCode(), e.getErrorCode());
        }
        conn.createStatement().execute("CREATE IMMUTABLE TABLE foo (K1 VARCHAR PRIMARY KEY)");
        try {
            conn.createStatement().execute("ALTER TABLE foo ADD xyz VARCHAR NOT NULL PRIMARY KEY");
            fail();
        } catch (SQLException e) { // expected
            assertEquals(SQLExceptionCode.NOT_NULLABLE_COLUMN_IN_ROW_KEY.getErrorCode(), e.getErrorCode());
        }
        conn.createStatement().execute("ALTER TABLE FOO ADD xyz VARCHAR NOT NULL");
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
    public void testCastingTimestampToDateInSelect() throws Exception {
        String query = "SELECT CAST (a_timestamp AS DATE) FROM aTable";
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
    public void testNotOrderByOrderPreservingForAggregation() throws Exception {
        Connection conn = DriverManager.getConnection(getUrl());
        conn.createStatement().execute("CREATE TABLE IF NOT EXISTS VA_TEST(ID VARCHAR NOT NULL PRIMARY KEY, VAL1 VARCHAR, VAL2 INTEGER)");
        String[] queries = {
                "select distinct ID, VAL1, VAL2 from VA_TEST where \"ID\" in ('ABC','ABD','ABE','ABF','ABG','ABH','AAA', 'AAB', 'AAC','AAD','AAE','AAF') order by VAL1 ASC"
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
    public void testArrayAppendSingleArg() throws SQLException {
        Connection conn = DriverManager.getConnection(getUrl());
        try {
            conn.createStatement().execute("CREATE TABLE t (p INTEGER PRIMARY KEY, arr1 INTEGER ARRAY, arr2 INTEGER ARRAY)");
            conn.createStatement().executeQuery("SELECT ARRAY_APPEND(arr2) from t");
            fail();
        } catch (SQLException e) {
            assertEquals(SQLExceptionCode.FUNCTION_UNDEFINED.getErrorCode(),e.getErrorCode());
        } finally {
            conn.close();
        }
    }

    @Test
    public void testArrayPrependSingleArg() throws SQLException {
        Connection conn = DriverManager.getConnection(getUrl());
        try {
            conn.createStatement().execute("CREATE TABLE t (p INTEGER PRIMARY KEY, arr1 INTEGER ARRAY, arr2 INTEGER ARRAY)");
            conn.createStatement().executeQuery("SELECT ARRAY_PREPEND(arr2) from t");
            fail();
        } catch (SQLException e) {
            assertEquals(SQLExceptionCode.FUNCTION_UNDEFINED.getErrorCode(),e.getErrorCode());
        } finally {
            conn.close();
        }
    }

    @Test
    public void testArrayConcatSingleArg() throws SQLException {
        Connection conn = DriverManager.getConnection(getUrl());
        try {
            conn.createStatement().execute("CREATE TABLE t (p INTEGER PRIMARY KEY, arr1 INTEGER ARRAY, arr2 INTEGER ARRAY)");
            conn.createStatement().executeQuery("SELECT ARRAY_CAT(arr2) from t");
            fail();
        } catch (SQLException e) {
            assertEquals(SQLExceptionCode.FUNCTION_UNDEFINED.getErrorCode(),e.getErrorCode());
        } finally {
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
    public void testDMLOfNonIndexWithBuildIndexAt() throws Exception {
        Properties props = PropertiesUtil.deepCopy(TEST_PROPERTIES);
        try (Connection conn = DriverManager.getConnection(getUrl(), props);) {
            conn.createStatement().execute(
                    "CREATE TABLE t (k VARCHAR NOT NULL PRIMARY KEY, v1 VARCHAR)");
        }
        props.put(PhoenixRuntime.BUILD_INDEX_AT_ATTRIB, Long.toString(EnvironmentEdgeManager.currentTimeMillis()+1));
        try (Connection conn = DriverManager.getConnection(getUrl(), props);) {
            try {
            	conn.createStatement().execute("UPSERT INTO T (k,v1) SELECT k,v1 FROM T");
                fail();
            } catch (SQLException e) {
                assertEquals("Unexpected Exception",
                        SQLExceptionCode.ONLY_INDEX_UPDATABLE_AT_SCN
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
                    (BaseScannerRegionObserver.AGGREGATORS), null, null);
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
                assertTrue(ranges.intersectRegion(regionLocation.getRegion().getStartKey(),
                    regionLocation.getRegion().getEndKey(), false));
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
    public void testNotNullKeyValueColumnSalted() throws Exception {
        testNotNullKeyValueColumn(3);
    }
    @Test
    public void testNotNullKeyValueColumnUnsalted() throws Exception {
        testNotNullKeyValueColumn(0);
    }
    
    private void testNotNullKeyValueColumn(int saltBuckets) throws Exception {
        Connection conn = DriverManager.getConnection(getUrl());
        try {
            conn.createStatement().execute("CREATE TABLE t1 (k integer not null primary key, v bigint not null) IMMUTABLE_ROWS=true" + (saltBuckets == 0 ? "" : (",SALT_BUCKETS="+saltBuckets)));
            conn.createStatement().execute("UPSERT INTO t1 VALUES(0)");
            fail();
        } catch (SQLException e) {
            assertEquals(SQLExceptionCode.CONSTRAINT_VIOLATION.getErrorCode(), e.getErrorCode());
        }
        try {
            conn.createStatement().execute("CREATE TABLE t2 (k integer not null primary key, v1 bigint not null, v2 varchar, v3 tinyint not null) IMMUTABLE_ROWS=true" + (saltBuckets == 0 ? "" : (",SALT_BUCKETS="+saltBuckets)));
            conn.createStatement().execute("UPSERT INTO t2(k, v3) VALUES(0,0)");
            fail();
        } catch (SQLException e) {
            assertEquals(SQLExceptionCode.CONSTRAINT_VIOLATION.getErrorCode(), e.getErrorCode());
        }
        try {
            conn.createStatement().execute("CREATE TABLE t3 (k integer not null primary key, v1 bigint not null, v2 varchar, v3 tinyint not null) IMMUTABLE_ROWS=true" + (saltBuckets == 0 ? "" : (",SALT_BUCKETS="+saltBuckets)));
            conn.createStatement().execute("UPSERT INTO t3(k, v1) VALUES(0,0)");
            fail();
        } catch (SQLException e) {
            assertEquals(SQLExceptionCode.CONSTRAINT_VIOLATION.getErrorCode(), e.getErrorCode());
        }
        conn.createStatement().execute("CREATE TABLE t4 (k integer not null primary key, v1 bigint not null) IMMUTABLE_ROWS=true" + (saltBuckets == 0 ? "" : (",SALT_BUCKETS="+saltBuckets)));
        conn.createStatement().execute("UPSERT INTO t4 VALUES(0,0)");
        conn.createStatement().execute("CREATE TABLE t5 (k integer not null primary key, v1 bigint not null default 0) IMMUTABLE_ROWS=true" + (saltBuckets == 0 ? "" : (",SALT_BUCKETS="+saltBuckets)));
        conn.createStatement().execute("UPSERT INTO t5 VALUES(0)");
        conn.close();
    }

    @Test
    public void testAlterAddNotNullKeyValueColumn() throws Exception {
        Connection conn = DriverManager.getConnection(getUrl());
        conn.createStatement().execute("CREATE TABLE t1 (k integer not null primary key, v1 bigint not null) IMMUTABLE_ROWS=true");
        try {
            conn.createStatement().execute("ALTER TABLE t1 ADD k2 bigint not null primary key");
            fail();
        } catch (SQLException e) {
            assertEquals(SQLExceptionCode.NOT_NULLABLE_COLUMN_IN_ROW_KEY.getErrorCode(), e.getErrorCode());
        }
        conn.createStatement().execute("ALTER TABLE t1 ADD v2 bigint not null");
        try {
            conn.createStatement().execute("UPSERT INTO t1(k, v1) VALUES(0,0)");
            fail();
        } catch (SQLException e) {
            assertEquals(SQLExceptionCode.CONSTRAINT_VIOLATION.getErrorCode(), e.getErrorCode());
        }
        conn.createStatement().execute("UPSERT INTO t1 VALUES(0,0,0)");
        conn.createStatement().execute("UPSERT INTO t1(v1,k,v2) VALUES(0,0,0)");
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
    public void testOnDupKeyWithGlobalIndex() throws Exception {
        Connection conn = DriverManager.getConnection(getUrl());
        try {
            conn.createStatement().execute("CREATE TABLE t1 (k integer not null primary key, v bigint)");
            conn.createStatement().execute("CREATE INDEX idx ON t1 (v)");
            conn.createStatement().execute("UPSERT INTO t1 VALUES(0,0) ON DUPLICATE KEY UPDATE v = v + 1");
            fail();
        } catch (SQLException e) {
            assertEquals(SQLExceptionCode.CANNOT_USE_ON_DUP_KEY_WITH_GLOBAL_IDX.getErrorCode(), e.getErrorCode());
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
    public void testOrderPreservingGroupByForNotPkColumns() throws Exception {
         try (Connection conn= DriverManager.getConnection(getUrl())) {
             conn.createStatement().execute("CREATE TABLE test (\n" +
                    "            pk1 varchar, \n" +
                    "            pk2 varchar, \n" +
                    "            pk3 varchar, \n" +
                    "            pk4 varchar, \n" +
                    "            v1 varchar, \n" +
                    "            v2 varchar,\n" +
                    "            CONSTRAINT pk PRIMARY KEY (\n" +
                    "               pk1,\n" +
                    "               pk2,\n" +
                    "               pk3,\n" +
                    "               pk4\n" +
                    "             )\n" +
                    "         )");
            String[] queries = new String[] {
                    "SELECT pk3 FROM test WHERE v2 = 'a' GROUP BY substr(v2,0,1),pk3 ORDER BY pk3",
                    "SELECT pk3 FROM test WHERE pk1 = 'c' and v2 = substr('abc',1,1) GROUP BY v2,pk3 ORDER BY pk3",
                    "SELECT pk3 FROM test WHERE v1 = 'a' and v2 = 'b' GROUP BY length(v1)+length(v2),pk3 ORDER BY pk3",
                    "SELECT pk3 FROM test WHERE pk1 = 'a' and v2 = 'b' GROUP BY length(pk1)+length(v2),pk3 ORDER BY pk3",
                    "SELECT pk3 FROM test WHERE v1 = 'a' and v2 = substr('abc',2,1) GROUP BY pk4,CASE WHEN v1 > v2 THEN v1 ELSE v2 END,pk3 ORDER BY pk4,pk3",
                    "SELECT pk3 FROM test WHERE pk1 = 'a' and v2 = substr('abc',2,1) GROUP BY pk4,CASE WHEN pk1 > v2 THEN pk1 ELSE v2 END,pk3 ORDER BY pk4,pk3",
                    "SELECT pk3 FROM test WHERE pk1 = 'a' and pk2 = 'b' and v1 = 'c' GROUP BY CASE WHEN pk1 > pk2 THEN v1 WHEN pk1 = pk2 THEN pk1 ELSE pk2 END,pk3 ORDER BY pk3"
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
    public void testOrderPreservingGroupByForClientAggregatePlan() throws Exception {
        Connection conn = null;
         try {
             conn = DriverManager.getConnection(getUrl());
             String tableName = "test_table";
             String sql = "create table " + tableName + "( "+
                     " pk1 varchar not null , " +
                     " pk2 varchar not null, " +
                     " pk3 varchar not null," +
                     " v1 varchar, " +
                     " v2 varchar, " +
                     " CONSTRAINT TEST_PK PRIMARY KEY ( "+
                        "pk1,"+
                        "pk2,"+
                        "pk3 ))";
             conn.createStatement().execute(sql);

             String[] queries = new String[] {
                   "select a.ak3 "+
                   "from (select substr(pk1,1,1) ak1,substr(pk2,1,1) ak2,substr(pk3,1,1) ak3,substr(v1,1,1) av1,substr(v2,1,1) av2 from "+tableName+" order by pk2,pk3 limit 10) a "+
                   "group by a.ak3,a.av1 order by a.ak3,a.av1",

                   "select a.ak3 "+
                   "from (select substr(pk1,1,1) ak1,substr(pk2,1,1) ak2,substr(pk3,1,1) ak3,substr(v1,1,1) av1,substr(v2,1,1) av2 from "+tableName+" order by pk2,pk3 limit 10) a "+
                   "where a.av2 = 'a' GROUP BY substr(a.av2,0,1),ak3 ORDER BY ak3",

                   //for InListExpression
                   "select a.ak3 "+
                   "from (select substr(pk1,1,1) ak1,substr(pk2,1,1) ak2,substr(pk3,1,1) ak3,substr(v1,1,1) av1,substr(v2,1,1) av2 from "+tableName+" order by pk2,pk3 limit 10) a "+
                   "where a.av2 in('a') GROUP BY substr(a.av2,0,1),ak3 ORDER BY ak3",

                   "select a.ak3 "+
                   "from (select substr(pk1,1,1) ak1,substr(pk2,1,1) ak2,substr(pk3,1,1) ak3,substr(v1,1,1) av1,substr(v2,1,1) av2 from "+tableName+" order by pk2,pk3 limit 10) a "+
                   "where a.ak1 = 'c' and a.av2 = substr('abc',1,1) GROUP BY a.av2,a.ak3 ORDER BY a.ak3",

                   //for RVC
                   "select a.ak3 "+
                   "from (select substr(pk1,1,1) ak1,substr(pk2,1,1) ak2,substr(pk3,1,1) ak3,substr(v1,1,1) av1,substr(v2,1,1) av2 from "+tableName+" order by pk2,pk3 limit 10) a "+
                   "where (a.ak1,a.av2) = ('c', substr('abc',1,1)) GROUP BY a.av2,a.ak3 ORDER BY a.ak3",

                   "select a.ak3 "+
                   "from (select substr(pk1,1,1) ak1,substr(pk2,1,1) ak2,substr(pk3,1,1) ak3,substr(v1,1,1) av1,substr(v2,1,1) av2 from "+tableName+" order by pk2,pk3 limit 10) a "+
                   "where a.av1 = 'a' and a.av2 = 'b' GROUP BY length(a.av1)+length(a.av2),a.ak3 ORDER BY a.ak3",

                   "select a.ak3 "+
                   "from (select substr(pk1,1,1) ak1,substr(pk2,1,1) ak2,substr(pk3,1,1) ak3,substr(v1,1,1) av1,substr(v2,1,1) av2 from "+tableName+" order by pk2,pk3 limit 10) a "+
                   "where a.ak1 = 'a' and a.av2 = 'b' GROUP BY length(a.ak1)+length(a.av2),a.ak3 ORDER BY a.ak3",

                   "select a.ak3 "+
                   "from (select substr(pk1,1,1) ak1,substr(pk2,1,1) ak2,substr(pk3,1,1) ak3, coalesce(pk3,'1') ak4, substr(v1,1,1) av1,substr(v2,1,1) av2 from "+tableName+" order by pk2,pk3 limit 10) a "+
                   "where a.av1 = 'a' and a.av2 = substr('abc',2,1) GROUP BY a.ak4,CASE WHEN a.av1 > a.av2 THEN a.av1 ELSE a.av2 END,a.ak3 ORDER BY a.ak4,a.ak3",

                   "select a.ak3 "+
                   "from (select rand() ak1,length(pk2) ak2,length(pk3) ak3,length(v1) av1,length(v2) av2 from "+tableName+" order by pk2,pk3 limit 10) a "+
                   "where a.ak1 = 0.0 and a.av2 = (5+3*2) GROUP BY a.ak3,CASE WHEN a.ak1 > a.av2 THEN a.ak1 ELSE a.av2 END,a.av1 ORDER BY a.ak3,a.av1",

                   "select a.ak3 "+
                   "from (select rand() ak1,length(pk2) ak2,length(pk3) ak3,length(v1) av1,length(v2) av2 from "+tableName+" order by pk2,pk3 limit 10) a "+
                   "where a.ak1 = 0.0 and a.av2 = length(substr('abc',1,1)) GROUP BY a.ak3,CASE WHEN a.ak1 > a.av2 THEN a.ak1 ELSE a.av2 END,a.av1 ORDER BY a.ak3,a.av1",

                   "select a.ak3 "+
                   "from (select rand() ak1,length(pk2) ak2,length(pk3) ak3,length(v1) av1,length(v2) av2 from "+tableName+" order by pk2,pk3 limit 10) a "+
                   "where a.ak1 = 0.0 and a.av2 = length(substr('abc',1,1)) GROUP BY a.ak3,CASE WHEN coalesce(a.ak1,1) > coalesce(a.av2,2) THEN coalesce(a.ak1,1) ELSE coalesce(a.av2,2) END,a.av1 ORDER BY a.ak3,a.av1",

                   //for IS NULL
                   "select a.ak3 "+
                   "from (select rand() ak1,length(pk2) ak2,length(pk3) ak3,length(v1) av1,length(v2) av2 from "+tableName+" order by pk2,pk3 limit 10) a "+
                   "where a.ak1 is null and a.av2 = length(substr('abc',1,1)) GROUP BY a.ak3,CASE WHEN coalesce(a.ak1,1) > coalesce(a.av2,2) THEN coalesce(a.ak1,1) ELSE coalesce(a.av2,2) END,a.av1 ORDER BY a.ak3,a.av1",

                   "select a.ak3 "+
                   "from (select rand() ak1,length(pk2) ak2,length(pk3) ak3,length(v1) av1,length(v2) av2 from "+tableName+" order by pk2,pk3 limit 10) a "+
                   "where a.ak1 = 0.0 and a.av2 is null GROUP BY a.ak3,CASE WHEN coalesce(a.ak1,1) > coalesce(a.av2,2) THEN coalesce(a.ak1,1) ELSE coalesce(a.av2,2) END,a.av1 ORDER BY a.ak3,a.av1",
             };
              int index = 0;
             for (String query : queries) {
                 QueryPlan plan =  TestUtil.getOptimizeQueryPlan(conn, query);
                 assertTrue((index + 1) + ") " + queries[index], plan.getOrderBy()== OrderBy.FWD_ROW_KEY_ORDER_BY);
                 index++;
             }
         }
         finally {
             if(conn != null) {
                 conn.close();
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
    public void testNotOrderPreservingGroupByForNotPkColumns() throws Exception {
        try (Connection conn= DriverManager.getConnection(getUrl())) {
            conn.createStatement().execute("CREATE TABLE test (\n" +
                    "            pk1 varchar,\n" +
                    "            pk2 varchar,\n" +
                    "            pk3 varchar,\n" +
                    "            pk4 varchar,\n" +
                    "            v1 varchar,\n" +
                    "            v2 varchar,\n" +
                    "            CONSTRAINT pk PRIMARY KEY (\n" +
                    "               pk1,\n" +
                    "               pk2,\n" +
                    "               pk3,\n" +
                    "               pk4\n" +
                    "             )\n" +
                    "         )");
             String[] queries = new String[] {
                     "SELECT pk3 FROM test WHERE (pk1 = 'a' and pk2 = 'b') or v1 ='c' GROUP BY pk4,CASE WHEN pk1 > pk2 THEN coalesce(v1,'1') ELSE pk2 END,pk3 ORDER BY pk4,pk3",
                     "SELECT pk3 FROM test WHERE pk1 = 'a' or pk2 = 'b' GROUP BY CASE WHEN pk1 > pk2 THEN v1 WHEN pk1 = pk2 THEN pk1 ELSE pk2 END,pk3 ORDER BY pk3",
                     "SELECT pk3 FROM test WHERE pk1 = 'a' and (pk2 = 'b' or v1 = 'c') GROUP BY CASE WHEN pk1 > pk2 THEN v1 WHEN pk1 = pk2 THEN pk1 ELSE pk2 END,pk3 ORDER BY pk3",
                     "SELECT v2 FROM test GROUP BY v1,v2 ORDER BY v2",
                     "SELECT pk3 FROM test WHERE v1 = 'a' GROUP BY v1,v2,pk3 ORDER BY pk3",
                     "SELECT length(pk3) FROM test WHERE v1 = 'a' GROUP BY RAND()+length(v1),length(v2),length(pk3) ORDER BY length(v2),length(pk3)",
                     "SELECT length(pk3) FROM test WHERE v1 = 'a' and v2 = 'b' GROUP BY CASE WHEN v1 > v2 THEN length(v1) ELSE RAND(1) END,length(pk3) ORDER BY length(pk3)",
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
    public void testNotOrderPreservingGroupByForClientAggregatePlan() throws Exception {
        Connection conn = null;
        try {
            conn = DriverManager.getConnection(getUrl());
            String tableName = "table_test";
            String sql = "create table " + tableName + "( "+
                    " pk1 varchar not null , " +
                    " pk2 varchar not null, " +
                    " pk3 varchar not null," +
                    " v1 varchar, " +
                    " v2 varchar, " +
                    " CONSTRAINT TEST_PK PRIMARY KEY ( "+
                    "pk1,"+
                    "pk2,"+
                    "pk3 ))";
            conn.createStatement().execute(sql);

            String[] queries = new String[] {
                  "select a.ak3 "+
                   "from (select substr(pk1,1,1) ak1,substr(pk2,1,1) ak2,substr(pk3,1,1) ak3,coalesce(pk3,'1') ak4, substr(v1,1,1) av1,substr(v2,1,1) av2 from "+tableName+" order by pk2,pk3 limit 10) a "+
                   "where (a.ak1 = 'a' and a.ak2 = 'b') or a.av1 ='c' GROUP BY a.ak4,CASE WHEN a.ak1 > a.ak2 THEN coalesce(a.av1,'1') ELSE a.ak2 END,a.ak3 ORDER BY a.ak4,a.ak3",

                   "select a.ak3 "+
                   "from (select substr(pk1,1,1) ak1,substr(pk2,1,1) ak2,substr(pk3,1,1) ak3,coalesce(pk3,'1') ak4, substr(v1,1,1) av1,substr(v2,1,1) av2 from "+tableName+" order by pk2,pk3 limit 10) a "+
                   "where a.ak1 = 'a' or a.ak2 = 'b' GROUP BY CASE WHEN a.ak1 > a.ak2 THEN a.av1 WHEN a.ak1 = a.ak2 THEN a.ak1 ELSE a.ak2 END,a.ak3 ORDER BY a.ak3",

                   //for in
                   "select a.ak3 "+
                   "from (select substr(pk1,1,1) ak1,substr(pk2,1,1) ak2,substr(pk3,1,1) ak3,coalesce(pk3,'1') ak4, substr(v1,1,1) av1,substr(v2,1,1) av2 from "+tableName+" order by pk2,pk3 limit 10) a "+
                   "where a.ak1 in ( 'a','b') GROUP BY CASE WHEN a.ak1 > a.ak2 THEN a.av1 WHEN a.ak1 = a.ak2 THEN a.ak1 ELSE a.ak2 END,a.ak3 ORDER BY a.ak3",

                   "select a.ak3 "+
                   "from (select substr(pk1,1,1) ak1,substr(pk2,1,1) ak2,substr(pk3,1,1) ak3,coalesce(pk3,'1') ak4, substr(v1,1,1) av1,substr(v2,1,1) av2 from "+tableName+" order by pk2,pk3 limit 10) a "+
                   "where a.ak1 = 'a' and (a.ak2 = 'b' or a.av1 = 'c') GROUP BY CASE WHEN a.ak1 > a.ak2 THEN a.av1 WHEN a.ak1 = a.ak2 THEN a.ak1 ELSE a.ak2 END,a.ak3 ORDER BY a.ak3",

                   "select a.av2 "+
                   "from (select substr(pk1,1,1) ak1,substr(pk2,1,1) ak2,substr(pk3,1,1) ak3,coalesce(pk3,'1') ak4, substr(v1,1,1) av1,substr(v2,1,1) av2 from "+tableName+" order by pk2,pk3 limit 10) a "+
                   "GROUP BY a.av1,a.av2 ORDER BY a.av2",

                   "select a.ak3 "+
                   "from (select substr(pk1,1,1) ak1,substr(pk2,1,1) ak2,substr(pk3,1,1) ak3,coalesce(pk3,'1') ak4, substr(v1,1,1) av1,substr(v2,1,1) av2 from "+tableName+" order by pk2,pk3 limit 10) a "+
                   "where a.av1 = 'a' GROUP BY a.av1,a.av2,a.ak3 ORDER BY a.ak3",

                   "select length(a.ak3) "+
                   "from (select substr(pk1,1,1) ak1,substr(pk2,1,1) ak2,substr(pk3,1,1) ak3,coalesce(pk3,'1') ak4, substr(v1,1,1) av1,substr(v2,1,1) av2 from "+tableName+" order by pk2,pk3 limit 10) a "+
                   "where a.av1 = 'a' GROUP BY RAND()+length(a.av1),length(a.av2),length(a.ak3) ORDER BY length(a.av2),length(a.ak3)",

                   "select length(a.ak3) "+
                   "from (select substr(pk1,1,1) ak1,substr(pk2,1,1) ak2,substr(pk3,1,1) ak3,coalesce(pk3,'1') ak4, substr(v1,1,1) av1,substr(v2,1,1) av2 from "+tableName+" order by pk2,pk3 limit 10) a "+
                   "where a.av1 = 'a' and a.av2 = 'b' GROUP BY CASE WHEN a.av1 > a.av2 THEN length(a.av1) ELSE RAND(1) END,length(a.ak3) ORDER BY length(a.ak3)",

                   "select a.ak3 "+
                   "from (select rand() ak1,length(pk2) ak2,length(pk3) ak3,length(v1) av1,length(v2) av2 from "+tableName+" order by pk2,pk3 limit 10) a "+
                   "where a.ak1 > 0.0 and a.av2 = (5+3*2) GROUP BY a.ak3,CASE WHEN a.ak1 > a.av2 THEN a.ak1 ELSE a.av2 END,a.av1 ORDER BY a.ak3,a.av1",

                   //for CoerceExpression
                   "select a.ak3 "+
                   "from (select rand() ak1,length(pk2) ak2,length(pk3) ak3,length(v1) av1,length(v2) av2 from "+tableName+" order by pk2,pk3 limit 10) a "+
                   "where CAST(a.ak1 AS INTEGER) = 0 and a.av2 = (5+3*2) GROUP BY a.ak3,a.ak1,a.av1 ORDER BY a.ak3,a.av1",

                   "select a.ak3 "+
                   "from (select rand() ak1,length(pk2) ak2,length(pk3) ak3,length(v1) av1,length(v2) av2 from "+tableName+" order by pk2,pk3 limit 10) a "+
                   "where a.ak1 = 0.0 or a.av2 = length(substr('abc',1,1)) GROUP BY a.ak3,CASE WHEN coalesce(a.ak1,1) > coalesce(a.av2,2) THEN coalesce(a.ak1,1) ELSE coalesce(a.av2,2) END,a.av1 ORDER BY a.ak3,a.av1",

                   //for IS NULL
                   "select a.ak3 "+
                   "from (select rand() ak1,length(pk2) ak2,length(pk3) ak3,length(v1) av1,length(v2) av2 from "+tableName+" order by pk2,pk3 limit 10) a "+
                   "where a.ak1 is not null and a.av2 = length(substr('abc',1,1)) GROUP BY a.ak3,CASE WHEN coalesce(a.ak1,1) > coalesce(a.av2,2) THEN coalesce(a.ak1,1) ELSE coalesce(a.av2,2) END,a.av1 ORDER BY a.ak3,a.av1",

                   "select a.ak3 "+
                   "from (select rand() ak1,length(pk2) ak2,length(pk3) ak3,length(v1) av1,length(v2) av2 from "+tableName+" order by pk2,pk3 limit 10) a "+
                   "where a.ak1 is null or a.av2 = length(substr('abc',1,1)) GROUP BY a.ak3,CASE WHEN coalesce(a.ak1,1) > coalesce(a.av2,2) THEN coalesce(a.ak1,1) ELSE coalesce(a.av2,2) END,a.av1 ORDER BY a.ak3,a.av1",

                   "select a.ak3 "+
                   "from (select rand() ak1,length(pk2) ak2,length(pk3) ak3,length(v1) av1,length(v2) av2 from "+tableName+" order by pk2,pk3 limit 10) a "+
                   "where a.ak1 is null and a.av2 = length(substr('abc',1,1)) and a.ak1 = 0.0 GROUP BY a.ak3,CASE WHEN coalesce(a.ak1,1) > coalesce(a.av2,2) THEN coalesce(a.ak1,1) ELSE coalesce(a.av2,2) END,a.av1 ORDER BY a.ak3,a.av1",

                   "select a.ak3 "+
                   "from (select rand() ak1,length(pk2) ak2,length(pk3) ak3,length(v1) av1,length(v2) av2 from "+tableName+" order by pk2,pk3 limit 10) a "+
                   "where a.ak1 is null and a.av2 = length(substr('abc',1,1)) or a.ak1 = 0.0 GROUP BY a.ak3,CASE WHEN coalesce(a.ak1,1) > coalesce(a.av2,2) THEN coalesce(a.ak1,1) ELSE coalesce(a.av2,2) END,a.av1 ORDER BY a.ak3,a.av1",
             };
            int index = 0;
            for (String query : queries) {
                QueryPlan plan = TestUtil.getOptimizeQueryPlan(conn, query);
                assertTrue((index + 1) + ") " + queries[index], plan.getOrderBy().getOrderByExpressions().size() > 0);
                index++;
            }
        }
        finally {
            if(conn != null) {
                conn.close();
            }
        }
    }

    @Test
    public void testOrderByOptimizeForClientAggregatePlanAndDesc() throws Exception {
        Connection conn = null;
        try {
            conn = DriverManager.getConnection(getUrl());
            String tableName = "test_table";
            String sql = "create table " + tableName + "( "+
                    " pk1 varchar not null, " +
                    " pk2 varchar not null, " +
                    " pk3 varchar not null, " +
                    " v1 varchar, " +
                    " v2 varchar, " +
                    " CONSTRAINT TEST_PK PRIMARY KEY ( "+
                    "pk1 desc,"+
                    "pk2 desc,"+
                    "pk3 desc))";
            conn.createStatement().execute(sql);

            String[] queries = new String[] {
                     "select a.ak3 "+
                     "from (select pk1 ak1,pk2 ak2,pk3 ak3, substr(v1,1,1) av1,substr(v2,1,1) av2 from "+tableName+" order by pk2,pk3 limit 10) a "+
                     "group by a.ak3,a.av1 order by a.ak3 desc,a.av1",

                     "select a.ak3 "+
                     "from (select pk1 ak1,pk2 ak2,pk3 ak3,substr(v1,1,1) av1,substr(v2,1,1) av2 from "+tableName+" order by pk2,pk3 limit 10) a "+
                     "where a.av1 = 'a' group by a.av1,a.ak3 order by a.ak3 desc",

                     "select a.ak3 "+
                     "from (select pk1 ak1,pk2 ak2,pk3 ak3,substr(v1,1,1) av1,substr(v2,1,1) av2 from "+tableName+" order by pk2,pk3 limit 10) a "+
                     "where a.av1 = 'a' and a.av2= 'b' group by CASE WHEN a.av1 > a.av2 THEN a.av1 ELSE a.av2 END,a.ak3,a.ak2 order by a.ak3 desc,a.ak2 desc"
            };

            int index = 0;
            for (String query : queries) {
                QueryPlan plan =  TestUtil.getOptimizeQueryPlan(conn, query);
                assertTrue((index + 1) + ") " + queries[index], plan.getOrderBy()== OrderBy.FWD_ROW_KEY_ORDER_BY);
                index++;
            }

            queries = new String[] {
                     "select a.ak3 "+
                     "from (select pk1 ak1,pk2 ak2,pk3 ak3,substr(v1,1,1) av1,substr(v2,1,1) av2 from "+tableName+" order by pk2,pk3 limit 10) a "+
                     "group by a.ak3,a.av1 order by a.ak3,a.av1",

                     "select a.ak3 "+
                     "from (select pk1 ak1,pk2 ak2,pk3 ak3,substr(v1,1,1) av1,substr(v2,1,1) av2 from "+tableName+" order by pk2,pk3 limit 10) a "+
                     "where a.av1 = 'a' group by a.av1,a.ak3 order by a.ak3",

                     "select a.ak3 "+
                     "from (select pk1 ak1,pk2 ak2,pk3 ak3,substr(v1,1,1) av1,substr(v2,1,1) av2 from "+tableName+" order by pk2,pk3 limit 10) a "+
                     "where a.av1 = 'a' and a.av2= 'b' group by CASE WHEN a.av1 > a.av2 THEN a.av1 ELSE a.av2 END,a.ak3,a.ak2 order by a.ak3,a.ak2",

                     "select a.ak3 "+
                     "from (select pk1 ak1,pk2 ak2,pk3 ak3,substr(v1,1,1) av1,substr(v2,1,1) av2 from "+tableName+" order by pk2,pk3 limit 10) a "+
                     "where a.av1 = 'a' and a.av2= 'b' group by CASE WHEN a.av1 > a.av2 THEN a.av1 ELSE a.av2 END,a.ak3,a.ak2 order by a.ak3 asc,a.ak2 desc"
            };
            index = 0;
            for (String query : queries) {
                QueryPlan plan = TestUtil.getOptimizeQueryPlan(conn, query);
                assertTrue((index + 1) + ") " + queries[index], plan.getOrderBy().getOrderByExpressions().size() > 0);
                index++;
            }
        }
        finally {
            if(conn != null) {
                conn.close();
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

    @Test
    public void testSortMergeJoinSubQueryOrderByOverrideBug3745() throws Exception {
        Connection conn = null;
        try {
            conn= DriverManager.getConnection(getUrl());

            String tableName1="MERGE1";
            String tableName2="MERGE2";

            conn.createStatement().execute("DROP TABLE if exists "+tableName1);

            String sql="CREATE TABLE IF NOT EXISTS "+tableName1+" ( "+
                    "AID INTEGER PRIMARY KEY,"+
                    "AGE INTEGER"+
                    ")";
            conn.createStatement().execute(sql);

            conn.createStatement().execute("DROP TABLE if exists "+tableName2);
            sql="CREATE TABLE IF NOT EXISTS "+tableName2+" ( "+
                    "BID INTEGER PRIMARY KEY,"+
                    "CODE INTEGER"+
                    ")";
            conn.createStatement().execute(sql);

            //test for simple scan
            sql="select /*+ USE_SORT_MERGE_JOIN */ a.aid,b.code from (select aid,age from "+tableName1+" where age >=11 and age<=33 order by age limit 3) a inner join "+
                    "(select bid,code from "+tableName2+" order by code limit 1) b on a.aid=b.bid ";

            QueryPlan queryPlan=getQueryPlan(conn, sql);
            SortMergeJoinPlan sortMergeJoinPlan=(SortMergeJoinPlan)((ClientScanPlan)queryPlan).getDelegate();

            ClientScanPlan lhsOuterPlan=(ClientScanPlan)((TupleProjectionPlan)(sortMergeJoinPlan.getLhsPlan())).getDelegate();
            OrderBy orderBy=lhsOuterPlan.getOrderBy();
            assertTrue(orderBy.getOrderByExpressions().size() == 1);
            assertTrue(orderBy.getOrderByExpressions().get(0).toString().equals("AID"));
            ScanPlan innerScanPlan=(ScanPlan)((TupleProjectionPlan)lhsOuterPlan.getDelegate()).getDelegate();
            orderBy=innerScanPlan.getOrderBy();
            assertTrue(orderBy.getOrderByExpressions().size() == 1);
            assertTrue(orderBy.getOrderByExpressions().get(0).toString().equals("AGE"));
            assertTrue(innerScanPlan.getLimit().intValue() == 3);

            ClientScanPlan rhsOuterPlan=(ClientScanPlan)((TupleProjectionPlan)(sortMergeJoinPlan.getRhsPlan())).getDelegate();
            orderBy=rhsOuterPlan.getOrderBy();
            assertTrue(orderBy.getOrderByExpressions().size() == 1);
            assertTrue(orderBy.getOrderByExpressions().get(0).toString().equals("BID"));
            innerScanPlan=(ScanPlan)((TupleProjectionPlan)rhsOuterPlan.getDelegate()).getDelegate();
            orderBy=innerScanPlan.getOrderBy();
            assertTrue(orderBy.getOrderByExpressions().size() == 1);
            assertTrue(orderBy.getOrderByExpressions().get(0).toString().equals("CODE"));
            assertTrue(innerScanPlan.getLimit().intValue() == 1);

            //test for aggregate
            sql="select /*+ USE_SORT_MERGE_JOIN */ a.aid,b.codesum from (select aid,sum(age) agesum from "+tableName1+" where age >=11 and age<=33 group by aid order by agesum limit 3) a inner join "+
                    "(select bid,sum(code) codesum from "+tableName2+" group by bid order by codesum limit 1) b on a.aid=b.bid ";


            queryPlan=getQueryPlan(conn, sql);
            sortMergeJoinPlan=(SortMergeJoinPlan)((ClientScanPlan)queryPlan).getDelegate();

            lhsOuterPlan=(ClientScanPlan)((TupleProjectionPlan)(sortMergeJoinPlan.getLhsPlan())).getDelegate();
            orderBy=lhsOuterPlan.getOrderBy();
            assertTrue(orderBy.getOrderByExpressions().size() == 1);
            assertTrue(orderBy.getOrderByExpressions().get(0).toString().equals("AID"));
            AggregatePlan innerAggregatePlan=(AggregatePlan)((TupleProjectionPlan)lhsOuterPlan.getDelegate()).getDelegate();
            orderBy=innerAggregatePlan.getOrderBy();
            assertTrue(orderBy.getOrderByExpressions().size() == 1);
            assertTrue(orderBy.getOrderByExpressions().get(0).toString().equals("SUM(AGE)"));
            assertTrue(innerAggregatePlan.getLimit().intValue() == 3);

            rhsOuterPlan=(ClientScanPlan)((TupleProjectionPlan)(sortMergeJoinPlan.getRhsPlan())).getDelegate();
            orderBy=rhsOuterPlan.getOrderBy();
            assertTrue(orderBy.getOrderByExpressions().size() == 1);
            assertTrue(orderBy.getOrderByExpressions().get(0).toString().equals("BID"));
            innerAggregatePlan=(AggregatePlan)((TupleProjectionPlan)rhsOuterPlan.getDelegate()).getDelegate();
            orderBy=innerAggregatePlan.getOrderBy();
            assertTrue(orderBy.getOrderByExpressions().size() == 1);
            assertTrue(orderBy.getOrderByExpressions().get(0).toString().equals("SUM(CODE)"));
            assertTrue(innerAggregatePlan.getLimit().intValue() == 1);

            String tableName3="merge3";
            conn.createStatement().execute("DROP TABLE if exists "+tableName3);
            sql="CREATE TABLE IF NOT EXISTS "+tableName3+" ( "+
                    "CID INTEGER PRIMARY KEY,"+
                    "REGION INTEGER"+
                    ")";
            conn.createStatement().execute(sql);

            //test for join
            sql="select t1.aid,t1.code,t2.region from "+
                "(select a.aid,b.code from "+tableName1+" a inner join "+tableName2+" b on a.aid=b.bid where b.code >=44 and b.code<=66 order by b.code limit 3) t1 inner join "+
                "(select a.aid,c.region from "+tableName1+" a inner join "+tableName3+" c on a.aid=c.cid where c.region>=77 and c.region<=99 order by c.region desc limit 1) t2 on t1.aid=t2.aid";

            PhoenixPreparedStatement phoenixPreparedStatement = conn.prepareStatement(sql).unwrap(PhoenixPreparedStatement.class);
            queryPlan = phoenixPreparedStatement.optimizeQuery(sql);
            sortMergeJoinPlan=(SortMergeJoinPlan)((ClientScanPlan)queryPlan).getDelegate();

            lhsOuterPlan=(ClientScanPlan)((TupleProjectionPlan)(sortMergeJoinPlan.getLhsPlan())).getDelegate();
            orderBy=lhsOuterPlan.getOrderBy();
            assertTrue(orderBy.getOrderByExpressions().size() == 1);
            assertTrue(orderBy.getOrderByExpressions().get(0).toString().equals("AID"));
            innerScanPlan=(ScanPlan)((HashJoinPlan)((TupleProjectionPlan)lhsOuterPlan.getDelegate()).getDelegate()).getDelegate();
            orderBy=innerScanPlan.getOrderBy();
            assertTrue(orderBy.getOrderByExpressions().size() == 1);
            assertTrue(orderBy.getOrderByExpressions().get(0).toString().equals("B.CODE"));
            assertTrue(innerScanPlan.getLimit().intValue() == 3);

            rhsOuterPlan=(ClientScanPlan)((TupleProjectionPlan)(sortMergeJoinPlan.getRhsPlan())).getDelegate();
            orderBy=rhsOuterPlan.getOrderBy();
            assertTrue(orderBy.getOrderByExpressions().size() == 1);
            assertTrue(orderBy.getOrderByExpressions().get(0).toString().equals("AID"));
            innerScanPlan=(ScanPlan)((HashJoinPlan)((TupleProjectionPlan)rhsOuterPlan.getDelegate()).getDelegate()).getDelegate();
            orderBy=innerScanPlan.getOrderBy();
            assertTrue(orderBy.getOrderByExpressions().size() == 1);
            assertTrue(orderBy.getOrderByExpressions().get(0).toString().equals("C.REGION DESC"));
            assertTrue(innerScanPlan.getLimit().intValue() == 1);

            //test for join and aggregate
            sql="select t1.aid,t1.codesum,t2.regionsum from "+
                "(select a.aid,sum(b.code) codesum from "+tableName1+" a inner join "+tableName2+" b on a.aid=b.bid where b.code >=44 and b.code<=66 group by a.aid order by codesum limit 3) t1 inner join "+
                "(select a.aid,sum(c.region) regionsum from "+tableName1+" a inner join "+tableName3+" c on a.aid=c.cid where c.region>=77 and c.region<=99 group by a.aid order by regionsum desc limit 2) t2 on t1.aid=t2.aid";

            phoenixPreparedStatement = conn.prepareStatement(sql).unwrap(PhoenixPreparedStatement.class);
            queryPlan = phoenixPreparedStatement.optimizeQuery(sql);
            sortMergeJoinPlan=(SortMergeJoinPlan)((ClientScanPlan)queryPlan).getDelegate();

            lhsOuterPlan=(ClientScanPlan)((TupleProjectionPlan)(sortMergeJoinPlan.getLhsPlan())).getDelegate();
            orderBy=lhsOuterPlan.getOrderBy();
            assertTrue(orderBy.getOrderByExpressions().size() == 1);
            assertTrue(orderBy.getOrderByExpressions().get(0).toString().equals("AID"));
            innerAggregatePlan=(AggregatePlan)((HashJoinPlan)((TupleProjectionPlan)lhsOuterPlan.getDelegate()).getDelegate()).getDelegate();
            orderBy=innerAggregatePlan.getOrderBy();
            assertTrue(orderBy.getOrderByExpressions().size() == 1);
            assertTrue(orderBy.getOrderByExpressions().get(0).toString().equals("SUM(B.CODE)"));
            assertTrue(innerAggregatePlan.getLimit().intValue() == 3);

            rhsOuterPlan=(ClientScanPlan)((TupleProjectionPlan)(sortMergeJoinPlan.getRhsPlan())).getDelegate();
            orderBy=rhsOuterPlan.getOrderBy();
            assertTrue(orderBy.getOrderByExpressions().size() == 1);
            assertTrue(orderBy.getOrderByExpressions().get(0).toString().equals("AID"));
            innerAggregatePlan=(AggregatePlan)((HashJoinPlan)((TupleProjectionPlan)rhsOuterPlan.getDelegate()).getDelegate()).getDelegate();
            orderBy=innerAggregatePlan.getOrderBy();
            assertTrue(orderBy.getOrderByExpressions().size() == 1);
            assertTrue(orderBy.getOrderByExpressions().get(0).toString().equals("SUM(C.REGION) DESC"));
            assertTrue(innerAggregatePlan.getLimit().intValue() == 2);

            //test for if SubselectRewriter.isOrderByPrefix had take effect
            sql="select t1.aid,t1.codesum,t2.regionsum from "+
                "(select a.aid,sum(b.code) codesum from "+tableName1+" a inner join "+tableName2+" b on a.aid=b.bid where b.code >=44 and b.code<=66 group by a.aid order by a.aid,codesum limit 3) t1 inner join "+
                "(select a.aid,sum(c.region) regionsum from "+tableName1+" a inner join "+tableName3+" c on a.aid=c.cid where c.region>=77 and c.region<=99 group by a.aid order by a.aid desc,regionsum desc limit 2) t2 on t1.aid=t2.aid "+
                 "order by t1.aid desc";

            phoenixPreparedStatement = conn.prepareStatement(sql).unwrap(PhoenixPreparedStatement.class);
            queryPlan = phoenixPreparedStatement.optimizeQuery(sql);
            orderBy=queryPlan.getOrderBy();
            assertTrue(orderBy.getOrderByExpressions().size() == 1);
            assertTrue(orderBy.getOrderByExpressions().get(0).toString().equals("T1.AID DESC"));
            sortMergeJoinPlan=(SortMergeJoinPlan)((ClientScanPlan)queryPlan).getDelegate();

            innerAggregatePlan=(AggregatePlan)((HashJoinPlan)(((TupleProjectionPlan)sortMergeJoinPlan.getLhsPlan()).getDelegate())).getDelegate();
            orderBy=innerAggregatePlan.getOrderBy();
            assertTrue(orderBy.getOrderByExpressions().size() == 2);
            assertTrue(orderBy.getOrderByExpressions().get(0).toString().equals("A.AID"));
            assertTrue(orderBy.getOrderByExpressions().get(1).toString().equals("SUM(B.CODE)"));
            assertTrue(innerAggregatePlan.getLimit().intValue() == 3);

            rhsOuterPlan=(ClientScanPlan)((TupleProjectionPlan)(sortMergeJoinPlan.getRhsPlan())).getDelegate();
            orderBy=rhsOuterPlan.getOrderBy();
            assertTrue(orderBy.getOrderByExpressions().size() == 1);
            assertTrue(orderBy.getOrderByExpressions().get(0).toString().equals("AID"));
            innerAggregatePlan=(AggregatePlan)((HashJoinPlan)((TupleProjectionPlan)rhsOuterPlan.getDelegate()).getDelegate()).getDelegate();
            orderBy=innerAggregatePlan.getOrderBy();
            assertTrue(orderBy.getOrderByExpressions().size() == 2);
            assertTrue(orderBy.getOrderByExpressions().get(0).toString().equals("A.AID DESC"));
            assertTrue(orderBy.getOrderByExpressions().get(1).toString().equals("SUM(C.REGION) DESC"));
            assertTrue(innerAggregatePlan.getLimit().intValue() == 2);
        } finally {
            if(conn!=null) {
                conn.close();
            }
        }
    }

    @Test
    public void testUnionDifferentColumnNumber() throws Exception {
        Connection conn = DriverManager.getConnection(getUrl());
        Statement statement = conn.createStatement();
        try {
            String create = "CREATE TABLE s.t1 (k integer not null primary key, f1.v1 varchar, f1.v2 varchar, " +
                    "f2.v3 varchar, v4 varchar)";
            statement.execute(create);
            create = "CREATE TABLE s.t2 (k integer not null primary key, f1.v1 varchar, f1.v2 varchar, f2.v3 varchar)";
            statement.execute(create);
            String query = "SELECT *  FROM s.t1 UNION ALL select * FROM s.t2";
            statement.executeQuery(query);
            fail("Should fail with different column numbers ");
        } catch (SQLException e) {
            assertEquals(e.getMessage(), "ERROR 525 (42902): SELECT column number differs in a Union All query " +
                    "is not allowed. 1st query has 5 columns whereas 2nd query has 4");
        } finally {
            statement.execute("DROP TABLE IF EXISTS s.t1");
            statement.execute("DROP TABLE IF EXISTS s.t2");
            conn.close();
        }
    }

    @Test
    public void testUnionDifferentColumnType() throws Exception {
        Connection conn = DriverManager.getConnection(getUrl());
        Statement statement = conn.createStatement();
        try {
            String create = "CREATE TABLE s.t1 (k integer not null primary key, f1.v1 varchar, f1.v2 varchar, " +
                    "f2.v3 varchar, v4 varchar)";
            statement.execute(create);
            create = "CREATE TABLE s.t2 (k integer not null primary key, f1.v1 varchar, f1.v2 integer, " +
                    "f2.v3 varchar, f2.v4 varchar)";
            statement.execute(create);
            String query = "SELECT *  FROM s.t1 UNION ALL select * FROM s.t2";
            statement.executeQuery(query);
            fail("Should fail with different column types ");
        } catch (SQLException e) {
            assertEquals(e.getMessage(), "ERROR 526 (42903): SELECT column types differ in a Union All query " +
                    "is not allowed. Column # 2 is VARCHAR in 1st query where as it is INTEGER in 2nd query");
        } finally {
            statement.execute("DROP TABLE IF EXISTS s.t1");
            statement.execute("DROP TABLE IF EXISTS s.t2");
            conn.close();
        }
    }
    
    @Test
    public void testCannotCreateStatementOnClosedConnection() throws Exception {
        Connection conn = DriverManager.getConnection(getUrl());
        conn.close();
        try {
            conn.createStatement();
            fail();
        } catch (SQLException e) {
            assertEquals(e.getErrorCode(), SQLExceptionCode.CONNECTION_CLOSED.getErrorCode());
        }
        try {
            conn.prepareStatement("SELECT * FROM SYSTEM.CATALOG");
            fail();
        } catch (SQLException e) {
            assertEquals(e.getErrorCode(), SQLExceptionCode.CONNECTION_CLOSED.getErrorCode());
        }
    }

    @Test
    public void testSingleColLocalIndexPruning() throws SQLException {
        Properties props = PropertiesUtil.deepCopy(TEST_PROPERTIES);
        try (Connection conn = DriverManager.getConnection(getUrl(), props)) {
            conn.createStatement().execute("CREATE TABLE T (\n" + 
                    "    A CHAR(1) NOT NULL,\n" + 
                    "    B CHAR(1) NOT NULL,\n" + 
                    "    C CHAR(1) NOT NULL,\n" + 
                    "    CONSTRAINT PK PRIMARY KEY (\n" + 
                    "        A,\n" + 
                    "        B,\n" + 
                    "        C\n" + 
                    "    )\n" + 
                    ") SPLIT ON ('A','C','E','G','I')");
            conn.createStatement().execute("CREATE LOCAL INDEX IDX ON T(A,C)");
            String query = "SELECT * FROM T WHERE A = 'B' and C='C'";
            PhoenixStatement statement = conn.createStatement().unwrap(PhoenixStatement.class);
            QueryPlan plan = statement.optimizeQuery(query);
            assertEquals("IDX", plan.getContext().getCurrentTable().getTable().getName().getString());
            plan.iterator();
            List<List<Scan>> outerScans = plan.getScans();
            assertEquals(1, outerScans.size());
            List<Scan> innerScans = outerScans.get(0);
            assertEquals(1, innerScans.size());
            Scan scan = innerScans.get(0);
            assertEquals("A", Bytes.toString(scan.getStartRow()).trim());
            assertEquals("C", Bytes.toString(scan.getStopRow()).trim());
        }
    }

    @Test
    public void testMultiColLocalIndexPruning() throws SQLException {
        Properties props = PropertiesUtil.deepCopy(TEST_PROPERTIES);
        try (Connection conn = DriverManager.getConnection(getUrl(), props)) {
            conn.createStatement().execute("CREATE TABLE T (\n" + 
                    "    A CHAR(1) NOT NULL,\n" + 
                    "    B CHAR(1) NOT NULL,\n" + 
                    "    C CHAR(1) NOT NULL,\n" + 
                    "    D CHAR(1) NOT NULL,\n" + 
                    "    CONSTRAINT PK PRIMARY KEY (\n" + 
                    "        A,\n" + 
                    "        B,\n" + 
                    "        C,\n" + 
                    "        D\n" + 
                    "    )\n" + 
                    ") SPLIT ON ('A','C','E','G','I')");
            conn.createStatement().execute("CREATE LOCAL INDEX IDX ON T(A,B,D)");
            String query = "SELECT * FROM T WHERE A = 'C' and B = 'X' and D='C'";
            PhoenixStatement statement = conn.createStatement().unwrap(PhoenixStatement.class);
            QueryPlan plan = statement.optimizeQuery(query);
            assertEquals("IDX", plan.getContext().getCurrentTable().getTable().getName().getString());
            plan.iterator();
            List<List<Scan>> outerScans = plan.getScans();
            assertEquals(1, outerScans.size());
            List<Scan> innerScans = outerScans.get(0);
            assertEquals(1, innerScans.size());
            Scan scan = innerScans.get(0);
            assertEquals("C", Bytes.toString(scan.getStartRow()).trim());
            assertEquals("E", Bytes.toString(scan.getStopRow()).trim());
        }
    }

    @Test
    public void testSkipScanLocalIndexPruning() throws SQLException {
        Properties props = PropertiesUtil.deepCopy(TEST_PROPERTIES);
        try (Connection conn = DriverManager.getConnection(getUrl(), props)) {
            conn.createStatement().execute("CREATE TABLE T (\n" + 
                    "    A CHAR(1) NOT NULL,\n" + 
                    "    B CHAR(1) NOT NULL,\n" + 
                    "    C CHAR(1) NOT NULL,\n" + 
                    "    D CHAR(1) NOT NULL,\n" + 
                    "    CONSTRAINT PK PRIMARY KEY (\n" + 
                    "        A,\n" + 
                    "        B,\n" + 
                    "        C,\n" + 
                    "        D\n" + 
                    "    )\n" + 
                    ") SPLIT ON ('A','C','E','G','I')");
            conn.createStatement().execute("CREATE LOCAL INDEX IDX ON T(A,B,D)");
            String query = "SELECT * FROM T WHERE A IN ('A','G') and B = 'A' and D = 'D'";
            PhoenixStatement statement = conn.createStatement().unwrap(PhoenixStatement.class);
            QueryPlan plan = statement.optimizeQuery(query);
            assertEquals("IDX", plan.getContext().getCurrentTable().getTable().getName().getString());
            plan.iterator();
            List<List<Scan>> outerScans = plan.getScans();
            assertEquals(2, outerScans.size());
            List<Scan> innerScans1 = outerScans.get(0);
            assertEquals(1, innerScans1.size());
            Scan scan1 = innerScans1.get(0);
            assertEquals("A", Bytes.toString(scan1.getStartRow()).trim());
            assertEquals("C", Bytes.toString(scan1.getStopRow()).trim());
            List<Scan> innerScans2 = outerScans.get(1);
            assertEquals(1, innerScans2.size());
            Scan scan2 = innerScans2.get(0);
            assertEquals("G", Bytes.toString(scan2.getStartRow()).trim());
            assertEquals("I", Bytes.toString(scan2.getStopRow()).trim());
        }
    }

    @Test
    public void testRVCLocalIndexPruning() throws SQLException {
        Properties props = PropertiesUtil.deepCopy(TEST_PROPERTIES);
        try (Connection conn = DriverManager.getConnection(getUrl(), props)) {
            conn.createStatement().execute("CREATE TABLE T (\n" + 
                    "    A CHAR(1) NOT NULL,\n" + 
                    "    B CHAR(1) NOT NULL,\n" + 
                    "    C CHAR(1) NOT NULL,\n" + 
                    "    D CHAR(1) NOT NULL,\n" + 
                    "    CONSTRAINT PK PRIMARY KEY (\n" + 
                    "        A,\n" + 
                    "        B,\n" + 
                    "        C,\n" + 
                    "        D\n" + 
                    "    )\n" + 
                    ") SPLIT ON ('A','C','E','G','I')");
            conn.createStatement().execute("CREATE LOCAL INDEX IDX ON T(A,B,D)");
            String query = "SELECT * FROM T WHERE A='I' and (B,D) IN (('A','D'),('B','I'))";
            PhoenixStatement statement = conn.createStatement().unwrap(PhoenixStatement.class);
            QueryPlan plan = statement.optimizeQuery(query);
            assertEquals("IDX", plan.getContext().getCurrentTable().getTable().getName().getString());
            plan.iterator();
            List<List<Scan>> outerScans = plan.getScans();
            assertEquals(1, outerScans.size());
            List<Scan> innerScans = outerScans.get(0);
            assertEquals(1, innerScans.size());
            Scan scan = innerScans.get(0);
            assertEquals("I", Bytes.toString(scan.getStartRow()).trim());
            assertEquals(0, scan.getStopRow().length);
        }
    }

    @Test
    public void testRVCLocalIndexPruning2() throws SQLException {
        Properties props = PropertiesUtil.deepCopy(TEST_PROPERTIES);
        try (Connection conn = DriverManager.getConnection(getUrl(), props)) {
            conn.createStatement().execute("CREATE TABLE T (\n" + 
                    "    A CHAR(1) NOT NULL,\n" + 
                    "    B VARCHAR,\n" + 
                    "    C VARCHAR,\n" + 
                    "    D VARCHAR,\n" + 
                    "    E VARCHAR,\n" + 
                    "    F VARCHAR,\n" + 
                    "    G VARCHAR,\n" + 
                    "    CONSTRAINT PK PRIMARY KEY (\n" + 
                    "        A,\n" + 
                    "        B,\n" + 
                    "        C,\n" + 
                    "        D,\n" + 
                    "        E,\n" + 
                    "        F,\n" + 
                    "        G\n" + 
                    "    )\n" + 
                    ") SPLIT ON ('A','C','E','G','I')");
            conn.createStatement().execute("CREATE LOCAL INDEX IDX ON T(A,B,C,F,G)");
            String query = "SELECT * FROM T WHERE (A,B,C,D) IN (('I','D','F','X'),('I','I','G','Y')) and F='X' and G='Y'";
            PhoenixStatement statement = conn.createStatement().unwrap(PhoenixStatement.class);
            QueryPlan plan = statement.optimizeQuery(query);
            assertEquals("IDX", plan.getContext().getCurrentTable().getTable().getName().getString());
            plan.iterator();
            List<List<Scan>> outerScans = plan.getScans();
            assertEquals(1, outerScans.size());
            List<Scan> innerScans = outerScans.get(0);
            assertEquals(1, innerScans.size());
            Scan scan = innerScans.get(0);
            assertEquals("I", Bytes.toString(scan.getStartRow()).trim());
            assertEquals(0, scan.getStopRow().length);
        }
    }

    @Test
    public void testMinMaxRangeLocalIndexPruning() throws SQLException {
        Properties props = PropertiesUtil.deepCopy(TEST_PROPERTIES);
        try (Connection conn = DriverManager.getConnection(getUrl(), props)) {
            conn.createStatement().execute("CREATE TABLE T (\n" + 
                    "    A CHAR(1) NOT NULL,\n" + 
                    "    B CHAR(1) NOT NULL,\n" + 
                    "    C CHAR(1) NOT NULL,\n" + 
                    "    D CHAR(1) NOT NULL,\n" + 
                    "    CONSTRAINT PK PRIMARY KEY (\n" + 
                    "        A,\n" + 
                    "        B,\n" + 
                    "        C,\n" + 
                    "        D\n" + 
                    "    )\n" + 
                    ") SPLIT ON ('A','C','E','G','I')");
            conn.createStatement().execute("CREATE LOCAL INDEX IDX ON T(A,B,D)");
            String query = "SELECT * FROM T WHERE A = 'C' and (A,B,D) > ('C','B','X') and B < 'Z' and D='C'";
            PhoenixStatement statement = conn.createStatement().unwrap(PhoenixStatement.class);
            QueryPlan plan = statement.optimizeQuery(query);
            assertEquals("IDX", plan.getContext().getCurrentTable().getTable().getName().getString());
            plan.iterator();
            List<List<Scan>> outerScans = plan.getScans();
            assertEquals(1, outerScans.size());
            List<Scan> innerScans = outerScans.get(0);
            assertEquals(1, innerScans.size());
            Scan scan = innerScans.get(0);
            assertEquals("C", Bytes.toString(scan.getStartRow()).trim());
            assertEquals("E", Bytes.toString(scan.getStopRow()).trim());
        }
    }

    @Test
    public void testNoLocalIndexPruning() throws SQLException {
        Properties props = PropertiesUtil.deepCopy(TEST_PROPERTIES);
        try (Connection conn = DriverManager.getConnection(getUrl(), props)) {
            conn.createStatement().execute("CREATE TABLE T (\n" + 
                    "    A CHAR(1) NOT NULL,\n" + 
                    "    B CHAR(1) NOT NULL,\n" + 
                    "    C CHAR(1) NOT NULL,\n" + 
                    "    CONSTRAINT PK PRIMARY KEY (\n" + 
                    "        A,\n" + 
                    "        B,\n" + 
                    "        C\n" + 
                    "    )\n" + 
                    ") SPLIT ON ('A','C','E','G','I')");
            conn.createStatement().execute("CREATE LOCAL INDEX IDX ON T(C)");
            String query = "SELECT * FROM T WHERE C='C'";
            PhoenixStatement statement = conn.createStatement().unwrap(PhoenixStatement.class);
            QueryPlan plan = statement.optimizeQuery(query);
            assertEquals("IDX", plan.getContext().getCurrentTable().getTable().getName().getString());
            plan.iterator();
            List<List<Scan>> outerScans = plan.getScans();
            assertEquals(6, outerScans.size());
        }
    }

    @Test
    public void testLocalIndexRegionPruning() throws SQLException {
        Properties props = PropertiesUtil.deepCopy(TEST_PROPERTIES);
        try (Connection conn = DriverManager.getConnection(getUrl(), props)) {
            conn.createStatement().execute("CREATE TABLE T (\n" + 
                    "    A CHAR(1) NOT NULL,\n" + 
                    "    B CHAR(1) NOT NULL,\n" + 
                    "    C CHAR(1) NOT NULL,\n" + 
                    "    D CHAR(1),\n" + 
                    "    CONSTRAINT PK PRIMARY KEY (\n" + 
                    "        A,\n" + 
                    "        B,\n" + 
                    "        C\n" + 
                    "    )\n" + 
                    ") SPLIT ON ('A','C','E','G','I')");

            conn.createStatement().execute("CREATE LOCAL INDEX IDX ON T(D)");

            // un-pruned, need to scan all six regions
            String query = "SELECT * FROM T WHERE D = 'C'";
            PhoenixStatement statement = conn.createStatement().unwrap(PhoenixStatement.class);
            QueryPlan plan = statement.optimizeQuery(query);
            assertEquals("IDX", plan.getContext().getCurrentTable().getTable().getName().getString());
            plan.iterator();
            assertEquals(6, plan.getScans().size());

            // fixing first part of the key, can limit scanning to two regions
            query = "SELECT * FROM T WHERE A = 'A' AND D = 'C'";
            statement = conn.createStatement().unwrap(PhoenixStatement.class);
            plan = statement.optimizeQuery(query);
            assertEquals("IDX", plan.getContext().getCurrentTable().getTable().getName().getString());
            plan.iterator();
            assertEquals(2, plan.getScans().size());

            // same with skipscan filter
            query = "SELECT * FROM T WHERE A IN ('A', 'C') AND D = 'C'";
            statement = conn.createStatement().unwrap(PhoenixStatement.class);
            plan = statement.optimizeQuery(query);
            assertEquals("IDX", plan.getContext().getCurrentTable().getTable().getName().getString());
            plan.iterator();
            assertEquals(3, plan.getScans().size());

            // two parts of key fixed, need to scan a single region only
            query = "SELECT * FROM T WHERE A = 'A' AND B = 'A' AND D = 'C'";
            statement = conn.createStatement().unwrap(PhoenixStatement.class);
            plan = statement.optimizeQuery(query);
            assertEquals("IDX", plan.getContext().getCurrentTable().getTable().getName().getString());
            plan.iterator();
            assertEquals(1, plan.getScans().size());

            // same with skipscan filter
            query = "SELECT * FROM T WHERE A IN ('A', 'C') AND B = 'A' AND D = 'C'";
            statement = conn.createStatement().unwrap(PhoenixStatement.class);
            plan = statement.optimizeQuery(query);
            assertEquals("IDX", plan.getContext().getCurrentTable().getTable().getName().getString());
            plan.iterator();
            assertEquals(2, plan.getScans().size());
        }
    }

    @Test
    public void testSmallScanForPointLookups() throws SQLException {
        Properties props = PropertiesUtil.deepCopy(new Properties());
        createTestTable(getUrl(), "CREATE TABLE FOO(\n" +
                      "                a VARCHAR NOT NULL,\n" +
                      "                b VARCHAR NOT NULL,\n" +
                      "                c VARCHAR,\n" +
                      "                CONSTRAINT pk PRIMARY KEY (a, b DESC, c)\n" +
                      "              )");

        props.put(QueryServices.SMALL_SCAN_THRESHOLD_ATTRIB, "2");
        try (Connection conn = DriverManager.getConnection(getUrl(), props)) {
            String query = "select * from foo where a = 'a' and b = 'b' and c in ('x','y','z')";
            PhoenixStatement stmt = conn.createStatement().unwrap(PhoenixStatement.class);
            QueryPlan plan = stmt.optimizeQuery(query);
            plan.iterator();
            //Fail since we have 3 rows in pointLookup
            assertFalse(plan.getContext().getScan().isSmall());
            query = "select * from foo where a = 'a' and b = 'b' and c = 'c'";
            plan = stmt.compileQuery(query);
            plan.iterator();
            //Should be small scan, query is for single row pointLookup
            assertTrue(plan.getContext().getScan().isSmall());
        }
    }

    @Test
    public void testLocalIndexPruningInSortMergeJoin() throws SQLException {
        verifyLocalIndexPruningWithMultipleTables("SELECT /*+ USE_SORT_MERGE_JOIN*/ *\n" +
                "FROM T1 JOIN T2 ON T1.A = T2.A\n" +
                "WHERE T1.A = 'B' and T1.C='C' and T2.A IN ('A','G') and T2.B = 'A' and T2.D = 'D'");
    }

    @Ignore("Blocked by PHOENIX-4614")
    @Test
    public void testLocalIndexPruningInLeftOrInnerHashJoin() throws SQLException {
        verifyLocalIndexPruningWithMultipleTables("SELECT *\n" +
                "FROM T1 JOIN T2 ON T1.A = T2.A\n" +
                "WHERE T1.A = 'B' and T1.C='C' and T2.A IN ('A','G') and T2.B = 'A' and T2.D = 'D'");
    }

    @Ignore("Blocked by PHOENIX-4614")
    @Test
    public void testLocalIndexPruningInRightHashJoin() throws SQLException {
        verifyLocalIndexPruningWithMultipleTables("SELECT *\n" +
                "FROM (\n" +
                "    SELECT A, B, C, D FROM T2 WHERE T2.A IN ('A','G') and T2.B = 'A' and T2.D = 'D'\n" +
                ") T2\n" +
                "RIGHT JOIN T1 ON T2.A = T1.A\n" +
                "WHERE T1.A = 'B' and T1.C='C'");
    }

    @Test
    public void testLocalIndexPruningInUinon() throws SQLException {
        verifyLocalIndexPruningWithMultipleTables("SELECT A, B, C FROM T1\n" +
                "WHERE A = 'B' and C='C'\n" +
                "UNION ALL\n" +
                "SELECT A, B, C FROM T2\n" +
                "WHERE A IN ('A','G') and B = 'A' and D = 'D'");
    }

    private void verifyLocalIndexPruningWithMultipleTables(String query) throws SQLException {
        Properties props = PropertiesUtil.deepCopy(TEST_PROPERTIES);
        try (Connection conn = DriverManager.getConnection(getUrl(), props)) {
            conn.createStatement().execute("CREATE TABLE T1 (\n" +
                    "    A CHAR(1) NOT NULL,\n" +
                    "    B CHAR(1) NOT NULL,\n" +
                    "    C CHAR(1) NOT NULL,\n" +
                    "    CONSTRAINT PK PRIMARY KEY (\n" +
                    "        A,\n" +
                    "        B,\n" +
                    "        C\n" +
                    "    )\n" +
                    ") SPLIT ON ('A','C','E','G','I')");
            conn.createStatement().execute("CREATE LOCAL INDEX IDX1 ON T1(A,C)");
            conn.createStatement().execute("CREATE TABLE T2 (\n" +
                    "    A CHAR(1) NOT NULL,\n" +
                    "    B CHAR(1) NOT NULL,\n" +
                    "    C CHAR(1) NOT NULL,\n" +
                    "    D CHAR(1) NOT NULL,\n" +
                    "    CONSTRAINT PK PRIMARY KEY (\n" +
                    "        A,\n" +
                    "        B,\n" +
                    "        C,\n" +
                    "        D\n" +
                    "    )\n" +
                    ") SPLIT ON ('A','C','E','G','I')");
            conn.createStatement().execute("CREATE LOCAL INDEX IDX2 ON T2(A,B,D)");
            PhoenixStatement statement = conn.createStatement().unwrap(PhoenixStatement.class);
            QueryPlan plan = statement.optimizeQuery(query);
            List<QueryPlan> childPlans = plan.accept(new MultipleChildrenExtractor());
            assertEquals(2, childPlans.size());
            // Check left child
            assertEquals("IDX1", childPlans.get(0).getContext().getCurrentTable().getTable().getName().getString());
            childPlans.get(0).iterator();
            List<List<Scan>> outerScansL = childPlans.get(0).getScans();
            assertEquals(1, outerScansL.size());
            List<Scan> innerScansL = outerScansL.get(0);
            assertEquals(1, innerScansL.size());
            Scan scanL = innerScansL.get(0);
            assertEquals("A", Bytes.toString(scanL.getStartRow()).trim());
            assertEquals("C", Bytes.toString(scanL.getStopRow()).trim());
            // Check right child
            assertEquals("IDX2", childPlans.get(1).getContext().getCurrentTable().getTable().getName().getString());
            childPlans.get(1).iterator();
            List<List<Scan>> outerScansR = childPlans.get(1).getScans();
            assertEquals(2, outerScansR.size());
            List<Scan> innerScansR1 = outerScansR.get(0);
            assertEquals(1, innerScansR1.size());
            Scan scanR1 = innerScansR1.get(0);
            assertEquals("A", Bytes.toString(scanR1.getStartRow()).trim());
            assertEquals("C", Bytes.toString(scanR1.getStopRow()).trim());
            List<Scan> innerScansR2 = outerScansR.get(1);
            assertEquals(1, innerScansR2.size());
            Scan scanR2 = innerScansR2.get(0);
            assertEquals("G", Bytes.toString(scanR2.getStartRow()).trim());
            assertEquals("I", Bytes.toString(scanR2.getStopRow()).trim());
        }
    }

    @Test
    public void testQueryPlanSourceRefsInHashJoin() throws SQLException {
        String query = "SELECT * FROM (\n" +
                "    SELECT K1, V1 FROM A WHERE V1 = 'A'\n" +
                ") T1 JOIN (\n" +
                "    SELECT K2, V2 FROM B WHERE V2 = 'B'\n" +
                ") T2 ON K1 = K2 ORDER BY V1";
        verifyQueryPlanSourceRefs(query, 2);
    }

    @Test
    public void testQueryPlanSourceRefsInSortMergeJoin() throws SQLException {
        String query = "SELECT * FROM (\n" +
                "    SELECT max(K1) KEY1, V1 FROM A GROUP BY V1\n" +
                ") T1 JOIN (\n" +
                "    SELECT max(K2) KEY2, V2 FROM B GROUP BY V2\n" +
                ") T2 ON KEY1 = KEY2 ORDER BY V1";
        verifyQueryPlanSourceRefs(query, 2);
    }

    @Test
    public void testQueryPlanSourceRefsInSubquery() throws SQLException {
        String query = "SELECT * FROM A\n" +
                "WHERE K1 > (\n" +
                "    SELECT max(K2) FROM B WHERE V2 = V1\n" +
                ") ORDER BY V1";
        verifyQueryPlanSourceRefs(query, 2);
    }

    @Test
    public void testQueryPlanSourceRefsInSubquery2() throws SQLException {
        String query = "SELECT * FROM A\n" +
                "WHERE V1 > ANY (\n" +
                "    SELECT K2 FROM B WHERE V2 = 'B'\n" +
                ")";
        verifyQueryPlanSourceRefs(query, 2);
    }

    @Test
    public void testQueryPlanSourceRefsInSubquery3() throws SQLException {
        String query = "SELECT * FROM A\n" +
                "WHERE V1 > ANY (\n" +
                "    SELECT K2 FROM B B1" +
                "    WHERE V2 = (\n" +
                "        SELECT max(V2) FROM B B2\n" +
                "        WHERE B2.K2 = B1.K2 AND V2 < 'K'\n" +
                "    )\n" +
                ")";
        verifyQueryPlanSourceRefs(query, 3);
    }

    @Test
    public void testQueryPlanSourceRefsInSubquery4() throws SQLException {
        String query = "SELECT * FROM (\n" +
                "    SELECT K1, K2 FROM A\n" +
                "    JOIN B ON K1 = K2\n" +
                "    WHERE V1 = 'A' AND V2 = 'B'\n" +
                "    LIMIT 10\n" +
                ") ORDER BY K1";
        verifyQueryPlanSourceRefs(query, 2);
    }

    @Test
    public void testQueryPlanSourceRefsInSubquery5() throws SQLException {
        String query = "SELECT * FROM (\n" +
                "    SELECT KEY1, KEY2 FROM (\n" +
                "        SELECT max(K1) KEY1, V1 FROM A GROUP BY V1\n" +
                "    ) T1 JOIN (\n" +
                "        SELECT max(K2) KEY2, V2 FROM B GROUP BY V2\n" +
                "    ) T2 ON KEY1 = KEY2 LIMIT 10\n" +
                ") ORDER BY KEY1";
        verifyQueryPlanSourceRefs(query, 2);
    }

    @Test
    public void testQueryPlanSourceRefsInUnion() throws SQLException {
        String query = "SELECT K1, V1 FROM A WHERE V1 = 'A'\n" +
                "UNION ALL\n" +
                "SELECT K2, V2 FROM B WHERE V2 = 'B'";
        verifyQueryPlanSourceRefs(query, 2);
    }

    private void verifyQueryPlanSourceRefs(String query, int refCount) throws SQLException {
        Properties props = PropertiesUtil.deepCopy(TEST_PROPERTIES);
        try (Connection conn = DriverManager.getConnection(getUrl(), props)) {
            conn.createStatement().execute("CREATE TABLE A (\n" +
                    "    K1 VARCHAR(10) NOT NULL PRIMARY KEY,\n" +
                    "    V1 VARCHAR(10))");
            conn.createStatement().execute("CREATE LOCAL INDEX IDX1 ON A(V1)");
            conn.createStatement().execute("CREATE TABLE B (\n" +
                    "    K2 VARCHAR(10) NOT NULL PRIMARY KEY,\n" +
                    "    V2 VARCHAR(10))");
            conn.createStatement().execute("CREATE LOCAL INDEX IDX2 ON B(V2)");
            PhoenixStatement stmt = conn.createStatement().unwrap(PhoenixStatement.class);
            QueryPlan plan = stmt.compileQuery(query);
            Set<TableRef> sourceRefs = plan.getSourceRefs();
            assertEquals(refCount, sourceRefs.size());
            for (TableRef table : sourceRefs) {
                assertTrue(table.getTable().getType() == PTableType.TABLE);
            }
            plan = stmt.optimizeQuery(query);
            sourceRefs = plan.getSourceRefs();
            assertEquals(refCount, sourceRefs.size());
            for (TableRef table : sourceRefs) {
                assertTrue(table.getTable().getType() == PTableType.INDEX);
            }
        }
    }

    private static class MultipleChildrenExtractor implements QueryPlanVisitor<List<QueryPlan>> {

        @Override
        public List<QueryPlan> defaultReturn(QueryPlan plan) {
            return Collections.emptyList();
        }

        @Override
        public List<QueryPlan> visit(AggregatePlan plan) {
            return Collections.emptyList();
        }

        @Override
        public List<QueryPlan> visit(ScanPlan plan) {
            return Collections.emptyList();
        }

        @Override
        public List<QueryPlan> visit(ClientAggregatePlan plan) {
            return plan.getDelegate().accept(this);
        }

        @Override
        public List<QueryPlan> visit(ClientScanPlan plan) {
            return plan.getDelegate().accept(this);
        }

        @Override
        public List<QueryPlan> visit(LiteralResultIterationPlan plan) {
            return Collections.emptyList();
        }

        @Override
        public List<QueryPlan> visit(TupleProjectionPlan plan) {
            return plan.getDelegate().accept(this);
        }

        @Override
        public List<QueryPlan> visit(HashJoinPlan plan) {
            List<QueryPlan> children = new ArrayList<QueryPlan>(plan.getSubPlans().length + 1);
            children.add(plan.getDelegate());
            for (HashJoinPlan.SubPlan subPlan : plan.getSubPlans()) {
                children.add(subPlan.getInnerPlan());
            }
            return children;
        }

        @Override
        public List<QueryPlan> visit(SortMergeJoinPlan plan) {
            return Lists.newArrayList(plan.getLhsPlan(), plan.getRhsPlan());
        }

        @Override
        public List<QueryPlan> visit(UnionPlan plan) {
            return plan.getSubPlans();
        }

        @Override
        public List<QueryPlan> visit(UnnestArrayPlan plan) {
            return Collections.emptyList();
        }

        @Override
        public List<QueryPlan> visit(CursorFetchPlan plan) {
            return Collections.emptyList();
        }

        @Override
        public List<QueryPlan> visit(ListJarsQueryPlan plan) {
            return Collections.emptyList();
        }

        @Override
        public List<QueryPlan> visit(TraceQueryPlan plan) {
            return Collections.emptyList();
        }
    }

    @Test
    public void testGroupByOrderMatchPkColumnOrder4690() throws Exception{
        this.doTestGroupByOrderMatchPkColumnOrderBug4690(false, false);
        this.doTestGroupByOrderMatchPkColumnOrderBug4690(false, true);
        this.doTestGroupByOrderMatchPkColumnOrderBug4690(true, false);
        this.doTestGroupByOrderMatchPkColumnOrderBug4690(true, true);
    }

    private void doTestGroupByOrderMatchPkColumnOrderBug4690(boolean desc ,boolean salted) throws Exception {
        Connection conn = null;
        try {
            conn = DriverManager.getConnection(getUrl());
            String tableName = generateUniqueName();
            String sql = "create table " + tableName + "( "+
                    " pk1 integer not null , " +
                    " pk2 integer not null, " +
                    " pk3 integer not null," +
                    " pk4 integer not null,"+
                    " v integer, " +
                    " CONSTRAINT TEST_PK PRIMARY KEY ( "+
                       "pk1 "+(desc ? "desc" : "")+", "+
                       "pk2 "+(desc ? "desc" : "")+", "+
                       "pk3 "+(desc ? "desc" : "")+", "+
                       "pk4 "+(desc ? "desc" : "")+
                    " )) "+(salted ? "SALT_BUCKETS =4" : "split on(2)");
            conn.createStatement().execute(sql);

            sql = "select pk2,pk1,count(v) from " + tableName + " group by pk2,pk1 order by pk2,pk1";
            QueryPlan queryPlan = TestUtil.getOptimizeQueryPlan(conn, sql);
            assertTrue(queryPlan.getGroupBy().isOrderPreserving());
            assertTrue(queryPlan.getOrderBy().getOrderByExpressions().size() ==2);
            assertTrue(queryPlan.getOrderBy().getOrderByExpressions().get(0).toString().equals("PK2"));
            assertTrue(queryPlan.getOrderBy().getOrderByExpressions().get(1).toString().equals("PK1"));

            sql = "select pk1,pk2,count(v) from " + tableName + " group by pk2,pk1 order by pk1,pk2";
            queryPlan = TestUtil.getOptimizeQueryPlan(conn, sql);
            assertTrue(queryPlan.getGroupBy().isOrderPreserving());
            assertTrue(queryPlan.getOrderBy() == (!desc ? OrderBy.FWD_ROW_KEY_ORDER_BY : OrderBy.REV_ROW_KEY_ORDER_BY));

            sql = "select pk2,pk1,count(v) from " + tableName + " group by pk2,pk1 order by pk2 desc,pk1 desc";
            queryPlan = TestUtil.getOptimizeQueryPlan(conn, sql);
            assertTrue(queryPlan.getGroupBy().isOrderPreserving());
            assertTrue(queryPlan.getOrderBy().getOrderByExpressions().size() ==2);
            assertTrue(queryPlan.getOrderBy().getOrderByExpressions().get(0).toString().equals("PK2 DESC"));
            assertTrue(queryPlan.getOrderBy().getOrderByExpressions().get(1).toString().equals("PK1 DESC"));

            sql = "select pk1,pk2,count(v) from " + tableName + " group by pk2,pk1 order by pk1 desc,pk2 desc";
            queryPlan = TestUtil.getOptimizeQueryPlan(conn, sql);
            assertTrue(queryPlan.getGroupBy().isOrderPreserving());
            assertTrue(queryPlan.getOrderBy() == (!desc ? OrderBy.REV_ROW_KEY_ORDER_BY : OrderBy.FWD_ROW_KEY_ORDER_BY));


            sql = "select pk3,pk2,count(v) from " + tableName + " where pk1=1 group by pk3,pk2 order by pk3,pk2";
            queryPlan = TestUtil.getOptimizeQueryPlan(conn, sql);
            assertTrue(queryPlan.getGroupBy().isOrderPreserving());
            assertTrue(queryPlan.getOrderBy().getOrderByExpressions().size() == 2);
            assertTrue(queryPlan.getOrderBy().getOrderByExpressions().get(0).toString().equals("PK3"));
            assertTrue(queryPlan.getOrderBy().getOrderByExpressions().get(1).toString().equals("PK2"));

            sql = "select pk2,pk3,count(v) from " + tableName + " where pk1=1 group by pk3,pk2 order by pk2,pk3";
            queryPlan = TestUtil.getOptimizeQueryPlan(conn, sql);
            assertTrue(queryPlan.getGroupBy().isOrderPreserving());
            assertTrue(queryPlan.getOrderBy() == (!desc ? OrderBy.FWD_ROW_KEY_ORDER_BY : OrderBy.REV_ROW_KEY_ORDER_BY));

            sql = "select pk3,pk2,count(v) from " + tableName + " where pk1=1 group by pk3,pk2 order by pk3 desc,pk2 desc";
            queryPlan = TestUtil.getOptimizeQueryPlan(conn, sql);
            assertTrue(queryPlan.getGroupBy().isOrderPreserving());
            assertTrue(queryPlan.getOrderBy().getOrderByExpressions().size() == 2);
            assertTrue(queryPlan.getOrderBy().getOrderByExpressions().get(0).toString().equals("PK3 DESC"));
            assertTrue(queryPlan.getOrderBy().getOrderByExpressions().get(1).toString().equals("PK2 DESC"));

            sql = "select pk2,pk3,count(v) from " + tableName + " where pk1=1 group by pk3,pk2 order by pk2 desc,pk3 desc";
            queryPlan = TestUtil.getOptimizeQueryPlan(conn, sql);
            assertTrue(queryPlan.getGroupBy().isOrderPreserving());
            assertTrue(queryPlan.getOrderBy() == (!desc ? OrderBy.REV_ROW_KEY_ORDER_BY : OrderBy.FWD_ROW_KEY_ORDER_BY));


            sql = "select pk4,pk3,pk1,count(v) from " + tableName + " where pk2=9 group by pk4,pk3,pk1 order by pk4,pk3,pk1";
            queryPlan = TestUtil.getOptimizeQueryPlan(conn, sql);
            assertTrue(queryPlan.getGroupBy().isOrderPreserving());
            assertTrue(queryPlan.getOrderBy().getOrderByExpressions().size() == 3);
            assertTrue(queryPlan.getOrderBy().getOrderByExpressions().get(0).toString().equals("PK4"));
            assertTrue(queryPlan.getOrderBy().getOrderByExpressions().get(1).toString().equals("PK3"));
            assertTrue(queryPlan.getOrderBy().getOrderByExpressions().get(2).toString().equals("PK1"));

            sql = "select pk1,pk3,pk4,count(v) from " + tableName + " where pk2=9 group by pk4,pk3,pk1 order by pk1,pk3,pk4";
            queryPlan = TestUtil.getOptimizeQueryPlan(conn, sql);
            assertTrue(queryPlan.getGroupBy().isOrderPreserving());
            assertTrue(queryPlan.getOrderBy() == (!desc ? OrderBy.FWD_ROW_KEY_ORDER_BY : OrderBy.REV_ROW_KEY_ORDER_BY));

            sql = "select pk4,pk3,pk1,count(v) from " + tableName + " where pk2=9 group by pk4,pk3,pk1 order by pk4 desc,pk3 desc,pk1 desc";
            queryPlan = TestUtil.getOptimizeQueryPlan(conn, sql);
            assertTrue(queryPlan.getGroupBy().isOrderPreserving());
            assertTrue(queryPlan.getOrderBy().getOrderByExpressions().size() == 3);
            assertTrue(queryPlan.getOrderBy().getOrderByExpressions().get(0).toString().equals("PK4 DESC"));
            assertTrue(queryPlan.getOrderBy().getOrderByExpressions().get(1).toString().equals("PK3 DESC"));
            assertTrue(queryPlan.getOrderBy().getOrderByExpressions().get(2).toString().equals("PK1 DESC"));

            sql = "select pk1,pk3,pk4,count(v) from " + tableName + " where pk2=9 group by pk4,pk3,pk1 order by pk1 desc,pk3 desc,pk4 desc";
            queryPlan = TestUtil.getOptimizeQueryPlan(conn, sql);
            assertTrue(queryPlan.getGroupBy().isOrderPreserving());
            assertTrue(queryPlan.getOrderBy() == (!desc ? OrderBy.REV_ROW_KEY_ORDER_BY : OrderBy.FWD_ROW_KEY_ORDER_BY));
        } finally {
            if(conn != null) {
                conn.close();
            }
        }
    }

    @Test
    public void testSortMergeJoinPushFilterThroughSortBug5105() throws Exception {
        Connection conn = null;
        try {
            conn= DriverManager.getConnection(getUrl());

            String tableName1="MERGE1";
            String tableName2="MERGE2";

            conn.createStatement().execute("DROP TABLE if exists "+tableName1);

            String sql="CREATE TABLE IF NOT EXISTS "+tableName1+" ( "+
                    "AID INTEGER PRIMARY KEY,"+
                    "AGE INTEGER"+
                    ")";
            conn.createStatement().execute(sql);

            conn.createStatement().execute("DROP TABLE if exists "+tableName2);
            sql="CREATE TABLE IF NOT EXISTS "+tableName2+" ( "+
                    "BID INTEGER PRIMARY KEY,"+
                    "CODE INTEGER"+
                    ")";
            conn.createStatement().execute(sql);

            //test for simple scan
            sql="select /*+ USE_SORT_MERGE_JOIN */ a.aid,b.code from (select aid,age from "+tableName1+" where age >=11 and age<=33 order by age limit 3) a inner join "+
                "(select bid,code from "+tableName2+" order by code limit 1) b on a.aid=b.bid where b.code > 50";

            QueryPlan queryPlan=getQueryPlan(conn, sql);
            SortMergeJoinPlan sortMergeJoinPlan=(SortMergeJoinPlan)((ClientScanPlan)queryPlan).getDelegate();

            ClientScanPlan lhsOuterPlan=(ClientScanPlan)((TupleProjectionPlan)(sortMergeJoinPlan.getLhsPlan())).getDelegate();
            OrderBy orderBy=lhsOuterPlan.getOrderBy();
            assertTrue(orderBy.getOrderByExpressions().size() == 1);
            assertTrue(orderBy.getOrderByExpressions().get(0).toString().equals("AID"));
            ScanPlan innerScanPlan=(ScanPlan)((TupleProjectionPlan)lhsOuterPlan.getDelegate()).getDelegate();
            orderBy=innerScanPlan.getOrderBy();
            assertTrue(orderBy.getOrderByExpressions().size() == 1);
            assertTrue(orderBy.getOrderByExpressions().get(0).toString().equals("AGE"));
            assertTrue(innerScanPlan.getLimit().intValue() == 3);

            ClientScanPlan rhsOuterPlan=(ClientScanPlan)((TupleProjectionPlan)(sortMergeJoinPlan.getRhsPlan())).getDelegate();
            String tableAlias = rhsOuterPlan.getTableRef().getTableAlias();
            String rewrittenSql = "SELECT "+tableAlias+".BID BID,"+tableAlias+".CODE CODE FROM (SELECT BID,CODE FROM MERGE2  ORDER BY CODE LIMIT 1) "+tableAlias+" WHERE "+tableAlias+".CODE > 50 ORDER BY "+tableAlias+".BID";
            assertTrue(rhsOuterPlan.getStatement().toString().equals(rewrittenSql));

            orderBy=rhsOuterPlan.getOrderBy();
            assertTrue(orderBy.getOrderByExpressions().size() == 1);
            assertTrue(orderBy.getOrderByExpressions().get(0).toString().equals("BID"));
            innerScanPlan=(ScanPlan)((TupleProjectionPlan)rhsOuterPlan.getDelegate()).getDelegate();
            orderBy=innerScanPlan.getOrderBy();
            assertTrue(orderBy.getOrderByExpressions().size() == 1);
            assertTrue(orderBy.getOrderByExpressions().get(0).toString().equals("CODE"));
            assertTrue(innerScanPlan.getLimit().intValue() == 1);

            //test for aggregate
            sql="select /*+ USE_SORT_MERGE_JOIN */ a.aid,b.codesum from (select aid,sum(age) agesum from "+tableName1+" where age >=11 and age<=33 group by aid order by agesum limit 3) a inner join "+
                "(select bid,sum(code) codesum from "+tableName2+" group by bid order by codesum limit 1) b on a.aid=b.bid where b.codesum > 50";


            queryPlan=getQueryPlan(conn, sql);
            sortMergeJoinPlan=(SortMergeJoinPlan)((ClientScanPlan)queryPlan).getDelegate();

            lhsOuterPlan=(ClientScanPlan)((TupleProjectionPlan)(sortMergeJoinPlan.getLhsPlan())).getDelegate();
            orderBy=lhsOuterPlan.getOrderBy();
            assertTrue(orderBy.getOrderByExpressions().size() == 1);
            assertTrue(orderBy.getOrderByExpressions().get(0).toString().equals("AID"));
            AggregatePlan innerAggregatePlan=(AggregatePlan)((TupleProjectionPlan)lhsOuterPlan.getDelegate()).getDelegate();
            orderBy=innerAggregatePlan.getOrderBy();
            assertTrue(orderBy.getOrderByExpressions().size() == 1);
            assertTrue(orderBy.getOrderByExpressions().get(0).toString().equals("SUM(AGE)"));
            assertTrue(innerAggregatePlan.getLimit().intValue() == 3);

            rhsOuterPlan=(ClientScanPlan)((TupleProjectionPlan)(sortMergeJoinPlan.getRhsPlan())).getDelegate();
            tableAlias = rhsOuterPlan.getTableRef().getTableAlias();
            rewrittenSql = "SELECT "+tableAlias+".BID BID,"+tableAlias+".CODESUM CODESUM FROM (SELECT BID, SUM(CODE) CODESUM FROM MERGE2  GROUP BY BID ORDER BY  SUM(CODE) LIMIT 1) "+tableAlias+" WHERE "+tableAlias+".CODESUM > 50 ORDER BY "+tableAlias+".BID";
            assertTrue(rhsOuterPlan.getStatement().toString().equals(rewrittenSql));

            orderBy=rhsOuterPlan.getOrderBy();
            assertTrue(orderBy.getOrderByExpressions().size() == 1);
            assertTrue(orderBy.getOrderByExpressions().get(0).toString().equals("BID"));
            innerAggregatePlan=(AggregatePlan)((TupleProjectionPlan)rhsOuterPlan.getDelegate()).getDelegate();
            orderBy=innerAggregatePlan.getOrderBy();
            assertTrue(orderBy.getOrderByExpressions().size() == 1);
            assertTrue(orderBy.getOrderByExpressions().get(0).toString().equals("SUM(CODE)"));
            assertTrue(innerAggregatePlan.getLimit().intValue() == 1);

            String tableName3="merge3";
            conn.createStatement().execute("DROP TABLE if exists "+tableName3);
            sql="CREATE TABLE IF NOT EXISTS "+tableName3+" ( "+
                    "CID INTEGER PRIMARY KEY,"+
                    "REGION INTEGER"+
                    ")";
            conn.createStatement().execute(sql);

            //test for join
            sql="select t1.aid,t1.code,t2.region from "+
                "(select a.aid,b.code from "+tableName1+" a inner join "+tableName2+" b on a.aid=b.bid where b.code >=44 and b.code<=66 order by b.code limit 3) t1 inner join "+
                "(select a.aid,c.region from "+tableName1+" a inner join "+tableName3+" c on a.aid=c.cid where c.region>=77 and c.region<=99 order by c.region desc limit 1) t2 on t1.aid=t2.aid "+
                "where t1.code > 50";

            PhoenixPreparedStatement phoenixPreparedStatement = conn.prepareStatement(sql).unwrap(PhoenixPreparedStatement.class);
            queryPlan = phoenixPreparedStatement.optimizeQuery(sql);
            sortMergeJoinPlan=(SortMergeJoinPlan)((ClientScanPlan)queryPlan).getDelegate();

            lhsOuterPlan=(ClientScanPlan)((TupleProjectionPlan)(sortMergeJoinPlan.getLhsPlan())).getDelegate();
            tableAlias = lhsOuterPlan.getTableRef().getTableAlias();
            rewrittenSql = "SELECT "+tableAlias+".AID AID,"+tableAlias+".CODE CODE FROM (SELECT A.AID,B.CODE FROM MERGE1 A  Inner JOIN MERGE2 B  ON (A.AID = B.BID) WHERE (B.CODE >= 44 AND B.CODE <= 66) ORDER BY B.CODE LIMIT 3) "+
                           tableAlias+" WHERE "+tableAlias+".CODE > 50 ORDER BY "+tableAlias+".AID";
            assertTrue(lhsOuterPlan.getStatement().toString().equals(rewrittenSql));

            orderBy=lhsOuterPlan.getOrderBy();
            assertTrue(orderBy.getOrderByExpressions().size() == 1);
            assertTrue(orderBy.getOrderByExpressions().get(0).toString().equals("AID"));
            innerScanPlan=(ScanPlan)((HashJoinPlan)((TupleProjectionPlan)lhsOuterPlan.getDelegate()).getDelegate()).getDelegate();
            orderBy=innerScanPlan.getOrderBy();
            assertTrue(orderBy.getOrderByExpressions().size() == 1);
            assertTrue(orderBy.getOrderByExpressions().get(0).toString().equals("B.CODE"));
            assertTrue(innerScanPlan.getLimit().intValue() == 3);

            rhsOuterPlan=(ClientScanPlan)((TupleProjectionPlan)(sortMergeJoinPlan.getRhsPlan())).getDelegate();
            orderBy=rhsOuterPlan.getOrderBy();
            assertTrue(orderBy.getOrderByExpressions().size() == 1);
            assertTrue(orderBy.getOrderByExpressions().get(0).toString().equals("AID"));
            innerScanPlan=(ScanPlan)((HashJoinPlan)((TupleProjectionPlan)rhsOuterPlan.getDelegate()).getDelegate()).getDelegate();
            orderBy=innerScanPlan.getOrderBy();
            assertTrue(orderBy.getOrderByExpressions().size() == 1);
            assertTrue(orderBy.getOrderByExpressions().get(0).toString().equals("C.REGION DESC"));
            assertTrue(innerScanPlan.getLimit().intValue() == 1);

            //test for join and aggregate
            sql="select t1.aid,t1.codesum,t2.regionsum from "+
                "(select a.aid,sum(b.code) codesum from "+tableName1+" a inner join "+tableName2+" b on a.aid=b.bid where b.code >=44 and b.code<=66 group by a.aid order by codesum limit 3) t1 inner join "+
                "(select a.aid,sum(c.region) regionsum from "+tableName1+" a inner join "+tableName3+" c on a.aid=c.cid where c.region>=77 and c.region<=99 group by a.aid order by regionsum desc limit 2) t2 on t1.aid=t2.aid "+
                "where t1.codesum >=40 and t2.regionsum >= 90";

            phoenixPreparedStatement = conn.prepareStatement(sql).unwrap(PhoenixPreparedStatement.class);
            queryPlan = phoenixPreparedStatement.optimizeQuery(sql);
            sortMergeJoinPlan=(SortMergeJoinPlan)((ClientScanPlan)queryPlan).getDelegate();

            lhsOuterPlan=(ClientScanPlan)((TupleProjectionPlan)(sortMergeJoinPlan.getLhsPlan())).getDelegate();
            tableAlias = lhsOuterPlan.getTableRef().getTableAlias();
            rewrittenSql = "SELECT "+tableAlias+".AID AID,"+tableAlias+".CODESUM CODESUM FROM (SELECT A.AID, SUM(B.CODE) CODESUM FROM MERGE1 A  Inner JOIN MERGE2 B  ON (A.AID = B.BID) WHERE (B.CODE >= 44 AND B.CODE <= 66) GROUP BY A.AID ORDER BY  SUM(B.CODE) LIMIT 3) "+tableAlias+
                           " WHERE "+tableAlias+".CODESUM >= 40 ORDER BY "+tableAlias+".AID";
            assertTrue(lhsOuterPlan.getStatement().toString().equals(rewrittenSql));

            orderBy=lhsOuterPlan.getOrderBy();
            assertTrue(orderBy.getOrderByExpressions().size() == 1);
            assertTrue(orderBy.getOrderByExpressions().get(0).toString().equals("AID"));
            innerAggregatePlan=(AggregatePlan)((HashJoinPlan)((TupleProjectionPlan)lhsOuterPlan.getDelegate()).getDelegate()).getDelegate();
            orderBy=innerAggregatePlan.getOrderBy();
            assertTrue(orderBy.getOrderByExpressions().size() == 1);
            assertTrue(orderBy.getOrderByExpressions().get(0).toString().equals("SUM(B.CODE)"));
            assertTrue(innerAggregatePlan.getLimit().intValue() == 3);

            rhsOuterPlan=(ClientScanPlan)((TupleProjectionPlan)(sortMergeJoinPlan.getRhsPlan())).getDelegate();
            tableAlias = rhsOuterPlan.getTableRef().getTableAlias();
            rewrittenSql = "SELECT "+tableAlias+".AID AID,"+tableAlias+".REGIONSUM REGIONSUM FROM (SELECT A.AID, SUM(C.REGION) REGIONSUM FROM MERGE1 A  Inner JOIN MERGE3 C  ON (A.AID = C.CID) WHERE (C.REGION >= 77 AND C.REGION <= 99) GROUP BY A.AID ORDER BY  SUM(C.REGION) DESC LIMIT 2) "+tableAlias+
                           " WHERE "+tableAlias+".REGIONSUM >= 90 ORDER BY "+tableAlias+".AID";
            assertTrue(rhsOuterPlan.getStatement().toString().equals(rewrittenSql));

            orderBy=rhsOuterPlan.getOrderBy();
            assertTrue(orderBy.getOrderByExpressions().size() == 1);
            assertTrue(orderBy.getOrderByExpressions().get(0).toString().equals("AID"));
            innerAggregatePlan=(AggregatePlan)((HashJoinPlan)((TupleProjectionPlan)rhsOuterPlan.getDelegate()).getDelegate()).getDelegate();
            orderBy=innerAggregatePlan.getOrderBy();
            assertTrue(orderBy.getOrderByExpressions().size() == 1);
            assertTrue(orderBy.getOrderByExpressions().get(0).toString().equals("SUM(C.REGION) DESC"));
            assertTrue(innerAggregatePlan.getLimit().intValue() == 2);

            //test for if SubselectRewriter.isOrderByPrefix had take effect
            sql="select t1.aid,t1.codesum,t2.regionsum from "+
                "(select a.aid,sum(b.code) codesum from "+tableName1+" a inner join "+tableName2+" b on a.aid=b.bid where b.code >=44 and b.code<=66 group by a.aid order by a.aid,codesum limit 3) t1 inner join "+
                "(select a.aid,sum(c.region) regionsum from "+tableName1+" a inner join "+tableName3+" c on a.aid=c.cid where c.region>=77 and c.region<=99 group by a.aid order by a.aid desc,regionsum desc limit 2) t2 on t1.aid=t2.aid "+
                 "where t1.codesum >=40 and t2.regionsum >= 90 order by t1.aid desc";

            phoenixPreparedStatement = conn.prepareStatement(sql).unwrap(PhoenixPreparedStatement.class);
            queryPlan = phoenixPreparedStatement.optimizeQuery(sql);
            orderBy=queryPlan.getOrderBy();
            assertTrue(orderBy.getOrderByExpressions().size() == 1);
            assertTrue(orderBy.getOrderByExpressions().get(0).toString().equals("T1.AID DESC"));
            sortMergeJoinPlan=(SortMergeJoinPlan)((ClientScanPlan)queryPlan).getDelegate();

            lhsOuterPlan = (ClientScanPlan)(((TupleProjectionPlan)sortMergeJoinPlan.getLhsPlan()).getDelegate());
            tableAlias = lhsOuterPlan.getTableRef().getTableAlias();
            rewrittenSql = "SELECT "+tableAlias+".AID AID,"+tableAlias+".CODESUM CODESUM FROM (SELECT A.AID, SUM(B.CODE) CODESUM FROM MERGE1 A  Inner JOIN MERGE2 B  ON (A.AID = B.BID) WHERE (B.CODE >= 44 AND B.CODE <= 66) GROUP BY A.AID ORDER BY A.AID, SUM(B.CODE) LIMIT 3) "+tableAlias+
                           " WHERE "+tableAlias+".CODESUM >= 40";
            assertTrue(lhsOuterPlan.getStatement().toString().equals(rewrittenSql));

            innerAggregatePlan=(AggregatePlan)((HashJoinPlan)((TupleProjectionPlan)lhsOuterPlan.getDelegate()).getDelegate()).getDelegate();
            orderBy=innerAggregatePlan.getOrderBy();
            assertTrue(orderBy.getOrderByExpressions().size() == 2);
            assertTrue(orderBy.getOrderByExpressions().get(0).toString().equals("A.AID"));
            assertTrue(orderBy.getOrderByExpressions().get(1).toString().equals("SUM(B.CODE)"));
            assertTrue(innerAggregatePlan.getLimit().intValue() == 3);

            rhsOuterPlan=(ClientScanPlan)((TupleProjectionPlan)(sortMergeJoinPlan.getRhsPlan())).getDelegate();
            tableAlias = rhsOuterPlan.getTableRef().getTableAlias();
            rewrittenSql = "SELECT "+tableAlias+".AID AID,"+tableAlias+".REGIONSUM REGIONSUM FROM (SELECT A.AID, SUM(C.REGION) REGIONSUM FROM MERGE1 A  Inner JOIN MERGE3 C  ON (A.AID = C.CID) WHERE (C.REGION >= 77 AND C.REGION <= 99) GROUP BY A.AID ORDER BY A.AID DESC, SUM(C.REGION) DESC LIMIT 2) "+tableAlias+
                           " WHERE "+tableAlias+".REGIONSUM >= 90 ORDER BY "+tableAlias+".AID";
            assertTrue(rhsOuterPlan.getStatement().toString().equals(rewrittenSql));

            orderBy=rhsOuterPlan.getOrderBy();
            assertTrue(orderBy.getOrderByExpressions().size() == 1);
            assertTrue(orderBy.getOrderByExpressions().get(0).toString().equals("AID"));
            innerAggregatePlan=(AggregatePlan)((HashJoinPlan)((TupleProjectionPlan)rhsOuterPlan.getDelegate()).getDelegate()).getDelegate();
            orderBy=innerAggregatePlan.getOrderBy();
            assertTrue(orderBy.getOrderByExpressions().size() == 2);
            assertTrue(orderBy.getOrderByExpressions().get(0).toString().equals("A.AID DESC"));
            assertTrue(orderBy.getOrderByExpressions().get(1).toString().equals("SUM(C.REGION) DESC"));
            assertTrue(innerAggregatePlan.getLimit().intValue() == 2);
        } finally {
            if(conn!=null) {
                conn.close();
            }
        }
    }

    @Test
    public void testOrderPreservingForClientScanPlanBug5148() throws Exception {
        doTestOrderPreservingForClientScanPlanBug5148(false,false);
        doTestOrderPreservingForClientScanPlanBug5148(false,true);
        doTestOrderPreservingForClientScanPlanBug5148(true, false);
        doTestOrderPreservingForClientScanPlanBug5148(true, true);
    }

    private void doTestOrderPreservingForClientScanPlanBug5148(boolean desc, boolean salted) throws Exception {
        Connection conn = null;
        try {
            conn = DriverManager.getConnection(getUrl());
            String tableName = generateUniqueName();
            String sql = "create table " + tableName + "( "+
                    " pk1 char(20) not null , " +
                    " pk2 char(20) not null, " +
                    " pk3 char(20) not null," +
                    " v1 varchar, " +
                    " v2 varchar, " +
                    " CONSTRAINT TEST_PK PRIMARY KEY ( "+
                    "pk1 "+(desc ? "desc" : "")+", "+
                    "pk2 "+(desc ? "desc" : "")+", "+
                    "pk3 "+(desc ? "desc" : "")+
                    " )) "+(salted ? "SALT_BUCKETS =4" : "");
            conn.createStatement().execute(sql);

            sql = "select v1 from (select v1,v2,pk3 from "+tableName+" t where pk1 = '6' order by t.v2,t.pk3,t.v1 limit 10) a order by v2,pk3";
            QueryPlan plan =  TestUtil.getOptimizeQueryPlan(conn, sql);
            assertTrue(plan.getOrderBy() == OrderBy.FWD_ROW_KEY_ORDER_BY);

            sql = "select v1 from (select v1,v2,pk3 from "+tableName+" t where pk1 = '6' order by t.v2,t.pk3,t.v1 limit 10) a where pk3 = '8' order by v2,v1";
            plan =  TestUtil.getOptimizeQueryPlan(conn, sql);
            assertTrue(plan.getOrderBy() == OrderBy.FWD_ROW_KEY_ORDER_BY);

            sql = "select v1 from (select v1,v2,pk3 from "+tableName+" t where pk1 = '6' order by t.v2 desc,t.pk3 desc,t.v1 desc limit 10) a order by v2 desc ,pk3 desc";
            plan =  TestUtil.getOptimizeQueryPlan(conn, sql);
            assertTrue(plan.getOrderBy() == OrderBy.FWD_ROW_KEY_ORDER_BY);

            sql = "select sub from (select substr(v2,0,2) sub,cast (count(pk3) as bigint) cnt from "+tableName+" t where pk1 = '6' group by v1 ,v2 order by count(pk3),t.v2 limit 10) a order by cnt,sub";
            plan =  TestUtil.getOptimizeQueryPlan(conn, sql);
            assertTrue(plan.getOrderBy() == OrderBy.FWD_ROW_KEY_ORDER_BY);

            sql = "select sub from (select substr(v2,0,2) sub,count(pk3) cnt from "+tableName+" t where pk1 = '6' group by v1 ,v2 order by count(pk3),t.v2 limit 10) a order by cast(cnt as bigint),substr(sub,0,1)";
            plan =  TestUtil.getOptimizeQueryPlan(conn, sql);
            assertTrue(plan.getOrderBy() == OrderBy.FWD_ROW_KEY_ORDER_BY);

            sql = "select sub from (select substr(v2,0,2) sub,cast (count(pk3) as bigint) cnt from "+tableName+" t where pk1 = '6' group by v1 ,v2 order by count(pk3) desc,t.v2 desc limit 10) a order by cnt desc ,sub desc";
            plan =  TestUtil.getOptimizeQueryPlan(conn, sql);
            assertTrue(plan.getOrderBy() == OrderBy.FWD_ROW_KEY_ORDER_BY);

            sql = "select sub from (select substr(v2,0,2) sub,pk2 from "+tableName+" t where pk1 = '6' group by pk2,v2 limit 10) a order by pk2,sub";
            plan =  TestUtil.getOptimizeQueryPlan(conn, sql);
            if(desc) {
                assertTrue(plan.getOrderBy().getOrderByExpressions().size() > 0);
            } else {
                assertTrue(plan.getOrderBy() == OrderBy.FWD_ROW_KEY_ORDER_BY);
            }

            sql = "select sub from (select substr(v2,0,2) sub,pk2 from "+tableName+" t where pk1 = '6' group by pk2,v2 limit 10) a order by pk2 desc,sub";
            plan =  TestUtil.getOptimizeQueryPlan(conn, sql);
            if(desc) {
                assertTrue(plan.getOrderBy() == OrderBy.FWD_ROW_KEY_ORDER_BY);
            } else {
                assertTrue(plan.getOrderBy().getOrderByExpressions().size() > 0);
            }

            sql = "select sub from (select substr(v2,0,2) sub,count(pk3) cnt from "+tableName+" t where pk1 = '6' group by v1 ,v2 order by t.v2 ,count(pk3) limit 10) a order by sub ,cnt";
            plan =  TestUtil.getOptimizeQueryPlan(conn, sql);
            assertTrue(plan.getOrderBy().getOrderByExpressions().size() > 0);

            sql = "select sub from (select substr(v2,0,2) sub,count(pk3) cnt from "+tableName+" t where pk1 = '6' group by v1 ,v2 order by t.v2 ,count(pk3) limit 10) a order by substr(sub,0,1) ,cast(cnt as bigint)";
            plan =  TestUtil.getOptimizeQueryPlan(conn, sql);
            assertTrue(plan.getOrderBy().getOrderByExpressions().size() > 0);

            sql = "select sub from (select substr(v2,0,2) sub,count(pk3) cnt from "+tableName+" t where pk1 = '6' group by v1 ,v2 order by t.v2 ,count(pk3) limit 10) a order by sub ,cnt";
            plan =  TestUtil.getOptimizeQueryPlan(conn, sql);
            assertTrue(plan.getOrderBy().getOrderByExpressions().size() > 0);

            sql = "select v1 from (select v1,v2,pk3  from "+tableName+" t where pk1 = '6' order by t.v2 desc,t.pk3 desc,t.v1 desc limit 10) a order by v2 ,pk3";
            plan =  TestUtil.getOptimizeQueryPlan(conn, sql);
            assertTrue(plan.getOrderBy().getOrderByExpressions().size() > 0);

            sql = "select v1 from (select v1,v2,pk3 from "+tableName+" t where pk1 = '6' order by t.v2,t.pk3,t.v1 limit 10) a where pk3 = '8' or (v2 < 'abc' and pk3 > '11') order by v2,v1";
            plan =  TestUtil.getOptimizeQueryPlan(conn, sql);
            assertTrue(plan.getOrderBy().getOrderByExpressions().size() > 0);

            //test innerQueryPlan is ordered by rowKey
            sql = "select pk1 from (select pk3,pk2,pk1 from "+tableName+" t where v1 = '6' order by t.pk1,t.pk2 limit 10) a where pk3 > '8' order by pk1,pk2,pk3";
            plan =  TestUtil.getOptimizeQueryPlan(conn, sql);
            assertTrue(plan.getOrderBy() == OrderBy.FWD_ROW_KEY_ORDER_BY);

            sql = "select pk1 from (select substr(pk3,0,3) sub,pk2,pk1 from "+tableName+" t where v1 = '6' order by t.pk1,t.pk2 limit 10) a where sub > '8' order by pk1,pk2,sub";
            plan =  TestUtil.getOptimizeQueryPlan(conn, sql);
            assertTrue(plan.getOrderBy() == OrderBy.FWD_ROW_KEY_ORDER_BY);

            sql = "select pk1 from (select pk3,pk2,pk1 from "+tableName+" t where v1 = '6' order by t.pk1 desc,t.pk2 desc limit 10) a where pk3 > '8' order by pk1 desc ,pk2 desc ,pk3 desc";
            plan =  TestUtil.getOptimizeQueryPlan(conn, sql);
            assertTrue(plan.getOrderBy() == OrderBy.FWD_ROW_KEY_ORDER_BY);

            sql = "select pk1 from (select substr(pk3,0,3) sub,pk2,pk1 from "+tableName+" t where v1 = '6' order by t.pk1 desc,t.pk2 desc limit 10) a where sub > '8' order by pk1 desc,pk2 desc,sub desc";
            plan =  TestUtil.getOptimizeQueryPlan(conn, sql);
            assertTrue(plan.getOrderBy() == OrderBy.FWD_ROW_KEY_ORDER_BY);
        } finally {
            if(conn != null) {
                conn.close();
            }
        }
    }

    @Test
    public void testGroupByOrderPreservingForClientAggregatePlanBug5148() throws Exception {
        doTestGroupByOrderPreservingForClientAggregatePlanBug5148(false, false);
        doTestGroupByOrderPreservingForClientAggregatePlanBug5148(false, true);
        doTestGroupByOrderPreservingForClientAggregatePlanBug5148(true, false);
        doTestGroupByOrderPreservingForClientAggregatePlanBug5148(true, true);
    }

    private void doTestGroupByOrderPreservingForClientAggregatePlanBug5148(boolean desc, boolean salted) throws Exception {
        Connection conn = null;
        try {
            conn = DriverManager.getConnection(getUrl());
            String tableName = generateUniqueName();
            String sql = "create table " + tableName + "( "+
                    " pk1 varchar not null , " +
                    " pk2 varchar not null, " +
                    " pk3 varchar not null," +
                    " v1 varchar, " +
                    " v2 varchar, " +
                    " CONSTRAINT TEST_PK PRIMARY KEY ( "+
                    "pk1 "+(desc ? "desc" : "")+", "+
                    "pk2 "+(desc ? "desc" : "")+", "+
                    "pk3 "+(desc ? "desc" : "")+
                    " )) "+(salted ? "SALT_BUCKETS =4" : "");
            conn.createStatement().execute(sql);

            sql = "select v1 from (select v1,pk2,pk1 from "+tableName+" t where pk1 = '6' order by t.pk2,t.v1,t.pk1 limit 10) a group by pk2,v1 order by pk2,v1";
            QueryPlan plan =  TestUtil.getOptimizeQueryPlan(conn, sql);
            assertTrue(plan.getGroupBy().isOrderPreserving());
            assertTrue(plan.getOrderBy() == OrderBy.FWD_ROW_KEY_ORDER_BY);

            sql = "select v1 from (select v1,pk2,pk1 from "+tableName+" t where pk1 = '6' order by t.pk2,t.v1,t.pk1 limit 10) a where pk2 = '8' group by v1, pk1 order by v1,pk1";
            plan =  TestUtil.getOptimizeQueryPlan(conn, sql);
            assertTrue(plan.getGroupBy().isOrderPreserving());
            assertTrue(plan.getOrderBy() == OrderBy.FWD_ROW_KEY_ORDER_BY);

            sql = "select v1 from (select v1,pk2,pk1 from "+tableName+" t where pk1 = '6' order by t.pk2 desc,t.v1 desc,t.pk1 limit 10) a group by pk2, v1 order by pk2 desc,v1 desc";
            plan =  TestUtil.getOptimizeQueryPlan(conn, sql);
            assertTrue(plan.getGroupBy().isOrderPreserving());
            assertTrue(plan.getOrderBy() == OrderBy.FWD_ROW_KEY_ORDER_BY);

            sql = "select v1 from (select v1,pk2,pk1 from "+tableName+" t where pk1 = '6' order by t.pk2,t.v1,t.pk1 limit 10) a where pk2 = '8' or (v1 < 'abc' and pk2 > '11') group by v1, pk1 order by v1,pk1";
            plan =  TestUtil.getOptimizeQueryPlan(conn, sql);
            assertTrue(!plan.getGroupBy().isOrderPreserving());
            if(desc) {
                assertTrue(plan.getOrderBy().getOrderByExpressions().size() > 0);
            } else {
                assertTrue(plan.getOrderBy() == OrderBy.FWD_ROW_KEY_ORDER_BY);
            }

            sql = "select v1 from (select v1,pk2,pk1 from "+tableName+" t where pk1 = '6' order by t.pk2,t.v1,t.pk1 limit 10) a where pk2 = '8' or (v1 < 'abc' and pk2 > '11') group by v1, pk1 order by v1,pk1 desc";
            plan =  TestUtil.getOptimizeQueryPlan(conn, sql);
            assertTrue(!plan.getGroupBy().isOrderPreserving());
            if(desc) {
                assertTrue(plan.getOrderBy() == OrderBy.FWD_ROW_KEY_ORDER_BY);
            } else {
                assertTrue(plan.getOrderBy().getOrderByExpressions().size() > 0);
            }

            sql = "select sub from (select v1,pk2,substr(pk1,0,1) sub from "+tableName+" t where v2 = '6' order by t.pk2,t.v1,t.pk1 limit 10) a where pk2 = '8' group by v1,sub order by v1,sub";
            plan =  TestUtil.getOptimizeQueryPlan(conn, sql);
            assertTrue(plan.getGroupBy().isOrderPreserving());
            assertTrue(plan.getOrderBy() == OrderBy.FWD_ROW_KEY_ORDER_BY);

            sql = "select sub from (select substr(v1,0,1) sub,pk2,pk1 from "+tableName+" t where v2 = '6' order by t.pk2,t.v1,t.pk1 limit 10) a where pk2 = '8' group by sub,pk1 order by sub,pk1";
            plan =  TestUtil.getOptimizeQueryPlan(conn, sql);
            assertTrue(!plan.getGroupBy().isOrderPreserving());
            if(desc) {
                assertTrue(plan.getOrderBy().getOrderByExpressions().size() > 0);
            } else {
                assertTrue(plan.getOrderBy() == OrderBy.FWD_ROW_KEY_ORDER_BY);
            }

            sql = "select sub from (select substr(v1,0,1) sub,pk2,pk1 from "+tableName+" t where v2 = '6' order by t.pk2,t.v1,t.pk1 limit 10) a where pk2 = '8' group by sub,pk1 order by sub,pk1 desc";
            plan =  TestUtil.getOptimizeQueryPlan(conn, sql);
            assertTrue(!plan.getGroupBy().isOrderPreserving());
            if(desc) {
                assertTrue(plan.getOrderBy() == OrderBy.FWD_ROW_KEY_ORDER_BY);
            } else {
                assertTrue(plan.getOrderBy().getOrderByExpressions().size() > 0);
            }

            sql = "select sub from (select substr(v2,0,2) sub,cast (count(pk3) as bigint) cnt from "+tableName+" t where pk1 = '6' group by v1,v2 order by count(pk3),t.v2 limit 10) a group by cnt,sub order by cnt,sub";
            plan =  TestUtil.getOptimizeQueryPlan(conn, sql);
            assertTrue(plan.getGroupBy().isOrderPreserving());
            assertTrue(plan.getOrderBy() == OrderBy.FWD_ROW_KEY_ORDER_BY);

            sql = "select substr(sub,0,1) from (select substr(v2,0,2) sub,count(pk3) cnt from "+tableName+" t where pk1 = '6' group by v1 ,v2 order by count(pk3),t.v2 limit 10) a "+
                  "group by cast(cnt as bigint),substr(sub,0,1) order by cast(cnt as bigint),substr(sub,0,1)";
            plan =  TestUtil.getOptimizeQueryPlan(conn, sql);
            assertTrue(plan.getGroupBy().isOrderPreserving());
            assertTrue(plan.getOrderBy() == OrderBy.FWD_ROW_KEY_ORDER_BY);

            sql = "select sub from (select substr(v2,0,2) sub,count(pk3) cnt from "+tableName+" t where pk1 = '6' group by v1 ,v2 order by count(pk3) desc,t.v2 desc limit 10) a group by cnt,sub order by cnt desc ,sub desc";
            plan =  TestUtil.getOptimizeQueryPlan(conn, sql);
            assertTrue(plan.getGroupBy().isOrderPreserving());
            assertTrue(plan.getOrderBy() == OrderBy.FWD_ROW_KEY_ORDER_BY);

            sql = "select substr(sub,0,1) from (select substr(v2,0,2) sub,count(pk3) cnt from "+tableName+" t where pk1 = '6' group by v1 ,v2 order by count(pk3) desc,t.v2 desc limit 10) a "+
                  "group by cast(cnt as bigint),substr(sub,0,1) order by cast(cnt as bigint) desc,substr(sub,0,1) desc";
            plan =  TestUtil.getOptimizeQueryPlan(conn, sql);
            assertTrue(plan.getGroupBy().isOrderPreserving());
            assertTrue(plan.getOrderBy() == OrderBy.FWD_ROW_KEY_ORDER_BY);

            sql = "select sub from (select substr(v2,0,2) sub,pk2 from "+tableName+" t where pk1 = '6' group by pk2,v2 limit 10) a group by pk2,sub order by pk2,sub";
            plan =  TestUtil.getOptimizeQueryPlan(conn, sql);
            assertTrue(plan.getGroupBy().isOrderPreserving());
            if(desc) {
                assertTrue(plan.getOrderBy().getOrderByExpressions().size() > 0);
            } else {
                assertTrue(plan.getOrderBy() == OrderBy.FWD_ROW_KEY_ORDER_BY);
            }

            sql = "select sub from (select substr(v2,0,2) sub,pk2 from "+tableName+" t where pk1 = '6' group by pk2,v2 limit 10) a group by pk2,sub order by pk2 desc,sub";
            plan =  TestUtil.getOptimizeQueryPlan(conn, sql);
            if(desc) {
                assertTrue(plan.getOrderBy() == OrderBy.FWD_ROW_KEY_ORDER_BY);
            } else {
                assertTrue(plan.getOrderBy().getOrderByExpressions().size() > 0);
            }

            //test innerQueryPlan is ordered by rowKey
            sql = "select pk1 from (select pk3,pk2,pk1 from "+tableName+" t where v1 = '6' order by t.pk1,t.pk2 limit 10) a where pk3 > '8' group by pk1,pk2,pk3 order by pk1,pk2,pk3";
            plan =  TestUtil.getOptimizeQueryPlan(conn, sql);
            assertTrue(plan.getGroupBy().isOrderPreserving());
            assertTrue(plan.getOrderBy() == OrderBy.FWD_ROW_KEY_ORDER_BY);

            sql = "select pk1 from (select substr(pk3,0,3) sub,pk2,pk1 from "+tableName+" t where v1 = '6' order by t.pk1,t.pk2 limit 10) a where sub > '8' group by pk1,pk2,sub order by pk1,pk2";
            plan =  TestUtil.getOptimizeQueryPlan(conn, sql);
            assertTrue(plan.getGroupBy().isOrderPreserving());
            assertTrue(plan.getOrderBy() == OrderBy.FWD_ROW_KEY_ORDER_BY);

            sql = "select pk1 from (select pk3,pk2,pk1 from "+tableName+" t where v1 = '6' order by t.pk1 desc,t.pk2 desc limit 10) a where pk3 > '8' group by pk1, pk2, pk3 order by pk1 desc ,pk2 desc ,pk3 desc";
            plan =  TestUtil.getOptimizeQueryPlan(conn, sql);
            assertTrue(plan.getGroupBy().isOrderPreserving());
            assertTrue(plan.getOrderBy() == OrderBy.FWD_ROW_KEY_ORDER_BY);

            sql = "select pk1 from (select substr(pk3,0,3) sub,pk2,pk1 from "+tableName+" t where v1 = '6' order by t.pk1 desc,t.pk2 desc limit 10) a where sub > '8' group by pk1,pk2,sub order by pk1 desc,pk2 desc";
            plan =  TestUtil.getOptimizeQueryPlan(conn, sql);
            assertTrue(plan.getGroupBy().isOrderPreserving());
            assertTrue(plan.getOrderBy() == OrderBy.FWD_ROW_KEY_ORDER_BY);
        } finally {
            if(conn != null) {
                conn.close();
            }
        }
    }

    @Test
    public void testOrderPreservingForSortMergeJoinBug5148() throws Exception {
        doTestOrderPreservingForSortMergeJoinBug5148(false, false);
        doTestOrderPreservingForSortMergeJoinBug5148(false, true);
        doTestOrderPreservingForSortMergeJoinBug5148(true, false);
        doTestOrderPreservingForSortMergeJoinBug5148(true, true);
    }

    private void doTestOrderPreservingForSortMergeJoinBug5148(boolean desc, boolean salted) throws Exception {
        Connection conn = null;
        try {
            conn = DriverManager.getConnection(getUrl());

            String tableName1 = generateUniqueName();
            String tableName2 = generateUniqueName();

            String sql = "CREATE TABLE IF NOT EXISTS "+tableName1+" ( "+
                    "AID INTEGER PRIMARY KEY "+(desc ? "desc" : "")+","+
                    "AGE INTEGER"+
                    ") "+(salted ? "SALT_BUCKETS =4" : "");
            conn.createStatement().execute(sql);

            sql = "CREATE TABLE IF NOT EXISTS "+tableName2+" ( "+
                    "BID INTEGER PRIMARY KEY "+(desc ? "desc" : "")+","+
                    "CODE INTEGER"+
                    ")"+(salted ? "SALT_BUCKETS =4" : "");
            conn.createStatement().execute(sql);

            sql = "select /*+ USE_SORT_MERGE_JOIN */ a.aid,b.code from (select aid,age from "+tableName1+" where age >=11 and age<=33 order by age limit 3) a inner join "+
                    "(select bid,code from "+tableName2+" order by code limit 1) b on a.aid=b.bid and a.age = b.code order by a.aid ,a.age";
            QueryPlan queryPlan = getQueryPlan(conn, sql);
            assertTrue(queryPlan.getOrderBy() == OrderBy.FWD_ROW_KEY_ORDER_BY);

            sql = "select /*+ USE_SORT_MERGE_JOIN */ a.aid,b.code from (select aid,age from "+tableName1+" where age >=11 and age<=33 order by age limit 3) a inner join "+
                    "(select bid,code from "+tableName2+" order by code limit 1) b on a.aid=b.bid and a.age = b.code order by a.aid desc,a.age desc";
            queryPlan = getQueryPlan(conn, sql);
            assertTrue(queryPlan.getOrderBy().getOrderByExpressions().size() > 0);

            sql = "select /*+ USE_SORT_MERGE_JOIN */ a.aid,a.age from (select aid,age from "+tableName1+" where age >=11 and age<=33 order by age limit 3) a inner join "+
                    "(select bid,code from "+tableName2+" order by code limit 1) b on a.aid=b.bid and a.age = b.code group by a.aid,a.age order by a.aid ,a.age";
            queryPlan = getQueryPlan(conn, sql);
            assertTrue(queryPlan.getGroupBy().isOrderPreserving());
            assertTrue(queryPlan.getOrderBy() == OrderBy.FWD_ROW_KEY_ORDER_BY);

            sql = "select /*+ USE_SORT_MERGE_JOIN */ a.aid,a.age from (select aid,age from "+tableName1+" where age >=11 and age<=33 order by age limit 3) a inner join "+
                    "(select bid,code from "+tableName2+" order by code limit 1) b on a.aid=b.bid and a.age = b.code group by a.aid,a.age order by a.aid desc,a.age desc";
            queryPlan = getQueryPlan(conn, sql);
            assertTrue(queryPlan.getGroupBy().isOrderPreserving());
            assertTrue(queryPlan.getOrderBy().getOrderByExpressions().size() > 0);

            sql = "select /*+ USE_SORT_MERGE_JOIN */ a.aid,b.code from (select aid,age from "+tableName1+" where age >=11 and age<=33 order by age limit 3) a inner join "+
                    "(select bid,code from "+tableName2+" order by code limit 1) b on a.aid=b.bid and a.age = b.code order by b.bid ,b.code";
            queryPlan = getQueryPlan(conn, sql);
            assertTrue(queryPlan.getOrderBy() == OrderBy.FWD_ROW_KEY_ORDER_BY);

            sql = "select /*+ USE_SORT_MERGE_JOIN */ a.aid,b.code from (select aid,age from "+tableName1+" where age >=11 and age<=33 order by age limit 3) a inner join "+
                    "(select bid,code from "+tableName2+" order by code limit 1) b on a.aid=b.bid and a.age = b.code order by b.bid desc ,b.code desc";
            queryPlan = getQueryPlan(conn, sql);
            assertTrue(queryPlan.getOrderBy().getOrderByExpressions().size() > 0);

            sql = "select /*+ USE_SORT_MERGE_JOIN */ b.code from (select aid,age from "+tableName1+" where age >=11 and age<=33 order by age limit 3) a inner join "+
                    "(select bid,code from "+tableName2+" order by code limit 1) b on a.aid=b.bid and a.age = b.code group by b.bid, b.code order by b.bid ,b.code";
            queryPlan = getQueryPlan(conn, sql);
            assertTrue(queryPlan.getGroupBy().isOrderPreserving());
            assertTrue(queryPlan.getOrderBy() == OrderBy.FWD_ROW_KEY_ORDER_BY);

            sql = "select /*+ USE_SORT_MERGE_JOIN */ b.code from (select aid,age from "+tableName1+" where age >=11 and age<=33 order by age limit 3) a inner join "+
                    "(select bid,code from "+tableName2+" order by code limit 1) b on a.aid=b.bid and a.age = b.code group by b.bid, b.code order by b.bid desc,b.code desc";
            queryPlan = getQueryPlan(conn, sql);
            assertTrue(queryPlan.getGroupBy().isOrderPreserving());
            assertTrue(queryPlan.getOrderBy().getOrderByExpressions().size() > 0);
            //test part column
            sql = "select /*+ USE_SORT_MERGE_JOIN */ a.aid,b.code from "+tableName1+" a inner join "+tableName2+" b on a.aid=b.bid and a.age = b.code order by a.aid";
            queryPlan = getQueryPlan(conn, sql);
            assertTrue(queryPlan.getOrderBy() == OrderBy.FWD_ROW_KEY_ORDER_BY);

            sql = "select /*+ USE_SORT_MERGE_JOIN */ a.aid,b.code from "+tableName1+" a inner join "+tableName2+" b on a.aid=b.bid and a.age = b.code order by a.aid desc";
            queryPlan = getQueryPlan(conn, sql);
            assertTrue(queryPlan.getOrderBy().getOrderByExpressions().size() > 0);

            sql = "select /*+ USE_SORT_MERGE_JOIN */ a.aid from "+tableName1+" a inner join "+tableName2+" b on a.aid=b.bid and a.age = b.code group by a.aid order by a.aid";
            queryPlan = getQueryPlan(conn, sql);
            assertTrue(queryPlan.getGroupBy().isOrderPreserving());
            assertTrue(queryPlan.getOrderBy() == OrderBy.FWD_ROW_KEY_ORDER_BY);

            sql = "select /*+ USE_SORT_MERGE_JOIN */ a.aid from "+tableName1+" a inner join "+tableName2+" b on a.aid=b.bid and a.age = b.code group by a.aid order by a.aid desc";
            queryPlan = getQueryPlan(conn, sql);
            assertTrue(queryPlan.getGroupBy().isOrderPreserving());
            assertTrue(queryPlan.getOrderBy().getOrderByExpressions().size() > 0);

            sql = "select /*+ USE_SORT_MERGE_JOIN */ a.aid,b.code from "+tableName1+" a inner join "+tableName2+" b on a.aid=b.bid and a.age = b.code order by a.age";
            queryPlan = getQueryPlan(conn, sql);
            assertTrue(queryPlan.getOrderBy().getOrderByExpressions().size() > 0);

            sql = "select /*+ USE_SORT_MERGE_JOIN */ b.bid,a.age from "+tableName1+" a inner join "+tableName2+" b on a.aid=b.bid and a.age = b.code order by b.bid";
            queryPlan = getQueryPlan(conn, sql);
            assertTrue(queryPlan.getOrderBy() == OrderBy.FWD_ROW_KEY_ORDER_BY);

            sql = "select /*+ USE_SORT_MERGE_JOIN */ b.bid,a.age from "+tableName1+" a inner join "+tableName2+" b on a.aid=b.bid and a.age = b.code order by b.bid desc";
            queryPlan = getQueryPlan(conn, sql);
            assertTrue(queryPlan.getOrderBy().getOrderByExpressions().size() > 0);

            sql = "select /*+ USE_SORT_MERGE_JOIN */ b.bid from "+tableName1+" a inner join "+tableName2+" b on a.aid=b.bid and a.age = b.code group by b.bid order by b.bid";
            queryPlan = getQueryPlan(conn, sql);
            assertTrue(queryPlan.getGroupBy().isOrderPreserving());
            assertTrue(queryPlan.getOrderBy() == OrderBy.FWD_ROW_KEY_ORDER_BY);

            sql = "select /*+ USE_SORT_MERGE_JOIN */ b.bid from "+tableName1+" a inner join "+tableName2+" b on a.aid=b.bid and a.age = b.code group by b.bid order by b.bid desc";
            queryPlan = getQueryPlan(conn, sql);
            assertTrue(queryPlan.getGroupBy().isOrderPreserving());
            assertTrue(queryPlan.getOrderBy().getOrderByExpressions().size() > 0);

            sql = "select /*+ USE_SORT_MERGE_JOIN */ b.bid,a.age from "+tableName1+" a inner join "+tableName2+" b on a.aid=b.bid and a.age = b.code order by b.code";
            queryPlan = getQueryPlan(conn, sql);
            assertTrue(queryPlan.getOrderBy().getOrderByExpressions().size() > 0);
        } finally {
            if(conn!=null) {
                conn.close();
            }
        }
    }

    @Test
    public void testSortMergeBug4508() throws Exception {
        Connection conn = null;
        Connection conn010 = null;
        try {
            // Salted tables
            Properties props = PropertiesUtil.deepCopy(TEST_PROPERTIES);
            conn = DriverManager.getConnection(getUrl(), props);
            props = PropertiesUtil.deepCopy(TEST_PROPERTIES);
            props.setProperty("TenantId", "010");
            conn010 = DriverManager.getConnection(getUrl(), props);

            String peopleTable1 = generateUniqueName();
            String myTable1 = generateUniqueName();
            conn.createStatement().execute("CREATE TABLE " + peopleTable1 + " (\n" +
                    "PERSON_ID VARCHAR NOT NULL,\n" +
                    "NAME VARCHAR\n" +
                    "CONSTRAINT PK_TEST_PEOPLE PRIMARY KEY (PERSON_ID)) SALT_BUCKETS = 3");
            conn.createStatement().execute("CREATE TABLE " + myTable1 + " (\n" +
                    "LOCALID VARCHAR NOT NULL,\n" +
                    "DSID VARCHAR(255) NOT NULL, \n" +
                    "EID CHAR(40),\n" +
                    "HAS_CANDIDATES BOOLEAN\n" +
                    "CONSTRAINT PK_MYTABLE PRIMARY KEY (LOCALID, DSID)) SALT_BUCKETS = 3");
            verifyQueryPlanForSortMergeBug4508(conn, peopleTable1, myTable1);

            // Salted multi-tenant tables
            String peopleTable2 = generateUniqueName();
            String myTable2 = generateUniqueName();
            conn.createStatement().execute("CREATE TABLE " + peopleTable2 + " (\n" +
                    "TENANT_ID VARCHAR NOT NULL,\n" +
                    "PERSON_ID VARCHAR NOT NULL,\n" +
                    "NAME VARCHAR\n" +
                    "CONSTRAINT PK_TEST_PEOPLE PRIMARY KEY (TENANT_ID, PERSON_ID))\n" +
                    "SALT_BUCKETS = 3, MULTI_TENANT=true");
            conn.createStatement().execute("CREATE TABLE " + myTable2 + " (\n" +
                    "TENANT_ID VARCHAR NOT NULL,\n" +
                    "LOCALID VARCHAR NOT NULL,\n" +
                    "DSID VARCHAR(255) NOT NULL, \n" +
                    "EID CHAR(40),\n" +
                    "HAS_CANDIDATES BOOLEAN\n" +
                    "CONSTRAINT PK_MYTABLE PRIMARY KEY (TENANT_ID, LOCALID, DSID))\n" +
                    "SALT_BUCKETS = 3, MULTI_TENANT=true");
            verifyQueryPlanForSortMergeBug4508(conn010, peopleTable2, myTable2);
        } finally {
            if(conn!=null) {
                conn.close();
            }
            if(conn010 != null) {
                conn010.close();
            }
        }
    }

    private static void verifyQueryPlanForSortMergeBug4508(Connection conn, String peopleTable, String myTable) throws Exception {
        String query1 = "SELECT /*+ USE_SORT_MERGE_JOIN*/ COUNT(*)\n" +
                "FROM " + peopleTable + " ds JOIN " + myTable + " l\n" +
                "ON ds.PERSON_ID = l.LOCALID\n" +
                "WHERE l.EID IS NULL AND l.DSID = 'PEOPLE' AND l.HAS_CANDIDATES = FALSE";
        String query2 = "SELECT /*+ USE_SORT_MERGE_JOIN */ COUNT(*)\n" +
                "FROM (SELECT LOCALID FROM " + myTable + "\n" +
                "WHERE EID IS NULL AND DSID = 'PEOPLE' AND HAS_CANDIDATES = FALSE) l\n" +
                "JOIN " + peopleTable + " ds ON ds.PERSON_ID = l.LOCALID";

        for (String q : new String[]{query1, query2}) {
            ResultSet rs = conn.createStatement().executeQuery("explain " + q);
            String plan = QueryUtil.getExplainPlan(rs);
            assertFalse("Tables should not require sort over their PKs:\n" + plan,
                    plan.contains("SERVER SORTED BY"));
        }
    }

    @Test
    public void testDistinctCountLimitBug5217() throws Exception {
        Connection conn = null;
        try {
            conn = DriverManager.getConnection(getUrl());
            String tableName = generateUniqueName();
            String sql = "create table " + tableName + "( "+
                    " pk1 integer not null , " +
                    " pk2 integer not null, " +
                    " v integer, " +
                    " CONSTRAINT TEST_PK PRIMARY KEY (pk1,pk2))";
            conn.createStatement().execute(sql);

            sql = "select count(distinct pk1) from " + tableName + " limit 1";
            QueryPlan plan =  TestUtil.getOptimizeQueryPlan(conn, sql);
            Scan scan = plan.getContext().getScan();
            assertFalse(TestUtil.hasFilter(scan, PageFilter.class));
        } finally {
            if(conn!=null) {
                conn.close();
            }
        }
    }

    @Test
    public void testPushDownPostFilterToSubJoinBug5389() throws Exception {
        Connection conn = null;
        try {
            conn = DriverManager.getConnection(getUrl());
            String orderTableName = "order_table";
            String itemTableName = "item_table";
            String supplierTableName = "supplier_table";
            String sql = "create table " + orderTableName +
                    "   (order_id varchar(15) not null primary key, " +
                    "    customer_id varchar(10), " +
                    "    item_id varchar(10), " +
                    "    price integer, " +
                    "    quantity integer, " +
                    "    date timestamp)";
            conn.createStatement().execute(sql);

            sql = "create table " + itemTableName +
                    "   (item_id varchar(10) not null primary key, " +
                    "    name varchar, " +
                    "    price integer, " +
                    "    discount1 integer, " +
                    "    discount2 integer, " +
                    "    supplier_id varchar(10), " +
                    "    description varchar)";
            conn.createStatement().execute(sql);

            sql = "create table " + supplierTableName +
                    "   (supplier_id varchar(10) not null primary key, " +
                    "    name varchar, " +
                    "    phone varchar(12), " +
                    "    address varchar, " +
                    "    loc_id varchar(5))";
            conn.createStatement().execute(sql);

            doTestPushDownPostFilterToSubJoinForNoStarJoinBug5389(conn, supplierTableName, itemTableName, orderTableName);
            doTestPushDownPostFilterToSubJoinForSortMergeJoinBug5389(conn, supplierTableName, itemTableName, orderTableName);
        } finally {
            if(conn != null) {
                conn.close();
            }
        }
    }

    private void doTestPushDownPostFilterToSubJoinForNoStarJoinBug5389(
            Connection conn,
            String supplierTableName,
            String itemTableName,
            String orderTableName) throws Exception {
        //one condition push down.
        String sql = "select /*+ NO_STAR_JOIN */ COALESCE(o.order_id,'empty_order_id'),i.item_id, i.discount2+5, s.supplier_id, lower(s.name) from "+
                supplierTableName + " s inner join " + itemTableName + " i on  s.supplier_id = i.supplier_id "+
                "inner join " + orderTableName + " o on  i.item_id = o.item_id "+
                "where (o.price < 10 or o.price > 20) and "+
                "(i.supplier_id != 'medi' or s.address = 'hai')";
        QueryPlan queryPlan = TestUtil.getOptimizeQueryPlanNoIterator(conn, sql);
        HashJoinPlan hashJoinPlan = (HashJoinPlan)queryPlan;
        assertTrue(hashJoinPlan.getJoinInfo().getPostJoinFilterExpression() == null);
        HashSubPlan[] hashSubPlans = (HashSubPlan[])hashJoinPlan.getSubPlans();
        assertTrue(hashSubPlans.length == 1);
        HashJoinPlan subHashJoinPlan = (HashJoinPlan)(hashSubPlans[0].getInnerPlan());
        Expression postFilterExpression = subHashJoinPlan.getJoinInfo().getPostJoinFilterExpression();
        assertTrue(postFilterExpression.toString().equals(
                "(I.SUPPLIER_ID != 'medi' OR S.ADDRESS = 'hai')"));

        //postFilter references all tables can not push down to subjoin.
        sql = "select /*+ NO_STAR_JOIN */ COALESCE(o.order_id,'empty_order_id'),i.item_id, i.discount2+5, s.supplier_id, lower(s.name) from "+
                supplierTableName + " s inner join " + itemTableName + " i on  s.supplier_id = i.supplier_id "+
                "inner join " + orderTableName + " o on  i.item_id = o.item_id "+
                "where (o.price < 10 or o.price > 20) and "+
                "(i.supplier_id != 'medi' or s.address = 'hai' or o.quantity = 8)";
        queryPlan = TestUtil.getOptimizeQueryPlanNoIterator(conn, sql);
        hashJoinPlan = (HashJoinPlan)queryPlan;
        assertTrue(hashJoinPlan.getJoinInfo().getPostJoinFilterExpression().toString().equals(
                "(I.SUPPLIER_ID != 'medi' OR S.ADDRESS = 'hai' OR O.QUANTITY = 8)"));
        hashSubPlans = (HashSubPlan[])hashJoinPlan.getSubPlans();
        assertTrue(hashSubPlans.length == 1);
        subHashJoinPlan = (HashJoinPlan)(hashSubPlans[0].getInnerPlan());
        assertTrue(subHashJoinPlan.getJoinInfo().getPostJoinFilterExpression() == null);

        //one condition can not push down and other two conditions can push down.
        sql = "select /*+ NO_STAR_JOIN */ COALESCE(o.order_id,'empty_order_id'),i.item_id, i.discount2+5, s.supplier_id, lower(s.name) from "+
                supplierTableName + " s inner join " + itemTableName + " i on  s.supplier_id = i.supplier_id "+
                "inner join " + orderTableName + " o on  i.item_id = o.item_id "+
                "where (o.price < 10 or o.price > 20) and "+
                "(i.description= 'desc1' or o.quantity > 10) and (i.supplier_id != 'medi' or s.address = 'hai') and (i.name is not null or s.loc_id != '8')";
        queryPlan = TestUtil.getOptimizeQueryPlanNoIterator(conn, sql);
        hashJoinPlan = (HashJoinPlan)queryPlan;
        assertTrue(hashJoinPlan.getJoinInfo().getPostJoinFilterExpression().toString().equals(
                "(I.DESCRIPTION = 'desc1' OR O.QUANTITY > 10)"));
        hashSubPlans = (HashSubPlan[])hashJoinPlan.getSubPlans();
        assertTrue(hashSubPlans.length == 1);
        subHashJoinPlan = (HashJoinPlan)(hashSubPlans[0].getInnerPlan());
        postFilterExpression = subHashJoinPlan.getJoinInfo().getPostJoinFilterExpression();
        assertTrue(postFilterExpression.toString().equals(
                "((I.SUPPLIER_ID != 'medi' OR S.ADDRESS = 'hai') AND (I.NAME IS NOT NULL OR S.LOC_ID != '8'))"));

        //for right join,can not push down
        sql = "select /*+ NO_STAR_JOIN */ COALESCE(o.order_id,'empty_order_id'),i.item_id, i.discount2+5, s.supplier_id, lower(s.name) from "+
                supplierTableName + " s inner join " + itemTableName + " i on  s.supplier_id = i.supplier_id "+
                "right join " + orderTableName + " o on  i.item_id = o.item_id "+
                "where (o.price < 10 or o.price > 20) and "+
                "(i.supplier_id != 'medi' or s.address = 'hai')";
        queryPlan = TestUtil.getOptimizeQueryPlanNoIterator(conn, sql);
        hashJoinPlan = (HashJoinPlan)queryPlan;
        assertTrue(hashJoinPlan.getJoinInfo().getPostJoinFilterExpression().toString().equals(
                "(I.SUPPLIER_ID != 'medi' OR S.ADDRESS = 'hai')"));
        hashSubPlans = (HashSubPlan[])hashJoinPlan.getSubPlans();
        assertTrue(hashSubPlans.length == 1);
        subHashJoinPlan = (HashJoinPlan)(hashSubPlans[0].getInnerPlan());
        assertTrue(subHashJoinPlan.getJoinInfo().getPostJoinFilterExpression() == null);

        //for right join,can not push down
        sql = "select /*+ NO_STAR_JOIN */ COALESCE(o.order_id,'empty_order_id'),i.item_id, i.discount2+5, s.supplier_id, lower(s.name) from "+
                supplierTableName + " s inner join " + itemTableName + " i on  s.supplier_id = i.supplier_id "+
                "right join " + orderTableName + " o on  i.item_id = o.item_id "+
                "where (o.price < 10 or o.price > 20) and "+
                "(i.description= 'desc1' or o.quantity > 10) and (i.supplier_id != 'medi' or s.address = 'hai') and (i.name is not null or s.loc_id != '8')";
        queryPlan = TestUtil.getOptimizeQueryPlanNoIterator(conn, sql);
        hashJoinPlan = (HashJoinPlan)queryPlan;
        assertTrue(hashJoinPlan.getJoinInfo().getPostJoinFilterExpression().toString().equals(
                "((I.DESCRIPTION = 'desc1' OR O.QUANTITY > 10) AND (I.SUPPLIER_ID != 'medi' OR S.ADDRESS = 'hai') AND (I.NAME IS NOT NULL OR S.LOC_ID != '8'))"));
        hashSubPlans = (HashSubPlan[])hashJoinPlan.getSubPlans();
        assertTrue(hashSubPlans.length == 1);
        subHashJoinPlan = (HashJoinPlan)(hashSubPlans[0].getInnerPlan());
        assertTrue(subHashJoinPlan.getJoinInfo().getPostJoinFilterExpression() == null);
    }

    private void doTestPushDownPostFilterToSubJoinForSortMergeJoinBug5389(
            Connection conn,
            String supplierTableName,
            String itemTableName,
            String orderTableName) throws Exception {
        //one condition push down.
        String sql = "select /*+ USE_SORT_MERGE_JOIN */ COALESCE(o.order_id,'empty_order_id'),i.item_id, i.discount2+5, s.supplier_id, lower(s.name) from "+
                supplierTableName+" s inner join "+itemTableName+" i on  s.supplier_id = i.supplier_id "+
                "inner join "+orderTableName+" o on  i.item_id = o.item_id "+
                "where (o.price < 10 or o.price > 20) and "+
                "(i.supplier_id != 'medi' or s.address = 'hai')";
        QueryPlan queryPlan = TestUtil.getOptimizeQueryPlanNoIterator(conn, sql);
        ClientScanPlan clientScanPlan = (ClientScanPlan)queryPlan;
        assertTrue(clientScanPlan.getWhere() == null);
        SortMergeJoinPlan sortMergeJoinPlan = (SortMergeJoinPlan)clientScanPlan.getDelegate();
        ClientScanPlan lhsClientScanPlan = (ClientScanPlan)sortMergeJoinPlan.getLhsPlan();
        assertTrue(lhsClientScanPlan.getWhere().toString().equals(
                "(I.SUPPLIER_ID != 'medi' OR S.ADDRESS = 'hai')"));

        //can not push down to subjoin.
        sql = "select /*+ USE_SORT_MERGE_JOIN */ COALESCE(o.order_id,'empty_order_id'),i.item_id, i.discount2+5, s.supplier_id, lower(s.name) from "+
                supplierTableName+" s inner join "+itemTableName+" i on  s.supplier_id = i.supplier_id "+
                "inner join "+orderTableName+" o on  i.item_id = o.item_id "+
                "where (o.price < 10 or o.price > 20) and "+
                "(i.supplier_id != 'medi' or s.address = 'hai' or o.quantity = 8)";
        queryPlan = TestUtil.getOptimizeQueryPlanNoIterator(conn, sql);
        clientScanPlan = (ClientScanPlan)queryPlan;
        assertTrue(clientScanPlan.getWhere().toString().equals(
                "(I.SUPPLIER_ID != 'medi' OR S.ADDRESS = 'hai' OR O.QUANTITY = 8)"));
        sortMergeJoinPlan = (SortMergeJoinPlan)clientScanPlan.getDelegate();
        lhsClientScanPlan = (ClientScanPlan)sortMergeJoinPlan.getLhsPlan();
        assertTrue(lhsClientScanPlan.getWhere() == null);

        //one condition can not push down and other two conditions can push down.
        sql = "select /*+ USE_SORT_MERGE_JOIN */ COALESCE(o.order_id,'empty_order_id'),i.item_id, i.discount2+5, s.supplier_id, lower(s.name) from "+
                supplierTableName+" s inner join "+itemTableName+" i on  s.supplier_id = i.supplier_id "+
                "inner join "+orderTableName+" o on  i.item_id = o.item_id "+
                "where (o.price < 10 or o.price > 20) and "+
                "(i.description= 'desc1' or o.quantity > 10) and (i.supplier_id != 'medi' or s.address = 'hai') and (i.name is not null or s.loc_id != '8')";
        queryPlan = TestUtil.getOptimizeQueryPlanNoIterator(conn, sql);
        clientScanPlan = (ClientScanPlan)queryPlan;
        assertTrue(clientScanPlan.getWhere().toString().equals(
                "(I.DESCRIPTION = 'desc1' OR O.QUANTITY > 10)"));
        sortMergeJoinPlan = (SortMergeJoinPlan)clientScanPlan.getDelegate();
        lhsClientScanPlan = (ClientScanPlan)sortMergeJoinPlan.getLhsPlan();
        assertTrue(lhsClientScanPlan.getWhere().toString().equals(
                "((I.SUPPLIER_ID != 'medi' OR S.ADDRESS = 'hai') AND (I.NAME IS NOT NULL OR S.LOC_ID != '8'))"));

       //for right join,can not push down
        sql = "select /*+ USE_SORT_MERGE_JOIN */ COALESCE(o.order_id,'empty_order_id'),i.item_id, i.discount2+5, s.supplier_id, lower(s.name) from "+
                supplierTableName+" s inner join "+itemTableName+" i on  s.supplier_id = i.supplier_id "+
                "right join "+orderTableName+" o on  i.item_id = o.item_id "+
                "where (o.price < 10 or o.price > 20) and "+
                "(i.supplier_id != 'medi' or s.address = 'hai')";
        queryPlan = TestUtil.getOptimizeQueryPlanNoIterator(conn, sql);
        clientScanPlan = (ClientScanPlan)queryPlan;
        assertTrue(clientScanPlan.getWhere().toString().equals(
                "(I.SUPPLIER_ID != 'medi' OR S.ADDRESS = 'hai')"));
        sortMergeJoinPlan = (SortMergeJoinPlan)clientScanPlan.getDelegate();
        //for right join, SortMergeJoinPlan exchanges left and right
        ClientScanPlan rhsClientScanPlan = (ClientScanPlan)sortMergeJoinPlan.getRhsPlan();
        assertTrue(rhsClientScanPlan.getWhere() == null);

        //for full join,can not push down
        sql = "select /*+ USE_SORT_MERGE_JOIN */ COALESCE(o.order_id,'empty_order_id'),i.item_id, i.discount2+5, s.supplier_id, lower(s.name) from "+
                supplierTableName+" s inner join "+itemTableName+" i on  s.supplier_id = i.supplier_id "+
                "full join "+orderTableName+" o on  i.item_id = o.item_id "+
                "where (o.price < 10 or o.price > 20) and "+
                "(i.description= 'desc1' or o.quantity > 10) and (i.supplier_id != 'medi' or s.address = 'hai') and (i.name is not null or s.loc_id != '8')";
        queryPlan = TestUtil.getOptimizeQueryPlanNoIterator(conn, sql);
        clientScanPlan = (ClientScanPlan)queryPlan;
        assertTrue(clientScanPlan.getWhere().toString().equals(
                "((O.PRICE < 10 OR O.PRICE > 20) AND (I.DESCRIPTION = 'desc1' OR O.QUANTITY > 10) AND (I.SUPPLIER_ID != 'medi' OR S.ADDRESS = 'hai') AND (I.NAME IS NOT NULL OR S.LOC_ID != '8'))"));
        sortMergeJoinPlan = (SortMergeJoinPlan)clientScanPlan.getDelegate();
        lhsClientScanPlan = (ClientScanPlan)sortMergeJoinPlan.getLhsPlan();
        assertTrue(lhsClientScanPlan.getWhere() == null);
    }

    @Test
    public void testSubselectColumnPruneForJoinBug5451() throws Exception {
        PhoenixConnection conn = null;
        try {
            conn = DriverManager.getConnection(getUrl()).unwrap(PhoenixConnection.class);
            String sql = null;
            QueryPlan queryPlan = null;
            //testNestedDerivedTable require index with same name be created
            String tableName = "testA";
            sql = "create table " + tableName +
            "   (organization_id char(15) not null, \n" +
            "    entity_id char(15) not null,\n" +
            "    a_string varchar(100),\n" +
            "    b_string varchar(100),\n" +
            "    a_integer integer,\n" +
            "    a_date date,\n" +
            "    a_time time,\n" +
            "    a_timestamp timestamp,\n" +
            "    x_decimal decimal(31,10),\n" +
            "    x_long bigint,\n" +
            "    x_integer integer,\n" +
            "    y_integer integer,\n" +
            "    a_byte tinyint,\n" +
            "    a_short smallint,\n" +
            "    a_float float,\n" +
            "    a_double double,\n" +
            "    a_unsigned_float unsigned_float,\n" +
            "    a_unsigned_double unsigned_double\n" +
            "    CONSTRAINT pk PRIMARY KEY (organization_id, entity_id)\n" +
            ") ";
            conn.createStatement().execute(sql);

            //test for subquery
            sql = "SELECT q.id, q.x10 * 10 FROM " +
                  "(SELECT t.eid id, t.x + 9 x10, t.astr a, t.bstr b, aint ai, adouble ad FROM "+
                    "(SELECT entity_id eid, a_string astr, b_string bstr, a_integer aint, a_double adouble, a_byte + 1 x FROM " + tableName + " WHERE a_byte + 1 < 9 limit 2) AS t "+
                  "ORDER BY b, id limit 3) AS q WHERE q.a = 'a' OR q.b = 'b' OR q.b = 'c'";
            queryPlan = TestUtil.getOptimizeQueryPlanNoIterator(conn, sql);
            ClientScanPlan clientScanPlan = (ClientScanPlan)queryPlan;
            TestUtil.assertSelectStatement(clientScanPlan.getStatement(),
                    "SELECT Q.ID,(Q.X10 * 10) FROM "+
                    "(SELECT T.EID ID,(T.X + 9) X10,T.ASTR A,T.BSTR B FROM "+
                     "(SELECT ENTITY_ID EID,A_STRING ASTR,B_STRING BSTR,A_INTEGER AINT,A_DOUBLE ADOUBLE,(A_BYTE + 1) X FROM TESTA  WHERE (A_BYTE + 1) < 9 LIMIT 2) T "+
                    "ORDER BY T.BSTR,T.EID LIMIT 3) Q WHERE (Q.A = 'a' OR Q.B = 'b' OR Q.B = 'c')");
            clientScanPlan =
                    (ClientScanPlan)((TupleProjectionPlan)clientScanPlan.getDelegate()).getDelegate();
            TestUtil.assertSelectStatement(clientScanPlan.getStatement(),
                    "SELECT T.EID ID,(T.X + 9) X10,T.ASTR A,T.BSTR B FROM "+
                    "(SELECT ENTITY_ID EID,A_STRING ASTR,B_STRING BSTR,(A_BYTE + 1) X FROM TESTA  WHERE (A_BYTE + 1) < 9 LIMIT 2) T "+
                    "ORDER BY T.BSTR,T.EID LIMIT 3");
            ScanPlan scanPlan =
                    (ScanPlan)((TupleProjectionPlan)clientScanPlan.getDelegate()).getDelegate();
            TestUtil.assertSelectStatement(
                    scanPlan.getStatement(),
                    "SELECT ENTITY_ID EID,A_STRING ASTR,B_STRING BSTR,(A_BYTE + 1) X FROM TESTA  WHERE (A_BYTE + 1) < 9 LIMIT 2");

            //test for subquery with wildcard
            sql = "SELECT * FROM " +
                  "(SELECT t.eid id, t.x + 9 x10, t.astr a, t.bstr b, aint ai, adouble ad FROM "+
                    "(SELECT entity_id eid, a_string astr, b_string bstr, a_integer aint, a_double adouble, a_byte + 1 x FROM " + tableName + " WHERE a_byte + 1 < 9 limit 2) AS t "+
                  "ORDER BY b, id limit 3) AS q WHERE q.a = 'a' OR q.b = 'b' OR q.b = 'c'";
            queryPlan = TestUtil.getOptimizeQueryPlanNoIterator(conn, sql);
            clientScanPlan = (ClientScanPlan)queryPlan;
            TestUtil.assertSelectStatement(clientScanPlan.getStatement(),
                    "SELECT  *  FROM "+
                    "(SELECT T.EID ID,(T.X + 9) X10,T.ASTR A,T.BSTR B,AINT AI,ADOUBLE AD FROM "+
                     "(SELECT ENTITY_ID EID,A_STRING ASTR,B_STRING BSTR,A_INTEGER AINT,A_DOUBLE ADOUBLE,(A_BYTE + 1) X FROM TESTA  WHERE (A_BYTE + 1) < 9 LIMIT 2) T "+
                    "ORDER BY B,ID LIMIT 3) Q WHERE (Q.A = 'a' OR Q.B = 'b' OR Q.B = 'c')");
            clientScanPlan = (ClientScanPlan)((TupleProjectionPlan)clientScanPlan.getDelegate()).getDelegate();
            TestUtil.assertSelectStatement(clientScanPlan.getStatement(),
                    "SELECT T.EID ID,(T.X + 9) X10,T.ASTR A,T.BSTR B,AINT AI,ADOUBLE AD FROM "+
                    "(SELECT ENTITY_ID EID,A_STRING ASTR,B_STRING BSTR,A_INTEGER AINT,A_DOUBLE ADOUBLE,(A_BYTE + 1) X FROM TESTA  WHERE (A_BYTE + 1) < 9 LIMIT 2) T "+
                    "ORDER BY T.BSTR,T.EID LIMIT 3");
            scanPlan = (ScanPlan)((TupleProjectionPlan)clientScanPlan.getDelegate()).getDelegate();
            TestUtil.assertSelectStatement(
                    scanPlan.getStatement(),
                    "SELECT ENTITY_ID EID,A_STRING ASTR,B_STRING BSTR,A_INTEGER AINT,A_DOUBLE ADOUBLE,(A_BYTE + 1) X FROM TESTA  WHERE (A_BYTE + 1) < 9 LIMIT 2");

            //test for some trival cases of subquery.
            sql = "SELECT count(*) FROM (SELECT count(*) c FROM "+tableName+" ) AS t";
            queryPlan = TestUtil.getOptimizeQueryPlan(conn, sql);
            ClientAggregatePlan clientAggregatePlan = (ClientAggregatePlan)queryPlan;
            TestUtil.assertSelectStatement(clientAggregatePlan.getStatement(), "SELECT  COUNT(1) FROM (SELECT  COUNT(1) C FROM TESTA ) T");
            AggregatePlan aggregatePlan =
                    (AggregatePlan)((TupleProjectionPlan)clientAggregatePlan.getDelegate()).getDelegate();
            TestUtil.assertSelectStatement(aggregatePlan.getStatement(), "SELECT  COUNT(1) C FROM TESTA");

            sql = "SELECT count(*) FROM (SELECT count(*) c FROM "+tableName+" GROUP BY a_string) AS t";
            queryPlan = TestUtil.getOptimizeQueryPlan(conn, sql);
            clientAggregatePlan = (ClientAggregatePlan)queryPlan;
            TestUtil.assertSelectStatement(
                    clientAggregatePlan.getStatement(),
                    "SELECT  COUNT(1) FROM (SELECT  COUNT(1) C FROM TESTA  GROUP BY A_STRING) T");
            aggregatePlan =
                    (AggregatePlan)((TupleProjectionPlan)clientAggregatePlan.getDelegate()).getDelegate();
            TestUtil.assertSelectStatement(
                    aggregatePlan.getStatement(),
                    "SELECT  COUNT(1) C FROM TESTA  GROUP BY A_STRING");

            sql = "SELECT 1 FROM (SELECT count(*) c FROM "+tableName+" GROUP BY a_string) AS t";
            queryPlan = TestUtil.getOptimizeQueryPlan(conn, sql);
            aggregatePlan = (AggregatePlan)queryPlan;
            TestUtil.assertSelectStatement(aggregatePlan.getStatement(), "SELECT 1 FROM TESTA  GROUP BY A_STRING");

            sql = "SELECT count(*) FROM (SELECT DISTINCT a_string FROM "+tableName+") AS t";
            queryPlan = TestUtil.getOptimizeQueryPlan(conn, sql);
            clientAggregatePlan = (ClientAggregatePlan)queryPlan;
            TestUtil.assertSelectStatement(clientAggregatePlan.getStatement(), "SELECT  COUNT(1) FROM (SELECT DISTINCT A_STRING FROM TESTA ) T");
            aggregatePlan =
                    (AggregatePlan)((TupleProjectionPlan)clientAggregatePlan.getDelegate()).getDelegate();
            TestUtil.assertSelectStatement(aggregatePlan.getStatement(), "SELECT DISTINCT A_STRING FROM TESTA");

            //test for hash join
            sql = "SELECT q1.id, q2.id FROM (SELECT t.eid id, t.astr a, t.bstr b FROM (SELECT entity_id eid, a_string astr, b_string bstr, a_byte abyte FROM "+tableName+") AS t WHERE t.abyte >= 8) AS q1"
                    + " JOIN (SELECT t.eid id, t.astr a, t.bstr b, t.abyte x FROM (SELECT entity_id eid, a_string astr, b_string bstr, a_byte abyte FROM "+tableName+") AS t) AS q2 ON q1.a = q2.b"
                    + " WHERE q2.x != 5 ORDER BY q1.id, q2.id DESC";
            JoinTable joinTablesContext = TestUtil.getJoinTable(sql, conn);
            Table leftmostTableContext = joinTablesContext.getLeftTable();
            TestUtil.assertSelectStatement(
                    leftmostTableContext.getSubselectStatement(),
                    "SELECT ENTITY_ID ID,A_STRING A FROM TESTA  WHERE A_BYTE >= 8");
            assertTrue(leftmostTableContext.getPreFilterParseNodes().isEmpty());

            Table rightTableContext = joinTablesContext.getJoinSpecs().get(0).getRhsJoinTable().getLeftTable();
            TestUtil.assertSelectStatement(rightTableContext.getSubselectStatement(), "SELECT ENTITY_ID ID,B_STRING B FROM TESTA");
            assertTrue(rightTableContext.getPreFilterParseNodes().size() == 1);
            assertTrue(rightTableContext.getPreFilterParseNodes().get(0).toString().equals("A_BYTE != 5"));

            queryPlan = TestUtil.getOptimizeQueryPlanNoIterator(conn, sql);
            HashJoinPlan hashJoinPlan = (HashJoinPlan)queryPlan;
            Scan scan = hashJoinPlan.getContext().getScan();
            TupleProjector tupleColumnProjector =
                    TupleProjector.deserializeProjectorFromScan(scan);
            Expression[] expressions = tupleColumnProjector.getExpressions();
            assertTrue(expressions.length == 2);

            TestUtil.assertSelectStatement(
                    hashJoinPlan.getDelegate().getStatement(),
                    "SELECT Q1.ID,Q2.ID FROM TESTA  WHERE A_BYTE >= 8 ORDER BY Q1.ID,Q2.ID DESC");
            HashSubPlan[] hashSubPlans = (HashSubPlan[])hashJoinPlan.getSubPlans();
            assertTrue(hashSubPlans.length == 1);
            scanPlan =(ScanPlan)((TupleProjectionPlan)(hashSubPlans[0].getInnerPlan())).getDelegate();
            TestUtil.assertSelectStatement(
                    scanPlan.getStatement(),
                    "SELECT ENTITY_ID ID,B_STRING B FROM TESTA  WHERE A_BYTE != 5");

            //test for hash join with wildcard
            sql = "SELECT * FROM (SELECT t.eid id, t.astr a, t.bstr b FROM (SELECT entity_id eid, a_string astr, b_string bstr, a_byte abyte FROM "+tableName+") AS t WHERE t.abyte >= 8) AS q1"
                    + " JOIN (SELECT t.eid id, t.astr a, t.bstr b, t.abyte x FROM (SELECT entity_id eid, a_string astr, b_string bstr, a_byte abyte FROM "+tableName+") AS t) AS q2 ON q1.a = q2.b"
                    + " WHERE q2.x != 5 ORDER BY q1.id, q2.id DESC";
            joinTablesContext = TestUtil.getJoinTable(sql, conn);
            leftmostTableContext = joinTablesContext.getLeftTable();
            TestUtil.assertSelectStatement(
                    leftmostTableContext.getSubselectStatement(),
                    "SELECT ENTITY_ID ID,A_STRING A,B_STRING B FROM TESTA  WHERE A_BYTE >= 8");
            assertTrue(leftmostTableContext.getPreFilterParseNodes().isEmpty());

            rightTableContext = joinTablesContext.getJoinSpecs().get(0).getRhsJoinTable().getLeftTable();
            TestUtil.assertSelectStatement(
                    rightTableContext.getSubselectStatement(),
                    "SELECT ENTITY_ID ID,A_STRING A,B_STRING B,A_BYTE X FROM TESTA");
            assertTrue(rightTableContext.getPreFilterParseNodes().size() == 1);
            assertTrue(rightTableContext.getPreFilterParseNodes().get(0).toString().equals("A_BYTE != 5"));

            queryPlan = TestUtil.getOptimizeQueryPlanNoIterator(conn, sql);
            hashJoinPlan = (HashJoinPlan)queryPlan;
            scan = hashJoinPlan.getContext().getScan();
            tupleColumnProjector =
                    TupleProjector.deserializeProjectorFromScan(scan);
            expressions = tupleColumnProjector.getExpressions();
            assertTrue(expressions.length == 3);

            TestUtil.assertSelectStatement(
                    hashJoinPlan.getDelegate().getStatement(),
                    "SELECT Q1.*,Q2.* FROM TESTA  WHERE A_BYTE >= 8 ORDER BY Q1.ID,Q2.ID DESC");
            hashSubPlans = (HashSubPlan[])hashJoinPlan.getSubPlans();
            assertTrue(hashSubPlans.length == 1);
            scanPlan = (ScanPlan)((TupleProjectionPlan)(hashSubPlans[0].getInnerPlan())).getDelegate();
            TestUtil.assertSelectStatement(
                    scanPlan.getStatement(),
                    "SELECT ENTITY_ID ID,A_STRING A,B_STRING B,A_BYTE X FROM TESTA  WHERE A_BYTE != 5");

            //test for sortmergejoin
            sql = "SELECT /*+ USE_SORT_MERGE_JOIN */ q1.id, q2.id FROM " +
                  "(SELECT t.eid id, t.astr a, t.bstr b FROM (SELECT entity_id eid, a_string astr, b_string bstr, a_byte abyte FROM "+tableName+") AS t WHERE t.abyte >= 8) AS q1 " +
                  "JOIN (SELECT t.eid id, t.astr a, t.bstr b, t.abyte x FROM (SELECT entity_id eid, a_string astr, b_string bstr, a_byte abyte FROM "+tableName+") AS t) AS q2 "+
                  "ON q1.a = q2.b WHERE q2.x != 5 ORDER BY q1.id, q2.id DESC";
            queryPlan = TestUtil.getOptimizeQueryPlanNoIterator(conn, sql);
            clientScanPlan = (ClientScanPlan)queryPlan;
            SortMergeJoinPlan sortMergeJoinPlan = (SortMergeJoinPlan)clientScanPlan.getDelegate();
            ScanPlan lhsPlan =
                    (ScanPlan)((TupleProjectionPlan)sortMergeJoinPlan.getLhsPlan()).getDelegate();
            TestUtil.assertSelectStatement(
                    lhsPlan.getStatement(),
                    "SELECT ENTITY_ID ID,A_STRING A FROM TESTA  WHERE A_BYTE >= 8 ORDER BY A_STRING");
            ScanPlan rhsPlan =
                    (ScanPlan)((TupleProjectionPlan)sortMergeJoinPlan.getRhsPlan()).getDelegate();
            TestUtil.assertSelectStatement(
                    rhsPlan.getStatement(),
                    "SELECT ENTITY_ID ID,B_STRING B FROM TESTA  WHERE A_BYTE != 5 ORDER BY B_STRING");

            //test for sortmergejoin with wildcard
            sql = "SELECT /*+ USE_SORT_MERGE_JOIN */ * FROM "+
                  "(SELECT t.eid id, t.astr a, t.bstr b FROM (SELECT entity_id eid, a_string astr, b_string bstr, a_byte abyte FROM "+tableName+") AS t WHERE t.abyte >= 8) AS q1 "+
                  "JOIN (SELECT t.eid id, t.astr a, t.bstr b, t.abyte x FROM (SELECT entity_id eid, a_string astr, b_string bstr, a_byte abyte FROM "+tableName+") AS t) AS q2 "+
                  "ON q1.a = q2.b WHERE q2.x != 5 ORDER BY q1.id, q2.id DESC";
            queryPlan = TestUtil.getOptimizeQueryPlanNoIterator(conn, sql);
            clientScanPlan = (ClientScanPlan)queryPlan;
            sortMergeJoinPlan = (SortMergeJoinPlan)clientScanPlan.getDelegate();
            lhsPlan = (ScanPlan)((TupleProjectionPlan)sortMergeJoinPlan.getLhsPlan()).getDelegate();
            TestUtil.assertSelectStatement(lhsPlan.getStatement(),
                    "SELECT ENTITY_ID ID,A_STRING A,B_STRING B FROM TESTA  WHERE A_BYTE >= 8 ORDER BY A_STRING");
            rhsPlan = (ScanPlan)((TupleProjectionPlan)sortMergeJoinPlan.getRhsPlan()).getDelegate();
            TestUtil.assertSelectStatement(rhsPlan.getStatement(),
                   "SELECT ENTITY_ID ID,A_STRING A,B_STRING B,A_BYTE X FROM TESTA  WHERE A_BYTE != 5 ORDER BY B_STRING");
        } finally {
            conn.close();
        }
    }

    @Test
    public void testInSubqueryBug6224() throws Exception {
        Connection conn = null;
        try {
            conn = DriverManager.getConnection(getUrl());
            String itemTableName = "item_table";
            String sql ="create table " + itemTableName +
                "   (item_id varchar not null primary key, " +
                "    name varchar, " +
                "    price integer, " +
                "    discount1 integer, " +
                "    discount2 integer, " +
                "    supplier_id varchar, " +
                "    description varchar)";
            conn.createStatement().execute(sql);

            String orderTableName = "order_table";
            sql = "create table " + orderTableName +
                "   (order_id varchar not null primary key, " +
                "    customer_id varchar, " +
                "    item_id varchar, " +
                "    price integer, " +
                "    quantity integer, " +
                "    date timestamp)";
            conn.createStatement().execute(sql);
            //test simple Correlated subquery
            sql= "SELECT item_id, name FROM " + itemTableName + " i WHERE i.item_id IN "+
                    "(SELECT item_id FROM " + orderTableName + " o  where o.price = i.price) ORDER BY name";
            QueryPlan queryPlan= TestUtil.getOptimizeQueryPlanNoIterator(conn, sql);
            assertTrue(queryPlan instanceof HashJoinPlan);
            TestUtil.assertSelectStatement(
                    queryPlan.getStatement(),
                    "SELECT ITEM_ID,NAME FROM ITEM_TABLE I  Semi JOIN (SELECT DISTINCT 1 $3,ITEM_ID $4,O.PRICE $2 FROM ORDER_TABLE O ) $1 "+
                    "ON ((I.ITEM_ID = $1.$4 AND $1.$2 = I.PRICE)) ORDER BY NAME");

            //test Correlated subquery with AggregateFunction but no groupBy
            sql= "SELECT item_id, name FROM " + itemTableName + " i WHERE i.item_id IN "+
                    "(SELECT max(item_id) FROM " + orderTableName + " o  where o.price = i.price) ORDER BY name";
            queryPlan= TestUtil.getOptimizeQueryPlanNoIterator(conn, sql);
            assertTrue(queryPlan instanceof HashJoinPlan);
            TestUtil.assertSelectStatement(
                    queryPlan.getStatement(),
                   "SELECT ITEM_ID,NAME FROM ITEM_TABLE I  Semi JOIN "+
                   "(SELECT DISTINCT 1 $11, MAX(ITEM_ID) $12,O.PRICE $10 FROM ORDER_TABLE O  GROUP BY O.PRICE) $9 "+
                   "ON ((I.ITEM_ID = $9.$12 AND $9.$10 = I.PRICE)) ORDER BY NAME");

            //test Correlated subquery with AggregateFunction with groupBy
            sql= "SELECT item_id, name FROM " + itemTableName + " i WHERE i.item_id IN "+
                    "(SELECT max(item_id) FROM " + orderTableName + " o  where o.price = i.price group by o.customer_id) ORDER BY name";
            queryPlan= TestUtil.getOptimizeQueryPlanNoIterator(conn, sql);
            assertTrue(queryPlan instanceof HashJoinPlan);
            TestUtil.assertSelectStatement(
                    queryPlan.getStatement(),
                    "SELECT ITEM_ID,NAME FROM ITEM_TABLE I  Semi JOIN "+
                    "(SELECT DISTINCT 1 $19, MAX(ITEM_ID) $20,O.PRICE $18 FROM ORDER_TABLE O  GROUP BY O.PRICE,O.CUSTOMER_ID) $17 "+
                    "ON ((I.ITEM_ID = $17.$20 AND $17.$18 = I.PRICE)) ORDER BY NAME");

            //for Correlated subquery, the extracted join condition must be equal expression.
            sql= "SELECT item_id, name FROM " + itemTableName + " i WHERE i.item_id IN "+
                    "(SELECT max(item_id) FROM " + orderTableName + " o  where o.price = i.price or o.quantity > 1 group by o.customer_id) ORDER BY name";
            try {
                queryPlan= TestUtil.getOptimizeQueryPlanNoIterator(conn, sql);
                fail();
            } catch(SQLFeatureNotSupportedException exception) {

            }

            //test Correlated subquery with AggregateFunction with groupBy and is ORed part of the where clause.
            sql= "SELECT item_id, name FROM " + itemTableName + " i WHERE i.item_id IN "+
                    "(SELECT max(item_id) FROM " + orderTableName + " o  where o.price = i.price group by o.customer_id) or i.discount1 > 10 ORDER BY name";
            queryPlan= TestUtil.getOptimizeQueryPlanNoIterator(conn, sql);
            assertTrue(queryPlan instanceof HashJoinPlan);
            TestUtil.assertSelectStatement(
                    queryPlan.getStatement(),
                    "SELECT ITEM_ID,NAME FROM ITEM_TABLE I  Left JOIN "+
                    "(SELECT DISTINCT 1 $28, MAX(ITEM_ID) $29,O.PRICE $27 FROM ORDER_TABLE O  GROUP BY O.PRICE,O.CUSTOMER_ID) $26 "+
                    "ON ((I.ITEM_ID = $26.$29 AND $26.$27 = I.PRICE)) WHERE ($26.$28 IS NOT NULL  OR I.DISCOUNT1 > 10) ORDER BY NAME");

            // test NonCorrelated subquery
            sql= "SELECT item_id, name FROM " + itemTableName + " i WHERE i.item_id IN "+
                    "(SELECT item_id FROM " + orderTableName + " o  where o.price > 8) ORDER BY name";
            queryPlan= TestUtil.getOptimizeQueryPlanNoIterator(conn, sql);
            assertTrue(queryPlan instanceof HashJoinPlan);
            TestUtil.assertSelectStatement(
                    queryPlan.getStatement(),
                    "SELECT ITEM_ID,NAME FROM ITEM_TABLE I  Semi JOIN "+
                    "(SELECT DISTINCT 1 $35,ITEM_ID $36 FROM ORDER_TABLE O  WHERE O.PRICE > 8) $34 ON (I.ITEM_ID = $34.$36) ORDER BY NAME");

            sql= "SELECT item_id, name FROM " + itemTableName + " i WHERE i.item_id IN "+
                    "(SELECT max(item_id) FROM " + orderTableName + " o  where o.price > 8) ORDER BY name";
            queryPlan= TestUtil.getOptimizeQueryPlanNoIterator(conn, sql);
            assertTrue(queryPlan instanceof HashJoinPlan);
            TestUtil.assertSelectStatement(
                    queryPlan.getStatement(),
                    "SELECT ITEM_ID,NAME FROM ITEM_TABLE I  Semi JOIN "+
                    "(SELECT DISTINCT 1 $42, MAX(ITEM_ID) $43 FROM ORDER_TABLE O  WHERE O.PRICE > 8) $41 ON (I.ITEM_ID = $41.$43) ORDER BY NAME");

            sql= "SELECT item_id, name FROM " + itemTableName + " i WHERE i.item_id IN "+
                    "(SELECT max(item_id) FROM " + orderTableName + " o  where o.price > 8 group by o.customer_id,o.item_id) ORDER BY name";
            queryPlan= TestUtil.getOptimizeQueryPlanNoIterator(conn, sql);
            assertTrue(queryPlan instanceof HashJoinPlan);
            TestUtil.assertSelectStatement(
                    queryPlan.getStatement(),
                    "SELECT ITEM_ID,NAME FROM ITEM_TABLE I  Semi JOIN "+
                    "(SELECT DISTINCT 1 $49, MAX(ITEM_ID) $50 FROM ORDER_TABLE O  WHERE O.PRICE > 8 GROUP BY O.CUSTOMER_ID,O.ITEM_ID) $48 "+
                    "ON (I.ITEM_ID = $48.$50) ORDER BY NAME");

            sql= "SELECT item_id, name FROM " + itemTableName + " i WHERE i.item_id IN "+
                    "(SELECT max(item_id) FROM " + orderTableName + " o  where o.price > 8 group by o.customer_id,o.item_id) or i.discount1 > 10 ORDER BY name";
            queryPlan= TestUtil.getOptimizeQueryPlanNoIterator(conn, sql);
            assertTrue(queryPlan instanceof HashJoinPlan);
            TestUtil.assertSelectStatement(
                    queryPlan.getStatement(),
                    "SELECT ITEM_ID,NAME FROM ITEM_TABLE I  Left JOIN "+
                    "(SELECT DISTINCT 1 $56, MAX(ITEM_ID) $57 FROM ORDER_TABLE O  WHERE O.PRICE > 8 GROUP BY O.CUSTOMER_ID,O.ITEM_ID) $55 "+
                    "ON (I.ITEM_ID = $55.$57) WHERE ($55.$56 IS NOT NULL  OR I.DISCOUNT1 > 10) ORDER BY NAME");
        } finally {
            conn.close();
        }
    }

    @Test
    public void testHashJoinBug6232() throws Exception {
        Connection conn = null;
        try {
            conn = DriverManager.getConnection(getUrl());
            String sql ="CREATE TABLE test (" +
                    "      id INTEGER NOT NULL," +
                    "      test_id INTEGER," +
                    "      lastchanged TIMESTAMP," +
                    "      CONSTRAINT my_pk PRIMARY KEY (id))";
            conn.createStatement().execute(sql);

            //test for LHS is Correlated subquery,the RHS would be as the probe side of Hash join.
            sql= "SELECT AAA.* FROM " +
                    "(SELECT id, test_id, lastchanged FROM test T " +
                    "  WHERE lastchanged = ( SELECT max(lastchanged) FROM test WHERE test_id = T.test_id )) AAA " +
                    "inner join " +
                    "(SELECT id FROM test) BBB " +
                    "on AAA.id = BBB.id";
            QueryPlan queryPlan= TestUtil.getOptimizeQueryPlanNoIterator(conn, sql);
            assertTrue(queryPlan instanceof HashJoinPlan);
            HashJoinPlan hashJoinPlan = (HashJoinPlan)queryPlan;
            assertTrue(hashJoinPlan.getDelegate() instanceof ScanPlan);
            TestUtil.assertSelectStatement(
                    hashJoinPlan.getDelegate().getStatement(), "SELECT AAA.* FROM TEST");
            SubPlan[] subPlans = hashJoinPlan.getSubPlans();
            assertTrue(subPlans.length == 1);
            assertTrue(subPlans[0] instanceof HashSubPlan);
            assertTrue(subPlans[0].getInnerPlan() instanceof TupleProjectionPlan);
            assertTrue(
               ((TupleProjectionPlan)(subPlans[0].getInnerPlan())).getDelegate() instanceof HashJoinPlan);

            //test for LHS is Correlated subquery,the RHS could not as the probe side of hash join,
            //so use SortMergeJoinPlan
            sql= "SELECT AAA.* FROM " +
                    "(SELECT id, test_id, lastchanged FROM test T " +
                    "  WHERE lastchanged = ( SELECT max(lastchanged) FROM test WHERE test_id = T.test_id )) AAA " +
                    "inner join " +
                    "(SELECT id FROM test limit 10) BBB " +
                    "on AAA.id = BBB.id";
           queryPlan= TestUtil.getOptimizeQueryPlanNoIterator(conn, sql);
           assertTrue(queryPlan instanceof ClientScanPlan);
           assertTrue(((ClientScanPlan)queryPlan).getDelegate() instanceof SortMergeJoinPlan);

           //test for LHS is NonCorrelated subquery ,would use HashJoin.
           String GRAMMAR_TABLE = "CREATE TABLE IF NOT EXISTS GRAMMAR_TABLE (ID INTEGER PRIMARY KEY, " +
                   "unsig_id UNSIGNED_INT, big_id BIGINT, unsig_long_id UNSIGNED_LONG, tiny_id TINYINT," +
                   "unsig_tiny_id UNSIGNED_TINYINT, small_id SMALLINT, unsig_small_id UNSIGNED_SMALLINT," +
                   "float_id FLOAT, unsig_float_id UNSIGNED_FLOAT, double_id DOUBLE, unsig_double_id UNSIGNED_DOUBLE," +
                   "decimal_id DECIMAL, boolean_id BOOLEAN, time_id TIME, date_id DATE, timestamp_id TIMESTAMP," +
                   "unsig_time_id TIME, unsig_date_id DATE, unsig_timestamp_id TIMESTAMP, varchar_id VARCHAR (30)," +
                   "char_id CHAR (30), binary_id BINARY (100), varbinary_id VARBINARY (100))";
           conn.createStatement().execute(GRAMMAR_TABLE);

           String LARGE_TABLE = "CREATE TABLE IF NOT EXISTS LARGE_TABLE (ID INTEGER PRIMARY KEY, " +
                   "unsig_id UNSIGNED_INT, big_id BIGINT, unsig_long_id UNSIGNED_LONG, tiny_id TINYINT," +
                   "unsig_tiny_id UNSIGNED_TINYINT, small_id SMALLINT, unsig_small_id UNSIGNED_SMALLINT," +
                   "float_id FLOAT, unsig_float_id UNSIGNED_FLOAT, double_id DOUBLE, unsig_double_id UNSIGNED_DOUBLE," +
                   "decimal_id DECIMAL, boolean_id BOOLEAN, time_id TIME, date_id DATE, timestamp_id TIMESTAMP," +
                   "unsig_time_id TIME, unsig_date_id DATE, unsig_timestamp_id TIMESTAMP, varchar_id VARCHAR (30)," +
                   "char_id CHAR (30), binary_id BINARY (100), varbinary_id VARBINARY (100))";
           conn.createStatement().execute(LARGE_TABLE);

           String SECONDARY_LARGE_TABLE = "CREATE TABLE IF NOT EXISTS SECONDARY_LARGE_TABLE (SEC_ID INTEGER PRIMARY KEY," +
                   "sec_unsig_id UNSIGNED_INT, sec_big_id BIGINT, sec_usnig_long_id UNSIGNED_LONG, sec_tiny_id TINYINT," +
                   "sec_unsig_tiny_id UNSIGNED_TINYINT, sec_small_id SMALLINT, sec_unsig_small_id UNSIGNED_SMALLINT," +
                   "sec_float_id FLOAT, sec_unsig_float_id UNSIGNED_FLOAT, sec_double_id DOUBLE, sec_unsig_double_id UNSIGNED_DOUBLE," +
                   "sec_decimal_id DECIMAL, sec_boolean_id BOOLEAN, sec_time_id TIME, sec_date_id DATE," +
                   "sec_timestamp_id TIMESTAMP, sec_unsig_time_id TIME, sec_unsig_date_id DATE, sec_unsig_timestamp_id TIMESTAMP," +
                   "sec_varchar_id VARCHAR (30), sec_char_id CHAR (30), sec_binary_id BINARY (100), sec_varbinary_id VARBINARY (100))";
           conn.createStatement().execute(SECONDARY_LARGE_TABLE);

          sql = "SELECT * FROM (SELECT ID, BIG_ID, DATE_ID FROM LARGE_TABLE AS A WHERE (A.ID % 5) = 0) AS A " +
                   "INNER JOIN (SELECT SEC_ID, SEC_TINY_ID, SEC_UNSIG_FLOAT_ID FROM SECONDARY_LARGE_TABLE AS B WHERE (B.SEC_ID % 5) = 0) AS B " +
                   "ON A.ID=B.SEC_ID WHERE A.DATE_ID > ALL (SELECT SEC_DATE_ID FROM SECONDARY_LARGE_TABLE LIMIT 100) " +
                   "AND B.SEC_UNSIG_FLOAT_ID = ANY (SELECT sec_unsig_float_id FROM SECONDARY_LARGE_TABLE " +
                   "WHERE SEC_ID > ALL (SELECT MIN (ID) FROM GRAMMAR_TABLE WHERE UNSIG_ID IS NULL) AND " +
                   "SEC_UNSIG_ID < ANY (SELECT DISTINCT(UNSIG_ID) FROM LARGE_TABLE WHERE UNSIG_ID<2500) LIMIT 1000) " +
                   "AND A.ID < 10000";
           queryPlan= TestUtil.getOptimizeQueryPlanNoIterator(conn, sql);
           assertTrue(queryPlan instanceof HashJoinPlan);
           hashJoinPlan = (HashJoinPlan)queryPlan;
           subPlans = hashJoinPlan.getSubPlans();
           assertTrue(subPlans.length == 2);
           assertTrue(subPlans[0] instanceof WhereClauseSubPlan);
           assertTrue(subPlans[1] instanceof HashSubPlan);


           String tableName1 = generateUniqueName();
           String tableName2 = generateUniqueName();

           sql="CREATE TABLE IF NOT EXISTS "+tableName1+" ( "+
                   "AID INTEGER PRIMARY KEY,"+
                   "AGE INTEGER"+
                   ")";
           conn.createStatement().execute(sql);

           sql="CREATE TABLE IF NOT EXISTS "+tableName2+" ( "+
                   "BID INTEGER PRIMARY KEY,"+
                   "CODE INTEGER"+
                   ")";
           conn.createStatement().execute(sql);

           //test for LHS is a flat table and pushed down NonCorrelated subquery as preFiter.
           //would use HashJoin.
           sql="select a.aid from " + tableName1 + " a inner join  "+
                   "(select bid,code from "  + tableName2 + " where code > 10 limit 3) b on a.aid = b.bid "+
                   "where a.age > (select code from " + tableName2 + " c where c.bid = 2) order by a.aid";
           queryPlan= TestUtil.getOptimizeQueryPlanNoIterator(conn, sql);
           assertTrue(queryPlan instanceof HashJoinPlan);
           hashJoinPlan = (HashJoinPlan)queryPlan;
           ScanPlan scanPlan=(ScanPlan)(hashJoinPlan.getDelegate());
           TestUtil.assertSelectStatement(
                   scanPlan.getStatement(),
                   "SELECT A.AID FROM " +tableName1+ " A  WHERE A.AGE > (SELECT CODE FROM " + tableName2 + " C  WHERE C.BID = 2 LIMIT 2) ORDER BY A.AID");
           subPlans = hashJoinPlan.getSubPlans();
           assertTrue(subPlans.length == 2);
           assertTrue(subPlans[0] instanceof WhereClauseSubPlan);
           WhereClauseSubPlan whereClauseSubPlan = (WhereClauseSubPlan)subPlans[0];
           TestUtil.assertSelectStatement(
                   whereClauseSubPlan.getInnerPlan().getStatement(),
                   "SELECT CODE FROM " + tableName2 + " C  WHERE C.BID = 2 LIMIT 2");
           assertTrue(subPlans[1] instanceof HashSubPlan);

           //test for LHS is a subselect and pushed down NonCorrelated subquery as preFiter.
           //would use HashJoin.
           sql="select a.aid from (select aid,age from " + tableName1 + " where age >=11 and age<=33) a inner join  "+
                   "(select bid,code from "  + tableName2 + " where code > 10 limit 3) b on a.aid = b.bid "+
                   "where a.age > (select code from " + tableName2 + " c where c.bid = 2) order by a.aid";
           queryPlan= TestUtil.getOptimizeQueryPlanNoIterator(conn, sql);
           assertTrue(queryPlan instanceof HashJoinPlan);
           hashJoinPlan = (HashJoinPlan)queryPlan;
           scanPlan=(ScanPlan)(hashJoinPlan.getDelegate());
           TestUtil.assertSelectStatement(
                   scanPlan.getStatement(),
                   "SELECT A.AID FROM " +tableName1+ "  WHERE (AGE > (SELECT CODE FROM " + tableName2 + " C  WHERE C.BID = 2 LIMIT 2) AND (AGE >= 11 AND AGE <= 33)) ORDER BY A.AID");
           subPlans = hashJoinPlan.getSubPlans();
           assertTrue(subPlans.length == 2);
           assertTrue(subPlans[0] instanceof WhereClauseSubPlan);
           whereClauseSubPlan = (WhereClauseSubPlan)subPlans[0];
           TestUtil.assertSelectStatement(
                   whereClauseSubPlan.getInnerPlan().getStatement(),
                   "SELECT CODE FROM " + tableName2 + " C  WHERE C.BID = 2 LIMIT 2");
           assertTrue(subPlans[1] instanceof HashSubPlan);

           //test for LHS is a subselect and pushed down aggregate NonCorrelated subquery as preFiter.
           //would use HashJoin.
           sql = "select a.aid from (select aid,age from " + tableName1 + " where age >=11 and age<=33) a inner join  "+
                   "(select bid,code from "  + tableName2 + " where code > 10 limit 3) b on a.aid = b.bid "+
                   "where a.age > (select max(code) from " + tableName2 + " c where c.bid >= 1) order by a.aid";
           queryPlan= TestUtil.getOptimizeQueryPlanNoIterator(conn, sql);
           assertTrue(queryPlan instanceof HashJoinPlan);
           hashJoinPlan = (HashJoinPlan)queryPlan;
           scanPlan=(ScanPlan)(hashJoinPlan.getDelegate());
           TestUtil.assertSelectStatement(
                   scanPlan.getStatement(),
                   "SELECT A.AID FROM " + tableName1 +
                   "  WHERE (AGE > (SELECT  MAX(CODE) FROM " + tableName2 + " C  WHERE C.BID >= 1 LIMIT 2) AND (AGE >= 11 AND AGE <= 33)) ORDER BY A.AID");
           subPlans = hashJoinPlan.getSubPlans();
           assertTrue(subPlans.length == 2);
           assertTrue(subPlans[0] instanceof WhereClauseSubPlan);
           whereClauseSubPlan = (WhereClauseSubPlan)subPlans[0];
           TestUtil.assertSelectStatement(
                   whereClauseSubPlan.getInnerPlan().getStatement(),
                   "SELECT  MAX(CODE) FROM " + tableName2 + " C  WHERE C.BID >= 1 LIMIT 2");
           assertTrue(subPlans[1] instanceof HashSubPlan);

           /**
            * test for LHS is a subselect and has an aggregate Correlated subquery as preFiter,
            * but the aggregate Correlated subquery would be rewrite as HashJoin before
            * {@link JoinCompiler#compile}.
            */
           sql = "select a.aid from (select aid,age from " + tableName1 + " where age >=11 and age<=33) a inner join  "+
                   "(select bid,code from "  + tableName2 + " where code > 10 limit 3) b on a.aid = b.bid "+
                   "where a.age > (select max(code) from " + tableName2 + " c where c.bid = a.aid) order by a.aid";
           queryPlan= TestUtil.getOptimizeQueryPlanNoIterator(conn, sql);
           assertTrue(queryPlan instanceof HashJoinPlan);
           hashJoinPlan = (HashJoinPlan)queryPlan;
           subPlans = hashJoinPlan.getSubPlans();
           assertTrue(subPlans.length == 2);
           assertTrue(subPlans[0] instanceof HashSubPlan);
           assertTrue(subPlans[1] instanceof HashSubPlan);
        } finally {
            conn.close();
        }
    }

    @Test
    public void testExistsSubqueryBug6498() throws Exception {
        Connection conn = null;
        try {
            conn = DriverManager.getConnection(getUrl());
            String itemTableName = "item_table";
            String sql ="create table " + itemTableName +
                "   (item_id varchar not null primary key, " +
                "    name varchar, " +
                "    price integer, " +
                "    discount1 integer, " +
                "    discount2 integer, " +
                "    supplier_id varchar, " +
                "    description varchar)";
            conn.createStatement().execute(sql);

            String orderTableName = "order_table";
            sql = "create table " + orderTableName +
                "   (order_id varchar not null primary key, " +
                "    customer_id varchar, " +
                "    item_id varchar, " +
                "    price integer, " +
                "    quantity integer, " +
                "    date timestamp)";
            conn.createStatement().execute(sql);

            //test simple Correlated subquery
            ParseNodeFactory.setTempAliasCounterValue(0);
            sql= "SELECT item_id, name FROM " + itemTableName + " i WHERE exists "+
                 "(SELECT 1 FROM " + orderTableName + " o  where o.price = i.price and o.quantity = 5 ) ORDER BY name";
            QueryPlan queryPlan = TestUtil.getOptimizeQueryPlanNoIterator(conn, sql);
            assertTrue(queryPlan instanceof HashJoinPlan);
            System.out.println(queryPlan.getStatement());
            TestUtil.assertSelectStatement(
                    queryPlan.getStatement(),
                    "SELECT ITEM_ID,NAME FROM ITEM_TABLE I  Semi JOIN " +
                    "(SELECT DISTINCT 1 $3,O.PRICE $2 FROM ORDER_TABLE O  WHERE O.QUANTITY = 5) $1 "+
                    "ON ($1.$2 = I.PRICE) ORDER BY NAME");

            //test Correlated subquery with AggregateFunction and groupBy
            ParseNodeFactory.setTempAliasCounterValue(0);
            sql= "SELECT item_id, name FROM " + itemTableName + " i WHERE exists "+
                 "(SELECT 1 FROM " + orderTableName + " o  where o.item_id = i.item_id group by customer_id having count(order_id) > 1) " +
                 "ORDER BY name";
            queryPlan = TestUtil.getOptimizeQueryPlanNoIterator(conn, sql);
            assertTrue(queryPlan instanceof HashJoinPlan);
            TestUtil.assertSelectStatement(
                    queryPlan.getStatement(),
                    "SELECT ITEM_ID,NAME FROM ITEM_TABLE I  Semi JOIN " +
                    "(SELECT DISTINCT 1 $3,O.ITEM_ID $2 FROM ORDER_TABLE O  GROUP BY O.ITEM_ID,CUSTOMER_ID HAVING  COUNT(ORDER_ID) > 1) $1 " +
                    "ON ($1.$2 = I.ITEM_ID) ORDER BY NAME");

            //for Correlated subquery, the extracted join condition must be equal expression.
            sql= "SELECT item_id, name FROM " + itemTableName + " i WHERE exists "+
                    "(SELECT 1 FROM " + orderTableName + " o  where o.price = i.price or o.quantity > 1 group by o.customer_id) ORDER BY name";
            try {
                queryPlan= TestUtil.getOptimizeQueryPlanNoIterator(conn, sql);
                fail();
            } catch(SQLFeatureNotSupportedException exception) {

            }

            //test Correlated subquery with AggregateFunction with groupBy and is ORed part of the where clause.
            ParseNodeFactory.setTempAliasCounterValue(0);
            sql= "SELECT item_id, name FROM " + itemTableName + " i WHERE exists "+
                 "(SELECT 1 FROM " + orderTableName + " o  where o.item_id = i.item_id group by customer_id having count(order_id) > 1) "+
                 " or i.discount1 > 10 ORDER BY name";
            queryPlan= TestUtil.getOptimizeQueryPlanNoIterator(conn, sql);
            assertTrue(queryPlan instanceof HashJoinPlan);
            TestUtil.assertSelectStatement(
                    queryPlan.getStatement(),
                    "SELECT ITEM_ID,NAME FROM ITEM_TABLE I  Left JOIN " +
                    "(SELECT DISTINCT 1 $3,O.ITEM_ID $2 FROM ORDER_TABLE O  GROUP BY O.ITEM_ID,CUSTOMER_ID HAVING  COUNT(ORDER_ID) > 1) $1 " +
                    "ON ($1.$2 = I.ITEM_ID) WHERE ($1.$3 IS NOT NULL  OR I.DISCOUNT1 > 10) ORDER BY NAME");

            // test NonCorrelated subquery
            ParseNodeFactory.setTempAliasCounterValue(0);
            sql= "SELECT item_id, name FROM " + itemTableName + " i WHERE exists "+
                    "(SELECT 1 FROM " + orderTableName + " o  where o.price > 8) ORDER BY name";
            queryPlan= TestUtil.getOptimizeQueryPlanNoIterator(conn, sql);
            assertTrue(queryPlan instanceof HashJoinPlan);
            System.out.println(queryPlan.getStatement());
            TestUtil.assertSelectStatement(
                    queryPlan.getStatement(),
                    "SELECT ITEM_ID,NAME FROM ITEM_TABLE I  WHERE  EXISTS (SELECT 1 FROM ORDER_TABLE O  WHERE O.PRICE > 8 LIMIT 1) ORDER BY NAME");

            sql= "SELECT item_id, name FROM " + itemTableName + " i WHERE exists "+
                 "(SELECT 1 FROM " + orderTableName + " o  where o.price > 8 group by o.customer_id,o.item_id having count(order_id) > 1)" +
                 " ORDER BY name";
            queryPlan= TestUtil.getOptimizeQueryPlanNoIterator(conn, sql);
            assertTrue(queryPlan instanceof HashJoinPlan);
            TestUtil.assertSelectStatement(
                    queryPlan.getStatement(),
                    "SELECT ITEM_ID,NAME FROM ITEM_TABLE I  WHERE  EXISTS "+
                    "(SELECT 1 FROM ORDER_TABLE O  WHERE O.PRICE > 8 GROUP BY O.CUSTOMER_ID,O.ITEM_ID HAVING  COUNT(ORDER_ID) > 1 LIMIT 1)" +
                    " ORDER BY NAME");

            sql= "SELECT item_id, name FROM " + itemTableName + " i WHERE exists "+
                 "(SELECT 1 FROM " + orderTableName + " o  where o.price > 8 group by o.customer_id,o.item_id having count(order_id) > 1)" +
                 " or i.discount1 > 10 ORDER BY name";
            queryPlan= TestUtil.getOptimizeQueryPlanNoIterator(conn, sql);
            assertTrue(queryPlan instanceof HashJoinPlan);
            TestUtil.assertSelectStatement(
                    queryPlan.getStatement(),
                    "SELECT ITEM_ID,NAME FROM ITEM_TABLE I  WHERE " +
                    "( EXISTS (SELECT 1 FROM ORDER_TABLE O  WHERE O.PRICE > 8 GROUP BY O.CUSTOMER_ID,O.ITEM_ID HAVING  COUNT(ORDER_ID) > 1 LIMIT 1)" +
                    " OR I.DISCOUNT1 > 10) ORDER BY NAME");
        } finally {
            conn.close();
        }
    }

    @Test
    public void testEliminateUnnecessaryReversedScanBug6798() throws Exception {
        Connection conn = null;
        try {
            conn = DriverManager.getConnection(getUrl());
            String tableName = generateUniqueName();

            String sql =
                    "create table " + tableName + "(group_id integer not null, "
                            + " keyword varchar not null, " + " cost integer, "
                            + " CONSTRAINT TEST_PK PRIMARY KEY (group_id,keyword)) ";
            conn.createStatement().execute(sql);

            /**
             * Test {@link GroupBy#isOrderPreserving} is false and {@link OrderBy} is reversed.
             */
            sql =
                    "select keyword,sum(cost) from " + tableName
                            + " group by keyword order by keyword desc";
            QueryPlan queryPlan = TestUtil.getOptimizeQueryPlan(conn, sql);
            Scan scan = queryPlan.getContext().getScan();
            assertTrue(!queryPlan.getGroupBy().isOrderPreserving());
            assertTrue(queryPlan.getOrderBy() == OrderBy.REV_ROW_KEY_ORDER_BY);
            assertTrue(!ScanUtil.isReversed(scan));

            /**
             * Test {@link GroupBy#isOrderPreserving} is true and {@link OrderBy} is reversed.
             */
            sql =
                    "select keyword,sum(cost) from " + tableName
                            + " group by group_id,keyword order by group_id desc,keyword desc";
            queryPlan = TestUtil.getOptimizeQueryPlan(conn, sql);
            scan = queryPlan.getContext().getScan();
            assertTrue(queryPlan.getGroupBy().isOrderPreserving());
            assertTrue(queryPlan.getOrderBy() == OrderBy.REV_ROW_KEY_ORDER_BY);
            assertTrue(ScanUtil.isReversed(scan));
        } finally {
            conn.close();
        }
    }
}
