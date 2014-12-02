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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;

import java.sql.Array;
import java.sql.Connection;
import java.sql.Date;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

import org.apache.hadoop.hbase.util.Pair;
import org.apache.phoenix.compile.OrderByCompiler.OrderBy;
import org.apache.phoenix.jdbc.PhoenixPreparedStatement;
import org.apache.phoenix.jdbc.PhoenixStatement;
import org.apache.phoenix.query.BaseConnectionlessQueryTest;
import org.apache.phoenix.query.QueryConstants;
import org.apache.phoenix.schema.PTableType;
import org.apache.phoenix.util.PhoenixRuntime;
import org.apache.phoenix.util.QueryUtil;
import org.apache.phoenix.util.SchemaUtil;
import org.junit.Test;

import com.google.common.base.Joiner;
import com.google.common.base.Splitter;

public class QueryOptimizerTest extends BaseConnectionlessQueryTest {
    
    public static final String SCHEMA_NAME = "";
    public static final String DATA_TABLE_NAME = "T";
    public static final String INDEX_TABLE_NAME = "I";
    public static final String DATA_TABLE_FULL_NAME = SchemaUtil.getTableName(SCHEMA_NAME, "T");
    public static final String INDEX_TABLE_FULL_NAME = SchemaUtil.getTableName(SCHEMA_NAME, "I");

    public QueryOptimizerTest() {
    }

    @Test
    public void testRVCUsingPkColsReturnedByPlanShouldUseIndex() throws Exception {
        Connection conn = DriverManager.getConnection(getUrl());
        conn.createStatement().execute("CREATE TABLE T (k VARCHAR NOT NULL PRIMARY KEY, v1 CHAR(15), v2 VARCHAR)");
        conn.createStatement().execute("CREATE INDEX IDX ON T(v1, v2)");
        PhoenixStatement stmt = conn.createStatement().unwrap(PhoenixStatement.class);
        String query = "select * from t where (v1, v2, k) > ('1', '2', '3')";
        QueryPlan plan = stmt.optimizeQuery(query);
        assertEquals("IDX", plan.getTableRef().getTable().getTableName().getString());
    }

    @Test
    public void testOrderByOptimizedOut() throws Exception {
        Connection conn = DriverManager.getConnection(getUrl());
        conn.createStatement().execute("CREATE TABLE foo (k VARCHAR NOT NULL PRIMARY KEY, v VARCHAR) IMMUTABLE_ROWS=true");
        PhoenixStatement stmt = conn.createStatement().unwrap(PhoenixStatement.class);
        QueryPlan plan = stmt.optimizeQuery("SELECT * FROM foo ORDER BY k");
        assertEquals(OrderBy.FWD_ROW_KEY_ORDER_BY,plan.getOrderBy());
    }

    @Test
    public void testOrderByDropped() throws Exception {
        Connection conn = DriverManager.getConnection(getUrl());
        conn.createStatement().execute("CREATE TABLE foo (k VARCHAR NOT NULL PRIMARY KEY, v VARCHAR) IMMUTABLE_ROWS=true");
        PhoenixStatement stmt = conn.createStatement().unwrap(PhoenixStatement.class);
        QueryPlan plan = stmt.optimizeQuery("SELECT * FROM foo ORDER BY 1,2,3");
        assertEquals(OrderBy.EMPTY_ORDER_BY,plan.getOrderBy());
    }

    @Test
    public void testOrderByNotDropped() throws Exception {
        Connection conn = DriverManager.getConnection(getUrl());
        conn.createStatement().execute("CREATE TABLE foo (k VARCHAR NOT NULL PRIMARY KEY, v VARCHAR) IMMUTABLE_ROWS=true");
        PhoenixStatement stmt = conn.createStatement().unwrap(PhoenixStatement.class);
        QueryPlan plan = stmt.optimizeQuery("SELECT * FROM foo ORDER BY v");
        assertFalse(plan.getOrderBy().getOrderByExpressions().isEmpty());
    }
    
    @Test
    public void testOrderByDroppedCompositeKey() throws Exception {
        Connection conn = DriverManager.getConnection(getUrl());
        conn.createStatement().execute("CREATE TABLE foo (j INTEGER NOT NULL, k BIGINT NOT NULL, v VARCHAR CONSTRAINT pk PRIMARY KEY (j,k)) IMMUTABLE_ROWS=true");
        PhoenixStatement stmt = conn.createStatement().unwrap(PhoenixStatement.class);
        QueryPlan plan = stmt.optimizeQuery("SELECT * FROM foo ORDER BY j,k");
        assertEquals(OrderBy.FWD_ROW_KEY_ORDER_BY,plan.getOrderBy());
    }

    @Test
    public void testOrderByNotDroppedCompositeKey() throws Exception {
        Connection conn = DriverManager.getConnection(getUrl());
        conn.createStatement().execute("CREATE TABLE foo (j INTEGER NOT NULL, k BIGINT NOT NULL, v VARCHAR CONSTRAINT pk PRIMARY KEY (j,k)) IMMUTABLE_ROWS=true");
        PhoenixStatement stmt = conn.createStatement().unwrap(PhoenixStatement.class);
        QueryPlan plan = stmt.optimizeQuery("SELECT * FROM foo ORDER BY k,j");
        assertFalse(plan.getOrderBy().getOrderByExpressions().isEmpty());
    }

    @Test
    public void testChooseIndexOverTable() throws Exception {
        Connection conn = DriverManager.getConnection(getUrl());
        conn.createStatement().execute("CREATE TABLE t (k INTEGER NOT NULL PRIMARY KEY, v1 VARCHAR, v2 VARCHAR) IMMUTABLE_ROWS=true");
        conn.createStatement().execute("CREATE INDEX idx ON t(v1)");
        PhoenixStatement stmt = conn.createStatement().unwrap(PhoenixStatement.class);
        QueryPlan plan = stmt.optimizeQuery("SELECT k FROM t WHERE v1 = 'bar'");
        assertEquals("IDX", plan.getTableRef().getTable().getTableName().getString());
    }

    @Test
    public void testChooseTableOverIndex() throws Exception {
        Connection conn = DriverManager.getConnection(getUrl());
        conn.createStatement().execute("CREATE TABLE t (k INTEGER NOT NULL PRIMARY KEY, v1 VARCHAR, v2 VARCHAR) IMMUTABLE_ROWS=true");
        conn.createStatement().execute("CREATE INDEX idx ON t(v1)");
        PhoenixStatement stmt = conn.createStatement().unwrap(PhoenixStatement.class);
        QueryPlan plan = stmt.optimizeQuery("SELECT v1 FROM t WHERE k = 1");
        assertEquals("T", plan.getTableRef().getTable().getTableName().getString());
    }
    
    @Test
    public void testChooseTableForSelection() throws Exception {
        Connection conn = DriverManager.getConnection(getUrl());
        conn.createStatement().execute("CREATE TABLE t (k INTEGER NOT NULL PRIMARY KEY, v1 VARCHAR, v2 VARCHAR) IMMUTABLE_ROWS=true");
        conn.createStatement().execute("CREATE INDEX idx ON t(v1)");
        PhoenixStatement stmt = conn.createStatement().unwrap(PhoenixStatement.class);
        QueryPlan plan = stmt.optimizeQuery("SELECT v1,v2 FROM t WHERE v1 = 'bar'");
        // Choose T because v2 is not in index
        assertEquals("T", plan.getTableRef().getTable().getTableName().getString());
    }
    
    @Test
    public void testChooseTableForDynCols() throws Exception {
        Connection conn = DriverManager.getConnection(getUrl());
        conn.createStatement().execute("CREATE TABLE t (k INTEGER NOT NULL PRIMARY KEY, v1 VARCHAR, v2 VARCHAR) IMMUTABLE_ROWS=true");
        conn.createStatement().execute("CREATE INDEX idx ON t(v1)");
        PhoenixStatement stmt = conn.createStatement().unwrap(PhoenixStatement.class);
        QueryPlan plan = stmt.optimizeQuery("SELECT k FROM t(v3 VARCHAR) WHERE v1 = 'bar'");
        assertEquals("T", plan.getTableRef().getTable().getTableName().getString());
    }
    
    @Test
    public void testChooseTableForSelectionStar() throws Exception {
        Connection conn = DriverManager.getConnection(getUrl());
        conn.createStatement().execute("CREATE TABLE t (k INTEGER NOT NULL PRIMARY KEY, v1 VARCHAR, v2 VARCHAR) IMMUTABLE_ROWS=true");
        conn.createStatement().execute("CREATE INDEX idx ON t(v1)");
        PhoenixStatement stmt = conn.createStatement().unwrap(PhoenixStatement.class);
        QueryPlan plan = stmt.optimizeQuery("SELECT * FROM t WHERE v1 = 'bar'");
        // Choose T because v2 is not in index
        assertEquals("T", plan.getTableRef().getTable().getTableName().getString());
    }

    @Test
    public void testChooseIndexEvenWithSelectionStar() throws Exception {
        Connection conn = DriverManager.getConnection(getUrl());
        conn.createStatement().execute("CREATE TABLE t (k INTEGER NOT NULL PRIMARY KEY, v1 VARCHAR, v2 VARCHAR) IMMUTABLE_ROWS=true");
        conn.createStatement().execute("CREATE INDEX idx ON t(v1) INCLUDE (v2)");
        PhoenixStatement stmt = conn.createStatement().unwrap(PhoenixStatement.class);
        QueryPlan plan = stmt.optimizeQuery("SELECT * FROM t WHERE v1 = 'bar'");
        assertEquals("IDX", plan.getTableRef().getTable().getTableName().getString());
    }

    @Test
    public void testChooseIndexFromOrderBy() throws Exception {
        Connection conn = DriverManager.getConnection(getUrl());
        conn.createStatement().execute("CREATE TABLE t (k INTEGER NOT NULL PRIMARY KEY, v1 VARCHAR, v2 VARCHAR) IMMUTABLE_ROWS=true");
        conn.createStatement().execute("CREATE INDEX idx ON t(v1)");
        PhoenixStatement stmt = conn.createStatement().unwrap(PhoenixStatement.class);
        QueryPlan plan = stmt.optimizeQuery("SELECT k FROM t WHERE k > 30 ORDER BY v1 LIMIT 5");
        assertEquals("IDX", plan.getTableRef().getTable().getTableName().getString());
    }
    
    @Test
    public void testChoosePointLookupOverOrderByRemoval() throws Exception {
        Connection conn = DriverManager.getConnection(getUrl());
        conn.createStatement().execute("CREATE TABLE t (k INTEGER NOT NULL PRIMARY KEY, v1 VARCHAR, v2 VARCHAR) IMMUTABLE_ROWS=true");
        conn.createStatement().execute("CREATE INDEX idx ON t(v1)");
        PhoenixStatement stmt = conn.createStatement().unwrap(PhoenixStatement.class);
        QueryPlan plan = stmt.optimizeQuery("SELECT k FROM t WHERE k = 30 ORDER BY v1 LIMIT 5"); // Prefer 
        assertEquals("T", plan.getTableRef().getTable().getTableName().getString());
    }
    
    @Test
    public void testChooseIndexFromOrderByDesc() throws Exception {
        Connection conn = DriverManager.getConnection(getUrl());
        conn.createStatement().execute("CREATE TABLE t (k INTEGER NOT NULL PRIMARY KEY DESC, v1 VARCHAR, v2 VARCHAR) IMMUTABLE_ROWS=true");
        conn.createStatement().execute("CREATE INDEX idx ON t(v1)");
        PhoenixStatement stmt = conn.createStatement().unwrap(PhoenixStatement.class);
        QueryPlan plan = stmt.optimizeQuery("SELECT k FROM t WHERE k > 30 ORDER BY v1, k DESC LIMIT 5");
        assertEquals("IDX", plan.getTableRef().getTable().getTableName().getString());
    }
    
    @Test
    public void testChooseTableFromOrderByAsc() throws Exception {
        Connection conn = DriverManager.getConnection(getUrl());
        conn.createStatement().execute("CREATE TABLE t (k INTEGER NOT NULL PRIMARY KEY DESC, v1 VARCHAR, v2 VARCHAR) IMMUTABLE_ROWS=true");
        conn.createStatement().execute("CREATE INDEX idx ON t(v1)");
        PhoenixStatement stmt = conn.createStatement().unwrap(PhoenixStatement.class);
        QueryPlan plan = stmt.optimizeQuery("SELECT k FROM t WHERE k > 30 ORDER BY v1, k LIMIT 5");
        assertEquals("T", plan.getTableRef().getTable().getTableName().getString());
    }
    
    @Test
    public void testChooseIndexFromOrderByAsc() throws Exception {
        Connection conn = DriverManager.getConnection(getUrl());
        conn.createStatement().execute("CREATE TABLE t (k INTEGER NOT NULL PRIMARY KEY DESC, v1 VARCHAR, v2 VARCHAR) IMMUTABLE_ROWS=true");
        conn.createStatement().execute("CREATE INDEX idx ON t(v1, k)");
        PhoenixStatement stmt = conn.createStatement().unwrap(PhoenixStatement.class);
        QueryPlan plan = stmt.optimizeQuery("SELECT k FROM t WHERE k > 30 ORDER BY v1, k LIMIT 5");
        assertEquals("IDX", plan.getTableRef().getTable().getTableName().getString());
    }
    
    @Test
    public void testChoosePointLookupOverOrderByDesc() throws Exception {
        Connection conn = DriverManager.getConnection(getUrl());
        conn.createStatement().execute("CREATE TABLE t (k INTEGER NOT NULL PRIMARY KEY DESC, v1 VARCHAR, v2 VARCHAR) IMMUTABLE_ROWS=true");
        conn.createStatement().execute("CREATE INDEX idx ON t(v1, k)");
        PhoenixStatement stmt = conn.createStatement().unwrap(PhoenixStatement.class);
        QueryPlan plan = stmt.optimizeQuery("SELECT k FROM t WHERE k = 30 ORDER BY v1, k LIMIT 5");
        assertEquals("T", plan.getTableRef().getTable().getTableName().getString());
    }
    

    @Test
    public void testChooseIndexWithLongestRowKey() throws Exception {
        Connection conn = DriverManager.getConnection(getUrl());
        conn.createStatement().execute("CREATE TABLE t (k INTEGER NOT NULL PRIMARY KEY, v1 VARCHAR, v2 VARCHAR) IMMUTABLE_ROWS=true");
        conn.createStatement().execute("CREATE INDEX idx1 ON t(v1) INCLUDE(v2)");
        conn.createStatement().execute("CREATE INDEX idx2 ON t(v1,v2)");
        PhoenixStatement stmt = conn.createStatement().unwrap(PhoenixStatement.class);
        QueryPlan plan = stmt.optimizeQuery("SELECT k FROM t WHERE v1 = 'foo' AND v2 = 'bar'");
        assertEquals("IDX2", plan.getTableRef().getTable().getTableName().getString());
    }

    @Test
    public void testIgnoreIndexesBasedOnHint() throws Exception {
        Connection conn = DriverManager.getConnection(getUrl());
        conn.createStatement().execute("CREATE TABLE t (k INTEGER NOT NULL PRIMARY KEY, v1 VARCHAR, v2 VARCHAR) IMMUTABLE_ROWS=true");
        conn.createStatement().execute("CREATE INDEX idx1 ON t(v1) INCLUDE(v2)");
        conn.createStatement().execute("CREATE INDEX idx2 ON t(v1,v2)");
        PhoenixStatement stmt = conn.createStatement().unwrap(PhoenixStatement.class);
        QueryPlan plan = stmt.optimizeQuery("SELECT /*+NO_INDEX*/ k FROM t WHERE v1 = 'foo' AND v2 = 'bar'");
        assertEquals("T", plan.getTableRef().getTable().getTableName().getString());
    }
    
    @Test
    public void testChooseIndexFromHint() throws Exception {
        Connection conn = DriverManager.getConnection(getUrl());
        conn.createStatement().execute("CREATE TABLE t (k INTEGER NOT NULL PRIMARY KEY, v1 VARCHAR, v2 VARCHAR) IMMUTABLE_ROWS=true");
        conn.createStatement().execute("CREATE INDEX idx1 ON t(v1) INCLUDE(v2)");
        conn.createStatement().execute("CREATE INDEX idx2 ON t(v1,v2)");
        PhoenixStatement stmt = conn.createStatement().unwrap(PhoenixStatement.class);
        QueryPlan plan = stmt.optimizeQuery("SELECT /*+ INDEX(t  idx1) */ k FROM t WHERE v1 = 'foo' AND v2 = 'bar'");
        assertEquals("IDX1", plan.getTableRef().getTable().getTableName().getString());
    }

    
    @Test
    public void testChooseIndexFromDoubleQuotedHint() throws Exception {
        Connection conn = DriverManager.getConnection(getUrl());
        conn.createStatement().execute("CREATE TABLE t (k INTEGER NOT NULL PRIMARY KEY, v1 VARCHAR, v2 VARCHAR) IMMUTABLE_ROWS=true");
        conn.createStatement().execute("CREATE INDEX idx1 ON t(v1) INCLUDE(v2)");
        conn.createStatement().execute("CREATE INDEX idx2 ON t(v1,v2)");
        PhoenixStatement stmt = conn.createStatement().unwrap(PhoenixStatement.class);
        QueryPlan plan = stmt.optimizeQuery("SELECT /*+ INDEX(t  \"IDX1\") INDEX(t  idx3) */ k FROM t WHERE v1 = 'foo' AND v2 = 'bar'");
        assertEquals("IDX1", plan.getTableRef().getTable().getTableName().getString());
    }
    
    @Test
    public void testIndexHintParsing() throws Exception {
        Connection conn = DriverManager.getConnection(getUrl());
        conn.createStatement().execute("CREATE TABLE t (k INTEGER NOT NULL PRIMARY KEY, v1 VARCHAR, v2 VARCHAR) IMMUTABLE_ROWS=true");
        conn.createStatement().execute("CREATE INDEX idx1 ON t(v1) INCLUDE(v2)");
        conn.createStatement().execute("CREATE INDEX idx2 ON t(v1,v2)");
        PhoenixStatement stmt = conn.createStatement().unwrap(PhoenixStatement.class);
        QueryPlan plan = stmt.optimizeQuery("SELECT /*+  INDEX(t  idx3 idx4 \"idx5\") INDEX(t idx6 idx1) */ k FROM t WHERE v1 = 'foo' AND v2 = 'bar'");
        assertEquals("IDX1", plan.getTableRef().getTable().getTableName().getString());
    }
    
    @Test
    public void testChooseSmallerTable() throws Exception {
        Connection conn = DriverManager.getConnection(getUrl());
        conn.createStatement().execute("CREATE TABLE t (k INTEGER NOT NULL PRIMARY KEY, v1 VARCHAR, v2 VARCHAR) IMMUTABLE_ROWS=true");
        conn.createStatement().execute("CREATE INDEX idx ON t(v1)");
        PhoenixStatement stmt = conn.createStatement().unwrap(PhoenixStatement.class);
        QueryPlan plan = stmt.optimizeQuery("SELECT count(*) FROM t");
        assertEquals("IDX", plan.getTableRef().getTable().getTableName().getString());
    }
    
    @Test
    public void testRVCForTableWithSecondaryIndexBasic() throws Exception {
        Connection conn = DriverManager.getConnection(getUrl());
        conn.createStatement().execute("CREATE TABLE T (k VARCHAR NOT NULL PRIMARY KEY, v1 VARCHAR, v2 VARCHAR)");
        conn.createStatement().execute("CREATE INDEX IDX ON T(v1, v2)");
        PhoenixStatement stmt = conn.createStatement().unwrap(PhoenixStatement.class);
        String query = "select * from t where (v1, v2) <= ('1', '2')";
        QueryPlan plan = stmt.optimizeQuery(query);
        assertEquals("IDX", plan.getTableRef().getTable().getTableName().getString());
    }
    
    @Test
    public void testRVCAllColsForTableWithSecondaryIndexBasic() throws Exception {
        Connection conn = DriverManager.getConnection(getUrl());
        conn.createStatement().execute("CREATE TABLE T (k VARCHAR NOT NULL PRIMARY KEY, v1 VARCHAR, v2 VARCHAR)");
        conn.createStatement().execute("CREATE INDEX IDX ON T(v1, v2)");
        PhoenixStatement stmt = conn.createStatement().unwrap(PhoenixStatement.class);
        String query = "select * from t where (k, v1, v2) <= ('3', '1', '2')";
        QueryPlan plan = stmt.optimizeQuery(query);
        assertEquals("T", plan.getTableRef().getTable().getTableName().getString());
    }
    
    @Test
    // Multi-tenant = false; Query uses index = false; Salted = true
    public void testAssertQueryPlanDetails1() throws Exception {
        testAssertQueryPlanDetails(false, false, true);
    }
    
    @Test
    // Multi-tenant = true; Query uses index = false; Salted = true
    public void testAssertQueryPlanDetails2() throws Exception {
        testAssertQueryPlanDetails(true, false, true);
    }
    
    @Test
    // Multi-tenant = true; Query uses index = true; Salted = false
    public void testAssertQueryPlanDetails3() throws Exception {
        testAssertQueryPlanDetails(true, true, true);
    }
    
    @Test
    // Multi-tenant = false; Query uses index = true; Salted = true
    public void testAssertQueryPlanDetails4() throws Exception {
        testAssertQueryPlanDetails(false, true, true);
    }
    
    @Test
    // Multi-tenant = false; Query uses index = false; Salted = false
    public void testAssertQueryPlanDetails5() throws Exception {
        testAssertQueryPlanDetails(false, false, false);
    }
    
    @Test
    // Multi-tenant = true; Query uses index = false; Salted = false
    public void testAssertQueryPlanDetails6() throws Exception {
        testAssertQueryPlanDetails(true, false, false);
    }
    
    @Test
    // Multi-tenant = true; Query uses index = true; Salted = false
    public void testAssertQueryPlanDetails7() throws Exception {
        testAssertQueryPlanDetails(true, true, false);
    }
    
    @Test
    // Multi-tenant = false; Query uses index = true; Salted = false
    public void testAssertQueryPlanDetails8() throws Exception {
        testAssertQueryPlanDetails(false, true, false);
    }

    @Test
    public void testQueryOptimizerShouldSelectThePlanWithMoreNumberOfPKColumns() throws Exception {
        Connection conn1 = DriverManager.getConnection(getUrl());
        Connection conn2 = DriverManager.getConnection(getUrl());
        conn1.createStatement().execute("create table index_test_table (a varchar not null,b varchar not null,c varchar not null,d varchar,e varchar, f varchar constraint pk primary key(a,b,c))");
        conn1.createStatement().execute(
            "create index INDEX_TEST_TABLE_INDEX_D on INDEX_TEST_TABLE(A,D) include(B,C,E,F)");
        conn1.createStatement().execute(
            "create index INDEX_TEST_TABLE_INDEX_F on INDEX_TEST_TABLE(A,F) include(B,C,D,E)");
        ResultSet rs = conn2.createStatement().executeQuery("explain select * from INDEX_TEST_TABLE where A in ('1','2','3','4','5') and F in ('1111','2222','3333')");
        assertEquals("CLIENT PARALLEL 1-WAY SKIP SCAN ON 15 KEYS OVER INDEX_TEST_TABLE_INDEX_F ['1','1111'] - ['5','3333']", QueryUtil.getExplainPlan(rs));
    }

    private void testAssertQueryPlanDetails(boolean multitenant, boolean useIndex, boolean salted) throws Exception {
        String sql;
        PreparedStatement stmt;
        Connection conn = DriverManager.getConnection(getUrl(), new Properties());
        try {
            // create table
            conn.createStatement().execute("create table "
                    + "XYZ.ABC"
                    + "   (organization_id char(15) not null, \n"
                    + "    dec DECIMAL(10,2) not null,\n"
                    + "    a_string_array varchar(100) array[] not null,\n"
                    + "    b_string varchar(100),\n"
                    + "    CF.a_integer integer,\n"
                    + "    a_date date,\n"
                    + "    CONSTRAINT pk PRIMARY KEY (organization_id, dec, a_string_array)\n"
                    + ")" + (salted ? "SALT_BUCKETS=4" : "") + (multitenant == true ? (salted ? ",MULTI_TENANT=true" : "MULTI_TENANT=true") : ""));

            
            if (useIndex) {
                // create index
                conn.createStatement().execute("CREATE INDEX ABC_IDX ON XYZ.ABC (CF.a_integer) INCLUDE (a_date)");
            }
            
            // switch to a tenant specific connection if multi-tenant.
            conn = multitenant ? DriverManager.getConnection(getUrl("tenantId")) : conn;
            
            // create a tenant specific view if multi-tenant
            if (multitenant) {
                conn.createStatement().execute("CREATE VIEW ABC_VIEW (ORGANIZATION_ID VARCHAR) AS SELECT * FROM XYZ.ABC");
            }
            
            String expectedColNames = multitenant ? addQuotes(null, "DEC,A_STRING_ARRAY") : addQuotes(null,"ORGANIZATION_ID,DEC,A_STRING_ARRAY");
            String expectedColumnNameDataTypes = multitenant ? "\"DEC\" DECIMAL(10,2),\"A_STRING_ARRAY\" VARCHAR(100) ARRAY" : "\"ORGANIZATION_ID\" CHAR(15),\"DEC\" DECIMAL(10,2),\"A_STRING_ARRAY\" VARCHAR(100) ARRAY";
            String tableName = multitenant ? "ABC_VIEW" : "XYZ.ABC";
            String tenantFilter = multitenant ? "" : "organization_id = ? AND ";
            String orderByRowKeyClause = multitenant ? "dec" : "organization_id";
            
            // Filter on row key columns of data table. No order by. No limit.
            sql = "SELECT CF.a_integer FROM " + tableName + " where " + tenantFilter + " dec = ? and a_string_array = ?";
            stmt = conn.prepareStatement(sql);
            int counter = 1;
            if (!multitenant) {
                stmt.setString(counter++, "ORGID");
            }
            stmt.setDouble(counter++, 1.23);
            String[] strArray = new String[2];
            strArray[0] = "AB";
            strArray[1] = "CD";
            Array array = conn.createArrayOf("VARCHAR", strArray);
            stmt.setArray(counter++, array);
            assertPlanDetails(stmt, expectedColNames, expectedColumnNameDataTypes, false, 0);
            
            counter = 1;
            // Filter on row key columns of data table. Order by row key columns. Limit specified.
            sql = "SELECT CF.a_integer FROM " + tableName + " where " + tenantFilter + " dec = ? and a_string_array = ? ORDER BY " + orderByRowKeyClause + " LIMIT 100";
            stmt = conn.prepareStatement(sql);
            if (!multitenant) {
                stmt.setString(counter++, "ORGID");
            }
            stmt.setDouble(counter++, 1.23);
            array = conn.createArrayOf("VARCHAR", strArray);
            stmt.setArray(counter++, array);
            assertPlanDetails(stmt, expectedColNames, expectedColumnNameDataTypes, false, 100);
            
            counter = 1;
            // Filter on row key columns of data table. Order by non-row key columns. Limit specified.
            sql = "SELECT CF.a_integer FROM " + tableName + " where " + tenantFilter + " dec = ? and a_string_array = ? ORDER BY a_date LIMIT 100";
            stmt = conn.prepareStatement(sql);
            if (!multitenant) {
                stmt.setString(counter++, "ORGID");
            }
            stmt.setDouble(counter++, 1.23);
            array = conn.createArrayOf("VARCHAR", strArray);
            stmt.setArray(counter++, array);
            assertPlanDetails(stmt, expectedColNames, expectedColumnNameDataTypes, true, 100);
            
            if (useIndex) {
                
                expectedColNames = multitenant ? ("\"CF\".\"A_INTEGER\"" + ",\"DEC\"" + ",\"A_STRING_ARRAY\"") : ("\"CF\".\"A_INTEGER\"" + ",\"ORGANIZATION_ID\"" + ",\"DEC\"" + ",\"A_STRING_ARRAY\"");
                expectedColumnNameDataTypes = multitenant ? ("\"CF\".\"A_INTEGER\"" + " " + "INTEGER" + ",\"DEC\"" + " " + "DECIMAL(10,2)" + ",\"A_STRING_ARRAY\""+ " " + "VARCHAR(100) ARRAY") : ("\"CF\".\"A_INTEGER\"" + " " + "INTEGER" + ",\"ORGANIZATION_ID\"" + " " + "CHAR(15)" + ",\"DEC\"" + " " + "DECIMAL(10,2)" + ",\"A_STRING_ARRAY\""+ " " + "VARCHAR(100) ARRAY");
                
                // Filter on columns that the secondary index is on. No order by. No limit.
                sql = "SELECT a_date FROM " + tableName + " where CF.a_integer = ?";
                stmt = conn.prepareStatement(sql);
                stmt.setInt(1, 1000);
                assertPlanDetails(stmt, expectedColNames, expectedColumnNameDataTypes, false, 0);

                // Filter on columns that the secondary index is on. Order by on the indexed column. Limit specified.
                sql = "SELECT a_date FROM " + tableName + " where CF.a_integer = ? ORDER BY CF.a_integer LIMIT 100";
                stmt = conn.prepareStatement(sql);
                stmt.setInt(1, 1000);
                assertPlanDetails(stmt, expectedColNames, expectedColumnNameDataTypes, false, 100);

                // Filter on columns that the secondary index is on. Order by on the non-indexed column. Limit specified.
                sql = "SELECT a_integer FROM " + tableName + " where CF.a_integer = ? and a_date = ? ORDER BY a_date LIMIT 100";
                stmt = conn.prepareStatement(sql);
                stmt.setInt(1, 1000);
                stmt.setDate(2, new Date(909000));
                assertPlanDetails(stmt, expectedColNames, expectedColumnNameDataTypes, true, 100);
            }
        } finally {
            conn.close();
        }
    }
    
    @Test
    public void testAssertQueryAgainstTenantSpecificViewGoesThroughIndex() throws Exception {
        Connection conn = DriverManager.getConnection(getUrl(), new Properties());
        
        // create table
        conn.createStatement().execute("create table "
                + "XYZ.ABC"
                + "   (organization_id char(15) not null, \n"
                + "    entity_id char(15) not null,\n"
                + "    a_string_array varchar(100) array[] not null,\n"
                + "    b_string varchar(100),\n"
                + "    a_string varchar,\n"
                + "    a_date date,\n"
                + "    CONSTRAINT pk PRIMARY KEY (organization_id, entity_id, a_string_array)\n"
                + ")" + "MULTI_TENANT=true");

        
        // create index
        conn.createStatement().execute("CREATE INDEX ABC_IDX ON XYZ.ABC (a_string) INCLUDE (a_date)");
        
        conn.close();
        
        // switch to a tenant specific connection
        conn = DriverManager.getConnection(getUrl("tenantId"));
        
        // create a tenant specific view
        conn.createStatement().execute("CREATE VIEW ABC_VIEW AS SELECT * FROM XYZ.ABC");
        
        // query against the tenant specific view
        String sql = "SELECT a_date FROM ABC_VIEW where a_string = ?";
        PreparedStatement stmt = conn.prepareStatement(sql);
        stmt.setString(1, "1000");
        QueryPlan plan = stmt.unwrap(PhoenixPreparedStatement.class).optimizeQuery();
        assertEquals("Query should use index", PTableType.INDEX, plan.getTableRef().getTable().getType());
    }
    
    @Test
    public void testAssertQueryAgainstTenantSpecificViewDoesNotGoThroughIndex() throws Exception {
        Connection conn = DriverManager.getConnection(getUrl(), new Properties());
        
        // create table
        conn.createStatement().execute("create table "
                + "XYZ.ABC"
                + "   (organization_id char(15) not null, \n"
                + "    entity_id char(15) not null,\n"
                + "    a_string_array varchar(100) array[] not null,\n"
                + "    b_string varchar(100),\n"
                + "    a_string varchar,\n"
                + "    a_date date,\n"
                + "    CONSTRAINT pk PRIMARY KEY (organization_id, entity_id, a_string_array)\n"
                + ")" + "MULTI_TENANT=true");

        
        // create index
        conn.createStatement().execute("CREATE INDEX ABC_IDX ON XYZ.ABC (a_string) INCLUDE (a_date)");
        
        conn.close();
        
        // switch to a tenant specific connection
        conn = DriverManager.getConnection(getUrl("tenantId"));
        
        // create a tenant specific view
        conn.createStatement().execute("CREATE VIEW ABC_VIEW AS SELECT * FROM XYZ.ABC where b_string='foo'");
        
        // query against the tenant specific view
        String sql = "SELECT a_date FROM ABC_VIEW where a_string = ?";
        PreparedStatement stmt = conn.prepareStatement(sql);
        stmt.setString(1, "1000");
        QueryPlan plan = stmt.unwrap(PhoenixPreparedStatement.class).optimizeQuery();
        // should not use index as index does not contain b_string
        assertEquals("Query should not use index", PTableType.VIEW, plan.getTableRef().getTable().getType());
    }

    private void assertPlanDetails(PreparedStatement stmt, String expectedPkCols, String expectedPkColsDataTypes, boolean expectedHasOrderBy, int expectedLimit) throws SQLException {
        Connection conn = stmt.getConnection();
        QueryPlan plan = PhoenixRuntime.getOptimizedQueryPlan(stmt);
        
        List<Pair<String, String>> columns = new ArrayList<Pair<String, String>>();
        PhoenixRuntime.getPkColsForSql(columns, plan, conn, true);
        assertEquals(expectedPkCols, Joiner.on(",").join(getColumnNames(columns)));
        List<String> dataTypes = new ArrayList<String>();
        columns = new ArrayList<Pair<String,String>>();
        PhoenixRuntime.getPkColsDataTypesForSql(columns, dataTypes, plan, conn, true);
        
        assertEquals(expectedPkColsDataTypes, appendColNamesDataTypes(columns, dataTypes));
        assertEquals(expectedHasOrderBy, PhoenixRuntime.hasOrderBy(plan));
        assertEquals(expectedLimit, PhoenixRuntime.getLimit(plan));
    }
    
    private static List<String> getColumnNames(List<Pair<String, String>> columns) {
        List<String> columnNames = new ArrayList<String>(columns.size());
        for (Pair<String, String> col : columns) {
            String familyName = col.getFirst();
            String columnName = col.getSecond();
            if (familyName != null) {
                columnName = familyName + QueryConstants.NAME_SEPARATOR + columnName;
            }
            columnNames.add(columnName);
        }
        return columnNames;
    }
    
    private String addQuotes(String familyName, String columnNames) {
        Iterable<String> columnNamesList = Splitter.on(",").split(columnNames);
        List<String> quotedColumnNames = new ArrayList<String>();
        for (String columnName : columnNamesList) {
            String quotedColumnName = SchemaUtil.getQuotedFullColumnName(familyName, columnName);
            quotedColumnNames.add(quotedColumnName);
        }
        return Joiner.on(",").join(quotedColumnNames);
    }
    
    private String appendColNamesDataTypes(List<Pair<String, String>> columns, List<String> dataTypes) {
        int size = columns.size();
        assertEquals(size, dataTypes.size()); // they will be equal, but what the heck?
        List<String> pkColsDataTypes = new ArrayList<String>(size);
        for (int i = 0; i < size; i++) {
            String familyName = columns.get(i).getFirst();
            String columnName = columns.get(i).getSecond();
            if (familyName != null) {
                columnName = familyName + QueryConstants.NAME_SEPARATOR + columnName;
            }
            pkColsDataTypes.add(columnName + " " + dataTypes.get(i));
        }
        return Joiner.on(",").join(pkColsDataTypes);
    }
    
}