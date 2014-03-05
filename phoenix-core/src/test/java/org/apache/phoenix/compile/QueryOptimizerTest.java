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

import java.sql.Connection;
import java.sql.DriverManager;

import org.apache.phoenix.compile.OrderByCompiler.OrderBy;
import org.apache.phoenix.jdbc.PhoenixStatement;
import org.apache.phoenix.query.BaseConnectionlessQueryTest;
import org.apache.phoenix.util.SchemaUtil;
import org.junit.Test;

public class QueryOptimizerTest extends BaseConnectionlessQueryTest {
    
    public static final String SCHEMA_NAME = "";
    public static final String DATA_TABLE_NAME = "T";
    public static final String INDEX_TABLE_NAME = "I";
    public static final String DATA_TABLE_FULL_NAME = SchemaUtil.getTableName(SCHEMA_NAME, "T");
    public static final String INDEX_TABLE_FULL_NAME = SchemaUtil.getTableName(SCHEMA_NAME, "I");

    public QueryOptimizerTest() {
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
}