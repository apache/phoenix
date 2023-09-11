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

import static org.apache.phoenix.util.TestUtil.TEST_PROPERTIES;
import static org.apache.phoenix.util.TestUtil.assertResultSet;
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

import org.apache.phoenix.jdbc.PhoenixDatabaseMetaData;
import org.apache.phoenix.schema.AmbiguousColumnException;
import org.apache.phoenix.util.PropertiesUtil;
import org.apache.phoenix.util.QueryBuilder;
import org.apache.phoenix.util.TestUtil;
import org.junit.Test;
import org.junit.experimental.categories.Category;

@Category(ParallelStatsDisabledTest.class)
public class AggregateIT extends BaseAggregateIT {

    @Test
    public void testGroupByWithAliasWithSameColumnName() throws SQLException {
        Properties props = PropertiesUtil.deepCopy(TEST_PROPERTIES);
        Connection conn = DriverManager.getConnection(getUrl(), props);
        String tableName1 = generateUniqueName();
        String tableName2 = generateUniqueName();
        String tableName3 = generateUniqueName();
        String ddl = "create table " + tableName1 + " (pk integer primary key, col integer)";
        conn.createStatement().execute(ddl);
        ddl = "create table " + tableName2 + " (pk integer primary key, col integer)";
        conn.createStatement().execute(ddl);
        ddl = "create table " + tableName3 + " (notPk integer primary key, col integer)";
        conn.createStatement().execute(ddl);
        conn.createStatement().execute("UPSERT INTO " + tableName1 + " VALUES (1,2)");
        conn.createStatement().execute("UPSERT INTO " + tableName2 + " VALUES (1,2)");
        conn.createStatement().execute("UPSERT INTO " + tableName3 + " VALUES (1,2)");
        conn.createStatement().executeQuery("select " + tableName1 + ".pk as pk from " + tableName1 + " group by pk");
        conn.createStatement().executeQuery("select " + tableName1 + ".pk as pk from " + tableName1 + " group by " + tableName1 + ".pk");
        conn.createStatement().executeQuery("select " + tableName1 + ".pk as pk from " + tableName1 + " as t group by t.pk");
        conn.createStatement().executeQuery("select " + tableName1 + ".col as pk from " + tableName1);
        conn.createStatement()
                .executeQuery("select " + tableName1 + ".pk as pk from " + tableName1 + " join " + tableName3 + " on (" + tableName1 + ".pk=" + tableName3 + ".notPk) group by pk");
        try {
            conn.createStatement().executeQuery("select " + tableName1 + ".col as pk from " + tableName1 + " group by pk");
            fail();
        } catch (AmbiguousColumnException e) {}
        try {
            conn.createStatement().executeQuery("select col as pk from " + tableName1 + " group by pk");
            fail();
        } catch (AmbiguousColumnException e) {}
        try {
            conn.createStatement()
                    .executeQuery("select " + tableName1 + ".pk as pk from " + tableName1 + " join " + tableName2 + " on (" + tableName1 + ".pk=" + tableName2 + ".pk) group by pk");
            fail();
        } catch (AmbiguousColumnException e) {}
        conn.close();
    }

    @Test
    public void testGroupByCoerceExpressionBug3453() throws Exception {
        final Connection conn = DriverManager.getConnection(getUrl());
        try {
            //Type is INT
            String intTableName=generateUniqueName();
            String sql="CREATE TABLE "+ intTableName +"("+
                    "ENTITY_ID INTEGER NOT NULL,"+
                    "CONTAINER_ID INTEGER NOT NULL,"+
                    "SCORE INTEGER NOT NULL,"+
                    "CONSTRAINT TEST_PK PRIMARY KEY (ENTITY_ID DESC,CONTAINER_ID DESC,SCORE DESC))";

            conn.createStatement().execute(sql);
            conn.createStatement().execute("UPSERT INTO "+intTableName+" VALUES (1,1,1)");
            conn.commit();

            sql="select DISTINCT entity_id, score from ( select entity_id, score from "+intTableName+" limit 1)";
            ResultSet rs=conn.prepareStatement(sql).executeQuery();
            assertResultSet(rs, new Object[][]{{1,1}});

            conn.createStatement().execute("UPSERT INTO "+intTableName+" VALUES (2,2,2)");
            conn.createStatement().execute("UPSERT INTO "+intTableName+" VALUES (3,3,3)");
            conn.commit();

            sql="select DISTINCT entity_id, score from ( select entity_id, score from "+intTableName+" limit 3) order by entity_id";
            rs=conn.prepareStatement(sql).executeQuery();
            assertResultSet(rs, new Object[][]{{1,1},{2,2},{3,3}});

            sql="select DISTINCT entity_id, score from ( select entity_id, score from "+intTableName+" limit 3) order by entity_id desc";
            rs=conn.prepareStatement(sql).executeQuery();
            assertResultSet(rs, new Object[][]{{3,3},{2,2},{1,1}});

            //Type is CHAR
            String charTableName=generateUniqueName();
            sql="CREATE TABLE "+ charTableName +"("+
                    "ENTITY_ID CHAR(15) NOT NULL,"+
                    "CONTAINER_ID INTEGER NOT NULL,"+
                    "SCORE INTEGER NOT NULL,"+
                    "CONSTRAINT TEST_PK PRIMARY KEY (ENTITY_ID DESC,CONTAINER_ID DESC,SCORE DESC))";

            conn.createStatement().execute(sql);
            conn.createStatement().execute("UPSERT INTO "+charTableName+" VALUES ('entity1',1,1)");
            conn.createStatement().execute("UPSERT INTO "+charTableName+" VALUES ('entity2',2,2)");
            conn.createStatement().execute("UPSERT INTO "+charTableName+" VALUES ('entity3',3,3)");
            conn.commit();

            sql="select DISTINCT entity_id, score from ( select entity_id, score from "+charTableName+" limit 3) order by entity_id";
            rs=conn.prepareStatement(sql).executeQuery();
            assertResultSet(rs, new Object[][]{{"entity1",1},{"entity2",2},{"entity3",3}});

            sql="select DISTINCT entity_id, score from ( select entity_id, score from "+charTableName+" limit 3) order by entity_id desc";
            rs=conn.prepareStatement(sql).executeQuery();
            assertResultSet(rs, new Object[][]{{"entity3",3},{"entity2",2},{"entity1",1}});
        } finally {
            if(conn!=null) {
                conn.close();
            }
        }
    }

    @Test
    public void testNestedGroupedAggregationWithBigInt() throws Exception {
        Properties props = PropertiesUtil.deepCopy(TEST_PROPERTIES);
        String tableName = generateUniqueName();
        try(Connection conn = DriverManager.getConnection(getUrl(), props);) {
            String createQuery="CREATE TABLE "+tableName+" (a BIGINT NOT NULL,c BIGINT NOT NULL CONSTRAINT PK PRIMARY KEY (a, c))";
            String updateQuery="UPSERT INTO "+tableName+"(a,c) VALUES(4444444444444444444, 5555555555555555555)";
            String query="SELECT a FROM (SELECT a, c FROM "+tableName+" GROUP BY a, c) GROUP BY a, c";
            conn.prepareStatement(createQuery).execute();
            conn.prepareStatement(updateQuery).execute();
            conn.commit();
            PreparedStatement statement = conn.prepareStatement(query);
            ResultSet rs = statement.executeQuery();
            assertTrue(rs.next());
            assertEquals(4444444444444444444L,rs.getLong(1));
            assertFalse(rs.next());
        }
    }

    @Test
    public void testAvgGroupByOrderPreservingWithStats() throws Exception {
        Properties props = PropertiesUtil.deepCopy(TEST_PROPERTIES);
        Connection conn = DriverManager.getConnection(getUrl(), props);
        String tableName = generateUniqueName();
        QueryBuilder queryBuilder = new QueryBuilder()
            .setSelectExpression("COUNT(*)")
            .setFullTableName(PhoenixDatabaseMetaData.SYSTEM_STATS_NAME)
            .setWhereClause(PhoenixDatabaseMetaData.PHYSICAL_NAME + " ='" + tableName + "'");
        ResultSet rs = executeQuery(conn, queryBuilder);
        assertTrue(rs.next());
        assertEquals(0,rs.getInt(1));
        initAvgGroupTable(conn, tableName, PhoenixDatabaseMetaData.GUIDE_POSTS_WIDTH + "=20 ");
        // When ParallelStats are disabled it is 4. When enabled it is 13
        testAvgGroupByOrderPreserving(conn, tableName, 4);
        rs = executeQuery(conn, queryBuilder);
        assertTrue(rs.next());
        assertEquals(13,rs.getInt(1));
        conn.setAutoCommit(true);
        conn.createStatement().execute("DELETE FROM " + "\"SYSTEM\".\"STATS\"");
        rs = executeQuery(conn, queryBuilder);
        assertTrue(rs.next());
        assertEquals(0,rs.getInt(1));
        TestUtil.doMajorCompaction(conn, tableName);
        rs = executeQuery(conn, queryBuilder);
        assertTrue(rs.next());
        assertEquals(13,rs.getInt(1));
        testAvgGroupByOrderPreserving(conn, tableName, 4);
        conn.createStatement().execute("ALTER TABLE " + tableName + " SET " + PhoenixDatabaseMetaData.GUIDE_POSTS_WIDTH + "=100");
        testAvgGroupByOrderPreserving(conn, tableName, 6);
        conn.createStatement().execute("ALTER TABLE " + tableName + " SET " + PhoenixDatabaseMetaData.GUIDE_POSTS_WIDTH + "=null");
        testAvgGroupByOrderPreserving(conn, tableName, 4);
    }

    @Override
    protected void testCountNullInNonEmptyKeyValueCF(int columnEncodedBytes) throws Exception {
        try (Connection conn = DriverManager.getConnection(getUrl())) {
            //Type is INT
            String intTableName=generateUniqueName();
            String sql="create table " + intTableName + " (mykey integer not null primary key, A.COLA integer, B.COLB integer) "
                    + "IMMUTABLE_ROWS=true, IMMUTABLE_STORAGE_SCHEME = ONE_CELL_PER_COLUMN, COLUMN_ENCODED_BYTES = " + columnEncodedBytes + ", DISABLE_WAL=true";

            conn.createStatement().execute(sql);
            conn.createStatement().execute("UPSERT INTO "+intTableName+" VALUES (1,1)");
            conn.createStatement().execute("UPSERT INTO "+intTableName+" VALUES (2,1)");
            conn.createStatement().execute("UPSERT INTO "+intTableName+" VALUES (3,1,2)");
            conn.createStatement().execute("UPSERT INTO "+intTableName+" VALUES (4,1)");
            conn.createStatement().execute("UPSERT INTO "+intTableName+" VALUES (5,1)");
            conn.commit();

            sql="select count(*) from "+intTableName;
            QueryBuilder queryBuilder = new QueryBuilder()
                .setSelectExpression("COUNT(*)")
                .setFullTableName(intTableName);
            ResultSet rs = executeQuery(conn, queryBuilder);
            assertTrue(rs.next());
            assertEquals(5, rs.getLong(1));

            sql="select count(*) from "+intTableName + " where b.colb is not null";
            queryBuilder.setWhereClause("B.COLB IS NOT NULL");
            rs = executeQuery(conn, queryBuilder);
            assertTrue(rs.next());
            assertEquals(1, rs.getLong(1));

            sql="select count(*) from "+intTableName + " where b.colb is null";
            queryBuilder.setWhereClause("B.COLB IS NULL");
            rs = executeQuery(conn, queryBuilder);
            assertTrue(rs.next());
            assertEquals(4, rs.getLong(1));
        }
    }

    @Test
    public void testOrderByOptimizeForClientAggregatePlanBug4820() throws Exception {
        doTestOrderByOptimizeForClientAggregatePlanBug4820(false,false);
        doTestOrderByOptimizeForClientAggregatePlanBug4820(false,true);
        doTestOrderByOptimizeForClientAggregatePlanBug4820(true,false);
        doTestOrderByOptimizeForClientAggregatePlanBug4820(true,true);
    }

    private void doTestOrderByOptimizeForClientAggregatePlanBug4820(boolean desc ,boolean salted) throws Exception {
        Properties props = PropertiesUtil.deepCopy(TEST_PROPERTIES);
        Connection conn = null;
        try {
            conn = DriverManager.getConnection(getUrl(), props);
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
                    " )) "+(salted ? "SALT_BUCKETS =4" : "split on('b')");
            conn.createStatement().execute(sql);

            conn.createStatement().execute("UPSERT INTO "+tableName+" VALUES ('a11','a12','a13','a14','a15')");
            conn.createStatement().execute("UPSERT INTO "+tableName+" VALUES ('a21','a22','a23','a24','a25')");
            conn.createStatement().execute("UPSERT INTO "+tableName+" VALUES ('a31','a32','a33','a34','a35')");
            conn.createStatement().execute("UPSERT INTO "+tableName+" VALUES ('b11','b12','b13','b14','b15')");
            conn.createStatement().execute("UPSERT INTO "+tableName+" VALUES ('b21','b22','b23','b24','b25')");
            conn.createStatement().execute("UPSERT INTO "+tableName+" VALUES ('b31','b32','b33','b34','b35')");
            conn.commit();

            sql = "select a.ak3 "+
                  "from (select pk1 ak1,pk2 ak2,pk3 ak3, substr(v1,1,1) av1,substr(v2,1,1) av2 from "+tableName+" order by pk2,pk3 limit 10) a "+
                  "group by a.ak3,a.av1 order by a.ak3 desc,a.av1";
            ResultSet rs = conn.prepareStatement(sql).executeQuery();
            assertResultSet(rs, new Object[][]{{"b33"},{"b23"},{"b13"},{"a33"},{"a23"},{"a13"}});

            sql = "select a.ak3 "+
                  "from (select pk1 ak1,pk2 ak2,pk3 ak3, substr(v1,1,1) av1,substr(v2,1,1) av2 from "+tableName+" order by pk2,pk3 limit 10) a "+
                  "group by a.ak3,a.av1 order by a.ak3,a.av1";
            rs = conn.prepareStatement(sql).executeQuery();
            assertResultSet(rs, new Object[][]{{"a13"},{"a23"},{"a33"},{"b13"},{"b23"},{"b33"}});

            sql = "select a.ak3 "+
                  "from (select pk1 ak1,pk2 ak2,pk3 ak3,substr(v1,1,1) av1,substr(v2,1,1) av2 from "+tableName+" order by pk2,pk3 limit 10) a "+
                  "where a.av1 = 'a' group by a.av1,a.ak3 order by a.ak3 desc";
            rs = conn.prepareStatement(sql).executeQuery();
            assertResultSet(rs, new Object[][]{{"a33"},{"a23"},{"a13"}});

            sql = "select a.ak3 "+
                    "from (select pk1 ak1,pk2 ak2,pk3 ak3,substr(v1,1,1) av1,substr(v2,1,1) av2 from "+tableName+" order by pk2,pk3 limit 10) a "+
                    "where a.av1 = 'a' group by a.av1,a.ak3 order by a.ak3";
            rs = conn.prepareStatement(sql).executeQuery();
            assertResultSet(rs, new Object[][]{{"a13"},{"a23"},{"a33"}});

            sql = "select a.ak3 "+
                    "from (select pk1 ak1,pk2 ak2,pk3 ak3,substr(v1,1,1) av1,substr(v2,1,1) av2 from "+tableName+" order by pk2,pk3 limit 10) a "+
                    "where a.av1 = 'b' and a.av2= 'b' group by CASE WHEN a.av1 > a.av2 THEN a.av1 ELSE a.av2 END,a.ak3,a.ak2 order by a.ak3 desc,a.ak2 desc";
            rs = conn.prepareStatement(sql).executeQuery();
            assertResultSet(rs, new Object[][]{{"b33"},{"b23"},{"b13"}});

            sql = "select a.ak3 "+
                    "from (select pk1 ak1,pk2 ak2,pk3 ak3,substr(v1,1,1) av1,substr(v2,1,1) av2 from "+tableName+" order by pk2,pk3 limit 10) a "+
                    "where a.av1 = 'b' and a.av2= 'b' group by CASE WHEN a.av1 > a.av2 THEN a.av1 ELSE a.av2 END,a.ak3,a.ak2 order by a.ak3,a.ak2 desc";
            rs = conn.prepareStatement(sql).executeQuery();
            assertResultSet(rs, new Object[][]{{"b13"},{"b23"},{"b33"}});

            tableName = generateUniqueName();
            sql = "create table " + tableName + "( "+
                    " pk1 double not null , " +
                    " pk2 double not null, " +
                    " pk3 double not null," +
                    " v1 varchar, " +
                    " CONSTRAINT TEST_PK PRIMARY KEY ( "+
                    "pk1 "+(desc ? "desc" : "")+", "+
                    "pk2 "+(desc ? "desc" : "")+", "+
                    "pk3 "+(desc ? "desc" : "")+
                    " )) "+(salted ? "SALT_BUCKETS =4" : "split on(2.3)");
            conn.createStatement().execute(sql);
            conn.createStatement().execute("UPSERT INTO "+tableName+" VALUES (2.1,2.11,2.12,'e')");
            conn.createStatement().execute("UPSERT INTO "+tableName+" VALUES (2.2,2.21,2.23,'d')");
            conn.createStatement().execute("UPSERT INTO "+tableName+" VALUES (2.3,2.31,2.32,'c')");
            conn.createStatement().execute("UPSERT INTO "+tableName+" VALUES (2.4,2.41,2.42,'b')");
            conn.createStatement().execute("UPSERT INTO "+tableName+" VALUES (2.5,2.51,2.52,'a')");
            conn.commit();

            sql = "select a.av1 "+
                  "from (select pk1 ak1,pk2 ak2,pk3 ak3, substr(v1,1,1) av1 from "+tableName+" order by pk1,pk2 limit 10) a "+
                  "where cast(a.ak1 as integer)=2 group by a.ak1,a.av1 order by a.av1";
            rs = conn.prepareStatement(sql).executeQuery();
            assertResultSet(rs, new Object[][]{{"a"},{"b"},{"c"},{"d"},{"e"}});

        } finally {
            if(conn != null) {
                conn.close();
            }
        }
    }

    @Test
    public void testDistinctAggregatingResultIteratorBug6507() throws Exception {
        doTestDistinctAggregatingResultIteratorBug6507(false, false);
        doTestDistinctAggregatingResultIteratorBug6507(false, true);
        doTestDistinctAggregatingResultIteratorBug6507(true, false);
        doTestDistinctAggregatingResultIteratorBug6507(true, true);
    }

    private void doTestDistinctAggregatingResultIteratorBug6507(boolean desc ,boolean salted) throws Exception {
        Properties props = PropertiesUtil.deepCopy(TEST_PROPERTIES);
        Connection conn = null;
        try {
            conn = DriverManager.getConnection(getUrl(), props);
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
                    " )) "+(salted ? "SALT_BUCKETS =4" : "split on('b')");
            conn.createStatement().execute(sql);

            conn.createStatement().execute("UPSERT INTO "+tableName+" VALUES ('a11','a12','a13','a14','a15')");
            conn.createStatement().execute("UPSERT INTO "+tableName+" VALUES ('a21','a22','a23','a24','a25')");
            conn.createStatement().execute("UPSERT INTO "+tableName+" VALUES ('a31','a32','a33','a38','a35')");
            conn.createStatement().execute("UPSERT INTO "+tableName+" VALUES ('b11','b12','b13','b14','b15')");
            conn.createStatement().execute("UPSERT INTO "+tableName+" VALUES ('b21','b22','b23','b24','b25')");
            conn.createStatement().execute("UPSERT INTO "+tableName+" VALUES ('b31','b32','b33','b34','b35')");
            conn.createStatement().execute("UPSERT INTO "+tableName+" VALUES ('a31','c12','c13','a34','a35')");
            conn.createStatement().execute("UPSERT INTO "+tableName+" VALUES ('a31','a32','c13','a34','a35')");
            conn.createStatement().execute("UPSERT INTO "+tableName+" VALUES ('a31','a32','d13','a35','a35')");
            conn.createStatement().execute("UPSERT INTO "+tableName+" VALUES ('d31','a32','c13','a35','a35')");
            conn.commit();

            sql = "select distinct pk1,max(v1) from "+tableName+" group by pk1,pk2,pk3 order by pk1,pk2,pk3";

            ResultSet rs = conn.prepareStatement(sql).executeQuery();
            assertResultSet(rs, new Object[][]{
                {"a11","a14"},
                {"a21","a24"},
                {"a31","a38"},
                {"a31","a34"},
                {"a31","a35"},
                {"b11","b14"},
                {"b21","b24"},
                {"b31","b34"},
                {"d31","a35"}});

            sql = "select distinct pk2,max(v1) from "+tableName+" group by pk2,pk3 order by pk2,pk3";

            rs = conn.prepareStatement(sql).executeQuery();
            assertResultSet(rs, new Object[][]{
                {"a12","a14"},
                {"a22","a24"},
                {"a32","a38"},
                {"a32","a35"},
                {"b12","b14"},
                {"b22","b24"},
                {"b32","b34"},
                {"c12","a34"}});
        } finally {
            if(conn != null) {
                conn.close();
            }
        }
    }
}

