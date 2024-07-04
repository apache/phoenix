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

import static org.apache.phoenix.util.TestUtil.assertResultSet;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import org.junit.Test;
import org.junit.experimental.categories.Category;


@Category(ParallelStatsDisabledTest.class)
public class OrderByIT extends BaseOrderByIT {

    @Test
    public void testPartialOrderForTupleProjectionPlanBug7352() throws Exception {
        doTestPartialOrderForTupleProjectionPlanBug7352(false, false);
        doTestPartialOrderForTupleProjectionPlanBug7352(false, true);
        doTestPartialOrderForTupleProjectionPlanBug7352(true, false);
        doTestPartialOrderForTupleProjectionPlanBug7352(true, true);
    }

    private void doTestPartialOrderForTupleProjectionPlanBug7352(
            boolean desc, boolean salted) throws Exception {
        Connection conn = null;
        try {
            conn = DriverManager.getConnection(getUrl(), props);
            String tableName = generateUniqueName();
            String sql = "create table " + tableName + "( "+
                    " pk1 char(20) not null , " +
                    " pk2 char(20) not null, " +
                    " pk3 char(20) not null," +
                    " v1 varchar, " +
                    " v2 varchar, " +
                    " v3 varchar, " +
                    " CONSTRAINT TEST_PK PRIMARY KEY ( "+
                    "pk1 "+(desc ? "desc" : "")+", "+
                    "pk2 "+(desc ? "desc" : "")+", "+
                    "pk3 "+(desc ? "desc" : "")+
                    " )) "+(salted ? "SALT_BUCKETS =4" : "split on('b')");
            conn.createStatement().execute(sql);

            conn.createStatement().execute("UPSERT INTO "+tableName+" VALUES ('a11','a12','a13','a14','a15','a16')");
            conn.createStatement().execute("UPSERT INTO "+tableName+" VALUES ('a21','a22','a23','a24','a25','a26')");
            conn.createStatement().execute("UPSERT INTO "+tableName+" VALUES ('a31','a32','a33','a34','a35','a36')");
            conn.createStatement().execute("UPSERT INTO "+tableName+" VALUES ('b11','b12','b13','b14','b15','b16')");
            conn.createStatement().execute("UPSERT INTO "+tableName+" VALUES ('b21','b22','b23','b24','b25', 'b26')");
            conn.createStatement().execute("UPSERT INTO "+tableName+" VALUES ('b31','b32','b33','b34','b35', 'b36')");
            conn.commit();

            sql = "select pk3,v1,v2 from (select v1,v2,pk3 from " + tableName + " t where pk1 > 'a10' order by t.v2,t.v1 limit 10) a order by v2";
            ResultSet rs = conn.prepareStatement(sql).executeQuery();
            assertResultSet(rs, new Object[][]{{"a13"},{"a23"},{"a33"},{"b13"},{"b23"},{"b33"}});

            sql = "select pk3,v1,v2 from (select v1,v2,pk3 from " + tableName
                    + " t where pk1 > 'a10' order by t.v2 desc,t.v1 desc limit 10) a order by v2 desc";
            rs = conn.prepareStatement(sql).executeQuery();
            assertResultSet(rs, new Object[][]{{"b33"},{"b23"},{"b13"},{"a33"},{"a23"},{"a13"}});

            sql = "select pk3,v1,v2 from (select v1,v2,pk3 from " + tableName
                    + " t where pk1 > 'a10' order by t.v2 desc,t.v1 desc, t.v3 desc limit 10) a order by v2 desc";
            rs = conn.prepareStatement(sql).executeQuery();
            assertResultSet(rs, new Object[][]{{"b33"},{"b23"},{"b13"},{"a33"},{"a23"},{"a13"}});

            sql = "select pk3,v1,v2 from (select v1,v2,pk3 from " + tableName
                    + " t where pk1 > 'a10' order by t.v2 desc,t.v1 desc, t.v3 asc limit 10) a order by v2 desc";
            rs = conn.prepareStatement(sql).executeQuery();
            assertResultSet(rs, new Object[][]{{"b33"},{"b23"},{"b13"},{"a33"},{"a23"},{"a13"}});

            sql = "select v2,cnt from (select count(pk3) cnt,v1,v2 from " + tableName
                    + " t where pk1 > 'a10' group by t.v1, t.v2, t.v3 limit 10) a order by v1";
            rs = conn.prepareStatement(sql).executeQuery();
            assertResultSet(rs, new Object[][]{{"a15"},{"a25"},{"a35"},{"b15"},{"b25"},{"b35"}});

            sql = "select sub, pk2Cnt from (select substr(v2,0,3) sub, cast (count(pk3) as bigint) cnt, count(pk2) pk2Cnt from "
                    + tableName
                    + " t where pk1 > 'a10' group by t.v1 ,t.v2, t.v3 "
                    + " order by count(pk3) desc,t.v2 desc,t.v3 desc limit 10) a order by cnt desc ,sub desc";
            rs = conn.prepareStatement(sql).executeQuery();
            assertResultSet(rs, new Object[][]{{"b35"},{"b25"},{"b15"},{"a35"},{"a25"},{"a15"}});

            sql = "select sub, pk2Cnt from (select substr(v2,0,3) sub,cast (count(pk3) as bigint) cnt, count(pk2) pk2Cnt from "
                    + tableName
                    + " t where pk1 > 'a10' group by t.v1 ,t.v2, t.v3 "
                    + " order by count(pk3) desc,t.v2 desc,t.v3 asc limit 10) a order by cnt desc ,sub desc";
            rs = conn.prepareStatement(sql).executeQuery();
            assertResultSet(rs, new Object[][]{{"b35"},{"b25"},{"b15"},{"a35"},{"a25"},{"a15"}});

            sql = "select sub, pk2Cnt from (select substr(v2,0,3) sub,cast (count(pk3) as bigint) cnt, count(pk2) pk2Cnt from "
                    + tableName
                    + " t where pk1 > 'a10' group by t.v1 ,t.v2, t.v3 "
                    + " order by t.v2 desc, count(pk3) desc, t.v3 desc limit 10) a order by sub desc, cnt desc";
            rs = conn.prepareStatement(sql).executeQuery();
            assertResultSet(rs, new Object[][]{{"b35"},{"b25"},{"b15"},{"a35"},{"a25"},{"a15"}});

            sql = "select sub, pk2Cnt from (select substr(v2,0,3) sub,cast (count(pk3) as bigint) cnt, count(pk2) pk2Cnt from "
                    + tableName
                    + " t where pk1 > 'a10' group by t.v1 ,t.v2, t.v3 "
                    + " order by t.v2 desc, count(pk3) desc, t.v3 asc limit 10) a order by sub desc, cnt desc";
            rs = conn.prepareStatement(sql).executeQuery();
            assertResultSet(rs, new Object[][]{{"b35"},{"b25"},{"b15"},{"a35"},{"a25"},{"a15"}});

            sql = "select v1, pk3, v2 from (select v1,v2,pk3 from " + tableName
                    + " t where pk1 > 'a10' order by t.v2,t.v1, t.v3 limit 10) a order by v1";
            rs = conn.prepareStatement(sql).executeQuery();
            assertResultSet(rs, new Object[][]{{"a14"},{"a24"},{"a34"},{"b14"},{"b24"},{"b34"}});

            sql = "select pk3,pk1,pk2 from (select pk1,pk2,pk3 from " + tableName
                    + " t where pk1 > 'a10' order by t.v2, t.v1, t.v3 limit 10) a order by pk3";
            rs = conn.prepareStatement(sql).executeQuery();
            assertResultSet(rs, new Object[][]{{"a13"},{"a23"},{"a33"},{"b13"},{"b23"},{"b33"}});

            conn.createStatement().execute("UPSERT INTO "+tableName+" VALUES ('b31','a12','a13','a14','a15','a16')");
            conn.createStatement().execute("UPSERT INTO "+tableName+" VALUES ('b31','a22','a23','a24','a25','a26')");
            conn.createStatement().execute("UPSERT INTO "+tableName+" VALUES ('b31','a32','a33','a34','a35','a36')");
            conn.createStatement().execute("UPSERT INTO "+tableName+" VALUES ('b31','b12','b13','b14','b15','b16')");
            conn.createStatement().execute("UPSERT INTO "+tableName+" VALUES ('b31','b22','b23','b24','b25', 'b26')");
            conn.commit();

            sql = "select sub, v1 from (select substr(pk3,0,3) sub, pk2, v1 from "
                    + tableName + " t where pk1 = 'b31' order by pk2, pk3 limit 10) a order by pk2 desc ,sub desc";
            rs = conn.prepareStatement(sql).executeQuery();
            assertResultSet(rs, new Object[][]{{"b33"},{"b23"},{"b13"},{"a33"},{"a23"},{"a13"}});

            sql = "select sub, v1 from (select substr(pk3,0,3) sub, pk2, v1 from "
                    + tableName + " t where pk1 = 'b31' order by pk2 desc, pk3 desc limit 10) a order by pk2 desc ,sub desc";
            rs = conn.prepareStatement(sql).executeQuery();
            assertResultSet(rs, new Object[][]{{"b33"},{"b23"},{"b13"},{"a33"},{"a23"},{"a13"}});

            sql = "select sub, v1 from (select substr(pk2,0,3) sub, pk3, v1 from "
                    + tableName + " t where pk1 = 'b31' order by pk2, pk3 limit 10) a order by sub desc ,pk3 desc";
            rs = conn.prepareStatement(sql).executeQuery();
            assertResultSet(rs, new Object[][]{{"b32"},{"b22"},{"b12"},{"a32"},{"a22"},{"a12"}});

            sql = "select sub, v1 from (select substr(pk2,0,3) sub, pk3, v1 from "
                    + tableName + " t where pk1 = 'b31' order by pk2 desc, pk3 desc limit 10) a order by sub desc, pk3 desc";
            rs = conn.prepareStatement(sql).executeQuery();
            assertResultSet(rs, new Object[][]{{"b32"},{"b22"},{"b12"},{"a32"},{"a22"},{"a12"}});
        } finally {
            if(conn != null) {
                conn.close();
            }
        }
    }

}
