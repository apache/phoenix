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
package org.apache.phoenix.end2end.join;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;

import org.apache.phoenix.end2end.ParallelStatsDisabledIT;
import org.apache.phoenix.end2end.ParallelStatsDisabledTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;

@Category(ParallelStatsDisabledTest.class)
public class WhereOptimizerForJoinFiltersIT extends ParallelStatsDisabledIT {

  String allValues =
      "('201904','ID2_VAL','ID3_VAL','01','000000','ID4_VAL','ID5_VAL','2019-05-30 22:41:37.000')\n"
          + "('201905','ID2_VAL','ID3_VAL','01','000000','ID4_VAL','ID5_VAL','2019-12-31 22:59:59.000') \n"
          + "('201905','ID2_VAL','ID3_VAL','30','000000','ID4_VAL','ID5_VAL','2019-12-31 22:59:59.000') \n"
          + "('201904','ID2_VAL','ID3_VAL2','01','000000','ID4_VAL','ID5_VAL','2019-05-30 22:41:37.000')\n"
          + "('201905','ID2_VAL','ID3_VAL2','30','000000','ID4_VAL','ID5_VAL','2019-12-31 22:59:59.000')";

  private void createTable(Connection conn, String tableName) throws SQLException {
    conn.createStatement().execute("CREATE TABLE " + tableName + " (" + "    id1 CHAR(6) NOT NULL,"
        + "    id2 VARCHAR(22) NOT NULL," + "    id3 VARCHAR(12) NOT NULL,"
        + "    id4 CHAR(2) NOT NULL," + "    id5 CHAR(6) NOT NULL, "
        + "    id6 VARCHAR(200) NOT NULL," + "    id7 VARCHAR(50) NOT NULL," + "    ts TIMESTAMP ,"
        + "    CONSTRAINT PK_JOIN_AND_INTERSECTION_TABLE PRIMARY KEY(id1,id2,id3,id4,id5,id6,id7)"
        + ")");
  }

  @Test public void testJoin() throws SQLException {
    String leftTable = generateUniqueName();
    String rightTable = generateUniqueName();
    Connection conn = DriverManager.getConnection(getUrl());
    createTable(conn, leftTable);
    createTable(conn, rightTable);
    for (String values : allValues.split("\n")) {
      conn.createStatement().execute(
          "UPSERT INTO " + leftTable + "(id1,id2,id3,id4, id5, id6, id7,ts) VALUES" + values);
      conn.createStatement().execute(
          "UPSERT INTO " + rightTable + "(id1,id2,id3,id4, id5, id6, id7,ts) VALUES" + values);
    }
    conn.commit();
    ResultSet rs =
        conn.createStatement().executeQuery("select count(*) from "+leftTable);
    assertTrue(rs.next());
    assertEquals(5, rs.getInt(1));
    String query =
        "SELECT m.*,r.* FROM "+leftTable+" m join "+rightTable+" r "
            + " on m.id3 = r.id3  and m.id2 = r.id2 "
            + " and m.id4 = r.id4 and m.id5 = r.id5 "
            + " and m.id1 = r.id1 and m.ts = r.ts "
            + " where m.id1 IN ('201904','201905') "
            + " and r.id1 IN ('201904','201905') and r.id2 = 'ID2_VAL' and m.id2 = 'ID2_VAL' "
            + "            and m.id3 IN ('ID3_VAL','ID3_VAL2') "
            + " and r.id3 IN ('ID3_VAL','ID3_VAL2') LIMIT 1000000000";
    rs = conn.createStatement().executeQuery(query);
    assertTrue(rs.next());
    query =
        "SELECT m.*,r.* FROM "+leftTable+" m join "+rightTable+" r "
            + " on m.id3 = r.id3 and m.id2 = r.id2 "
            + " and m.id4 = r.id4  and m.id5 = r.id5 "
            + " and m.id1 = r.id1  and m.ts = r.ts "
            + " where  r.id1 IN ('201904','201905') "
            + " and r.id2 = 'ID2_VAL' "
            + " and r.id3 IN ('ID3_VAL','ID3_VAL2') LIMIT 1000000000";
    rs = conn.createStatement().executeQuery(query);
    assertTrue(rs.next());
    conn.close();
  }

}
