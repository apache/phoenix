/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.phoenix.end2end;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.util.Properties;
import org.junit.Test;
import org.junit.experimental.categories.Category;

@Category(ParallelStatsDisabledTest.class)
public class MultipleUpsertIT extends ParallelStatsDisabledIT {
  @Test
  public void testUpsertMultiple() throws Exception {
    Properties props = new Properties();
    Connection conn = DriverManager.getConnection(getUrl(), props);
    String tableName = generateUniqueName();
    String ddl =
      "CREATE TABLE " + tableName + "(K VARCHAR NOT NULL PRIMARY KEY, INT INTEGER, INT2 INTEGER)";
    conn.createStatement().execute(ddl);

    conn.createStatement().execute("UPSERT INTO " + tableName + " VALUES ('A', 11, 12)");
    conn.createStatement().execute("UPSERT INTO " + tableName + "(K, INT) VALUES ('B', 2)");
    conn.createStatement()
      .execute("UPSERT INTO " + tableName + "(K, INT, INT2) VALUES ('E', 5, 5),('F', 61, 6)");
    conn.createStatement()
      .execute("UPSERT INTO " + tableName + " VALUES ('C', 31, 32),('D', 41, 42)");
    conn.createStatement().execute("UPSERT INTO " + tableName + " VALUES ('G', 7, 72),('H', 8)");
    conn.createStatement().execute("UPSERT INTO " + tableName + " VALUES ('I', 9),('I', 10)");
    conn.commit();

    ResultSet rs = conn.createStatement().executeQuery("SELECT COUNT(*) FROM " + tableName);
    assertTrue(rs.next());
    assertEquals(9, rs.getInt(1));

    rs = conn.createStatement().executeQuery("SELECT * FROM " + tableName + " ORDER BY K");
    rs.next();
    assertEquals(rs.getString(1), "A");
    assertEquals(rs.getInt(2), 11);
    assertEquals(rs.getInt(3), 12);
    rs.next();
    assertEquals(rs.getString(1), "B");
    assertEquals(rs.getInt(2), 2);
    rs.next();
    assertEquals(rs.getString(1), "C");
    assertEquals(rs.getInt(2), 31);
    assertEquals(rs.getInt(3), 32);
    rs.next();
    assertEquals(rs.getString(1), "D");
    assertEquals(rs.getInt(2), 41);
    assertEquals(rs.getInt(3), 42);
    rs.next();
    assertEquals(rs.getString(1), "E");
    assertEquals(rs.getInt(2), 5);
    assertEquals(rs.getInt(3), 5);
    rs.next();
    assertEquals(rs.getString(1), "F");
    assertEquals(rs.getInt(2), 61);
    assertEquals(rs.getInt(3), 6);
    rs.next();
    assertEquals(rs.getString(1), "G");
    assertEquals(rs.getInt(2), 7);
    assertEquals(rs.getInt(3), 72);
    rs.next();
    assertEquals(rs.getString(1), "H");
    assertEquals(rs.getInt(2), 8);
    rs.next();
    assertEquals(rs.getString(1), "I");
    assertEquals(rs.getInt(2), 10);
  }

  @Test
  public void testUpsertMultiple2() throws Exception {
    Properties props = new Properties();
    Connection conn = DriverManager.getConnection(getUrl(), props);
    String tableName = generateUniqueName();
    String ddl = "CREATE TABLE " + tableName + "(K VARCHAR NOT NULL PRIMARY KEY, INT INTEGER)";
    conn.createStatement().execute(ddl);

    conn.createStatement()
      .execute("UPSERT INTO " + tableName + " VALUES ('A', 1),(SUBSTR('APPLE',0,2), 2*2)");
    conn.createStatement()
      .execute("UPSERT INTO " + tableName + " VALUES (SUBSTR('DELTA',0,1), 5),('C', 2*3)");
    conn.commit();

    ResultSet rs =
      conn.createStatement().executeQuery("SELECT * FROM " + tableName + " ORDER BY K");
    rs.next();
    assertEquals(rs.getString(1), "A");
    assertEquals(rs.getInt(2), 1);
    rs.next();
    assertEquals(rs.getString(1), "AP");
    assertEquals(rs.getInt(2), 4);
    rs.next();
    assertEquals(rs.getString(1), "C");
    assertEquals(rs.getInt(2), 6);
    rs.next();
    assertEquals(rs.getString(1), "D");
    assertEquals(rs.getInt(2), 5);
    assertFalse(rs.next());

  }
}
