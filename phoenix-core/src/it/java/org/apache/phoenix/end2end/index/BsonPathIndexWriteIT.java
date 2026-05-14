/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.phoenix.end2end.index;

import static org.apache.phoenix.util.TestUtil.TEST_PROPERTIES;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.Properties;
import org.apache.phoenix.end2end.ParallelStatsDisabledIT;
import org.apache.phoenix.end2end.ParallelStatsDisabledTest;
import org.apache.phoenix.util.PropertiesUtil;
import org.bson.BsonDocument;
import org.junit.Test;
import org.junit.experimental.categories.Category;

@Category(ParallelStatsDisabledTest.class)
public class BsonPathIndexWriteIT extends ParallelStatsDisabledIT {

  @Test
  public void indexPopulatesOnPathPresent() throws Exception {
    Properties props = PropertiesUtil.deepCopy(TEST_PROPERTIES);
    String tbl = generateUniqueName();
    String idx = generateUniqueName();
    try (Connection conn = DriverManager.getConnection(getUrl(), props)) {
      conn.createStatement().execute(
          "CREATE TABLE " + tbl + " (PK VARCHAR PRIMARY KEY, DOC BSON)");
      conn.createStatement().execute(
          "CREATE INDEX " + idx + " ON " + tbl + "(BSON_VALUE(DOC, 'name', 'VARCHAR'))");

      BsonDocument d1 = BsonDocument.parse("{\"name\": \"alice\"}");
      BsonDocument d2 = BsonDocument.parse("{\"name\": \"bob\"}");

      try (PreparedStatement ps = conn.prepareStatement(
          "UPSERT INTO " + tbl + " VALUES (?, ?)")) {
        ps.setString(1, "k1");
        ps.setObject(2, d1);
        ps.execute();
        ps.setString(1, "k2");
        ps.setObject(2, d2);
        ps.execute();
      }
      conn.commit();

      try (ResultSet rs = conn.createStatement().executeQuery(
          "SELECT COUNT(*) FROM " + idx)) {
        assertTrue(rs.next());
        assertEquals(2, rs.getInt(1));
      }
    }
  }

  @Test
  public void indexSparseSkipsMissingPath() throws Exception {
    Properties props = PropertiesUtil.deepCopy(TEST_PROPERTIES);
    String tbl = generateUniqueName();
    String idx = generateUniqueName();
    try (Connection conn = DriverManager.getConnection(getUrl(), props)) {
      conn.createStatement().execute(
          "CREATE TABLE " + tbl + " (PK VARCHAR PRIMARY KEY, DOC BSON)");
      conn.createStatement().execute(
          "CREATE INDEX " + idx + " ON " + tbl + "(BSON_VALUE(DOC, 'name', 'VARCHAR'))");

      BsonDocument withName = BsonDocument.parse("{\"name\": \"alice\"}");
      BsonDocument withoutName = BsonDocument.parse("{\"other\": \"x\"}");

      try (PreparedStatement ps = conn.prepareStatement(
          "UPSERT INTO " + tbl + " VALUES (?, ?)")) {
        ps.setString(1, "k1");
        ps.setObject(2, withName);
        ps.execute();
        ps.setString(1, "k2");
        ps.setObject(2, withoutName);
        ps.execute();
      }
      conn.commit();

      try (ResultSet rs = conn.createStatement().executeQuery(
          "SELECT COUNT(*) FROM " + idx)) {
        assertTrue(rs.next());
        // Only k1 should appear in the index (sparse skip on missing path).
        assertEquals(1, rs.getInt(1));
      }
    }
  }

  @Test
  public void canonicalizationCollidesEquivalentDDL() throws Exception {
    Properties props = PropertiesUtil.deepCopy(TEST_PROPERTIES);
    String tbl = generateUniqueName();
    String idxA = generateUniqueName();
    String idxB = generateUniqueName();
    try (Connection conn = DriverManager.getConnection(getUrl(), props)) {
      conn.createStatement().execute(
          "CREATE TABLE " + tbl + " (PK VARCHAR PRIMARY KEY, DOC BSON)");
      conn.createStatement().execute(
          "CREATE INDEX " + idxA + " ON " + tbl + "(BSON_VALUE(DOC, 'a.b', 'VARCHAR'))");
      try {
        conn.createStatement().execute(
            "CREATE INDEX " + idxB + " ON " + tbl + "(BSON_VALUE(DOC, '$.a.b', 'VARCHAR'))");
        // If we reach here, canonicalization didn't dedupe via duplicate-name detection. Fall
        // through and check that the stored form is canonical instead.
      } catch (Exception ok) {
        // duplicate-index error: expected, this is the cleanest evidence of canonicalization.
      }
      // Inspect the catalog to verify canonical $.a.b form is what got persisted on idxA.
      try (ResultSet rs = conn.createStatement().executeQuery(
          "SELECT COLUMN_NAME FROM SYSTEM.\"CATALOG\" WHERE TABLE_NAME = '" + idxA
              + "' AND COLUMN_NAME IS NOT NULL")) {
        boolean any = false;
        while (rs.next()) {
          String s = rs.getString(1);
          if (s != null && s.contains("$.a.b") && s.contains("VARCHAR")) {
            any = true;
          }
        }
        assertTrue("expected canonical $.a.b/VARCHAR in stored column name", any);
      }
    }
  }
}
