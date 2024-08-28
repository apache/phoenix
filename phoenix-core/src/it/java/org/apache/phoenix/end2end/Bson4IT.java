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

import java.io.File;
import java.io.IOException;
import java.net.URL;
import java.nio.charset.Charset;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.Collection;
import java.util.Properties;

import org.bson.BsonArray;
import org.bson.BsonBinary;
import org.bson.BsonDocument;
import org.bson.BsonNull;
import org.bson.BsonString;
import org.bson.RawBsonDocument;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import org.apache.commons.io.FileUtils;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.phoenix.compile.ExplainPlan;
import org.apache.phoenix.compile.ExplainPlanAttributes;
import org.apache.phoenix.jdbc.PhoenixPreparedStatement;
import org.apache.phoenix.thirdparty.com.google.common.base.Preconditions;
import org.apache.phoenix.util.PropertiesUtil;

import static org.apache.phoenix.util.TestUtil.TEST_PROPERTIES;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

/**
 * Tests for BSON.
 */
@Category(ParallelStatsDisabledTest.class)
@RunWith(Parameterized.class)
public class Bson4IT extends ParallelStatsDisabledIT {

  private final boolean columnEncoded;
  private final boolean coveredIndex;

  public Bson4IT(boolean columnEncoded, boolean coveredIndex) {
    this.columnEncoded = columnEncoded;
    this.coveredIndex = coveredIndex;
  }

  @Parameterized.Parameters(name =
      "Bson4IT_columnEncoded={0}, coveredIndex={1}")
  public static synchronized Collection<Object[]> data() {
    return Arrays.asList(
        new Object[][] {
            {false, false},
            {false, true},
            {true, false},
            {true, true}
        });
  }

  private static String getJsonString(String jsonFilePath) throws IOException {
    URL fileUrl = Bson4IT.class.getClassLoader().getResource(jsonFilePath);
    Preconditions.checkArgument(fileUrl != null, "File path " + jsonFilePath + " seems invalid");
    return FileUtils.readFileToString(new File(fileUrl.getFile()), Charset.defaultCharset());
  }

  @Test
  public void testBsonValueFunction() throws Exception {
    Properties props = PropertiesUtil.deepCopy(TEST_PROPERTIES);
    String tableName = generateUniqueName();
    String indexName1 = "IDX1_" + tableName;
    String indexName2 = "IDX2_" + tableName;
    try (Connection conn = DriverManager.getConnection(getUrl(), props)) {
      String ddl = "CREATE TABLE " + tableName
          + " (PK1 VARCHAR NOT NULL, C1 VARCHAR, COL BSON"
          + " CONSTRAINT pk PRIMARY KEY(PK1)) "
          + (this.columnEncoded ? "" : "COLUMN_ENCODED_BYTES=0");

      final String indexDdl1;
      if (!this.coveredIndex) {
        indexDdl1 = "CREATE UNCOVERED INDEX " + indexName1 + " ON " + tableName
            + "(BSON_VALUE(COL, 'rather[3].outline.clock', 'VARCHAR')) WHERE "
            + "BSON_VALUE(COL, 'rather[3].outline.clock', 'VARCHAR') IS NOT NULL";
      } else {
        indexDdl1 = "CREATE INDEX " + indexName1 + " ON " + tableName
            + "(BSON_VALUE(COL, 'rather[3].outline.clock', 'VARCHAR')) INCLUDE(COL) WHERE "
            + "BSON_VALUE(COL, 'rather[3].outline.clock', 'VARCHAR') IS NOT NULL";
      }

      final String indexDdl2;
      if (!this.coveredIndex) {
        indexDdl2 = "CREATE UNCOVERED INDEX " + indexName2 + " ON " + tableName
            + "(BSON_VALUE(COL, 'result[1].location.coordinates.longitude', 'DOUBLE')) WHERE "
            + "BSON_VALUE(COL, 'result[1].location.coordinates.longitude', 'DOUBLE') IS NOT NULL";
      } else {
        indexDdl2 = "CREATE INDEX " + indexName2 + " ON " + tableName
            + "(BSON_VALUE(COL, 'result[1].location.coordinates.longitude', 'DOUBLE')) "
            + "INCLUDE(COL) WHERE "
            + "BSON_VALUE(COL, 'result[1].location.coordinates.longitude', 'DOUBLE') IS NOT NULL";
      }

      conn.createStatement().execute(ddl);
      conn.createStatement().execute(indexDdl1);
      conn.createStatement().execute(indexDdl2);

      String sample1 = getJsonString("json/sample_01.json");
      String sample2 = getJsonString("json/sample_02.json");
      String sample3 = getJsonString("json/sample_03.json");
      BsonDocument bsonDocument1 = RawBsonDocument.parse(sample1);
      BsonDocument bsonDocument2 = RawBsonDocument.parse(sample2);
      BsonDocument bsonDocument3 = RawBsonDocument.parse(sample3);

      PreparedStatement stmt =
              conn.prepareStatement("UPSERT INTO " + tableName + " VALUES (?,?,?)");
      stmt.setString(1, "pk0001");
      stmt.setString(2, "0002");
      stmt.setObject(3, bsonDocument1);
      stmt.executeUpdate();

      stmt.setString(1, "pk1010");
      stmt.setString(2, "1010");
      stmt.setObject(3, bsonDocument2);
      stmt.executeUpdate();

      stmt.setString(1, "pk1011");
      stmt.setString(2, "1011");
      stmt.setObject(3, bsonDocument3);
      stmt.executeUpdate();

      conn.commit();

      ResultSet rs = conn.createStatement().executeQuery("SELECT count(*) FROM " + tableName);
      assertTrue(rs.next());
      assertEquals(3, rs.getInt(1));

      rs = conn.createStatement().executeQuery("SELECT count(*) FROM " + indexName1);
      assertTrue(rs.next());
      assertEquals(1, rs.getInt(1));

      rs = conn.createStatement().executeQuery("SELECT count(*) FROM " + indexName2);
      assertTrue(rs.next());
      assertEquals(1, rs.getInt(1));

      PreparedStatement ps = conn.prepareStatement("SELECT PK1, COL FROM " + tableName
          + " WHERE BSON_VALUE(COL, 'result[1].location.coordinates.longitude', 'DOUBLE') = ?");
      ps.setDouble(1, 52.3736);

      rs = ps.executeQuery();

      assertTrue(rs.next());
      assertEquals("pk1011", rs.getString(1));
      BsonDocument actualDoc = (BsonDocument) rs.getObject(2);
      assertEquals(bsonDocument3, actualDoc);

      assertFalse(rs.next());

      validateIndexUsed(ps, indexName2);

      ps = conn.prepareStatement("SELECT PK1, COL FROM " + tableName
          + " WHERE BSON_VALUE(COL, 'rather[3].outline.clock', 'VARCHAR') = ?");
      ps.setString(1, "personal");

      rs = ps.executeQuery();

      assertTrue(rs.next());
      assertEquals("pk1010", rs.getString(1));
      actualDoc = (BsonDocument) rs.getObject(2);
      assertEquals(bsonDocument2, actualDoc);

      assertFalse(rs.next());

      validateIndexUsed(ps, indexName1);

      BsonDocument updateExp = new BsonDocument()
              .append("$ADD", new BsonDocument()
                      .append("new_samples",
                              new BsonDocument().append("$set",
                                      new BsonArray(Arrays.asList(
                                              new BsonBinary(Bytes.toBytes("Sample10")),
                                              new BsonBinary(Bytes.toBytes("Sample12")),
                                              new BsonBinary(Bytes.toBytes("Sample13")),
                                              new BsonBinary(Bytes.toBytes("Sample14"))
                                      )))))
              .append("$DELETE_FROM_SET", new BsonDocument()
                      .append("new_samples",
                              new BsonDocument().append("$set",
                                      new BsonArray(Arrays.asList(
                                              new BsonBinary(Bytes.toBytes("Sample02")),
                                              new BsonBinary(Bytes.toBytes("Sample03"))
                                      )))))
              .append("$SET", new BsonDocument()
                      .append("rather[3].outline.clock", new BsonString("personal2")))
              .append("$UNSET", new BsonDocument()
                      .append("rather[3].outline.halfway.so[2][2]", new BsonNull()));

      String conditionExpression =
              "field_not_exists(newrecord) AND field_exists(rather[3].outline.halfway.so[2][2])";

      BsonDocument conditionDoc = new BsonDocument();
      conditionDoc.put("$EXPR", new BsonString(conditionExpression));
      conditionDoc.put("$VAL", new BsonDocument());

      stmt = conn.prepareStatement("UPSERT INTO " + tableName
              + " VALUES (?) ON DUPLICATE KEY UPDATE COL = CASE WHEN"
              + " BSON_CONDITION_EXPRESSION(COL, '" + conditionDoc.toJson() + "')"
              + " THEN BSON_UPDATE_EXPRESSION(COL, '" + updateExp + "') ELSE COL END");

      stmt.setString(1, "pk1010");
      stmt.executeUpdate();

      conn.commit();

      ps = conn.prepareStatement("SELECT PK1, COL FROM " + tableName
          + " WHERE BSON_VALUE(COL, 'rather[3].outline.clock', 'VARCHAR') = ?");
      ps.setString(1, "personal");

      rs = ps.executeQuery();
      assertFalse(rs.next());

      validateIndexUsed(ps, indexName1);

      ps = conn.prepareStatement("SELECT PK1, COL FROM " + tableName
          + " WHERE BSON_VALUE(COL, 'result[1].location.coordinates.longitude', 'DOUBLE') = ?");
      ps.setDouble(1, 52.37);

      rs = ps.executeQuery();
      assertFalse(rs.next());

      validateIndexUsed(ps, indexName2);
    }
  }

  private static void validateIndexUsed(PreparedStatement ps, String indexName)
      throws SQLException {
    ExplainPlan plan = ps.unwrap(PhoenixPreparedStatement.class).optimizeQuery().getExplainPlan();
    ExplainPlanAttributes explainPlanAttributes = plan.getPlanStepsAsAttributes();
    assertEquals(indexName, explainPlanAttributes.getTableName());
    assertEquals("PARALLEL 1-WAY", explainPlanAttributes.getIteratorTypeAndScanSize());
    assertEquals("RANGE SCAN ", explainPlanAttributes.getExplainScanType());
  }

}