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

import static org.apache.phoenix.util.TestUtil.TEST_PROPERTIES;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.File;
import java.io.IOException;
import java.net.URL;
import java.nio.charset.Charset;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.util.Arrays;
import java.util.Base64;
import java.util.Map;
import java.util.Properties;
import org.apache.commons.io.FileUtils;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.phoenix.compile.ExplainPlan;
import org.apache.phoenix.compile.ExplainPlanAttributes;
import org.apache.phoenix.jdbc.PhoenixPreparedStatement;
import org.apache.phoenix.query.QueryConstants;
import org.apache.phoenix.schema.types.PDouble;
import org.apache.phoenix.util.PropertiesUtil;
import org.bson.BsonArray;
import org.bson.BsonBinary;
import org.bson.BsonDocument;
import org.bson.BsonDouble;
import org.bson.BsonNull;
import org.bson.BsonString;
import org.bson.RawBsonDocument;
import org.junit.Assert;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import org.apache.phoenix.thirdparty.com.google.common.base.Preconditions;

/**
 * Tests for BSON with expression field key alias.
 */
@Category(ParallelStatsDisabledTest.class)
public class Bson5IT extends ParallelStatsDisabledIT {

  private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

  private static String getJsonString(String jsonFilePath) throws IOException {
    URL fileUrl = Bson5IT.class.getClassLoader().getResource(jsonFilePath);
    Preconditions.checkArgument(fileUrl != null, "File path " + jsonFilePath + " seems invalid");
    return FileUtils.readFileToString(new File(fileUrl.getFile()), Charset.defaultCharset());
  }

  /**
   * Conditional Upserts for BSON pass where the Condition Expression is of SQL style.
   */
  @Test
  public void testBsonOpsWithSqlConditionsUpdateSuccess() throws Exception {
    Properties props = PropertiesUtil.deepCopy(TEST_PROPERTIES);
    String tableName = generateUniqueName();
    String cdcName = generateUniqueName();
    String indexName1 = "IDX1_" + tableName;
    String indexName2 = "IDX2_" + tableName;
    try (Connection conn = DriverManager.getConnection(getUrl(), props)) {
      String ddl = "CREATE TABLE " + tableName + " (PK1 VARCHAR NOT NULL, C1 VARCHAR, COL BSON"
        + " CONSTRAINT pk PRIMARY KEY(PK1))";
      String cdcDdl = "CREATE CDC " + cdcName + " ON " + tableName;
      String indexDdl1 = "CREATE UNCOVERED INDEX " + indexName1 + " ON " + tableName
        + "(BSON_VALUE(COL, 'rather[3].outline.clock', 'VARCHAR')) "
        + "WHERE BSON_VALUE(COL, 'rather[3].outline.clock', 'VARCHAR') IS NOT NULL "
        + "CONSISTENCY = EVENTUAL";
      String indexDdl2 = "CREATE UNCOVERED INDEX " + indexName2 + " ON " + tableName
        + "(BSON_VALUE(COL, 'result[1].location.coordinates.longitude', 'DOUBLE')) "
        + "WHERE BSON_VALUE(COL, 'result[1].location.coordinates.longitude', 'DOUBLE') IS NOT NULL "
        + "CONSISTENCY = EVENTUAL";
      conn.createStatement().execute(ddl);
      conn.createStatement().execute(cdcDdl);
      conn.createStatement().execute(indexDdl1);
      conn.createStatement().execute(indexDdl2);
      Timestamp ts1 = new Timestamp(System.currentTimeMillis());
      Thread.sleep(100);

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
      Thread.sleep(100);
      Timestamp ts2 = new Timestamp(System.currentTimeMillis());

      PreparedStatement indexPs = conn.prepareStatement("SELECT PK1, COL FROM " + tableName
        + " WHERE BSON_VALUE(COL, 'rather[3].outline.clock', 'VARCHAR') = ?");
      indexPs.setString(1, "personal");
      ResultSet indexRs = indexPs.executeQuery();
      assertFalse(indexRs.next());

      Thread.sleep(11000);
      indexRs = indexPs.executeQuery();
      assertTrue(indexRs.next());
      assertEquals("pk1010", indexRs.getString(1));
      BsonDocument actualDoc = (BsonDocument) indexRs.getObject(2);
      assertEquals(bsonDocument2, actualDoc);
      assertFalse(indexRs.next());
      validateExplainPlan(indexPs, indexName1, "RANGE SCAN ");

      indexPs = conn.prepareStatement("SELECT PK1 FROM " + tableName
        + " WHERE BSON_VALUE(COL, 'result[1].location.coordinates.longitude', 'DOUBLE') = ?");
      indexPs.setDouble(1, 52.3736);
      indexRs = indexPs.executeQuery();
      assertTrue(indexRs.next());
      assertEquals("pk1011", indexRs.getString(1));
      assertFalse(indexRs.next());
      validateExplainPlan(indexPs, indexName2, "RANGE SCAN ");

      testCDCAfterFirstUpsert(conn, cdcName, ts1, ts2, bsonDocument1, bsonDocument2, bsonDocument3);

      ts1 = new Timestamp(System.currentTimeMillis());
      Thread.sleep(100);

      String conditionExpression =
        "#press = :press AND #track[0].#shot[2][0].#city.#standard[50] = :softly";

      // {
      // "$EXPR": "#press = :press AND #track[0].#shot[2][0].#city.#standard[50] = :softly",
      // "$VAL": {
      // ":press": "beat",
      // ":softly": "softly"
      // },
      // "$KEYS": {
      // "#press": "press",
      // "#track": "track",
      // "#shot": "shot",
      // "#city": "city",
      // "#standard": "standard"
      // }
      // }
      BsonDocument conditionDoc = new BsonDocument();
      conditionDoc.put("$EXPR", new BsonString(conditionExpression));
      conditionDoc.put("$VAL", new BsonDocument().append(":press", new BsonString("beat"))
        .append(":softly", new BsonString("softly")));
      conditionDoc.put("$KEYS",
        new BsonDocument().append("#press", new BsonString("press"))
          .append("#track", new BsonString("track")).append("#shot", new BsonString("shot"))
          .append("#city", new BsonString("city")).append("#standard", new BsonString("standard")));

      String query = "SELECT * FROM " + tableName
        + " WHERE PK1 = 'pk0001' AND C1 = '0002' AND NOT BSON_CONDITION_EXPRESSION(COL, '"
        + conditionDoc.toJson() + "')";
      ResultSet rs = conn.createStatement().executeQuery(query);

      assertTrue(rs.next());
      assertEquals("pk0001", rs.getString(1));
      assertEquals("0002", rs.getString(2));
      BsonDocument document1 = (BsonDocument) rs.getObject(3);
      assertEquals(bsonDocument1, document1);

      assertFalse(rs.next());

      conditionExpression =
        "#press = :press AND #track[0].#shot[2][0].#city.#standard[5] = :softly";

      conditionDoc = new BsonDocument();
      conditionDoc.put("$EXPR", new BsonString(conditionExpression));
      conditionDoc.put("$VAL", new BsonDocument().append(":press", new BsonString("beat"))
        .append(":softly", new BsonString("softly")));
      conditionDoc.put("$KEYS",
        new BsonDocument().append("#press", new BsonString("press"))
          .append("#track", new BsonString("track")).append("#shot", new BsonString("shot"))
          .append("#city", new BsonString("city")).append("#standard", new BsonString("standard")));

      query = "SELECT * FROM " + tableName
        + " WHERE PK1 = ? AND C1 = ? AND BSON_CONDITION_EXPRESSION(COL, ?)";
      PreparedStatement ps = conn.prepareStatement(query);
      ps.setString(1, "pk0001");
      ps.setString(2, "0002");
      ps.setObject(3, conditionDoc);

      rs = ps.executeQuery();

      assertTrue(rs.next());
      assertEquals("pk0001", rs.getString(1));
      assertEquals("0002", rs.getString(2));
      document1 = (BsonDocument) rs.getObject(3);
      assertEquals(bsonDocument1, document1);

      assertFalse(rs.next());

      BsonDocument updateExp = new BsonDocument().append("$SET",
        new BsonDocument()
          .append("browserling", new BsonBinary(PDouble.INSTANCE.toBytes(-505169340.54880095)))
          .append("track[0].shot[2][0].city.standard[5]", new BsonString("soft"))
          .append("track[0].shot[2][0].city.problem[2]",
            new BsonString("track[0].shot[2][0].city.problem[2] + 529.435")))
        .append("$UNSET",
          new BsonDocument().append("track[0].shot[2][0].city.flame", new BsonNull()));

      stmt = conn.prepareStatement(
        "UPSERT INTO " + tableName + " VALUES (?) ON DUPLICATE KEY UPDATE COL = CASE WHEN"
          + " BSON_CONDITION_EXPRESSION(COL, '" + conditionDoc.toJson() + "')"
          + " THEN BSON_UPDATE_EXPRESSION(COL, '" + updateExp + "') ELSE COL END," + " C1 = ?");
      stmt.setString(1, "pk0001");
      stmt.setString(2, "0003");
      stmt.executeUpdate();

      updateExp = new BsonDocument()
        .append("$ADD",
          new BsonDocument().append("new_samples", new BsonDocument().append("$set",
            new BsonArray(Arrays.asList(new BsonBinary(Bytes.toBytes("Sample10")),
              new BsonBinary(Bytes.toBytes("Sample12")), new BsonBinary(Bytes.toBytes("Sample13")),
              new BsonBinary(Bytes.toBytes("Sample14")))))))
        .append("$DELETE_FROM_SET",
          new BsonDocument().append("new_samples",
            new BsonDocument().append("$set",
              new BsonArray(Arrays.asList(new BsonBinary(Bytes.toBytes("Sample02")),
                new BsonBinary(Bytes.toBytes("Sample03")))))))
        .append("$SET",
          new BsonDocument().append("newrecord", ((BsonArray) (document1.get("track"))).get(0)))
        .append("$UNSET",
          new BsonDocument().append("rather[3].outline.halfway.so[2][2]", new BsonNull()));

      conditionExpression =
        "field_not_exists(newrecord) AND field_exists(#rather[3].#outline.#halfway.#so[2][2])";

      conditionDoc = new BsonDocument();
      conditionDoc.put("$EXPR", new BsonString(conditionExpression));
      conditionDoc.put("$VAL", new BsonDocument());
      conditionDoc.put("$KEYS",
        new BsonDocument().append("#rather", new BsonString("rather"))
          .append("#outline", new BsonString("outline"))
          .append("#halfway", new BsonString("halfway")).append("#so", new BsonString("so")));

      stmt = conn.prepareStatement(
        "UPSERT INTO " + tableName + " VALUES (?) ON DUPLICATE KEY UPDATE COL = CASE WHEN"
          + " BSON_CONDITION_EXPRESSION(COL, '" + conditionDoc.toJson() + "')"
          + " THEN BSON_UPDATE_EXPRESSION(COL, '" + updateExp + "') ELSE COL END");

      stmt.setString(1, "pk1010");
      stmt.executeUpdate();

      updateExp = new BsonDocument()
        .append("$SET", new BsonDocument().append("result[1].location.state", new BsonString("AK")))
        .append("$UNSET", new BsonDocument().append("result[4].emails[1]", new BsonNull()));

      conditionExpression = "#result[2].#location.#coordinates.#latitude > :latitude OR "
        + "(field_exists(#result[1].#location) AND #result[1].#location.#state != :state"
        + " AND field_exists(#result[4].#emails[1]))";

      conditionDoc = new BsonDocument();
      conditionDoc.put("$EXPR", new BsonString(conditionExpression));
      conditionDoc.put("$VAL", new BsonDocument().append(":latitude", new BsonDouble(0))
        .append(":state", new BsonString("AK")));
      conditionDoc.put("$KEYS",
        new BsonDocument().append("#result", new BsonString("result"))
          .append("#location", new BsonString("location"))
          .append("#coordinates", new BsonString("coordinates"))
          .append("#latitude", new BsonString("latitude")).append("#state", new BsonString("state"))
          .append("#emails", new BsonString("emails")));

      stmt = conn.prepareStatement(
        "UPSERT INTO " + tableName + " VALUES (?) ON DUPLICATE KEY UPDATE COL = CASE WHEN"
          + " BSON_CONDITION_EXPRESSION(COL, '" + conditionDoc.toJson() + "')"
          + " THEN BSON_UPDATE_EXPRESSION(COL, '" + updateExp + "') ELSE COL END");

      stmt.setString(1, "pk1011");
      stmt.executeUpdate();

      conn.commit();
      Thread.sleep(100);
      ts2 = new Timestamp(System.currentTimeMillis());

      testCDCPostUpdate(conn, cdcName, ts1, ts2, bsonDocument1, bsonDocument2, bsonDocument3);

      query = "SELECT * FROM " + tableName;
      rs = conn.createStatement().executeQuery(query);

      assertTrue(rs.next());
      assertEquals("pk0001", rs.getString(1));
      assertEquals("0003", rs.getString(2));
      document1 = (BsonDocument) rs.getObject(3);

      String updatedJson = getJsonString("json/sample_updated_01.json");
      assertEquals(RawBsonDocument.parse(updatedJson), document1);

      assertTrue(rs.next());
      assertEquals("pk1010", rs.getString(1));
      assertEquals("1010", rs.getString(2));
      BsonDocument document2 = (BsonDocument) rs.getObject(3);

      updatedJson = getJsonString("json/sample_updated_02.json");
      assertEquals(RawBsonDocument.parse(updatedJson), document2);

      assertTrue(rs.next());
      assertEquals("pk1011", rs.getString(1));
      assertEquals("1011", rs.getString(2));
      BsonDocument document3 = (BsonDocument) rs.getObject(3);

      updatedJson = getJsonString("json/sample_updated_03.json");
      assertEquals(RawBsonDocument.parse(updatedJson), document3);

      assertFalse(rs.next());
    }
  }

  @Test
  public void testBsonOpsWithSqlConditionsUpdateFailure() throws Exception {
    Properties props = PropertiesUtil.deepCopy(TEST_PROPERTIES);
    String tableName = generateUniqueName();
    String cdcName = generateUniqueName();
    String indexName1 = "IDX1_" + tableName;
    String indexName2 = "IDX2_" + tableName;
    try (Connection conn = DriverManager.getConnection(getUrl(), props)) {
      String ddl = "CREATE TABLE " + tableName + " (PK1 VARCHAR NOT NULL, C1 VARCHAR, COL BSON"
        + " CONSTRAINT pk PRIMARY KEY(PK1))";
      String cdcDdl = "CREATE CDC " + cdcName + " ON " + tableName;
      String indexDdl1 = "CREATE INDEX " + indexName1 + " ON " + tableName
        + "(BSON_VALUE(COL, 'rather[3].outline.clock', 'VARCHAR')) INCLUDE(COL) "
        + "WHERE BSON_VALUE(COL, 'rather[3].outline.clock', 'VARCHAR') IS NOT NULL "
        + "CONSISTENCY = EVENTUAL";
      String indexDdl2 = "CREATE UNCOVERED INDEX " + indexName2 + " ON " + tableName
        + "(BSON_VALUE(COL, 'result[1].location.coordinates.longitude', 'DOUBLE')) "
        + "WHERE BSON_VALUE(COL, 'result[1].location.coordinates.longitude', 'DOUBLE') IS NOT NULL "
        + "CONSISTENCY = EVENTUAL";
      conn.createStatement().execute(ddl);
      conn.createStatement().execute(cdcDdl);
      conn.createStatement().execute(indexDdl1);
      conn.createStatement().execute(indexDdl2);
      Timestamp ts1 = new Timestamp(System.currentTimeMillis());
      Thread.sleep(100);

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

      Thread.sleep(100);
      Timestamp ts2 = new Timestamp(System.currentTimeMillis());

      Thread.sleep(11000);
      PreparedStatement indexPs = conn.prepareStatement("SELECT PK1, COL FROM " + tableName
        + " WHERE BSON_VALUE(COL, 'rather[3].outline.clock', 'VARCHAR') = ?");
      indexPs.setString(1, "personal");
      ResultSet indexRs = indexPs.executeQuery();
      assertTrue(indexRs.next());
      assertEquals("pk1010", indexRs.getString(1));
      BsonDocument actualDoc = (BsonDocument) indexRs.getObject(2);
      assertEquals(bsonDocument2, actualDoc);
      assertFalse(indexRs.next());
      validateExplainPlan(indexPs, indexName1, "RANGE SCAN ");

      indexPs = conn.prepareStatement("SELECT PK1 FROM " + tableName
        + " WHERE BSON_VALUE(COL, 'result[1].location.coordinates.longitude', 'DOUBLE') = ?");
      indexPs.setDouble(1, 52.3736);
      indexRs = indexPs.executeQuery();
      assertTrue(indexRs.next());
      assertEquals("pk1011", indexRs.getString(1));
      assertFalse(indexRs.next());
      validateExplainPlan(indexPs, indexName2, "RANGE SCAN ");

      testCDCAfterFirstUpsert(conn, cdcName, ts1, ts2, bsonDocument1, bsonDocument2, bsonDocument3);

      Thread.sleep(100);
      ts1 = new Timestamp(System.currentTimeMillis());
      Thread.sleep(100);

      String conditionExpression =
        "#press = :press AND #track[0].#shot[2][0].#city.#standard[50] = :softly";

      BsonDocument conditionDoc = new BsonDocument();
      conditionDoc.put("$EXPR", new BsonString(conditionExpression));
      conditionDoc.put("$VAL", new BsonDocument().append(":press", new BsonString("beat"))
        .append(":softly", new BsonString("softly")));
      conditionDoc.put("$KEYS",
        new BsonDocument().append("#press", new BsonString("press"))
          .append("#track", new BsonString("track")).append("#shot", new BsonString("shot"))
          .append("#city", new BsonString("city")).append("#standard", new BsonString("standard")));

      String query = "SELECT * FROM " + tableName
        + " WHERE PK1 = 'pk0001' AND C1 = '0002' AND NOT BSON_CONDITION_EXPRESSION(COL, '"
        + conditionDoc.toJson() + "')";
      ResultSet rs = conn.createStatement().executeQuery(query);

      assertTrue(rs.next());
      assertEquals("pk0001", rs.getString(1));
      assertEquals("0002", rs.getString(2));
      BsonDocument document1 = (BsonDocument) rs.getObject(3);
      assertEquals(bsonDocument1, document1);

      assertFalse(rs.next());

      conditionExpression =
        "#press = :press AND #track[0].#shot[2][0].#city.#standard[5] <> :softly";

      conditionDoc = new BsonDocument();
      conditionDoc.put("$EXPR", new BsonString(conditionExpression));
      conditionDoc.put("$VAL", new BsonDocument().append(":press", new BsonString("beat"))
        .append(":softly", new BsonString("softly")));
      conditionDoc.put("$KEYS",
        new BsonDocument().append("#press", new BsonString("press"))
          .append("#track", new BsonString("track")).append("#shot", new BsonString("shot"))
          .append("#city", new BsonString("city")).append("#standard", new BsonString("standard")));

      BsonDocument updateExp = new BsonDocument().append("$SET",
        new BsonDocument()
          .append("browserling", new BsonBinary(PDouble.INSTANCE.toBytes(-505169340.54880095)))
          .append("track[0].shot[2][0].city.standard[5]", new BsonString("soft"))
          .append("track[0].shot[2][0].city.problem[2]",
            new BsonString("track[0].shot[2][0].city.problem[2] + 529.435")))
        .append("$UNSET",
          new BsonDocument().append("track[0].shot[2][0].city.flame", new BsonNull()));

      stmt = conn.prepareStatement(
        "UPSERT INTO " + tableName + " VALUES (?) ON DUPLICATE KEY UPDATE COL = CASE WHEN"
          + " BSON_CONDITION_EXPRESSION(COL, '" + conditionDoc.toJson() + "')"
          + " THEN BSON_UPDATE_EXPRESSION(COL, '" + updateExp + "') ELSE COL END," + " C1 = ?");
      stmt.setString(1, "pk0001");
      stmt.setString(2, "0003");
      stmt.executeUpdate();

      updateExp = new BsonDocument()
        .append("$ADD",
          new BsonDocument().append("new_samples", new BsonDocument().append("$set",
            new BsonArray(Arrays.asList(new BsonBinary(Bytes.toBytes("Sample10")),
              new BsonBinary(Bytes.toBytes("Sample12")), new BsonBinary(Bytes.toBytes("Sample13")),
              new BsonBinary(Bytes.toBytes("Sample14")))))))
        .append("$DELETE_FROM_SET",
          new BsonDocument().append("new_samples",
            new BsonDocument().append("$set",
              new BsonArray(Arrays.asList(new BsonBinary(Bytes.toBytes("Sample02")),
                new BsonBinary(Bytes.toBytes("Sample03")))))))
        .append("$SET",
          new BsonDocument().append("newrecord", ((BsonArray) (document1.get("track"))).get(0)))
        .append("$UNSET",
          new BsonDocument().append("rather[3].outline.halfway.so[2][2]", new BsonNull()));

      conditionExpression =
        "field_not_exists(newrecord) AND field_exists(#rather[3].#outline.#halfway.#so[2][20])";

      conditionDoc = new BsonDocument();
      conditionDoc.put("$EXPR", new BsonString(conditionExpression));
      conditionDoc.put("$VAL", new BsonDocument());
      conditionDoc.put("$KEYS",
        new BsonDocument().append("#rather", new BsonString("rather"))
          .append("#outline", new BsonString("outline"))
          .append("#halfway", new BsonString("halfway")).append("#so", new BsonString("so")));

      stmt = conn.prepareStatement(
        "UPSERT INTO " + tableName + " VALUES (?) ON DUPLICATE KEY UPDATE COL = CASE WHEN"
          + " BSON_CONDITION_EXPRESSION(COL, '" + conditionDoc.toJson() + "')"
          + " THEN BSON_UPDATE_EXPRESSION(COL, '" + updateExp + "') ELSE COL END");

      stmt.setString(1, "pk1010");
      stmt.executeUpdate();

      updateExp = new BsonDocument()
        .append("$SET", new BsonDocument().append("result[1].location.state", new BsonString("AK")))
        .append("$UNSET", new BsonDocument().append("result[4].emails[1]", new BsonNull()));

      conditionExpression = "#result[2].#location.#coordinates.#latitude > :latitude OR "
        + "(field_exists(#result[1].#location) AND #result[1].#location.#state != :state"
        + " AND field_not_exists(#result[4].#emails[1]))";

      conditionDoc = new BsonDocument();
      conditionDoc.put("$EXPR", new BsonString(conditionExpression));
      conditionDoc.put("$VAL", new BsonDocument().append(":latitude", new BsonDouble(0))
        .append(":state", new BsonString("AK")));
      conditionDoc.put("$KEYS",
        new BsonDocument().append("#result", new BsonString("result"))
          .append("#location", new BsonString("location"))
          .append("#coordinates", new BsonString("coordinates"))
          .append("#latitude", new BsonString("latitude")).append("#state", new BsonString("state"))
          .append("#emails", new BsonString("emails")));

      stmt = conn.prepareStatement(
        "UPSERT INTO " + tableName + " VALUES (?) ON DUPLICATE KEY UPDATE COL = CASE WHEN"
          + " BSON_CONDITION_EXPRESSION(COL, '" + conditionDoc.toJson() + "')"
          + " THEN BSON_UPDATE_EXPRESSION(COL, '" + updateExp + "') ELSE COL END");

      stmt.setString(1, "pk1011");
      stmt.executeUpdate();

      conn.commit();

      Thread.sleep(100);
      ts2 = new Timestamp(System.currentTimeMillis());

      testCDCUpdateOneRowChange(conn, cdcName, ts1, ts2, bsonDocument1);

      query = "SELECT * FROM " + tableName;
      rs = conn.createStatement().executeQuery(query);

      assertTrue(rs.next());
      assertEquals("pk0001", rs.getString(1));
      assertEquals("0003", rs.getString(2));
      document1 = (BsonDocument) rs.getObject(3);

      assertEquals(bsonDocument1, document1);

      assertTrue(rs.next());
      assertEquals("pk1010", rs.getString(1));
      assertEquals("1010", rs.getString(2));
      BsonDocument document2 = (BsonDocument) rs.getObject(3);

      assertEquals(bsonDocument2, document2);

      assertTrue(rs.next());
      assertEquals("pk1011", rs.getString(1));
      assertEquals("1011", rs.getString(2));
      BsonDocument document3 = (BsonDocument) rs.getObject(3);

      assertEquals(bsonDocument3, document3);

      assertFalse(rs.next());
    }
  }

  private static void testCDCAfterFirstUpsert(Connection conn, String cdcName, Timestamp ts1,
    Timestamp ts2, BsonDocument bsonDocument1, BsonDocument bsonDocument2,
    BsonDocument bsonDocument3) throws SQLException, JsonProcessingException {
    try (
      PreparedStatement pst = conn.prepareStatement("SELECT /*+ CDC_INCLUDE(PRE, POST) */ * FROM "
        + cdcName + " WHERE PHOENIX_ROW_TIMESTAMP() >= ? AND PHOENIX_ROW_TIMESTAMP() <= ?")) {
      pst.setTimestamp(1, ts1);
      pst.setTimestamp(2, ts2);

      ResultSet rs = pst.executeQuery();
      Assert.assertTrue(rs.next());

      String cdcVal = rs.getString(3);
      Map<String, Object> map = OBJECT_MAPPER.readValue(cdcVal, Map.class);
      Map<String, Object> preImage = (Map<String, Object>) map.get(QueryConstants.CDC_PRE_IMAGE);
      Assert.assertNull(preImage.get("COL"));
      Map<String, Object> postImage = (Map<String, Object>) map.get(QueryConstants.CDC_POST_IMAGE);
      String encodedBytes = (String) postImage.get("COL");
      byte[] bytes = Base64.getDecoder().decode(encodedBytes);
      RawBsonDocument r1 = new RawBsonDocument(bytes, 0, bytes.length);
      Assert.assertEquals(bsonDocument1, r1);

      Assert.assertTrue(rs.next());

      cdcVal = rs.getString(3);
      map = OBJECT_MAPPER.readValue(cdcVal, Map.class);
      preImage = (Map<String, Object>) map.get(QueryConstants.CDC_PRE_IMAGE);
      Assert.assertNull(preImage.get("COL"));
      postImage = (Map<String, Object>) map.get(QueryConstants.CDC_POST_IMAGE);
      encodedBytes = (String) postImage.get("COL");
      bytes = Base64.getDecoder().decode(encodedBytes);
      RawBsonDocument r2 = new RawBsonDocument(bytes, 0, bytes.length);
      Assert.assertEquals(bsonDocument2, r2);

      Assert.assertTrue(rs.next());

      cdcVal = rs.getString(3);
      map = OBJECT_MAPPER.readValue(cdcVal, Map.class);
      preImage = (Map<String, Object>) map.get(QueryConstants.CDC_PRE_IMAGE);
      Assert.assertNull(preImage.get("COL"));
      postImage = (Map<String, Object>) map.get(QueryConstants.CDC_POST_IMAGE);
      encodedBytes = (String) postImage.get("COL");
      bytes = Base64.getDecoder().decode(encodedBytes);
      RawBsonDocument r3 = new RawBsonDocument(bytes, 0, bytes.length);
      Assert.assertEquals(bsonDocument3, r3);

      Assert.assertFalse(rs.next());
    }
  }

  private static void testCDCPostUpdate(Connection conn, String cdcName, Timestamp ts1,
    Timestamp ts2, BsonDocument bsonDocument1, BsonDocument bsonDocument2,
    BsonDocument bsonDocument3) throws SQLException, IOException {
    ResultSet rs;
    try (
      PreparedStatement pst = conn.prepareStatement("SELECT /*+ CDC_INCLUDE(PRE, POST) */ * FROM "
        + cdcName + " WHERE PHOENIX_ROW_TIMESTAMP() >= ? AND PHOENIX_ROW_TIMESTAMP() <= ?")) {
      pst.setTimestamp(1, ts1);
      pst.setTimestamp(2, ts2);

      rs = pst.executeQuery();
      Assert.assertTrue(rs.next());

      String cdcVal = rs.getString(3);
      Map<String, Object> map = OBJECT_MAPPER.readValue(cdcVal, Map.class);
      Map<String, Object> preImage = (Map<String, Object>) map.get(QueryConstants.CDC_PRE_IMAGE);
      String encodedBytes = (String) preImage.get("COL");
      byte[] bytes = Base64.getDecoder().decode(encodedBytes);
      RawBsonDocument preDoc = new RawBsonDocument(bytes, 0, bytes.length);
      Assert.assertEquals(bsonDocument1, preDoc);

      Map<String, Object> postImage = (Map<String, Object>) map.get(QueryConstants.CDC_POST_IMAGE);
      encodedBytes = (String) postImage.get("COL");
      bytes = Base64.getDecoder().decode(encodedBytes);
      RawBsonDocument postDoc = new RawBsonDocument(bytes, 0, bytes.length);
      Assert.assertEquals(RawBsonDocument.parse(getJsonString("json/sample_updated_01.json")),
        postDoc);

      Assert.assertTrue(rs.next());

      cdcVal = rs.getString(3);
      map = OBJECT_MAPPER.readValue(cdcVal, Map.class);
      preImage = (Map<String, Object>) map.get(QueryConstants.CDC_PRE_IMAGE);
      encodedBytes = (String) preImage.get("COL");
      bytes = Base64.getDecoder().decode(encodedBytes);
      preDoc = new RawBsonDocument(bytes, 0, bytes.length);
      Assert.assertEquals(bsonDocument2, preDoc);

      postImage = (Map<String, Object>) map.get(QueryConstants.CDC_POST_IMAGE);
      encodedBytes = (String) postImage.get("COL");
      bytes = Base64.getDecoder().decode(encodedBytes);
      postDoc = new RawBsonDocument(bytes, 0, bytes.length);
      Assert.assertEquals(RawBsonDocument.parse(getJsonString("json/sample_updated_02.json")),
        postDoc);

      Assert.assertTrue(rs.next());

      cdcVal = rs.getString(3);
      map = OBJECT_MAPPER.readValue(cdcVal, Map.class);
      preImage = (Map<String, Object>) map.get(QueryConstants.CDC_PRE_IMAGE);
      encodedBytes = (String) preImage.get("COL");
      bytes = Base64.getDecoder().decode(encodedBytes);
      preDoc = new RawBsonDocument(bytes, 0, bytes.length);
      Assert.assertEquals(bsonDocument3, preDoc);

      postImage = (Map<String, Object>) map.get(QueryConstants.CDC_POST_IMAGE);
      encodedBytes = (String) postImage.get("COL");
      bytes = Base64.getDecoder().decode(encodedBytes);
      postDoc = new RawBsonDocument(bytes, 0, bytes.length);
      Assert.assertEquals(RawBsonDocument.parse(getJsonString("json/sample_updated_03.json")),
        postDoc);

      Assert.assertFalse(rs.next());
    }
  }

  private static void testCDCUpdateOneRowChange(Connection conn, String cdcName, Timestamp ts1,
    Timestamp ts2, BsonDocument bsonDocument1) throws SQLException, IOException {
    try (
      PreparedStatement pst = conn.prepareStatement("SELECT /*+ CDC_INCLUDE(PRE, POST) */ * FROM "
        + cdcName + " WHERE PHOENIX_ROW_TIMESTAMP() >= ? AND PHOENIX_ROW_TIMESTAMP() <= ?")) {
      pst.setTimestamp(1, ts1);
      pst.setTimestamp(2, ts2);

      ResultSet rs = pst.executeQuery();
      Assert.assertTrue(rs.next());

      String cdcVal = rs.getString(3);
      Map<String, Object> map = OBJECT_MAPPER.readValue(cdcVal, Map.class);
      Map<String, Object> preImage = (Map<String, Object>) map.get(QueryConstants.CDC_PRE_IMAGE);
      String encodedBytes = (String) preImage.get("COL");
      byte[] bytes = Base64.getDecoder().decode(encodedBytes);
      RawBsonDocument preDoc = new RawBsonDocument(bytes, 0, bytes.length);
      Assert.assertEquals(bsonDocument1, preDoc);

      Map<String, Object> postImage = (Map<String, Object>) map.get(QueryConstants.CDC_POST_IMAGE);
      encodedBytes = (String) postImage.get("COL");
      bytes = Base64.getDecoder().decode(encodedBytes);
      RawBsonDocument postDoc = new RawBsonDocument(bytes, 0, bytes.length);
      Assert.assertEquals(bsonDocument1, postDoc);

      Assert.assertFalse(rs.next());
    }
  }

  private static void validateExplainPlan(PreparedStatement ps, String tableName, String scanType)
    throws SQLException {
    ExplainPlan plan = ps.unwrap(PhoenixPreparedStatement.class).optimizeQuery().getExplainPlan();
    ExplainPlanAttributes explainPlanAttributes = plan.getPlanStepsAsAttributes();
    assertEquals(tableName, explainPlanAttributes.getTableName());
    assertEquals("PARALLEL 1-WAY", explainPlanAttributes.getIteratorTypeAndScanSize());
    assertEquals(scanType, explainPlanAttributes.getExplainScanType());
  }

}
