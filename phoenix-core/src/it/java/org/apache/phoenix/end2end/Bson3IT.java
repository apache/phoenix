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

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.commons.io.FileUtils;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.phoenix.query.QueryConstants;
import org.apache.phoenix.schema.types.PDouble;
import org.apache.phoenix.thirdparty.com.google.common.base.Preconditions;
import org.apache.phoenix.util.CDCUtil;
import org.apache.phoenix.util.PropertiesUtil;
import org.bson.BsonArray;
import org.bson.BsonBinary;
import org.bson.BsonBoolean;
import org.bson.BsonDocument;
import org.bson.BsonDouble;
import org.bson.BsonNull;
import org.bson.BsonString;
import org.bson.RawBsonDocument;
import org.junit.Assert;
import org.junit.Test;
import org.junit.experimental.categories.Category;

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

import static org.apache.phoenix.util.TestUtil.TEST_PROPERTIES;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

/**
 * Tests for BSON.
 */
@Category(ParallelStatsDisabledTest.class)
public class Bson3IT extends ParallelStatsDisabledIT {

  private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

  private static String getJsonString(String jsonFilePath) throws IOException {
    URL fileUrl = Bson3IT.class.getClassLoader().getResource(jsonFilePath);
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
    try (Connection conn = DriverManager.getConnection(getUrl(), props)) {
      String ddl = "CREATE TABLE " + tableName
          + " (PK1 VARCHAR NOT NULL, C1 VARCHAR, COL BSON"
          + " CONSTRAINT pk PRIMARY KEY(PK1))";
      String cdcDdl = "CREATE CDC " + cdcName + " ON " + tableName;
      conn.createStatement().execute(ddl);
      conn.createStatement().execute(cdcDdl);
      IndexToolIT.runIndexTool(false, "", tableName,
              "\"" + CDCUtil.getCDCIndexName(cdcName) + "\"");
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

      testCDCAfterFirstUpsert(conn, cdcName, ts1, ts2, bsonDocument1, bsonDocument2, bsonDocument3);

      ts1 = new Timestamp(System.currentTimeMillis());
      Thread.sleep(100);

      String conditionExpression =
              "press = $press AND track[0].shot[2][0].city.standard[50] = $softly";

      //{
      //  "$EXPR": "press = $press AND track[0].shot[2][0].city.standard[50] = $softly",
      //  "$VAL": {
      //    "$press": "beat",
      //    "$softly": "softly"
      //  }
      //}
      BsonDocument conditionDoc = new BsonDocument();
      conditionDoc.put("$EXPR", new BsonString(conditionExpression));
      conditionDoc.put("$VAL", new BsonDocument()
              .append("$press", new BsonString("beat"))
              .append("$softly", new BsonString("softly")));

      String query = "SELECT * FROM " + tableName +
              " WHERE PK1 = 'pk0001' AND C1 = '0002' AND NOT BSON_CONDITION_EXPRESSION(COL, '"
              + conditionDoc.toJson() + "')";
      ResultSet rs = conn.createStatement().executeQuery(query);

      assertTrue(rs.next());
      assertEquals("pk0001", rs.getString(1));
      assertEquals("0002", rs.getString(2));
      BsonDocument document1 = (BsonDocument) rs.getObject(3);
      assertEquals(bsonDocument1, document1);

      assertFalse(rs.next());

      conditionExpression =
              "press = $press AND track[0].shot[2][0].city.standard[5] = $softly";

      //{
      //  "$EXPR": "press = $press AND track[0].shot[2][0].city.standard[5] = $softly",
      //  "$VAL": {
      //    "$press": "beat",
      //    "$softly": "softly"
      //  }
      //}
      conditionDoc = new BsonDocument();
      conditionDoc.put("$EXPR", new BsonString(conditionExpression));
      conditionDoc.put("$VAL", new BsonDocument()
              .append("$press", new BsonString("beat"))
              .append("$softly", new BsonString("softly")));

      query = "SELECT * FROM " + tableName +
              " WHERE PK1 = 'pk0001' AND C1 = '0002' AND BSON_CONDITION_EXPRESSION(COL, '"
              + conditionDoc.toJson() + "')";
      rs = conn.createStatement().executeQuery(query);

      assertTrue(rs.next());
      assertEquals("pk0001", rs.getString(1));
      assertEquals("0002", rs.getString(2));
      document1 = (BsonDocument) rs.getObject(3);
      assertEquals(bsonDocument1, document1);

      assertFalse(rs.next());

      //{
      //  "$SET": {
      //    "browserling": {
      //      "$binary": {
      //        "base64": "PkHjukNzgcg=",
      //        "subType": "00"
      //      }
      //    },
      //    "track[0].shot[2][0].city.standard[5]": "soft",
      //    "track[0].shot[2][0].city.problem[2]": "track[0].shot[2][0].city.problem[2] + 529.435"
      //  },
      //  "$UNSET": {
      //    "track[0].shot[2][0].city.flame": null
      //  }
      //}
      BsonDocument updateExp = new BsonDocument()
              .append("$SET", new BsonDocument()
                      .append("browserling",
                              new BsonBinary(PDouble.INSTANCE.toBytes(-505169340.54880095)))
                      .append("track[0].shot[2][0].city.standard[5]", new BsonString("soft"))
                      .append("track[0].shot[2][0].city.problem[2]",
                              new BsonString("track[0].shot[2][0].city.problem[2] + 529.435")))
              .append("$UNSET", new BsonDocument()
                      .append("track[0].shot[2][0].city.flame", new BsonNull()));

      stmt = conn.prepareStatement("UPSERT INTO " + tableName
              + " VALUES (?) ON DUPLICATE KEY UPDATE COL = CASE WHEN"
              + " BSON_CONDITION_EXPRESSION(COL, '" + conditionDoc.toJson() + "')"
              + " THEN BSON_UPDATE_EXPRESSION(COL, '" + updateExp + "') ELSE COL END,"
              + " C1 = ?");
      stmt.setString(1, "pk0001");
      stmt.setString(2, "0003");
      stmt.executeUpdate();

      //{
      //  "$ADD": {
      //    "new_samples": {
      //      "$set": [
      //        {
      //          "$binary": {
      //            "base64": "U2FtcGxlMTA=",
      //            "subType": "00"
      //          }
      //        },
      //        {
      //          "$binary": {
      //            "base64": "U2FtcGxlMTI=",
      //            "subType": "00"
      //          }
      //        },
      //        {
      //          "$binary": {
      //            "base64": "U2FtcGxlMTM=",
      //            "subType": "00"
      //          }
      //        },
      //        {
      //          "$binary": {
      //            "base64": "U2FtcGxlMTQ=",
      //            "subType": "00"
      //          }
      //        }
      //      ]
      //    }
      //  },
      //  "$DELETE_FROM_SET": {
      //    "new_samples": {
      //      "$set": [
      //        {
      //          "$binary": {
      //            "base64": "U2FtcGxlMDI=",
      //            "subType": "00"
      //          }
      //        },
      //        {
      //          "$binary": {
      //            "base64": "U2FtcGxlMDM=",
      //            "subType": "00"
      //          }
      //        }
      //      ]
      //    }
      //  },
      //  "$SET": {
      //    "newrecord": {
      //      "speed": "sun",
      //      "shot": [
      //        "fun",
      //        true,
      //        [
      //          {
      //            "character": 413968576,
      //            "earth": "helpful",
      //            "money": false,
      //            "city": {
      //              "softly": "service",
      //              "standard": [
      //                "pour",
      //                false,
      //                true,
      //                true,
      //                true,
      //                "softly",
      //                "happened"
      //              ],
      //              "problem": [
      //                -687102682.7731872,
      //                "tightly",
      //                1527061470.2690287,
      //                {
      //                  "condition": "else",
      //                  "higher": 1462910924.088698,
      //                  "scene": false,
      //                  "safety": 240784722.66658115,
      //                  "catch": false,
      //                  "behavior": true,
      //                  "protection": true
      //                },
      //                "torn",
      //                false,
      //                "eat"
      //              ],
      //              "flame": 1066643931,
      //              "rest": -1053428849,
      //              "additional": -442539394.7937908,
      //              "brought": "rock"
      //            },
      //            "upward": -1306729583.8727202,
      //            "sky": "act",
      //            "height": true
      //          },
      //          -2042805074.4290242,
      //          "settlers",
      //          1455555511.4875226,
      //          -1448763321,
      //          false,
      //          379701747
      //        ],
      //        false,
      //        1241794365,
      //        "capital",
      //        false
      //      ],
      //      "hidden": false,
      //      "truth": "south",
      //      "other": true,
      //      "disease": "disease"
      //    }
      //  },
      //  "$UNSET": {
      //    "rather[3].outline.halfway.so[2][2]": null
      //  }
      //}
      updateExp = new BsonDocument()
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
                      .append("newrecord", ((BsonArray) (document1.get("track"))).get(0)))
              .append("$UNSET", new BsonDocument()
                      .append("rather[3].outline.halfway.so[2][2]", new BsonNull()));

      conditionExpression =
              "field_not_exists(newrecord) AND field_exists(rather[3].outline.halfway.so[2][2])";

      //{
      //  "$EXPR": "field_not_exists(newrecord) AND field_exists(rather[3].outline.halfway.so[2][2])",
      //  "$VAL": {}
      //}
      conditionDoc = new BsonDocument();
      conditionDoc.put("$EXPR", new BsonString(conditionExpression));
      conditionDoc.put("$VAL", new BsonDocument());

      stmt = conn.prepareStatement("UPSERT INTO " + tableName
              + " VALUES (?) ON DUPLICATE KEY UPDATE COL = CASE WHEN"
              + " BSON_CONDITION_EXPRESSION(COL, '" + conditionDoc.toJson() + "')"
              + " THEN BSON_UPDATE_EXPRESSION(COL, '" + updateExp + "') ELSE COL END");

      stmt.setString(1, "pk1010");
      stmt.executeUpdate();

      //{
      //  "$SET": {
      //    "result[1].location.state": "AK"
      //  },
      //  "$UNSET": {
      //    "result[4].emails[1]": null
      //  }
      //}
      updateExp = new BsonDocument()
              .append("$SET", new BsonDocument()
                      .append("result[1].location.state", new BsonString("AK")))
              .append("$UNSET", new BsonDocument()
                      .append("result[4].emails[1]", new BsonNull()));

      conditionExpression =
              "result[2].location.coordinates.latitude > $latitude OR "
                      + "(field_exists(result[1].location) AND result[1].location.state != $state" +
                      " AND field_exists(result[4].emails[1]))";

      //{
      //  "$EXPR": "result[2].location.coordinates.latitude > $latitude OR (field_exists(result[1].location) AND result[1].location.state != $state AND field_exists(result[4].emails[1]))",
      //  "$VAL": {
      //    "$latitude": 0,
      //    "$state": "AK"
      //  }
      //}
      conditionDoc = new BsonDocument();
      conditionDoc.put("$EXPR", new BsonString(conditionExpression));
      conditionDoc.put("$VAL", new BsonDocument()
              .append("$latitude", new BsonDouble(0))
              .append("$state", new BsonString("AK")));

      stmt = conn.prepareStatement("UPSERT INTO " + tableName
              + " VALUES (?) ON DUPLICATE KEY UPDATE COL = CASE WHEN"
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

  private static void testCDCAfterFirstUpsert(Connection conn, String cdcName, Timestamp ts1,
                                              Timestamp ts2,
                                              BsonDocument bsonDocument1,
                                              BsonDocument bsonDocument2,
                                              BsonDocument bsonDocument3)
          throws SQLException, JsonProcessingException {
    try (PreparedStatement pst = conn.prepareStatement(
            "SELECT /*+ CDC_INCLUDE(PRE, POST) */ * FROM " + cdcName +
                    " WHERE PHOENIX_ROW_TIMESTAMP() >= ? AND PHOENIX_ROW_TIMESTAMP() <= ?")) {
      pst.setTimestamp(1, ts1);
      pst.setTimestamp(2, ts2);

      ResultSet rs = pst.executeQuery();
      Assert.assertTrue(rs.next());

      String cdcVal = rs.getString(3);
      Map<String, Object> map = OBJECT_MAPPER.readValue(cdcVal, Map.class);
      Map<String, Object> preImage = (Map<String, Object>) map.get(QueryConstants.CDC_PRE_IMAGE);
      Assert.assertNull(preImage.get("COL"));
      Map<String, Object> postImage =
              (Map<String, Object>) map.get(QueryConstants.CDC_POST_IMAGE);
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
                                        Timestamp ts2, BsonDocument bsonDocument1,
                                        BsonDocument bsonDocument2,
                                        BsonDocument bsonDocument3)
          throws SQLException, IOException {
    ResultSet rs;
    try (PreparedStatement pst = conn.prepareStatement(
            "SELECT /*+ CDC_INCLUDE(PRE, POST) */ * FROM " + cdcName +
                    " WHERE PHOENIX_ROW_TIMESTAMP() >= ? AND PHOENIX_ROW_TIMESTAMP() <= ?")) {
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

      Map<String, Object> postImage =
              (Map<String, Object>) map.get(QueryConstants.CDC_POST_IMAGE);
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
                                                Timestamp ts2, BsonDocument bsonDocument1)
          throws SQLException, IOException {
    ResultSet rs;
    try (PreparedStatement pst = conn.prepareStatement(
            "SELECT /*+ CDC_INCLUDE(PRE, POST) */ * FROM " + cdcName +
                    " WHERE PHOENIX_ROW_TIMESTAMP() >= ? AND PHOENIX_ROW_TIMESTAMP() <= ?")) {
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

      Map<String, Object> postImage =
              (Map<String, Object>) map.get(QueryConstants.CDC_POST_IMAGE);
      encodedBytes = (String) postImage.get("COL");
      bytes = Base64.getDecoder().decode(encodedBytes);
      RawBsonDocument postDoc = new RawBsonDocument(bytes, 0, bytes.length);
      Assert.assertEquals(bsonDocument1, postDoc);

      Assert.assertFalse(rs.next());
    }
  }

  /**
   * Conditional Upserts for BSON pass where the Condition Expression is of Document style.
   */
  @Test
  public void testBsonOpsWithDocumentConditionsUpdateSuccess() throws Exception {
    Properties props = PropertiesUtil.deepCopy(TEST_PROPERTIES);
    String tableName = generateUniqueName();
    String cdcName = generateUniqueName();
    try (Connection conn = DriverManager.getConnection(getUrl(), props)) {
      String ddl = "CREATE TABLE " + tableName
              + " (PK1 VARCHAR NOT NULL, C1 VARCHAR, COL BSON"
              + " CONSTRAINT pk PRIMARY KEY(PK1))";
      String cdcDdl = "CREATE CDC " + cdcName + " ON " + tableName;
      conn.createStatement().execute(ddl);
      conn.createStatement().execute(cdcDdl);
      IndexToolIT.runIndexTool(false, "", tableName,
              "\"" + CDCUtil.getCDCIndexName(cdcName) + "\"");
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

      testCDCAfterFirstUpsert(conn, cdcName, ts1, ts2, bsonDocument1, bsonDocument2, bsonDocument3);

      ts1 = new Timestamp(System.currentTimeMillis());
      Thread.sleep(100);

      //{
      //  "$and": [
      //    {
      //      "press": {
      //        "$eq": "beat"
      //      }
      //    },
      //    {
      //      "track[0].shot[2][0].city.standard[50]": {
      //        "$eq": "softly"
      //      }
      //    }
      //  ]
      //}
      BsonDocument conditionDoc = new BsonDocument();
      BsonArray andList1 = new BsonArray();
      andList1.add(new BsonDocument()
              .append("press", new BsonDocument()
                      .append("$eq", new BsonString("beat"))));
      andList1.add(new BsonDocument()
              .append("track[0].shot[2][0].city.standard[50]", new BsonDocument()
                      .append("$eq", new BsonString("softly"))));
      conditionDoc.put("$and", andList1);

      String query = "SELECT * FROM " + tableName +
              " WHERE PK1 = 'pk0001' AND C1 = '0002' AND NOT BSON_CONDITION_EXPRESSION(COL, '"
              + conditionDoc.toJson() + "')";
      ResultSet rs = conn.createStatement().executeQuery(query);

      assertTrue(rs.next());
      assertEquals("pk0001", rs.getString(1));
      assertEquals("0002", rs.getString(2));
      BsonDocument document1 = (BsonDocument) rs.getObject(3);
      assertEquals(bsonDocument1, document1);

      assertFalse(rs.next());

      //{
      //  "$and": [
      //    {
      //      "press": {
      //        "$eq": "beat"
      //      }
      //    },
      //    {
      //      "track[0].shot[2][0].city.standard[5]": {
      //        "$eq": "softly"
      //      }
      //    }
      //  ]
      //}
      conditionDoc = new BsonDocument();
      andList1 = new BsonArray();
      andList1.add(new BsonDocument()
              .append("press", new BsonDocument()
                      .append("$eq", new BsonString("beat"))));
      andList1.add(new BsonDocument()
              .append("track[0].shot[2][0].city.standard[5]", new BsonDocument()
                      .append("$eq", new BsonString("softly"))));
      conditionDoc.put("$and", andList1);

      query = "SELECT * FROM " + tableName +
              " WHERE PK1 = 'pk0001' AND C1 = '0002' AND BSON_CONDITION_EXPRESSION(COL, '"
              + conditionDoc.toJson() + "')";
      rs = conn.createStatement().executeQuery(query);

      assertTrue(rs.next());
      assertEquals("pk0001", rs.getString(1));
      assertEquals("0002", rs.getString(2));
      document1 = (BsonDocument) rs.getObject(3);
      assertEquals(bsonDocument1, document1);

      assertFalse(rs.next());

      //{
      //  "$SET": {
      //    "browserling": {
      //      "$binary": {
      //        "base64": "PkHjukNzgcg=",
      //        "subType": "00"
      //      }
      //    },
      //    "track[0].shot[2][0].city.standard[5]": "soft",
      //    "track[0].shot[2][0].city.problem[2]": "track[0].shot[2][0].city.problem[2] + 529.435"
      //  },
      //  "$UNSET": {
      //    "track[0].shot[2][0].city.flame": null
      //  }
      //}
      BsonDocument updateExp = new BsonDocument()
              .append("$SET", new BsonDocument()
                      .append("browserling",
                              new BsonBinary(PDouble.INSTANCE.toBytes(-505169340.54880095)))
                      .append("track[0].shot[2][0].city.standard[5]", new BsonString("soft"))
                      .append("track[0].shot[2][0].city.problem[2]",
                              new BsonString("track[0].shot[2][0].city.problem[2] + 529.435")))
              .append("$UNSET", new BsonDocument()
                      .append("track[0].shot[2][0].city.flame", new BsonNull()));

      stmt = conn.prepareStatement("UPSERT INTO " + tableName
              + " VALUES (?) ON DUPLICATE KEY UPDATE COL = CASE WHEN"
              + " BSON_CONDITION_EXPRESSION(COL, '" + conditionDoc.toJson() + "')"
              + " THEN BSON_UPDATE_EXPRESSION(COL, '" + updateExp + "') ELSE COL END,"
              + " C1 = ?");
      stmt.setString(1, "pk0001");
      stmt.setString(2, "0003");
      stmt.executeUpdate();

      //{
      //  "$ADD": {
      //    "new_samples": {
      //      "$set": [
      //        {
      //          "$binary": {
      //            "base64": "U2FtcGxlMTA=",
      //            "subType": "00"
      //          }
      //        },
      //        {
      //          "$binary": {
      //            "base64": "U2FtcGxlMTI=",
      //            "subType": "00"
      //          }
      //        },
      //        {
      //          "$binary": {
      //            "base64": "U2FtcGxlMTM=",
      //            "subType": "00"
      //          }
      //        },
      //        {
      //          "$binary": {
      //            "base64": "U2FtcGxlMTQ=",
      //            "subType": "00"
      //          }
      //        }
      //      ]
      //    }
      //  },
      //  "$DELETE_FROM_SET": {
      //    "new_samples": {
      //      "$set": [
      //        {
      //          "$binary": {
      //            "base64": "U2FtcGxlMDI=",
      //            "subType": "00"
      //          }
      //        },
      //        {
      //          "$binary": {
      //            "base64": "U2FtcGxlMDM=",
      //            "subType": "00"
      //          }
      //        }
      //      ]
      //    }
      //  },
      //  "$SET": {
      //    "newrecord": {
      //      "speed": "sun",
      //      "shot": [
      //        "fun",
      //        true,
      //        [
      //          {
      //            "character": 413968576,
      //            "earth": "helpful",
      //            "money": false,
      //            "city": {
      //              "softly": "service",
      //              "standard": [
      //                "pour",
      //                false,
      //                true,
      //                true,
      //                true,
      //                "softly",
      //                "happened"
      //              ],
      //              "problem": [
      //                -687102682.7731872,
      //                "tightly",
      //                1527061470.2690287,
      //                {
      //                  "condition": "else",
      //                  "higher": 1462910924.088698,
      //                  "scene": false,
      //                  "safety": 240784722.66658115,
      //                  "catch": false,
      //                  "behavior": true,
      //                  "protection": true
      //                },
      //                "torn",
      //                false,
      //                "eat"
      //              ],
      //              "flame": 1066643931,
      //              "rest": -1053428849,
      //              "additional": -442539394.7937908,
      //              "brought": "rock"
      //            },
      //            "upward": -1306729583.8727202,
      //            "sky": "act",
      //            "height": true
      //          },
      //          -2042805074.4290242,
      //          "settlers",
      //          1455555511.4875226,
      //          -1448763321,
      //          false,
      //          379701747
      //        ],
      //        false,
      //        1241794365,
      //        "capital",
      //        false
      //      ],
      //      "hidden": false,
      //      "truth": "south",
      //      "other": true,
      //      "disease": "disease"
      //    }
      //  },
      //  "$UNSET": {
      //    "rather[3].outline.halfway.so[2][2]": null
      //  }
      //}
      updateExp = new BsonDocument()
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
                      .append("newrecord", ((BsonArray) (document1.get("track"))).get(0)))
              .append("$UNSET", new BsonDocument()
                      .append("rather[3].outline.halfway.so[2][2]", new BsonNull()));

      //{
      //  "$and": [
      //    {
      //      "press": {
      //        "$exists": false
      //      }
      //    },
      //    {
      //      "rather[3].outline.halfway.so[2][2]": {
      //        "$exists": true
      //      }
      //    }
      //  ]
      //}
      conditionDoc = new BsonDocument();
      andList1 = new BsonArray();
      andList1.add(new BsonDocument()
              .append("press", new BsonDocument()
                      .append("$exists", new BsonBoolean(false))));
      andList1.add(new BsonDocument()
              .append("rather[3].outline.halfway.so[2][2]", new BsonDocument()
                      .append("$exists", new BsonBoolean(true))));
      conditionDoc.put("$and", andList1);

      stmt = conn.prepareStatement("UPSERT INTO " + tableName
              + " VALUES (?) ON DUPLICATE KEY UPDATE COL = CASE WHEN"
              + " BSON_CONDITION_EXPRESSION(COL, '" + conditionDoc.toJson() + "')"
              + " THEN BSON_UPDATE_EXPRESSION(COL, '" + updateExp + "') ELSE COL END");

      stmt.setString(1, "pk1010");
      stmt.executeUpdate();

      //{
      //  "$SET": {
      //    "result[1].location.state": "AK"
      //  },
      //  "$UNSET": {
      //    "result[4].emails[1]": null
      //  }
      //}
      updateExp = new BsonDocument()
              .append("$SET", new BsonDocument()
                      .append("result[1].location.state", new BsonString("AK")))
              .append("$UNSET", new BsonDocument()
                      .append("result[4].emails[1]", new BsonNull()));

      //{
      //  "$or": [
      //    {
      //      "result[2].location.coordinates.latitude": {
      //        "$gt": 0
      //      }
      //    },
      //    {
      //      "$and": [
      //        {
      //          "result[1].location": {
      //            "$exists": true
      //          }
      //        },
      //        {
      //          "result[1].location.state": {
      //            "$ne": "AK"
      //          }
      //        },
      //        {
      //          "result[4].emails[1]": {
      //            "$exists": true
      //          }
      //        }
      //      ]
      //    }
      //  ]
      //}
      conditionDoc = new BsonDocument();
      BsonArray orList1 = new BsonArray();
      andList1 = new BsonArray();
      BsonDocument andDoc1 = new BsonDocument();
      andList1.add(new BsonDocument()
              .append("result[1].location", new BsonDocument()
                      .append("$exists", new BsonBoolean(true))));
      andList1.add(new BsonDocument()
              .append("result[1].location.state", new BsonDocument()
                      .append("$ne", new BsonString("AK"))));
      andList1.add(new BsonDocument()
              .append("result[4].emails[1]", new BsonDocument()
                      .append("$exists", new BsonBoolean(true))));
      andDoc1.put("$and", andList1);

      orList1.add(new BsonDocument()
              .append("result[2].location.coordinates.latitude",
                      new BsonDocument("$gt", new BsonDouble(0))));
      orList1.add(andDoc1);

      conditionDoc.put("$or", orList1);

      stmt = conn.prepareStatement("UPSERT INTO " + tableName
              + " VALUES (?) ON DUPLICATE KEY UPDATE COL = CASE WHEN"
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

  /**
   * Conditional Upserts for BSON fail i.e. BSON value is not updated, where the Condition
   * Expression is of SQL style.
   */
  @Test
  public void testBsonOpsWithSqlConditionsUpdateFailure() throws Exception {
    Properties props = PropertiesUtil.deepCopy(TEST_PROPERTIES);
    String tableName = generateUniqueName();
    String cdcName = generateUniqueName();
    try (Connection conn = DriverManager.getConnection(getUrl(), props)) {
      String ddl = "CREATE TABLE " + tableName
              + " (PK1 VARCHAR NOT NULL, C1 VARCHAR, COL BSON"
              + " CONSTRAINT pk PRIMARY KEY(PK1))";
      String cdcDdl = "CREATE CDC " + cdcName + " ON " + tableName;
      conn.createStatement().execute(ddl);
      conn.createStatement().execute(cdcDdl);
      IndexToolIT.runIndexTool(false, "", tableName,
              "\"" + CDCUtil.getCDCIndexName(cdcName) + "\"");
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

      testCDCAfterFirstUpsert(conn, cdcName, ts1, ts2, bsonDocument1, bsonDocument2, bsonDocument3);

      Thread.sleep(100);
      ts1 = new Timestamp(System.currentTimeMillis());
      Thread.sleep(100);

      String conditionExpression =
              "press = $press AND track[0].shot[2][0].city.standard[50] = $softly";

      //{
      //  "$EXPR": "press = $press AND track[0].shot[2][0].city.standard[50] = $softly",
      //  "$VAL": {
      //    "$press": "beat",
      //    "$softly": "softly"
      //  }
      //}
      BsonDocument conditionDoc = new BsonDocument();
      conditionDoc.put("$EXPR", new BsonString(conditionExpression));
      conditionDoc.put("$VAL", new BsonDocument()
              .append("$press", new BsonString("beat"))
              .append("$softly", new BsonString("softly")));

      String query = "SELECT * FROM " + tableName +
              " WHERE PK1 = 'pk0001' AND C1 = '0002' AND NOT BSON_CONDITION_EXPRESSION(COL, '"
              + conditionDoc.toJson() + "')";
      ResultSet rs = conn.createStatement().executeQuery(query);

      assertTrue(rs.next());
      assertEquals("pk0001", rs.getString(1));
      assertEquals("0002", rs.getString(2));
      BsonDocument document1 = (BsonDocument) rs.getObject(3);
      assertEquals(bsonDocument1, document1);

      assertFalse(rs.next());

      conditionExpression =
              "press = $press AND track[0].shot[2][0].city.standard[5] <> $softly";

      //{
      //  "$EXPR": "press = $press AND track[0].shot[2][0].city.standard[5] <> $softly",
      //  "$VAL": {
      //    "$press": "beat",
      //    "$softly": "softly"
      //  }
      //}
      conditionDoc = new BsonDocument();
      conditionDoc.put("$EXPR", new BsonString(conditionExpression));
      conditionDoc.put("$VAL", new BsonDocument()
              .append("$press", new BsonString("beat"))
              .append("$softly", new BsonString("softly")));

      //{
      //  "$SET": {
      //    "browserling": {
      //      "$binary": {
      //        "base64": "PkHjukNzgcg=",
      //        "subType": "00"
      //      }
      //    },
      //    "track[0].shot[2][0].city.standard[5]": "soft",
      //    "track[0].shot[2][0].city.problem[2]": "track[0].shot[2][0].city.problem[2] + 529.435"
      //  },
      //  "$UNSET": {
      //    "track[0].shot[2][0].city.flame": null
      //  }
      //}
      BsonDocument updateExp = new BsonDocument()
              .append("$SET", new BsonDocument()
                      .append("browserling",
                              new BsonBinary(PDouble.INSTANCE.toBytes(-505169340.54880095)))
                      .append("track[0].shot[2][0].city.standard[5]", new BsonString("soft"))
                      .append("track[0].shot[2][0].city.problem[2]",
                              new BsonString("track[0].shot[2][0].city.problem[2] + 529.435")))
              .append("$UNSET", new BsonDocument()
                      .append("track[0].shot[2][0].city.flame", new BsonNull()));

      stmt = conn.prepareStatement("UPSERT INTO " + tableName
              + " VALUES (?) ON DUPLICATE KEY UPDATE COL = CASE WHEN"
              + " BSON_CONDITION_EXPRESSION(COL, '" + conditionDoc.toJson() + "')"
              + " THEN BSON_UPDATE_EXPRESSION(COL, '" + updateExp + "') ELSE COL END,"
              + " C1 = ?");
      stmt.setString(1, "pk0001");
      stmt.setString(2, "0003");
      stmt.executeUpdate();

      //{
      //  "$ADD": {
      //    "new_samples": {
      //      "$set": [
      //        {
      //          "$binary": {
      //            "base64": "U2FtcGxlMTA=",
      //            "subType": "00"
      //          }
      //        },
      //        {
      //          "$binary": {
      //            "base64": "U2FtcGxlMTI=",
      //            "subType": "00"
      //          }
      //        },
      //        {
      //          "$binary": {
      //            "base64": "U2FtcGxlMTM=",
      //            "subType": "00"
      //          }
      //        },
      //        {
      //          "$binary": {
      //            "base64": "U2FtcGxlMTQ=",
      //            "subType": "00"
      //          }
      //        }
      //      ]
      //    }
      //  },
      //  "$DELETE_FROM_SET": {
      //    "new_samples": {
      //      "$set": [
      //        {
      //          "$binary": {
      //            "base64": "U2FtcGxlMDI=",
      //            "subType": "00"
      //          }
      //        },
      //        {
      //          "$binary": {
      //            "base64": "U2FtcGxlMDM=",
      //            "subType": "00"
      //          }
      //        }
      //      ]
      //    }
      //  },
      //  "$SET": {
      //    "newrecord": {
      //      "speed": "sun",
      //      "shot": [
      //        "fun",
      //        true,
      //        [
      //          {
      //            "character": 413968576,
      //            "earth": "helpful",
      //            "money": false,
      //            "city": {
      //              "softly": "service",
      //              "standard": [
      //                "pour",
      //                false,
      //                true,
      //                true,
      //                true,
      //                "softly",
      //                "happened"
      //              ],
      //              "problem": [
      //                -687102682.7731872,
      //                "tightly",
      //                1527061470.2690287,
      //                {
      //                  "condition": "else",
      //                  "higher": 1462910924.088698,
      //                  "scene": false,
      //                  "safety": 240784722.66658115,
      //                  "catch": false,
      //                  "behavior": true,
      //                  "protection": true
      //                },
      //                "torn",
      //                false,
      //                "eat"
      //              ],
      //              "flame": 1066643931,
      //              "rest": -1053428849,
      //              "additional": -442539394.7937908,
      //              "brought": "rock"
      //            },
      //            "upward": -1306729583.8727202,
      //            "sky": "act",
      //            "height": true
      //          },
      //          -2042805074.4290242,
      //          "settlers",
      //          1455555511.4875226,
      //          -1448763321,
      //          false,
      //          379701747
      //        ],
      //        false,
      //        1241794365,
      //        "capital",
      //        false
      //      ],
      //      "hidden": false,
      //      "truth": "south",
      //      "other": true,
      //      "disease": "disease"
      //    }
      //  },
      //  "$UNSET": {
      //    "rather[3].outline.halfway.so[2][2]": null
      //  }
      //}
      updateExp = new BsonDocument()
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
                      .append("newrecord", ((BsonArray) (document1.get("track"))).get(0)))
              .append("$UNSET", new BsonDocument()
                      .append("rather[3].outline.halfway.so[2][2]", new BsonNull()));

      conditionExpression =
              "field_not_exists(newrecord) AND field_exists(rather[3].outline.halfway.so[2][20])";

      //{
      //  "$EXPR": "field_not_exists(newrecord) AND field_exists(rather[3].outline.halfway.so[2][20])",
      //  "$VAL": {}
      //}
      conditionDoc = new BsonDocument();
      conditionDoc.put("$EXPR", new BsonString(conditionExpression));
      conditionDoc.put("$VAL", new BsonDocument());

      stmt = conn.prepareStatement("UPSERT INTO " + tableName
              + " VALUES (?) ON DUPLICATE KEY UPDATE COL = CASE WHEN"
              + " BSON_CONDITION_EXPRESSION(COL, '" + conditionDoc.toJson() + "')"
              + " THEN BSON_UPDATE_EXPRESSION(COL, '" + updateExp + "') ELSE COL END");

      stmt.setString(1, "pk1010");
      stmt.executeUpdate();

      //{
      //  "$SET": {
      //    "result[1].location.state": "AK"
      //  },
      //  "$UNSET": {
      //    "result[4].emails[1]": null
      //  }
      //}
      updateExp = new BsonDocument()
              .append("$SET", new BsonDocument()
                      .append("result[1].location.state", new BsonString("AK")))
              .append("$UNSET", new BsonDocument()
                      .append("result[4].emails[1]", new BsonNull()));

      conditionExpression =
              "result[2].location.coordinates.latitude > $latitude OR "
                      + "(field_exists(result[1].location) AND result[1].location.state != $state" +
                      " AND field_not_exists(result[4].emails[1]))";

      //{
      //  "$EXPR": "result[2].location.coordinates.latitude > $latitude OR (field_exists(result[1].location) AND result[1].location.state != $state AND field_exists(result[4].emails[1]))",
      //  "$VAL": {
      //    "$latitude": 0,
      //    "$state": "AK"
      //  }
      //}
      conditionDoc = new BsonDocument();
      conditionDoc.put("$EXPR", new BsonString(conditionExpression));
      conditionDoc.put("$VAL", new BsonDocument()
              .append("$latitude", new BsonDouble(0))
              .append("$state", new BsonString("AK")));

      stmt = conn.prepareStatement("UPSERT INTO " + tableName
              + " VALUES (?) ON DUPLICATE KEY UPDATE COL = CASE WHEN"
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

  /**
   * Conditional Upserts for BSON fail i.e. BSON value is not updated, where the Condition
   * Expression is of Document style.
   */
  @Test
  public void testBsonOpsWithDocumentConditionsUpdateFailure() throws Exception {
    Properties props = PropertiesUtil.deepCopy(TEST_PROPERTIES);
    String tableName = generateUniqueName();
    String cdcName = generateUniqueName();
    try (Connection conn = DriverManager.getConnection(getUrl(), props)) {
      String ddl = "CREATE TABLE " + tableName
              + " (PK1 VARCHAR NOT NULL, C1 VARCHAR, COL BSON"
              + " CONSTRAINT pk PRIMARY KEY(PK1))";
      String cdcDdl = "CREATE CDC " + cdcName + " ON " + tableName;
      conn.createStatement().execute(ddl);
      conn.createStatement().execute(cdcDdl);
      IndexToolIT.runIndexTool(false, "", tableName,
              "\"" + CDCUtil.getCDCIndexName(cdcName) + "\"");
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

      testCDCAfterFirstUpsert(conn, cdcName, ts1, ts2, bsonDocument1, bsonDocument2, bsonDocument3);

      Thread.sleep(100);
      ts1 = new Timestamp(System.currentTimeMillis());
      Thread.sleep(100);

      //{
      //  "$and": [
      //    {
      //      "press": {
      //        "$eq": "beat"
      //      }
      //    },
      //    {
      //      "track[0].shot[2][0].city.standard[50]": {
      //        "$eq": "softly"
      //      }
      //    }
      //  ]
      //}
      BsonDocument conditionDoc = new BsonDocument();
      BsonArray andList1 = new BsonArray();
      andList1.add(new BsonDocument()
              .append("press", new BsonDocument()
                      .append("$eq", new BsonString("beat"))));
      andList1.add(new BsonDocument()
              .append("track[0].shot[2][0].city.standard[50]", new BsonDocument()
                      .append("$eq", new BsonString("softly"))));
      conditionDoc.put("$and", andList1);

      String query = "SELECT * FROM " + tableName +
              " WHERE PK1 = 'pk0001' AND C1 = '0002' AND NOT BSON_CONDITION_EXPRESSION(COL, '"
              + conditionDoc.toJson() + "')";
      ResultSet rs = conn.createStatement().executeQuery(query);

      assertTrue(rs.next());
      assertEquals("pk0001", rs.getString(1));
      assertEquals("0002", rs.getString(2));
      BsonDocument document1 = (BsonDocument) rs.getObject(3);
      assertEquals(bsonDocument1, document1);

      assertFalse(rs.next());

      //{
      //  "$and": [
      //    {
      //      "press": {
      //        "$eq": "beat"
      //      }
      //    },
      //    {
      //      "track[0].shot[2][0].city.standard[5]": {
      //        "$ne": "softly"
      //      }
      //    }
      //  ]
      //}
      conditionDoc = new BsonDocument();
      andList1 = new BsonArray();
      andList1.add(new BsonDocument()
              .append("press", new BsonDocument()
                      .append("$eq", new BsonString("beat"))));
      andList1.add(new BsonDocument()
              .append("track[0].shot[2][0].city.standard[5]", new BsonDocument()
                      .append("$ne", new BsonString("softly"))));
      conditionDoc.put("$and", andList1);

      //{
      //  "$SET": {
      //    "browserling": {
      //      "$binary": {
      //        "base64": "PkHjukNzgcg=",
      //        "subType": "00"
      //      }
      //    },
      //    "track[0].shot[2][0].city.standard[5]": "soft",
      //    "track[0].shot[2][0].city.problem[2]": "track[0].shot[2][0].city.problem[2] + 529.435"
      //  },
      //  "$UNSET": {
      //    "track[0].shot[2][0].city.flame": null
      //  }
      //}
      BsonDocument updateExp = new BsonDocument()
              .append("$SET", new BsonDocument()
                      .append("browserling",
                              new BsonBinary(PDouble.INSTANCE.toBytes(-505169340.54880095)))
                      .append("track[0].shot[2][0].city.standard[5]", new BsonString("soft"))
                      .append("track[0].shot[2][0].city.problem[2]",
                              new BsonString("track[0].shot[2][0].city.problem[2] + 529.435")))
              .append("$UNSET", new BsonDocument()
                      .append("track[0].shot[2][0].city.flame", new BsonNull()));

      stmt = conn.prepareStatement("UPSERT INTO " + tableName
              + " VALUES (?) ON DUPLICATE KEY UPDATE COL = CASE WHEN"
              + " BSON_CONDITION_EXPRESSION(COL, '" + conditionDoc.toJson() + "')"
              + " THEN BSON_UPDATE_EXPRESSION(COL, '" + updateExp + "') ELSE COL END,"
              + " C1 = ?");
      stmt.setString(1, "pk0001");
      stmt.setString(2, "0003");
      stmt.executeUpdate();

      //{
      //  "$ADD": {
      //    "new_samples": {
      //      "$set": [
      //        {
      //          "$binary": {
      //            "base64": "U2FtcGxlMTA=",
      //            "subType": "00"
      //          }
      //        },
      //        {
      //          "$binary": {
      //            "base64": "U2FtcGxlMTI=",
      //            "subType": "00"
      //          }
      //        },
      //        {
      //          "$binary": {
      //            "base64": "U2FtcGxlMTM=",
      //            "subType": "00"
      //          }
      //        },
      //        {
      //          "$binary": {
      //            "base64": "U2FtcGxlMTQ=",
      //            "subType": "00"
      //          }
      //        }
      //      ]
      //    }
      //  },
      //  "$DELETE_FROM_SET": {
      //    "new_samples": {
      //      "$set": [
      //        {
      //          "$binary": {
      //            "base64": "U2FtcGxlMDI=",
      //            "subType": "00"
      //          }
      //        },
      //        {
      //          "$binary": {
      //            "base64": "U2FtcGxlMDM=",
      //            "subType": "00"
      //          }
      //        }
      //      ]
      //    }
      //  },
      //  "$SET": {
      //    "newrecord": {
      //      "speed": "sun",
      //      "shot": [
      //        "fun",
      //        true,
      //        [
      //          {
      //            "character": 413968576,
      //            "earth": "helpful",
      //            "money": false,
      //            "city": {
      //              "softly": "service",
      //              "standard": [
      //                "pour",
      //                false,
      //                true,
      //                true,
      //                true,
      //                "softly",
      //                "happened"
      //              ],
      //              "problem": [
      //                -687102682.7731872,
      //                "tightly",
      //                1527061470.2690287,
      //                {
      //                  "condition": "else",
      //                  "higher": 1462910924.088698,
      //                  "scene": false,
      //                  "safety": 240784722.66658115,
      //                  "catch": false,
      //                  "behavior": true,
      //                  "protection": true
      //                },
      //                "torn",
      //                false,
      //                "eat"
      //              ],
      //              "flame": 1066643931,
      //              "rest": -1053428849,
      //              "additional": -442539394.7937908,
      //              "brought": "rock"
      //            },
      //            "upward": -1306729583.8727202,
      //            "sky": "act",
      //            "height": true
      //          },
      //          -2042805074.4290242,
      //          "settlers",
      //          1455555511.4875226,
      //          -1448763321,
      //          false,
      //          379701747
      //        ],
      //        false,
      //        1241794365,
      //        "capital",
      //        false
      //      ],
      //      "hidden": false,
      //      "truth": "south",
      //      "other": true,
      //      "disease": "disease"
      //    }
      //  },
      //  "$UNSET": {
      //    "rather[3].outline.halfway.so[2][2]": null
      //  }
      //}
      updateExp = new BsonDocument()
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
                      .append("newrecord", ((BsonArray) (document1.get("track"))).get(0)))
              .append("$UNSET", new BsonDocument()
                      .append("rather[3].outline.halfway.so[2][2]", new BsonNull()));

      //{
      //  "$and": [
      //    {
      //      "press": {
      //        "$exists": false
      //      }
      //    },
      //    {
      //      "rather[3].outline.halfway.so[2][20]": {
      //        "$exists": true
      //      }
      //    }
      //  ]
      //}
      conditionDoc = new BsonDocument();
      andList1 = new BsonArray();
      andList1.add(new BsonDocument()
              .append("press", new BsonDocument()
                      .append("$exists", new BsonBoolean(false))));
      andList1.add(new BsonDocument()
              .append("rather[3].outline.halfway.so[2][20]", new BsonDocument()
                      .append("$exists", new BsonBoolean(true))));
      conditionDoc.put("$and", andList1);

      stmt = conn.prepareStatement("UPSERT INTO " + tableName
              + " VALUES (?) ON DUPLICATE KEY UPDATE COL = CASE WHEN"
              + " BSON_CONDITION_EXPRESSION(COL, '" + conditionDoc.toJson() + "')"
              + " THEN BSON_UPDATE_EXPRESSION(COL, '" + updateExp + "') ELSE COL END");

      stmt.setString(1, "pk1010");
      stmt.executeUpdate();

      //{
      //  "$SET": {
      //    "result[1].location.state": "AK"
      //  },
      //  "$UNSET": {
      //    "result[4].emails[1]": null
      //  }
      //}
      updateExp = new BsonDocument()
              .append("$SET", new BsonDocument()
                      .append("result[1].location.state", new BsonString("AK")))
              .append("$UNSET", new BsonDocument()
                      .append("result[4].emails[1]", new BsonNull()));

      //{
      //  "$or": [
      //    {
      //      "result[2].location.coordinates.latitude": {
      //        "$gt": 0
      //      }
      //    },
      //    {
      //      "$and": [
      //        {
      //          "result[1].location": {
      //            "$exists": true
      //          }
      //        },
      //        {
      //          "result[1].location.state": {
      //            "$ne": "AK"
      //          }
      //        },
      //        {
      //          "result[4].emails[1]": {
      //            "$exists": false
      //          }
      //        }
      //      ]
      //    }
      //  ]
      //}
      conditionDoc = new BsonDocument();
      BsonArray orList1 = new BsonArray();
      andList1 = new BsonArray();
      BsonDocument andDoc1 = new BsonDocument();
      andList1.add(new BsonDocument()
              .append("result[1].location", new BsonDocument()
                      .append("$exists", new BsonBoolean(true))));
      andList1.add(new BsonDocument()
              .append("result[1].location.state", new BsonDocument()
                      .append("$ne", new BsonString("AK"))));
      andList1.add(new BsonDocument()
              .append("result[4].emails[1]", new BsonDocument()
                      .append("$exists", new BsonBoolean(false))));
      andDoc1.put("$and", andList1);

      orList1.add(new BsonDocument()
              .append("result[2].location.coordinates.latitude",
                      new BsonDocument("$gt", new BsonDouble(0))));
      orList1.add(andDoc1);

      conditionDoc.put("$or", orList1);

      stmt = conn.prepareStatement("UPSERT INTO " + tableName
              + " VALUES (?) ON DUPLICATE KEY UPDATE COL = CASE WHEN"
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

}
