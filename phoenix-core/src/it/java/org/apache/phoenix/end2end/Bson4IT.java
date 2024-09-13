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

import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.util.Pair;
import org.apache.phoenix.jdbc.PhoenixConnection;
import org.apache.phoenix.query.QueryConstants;
import org.apache.phoenix.schema.PTable;
import org.apache.phoenix.schema.tuple.Tuple;
import org.apache.phoenix.schema.types.PBson;
import org.apache.phoenix.schema.types.PDouble;
import org.bson.BsonArray;
import org.bson.BsonBinary;
import org.bson.BsonDocument;
import org.bson.BsonDouble;
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

      upsertRows(conn, tableName, bsonDocument1, bsonDocument2, bsonDocument3);
      PreparedStatement stmt;

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

  @Test
  public void testConditionalUpsertReturnRow() throws Exception {
    Properties props = PropertiesUtil.deepCopy(TEST_PROPERTIES);
    String tableName = generateUniqueName();
    try (Connection conn = DriverManager.getConnection(getUrl(), props)) {
      conn.setAutoCommit(true);
      String ddl = "CREATE TABLE " + tableName
              + " (PK1 VARCHAR NOT NULL, C1 VARCHAR, COL BSON"
              + " CONSTRAINT pk PRIMARY KEY(PK1))";
      conn.createStatement().execute(ddl);

      String sample1 = getJsonString("json/sample_01.json");
      String sample2 = getJsonString("json/sample_02.json");
      String sample3 = getJsonString("json/sample_03.json");
      BsonDocument bsonDocument1 = RawBsonDocument.parse(sample1);
      BsonDocument bsonDocument2 = RawBsonDocument.parse(sample2);
      BsonDocument bsonDocument3 = RawBsonDocument.parse(sample3);

      upsertRows(conn, tableName, bsonDocument1, bsonDocument2, bsonDocument3);
      PreparedStatement stmt;

      String conditionExpression =
              "press = :press AND track[0].shot[2][0].city.standard[50] = :softly";

      BsonDocument conditionDoc = new BsonDocument();
      conditionDoc.put("$EXPR", new BsonString(conditionExpression));
      conditionDoc.put("$VAL", new BsonDocument()
              .append(":press", new BsonString("beat"))
              .append(":softly", new BsonString("softly")));

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
              "press = :press AND track[0].shot[2][0].city.standard[5] = :softly";

      conditionDoc = new BsonDocument();
      conditionDoc.put("$EXPR", new BsonString(conditionExpression));
      conditionDoc.put("$VAL", new BsonDocument()
              .append(":press", new BsonString("beat"))
              .append(":softly", new BsonString("softly")));

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

      // Conditional Upsert successful
      assertReturnedRowResult(stmt, conn, tableName, "json/sample_updated_01.json", true);

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

      conditionDoc = new BsonDocument();
      conditionDoc.put("$EXPR", new BsonString(conditionExpression));
      conditionDoc.put("$VAL", new BsonDocument());

      stmt = conn.prepareStatement("UPSERT INTO " + tableName
              + " VALUES (?) ON DUPLICATE KEY UPDATE COL = CASE WHEN"
              + " BSON_CONDITION_EXPRESSION(COL, '" + conditionDoc.toJson() + "')"
              + " THEN BSON_UPDATE_EXPRESSION(COL, '" + updateExp + "') ELSE COL END");

      stmt.setString(1, "pk1010");

      // Conditional Upsert successful
      assertReturnedRowResult(stmt, conn, tableName, "json/sample_updated_02.json", true);

      updateExp = new BsonDocument()
              .append("$SET", new BsonDocument()
                      .append("result[1].location.state", new BsonString("AK")))
              .append("$UNSET", new BsonDocument()
                      .append("result[4].emails[1]", new BsonNull()));

      conditionExpression =
              "result[2].location.coordinates.latitude > :latitude OR "
                      + "(field_exists(result[1].location) AND result[1].location.state != :state" +
                      " AND field_exists(result[4].emails[1]))";

      conditionDoc = new BsonDocument();
      conditionDoc.put("$EXPR", new BsonString(conditionExpression));
      conditionDoc.put("$VAL", new BsonDocument()
              .append(":latitude", new BsonDouble(0))
              .append(":state", new BsonString("AK")));

      stmt = conn.prepareStatement("UPSERT INTO " + tableName
              + " VALUES (?) ON DUPLICATE KEY UPDATE COL = CASE WHEN"
              + " BSON_CONDITION_EXPRESSION(COL, '" + conditionDoc.toJson() + "')"
              + " THEN BSON_UPDATE_EXPRESSION(COL, '" + updateExp + "') ELSE COL END");

      stmt.setString(1, "pk1011");

      // Conditional Upsert successful
      assertReturnedRowResult(stmt, conn, tableName, "json/sample_updated_03.json", true);

      conditionExpression =
              "press = :press AND track[0].shot[2][0].city.standard[5] = :softly";

      conditionDoc = new BsonDocument();
      conditionDoc.put("$EXPR", new BsonString(conditionExpression));
      conditionDoc.put("$VAL", new BsonDocument()
              .append(":press", new BsonString("incorrect_value"))
              .append(":softly", new BsonString("incorrect_value")));

      updateExp = new BsonDocument()
              .append("$SET", new BsonDocument()
                      .append("new_field1",
                              new BsonBinary(PDouble.INSTANCE.toBytes(-505169340.54880095)))
                      .append("track[0].shot[2][0].city.standard[5]", new BsonString(
                              "soft_new_val"))
                      .append("track[0].shot[2][0].city.problem[2]",
                              new BsonString("track[0].shot[2][0].city.problem[2] + 123")));

      stmt = conn.prepareStatement("UPSERT INTO " + tableName
              + " VALUES (?) ON DUPLICATE KEY UPDATE COL = CASE WHEN"
              + " BSON_CONDITION_EXPRESSION(COL, '" + conditionDoc.toJson() + "')"
              + " THEN BSON_UPDATE_EXPRESSION(COL, '" + updateExp + "') ELSE COL END");
      stmt.setString(1, "pk0001");

      // Conditional Upsert not successful
      assertReturnedRowResult(stmt, conn, tableName, "json/sample_updated_01.json", false);

      verifyRows(tableName, conn);
    }
  }

  private static void upsertRows(Connection conn, String tableName, BsonDocument bsonDocument1,
                                BsonDocument bsonDocument2, BsonDocument bsonDocument3)
          throws SQLException {
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
  }

  private static void verifyRows(String tableName, Connection conn)
          throws SQLException, IOException {
    String query;
    ResultSet rs;
    BsonDocument document1;
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

  private static void assertReturnedRowResult(PreparedStatement stmt,
                                              Connection conn,
                                              String tableName,
                                              String jsonPath,
                                              boolean success)
          throws SQLException, IOException {
    Pair<Integer, Tuple> resultPair =
            stmt.unwrap(PhoenixPreparedStatement.class).executeUpdateReturnRow();
    assertEquals(success ? 1 : 0, resultPair.getFirst().intValue());
    Tuple result = resultPair.getSecond();
    PTable table = conn.unwrap(PhoenixConnection.class).getTable(tableName);

    Cell cell = result.getValue(QueryConstants.DEFAULT_COLUMN_FAMILY_BYTES,
            table.getColumns().get(2).getColumnQualifierBytes());
    assertEquals(RawBsonDocument.parse(getJsonString(jsonPath)),
            PBson.INSTANCE.toObject(cell.getValueArray(), cell.getValueOffset(),
                    cell.getValueLength()));
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