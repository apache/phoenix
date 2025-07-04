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
import java.sql.Statement;
import java.util.Arrays;
import java.util.Collection;
import java.util.Properties;

import org.apache.hadoop.hbase.util.Pair;
import org.apache.phoenix.jdbc.PhoenixStatement;
import org.apache.phoenix.jdbc.PhoenixPreparedStatement;
import org.apache.phoenix.schema.types.PDouble;
import org.bson.BsonArray;
import org.bson.BsonBinary;
import org.bson.BsonDocument;
import org.bson.BsonDouble;
import org.bson.BsonNull;
import org.bson.BsonString;
import org.bson.RawBsonDocument;
import org.bson.Document;
import org.junit.Assume;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import org.apache.commons.io.FileUtils;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.phoenix.compile.ExplainPlan;
import org.apache.phoenix.compile.ExplainPlanAttributes;
import org.apache.phoenix.thirdparty.com.google.common.base.Preconditions;
import org.apache.phoenix.util.PropertiesUtil;

import static org.apache.phoenix.util.TestUtil.TEST_PROPERTIES;
import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
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

      validateExplainPlan(ps, indexName2, "RANGE SCAN ");

      ps = conn.prepareStatement("SELECT PK1, COL FROM " + tableName
          + " WHERE BSON_VALUE(COL, 'rather[3].outline.clock', 'VARCHAR') = ?");
      ps.setString(1, "personal");

      rs = ps.executeQuery();

      assertTrue(rs.next());
      assertEquals("pk1010", rs.getString(1));
      actualDoc = (BsonDocument) rs.getObject(2);
      assertEquals(bsonDocument2, actualDoc);

      assertFalse(rs.next());

      validateExplainPlan(ps, indexName1, "RANGE SCAN ");

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

      validateExplainPlan(ps, indexName1, "RANGE SCAN ");

      ps = conn.prepareStatement("SELECT PK1, COL FROM " + tableName
          + " WHERE BSON_VALUE(COL, 'result[1].location.coordinates.longitude', 'DOUBLE') = ?");
      ps.setDouble(1, 52.37);

      rs = ps.executeQuery();
      assertFalse(rs.next());

      validateExplainPlan(ps, indexName2, "RANGE SCAN ");
    }
  }

  @Test
  public void testBsonValueWithBinaryEncoded() throws Exception {

    byte[] doc1Field1 = {
            0, 1, 2, 3, 0, 1, 5, 6, 7, 8, 0,
            10, 11, 12, 13, 0, 0, -1, 15, 16, 17, 18, 19};
    byte[] doc1Field2 = {
            1, 0, 2, 3, 4, 0, 6, 7, 8, 9,
            0, 11, 12, 13, 14, 0, 16, 17, 18, 19, 20};
    byte[] doc1Field3 = {
            1, 2, 0, 3, 4, 5, 0, 7, 8, 9,
            10, 0, 12, 13, 14, 15, 0, 17, 18, 19, 20, 21};
    byte[] doc1Field4 = {
            1, 2, 3, 0, 4, 5, 6, 0, 8, 9,
            10, 11, 0, 13, 14, 15, 16, 0, 18, 19, 20, 21, 22};
    byte[] doc1Field5 = {
            1, 2, 3, 4, 0, 5, 6, 7, 0, 9,
            10, 11, 12, 0, 14, 15, 16, 17, 0, 19, 20, 21, 22, 23};

    byte[] doc2Field1 = {
            0, 25, 35, 45, 0, 55, 65, 75, 85, 0,
            95, 105, 115, 125, 0, 15, 25, 35, 45, 55};
    byte[] doc2Field2 = {
            25, 0, 35, 45, 55, 0, 65, 75, 85, 95,
            0, 105, 115, 125, -125, 0, 15, 25, 35, 45, 55};
    byte[] doc2Field3 = {
            25, 35, 0, 45, 55, 65, 0, 75, 85, 95,
            105, 0, 115, 125, -125, -115, 0, 15, 25, 35, 45, 55};
    byte[] doc2Field4 = {
            25, 35, 45, 0, 55, 65, 75, 0, 85, 95,
            105, 115, 0, 125, -125, -115, -105, 0, 15, 25, 35, 45, 55};
    byte[] doc2Field5 = {
            25, 35, 45, 55, 0, 65, 75, 85, 0, 95,
            105, 115, 125, 0, -125, -115, -105, -95, 0, 15, 25, 35, 45, 55};

    byte[] doc3Field1 = {
            0, -1, -2, -3, 0, -5, -6, -7, -8, 0,
            -10, -11, -12, -13, 0, -15, -16, -17, -18, -19};
    byte[] doc3Field2 = {
            -1, 0, -2, -3, -4, 0, -6, -7, -8, -9,
            0, -11, -12, -13, -14, 0, -16, -17, -18, -19, -20};
    byte[] doc3Field3 = {
            -1, -2, 0, -3, -4, -5, 0, -7, -8, -9,
            -10, 0, -12, -13, -14, -15, 0, -17, -18, -19, -20, -21};
    byte[] doc3Field4 = {
            -1, -2, -3, 0, -4, -5, -6, 0, -8, -9,
            -10, -11, 0, -13, -14, -15, -16, 0, -18, -19, -20, -21, -22};
    byte[] doc3Field5 = {
            -1, -2, -3, -4, 0, -5, -6, -7, 0, -9,
            -10, -11, -12, 0, -14, -15, -16, -17, 0, -19, -20, -21, -22, -23};

    byte[] doc4Field1 = {
            0, -1, -2, -3, 0, 0, 1 - 5, -6, -7, -8, 0,
            -10, -11, -12, -13, 0, -15, -16, -17, -18, -19};
    byte[] doc4Field2 = {
            -1, 0, -2, -3, -4, 0, 6, -7, -8, -9,
            0, -11, -12, -13, -14, 0, -16, -17, -18, -19, -20};
    byte[] doc4Field30 = {
            -1, -2, 0, -3, -4, -5, 0, 0, -7, 8, -9,
            -10, -1, 0, -1, -12, -13, -14, -15, 0, -17, -18, -19, -20, -21};
    byte[] doc4Field4 = {
            -1, -2, -3, 0, -4, -5, -6, 0, -8, -9,
            -10, -11, -1, 0, 1, -13, -14, -15, -16, 0, -18, -19, -20, -21, -22};
    byte[] doc4Field5 = {
            -1, -1, 0, -2, -3, -4, 0, -5, -6, -7, 0, -9,
            -10, -11, -12, 0, -14, -15, -16, -17, 0, -19, -20, -21, -22, -23};

    Properties props = PropertiesUtil.deepCopy(TEST_PROPERTIES);
    String tableName = generateUniqueName();
    String indexName1 = "IDX1_" + tableName;
    String indexName2 = "IDX2_" + tableName;

    try (Connection conn = DriverManager.getConnection(getUrl(), props)) {

      String ddl = "CREATE TABLE " + tableName
              + " (PK1 VARCHAR NOT NULL, C1 VARCHAR, COL BSON"
              + " CONSTRAINT pk PRIMARY KEY(PK1))";

      final String indexDdl1;
      if (!this.coveredIndex) {
        indexDdl1 = "CREATE UNCOVERED INDEX " + indexName1 + " ON " + tableName
                + "(BSON_VALUE(COL, 'binary_field1', 'VARBINARY_ENCODED')) WHERE "
                + "BSON_VALUE(COL, 'binary_field1', 'VARBINARY_ENCODED') IS NOT NULL";
      } else {
        indexDdl1 = "CREATE INDEX " + indexName1 + " ON " + tableName
                + "(BSON_VALUE(COL, 'binary_field1', 'VARBINARY_ENCODED')) INCLUDE(COL, C1) WHERE "
                + "BSON_VALUE(COL, 'binary_field1', 'VARBINARY_ENCODED') IS NOT NULL";
      }

      final String indexDdl2;
      if (!this.coveredIndex) {
        indexDdl2 = "CREATE UNCOVERED INDEX " + indexName2 + " ON " + tableName
                + "(BSON_VALUE(COL, 'binary_field3', 'VARBINARY_ENCODED')) WHERE "
                + "BSON_VALUE(COL, 'binary_field3', 'VARBINARY_ENCODED') IS NOT NULL";
      } else {
        indexDdl2 = "CREATE INDEX " + indexName2 + " ON " + tableName
                + "(BSON_VALUE(COL, 'binary_field3', 'VARBINARY_ENCODED')) INCLUDE(COL, C1) WHERE "
                + "BSON_VALUE(COL, 'binary_field3', 'VARBINARY_ENCODED') IS NOT NULL";
      }

      conn.createStatement().execute(ddl);
      conn.createStatement().execute(indexDdl1);
      conn.createStatement().execute(indexDdl2);

      BsonDocument doc1 = new BsonDocument()
              .append("binary_field1", new BsonBinary(doc1Field1))
              .append("binary_field2", new BsonBinary(doc1Field2))
              .append("binary_field3", new BsonBinary(doc1Field3))
              .append("binary_field4", new BsonBinary(doc1Field4))
              .append("binary_field5", new BsonBinary(doc1Field5));

      BsonDocument doc2 = new BsonDocument()
              .append("binary_field1", new BsonBinary(doc2Field1))
              .append("binary_field2", new BsonBinary(doc2Field2))
              .append("binary_field3", new BsonBinary(doc2Field3))
              .append("binary_field4", new BsonBinary(doc2Field4))
              .append("binary_field5", new BsonBinary(doc2Field5));

      BsonDocument doc3 = new BsonDocument()
              .append("binary_field1", new BsonBinary(doc3Field1))
              .append("binary_field2", new BsonBinary(doc3Field2))
              .append("binary_field3", new BsonBinary(doc3Field3))
              .append("binary_field4", new BsonBinary(doc3Field4))
              .append("binary_field5", new BsonBinary(doc3Field5));

      BsonDocument doc4 = new BsonDocument()
              .append("binary_field1", new BsonBinary(doc4Field1))
              .append("binary_field2", new BsonBinary(doc4Field2))
              .append("binary_field30", new BsonBinary(doc4Field30))
              .append("binary_field4", new BsonBinary(doc4Field4))
              .append("binary_field5", new BsonBinary(doc4Field5));

      PreparedStatement stmt =
              conn.prepareStatement("UPSERT INTO " + tableName + " VALUES (?,?,?)");

      stmt.setString(1, "pk1");
      stmt.setString(2, "c1_value1");
      stmt.setObject(3, doc1);
      stmt.executeUpdate();

      stmt.setString(1, "pk2");
      stmt.setString(2, "c1_value2");
      stmt.setObject(3, doc2);
      stmt.executeUpdate();

      stmt.setString(1, "pk3");
      stmt.setString(2, "c1_value3");
      stmt.setObject(3, doc3);
      stmt.executeUpdate();

      stmt.setString(1, "pk4");
      stmt.setString(2, "c1_value4");
      stmt.setObject(3, doc4);
      stmt.executeUpdate();

      conn.commit();

      ResultSet rs = conn.createStatement().executeQuery("SELECT count(*) FROM " + tableName);
      assertTrue(rs.next());
      assertEquals(4, rs.getInt(1));

      rs = conn.createStatement().executeQuery("SELECT count(*) FROM " + indexName1);
      assertTrue(rs.next());
      assertEquals(4, rs.getInt(1));

      rs = conn.createStatement().executeQuery("SELECT count(*) FROM " + indexName2);
      assertTrue(rs.next());
      assertEquals(3, rs.getInt(1));

      PreparedStatement ps = conn.prepareStatement(
              "SELECT PK1, C1, COL, BSON_VALUE(COL, 'binary_field2', 'VARBINARY_ENCODED') FROM "
                      + tableName
                      + " WHERE BSON_VALUE(COL, 'binary_field1', 'VARBINARY_ENCODED') = ?");
      ps.setBytes(1, doc1Field1);

      rs = ps.executeQuery();
      assertTrue(rs.next());
      assertEquals("pk1", rs.getString(1));
      assertEquals("c1_value1", rs.getString(2));
      BsonDocument actualDoc = (BsonDocument) rs.getObject(3);
      assertEquals(doc1, actualDoc);
      assertArrayEquals(doc1Field2, rs.getBytes(4));
      assertFalse(rs.next());

      validateExplainPlan(ps, indexName1, "RANGE SCAN ");

      ps = conn.prepareStatement("SELECT PK1, C1, COL FROM " + tableName
              + " WHERE BSON_VALUE(COL, 'binary_field3', 'VARBINARY_ENCODED') = ?");
      ps.setBytes(1, doc2Field3);

      rs = ps.executeQuery();
      assertTrue(rs.next());
      assertEquals("pk2", rs.getString(1));
      assertEquals("c1_value2", rs.getString(2));
      actualDoc = (BsonDocument) rs.getObject(3);
      assertEquals(doc2, actualDoc);
      assertFalse(rs.next());

      validateExplainPlan(ps, indexName2, "RANGE SCAN ");

      ps = conn.prepareStatement("SELECT PK1, C1, COL FROM " + tableName
              + " WHERE BSON_VALUE(COL, 'binary_field1', 'VARBINARY_ENCODED') = ?");
      ps.setBytes(1, new byte[]{
              0, 1, 2, 3, 0, 0, 1, 5, 6, 7, 8, 0,
              10, 11, 12, 13, 0, 0, -1, 15, 16, 17, 18, 19});

      rs = ps.executeQuery();
      assertFalse(rs.next());

      validateExplainPlan(ps, indexName1, "RANGE SCAN ");

      ps = conn.prepareStatement(
              "SELECT PK1, C1, COL, BSON_VALUE(COL, 'binary_field5', 'VARBINARY_ENCODED') FROM "
                      + tableName + " WHERE C1 = ? AND "
                      + "BSON_VALUE(COL, 'binary_field1', 'VARBINARY_ENCODED') = ?");
      ps.setString(1, "c1_value2");
      ps.setBytes(2, doc2Field1);

      rs = ps.executeQuery();
      assertTrue(rs.next());
      assertEquals("pk2", rs.getString(1));
      assertEquals("c1_value2", rs.getString(2));
      actualDoc = (BsonDocument) rs.getObject(3);
      assertEquals(doc2, actualDoc);
      assertArrayEquals(doc2Field5, rs.getBytes(4));
      assertFalse(rs.next());

      validateExplainPlan(ps, indexName1, "RANGE SCAN ");

      ps = conn.prepareStatement("SELECT PK1, C1, COL FROM " + tableName
              + " WHERE C1 = ?");
      ps.setString(1, "c1_value3");

      rs = ps.executeQuery();
      assertTrue(rs.next());
      assertEquals("pk3", rs.getString(1));
      assertEquals("c1_value3", rs.getString(2));
      actualDoc = (BsonDocument) rs.getObject(3);
      assertEquals(doc3, actualDoc);
      assertFalse(rs.next());

      validateExplainPlan(ps, tableName, "FULL SCAN ");
    }
  }

  @Test
  public void testBsonValueFunctionWithBSONType() throws Exception {
    Assume.assumeTrue(this.coveredIndex && this.columnEncoded); // Since indexing on BSON not supported PHOENIX-7654
    Properties props = PropertiesUtil.deepCopy(TEST_PROPERTIES);
    String tableName = generateUniqueName();
    try (Connection conn = DriverManager.getConnection(getUrl(), props)) {
      String ddl = "CREATE TABLE " + tableName
          + " (PK1 VARCHAR NOT NULL, C1 VARCHAR, COL BSON"
          + " CONSTRAINT pk PRIMARY KEY(PK1)) "
          + (this.columnEncoded ? "" : "COLUMN_ENCODED_BYTES=0");

      conn.createStatement().execute(ddl);

      String sample1 = getJsonString("json/sample_01.json");
      String sample2 = getJsonString("json/sample_02.json");
      String sample3 = getJsonString("json/sample_03.json");
      String result = getJsonString("json/result_03.json");
      BsonDocument bsonDocument1 = RawBsonDocument.parse(sample1);
      BsonDocument bsonDocument2 = RawBsonDocument.parse(sample2);
      BsonDocument bsonDocument3 = RawBsonDocument.parse(sample3);
      BsonDocument bsonResultDocument = RawBsonDocument.parse(result);

      upsertRows(conn, tableName, bsonDocument1, bsonDocument2, bsonDocument3);

      conn.commit();

      ResultSet rs = conn.createStatement().executeQuery("SELECT count(*) FROM " + tableName);
      assertTrue(rs.next());
      assertEquals(3, rs.getInt(1));


      PreparedStatement ps = conn.prepareStatement("SELECT PK1, COL, "
          + "BSON_VALUE(COL, 'result[1]', 'BSON') FROM " + tableName
          + " WHERE BSON_VALUE(COL, 'result[1].location', 'BSON') = ?");

      Document info = new Document()
          .append("street", "4897 Gerhold Lodge")
          .append("city", "Minneapolis")
          .append("state", "Arkansas")
          .append("country", "Democratic Republic of the Congo")
          .append("zip", "79299")
          .append("coordinates", new Document()
              .append("latitude", 3.4015)
              .append("longitude", 52.3736));
      RawBsonDocument document = RawBsonDocument.parse(info.toJson());

      ps.setObject(1, document);
      rs = ps.executeQuery();

      assertTrue(rs.next());
      assertEquals("pk1011", rs.getString(1));
      BsonDocument actualDoc = (BsonDocument) rs.getObject(3);
      assertEquals(bsonResultDocument, actualDoc);
      assertFalse(rs.next());

      validateExplainPlan(ps, tableName, "FULL SCAN ");
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
              + " VALUES (?) ON DUPLICATE KEY UPDATE_ONLY COL = CASE WHEN"
              + " BSON_CONDITION_EXPRESSION(COL, '" + conditionDoc.toJson() + "')"
              + " THEN BSON_UPDATE_EXPRESSION(COL, '" + updateExp + "') ELSE COL END,"
              + " C1 = ?");
      stmt.setString(1, "pk0001");
      stmt.setString(2, "0003");

      // Conditional Upsert successful
      assertReturnedRowResult(stmt, "json/sample_updated_01.json", true);

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
      assertReturnedRowResult(stmt, "json/sample_updated_02.json", true);

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
      assertReturnedRowResult(stmt, "json/sample_updated_03.json", true);

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
      assertReturnedRowResult(stmt, "json/sample_updated_01.json", false);

      stmt = conn.prepareStatement("UPSERT INTO " + tableName
              + " VALUES (?) ON DUPLICATE KEY UPDATE_ONLY COL = CASE WHEN"
              + " BSON_CONDITION_EXPRESSION(COL, '" + conditionDoc.toJson() + "')"
              + " THEN BSON_UPDATE_EXPRESSION(COL, '" + updateExp + "') ELSE COL END");
      // the row does not exist already
      stmt.setString(1, "pk000111");

      // Conditional Upsert not successful, no row upserted
      assertReturnedRowResult(stmt, null, false);

      verifyRows(tableName, conn, false);

      stmt = conn.prepareStatement("UPSERT INTO " + tableName
              + " VALUES (?) ON DUPLICATE KEY UPDATE COL = CASE WHEN"
              + " BSON_CONDITION_EXPRESSION(COL, '" + conditionDoc.toJson() + "')"
              + " THEN BSON_UPDATE_EXPRESSION(COL, '" + updateExp + "') ELSE COL END");
      // the row does not exist already
      stmt.setString(1, "pk000123456");

      // Conditional Upsert successful, row upserted
      assertReturnedRowResult(stmt, null, true);
      verifyRows(tableName, conn, true);
    }
  }

  @Test
  public void testConditionalUpsertReturnOldRow() throws Exception {
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

      assertReturnedOldRowResult(stmt, "json/sample_01.json", true);

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

      assertReturnedOldRowResult(stmt, "json/sample_02.json", true);

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
              + " VALUES (?) ON DUPLICATE KEY UPDATE_ONLY COL = CASE WHEN"
              + " BSON_CONDITION_EXPRESSION(COL, '" + conditionDoc.toJson() + "')"
              + " THEN BSON_UPDATE_EXPRESSION(COL, '" + updateExp + "') ELSE COL END");

      stmt.setString(1, "pk1011");

      assertReturnedOldRowResult(stmt, "json/sample_03.json", true);

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
              + " VALUES (?) ON DUPLICATE KEY UPDATE_ONLY COL = CASE WHEN"
              + " BSON_CONDITION_EXPRESSION(COL, '" + conditionDoc.toJson() + "')"
              + " THEN BSON_UPDATE_EXPRESSION(COL, '" + updateExp + "') ELSE COL END");
      stmt.setString(1, "pk0001");

      assertReturnedOldRowResult(stmt, "json/sample_updated_01.json", false);

      stmt = conn.prepareStatement("UPSERT INTO " + tableName
              + " VALUES (?) ON DUPLICATE KEY UPDATE_ONLY COL = CASE WHEN"
              + " BSON_CONDITION_EXPRESSION(COL, '" + conditionDoc.toJson() + "')"
              + " THEN BSON_UPDATE_EXPRESSION(COL, '" + updateExp + "') ELSE COL END");
      // row does not exist
      stmt.setString(1, "pk00012345");

      assertReturnedOldRowResult(stmt, null, false);

      verifyRows(tableName, conn, false);

      stmt = conn.prepareStatement("UPSERT INTO " + tableName
              + " VALUES (?) ON DUPLICATE KEY UPDATE COL = CASE WHEN"
              + " BSON_CONDITION_EXPRESSION(COL, '" + conditionDoc.toJson() + "')"
              + " THEN BSON_UPDATE_EXPRESSION(COL, '" + updateExp + "') ELSE COL END");
      // row does not exist
      stmt.setString(1, "pk000123456");

      assertReturnedOldRowResult(stmt, null, true);
      verifyRows(tableName, conn, true);
    }
  }

  @Test
  public void testBsonPk() throws Exception {
    Properties props = PropertiesUtil.deepCopy(TEST_PROPERTIES);
    String tableName = generateUniqueName();
    try (Connection conn = DriverManager.getConnection(getUrl(), props)) {
      conn.setAutoCommit(true);
      String ddl = "CREATE TABLE " + tableName
              + " (PK1 BSON NOT NULL, C1 VARCHAR"
              + " CONSTRAINT pk PRIMARY KEY(PK1))";
      conn.createStatement().execute(ddl);

      String sample1 = getJsonString("json/sample_01.json");
      String sample2 = getJsonString("json/sample_02.json");
      String sample3 = getJsonString("json/sample_03.json");
      BsonDocument bsonDocument1 = RawBsonDocument.parse(sample1);
      BsonDocument bsonDocument2 = RawBsonDocument.parse(sample2);
      BsonDocument bsonDocument3 = RawBsonDocument.parse(sample3);

      upsertRowsWithBsonPkCol(conn, tableName, bsonDocument1, bsonDocument2, bsonDocument3);

      String conditionExpression =
              "press = :press AND track[0].shot[2][0].city.standard[5] = :softly";

      BsonDocument conditionDoc = new BsonDocument();
      conditionDoc.put("$EXPR", new BsonString(conditionExpression));
      conditionDoc.put("$VAL", new BsonDocument()
              .append(":press", new BsonString("beat"))
              .append(":softly", new BsonString("softly")));

      String query = "SELECT * FROM " + tableName +
              " WHERE PK1 = ?";
      PreparedStatement pst = conn.prepareStatement(query);
      pst.setObject(1, bsonDocument1);
      ResultSet rs = pst.executeQuery(query);

      assertTrue(rs.next());
      assertEquals("0002", rs.getString(2));
      BsonDocument document1 = (BsonDocument) rs.getObject(1);
      assertEquals(bsonDocument1, document1);

      assertFalse(rs.next());

      validateExplainPlan(pst, tableName, "POINT LOOKUP ON 1 KEY ");

      query = "SELECT * FROM " + tableName +
              " WHERE PK1 = CAST('" + sample2 + "' AS BSON)";
      Statement stmt = conn.createStatement();
      rs = stmt.executeQuery(query);

      assertTrue(rs.next());
      assertEquals("1010", rs.getString(2));
      BsonDocument document2 = (BsonDocument) rs.getObject(1);
      assertEquals(bsonDocument2, document2);

      assertFalse(rs.next());

      validateExplainPlan(stmt, query, tableName, "POINT LOOKUP ON 1 KEY ");

      query = "SELECT * FROM " + tableName +
              " WHERE PK1 != CAST('" + sample1 + "' AS BSON)";
      stmt = conn.createStatement();
      rs = stmt.executeQuery(query);

      assertTrue(rs.next());
      assertEquals("1011", rs.getString(2));
      document2 = (BsonDocument) rs.getObject(1);
      assertEquals(bsonDocument3, document2);

      assertTrue(rs.next());
      assertEquals("1010", rs.getString(2));
      BsonDocument document3 = (BsonDocument) rs.getObject(1);
      assertEquals(bsonDocument2, document3);

      assertFalse(rs.next());

      validateExplainPlan(stmt, query, tableName, "FULL SCAN ");

      query = "SELECT * FROM " + tableName;
      rs = conn.createStatement().executeQuery(query);

      assertTrue(rs.next());
      assertEquals("0002", rs.getString(2));
      document1 = (BsonDocument) rs.getObject(1);
      assertEquals(bsonDocument1, document1);

      assertTrue(rs.next());
      assertEquals("1011", rs.getString(2));
      document2 = (BsonDocument) rs.getObject(1);
      assertEquals(bsonDocument3, document2);

      assertTrue(rs.next());
      assertEquals("1010", rs.getString(2));
      document3 = (BsonDocument) rs.getObject(1);
      assertEquals(bsonDocument2, document3);

      assertFalse(rs.next());
    }
  }

  private static void upsertRowsWithBsonPkCol(Connection conn, String tableName,
                                              BsonDocument bsonDocument1,
                                              BsonDocument bsonDocument2,
                                              BsonDocument bsonDocument3)
          throws SQLException {
    PreparedStatement stmt =
            conn.prepareStatement("UPSERT INTO " + tableName + " VALUES (?,?)");
    stmt.setString(2, "0002");
    stmt.setObject(1, bsonDocument1);
    stmt.executeUpdate();

    stmt.setString(2, "1010");
    stmt.setObject(1, bsonDocument2);
    stmt.executeUpdate();

    stmt.setString(2, "1011");
    stmt.setObject(1, bsonDocument3);
    stmt.executeUpdate();
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

  private static void verifyRows(String tableName, Connection conn, boolean isNewRowAdded)
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

    if (isNewRowAdded) {
      assertTrue(rs.next());
      assertEquals("pk000123456", rs.getString(1));
      assertNull(rs.getString(2));
      assertNull(rs.getObject(3));
    }

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
                                              String jsonPath,
                                              boolean success)
          throws SQLException, IOException {
    stmt.execute();
    assertEquals(success ? 1 : 0, stmt.getUpdateCount());
    ResultSet resultSet = stmt.getResultSet();
    assertEquals(jsonPath == null ? null : RawBsonDocument.parse(getJsonString(jsonPath)),
            resultSet.getObject(3));
  }

  private static void assertReturnedOldRowResult(PreparedStatement stmt,
                                                 String jsonPath,
                                                 boolean success)
          throws SQLException, IOException {
    Pair<Integer, ResultSet> resultPair =
            stmt.unwrap(PhoenixPreparedStatement.class).executeAtomicUpdateReturnOldRow();
    assertEquals(success ? 1 : 0, (int) resultPair.getFirst());
    ResultSet resultSet = resultPair.getSecond();
    if (success) {
      assertEquals(jsonPath == null ? null : RawBsonDocument.parse(getJsonString(jsonPath)),
              resultSet.getObject(3));
      assertFalse(resultSet.next());
    } else {
      assertEquals(jsonPath == null ? null : RawBsonDocument.parse(getJsonString(jsonPath)),
              resultSet.getObject(3));
    }
  }

  private static void validateExplainPlan(PreparedStatement ps, String tableName, String scanType)
      throws SQLException {
    ExplainPlan plan = ps.unwrap(PhoenixPreparedStatement.class).optimizeQuery().getExplainPlan();
    validatePlan(tableName, scanType, plan);
  }

  private static void validateExplainPlan(Statement stmt, String query, String tableName,
                                          String scanType)
          throws SQLException {
    ExplainPlan plan = stmt.unwrap(PhoenixStatement.class).optimizeQuery(query).getExplainPlan();
    validatePlan(tableName, scanType, plan);
  }

  private static void validatePlan(String tableName, String scanType, ExplainPlan plan) {
    ExplainPlanAttributes explainPlanAttributes = plan.getPlanStepsAsAttributes();
    assertEquals(tableName, explainPlanAttributes.getTableName());
    assertEquals("PARALLEL 1-WAY", explainPlanAttributes.getIteratorTypeAndScanSize());
    assertEquals(scanType, explainPlanAttributes.getExplainScanType());
  }

}
