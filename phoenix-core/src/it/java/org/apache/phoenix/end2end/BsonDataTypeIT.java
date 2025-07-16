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

import org.apache.commons.io.FileUtils;
import org.apache.phoenix.thirdparty.com.google.common.base.Preconditions;
import org.apache.phoenix.schema.types.PBoolean;
import org.apache.phoenix.schema.types.PBson;
import org.apache.phoenix.schema.types.PDouble;
import org.apache.phoenix.schema.types.PInteger;
import org.apache.phoenix.schema.types.PVarchar;
import org.apache.phoenix.util.PropertiesUtil;
import org.bson.BsonArray;
import org.bson.BsonBinary;
import org.bson.BsonBoolean;
import org.bson.BsonDateTime;
import org.bson.BsonDocument;
import org.bson.BsonDouble;
import org.bson.BsonInt32;
import org.bson.BsonInt64;
import org.bson.BsonNull;
import org.bson.BsonString;
import org.bson.RawBsonDocument;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import static org.apache.phoenix.util.TestUtil.TEST_PROPERTIES;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

@Category(ParallelStatsDisabledTest.class)
@RunWith(Parameterized.class)
public class BsonDataTypeIT extends ParallelStatsDisabledIT {

  private final boolean columnEncoded;

  public BsonDataTypeIT(boolean columnEncoded) {
    this.columnEncoded = columnEncoded;
  }

  @Parameterized.Parameters(name = "BsonDataTypeIT_columnEncoded={0}")
  public static synchronized Collection<Object[]> data() {
    return Arrays.asList(new Object[][] { { false }, { true } });
  }

  private static String getJsonString(String jsonFilePath) throws IOException {
    URL fileUrl = BsonDataTypeIT.class.getClassLoader().getResource(jsonFilePath);
    Preconditions.checkArgument(fileUrl != null, "File path " + jsonFilePath + " seems invalid");
    return FileUtils.readFileToString(new File(fileUrl.getFile()), Charset.defaultCharset());
  }

  @Test
  public void testBsonDataTypeFunction() throws Exception {
    Properties props = PropertiesUtil.deepCopy(TEST_PROPERTIES);
    String tableName = generateUniqueName();

    try (Connection conn = DriverManager.getConnection(getUrl(), props)) {
      String ddl = "CREATE TABLE " + tableName + " (PK1 VARCHAR NOT NULL, COL BSON"
          + " CONSTRAINT pk PRIMARY KEY(PK1)) " + (this.columnEncoded
                                                   ? ""
                                                   : "COLUMN_ENCODED_BYTES=0");

      conn.createStatement().execute(ddl);

      String sample1 = getJsonString("json/sample_01.json");
      String sample2 = getJsonString("json/sample_02.json");
      String sample3 = getJsonString("json/sample_03.json");

      BsonDocument bsonDocument1 = RawBsonDocument.parse(sample1);
      BsonDocument bsonDocument2 = RawBsonDocument.parse(sample2);
      BsonDocument bsonDocument3 = RawBsonDocument.parse(sample3);

      PreparedStatement stmt = conn.prepareStatement("UPSERT INTO " + tableName + " VALUES (?,?)");

      stmt.setString(1, "pk1");
      stmt.setObject(2, bsonDocument1);
      stmt.executeUpdate();

      stmt.setString(1, "pk2");
      stmt.setObject(2, bsonDocument2);
      stmt.executeUpdate();

      stmt.setString(1, "pk3");
      stmt.setObject(2, bsonDocument3);
      stmt.executeUpdate();

      conn.commit();

      ResultSet rs = conn.createStatement().executeQuery(
          "SELECT BSON_DATA_TYPE(COL, 'press') FROM " + tableName + " WHERE PK1 = 'pk1'");
      assertTrue(rs.next());
      assertEquals(PVarchar.INSTANCE.getSqlTypeName(), rs.getString(1));
      assertFalse(rs.next());

      rs = conn.createStatement().executeQuery(
          "SELECT BSON_DATA_TYPE(COL, 'hurry') FROM " + tableName + " WHERE PK1 = 'pk1'");
      assertTrue(rs.next());
      assertEquals(PDouble.INSTANCE.getSqlTypeName(), rs.getString(1));
      assertFalse(rs.next());

      rs = conn.createStatement().executeQuery(
          "SELECT BSON_DATA_TYPE(COL, 'track') FROM " + tableName + " WHERE PK1 = 'pk1'");
      assertTrue(rs.next());
      assertEquals(PBson.INSTANCE.getSqlTypeName(), rs.getString(1));
      assertFalse(rs.next());

      rs = conn.createStatement().executeQuery(
          "SELECT BSON_DATA_TYPE(COL, 'gpu_name') FROM " + tableName + " WHERE PK1 = 'pk2'");
      assertTrue(rs.next());
      assertEquals(PVarchar.INSTANCE.getSqlTypeName(), rs.getString(1));
      assertFalse(rs.next());

      rs = conn.createStatement().executeQuery(
          "SELECT BSON_DATA_TYPE(COL, 'answer') FROM " + tableName + " WHERE PK1 = 'pk2'");
      assertTrue(rs.next());
      assertEquals(PDouble.INSTANCE.getSqlTypeName(), rs.getString(1));
      assertFalse(rs.next());

      rs = conn.createStatement().executeQuery(
          "SELECT BSON_DATA_TYPE(COL, 'result') FROM " + tableName + " WHERE PK1 = 'pk3'");
      assertTrue(rs.next());
      assertEquals(PBson.INSTANCE.getSqlTypeName(), rs.getString(1));
      assertFalse(rs.next());

      rs = conn.createStatement().executeQuery(
          "SELECT BSON_DATA_TYPE(COL, 'result[0]') FROM " + tableName + " WHERE PK1 = 'pk3'");
      assertTrue(rs.next());
      assertEquals(PBson.INSTANCE.getSqlTypeName(), rs.getString(1));
      assertFalse(rs.next());

      rs = conn.createStatement().executeQuery(
          "SELECT BSON_DATA_TYPE(COL, 'result[0].status') FROM " + tableName
              + " WHERE PK1 = 'pk3'");
      assertTrue(rs.next());
      assertEquals(PVarchar.INSTANCE.getSqlTypeName(), rs.getString(1));
      assertFalse(rs.next());

      rs = conn.createStatement().executeQuery(
          "SELECT BSON_DATA_TYPE(COL, 'track[0].shot[2][0].city.standard[5]') FROM " + tableName
              + " WHERE PK1 = 'pk1'");
      assertTrue(rs.next());
      assertEquals(PVarchar.INSTANCE.getSqlTypeName(), rs.getString(1));
      assertFalse(rs.next());

      rs = conn.createStatement().executeQuery(
          "SELECT BSON_DATA_TYPE(COL, 'track[0].shot[2][0].city.problem[2]') FROM " + tableName
              + " WHERE PK1 = 'pk1'");
      assertTrue(rs.next());
      assertEquals(PDouble.INSTANCE.getSqlTypeName(), rs.getString(1));
      assertFalse(rs.next());

      rs = conn.createStatement().executeQuery(
          "SELECT BSON_DATA_TYPE(COL, 'track[0].shot[2][0].city.problem[3]') FROM " + tableName
              + " WHERE PK1 = 'pk1'");
      assertTrue(rs.next());
      assertEquals(PBson.INSTANCE.getSqlTypeName(), rs.getString(1));
      assertFalse(rs.next());

      rs = conn.createStatement().executeQuery(
          "SELECT BSON_DATA_TYPE(COL, 'track[0].shot[2][0].city.problem[3].scene') FROM "
              + tableName + " WHERE PK1 = 'pk1'");
      assertTrue(rs.next());
      assertEquals(PBoolean.INSTANCE.getSqlTypeName(), rs.getString(1));
      assertFalse(rs.next());

      rs = conn.createStatement().executeQuery(
          "SELECT BSON_DATA_TYPE(COL, 'track[0].shot[2][0].character') FROM " + tableName
              + " WHERE PK1 = 'pk1'");
      assertTrue(rs.next());
      assertEquals(PInteger.INSTANCE.getSqlTypeName(), rs.getString(1));
      assertFalse(rs.next());

      rs = conn.createStatement().executeQuery(
          "SELECT BSON_DATA_TYPE(COL, 'attention') FROM " + tableName + " WHERE PK1 = 'pk1'");
      assertTrue(rs.next());
      assertEquals(PInteger.INSTANCE.getSqlTypeName(), rs.getString(1));
      assertFalse(rs.next());

      rs = conn.createStatement().executeQuery(
          "SELECT BSON_DATA_TYPE(COL, 'track[0]') FROM " + tableName + " WHERE PK1 = 'pk1'");
      assertTrue(rs.next());
      assertEquals(PBson.INSTANCE.getSqlTypeName(), rs.getString(1));
      assertFalse(rs.next());

      rs = conn.createStatement().executeQuery(
          "SELECT BSON_DATA_TYPE(COL, 'track[1]') FROM " + tableName + " WHERE PK1 = 'pk1'");
      assertTrue(rs.next());
      assertEquals(PVarchar.INSTANCE.getSqlTypeName(), rs.getString(1));
      assertFalse(rs.next());

      rs = conn.createStatement().executeQuery(
          "SELECT BSON_DATA_TYPE(COL, 'non_existent_field') FROM " + tableName
              + " WHERE PK1 = 'pk1'");
      assertTrue(rs.next());
      assertEquals("NULL", rs.getString(1));
      assertFalse(rs.next());

      rs = conn.createStatement().executeQuery(
          "SELECT BSON_DATA_TYPE(COL, 'track[0].shot[2][0].city') FROM " + tableName
              + " WHERE PK1 = 'pk1'");
      assertTrue(rs.next());
      assertEquals(PBson.INSTANCE.getSqlTypeName(), rs.getString(1));
      assertFalse(rs.next());

      rs = conn.createStatement().executeQuery(
          "SELECT BSON_DATA_TYPE(COL, 'track[0].shot[2][0].city.standard') FROM " + tableName
              + " WHERE PK1 = 'pk1'");
      assertTrue(rs.next());
      assertEquals(PBson.INSTANCE.getSqlTypeName(), rs.getString(1));
      assertFalse(rs.next());

      rs = conn.createStatement().executeQuery(
          "SELECT BSON_DATA_TYPE(COL, 'rather[3].outline') FROM " + tableName
              + " WHERE PK1 = 'pk2'");
      assertTrue(rs.next());
      assertEquals(PBson.INSTANCE.getSqlTypeName(), rs.getString(1));
      assertFalse(rs.next());

      rs = conn.createStatement().executeQuery(
          "SELECT BSON_DATA_TYPE(COL, 'rather[3].outline.halfway') FROM " + tableName
              + " WHERE PK1 = 'pk2'");
      assertTrue(rs.next());
      assertEquals(PBson.INSTANCE.getSqlTypeName(), rs.getString(1));
      assertFalse(rs.next());
    }
  }

  @Test
  public void testBsonValueWithBsonDataType() throws Exception {
    Properties props = PropertiesUtil.deepCopy(TEST_PROPERTIES);
    String tableName = generateUniqueName();

    try (Connection conn = DriverManager.getConnection(getUrl(), props)) {
      String ddl = "CREATE TABLE " + tableName + " (PK1 VARCHAR NOT NULL, COL BSON"
          + " CONSTRAINT pk PRIMARY KEY(PK1)) " + (this.columnEncoded
                                                   ? ""
                                                   : "COLUMN_ENCODED_BYTES=0");

      conn.createStatement().execute(ddl);

      String sample1 = getJsonString("json/sample_01.json");
      String sample2 = getJsonString("json/sample_02.json");
      BsonDocument bsonDocument1 = RawBsonDocument.parse(sample1);
      BsonDocument bsonDocument2 = RawBsonDocument.parse(sample2);

      PreparedStatement stmt = conn.prepareStatement("UPSERT INTO " + tableName + " VALUES (?,?)");

      stmt.setString(1, "pk1");
      stmt.setObject(2, bsonDocument1);
      stmt.executeUpdate();

      stmt.setString(1, "pk2");
      stmt.setObject(2, bsonDocument2);
      stmt.executeUpdate();

      conn.commit();

      ResultSet rs = conn.createStatement().executeQuery(
          "SELECT PK1, BSON_DATA_TYPE(COL, 'track[0].shot[2][0].city.standard[5]') FROM "
              + tableName + " WHERE PK1 = 'pk1'");
      assertTrue(rs.next());
      assertEquals("pk1", rs.getString(1));
      assertEquals(PVarchar.INSTANCE.getSqlTypeName(), rs.getString(2));
      assertFalse(rs.next());

      rs = conn.createStatement().executeQuery(
          "SELECT PK1, BSON_VALUE(COL, 'track[0].shot[2][0].city.standard[5]', 'VARCHAR') FROM "
              + tableName + " WHERE PK1 = 'pk1'");
      assertTrue(rs.next());
      assertEquals("pk1", rs.getString(1));
      assertEquals("softly", rs.getString(2));
      assertFalse(rs.next());

      rs = conn.createStatement().executeQuery(
          "SELECT PK1, BSON_DATA_TYPE(COL, 'track[0].shot[2][0].city.problem[2]') FROM " + tableName
              + " WHERE PK1 = 'pk1'");
      assertTrue(rs.next());
      assertEquals("pk1", rs.getString(1));
      assertEquals(PDouble.INSTANCE.getSqlTypeName(), rs.getString(2));
      assertFalse(rs.next());

      rs = conn.createStatement().executeQuery(
          "SELECT PK1, BSON_VALUE(COL, 'track[0].shot[2][0].city.problem[2]', 'DOUBLE') FROM "
              + tableName + " WHERE PK1 = 'pk1'");
      assertTrue(rs.next());
      assertEquals("pk1", rs.getString(1));
      assertEquals(1527061470.2690287, rs.getDouble(2), 0.000001);
      assertFalse(rs.next());

      rs = conn.createStatement().executeQuery(
          "SELECT PK1, BSON_DATA_TYPE(COL, 'track[0].shot[2][0].city.problem[3]') FROM " + tableName
              + " WHERE PK1 = 'pk1'");
      assertTrue(rs.next());
      assertEquals("pk1", rs.getString(1));
      assertEquals(PBson.INSTANCE.getSqlTypeName(), rs.getString(2));
      assertFalse(rs.next());

      rs = conn.createStatement().executeQuery(
          "SELECT PK1, BSON_VALUE(COL, 'track[0].shot[2][0].city.problem[3]', 'BSON') FROM "
              + tableName + " WHERE PK1 = 'pk1'");
      assertTrue(rs.next());
      assertEquals("pk1", rs.getString(1));
      BsonDocument nestedDoc = (BsonDocument) rs.getObject(2);
      assertNotNull(nestedDoc);
      assertTrue(nestedDoc.containsKey("condition"));
      assertTrue(nestedDoc.containsKey("higher"));
      assertTrue(nestedDoc.containsKey("scene"));
      assertFalse(rs.next());

      rs = conn.createStatement().executeQuery(
          "SELECT PK1, BSON_DATA_TYPE(COL, 'rather[3].outline.clock') FROM " + tableName
              + " WHERE PK1 = 'pk2'");
      assertTrue(rs.next());
      assertEquals("pk2", rs.getString(1));
      assertEquals(PVarchar.INSTANCE.getSqlTypeName(), rs.getString(2));
      assertFalse(rs.next());

      rs = conn.createStatement().executeQuery(
          "SELECT PK1, BSON_VALUE(COL, 'rather[3].outline.clock', 'VARCHAR') FROM " + tableName
              + " WHERE PK1 = 'pk2'");
      assertTrue(rs.next());
      assertEquals("pk2", rs.getString(1));
      assertEquals("personal", rs.getString(2));
      assertFalse(rs.next());
    }
  }

  @Test
  public void testBsonDataTypeNegativeCases() throws Exception {
    Properties props = PropertiesUtil.deepCopy(TEST_PROPERTIES);
    String tableName = generateUniqueName();

    try (Connection conn = DriverManager.getConnection(getUrl(), props)) {
      String ddl = "CREATE TABLE " + tableName + " (PK1 VARCHAR NOT NULL, COL BSON"
          + " CONSTRAINT pk PRIMARY KEY(PK1)) " + (this.columnEncoded
                                                   ? ""
                                                   : "COLUMN_ENCODED_BYTES=0");

      conn.createStatement().execute(ddl);

      String sample1 = getJsonString("json/sample_01.json");
      BsonDocument bsonDocument1 = RawBsonDocument.parse(sample1);

      PreparedStatement stmt = conn.prepareStatement("UPSERT INTO " + tableName + " VALUES (?,?)");
      stmt.setString(1, "pk1");
      stmt.setObject(2, bsonDocument1);
      stmt.executeUpdate();
      conn.commit();

      // Test 1: Null field path
      ResultSet rs = conn.createStatement().executeQuery(
          "SELECT BSON_DATA_TYPE(COL, NULL) FROM " + tableName + " WHERE PK1 = 'pk1'");
      assertTrue(rs.next());
      assertNull(rs.getString(1));
      assertFalse(rs.next());

      // Test 2: Empty field path
      rs = conn.createStatement()
          .executeQuery("SELECT BSON_DATA_TYPE(COL, '') FROM " + tableName + " WHERE PK1 = 'pk1'");
      assertTrue(rs.next());
      assertNull(rs.getString(1));
      assertFalse(rs.next());

      // Test 3: Non-existent field (this works because it returns null)
      rs = conn.createStatement().executeQuery(
          "SELECT BSON_DATA_TYPE(COL, 'non_existent_field') FROM " + tableName
              + " WHERE PK1 = 'pk1'");
      assertTrue(rs.next());
      assertEquals("NULL", rs.getString(1));
      assertFalse(rs.next());

      // Test 4: Invalid nested path (deeply nested non-existent field)
      rs = conn.createStatement().executeQuery(
          "SELECT BSON_DATA_TYPE(COL, 'track[0].nonexistent.deeply.nested.field') FROM " + tableName
              + " WHERE PK1 = 'pk1'");
      assertTrue(rs.next());
      assertEquals("NULL", rs.getString(1));
      assertFalse(rs.next());
    }
  }
}