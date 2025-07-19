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
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.Arrays;
import org.apache.commons.lang3.StringUtils;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;

@Category(ParallelStatsDisabledTest.class)
public class DecodeBinaryFunctionIT extends ParallelStatsDisabledIT {

  private static String testTable;
  private static final String helloPhoenixString = "HelloPhoenix";
  private static final byte[] helloPhoenixBytes = "HelloPhoenix".getBytes();
  private static final String base64Chunk = "SGVsbG9QaG9lbml4";
  private static final String hex48String = "48656c6c6f";

  @BeforeClass
  public static void setUpBeforeClass() throws Exception {
    Connection conn = DriverManager.getConnection(getUrl());
    testTable = generateUniqueName();
    String ddl = "CREATE TABLE " + testTable + " (id INTEGER PRIMARY KEY, data VARCHAR)";
    conn.createStatement().execute(ddl);

    PreparedStatement ps =
      conn.prepareStatement("UPSERT INTO " + testTable + " (id, data) VALUES (1, ?)");
    ps.setString(1, helloPhoenixString);
    ps.execute();
    conn.commit();
    conn.close();
  }

  @Test
  public void testDecodeHBaseFromHexLiteral() throws Exception {
    Connection conn = DriverManager.getConnection(getUrl());

    ResultSet rs = conn.createStatement().executeQuery(
      "SELECT DECODE_BINARY('\\x48\\x65\\x6C\\x6C\\x6F\\x50\\x68\\x6F\\x65\\x6E\\x69\\x78', "
        + "'HBASE') FROM " + testTable);
    assertTrue(rs.next());
    byte[] actualBytes = rs.getBytes(1);
    assertTrue(Arrays.equals(helloPhoenixBytes, actualBytes));
  }

  @Test
  public void testNullAndEmptyStringDecoding() throws Exception {
    Connection conn = DriverManager.getConnection(getUrl());

    ResultSet rs = conn.createStatement()
      .executeQuery("SELECT DECODE_BINARY(data, 'HBASE') FROM " + testTable + " WHERE ID=-10");
    assertFalse(rs.next());

    PreparedStatement ps =
      conn.prepareStatement("UPSERT INTO " + testTable + " (id, data) VALUES (6, ?)");
    ps.setString(1, StringUtils.EMPTY);
    ps.execute();
    conn.commit();

    ResultSet rs2 = conn.createStatement()
      .executeQuery("SELECT DECODE_BINARY(data, 'HEX') FROM " + testTable + " WHERE ID=6");
    assertTrue(rs2.next());
    assertEquals(null, rs2.getString(1));
  }

  @Test
  public void testLongBase64Decoding() throws Exception {
    Connection conn = DriverManager.getConnection(getUrl());

    StringBuilder base64String = new StringBuilder();
    for (int i = 0; i < 20; i++) {
      base64String.append(base64Chunk);
    }

    PreparedStatement ps =
      conn.prepareStatement("UPSERT INTO " + testTable + " (id, data) VALUES (3, ?)");
    ps.setString(1, base64String.toString());
    ps.execute();
    conn.commit();

    ResultSet rs = conn.createStatement()
      .executeQuery("SELECT DECODE_BINARY(data, 'BASE64') FROM " + testTable + " WHERE ID=3");
    assertTrue(rs.next());

    StringBuilder expectedString = new StringBuilder();
    for (int i = 0; i < 20; i++) {
      expectedString.append(helloPhoenixString);
    }

    byte[] actualBytes = rs.getBytes(1);
    assertTrue(Arrays.equals(expectedString.toString().getBytes(), actualBytes));
    assertFalse(rs.next());
  }

  @Test
  public void testDecodeBase64WithPadding() throws Exception {
    Connection conn = DriverManager.getConnection(getUrl());

    PreparedStatement ps =
      conn.prepareStatement("UPSERT INTO " + testTable + " (id, data) VALUES (7, ?)");
    ps.setString(1, "SQ==");
    ps.execute();
    conn.commit();

    ResultSet rs = conn.createStatement()
      .executeQuery("SELECT DECODE_BINARY(data, 'BASE64') FROM " + testTable + " WHERE ID=7");
    assertTrue(rs.next());

    byte[] actualBytes = rs.getBytes(1);
    assertTrue(Arrays.equals("I".getBytes(), actualBytes));
    assertFalse(rs.next());
  }

  @Test
  public void testDecodeEncodeRoundHex() throws Exception {
    Connection conn = DriverManager.getConnection(getUrl());

    PreparedStatement ps =
      conn.prepareStatement("UPSERT INTO " + testTable + " (id, data) VALUES (4, ?)");
    ps.setString(1, hex48String);
    ps.execute();
    conn.commit();

    ResultSet rs = conn.createStatement().executeQuery(
      "SELECT ENCODE_BINARY(DECODE_BINARY(data, 'HEX'), 'HEX') FROM " + testTable + " WHERE ID=4");
    assertTrue(rs.next());
    String actualString = rs.getString(1);
    assertEquals(hex48String, actualString);
    assertFalse(rs.next());
  }

  @Test
  public void testDecodeEncodeRoundBase64() throws Exception {
    Connection conn = DriverManager.getConnection(getUrl());

    PreparedStatement ps =
      conn.prepareStatement("UPSERT INTO " + testTable + " (id, data) VALUES (5, ?)");
    ps.setString(1, base64Chunk);
    ps.execute();
    conn.commit();

    ResultSet rs = conn.createStatement()
      .executeQuery("SELECT ENCODE_BINARY(DECODE_BINARY(data, 'BASE64'), 'BASE64') FROM "
        + testTable + " WHERE ID=5");
    assertTrue(rs.next());
    String actualString = rs.getString(1);
    assertEquals(base64Chunk, actualString);
    assertFalse(rs.next());
  }

  @Test
  public void testDecodeEncodeRoundHbase() throws Exception {
    Connection conn = DriverManager.getConnection(getUrl());

    ResultSet rs = conn.createStatement()
      .executeQuery("SELECT ENCODE_BINARY(DECODE_BINARY(data, 'HBASE'), 'HBASE') FROM " + testTable
        + " WHERE ID=1");
    assertTrue(rs.next());
    String actualString = rs.getString(1);
    assertEquals(helloPhoenixString, actualString);
    assertFalse(rs.next());
  }
}
