/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to you under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
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

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.Collection;
import java.util.Properties;

import org.junit.Assert;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import org.apache.hadoop.hbase.util.Bytes;
import org.apache.phoenix.compile.ExplainPlan;
import org.apache.phoenix.compile.ExplainPlanAttributes;
import org.apache.phoenix.jdbc.PhoenixPreparedStatement;
import org.apache.phoenix.util.PropertiesUtil;

import static org.apache.phoenix.util.TestUtil.TEST_PROPERTIES;

@Category(ParallelStatsDisabledTest.class)
@RunWith(Parameterized.class)
public class VarBinaryEncoded2IT extends ParallelStatsDisabledIT {

  private final boolean columnEncoded;
  private final boolean coveredIndex;

  public VarBinaryEncoded2IT(boolean columnEncoded, boolean coveredIndex) {
    this.columnEncoded = columnEncoded;
    this.coveredIndex = coveredIndex;
  }

  @Parameterized.Parameters(name =
      "VarBinary2IT_columnEncoded={0}, coveredIndex={1}")
  public static synchronized Collection<Object[]> data() {
    return Arrays.asList(
        new Object[][] {
            {false, false},
            {false, true},
            {true, false},
            {true, true}
        });
  }

  @Test
  public void testVarBinaryPkSchema1() throws Exception {
    Properties props = PropertiesUtil.deepCopy(TEST_PROPERTIES);
    final String tableName = generateUniqueName();
    final String indexName = generateUniqueName();
    try (Connection conn = DriverManager.getConnection(getUrl(), props)) {
      conn.createStatement().execute("CREATE TABLE " + tableName
          + " (PK1 VARBINARY_ENCODED, PK2 VARBINARY_ENCODED, PK3 VARBINARY_ENCODED,"
          + " COL1 VARCHAR, COL2 VARBINARY_ENCODED,"
          + " COL3 VARBINARY_ENCODED CONSTRAINT pk PRIMARY KEY(PK1, PK2, PK3)) "
          + (this.columnEncoded ? "" : "COLUMN_ENCODED_BYTES=0"));

      if (this.coveredIndex) {
        conn.createStatement().execute(
            "CREATE INDEX " + indexName + " ON " + tableName + " (COL1, COL2) INCLUDE (COL3)");
      } else {
        conn.createStatement().execute("CREATE UNCOVERED INDEX " + indexName + " ON " + tableName
            + " (COL1, COL2)");
      }

      byte[] b1 = new byte[] {1, 1, 19, -28, 24, 1, 1, -11, -21, 1};
      byte[] b2 = new byte[] {57, -83, 2, 83, -7, 12, -13, 4};
      byte[] b3 = new byte[] {4, 34, -19, 8, -73, 3, 4, 23};
      String b4 = "TnM5+UZ#J#GV20fn45#_$593+12*yT0Vd%Y+Q4FaVScnmQP3+SfTPt1OeWp4K+N&PB";
      byte[] b5 = new byte[] {10, 55, 19, -5, -34, 0, 0, 0, 0, 1};
      byte[] b6 = new byte[] {-11, 55, -119, 8, 0, 1, 2, -4, 33};

      byte[] b10 = new byte[] {1, 1, 19, -28, 25, -1, 1, -11, -21, -1};
      byte[] b20 = new byte[] {57, -83, -2, 83, 0, -7, -12, -13, 4};
      byte[] b30 = new byte[] {4, 1, -19, 8, 0, -73, 3, 4, 23};
      String b40 = "TnM5+UZ#J#GV20fn45#_$593+12*yT0Vd%Y+Q4FaVScnmQP3+SfTPt1OeWp4K+N&PB";
      byte[] b50 = new byte[] {10, 55, 19, -5, -34, 0, 0, 0, 0, 1};
      byte[] b60 = new byte[] {-11, 55, -119, 8, 0, 1, 2, -4, 33};

      byte[] b11 = new byte[] {1, 1, 20, -28, 0, -1, 0, -11, -21, -1};
      byte[] b21 = new byte[] {57, -83, 0, -2, 0, -7, -12, -13, 4};
      byte[] b31 = new byte[] {4, 1, 0, 0, 0, 73, 3, 0, 23};
      String b41 = "TnM5+UZ#J#GV20fn45#_$593+12*yT0Vd%Y+Q4FaVScnmQP3+SfTPt1OeWp4K+N&PB";
      byte[] b51 = new byte[] {10, 55, 0, 19, -5, -34, 0, 0, 0, 0, 1};
      byte[] b61 = new byte[] {-11, 55, -119, 0, 8, 0, 1, 2, -4, 33};

      byte[] b12 = new byte[] {1, 1, 20, -28, 0, -1, 0, -11, -21, -1};
      byte[] b22 = new byte[] {57, -83, 0, -2, 0, -7, -12, -13, 4, 0};
      byte[] b32 = new byte[] {4, 1, 75, 0, 0, 73, 0, -24, 3, 0, 12, 99, 23};
      String b42 = "tT3GZmtUkcmt@GgqOB3S9ju4yyc1BSN@e9RvVUcG&tuJh3Qn=K";
      byte[] b52 = new byte[] {10, 55, 0, 19, -5, -34, 0, 0, 0, 0, 1};
      byte[] b62 = new byte[] {-11, 55, -119, 0, 8, 0, 1, 2, -4, 33};

      byte[] b13 = new byte[] {1, 1, 20, -28, 0, -1, 0, -11, -21, -1};
      byte[] b23 = new byte[] {57, -83, 0, -2, 0, -7, -12, -13, 4};
      byte[] b33 = new byte[] {4, 1, 0, 0, 0, 0, 22, 122, 48, -121, 73, 3, 0, 23};
      String b43 = "tT3GZmtUkcmut@GgqOB3S9ju4yyc1BSN@e9RvVUcG&tuJh3Qn=K";
      byte[] b53 = new byte[] {10, 55, 0, 19, -5, -34, 0, -12, 0, 0, 0, 1};
      byte[] b63 = new byte[] {-11, 55, -119, 0, 8, 0, 1, 2, -4, 33};

      byte[] b14 = new byte[] {1, 1, 20, -28, 0, -1, 0, -11, -21, -1};
      byte[] b24 = null;
      byte[] b34 = new byte[] {5, 1, 0, 0, 0, 0, 22, 122, 48, -121, 73, 3, 0, 23};
      String b44 = "tT3GZmtUkcmut@GgqOB3S9ju4yyc1BSN@e9RvVUcG&tuJh3Qn=K";
      byte[] b54 = new byte[] {10, 55, 0, 19, -5, -34, 0, -12, 0, 0, 0, 1};
      byte[] b64 = new byte[] {-11, 55, -119, 0, 8, 0, 1, 2, -4, 33};

      byte[] b15 = new byte[] {1, 1, 20, -28, 0, -1, 0, -11, -21, -1};
      byte[] b25 = new byte[] {57, -83, 0, -2, 0, -7, -12, -13, 4};
      byte[] b35 = null;
      String b45 = "tT3GZmtUkcmut@GgqOB3S9ju4yyc1BSN@e9RvVUcG&tuJh3Qn=K";
      byte[] b55 = new byte[] {10, 55, 0, 19, -5, -34, 0, -12, 0, 0, 0, 1};
      byte[] b65 = new byte[] {-11, 55, -119, 0, 8, 0, 1, 2, -4, 33};

      byte[] b16 = new byte[] {1, 1, 19, -28, 25, -1, 1, -11, -21, -1};
      byte[] b26 = new byte[] {57, -83, -2, 83, 0, -7, -12, -13, 4};
      byte[] b36 = new byte[] {4, 1, -19, 8, 0, -73, 3, 4, 23, 0};
      String b46 = "TnM5+UZ#J#GV20fn45#_$593+12*yT0Vd%Y+Q4FaVScnmQP3+SfTPt1OeWp4K+N&PB";
      byte[] b56 = null;
      byte[] b66 = new byte[] {-11, 55, -119, 8, 0, 1, 2, -4, 33};

      try (PreparedStatement preparedStatement = conn.prepareStatement("UPSERT INTO " + tableName
          + "(PK1, PK2, PK3, COL1, COL2, COL3) VALUES (?, ?, ?, ?, ?, ?)")) {
        upsertRow(preparedStatement, Bytes.toBytes("pk1-ehgir4jf"), Bytes.toBytes("pk22p0jfdkhrgi"),
            Bytes.toBytes("pk33ogjirhhf"), "col19fnbb0hf0t@4687755*^^%6546",
            Bytes.toBytes("col21048rnbfpe3-"), Bytes.toBytes("col319efnrugifj"));
        upsertRow(preparedStatement, b10, b20, b30, b40, b50, b60);
        upsertRow(preparedStatement, b1, b2, b3, b4, b5, b6);
        upsertRow(preparedStatement, b11, b21, b31, b41, b51, b61);
        upsertRow(preparedStatement, b12, b22, b32, b42, b52, b62);
        upsertRow(preparedStatement, b13, b23, b33, b43, b53, b63);
        upsertRow(preparedStatement, b14, b24, b34, b44, b54, b64);
        upsertRow(preparedStatement, b15, b25, b35, b45, b55, b65);
        upsertRow(preparedStatement, b16, b26, b36, b46, b56, b66);
      }
      conn.commit();

      PreparedStatement preparedStatement =
          conn.prepareStatement("SELECT * FROM " + tableName + " WHERE COL1 = ? AND COL2 = ?");

      preparedStatement.setString(1, b4);
      preparedStatement.setBytes(2, b5);

      assertIndexUsed(preparedStatement, indexName, "RANGE SCAN ");

      ResultSet resultSet = preparedStatement.executeQuery();

      Assert.assertTrue(resultSet.next());

      Assert.assertArrayEquals(b1, resultSet.getBytes(1));
      Assert.assertArrayEquals(b2, resultSet.getBytes(2));
      Assert.assertArrayEquals(b3, resultSet.getBytes(3));
      Assert.assertEquals(b4, resultSet.getString(4));
      Assert.assertArrayEquals(b5, resultSet.getBytes(5));
      Assert.assertArrayEquals(b6, resultSet.getBytes(6));

      Assert.assertTrue(resultSet.next());

      Assert.assertArrayEquals(b10, resultSet.getBytes(1));
      Assert.assertArrayEquals(b20, resultSet.getBytes(2));
      Assert.assertArrayEquals(b30, resultSet.getBytes(3));
      Assert.assertEquals(b40, resultSet.getString(4));
      Assert.assertArrayEquals(b50, resultSet.getBytes(5));
      Assert.assertArrayEquals(b60, resultSet.getBytes(6));

      Assert.assertFalse(resultSet.next());

      preparedStatement =
          conn.prepareStatement("SELECT * FROM " + tableName + " WHERE COL1 = ?");

      preparedStatement.setString(1, b4);

      assertIndexUsed(preparedStatement, indexName, "RANGE SCAN ");

      resultSet = preparedStatement.executeQuery();

      Assert.assertTrue(resultSet.next());

      Assert.assertArrayEquals(b16, resultSet.getBytes(1));
      Assert.assertArrayEquals(b26, resultSet.getBytes(2));
      Assert.assertArrayEquals(b36, resultSet.getBytes(3));
      Assert.assertEquals(b46, resultSet.getString(4));
      Assert.assertNull(resultSet.getBytes(5));
      Assert.assertArrayEquals(b66, resultSet.getBytes(6));

      Assert.assertTrue(resultSet.next());

      Assert.assertArrayEquals(b11, resultSet.getBytes(1));
      Assert.assertArrayEquals(b21, resultSet.getBytes(2));
      Assert.assertArrayEquals(b31, resultSet.getBytes(3));
      Assert.assertEquals(b41, resultSet.getString(4));
      Assert.assertArrayEquals(b51, resultSet.getBytes(5));
      Assert.assertArrayEquals(b61, resultSet.getBytes(6));

      Assert.assertTrue(resultSet.next());

      Assert.assertArrayEquals(b1, resultSet.getBytes(1));
      Assert.assertArrayEquals(b2, resultSet.getBytes(2));
      Assert.assertArrayEquals(b3, resultSet.getBytes(3));
      Assert.assertEquals(b4, resultSet.getString(4));
      Assert.assertArrayEquals(b5, resultSet.getBytes(5));
      Assert.assertArrayEquals(b6, resultSet.getBytes(6));

      Assert.assertTrue(resultSet.next());

      Assert.assertArrayEquals(b10, resultSet.getBytes(1));
      Assert.assertArrayEquals(b20, resultSet.getBytes(2));
      Assert.assertArrayEquals(b30, resultSet.getBytes(3));
      Assert.assertEquals(b40, resultSet.getString(4));
      Assert.assertArrayEquals(b50, resultSet.getBytes(5));
      Assert.assertArrayEquals(b60, resultSet.getBytes(6));

      Assert.assertFalse(resultSet.next());

      preparedStatement = conn.prepareStatement(
          "SELECT * FROM " + tableName + " WHERE COL1 = ? AND COL2 IS NOT NULL");

      preparedStatement.setString(1, b4);

      assertIndexUsed(preparedStatement, indexName, "RANGE SCAN ");

      resultSet = preparedStatement.executeQuery();

      Assert.assertTrue(resultSet.next());

      Assert.assertArrayEquals(b11, resultSet.getBytes(1));
      Assert.assertArrayEquals(b21, resultSet.getBytes(2));
      Assert.assertArrayEquals(b31, resultSet.getBytes(3));
      Assert.assertEquals(b41, resultSet.getString(4));
      Assert.assertArrayEquals(b51, resultSet.getBytes(5));
      Assert.assertArrayEquals(b61, resultSet.getBytes(6));

      Assert.assertTrue(resultSet.next());

      Assert.assertArrayEquals(b1, resultSet.getBytes(1));
      Assert.assertArrayEquals(b2, resultSet.getBytes(2));
      Assert.assertArrayEquals(b3, resultSet.getBytes(3));
      Assert.assertEquals(b4, resultSet.getString(4));
      Assert.assertArrayEquals(b5, resultSet.getBytes(5));
      Assert.assertArrayEquals(b6, resultSet.getBytes(6));

      Assert.assertTrue(resultSet.next());

      Assert.assertArrayEquals(b10, resultSet.getBytes(1));
      Assert.assertArrayEquals(b20, resultSet.getBytes(2));
      Assert.assertArrayEquals(b30, resultSet.getBytes(3));
      Assert.assertEquals(b40, resultSet.getString(4));
      Assert.assertArrayEquals(b50, resultSet.getBytes(5));
      Assert.assertArrayEquals(b60, resultSet.getBytes(6));

      Assert.assertFalse(resultSet.next());

      preparedStatement = conn.prepareStatement("SELECT * FROM " + tableName
          + " WHERE COL1 = ? AND COL2 BETWEEN ? AND ?");
      preparedStatement.setString(1, b4);
      preparedStatement.setBytes(2, b51);
      preparedStatement.setBytes(3, b5);

      assertIndexUsed(preparedStatement, indexName, "RANGE SCAN ");

      resultSet = preparedStatement.executeQuery();

      Assert.assertTrue(resultSet.next());

      Assert.assertArrayEquals(b11, resultSet.getBytes(1));
      Assert.assertArrayEquals(b21, resultSet.getBytes(2));
      Assert.assertArrayEquals(b31, resultSet.getBytes(3));
      Assert.assertEquals(b41, resultSet.getString(4));
      Assert.assertArrayEquals(b51, resultSet.getBytes(5));
      Assert.assertArrayEquals(b61, resultSet.getBytes(6));

      Assert.assertTrue(resultSet.next());

      Assert.assertArrayEquals(b1, resultSet.getBytes(1));
      Assert.assertArrayEquals(b2, resultSet.getBytes(2));
      Assert.assertArrayEquals(b3, resultSet.getBytes(3));
      Assert.assertEquals(b4, resultSet.getString(4));
      Assert.assertArrayEquals(b5, resultSet.getBytes(5));
      Assert.assertArrayEquals(b6, resultSet.getBytes(6));

      Assert.assertTrue(resultSet.next());

      Assert.assertArrayEquals(b10, resultSet.getBytes(1));
      Assert.assertArrayEquals(b20, resultSet.getBytes(2));
      Assert.assertArrayEquals(b30, resultSet.getBytes(3));
      Assert.assertEquals(b40, resultSet.getString(4));
      Assert.assertArrayEquals(b50, resultSet.getBytes(5));
      Assert.assertArrayEquals(b60, resultSet.getBytes(6));

      Assert.assertFalse(resultSet.next());

      preparedStatement = conn.prepareStatement("SELECT * FROM " + tableName
          + " WHERE COL1 = ? AND COL2 IN (?, ?)");
      preparedStatement.setString(1, b4);
      preparedStatement.setBytes(2, b51);
      preparedStatement.setBytes(3, b5);

      assertIndexUsed(preparedStatement, indexName, "SKIP SCAN ON 2 KEYS ");

      resultSet = preparedStatement.executeQuery();

      Assert.assertTrue(resultSet.next());

      Assert.assertArrayEquals(b11, resultSet.getBytes(1));
      Assert.assertArrayEquals(b21, resultSet.getBytes(2));
      Assert.assertArrayEquals(b31, resultSet.getBytes(3));
      Assert.assertEquals(b41, resultSet.getString(4));
      Assert.assertArrayEquals(b51, resultSet.getBytes(5));
      Assert.assertArrayEquals(b61, resultSet.getBytes(6));

      Assert.assertTrue(resultSet.next());

      Assert.assertArrayEquals(b1, resultSet.getBytes(1));
      Assert.assertArrayEquals(b2, resultSet.getBytes(2));
      Assert.assertArrayEquals(b3, resultSet.getBytes(3));
      Assert.assertEquals(b4, resultSet.getString(4));
      Assert.assertArrayEquals(b5, resultSet.getBytes(5));
      Assert.assertArrayEquals(b6, resultSet.getBytes(6));

      Assert.assertTrue(resultSet.next());

      Assert.assertArrayEquals(b10, resultSet.getBytes(1));
      Assert.assertArrayEquals(b20, resultSet.getBytes(2));
      Assert.assertArrayEquals(b30, resultSet.getBytes(3));
      Assert.assertEquals(b40, resultSet.getString(4));
      Assert.assertArrayEquals(b50, resultSet.getBytes(5));
      Assert.assertArrayEquals(b60, resultSet.getBytes(6));

      Assert.assertFalse(resultSet.next());
    }
  }

  @Test
  public void testVarBinaryPkSchema2() throws Exception {
    Properties props = PropertiesUtil.deepCopy(TEST_PROPERTIES);
    final String tableName = generateUniqueName();
    final String indexName = generateUniqueName();
    try (Connection conn = DriverManager.getConnection(getUrl(), props)) {
      conn.createStatement().execute("CREATE TABLE " + tableName
          + " (PK1 VARBINARY_ENCODED, PK2 VARBINARY_ENCODED, PK3 VARBINARY_ENCODED,"
          + " COL1 VARBINARY_ENCODED, COL2 VARBINARY_ENCODED,"
          + " COL3 VARBINARY_ENCODED CONSTRAINT pk PRIMARY KEY(PK1, PK2, PK3)) "
          + (this.columnEncoded ? "" : "COLUMN_ENCODED_BYTES=0"));

      if (this.coveredIndex) {
        conn.createStatement().execute(
            "CREATE INDEX " + indexName + " ON " + tableName + " (COL1, COL2) INCLUDE (COL3)");
      } else {
        conn.createStatement().execute("CREATE UNCOVERED INDEX " + indexName + " ON " + tableName
            + " (COL1, COL2)");
      }

      byte[] b1 = new byte[] {1, 1, 19, -28, 24, 1, 1, -11, -21, 1};
      byte[] b2 = new byte[] {57, -83, 2, 83, -7, 12, -13, 4};
      byte[] b3 = new byte[] {4, 34, -19, 8, -73, 3, 4, 23};
      byte[] b4 = new byte[] {56, 50, 19, 34, -101, 0, 0, 1, 1, 0, -1, -1, -2, -23, 83, -102, 91};
      byte[] b5 = new byte[] {10, 55, 19, -5, -34, 0, 0, 0, 0, 1};
      byte[] b6 = new byte[] {-11, 55, -119, 8, 0, 1, 2, -4, 33};

      byte[] b10 = new byte[] {1, 1, 19, -28, 25, -1, 1, -11, -21, -1};
      byte[] b20 = new byte[] {57, -83, -2, 83, 0, -7, -12, -13, 4};
      byte[] b30 = new byte[] {4, 1, -19, 8, 0, -73, 3, 4, 23};
      byte[] b40 = new byte[] {56, 50, 19, 34, -101, 0, 0, 1, 1, 0, -1, -1, -2, -23, 83, -102, 91};
      byte[] b50 = new byte[] {10, 55, 19, -5, -34, 0, 0, 0, 0, 1};
      byte[] b60 = new byte[] {-11, 55, -119, 8, 0, 1, 2, -4, 33};

      byte[] b11 = new byte[] {1, 1, 20, -28, 0, -1, 0, -11, -21, -1};
      byte[] b21 = new byte[] {57, -83, 0, -2, 0, -7, -12, -13, 4};
      byte[] b31 = new byte[] {4, 1, 0, 0, 0, 73, 3, 0, 23};
      byte[] b41 = new byte[] {56, 50, 19, 34, -101, 0, 0, 1, 1, 0, -1, -1, -2, -23, 83, -102, 91};
      byte[] b51 = new byte[] {10, 55, 0, 19, -5, -34, 0, 0, 0, 0, 1};
      byte[] b61 = new byte[] {-11, 55, -119, 0, 8, 0, 1, 2, -4, 33};

      byte[] b12 = new byte[] {1, 1, 20, -28, 0, -1, 0, -11, -21, -1};
      byte[] b22 = new byte[] {57, -83, 0, -2, 0, -7, -12, -13, 4, 0};
      byte[] b32 = new byte[] {4, 1, 75, 0, 0, 73, 0, -24, 3, 0, 12, 99, 23};
      byte[] b42 = new byte[] {56, 50, 19, 0, 34, 83, -101, -102, 91};
      byte[] b52 = new byte[] {10, 55, 0, 19, -5, -34, 0, 0, 0, 0, 1};
      byte[] b62 = new byte[] {-11, 55, -119, 0, 8, 0, 1, 2, -4, 33};

      byte[] b13 = new byte[] {1, 1, 20, -28, 0, -1, 0, -11, -21, -1};
      byte[] b23 = new byte[] {57, -83, 0, -2, 0, -7, -12, -13, 4};
      byte[] b33 = new byte[] {4, 1, 0, 0, 0, 0, 22, 122, 48, -121, 73, 3, 0, 23};
      byte[] b43 = new byte[] {56, 50, 19, 0, 34, 83, -101, -102, 91, 92};
      byte[] b53 = new byte[] {10, 55, 0, 19, -5, -34, 0, -12, 0, 0, 0, 1};
      byte[] b63 = new byte[] {-11, 55, -119, 0, 8, 0, 1, 2, -4, 33};

      byte[] b14 = new byte[] {1, 1, 20, -28, 0, -1, 0, -11, -21, -1};
      byte[] b24 = null;
      byte[] b34 = new byte[] {5, 1, 0, 0, 0, 0, 22, 122, 48, -121, 73, 3, 0, 23};
      byte[] b44 = new byte[] {56, 50, 19, 0, 34, 83, -101, -102, 91, 92};
      byte[] b54 = new byte[] {10, 55, 0, 19, -5, -34, 0, -12, 0, 0, 0, 1};
      byte[] b64 = new byte[] {-11, 55, -119, 0, 8, 0, 1, 2, -4, 33};

      byte[] b15 = new byte[] {1, 1, 20, -28, 0, -1, 0, -11, -21, -1};
      byte[] b25 = new byte[] {57, -83, 0, -2, 0, -7, -12, -13, 4};
      byte[] b35 = null;
      byte[] b45 = new byte[] {56, 50, 19, 0, 34, 83, -101, -102, 91, 92};
      byte[] b55 = new byte[] {10, 55, 0, 19, -5, -34, 0, -12, 0, 0, 0, 1};
      byte[] b65 = new byte[] {-11, 55, -119, 0, 8, 0, 1, 2, -4, 33};

      byte[] b16 = new byte[] {1, 1, 19, -28, 25, -1, 1, -11, -21, -1};
      byte[] b26 = new byte[] {57, -83, -2, 83, 0, -7, -12, -13, 4};
      byte[] b36 = new byte[] {4, 1, -19, 8, 0, -73, 3, 4, 23, 0};
      byte[] b46 = new byte[] {56, 50, 19, 34, -101, 0, 0, 1, 1, 0, -1, -1, -2, -23, 83, -102, 91};
      byte[] b56 = null;
      byte[] b66 = new byte[] {-11, 55, -119, 8, 0, 1, 2, -4, 33};

      try (PreparedStatement preparedStatement = conn.prepareStatement("UPSERT INTO " + tableName
          + "(PK1, PK2, PK3, COL1, COL2, COL3) VALUES (?, ?, ?, ?, ?, ?)")) {
        upsertRow(preparedStatement, Bytes.toBytes("pk1-ehgir4jf"), Bytes.toBytes("pk22p0jfdkhrgi"),
            Bytes.toBytes("pk33ogjirhhf"), Bytes.toBytes("col19fnbb0hf0t"),
            Bytes.toBytes("col21048rnbfpe3-"), Bytes.toBytes("col319efnrugifj"));
        upsertRow(preparedStatement, b10, b20, b30, b40, b50, b60);
        upsertRow(preparedStatement, b1, b2, b3, b4, b5, b6);
        upsertRow(preparedStatement, b11, b21, b31, b41, b51, b61);
        upsertRow(preparedStatement, b12, b22, b32, b42, b52, b62);
        upsertRow(preparedStatement, b13, b23, b33, b43, b53, b63);
        upsertRow(preparedStatement, b14, b24, b34, b44, b54, b64);
        upsertRow(preparedStatement, b15, b25, b35, b45, b55, b65);
        upsertRow(preparedStatement, b16, b26, b36, b46, b56, b66);
      }
      conn.commit();

      PreparedStatement preparedStatement =
          conn.prepareStatement("SELECT * FROM " + tableName + " WHERE COL1 = ? AND COL2 = ?");

      preparedStatement.setBytes(1, b4);
      preparedStatement.setBytes(2, b5);

      assertIndexUsed(preparedStatement, indexName, "RANGE SCAN ");

      ResultSet resultSet = preparedStatement.executeQuery();

      Assert.assertTrue(resultSet.next());

      Assert.assertArrayEquals(b1, resultSet.getBytes(1));
      Assert.assertArrayEquals(b2, resultSet.getBytes(2));
      Assert.assertArrayEquals(b3, resultSet.getBytes(3));
      Assert.assertArrayEquals(b4, resultSet.getBytes(4));
      Assert.assertArrayEquals(b5, resultSet.getBytes(5));
      Assert.assertArrayEquals(b6, resultSet.getBytes(6));

      Assert.assertTrue(resultSet.next());

      Assert.assertArrayEquals(b10, resultSet.getBytes(1));
      Assert.assertArrayEquals(b20, resultSet.getBytes(2));
      Assert.assertArrayEquals(b30, resultSet.getBytes(3));
      Assert.assertArrayEquals(b40, resultSet.getBytes(4));
      Assert.assertArrayEquals(b50, resultSet.getBytes(5));
      Assert.assertArrayEquals(b60, resultSet.getBytes(6));

      Assert.assertFalse(resultSet.next());

      preparedStatement =
          conn.prepareStatement("SELECT * FROM " + tableName + " WHERE COL1 = ?");

      preparedStatement.setBytes(1, b4);

      assertIndexUsed(preparedStatement, indexName, "RANGE SCAN ");

      resultSet = preparedStatement.executeQuery();

      Assert.assertTrue(resultSet.next());

      Assert.assertArrayEquals(b16, resultSet.getBytes(1));
      Assert.assertArrayEquals(b26, resultSet.getBytes(2));
      Assert.assertArrayEquals(b36, resultSet.getBytes(3));
      Assert.assertArrayEquals(b46, resultSet.getBytes(4));
      Assert.assertNull(resultSet.getBytes(5));
      Assert.assertArrayEquals(b66, resultSet.getBytes(6));

      Assert.assertTrue(resultSet.next());

      Assert.assertArrayEquals(b11, resultSet.getBytes(1));
      Assert.assertArrayEquals(b21, resultSet.getBytes(2));
      Assert.assertArrayEquals(b31, resultSet.getBytes(3));
      Assert.assertArrayEquals(b41, resultSet.getBytes(4));
      Assert.assertArrayEquals(b51, resultSet.getBytes(5));
      Assert.assertArrayEquals(b61, resultSet.getBytes(6));

      Assert.assertTrue(resultSet.next());

      Assert.assertArrayEquals(b1, resultSet.getBytes(1));
      Assert.assertArrayEquals(b2, resultSet.getBytes(2));
      Assert.assertArrayEquals(b3, resultSet.getBytes(3));
      Assert.assertArrayEquals(b4, resultSet.getBytes(4));
      Assert.assertArrayEquals(b5, resultSet.getBytes(5));
      Assert.assertArrayEquals(b6, resultSet.getBytes(6));

      Assert.assertTrue(resultSet.next());

      Assert.assertArrayEquals(b10, resultSet.getBytes(1));
      Assert.assertArrayEquals(b20, resultSet.getBytes(2));
      Assert.assertArrayEquals(b30, resultSet.getBytes(3));
      Assert.assertArrayEquals(b40, resultSet.getBytes(4));
      Assert.assertArrayEquals(b50, resultSet.getBytes(5));
      Assert.assertArrayEquals(b60, resultSet.getBytes(6));

      Assert.assertFalse(resultSet.next());

      preparedStatement = conn.prepareStatement(
          "SELECT * FROM " + tableName + " WHERE COL1 = ? AND COL2 IS NOT NULL");

      preparedStatement.setBytes(1, b4);

      assertIndexUsed(preparedStatement, indexName, "RANGE SCAN ");

      resultSet = preparedStatement.executeQuery();

      Assert.assertTrue(resultSet.next());

      Assert.assertArrayEquals(b11, resultSet.getBytes(1));
      Assert.assertArrayEquals(b21, resultSet.getBytes(2));
      Assert.assertArrayEquals(b31, resultSet.getBytes(3));
      Assert.assertArrayEquals(b41, resultSet.getBytes(4));
      Assert.assertArrayEquals(b51, resultSet.getBytes(5));
      Assert.assertArrayEquals(b61, resultSet.getBytes(6));

      Assert.assertTrue(resultSet.next());

      Assert.assertArrayEquals(b1, resultSet.getBytes(1));
      Assert.assertArrayEquals(b2, resultSet.getBytes(2));
      Assert.assertArrayEquals(b3, resultSet.getBytes(3));
      Assert.assertArrayEquals(b4, resultSet.getBytes(4));
      Assert.assertArrayEquals(b5, resultSet.getBytes(5));
      Assert.assertArrayEquals(b6, resultSet.getBytes(6));

      Assert.assertTrue(resultSet.next());

      Assert.assertArrayEquals(b10, resultSet.getBytes(1));
      Assert.assertArrayEquals(b20, resultSet.getBytes(2));
      Assert.assertArrayEquals(b30, resultSet.getBytes(3));
      Assert.assertArrayEquals(b40, resultSet.getBytes(4));
      Assert.assertArrayEquals(b50, resultSet.getBytes(5));
      Assert.assertArrayEquals(b60, resultSet.getBytes(6));

      Assert.assertFalse(resultSet.next());

      preparedStatement = conn.prepareStatement("SELECT * FROM " + tableName
          + " WHERE COL1 = ? AND COL2 BETWEEN ? AND ?");
      preparedStatement.setBytes(1, b4);
      preparedStatement.setBytes(2, b51);
      preparedStatement.setBytes(3, b5);

      assertIndexUsed(preparedStatement, indexName, "RANGE SCAN ");

      resultSet = preparedStatement.executeQuery();

      Assert.assertTrue(resultSet.next());

      Assert.assertArrayEquals(b11, resultSet.getBytes(1));
      Assert.assertArrayEquals(b21, resultSet.getBytes(2));
      Assert.assertArrayEquals(b31, resultSet.getBytes(3));
      Assert.assertArrayEquals(b41, resultSet.getBytes(4));
      Assert.assertArrayEquals(b51, resultSet.getBytes(5));
      Assert.assertArrayEquals(b61, resultSet.getBytes(6));

      Assert.assertTrue(resultSet.next());

      Assert.assertArrayEquals(b1, resultSet.getBytes(1));
      Assert.assertArrayEquals(b2, resultSet.getBytes(2));
      Assert.assertArrayEquals(b3, resultSet.getBytes(3));
      Assert.assertArrayEquals(b4, resultSet.getBytes(4));
      Assert.assertArrayEquals(b5, resultSet.getBytes(5));
      Assert.assertArrayEquals(b6, resultSet.getBytes(6));

      Assert.assertTrue(resultSet.next());

      Assert.assertArrayEquals(b10, resultSet.getBytes(1));
      Assert.assertArrayEquals(b20, resultSet.getBytes(2));
      Assert.assertArrayEquals(b30, resultSet.getBytes(3));
      Assert.assertArrayEquals(b40, resultSet.getBytes(4));
      Assert.assertArrayEquals(b50, resultSet.getBytes(5));
      Assert.assertArrayEquals(b60, resultSet.getBytes(6));

      Assert.assertFalse(resultSet.next());

      preparedStatement = conn.prepareStatement("SELECT * FROM " + tableName
          + " WHERE COL1 = ? AND COL2 IN (?, ?)");
      preparedStatement.setBytes(1, b4);
      preparedStatement.setBytes(2, b51);
      preparedStatement.setBytes(3, b5);

      assertIndexUsed(preparedStatement, indexName, "SKIP SCAN ON 2 KEYS ");

      resultSet = preparedStatement.executeQuery();

      Assert.assertTrue(resultSet.next());

      Assert.assertArrayEquals(b11, resultSet.getBytes(1));
      Assert.assertArrayEquals(b21, resultSet.getBytes(2));
      Assert.assertArrayEquals(b31, resultSet.getBytes(3));
      Assert.assertArrayEquals(b41, resultSet.getBytes(4));
      Assert.assertArrayEquals(b51, resultSet.getBytes(5));
      Assert.assertArrayEquals(b61, resultSet.getBytes(6));

      Assert.assertTrue(resultSet.next());

      Assert.assertArrayEquals(b1, resultSet.getBytes(1));
      Assert.assertArrayEquals(b2, resultSet.getBytes(2));
      Assert.assertArrayEquals(b3, resultSet.getBytes(3));
      Assert.assertArrayEquals(b4, resultSet.getBytes(4));
      Assert.assertArrayEquals(b5, resultSet.getBytes(5));
      Assert.assertArrayEquals(b6, resultSet.getBytes(6));

      Assert.assertTrue(resultSet.next());

      Assert.assertArrayEquals(b10, resultSet.getBytes(1));
      Assert.assertArrayEquals(b20, resultSet.getBytes(2));
      Assert.assertArrayEquals(b30, resultSet.getBytes(3));
      Assert.assertArrayEquals(b40, resultSet.getBytes(4));
      Assert.assertArrayEquals(b50, resultSet.getBytes(5));
      Assert.assertArrayEquals(b60, resultSet.getBytes(6));

      Assert.assertFalse(resultSet.next());
    }
  }

  @Test
  public void testVarBinaryPkSchema3() throws Exception {
    Properties props = PropertiesUtil.deepCopy(TEST_PROPERTIES);
    final String tableName = generateUniqueName();
    final String indexName = generateUniqueName();
    try (Connection conn = DriverManager.getConnection(getUrl(), props)) {
      conn.createStatement().execute("CREATE TABLE " + tableName
          + " (PK1 VARBINARY_ENCODED, PK2 VARBINARY_ENCODED, PK3 VARBINARY_ENCODED,"
          + " COL1 DOUBLE, COL2 VARBINARY_ENCODED,"
          + " COL3 VARBINARY_ENCODED CONSTRAINT pk PRIMARY KEY(PK1, PK2, PK3)) "
          + (this.columnEncoded ? "" : "COLUMN_ENCODED_BYTES=0"));

      if (this.coveredIndex) {
        conn.createStatement().execute(
            "CREATE INDEX " + indexName + " ON " + tableName + " (COL1, COL2) INCLUDE (COL3)");
      } else {
        conn.createStatement().execute("CREATE UNCOVERED INDEX " + indexName + " ON " + tableName
            + " (COL1, COL2)");
      }

      byte[] b1 = new byte[] {1, 1, 19, -28, 24, 1, 1, -11, -21, 1};
      byte[] b2 = new byte[] {57, -83, 2, 83, -7, 12, -13, 4};
      byte[] b3 = new byte[] {4, 34, -19, 8, -73, 3, 4, 23};
      double b4 = 8691478.4226644;
      byte[] b5 = new byte[] {10, 55, 19, -5, -34, 0, 0, 0, 0, 1};
      byte[] b6 = new byte[] {-11, 55, -119, 8, 0, 1, 2, -4, 33};

      byte[] b10 = new byte[] {1, 1, 19, -28, 25, -1, 1, -11, -21, -1};
      byte[] b20 = new byte[] {57, -83, -2, 83, 0, -7, -12, -13, 4};
      byte[] b30 = new byte[] {4, 1, -19, 8, 0, -73, 3, 4, 23};
      double b40 = 8691478.4226644;
      byte[] b50 = new byte[] {10, 55, 19, -5, -34, 0, 0, 0, 0, 1};
      byte[] b60 = new byte[] {-11, 55, -119, 8, 0, 1, 2, -4, 33};

      byte[] b11 = new byte[] {1, 1, 20, -28, 0, -1, 0, -11, -21, -1};
      byte[] b21 = new byte[] {57, -83, 0, -2, 0, -7, -12, -13, 4};
      byte[] b31 = new byte[] {4, 1, 0, 0, 0, 73, 3, 0, 23};
      double b41 = 8691478.4226644;
      byte[] b51 = new byte[] {10, 55, 0, 19, -5, -34, 0, 0, 0, 0, 1};
      byte[] b61 = new byte[] {-11, 55, -119, 0, 8, 0, 1, 2, -4, 33};

      byte[] b12 = new byte[] {1, 1, 20, -28, 0, -1, 0, -11, -21, -1};
      byte[] b22 = new byte[] {57, -83, 0, -2, 0, -7, -12, -13, 4, 0};
      byte[] b32 = new byte[] {4, 1, 75, 0, 0, 73, 0, -24, 3, 0, 12, 99, 23};
      double b42 = 392703.9017679;
      byte[] b52 = new byte[] {10, 55, 0, 19, -5, -34, 0, 0, 0, 0, 1};
      byte[] b62 = new byte[] {-11, 55, -119, 0, 8, 0, 1, 2, -4, 33};

      byte[] b13 = new byte[] {1, 1, 20, -28, 0, -1, 0, -11, -21, -1};
      byte[] b23 = new byte[] {57, -83, 0, -2, 0, -7, -12, -13, 4};
      byte[] b33 = new byte[] {4, 1, 0, 0, 0, 0, 22, 122, 48, -121, 73, 3, 0, 23};
      double b43 = 392703.901768;
      byte[] b53 = new byte[] {10, 55, 0, 19, -5, -34, 0, -12, 0, 0, 0, 1};
      byte[] b63 = new byte[] {-11, 55, -119, 0, 8, 0, 1, 2, -4, 33};

      byte[] b14 = new byte[] {1, 1, 20, -28, 0, -1, 0, -11, -21, -1};
      byte[] b24 = null;
      byte[] b34 = new byte[] {5, 1, 0, 0, 0, 0, 22, 122, 48, -121, 73, 3, 0, 23};
      double b44 = 392703.901768;
      byte[] b54 = new byte[] {10, 55, 0, 19, -5, -34, 0, -12, 0, 0, 0, 1};
      byte[] b64 = new byte[] {-11, 55, -119, 0, 8, 0, 1, 2, -4, 33};

      byte[] b15 = new byte[] {1, 1, 20, -28, 0, -1, 0, -11, -21, -1};
      byte[] b25 = new byte[] {57, -83, 0, -2, 0, -7, -12, -13, 4};
      byte[] b35 = null;
      double b45 = 392703.901768;
      byte[] b55 = new byte[] {10, 55, 0, 19, -5, -34, 0, -12, 0, 0, 0, 1};
      byte[] b65 = new byte[] {-11, 55, -119, 0, 8, 0, 1, 2, -4, 33};

      byte[] b16 = new byte[] {1, 1, 19, -28, 25, -1, 1, -11, -21, -1};
      byte[] b26 = new byte[] {57, -83, -2, 83, 0, -7, -12, -13, 4};
      byte[] b36 = new byte[] {4, 1, -19, 8, 0, -73, 3, 4, 23, 0};
      double b46 = 8691478.4226644;
      byte[] b56 = null;
      byte[] b66 = new byte[] {-11, 55, -119, 8, 0, 1, 2, -4, 33};

      try (PreparedStatement preparedStatement = conn.prepareStatement("UPSERT INTO " + tableName
          + "(PK1, PK2, PK3, COL1, COL2, COL3) VALUES (?, ?, ?, ?, ?, ?)")) {
        upsertRow(preparedStatement, Bytes.toBytes("pk1-ehgir4jf"), Bytes.toBytes("pk22p0jfdkhrgi"),
            Bytes.toBytes("pk33ogjirhhf"), 8791478.4226644,
            Bytes.toBytes("col21048rnbfpe3-"), Bytes.toBytes("col319efnrugifj"));
        upsertRow(preparedStatement, b10, b20, b30, b40, b50, b60);
        upsertRow(preparedStatement, b1, b2, b3, b4, b5, b6);
        upsertRow(preparedStatement, b11, b21, b31, b41, b51, b61);
        upsertRow(preparedStatement, b12, b22, b32, b42, b52, b62);
        upsertRow(preparedStatement, b13, b23, b33, b43, b53, b63);
        upsertRow(preparedStatement, b14, b24, b34, b44, b54, b64);
        upsertRow(preparedStatement, b15, b25, b35, b45, b55, b65);
        upsertRow(preparedStatement, b16, b26, b36, b46, b56, b66);
      }
      conn.commit();

      PreparedStatement preparedStatement =
          conn.prepareStatement("SELECT * FROM " + tableName + " WHERE COL1 = ? AND COL2 = ?");

      preparedStatement.setDouble(1, b4);
      preparedStatement.setBytes(2, b5);

      assertIndexUsed(preparedStatement, indexName, "RANGE SCAN ");

      ResultSet resultSet = preparedStatement.executeQuery();

      Assert.assertTrue(resultSet.next());

      Assert.assertArrayEquals(b1, resultSet.getBytes(1));
      Assert.assertArrayEquals(b2, resultSet.getBytes(2));
      Assert.assertArrayEquals(b3, resultSet.getBytes(3));
      Assert.assertEquals(b4, resultSet.getDouble(4), 0D);
      Assert.assertArrayEquals(b5, resultSet.getBytes(5));
      Assert.assertArrayEquals(b6, resultSet.getBytes(6));

      Assert.assertTrue(resultSet.next());

      Assert.assertArrayEquals(b10, resultSet.getBytes(1));
      Assert.assertArrayEquals(b20, resultSet.getBytes(2));
      Assert.assertArrayEquals(b30, resultSet.getBytes(3));
      Assert.assertEquals(b40, resultSet.getDouble(4), 0D);
      Assert.assertArrayEquals(b50, resultSet.getBytes(5));
      Assert.assertArrayEquals(b60, resultSet.getBytes(6));

      Assert.assertFalse(resultSet.next());

      preparedStatement =
          conn.prepareStatement("SELECT * FROM " + tableName + " WHERE COL1 = ?");

      preparedStatement.setDouble(1, b4);

      assertIndexUsed(preparedStatement, indexName, "RANGE SCAN ");

      resultSet = preparedStatement.executeQuery();

      Assert.assertTrue(resultSet.next());

      Assert.assertArrayEquals(b16, resultSet.getBytes(1));
      Assert.assertArrayEquals(b26, resultSet.getBytes(2));
      Assert.assertArrayEquals(b36, resultSet.getBytes(3));
      Assert.assertEquals(b46, resultSet.getDouble(4), 0D);
      Assert.assertNull(resultSet.getBytes(5));
      Assert.assertArrayEquals(b66, resultSet.getBytes(6));

      Assert.assertTrue(resultSet.next());

      Assert.assertArrayEquals(b11, resultSet.getBytes(1));
      Assert.assertArrayEquals(b21, resultSet.getBytes(2));
      Assert.assertArrayEquals(b31, resultSet.getBytes(3));
      Assert.assertEquals(b41, resultSet.getDouble(4), 0D);
      Assert.assertArrayEquals(b51, resultSet.getBytes(5));
      Assert.assertArrayEquals(b61, resultSet.getBytes(6));

      Assert.assertTrue(resultSet.next());

      Assert.assertArrayEquals(b1, resultSet.getBytes(1));
      Assert.assertArrayEquals(b2, resultSet.getBytes(2));
      Assert.assertArrayEquals(b3, resultSet.getBytes(3));
      Assert.assertEquals(b4, resultSet.getDouble(4), 0D);
      Assert.assertArrayEquals(b5, resultSet.getBytes(5));
      Assert.assertArrayEquals(b6, resultSet.getBytes(6));

      Assert.assertTrue(resultSet.next());

      Assert.assertArrayEquals(b10, resultSet.getBytes(1));
      Assert.assertArrayEquals(b20, resultSet.getBytes(2));
      Assert.assertArrayEquals(b30, resultSet.getBytes(3));
      Assert.assertEquals(b40, resultSet.getDouble(4), 0D);
      Assert.assertArrayEquals(b50, resultSet.getBytes(5));
      Assert.assertArrayEquals(b60, resultSet.getBytes(6));

      Assert.assertFalse(resultSet.next());

      preparedStatement = conn.prepareStatement(
          "SELECT * FROM " + tableName + " WHERE COL1 = ? AND COL2 IS NOT NULL");

      preparedStatement.setDouble(1, b4);

      assertIndexUsed(preparedStatement, indexName, "RANGE SCAN ");

      resultSet = preparedStatement.executeQuery();

      Assert.assertTrue(resultSet.next());

      Assert.assertArrayEquals(b11, resultSet.getBytes(1));
      Assert.assertArrayEquals(b21, resultSet.getBytes(2));
      Assert.assertArrayEquals(b31, resultSet.getBytes(3));
      Assert.assertEquals(b41, resultSet.getDouble(4), 0D);
      Assert.assertArrayEquals(b51, resultSet.getBytes(5));
      Assert.assertArrayEquals(b61, resultSet.getBytes(6));

      Assert.assertTrue(resultSet.next());

      Assert.assertArrayEquals(b1, resultSet.getBytes(1));
      Assert.assertArrayEquals(b2, resultSet.getBytes(2));
      Assert.assertArrayEquals(b3, resultSet.getBytes(3));
      Assert.assertEquals(b4, resultSet.getDouble(4), 0D);
      Assert.assertArrayEquals(b5, resultSet.getBytes(5));
      Assert.assertArrayEquals(b6, resultSet.getBytes(6));

      Assert.assertTrue(resultSet.next());

      Assert.assertArrayEquals(b10, resultSet.getBytes(1));
      Assert.assertArrayEquals(b20, resultSet.getBytes(2));
      Assert.assertArrayEquals(b30, resultSet.getBytes(3));
      Assert.assertEquals(b40, resultSet.getDouble(4), 0D);
      Assert.assertArrayEquals(b50, resultSet.getBytes(5));
      Assert.assertArrayEquals(b60, resultSet.getBytes(6));

      Assert.assertFalse(resultSet.next());

      preparedStatement = conn.prepareStatement("SELECT * FROM " + tableName
          + " WHERE COL1 = ? AND COL2 BETWEEN ? AND ?");
      preparedStatement.setDouble(1, b4);
      preparedStatement.setBytes(2, b51);
      preparedStatement.setBytes(3, b5);

      assertIndexUsed(preparedStatement, indexName, "RANGE SCAN ");

      resultSet = preparedStatement.executeQuery();

      Assert.assertTrue(resultSet.next());

      Assert.assertArrayEquals(b11, resultSet.getBytes(1));
      Assert.assertArrayEquals(b21, resultSet.getBytes(2));
      Assert.assertArrayEquals(b31, resultSet.getBytes(3));
      Assert.assertEquals(b41, resultSet.getDouble(4), 0D);
      Assert.assertArrayEquals(b51, resultSet.getBytes(5));
      Assert.assertArrayEquals(b61, resultSet.getBytes(6));

      Assert.assertTrue(resultSet.next());

      Assert.assertArrayEquals(b1, resultSet.getBytes(1));
      Assert.assertArrayEquals(b2, resultSet.getBytes(2));
      Assert.assertArrayEquals(b3, resultSet.getBytes(3));
      Assert.assertEquals(b4, resultSet.getDouble(4), 0D);
      Assert.assertArrayEquals(b5, resultSet.getBytes(5));
      Assert.assertArrayEquals(b6, resultSet.getBytes(6));

      Assert.assertTrue(resultSet.next());

      Assert.assertArrayEquals(b10, resultSet.getBytes(1));
      Assert.assertArrayEquals(b20, resultSet.getBytes(2));
      Assert.assertArrayEquals(b30, resultSet.getBytes(3));
      Assert.assertEquals(b40, resultSet.getDouble(4), 0D);
      Assert.assertArrayEquals(b50, resultSet.getBytes(5));
      Assert.assertArrayEquals(b60, resultSet.getBytes(6));

      Assert.assertFalse(resultSet.next());

      preparedStatement = conn.prepareStatement("SELECT * FROM " + tableName
          + " WHERE COL1 = ? AND COL2 IN (?, ?)");
      preparedStatement.setDouble(1, b4);
      preparedStatement.setBytes(2, b51);
      preparedStatement.setBytes(3, b5);

      assertIndexUsed(preparedStatement, indexName, "SKIP SCAN ON 2 KEYS ");

      resultSet = preparedStatement.executeQuery();

      Assert.assertTrue(resultSet.next());

      Assert.assertArrayEquals(b11, resultSet.getBytes(1));
      Assert.assertArrayEquals(b21, resultSet.getBytes(2));
      Assert.assertArrayEquals(b31, resultSet.getBytes(3));
      Assert.assertEquals(b41, resultSet.getDouble(4), 0D);
      Assert.assertArrayEquals(b51, resultSet.getBytes(5));
      Assert.assertArrayEquals(b61, resultSet.getBytes(6));

      Assert.assertTrue(resultSet.next());

      Assert.assertArrayEquals(b1, resultSet.getBytes(1));
      Assert.assertArrayEquals(b2, resultSet.getBytes(2));
      Assert.assertArrayEquals(b3, resultSet.getBytes(3));
      Assert.assertEquals(b4, resultSet.getDouble(4), 0D);
      Assert.assertArrayEquals(b5, resultSet.getBytes(5));
      Assert.assertArrayEquals(b6, resultSet.getBytes(6));

      Assert.assertTrue(resultSet.next());

      Assert.assertArrayEquals(b10, resultSet.getBytes(1));
      Assert.assertArrayEquals(b20, resultSet.getBytes(2));
      Assert.assertArrayEquals(b30, resultSet.getBytes(3));
      Assert.assertEquals(b40, resultSet.getDouble(4), 0D);
      Assert.assertArrayEquals(b50, resultSet.getBytes(5));
      Assert.assertArrayEquals(b60, resultSet.getBytes(6));

      Assert.assertFalse(resultSet.next());
    }
  }

  @Test
  public void testVarBinaryPkSchema4() throws Exception {
    Properties props = PropertiesUtil.deepCopy(TEST_PROPERTIES);
    final String tableName = generateUniqueName();
    final String indexName = generateUniqueName();
    try (Connection conn = DriverManager.getConnection(getUrl(), props)) {
      conn.createStatement().execute("CREATE TABLE " + tableName
          + " (PK1 VARBINARY_ENCODED, PK2 VARBINARY_ENCODED, PK3 VARCHAR,"
          + " COL1 VARCHAR, COL2 VARBINARY_ENCODED,"
          + " COL3 VARBINARY_ENCODED CONSTRAINT pk PRIMARY KEY(PK1, PK2, PK3)) "
          + (this.columnEncoded ? "" : "COLUMN_ENCODED_BYTES=0"));

      if (this.coveredIndex) {
        conn.createStatement().execute(
            "CREATE INDEX " + indexName + " ON " + tableName + " (COL1, COL2) INCLUDE (COL3)");
      } else {
        conn.createStatement().execute("CREATE UNCOVERED INDEX " + indexName + " ON " + tableName
            + " (COL1, COL2)");
      }

      byte[] b1 = new byte[] {1, 1, 19, -28, 24, 1, 1, -11, -21, 1};
      byte[] b2 = new byte[] {57, -83, 2, 83, -7, 12, -13, 4};
      String b3 = "bc2p04fiu2j05-4n4go4k";
      String b4 = "TnM5+UZ#J#GV20fn45#_$593+12*yT0Vd%Y+Q4FaVScnmQP3+SfTPt1OeWp4K+N&PB";
      byte[] b5 = new byte[] {10, 55, 19, -5, -34, 0, 0, 0, 0, 1};
      byte[] b6 = new byte[] {-11, 55, -119, 8, 0, 1, 2, -4, 33};

      byte[] b10 = new byte[] {1, 1, 19, -28, 25, -1, 1, -11, -21, -1};
      byte[] b20 = new byte[] {57, -83, -2, 83, 0, -7, -12, -13, 4};
      String b30 = "aa2p04fiu2j05-3n4go4";
      String b40 = "TnM5+UZ#J#GV20fn45#_$593+12*yT0Vd%Y+Q4FaVScnmQP3+SfTPt1OeWp4K+N&PB";
      byte[] b50 = new byte[] {10, 55, 19, -5, -34, 0, 0, 0, 0, 1};
      byte[] b60 = new byte[] {-11, 55, -119, 8, 0, 1, 2, -4, 33};

      byte[] b11 = new byte[] {1, 1, 20, -28, 0, -1, 0, -11, -21, -1};
      byte[] b21 = new byte[] {57, -83, 0, -2, 0, -7, -12, -13, 4};
      String b31 = "ab2p04fiu2j05-4n4go4k";
      String b41 = "TnM5+UZ#J#GV20fn45#_$593+12*yT0Vd%Y+Q4FaVScnmQP3+SfTPt1OeWp4K+N&PB";
      byte[] b51 = new byte[] {10, 55, 0, 19, -5, -34, 0, 0, 0, 0, 1};
      byte[] b61 = new byte[] {-11, 55, -119, 0, 8, 0, 1, 2, -4, 33};

      byte[] b12 = new byte[] {1, 1, 20, -28, 0, -1, 0, -11, -21, -1};
      byte[] b22 = new byte[] {57, -83, 0, -2, 0, -7, -12, -13, 4, 0};
      String b32 = "bb2p04fiu2j05-4n4go4k";
      String b42 = "tT3GZmtUkcmt@GgqOB3S9ju4yyc1BSN@e9RvVUcG&tuJh3Qn=K";
      byte[] b52 = new byte[] {10, 55, 0, 19, -5, -34, 0, 0, 0, 0, 1};
      byte[] b62 = new byte[] {-11, 55, -119, 0, 8, 0, 1, 2, -4, 33};

      byte[] b13 = new byte[] {1, 1, 20, -28, 0, -1, 0, -11, -21, -1};
      byte[] b23 = new byte[] {57, -83, 0, -2, 0, -7, -12, -13, 4};
      String b33 = "ab2p04fiu2j05-3n4go4k";
      String b43 = "tT3GZmtUkcmut@GgqOB3S9ju4yyc1BSN@e9RvVUcG&tuJh3Qn=K";
      byte[] b53 = new byte[] {10, 55, 0, 19, -5, -34, 0, -12, 0, 0, 0, 1};
      byte[] b63 = new byte[] {-11, 55, -119, 0, 8, 0, 1, 2, -4, 33};

      byte[] b14 = new byte[] {1, 1, 20, -28, 0, -1, 0, -11, -21, -1};
      byte[] b24 = null;
      String b34 = "cc2p04fiu2j05-4n4go4k";
      String b44 = "tT3GZmtUkcmut@GgqOB3S9ju4yyc1BSN@e9RvVUcG&tuJh3Qn=K";
      byte[] b54 = new byte[] {10, 55, 0, 19, -5, -34, 0, -12, 0, 0, 0, 1};
      byte[] b64 = new byte[] {-11, 55, -119, 0, 8, 0, 1, 2, -4, 33};

      byte[] b15 = new byte[] {1, 1, 20, -28, 0, -1, 0, -11, -21, -1};
      byte[] b25 = new byte[] {57, -83, 0, -2, 0, -7, -12, -13, 4};
      byte[] b35 = null;
      String b45 = "tT3GZmtUkcmut@GgqOB3S9ju4yyc1BSN@e9RvVUcG&tuJh3Qn=K";
      byte[] b55 = new byte[] {10, 55, 0, 19, -5, -34, 0, -12, 0, 0, 0, 1};
      byte[] b65 = new byte[] {-11, 55, -119, 0, 8, 0, 1, 2, -4, 33};

      byte[] b16 = new byte[] {1, 1, 19, -28, 25, -1, 1, -11, -21, -1};
      byte[] b26 = new byte[] {57, -83, -2, 83, 0, -7, -12, -13, 4};
      String b36 = "aa2p04fiu2j05-3n4go4k";
      String b46 = "TnM5+UZ#J#GV20fn45#_$593+12*yT0Vd%Y+Q4FaVScnmQP3+SfTPt1OeWp4K+N&PB";
      byte[] b56 = null;
      byte[] b66 = new byte[] {-11, 55, -119, 8, 0, 1, 2, -4, 33};

      try (PreparedStatement preparedStatement = conn.prepareStatement("UPSERT INTO " + tableName
          + "(PK1, PK2, PK3, COL1, COL2, COL3) VALUES (?, ?, ?, ?, ?, ?)")) {
        upsertRow(preparedStatement, b10, b20, b30, b40, b50, b60);
        upsertRow(preparedStatement, b1, b2, b3, b4, b5, b6);
        upsertRow(preparedStatement, b11, b21, b31, b41, b51, b61);
        upsertRow(preparedStatement, b12, b22, b32, b42, b52, b62);
        upsertRow(preparedStatement, b13, b23, b33, b43, b53, b63);
        upsertRow(preparedStatement, b14, b24, b34, b44, b54, b64);
        upsertRow(preparedStatement, b15, b25, b35, b45, b55, b65);
        upsertRow(preparedStatement, b16, b26, b36, b46, b56, b66);
      }
      conn.commit();

      PreparedStatement preparedStatement =
          conn.prepareStatement("SELECT * FROM " + tableName + " WHERE COL1 = ? AND COL2 = ?");

      preparedStatement.setString(1, b4);
      preparedStatement.setBytes(2, b5);

      assertIndexUsed(preparedStatement, indexName, "RANGE SCAN ");

      ResultSet resultSet = preparedStatement.executeQuery();

      Assert.assertTrue(resultSet.next());

      Assert.assertArrayEquals(b1, resultSet.getBytes(1));
      Assert.assertArrayEquals(b2, resultSet.getBytes(2));
      Assert.assertEquals(b3, resultSet.getString(3));
      Assert.assertEquals(b4, resultSet.getString(4));
      Assert.assertArrayEquals(b5, resultSet.getBytes(5));
      Assert.assertArrayEquals(b6, resultSet.getBytes(6));

      Assert.assertTrue(resultSet.next());

      Assert.assertArrayEquals(b10, resultSet.getBytes(1));
      Assert.assertArrayEquals(b20, resultSet.getBytes(2));
      Assert.assertEquals(b30, resultSet.getString(3));
      Assert.assertEquals(b40, resultSet.getString(4));
      Assert.assertArrayEquals(b50, resultSet.getBytes(5));
      Assert.assertArrayEquals(b60, resultSet.getBytes(6));

      Assert.assertFalse(resultSet.next());

      preparedStatement =
          conn.prepareStatement("SELECT * FROM " + tableName + " WHERE COL1 = ?");

      preparedStatement.setString(1, b4);

      assertIndexUsed(preparedStatement, indexName, "RANGE SCAN ");

      resultSet = preparedStatement.executeQuery();

      Assert.assertTrue(resultSet.next());

      Assert.assertArrayEquals(b16, resultSet.getBytes(1));
      Assert.assertArrayEquals(b26, resultSet.getBytes(2));
      Assert.assertEquals(b36, resultSet.getString(3));
      Assert.assertEquals(b46, resultSet.getString(4));
      Assert.assertNull(resultSet.getBytes(5));
      Assert.assertArrayEquals(b66, resultSet.getBytes(6));

      Assert.assertTrue(resultSet.next());

      Assert.assertArrayEquals(b11, resultSet.getBytes(1));
      Assert.assertArrayEquals(b21, resultSet.getBytes(2));
      Assert.assertEquals(b31, resultSet.getString(3));
      Assert.assertEquals(b41, resultSet.getString(4));
      Assert.assertArrayEquals(b51, resultSet.getBytes(5));
      Assert.assertArrayEquals(b61, resultSet.getBytes(6));

      Assert.assertTrue(resultSet.next());

      Assert.assertArrayEquals(b1, resultSet.getBytes(1));
      Assert.assertArrayEquals(b2, resultSet.getBytes(2));
      Assert.assertEquals(b3, resultSet.getString(3));
      Assert.assertEquals(b4, resultSet.getString(4));
      Assert.assertArrayEquals(b5, resultSet.getBytes(5));
      Assert.assertArrayEquals(b6, resultSet.getBytes(6));

      Assert.assertTrue(resultSet.next());

      Assert.assertArrayEquals(b10, resultSet.getBytes(1));
      Assert.assertArrayEquals(b20, resultSet.getBytes(2));
      Assert.assertEquals(b30, resultSet.getString(3));
      Assert.assertEquals(b40, resultSet.getString(4));
      Assert.assertArrayEquals(b50, resultSet.getBytes(5));
      Assert.assertArrayEquals(b60, resultSet.getBytes(6));

      Assert.assertFalse(resultSet.next());

      preparedStatement = conn.prepareStatement(
          "SELECT * FROM " + tableName + " WHERE COL1 = ? AND COL2 IS NOT NULL");

      preparedStatement.setString(1, b4);

      assertIndexUsed(preparedStatement, indexName, "RANGE SCAN ");

      resultSet = preparedStatement.executeQuery();

      Assert.assertTrue(resultSet.next());

      Assert.assertArrayEquals(b11, resultSet.getBytes(1));
      Assert.assertArrayEquals(b21, resultSet.getBytes(2));
      Assert.assertEquals(b31, resultSet.getString(3));
      Assert.assertEquals(b41, resultSet.getString(4));
      Assert.assertArrayEquals(b51, resultSet.getBytes(5));
      Assert.assertArrayEquals(b61, resultSet.getBytes(6));

      Assert.assertTrue(resultSet.next());

      Assert.assertArrayEquals(b1, resultSet.getBytes(1));
      Assert.assertArrayEquals(b2, resultSet.getBytes(2));
      Assert.assertEquals(b3, resultSet.getString(3));
      Assert.assertEquals(b4, resultSet.getString(4));
      Assert.assertArrayEquals(b5, resultSet.getBytes(5));
      Assert.assertArrayEquals(b6, resultSet.getBytes(6));

      Assert.assertTrue(resultSet.next());

      Assert.assertArrayEquals(b10, resultSet.getBytes(1));
      Assert.assertArrayEquals(b20, resultSet.getBytes(2));
      Assert.assertEquals(b30, resultSet.getString(3));
      Assert.assertEquals(b40, resultSet.getString(4));
      Assert.assertArrayEquals(b50, resultSet.getBytes(5));
      Assert.assertArrayEquals(b60, resultSet.getBytes(6));

      Assert.assertFalse(resultSet.next());

      preparedStatement = conn.prepareStatement("SELECT * FROM " + tableName
          + " WHERE COL1 = ? AND COL2 BETWEEN ? AND ?");
      preparedStatement.setString(1, b4);
      preparedStatement.setBytes(2, b51);
      preparedStatement.setBytes(3, b5);

      assertIndexUsed(preparedStatement, indexName, "RANGE SCAN ");

      resultSet = preparedStatement.executeQuery();

      Assert.assertTrue(resultSet.next());

      Assert.assertArrayEquals(b11, resultSet.getBytes(1));
      Assert.assertArrayEquals(b21, resultSet.getBytes(2));
      Assert.assertEquals(b31, resultSet.getString(3));
      Assert.assertEquals(b41, resultSet.getString(4));
      Assert.assertArrayEquals(b51, resultSet.getBytes(5));
      Assert.assertArrayEquals(b61, resultSet.getBytes(6));

      Assert.assertTrue(resultSet.next());

      Assert.assertArrayEquals(b1, resultSet.getBytes(1));
      Assert.assertArrayEquals(b2, resultSet.getBytes(2));
      Assert.assertEquals(b3, resultSet.getString(3));
      Assert.assertEquals(b4, resultSet.getString(4));
      Assert.assertArrayEquals(b5, resultSet.getBytes(5));
      Assert.assertArrayEquals(b6, resultSet.getBytes(6));

      Assert.assertTrue(resultSet.next());

      Assert.assertArrayEquals(b10, resultSet.getBytes(1));
      Assert.assertArrayEquals(b20, resultSet.getBytes(2));
      Assert.assertEquals(b30, resultSet.getString(3));
      Assert.assertEquals(b40, resultSet.getString(4));
      Assert.assertArrayEquals(b50, resultSet.getBytes(5));
      Assert.assertArrayEquals(b60, resultSet.getBytes(6));

      Assert.assertFalse(resultSet.next());

      preparedStatement = conn.prepareStatement("SELECT * FROM " + tableName
          + " WHERE COL1 = ? AND COL2 IN (?, ?)");
      preparedStatement.setString(1, b4);
      preparedStatement.setBytes(2, b51);
      preparedStatement.setBytes(3, b5);

      assertIndexUsed(preparedStatement, indexName, "SKIP SCAN ON 2 KEYS ");

      resultSet = preparedStatement.executeQuery();

      Assert.assertTrue(resultSet.next());

      Assert.assertArrayEquals(b11, resultSet.getBytes(1));
      Assert.assertArrayEquals(b21, resultSet.getBytes(2));
      Assert.assertEquals(b31, resultSet.getString(3));
      Assert.assertEquals(b41, resultSet.getString(4));
      Assert.assertArrayEquals(b51, resultSet.getBytes(5));
      Assert.assertArrayEquals(b61, resultSet.getBytes(6));

      Assert.assertTrue(resultSet.next());

      Assert.assertArrayEquals(b1, resultSet.getBytes(1));
      Assert.assertArrayEquals(b2, resultSet.getBytes(2));
      Assert.assertEquals(b3, resultSet.getString(3));
      Assert.assertEquals(b4, resultSet.getString(4));
      Assert.assertArrayEquals(b5, resultSet.getBytes(5));
      Assert.assertArrayEquals(b6, resultSet.getBytes(6));

      Assert.assertTrue(resultSet.next());

      Assert.assertArrayEquals(b10, resultSet.getBytes(1));
      Assert.assertArrayEquals(b20, resultSet.getBytes(2));
      Assert.assertEquals(b30, resultSet.getString(3));
      Assert.assertEquals(b40, resultSet.getString(4));
      Assert.assertArrayEquals(b50, resultSet.getBytes(5));
      Assert.assertArrayEquals(b60, resultSet.getBytes(6));

      Assert.assertFalse(resultSet.next());
    }
  }

  private static void assertIndexUsed(PreparedStatement preparedStatement, String indexName,
      String scanType) throws SQLException {
    ExplainPlan plan =
        preparedStatement.unwrap(PhoenixPreparedStatement.class).optimizeQuery().getExplainPlan();
    ExplainPlanAttributes planAttributes = plan.getPlanStepsAsAttributes();

    Assert.assertEquals(indexName, planAttributes.getTableName());
    Assert.assertEquals(scanType, planAttributes.getExplainScanType());
  }

  private static void upsertRow(PreparedStatement preparedStatement, byte[] b10, byte[] b20,
      byte[] b30, byte[] b40, byte[] b50, byte[] b60) throws SQLException {
    preparedStatement.setBytes(1, b10);
    preparedStatement.setBytes(2, b20);
    preparedStatement.setBytes(3, b30);
    preparedStatement.setBytes(4, b40);
    preparedStatement.setBytes(5, b50);
    preparedStatement.setBytes(6, b60);
    preparedStatement.executeUpdate();
  }

  private static void upsertRow(PreparedStatement preparedStatement, byte[] b10, byte[] b20,
      byte[] b30, String b40, byte[] b50, byte[] b60) throws SQLException {
    preparedStatement.setBytes(1, b10);
    preparedStatement.setBytes(2, b20);
    preparedStatement.setBytes(3, b30);
    preparedStatement.setString(4, b40);
    preparedStatement.setBytes(5, b50);
    preparedStatement.setBytes(6, b60);
    preparedStatement.executeUpdate();
  }

  private static void upsertRow(PreparedStatement preparedStatement, byte[] b10, byte[] b20,
      String b30, String b40, byte[] b50, byte[] b60) throws SQLException {
    preparedStatement.setBytes(1, b10);
    preparedStatement.setBytes(2, b20);
    preparedStatement.setString(3, b30);
    preparedStatement.setString(4, b40);
    preparedStatement.setBytes(5, b50);
    preparedStatement.setBytes(6, b60);
    preparedStatement.executeUpdate();
  }

  private static void upsertRow(PreparedStatement preparedStatement, byte[] b10, byte[] b20,
      byte[] b30, double b40, byte[] b50, byte[] b60) throws SQLException {
    preparedStatement.setBytes(1, b10);
    preparedStatement.setBytes(2, b20);
    preparedStatement.setBytes(3, b30);
    preparedStatement.setDouble(4, b40);
    preparedStatement.setBytes(5, b50);
    preparedStatement.setBytes(6, b60);
    preparedStatement.executeUpdate();
  }

}
