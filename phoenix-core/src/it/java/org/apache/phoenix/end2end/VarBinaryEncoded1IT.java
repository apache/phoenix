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
import org.apache.phoenix.schema.PTableImpl;
import org.apache.phoenix.util.PropertiesUtil;

import static org.apache.phoenix.util.TestUtil.TEST_PROPERTIES;

@Category(ParallelStatsDisabledTest.class)
@RunWith(Parameterized.class)
public class VarBinaryEncoded1IT extends ParallelStatsDisabledIT {

  private final String tableDDLOptions;

  public VarBinaryEncoded1IT(boolean columnEncoded, String transactionProvider, boolean mutable) {
    StringBuilder optionBuilder = new StringBuilder();
    if (!columnEncoded) {
      if (optionBuilder.length() != 0) {
        optionBuilder.append(",");
      }
      optionBuilder.append("COLUMN_ENCODED_BYTES=0");
    }
    if (!mutable) {
      if (optionBuilder.length() != 0) {
        optionBuilder.append(",");
      }
      optionBuilder.append("IMMUTABLE_ROWS=true");
      if (!columnEncoded) {
        optionBuilder.append(
            ",IMMUTABLE_STORAGE_SCHEME=" + PTableImpl.ImmutableStorageScheme.ONE_CELL_PER_COLUMN);
      }
    }
    boolean transactional = transactionProvider != null;
    if (transactional) {
      if (optionBuilder.length() != 0) {
        optionBuilder.append(",");
      }
      optionBuilder.append(
          " TRANSACTIONAL=true,TRANSACTION_PROVIDER='" + transactionProvider + "'");
    }
    this.tableDDLOptions = optionBuilder.toString();
  }

  @Parameterized.Parameters(name =
      "VarBinary1IT_columnEncoded={0}, transactionProvider={1}, mutable={2}")
  public static synchronized Collection<Object[]> data() {
    return Arrays.asList(new Object[][] {
        {false, null, false},
        {false, "OMID", false},
        {false, null, true},
        {false, "OMID", true},
        {true, null, false},
        {true, "OMID", false},
        {true, null, true},
        {true, "OMID", true},
    });
  }

  @Test
  public void testVarBinaryPkSchema1() throws Exception {
    Properties props = PropertiesUtil.deepCopy(TEST_PROPERTIES);
    final String tableName = generateUniqueName();
    try (Connection conn = DriverManager.getConnection(getUrl(), props)) {
      conn.createStatement().execute("CREATE TABLE " + tableName
          + " (PK1 VARBINARY_ENCODED, PK2 VARBINARY_ENCODED, PK3 VARBINARY_ENCODED,"
          + " COL1 VARBINARY_ENCODED, COL2 VARBINARY_ENCODED,"
          + " COL3 VARBINARY_ENCODED CONSTRAINT pk PRIMARY KEY(PK1, PK2, PK3)) "
          + tableDDLOptions);

      byte[] b1 = new byte[] {1, 1, 19, -28, 24, 1, 1, -11, -21, 1};
      byte[] b2 = new byte[] {57, -83, 2, 83, -7, 12, -13, 4};
      byte[] b3 = new byte[] {4, 34, -19, 8, -73, 3, 4, 23};
      byte[] b4 = new byte[] {56, 50, 19, 34, 83, -101, -102, 91};
      byte[] b5 = new byte[] {10, 55, 19, -5, -34, 0, 0, 0, 0, 1};
      byte[] b6 = new byte[] {-11, 55, -119, 8, 0, 1, 2, -4, 33};

      byte[] b10 = new byte[] {1, 1, 19, -28, 25, -1, 1, -11, -21, -1};
      byte[] b20 = new byte[] {57, -83, -2, 83, 0, -7, -12, -13, 4};
      byte[] b30 = new byte[] {4, 1, -19, 8, 0, -73, 3, 4, 23};
      byte[] b40 = new byte[] {56, 50, 19, 34, 83, -101, -102, 91};
      byte[] b50 = new byte[] {10, 55, 19, -5, -34, 0, 0, 0, 0, 1};
      byte[] b60 = new byte[] {-11, 55, -119, 8, 0, 1, 2, -4, 33};

      byte[] b11 = new byte[] {1, 1, 20, -28, 0, -1, 0, -11, -21, -1};
      byte[] b21 = new byte[] {57, -83, 0, -2, 0, -7, -12, -13, 4};
      byte[] b31 = new byte[] {4, 1, 0, 0, 0, 73, 3, 0, 23};
      byte[] b41 = new byte[] {56, 50, 19, 0, 34, 83, -101, -102, 91};
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
      }
      conn.commit();

      PreparedStatement pst = conn.prepareStatement("SELECT * FROM " + tableName);
      ResultSet resultSet = pst.executeQuery();

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

      Assert.assertTrue(resultSet.next());

      Assert.assertArrayEquals(b14, resultSet.getBytes(1));
      Assert.assertNull(resultSet.getBytes(2));
      Assert.assertArrayEquals(b34, resultSet.getBytes(3));
      Assert.assertArrayEquals(b44, resultSet.getBytes(4));
      Assert.assertArrayEquals(b54, resultSet.getBytes(5));
      Assert.assertArrayEquals(b64, resultSet.getBytes(6));

      Assert.assertTrue(resultSet.next());

      Assert.assertArrayEquals(b15, resultSet.getBytes(1));
      Assert.assertArrayEquals(b25, resultSet.getBytes(2));
      Assert.assertNull(resultSet.getBytes(3));
      Assert.assertArrayEquals(b45, resultSet.getBytes(4));
      Assert.assertArrayEquals(b55, resultSet.getBytes(5));
      Assert.assertArrayEquals(b65, resultSet.getBytes(6));

      Assert.assertTrue(resultSet.next());

      Assert.assertArrayEquals(b13, resultSet.getBytes(1));
      Assert.assertArrayEquals(b23, resultSet.getBytes(2));
      Assert.assertArrayEquals(b33, resultSet.getBytes(3));
      Assert.assertArrayEquals(b43, resultSet.getBytes(4));
      Assert.assertArrayEquals(b53, resultSet.getBytes(5));
      Assert.assertArrayEquals(b63, resultSet.getBytes(6));

      Assert.assertTrue(resultSet.next());

      Assert.assertArrayEquals(b11, resultSet.getBytes(1));
      Assert.assertArrayEquals(b21, resultSet.getBytes(2));
      Assert.assertArrayEquals(b31, resultSet.getBytes(3));
      Assert.assertArrayEquals(b41, resultSet.getBytes(4));
      Assert.assertArrayEquals(b51, resultSet.getBytes(5));
      Assert.assertArrayEquals(b61, resultSet.getBytes(6));

      Assert.assertTrue(resultSet.next());

      Assert.assertArrayEquals(b12, resultSet.getBytes(1));
      Assert.assertArrayEquals(b22, resultSet.getBytes(2));
      Assert.assertArrayEquals(b32, resultSet.getBytes(3));
      Assert.assertArrayEquals(b42, resultSet.getBytes(4));
      Assert.assertArrayEquals(b52, resultSet.getBytes(5));
      Assert.assertArrayEquals(b62, resultSet.getBytes(6));

      Assert.assertTrue(resultSet.next());

      Assert.assertArrayEquals(Bytes.toBytes("pk1-ehgir4jf"), resultSet.getBytes(1));
      Assert.assertArrayEquals(Bytes.toBytes("pk22p0jfdkhrgi"), resultSet.getBytes(2));
      Assert.assertArrayEquals(Bytes.toBytes("pk33ogjirhhf"), resultSet.getBytes(3));
      Assert.assertArrayEquals(Bytes.toBytes("col19fnbb0hf0t"), resultSet.getBytes(4));
      Assert.assertArrayEquals(Bytes.toBytes("col21048rnbfpe3-"), resultSet.getBytes(5));
      Assert.assertArrayEquals(Bytes.toBytes("col319efnrugifj"), resultSet.getBytes(6));

      Assert.assertFalse(resultSet.next());

      PreparedStatement preparedStatement =
          conn.prepareStatement("SELECT * FROM " + tableName + " WHERE PK1 = ?");

      preparedStatement.setBytes(1, b11);
      resultSet = preparedStatement.executeQuery();

      Assert.assertTrue(resultSet.next());

      Assert.assertArrayEquals(b14, resultSet.getBytes(1));
      Assert.assertNull(resultSet.getBytes(2));
      Assert.assertArrayEquals(b34, resultSet.getBytes(3));
      Assert.assertArrayEquals(b44, resultSet.getBytes(4));
      Assert.assertArrayEquals(b54, resultSet.getBytes(5));
      Assert.assertArrayEquals(b64, resultSet.getBytes(6));

      Assert.assertTrue(resultSet.next());

      Assert.assertArrayEquals(b15, resultSet.getBytes(1));
      Assert.assertArrayEquals(b25, resultSet.getBytes(2));
      Assert.assertNull(resultSet.getBytes(3));
      Assert.assertArrayEquals(b45, resultSet.getBytes(4));
      Assert.assertArrayEquals(b55, resultSet.getBytes(5));
      Assert.assertArrayEquals(b65, resultSet.getBytes(6));

      Assert.assertTrue(resultSet.next());

      Assert.assertArrayEquals(b13, resultSet.getBytes(1));
      Assert.assertArrayEquals(b23, resultSet.getBytes(2));
      Assert.assertArrayEquals(b33, resultSet.getBytes(3));
      Assert.assertArrayEquals(b43, resultSet.getBytes(4));
      Assert.assertArrayEquals(b53, resultSet.getBytes(5));
      Assert.assertArrayEquals(b63, resultSet.getBytes(6));

      Assert.assertTrue(resultSet.next());

      Assert.assertArrayEquals(b11, resultSet.getBytes(1));
      Assert.assertArrayEquals(b21, resultSet.getBytes(2));
      Assert.assertArrayEquals(b31, resultSet.getBytes(3));
      Assert.assertArrayEquals(b41, resultSet.getBytes(4));
      Assert.assertArrayEquals(b51, resultSet.getBytes(5));
      Assert.assertArrayEquals(b61, resultSet.getBytes(6));

      Assert.assertTrue(resultSet.next());

      Assert.assertArrayEquals(b12, resultSet.getBytes(1));
      Assert.assertArrayEquals(b22, resultSet.getBytes(2));
      Assert.assertArrayEquals(b32, resultSet.getBytes(3));
      Assert.assertArrayEquals(b42, resultSet.getBytes(4));
      Assert.assertArrayEquals(b52, resultSet.getBytes(5));
      Assert.assertArrayEquals(b62, resultSet.getBytes(6));

      Assert.assertFalse(resultSet.next());


      preparedStatement = conn.prepareStatement(
          "SELECT * FROM " + tableName + " WHERE PK1 = ? AND PK2 IS NOT NULL ");

      preparedStatement.setBytes(1, b11);
      resultSet = preparedStatement.executeQuery();

      Assert.assertTrue(resultSet.next());

      Assert.assertArrayEquals(b15, resultSet.getBytes(1));
      Assert.assertArrayEquals(b25, resultSet.getBytes(2));
      Assert.assertNull(resultSet.getBytes(3));
      Assert.assertArrayEquals(b45, resultSet.getBytes(4));
      Assert.assertArrayEquals(b55, resultSet.getBytes(5));
      Assert.assertArrayEquals(b65, resultSet.getBytes(6));

      Assert.assertTrue(resultSet.next());

      Assert.assertArrayEquals(b13, resultSet.getBytes(1));
      Assert.assertArrayEquals(b23, resultSet.getBytes(2));
      Assert.assertArrayEquals(b33, resultSet.getBytes(3));
      Assert.assertArrayEquals(b43, resultSet.getBytes(4));
      Assert.assertArrayEquals(b53, resultSet.getBytes(5));
      Assert.assertArrayEquals(b63, resultSet.getBytes(6));

      Assert.assertTrue(resultSet.next());

      Assert.assertArrayEquals(b11, resultSet.getBytes(1));
      Assert.assertArrayEquals(b21, resultSet.getBytes(2));
      Assert.assertArrayEquals(b31, resultSet.getBytes(3));
      Assert.assertArrayEquals(b41, resultSet.getBytes(4));
      Assert.assertArrayEquals(b51, resultSet.getBytes(5));
      Assert.assertArrayEquals(b61, resultSet.getBytes(6));

      Assert.assertTrue(resultSet.next());

      Assert.assertArrayEquals(b12, resultSet.getBytes(1));
      Assert.assertArrayEquals(b22, resultSet.getBytes(2));
      Assert.assertArrayEquals(b32, resultSet.getBytes(3));
      Assert.assertArrayEquals(b42, resultSet.getBytes(4));
      Assert.assertArrayEquals(b52, resultSet.getBytes(5));
      Assert.assertArrayEquals(b62, resultSet.getBytes(6));

      Assert.assertFalse(resultSet.next());

      preparedStatement =
          conn.prepareStatement("SELECT * FROM " + tableName + " WHERE PK1 = ? AND PK2 = ?");
      preparedStatement.setBytes(1, b11);
      preparedStatement.setBytes(2, b21);
      resultSet = preparedStatement.executeQuery();

      Assert.assertTrue(resultSet.next());

      Assert.assertArrayEquals(b15, resultSet.getBytes(1));
      Assert.assertArrayEquals(b25, resultSet.getBytes(2));
      Assert.assertNull(resultSet.getBytes(3));
      Assert.assertArrayEquals(b45, resultSet.getBytes(4));
      Assert.assertArrayEquals(b55, resultSet.getBytes(5));
      Assert.assertArrayEquals(b65, resultSet.getBytes(6));

      Assert.assertTrue(resultSet.next());

      Assert.assertArrayEquals(b13, resultSet.getBytes(1));
      Assert.assertArrayEquals(b23, resultSet.getBytes(2));
      Assert.assertArrayEquals(b33, resultSet.getBytes(3));
      Assert.assertArrayEquals(b43, resultSet.getBytes(4));
      Assert.assertArrayEquals(b53, resultSet.getBytes(5));
      Assert.assertArrayEquals(b63, resultSet.getBytes(6));

      Assert.assertTrue(resultSet.next());

      Assert.assertArrayEquals(b11, resultSet.getBytes(1));
      Assert.assertArrayEquals(b21, resultSet.getBytes(2));
      Assert.assertArrayEquals(b31, resultSet.getBytes(3));
      Assert.assertArrayEquals(b41, resultSet.getBytes(4));
      Assert.assertArrayEquals(b51, resultSet.getBytes(5));
      Assert.assertArrayEquals(b61, resultSet.getBytes(6));

      Assert.assertFalse(resultSet.next());

      preparedStatement = conn.prepareStatement("SELECT * FROM " + tableName
          + " WHERE PK1 = ? AND PK2 BETWEEN ? AND ? AND PK3 IS NOT NULL");
      preparedStatement.setBytes(1, b11);
      preparedStatement.setBytes(2, new byte[] {57, -83, 0, -2, 0, -7, -12, -13, 3, 24, -121});
      preparedStatement.setBytes(3, new byte[] {57, -83, 0, -2, 0, -7, -12, -13, 4});
      resultSet = preparedStatement.executeQuery();

      Assert.assertTrue(resultSet.next());

      Assert.assertArrayEquals(b13, resultSet.getBytes(1));
      Assert.assertArrayEquals(b23, resultSet.getBytes(2));
      Assert.assertArrayEquals(b33, resultSet.getBytes(3));
      Assert.assertArrayEquals(b43, resultSet.getBytes(4));
      Assert.assertArrayEquals(b53, resultSet.getBytes(5));
      Assert.assertArrayEquals(b63, resultSet.getBytes(6));

      Assert.assertTrue(resultSet.next());

      Assert.assertArrayEquals(b11, resultSet.getBytes(1));
      Assert.assertArrayEquals(b21, resultSet.getBytes(2));
      Assert.assertArrayEquals(b31, resultSet.getBytes(3));
      Assert.assertArrayEquals(b41, resultSet.getBytes(4));
      Assert.assertArrayEquals(b51, resultSet.getBytes(5));
      Assert.assertArrayEquals(b61, resultSet.getBytes(6));

      Assert.assertFalse(resultSet.next());

      preparedStatement = conn.prepareStatement("SELECT * FROM " + tableName
          + " WHERE PK1 = ? AND PK2 IN (?, ?, ?)");
      preparedStatement.setBytes(1, b11);
      preparedStatement.setBytes(2, b21);
      preparedStatement.setBytes(3, b23);
      preparedStatement.setBytes(4, b22);
      resultSet = preparedStatement.executeQuery();

      Assert.assertTrue(resultSet.next());

      Assert.assertArrayEquals(b15, resultSet.getBytes(1));
      Assert.assertArrayEquals(b25, resultSet.getBytes(2));
      Assert.assertArrayEquals(b35, resultSet.getBytes(3));
      Assert.assertArrayEquals(b45, resultSet.getBytes(4));
      Assert.assertArrayEquals(b55, resultSet.getBytes(5));
      Assert.assertArrayEquals(b65, resultSet.getBytes(6));

      Assert.assertTrue(resultSet.next());

      Assert.assertArrayEquals(b13, resultSet.getBytes(1));
      Assert.assertArrayEquals(b23, resultSet.getBytes(2));
      Assert.assertArrayEquals(b33, resultSet.getBytes(3));
      Assert.assertArrayEquals(b43, resultSet.getBytes(4));
      Assert.assertArrayEquals(b53, resultSet.getBytes(5));
      Assert.assertArrayEquals(b63, resultSet.getBytes(6));

      Assert.assertTrue(resultSet.next());

      Assert.assertArrayEquals(b11, resultSet.getBytes(1));
      Assert.assertArrayEquals(b21, resultSet.getBytes(2));
      Assert.assertArrayEquals(b31, resultSet.getBytes(3));
      Assert.assertArrayEquals(b41, resultSet.getBytes(4));
      Assert.assertArrayEquals(b51, resultSet.getBytes(5));
      Assert.assertArrayEquals(b61, resultSet.getBytes(6));

      Assert.assertTrue(resultSet.next());

      Assert.assertArrayEquals(b12, resultSet.getBytes(1));
      Assert.assertArrayEquals(b22, resultSet.getBytes(2));
      Assert.assertArrayEquals(b32, resultSet.getBytes(3));
      Assert.assertArrayEquals(b42, resultSet.getBytes(4));
      Assert.assertArrayEquals(b52, resultSet.getBytes(5));
      Assert.assertArrayEquals(b62, resultSet.getBytes(6));

      Assert.assertFalse(resultSet.next());

      preparedStatement = conn.prepareStatement(
          "SELECT * FROM " + tableName + " WHERE PK1 = ? AND PK2 = ? AND PK3 = ?");
      preparedStatement.setBytes(1, b11);
      preparedStatement.setBytes(2, b21);
      preparedStatement.setBytes(3, b31);
      resultSet = preparedStatement.executeQuery();

      Assert.assertTrue(resultSet.next());

      Assert.assertArrayEquals(b11, resultSet.getBytes(1));
      Assert.assertArrayEquals(b21, resultSet.getBytes(2));
      Assert.assertArrayEquals(b31, resultSet.getBytes(3));
      Assert.assertArrayEquals(b41, resultSet.getBytes(4));
      Assert.assertArrayEquals(b51, resultSet.getBytes(5));
      Assert.assertArrayEquals(b61, resultSet.getBytes(6));

      Assert.assertFalse(resultSet.next());

      preparedStatement = conn.prepareStatement(
          "SELECT * FROM " + tableName + " WHERE PK1 = ? AND PK2 = ? AND PK3 IS NULL");
      preparedStatement.setBytes(1, b11);
      preparedStatement.setBytes(2, b21);
      resultSet = preparedStatement.executeQuery();

      Assert.assertTrue(resultSet.next());

      Assert.assertArrayEquals(b15, resultSet.getBytes(1));
      Assert.assertArrayEquals(b25, resultSet.getBytes(2));
      Assert.assertNull(resultSet.getBytes(3));
      Assert.assertArrayEquals(b45, resultSet.getBytes(4));
      Assert.assertArrayEquals(b55, resultSet.getBytes(5));
      Assert.assertArrayEquals(b65, resultSet.getBytes(6));

      Assert.assertFalse(resultSet.next());
    }
  }

  private static void upsertRow(PreparedStatement preparedStatement, byte[] b1, byte[] b2,
      byte[] b3, byte[] b4, byte[] b5, byte[] b6) throws SQLException {
    preparedStatement.setBytes(1, b1);
    preparedStatement.setBytes(2, b2);
    preparedStatement.setBytes(3, b3);
    preparedStatement.setBytes(4, b4);
    preparedStatement.setBytes(5, b5);
    preparedStatement.setBytes(6, b6);
    preparedStatement.executeUpdate();
  }

  @Test
  public void testVarBinaryPkSchema2() throws Exception {
    Properties props = PropertiesUtil.deepCopy(TEST_PROPERTIES);
    final String tableName = generateUniqueName();
    try (Connection conn = DriverManager.getConnection(getUrl(), props)) {
      conn.createStatement().execute("CREATE TABLE " + tableName
          + " (PK1 VARBINARY_ENCODED, PK2 VARBINARY_ENCODED, PK3 VARBINARY_ENCODED,"
          + " COL1 VARBINARY_ENCODED, COL2 VARBINARY_ENCODED,"
          + " COL3 VARBINARY_ENCODED CONSTRAINT pk PRIMARY KEY(PK1 DESC, PK2 DESC, PK3 DESC)) "
          + tableDDLOptions);

      byte[] b1 = new byte[] {1, 1, 19, -28, 24, 1, 1, -11, -21, 1};
      byte[] b2 = new byte[] {57, -83, 2, 83, -7, 12, -13, 4};
      byte[] b3 = new byte[] {4, 34, -19, 8, -73, 3, 4, 23};
      byte[] b4 = new byte[] {56, 50, 19, 34, 83, -101, -102, 91};
      byte[] b5 = new byte[] {10, 55, 19, -5, -34, 0, 0, 0, 0, 1};
      byte[] b6 = new byte[] {-11, 55, -119, 8, 0, 1, 2, -4, 33};

      byte[] b10 = new byte[] {1, 1, 19, -28, 25, -1, 1, -11, -21, -1};
      byte[] b20 = new byte[] {57, -83, -2, 83, 0, -7, -12, -13, 4};
      byte[] b30 = new byte[] {4, 1, -19, 8, 0, -73, 3, 4, 23};
      byte[] b40 = new byte[] {56, 50, 19, 34, 83, -101, -102, 91};
      byte[] b50 = new byte[] {10, 55, 19, -5, -34, 0, 0, 0, 0, 1};
      byte[] b60 = new byte[] {-11, 55, -119, 8, 0, 1, 2, -4, 33};

      byte[] b11 = new byte[] {1, 1, 20, -28, 0, -1, 0, -11, -21, -1};
      byte[] b21 = new byte[] {57, -83, 0, -2, 0, -7, -12, -13, 4};
      byte[] b31 = new byte[] {4, 1, 0, 0, 0, 73, 3, 0, 23};
      byte[] b41 = new byte[] {56, 50, 19, 0, 34, 83, -101, -102, 91};
      byte[] b51 = new byte[] {10, 55, 0, 19, -5, -34, 0, 0, 0, 0, 1};
      byte[] b61 = new byte[] {-11, 55, -119, 0, 8, 0, 1, 2, -4, 33};

      try (PreparedStatement preparedStatement = conn.prepareStatement("UPSERT INTO " + tableName
          + "(PK1, PK2, PK3, COL1, COL2, COL3) VALUES (?, ?, ?, ?, ?, ?)")) {
        upsertRow(preparedStatement, Bytes.toBytes("pk1-ehgir4jf"), Bytes.toBytes("pk22p0jfdkhrgi"),
            Bytes.toBytes("pk33ogjirhhf"), Bytes.toBytes("col19fnbb0hf0t"),
            Bytes.toBytes("col21048rnbfpe3-"), Bytes.toBytes("col319efnrugifj"));

        upsertRow(preparedStatement, b10, b20, b30, b40, b50, b60);

        upsertRow(preparedStatement, b1, b2, b3, b4, b5, b6);

        upsertRow(preparedStatement, b11, b21, b31, b41, b51, b61);
      }
      conn.commit();

      ResultSet resultSet = conn.createStatement().executeQuery("SELECT * FROM " + tableName);

      Assert.assertTrue(resultSet.next());

      Assert.assertArrayEquals(Bytes.toBytes("pk1-ehgir4jf"), resultSet.getBytes(1));
      Assert.assertArrayEquals(Bytes.toBytes("pk22p0jfdkhrgi"), resultSet.getBytes(2));
      Assert.assertArrayEquals(Bytes.toBytes("pk33ogjirhhf"), resultSet.getBytes(3));
      Assert.assertArrayEquals(Bytes.toBytes("col19fnbb0hf0t"), resultSet.getBytes(4));
      Assert.assertArrayEquals(Bytes.toBytes("col21048rnbfpe3-"), resultSet.getBytes(5));
      Assert.assertArrayEquals(Bytes.toBytes("col319efnrugifj"), resultSet.getBytes(6));

      Assert.assertTrue(resultSet.next());

      Assert.assertArrayEquals(b11, resultSet.getBytes(1));
      Assert.assertArrayEquals(b21, resultSet.getBytes(2));
      Assert.assertArrayEquals(b31, resultSet.getBytes(3));
      Assert.assertArrayEquals(b41, resultSet.getBytes(4));
      Assert.assertArrayEquals(b51, resultSet.getBytes(5));
      Assert.assertArrayEquals(b61, resultSet.getBytes(6));

      Assert.assertTrue(resultSet.next());

      Assert.assertArrayEquals(b10, resultSet.getBytes(1));
      Assert.assertArrayEquals(b20, resultSet.getBytes(2));
      Assert.assertArrayEquals(b30, resultSet.getBytes(3));
      Assert.assertArrayEquals(b40, resultSet.getBytes(4));
      Assert.assertArrayEquals(b50, resultSet.getBytes(5));
      Assert.assertArrayEquals(b60, resultSet.getBytes(6));

      Assert.assertTrue(resultSet.next());

      Assert.assertArrayEquals(b1, resultSet.getBytes(1));
      Assert.assertArrayEquals(b2, resultSet.getBytes(2));
      Assert.assertArrayEquals(b3, resultSet.getBytes(3));
      Assert.assertArrayEquals(b4, resultSet.getBytes(4));
      Assert.assertArrayEquals(b5, resultSet.getBytes(5));
      Assert.assertArrayEquals(b6, resultSet.getBytes(6));

      Assert.assertFalse(resultSet.next());
    }
  }

  @Test
  public void testVarBinaryPkSchema3() throws Exception {
    Properties props = PropertiesUtil.deepCopy(TEST_PROPERTIES);
    final String tableName = generateUniqueName();
    try (Connection conn = DriverManager.getConnection(getUrl(), props)) {
      conn.createStatement().execute("CREATE TABLE " + tableName
          + " (PK1 VARBINARY_ENCODED, PK2 VARBINARY_ENCODED, PK3 VARBINARY_ENCODED,"
          + " COL1 VARBINARY_ENCODED, COL2 VARBINARY_ENCODED,"
          + " COL3 VARBINARY_ENCODED CONSTRAINT pk PRIMARY KEY(PK1 DESC, PK2 ASC, PK3 DESC)) "
          + tableDDLOptions);

      byte[] b1 = new byte[] {1, 1, 19, -28, 24, 1, 1, -11, -21, 1};
      byte[] b2 = new byte[] {57, -83, 2, 83, -7, 12, -13, 4};
      byte[] b3 = new byte[] {4, 34, -19, 8, -73, 3, 4, 23};
      byte[] b4 = new byte[] {56, 50, 19, 34, 83, -101, -102, 91};
      byte[] b5 = new byte[] {10, 55, 19, -5, -34, 0, 0, 0, 0, 1};
      byte[] b6 = new byte[] {-11, 55, -119, 8, 0, 1, 2, -4, 33};

      byte[] b10 = new byte[] {1, 1, 19, -28, 25, -1, 1, -11, -21, -1};
      byte[] b20 = new byte[] {57, -83, -2, 83, 0, -7, -12, -13, 4};
      byte[] b30 = new byte[] {4, 1, -19, 8, 0, -73, 3, 4, 23};
      byte[] b40 = new byte[] {56, 50, 19, 34, 83, -101, -102, 91};
      byte[] b50 = new byte[] {10, 55, 19, -5, -34, 0, 0, 0, 0, 1};
      byte[] b60 = new byte[] {-11, 55, -119, 8, 0, 1, 2, -4, 33};

      byte[] b11 = new byte[] {1, 1, 20, -28, 0, -1, 0, -11, -21, -1};
      byte[] b21 = new byte[] {57, -83, 0, -2, 0, -7, -12, -13, 4};
      byte[] b31 = new byte[] {4, 1, 0, 0, 0, 73, 3, 0, 23};
      byte[] b41 = new byte[] {56, 50, 19, 0, 34, 83, -101, -102, 91};
      byte[] b51 = new byte[] {10, 55, 0, 19, -5, -34, 0, 0, 0, 0, 1};
      byte[] b61 = new byte[] {-11, 55, -119, 0, 8, 0, 1, 2, -4, 33};

      try (PreparedStatement preparedStatement = conn.prepareStatement("UPSERT INTO " + tableName
          + "(PK1, PK2, PK3, COL1, COL2, COL3) VALUES (?, ?, ?, ?, ?, ?)")) {
        upsertRow(preparedStatement, Bytes.toBytes("pk1-ehgir4jf"), Bytes.toBytes("pk22p0jfdkhrgi"),
            Bytes.toBytes("pk33ogjirhhf"), Bytes.toBytes("col19fnbb0hf0t"),
            Bytes.toBytes("col21048rnbfpe3-"), Bytes.toBytes("col319efnrugifj"));

        upsertRow(preparedStatement, b10, b20, b30, b40, b50, b60);

        upsertRow(preparedStatement, b1, b2, b3, b4, b5, b6);

        upsertRow(preparedStatement, b11, b21, b31, b41, b51, b61);
      }
      conn.commit();

      ResultSet resultSet = conn.createStatement().executeQuery("SELECT * FROM " + tableName);

      Assert.assertTrue(resultSet.next());

      Assert.assertArrayEquals(Bytes.toBytes("pk1-ehgir4jf"), resultSet.getBytes(1));
      Assert.assertArrayEquals(Bytes.toBytes("pk22p0jfdkhrgi"), resultSet.getBytes(2));
      Assert.assertArrayEquals(Bytes.toBytes("pk33ogjirhhf"), resultSet.getBytes(3));
      Assert.assertArrayEquals(Bytes.toBytes("col19fnbb0hf0t"), resultSet.getBytes(4));
      Assert.assertArrayEquals(Bytes.toBytes("col21048rnbfpe3-"), resultSet.getBytes(5));
      Assert.assertArrayEquals(Bytes.toBytes("col319efnrugifj"), resultSet.getBytes(6));

      Assert.assertTrue(resultSet.next());

      Assert.assertArrayEquals(b11, resultSet.getBytes(1));
      Assert.assertArrayEquals(b21, resultSet.getBytes(2));
      Assert.assertArrayEquals(b31, resultSet.getBytes(3));
      Assert.assertArrayEquals(b41, resultSet.getBytes(4));
      Assert.assertArrayEquals(b51, resultSet.getBytes(5));
      Assert.assertArrayEquals(b61, resultSet.getBytes(6));

      Assert.assertTrue(resultSet.next());

      Assert.assertArrayEquals(b10, resultSet.getBytes(1));
      Assert.assertArrayEquals(b20, resultSet.getBytes(2));
      Assert.assertArrayEquals(b30, resultSet.getBytes(3));
      Assert.assertArrayEquals(b40, resultSet.getBytes(4));
      Assert.assertArrayEquals(b50, resultSet.getBytes(5));
      Assert.assertArrayEquals(b60, resultSet.getBytes(6));

      Assert.assertTrue(resultSet.next());

      Assert.assertArrayEquals(b1, resultSet.getBytes(1));
      Assert.assertArrayEquals(b2, resultSet.getBytes(2));
      Assert.assertArrayEquals(b3, resultSet.getBytes(3));
      Assert.assertArrayEquals(b4, resultSet.getBytes(4));
      Assert.assertArrayEquals(b5, resultSet.getBytes(5));
      Assert.assertArrayEquals(b6, resultSet.getBytes(6));

      Assert.assertFalse(resultSet.next());
    }
  }

  @Test
  public void testVarBinaryPkSchema4() throws Exception {
    Properties props = PropertiesUtil.deepCopy(TEST_PROPERTIES);
    final String tableName = generateUniqueName();
    try (Connection conn = DriverManager.getConnection(getUrl(), props)) {
      conn.createStatement().execute("CREATE TABLE " + tableName
          + " (PK1 VARBINARY_ENCODED, PK2 VARCHAR, PK3 VARBINARY_ENCODED,"
          + " COL1 VARBINARY_ENCODED, COL2 VARBINARY_ENCODED,"
          + " COL3 VARBINARY_ENCODED CONSTRAINT pk PRIMARY KEY(PK1, PK2, PK3)) "
          + tableDDLOptions);

      byte[] b1 = new byte[] {1, 1, 19, -28, 24, 1, 1, -11, -21, 1};
      String b2 = "OfMOvIIuXZddTZ0VOkyAlPhdm";
      byte[] b3 = new byte[] {4, 34, -19, 8, -73, 3, 4, 23};
      byte[] b4 = new byte[] {56, 50, 19, 34, 83, -101, -102, 91};
      byte[] b5 = new byte[] {10, 55, 19, -5, -34, 0, 0, 0, 0, 1};
      byte[] b6 = new byte[] {-11, 55, -119, 8, 0, 1, 2, -4, 33};

      byte[] b10 = new byte[] {1, 1, 19, -28, 25, -1, 1, -11, -21, -1};
      String b20 = "OfMOpvIIuXZddTZ0VOkyAlPhdm";
      byte[] b30 = new byte[] {4, 1, -19, 8, 0, -73, 3, 4, 23};
      byte[] b40 = new byte[] {56, 50, 19, 34, 83, -101, -102, 91};
      byte[] b50 = new byte[] {10, 55, 19, -5, -34, 0, 0, 0, 0, 1};
      byte[] b60 = new byte[] {-11, 55, -119, 8, 0, 1, 2, -4, 33};

      byte[] b11 = new byte[] {1, 1, 20, -28, 0, -1, 0, -11, -21, -1};
      String b21 = "OfMOqvIIuXZddTZ0VOkyAlPhdm";
      byte[] b31 = new byte[] {4, 1, 0, 0, 0, 73, 3, 0, 23};
      byte[] b41 = new byte[] {56, 50, 19, 0, 34, 83, -101, -102, 91};
      byte[] b51 = new byte[] {10, 55, 0, 19, -5, -34, 0, 0, 0, 0, 1};
      byte[] b61 = new byte[] {-11, 55, -119, 0, 8, 0, 1, 2, -4, 33};

      byte[] b12 = new byte[] {1, 1, 20, -28, 0, -1, 0, -11, -21, -1};
      String b22 = "OfMOqvIIuXZddTZ0VOkyAlPhdma";
      byte[] b32 = new byte[] {4, 1, 75, 0, 0, 73, 0, -24, 3, 0, 12, 99, 23};
      byte[] b42 = new byte[] {56, 50, 19, 0, 34, 83, -101, -102, 91};
      byte[] b52 = new byte[] {10, 55, 0, 19, -5, -34, 0, 0, 0, 0, 1};
      byte[] b62 = new byte[] {-11, 55, -119, 0, 8, 0, 1, 2, -4, 33};

      byte[] b13 = new byte[] {1, 1, 20, -28, 0, -1, 0, -11, -21, -1};
      String b23 = "OfMOqvIIuXZddTZ0VOkyAlPhdm";
      byte[] b33 = new byte[] {4, 1, 0, 0, 0, 0, 22, 122, 48, -121, 73, 3, 0, 23};
      byte[] b43 = new byte[] {56, 50, 19, 0, 34, 83, -101, -102, 91, 92};
      byte[] b53 = new byte[] {10, 55, 0, 19, -5, -34, 0, -12, 0, 0, 0, 1};
      byte[] b63 = new byte[] {-11, 55, -119, 0, 8, 0, 1, 2, -4, 33};

      byte[] b14 = new byte[] {1, 1, 20, -28, 0, -1, 0, -11, -21, -1};
      String b24 = null;
      byte[] b34 = new byte[] {5, 1, 0, 0, 0, 0, 22, 122, 48, -121, 73, 3, 0, 23};
      byte[] b44 = new byte[] {56, 50, 19, 0, 34, 83, -101, -102, 91, 92};
      byte[] b54 = new byte[] {10, 55, 0, 19, -5, -34, 0, -12, 0, 0, 0, 1};
      byte[] b64 = new byte[] {-11, 55, -119, 0, 8, 0, 1, 2, -4, 33};

      byte[] b15 = new byte[] {1, 1, 20, -28, 0, -1, 0, -11, -21, -1};
      String b25 = "OfMOqvIIuXZddTZ0VOkyAlPhdm";
      byte[] b35 = null;
      byte[] b45 = new byte[] {56, 50, 19, 0, 34, 83, -101, -102, 91, 92};
      byte[] b55 = new byte[] {10, 55, 0, 19, -5, -34, 0, -12, 0, 0, 0, 1};
      byte[] b65 = new byte[] {-11, 55, -119, 0, 8, 0, 1, 2, -4, 33};

      try (PreparedStatement preparedStatement = conn.prepareStatement("UPSERT INTO " + tableName
          + "(PK1, PK2, PK3, COL1, COL2, COL3) VALUES (?, ?, ?, ?, ?, ?)")) {
        upsertRow(preparedStatement, Bytes.toBytes("pk1-ehgir4jf"), "pk22p0jfdkhrgi",
            Bytes.toBytes("pk33ogjirhhf"), Bytes.toBytes("col19fnbb0hf0t"),
            Bytes.toBytes("col21048rnbfpe3-"), Bytes.toBytes("col319efnrugifj"));
        upsertRow(preparedStatement, b10, b20, b30, b40, b50, b60);
        upsertRow(preparedStatement, b1, b2, b3, b4, b5, b6);
        upsertRow(preparedStatement, b11, b21, b31, b41, b51, b61);
        upsertRow(preparedStatement, b12, b22, b32, b42, b52, b62);
        upsertRow(preparedStatement, b13, b23, b33, b43, b53, b63);
        upsertRow(preparedStatement, b14, b24, b34, b44, b54, b64);
        upsertRow(preparedStatement, b15, b25, b35, b45, b55, b65);
      }
      conn.commit();

      PreparedStatement pst = conn.prepareStatement("SELECT * FROM " + tableName);
      ResultSet resultSet = pst.executeQuery();

      Assert.assertTrue(resultSet.next());

      Assert.assertArrayEquals(b1, resultSet.getBytes(1));
      Assert.assertEquals(b2, resultSet.getString(2));
      Assert.assertArrayEquals(b3, resultSet.getBytes(3));
      Assert.assertArrayEquals(b4, resultSet.getBytes(4));
      Assert.assertArrayEquals(b5, resultSet.getBytes(5));
      Assert.assertArrayEquals(b6, resultSet.getBytes(6));

      Assert.assertTrue(resultSet.next());

      Assert.assertArrayEquals(b10, resultSet.getBytes(1));
      Assert.assertEquals(b20, resultSet.getString(2));
      Assert.assertArrayEquals(b30, resultSet.getBytes(3));
      Assert.assertArrayEquals(b40, resultSet.getBytes(4));
      Assert.assertArrayEquals(b50, resultSet.getBytes(5));
      Assert.assertArrayEquals(b60, resultSet.getBytes(6));

      Assert.assertTrue(resultSet.next());

      Assert.assertArrayEquals(b14, resultSet.getBytes(1));
      Assert.assertNull(resultSet.getBytes(2));
      Assert.assertArrayEquals(b34, resultSet.getBytes(3));
      Assert.assertArrayEquals(b44, resultSet.getBytes(4));
      Assert.assertArrayEquals(b54, resultSet.getBytes(5));
      Assert.assertArrayEquals(b64, resultSet.getBytes(6));

      Assert.assertTrue(resultSet.next());

      Assert.assertArrayEquals(b15, resultSet.getBytes(1));
      Assert.assertEquals(b25, resultSet.getString(2));
      Assert.assertNull(resultSet.getBytes(3));
      Assert.assertArrayEquals(b45, resultSet.getBytes(4));
      Assert.assertArrayEquals(b55, resultSet.getBytes(5));
      Assert.assertArrayEquals(b65, resultSet.getBytes(6));

      Assert.assertTrue(resultSet.next());

      Assert.assertArrayEquals(b13, resultSet.getBytes(1));
      Assert.assertEquals(b23, resultSet.getString(2));
      Assert.assertArrayEquals(b33, resultSet.getBytes(3));
      Assert.assertArrayEquals(b43, resultSet.getBytes(4));
      Assert.assertArrayEquals(b53, resultSet.getBytes(5));
      Assert.assertArrayEquals(b63, resultSet.getBytes(6));

      Assert.assertTrue(resultSet.next());

      Assert.assertArrayEquals(b11, resultSet.getBytes(1));
      Assert.assertEquals(b21, resultSet.getString(2));
      Assert.assertArrayEquals(b31, resultSet.getBytes(3));
      Assert.assertArrayEquals(b41, resultSet.getBytes(4));
      Assert.assertArrayEquals(b51, resultSet.getBytes(5));
      Assert.assertArrayEquals(b61, resultSet.getBytes(6));

      Assert.assertTrue(resultSet.next());

      Assert.assertArrayEquals(b12, resultSet.getBytes(1));
      Assert.assertEquals(b22, resultSet.getString(2));
      Assert.assertArrayEquals(b32, resultSet.getBytes(3));
      Assert.assertArrayEquals(b42, resultSet.getBytes(4));
      Assert.assertArrayEquals(b52, resultSet.getBytes(5));
      Assert.assertArrayEquals(b62, resultSet.getBytes(6));

      Assert.assertTrue(resultSet.next());

      Assert.assertArrayEquals(Bytes.toBytes("pk1-ehgir4jf"), resultSet.getBytes(1));
      Assert.assertEquals("pk22p0jfdkhrgi", resultSet.getString(2));
      Assert.assertArrayEquals(Bytes.toBytes("pk33ogjirhhf"), resultSet.getBytes(3));
      Assert.assertArrayEquals(Bytes.toBytes("col19fnbb0hf0t"), resultSet.getBytes(4));
      Assert.assertArrayEquals(Bytes.toBytes("col21048rnbfpe3-"), resultSet.getBytes(5));
      Assert.assertArrayEquals(Bytes.toBytes("col319efnrugifj"), resultSet.getBytes(6));

      Assert.assertFalse(resultSet.next());

      PreparedStatement preparedStatement =
          conn.prepareStatement("SELECT * FROM " + tableName + " WHERE PK1 = ?");

      preparedStatement.setBytes(1, b11);
      resultSet = preparedStatement.executeQuery();

      Assert.assertTrue(resultSet.next());

      Assert.assertArrayEquals(b14, resultSet.getBytes(1));
      Assert.assertNull(resultSet.getBytes(2));
      Assert.assertArrayEquals(b34, resultSet.getBytes(3));
      Assert.assertArrayEquals(b44, resultSet.getBytes(4));
      Assert.assertArrayEquals(b54, resultSet.getBytes(5));
      Assert.assertArrayEquals(b64, resultSet.getBytes(6));

      Assert.assertTrue(resultSet.next());

      Assert.assertArrayEquals(b15, resultSet.getBytes(1));
      Assert.assertEquals(b25, resultSet.getString(2));
      Assert.assertNull(resultSet.getBytes(3));
      Assert.assertArrayEquals(b45, resultSet.getBytes(4));
      Assert.assertArrayEquals(b55, resultSet.getBytes(5));
      Assert.assertArrayEquals(b65, resultSet.getBytes(6));

      Assert.assertTrue(resultSet.next());

      Assert.assertArrayEquals(b13, resultSet.getBytes(1));
      Assert.assertEquals(b23, resultSet.getString(2));
      Assert.assertArrayEquals(b33, resultSet.getBytes(3));
      Assert.assertArrayEquals(b43, resultSet.getBytes(4));
      Assert.assertArrayEquals(b53, resultSet.getBytes(5));
      Assert.assertArrayEquals(b63, resultSet.getBytes(6));

      Assert.assertTrue(resultSet.next());

      Assert.assertArrayEquals(b11, resultSet.getBytes(1));
      Assert.assertEquals(b21, resultSet.getString(2));
      Assert.assertArrayEquals(b31, resultSet.getBytes(3));
      Assert.assertArrayEquals(b41, resultSet.getBytes(4));
      Assert.assertArrayEquals(b51, resultSet.getBytes(5));
      Assert.assertArrayEquals(b61, resultSet.getBytes(6));

      Assert.assertTrue(resultSet.next());

      Assert.assertArrayEquals(b12, resultSet.getBytes(1));
      Assert.assertEquals(b22, resultSet.getString(2));
      Assert.assertArrayEquals(b32, resultSet.getBytes(3));
      Assert.assertArrayEquals(b42, resultSet.getBytes(4));
      Assert.assertArrayEquals(b52, resultSet.getBytes(5));
      Assert.assertArrayEquals(b62, resultSet.getBytes(6));

      Assert.assertFalse(resultSet.next());

      preparedStatement = conn.prepareStatement(
          "SELECT * FROM " + tableName + " WHERE PK1 = ? AND PK2 IS NOT NULL ");

      preparedStatement.setBytes(1, b11);
      resultSet = preparedStatement.executeQuery();

      Assert.assertTrue(resultSet.next());

      Assert.assertArrayEquals(b15, resultSet.getBytes(1));
      Assert.assertEquals(b25, resultSet.getString(2));
      Assert.assertNull(resultSet.getBytes(3));
      Assert.assertArrayEquals(b45, resultSet.getBytes(4));
      Assert.assertArrayEquals(b55, resultSet.getBytes(5));
      Assert.assertArrayEquals(b65, resultSet.getBytes(6));

      Assert.assertTrue(resultSet.next());

      Assert.assertArrayEquals(b13, resultSet.getBytes(1));
      Assert.assertEquals(b23, resultSet.getString(2));
      Assert.assertArrayEquals(b33, resultSet.getBytes(3));
      Assert.assertArrayEquals(b43, resultSet.getBytes(4));
      Assert.assertArrayEquals(b53, resultSet.getBytes(5));
      Assert.assertArrayEquals(b63, resultSet.getBytes(6));

      Assert.assertTrue(resultSet.next());

      Assert.assertArrayEquals(b11, resultSet.getBytes(1));
      Assert.assertEquals(b21, resultSet.getString(2));
      Assert.assertArrayEquals(b31, resultSet.getBytes(3));
      Assert.assertArrayEquals(b41, resultSet.getBytes(4));
      Assert.assertArrayEquals(b51, resultSet.getBytes(5));
      Assert.assertArrayEquals(b61, resultSet.getBytes(6));

      Assert.assertTrue(resultSet.next());

      Assert.assertArrayEquals(b12, resultSet.getBytes(1));
      Assert.assertEquals(b22, resultSet.getString(2));
      Assert.assertArrayEquals(b32, resultSet.getBytes(3));
      Assert.assertArrayEquals(b42, resultSet.getBytes(4));
      Assert.assertArrayEquals(b52, resultSet.getBytes(5));
      Assert.assertArrayEquals(b62, resultSet.getBytes(6));

      Assert.assertFalse(resultSet.next());

      preparedStatement =
          conn.prepareStatement("SELECT * FROM " + tableName + " WHERE PK1 = ? AND PK2 = ?");
      preparedStatement.setBytes(1, b11);
      preparedStatement.setString(2, b21);
      resultSet = preparedStatement.executeQuery();

      Assert.assertTrue(resultSet.next());

      Assert.assertArrayEquals(b15, resultSet.getBytes(1));
      Assert.assertEquals(b25, resultSet.getString(2));
      Assert.assertNull(resultSet.getBytes(3));
      Assert.assertArrayEquals(b45, resultSet.getBytes(4));
      Assert.assertArrayEquals(b55, resultSet.getBytes(5));
      Assert.assertArrayEquals(b65, resultSet.getBytes(6));

      Assert.assertTrue(resultSet.next());

      Assert.assertArrayEquals(b13, resultSet.getBytes(1));
      Assert.assertEquals(b23, resultSet.getString(2));
      Assert.assertArrayEquals(b33, resultSet.getBytes(3));
      Assert.assertArrayEquals(b43, resultSet.getBytes(4));
      Assert.assertArrayEquals(b53, resultSet.getBytes(5));
      Assert.assertArrayEquals(b63, resultSet.getBytes(6));

      Assert.assertTrue(resultSet.next());

      Assert.assertArrayEquals(b11, resultSet.getBytes(1));
      Assert.assertEquals(b21, resultSet.getString(2));
      Assert.assertArrayEquals(b31, resultSet.getBytes(3));
      Assert.assertArrayEquals(b41, resultSet.getBytes(4));
      Assert.assertArrayEquals(b51, resultSet.getBytes(5));
      Assert.assertArrayEquals(b61, resultSet.getBytes(6));

      Assert.assertFalse(resultSet.next());

      preparedStatement = conn.prepareStatement("SELECT * FROM " + tableName
          + " WHERE PK1 = ? AND PK2 BETWEEN ? AND ? AND PK3 IS NOT NULL");
      preparedStatement.setBytes(1, b11);
      preparedStatement.setString(2, "OfMOqvIIuXZddTZ0VOkyAlPhcAB193hfo");
      preparedStatement.setString(3, "OfMOqvIIuXZddTZ0VOkyAlPhdm");
      resultSet = preparedStatement.executeQuery();

      Assert.assertTrue(resultSet.next());

      Assert.assertArrayEquals(b13, resultSet.getBytes(1));
      Assert.assertEquals(b23, resultSet.getString(2));
      Assert.assertArrayEquals(b33, resultSet.getBytes(3));
      Assert.assertArrayEquals(b43, resultSet.getBytes(4));
      Assert.assertArrayEquals(b53, resultSet.getBytes(5));
      Assert.assertArrayEquals(b63, resultSet.getBytes(6));

      Assert.assertTrue(resultSet.next());

      Assert.assertArrayEquals(b11, resultSet.getBytes(1));
      Assert.assertEquals(b21, resultSet.getString(2));
      Assert.assertArrayEquals(b31, resultSet.getBytes(3));
      Assert.assertArrayEquals(b41, resultSet.getBytes(4));
      Assert.assertArrayEquals(b51, resultSet.getBytes(5));
      Assert.assertArrayEquals(b61, resultSet.getBytes(6));

      Assert.assertFalse(resultSet.next());

      preparedStatement = conn.prepareStatement("SELECT * FROM " + tableName
          + " WHERE PK1 = ? AND PK2 IN (?, ?, ?)");
      preparedStatement.setBytes(1, b11);
      preparedStatement.setString(2, b21);
      preparedStatement.setString(3, b23);
      preparedStatement.setString(4, b22);
      resultSet = preparedStatement.executeQuery();

      Assert.assertTrue(resultSet.next());

      Assert.assertArrayEquals(b15, resultSet.getBytes(1));
      Assert.assertEquals(b25, resultSet.getString(2));
      Assert.assertArrayEquals(b35, resultSet.getBytes(3));
      Assert.assertArrayEquals(b45, resultSet.getBytes(4));
      Assert.assertArrayEquals(b55, resultSet.getBytes(5));
      Assert.assertArrayEquals(b65, resultSet.getBytes(6));

      Assert.assertTrue(resultSet.next());

      Assert.assertArrayEquals(b13, resultSet.getBytes(1));
      Assert.assertEquals(b23, resultSet.getString(2));
      Assert.assertArrayEquals(b33, resultSet.getBytes(3));
      Assert.assertArrayEquals(b43, resultSet.getBytes(4));
      Assert.assertArrayEquals(b53, resultSet.getBytes(5));
      Assert.assertArrayEquals(b63, resultSet.getBytes(6));

      Assert.assertTrue(resultSet.next());

      Assert.assertArrayEquals(b11, resultSet.getBytes(1));
      Assert.assertEquals(b21, resultSet.getString(2));
      Assert.assertArrayEquals(b31, resultSet.getBytes(3));
      Assert.assertArrayEquals(b41, resultSet.getBytes(4));
      Assert.assertArrayEquals(b51, resultSet.getBytes(5));
      Assert.assertArrayEquals(b61, resultSet.getBytes(6));

      Assert.assertTrue(resultSet.next());

      Assert.assertArrayEquals(b12, resultSet.getBytes(1));
      Assert.assertEquals(b22, resultSet.getString(2));
      Assert.assertArrayEquals(b32, resultSet.getBytes(3));
      Assert.assertArrayEquals(b42, resultSet.getBytes(4));
      Assert.assertArrayEquals(b52, resultSet.getBytes(5));
      Assert.assertArrayEquals(b62, resultSet.getBytes(6));

      Assert.assertFalse(resultSet.next());

      preparedStatement = conn.prepareStatement(
          "SELECT * FROM " + tableName + " WHERE PK1 = ? AND PK2 = ? AND PK3 = ?");
      preparedStatement.setBytes(1, b11);
      preparedStatement.setString(2, b21);
      preparedStatement.setBytes(3, b31);
      resultSet = preparedStatement.executeQuery();

      Assert.assertTrue(resultSet.next());

      Assert.assertArrayEquals(b11, resultSet.getBytes(1));
      Assert.assertEquals(b21, resultSet.getString(2));
      Assert.assertArrayEquals(b31, resultSet.getBytes(3));
      Assert.assertArrayEquals(b41, resultSet.getBytes(4));
      Assert.assertArrayEquals(b51, resultSet.getBytes(5));
      Assert.assertArrayEquals(b61, resultSet.getBytes(6));

      Assert.assertFalse(resultSet.next());

      preparedStatement = conn.prepareStatement(
          "SELECT * FROM " + tableName + " WHERE PK1 = ? AND PK2 = ? AND PK3 IS NULL");
      preparedStatement.setBytes(1, b11);
      preparedStatement.setString(2, b21);
      resultSet = preparedStatement.executeQuery();

      Assert.assertTrue(resultSet.next());

      Assert.assertArrayEquals(b15, resultSet.getBytes(1));
      Assert.assertEquals(b25, resultSet.getString(2));
      Assert.assertNull(resultSet.getBytes(3));
      Assert.assertArrayEquals(b45, resultSet.getBytes(4));
      Assert.assertArrayEquals(b55, resultSet.getBytes(5));
      Assert.assertArrayEquals(b65, resultSet.getBytes(6));

      Assert.assertFalse(resultSet.next());
    }
  }

  @Test
  public void testVarBinaryPkSchema5() throws Exception {
    Properties props = PropertiesUtil.deepCopy(TEST_PROPERTIES);
    final String tableName = generateUniqueName();
    try (Connection conn = DriverManager.getConnection(getUrl(), props)) {
      conn.createStatement().execute("CREATE TABLE " + tableName
          + " (PK1 VARCHAR, PK2 VARBINARY_ENCODED, PK3 VARCHAR,"
          + " COL1 VARBINARY_ENCODED, COL2 VARBINARY_ENCODED,"
          + " COL3 VARBINARY_ENCODED CONSTRAINT pk PRIMARY KEY(PK1, PK2, PK3)) "
          + tableDDLOptions);

      String b1 = "Rq1MxfBzM8DaPIjTamS94s9KbaC098Tou4V7kj4nQJ7YwiNBmfS5lg1WGE83s1z0";
      byte[] b2 = new byte[] {57, -83, 2, 83, -7, 12, -13, 4};
      String b3 = "RTHsOtc26ErkbgPDtTvsQl9M0fLOGQ6b";
      byte[] b4 = new byte[] {56, 50, 19, 34, 83, -101, -102, 91};
      byte[] b5 = new byte[] {10, 55, 19, -5, -34, 0, 0, 0, 0, 1};
      byte[] b6 = new byte[] {-11, 55, -119, 8, 0, 1, 2, -4, 33};

      String b10 = "Rq1MxfBzM8DaPIjTamS94s9KbaC098Uou4V7kj4nQJ7YwiNBmfS5lg1WGE83s1z0";
      byte[] b20 = new byte[] {57, -83, -2, 83, 0, -7, -12, -13, 4};
      String b30 = "YfDgzIC56WYUzouTGGISJdB4egYpmqbt";
      byte[] b40 = new byte[] {56, 50, 19, 34, 83, -101, -102, 91};
      byte[] b50 = new byte[] {10, 55, 19, -5, -34, 0, 0, 0, 0, 1};
      byte[] b60 = new byte[] {-11, 55, -119, 8, 0, 1, 2, -4, 33};

      String b11 = "Rq1MxfBzM8HDaPIjTamS94s9KbaC098Uou4V7kj4nQJ7YwiNBmfS5lg1WGE83s1z0";
      byte[] b21 = new byte[] {57, -83, 0, -2, 0, -7, -12, -13, 4};
      String b31 = "waWm2saGCnR8uE9fd0kww23947fu9@#54zGVOHoE0w4";
      byte[] b41 = new byte[] {56, 50, 19, 0, 34, 83, -101, -102, 91};
      byte[] b51 = new byte[] {10, 55, 0, 19, -5, -34, 0, 0, 0, 0, 1};
      byte[] b61 = new byte[] {-11, 55, -119, 0, 8, 0, 1, 2, -4, 33};

      String b12 = "Rq1MxfBzM8HDaPIjTamS94s9KbaC098Uou4V7kj4nQJ7YwiNBmfS5lg1WGE83s1z0";
      byte[] b22 = new byte[] {57, -83, 0, -2, 0, -7, -12, -13, 4, 0};
      String b32 = "u4V7kj4nQJ7YwiNBmfS5lg1WGE83s1z0";
      byte[] b42 = new byte[] {56, 50, 19, 0, 34, 83, -101, -102, 91};
      byte[] b52 = new byte[] {10, 55, 0, 19, -5, -34, 0, 0, 0, 0, 1};
      byte[] b62 = new byte[] {-11, 55, -119, 0, 8, 0, 1, 2, -4, 33};

      String b13 = "Rq1MxfBzM8HDaPIjTamS94s9KbaC098Uou4V7kj4nQJ7YwiNBmfS5lg1WGE83s1z0";
      byte[] b23 = new byte[] {57, -83, 0, -2, 0, -7, -12, -13, 4};
      String b33 = "fV5EeHtPbrEd92iNh7dnVJctScCT2jvP";
      byte[] b43 = new byte[] {56, 50, 19, 0, 34, 83, -101, -102, 91, 92};
      byte[] b53 = new byte[] {10, 55, 0, 19, -5, -34, 0, -12, 0, 0, 0, 1};
      byte[] b63 = new byte[] {-11, 55, -119, 0, 8, 0, 1, 2, -4, 33};

      String b14 = "Rq1MxfBzM8HDaPIjTamS94s9KbaC098Uou4V7kj4nQJ7YwiNBmfS5lg1WGE83s1z0";
      byte[] b24 = null;
      String b34 = "ifgTLSG0IlTqjmzN3rp0!@#%c93%^8Yu4d56NRNTHYbuR";
      byte[] b44 = new byte[] {56, 50, 19, 0, 34, 83, -101, -102, 91, 92};
      byte[] b54 = new byte[] {10, 55, 0, 19, -5, -34, 0, -12, 0, 0, 0, 1};
      byte[] b64 = new byte[] {-11, 55, -119, 0, 8, 0, 1, 2, -4, 33};

      String b15 = "Rq1MxfBzM8HDaPIjTamS94s9KbaC098Uou4V7kj4nQJ7YwiNBmfS5lg1WGE83s1z0";
      byte[] b25 = new byte[] {57, -83, 0, -2, 0, -7, -12, -13, 4};
      String b35 = null;
      byte[] b45 = new byte[] {56, 50, 19, 0, 34, 83, -101, -102, 91, 92};
      byte[] b55 = new byte[] {10, 55, 0, 19, -5, -34, 0, -12, 0, 0, 0, 1};
      byte[] b65 = new byte[] {-11, 55, -119, 0, 8, 0, 1, 2, -4, 33};

      try (PreparedStatement preparedStatement = conn.prepareStatement("UPSERT INTO " + tableName
          + "(PK1, PK2, PK3, COL1, COL2, COL3) VALUES (?, ?, ?, ?, ?, ?)")) {
        upsertRow(preparedStatement, "pk1-ehgir4jf", Bytes.toBytes("pk22p0jfdkhrgi"),
            "pk33ogjirhhf", Bytes.toBytes("col19fnbb0hf0t"),
            Bytes.toBytes("col21048rnbfpe3-"), Bytes.toBytes("col319efnrugifj"));
        upsertRow(preparedStatement, b10, b20, b30, b40, b50, b60);
        upsertRow(preparedStatement, b1, b2, b3, b4, b5, b6);
        upsertRow(preparedStatement, b11, b21, b31, b41, b51, b61);
        upsertRow(preparedStatement, b12, b22, b32, b42, b52, b62);
        upsertRow(preparedStatement, b13, b23, b33, b43, b53, b63);
        upsertRow(preparedStatement, b14, b24, b34, b44, b54, b64);
        upsertRow(preparedStatement, b15, b25, b35, b45, b55, b65);
      }
      conn.commit();

      PreparedStatement pst = conn.prepareStatement("SELECT * FROM " + tableName);
      ResultSet resultSet = pst.executeQuery();

      Assert.assertTrue(resultSet.next());

      Assert.assertEquals(b1, resultSet.getString(1));
      Assert.assertArrayEquals(b2, resultSet.getBytes(2));
      Assert.assertEquals(b3, resultSet.getString(3));
      Assert.assertArrayEquals(b4, resultSet.getBytes(4));
      Assert.assertArrayEquals(b5, resultSet.getBytes(5));
      Assert.assertArrayEquals(b6, resultSet.getBytes(6));

      Assert.assertTrue(resultSet.next());

      Assert.assertEquals(b10, resultSet.getString(1));
      Assert.assertArrayEquals(b20, resultSet.getBytes(2));
      Assert.assertEquals(b30, resultSet.getString(3));
      Assert.assertArrayEquals(b40, resultSet.getBytes(4));
      Assert.assertArrayEquals(b50, resultSet.getBytes(5));
      Assert.assertArrayEquals(b60, resultSet.getBytes(6));

      Assert.assertTrue(resultSet.next());

      Assert.assertEquals(b14, resultSet.getString(1));
      Assert.assertNull(resultSet.getBytes(2));
      Assert.assertEquals(b34, resultSet.getString(3));
      Assert.assertArrayEquals(b44, resultSet.getBytes(4));
      Assert.assertArrayEquals(b54, resultSet.getBytes(5));
      Assert.assertArrayEquals(b64, resultSet.getBytes(6));

      Assert.assertTrue(resultSet.next());

      Assert.assertEquals(b15, resultSet.getString(1));
      Assert.assertArrayEquals(b25, resultSet.getBytes(2));
      Assert.assertNull(resultSet.getBytes(3));
      Assert.assertArrayEquals(b45, resultSet.getBytes(4));
      Assert.assertArrayEquals(b55, resultSet.getBytes(5));
      Assert.assertArrayEquals(b65, resultSet.getBytes(6));

      Assert.assertTrue(resultSet.next());

      Assert.assertEquals(b13, resultSet.getString(1));
      Assert.assertArrayEquals(b23, resultSet.getBytes(2));
      Assert.assertEquals(b33, resultSet.getString(3));
      Assert.assertArrayEquals(b43, resultSet.getBytes(4));
      Assert.assertArrayEquals(b53, resultSet.getBytes(5));
      Assert.assertArrayEquals(b63, resultSet.getBytes(6));

      Assert.assertTrue(resultSet.next());

      Assert.assertEquals(b11, resultSet.getString(1));
      Assert.assertArrayEquals(b21, resultSet.getBytes(2));
      Assert.assertEquals(b31, resultSet.getString(3));
      Assert.assertArrayEquals(b41, resultSet.getBytes(4));
      Assert.assertArrayEquals(b51, resultSet.getBytes(5));
      Assert.assertArrayEquals(b61, resultSet.getBytes(6));

      Assert.assertTrue(resultSet.next());

      Assert.assertEquals(b12, resultSet.getString(1));
      Assert.assertArrayEquals(b22, resultSet.getBytes(2));
      Assert.assertEquals(b32, resultSet.getString(3));
      Assert.assertArrayEquals(b42, resultSet.getBytes(4));
      Assert.assertArrayEquals(b52, resultSet.getBytes(5));
      Assert.assertArrayEquals(b62, resultSet.getBytes(6));

      Assert.assertTrue(resultSet.next());

      Assert.assertEquals("pk1-ehgir4jf", resultSet.getString(1));
      Assert.assertArrayEquals(Bytes.toBytes("pk22p0jfdkhrgi"), resultSet.getBytes(2));
      Assert.assertEquals("pk33ogjirhhf", resultSet.getString(3));
      Assert.assertArrayEquals(Bytes.toBytes("col19fnbb0hf0t"), resultSet.getBytes(4));
      Assert.assertArrayEquals(Bytes.toBytes("col21048rnbfpe3-"), resultSet.getBytes(5));
      Assert.assertArrayEquals(Bytes.toBytes("col319efnrugifj"), resultSet.getBytes(6));

      Assert.assertFalse(resultSet.next());

      PreparedStatement preparedStatement =
          conn.prepareStatement("SELECT * FROM " + tableName + " WHERE PK1 = ?");

      preparedStatement.setString(1, b11);
      resultSet = preparedStatement.executeQuery();

      Assert.assertTrue(resultSet.next());

      Assert.assertEquals(b14, resultSet.getString(1));
      Assert.assertNull(resultSet.getBytes(2));
      Assert.assertEquals(b34, resultSet.getString(3));
      Assert.assertArrayEquals(b44, resultSet.getBytes(4));
      Assert.assertArrayEquals(b54, resultSet.getBytes(5));
      Assert.assertArrayEquals(b64, resultSet.getBytes(6));

      Assert.assertTrue(resultSet.next());

      Assert.assertEquals(b15, resultSet.getString(1));
      Assert.assertArrayEquals(b25, resultSet.getBytes(2));
      Assert.assertNull(resultSet.getBytes(3));
      Assert.assertArrayEquals(b45, resultSet.getBytes(4));
      Assert.assertArrayEquals(b55, resultSet.getBytes(5));
      Assert.assertArrayEquals(b65, resultSet.getBytes(6));

      Assert.assertTrue(resultSet.next());

      Assert.assertEquals(b13, resultSet.getString(1));
      Assert.assertArrayEquals(b23, resultSet.getBytes(2));
      Assert.assertEquals(b33, resultSet.getString(3));
      Assert.assertArrayEquals(b43, resultSet.getBytes(4));
      Assert.assertArrayEquals(b53, resultSet.getBytes(5));
      Assert.assertArrayEquals(b63, resultSet.getBytes(6));

      Assert.assertTrue(resultSet.next());

      Assert.assertEquals(b11, resultSet.getString(1));
      Assert.assertArrayEquals(b21, resultSet.getBytes(2));
      Assert.assertEquals(b31, resultSet.getString(3));
      Assert.assertArrayEquals(b41, resultSet.getBytes(4));
      Assert.assertArrayEquals(b51, resultSet.getBytes(5));
      Assert.assertArrayEquals(b61, resultSet.getBytes(6));

      Assert.assertTrue(resultSet.next());

      Assert.assertEquals(b12, resultSet.getString(1));
      Assert.assertArrayEquals(b22, resultSet.getBytes(2));
      Assert.assertEquals(b32, resultSet.getString(3));
      Assert.assertArrayEquals(b42, resultSet.getBytes(4));
      Assert.assertArrayEquals(b52, resultSet.getBytes(5));
      Assert.assertArrayEquals(b62, resultSet.getBytes(6));

      Assert.assertFalse(resultSet.next());

      preparedStatement = conn.prepareStatement(
          "SELECT * FROM " + tableName + " WHERE PK1 = ? AND PK2 IS NOT NULL ");

      preparedStatement.setString(1, b11);
      resultSet = preparedStatement.executeQuery();

      Assert.assertTrue(resultSet.next());

      Assert.assertEquals(b15, resultSet.getString(1));
      Assert.assertArrayEquals(b25, resultSet.getBytes(2));
      Assert.assertNull(resultSet.getBytes(3));
      Assert.assertArrayEquals(b45, resultSet.getBytes(4));
      Assert.assertArrayEquals(b55, resultSet.getBytes(5));
      Assert.assertArrayEquals(b65, resultSet.getBytes(6));

      Assert.assertTrue(resultSet.next());

      Assert.assertEquals(b13, resultSet.getString(1));
      Assert.assertArrayEquals(b23, resultSet.getBytes(2));
      Assert.assertEquals(b33, resultSet.getString(3));
      Assert.assertArrayEquals(b43, resultSet.getBytes(4));
      Assert.assertArrayEquals(b53, resultSet.getBytes(5));
      Assert.assertArrayEquals(b63, resultSet.getBytes(6));

      Assert.assertTrue(resultSet.next());

      Assert.assertEquals(b11, resultSet.getString(1));
      Assert.assertArrayEquals(b21, resultSet.getBytes(2));
      Assert.assertEquals(b31, resultSet.getString(3));
      Assert.assertArrayEquals(b41, resultSet.getBytes(4));
      Assert.assertArrayEquals(b51, resultSet.getBytes(5));
      Assert.assertArrayEquals(b61, resultSet.getBytes(6));

      Assert.assertTrue(resultSet.next());

      Assert.assertEquals(b12, resultSet.getString(1));
      Assert.assertArrayEquals(b22, resultSet.getBytes(2));
      Assert.assertEquals(b32, resultSet.getString(3));
      Assert.assertArrayEquals(b42, resultSet.getBytes(4));
      Assert.assertArrayEquals(b52, resultSet.getBytes(5));
      Assert.assertArrayEquals(b62, resultSet.getBytes(6));

      Assert.assertFalse(resultSet.next());

      preparedStatement =
          conn.prepareStatement("SELECT * FROM " + tableName + " WHERE PK1 = ? AND PK2 = ?");
      preparedStatement.setString(1, b11);
      preparedStatement.setBytes(2, b21);
      resultSet = preparedStatement.executeQuery();

      Assert.assertTrue(resultSet.next());

      Assert.assertEquals(b15, resultSet.getString(1));
      Assert.assertArrayEquals(b25, resultSet.getBytes(2));
      Assert.assertNull(resultSet.getBytes(3));
      Assert.assertArrayEquals(b45, resultSet.getBytes(4));
      Assert.assertArrayEquals(b55, resultSet.getBytes(5));
      Assert.assertArrayEquals(b65, resultSet.getBytes(6));

      Assert.assertTrue(resultSet.next());

      Assert.assertEquals(b13, resultSet.getString(1));
      Assert.assertArrayEquals(b23, resultSet.getBytes(2));
      Assert.assertEquals(b33, resultSet.getString(3));
      Assert.assertArrayEquals(b43, resultSet.getBytes(4));
      Assert.assertArrayEquals(b53, resultSet.getBytes(5));
      Assert.assertArrayEquals(b63, resultSet.getBytes(6));

      Assert.assertTrue(resultSet.next());

      Assert.assertEquals(b11, resultSet.getString(1));
      Assert.assertArrayEquals(b21, resultSet.getBytes(2));
      Assert.assertEquals(b31, resultSet.getString(3));
      Assert.assertArrayEquals(b41, resultSet.getBytes(4));
      Assert.assertArrayEquals(b51, resultSet.getBytes(5));
      Assert.assertArrayEquals(b61, resultSet.getBytes(6));

      Assert.assertFalse(resultSet.next());

      preparedStatement = conn.prepareStatement("SELECT * FROM " + tableName
          + " WHERE PK1 = ? AND PK2 BETWEEN ? AND ? AND PK3 IS NOT NULL");
      preparedStatement.setString(1, b11);
      preparedStatement.setBytes(2, new byte[] {57, -83, 0, -2, 0, -7, -12, -13, 3, 24, -121});
      preparedStatement.setBytes(3, new byte[] {57, -83, 0, -2, 0, -7, -12, -13, 4});
      resultSet = preparedStatement.executeQuery();

      Assert.assertTrue(resultSet.next());

      Assert.assertEquals(b13, resultSet.getString(1));
      Assert.assertArrayEquals(b23, resultSet.getBytes(2));
      Assert.assertEquals(b33, resultSet.getString(3));
      Assert.assertArrayEquals(b43, resultSet.getBytes(4));
      Assert.assertArrayEquals(b53, resultSet.getBytes(5));
      Assert.assertArrayEquals(b63, resultSet.getBytes(6));

      Assert.assertTrue(resultSet.next());

      Assert.assertEquals(b11, resultSet.getString(1));
      Assert.assertArrayEquals(b21, resultSet.getBytes(2));
      Assert.assertEquals(b31, resultSet.getString(3));
      Assert.assertArrayEquals(b41, resultSet.getBytes(4));
      Assert.assertArrayEquals(b51, resultSet.getBytes(5));
      Assert.assertArrayEquals(b61, resultSet.getBytes(6));

      Assert.assertFalse(resultSet.next());

      preparedStatement = conn.prepareStatement("SELECT * FROM " + tableName
          + " WHERE PK1 = ? AND PK2 IN (?, ?, ?)");
      preparedStatement.setString(1, b11);
      preparedStatement.setBytes(2, b21);
      preparedStatement.setBytes(3, b23);
      preparedStatement.setBytes(4, b22);
      resultSet = preparedStatement.executeQuery();

      Assert.assertTrue(resultSet.next());

      Assert.assertEquals(b15, resultSet.getString(1));
      Assert.assertArrayEquals(b25, resultSet.getBytes(2));
      Assert.assertEquals(b35, resultSet.getString(3));
      Assert.assertArrayEquals(b45, resultSet.getBytes(4));
      Assert.assertArrayEquals(b55, resultSet.getBytes(5));
      Assert.assertArrayEquals(b65, resultSet.getBytes(6));

      Assert.assertTrue(resultSet.next());

      Assert.assertEquals(b13, resultSet.getString(1));
      Assert.assertArrayEquals(b23, resultSet.getBytes(2));
      Assert.assertEquals(b33, resultSet.getString(3));
      Assert.assertArrayEquals(b43, resultSet.getBytes(4));
      Assert.assertArrayEquals(b53, resultSet.getBytes(5));
      Assert.assertArrayEquals(b63, resultSet.getBytes(6));

      Assert.assertTrue(resultSet.next());

      Assert.assertEquals(b11, resultSet.getString(1));
      Assert.assertArrayEquals(b21, resultSet.getBytes(2));
      Assert.assertEquals(b31, resultSet.getString(3));
      Assert.assertArrayEquals(b41, resultSet.getBytes(4));
      Assert.assertArrayEquals(b51, resultSet.getBytes(5));
      Assert.assertArrayEquals(b61, resultSet.getBytes(6));

      Assert.assertTrue(resultSet.next());

      Assert.assertEquals(b12, resultSet.getString(1));
      Assert.assertArrayEquals(b22, resultSet.getBytes(2));
      Assert.assertEquals(b32, resultSet.getString(3));
      Assert.assertArrayEquals(b42, resultSet.getBytes(4));
      Assert.assertArrayEquals(b52, resultSet.getBytes(5));
      Assert.assertArrayEquals(b62, resultSet.getBytes(6));

      Assert.assertFalse(resultSet.next());

      preparedStatement = conn.prepareStatement(
          "SELECT * FROM " + tableName + " WHERE PK1 = ? AND PK2 = ? AND PK3 = ?");
      preparedStatement.setString(1, b11);
      preparedStatement.setBytes(2, b21);
      preparedStatement.setString(3, b31);
      resultSet = preparedStatement.executeQuery();

      Assert.assertTrue(resultSet.next());

      Assert.assertEquals(b11, resultSet.getString(1));
      Assert.assertArrayEquals(b21, resultSet.getBytes(2));
      Assert.assertEquals(b31, resultSet.getString(3));
      Assert.assertArrayEquals(b41, resultSet.getBytes(4));
      Assert.assertArrayEquals(b51, resultSet.getBytes(5));
      Assert.assertArrayEquals(b61, resultSet.getBytes(6));

      Assert.assertFalse(resultSet.next());

      preparedStatement = conn.prepareStatement(
          "SELECT * FROM " + tableName + " WHERE PK1 = ? AND PK2 = ? AND PK3 IS NULL");
      preparedStatement.setString(1, b11);
      preparedStatement.setBytes(2, b21);
      resultSet = preparedStatement.executeQuery();

      Assert.assertTrue(resultSet.next());

      Assert.assertEquals(b15, resultSet.getString(1));
      Assert.assertArrayEquals(b25, resultSet.getBytes(2));
      Assert.assertNull(resultSet.getBytes(3));
      Assert.assertArrayEquals(b45, resultSet.getBytes(4));
      Assert.assertArrayEquals(b55, resultSet.getBytes(5));
      Assert.assertArrayEquals(b65, resultSet.getBytes(6));

      Assert.assertFalse(resultSet.next());
    }
  }

  @Test
  public void testVarBinaryPkSchema6() throws Exception {
    Properties props = PropertiesUtil.deepCopy(TEST_PROPERTIES);
    final String tableName = generateUniqueName();
    try (Connection conn = DriverManager.getConnection(getUrl(), props)) {
      conn.createStatement().execute("CREATE TABLE " + tableName
          + " (PK1 VARBINARY_ENCODED, PK2 DOUBLE NOT NULL, PK3 VARBINARY_ENCODED,"
          + " COL1 VARBINARY_ENCODED, COL2 VARBINARY_ENCODED,"
          + " COL3 VARBINARY_ENCODED CONSTRAINT pk PRIMARY KEY(PK1, PK2, PK3)) "
          + tableDDLOptions);

      byte[] b1 = new byte[] {1, 1, 19, -28, 24, 1, 1, -11, -21, 1};
      double b2 = 4148316.50906;
      byte[] b3 = new byte[] {4, 34, -19, 8, -73, 3, 4, 23};
      byte[] b4 = new byte[] {56, 50, 19, 34, 83, -101, -102, 91};
      byte[] b5 = new byte[] {10, 55, 19, -5, -34, 0, 0, 0, 0, 1};
      byte[] b6 = new byte[] {-11, 55, -119, 8, 0, 1, 2, -4, 33};

      byte[] b10 = new byte[] {1, 1, 19, -28, 25, -1, 1, -11, -21, -1};
      double b20 = 3479039.03887;
      byte[] b30 = new byte[] {4, 1, -19, 8, 0, -73, 3, 4, 23};
      byte[] b40 = new byte[] {56, 50, 19, 34, 83, -101, -102, 91};
      byte[] b50 = new byte[] {10, 55, 19, -5, -34, 0, 0, 0, 0, 1};
      byte[] b60 = new byte[] {-11, 55, -119, 8, 0, 1, 2, -4, 33};

      byte[] b11 = new byte[] {1, 1, 20, -28, 0, -1, 0, -11, -21, -1};
      double b21 = 3579039.03887;
      byte[] b31 = new byte[] {4, 1, 0, 0, 0, 73, 3, 0, 23};
      byte[] b41 = new byte[] {56, 50, 19, 0, 34, 83, -101, -102, 91};
      byte[] b51 = new byte[] {10, 55, 0, 19, -5, -34, 0, 0, 0, 0, 1};
      byte[] b61 = new byte[] {-11, 55, -119, 0, 8, 0, 1, 2, -4, 33};

      byte[] b12 = new byte[] {1, 1, 20, -28, 0, -1, 0, -11, -21, -1};
      double b22 = 3579039.038871;
      byte[] b32 = new byte[] {4, 1, 75, 0, 0, 73, 0, -24, 3, 0, 12, 99, 23};
      byte[] b42 = new byte[] {56, 50, 19, 0, 34, 83, -101, -102, 91};
      byte[] b52 = new byte[] {10, 55, 0, 19, -5, -34, 0, 0, 0, 0, 1};
      byte[] b62 = new byte[] {-11, 55, -119, 0, 8, 0, 1, 2, -4, 33};

      byte[] b13 = new byte[] {1, 1, 20, -28, 0, -1, 0, -11, -21, -1};
      double b23 = 3579039.03887;
      byte[] b33 = new byte[] {4, 1, 0, 0, 0, 0, 22, 122, 48, -121, 73, 3, 0, 23};
      byte[] b43 = new byte[] {56, 50, 19, 0, 34, 83, -101, -102, 91, 92};
      byte[] b53 = new byte[] {10, 55, 0, 19, -5, -34, 0, -12, 0, 0, 0, 1};
      byte[] b63 = new byte[] {-11, 55, -119, 0, 8, 0, 1, 2, -4, 33};

      byte[] b15 = new byte[] {1, 1, 20, -28, 0, -1, 0, -11, -21, -1};
      double b25 = 3579039.03887;
      byte[] b35 = null;
      byte[] b45 = new byte[] {56, 50, 19, 0, 34, 83, -101, -102, 91, 92};
      byte[] b55 = new byte[] {10, 55, 0, 19, -5, -34, 0, -12, 0, 0, 0, 1};
      byte[] b65 = new byte[] {-11, 55, -119, 0, 8, 0, 1, 2, -4, 33};

      try (PreparedStatement preparedStatement = conn.prepareStatement("UPSERT INTO " + tableName
          + "(PK1, PK2, PK3, COL1, COL2, COL3) VALUES (?, ?, ?, ?, ?, ?)")) {
        upsertRow(preparedStatement, Bytes.toBytes("pk1-ehgir4jf"), 937598.284D,
            Bytes.toBytes("pk33ogjirhhf"), Bytes.toBytes("col19fnbb0hf0t"),
            Bytes.toBytes("col21048rnbfpe3-"), Bytes.toBytes("col319efnrugifj"));
        upsertRow(preparedStatement, b10, b20, b30, b40, b50, b60);
        upsertRow(preparedStatement, b1, b2, b3, b4, b5, b6);
        upsertRow(preparedStatement, b11, b21, b31, b41, b51, b61);
        upsertRow(preparedStatement, b12, b22, b32, b42, b52, b62);
        upsertRow(preparedStatement, b13, b23, b33, b43, b53, b63);
        upsertRow(preparedStatement, b15, b25, b35, b45, b55, b65);
      }
      conn.commit();

      PreparedStatement pst = conn.prepareStatement("SELECT * FROM " + tableName);
      ResultSet resultSet = pst.executeQuery();

      Assert.assertTrue(resultSet.next());

      Assert.assertArrayEquals(b1, resultSet.getBytes(1));
      Assert.assertEquals(b2, resultSet.getDouble(2), 0D);
      Assert.assertArrayEquals(b3, resultSet.getBytes(3));
      Assert.assertArrayEquals(b4, resultSet.getBytes(4));
      Assert.assertArrayEquals(b5, resultSet.getBytes(5));
      Assert.assertArrayEquals(b6, resultSet.getBytes(6));

      Assert.assertTrue(resultSet.next());

      Assert.assertArrayEquals(b10, resultSet.getBytes(1));
      Assert.assertEquals(b20, resultSet.getDouble(2), 0D);
      Assert.assertArrayEquals(b30, resultSet.getBytes(3));
      Assert.assertArrayEquals(b40, resultSet.getBytes(4));
      Assert.assertArrayEquals(b50, resultSet.getBytes(5));
      Assert.assertArrayEquals(b60, resultSet.getBytes(6));

      Assert.assertTrue(resultSet.next());

      Assert.assertArrayEquals(b15, resultSet.getBytes(1));
      Assert.assertEquals(b25, resultSet.getDouble(2), 0D);
      Assert.assertNull(resultSet.getBytes(3));
      Assert.assertArrayEquals(b45, resultSet.getBytes(4));
      Assert.assertArrayEquals(b55, resultSet.getBytes(5));
      Assert.assertArrayEquals(b65, resultSet.getBytes(6));

      Assert.assertTrue(resultSet.next());

      Assert.assertArrayEquals(b13, resultSet.getBytes(1));
      Assert.assertEquals(b23, resultSet.getDouble(2), 0D);
      Assert.assertArrayEquals(b33, resultSet.getBytes(3));
      Assert.assertArrayEquals(b43, resultSet.getBytes(4));
      Assert.assertArrayEquals(b53, resultSet.getBytes(5));
      Assert.assertArrayEquals(b63, resultSet.getBytes(6));

      Assert.assertTrue(resultSet.next());

      Assert.assertArrayEquals(b11, resultSet.getBytes(1));
      Assert.assertEquals(b21, resultSet.getDouble(2), 0D);
      Assert.assertArrayEquals(b31, resultSet.getBytes(3));
      Assert.assertArrayEquals(b41, resultSet.getBytes(4));
      Assert.assertArrayEquals(b51, resultSet.getBytes(5));
      Assert.assertArrayEquals(b61, resultSet.getBytes(6));

      Assert.assertTrue(resultSet.next());

      Assert.assertArrayEquals(b12, resultSet.getBytes(1));
      Assert.assertEquals(b22, resultSet.getDouble(2), 0D);
      Assert.assertArrayEquals(b32, resultSet.getBytes(3));
      Assert.assertArrayEquals(b42, resultSet.getBytes(4));
      Assert.assertArrayEquals(b52, resultSet.getBytes(5));
      Assert.assertArrayEquals(b62, resultSet.getBytes(6));

      Assert.assertTrue(resultSet.next());

      Assert.assertArrayEquals(Bytes.toBytes("pk1-ehgir4jf"), resultSet.getBytes(1));
      Assert.assertEquals(937598.284D, resultSet.getDouble(2), 0D);
      Assert.assertArrayEquals(Bytes.toBytes("pk33ogjirhhf"), resultSet.getBytes(3));
      Assert.assertArrayEquals(Bytes.toBytes("col19fnbb0hf0t"), resultSet.getBytes(4));
      Assert.assertArrayEquals(Bytes.toBytes("col21048rnbfpe3-"), resultSet.getBytes(5));
      Assert.assertArrayEquals(Bytes.toBytes("col319efnrugifj"), resultSet.getBytes(6));

      Assert.assertFalse(resultSet.next());

      PreparedStatement preparedStatement =
          conn.prepareStatement("SELECT * FROM " + tableName + " WHERE PK1 = ?");

      preparedStatement.setBytes(1, b11);
      resultSet = preparedStatement.executeQuery();

      Assert.assertTrue(resultSet.next());

      Assert.assertArrayEquals(b15, resultSet.getBytes(1));
      Assert.assertEquals(b25, resultSet.getDouble(2), 0D);
      Assert.assertNull(resultSet.getBytes(3));
      Assert.assertArrayEquals(b45, resultSet.getBytes(4));
      Assert.assertArrayEquals(b55, resultSet.getBytes(5));
      Assert.assertArrayEquals(b65, resultSet.getBytes(6));

      Assert.assertTrue(resultSet.next());

      Assert.assertArrayEquals(b13, resultSet.getBytes(1));
      Assert.assertEquals(b23, resultSet.getDouble(2), 0D);
      Assert.assertArrayEquals(b33, resultSet.getBytes(3));
      Assert.assertArrayEquals(b43, resultSet.getBytes(4));
      Assert.assertArrayEquals(b53, resultSet.getBytes(5));
      Assert.assertArrayEquals(b63, resultSet.getBytes(6));

      Assert.assertTrue(resultSet.next());

      Assert.assertArrayEquals(b11, resultSet.getBytes(1));
      Assert.assertEquals(b21, resultSet.getDouble(2), 0D);
      Assert.assertArrayEquals(b31, resultSet.getBytes(3));
      Assert.assertArrayEquals(b41, resultSet.getBytes(4));
      Assert.assertArrayEquals(b51, resultSet.getBytes(5));
      Assert.assertArrayEquals(b61, resultSet.getBytes(6));

      Assert.assertTrue(resultSet.next());

      Assert.assertArrayEquals(b12, resultSet.getBytes(1));
      Assert.assertEquals(b22, resultSet.getDouble(2), 0D);
      Assert.assertArrayEquals(b32, resultSet.getBytes(3));
      Assert.assertArrayEquals(b42, resultSet.getBytes(4));
      Assert.assertArrayEquals(b52, resultSet.getBytes(5));
      Assert.assertArrayEquals(b62, resultSet.getBytes(6));

      Assert.assertFalse(resultSet.next());

      preparedStatement = conn.prepareStatement(
          "SELECT * FROM " + tableName + " WHERE PK1 = ? AND PK2 IS NOT NULL ");

      preparedStatement.setBytes(1, b11);
      resultSet = preparedStatement.executeQuery();

      Assert.assertTrue(resultSet.next());

      Assert.assertArrayEquals(b15, resultSet.getBytes(1));
      Assert.assertEquals(b25, resultSet.getDouble(2), 0D);
      Assert.assertNull(resultSet.getBytes(3));
      Assert.assertArrayEquals(b45, resultSet.getBytes(4));
      Assert.assertArrayEquals(b55, resultSet.getBytes(5));
      Assert.assertArrayEquals(b65, resultSet.getBytes(6));

      Assert.assertTrue(resultSet.next());

      Assert.assertArrayEquals(b13, resultSet.getBytes(1));
      Assert.assertEquals(b23, resultSet.getDouble(2), 0D);
      Assert.assertArrayEquals(b33, resultSet.getBytes(3));
      Assert.assertArrayEquals(b43, resultSet.getBytes(4));
      Assert.assertArrayEquals(b53, resultSet.getBytes(5));
      Assert.assertArrayEquals(b63, resultSet.getBytes(6));

      Assert.assertTrue(resultSet.next());

      Assert.assertArrayEquals(b11, resultSet.getBytes(1));
      Assert.assertEquals(b21, resultSet.getDouble(2), 0D);
      Assert.assertArrayEquals(b31, resultSet.getBytes(3));
      Assert.assertArrayEquals(b41, resultSet.getBytes(4));
      Assert.assertArrayEquals(b51, resultSet.getBytes(5));
      Assert.assertArrayEquals(b61, resultSet.getBytes(6));

      Assert.assertTrue(resultSet.next());

      Assert.assertArrayEquals(b12, resultSet.getBytes(1));
      Assert.assertEquals(b22, resultSet.getDouble(2), 0D);
      Assert.assertArrayEquals(b32, resultSet.getBytes(3));
      Assert.assertArrayEquals(b42, resultSet.getBytes(4));
      Assert.assertArrayEquals(b52, resultSet.getBytes(5));
      Assert.assertArrayEquals(b62, resultSet.getBytes(6));

      Assert.assertFalse(resultSet.next());

      preparedStatement =
          conn.prepareStatement("SELECT * FROM " + tableName + " WHERE PK1 = ? AND PK2 = ?");
      preparedStatement.setBytes(1, b11);
      preparedStatement.setDouble(2, b21);
      resultSet = preparedStatement.executeQuery();

      Assert.assertTrue(resultSet.next());

      Assert.assertArrayEquals(b15, resultSet.getBytes(1));
      Assert.assertEquals(b25, resultSet.getDouble(2), 0D);
      Assert.assertNull(resultSet.getBytes(3));
      Assert.assertArrayEquals(b45, resultSet.getBytes(4));
      Assert.assertArrayEquals(b55, resultSet.getBytes(5));
      Assert.assertArrayEquals(b65, resultSet.getBytes(6));

      Assert.assertTrue(resultSet.next());

      Assert.assertArrayEquals(b13, resultSet.getBytes(1));
      Assert.assertEquals(b23, resultSet.getDouble(2), 0D);
      Assert.assertArrayEquals(b33, resultSet.getBytes(3));
      Assert.assertArrayEquals(b43, resultSet.getBytes(4));
      Assert.assertArrayEquals(b53, resultSet.getBytes(5));
      Assert.assertArrayEquals(b63, resultSet.getBytes(6));

      Assert.assertTrue(resultSet.next());

      Assert.assertArrayEquals(b11, resultSet.getBytes(1));
      Assert.assertEquals(b21, resultSet.getDouble(2), 0D);
      Assert.assertArrayEquals(b31, resultSet.getBytes(3));
      Assert.assertArrayEquals(b41, resultSet.getBytes(4));
      Assert.assertArrayEquals(b51, resultSet.getBytes(5));
      Assert.assertArrayEquals(b61, resultSet.getBytes(6));

      Assert.assertFalse(resultSet.next());

      preparedStatement = conn.prepareStatement("SELECT * FROM " + tableName
          + " WHERE PK1 = ? AND PK2 BETWEEN ? AND ? AND PK3 IS NOT NULL");
      preparedStatement.setBytes(1, b11);
      preparedStatement.setDouble(2, 3579039.0388);
      preparedStatement.setDouble(3, 3579039.03887);
      resultSet = preparedStatement.executeQuery();

      Assert.assertTrue(resultSet.next());

      Assert.assertArrayEquals(b13, resultSet.getBytes(1));
      Assert.assertEquals(b23, resultSet.getDouble(2), 0D);
      Assert.assertArrayEquals(b33, resultSet.getBytes(3));
      Assert.assertArrayEquals(b43, resultSet.getBytes(4));
      Assert.assertArrayEquals(b53, resultSet.getBytes(5));
      Assert.assertArrayEquals(b63, resultSet.getBytes(6));

      Assert.assertTrue(resultSet.next());

      Assert.assertArrayEquals(b11, resultSet.getBytes(1));
      Assert.assertEquals(b21, resultSet.getDouble(2), 0D);
      Assert.assertArrayEquals(b31, resultSet.getBytes(3));
      Assert.assertArrayEquals(b41, resultSet.getBytes(4));
      Assert.assertArrayEquals(b51, resultSet.getBytes(5));
      Assert.assertArrayEquals(b61, resultSet.getBytes(6));

      Assert.assertFalse(resultSet.next());

      preparedStatement = conn.prepareStatement("SELECT * FROM " + tableName
          + " WHERE PK1 = ? AND PK2 IN (?, ?, ?)");
      preparedStatement.setBytes(1, b11);
      preparedStatement.setDouble(2, b21);
      preparedStatement.setDouble(3, b23);
      preparedStatement.setDouble(4, b22);
      resultSet = preparedStatement.executeQuery();

      Assert.assertTrue(resultSet.next());

      Assert.assertArrayEquals(b15, resultSet.getBytes(1));
      Assert.assertEquals(b25, resultSet.getDouble(2), 0D);
      Assert.assertArrayEquals(b35, resultSet.getBytes(3));
      Assert.assertArrayEquals(b45, resultSet.getBytes(4));
      Assert.assertArrayEquals(b55, resultSet.getBytes(5));
      Assert.assertArrayEquals(b65, resultSet.getBytes(6));

      Assert.assertTrue(resultSet.next());

      Assert.assertArrayEquals(b13, resultSet.getBytes(1));
      Assert.assertEquals(b23, resultSet.getDouble(2), 0D);
      Assert.assertArrayEquals(b33, resultSet.getBytes(3));
      Assert.assertArrayEquals(b43, resultSet.getBytes(4));
      Assert.assertArrayEquals(b53, resultSet.getBytes(5));
      Assert.assertArrayEquals(b63, resultSet.getBytes(6));

      Assert.assertTrue(resultSet.next());

      Assert.assertArrayEquals(b11, resultSet.getBytes(1));
      Assert.assertEquals(b21, resultSet.getDouble(2), 0D);
      Assert.assertArrayEquals(b31, resultSet.getBytes(3));
      Assert.assertArrayEquals(b41, resultSet.getBytes(4));
      Assert.assertArrayEquals(b51, resultSet.getBytes(5));
      Assert.assertArrayEquals(b61, resultSet.getBytes(6));

      Assert.assertTrue(resultSet.next());

      Assert.assertArrayEquals(b12, resultSet.getBytes(1));
      Assert.assertEquals(b22, resultSet.getDouble(2), 0D);
      Assert.assertArrayEquals(b32, resultSet.getBytes(3));
      Assert.assertArrayEquals(b42, resultSet.getBytes(4));
      Assert.assertArrayEquals(b52, resultSet.getBytes(5));
      Assert.assertArrayEquals(b62, resultSet.getBytes(6));

      Assert.assertFalse(resultSet.next());

      preparedStatement = conn.prepareStatement(
          "SELECT * FROM " + tableName + " WHERE PK1 = ? AND PK2 = ? AND PK3 = ?");
      preparedStatement.setBytes(1, b11);
      preparedStatement.setDouble(2, b21);
      preparedStatement.setBytes(3, b31);
      resultSet = preparedStatement.executeQuery();

      Assert.assertTrue(resultSet.next());

      Assert.assertArrayEquals(b11, resultSet.getBytes(1));
      Assert.assertEquals(b21, resultSet.getDouble(2), 0D);
      Assert.assertArrayEquals(b31, resultSet.getBytes(3));
      Assert.assertArrayEquals(b41, resultSet.getBytes(4));
      Assert.assertArrayEquals(b51, resultSet.getBytes(5));
      Assert.assertArrayEquals(b61, resultSet.getBytes(6));

      Assert.assertFalse(resultSet.next());

      preparedStatement = conn.prepareStatement(
          "SELECT * FROM " + tableName + " WHERE PK1 = ? AND PK2 = ? AND PK3 IS NULL");
      preparedStatement.setBytes(1, b11);
      preparedStatement.setDouble(2, b21);
      resultSet = preparedStatement.executeQuery();

      Assert.assertTrue(resultSet.next());

      Assert.assertArrayEquals(b15, resultSet.getBytes(1));
      Assert.assertEquals(b25, resultSet.getDouble(2), 0D);
      Assert.assertNull(resultSet.getBytes(3));
      Assert.assertArrayEquals(b45, resultSet.getBytes(4));
      Assert.assertArrayEquals(b55, resultSet.getBytes(5));
      Assert.assertArrayEquals(b65, resultSet.getBytes(6));

      Assert.assertFalse(resultSet.next());
    }
  }

  @Test
  public void testVarBinaryPkSchema7() throws Exception {
    Properties props = PropertiesUtil.deepCopy(TEST_PROPERTIES);
    final String tableName = generateUniqueName();
    try (Connection conn = DriverManager.getConnection(getUrl(), props)) {
      conn.createStatement().execute("CREATE TABLE " + tableName
          + " (PK1 DOUBLE NOT NULL, PK2 VARBINARY_ENCODED, PK3 DOUBLE NOT NULL,"
          + " COL1 VARBINARY_ENCODED, COL2 VARBINARY_ENCODED,"
          + " COL3 VARBINARY_ENCODED CONSTRAINT pk PRIMARY KEY(PK1, PK2, PK3)) "
          + tableDDLOptions);

      double b1 = -8594240.7859859;
      byte[] b2 = new byte[] {57, -83, 2, 83, -7, 12, -13, 4};
      double b3 = 41404609.573566;
      byte[] b4 = new byte[] {56, 50, 19, 34, 83, -101, -102, 91};
      byte[] b5 = new byte[] {10, 55, 19, -5, -34, 0, 0, 0, 0, 1};
      byte[] b6 = new byte[] {-11, 55, -119, 8, 0, 1, 2, -4, 33};

      double b10 = 8595240.7859859;
      byte[] b20 = new byte[] {57, -83, -2, 83, 0, -7, -12, -13, 4};
      double b30 = 953210943.84728;
      byte[] b40 = new byte[] {56, 50, 19, 34, 83, -101, -102, 91};
      byte[] b50 = new byte[] {10, 55, 19, -5, -34, 0, 0, 0, 0, 1};
      byte[] b60 = new byte[] {-11, 55, -119, 8, 0, 1, 2, -4, 33};

      double b11 = 561800793.2343711;
      byte[] b21 = new byte[] {57, -83, 0, -2, 0, -7, -12, -13, 4};
      double b31 = 470709953.73489;
      byte[] b41 = new byte[] {56, 50, 19, 0, 34, 83, -101, -102, 91};
      byte[] b51 = new byte[] {10, 55, 0, 19, -5, -34, 0, 0, 0, 0, 1};
      byte[] b61 = new byte[] {-11, 55, -119, 0, 8, 0, 1, 2, -4, 33};

      double b12 = 561800793.2343711;
      byte[] b22 = new byte[] {57, -83, 0, -2, 0, -7, -12, -13, 4, 0};
      double b32 = 738774048.49662;
      byte[] b42 = new byte[] {56, 50, 19, 0, 34, 83, -101, -102, 91};
      byte[] b52 = new byte[] {10, 55, 0, 19, -5, -34, 0, 0, 0, 0, 1};
      byte[] b62 = new byte[] {-11, 55, -119, 0, 8, 0, 1, 2, -4, 33};

      double b13 = 561800793.2343711;
      byte[] b23 = new byte[] {57, -83, 0, -2, 0, -7, -12, -13, 4};
      double b33 = 89048526.181873;
      byte[] b43 = new byte[] {56, 50, 19, 0, 34, 83, -101, -102, 91, 92};
      byte[] b53 = new byte[] {10, 55, 0, 19, -5, -34, 0, -12, 0, 0, 0, 1};
      byte[] b63 = new byte[] {-11, 55, -119, 0, 8, 0, 1, 2, -4, 33};

      double b14 = 561800793.2343711;
      byte[] b24 = null;
      double b34 = 969455745.04936;
      byte[] b44 = new byte[] {56, 50, 19, 0, 34, 83, -101, -102, 91, 92};
      byte[] b54 = new byte[] {10, 55, 0, 19, -5, -34, 0, -12, 0, 0, 0, 1};
      byte[] b64 = new byte[] {-11, 55, -119, 0, 8, 0, 1, 2, -4, 33};

      try (PreparedStatement preparedStatement = conn.prepareStatement("UPSERT INTO " + tableName
          + "(PK1, PK2, PK3, COL1, COL2, COL3) VALUES (?, ?, ?, ?, ?, ?)")) {
        upsertRow(preparedStatement, 5618007934.232343711, Bytes.toBytes("pk22p0jfdkhrgi"),
            -969455745.04936, Bytes.toBytes("col19fnbb0hf0t"),
            Bytes.toBytes("col21048rnbfpe3-"), Bytes.toBytes("col319efnrugifj"));
        upsertRow(preparedStatement, b10, b20, b30, b40, b50, b60);
        upsertRow(preparedStatement, b1, b2, b3, b4, b5, b6);
        upsertRow(preparedStatement, b11, b21, b31, b41, b51, b61);
        upsertRow(preparedStatement, b12, b22, b32, b42, b52, b62);
        upsertRow(preparedStatement, b13, b23, b33, b43, b53, b63);
        upsertRow(preparedStatement, b14, b24, b34, b44, b54, b64);
      }
      conn.commit();

      PreparedStatement pst = conn.prepareStatement("SELECT * FROM " + tableName);
      ResultSet resultSet = pst.executeQuery();

      Assert.assertTrue(resultSet.next());

      Assert.assertEquals(b1, resultSet.getDouble(1), 0D);
      Assert.assertArrayEquals(b2, resultSet.getBytes(2));
      Assert.assertEquals(b3, resultSet.getDouble(3), 0D);
      Assert.assertArrayEquals(b4, resultSet.getBytes(4));
      Assert.assertArrayEquals(b5, resultSet.getBytes(5));
      Assert.assertArrayEquals(b6, resultSet.getBytes(6));

      Assert.assertTrue(resultSet.next());

      Assert.assertEquals(b10, resultSet.getDouble(1), 0D);
      Assert.assertArrayEquals(b20, resultSet.getBytes(2));
      Assert.assertEquals(b30, resultSet.getDouble(3), 0D);
      Assert.assertArrayEquals(b40, resultSet.getBytes(4));
      Assert.assertArrayEquals(b50, resultSet.getBytes(5));
      Assert.assertArrayEquals(b60, resultSet.getBytes(6));

      Assert.assertTrue(resultSet.next());

      Assert.assertEquals(b14, resultSet.getDouble(1), 0D);
      Assert.assertNull(resultSet.getBytes(2));
      Assert.assertEquals(b34, resultSet.getDouble(3), 0D);
      Assert.assertArrayEquals(b44, resultSet.getBytes(4));
      Assert.assertArrayEquals(b54, resultSet.getBytes(5));
      Assert.assertArrayEquals(b64, resultSet.getBytes(6));

      Assert.assertTrue(resultSet.next());

      Assert.assertEquals(b13, resultSet.getDouble(1), 0D);
      Assert.assertArrayEquals(b23, resultSet.getBytes(2));
      Assert.assertEquals(b33, resultSet.getDouble(3), 0D);
      Assert.assertArrayEquals(b43, resultSet.getBytes(4));
      Assert.assertArrayEquals(b53, resultSet.getBytes(5));
      Assert.assertArrayEquals(b63, resultSet.getBytes(6));

      Assert.assertTrue(resultSet.next());

      Assert.assertEquals(b11, resultSet.getDouble(1), 0D);
      Assert.assertArrayEquals(b21, resultSet.getBytes(2));
      Assert.assertEquals(b31, resultSet.getDouble(3), 0D);
      Assert.assertArrayEquals(b41, resultSet.getBytes(4));
      Assert.assertArrayEquals(b51, resultSet.getBytes(5));
      Assert.assertArrayEquals(b61, resultSet.getBytes(6));

      Assert.assertTrue(resultSet.next());

      Assert.assertEquals(b12, resultSet.getDouble(1), 0D);
      Assert.assertArrayEquals(b22, resultSet.getBytes(2));
      Assert.assertEquals(b32, resultSet.getDouble(3), 0D);
      Assert.assertArrayEquals(b42, resultSet.getBytes(4));
      Assert.assertArrayEquals(b52, resultSet.getBytes(5));
      Assert.assertArrayEquals(b62, resultSet.getBytes(6));

      Assert.assertTrue(resultSet.next());

      Assert.assertEquals(5618007934.232343711, resultSet.getDouble(1), 0D);
      Assert.assertArrayEquals(Bytes.toBytes("pk22p0jfdkhrgi"), resultSet.getBytes(2));
      Assert.assertEquals(-969455745.04936, resultSet.getDouble(3), 0D);
      Assert.assertArrayEquals(Bytes.toBytes("col19fnbb0hf0t"), resultSet.getBytes(4));
      Assert.assertArrayEquals(Bytes.toBytes("col21048rnbfpe3-"), resultSet.getBytes(5));
      Assert.assertArrayEquals(Bytes.toBytes("col319efnrugifj"), resultSet.getBytes(6));

      Assert.assertFalse(resultSet.next());

      PreparedStatement preparedStatement =
          conn.prepareStatement("SELECT * FROM " + tableName + " WHERE PK1 = ?");

      preparedStatement.setDouble(1, b11);
      resultSet = preparedStatement.executeQuery();

      Assert.assertTrue(resultSet.next());

      Assert.assertEquals(b14, resultSet.getDouble(1), 0D);
      Assert.assertNull(resultSet.getBytes(2));
      Assert.assertEquals(b34, resultSet.getDouble(3), 0D);
      Assert.assertArrayEquals(b44, resultSet.getBytes(4));
      Assert.assertArrayEquals(b54, resultSet.getBytes(5));
      Assert.assertArrayEquals(b64, resultSet.getBytes(6));

      Assert.assertTrue(resultSet.next());

      Assert.assertEquals(b13, resultSet.getDouble(1), 0D);
      Assert.assertArrayEquals(b23, resultSet.getBytes(2));
      Assert.assertEquals(b33, resultSet.getDouble(3), 0D);
      Assert.assertArrayEquals(b43, resultSet.getBytes(4));
      Assert.assertArrayEquals(b53, resultSet.getBytes(5));
      Assert.assertArrayEquals(b63, resultSet.getBytes(6));

      Assert.assertTrue(resultSet.next());

      Assert.assertEquals(b11, resultSet.getDouble(1), 0D);
      Assert.assertArrayEquals(b21, resultSet.getBytes(2));
      Assert.assertEquals(b31, resultSet.getDouble(3), 0D);
      Assert.assertArrayEquals(b41, resultSet.getBytes(4));
      Assert.assertArrayEquals(b51, resultSet.getBytes(5));
      Assert.assertArrayEquals(b61, resultSet.getBytes(6));

      Assert.assertTrue(resultSet.next());

      Assert.assertEquals(b12, resultSet.getDouble(1), 0D);
      Assert.assertArrayEquals(b22, resultSet.getBytes(2));
      Assert.assertEquals(b32, resultSet.getDouble(3), 0D);
      Assert.assertArrayEquals(b42, resultSet.getBytes(4));
      Assert.assertArrayEquals(b52, resultSet.getBytes(5));
      Assert.assertArrayEquals(b62, resultSet.getBytes(6));

      Assert.assertFalse(resultSet.next());

      preparedStatement = conn.prepareStatement(
          "SELECT * FROM " + tableName + " WHERE PK1 = ? AND PK2 IS NOT NULL");

      preparedStatement.setDouble(1, b11);
      resultSet = preparedStatement.executeQuery();

      Assert.assertTrue(resultSet.next());

      Assert.assertEquals(b13, resultSet.getDouble(1), 0D);
      Assert.assertArrayEquals(b23, resultSet.getBytes(2));
      Assert.assertEquals(b33, resultSet.getDouble(3), 0D);
      Assert.assertArrayEquals(b43, resultSet.getBytes(4));
      Assert.assertArrayEquals(b53, resultSet.getBytes(5));
      Assert.assertArrayEquals(b63, resultSet.getBytes(6));

      Assert.assertTrue(resultSet.next());

      Assert.assertEquals(b11, resultSet.getDouble(1), 0D);
      Assert.assertArrayEquals(b21, resultSet.getBytes(2));
      Assert.assertEquals(b31, resultSet.getDouble(3), 0D);
      Assert.assertArrayEquals(b41, resultSet.getBytes(4));
      Assert.assertArrayEquals(b51, resultSet.getBytes(5));
      Assert.assertArrayEquals(b61, resultSet.getBytes(6));

      Assert.assertTrue(resultSet.next());

      Assert.assertEquals(b12, resultSet.getDouble(1), 0D);
      Assert.assertArrayEquals(b22, resultSet.getBytes(2));
      Assert.assertEquals(b32, resultSet.getDouble(3), 0D);
      Assert.assertArrayEquals(b42, resultSet.getBytes(4));
      Assert.assertArrayEquals(b52, resultSet.getBytes(5));
      Assert.assertArrayEquals(b62, resultSet.getBytes(6));

      Assert.assertFalse(resultSet.next());

      preparedStatement =
          conn.prepareStatement("SELECT * FROM " + tableName + " WHERE PK1 = ? AND PK2 = ?");
      preparedStatement.setDouble(1, b11);
      preparedStatement.setBytes(2, b21);
      resultSet = preparedStatement.executeQuery();

      Assert.assertTrue(resultSet.next());

      Assert.assertEquals(b13, resultSet.getDouble(1), 0D);
      Assert.assertArrayEquals(b23, resultSet.getBytes(2));
      Assert.assertEquals(b33, resultSet.getDouble(3), 0D);
      Assert.assertArrayEquals(b43, resultSet.getBytes(4));
      Assert.assertArrayEquals(b53, resultSet.getBytes(5));
      Assert.assertArrayEquals(b63, resultSet.getBytes(6));

      Assert.assertTrue(resultSet.next());

      Assert.assertEquals(b11, resultSet.getDouble(1), 0D);
      Assert.assertArrayEquals(b21, resultSet.getBytes(2));
      Assert.assertEquals(b31, resultSet.getDouble(3), 0D);
      Assert.assertArrayEquals(b41, resultSet.getBytes(4));
      Assert.assertArrayEquals(b51, resultSet.getBytes(5));
      Assert.assertArrayEquals(b61, resultSet.getBytes(6));

      Assert.assertFalse(resultSet.next());

      preparedStatement = conn.prepareStatement("SELECT * FROM " + tableName
          + " WHERE PK1 = ? AND PK2 BETWEEN ? AND ? AND PK3 IS NOT NULL");
      preparedStatement.setDouble(1, b11);
      preparedStatement.setBytes(2, new byte[] {57, -83, 0, -2, 0, -7, -12, -13, 3, 24, -121});
      preparedStatement.setBytes(3, new byte[] {57, -83, 0, -2, 0, -7, -12, -13, 4});
      resultSet = preparedStatement.executeQuery();

      Assert.assertTrue(resultSet.next());

      Assert.assertEquals(b13, resultSet.getDouble(1), 0D);
      Assert.assertArrayEquals(b23, resultSet.getBytes(2));
      Assert.assertEquals(b33, resultSet.getDouble(3), 0D);
      Assert.assertArrayEquals(b43, resultSet.getBytes(4));
      Assert.assertArrayEquals(b53, resultSet.getBytes(5));
      Assert.assertArrayEquals(b63, resultSet.getBytes(6));

      Assert.assertTrue(resultSet.next());

      Assert.assertEquals(b11, resultSet.getDouble(1), 0D);
      Assert.assertArrayEquals(b21, resultSet.getBytes(2));
      Assert.assertEquals(b31, resultSet.getDouble(3), 0D);
      Assert.assertArrayEquals(b41, resultSet.getBytes(4));
      Assert.assertArrayEquals(b51, resultSet.getBytes(5));
      Assert.assertArrayEquals(b61, resultSet.getBytes(6));

      Assert.assertFalse(resultSet.next());

      preparedStatement = conn.prepareStatement("SELECT * FROM " + tableName
          + " WHERE PK1 = ? AND PK2 IN (?, ?, ?)");
      preparedStatement.setDouble(1, b11);
      preparedStatement.setBytes(2, b21);
      preparedStatement.setBytes(3, b23);
      preparedStatement.setBytes(4, b22);
      resultSet = preparedStatement.executeQuery();

      Assert.assertTrue(resultSet.next());

      Assert.assertEquals(b13, resultSet.getDouble(1), 0D);
      Assert.assertArrayEquals(b23, resultSet.getBytes(2));
      Assert.assertEquals(b33, resultSet.getDouble(3), 0D);
      Assert.assertArrayEquals(b43, resultSet.getBytes(4));
      Assert.assertArrayEquals(b53, resultSet.getBytes(5));
      Assert.assertArrayEquals(b63, resultSet.getBytes(6));

      Assert.assertTrue(resultSet.next());

      Assert.assertEquals(b11, resultSet.getDouble(1), 0D);
      Assert.assertArrayEquals(b21, resultSet.getBytes(2));
      Assert.assertEquals(b31, resultSet.getDouble(3), 0D);
      Assert.assertArrayEquals(b41, resultSet.getBytes(4));
      Assert.assertArrayEquals(b51, resultSet.getBytes(5));
      Assert.assertArrayEquals(b61, resultSet.getBytes(6));

      Assert.assertTrue(resultSet.next());

      Assert.assertEquals(b12, resultSet.getDouble(1), 0D);
      Assert.assertArrayEquals(b22, resultSet.getBytes(2));
      Assert.assertEquals(b32, resultSet.getDouble(3), 0D);
      Assert.assertArrayEquals(b42, resultSet.getBytes(4));
      Assert.assertArrayEquals(b52, resultSet.getBytes(5));
      Assert.assertArrayEquals(b62, resultSet.getBytes(6));

      Assert.assertFalse(resultSet.next());

      preparedStatement = conn.prepareStatement(
          "SELECT * FROM " + tableName + " WHERE PK1 = ? AND PK2 = ? AND PK3 = ?");
      preparedStatement.setDouble(1, b11);
      preparedStatement.setBytes(2, b21);
      preparedStatement.setDouble(3, b31);
      resultSet = preparedStatement.executeQuery();

      Assert.assertTrue(resultSet.next());

      Assert.assertEquals(b11, resultSet.getDouble(1), 0D);
      Assert.assertArrayEquals(b21, resultSet.getBytes(2));
      Assert.assertEquals(b31, resultSet.getDouble(3), 0D);
      Assert.assertArrayEquals(b41, resultSet.getBytes(4));
      Assert.assertArrayEquals(b51, resultSet.getBytes(5));
      Assert.assertArrayEquals(b61, resultSet.getBytes(6));

      Assert.assertFalse(resultSet.next());

      preparedStatement = conn.prepareStatement(
          "SELECT * FROM " + tableName + " WHERE PK1 = ? AND PK2 = ? AND PK3 IS NULL");
      preparedStatement.setDouble(1, b11);
      preparedStatement.setBytes(2, b21);
      resultSet = preparedStatement.executeQuery();

      Assert.assertFalse(resultSet.next());
    }
  }

  private static void upsertRow(PreparedStatement preparedStatement, byte[] b10, String b20,
      byte[] b30, byte[] b40, byte[] b50, byte[] b60) throws SQLException {
    preparedStatement.setBytes(1, b10);
    preparedStatement.setString(2, b20);
    preparedStatement.setBytes(3, b30);
    preparedStatement.setBytes(4, b40);
    preparedStatement.setBytes(5, b50);
    preparedStatement.setBytes(6, b60);
    preparedStatement.executeUpdate();
  }

  private static void upsertRow(PreparedStatement preparedStatement, byte[] b10, Double b20,
      byte[] b30, byte[] b40, byte[] b50, byte[] b60) throws SQLException {
    preparedStatement.setBytes(1, b10);
    preparedStatement.setDouble(2, b20);
    preparedStatement.setBytes(3, b30);
    preparedStatement.setBytes(4, b40);
    preparedStatement.setBytes(5, b50);
    preparedStatement.setBytes(6, b60);
    preparedStatement.executeUpdate();
  }

  private static void upsertRow(PreparedStatement preparedStatement, String b10, byte[] b20,
      String b30, byte[] b40, byte[] b50, byte[] b60) throws SQLException {
    preparedStatement.setString(1, b10);
    preparedStatement.setBytes(2, b20);
    preparedStatement.setString(3, b30);
    preparedStatement.setBytes(4, b40);
    preparedStatement.setBytes(5, b50);
    preparedStatement.setBytes(6, b60);
    preparedStatement.executeUpdate();
  }

  private static void upsertRow(PreparedStatement preparedStatement, double b10, byte[] b20,
      double b30, byte[] b40, byte[] b50, byte[] b60) throws SQLException {
    preparedStatement.setDouble(1, b10);
    preparedStatement.setBytes(2, b20);
    preparedStatement.setDouble(3, b30);
    preparedStatement.setBytes(4, b40);
    preparedStatement.setBytes(5, b50);
    preparedStatement.setBytes(6, b60);
    preparedStatement.executeUpdate();
  }

}
