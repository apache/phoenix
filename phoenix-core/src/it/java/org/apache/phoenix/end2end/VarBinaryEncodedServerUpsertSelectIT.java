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

import static org.apache.phoenix.end2end.VarBinaryEncodedUpsertSelectTestUtil.SINGLE_PK_COLUMNS;
import static org.apache.phoenix.end2end.VarBinaryEncodedUpsertSelectTestUtil.assertRows;
import static org.apache.phoenix.end2end.VarBinaryEncodedUpsertSelectTestUtil.assertRowsAndRowKeys;
import static org.apache.phoenix.end2end.VarBinaryEncodedUpsertSelectTestUtil.createTable;
import static org.apache.phoenix.end2end.VarBinaryEncodedUpsertSelectTestUtil.upsertEncodedRow;
import static org.apache.phoenix.util.TestUtil.TEST_PROPERTIES;

import java.sql.Connection;
import java.sql.DriverManager;
import java.util.Map;
import java.util.Properties;
import org.apache.phoenix.query.QueryServices;
import org.apache.phoenix.util.PropertiesUtil;
import org.apache.phoenix.util.ReadOnlyProps;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import org.apache.phoenix.thirdparty.com.google.common.collect.Maps;

@Category(NeedsOwnMiniClusterTest.class)
public class VarBinaryEncodedServerUpsertSelectIT extends ParallelStatsDisabledIT {

  @BeforeClass
  public static synchronized void doSetup() throws Exception {
    Map<String, String> props = Maps.newHashMapWithExpectedSize(1);
    props.put(QueryServices.ENABLE_SERVER_UPSERT_SELECT, Boolean.TRUE.toString());
    setUpTestDriver(new ReadOnlyProps(props.entrySet().iterator()));
  }

  @Test
  public void testServerUpsertSelectVarBinaryEncoded() throws Exception {
    Properties props = PropertiesUtil.deepCopy(TEST_PROPERTIES);
    final String sourceTable = generateUniqueName();
    final String targetTable = generateUniqueName();

    byte[] pk1 = new byte[] { 1, 0, 2 };
    byte[] col1 = new byte[] { 0, -1, 5 };
    byte[] pk2 = new byte[] { 0, 0, 3, 0 };
    byte[] col2 = new byte[] { 40, 50 };

    try (Connection conn = DriverManager.getConnection(getUrl(), props)) {
      conn.setAutoCommit(true);
      createTable(conn, sourceTable, "");
      createTable(conn, targetTable, "");

      upsertEncodedRow(conn, sourceTable, pk1, col1);
      upsertEncodedRow(conn, sourceTable, pk2, col2);
      upsertEncodedRow(conn, targetTable, pk1, col1);
      upsertEncodedRow(conn, targetTable, pk2, col2);

      byte[][] row2 = new byte[][] { pk2, col2 };
      byte[][] row1 = new byte[][] { pk1, col1 };
      assertRowsAndRowKeys(conn, sourceTable, row2, row1);
      assertRowsAndRowKeys(conn, targetTable, row2, row1);

      String upsertSelect =
        "UPSERT INTO " + targetTable + " (PK1, COL1) SELECT PK1, COL1 FROM " + sourceTable;
      conn.createStatement().execute(upsertSelect);

      assertRowsAndRowKeys(conn, targetTable, row2, row1);
    }
  }

  @Test
  public void testServerUpsertSelectVarBinaryEncodedIntoDescendingPk() throws Exception {
    Properties props = PropertiesUtil.deepCopy(TEST_PROPERTIES);
    final String sourceTable = generateUniqueName();
    final String targetTable = generateUniqueName();

    byte[] pk1 = new byte[] { 1, 0, 2 };
    byte[] col1 = new byte[] { 0, -1, 5 };
    byte[] pk2 = new byte[] { 0, 0, 3, 0 };
    byte[] col2 = new byte[] { 40, 50 };

    try (Connection conn = DriverManager.getConnection(getUrl(), props)) {
      conn.setAutoCommit(true);
      createTable(conn, sourceTable, "");
      createTable(conn, targetTable, "", true);

      upsertEncodedRow(conn, sourceTable, pk1, col1);
      upsertEncodedRow(conn, sourceTable, pk2, col2);
      upsertEncodedRow(conn, targetTable, pk1, col1);
      upsertEncodedRow(conn, targetTable, pk2, col2);

      byte[][] row1 = new byte[][] { pk1, col1 };
      byte[][] row2 = new byte[][] { pk2, col2 };
      assertRowsAndRowKeys(conn, sourceTable, row2, row1);
      assertRows(conn, targetTable, SINGLE_PK_COLUMNS, row1, row2);

      String upsertSelect =
        "UPSERT INTO " + targetTable + " (PK1, COL1) SELECT PK1, COL1 FROM " + sourceTable;
      conn.createStatement().execute(upsertSelect);

      assertRows(conn, targetTable, SINGLE_PK_COLUMNS, row1, row2);
    }
  }

}
