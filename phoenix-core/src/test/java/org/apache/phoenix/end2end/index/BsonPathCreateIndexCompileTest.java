/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.phoenix.end2end.index;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.Properties;
import org.apache.phoenix.exception.SQLExceptionCode;
import org.apache.phoenix.jdbc.PhoenixConnection;
import org.apache.phoenix.query.BaseConnectionlessQueryTest;
import org.apache.phoenix.query.QueryServices;
import org.apache.phoenix.schema.PColumn;
import org.apache.phoenix.schema.PTable;
import org.apache.phoenix.util.PropertiesUtil;
import org.junit.Test;

public class BsonPathCreateIndexCompileTest extends BaseConnectionlessQueryTest {

  @Test
  public void disableFlagRejectsCreateIndex() throws Exception {
    Properties props = PropertiesUtil.deepCopy(new Properties());
    props.setProperty(QueryServices.BSON_INDEX_ENABLED_ATTRIB, "false");
    try (Connection conn = DriverManager.getConnection(getUrl(), props)) {
      conn.createStatement().execute(
          "CREATE TABLE T_BSON_X (PK VARCHAR PRIMARY KEY, DOC BSON)");
      try {
        conn.createStatement().execute(
            "CREATE INDEX IDX_X ON T_BSON_X (BSON_VALUE(DOC, '$.a', 'VARCHAR'))");
        org.junit.Assert.fail("expected BSON_INDEX_DISABLED");
      } catch (SQLException e) {
        assertEquals(SQLExceptionCode.BSON_INDEX_DISABLED.getErrorCode(), e.getErrorCode());
      }
    }
  }

  @Test
  public void canonicalizationStoresCanonicalForm() throws Exception {
    // Verify canonicalization is applied: index expressions written without "$." prefix or
    // with mixed-case type are persisted in canonical form (with "$." prefix, uppercased type).
    // The connectionless driver doesn't always raise duplicate-index errors via SQL execution,
    // so we verify the stored form by inspecting the in-memory index PTable's column names —
    // for expression indexes, the index column name is derived from the canonical expressionStr.
    Properties props = PropertiesUtil.deepCopy(new Properties());
    try (PhoenixConnection conn =
        DriverManager.getConnection(getUrl(), props).unwrap(PhoenixConnection.class)) {
      conn.createStatement().execute(
          "CREATE TABLE T_BSON_Y (PK VARCHAR PRIMARY KEY, DOC BSON)");
      conn.createStatement().execute(
          "CREATE INDEX IDX_Y1 ON T_BSON_Y (BSON_VALUE(DOC, 'a.b', 'varchar'))");

      PTable indexTable = conn.getTable("IDX_Y1");
      boolean foundCanonical = false;
      for (PColumn col : indexTable.getColumns()) {
        String colName = col.getName().getString();
        if (colName.contains("$.a.b") && colName.contains("VARCHAR")) {
          foundCanonical = true;
          break;
        }
      }
      assertTrue("expected canonical $.a.b / VARCHAR form in index column names",
          foundCanonical);
    }
  }
}
