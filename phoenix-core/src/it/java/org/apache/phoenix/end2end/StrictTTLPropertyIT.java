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

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import org.apache.phoenix.jdbc.PhoenixConnection;
import org.apache.phoenix.schema.PTable;
import org.apache.phoenix.util.SchemaUtil;
import org.junit.Test;
import org.junit.experimental.categories.Category;

/**
 * Tests for Strict and Relaxed TTL property.
 */
@Category(NeedsOwnMiniClusterTest.class)
public class StrictTTLPropertyIT extends ParallelStatsDisabledIT {

  @Test
  public void testCreateTableWithStrictTTLDefault() throws Exception {
    String schemaName = generateUniqueName();
    String tableName = generateUniqueName();
    String fullTableName = SchemaUtil.getTableName(schemaName, tableName);

    try (PhoenixConnection conn = (PhoenixConnection) DriverManager.getConnection(getUrl())) {
      // Create table without specifying IS_STRICT_TTL - should default to true
      String ddl =
        "CREATE TABLE " + fullTableName + " (id char(1) NOT NULL," + " col1 integer NOT NULL,"
          + " col2 bigint NOT NULL," + " CONSTRAINT NAME_PK PRIMARY KEY (id, col1, col2))";
      conn.createStatement().execute(ddl);

      PTable table = conn.getTableNoCache(fullTableName);
      assertTrue("IS_STRICT_TTL should default to true", table.isStrictTTL());
    }
  }

  @Test
  public void testCreateTableWithStrictTTLTrue() throws Exception {
    String schemaName = generateUniqueName();
    String tableName = generateUniqueName();
    String fullTableName = SchemaUtil.getTableName(schemaName, tableName);

    try (PhoenixConnection conn = (PhoenixConnection) DriverManager.getConnection(getUrl())) {
      // Create table with IS_STRICT_TTL=true
      String ddl = "CREATE TABLE " + fullTableName + " (id char(1) NOT NULL,"
        + " col1 integer NOT NULL," + " col2 bigint NOT NULL,"
        + " CONSTRAINT NAME_PK PRIMARY KEY (id, col1, col2))" + " IS_STRICT_TTL=true";
      conn.createStatement().execute(ddl);

      PTable table = conn.getTableNoCache(fullTableName);
      assertTrue("IS_STRICT_TTL should be true", table.isStrictTTL());
    }
  }

  @Test
  public void testCreateTableWithStrictTTLFalse() throws Exception {
    String schemaName = generateUniqueName();
    String tableName = generateUniqueName();
    String fullTableName = SchemaUtil.getTableName(schemaName, tableName);

    try (PhoenixConnection conn = (PhoenixConnection) DriverManager.getConnection(getUrl())) {
      // Create table with IS_STRICT_TTL=false
      String ddl = "CREATE TABLE " + fullTableName + " (id char(1) NOT NULL,"
        + " col1 integer NOT NULL," + " col2 bigint NOT NULL,"
        + " CONSTRAINT NAME_PK PRIMARY KEY (id, col1, col2))" + " IS_STRICT_TTL=false";
      conn.createStatement().execute(ddl);

      PTable table = conn.getTableNoCache(fullTableName);
      assertFalse("IS_STRICT_TTL should be false", table.isStrictTTL());
    }
  }

  @Test
  public void testAlterTableSetStrictTTLTrue() throws Exception {
    String schemaName = generateUniqueName();
    String tableName = generateUniqueName();
    String fullTableName = SchemaUtil.getTableName(schemaName, tableName);

    try (PhoenixConnection conn = (PhoenixConnection) DriverManager.getConnection(getUrl())) {
      // Create table with IS_STRICT_TTL=false
      String createDdl = "CREATE TABLE " + fullTableName + " (id char(1) NOT NULL,"
        + " col1 integer NOT NULL," + " col2 bigint NOT NULL,"
        + " CONSTRAINT NAME_PK PRIMARY KEY (id, col1, col2))" + " IS_STRICT_TTL=false";
      conn.createStatement().execute(createDdl);

      PTable table = conn.getTableNoCache(fullTableName);
      assertFalse("IS_STRICT_TTL should be false initially", table.isStrictTTL());

      // Alter table to set IS_STRICT_TTL=true
      String alterDdl = "ALTER TABLE " + fullTableName + " SET IS_STRICT_TTL=true";
      conn.createStatement().execute(alterDdl);

      PTable alteredTable = conn.getTableNoCache(fullTableName);
      assertTrue("IS_STRICT_TTL should be true after ALTER", alteredTable.isStrictTTL());
    }
  }

  @Test
  public void testAlterTableSetStrictTTLFalse() throws Exception {
    String schemaName = generateUniqueName();
    String tableName = generateUniqueName();
    String fullTableName = SchemaUtil.getTableName(schemaName, tableName);

    try (PhoenixConnection conn = (PhoenixConnection) DriverManager.getConnection(getUrl())) {
      // Create table with default IS_STRICT_TTL (should be true)
      String createDdl =
        "CREATE TABLE " + fullTableName + " (id char(1) NOT NULL," + " col1 integer NOT NULL,"
          + " col2 bigint NOT NULL," + " CONSTRAINT NAME_PK PRIMARY KEY (id, col1, col2))";
      conn.createStatement().execute(createDdl);

      PTable table = conn.getTableNoCache(fullTableName);
      assertTrue("IS_STRICT_TTL should be true initially", table.isStrictTTL());

      // Alter table to set IS_STRICT_TTL=false
      String alterDdl = "ALTER TABLE " + fullTableName + " SET IS_STRICT_TTL=false";
      conn.createStatement().execute(alterDdl);

      PTable alteredTable = conn.getTableNoCache(fullTableName);
      assertFalse("IS_STRICT_TTL should be false after ALTER", alteredTable.isStrictTTL());
    }
  }

  @Test
  public void testStrictTTLPropertyPersistsInSystemCatalog() throws Exception {
    String schemaName = generateUniqueName();
    String tableName = generateUniqueName();
    String fullTableName = SchemaUtil.getTableName(schemaName, tableName);

    try (Connection conn = DriverManager.getConnection(getUrl())) {
      // Create table with IS_STRICT_TTL=false
      String ddl = "CREATE TABLE " + fullTableName + " (id char(1) NOT NULL,"
        + " col1 integer NOT NULL," + " col2 bigint NOT NULL,"
        + " CONSTRAINT NAME_PK PRIMARY KEY (id, col1, col2))" + " IS_STRICT_TTL=false";
      conn.createStatement().execute(ddl);

      // Verify the property is persisted in SYSTEM.CATALOG
      String query = "SELECT IS_STRICT_TTL FROM SYSTEM.CATALOG "
        + "WHERE TABLE_SCHEM = ? AND TABLE_NAME = ? AND COLUMN_NAME IS NULL";
      try (PreparedStatement stmt = conn.prepareStatement(query)) {
        stmt.setString(1, schemaName);
        stmt.setString(2, tableName);
        ResultSet rs = stmt.executeQuery();
        assertTrue("Should find table row", rs.next());
        assertFalse("IS_STRICT_TTL should be false in SYSTEM.CATALOG",
          rs.getBoolean("IS_STRICT_TTL"));
      }
    }
  }
}
