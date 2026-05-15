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
package org.apache.phoenix.compile;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.Properties;

import static org.apache.phoenix.util.TestUtil.TEST_PROPERTIES;

import org.apache.phoenix.exception.SQLExceptionCode;
import org.apache.phoenix.query.BaseConnectionlessQueryTest;
import org.apache.phoenix.util.PropertiesUtil;
import org.junit.Test;

public class UpsertCompilerVirtualConflictTest extends BaseConnectionlessQueryTest {

  @Test
  public void rejectsConflictingType() throws Exception {
    Properties props = PropertiesUtil.deepCopy(TEST_PROPERTIES);
    try (Connection c = DriverManager.getConnection(getUrl(), props)) {
      c.createStatement().execute(
          "CREATE TABLE t (pk VARCHAR PRIMARY KEY, regular VARCHAR)");
      VirtualColumnTestUtil.injectVirtualColumn(c, "T", "EXTRA", "VARCHAR");

      try {
        c.prepareStatement(
            "UPSERT INTO t (pk, extra INTEGER) VALUES ('a', 42)").executeUpdate();
        fail("expected SQLException");
      } catch (SQLException ex) {
        assertEquals(
            SQLExceptionCode.PHOENIX_DYNAMIC_TYPE_CONFLICT.getErrorCode(),
            ex.getErrorCode());
      }
    }
  }

  @Test
  public void acceptsMatchingType() throws Exception {
    Properties props = PropertiesUtil.deepCopy(TEST_PROPERTIES);
    try (Connection c = DriverManager.getConnection(getUrl(), props)) {
      c.createStatement().execute(
          "CREATE TABLE t2 (pk VARCHAR PRIMARY KEY, regular VARCHAR)");
      VirtualColumnTestUtil.injectVirtualColumn(c, "T2", "EXTRA", "VARCHAR");

      // Compiles cleanly — connectionless driver doesn't actually execute against HBase.
      c.prepareStatement(
          "UPSERT INTO t2 (pk, extra VARCHAR) VALUES ('a', 'hi')");
    }
  }

  @Test
  public void acceptsOmittedColumn() throws Exception {
    Properties props = PropertiesUtil.deepCopy(TEST_PROPERTIES);
    try (Connection c = DriverManager.getConnection(getUrl(), props)) {
      c.createStatement().execute(
          "CREATE TABLE t3 (pk VARCHAR PRIMARY KEY, regular VARCHAR)");
      VirtualColumnTestUtil.injectVirtualColumn(c, "T3", "EXTRA", "VARCHAR");

      // No reference to extra at all — should compile.
      c.prepareStatement(
          "UPSERT INTO t3 (pk, regular) VALUES ('a', 'hi')");
    }
  }

  @Test
  public void acceptsImplicitTypeFromCatalog() throws Exception {
    // UPSERT without inline ColumnDef — type is read from catalog (virtual VARCHAR).
    Properties props = PropertiesUtil.deepCopy(TEST_PROPERTIES);
    try (Connection c = DriverManager.getConnection(getUrl(), props)) {
      c.createStatement().execute(
          "CREATE TABLE t4 (pk VARCHAR PRIMARY KEY, regular VARCHAR)");
      VirtualColumnTestUtil.injectVirtualColumn(c, "T4", "EXTRA", "VARCHAR");

      c.prepareStatement("UPSERT INTO t4 (pk, extra) VALUES ('a', 'hi')");
    }
  }
}
