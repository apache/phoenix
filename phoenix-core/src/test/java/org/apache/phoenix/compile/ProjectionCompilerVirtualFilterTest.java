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

import static org.apache.phoenix.util.TestUtil.TEST_PROPERTIES;
import static org.junit.Assert.assertEquals;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.util.Properties;

import org.apache.phoenix.query.BaseConnectionlessQueryTest;
import org.apache.phoenix.util.PropertiesUtil;
import org.junit.Test;

/**
 * Unit-style test using the connectionless driver. Verifies that a column
 * with isVirtual=true does NOT appear in SELECT * but DOES resolve via
 * explicit reference. Phase 0 cannot create virtual columns via DDL yet,
 * so this test injects one via PTableImpl.
 */
public class ProjectionCompilerVirtualFilterTest extends BaseConnectionlessQueryTest {

  @Test
  public void wildcardSkipsVirtualColumn() throws Exception {
    Properties props = PropertiesUtil.deepCopy(TEST_PROPERTIES);
    try (Connection conn = DriverManager.getConnection(getUrl(), props)) {
      conn.createStatement().execute(
          "CREATE TABLE t (pk VARCHAR PRIMARY KEY, regular VARCHAR) "
              + "COLUMN_ENCODED_BYTES = 0");
      VirtualColumnTestUtil.injectVirtualColumn(conn, "T", "EXTRA", "VARCHAR");

      ResultSet rs = conn.createStatement().executeQuery(
          "SELECT * FROM t");
      ResultSetMetaData md = rs.getMetaData();
      // Expect 2 columns: PK + REGULAR. EXTRA must be skipped.
      assertEquals(2, md.getColumnCount());
      assertEquals("PK", md.getColumnName(1));
      assertEquals("REGULAR", md.getColumnName(2));
    }
  }

  @Test
  public void explicitReferenceResolvesVirtualColumn() throws Exception {
    Properties props = PropertiesUtil.deepCopy(TEST_PROPERTIES);
    try (Connection conn = DriverManager.getConnection(getUrl(), props)) {
      conn.createStatement().execute(
          "CREATE TABLE t2 (pk VARCHAR PRIMARY KEY, regular VARCHAR) "
              + "COLUMN_ENCODED_BYTES = 0");
      VirtualColumnTestUtil.injectVirtualColumn(conn, "T2", "EXTRA", "VARCHAR");

      // Should compile without error.
      conn.prepareStatement("SELECT extra FROM t2").executeQuery().close();
    }
  }
}
