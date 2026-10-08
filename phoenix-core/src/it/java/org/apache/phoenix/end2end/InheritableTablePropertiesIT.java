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
import static org.junit.Assert.fail;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.Map;
import java.util.Properties;

import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.TableDescriptor;
import org.apache.phoenix.exception.SQLExceptionCode;
import org.apache.phoenix.jdbc.PhoenixConnection;
import org.apache.phoenix.query.QueryServices;
import org.apache.phoenix.util.MetaDataUtil;
import org.apache.phoenix.util.ReadOnlyProps;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import org.apache.phoenix.thirdparty.com.google.common.collect.Maps;

/**
 * Integration tests for PHOENIX-6868: Inheritable table-level (TableDescriptor) properties from
 * data tables to their indexes.
 */
@Category(NeedsOwnMiniClusterTest.class)
public class InheritableTablePropertiesIT extends ParallelStatsDisabledIT {

  private static final String CUSTOM_PROP_1 = "custom.prop.one";
  private static final String CUSTOM_PROP_2 = "custom.prop.two";
  private static final String INITIAL_PROP_1_VALUE = "value1";
  private static final String INITIAL_PROP_2_VALUE = "value2";
  private static final String MODIFIED_PROP_1_VALUE = "modified_value1";
  private static final String MODIFIED_PROP_2_VALUE = "modified_value2";

  @BeforeClass
  public static synchronized void doSetup() throws Exception {
    Map<String, String> props = Maps.newHashMapWithExpectedSize(2);
    props.put(QueryServices.USE_STATS_FOR_PARALLELIZATION, Boolean.toString(false));
    props.put(QueryServices.INDEX_INHERITABLE_TABLE_DESCRIPTOR_PROPERTIES,
      CUSTOM_PROP_1 + "," + CUSTOM_PROP_2);
    setUpTestDriver(new ReadOnlyProps(props.entrySet().iterator()));
  }

  // Test that custom table descriptor properties on data table are inherited by a global index
  @Test
  public void testGlobalIndexInheritsTableDescriptorProps() throws Exception {
    try (Connection conn = DriverManager.getConnection(getUrl(), new Properties())) {
      String tableName = generateUniqueName();
      String indexName = generateUniqueName();
      conn.createStatement().execute("CREATE TABLE " + tableName
        + " (id INTEGER NOT NULL PRIMARY KEY, name VARCHAR(10), flag BOOLEAN)"
        + " \"" + CUSTOM_PROP_1 + "\"='" + INITIAL_PROP_1_VALUE + "',"
        + " \"" + CUSTOM_PROP_2 + "\"='" + INITIAL_PROP_2_VALUE + "'");
      conn.createStatement()
        .execute("CREATE INDEX " + indexName + " ON " + tableName + "(name)");

      verifyTableDescriptorProperty(conn, indexName, CUSTOM_PROP_1, INITIAL_PROP_1_VALUE);
      verifyTableDescriptorProperty(conn, indexName, CUSTOM_PROP_2, INITIAL_PROP_2_VALUE);
    }
  }

  // Test that custom table descriptor properties on data table are inherited by a local index
  // (local indexes live on the same physical table, so the properties are implicitly shared)
  @Test
  public void testLocalIndexInheritsTableDescriptorProps() throws Exception {
    try (Connection conn = DriverManager.getConnection(getUrl(), new Properties())) {
      String tableName = generateUniqueName();
      String localIndexName = generateUniqueName();
      conn.createStatement().execute("CREATE TABLE " + tableName
        + " (id INTEGER NOT NULL PRIMARY KEY, name VARCHAR(10), flag BOOLEAN)"
        + " \"" + CUSTOM_PROP_1 + "\"='" + INITIAL_PROP_1_VALUE + "',"
        + " \"" + CUSTOM_PROP_2 + "\"='" + INITIAL_PROP_2_VALUE + "'");
      conn.createStatement()
        .execute("CREATE LOCAL INDEX " + localIndexName + " ON " + tableName + "(name)");

      // Local index is on the same physical table, so properties are inherently shared
      verifyTableDescriptorProperty(conn, tableName, CUSTOM_PROP_1, INITIAL_PROP_1_VALUE);
      verifyTableDescriptorProperty(conn, tableName, CUSTOM_PROP_2, INITIAL_PROP_2_VALUE);
    }
  }

  // Test that custom table descriptor properties on data table are inherited by a view index
  @Test
  public void testViewIndexInheritsTableDescriptorProps() throws Exception {
    try (Connection conn = DriverManager.getConnection(getUrl(), new Properties())) {
      String tableName = generateUniqueName();
      String viewName = generateUniqueName();
      String viewIndexName = generateUniqueName();
      conn.createStatement().execute("CREATE TABLE " + tableName
        + " (id INTEGER NOT NULL PRIMARY KEY, name VARCHAR(10), flag BOOLEAN)"
        + " \"" + CUSTOM_PROP_1 + "\"='" + INITIAL_PROP_1_VALUE + "',"
        + " \"" + CUSTOM_PROP_2 + "\"='" + INITIAL_PROP_2_VALUE + "'");
      conn.createStatement().execute("CREATE VIEW " + viewName
        + " AS SELECT * FROM " + tableName + " WHERE id > 1");
      conn.createStatement()
        .execute("CREATE INDEX " + viewIndexName + " ON " + viewName + "(name)");

      String physicalViewIndexName =
        MetaDataUtil.getViewIndexPhysicalName(tableName);
      verifyTableDescriptorProperty(conn, physicalViewIndexName, CUSTOM_PROP_1,
        INITIAL_PROP_1_VALUE);
      verifyTableDescriptorProperty(conn, physicalViewIndexName, CUSTOM_PROP_2,
        INITIAL_PROP_2_VALUE);
    }
  }

  // Test that altering custom table descriptor properties on data table propagates to global index
  @Test
  public void testAlterTablePropagatesInheritablePropsToGlobalIndex() throws Exception {
    try (Connection conn = DriverManager.getConnection(getUrl(), new Properties())) {
      String tableName = generateUniqueName();
      String indexName = generateUniqueName();
      conn.createStatement().execute("CREATE TABLE " + tableName
        + " (id INTEGER NOT NULL PRIMARY KEY, name VARCHAR(10), flag BOOLEAN)"
        + " \"" + CUSTOM_PROP_1 + "\"='" + INITIAL_PROP_1_VALUE + "',"
        + " \"" + CUSTOM_PROP_2 + "\"='" + INITIAL_PROP_2_VALUE + "'");
      conn.createStatement()
        .execute("CREATE INDEX " + indexName + " ON " + tableName + "(name)");

      // Alter the base table's custom property
      conn.createStatement().execute("ALTER TABLE " + tableName
        + " SET \"" + CUSTOM_PROP_1 + "\"='" + MODIFIED_PROP_1_VALUE + "'");

      verifyTableDescriptorProperty(conn, tableName, CUSTOM_PROP_1, MODIFIED_PROP_1_VALUE);
      verifyTableDescriptorProperty(conn, indexName, CUSTOM_PROP_1, MODIFIED_PROP_1_VALUE);
      // CUSTOM_PROP_2 should remain unchanged
      verifyTableDescriptorProperty(conn, indexName, CUSTOM_PROP_2, INITIAL_PROP_2_VALUE);
    }
  }

  // Test that altering custom table descriptor properties on data table propagates to view index
  @Test
  public void testAlterTablePropagatesInheritablePropsToViewIndex() throws Exception {
    try (Connection conn = DriverManager.getConnection(getUrl(), new Properties())) {
      String tableName = generateUniqueName();
      String viewName = generateUniqueName();
      String viewIndexName = generateUniqueName();
      conn.createStatement().execute("CREATE TABLE " + tableName
        + " (id INTEGER NOT NULL PRIMARY KEY, name VARCHAR(10), flag BOOLEAN)"
        + " \"" + CUSTOM_PROP_1 + "\"='" + INITIAL_PROP_1_VALUE + "',"
        + " \"" + CUSTOM_PROP_2 + "\"='" + INITIAL_PROP_2_VALUE + "'");
      conn.createStatement().execute("CREATE VIEW " + viewName
        + " AS SELECT * FROM " + tableName + " WHERE id > 1");
      conn.createStatement()
        .execute("CREATE INDEX " + viewIndexName + " ON " + viewName + "(name)");

      conn.createStatement().execute("ALTER TABLE " + tableName
        + " SET \"" + CUSTOM_PROP_1 + "\"='" + MODIFIED_PROP_1_VALUE + "',"
        + " \"" + CUSTOM_PROP_2 + "\"='" + MODIFIED_PROP_2_VALUE + "'");

      String physicalViewIndexName =
        MetaDataUtil.getViewIndexPhysicalName(tableName);
      verifyTableDescriptorProperty(conn, physicalViewIndexName, CUSTOM_PROP_1,
        MODIFIED_PROP_1_VALUE);
      verifyTableDescriptorProperty(conn, physicalViewIndexName, CUSTOM_PROP_2,
        MODIFIED_PROP_2_VALUE);
    }
  }

  // Test that altering custom table descriptor properties on data table propagates to multiple
  // indexes simultaneously
  @Test
  public void testAlterTablePropagatesInheritablePropsToMultipleIndexes() throws Exception {
    try (Connection conn = DriverManager.getConnection(getUrl(), new Properties())) {
      String tableName = generateUniqueName();
      String globalIndex1 = generateUniqueName();
      String globalIndex2 = generateUniqueName();
      String viewName = generateUniqueName();
      String viewIndexName = generateUniqueName();
      conn.createStatement().execute("CREATE TABLE " + tableName
        + " (id INTEGER NOT NULL PRIMARY KEY, name VARCHAR(10), flag BOOLEAN)"
        + " \"" + CUSTOM_PROP_1 + "\"='" + INITIAL_PROP_1_VALUE + "'");
      conn.createStatement()
        .execute("CREATE INDEX " + globalIndex1 + " ON " + tableName + "(name)");
      conn.createStatement()
        .execute("CREATE INDEX " + globalIndex2 + " ON " + tableName + "(flag)");
      conn.createStatement().execute("CREATE VIEW " + viewName
        + " AS SELECT * FROM " + tableName + " WHERE id > 1");
      conn.createStatement()
        .execute("CREATE INDEX " + viewIndexName + " ON " + viewName + "(name)");

      conn.createStatement().execute("ALTER TABLE " + tableName
        + " SET \"" + CUSTOM_PROP_1 + "\"='" + MODIFIED_PROP_1_VALUE + "'");

      verifyTableDescriptorProperty(conn, tableName, CUSTOM_PROP_1, MODIFIED_PROP_1_VALUE);
      verifyTableDescriptorProperty(conn, globalIndex1, CUSTOM_PROP_1, MODIFIED_PROP_1_VALUE);
      verifyTableDescriptorProperty(conn, globalIndex2, CUSTOM_PROP_1, MODIFIED_PROP_1_VALUE);
      String physicalViewIndexName =
        MetaDataUtil.getViewIndexPhysicalName(tableName);
      verifyTableDescriptorProperty(conn, physicalViewIndexName, CUSTOM_PROP_1,
        MODIFIED_PROP_1_VALUE);
    }
  }

  // Test that setting an inheritable property directly on a global index is disallowed
  @Test
  public void testDisallowSettingInheritablePropOnGlobalIndex() throws Exception {
    try (Connection conn = DriverManager.getConnection(getUrl(), new Properties())) {
      String tableName = generateUniqueName();
      String indexName = generateUniqueName();
      conn.createStatement().execute("CREATE TABLE " + tableName
        + " (id INTEGER NOT NULL PRIMARY KEY, name VARCHAR(10), flag BOOLEAN)");
      conn.createStatement()
        .execute("CREATE INDEX " + indexName + " ON " + tableName + "(name)");

      try {
        conn.createStatement().execute("ALTER TABLE " + indexName
          + " SET \"" + CUSTOM_PROP_1 + "\"='" + INITIAL_PROP_1_VALUE + "'");
        fail("Should fail when setting an inheritable property directly on an index table");
      } catch (SQLException e) {
        assertEquals(
          SQLExceptionCode.CANNOT_SET_OR_ALTER_PROPERTY_FOR_INDEX.getErrorCode(),
          e.getErrorCode());
      }
    }
  }

  // Test that specifying an inheritable property during CREATE INDEX is disallowed
  @Test
  public void testDisallowInheritablePropDuringCreateIndex() throws Exception {
    try (Connection conn = DriverManager.getConnection(getUrl(), new Properties())) {
      String tableName = generateUniqueName();
      String indexName = generateUniqueName();
      conn.createStatement().execute("CREATE TABLE " + tableName
        + " (id INTEGER NOT NULL PRIMARY KEY, name VARCHAR(10), flag BOOLEAN)");

      try {
        conn.createStatement().execute("CREATE INDEX " + indexName + " ON " + tableName
          + "(name) \"" + CUSTOM_PROP_1 + "\"='" + INITIAL_PROP_1_VALUE + "'");
        fail("Should fail when specifying an inheritable property during CREATE INDEX");
      } catch (SQLException e) {
        assertEquals(
          SQLExceptionCode.CANNOT_SET_OR_ALTER_PROPERTY_FOR_INDEX.getErrorCode(),
          e.getErrorCode());
      }
    }
  }

  // Test that non-inheritable custom properties are not propagated to indexes
  @Test
  public void testNonInheritablePropsNotPropagated() throws Exception {
    try (Connection conn = DriverManager.getConnection(getUrl(), new Properties())) {
      String tableName = generateUniqueName();
      String indexName = generateUniqueName();
      String nonInheritableProp = "non.inheritable.prop";
      conn.createStatement().execute("CREATE TABLE " + tableName
        + " (id INTEGER NOT NULL PRIMARY KEY, name VARCHAR(10), flag BOOLEAN)"
        + " \"" + nonInheritableProp + "\"='somevalue'");
      conn.createStatement()
        .execute("CREATE INDEX " + indexName + " ON " + tableName + "(name)");

      // The non-inheritable property should NOT be on the index table
      verifyTableDescriptorProperty(conn, indexName, nonInheritableProp, null);
    }
  }

  // Test that indexes created after ALTER TABLE get the latest property values
  @Test
  public void testNewIndexGetsLatestInheritableProps() throws Exception {
    try (Connection conn = DriverManager.getConnection(getUrl(), new Properties())) {
      String tableName = generateUniqueName();
      String indexBefore = generateUniqueName();
      String indexAfter = generateUniqueName();
      conn.createStatement().execute("CREATE TABLE " + tableName
        + " (id INTEGER NOT NULL PRIMARY KEY, name VARCHAR(10), flag BOOLEAN)"
        + " \"" + CUSTOM_PROP_1 + "\"='" + INITIAL_PROP_1_VALUE + "'");
      conn.createStatement()
        .execute("CREATE INDEX " + indexBefore + " ON " + tableName + "(name)");

      // Alter the base table
      conn.createStatement().execute("ALTER TABLE " + tableName
        + " SET \"" + CUSTOM_PROP_1 + "\"='" + MODIFIED_PROP_1_VALUE + "'");

      // Create a new index after alteration
      conn.createStatement()
        .execute("CREATE INDEX " + indexAfter + " ON " + tableName + "(flag)");

      verifyTableDescriptorProperty(conn, indexBefore, CUSTOM_PROP_1, MODIFIED_PROP_1_VALUE);
      verifyTableDescriptorProperty(conn, indexAfter, CUSTOM_PROP_1, MODIFIED_PROP_1_VALUE);
    }
  }

  // Test when no inheritable properties are configured, custom properties should not propagate
  // (this test uses the default configured properties from doSetup, so we check that only
  // CUSTOM_PROP_1 and CUSTOM_PROP_2 propagate and nothing else)
  @Test
  public void testOnlyConfiguredPropsAreInherited() throws Exception {
    try (Connection conn = DriverManager.getConnection(getUrl(), new Properties())) {
      String tableName = generateUniqueName();
      String indexName = generateUniqueName();
      String extraProp = "some.other.custom.prop";
      conn.createStatement().execute("CREATE TABLE " + tableName
        + " (id INTEGER NOT NULL PRIMARY KEY, name VARCHAR(10), flag BOOLEAN)"
        + " \"" + CUSTOM_PROP_1 + "\"='" + INITIAL_PROP_1_VALUE + "',"
        + " \"" + extraProp + "\"='extraval'");
      conn.createStatement()
        .execute("CREATE INDEX " + indexName + " ON " + tableName + "(name)");

      // Configured inheritable prop should propagate
      verifyTableDescriptorProperty(conn, indexName, CUSTOM_PROP_1, INITIAL_PROP_1_VALUE);
      // Non-configured prop should not propagate
      verifyTableDescriptorProperty(conn, indexName, extraProp, null);
    }
  }

  private void verifyTableDescriptorProperty(Connection conn, String tableName,
      String propertyName, String expectedValue) throws Exception {
    try (Admin admin = conn.unwrap(PhoenixConnection.class).getQueryServices().getAdmin()) {
      TableDescriptor td = admin.getDescriptor(TableName.valueOf(tableName));
      String actualValue = td.getValue(propertyName);
      assertEquals("Mismatch for property " + propertyName + " on table " + tableName,
        expectedValue, actualValue);
    }
  }
}
