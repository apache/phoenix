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

import static java.lang.String.format;
import static org.apache.phoenix.jdbc.PhoenixDatabaseMetaData.TABLE_CAT;
import static org.apache.phoenix.jdbc.PhoenixDatabaseMetaData.TABLE_CATALOG;
import static org.apache.phoenix.jdbc.PhoenixDatabaseMetaData.TABLE_SCHEM;
import static org.apache.phoenix.query.QueryConstants.SYSTEM_SCHEMA_NAME;
import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.Statement;
import java.util.Properties;
import java.util.concurrent.TimeUnit;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.phoenix.query.QueryServices;
import org.apache.phoenix.queryserver.client.ThinClientUtil;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

/**
 * Smoke test for query server.
 */
public class QueryServerBasicsIT extends BaseHBaseManagedTimeIT {

  private static final Log LOG = LogFactory.getLog(QueryServerBasicsIT.class);

  private static QueryServerThread AVATICA_SERVER;
  private static Configuration CONF;
  private static String CONN_STRING;

  @BeforeClass
  public static void beforeClass() throws Exception {
    CONF = getTestClusterConfig();
    CONF.setInt(QueryServices.QUERY_SERVER_HTTP_PORT_ATTRIB, 0);
    String url = getUrl();
    AVATICA_SERVER = new QueryServerThread(new String[] { url }, CONF,
            QueryServerBasicsIT.class.getName());
    AVATICA_SERVER.start();
    AVATICA_SERVER.getQueryServer().awaitRunning();
    final int port = AVATICA_SERVER.getQueryServer().getPort();
    LOG.info("Avatica server started on port " + port);
    CONN_STRING = ThinClientUtil.getConnectionUrl("localhost", port);
    LOG.info("JDBC connection string is " + CONN_STRING);
  }

  @AfterClass
  public static void afterClass() throws Exception {
    if (AVATICA_SERVER != null) {
      AVATICA_SERVER.join(TimeUnit.MINUTES.toMillis(1));
      Throwable t = AVATICA_SERVER.getQueryServer().getThrowable();
      if (t != null) {
        fail("query server threw. " + t.getMessage());
      }
      assertEquals("query server didn't exit cleanly", 0, AVATICA_SERVER.getQueryServer()
        .getRetCode());
    }
  }

  @Test
  public void testCatalogs() throws Exception {
    try (final Connection connection = DriverManager.getConnection(CONN_STRING)) {
      assertThat(connection.isClosed(), is(false));
      try (final ResultSet resultSet = connection.getMetaData().getCatalogs()) {
        final ResultSetMetaData metaData = resultSet.getMetaData();
        assertFalse("unexpected populated resultSet", resultSet.next());
        assertEquals(1, metaData.getColumnCount());
        assertEquals(TABLE_CAT, metaData.getColumnName(1));
      }
    }
  }

  @Test
  public void testSchemas() throws Exception {
      Properties props=new Properties();
      props.setProperty(QueryServices.IS_NAMESPACE_MAPPING_ENABLED, Boolean.toString(true));
      try (final Connection connection = DriverManager.getConnection(CONN_STRING, props)) {
      connection.createStatement().executeUpdate("CREATE SCHEMA IF NOT EXISTS " + SYSTEM_SCHEMA_NAME);
      assertThat(connection.isClosed(), is(false));
      try (final ResultSet resultSet = connection.getMetaData().getSchemas()) {
        final ResultSetMetaData metaData = resultSet.getMetaData();
        assertTrue("unexpected empty resultset", resultSet.next());
        assertEquals(2, metaData.getColumnCount());
        assertEquals(TABLE_SCHEM, metaData.getColumnName(1));
        assertEquals(TABLE_CATALOG, metaData.getColumnName(2));
        boolean containsSystem = false;
        do {
          if (resultSet.getString(1).equalsIgnoreCase(SYSTEM_SCHEMA_NAME)) containsSystem = true;
        } while (resultSet.next());
        assertTrue(format("should contain at least %s schema.", SYSTEM_SCHEMA_NAME), containsSystem);
      }
    }
  }

  @Test
  public void smokeTest() throws Exception {
    final String tableName = getClass().getSimpleName().toUpperCase() + System.currentTimeMillis();
    try (final Connection connection = DriverManager.getConnection(CONN_STRING)) {
      assertThat(connection.isClosed(), is(false));
      connection.setAutoCommit(true);
      try (final Statement stmt = connection.createStatement()) {
        assertFalse(stmt.execute("DROP TABLE IF EXISTS " + tableName));
        assertFalse(stmt.execute("CREATE TABLE " + tableName + "("
            + "id INTEGER NOT NULL, "
            + "pk varchar(3) NOT NULL "
            + "CONSTRAINT PK_CONSTRAINT PRIMARY KEY (id, pk))"));
        assertEquals(0, stmt.getUpdateCount());
        assertEquals(1, stmt.executeUpdate("UPSERT INTO " + tableName + " VALUES(1, 'foo')"));
        assertEquals(1, stmt.executeUpdate("UPSERT INTO " + tableName + " VALUES(2, 'bar')"));
        assertTrue(stmt.execute("SELECT * FROM " + tableName));
        try (final ResultSet resultSet = stmt.getResultSet()) {
          assertTrue(resultSet.next());
          assertEquals(1, resultSet.getInt(1));
          assertEquals("foo", resultSet.getString(2));
          assertTrue(resultSet.next());
          assertEquals(2, resultSet.getInt(1));
          assertEquals("bar", resultSet.getString(2));
        }
      }
      final String sql = "SELECT * FROM " + tableName + " WHERE id = ?";
      try (final PreparedStatement stmt = connection.prepareStatement(sql)) {
        stmt.setInt(1, 1);
        try (ResultSet resultSet = stmt.executeQuery()) {
          assertTrue(resultSet.next());
          assertEquals(1, resultSet.getInt(1));
          assertEquals("foo", resultSet.getString(2));
        }
        stmt.clearParameters();
        stmt.setInt(1, 5);
        try (final ResultSet resultSet = stmt.executeQuery()) {
          assertFalse(resultSet.next());
        }
      }
    }
  }
}
