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

import static org.apache.phoenix.util.TestUtil.TEST_PROPERTIES;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.Map;
import java.util.Properties;
import org.apache.phoenix.coprocessorclient.BaseScannerRegionObserverConstants;
import org.apache.phoenix.query.BaseTest;
import org.apache.phoenix.query.QueryServices;
import org.apache.phoenix.util.PropertiesUtil;
import org.apache.phoenix.util.QueryUtil;
import org.apache.phoenix.util.ReadOnlyProps;
import org.apache.phoenix.util.StringUtil;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import org.apache.phoenix.thirdparty.com.google.common.collect.Maps;

/**
 * Test to verify that metadata operations work correctly with a single metadata handler and a
 * single server-to-server handler.
 */
@Category(NeedsOwnMiniClusterTest.class)
public class SingleMetadataHandlerIT extends BaseTest {

  private Properties props = PropertiesUtil.deepCopy(TEST_PROPERTIES);

  @BeforeClass
  public static synchronized void doSetup() throws Exception {
    Map<String, String> props = Maps.newConcurrentMap();
    props.put(QueryServices.DROP_METADATA_ATTRIB, Boolean.TRUE.toString());
    props.put(BaseScannerRegionObserverConstants.PHOENIX_MAX_LOOKBACK_AGE_CONF_KEY,
      Integer.toString(60 * 60 * 1000));
    // single metadata rpc handler thread
    props.put(QueryServices.METADATA_HANDLER_COUNT_ATTRIB, Integer.toString(1));
    // single server-to-server rpc handler thread
    props.put(QueryServices.SERVER_SIDE_HANDLER_COUNT_ATTRIB, Integer.toString(1));
    setUpTestDriver(new ReadOnlyProps(props.entrySet().iterator()));
  }

  private Connection getConnection(Properties props) throws Exception {
    props.setProperty(QueryServices.DROP_METADATA_ATTRIB, Boolean.toString(true));
    props.setProperty(QueryServices.EXTRA_JDBC_ARGUMENTS_ATTRIB, StringUtil.EMPTY_STRING);
    String url = QueryUtil.getConnectionUrl(props, config, "PRINCIPAL");
    return DriverManager.getConnection(url, props);
  }

  private void createTable(Connection conn, String tableName) throws Exception {
    String createTableSql =
      "CREATE TABLE " + tableName + " (PK1 VARCHAR NOT NULL, V1 VARCHAR, V2 INTEGER, V3 INTEGER "
        + "CONSTRAINT NAME_PK PRIMARY KEY(PK1)) COLUMN_ENCODED_BYTES=0";
    conn.createStatement().execute(createTableSql);
  }

  private void createIndexOnTable(Connection conn, String tableName, String indexName)
    throws SQLException {
    String createIndexSql =
      "CREATE INDEX " + indexName + " ON " + tableName + " (V1) INCLUDE (V2, V3) ";
    conn.createStatement().execute(createIndexSql);
  }

  /**
   * Test that verifies metadata operations work correctly with a single metadata handler and single
   * server side handler.
   */
  @Test
  public void testSingleMetadataHandler() throws Exception {
    try (Connection conn = getConnection(props)) {
      String tableName = "TBL_" + generateUniqueName();

      conn.setAutoCommit(true);
      createTable(conn, tableName);
      createIndexOnTable(conn, tableName, "IDX_" + generateUniqueName());

      String view1DDL = "CREATE VIEW IF NOT EXISTS VIEW1"
        + " ( VIEW_COL1 VARCHAR, VIEW_COL2 VARCHAR) AS SELECT * FROM " + tableName;
      conn.createStatement().execute(view1DDL);
      String indexDDL =
        "CREATE INDEX IF NOT EXISTS V1IDX1" + " ON VIEW1" + " (V1) include (V2, V3, VIEW_COL2) ";
      conn.createStatement().execute(indexDDL);
      conn.commit();
    }
  }
}
