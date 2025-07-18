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

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.Statement;
import java.util.HashMap;
import java.util.Map;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.phoenix.hbase.index.util.IndexManagementUtil;
import org.apache.phoenix.query.ConfigurationFactory;
import org.apache.phoenix.query.QueryServices;
import org.apache.phoenix.util.InstanceResolver;
import org.apache.phoenix.util.ReadOnlyProps;
import org.junit.BeforeClass;
import org.junit.experimental.categories.Category;

@Category(NeedsOwnMiniClusterTest.class)
public class LongViewIndexEnabledBaseRowKeyMatcherIT extends BaseRowKeyMatcherTestIT {

  @BeforeClass
  public static synchronized void doSetup() throws Exception {
    final Configuration conf = HBaseConfiguration.create();
    conf.set(QueryServices.PHOENIX_COMPACTION_ENABLED, String.valueOf(true));
    conf.set(QueryServices.SYSTEM_CATALOG_INDEXES_ENABLED, String.valueOf(true));
    conf.set(QueryServices.LONG_VIEW_INDEX_ENABLED_ATTRIB, String.valueOf(true));
    conf.set(QueryServices.INDEX_REGION_OBSERVER_ENABLED_ATTRIB, "true");
    conf.set(IndexManagementUtil.WAL_EDIT_CODEC_CLASS_KEY,
      "org.apache.hadoop.hbase.regionserver.wal.IndexedWALEditCodec");

    // Clear the cached singletons so we can inject our own.
    InstanceResolver.clearSingletons();
    // Make sure the ConnectionInfo doesn't try to pull a default Configuration
    InstanceResolver.getSingleton(ConfigurationFactory.class, new ConfigurationFactory() {
      @Override
      public Configuration getConfiguration() {
        return conf;
      }

      @Override
      public Configuration getConfiguration(Configuration confToClone) {
        Configuration copy = new Configuration(conf);
        copy.addResource(confToClone);
        return copy;
      }
    });

    // Turn on the Long view index feature
    Map<String, String> DEFAULT_PROPERTIES = new HashMap<String, String>() {
      {
        put(QueryServices.LONG_VIEW_INDEX_ENABLED_ATTRIB, String.valueOf(true));
      }
    };

    setUpTestDriver(
      new ReadOnlyProps(ReadOnlyProps.EMPTY_PROPS, DEFAULT_PROPERTIES.entrySet().iterator()));

    // Create the CATALOG indexes for additional verifications using the catalog indexes
    try (Connection conn = DriverManager.getConnection(getUrl());
      Statement stmt = conn.createStatement()) {
      // TestUtil.dumpTable(conn,
      // TableName.valueOf(PhoenixDatabaseMetaData.SYSTEM_CATALOG_NAME_BYTES));
      stmt.execute(
        "CREATE INDEX IF NOT EXISTS SYS_VIEW_HDR_IDX ON SYSTEM.CATALOG(TENANT_ID, TABLE_SCHEM, TABLE_NAME, COLUMN_NAME, COLUMN_FAMILY) INCLUDE (TABLE_TYPE, VIEW_STATEMENT, TTL, ROW_KEY_MATCHER) WHERE TABLE_TYPE = 'v'");
      stmt.execute(
        "CREATE INDEX IF NOT EXISTS SYS_ROW_KEY_MATCHER_IDX ON SYSTEM.CATALOG(ROW_KEY_MATCHER, TTL, TABLE_TYPE, TENANT_ID, TABLE_SCHEM, TABLE_NAME) INCLUDE (VIEW_STATEMENT) WHERE TABLE_TYPE = 'v' AND ROW_KEY_MATCHER IS NOT NULL");
      stmt.execute(
        "CREATE INDEX IF NOT EXISTS SYS_VIEW_INDEX_HDR_IDX ON SYSTEM.CATALOG(DECODE_VIEW_INDEX_ID(VIEW_INDEX_ID, VIEW_INDEX_ID_DATA_TYPE), TENANT_ID, TABLE_SCHEM, TABLE_NAME) INCLUDE(TABLE_TYPE, LINK_TYPE, VIEW_INDEX_ID, VIEW_INDEX_ID_DATA_TYPE)  WHERE TABLE_TYPE = 'i' AND LINK_TYPE IS NULL AND VIEW_INDEX_ID IS NOT NULL");
      conn.commit();
    }
  }

  @Override
  protected boolean hasLongViewIndexEnabled() {
    return true;
  }
}
