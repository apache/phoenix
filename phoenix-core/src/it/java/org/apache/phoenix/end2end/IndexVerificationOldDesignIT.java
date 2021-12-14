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

import org.apache.phoenix.compat.hbase.coprocessor.CompatBaseScannerRegionObserver;
import org.apache.phoenix.thirdparty.com.google.common.collect.Maps;
import org.apache.phoenix.mapreduce.index.IndexTool;
import org.apache.phoenix.query.BaseTest;
import org.apache.phoenix.query.QueryServices;
import org.apache.phoenix.query.QueryServicesOptions;
import org.apache.phoenix.util.EnvironmentEdgeManager;
import org.apache.phoenix.util.ManualEnvironmentEdge;
import org.apache.phoenix.util.PropertiesUtil;
import org.apache.phoenix.util.ReadOnlyProps;
import org.apache.phoenix.util.SchemaUtil;

import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.Map;
import java.util.Properties;

import static org.apache.phoenix.mapreduce.index.PhoenixIndexToolJobCounters.BEFORE_REBUILD_BEYOND_MAXLOOKBACK_MISSING_INDEX_ROW_COUNT;
import static org.apache.phoenix.mapreduce.index.PhoenixIndexToolJobCounters.BEFORE_REBUILD_EXPIRED_INDEX_ROW_COUNT;
import static org.apache.phoenix.mapreduce.index.PhoenixIndexToolJobCounters.BEFORE_REBUILD_INVALID_INDEX_ROW_COUNT;
import static org.apache.phoenix.mapreduce.index.PhoenixIndexToolJobCounters.BEFORE_REBUILD_MISSING_INDEX_ROW_COUNT;
import static org.apache.phoenix.mapreduce.index.PhoenixIndexToolJobCounters.BEFORE_REBUILD_VALID_INDEX_ROW_COUNT;
import static org.apache.phoenix.util.TestUtil.TEST_PROPERTIES;
import static org.junit.Assert.assertEquals;

@Category(NeedsOwnMiniClusterTest.class)
public class IndexVerificationOldDesignIT extends BaseTest {

    ManualEnvironmentEdge injectEdge;

    @BeforeClass
    public static synchronized void setup() throws Exception {
        Map<String, String> serverProps = Maps.newHashMapWithExpectedSize(2);
        serverProps.put(QueryServices.STATS_GUIDEPOST_WIDTH_BYTES_ATTRIB, Long.toString(20));
        serverProps.put(QueryServices.MAX_SERVER_METADATA_CACHE_TIME_TO_LIVE_MS_ATTRIB, Long.toString(5));
        serverProps.put(QueryServices.EXTRA_JDBC_ARGUMENTS_ATTRIB,
                QueryServicesOptions.DEFAULT_EXTRA_JDBC_ARGUMENTS);
        serverProps.put(QueryServices.INDEX_REBUILD_PAGE_SIZE_IN_ROWS, Long.toString(8));
        serverProps.put(CompatBaseScannerRegionObserver.PHOENIX_MAX_LOOKBACK_AGE_CONF_KEY, Integer.toString(60*60)); // An hour
        Map<String, String> clientProps = Maps.newHashMapWithExpectedSize(2);
        clientProps.put(QueryServices.USE_STATS_FOR_PARALLELIZATION, Boolean.toString(true));
        clientProps.put(QueryServices.STATS_UPDATE_FREQ_MS_ATTRIB, Long.toString(5));
        clientProps.put(QueryServices.TRANSACTIONS_ENABLED, Boolean.TRUE.toString());
        clientProps.put(QueryServices.FORCE_ROW_KEY_ORDER_ATTRIB, Boolean.TRUE.toString());
        clientProps.put(QueryServices.INDEX_REGION_OBSERVER_ENABLED_ATTRIB,
                Boolean.toString(false));
        setUpTestDriver(new ReadOnlyProps(serverProps.entrySet().iterator()),
                new ReadOnlyProps(clientProps.entrySet().iterator()));
    }

    @Test
    public void testIndexToolOnlyVerifyOption() throws Exception {
        long ttl=3600;
        String schemaName = generateUniqueName();
        String dataTableName = generateUniqueName();
        String dataTableFullName = SchemaUtil.getTableName(schemaName, dataTableName);
        String indexTableName = generateUniqueName();
        String indexTableFullName = SchemaUtil.getTableName(schemaName, indexTableName);
        Properties props = PropertiesUtil.deepCopy(TEST_PROPERTIES);
        try (Connection conn = DriverManager.getConnection(getUrl(), props)) {
            conn.createStatement().execute("CREATE TABLE " + dataTableFullName
                    + " (ID INTEGER NOT NULL PRIMARY KEY, NAME VARCHAR, CODE VARCHAR) COLUMN_ENCODED_BYTES=0, TTL="+ttl);
            conn.createStatement().execute(String.format(
                    "CREATE INDEX %s ON %s (NAME) INCLUDE (CODE)", indexTableName, dataTableFullName));

            upsertValidRows(conn, dataTableFullName);

            IndexTool indexTool = IndexToolIT.runIndexTool(false, schemaName, dataTableName, indexTableName,
                    null, 0, IndexTool.IndexVerifyType.ONLY);

            assertEquals(0, indexTool.getJob().getCounters().findCounter(BEFORE_REBUILD_INVALID_INDEX_ROW_COUNT).getValue());
            assertEquals(6, indexTool.getJob().getCounters().findCounter(BEFORE_REBUILD_VALID_INDEX_ROW_COUNT).getValue());
            assertEquals(0, indexTool.getJob().getCounters().findCounter(BEFORE_REBUILD_MISSING_INDEX_ROW_COUNT).getValue());

            conn.createStatement().execute("upsert into " + indexTableFullName + " values ('Phoenix5', 6,'G')");
            conn.commit();
            indexTool = IndexToolIT.runIndexTool(false, schemaName, dataTableName, indexTableName,
                    null, 0, IndexTool.IndexVerifyType.ONLY);

            assertEquals(1, indexTool.getJob().getCounters().findCounter(BEFORE_REBUILD_INVALID_INDEX_ROW_COUNT).getValue());
            assertEquals(5, indexTool.getJob().getCounters().findCounter(BEFORE_REBUILD_VALID_INDEX_ROW_COUNT).getValue());
            assertEquals(0, indexTool.getJob().getCounters().findCounter(BEFORE_REBUILD_MISSING_INDEX_ROW_COUNT).getValue());

            injectEdge = new ManualEnvironmentEdge();
            injectEdge.setValue(EnvironmentEdgeManager.currentTimeMillis() + ttl*1000);
            EnvironmentEdgeManager.injectEdge(injectEdge);

            indexTool = IndexToolIT.runIndexTool(false, schemaName, dataTableName, indexTableName,
                    null, 0, IndexTool.IndexVerifyType.ONLY);

            assertEquals(0, indexTool.getJob().getCounters().findCounter(BEFORE_REBUILD_INVALID_INDEX_ROW_COUNT).getValue());
            assertEquals(0, indexTool.getJob().getCounters().findCounter(BEFORE_REBUILD_VALID_INDEX_ROW_COUNT).getValue());
            assertEquals(0, indexTool.getJob().getCounters().findCounter(BEFORE_REBUILD_MISSING_INDEX_ROW_COUNT).getValue());
            assertEquals(0, indexTool.getJob().getCounters().findCounter(BEFORE_REBUILD_EXPIRED_INDEX_ROW_COUNT).getValue());
        } finally {
            EnvironmentEdgeManager.reset();
        }
    }

    @Test
    public void testIndexToolOnlyVerifyOption_viewIndex() throws Exception {
        String schemaName = generateUniqueName();
        String dataTableName = generateUniqueName();
        String dataTableFullName = SchemaUtil.getTableName(schemaName, dataTableName);
        String indexTableName = generateUniqueName();
        String viewName = generateUniqueName();
        String fullViewName = SchemaUtil.getTableName(schemaName, viewName);
        String indexTableFullName = SchemaUtil.getTableName(schemaName, indexTableName);
        Properties props = PropertiesUtil.deepCopy(TEST_PROPERTIES);
        try (Connection conn = DriverManager.getConnection(getUrl(), props)) {
            conn.createStatement().execute("CREATE TABLE " + dataTableFullName
                    + " (ID INTEGER NOT NULL PRIMARY KEY, NAME VARCHAR, CODE VARCHAR) COLUMN_ENCODED_BYTES=0");
            conn.createStatement().execute("CREATE VIEW " + fullViewName
                    + " AS SELECT * FROM "+dataTableFullName);
            conn.createStatement().execute(String.format(
                    "CREATE INDEX %s ON %s (NAME) INCLUDE (CODE)", indexTableName, fullViewName));

            upsertValidRows(conn, fullViewName);

            IndexToolIT.runIndexTool(false, schemaName, viewName, indexTableName,
                    null, 0, IndexTool.IndexVerifyType.ONLY);

            conn.createStatement().execute("upsert into " + indexTableFullName + " values ('Phoenix5', 6,'G')");
            conn.createStatement().execute("delete from " + indexTableFullName + " where \"0:CODE\" = 'D'");
            conn.commit();

            IndexTool indexTool = IndexToolIT.runIndexTool(false, schemaName, viewName, indexTableName,
                    null, 0, IndexTool.IndexVerifyType.ONLY);
            assertEquals(1, indexTool.getJob().getCounters().findCounter(BEFORE_REBUILD_INVALID_INDEX_ROW_COUNT).getValue());
            assertEquals(4, indexTool.getJob().getCounters().findCounter(BEFORE_REBUILD_VALID_INDEX_ROW_COUNT).getValue());
            if (CompatBaseScannerRegionObserver.isMaxLookbackTimeEnabled(getUtility().getConfiguration())) {
                assertEquals(1, indexTool.getJob().getCounters().findCounter(BEFORE_REBUILD_MISSING_INDEX_ROW_COUNT).getValue());
            } else {
                assertEquals(1, indexTool.getJob().getCounters().findCounter(BEFORE_REBUILD_BEYOND_MAXLOOKBACK_MISSING_INDEX_ROW_COUNT).getValue());
            }
        }
    }

    private void upsertValidRows(Connection conn, String table) throws SQLException {
        conn.createStatement().execute("upsert into " + table + " values (1, 'Phoenix', 'A')");
        conn.createStatement().execute("upsert into " + table + " values (2, 'Phoenix1', 'B')");
        conn.createStatement().execute("upsert into " + table + " values (3, 'Phoenix2', 'C')");
        conn.createStatement().execute("upsert into " + table + " values (4, 'Phoenix3', 'D')");
        conn.createStatement().execute("upsert into " + table + " values (5, 'Phoenix4', 'E')");
        conn.createStatement().execute("upsert into " + table + " values (6, 'Phoenix5', 'F')");
        conn.commit();
    }

}
