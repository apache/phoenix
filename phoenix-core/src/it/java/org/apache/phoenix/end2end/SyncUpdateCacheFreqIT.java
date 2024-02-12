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

import org.apache.phoenix.jdbc.PhoenixConnection;
import org.apache.phoenix.query.BaseTest;
import org.apache.phoenix.thirdparty.com.google.common.collect.ImmutableList;
import org.apache.phoenix.thirdparty.com.google.common.collect.ImmutableMap;
import org.apache.phoenix.thirdparty.com.google.common.collect.ImmutableSet;
import org.apache.phoenix.thirdparty.com.google.common.collect.Maps;
import org.apache.phoenix.util.PhoenixRuntime;
import org.apache.phoenix.util.ReadOnlyProps;
import org.apache.phoenix.util.SchemaUtil;
import org.apache.phoenix.util.UpgradeUtil;
import org.junit.BeforeClass;
import org.junit.Test;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Random;
import java.util.Set;

import static org.apache.phoenix.util.UpgradeUtil.UPSERT_UPDATE_CACHE_FREQUENCY;
import static org.junit.Assert.assertEquals;

//FIXME this class no @Category, and will not be run by maven
public class SyncUpdateCacheFreqIT extends BaseTest {

    private static final String SCHEMA_NAME = "SCHEMA2";
    private static final String TABLE_NAME = generateUniqueName();
    private static String tenant_name;
    private static final String GLOBAL_INDEX = "GLOBAL_INDEX";
    private static final String LOCAL_INDEX = "LOCAL_INDEX";
    private static final String VIEW1_NAME = "VIEW1";
    private static final String VIEW1_INDEX1_NAME = "VIEW1_INDEX1";
    private static final String VIEW1_INDEX2_NAME = "VIEW1_INDEX2";
    private static final String VIEW2_NAME = "VIEW2";
    private static final String VIEW2_INDEX1_NAME = "VIEW2_INDEX1";
    private static final String VIEW2_INDEX2_NAME = "VIEW2_INDEX2";
    private static final String VIEW_INDEX_COL = "v2";
    private static final List<String> INDEXS_TO_UPDATE_CACHE_FREQ =
        ImmutableList.of(VIEW1_INDEX1_NAME, VIEW2_INDEX1_NAME, VIEW1_INDEX2_NAME,
            VIEW2_INDEX2_NAME);
    private static final Map<String, List<String>> TABLE_TO_INDEX =
        ImmutableMap.of(TABLE_NAME, ImmutableList.of(GLOBAL_INDEX, LOCAL_INDEX),
            VIEW1_NAME, ImmutableList.of(VIEW1_INDEX1_NAME, VIEW1_INDEX2_NAME),
            VIEW2_NAME, ImmutableList.of(VIEW2_INDEX1_NAME, VIEW2_INDEX2_NAME));
    private static final Set<String> GLOBAL_TABLES =
        ImmutableSet.of(GLOBAL_INDEX, LOCAL_INDEX, TABLE_NAME);

    private static final int TABLE_CACHE_FREQ = 5000;
    private static final int VIEW_CACHE_FREQ = 7000;
    private static final Random RANDOM_INT = new Random();

    private static final String CREATE_GLOBAL_INDEX = "CREATE INDEX %s ON %s(%s)";
    private static final String CREATE_LOCAL_INDEX = "CREATE LOCAL INDEX %s ON %s(%s)";

    @BeforeClass
    public static synchronized void doSetup() throws Exception {
        Map<String, String> props = Maps.newHashMapWithExpectedSize(1);
        setUpTestDriver(new ReadOnlyProps(props.entrySet().iterator()));
        createBaseTable(SCHEMA_NAME, TABLE_NAME, true, TABLE_CACHE_FREQ);
        createIndex(getConnection(), SCHEMA_NAME, GLOBAL_INDEX, TABLE_NAME,
            VIEW_INDEX_COL, false);
        createIndex(getConnection(), SCHEMA_NAME, LOCAL_INDEX, TABLE_NAME,
            VIEW_INDEX_COL, true);
    }

    @Test
    public void testSyncCacheFreqWithTenantView() throws Exception {
        for (int i = 1; i <= 3; i++) {
            // verify tenant view resolution with cache freq sync with different
            // tenants
            tenant_name = "TENANT_" + i;
            try (Connection conn = getTenantConnection(tenant_name)) {
                createView(conn, SCHEMA_NAME, VIEW1_NAME, TABLE_NAME);
                createIndex(conn, SCHEMA_NAME, VIEW1_INDEX1_NAME, VIEW1_NAME, VIEW_INDEX_COL,
                    false);
                createIndex(conn, SCHEMA_NAME, VIEW1_INDEX2_NAME, VIEW1_NAME, VIEW_INDEX_COL,
                    false);
                createView(conn, SCHEMA_NAME, VIEW2_NAME, VIEW1_NAME);
                createIndex(conn, SCHEMA_NAME, VIEW2_INDEX1_NAME, VIEW2_NAME, VIEW_INDEX_COL,
                    false);
                createIndex(conn, SCHEMA_NAME, VIEW2_INDEX2_NAME, VIEW2_NAME, VIEW_INDEX_COL,
                    false);
            }

            try (PhoenixConnection conn = (PhoenixConnection) getConnection()) {
                PreparedStatement stmt =
                    conn.prepareStatement(UPSERT_UPDATE_CACHE_FREQUENCY);

                Map<String, Long> updatedIndexFreqMap = new HashMap<>();
                // use random numbers to update frequencies of all indexes
                for (String index : INDEXS_TO_UPDATE_CACHE_FREQ) {
                    long updatedCacheFreq = RANDOM_INT.nextInt(4000);
                    updatedIndexFreqMap.put(index, updatedCacheFreq);
                    updateCacheFreq(index, updatedCacheFreq, stmt);
                }
                stmt.executeBatch();
                conn.commit();

                // clear the server-side cache to get the latest built PTables
                conn.unwrap(PhoenixConnection.class).getQueryServices().clearCache();

                // assert that new updated cache frequencies are present
                // hence, index frequencies are different from parent table/view cache frequencies
                for (String table : updatedIndexFreqMap.keySet()) {
                    assertTableFrequencies(conn, table,
                        updatedIndexFreqMap.get(table));
                }

                // clear the server-side cache to get the latest built PTables
                conn.unwrap(PhoenixConnection.class).getQueryServices().clearCache();
                PhoenixConnection pcon = conn.unwrap(PhoenixConnection.class);
                pcon.setRunningUpgrade(true);

                UpgradeUtil.syncUpdateCacheFreqAllIndexes(pcon,
                    conn.getTableNoCache(SchemaUtil.getTableName(SCHEMA_NAME, TABLE_NAME)));

                // assert that index frequencies are in sync with table/view cache frequencies
                for (String tableOrView : TABLE_TO_INDEX.keySet()) {
                    final long expectedFreqForTableAndIndex;
                    if (tableOrView.equals(TABLE_NAME)) {
                        expectedFreqForTableAndIndex = TABLE_CACHE_FREQ;
                    } else {
                        expectedFreqForTableAndIndex = VIEW_CACHE_FREQ;
                    }
                    assertTableFrequencies(conn, tableOrView,
                        expectedFreqForTableAndIndex);
                    for (String index : TABLE_TO_INDEX.get(tableOrView)) {
                        assertTableFrequencies(conn, index, expectedFreqForTableAndIndex);
                    }
                }
            }
        }
    }

    private void updateCacheFreq(String tableName,
            long freq, PreparedStatement stmt) throws SQLException {
        stmt.setString(1, tenant_name);
        stmt.setString(2, SCHEMA_NAME);
        stmt.setString(3, tableName);
        stmt.setLong(4, freq);
        stmt.addBatch();
    }

    private void assertTableFrequencies(Connection conn,
            String tableName, long expectedCacheFreq) throws SQLException {
        conn.unwrap(PhoenixConnection.class).getQueryServices().clearCache();
        ResultSet rs;
        if (GLOBAL_TABLES.contains(tableName)) {
            rs = conn.createStatement().executeQuery(String.format(
              "SELECT UPDATE_CACHE_FREQUENCY FROM SYSTEM.CATALOG WHERE "
                + "TABLE_NAME='%s'", tableName));
        } else {
            rs = conn.createStatement().executeQuery(String.format(
              "SELECT UPDATE_CACHE_FREQUENCY FROM SYSTEM.CATALOG WHERE "
                + "TABLE_NAME='%s' AND TENANT_ID='%s'",
              tableName, tenant_name));
        }
        rs.next();
        long cacheFreq = rs.getLong(1);
        assertEquals("Cache Freq for " + tableName + " not matching. actual: "
            + cacheFreq + " , expected: " + expectedCacheFreq,
            expectedCacheFreq, cacheFreq);
    }

    private static void createBaseTable(String schemaName, String tableName,
            boolean multiTenant, int cacheFre) throws SQLException {
        Connection conn = getConnection();
        String ddl =
          "CREATE TABLE " + SchemaUtil.getTableName(schemaName, tableName)
            + " (t_id VARCHAR NOT NULL,\n" + "k1 VARCHAR NOT NULL,\n"
            + "k2 INTEGER,\n" + "v1 VARCHAR,\n" + VIEW_INDEX_COL
            + " INTEGER,\n" + "CONSTRAINT pk PRIMARY KEY (t_id, k1))\n";
        String ddlOptions = multiTenant ? "MULTI_TENANT=true" : "";
        ddlOptions = ddlOptions + (ddlOptions.isEmpty() ? "" : ",")
            + "UPDATE_CACHE_FREQUENCY=" + cacheFre;
        conn.createStatement().execute(ddl + ddlOptions);
        conn.close();
    }

    private static void createIndex(Connection conn, String schemaName,
            String indexName, String tableName, String indexColumn, boolean isLocal)
            throws SQLException {
        String fullTableName = SchemaUtil.getTableName(schemaName, tableName);
        conn.createStatement().execute(String
            .format(isLocal ? CREATE_LOCAL_INDEX : CREATE_GLOBAL_INDEX,
                indexName, fullTableName, indexColumn));
        conn.commit();
    }

    private static void createView(Connection conn, String schemaName,
            String viewName, String baseTableName)
            throws SQLException {
        String fullViewName = SchemaUtil.getTableName(schemaName, viewName);
        String fullTableName = SchemaUtil.getTableName(schemaName, baseTableName);
        conn.createStatement().execute(String.format(
            "CREATE VIEW %s AS SELECT * FROM %s UPDATE_CACHE_FREQUENCY=%s",
                fullViewName, fullTableName, VIEW_CACHE_FREQ));
        conn.commit();
    }

    private static Connection getTenantConnection(String tenant)
            throws SQLException {
        Properties props = new Properties();
        props.setProperty(PhoenixRuntime.TENANT_ID_ATTRIB, tenant);
        return DriverManager.getConnection(getUrl(), props);
    }

    private static Connection getConnection() throws SQLException {
        Properties props = new Properties();
        return DriverManager.getConnection(getUrl(), props);
    }

}
