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
package org.apache.phoenix.monitoring;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.phoenix.end2end.NeedsOwnMiniClusterTest;
import org.apache.phoenix.jdbc.PhoenixDriver;
import org.apache.phoenix.query.BaseTest;
import org.apache.phoenix.query.ConfigurationFactory;
import org.apache.phoenix.query.QueryServices;
import org.apache.phoenix.util.InstanceResolver;
import org.apache.phoenix.util.PhoenixRuntime;
import org.apache.phoenix.util.ReadOnlyProps;
import org.apache.phoenix.thirdparty.com.google.common.collect.ImmutableMap;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import static org.apache.phoenix.monitoring.PhoenixTableLevelMetricsIT.assertSelectQueryTableMetrics;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.Statement;

@Category(NeedsOwnMiniClusterTest.class)
public class PhoenixTableLevelAllowedTablesIT extends BaseTest {

    private static final String ALLOWED_TABLE_1 = generateUniqueName();
    private static final String ALLOWED_TABLE_2 = generateUniqueName();

    @BeforeClass
    public static void doSetup() throws Exception {
        final Configuration conf = HBaseConfiguration.create();
        conf.set(QueryServices.TABLE_LEVEL_METRICS_ENABLED, String.valueOf(true));
        conf.set(QueryServices.COLLECT_REQUEST_LEVEL_METRICS, String.valueOf(true));
        conf.set(QueryServices.ALLOWED_LIST_FOR_TABLE_LEVEL_METRICS,
                ALLOWED_TABLE_1 + "," + ALLOWED_TABLE_2);
        InstanceResolver.clearSingletons();
        // Make sure the ConnectionInfo doesn't try to pull a default Configuration
        InstanceResolver.getSingleton(ConfigurationFactory.class, new ConfigurationFactory() {
            @Override public Configuration getConfiguration() {
                return conf;
            }

            @Override public Configuration getConfiguration(Configuration confToClone) {
                Configuration copy = new Configuration(conf);
                copy.addResource(confToClone);
                return copy;
            }
        });
        setUpTestDriver(new ReadOnlyProps(ImmutableMap.<String, String>of()));
        DriverManager.registerDriver(PhoenixDriver.INSTANCE);
    }

    /**
     * Generate some metrics randomly for a table and verify that
     * metrics exist in system for only whitelisting of tables, metrics
     * doesn't exist for tables which are not whitelisted.
     */
    @Test
    public void testAllowedListingFunctionality() throws Exception {
        String tableName = generateUniqueName();
        String ddl =
                "CREATE TABLE " + tableName + " (K VARCHAR NOT NULL PRIMARY KEY, V VARCHAR)";
        ResultSet rs;
        try (Connection ddlConn = DriverManager.getConnection(getUrl());
             Statement stmt = ddlConn.createStatement()) {
            stmt.execute(ddl);

            String dml = "UPSERT INTO " + tableName + " VALUES (?, ?)";
            try(PreparedStatement preparedStatement = ddlConn.prepareStatement(dml)){
                for (int i = 1; i <= 10; i++) {
                    preparedStatement.setString(1, "key" + i);
                    preparedStatement.setString(2, "value" + i);
                    preparedStatement.executeUpdate();
                }
                ddlConn.commit();
            }
        }
        assertTrue(PhoenixRuntime.getPhoenixTableClientMetrics().isEmpty());

        try (Connection conn = DriverManager.getConnection(getUrl())) {
            //creating a allowed table and making sure metrics gets stored in store.
            PhoenixMetricsIT.createTableAndInsertValues(ALLOWED_TABLE_1, false, false, 10, true, conn, false);
        }
        try (Connection conn = DriverManager.getConnection(getUrl());
             Statement stmt = conn.createStatement()) {
            String query = "SELECT * FROM " + ALLOWED_TABLE_1;
            rs = stmt.executeQuery(query);
            while (rs.next()) {
            }
            assertFalse(PhoenixRuntime.getPhoenixTableClientMetrics().isEmpty());
        }
        assertSelectQueryTableMetrics(ALLOWED_TABLE_1, false, 1, 0, 1, 0, 0, true, 0, 0, rs);
    }

}