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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Collections;
import java.util.Map;
import java.util.Properties;

import org.apache.hadoop.hbase.DoNotRetryIOException;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.RegionLocator;
import org.apache.phoenix.exception.SQLExceptionCode;
import org.apache.phoenix.query.BaseTest;
import org.apache.phoenix.query.QueryServices;
import org.apache.phoenix.util.PhoenixRuntime;
import org.apache.phoenix.util.ReadOnlyProps;
import org.apache.phoenix.util.SchemaUtil;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import com.google.common.collect.Maps;

@Category(NeedsOwnMiniClusterTest.class)
public class SystemCatalogIT extends BaseTest {
    private HBaseTestingUtility testUtil = null;

	@BeforeClass
	public static void doSetup() throws Exception {
		Map<String, String> serverProps = Maps.newHashMapWithExpectedSize(1);
		serverProps.put(QueryServices.SYSTEM_CATALOG_SPLITTABLE, "false");
        serverProps.put(QueryServices.ALLOW_SPLITTABLE_SYSTEM_CATALOG_ROLLBACK, "true");
		Map<String, String> clientProps = Collections.emptyMap();
		setUpTestDriver(new ReadOnlyProps(serverProps.entrySet().iterator()),
				new ReadOnlyProps(clientProps.entrySet().iterator()));
	}

    /**
     * Make sure that SYSTEM.CATALOG cannot be split if QueryServices.SYSTEM_CATALOG_SPLITTABLE is false
     */
    @Test
    public void testSystemTableSplit() throws Exception {
        testUtil = getUtility();
        for (int i=0; i<10; i++) {
            createTable("schema"+i+".table_"+i);
        }
        TableName systemCatalog = TableName.valueOf("SYSTEM.CATALOG");
        RegionLocator rl = testUtil.getConnection().getRegionLocator(systemCatalog);
        assertEquals(rl.getAllRegionLocations().size(), 1);
        try {
            // now attempt to split SYSTEM.CATALOG
            testUtil.getAdmin().split(systemCatalog);
            // make sure the split finishes (there's no synchronous splitting before HBase 2.x)
            testUtil.getAdmin().disableTable(systemCatalog);
            testUtil.getAdmin().enableTable(systemCatalog);
        } catch (DoNotRetryIOException e) {
            // table is not splittable
            assert (e.getMessage().contains("NOT splittable"));
        }

        // test again... Must still be exactly one region.
        rl = testUtil.getConnection().getRegionLocator(systemCatalog);
        assertEquals(1, rl.getAllRegionLocations().size());
    }

    private void createTable(String tableName) throws Exception {
        try (Connection conn = DriverManager.getConnection(getJdbcUrl());
            Statement stmt = conn.createStatement();) {
            stmt.execute("DROP TABLE IF EXISTS " + tableName);
            stmt.execute("CREATE TABLE " + tableName
                    + " (TENANT_ID VARCHAR NOT NULL, PK1 VARCHAR NOT NULL, V1 VARCHAR CONSTRAINT PK " +
                    "PRIMARY KEY(TENANT_ID, PK1)) MULTI_TENANT=true");
            try (Connection tenant1Conn = getTenantConnection("tenant1")) {
                String view1DDL = "CREATE VIEW " + tableName + "_view AS SELECT * FROM " + tableName;
                tenant1Conn.createStatement().execute(view1DDL);
            }
            conn.commit();
        }
    }

    private String getJdbcUrl() {
        return "jdbc:phoenix:localhost:" + getUtility().getZkCluster().getClientPort() + ":/hbase";
    }

    private Connection getTenantConnection(String tenantId) throws SQLException {
        Properties tenantProps = new Properties();
        tenantProps.setProperty(PhoenixRuntime.TENANT_ID_ATTRIB, tenantId);
        return DriverManager.getConnection(getJdbcUrl(), tenantProps);
    }

    /**
     * Ensure that we cannot add a column to a base table if QueryServices.BLOCK_METADATA_CHANGES_REQUIRE_PROPAGATION
     * is true
     */
    @Test
    public void testAddingColumnFails() throws Exception {
        try (Connection conn = DriverManager.getConnection(getJdbcUrl())) {
            String fullTableName = SchemaUtil.getTableName(generateUniqueName(), generateUniqueName());
            String fullViewName = SchemaUtil.getTableName(generateUniqueName(), generateUniqueName());
            String ddl = "CREATE TABLE " + fullTableName + " (k1 INTEGER NOT NULL, v1 INTEGER " +
                    "CONSTRAINT pk PRIMARY KEY (k1))";
            conn.createStatement().execute(ddl);

            ddl = "CREATE VIEW " + fullViewName + " AS SELECT * FROM " + fullTableName;
            conn.createStatement().execute(ddl);

            try {
                ddl = "ALTER TABLE " + fullTableName + " ADD v2 INTEGER";
                conn.createStatement().execute(ddl);
                fail();
            }
            catch (SQLException e) {
                assertEquals(SQLExceptionCode.CANNOT_MUTATE_TABLE.getErrorCode(), e.getErrorCode());
            }
        }
    }
}
