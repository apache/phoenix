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

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Properties;

import org.apache.hadoop.hbase.DoNotRetryIOException;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.RegionLocator;
import org.apache.phoenix.query.BaseTest;
import org.apache.phoenix.util.PhoenixRuntime;
import org.junit.After;
import org.junit.Test;
import org.junit.experimental.categories.Category;

@Category(NeedsOwnMiniClusterTest.class)
public class SystemCatalogIT extends BaseTest {
    private HBaseTestingUtility testUtil = null;

    @After
    public void cleanup() throws Exception {
        if (null != testUtil) {
          testUtil.shutdownMiniCluster();
          testUtil = null;
        }
    }

    /**
     * Make sure that SYSTEM.CATALOG cannot be split, even with schemas and multi-tenant views
     */
    @Test
    public void testSystemTableSplit() throws Exception {
        testUtil = new HBaseTestingUtility();
        testUtil.startMiniCluster(1);
        for (int i=0; i<10; i++) {
            createTable("schema"+i+".table_"+i);
        }
        TableName systemCatalog = TableName.valueOf("SYSTEM.CATALOG");
        RegionLocator rl = testUtil.getConnection().getRegionLocator(systemCatalog);
        assertEquals(rl.getAllRegionLocations().size(), 1);

        try{
        // now attempt to split SYSTEM.CATALOG
        testUtil.getAdmin().split(systemCatalog);

        // make sure the split finishes (there's no synchronous splitting before HBase 2.x)
        testUtil.getAdmin().disableTable(systemCatalog);
        testUtil.getAdmin().enableTable(systemCatalog);
        }catch(DoNotRetryIOException e){
            //table is not splittable
            assert(e.getMessage().contains("NOT splittable"));
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
                + " (TENANT_ID VARCHAR NOT NULL, PK1 VARCHAR NOT NULL, V1 VARCHAR CONSTRAINT PK PRIMARY KEY(TENANT_ID, PK1)) MULTI_TENANT=true");
            try (Connection tenant1Conn = getTenantConnection("tenant1")) {
                String view1DDL = "CREATE VIEW " + tableName + "_view AS SELECT * FROM " + tableName;
                tenant1Conn.createStatement().execute(view1DDL);
            }
            conn.commit();
        }
    }

    private String getJdbcUrl() {
        return "jdbc:phoenix:localhost:" + testUtil.getZkCluster().getClientPort() + ":/hbase";
    }

    private Connection getTenantConnection(String tenantId) throws SQLException {
        Properties tenantProps = new Properties();
        tenantProps.setProperty(PhoenixRuntime.TENANT_ID_ATTRIB, tenantId);
        return DriverManager.getConnection(getJdbcUrl(), tenantProps);
    }
}