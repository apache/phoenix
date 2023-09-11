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
package org.apache.phoenix.jdbc;

import org.apache.phoenix.end2end.NeedsOwnMiniClusterTest;
import org.apache.phoenix.query.BaseTest;
import org.apache.phoenix.query.ConnectionQueryServices;
import org.apache.phoenix.schema.PMetaData;
import org.apache.phoenix.schema.PTableKey;
import org.apache.phoenix.schema.PTableRef;
import org.apache.phoenix.schema.TableNotFoundException;
import org.apache.phoenix.thirdparty.com.google.common.collect.Maps;
import org.apache.phoenix.util.PropertiesUtil;
import org.apache.phoenix.util.QueryUtil;
import org.apache.phoenix.util.ReadOnlyProps;
import org.apache.phoenix.util.TestUtil;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.Map;
import java.util.Properties;

@Category(NeedsOwnMiniClusterTest.class)
public class PhoenixTestDriverIT extends BaseTest {

    @BeforeClass
    public static synchronized void doSetup() throws Exception {
        Map<String, String> props = Maps.newHashMapWithExpectedSize(1);
        setUpTestDriver(new ReadOnlyProps(props.entrySet().iterator()));
    }

    /**
     * Test that connections created using the same url have the same CQSI object.
     */
    @Test
    public void testSameCQSI() throws SQLException {
        Properties props = PropertiesUtil.deepCopy(TestUtil.TEST_PROPERTIES);
        String url = QueryUtil.getConnectionUrl(props, config, "client1");
        try (Connection conn1 = DriverManager.getConnection(url);
             Connection conn2 = DriverManager.getConnection(url)) {
            ConnectionQueryServices cqs1 = conn1.unwrap(PhoenixConnection.class).getQueryServices();
            ConnectionQueryServices cqs2 = conn2.unwrap(PhoenixConnection.class).getQueryServices();
            Assert.assertNotNull(cqs1);
            Assert.assertNotNull(cqs2);
            Assert.assertEquals("Connections using the same URL should have the same CQSI object.", cqs1, cqs2);
        }
    }

    /**
     * Test that connections created using different urls have different CQSI objects.
     */
    @Test
    public void testDifferentCQSI() throws SQLException {
        Properties props = PropertiesUtil.deepCopy(TestUtil.TEST_PROPERTIES);
        String url1 = QueryUtil.getConnectionUrl(props, config, "client1");
        String url2 = QueryUtil.getConnectionUrl(props, config, "client2");
        try (Connection conn1 = DriverManager.getConnection(url1);
             Connection conn2 = DriverManager.getConnection(url2)) {
            ConnectionQueryServices cqs1 = conn1.unwrap(PhoenixConnection.class).getQueryServices();
            ConnectionQueryServices cqs2 = conn2.unwrap(PhoenixConnection.class).getQueryServices();
            Assert.assertNotNull(cqs1);
            Assert.assertNotNull(cqs2);
            Assert.assertNotEquals("Connections using different URL should have different CQSI objects.", cqs1, cqs2);
        }
    }

    /**
     * Create 2 connections using URLs with different principals.
     * Create a table using one connection and verify that the other connection's cache
     * does not have this table's metadata.
     */
    @Test
    public void testDifferentCQSICache() throws SQLException {
        Properties props = PropertiesUtil.deepCopy(TestUtil.TEST_PROPERTIES);
        String url1 = QueryUtil.getConnectionUrl(props, config, "client1");
        String url2 = QueryUtil.getConnectionUrl(props, config, "client2");

        // create a table with url1
        String tableName = generateUniqueName();
        try (Connection conn1 = DriverManager.getConnection(url1)) {
            conn1.createStatement().execute("CREATE TABLE " + tableName
                    + "(k INTEGER NOT NULL PRIMARY KEY, v1 INTEGER)");

            // this connection's cqsi cache should have the table metadata
            PMetaData cache = conn1.unwrap(PhoenixConnection.class).getQueryServices().getMetaDataCache();
            PTableRef pTableRef = cache.getTableRef(new PTableKey(null, tableName));
            Assert.assertNotNull(pTableRef);
        }
        catch (TableNotFoundException e) {
            Assert.fail("Table should have been found in CQSI cache.");
        }

        // table metadata should not be present in the other cqsi cache
        Connection conn2 = DriverManager.getConnection(url2);
        PMetaData cache = conn2.unwrap(PhoenixConnection.class).getQueryServices().getMetaDataCache();
        try {
            cache.getTableRef(new PTableKey(null, tableName));
            Assert.fail("Table should not have been found in CQSI cache.");
        }
        catch (TableNotFoundException e) {
            // expected since this connection was created using a different CQSI.
        }
    }
}
