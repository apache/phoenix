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

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.ClusterConnection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.RegionLocator;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.phoenix.jdbc.PhoenixConnection;
import org.apache.phoenix.util.ByteUtil;
import org.apache.phoenix.util.PropertiesUtil;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runners.Parameterized.Parameters;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.Collection;
import java.util.Properties;

import static org.apache.phoenix.util.TestUtil.A_VALUE;
import static org.apache.phoenix.util.TestUtil.B_VALUE;
import static org.apache.phoenix.util.TestUtil.C_VALUE;
import static org.apache.phoenix.util.TestUtil.E_VALUE;
import static org.apache.phoenix.util.TestUtil.ROW4;
import static org.apache.phoenix.util.TestUtil.TEST_PROPERTIES;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

@Category(NeedsOwnMiniClusterTest.class)
public class AggregateQueryWithRegionMovesIT extends BaseQueryWithRegionMovesIT {

    @Parameters(name = "AggregateQueryWithRegionMovesIT_{index}")
    public static synchronized Collection<Object> data() {
        return BaseQueryWithRegionMovesIT.allIndexes();
    }

    public AggregateQueryWithRegionMovesIT(String indexDDL, boolean columnEncoded,
                                           boolean keepDeletedCells) {
        super(indexDDL, columnEncoded, keepDeletedCells);
    }

    @Before
    public void setUp() throws Exception {
        hasTestStarted = true;
    }

    @After
    public void tearDown() throws Exception {
        countOfDummyResults = 0;
        TABLE_NAMES.clear();
        hasTestStarted = false;
    }

    @Test
    public void testGroupByPlusOne() throws Exception {
        String query = "SELECT a_integer+1 FROM " + tableName +
                " WHERE organization_id=? and a_integer = 5 GROUP BY a_integer+1";
        TABLE_NAMES.add(tableName);
        TABLE_NAMES.add(indexName);
        Properties props = PropertiesUtil.deepCopy(TEST_PROPERTIES);
        try (Connection conn = DriverManager.getConnection(getUrl(), props)) {
            PreparedStatement statement = conn.prepareStatement(query);
            statement.setString(1, tenantId);
            ResultSet rs = statement.executeQuery();
            assertTrue(rs.next());
            moveRegionsOfTable(tableName);
            moveRegionsOfTable(indexName);
            assertEquals(6, rs.getInt(1));
            assertFalse(rs.next());
        }
    }

    @Test
    public void testSplitWithCachedMeta() throws Exception {
        TABLE_NAMES.add(tableName);
        TABLE_NAMES.add(indexName);
        // Tests that you don't get an ambiguous column exception when using the same alias as the column name
        String query = "SELECT a_string, b_string, count(1) FROM " + tableName +
                " WHERE organization_id=? and entity_id<=? GROUP BY a_string,b_string";
        Properties props = PropertiesUtil.deepCopy(TEST_PROPERTIES);
        Connection conn = DriverManager.getConnection(getUrl(), props);
        Admin admin = null;
        try {
            PreparedStatement statement = conn.prepareStatement(query);
            statement.setString(1, tenantId);
            statement.setString(2, ROW4);
            ResultSet rs = statement.executeQuery();
            assertTrue(rs.next());
            moveRegionsOfTable(tableName);
            moveRegionsOfTable(indexName);
            assertEquals(A_VALUE, rs.getString(1));
            assertEquals(B_VALUE, rs.getString(2));
            assertEquals(2, rs.getLong(3));
            assertTrue(rs.next());
            assertEquals(A_VALUE, rs.getString(1));
            assertEquals(C_VALUE, rs.getString(2));
            assertEquals(1, rs.getLong(3));
            moveRegionsOfTable(tableName);
            moveRegionsOfTable(indexName);
            assertTrue(rs.next());
            assertEquals(A_VALUE, rs.getString(1));
            assertEquals(E_VALUE, rs.getString(2));
            assertEquals(1, rs.getLong(3));
            assertFalse(rs.next());

            TableName tn = TableName.valueOf(tableName);
            admin = conn.unwrap(PhoenixConnection.class).getQueryServices().getAdmin();
            Configuration configuration =
                    conn.unwrap(PhoenixConnection.class).getQueryServices().getConfiguration();
            org.apache.hadoop.hbase.client.Connection hbaseConn =
                    ConnectionFactory.createConnection(configuration);
            ((ClusterConnection) hbaseConn).clearRegionCache(TableName.valueOf(tableName));
            RegionLocator regionLocator = hbaseConn.getRegionLocator(TableName.valueOf(tableName));
            int nRegions = regionLocator.getAllRegionLocations().size();
            admin.split(tn, ByteUtil.concat(Bytes.toBytes(tenantId), Bytes.toBytes("00A3")));
            int retryCount = 0;
            do {
                Thread.sleep(2000);
                retryCount++;
                //htable.clearRegionCache();
            } while (retryCount < 10 && regionLocator.getAllRegionLocations().size() == nRegions);
            assertNotEquals(nRegions, regionLocator.getAllRegionLocations().size());

            statement.setString(1, tenantId);
            rs = statement.executeQuery();
            assertTrue(rs.next());
            moveRegionsOfTable(tableName);
            moveRegionsOfTable(indexName);
            assertEquals(A_VALUE, rs.getString(1));
            assertEquals(B_VALUE, rs.getString(2));
            assertEquals(2, rs.getLong(3));
            assertTrue(rs.next());
            moveRegionsOfTable(tableName);
            moveRegionsOfTable(indexName);
            assertEquals(A_VALUE, rs.getString(1));
            assertEquals(C_VALUE, rs.getString(2));
            assertEquals(1, rs.getLong(3));
            assertTrue(rs.next());
            assertEquals(A_VALUE, rs.getString(1));
            assertEquals(E_VALUE, rs.getString(2));
            assertEquals(1, rs.getLong(3));
            assertFalse(rs.next());
        } finally {
            if (admin != null) {
                admin.close();
            }
            conn.close();
        }
    }

    @Test
    public void testCountIsNull() throws Exception {
        TABLE_NAMES.add(tableName);
        TABLE_NAMES.add(indexName);
        String query = "SELECT count(1) FROM " + tableName + " WHERE X_DECIMAL is null";
        Properties props = PropertiesUtil.deepCopy(TEST_PROPERTIES);
        try (Connection conn = DriverManager.getConnection(getUrl(), props)) {
            PreparedStatement statement = conn.prepareStatement(query);
            ResultSet rs = statement.executeQuery();
            assertTrue(rs.next());
            moveRegionsOfTable(tableName);
            moveRegionsOfTable(indexName);
            assertEquals(6, rs.getLong(1));
            assertFalse(rs.next());
        }
    }

    @Test
    public void testCountWithNoScanRanges() throws Exception {
        TABLE_NAMES.add(tableName);
        TABLE_NAMES.add(indexName);
        String query = "SELECT count(1) FROM " + tableName +
                " WHERE organization_id = 'not_existing_organization_id'";
        Properties props = PropertiesUtil.deepCopy(TEST_PROPERTIES);
        try (Connection conn = DriverManager.getConnection(getUrl(), props)) {
            PreparedStatement statement = conn.prepareStatement(query);
            ResultSet rs = statement.executeQuery();
            assertTrue(rs.next());
            moveRegionsOfTable(tableName);
            moveRegionsOfTable(indexName);
            assertEquals(0, rs.getLong(1));
            assertFalse(rs.next());
            query = "SELECT count(1) FROM " + tableName +
                    " WHERE organization_id = 'not_existing_organization_id' having count(*)>0";
            rs = conn.prepareStatement(query).executeQuery();
            assertFalse(rs.next());
            query = "SELECT count(1) FROM " + tableName +
                    " WHERE organization_id = 'not_existing_organization_id' limit 1 offset 1";
            rs = conn.prepareStatement(query).executeQuery();
            assertFalse(rs.next());
            query = "SELECT count(1),123 FROM " + tableName +
                    " WHERE organization_id = 'not_existing_organization_id'";
            rs = conn.prepareStatement(query).executeQuery();
            assertTrue(rs.next());
            moveRegionsOfTable(tableName);
            moveRegionsOfTable(indexName);
            assertEquals(0, rs.getLong(1));
            assertEquals("123", rs.getString(2));
            assertFalse(rs.next());
            query = "SELECT count(1),sum(x_decimal) FROM " + tableName +
                    " WHERE organization_id = 'not_existing_organization_id'";
            rs = conn.prepareStatement(query).executeQuery();
            assertTrue(rs.next());
            moveRegionsOfTable(tableName);
            moveRegionsOfTable(indexName);
            assertEquals(0, rs.getLong(1));
            assertNull(rs.getBigDecimal(2));
            assertFalse(rs.next());
        }
    }

    @Test
    public void testCountIsNotNull() throws Exception {
        TABLE_NAMES.add(tableName);
        TABLE_NAMES.add(indexName);
        String query = "SELECT count(1) FROM " + tableName + " WHERE X_DECIMAL is not null";
        Properties props = PropertiesUtil.deepCopy(TEST_PROPERTIES);
        try (Connection conn = DriverManager.getConnection(getUrl(), props)) {
            PreparedStatement statement = conn.prepareStatement(query);
            ResultSet rs = statement.executeQuery();
            assertTrue(rs.next());
            moveRegionsOfTable(tableName);
            moveRegionsOfTable(indexName);
            assertEquals(3, rs.getLong(1));
            assertFalse(rs.next());
        }
    }

    /**
     * Test to repro Null Pointer Exception
     */
    @Test
    public void testInFilterOnKey() throws Exception {
        TABLE_NAMES.add(tableName);
        TABLE_NAMES.add(indexName);
        String query =
                "SELECT count(entity_id) FROM " + tableName + " WHERE organization_id IN (?,?)";
        String url = getUrl();
        Properties props = PropertiesUtil.deepCopy(TEST_PROPERTIES);
        try (Connection conn = DriverManager.getConnection(url, props)) {
            PreparedStatement statement = conn.prepareStatement(query);
            statement.setString(1, tenantId);
            statement.setString(2, tenantId);
            ResultSet rs = statement.executeQuery();
            assertTrue(rs.next());
            moveRegionsOfTable(tableName);
            moveRegionsOfTable(indexName);
            assertEquals(9, rs.getInt(1));
            assertFalse(rs.next());
        }
    }

}
