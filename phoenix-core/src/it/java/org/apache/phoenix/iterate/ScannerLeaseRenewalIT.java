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
package org.apache.phoenix.iterate;

import static org.apache.hadoop.hbase.HConstants.HBASE_CLIENT_SCANNER_TIMEOUT_PERIOD;
import static org.apache.phoenix.query.BaseTest.setUpConfigForMiniCluster;
import static org.apache.phoenix.query.QueryServices.FORCE_ROW_KEY_ORDER_ATTRIB;
import static org.apache.phoenix.query.QueryServices.RENEW_LEASE_ENABLED;
import static org.apache.phoenix.query.QueryServices.RENEW_LEASE_THREAD_POOL_SIZE;
import static org.apache.phoenix.query.QueryServices.RENEW_LEASE_THRESHOLD_MILLISECONDS;
import static org.apache.phoenix.query.QueryServices.RUN_RENEW_LEASE_FREQUENCY_INTERVAL_MILLISECONDS;
import static org.apache.phoenix.util.TestUtil.TEST_PROPERTIES;
import static org.junit.Assert.assertEquals;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.Properties;
import java.util.Random;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.HConstants;
import org.apache.phoenix.end2end.NeedsOwnMiniClusterTest;
import org.apache.phoenix.jdbc.PhoenixConnection;
import org.apache.phoenix.jdbc.PhoenixDriver;
import org.apache.phoenix.query.ConnectionQueryServices.Feature;
import org.apache.phoenix.util.PhoenixRuntime;
import org.apache.phoenix.util.PropertiesUtil;
import org.junit.AfterClass;
import org.junit.Assume;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;

@Category(NeedsOwnMiniClusterTest.class)
public class ScannerLeaseRenewalIT {
    private static HBaseTestingUtility hbaseTestUtil;
    private static String zkQuorum;
    // If tests are failing because of scanner expiration errors in mini cluster startup,
    // increase this timeout. It would end up increasing the duration the tests run too, though.
    private static final long LEASE_TIMEOUT_PERIOD_MILLIS =
            HConstants.DEFAULT_HBASE_CLIENT_SCANNER_TIMEOUT_PERIOD / 10;
    private static String url;
    
    @BeforeClass
    public static void setUp() throws Exception {
        Configuration conf = HBaseConfiguration.create();
        hbaseTestUtil = new HBaseTestingUtility(conf);
        setUpConfigForMiniCluster(conf);
        conf.setLong(HConstants.HBASE_CLIENT_SCANNER_TIMEOUT_PERIOD, LEASE_TIMEOUT_PERIOD_MILLIS);
        hbaseTestUtil.startMiniCluster();
        // establish url and quorum. Need to use PhoenixDriver and not PhoenixTestDriver
        zkQuorum = "localhost:" + hbaseTestUtil.getZkCluster().getClientPort();
        url = PhoenixRuntime.JDBC_PROTOCOL + PhoenixRuntime.JDBC_PROTOCOL_SEPARATOR + zkQuorum;

        Properties driverProps = PropertiesUtil.deepCopy(TEST_PROPERTIES);
        driverProps.put(RENEW_LEASE_THREAD_POOL_SIZE, Long.toString(4));
        
        // if this property is false, tests will fail with UnknownScannerException errors. 
        driverProps.put(RENEW_LEASE_ENABLED, Boolean.toString(true));
        
        driverProps.put(RENEW_LEASE_THRESHOLD_MILLISECONDS,
            Long.toString(LEASE_TIMEOUT_PERIOD_MILLIS / 2));
        driverProps.put(RUN_RENEW_LEASE_FREQUENCY_INTERVAL_MILLISECONDS,
            Long.toString(LEASE_TIMEOUT_PERIOD_MILLIS / 4));
        driverProps.put(HBASE_CLIENT_SCANNER_TIMEOUT_PERIOD,
            Long.toString(LEASE_TIMEOUT_PERIOD_MILLIS));
        // use round robin iterator
        driverProps.put(FORCE_ROW_KEY_ORDER_ATTRIB, Boolean.toString(false));
        DriverManager.registerDriver(PhoenixDriver.INSTANCE);
        try (PhoenixConnection phxConn = DriverManager.getConnection(url, driverProps).unwrap(PhoenixConnection.class)) {
            // run test methods only if we are at the hbase version that supports lease renewal.
            Assume.assumeTrue(phxConn.getQueryServices().supportsFeature(Feature.RENEW_LEASE));
        }
    }
    
    @AfterClass
    public static void tearDownAfterClass() throws Exception {
        try {
            DriverManager.deregisterDriver(PhoenixDriver.INSTANCE);
        } finally {
            hbaseTestUtil.shutdownMiniCluster();
        }
    }

    @Test
    public void testRenewLeasePreventsSelectQueryFromFailing() throws Exception {
        String tableName = "testRenewLeasePreventsSelectQueryFromFailing";
        int numRecords = 5;
        try (Connection conn = DriverManager.getConnection(url)) {
            conn.createStatement().execute(
                "CREATE TABLE " + tableName + " (PK1 INTEGER NOT NULL PRIMARY KEY, KV1 VARCHAR)");
            int i = 0;
            String upsert = "UPSERT INTO " + tableName + " VALUES (?, ?)";
            Random random = new Random();
            PreparedStatement stmt = conn.prepareStatement(upsert);
            while (i < numRecords) {
                stmt.setInt(1, random.nextInt());
                stmt.setString(2, "KV" + random.nextInt());
                stmt.executeUpdate();
                i++;
            }
            conn.commit();
        }

        try (PhoenixConnection phxConn =
                DriverManager.getConnection(url).unwrap(PhoenixConnection.class)) {
            String sql = "SELECT * FROM " + tableName;
            // at every next call wait for this period. This will cause lease to expire.
            long delayOnNext = 2 * LEASE_TIMEOUT_PERIOD_MILLIS;
            phxConn.setTableResultIteratorFactory(new DelayedTableResultIteratorFactory(delayOnNext));
            Statement s = phxConn.createStatement();
            s.setFetchSize(2);
            ResultSet rs = s.executeQuery(sql);
            int count = 0;
            while (rs.next()) {
                count++;
            }
            assertEquals(numRecords, count);
        }
    }

    @Test
    public void testRenewLeasePreventsUpsertSelectFromFailing() throws Exception {
        String table1 = "testRenewLeasePreventsUpsertSelectFromFailing";
        String table2 = "testRenewLeasePreventsUpsertSelectFromFailing2";

        try (Connection conn = DriverManager.getConnection(url)) {
            conn.createStatement().execute(
                "CREATE TABLE " + table1 + " (PK1 INTEGER NOT NULL PRIMARY KEY, KV1 VARCHAR)");
            conn.createStatement().execute(
                "CREATE TABLE " + table2 + " (PK1 INTEGER NOT NULL PRIMARY KEY, KV1 VARCHAR)");
            int numRecords = 5;
            int i = 0;
            String upsert = "UPSERT INTO " + table1 + " VALUES (?, ?)";
            Random random = new Random();
            PreparedStatement stmt = conn.prepareStatement(upsert);
            while (i < numRecords) {
                stmt.setInt(1, random.nextInt());
                stmt.setString(2, "KV" + random.nextInt());
                stmt.executeUpdate();
                i++;
            }
            conn.commit();
        }

        try (PhoenixConnection phxConn =
                DriverManager.getConnection(url).unwrap(PhoenixConnection.class)) {
            String upsertSelect = "UPSERT INTO " + table2 + " SELECT PK1, KV1 FROM " + table1;
            // at every next call wait for this period. This will cause lease to expire.
            long delayAfterInit = 2 * LEASE_TIMEOUT_PERIOD_MILLIS;
            phxConn.setTableResultIteratorFactory(new DelayedTableResultIteratorFactory(
                    delayAfterInit));
            Statement s = phxConn.createStatement();
            s.setFetchSize(2);
            s.executeUpdate(upsertSelect);
        }
    }
}