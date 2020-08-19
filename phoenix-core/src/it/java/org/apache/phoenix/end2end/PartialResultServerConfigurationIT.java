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
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.MiniHBaseCluster;
import org.apache.phoenix.end2end.NeedsOwnMiniClusterTest;
import org.apache.phoenix.jdbc.PhoenixConnection;
import org.apache.phoenix.jdbc.PhoenixDriver;
import org.apache.phoenix.query.ConnectionQueryServices.Feature;
import org.apache.phoenix.query.QueryServices;
import org.apache.phoenix.util.PhoenixRuntime;
import org.apache.phoenix.util.PropertiesUtil;
import org.junit.AfterClass;
import org.junit.Assume;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.Properties;
import java.util.Random;
import java.util.UUID;

import static org.apache.hadoop.hbase.HConstants.HBASE_CLIENT_SCANNER_TIMEOUT_PERIOD;
import static org.apache.phoenix.query.BaseTest.generateUniqueName;
import static org.apache.phoenix.query.BaseTest.setUpConfigForMiniCluster;
import static org.apache.phoenix.query.QueryServices.*;
import static org.apache.phoenix.util.TestUtil.TEST_PROPERTIES;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;


/**
 * This is a separate from @PartialResultDisabledIT because it requires server side configuration
 */
@Category(NeedsOwnMiniClusterTest.class)
public class PartialResultServerConfigurationIT {
    private static HBaseTestingUtility hbaseTestUtil;
    private static String zkQuorum;
    private static String url;

    @BeforeClass
    public static synchronized void setUp() throws Exception {
        Configuration conf = HBaseConfiguration.create();
        hbaseTestUtil = new HBaseTestingUtility(conf);
        setUpConfigForMiniCluster(conf);

        //Enforce the limit of the result, so scans will stop between cells.
        conf.setLong(HConstants.HBASE_CLIENT_SCANNER_MAX_RESULT_SIZE_KEY, 5);
        conf.setLong(HConstants.HBASE_SERVER_SCANNER_MAX_RESULT_SIZE_KEY, 5);

        hbaseTestUtil.startMiniCluster();
        zkQuorum = "localhost:" + hbaseTestUtil.getZkCluster().getClientPort();
        url = PhoenixRuntime.JDBC_PROTOCOL + PhoenixRuntime.JDBC_PROTOCOL_SEPARATOR + zkQuorum;

        DriverManager.registerDriver(PhoenixDriver.INSTANCE);
    }

    @AfterClass
    public static synchronized void tearDownAfterClass() throws Exception {
        try {
            DriverManager.deregisterDriver(PhoenixDriver.INSTANCE);
        } finally {
            hbaseTestUtil.shutdownMiniCluster();
        }
    }

    /**
     * This test creates two tables with a single record at the end of each tables that match the join condition
     * if scanner context is used during the scan, it would produce a partial row with NULL values.
     * @throws Exception
     */
    @Test
    public void testJoinScan() throws Exception {
        String tableNameR = generateUniqueName();
        String tableNameL = generateUniqueName();

        int numRecords = 1000;
        try (Connection conn = DriverManager.getConnection(url)) {
            conn.createStatement().execute(
                    "CREATE TABLE " + tableNameR + " (PK1 INTEGER NOT NULL PRIMARY KEY, KV1 VARCHAR)");
            int i = 0;
            String upsert = "UPSERT INTO " + tableNameR + " VALUES (?, ?)";
            Random random = new Random();
            PreparedStatement stmt = conn.prepareStatement(upsert);
            while (i < numRecords) {
                stmt.setInt(1, i);
                stmt.setString(2, UUID.randomUUID().toString());
                stmt.executeUpdate();
                i++;
            }
            stmt.setInt(1, 9999);
            stmt.setString(2, UUID.randomUUID().toString());
            stmt.executeUpdate();
            conn.commit();
            conn.createStatement().execute(
                    "CREATE TABLE " + tableNameL + " (PK1 INTEGER NOT NULL PRIMARY KEY, KV1 VARCHAR)");
            upsert = "UPSERT INTO " + tableNameL + " VALUES (?, ?)";
            stmt = conn.prepareStatement(upsert);
            while (i < numRecords * 2) {
                stmt.setInt(1, random.nextInt());
                stmt.setString(2, "KV" + random.nextInt());
                stmt.executeUpdate();
                i++;
            }
            stmt.setInt(1, 9999);
            stmt.setString(2, UUID.randomUUID().toString());
            stmt.executeUpdate();
            conn.commit();

            String sql = "SELECT * FROM " + tableNameR + " A JOIN " + tableNameL + " B ON A.PK1 = B.PK1";
            Statement s = conn.createStatement();
            s.setFetchSize(2);
            ResultSet rs = s.executeQuery(sql);
            int count = 0;
            while (rs.next()) {
                if (rs.getString(2) == null || rs.getString(4) == null)
                    fail("Null value because of partial result from scan");
                count++;
            }
            assertEquals(count, 1);
        }
    }

}