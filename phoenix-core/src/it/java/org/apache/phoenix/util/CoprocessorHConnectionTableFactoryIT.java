
/*
 * Licensed to the Apache Software Foundation (ASF) under one or more contributor license
 * agreements. See the NOTICE file distributed with this work for additional information regarding
 * copyright ownership. The ASF licenses this file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License. You may obtain a
 * copy of the License at http://www.apache.org/licenses/LICENSE-2.0 Unless required by applicable
 * law or agreed to in writing, software distributed under the License is distributed on an "AS IS"
 * BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License
 * for the specific language governing permissions and limitations under the License.
 */

package org.apache.phoenix.util;

import static org.junit.Assert.assertTrue;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.MiniHBaseCluster;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.regionserver.HRegionServer;
import org.apache.phoenix.end2end.NeedsOwnMiniClusterTest;
import org.apache.phoenix.query.BaseTest;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/*
 * This test is wrt to https://issues.apache.org/jira/browse/PHOENIX-4993.Test checks region
 * close should not close the shared connections
 */
@Category(NeedsOwnMiniClusterTest.class)
public class CoprocessorHConnectionTableFactoryIT extends BaseTest {
  private static String ORG_PREFIX = "ORG";
  private static final Logger LOGGER =
          LoggerFactory.getLogger(CoprocessorHConnectionTableFactoryIT.class);

  @BeforeClass
  public static synchronized final void doSetup() throws Exception {

    setUpTestDriver(ReadOnlyProps.EMPTY_PROPS);

  }

  static String getOrgId(long id) {
    return ORG_PREFIX + "-" + id;
  }

  static String getRandomOrgId(int maxOrgId) {
    return getOrgId(Math.round(Math.random() * maxOrgId));
  }

  static void writeToTable(String tableName, Connection conn, int maxOrgId) throws SQLException {
    try {

      String orgId = getRandomOrgId(maxOrgId);
      Statement stmt = conn.createStatement();
      for (int i = 0; i < 10; i++) {
        stmt.executeUpdate("UPSERT INTO " + tableName + " VALUES('" + orgId + "'," + i + ","
            + (i + 1) + "," + (i + 2) + ")");

      }
      conn.commit();
    } catch (Exception e) {
      LOGGER.error("Client side exception:" + e);
    }

  }

  static int getActiveConnections(HRegionServer regionServer, Configuration conf) throws Exception {
    return ServerUtil.ConnectionFactory.getConnectionsCount();
  }

  @Test
  public void testCachedConnections() throws Exception {
    final String tableName = generateUniqueName();
    final String index1Name = generateUniqueName();
    final Connection conn = DriverManager.getConnection(getUrl());

    final HBaseAdmin admin = getUtility().getHBaseAdmin();
    final MiniHBaseCluster cluster = getUtility().getHBaseCluster();
    final HRegionServer regionServer = cluster.getRegionServer(0);
    Configuration conf = admin.getConfiguration();
    final int noOfOrgs = 20;
    final AtomicBoolean flag = new AtomicBoolean();
    flag.set(false);
    // create table and indices
    String createTableSql = "CREATE TABLE " + tableName
        + "(org_id VARCHAR NOT NULL PRIMARY KEY, v1 INTEGER, v2 INTEGER, v3 INTEGER) VERSIONS=1 SPLIT ON ('"
        + ORG_PREFIX + "-" + noOfOrgs / 2 + "')";
    conn.createStatement().execute(createTableSql);
    conn.createStatement().execute("CREATE INDEX " + index1Name + " ON " + tableName + "(v1)");
    List<HRegionInfo> regions = admin.getTableRegions(TableName.valueOf(tableName));
    final HRegionInfo regionInfo = regions.get(0);

    writeToTable(tableName, conn, noOfOrgs);
    int beforeRegionCloseCount = getActiveConnections(regionServer, conf);
    int regionsCount = admin.getOnlineRegions(regionServer.getServerName()).size();
    admin.unassign(regionInfo.getEncodedNameAsBytes(), true);
    while(!(admin.getOnlineRegions(regionServer.getServerName()).size() < regionsCount));
    int afterRegionCloseCount = getActiveConnections(regionServer, conf);
    assertTrue("Cached connections not closed when region closes: ",
    afterRegionCloseCount == beforeRegionCloseCount && afterRegionCloseCount > 0);

  }

}
