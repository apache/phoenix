/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.phoenix.end2end.index;

import static org.apache.phoenix.query.BaseTest.generateUniqueName;
import static org.apache.phoenix.query.QueryServices.CLUSTER_ROLE_BASED_MUTATION_BLOCK_ENABLED;
import static org.junit.Assert.assertTrue;

import java.sql.Connection;
import java.sql.DriverManager;
import java.util.Properties;
import org.apache.phoenix.end2end.NeedsOwnMiniClusterTest;
import org.apache.phoenix.hbase.index.metrics.MetricsHaBypassSourceFactory;
import org.apache.phoenix.hbase.index.metrics.MetricsHaBypassSourceImpl;
import org.apache.phoenix.jdbc.HABaseIT;
import org.apache.phoenix.jdbc.PhoenixDriver;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;

/**
 * Integration test for the server-side {@code bypassedMutationBlockCount} JMX counter. A "bypass"
 * is a mutation batch that reaches {@code IndexRegionObserver.preBatchMutate} without an associated
 * HA group attribute, causing the cluster-role-based mutation-block gate to be skipped. This IT
 * drives a write that does not carry {@code _HAGroupName} and asserts the counter increments.
 */
@Category(NeedsOwnMiniClusterTest.class)
public class BypassedMutationBlockMetricsIT extends HABaseIT {

  @BeforeClass
  public static synchronized void doSetup() throws Exception {
    CLUSTERS.getHBaseCluster1().getConfiguration()
      .setBoolean(CLUSTER_ROLE_BASED_MUTATION_BLOCK_ENABLED, true);
    CLUSTERS.getHBaseCluster2().getConfiguration()
      .setBoolean(CLUSTER_ROLE_BASED_MUTATION_BLOCK_ENABLED, true);
    CLUSTERS.start();
    DriverManager.registerDriver(PhoenixDriver.INSTANCE);
  }

  @Test(timeout = 300000)
  public void testBypassedMutationBlockCount() throws Exception {
    MetricsHaBypassSourceImpl source =
      (MetricsHaBypassSourceImpl) MetricsHaBypassSourceFactory.getInstance();
    long before = source.getBypassedMutationBlockCountForTesting();

    String dataTableName = generateUniqueName();
    String indexName = generateUniqueName();

    // Connect directly to cluster1 (single-cluster JDBC URL — no _HAGroupName attribute).
    // Use the master/RPC address rather than the ZK url so the rpc URL parser doesn't
    // choke on the `=` token that ZK urls embed.
    Properties props = new Properties();
    try (Connection conn =
      DriverManager.getConnection(CLUSTERS.getJdbcUrl(CLUSTERS.getMasterAddress1()), props)) {
      conn.createStatement().execute(
        "CREATE TABLE " + dataTableName + " (id VARCHAR PRIMARY KEY, name VARCHAR, age INTEGER)");
      conn.createStatement()
        .execute("CREATE INDEX " + indexName + " ON " + dataTableName + "(name)");
      conn.createStatement().execute("UPSERT INTO " + dataTableName + " VALUES ('1', 'A', 1)");
      conn.commit();
    }

    long after = source.getBypassedMutationBlockCountForTesting();
    assertTrue(
      "bypassedMutationBlockCount should increment for mutations without _HAGroupName attribute",
      after > before);
  }
}
