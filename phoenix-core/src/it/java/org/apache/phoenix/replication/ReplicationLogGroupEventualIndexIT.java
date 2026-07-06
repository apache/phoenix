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
package org.apache.phoenix.replication;

import static org.apache.phoenix.hbase.index.IndexRegionObserver.PHOENIX_INDEX_CDC_CONSUMER_ENABLED;
import static org.apache.phoenix.query.BaseTest.generateUniqueName;

import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.util.Arrays;
import org.apache.phoenix.end2end.NeedsOwnMiniClusterTest;
import org.apache.phoenix.jdbc.FailoverPhoenixConnection;
import org.apache.phoenix.util.CDCUtil;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;

/**
 * CDC index behind a CONSISTENCY=EVENTUAL secondary index. Lives in its own IT (not in
 * {@link ReplicationLogGroupIT}) because the {@code serializeCDCMutations} variant is a
 * cluster-level config and is exercised by the
 * {@link ReplicationLogGroupEventualIndexWithSerializeCDCIT} subclass, which inherits exactly this
 * one test. The base class runs it under the default {@code serializeCDCMutations=false}.
 */
@Category(NeedsOwnMiniClusterTest.class)
public class ReplicationLogGroupEventualIndexIT extends ReplicationLogGroupBaseIT {

  @BeforeClass
  public static void doSetup() throws Exception {
    setupEventualIndexClusters();
  }

  /**
   * Disables the IndexCDCConsumer on both clusters, then starts them. CDC-index regeneration is
   * verified directly and the consumer's downstream secondary-index convergence is out of scope, so
   * it stays off. The {@code serializeCDCMutations} subclass calls this after setting its own
   * toggle so the consumer-disable lives in one place.
   */
  protected static void setupEventualIndexClusters() throws Exception {
    conf1.setBoolean(PHOENIX_INDEX_CDC_CONSUMER_ENABLED, false);
    conf2.setBoolean(PHOENIX_INDEX_CDC_CONSUMER_ENABLED, false);
    setupClusters();
  }

  /**
   * An eventually-consistent secondary index auto-creates a CDC index behind it ({@code CDC_
   *
  <table>
   * } -&gt; {@code PHOENIX_CDC_INDEX_CDC_
   *
  <table>
   * }, MetaDataClient.java:2473). That CDC index is STRONG/uncovered and written inline on the data
   * path, so the standby regenerates it from the data record + per-(row,ts) pre-image with its own
   * partition_id, exactly like a plain CDC index. This verifies the CDC index table matches across
   * clusters (modulo partition_id). The eventual secondary index table itself is written only by
   * the (here-disabled) IndexCDCConsumer, so it stays empty on both clusters and is not compared.
   * The serialized downstream-index payload column is present iff {@code serializeCDCMutations} is
   * enabled (asserted via {@link #assertCDCIndexPayloadMatchesConfig}); the {@code serialize=true}
   * variant is exercised by {@link ReplicationLogGroupEventualIndexWithSerializeCDCIT}.
   */
  @Test
  public void testEventualIndexCDCTable() throws Exception {
    final String tableName = "T_" + generateUniqueName();
    final String indexName = "I_" + generateUniqueName();
    final String cdcIndexName = CDCUtil.getCDCIndexName("CDC_" + tableName);
    String createTableDdl = String.format(
      "create table if not exists %s (pk integer not null " + "primary key, a varchar, b varchar)",
      tableName);
    String createIndexDdl =
      String.format("create index if not exists %s on %s (a) include (b) consistency=eventual",
        indexName, tableName);
    // CONSISTENCY=EVENTUAL auto-creates this CDC object (CDC_<dataTable>) and its physical CDC
    // index. We must issue the CREATE CDC explicitly, before the index, on BOTH clusters. The two
    // mini-clusters share one JVM metadata cache: cluster 1's CREATE INDEX registers the index
    // PTable in that cache, so when the same DDL replays on cluster 2 the index appears to already
    // exist and createIndex returns early (table == null) WITHOUT reaching the nested
    // createCDCForEventuallyConsistentIndex (MetaDataClient.java:1915-1925). The CDC index physical
    // region would then never be created on cluster 2 and the replayed index writes fail with
    // "Cannot get replica 0 location". Issuing CREATE CDC IF NOT EXISTS directly runs CREATE
    // UNCOVERED INDEX -> ensureTableCreated (which checks the per-cluster HBase admin, not the
    // shared cache), so the physical CDC index region is created on each cluster.
    String createCdcDdl =
      String.format("create cdc if not exists \"CDC_%s\" on %s", tableName, tableName);

    try (FailoverPhoenixConnection conn = (FailoverPhoenixConnection) DriverManager
      .getConnection(CLUSTERS.getJdbcHAUrl(), clientProps)) {
      conn.createStatement().execute(createTableDdl);
      conn.createStatement().execute(createCdcDdl);
      conn.createStatement().execute(createIndexDdl);
      conn.commit();

      PreparedStatement upsert =
        conn.prepareStatement("upsert into " + tableName + " VALUES(?, ?, ?)");
      for (int i = 0; i < 10; ++i) {
        upsert.setInt(1, i);
        upsert.setString(2, "a_" + i);
        upsert.setString(3, "b_" + i);
        upsert.executeUpdate();
        conn.commit();
      }

      PreparedStatement update =
        conn.prepareStatement("upsert into " + tableName + " (pk, a) VALUES(?, ?)");
      for (int i = 0; i < 5; ++i) {
        update.setInt(1, i);
        update.setString(2, "a2_" + i);
        update.executeUpdate();
      }
      conn.commit();

      PreparedStatement delete =
        conn.prepareStatement("delete from " + tableName + " where pk = ?");
      for (int i = 5; i < 8; ++i) {
        delete.setInt(1, i);
        delete.executeUpdate();
      }
      conn.commit();

      replayAndVerifyAcrossClusters(Arrays.asList(createTableDdl, createCdcDdl, createIndexDdl),
        tableName);
      assertCDCIndexEqualAcrossClusters(cdcIndexName);
      // The cross-cluster compare above is symmetric, so it passes whether or not the serialized
      // downstream-index payload is present. Assert it positively against the live config so a
      // serialize=true run that failed to propagate would fail loudly rather than pass
      // degenerately.
      assertCDCIndexPayloadMatchesConfig(cdcIndexName);
    }
  }
}
