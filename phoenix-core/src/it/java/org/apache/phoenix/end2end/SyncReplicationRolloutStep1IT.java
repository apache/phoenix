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
package org.apache.phoenix.end2end;

import static org.apache.phoenix.coprocessorclient.BaseScannerRegionObserverConstants.HA_GROUP_NAME_ATTRIB;
import static org.apache.phoenix.query.QueryConstants.DEFAULT_COLUMN_FAMILY_BYTES;
import static org.apache.phoenix.query.QueryConstants.EMPTY_COLUMN_BYTES;
import static org.apache.phoenix.query.QueryConstants.EMPTY_COLUMN_VALUE_BYTES;
import static org.apache.phoenix.query.QueryServices.CLUSTER_ROLE_BASED_MUTATION_BLOCK_ENABLED;
import static org.apache.phoenix.query.QueryServices.HA_GROUP_STALE_FOR_MUTATION_CHECK_ENABLED;
import static org.apache.phoenix.query.QueryServices.SYNCHRONOUS_REPLICATION_ENABLED;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.util.Map;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.phoenix.query.BaseTest;
import org.apache.phoenix.util.ReadOnlyProps;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import org.apache.phoenix.thirdparty.com.google.common.collect.Maps;

/**
 * Reproduces the Step-1 window of the synchronous-replication rollout: servers are upgraded and the
 * new write-path code is live, but the feature is still dark
 * ({@code SYNCHRONOUS_REPLICATION_ENABLED
 * = false}) and the HA group has not been created yet, while clients — upgraded first — already
 * attach the {@code _HAGroupName} attribute to every mutation.
 * <p>
 * Production runs {@code phoenix.cluster.role.based.mutation.block.enabled = true} and the
 * stale-CRR check defaults on, so in this window {@code IndexRegionObserver.preBatchMutate} reaches
 * the cluster-role-based mutation-block / staleness gate — which keys off {@code _HAGroupName}
 * presence, not the feature flag — and resolves the (absent) HA group, failing the write.
 * <p>
 * The contract this test asserts (design decision D1): {@code SYNCHRONOUS_REPLICATION_ENABLED} is
 * the master switch for all HA semantics, so with the feature off the gate must not run and the
 * write must succeed regardless of the block/stale sub-flags or whether the HA group exists.
 */
@Category(NeedsOwnMiniClusterTest.class)
public class SyncReplicationRolloutStep1IT extends BaseTest {

  @BeforeClass
  public static synchronized void doSetup() throws Exception {
    Map<String, String> props = Maps.newHashMapWithExpectedSize(3);
    // Step-1 state: feature dark, but production-representative HA guard flags on.
    props.put(SYNCHRONOUS_REPLICATION_ENABLED, Boolean.FALSE.toString());
    props.put(CLUSTER_ROLE_BASED_MUTATION_BLOCK_ENABLED, Boolean.TRUE.toString());
    props.put(HA_GROUP_STALE_FOR_MUTATION_CHECK_ENABLED, Boolean.TRUE.toString());
    setUpTestDriver(new ReadOnlyProps(props.entrySet().iterator()));
  }

  @Test
  public void testWriteWithHAGroupAttributeSucceedsWhenFeatureDisabledAndGroupMissing()
    throws Exception {
    String tableName = generateUniqueName();
    try (Connection conn = DriverManager.getConnection(getUrl())) {
      // COLUMN_ENCODED_BYTES=0 so the raw Put below can use literal column qualifiers.
      conn.createStatement().execute("CREATE TABLE " + tableName
        + " (id VARCHAR PRIMARY KEY, v VARCHAR) COLUMN_ENCODED_BYTES=0");
    }

    // Emulate an upgraded client: a well-formed Phoenix row whose mutation carries _HAGroupName for
    // a group that has no server-side HAGroupStoreRecord (not created until rollout Step 2).
    Put put = new Put(Bytes.toBytes("row1"));
    put.addColumn(DEFAULT_COLUMN_FAMILY_BYTES, EMPTY_COLUMN_BYTES, EMPTY_COLUMN_VALUE_BYTES);
    put.addColumn(DEFAULT_COLUMN_FAMILY_BYTES, Bytes.toBytes("V"), Bytes.toBytes("hello"));
    put.setAttribute(HA_GROUP_NAME_ATTRIB, Bytes.toBytes("group_" + tableName));

    try (
      org.apache.hadoop.hbase.client.Connection hcon =
        ConnectionFactory.createConnection(getUtility().getConfiguration());
      Table htable = hcon.getTable(TableName.valueOf(tableName))) {
      // With the feature off this must not evaluate the HA gate for the missing group.
      htable.put(put);
    }

    try (Connection conn = DriverManager.getConnection(getUrl())) {
      ResultSet rs =
        conn.createStatement().executeQuery("SELECT v FROM " + tableName + " WHERE id = 'row1'");
      assertTrue("row written with the _HAGroupName attribute must be visible", rs.next());
      assertEquals("hello", rs.getString(1));
    }
  }
}
