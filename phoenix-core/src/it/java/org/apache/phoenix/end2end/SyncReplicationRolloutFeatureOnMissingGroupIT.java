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
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.fail;

import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.util.Map;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HConstants;
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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.phoenix.thirdparty.com.google.common.collect.Maps;

/**
 * Complement of {@link SyncReplicationRolloutStep1IT}: the same production-representative HA guard
 * flags, but with the master switch {@code SYNCHRONOUS_REPLICATION_ENABLED = true} while the HA
 * group has not been created yet — an out-of-order rollout (the feature enabled before the group
 * exists).
 * <p>
 * With the feature on, {@code IndexRegionObserver.preBatchMutate} runs the cluster-role-based
 * mutation-block / staleness gate for a {@code CLIENT_HA} write and tries to resolve the (absent)
 * HA group, so the write fails closed. This pins that fail-closed contract, ensuring the master
 * switch must not be flipped on until the HA group is created (rollout Step 2 before Step 3).
 */
@Category(NeedsOwnMiniClusterTest.class)
public class SyncReplicationRolloutFeatureOnMissingGroupIT extends BaseTest {

  private static final Logger LOG =
    LoggerFactory.getLogger(SyncReplicationRolloutFeatureOnMissingGroupIT.class);

  @BeforeClass
  public static synchronized void doSetup() throws Exception {
    Map<String, String> props = Maps.newHashMapWithExpectedSize(3);
    // Feature on, but the HA group does not exist yet; production-representative HA guard flags on.
    props.put(SYNCHRONOUS_REPLICATION_ENABLED, Boolean.TRUE.toString());
    props.put(CLUSTER_ROLE_BASED_MUTATION_BLOCK_ENABLED, Boolean.TRUE.toString());
    props.put(HA_GROUP_STALE_FOR_MUTATION_CHECK_ENABLED, Boolean.TRUE.toString());
    setUpTestDriver(new ReadOnlyProps(props.entrySet().iterator()));
  }

  @Test
  public void testWriteWithHAGroupAttributeFailsWhenFeatureEnabledAndGroupMissing()
    throws Exception {
    String tableName = generateUniqueName();
    try (Connection conn = DriverManager.getConnection(getUrl())) {
      // COLUMN_ENCODED_BYTES=0 so the raw Put below can use literal column qualifiers.
      conn.createStatement().execute("CREATE TABLE " + tableName
        + " (id VARCHAR PRIMARY KEY, v VARCHAR) COLUMN_ENCODED_BYTES=0");
    }

    // Emulate an upgraded client: a well-formed Phoenix row whose mutation carries _HAGroupName for
    // a group that has no server-side HAGroupStoreRecord (the feature was enabled before Step 2).
    Put put = new Put(Bytes.toBytes("row1"));
    put.addColumn(DEFAULT_COLUMN_FAMILY_BYTES, EMPTY_COLUMN_BYTES, EMPTY_COLUMN_VALUE_BYTES);
    put.addColumn(DEFAULT_COLUMN_FAMILY_BYTES, Bytes.toBytes("V"), Bytes.toBytes("hello"));
    put.setAttribute(HA_GROUP_NAME_ATTRIB, Bytes.toBytes("group_" + tableName));

    // Bound client retries so a retriable failure surfaces promptly rather than storming.
    Configuration conf = new Configuration(getUtility().getConfiguration());
    conf.setInt(HConstants.HBASE_CLIENT_RETRIES_NUMBER, 1);
    conf.setLong(HConstants.HBASE_CLIENT_OPERATION_TIMEOUT, 60000);

    try (org.apache.hadoop.hbase.client.Connection hcon = ConnectionFactory.createConnection(conf);
      Table htable = hcon.getTable(TableName.valueOf(tableName))) {
      // Feature on -> the HA gate runs, resolves the absent HA group, and fails the write.
      htable.put(put);
      fail("write carrying _HAGroupName must fail when the feature is enabled but the HA group "
        + "does not exist yet");
    } catch (IOException expected) {
      LOG.info("Write failed as expected for missing HA group", expected);
    }

    try (Connection conn = DriverManager.getConnection(getUrl())) {
      ResultSet rs =
        conn.createStatement().executeQuery("SELECT v FROM " + tableName + " WHERE id = 'row1'");
      assertFalse("a write that failed closed must not be visible", rs.next());
    }
  }
}
