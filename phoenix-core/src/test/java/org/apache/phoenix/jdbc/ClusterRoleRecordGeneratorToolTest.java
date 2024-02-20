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

import static org.apache.phoenix.jdbc.ClusterRoleRecordGeneratorTool.PHOENIX_HA_GROUP_POLICY_ATTR_FORMAT;
import static org.apache.phoenix.jdbc.ClusterRoleRecordGeneratorTool.PHOENIX_HA_GROUP_STORE_PEER_ID_DEFAULT;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.replication.ReplicationAdmin;
import org.apache.hadoop.hbase.replication.ReplicationPeerConfig;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TestName;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Unit test for {@link ClusterRoleRecordGeneratorTool}.
 *
 * @see ClusterRoleRecordGeneratorToolIT
 */
public class ClusterRoleRecordGeneratorToolTest {
    private static final Logger LOG = LoggerFactory.getLogger(ClusterRoleRecordGeneratorToolTest.class);

    private final Configuration conf = HBaseConfiguration.create();
    private final ClusterRoleRecordGeneratorTool generator = new ClusterRoleRecordGeneratorTool();

    @Rule
    public final TestName testName = new TestName();

    @Before
    public void before() {
        generator.setConf(conf);
    }

    @Test
    public void testGetPeerClusterKey() throws Exception {
        String peerZk = "localhost:2188:/hbase";
        ReplicationPeerConfig replicationConfig = mock(ReplicationPeerConfig.class);
        when(replicationConfig.getClusterKey()).thenReturn(peerZk);
        Admin admin = mock(Admin.class);

        String id = PHOENIX_HA_GROUP_STORE_PEER_ID_DEFAULT;
        when(admin.getReplicationPeerConfig(eq(id))).thenReturn(replicationConfig);
        assertEquals(peerZk, ClusterRoleRecordGeneratorTool.getPeerClusterKey(admin, id));

        id = "1984";
        when(admin.getReplicationPeerConfig(eq(id))).thenReturn(replicationConfig);
        assertEquals(peerZk, ClusterRoleRecordGeneratorTool.getPeerClusterKey(admin, id));
    }

    @Test
    public void testGetHaPolicy() throws IOException {
        String haGroupName = testName.getMethodName();
        // default HA policy is PARALLEL used for 1P
        assertEquals(HighAvailabilityPolicy.PARALLEL, generator.getHaPolicy(haGroupName));

        // return explicit HA policy set in the config
        conf.set(String.format(PHOENIX_HA_GROUP_POLICY_ATTR_FORMAT, haGroupName),
                HighAvailabilityPolicy.FAILOVER.name());
        assertEquals(HighAvailabilityPolicy.FAILOVER, generator.getHaPolicy(haGroupName));

        // other HA group still has default HA policy
        String haGroupName2 = haGroupName + 2;
        assertEquals(HighAvailabilityPolicy.PARALLEL, generator.getHaPolicy(haGroupName2));

        // invalid HA policy name
        String invalidHaPolicy = "foobar";
        conf.set(String.format(PHOENIX_HA_GROUP_POLICY_ATTR_FORMAT, haGroupName), invalidHaPolicy);
        try {
            generator.getHaPolicy(haGroupName);
            fail("Should have failed since no such HA policy named " + invalidHaPolicy);
        } catch (IOException e) {
            LOG.info("Got expected exception for invalid HA policy name {}", invalidHaPolicy, e);
            assertNotNull(e.getCause());
            assertTrue(e.getCause() instanceof IllegalArgumentException);
        }
    }
}
