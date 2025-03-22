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
import org.apache.phoenix.thirdparty.com.google.common.collect.Maps;
import org.apache.phoenix.util.ReadOnlyProps;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import java.util.List;
import java.util.Map;

import static org.apache.phoenix.jdbc.PhoenixHAAdmin.toPath;
import static org.apache.phoenix.query.QueryServices.CLUSTER_ROLE_BASED_MUTATION_BLOCK_ENABLED;
import static org.junit.Assert.assertFalse;


@Category(NeedsOwnMiniClusterTest.class)
public class HAGroupStoreManagerIT extends BaseTest {
    private final PhoenixHAAdmin haAdmin = new PhoenixHAAdmin(config);
    private static final Long ZK_CURATOR_EVENT_PROPAGATION_TIMEOUT_MS = 1000L;

    @BeforeClass
    public static synchronized void doSetup() throws Exception {
        Map<String, String> props = Maps.newHashMapWithExpectedSize(1);
        props.put(CLUSTER_ROLE_BASED_MUTATION_BLOCK_ENABLED, "true");
        setUpTestDriver(new ReadOnlyProps(props.entrySet().iterator()));
    }

    @Before
    public void before() throws Exception {
        // Clean up all the existing CRRs
        List<ClusterRoleRecord> crrs = haAdmin.listAllClusterRoleRecordsOnZookeeper();
        for (ClusterRoleRecord crr : crrs) {
            haAdmin.getCurator().delete().forPath(toPath(crr.getHaGroupName()));
        }
    }

    @Test
    public void testHACacheWithSingleCRR() throws Exception {
        HAGroupStoreManager haGroupStoreManager = HAGroupStoreManager.getInstance(config);
        // Setup initial CRRs
        ClusterRoleRecord crr1 = new ClusterRoleRecord("failover",
                HighAvailabilityPolicy.FAILOVER, haAdmin.getZkUrl(), ClusterRoleRecord.ClusterRole.ACTIVE,
                "random-zk-url", ClusterRoleRecord.ClusterRole.STANDBY, 1L);
        ClusterRoleRecord crr2 = new ClusterRoleRecord("parallel",
                HighAvailabilityPolicy.PARALLEL, haAdmin.getZkUrl(), ClusterRoleRecord.ClusterRole.ACTIVE,
                "random-zk-url", ClusterRoleRecord.ClusterRole.STANDBY, 1L);
        haAdmin.createOrUpdateDataOnZookeeper(crr1);
        haAdmin.createOrUpdateDataOnZookeeper(crr2);

        Thread.sleep(ZK_CURATOR_EVENT_PROPAGATION_TIMEOUT_MS);

        assertFalse(haGroupStoreManager.isMutationBlocked());

        crr1 = new ClusterRoleRecord("failover",
                HighAvailabilityPolicy.FAILOVER, haAdmin.getZkUrl(), ClusterRoleRecord.ClusterRole.ACTIVE,
                "random-zk-url", ClusterRoleRecord.ClusterRole.STANDBY, 2L);
        crr2 = new ClusterRoleRecord("parallel",
                HighAvailabilityPolicy.PARALLEL, haAdmin.getZkUrl(), ClusterRoleRecord.ClusterRole.ACTIVE_TO_STANDBY,
                "random-zk-url", ClusterRoleRecord.ClusterRole.STANDBY, 2L);
        haAdmin.createOrUpdateDataOnZookeeper(crr1);
        haAdmin.createOrUpdateDataOnZookeeper(crr2);

        Thread.sleep(ZK_CURATOR_EVENT_PROPAGATION_TIMEOUT_MS);

        assert haGroupStoreManager.isMutationBlocked();
    }
}
