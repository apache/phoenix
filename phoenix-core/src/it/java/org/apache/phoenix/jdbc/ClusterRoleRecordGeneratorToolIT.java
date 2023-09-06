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

import static org.apache.phoenix.jdbc.ClusterRoleRecordGeneratorTool.PHOENIX_HA_GENERATOR_FILE_ATTR;
import static org.apache.phoenix.jdbc.ClusterRoleRecordGeneratorTool.PHOENIX_HA_GROUPS_ATTR;
import static org.apache.phoenix.jdbc.ClusterRoleRecordGeneratorTool.PHOENIX_HA_GROUP_POLICY_ATTR_FORMAT;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import java.io.File;
import java.util.List;

import org.apache.commons.io.FileUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.util.ToolRunner;
import org.apache.phoenix.end2end.NeedsOwnMiniClusterTest;
import org.apache.phoenix.jdbc.ClusterRoleRecord.ClusterRole;
import org.apache.phoenix.jdbc.HighAvailabilityTestingUtility.HBaseTestingUtilityPair;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.TestName;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Integration test for {@link ClusterRoleRecordGeneratorTool}.
 *
 * @see ClusterRoleRecordGeneratorToolTest
 */
@Category(NeedsOwnMiniClusterTest.class)
public class ClusterRoleRecordGeneratorToolIT {
    private static final Logger LOG = LoggerFactory.getLogger(ClusterRoleRecordGeneratorToolIT.class);
    private static final HBaseTestingUtilityPair CLUSTERS = new HBaseTestingUtilityPair();

    @Rule
    public final TestName testName = new TestName();

    @BeforeClass
    public static void setUpBeforeClass() throws Exception {
        CLUSTERS.start();
    }

    @AfterClass
    public static void tearDownAfterClass() throws Exception {
        CLUSTERS.close();
    }

    @Test
    public void testRun() throws Exception {
        ClusterRoleRecordGeneratorTool generator = new ClusterRoleRecordGeneratorTool();
        Configuration conf = new Configuration(CLUSTERS.getHBaseCluster1().getConfiguration());
        String haGroupNames = String.format("%s,%s,%s", testName.getMethodName(),
                testName.getMethodName() + 1, testName.getMethodName() + 2);
        conf.set(PHOENIX_HA_GROUPS_ATTR, haGroupNames);
        // create a temp file
        File file = File.createTempFile("phoenix.ha.cluster.role.records", ".json");
        file.deleteOnExit();
        conf.set(PHOENIX_HA_GENERATOR_FILE_ATTR, file.getAbsolutePath());

        generator.setConf(conf);
        int ret = ToolRunner.run(conf, new ClusterRoleRecordGeneratorTool(), new String[]{});
        assertEquals(0, ret);
        String recordJson = FileUtils.readFileToString(file);
        LOG.info("The created file content is: \n{}", recordJson);
        for (String haGroupName : haGroupNames.split(",")) {
            assertTrue(recordJson.contains(haGroupName));
        }
    }

    @Test
    public void testListAllRecordsByGenerator1() throws Exception {
        ClusterRoleRecordGeneratorTool generator1 = new ClusterRoleRecordGeneratorTool();
        Configuration conf1 = new Configuration(CLUSTERS.getHBaseCluster1().getConfiguration());
        conf1.set(PHOENIX_HA_GROUPS_ATTR, testName.getMethodName());
        // explicitly set the failover policy as FAILOVER
        conf1.set(String.format(PHOENIX_HA_GROUP_POLICY_ATTR_FORMAT, testName.getMethodName()),
                HighAvailabilityPolicy.FAILOVER.name());
        generator1.setConf(conf1);

        // created with cluster1's conf, so cluster1 is ACTIVE
        List<ClusterRoleRecord> records = generator1.listAllRecordsByZk();
        assertNotNull(records);
        LOG.info("Generated following records from cluster1: {}", records);
        assertEquals(1, records.size());
        for (ClusterRoleRecord record : records) {
            assertEquals(HighAvailabilityPolicy.FAILOVER, record.getPolicy());
            assertEquals(ClusterRole.ACTIVE, record.getRole(CLUSTERS.getUrl1()));
            assertEquals(ClusterRole.STANDBY, record.getRole(CLUSTERS.getUrl2()));
        }
    }

    @Test
    public void testListAllRecordsByGenerator2() throws Exception {
        ClusterRoleRecordGeneratorTool generator2 = new ClusterRoleRecordGeneratorTool();
        Configuration conf2 = new Configuration(CLUSTERS.getHBaseCluster2().getConfiguration());
        // Here we set multiple (3) HA group names in conf for testing on store2
        String haGroupNames = String.format("%s,%s,%s", testName.getMethodName(),
                testName.getMethodName() + 1, testName.getMethodName() + 2);
        conf2.set(PHOENIX_HA_GROUPS_ATTR, haGroupNames);
        generator2.setConf(conf2);

        // created with cluster2's conf, so cluster2 is ACTIVE
        List<ClusterRoleRecord> records = generator2.listAllRecordsByZk();
        assertNotNull(records);
        LOG.info("Generated following records from cluster2: {}", records);
        assertEquals(3, records.size());
        for (ClusterRoleRecord record : records) {
            assertEquals(HighAvailabilityPolicy.PARALLEL, record.getPolicy());
            assertEquals(ClusterRole.ACTIVE, record.getRole(CLUSTERS.getUrl2()));
            assertEquals(ClusterRole.STANDBY, record.getRole(CLUSTERS.getUrl1()));
        }
    }
}
