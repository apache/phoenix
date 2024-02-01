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

import static org.apache.phoenix.jdbc.PhoenixHAAdminTool.RET_SUCCESS;
import static org.apache.phoenix.jdbc.PhoenixHAAdminTool.getLocalZkUrl;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.contains;
import static org.mockito.Mockito.atLeastOnce;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.PrintStream;
import java.util.List;
import java.util.Properties;

import org.apache.phoenix.thirdparty.org.apache.commons.cli.Option;
import org.apache.commons.lang3.RandomStringUtils;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.api.GetDataBuilder;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.util.ToolRunner;
import org.apache.phoenix.jdbc.ClusterRoleRecord.ClusterRole;
import org.apache.phoenix.jdbc.PhoenixHAAdminTool.PhoenixHAAdminHelper;
import org.apache.zookeeper.KeeperException.NoNodeException;
import org.junit.After;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TestName;
import org.mockito.Mockito;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Unit test for {@link PhoenixHAAdminTool} including the helper class {@link PhoenixHAAdminHelper}.
 *
 * @see PhoenixHAAdminToolIT
 */
public class PhoenixHAAdminToolTest {
    private static final Logger LOG = LoggerFactory.getLogger(PhoenixHAAdminToolTest.class);
    private static final String ZK1 = "zk1:2181:/hbase";
    private static final String ZK2 = "zk2:2181:/hbase";
    private static final PrintStream STDOUT = System.out;
    private static final ByteArrayOutputStream STDOUT_CAPTURE = new ByteArrayOutputStream();

    private final PhoenixHAAdminTool.HighAvailibilityCuratorProvider mockHighAvailibilityCuratorProvider = Mockito.mock(PhoenixHAAdminTool.HighAvailibilityCuratorProvider.class);

    /** Use mocked curator since there is no mini-ZK cluster. */
    private final CuratorFramework curator = Mockito.mock(CuratorFramework.class);
    /** HA admin to test for one test case. */
    private final PhoenixHAAdminHelper admin = new PhoenixHAAdminHelper(ZK1, new Configuration(), mockHighAvailibilityCuratorProvider);

    private String haGroupName;
    private ClusterRoleRecord recordV1;

    @Rule
    public final TestName testName = new TestName();

    @Before
    public void setup() throws Exception {
        when(mockHighAvailibilityCuratorProvider.getCurator(Mockito.anyString(), any(Properties.class))).thenReturn(curator);
        haGroupName = testName.getMethodName();
        recordV1 = new ClusterRoleRecord(
                haGroupName, HighAvailabilityPolicy.FAILOVER,
                ZK1, ClusterRole.ACTIVE,
                ZK2, ClusterRole.STANDBY,
                1);
        saveRecordV1ToZk();
    }

    @After
    public void after() {
        // reset STDOUT in case it was captured for testing
        System.setOut(STDOUT);
    }

    /**
     * Test command line options.
     *
     * In the test body, we split sections by {} to make sure no variable is reused in mistakenly.
     */
    @Test
    public void testCommandLineOption() throws Exception {
        { // no value for -m option
            String[] args = { "-m" };
            int ret = ToolRunner.run(new PhoenixHAAdminTool(), args);
            assertEquals(PhoenixHAAdminTool.RET_ARGUMENT_ERROR, ret);
        }
        { // -l does not work with -m option
            String[] args = { "-l", "-m", "cluster-role-records.yaml" };
            int ret = ToolRunner.run(new PhoenixHAAdminTool(), args);
            assertEquals(PhoenixHAAdminTool.RET_ARGUMENT_ERROR, ret);
        }
        { // -l does not work with -F/--forceful option
            String[] args = { "-l", "-F"};
            int ret = ToolRunner.run(new PhoenixHAAdminTool(), args);
            assertEquals(PhoenixHAAdminTool.RET_ARGUMENT_ERROR, ret);
        }
        { // -l does not work with --repair option
            String[] args = { "-l", "-r"};
            int ret = ToolRunner.run(new PhoenixHAAdminTool(), args);
            assertEquals(PhoenixHAAdminTool.RET_ARGUMENT_ERROR, ret);
        }
        { // -m does not work with --repair option
            String[] args = { "-m", "cluster-role-records.yaml", "-r"};
            int ret = ToolRunner.run(new PhoenixHAAdminTool(), args);
            assertEquals(PhoenixHAAdminTool.RET_ARGUMENT_ERROR, ret);
        }
    }

    /**
     * Test that helper method works for reading cluster role records from JSON file.
     */
    @Test
    public void testReadRecordsFromFileJson() throws Exception {
        { // one record in JSON file
            String fileName = ClusterRoleRecordTest.createJsonFileWithRecords(recordV1);
            List<ClusterRoleRecord> records = new PhoenixHAAdminTool().readRecordsFromFile(fileName);
            assertEquals(1, records.size());
            assertTrue(records.contains(recordV1));
        }

        { // two records in JSON file
            String haGroupName2 = haGroupName + RandomStringUtils.randomAlphabetic(3);
            ClusterRoleRecord record2 = new ClusterRoleRecord(
                    haGroupName2, HighAvailabilityPolicy.FAILOVER,
                    ZK1, ClusterRole.ACTIVE,
                    ZK2, ClusterRole.STANDBY,
                    1);
            String fileName = ClusterRoleRecordTest.createJsonFileWithRecords(recordV1, record2);
            List<ClusterRoleRecord> records = new PhoenixHAAdminTool().readRecordsFromFile(fileName);
            assertEquals(2, records.size());
            assertTrue(records.contains(recordV1));
            assertTrue(records.contains(record2));
        }
    }

    /**
     * Test that agent will try to create znode if it does not exist.
     */
    @Test
    public void testCreateIfNotExist() throws Exception {
        GetDataBuilder getDataBuilder = Mockito.mock(GetDataBuilder.class);
        when(getDataBuilder.forPath(anyString())).thenThrow(new NoNodeException());
        when(curator.getData()).thenReturn(getDataBuilder);

        try {
            admin.createOrUpdateDataOnZookeeper(recordV1);
        } catch (Exception e) {
            LOG.info("Got expected exception when creating the node without mocking it fully", e);
        }
        verify(curator, atLeastOnce()).create();
    }

    /**
     * Test that agent will try to update znode if given record has a newer version.
     */
    //Ignored as the updates to curator framework made the verify fail test is not stable interfaces
    @Ignore
    @Test
    public void testUpdate() throws Exception {
        boolean result = false;
        saveRecordV1ToZk();
        ClusterRoleRecord recordV2 = new ClusterRoleRecord(
                haGroupName, HighAvailabilityPolicy.FAILOVER,
                ZK1, ClusterRole.STANDBY,
                ZK2 , ClusterRole.STANDBY,
                2); // higher version than recordV1 so update should be tried
       try {
           result = admin.createOrUpdateDataOnZookeeper(recordV2);
       } catch (Exception e) {
           LOG.info("Got expected exception when creating the node without mocking it fully", e);
       }
       verify(curator, never()).create();
       // to update data, internally curator is used this way by DistributedAtomicValue
       verify(curator, atLeastOnce()).newNamespaceAwareEnsurePath(contains(haGroupName));
    }

    /**
     * Test that agent rejects to deal with the record if it is not associated to this ZK.
     */
    @Test
    public void testFailWithUnrelatedRecord() {
        ClusterRoleRecord record2 = new ClusterRoleRecord(
                haGroupName, HighAvailabilityPolicy.FAILOVER,
                ZK1 + RandomStringUtils.random(3), ClusterRole.ACTIVE,  // unrelated ZK
                ZK2 + RandomStringUtils.random(3), ClusterRole.STANDBY, // unrelated ZK
                1);
        try {
            admin.createOrUpdateDataOnZookeeper(record2);
        } catch (IOException e) {
            LOG.info("Got expected exception since the record is not totally related to this ZK");
            assertTrue(e.getMessage().contains("INTERNAL ERROR"));
        }
        verify(curator, never()).getData();  // not even try to read the znode
    }

    /**
     * Test that agent rejects to update the record if its version is lower.
     */
    @Test
    public void testRejectLowerVersionRecord() throws Exception {
        saveRecordV1ToZk();
        ClusterRoleRecord recordV0 = new ClusterRoleRecord(
                haGroupName, HighAvailabilityPolicy.FAILOVER,
                ZK1, ClusterRole.STANDBY,
                ZK2 , ClusterRole.STANDBY,
                0); // lower version than recordV1
        assertFalse(admin.createOrUpdateDataOnZookeeper(recordV0));

        verify(curator, never()).setData();
        verify(curator, never()).create();
    }

    /**
     * Test that agent rejects to update the record if it is inconsistent with existing data.
     */
    @Test
    public void testRejectInconsistentData() throws Exception {
        saveRecordV1ToZk();
        ClusterRoleRecord record2 = new ClusterRoleRecord(
                haGroupName, HighAvailabilityPolicy.FAILOVER,
                ZK1, ClusterRole.STANDBY,
                ZK2 , ClusterRole.STANDBY,
                1); // same version but different role1
        try {
            admin.createOrUpdateDataOnZookeeper(record2);
        } catch (IOException e) {
            LOG.info("Got expected exception in case of inconsistent record data", e);
            assertTrue(e.getMessage().contains("inconsistent"));
        }
    }

    /**
     * Test that the help message is comprehensive enough because this is our operation tool.
     */
    @SuppressWarnings("unchecked")
    @Test
    public void testHelpMessage() throws Exception {
        System.setOut(new PrintStream(STDOUT_CAPTURE));
        int ret = new PhoenixHAAdminTool().run(new String[]{"-h"});
        assertEquals(RET_SUCCESS, ret);
        PhoenixHAAdminTool.OPTIONS.getOptions().forEach(
                (o) -> assertTrue(STDOUT_CAPTURE.toString().contains(((Option) o).getLongOpt())));
    }

    @Test
    public void testGetZookeeperQuorum() {
        Configuration conf = HBaseConfiguration.create();
        // default local ZK is 127.0.0.1:2181:/hbase
        final String localZk = String.format("127.0.0.1:%d:%s",
                HConstants.DEFAULT_ZOOKEPER_CLIENT_PORT, HConstants.DEFAULT_ZOOKEEPER_ZNODE_PARENT);
        assertEquals(localZk, getLocalZkUrl(conf));

        // set host name only; use default port and znode parent
        final String host = "foobar";
        conf.set(HConstants.ZOOKEEPER_QUORUM, "foobar");
        final String expectedLocalZk = String.format("%s:%d:%s", host,
                HConstants.DEFAULT_ZOOKEPER_CLIENT_PORT, HConstants.DEFAULT_ZOOKEEPER_ZNODE_PARENT);
        assertEquals(expectedLocalZk, getLocalZkUrl(conf));

        // set host name and port; use default znode parent
        final int port = 21810;
        conf.setInt(HConstants.ZOOKEEPER_CLIENT_PORT, port);
        final String expectedLocalZk2 = String.format("%s:%d:%s", host, port,
                HConstants.DEFAULT_ZOOKEEPER_ZNODE_PARENT);
        assertEquals(expectedLocalZk2, getLocalZkUrl(conf));

        // set host name, port and znode parent
        final String znode = "/hbase2";
        conf.set(HConstants.ZOOKEEPER_ZNODE_PARENT, znode);
        final String expectedLocalZk3 = String.format("%s:%d:%s", host, port, znode);
        assertEquals(expectedLocalZk3, getLocalZkUrl(conf));

        // empty hostname is invalid
        conf.set(HConstants.ZOOKEEPER_QUORUM, "");
        try {
            getLocalZkUrl(conf);
            fail("Should have failed because " + HConstants.ZOOKEEPER_QUORUM + " is not set");
        } catch (IllegalArgumentException e) {
            LOG.info("Got expected exception when no ZK quorum is set", e);
        }

        // invalid port
        String invalidPort = "invalidPort";
        conf.set(HConstants.ZOOKEEPER_CLIENT_PORT, "invalidPort");
        try {
            getLocalZkUrl(conf);
            fail("Should have failed because port " + invalidPort + " is invalid");
        } catch (IllegalArgumentException e) {
            LOG.info("Got expected exception because port {} is invalid", invalidPort, e);
        }
    }

    /** Helper method to make curator return V1 record when agent reads data. */
    private void saveRecordV1ToZk() throws Exception {
        GetDataBuilder getDataBuilder = Mockito.mock(GetDataBuilder.class);
        when(getDataBuilder.forPath(anyString())).thenReturn(ClusterRoleRecord.toJson(recordV1));
        when(curator.getData()).thenReturn(getDataBuilder);
    }
}
