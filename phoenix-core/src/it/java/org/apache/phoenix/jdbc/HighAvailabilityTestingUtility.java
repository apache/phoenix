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

import org.apache.hadoop.hbase.*;
import org.apache.phoenix.end2end.PhoenixRegionServerEndpointTestImpl;
import org.apache.phoenix.end2end.ServerMetadataCacheTestImpl;
import org.apache.phoenix.hbase.index.write.TestTrackingParallelWriterIndexCommitter;
import org.apache.phoenix.jdbc.PhoenixHAAdmin;
import org.apache.phoenix.thirdparty.com.google.common.annotations.VisibleForTesting;
import org.apache.phoenix.thirdparty.com.google.common.collect.ImmutableList;
import org.apache.phoenix.util.HAGroupStoreTestUtil;
import org.apache.zookeeper.data.Stat;
import org.apache.commons.lang3.RandomUtils;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.curator.framework.CuratorFramework;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.ipc.PhoenixRpcSchedulerFactory;
import org.apache.hadoop.hbase.regionserver.RSRpcServices;
import org.apache.hadoop.hbase.replication.ReplicationPeerConfig;
import org.apache.phoenix.hbase.index.util.IndexManagementUtil;
import org.apache.phoenix.jdbc.ClusterRoleRecord.ClusterRole;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import java.util.Properties;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicReference;

import static org.apache.hadoop.hbase.HConstants.*;
import static org.apache.hadoop.hbase.coprocessor.CoprocessorHost.REGIONSERVER_COPROCESSOR_CONF_KEY;
import static org.apache.hadoop.hbase.ipc.RpcClient.*;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_REPLICATION_KEY;
import static org.apache.hadoop.test.GenericTestUtils.waitFor;
import static org.apache.phoenix.hbase.index.write.AbstractParallelWriterIndexCommitter.NUM_CONCURRENT_INDEX_WRITER_THREADS_CONF_KEY;
import static org.apache.phoenix.hbase.index.write.IndexWriter.INDEX_COMMITTER_CONF_KEY;
import static org.apache.phoenix.jdbc.ClusterRoleRecord.ClusterRole.ACTIVE;
import static org.apache.phoenix.jdbc.ClusterRoleRecord.ClusterRole.OFFLINE;
import static org.apache.phoenix.jdbc.ClusterRoleRecord.ClusterRole.STANDBY;
import static org.apache.phoenix.jdbc.ClusterRoleRecord.ClusterRole.UNKNOWN;
import static org.apache.phoenix.jdbc.ClusterRoleRecordGeneratorTool.PHOENIX_HA_GROUP_STORE_PEER_ID_DEFAULT;
import static org.apache.phoenix.jdbc.FailoverPhoenixConnection.FAILOVER_TIMEOUT_MS_ATTR;
import static org.apache.phoenix.jdbc.HAGroupStoreClient.PHOENIX_HA_GROUP_STORE_CLIENT_INITIALIZATION_TIMEOUT_MS;
import static org.apache.phoenix.jdbc.HighAvailabilityGroup.*;
import static org.apache.phoenix.jdbc.PhoenixDatabaseMetaData.SYSTEM_HA_GROUP_NAME;
import static org.apache.phoenix.jdbc.PhoenixHAAdmin.HighAvailibilityCuratorProvider;

import static org.apache.phoenix.jdbc.PhoenixHAAdmin.toPath;
import static org.apache.phoenix.query.QueryServices.COLLECT_REQUEST_LEVEL_METRICS;
import static org.apache.phoenix.util.PhoenixRuntime.JDBC_PROTOCOL_MASTER;
import static org.apache.phoenix.util.PhoenixRuntime.JDBC_PROTOCOL_RPC;
import static org.apache.phoenix.util.PhoenixRuntime.JDBC_PROTOCOL_ZK;
import static org.apache.phoenix.util.TestUtil.dumpTable;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

/**
 * Utility class for testing HBase failover.
 */
public class HighAvailabilityTestingUtility {
    private static final Logger LOG = LoggerFactory.getLogger(HighAvailabilityTestingUtility.class);

    /**
     * Utility class for creating and maintaining HBase DR cluster pair.
     *
     * TODO: @Bharath check if we can use utility from upstream HBase for a pair of mini clusters.
     */
    public static class HBaseTestingUtilityPair implements Closeable {
        private final HBaseTestingUtility hbaseCluster1 = new HBaseTestingUtility();
        private final HBaseTestingUtility hbaseCluster2 = new HBaseTestingUtility();
        /** The host\:port::/hbase format of the JDBC string for HBase cluster 1. */
        private String zkUrl1;
        /** The host\:port::/hbase format of the JDBC string for HBase cluster 2. */
        private String zkUrl2;
        private String masterAddress1;
        private String masterAddress2;
        private PhoenixHAAdmin haAdmin1;
        private PhoenixHAAdmin haAdmin2;
        private Admin admin1;
        private Admin admin2;
        @VisibleForTesting
        static final String PRINCIPAL = "USER_FOO";

        public HBaseTestingUtilityPair() {
            Configuration conf1 = hbaseCluster1.getConfiguration();
            Configuration conf2 = hbaseCluster2.getConfiguration();
            setUpDefaultHBaseConfig(conf1);
            setUpDefaultHBaseConfig(conf2);
        }

        /**
         * Instantiates and starts the two mini HBase DR cluster. Enable peering if configured.
         *
         * @throws Exception if fails to start either cluster
         */
        public void start() throws Exception {
            hbaseCluster1.startMiniCluster();
            hbaseCluster2.startMiniCluster();

            /*
            Note that in hbase2 testing utility these give inconsistent results, one gives ip other gives localhost
            String address1 = hbaseCluster1.getZkCluster().getAddress().toString();
            String confAddress = hbaseCluster1.getConfiguration().get(HConstants.ZOOKEEPER_QUORUM);
             */

            String confAddress1 = hbaseCluster1.getConfiguration().get(HConstants.ZOOKEEPER_QUORUM);
            String confAddress2 = hbaseCluster2.getConfiguration().get(HConstants.ZOOKEEPER_QUORUM);

            zkUrl1 = String.format("%s\\:%d::/hbase", confAddress1, hbaseCluster1.getZkCluster().getClientPort());
            zkUrl2 = String.format("%s\\:%d::/hbase", confAddress2, hbaseCluster2.getZkCluster().getClientPort());

            masterAddress1 = hbaseCluster1.getConfiguration().get(HConstants.MASTER_ADDRS_KEY).replaceAll(":", "\\\\:");
            masterAddress2 = hbaseCluster2.getConfiguration().get(HConstants.MASTER_ADDRS_KEY).replaceAll(":", "\\\\:");

            haAdmin1 = new PhoenixHAAdmin(getZkUrl1(), hbaseCluster1.getConfiguration(), HighAvailibilityCuratorProvider.INSTANCE, HAGroupStoreClient.ZK_CONSISTENT_HA_NAMESPACE);
            haAdmin2 = new PhoenixHAAdmin(getZkUrl2(), hbaseCluster2.getConfiguration(), HighAvailibilityCuratorProvider.INSTANCE, HAGroupStoreClient.ZK_CONSISTENT_HA_NAMESPACE);

            admin1 = hbaseCluster1.getConnection().getAdmin();
            admin2 = hbaseCluster2.getConnection().getAdmin();

            // Enable replication between the two HBase clusters.
            ReplicationPeerConfig replicationPeerConfig1 = ReplicationPeerConfig.newBuilder().setClusterKey(hbaseCluster2.getClusterKey()).build();
            ReplicationPeerConfig replicationPeerConfig2 = ReplicationPeerConfig.newBuilder().setClusterKey(hbaseCluster1.getClusterKey()).build();

            admin1.addReplicationPeer(PHOENIX_HA_GROUP_STORE_PEER_ID_DEFAULT, replicationPeerConfig1);
            admin2.addReplicationPeer(PHOENIX_HA_GROUP_STORE_PEER_ID_DEFAULT, replicationPeerConfig2);

            LOG.info("MiniHBase DR cluster pair is ready for testing.  Cluster Urls [{},{}] and Master Urls [{}, {}]",
                    getZkUrl1(), getZkUrl2(), getMasterAddress1(), getMasterAddress2());
            logClustersStates();
        }

        /**
         * Get Specific url of specific cluster based on index and registryType
         */
        public String getURL(int clusterIndex, ClusterRoleRecord.RegistryType registryType) {
            if (registryType == null) {
                return clusterIndex == 1 ? masterAddress1 : masterAddress2;
            }
            switch (registryType) {
                case ZK:
                    return clusterIndex == 1 ? zkUrl1 : zkUrl2;
                case RPC:
                case MASTER:
                default:
                    return clusterIndex == 1 ? masterAddress1 : masterAddress2;
                    
            }
        }

        /** initialize two HBase clusters for cluster role znode. */
        public void initClusterRole(String haGroupName, HighAvailabilityPolicy policy)
                throws Exception {
            HAGroupStoreRecord activeRecord = new HAGroupStoreRecord(HAGroupStoreRecord.DEFAULT_PROTOCOL_VERSION, haGroupName, ACTIVE.getDefaultHAGroupState(), HAGroupStoreRecord.DEFAULT_RECORD_VERSION);
            HAGroupStoreRecord standbyRecord = new HAGroupStoreRecord(HAGroupStoreRecord.DEFAULT_PROTOCOL_VERSION, haGroupName, STANDBY.getDefaultHAGroupState(), HAGroupStoreRecord.DEFAULT_RECORD_VERSION);

            upsertGroupRecordInBothSystemTable(haGroupName, ACTIVE, STANDBY, HAGroupStoreRecord.DEFAULT_RECORD_VERSION, HAGroupStoreRecord.DEFAULT_RECORD_VERSION, null, policy);
            addOrUpdateRoleRecordToClusters(haGroupName, activeRecord, standbyRecord);
        }

        public void initClusterRoleRecordFor1Cluster(String haGroupName, HighAvailabilityPolicy policy) throws Exception {
            HAGroupStoreRecord activeRecord = new HAGroupStoreRecord(HAGroupStoreRecord.DEFAULT_PROTOCOL_VERSION, haGroupName, ACTIVE.getDefaultHAGroupState(), HAGroupStoreRecord.DEFAULT_RECORD_VERSION);
            upsertGroupRecordInASystemTable(haGroupName, ACTIVE, STANDBY, HAGroupStoreRecord.DEFAULT_RECORD_VERSION, HAGroupStoreRecord.DEFAULT_RECORD_VERSION, null, policy, 1);
            addOrUpdateRoleRecordToClusters(haGroupName, activeRecord, null);
        }

        private void addOrUpdateRoleRecordToClusters(String haGroupName, HAGroupStoreRecord activeRecord, HAGroupStoreRecord standbyRecord) throws Exception {
            String path = PhoenixHAAdmin.toPath(haGroupName);
            int failures = 0;
            if (activeRecord != null) {
                do {
                    try {
                        if (haAdmin1.getCurator().checkExists().forPath(path) == null) {
                            haAdmin1.createHAGroupStoreRecordInZooKeeper(activeRecord);
                        } else {
                            final Pair<HAGroupStoreRecord, Stat> currentRecord = haAdmin1.getHAGroupStoreRecordInZooKeeper(haGroupName);
                            if (currentRecord.getRight() != null && currentRecord.getLeft() != null) {
                                haAdmin1.updateHAGroupStoreRecordInZooKeeper(haGroupName, activeRecord, currentRecord.getRight().getVersion());
                            } else {
                                throw new IOException("Current HAGroupStoreRecord in ZK is null, cannot update HAGroupStoreRecord " + haGroupName);
                            }
                        }
                    } catch (Exception e) {
                        failures++;
                    }
                } while (failures > 0 && failures < 4);
            }
            failures = 0;
            if (standbyRecord != null) {
                do {
                    try {
                        if (haAdmin2.getCurator().checkExists().forPath(path) == null) {
                            haAdmin2.createHAGroupStoreRecordInZooKeeper(standbyRecord);
                        } else {
                            final Pair<HAGroupStoreRecord, Stat> currentRecord = haAdmin2.getHAGroupStoreRecordInZooKeeper(haGroupName);
                            if (currentRecord.getRight() != null && currentRecord.getLeft() != null) {
                                haAdmin2.updateHAGroupStoreRecordInZooKeeper(haGroupName, standbyRecord, currentRecord.getRight().getVersion());
                            } else {
                                throw new IOException("Current HAGroupStoreRecord in ZK is null, cannot update HAGroupStoreRecord " + haGroupName);
                            }
                        }
                    } catch (Exception e) {
                        failures++;
                        Thread.sleep(200);
                    }
                } while (failures > 0 && failures < 4);
            }
            
        }

        private void upsertGroupRecordInBothSystemTable(String haGroupName, ClusterRole role1, ClusterRole role2, long version1, long version2, String overrideZKUrl, HighAvailabilityPolicy policy) throws Exception {
            HAGroupStoreTestUtil.upsertHAGroupRecordInSystemTable(haGroupName, getZkUrl1(), getZkUrl2(), getMasterAddress1(), getMasterAddress2(), role1, role2, version1, version2, overrideZKUrl, policy, getHATestProperties());
            HAGroupStoreTestUtil.upsertHAGroupRecordInSystemTable(haGroupName, getZkUrl2(), getZkUrl1(), getMasterAddress2(), getMasterAddress1(), role2, role1, version2, version1, overrideZKUrl, policy, getHATestProperties());
        }

        private void upsertGroupRecordInASystemTable(String haGroupName, ClusterRole role1, ClusterRole role2, long version1, long version2, String overrideZKUrl, HighAvailabilityPolicy policy, int clusterIndex) throws Exception {
            if (clusterIndex == 1) {
                HAGroupStoreTestUtil.upsertHAGroupRecordInSystemTable(haGroupName, getZkUrl1(), getZkUrl2(), getMasterAddress1(), getMasterAddress2(), role1, role2, version1, version2, overrideZKUrl, policy, getHATestProperties());
            } else {
                HAGroupStoreTestUtil.upsertHAGroupRecordInSystemTable(haGroupName, getZkUrl2(), getZkUrl1(), getMasterAddress2(), getMasterAddress1(), role2, role1, version2, version1, overrideZKUrl, policy, getHATestProperties());
            }
            
        }

        private void refreshSystemTableInBothClusters(String haGroupName, ClusterRole role1, ClusterRole role2, long version1, long version2, String overrideZKUrl, HighAvailabilityPolicy policy) throws Exception {
            HAGroupStoreTestUtil.deleteHAGroupRecordInSystemTable(haGroupName, getZkUrl1());
            HAGroupStoreTestUtil.deleteHAGroupRecordInSystemTable(haGroupName, getZkUrl2());
            upsertGroupRecordInBothSystemTable(haGroupName, role1, role2, version1, version2, overrideZKUrl, policy);
        }

        private void refreshSystemTableInOneCluster(String haGroupName, ClusterRole role1, ClusterRole role2, long version1, long version2, String overrideZKUrl, HighAvailabilityPolicy policy, int clusterIndex) throws Exception {
            String zkUrl = clusterIndex == 1 ? getZkUrl1() : getZkUrl2();
            HAGroupStoreTestUtil.deleteHAGroupRecordInSystemTable(haGroupName, zkUrl);
            upsertGroupRecordInASystemTable(haGroupName, role1, role2, version1, version2, overrideZKUrl, policy, clusterIndex);
        }

        public void doUpdatesMissedWhenClusterWasDown(HighAvailabilityGroup haGroup, ClusterRole role1, ClusterRole role2, int clusterIndex) throws Exception {
            String haGroupName = haGroup.getGroupInfo().getName();
            final Pair<HAGroupStoreRecord, Stat> currentRecord1 = getHaAdmin1().getHAGroupStoreRecordInZooKeeper(haGroupName);
            final Pair<HAGroupStoreRecord, Stat> currentRecord2 = getHaAdmin2().getHAGroupStoreRecordInZooKeeper(haGroupName);
            long newVersion;
            HAGroupStoreRecord newRecord;
            if (clusterIndex == 1) {
                newVersion = currentRecord1.getLeft().getRecordVersion() + 1;
                newRecord = new HAGroupStoreRecord(currentRecord1.getLeft().getProtocolVersion(), haGroupName, role1.getDefaultHAGroupState(), newVersion);
                refreshSystemTableInOneCluster(haGroupName, role1, role2, newVersion, currentRecord2.getLeft().getRecordVersion(), null, haGroup.getRoleRecord().getPolicy(), clusterIndex);
                addOrUpdateRoleRecordToClusters(haGroupName, newRecord,null);
                try{
                    HAGroupStoreManager.getInstance(hbaseCluster1.getConfiguration()).invalidateHAGroupStoreClient(haGroupName, true);
                } catch (Exception e) {
                    LOG.warn("Fail to invalidate HAGroupStoreClient for {} because {}", haGroupName, e.getMessage());
                }
            } else {
                newVersion = currentRecord2.getLeft().getRecordVersion() + 1;
                newRecord = new HAGroupStoreRecord(currentRecord2.getLeft().getProtocolVersion(), haGroupName, role2.getDefaultHAGroupState(), newVersion);
                refreshSystemTableInOneCluster(haGroupName, role1, role2, currentRecord1.getLeft().getRecordVersion(), newVersion, null, haGroup.getRoleRecord().getPolicy(), clusterIndex);
                addOrUpdateRoleRecordToClusters(haGroupName, null, newRecord);
                try{
                    HAGroupStoreManager.getInstance(hbaseCluster2.getConfiguration()).invalidateHAGroupStoreClient(haGroupName, true);
                } catch (Exception e) {
                    LOG.warn("Fail to invalidate HAGroupStoreClient for {} because {}", haGroupName, e.getMessage());
                }
            }
        }

        public void deleteHAGroupRecordFromBothClusters(String haGroupName) throws Exception {
            haAdmin1.getCurator().delete().quietly().forPath(toPath(haGroupName));
            haAdmin2.getCurator().delete().quietly().forPath(toPath(haGroupName));
            HAGroupStoreTestUtil.deleteHAGroupRecordInSystemTable(haGroupName, getZkUrl1());
            HAGroupStoreTestUtil.deleteHAGroupRecordInSystemTable(haGroupName, getZkUrl2());
        }

        public void rebuildBothClustersGroupStoreClient(String haGroupName) {
            try{
                HAGroupStoreManager.getInstance(hbaseCluster1.getConfiguration()).invalidateHAGroupStoreClient(haGroupName, true);
                HAGroupStoreManager.getInstance(hbaseCluster2.getConfiguration()).invalidateHAGroupStoreClient(haGroupName, true);
            } catch (Exception e) {
                LOG.warn("Fail to invalidate HAGroupStoreClient for {} because {}", haGroupName, e.getMessage());
            }
        }


        private ClusterRoleRecord generateCRRFromHAGroupStoreRecord(HighAvailabilityGroup haGroup, HAGroupStoreRecord activeRecord, HAGroupStoreRecord standbyRecord) {
            return new ClusterRoleRecord(
                    haGroup.getGroupInfo().getName(), haGroup.getRoleRecord().getPolicy(),
                    getURL(1, haGroup.getRoleRecord().getRegistryType()), activeRecord.getHAGroupState().getClusterRole(),
                    getURL(2, haGroup.getRoleRecord().getRegistryType()), standbyRecord.getHAGroupState().getClusterRole(),
                    combineCanonicalVersions(activeRecord.getRecordVersion(), standbyRecord.getRecordVersion())); // always use a newer version
        }

        /**
         * Combine two versions into a single long value to be used for comparision and sending back to client
         * @param version1 HAGroupRecord version for cluster 1  
         * @param version2 HAGroupRecord version for cluster 2
         * @return combined version
         */
        public long combineCanonicalVersions(long version1, long version2) {
            LOG.info("Combining versions: {} and {} with master addresses: {} and {}", version1, version2, masterAddress1, masterAddress2);
            if (masterAddress1.compareTo(masterAddress2) <= 0) {
                return (version2 << 32) | version1;
            } else {
                return (version1 << 32) | version2;
            }
        }

        public void transitClusterRole(HighAvailabilityGroup haGroup, ClusterRole role1,
                                       ClusterRole role2) throws Exception {
            transitClusterRole(haGroup, role1, role2, true);
        }

        /**
         * Set cluster roles for an HA group and wait the cluster role transition to happen.
         *
         * @param haGroup the HA group name
         * @param role1 cluster role for the first cluster in the group
         * @param role2 cluster role for the second cluster in the group
         */
        public void transitClusterRole(HighAvailabilityGroup haGroup, ClusterRole role1,
                ClusterRole role2, boolean doRefreshHAGroup) throws Exception {
            String haGroupName = haGroup.getGroupInfo().getName();
            final Pair<HAGroupStoreRecord, Stat> currentRecord1 = haAdmin1.getHAGroupStoreRecordInZooKeeper(haGroupName);
            final Pair<HAGroupStoreRecord, Stat> currentRecord2 = haAdmin2.getHAGroupStoreRecordInZooKeeper(haGroupName);
            long newVersion1 = currentRecord1.getLeft().getRecordVersion() + 1;
            long newVersion2 = currentRecord2.getLeft().getRecordVersion() + 1;

            HAGroupStoreRecord record1 = new HAGroupStoreRecord(currentRecord1.getLeft().getProtocolVersion(), haGroupName, role1.getDefaultHAGroupState(), newVersion1);
            HAGroupStoreRecord record2 = new HAGroupStoreRecord(currentRecord2.getLeft().getProtocolVersion(), haGroupName, role2.getDefaultHAGroupState(), newVersion2);

            refreshSystemTableInBothClusters(haGroupName, role1, role2, newVersion1, newVersion2, null, haGroup.getRoleRecord().getPolicy());
            addOrUpdateRoleRecordToClusters(haGroupName, record1, record2);
            try{
                HAGroupStoreManager.getInstance(hbaseCluster1.getConfiguration()).invalidateHAGroupStoreClient(haGroupName, true);
                HAGroupStoreManager.getInstance(hbaseCluster2.getConfiguration()).invalidateHAGroupStoreClient(haGroupName, true);
            } catch (Exception e) {
                LOG.warn("Fail to invalidate HAGroupStoreClient for {} because {}", haGroupName, e.getMessage());
            }

            if (doRefreshHAGroup) {
                haGroup.refreshClusterRoleRecord();

                // wait for the cluster roles are populated into client side from RegionServer endpoints.
                ClusterRoleRecord newRoleRecord = generateCRRFromHAGroupStoreRecord(haGroup, record1, record2);
                LOG.info("New role record to check: {}", newRoleRecord);
                waitFor(() -> newRoleRecord.equals(haGroup.getRoleRecord()), 1000, 10_000);
            }

        }

        public void transitClusterRoleWithCluster1Down(HighAvailabilityGroup haGroup, ClusterRole role1,
                ClusterRole role2) throws Exception {
            String haGroupName = haGroup.getGroupInfo().getName();
            final Pair<HAGroupStoreRecord, Stat> currentRecord2 = haAdmin2.getHAGroupStoreRecordInZooKeeper(haGroupName);
            long newVersion2 = currentRecord2.getLeft().getRecordVersion() + 1;

            HAGroupStoreRecord record2 = new HAGroupStoreRecord(currentRecord2.getLeft().getProtocolVersion(), haGroupName, role2.getDefaultHAGroupState(), newVersion2);

            refreshSystemTableInOneCluster(haGroupName, role1, role2, 1, newVersion2, null, haGroup.getRoleRecord().getPolicy(), 2);
            addOrUpdateRoleRecordToClusters(haGroupName, null, record2);
            try{
            HAGroupStoreManager.getInstance(hbaseCluster2.getConfiguration()).invalidateHAGroupStoreClient(haGroupName, true);
            } catch (Exception e) {
                LOG.warn("Fail to invalidate HAGroupStoreClient for {} because {}", haGroupName, e.getMessage());
            }
            haGroup.refreshClusterRoleRecord();

            //If cluster 1 is down, server won't be able to reach peer for states and will get version passed in refreshSystemTableInOneCluster and OFFLINE role.
            HAGroupStoreRecord record1 = new HAGroupStoreRecord(HAGroupStoreRecord.DEFAULT_PROTOCOL_VERSION, haGroupName, UNKNOWN.getDefaultHAGroupState(), HAGroupStoreRecord.DEFAULT_RECORD_VERSION);

            // wait for the cluster roles are populated into client side from RegionServer endpoints.
            ClusterRoleRecord newRoleRecord = generateCRRFromHAGroupStoreRecord(haGroup, record1, record2);
            LOG.info("New role record to check: {}", newRoleRecord);
            waitFor(() -> newRoleRecord.equals(haGroup.getRoleRecord()), 1000, 10_000);
        }

        public void transitClusterRoleWithCluster2Down(HighAvailabilityGroup haGroup, ClusterRole role1,
                ClusterRole role2) throws Exception {
            String haGroupName = haGroup.getGroupInfo().getName();
            final Pair<HAGroupStoreRecord, Stat> currentRecord1 = haAdmin1.getHAGroupStoreRecordInZooKeeper(haGroupName);
            long newVersion1 = currentRecord1.getLeft().getRecordVersion() + 1;

            HAGroupStoreRecord record1 = new HAGroupStoreRecord(currentRecord1.getLeft().getProtocolVersion(), haGroupName, role1.getDefaultHAGroupState(), newVersion1);

            refreshSystemTableInOneCluster(haGroupName, role1, role2, newVersion1, 1, null, haGroup.getRoleRecord().getPolicy(), 1);
            addOrUpdateRoleRecordToClusters(haGroupName, record1, null);
            try{
                HAGroupStoreManager.getInstance(hbaseCluster1.getConfiguration()).invalidateHAGroupStoreClient(haGroupName, true);
            } catch (Exception e) {
                LOG.warn("Fail to invalidate HAGroupStoreClient for {} because {}", haGroupName, e.getMessage());
            }
            haGroup.refreshClusterRoleRecord();

            //If cluster 2 is down, server won't be able to reach peer for states and will get version passed in refreshSystemTableInOneCluster and OFFLINE role.
            HAGroupStoreRecord record2 = new HAGroupStoreRecord(HAGroupStoreRecord.DEFAULT_PROTOCOL_VERSION, haGroupName, UNKNOWN.getDefaultHAGroupState(), HAGroupStoreRecord.DEFAULT_RECORD_VERSION);

            // wait for the cluster roles are populated into client side from RegionServer endpoints.
            ClusterRoleRecord newRoleRecord = generateCRRFromHAGroupStoreRecord(haGroup, record1, record2);
            LOG.info("New role record to check: {}", newRoleRecord);
            waitFor(() -> newRoleRecord.equals(haGroup.getRoleRecord()), 1000, 10_000);
        }

        public void refreshClusterRoleRecordAfterClusterRestart(HighAvailabilityGroup haGroup,
                                                ClusterRole role1, ClusterRole role2, int clusterIndex) throws Exception {
            String haGroupName = haGroup.getGroupInfo().getName();
            long newVersion1, newVersion2;
            //TODO:- Inducing version change, but how would we know that a URL is changed
            //as we don't store url in ZNodes so it doesn't increase version for that change
            if (clusterIndex == 1) {
                newVersion1 = 1L;
                newVersion2 = 2L;
            } else {
                newVersion1 = 2L;
                newVersion2 = 1L;
            }
            HAGroupStoreRecord record1 = new HAGroupStoreRecord(HAGroupStoreRecord.DEFAULT_PROTOCOL_VERSION, haGroupName, role1.getDefaultHAGroupState(), newVersion1);
            HAGroupStoreRecord record2 = new HAGroupStoreRecord(HAGroupStoreRecord.DEFAULT_PROTOCOL_VERSION, haGroupName, role2.getDefaultHAGroupState(), newVersion2);

            refreshSystemTableInBothClusters(haGroupName, role1, role2, newVersion1, newVersion2, null, haGroup.getRoleRecord().getPolicy());
            addOrUpdateRoleRecordToClusters(haGroupName, record1, record2);

            try{
                HAGroupStoreManager.getInstance(hbaseCluster1.getConfiguration()).invalidateHAGroupStoreClient(haGroupName, true);
                HAGroupStoreManager.getInstance(hbaseCluster2.getConfiguration()).invalidateHAGroupStoreClient(haGroupName, true);
            } catch (Exception e) {
                LOG.warn("Fail to invalidate HAGroupStoreClient for {} because {}", haGroupName, e.getMessage());
            }

            haGroup.refreshClusterRoleRecord();

            // wait for the cluster roles are populated into client side from RegionServer endpoints.
            ClusterRoleRecord newRoleRecord = generateCRRFromHAGroupStoreRecord(haGroup, record1, record2);
            LOG.info("New role record to check: {}", newRoleRecord);
            waitFor(() -> newRoleRecord.equals(haGroup.getRoleRecord()), 1000, 10_000);
        }

        /**
         * Log the state of both clusters
         */
        public void logClustersStates() {
            String cluster1Status, cluster2Status;
            try {
                cluster1Status = admin1.getClusterMetrics().toString();
            } catch (IOException e) {
                cluster1Status = "Unable to get cluster status.";
            }
            try {
                cluster2Status = admin2.getClusterMetrics().toString();
            } catch (IOException e){
                cluster2Status = "Unable to get cluster status.";
            }
            LOG.info("Cluster Status [\n{},\n{}\n]", cluster1Status, cluster2Status);
        }

        /**
         * @return testing utility for cluster 1
         */
        public HBaseTestingUtility getHBaseCluster1() {
            return hbaseCluster1;
        }

        /**
         * @return testing utility for cluster 2
         */
        public HBaseTestingUtility getHBaseCluster2() {
            return hbaseCluster2;
        }

        /**
         * Returns a Phoenix Connection to a cluster
         * @param clusterIndex 1 based
         * @return a Phoenix Connection to the indexed cluster
         */
        public Connection getClusterConnection(int clusterIndex, HighAvailabilityGroup haGroup) throws SQLException {
            Properties props = new Properties();
            String url = getJdbcUrl(haGroup, getURL(clusterIndex, haGroup.getRoleRecord().getRegistryType()));
            return DriverManager.getConnection(url, props);
        }

        /**
         * Returns a Phoenix Connection to cluster 1
         * @return a Phoenix Connection to the cluster
         */
        public Connection getCluster1Connection(HighAvailabilityGroup haGroup) throws SQLException {
            return getClusterConnection(1, haGroup);
        }

        /**
         * Returns a Phoenix Connection to cluster 2
         * @return a Phoenix Connection to the cluster
         */
        public Connection getCluster2Connection(HighAvailabilityGroup haGroup) throws SQLException {
            return getClusterConnection(2, haGroup);
        }

        //TODO: Replace with a real check for replication complete
        /**
         * Checks/waits with timeout if replication is done
         * @return true if replication is done else false
         */
        public boolean checkReplicationComplete() {
            try {
                //Can replace with a real check for replication complete in the future
                Thread.sleep(5000);
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
            return true;
        }

        /** A Testable interface that can throw checked exception. */
        @FunctionalInterface
        public interface Testable {
            void    test() throws Exception;
        }

        /**
         * Shutdown mini HBase cluster, do the test, and restart the HBase cluster.
         *
         * Please note the ZK cluster and DFS cluster are untouched.
         *
         * @param cluster the HBase cluster facility whose HBase cluster to restart
         * @param testable testing logic that is runnable
         * @throws Exception if fails to stop, test or restart HBase cluster
         */
        public void doTestWhenOneHBaseDown(HBaseTestingUtility cluster, Testable testable)
                throws Exception {
            final int zkClientPort = cluster.getZkCluster().getClientPort();
            final int masterPort = getGivenClusterMasterPort(cluster);
            try {
                LOG.info("Shutting down HBase cluster using ZK localhost:{}", zkClientPort);
                cluster.shutdownMiniHBaseCluster();
                LOG.info("Start testing when HBase is down using ZK localhost:{} and master address localhost:{}", zkClientPort, masterPort);
                testable.test();
                LOG.info("Test succeeded when HBase is down using ZK localhost:{}", zkClientPort);
            } finally {
                LOG.info("Finished testing when HBase is down using ZK localhost:{}", zkClientPort);
                resetPortsForRestart(cluster, masterPort);
                cluster.startMiniHBaseCluster(StartMiniClusterOption.builder().numMasters(1).numRegionServers(1).build());
                LOG.info("Master after restart :- {}, port before restart {} and masters before restart {} {}}", 
                    cluster.getConfiguration().get(HConstants.MASTER_ADDRS_KEY).replaceAll(":", "\\\\:"), 
                    masterPort, masterAddress1, masterAddress2);
                LOG.info("Restarted HBase cluster using ZK localhost:{}", zkClientPort);
            }
        }

        /**
         * Shutdown mini ZK and HBase cluster, do the test, and restart both HBase and ZK cluster.
         *
         * @param cluster the HBase cluster facility that has mini ZK, DFS and HBase cluster
         * @param testable testing logic that is runnable
         * @throws Exception if fails to stop, test or restart
         */
        public void doTestWhenOneZKDown(HBaseTestingUtility cluster, Testable testable)
                throws Exception {
            final int zkClientPort = cluster.getZkCluster().getClientPort();
            final int masterPort = getGivenClusterMasterPort(cluster);
            try {
                LOG.info("Shutting down HBase cluster using ZK localhost:{}", zkClientPort);
                cluster.shutdownMiniHBaseCluster();
                LOG.info("Shutting down ZK cluster at localhost:{}", zkClientPort);
                cluster.shutdownMiniZKCluster();
                LOG.info("Start testing when ZK & HBase is down at localhost:{} and master address localhost:{}", zkClientPort, masterPort);
                testable.test();
                LOG.info("Test succeeded when ZK & HBase is down at localhost:{}", zkClientPort);
            } finally {
                resetPortsForRestart(cluster, masterPort);
                LOG.info("Finished testing when ZK & HBase is down at localhost:{} and master address localhost:{}", zkClientPort, masterPort);
               cluster.startMiniZKCluster(1, zkClientPort);
                LOG.info("Restarted ZK cluster at localhost:{}", zkClientPort);
               cluster.startMiniHBaseCluster(StartMiniClusterOption.builder().numMasters(1).numRegionServers(1).build());
                LOG.info("Master after restart :- {}, port before restart {} and masters before restart {} {}}",
                    cluster.getConfiguration().get(HConstants.MASTER_ADDRS_KEY).replaceAll(":", "\\\\:"), 
                    masterPort, masterAddress1, masterAddress2);
                LOG.info("Restarted HBase cluster using ZK localhost:{}", zkClientPort);
            }
        }

        private void resetPortsForRestart(HBaseTestingUtility cluster, int masterPort) {
            //Disable random port assignment and set the master port to the given port
            cluster.getConfiguration().set("hbase.localcluster.assign.random.ports", "false");
            cluster.getConfiguration().set("hbase.master.port", String.valueOf(masterPort));

            //Set Random ports to everthing else for HBase cluster
            cluster.getConfiguration().set("hbase.master.info.port", String.valueOf(0));
            cluster.getConfiguration().set("hbase.regionserver.port", String.valueOf(0));
            cluster.getConfiguration().set("hbase.regionserver.info.port", String.valueOf(0));
        }

        private void resetPortsToRandomForNewRestart(HBaseTestingUtility cluster) {
            cluster.getConfiguration().set("hbase.localcluster.assign.random.ports", "true");
            cluster.getConfiguration().set("hbase.master.port", String.valueOf(0));
            cluster.getConfiguration().set("hbase.master.info.port", String.valueOf(0));
            cluster.getConfiguration().set("hbase.regionserver.port", String.valueOf(0));
            cluster.getConfiguration().set("hbase.regionserver.info.port", String.valueOf(0));
        }

        public int getGivenClusterMasterPort(HBaseTestingUtility cluster) {
            if (cluster == hbaseCluster1) {
                return Integer.parseInt(masterAddress1.split(":")[1]);
            } else {
                return Integer.parseInt(masterAddress2.split(":")[1]);
            }
        }




        /**
         * The JDBC connection url for the given clusters in particular format.
         * Note that HAUrl will contain ZK as host which is used to create HA Connection
         * from client, although wrapped connections can be using master url based on the
         * registry Type set in roleRecord.
         *
         * below methods will return urls based on registryType and HAUrl will be zk based
         */
        public String getJdbcHAUrl() {
            return getJdbcHAUrl(PRINCIPAL);
        }

        public String getJdbcHAUrl(String principal) {
            return String.format("%s:[%s|%s]:%s", JDBC_PROTOCOL_RPC, getMasterAddress1(), getMasterAddress2(), principal);
        }

        public String getJdbcHAUrlWithoutPrincipal() {
            return String.format("%s:[%s|%s]", JDBC_PROTOCOL_RPC, getMasterAddress1(), getMasterAddress2());
        }


        public String getJdbcUrl1(HighAvailabilityGroup haGroup) {
            return getJdbcUrl(haGroup, getURL(1, haGroup.getRoleRecord().getRegistryType()));
        }

        public String getJdbcUrl1(HighAvailabilityGroup haGroup, String principal) {
            return getJdbcUrl(haGroup, getURL(1, haGroup.getRoleRecord().getRegistryType()), principal);
        }

        public String getJdbcUrl2(HighAvailabilityGroup haGroup) {
            return getJdbcUrl(haGroup, getURL(2, haGroup.getRoleRecord().getRegistryType()));
        }

        public String getJdbcUrl2(HighAvailabilityGroup haGroup, String principal) {
            return getJdbcUrl(haGroup, getURL(2, haGroup.getRoleRecord().getRegistryType()), principal);
        }

        public String getJdbcUrl(HighAvailabilityGroup haGroup, String url) {
            String interimUrl = getUrlWithoutPrincipal(haGroup, url);
            return String.format("%s:%s",interimUrl, PRINCIPAL);
        }

        public String getJdbcUrl(HighAvailabilityGroup haGroup, String url, String principal) {
            String interimUrl = getUrlWithoutPrincipal(haGroup, url);
            return String.format("%s:%s",interimUrl, principal);
        }

        public String getJdbcUrl(String url) {
            return String.format("jdbc:phoenix+rpc:%s:::%s", url, PRINCIPAL);
        }

        public String getJdbcUrlWithoutPrincipal(HighAvailabilityGroup haGroup, String url) {
            String interimUrl = getUrlWithoutPrincipal(haGroup, url);
            if (interimUrl.endsWith("::")) {
                interimUrl = interimUrl.substring(0, interimUrl.length() - 2);
            }
            return interimUrl;
        }

        public String getZkUrl1() {
            return zkUrl1;
        }

        public String getZkUrl2() {
            return zkUrl2;
        }

        public String getMasterAddress1() {
            return masterAddress1;
        }

        public String getMasterAddress2() {
            return masterAddress2;
        }

        public PhoenixHAAdmin getHaAdmin1() {
            return haAdmin1;
        }

        public PhoenixHAAdmin getHaAdmin2() {
            return haAdmin2;
        }

        private String getUrlWithoutPrincipal(HighAvailabilityGroup haGroup, String url) {
            StringBuilder sb = new StringBuilder();
            switch (haGroup.getRoleRecord().getRegistryType()) {
                case ZK:
                    return sb.append(JDBC_PROTOCOL_ZK).append(":").append(url).toString();
                case MASTER:
                    return sb.append(JDBC_PROTOCOL_MASTER).append(":").append(url).append("::").toString();
                case RPC:
                default:
                    return sb.append(JDBC_PROTOCOL_RPC).append(":").append(url).append("::").toString();
            }
        }

        /**
         * @return a ZK client by curator framework for the cluster 1.
         */
        public CuratorFramework createCurator1() throws IOException {
            Properties properties = new Properties();
            getHBaseCluster1().getConfiguration()
                    .iterator()
                    .forEachRemaining(k -> properties.setProperty(k.getKey(), k.getValue()));
            return HighAvailabilityGroup.getCurator(getZkUrl1(), properties);
        }

        /**
         * @return a ZK client by curator framework for the cluster 2.
         */
        public CuratorFramework createCurator2() throws IOException {
            Properties properties = new Properties();
            getHBaseCluster2().getConfiguration()
                    .iterator()
                    .forEachRemaining(k -> properties.setProperty(k.getKey(), k.getValue()));
            return HighAvailabilityGroup.getCurator(getZkUrl2(), properties);
        }

        /**
         * Create table on two clusters and enable replication.
         *
         * @param tableName the table name
         * @throws SQLException if error happens
         */
        public void createTableOnClusterPair(HighAvailabilityGroup haGroup, String tableName) throws SQLException {
            createTableOnClusterPair(haGroup, tableName, true);
        }

        /**
         * Create table on two clusters.
         *
         * If the replication scope is true then enable replication for this table.
         *
         * @param tableName the table name
         * @param replicationScope the table replication scope true=1 and false=0
         * @throws SQLException if error happens
         */
        public void createTableOnClusterPair(HighAvailabilityGroup haGroup, String tableName, boolean replicationScope)
                throws SQLException {
            for (String url : Arrays.asList(getURL(1, haGroup.getRoleRecord().getRegistryType()),
                    getURL(2, haGroup.getRoleRecord().getRegistryType()))) {
                String jdbcUrl = getJdbcUrl(haGroup, url);
                try (Connection conn = DriverManager.getConnection(jdbcUrl, new Properties())) {
                    conn.createStatement().execute(String.format(
                            "CREATE TABLE IF NOT EXISTS %s (\n"
                                    + "id INTEGER PRIMARY KEY,\n"
                                    + "v INTEGER\n"
                                    + ") REPLICATION_SCOPE=%d",
                            tableName, replicationScope ? 1 : 0));
                    conn.createStatement().execute(
                            String.format("CREATE LOCAL INDEX IF NOT EXISTS IDX_%s ON %s(v)",
                                    tableName, tableName));
                    conn.commit();
                    /*
                    This is being called to clear the metadata coprocessor global cache singleton
                    which otherwise short circuits the table creation on the 2nd call
                    As the 2 region servers share a jvm
                     */
                    ((PhoenixConnection) conn).getQueryServices().clearCache();
                }
            }
            LOG.info("Created table {} on cluster pair {}", tableName, this);
        }

        /**
         * Create multi-tenant table and view with randomly generated table name.
         *
         * @param tableName the table name
         * @throws SQLException if error happens
         */
        public void createTenantSpecificTable(HighAvailabilityGroup haGroup, String tableName) throws SQLException {
            for (String url : Arrays.asList(getURL(1, haGroup.getRoleRecord().getRegistryType()),
                    getURL(2, haGroup.getRoleRecord().getRegistryType()))) {
                String jdbcUrl = getJdbcUrl(haGroup, url);
                try (Connection conn = DriverManager.getConnection(jdbcUrl, new Properties())) {
                    conn.createStatement().execute(String.format(
                            "CREATE TABLE IF NOT EXISTS %s (\n"
                                    + "tenant_id VARCHAR NOT NULL,\n"
                                    + "id INTEGER NOT NULL,\n"
                                    + "v INTEGER\n"
                                    + "CONSTRAINT pk PRIMARY KEY (tenant_id, id)"
                                    + ") REPLICATION_SCOPE=1, MULTI_TENANT=true",
                            tableName));
                    conn.commit();
                    /*
                    This is being called to clear the metadata coprocessor global cache singleton
                    which otherwise short circuits the table creation on the 2nd call
                    As the 2 region servers share a jvm
                     */
                    ((PhoenixConnection) conn).getQueryServices().clearCache();
                }
            }
            LOG.info("Created multi-tenant table {} on cluster pair {}", tableName, this);
        }

        public void restartCluster1() throws Exception {
            try {
                hbaseCluster1.shutdownMiniHBaseCluster();
                hbaseCluster1.shutdownMiniZKCluster();
            } finally {
                resetPortsToRandomForNewRestart(hbaseCluster1);
                hbaseCluster1.startMiniZKCluster(1);
                hbaseCluster1.startMiniHBaseCluster(StartMiniClusterOption.builder().numMasters(1).numRegionServers(1).build());

                String confAddress = hbaseCluster1.getConfiguration().get(HConstants.ZOOKEEPER_QUORUM);

                zkUrl1 = String.format("%s\\:%d::/hbase", confAddress, hbaseCluster1.getZkCluster().getClientPort());

                masterAddress1 = hbaseCluster1.getConfiguration().get(HConstants.MASTER_ADDRS_KEY).replaceAll(":", "\\\\:");

                haAdmin1 = new PhoenixHAAdmin(getZkUrl1(), hbaseCluster1.getConfiguration(), HighAvailibilityCuratorProvider.INSTANCE);

                admin1 = hbaseCluster1.getConnection().getAdmin();
            }

        }

        public void restartCluster2() throws Exception {
            String oldMasters = getMasterAddress1() + "  " + getMasterAddress2();
            try {
                hbaseCluster2.shutdownMiniHBaseCluster();
                hbaseCluster2.shutdownMiniZKCluster();
            } finally {
                resetPortsToRandomForNewRestart(hbaseCluster2);
                hbaseCluster2.startMiniZKCluster(1);
                hbaseCluster2.startMiniHBaseCluster(StartMiniClusterOption.builder().numMasters(1).numRegionServers(1).build());

                String confAddress = hbaseCluster2.getConfiguration().get(HConstants.ZOOKEEPER_QUORUM);

                zkUrl2 = String.format("%s\\:%d::/hbase", confAddress, hbaseCluster2.getZkCluster().getClientPort());

                masterAddress2 = hbaseCluster2.getConfiguration().get(HConstants.MASTER_ADDRS_KEY).replaceAll(":", "\\\\:");

                haAdmin2 = new PhoenixHAAdmin(getZkUrl2(), hbaseCluster2.getConfiguration(), HighAvailibilityCuratorProvider.INSTANCE);

                admin2 = hbaseCluster2.getConnection().getAdmin();
                LOG.info("Before restart master address is {} and after restart master address is {}", oldMasters, getMasterAddress1() + "  " + getMasterAddress2());
            }
        }

        /**
         * Shuts down the two hbase clusters.
         */
        @Override
        public void close() throws IOException {
            haAdmin1.close();
            haAdmin2.close();
            admin1.close();
            admin2.close();
            try {
                ServerMetadataCacheTestImpl.resetCache();
                hbaseCluster1.shutdownMiniCluster();
                hbaseCluster2.shutdownMiniCluster();
            } catch (Exception e) {
                LOG.error("Got exception to close HBaseTestingUtilityPair", e);
                throw new IOException(e);
            }
            LOG.info("Cluster pair {} is closed successfully.", this);
        }

        @Override
        public String toString() {
            return "HBaseTestingUtilityPair{" + getZkUrl1() + ", " + getZkUrl2() + "}";
        }

        /** Sets up the default HBase configuration for Phoenix HA testing. */
        private static void setUpDefaultHBaseConfig(Configuration conf) {
            // Set Phoenix HA timeout for ZK client to be a smaller number
            conf.setInt(PHOENIX_HA_ZK_CONNECTION_TIMEOUT_MS_KEY, 1000);
            conf.setInt(PHOENIX_HA_ZK_SESSION_TIMEOUT_MS_KEY, 1000);
            conf.setInt(PHOENIX_HA_ZK_RETRY_BASE_SLEEP_MS_KEY, 100);
            conf.setInt(PHOENIX_HA_ZK_RETRY_MAX_KEY, 2);
            conf.setInt(PHOENIX_HA_ZK_RETRY_MAX_SLEEP_MS_KEY, 1000);
            //This is Needed to get CRR when one cluster is down as RPC timeout is 2 sec and server
            //will wait below time to try to get Peer record info from dead cluster
            conf.setLong(PHOENIX_HA_GROUP_STORE_CLIENT_INITIALIZATION_TIMEOUT_MS, 1000L);

            // Set Phoenix related settings, eg. for index management
            conf.set(IndexManagementUtil.WAL_EDIT_CODEC_CLASS_KEY,
                    IndexManagementUtil.INDEX_WAL_EDIT_CODEC_CLASS_NAME);
            // set the server rpc scheduler factory, used to configure the cluster
            conf.set(RSRpcServices.REGION_SERVER_RPC_SCHEDULER_FACTORY_CLASS,
                    PhoenixRpcSchedulerFactory.class.getName());
            conf.setInt(NUM_CONCURRENT_INDEX_WRITER_THREADS_CONF_KEY, 1);


            conf.setLong(HConstants.ZK_SESSION_TIMEOUT, 12_000);
            conf.setLong(HConstants.ZOOKEEPER_TICK_TIME, 6_000);
            conf.setInt(HConstants.HBASE_CLIENT_RETRIES_NUMBER, 2);
            // Make HBase run faster by skipping sanity checks
            conf.setBoolean("hbase.unsafe.stream.capability.enforcefalse", false);
            conf.setBoolean("hbase.procedure.store.wal.use.hsync", false);
            conf.setBoolean("hbase.table.sanity.checks", false);
            /*
             * The default configuration of mini cluster ends up spawning a lot of threads
             * that are not really needed by phoenix for test purposes. Limiting these threads
             * helps us in running several mini clusters at the same time without hitting
             * the threads limit imposed by the OS.
             */
            conf.setInt(HConstants.REGION_SERVER_HANDLER_COUNT, 5);
            conf.setInt("hbase.regionserver.metahandler.count", 2);
            conf.setInt(HConstants.MASTER_HANDLER_COUNT, 2);
            conf.setInt("dfs.namenode.handler.count", 2);
            conf.setInt("dfs.namenode.service.handler.count", 2);
            conf.setInt("dfs.datanode.handler.count", 2);
            conf.setInt("ipc.server.read.threadpool.size", 2);
            conf.setInt("ipc.server.handler.threadpool.size", 2);
            conf.setInt("hbase.regionserver.hlog.syncer.count", 2);
            conf.setInt("hbase.hfile.compaction.discharger.interval", 5000);
            conf.setInt("hbase.hlog.asyncer.number", 2);
            conf.setInt("hbase.assignment.zkevent.workers", 5);
            conf.setInt("hbase.assignment.threads.max", 5);
            conf.setInt("hbase.catalogjanitor.interval", 5000);

            // Hadoop cluster settings to avoid failing tests
            conf.setInt(DFS_REPLICATION_KEY, 1); // we only need one replica for testing

            // Phoenix Region Server Endpoint needed for metadata caching
            conf.set(REGIONSERVER_COPROCESSOR_CONF_KEY,
                        PhoenixRegionServerEndpointTestImpl.class.getName());
            conf.set(INDEX_COMMITTER_CONF_KEY,
                    TestTrackingParallelWriterIndexCommitter.class.getName());

        }
    }

    /**
     * This tests that basic operation using a connection works.
     *
     * @param conn the JDBC connection to test; could be a tenant specific connection
     * @param tableName the table name to test
     * @param haGroupName the HA Group name for this test
     * @throws SQLException if it fails to test
     */
    public static void doTestBasicOperationsWithConnection(Connection conn, String tableName, String haGroupName)
            throws SQLException {
        try (Statement stmt = conn.createStatement()) {
            assertNotNull(conn.getClientInfo());
            assertEquals(haGroupName, conn.getClientInfo(PHOENIX_HA_GROUP_ATTR));
            doTestBasicOperationsWithStatement(conn, stmt, tableName);
        }
    }

    /**
     * This tests that basic operation using a Statement works.
     *
     * @param conn the JDBC connection from which the statement was created
     * @param stmt the JDBC statement to test
     * @param tableName the table name to test
     * @throws SQLException if it fails to test
     */
    public static void doTestBasicOperationsWithStatement(Connection conn, Statement stmt,
            String tableName) throws SQLException {
        int id = RandomUtils.nextInt();
        stmt.executeUpdate(String.format("UPSERT INTO %s VALUES(%d, 1984)", tableName, id));
        conn.commit();

        try (ResultSet rs = conn.createStatement().executeQuery(
                String.format("SELECT v FROM %s WHERE id = %d", tableName, id))) {
            assertTrue(rs.next());
            assertEquals(1984, rs.getInt(1));
        }
    }

    /**
     * Wrapper for HighAvailibilityGroup::get with some retries built in for timing issues on establishing connection
     * This helper method will wait up to 3 minutes to retrieve the HA Group
     */
    public static HighAvailabilityGroup getHighAvailibilityGroup(String jdbcUrl, Properties clientProperties) throws TimeoutException, InterruptedException {
        AtomicReference<HighAvailabilityGroup> haGroupRef = new AtomicReference<>();
        GenericTestUtils.waitFor(() -> {
            try {
                Optional<HighAvailabilityGroup> haGroup = HighAvailabilityGroup.get(jdbcUrl, clientProperties);
                if (!haGroup.isPresent()) {
                    return false;
                }
                haGroupRef.set(haGroup.get());
                return true;
            } catch (SQLException throwables) {
                return false;
            }
        },1_000,180_000);
        return haGroupRef.get();
    }

    public static List<PhoenixHAExecutorServiceProvider.PhoenixHAClusterExecutorServices> getListOfSingleThreadExecutorServices() {
        return ImmutableList.of( new PhoenixHAExecutorServiceProvider.PhoenixHAClusterExecutorServices(Executors.newFixedThreadPool(1), Executors.newFixedThreadPool(1)),
                new PhoenixHAExecutorServiceProvider.PhoenixHAClusterExecutorServices(Executors.newFixedThreadPool(1), Executors.newFixedThreadPool(1)));
    }

    /**
     * Properties with tuned retry and timeout configuration to bring up and down miniclusters and connect to them
     * @return set properteis
     */
    public static Properties getHATestProperties() {
        Properties properties = new Properties();
        // Set some client configurations to make test run faster
        properties.setProperty(COLLECT_REQUEST_LEVEL_METRICS, String.valueOf(true));
        properties.setProperty(FAILOVER_TIMEOUT_MS_ATTR, "30000");
        properties.setProperty(PHOENIX_HA_ZK_RETRY_MAX_KEY, "3");
        properties.setProperty(PHOENIX_HA_ZK_RETRY_MAX_SLEEP_MS_KEY, "1000");
        properties.setProperty(ZK_SYNC_BLOCKING_TIMEOUT_MS,"1000");
        properties.setProperty(ZK_SESSION_TIMEOUT, "3000");
        properties.setProperty(PHOENIX_HA_TRANSITION_TIMEOUT_MS_KEY, "3000");
        properties.setProperty("zookeeper.recovery.retry.maxsleeptime", "1000");
        properties.setProperty("zookeeper.recovery.retry","1");
        properties.setProperty("zookeeper.recovery.retry.intervalmill","10");
        properties.setProperty(HBASE_CLIENT_RETRIES_NUMBER,"4");
        properties.setProperty(HBASE_CLIENT_PAUSE,"2000"); //bad server elapses in 2sec
        properties.setProperty(HBASE_RPC_TIMEOUT_KEY,"2000");
        properties.setProperty(HBASE_CLIENT_META_OPERATION_TIMEOUT,"2000");
        properties.setProperty(SOCKET_TIMEOUT_CONNECT,"2000");
        properties.setProperty(SOCKET_TIMEOUT_READ,"2000");
        properties.setProperty(SOCKET_TIMEOUT_WRITE,"2000");
        properties.setProperty(REPLICATION_SOURCE_SHIPEDITS_TIMEOUT,"5000");
        properties.setProperty(HConstants.THREAD_WAKE_FREQUENCY, "100");
        properties.setProperty(PHOENIX_HA_CRR_POLLER_INTERVAL_MS_KEY, "1000");
        return properties;
    }
}
