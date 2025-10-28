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

import java.io.Closeable;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArraySet;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;

import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.curator.framework.recipes.cache.ChildData;
import org.apache.curator.framework.recipes.cache.PathChildrenCache;
import org.apache.curator.framework.recipes.cache.PathChildrenCacheListener;
import org.apache.curator.utils.ZKPaths;
import org.apache.hadoop.conf.Configuration;
import org.apache.phoenix.exception.InvalidClusterRoleTransitionException;
import org.apache.phoenix.exception.StaleHAGroupStoreRecordVersionException;
import org.apache.phoenix.jdbc.ClusterRoleRecord.ClusterRole;
import org.apache.phoenix.jdbc.ClusterRoleRecord.RegistryType;
import org.apache.phoenix.jdbc.HAGroupStoreRecord.HAGroupState;
import org.apache.phoenix.query.QueryServices;
import org.apache.phoenix.query.QueryServicesOptions;
import org.apache.phoenix.thirdparty.com.google.common.annotations.VisibleForTesting;
import org.apache.phoenix.thirdparty.com.google.common.base.Preconditions;
import org.apache.phoenix.util.JDBCUtil;
import org.apache.zookeeper.data.Stat;
import org.joda.time.DateTime;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.apache.hadoop.hbase.HConstants.DEFAULT_ZK_SESSION_TIMEOUT;
import static org.apache.hadoop.hbase.HConstants.ZK_SESSION_TIMEOUT;
import static org.apache.phoenix.jdbc.PhoenixDatabaseMetaData.CLUSTER_ROLE_1;
import static org.apache.phoenix.jdbc.PhoenixDatabaseMetaData.CLUSTER_ROLE_2;
import static org.apache.phoenix.jdbc.PhoenixDatabaseMetaData.CLUSTER_URL_1;
import static org.apache.phoenix.jdbc.PhoenixDatabaseMetaData.CLUSTER_URL_2;
import static org.apache.phoenix.jdbc.PhoenixDatabaseMetaData.HA_GROUP_NAME;
import static org.apache.phoenix.jdbc.PhoenixDatabaseMetaData.POLICY;
import static org.apache.phoenix.jdbc.PhoenixDatabaseMetaData.SYSTEM_HA_GROUP_NAME;
import static org.apache.phoenix.jdbc.PhoenixDatabaseMetaData.VERSION;
import static org.apache.phoenix.jdbc.PhoenixDatabaseMetaData.ZK_URL_1;
import static org.apache.phoenix.jdbc.PhoenixDatabaseMetaData.ZK_URL_2;
import static org.apache.phoenix.jdbc.PhoenixHAAdmin.getLocalZkUrl;
import static org.apache.phoenix.jdbc.PhoenixHAAdmin.toPath;
import static org.apache.phoenix.util.PhoenixRuntime.JDBC_PROTOCOL_SEPARATOR;
import static org.apache.phoenix.util.PhoenixRuntime.JDBC_PROTOCOL_ZK;
import static org.apache.phoenix.query.QueryServices.HA_GROUP_STORE_SYNC_INTERVAL_SECONDS;
import static org.apache.phoenix.query.QueryServicesOptions.DEFAULT_HA_GROUP_STORE_SYNC_INTERVAL_SECONDS;


/**
 * Main implementation of HAGroupStoreClient with peer support.
 * Write-through cache for HAGroupStore based on {@link HAGroupStoreRecord}.
 * Uses {@link PathChildrenCache} from {@link org.apache.curator.framework.CuratorFramework}.
 */
public class HAGroupStoreClient implements Closeable {

    public static final String ZK_CONSISTENT_HA_GROUP_RECORD_NAMESPACE = "phoenix"
                    + ZKPaths.PATH_SEPARATOR + "consistentHA";
    public static final String PHOENIX_HA_GROUP_STORE_CLIENT_INITIALIZATION_TIMEOUT_MS
            = "phoenix.ha.group.store.client.initialization.timeout.ms";
    static final long DEFAULT_HA_GROUP_STORE_CLIENT_INITIALIZATION_TIMEOUT_MS
            = 30000L;
    // Multiplier for ZK session timeout to account for time it will take for HMaster to abort
    // the region server in case ZK connection is lost from the region server.
    @VisibleForTesting
    static final double ZK_SESSION_TIMEOUT_MULTIPLIER = 1.1;
    // Maximum jitter in seconds for sync job start time (10 seconds)
    private static final long SYNC_JOB_MAX_JITTER_SECONDS = 10;
    private PhoenixHAAdmin phoenixHaAdmin;
    private PhoenixHAAdmin peerPhoenixHaAdmin;
    private static final Logger LOGGER = LoggerFactory.getLogger(HAGroupStoreClient.class);
    // Map of <ZKUrl, <HAGroupName, HAGroupStoreClientInstance>>
    private static final Map<String, ConcurrentHashMap<String, HAGroupStoreClient>> instances =
            new ConcurrentHashMap<>();
    // HAGroupName for this instance
    private final String haGroupName;
    // PathChildrenCache for current cluster and HAGroupName
    private PathChildrenCache pathChildrenCache = null;
    // PathChildrenCache for peer cluster and HAGroupName
    private PathChildrenCache peerPathChildrenCache = null;
    // Whether the client is healthy
    private volatile boolean isHealthy = false;
    // Configuration
    private final Configuration conf;
    // ZK URL for the current cluster and HAGroupName
    private String zkUrl;
    // Peer Custom Event Listener
    private final PathChildrenCacheListener peerCustomPathChildrenCacheListener;
    // Wait time for sync mode
    private final long waitTimeForSyncModeInMs;
    // Rotation time for a log
    private final long rotationTimeMs;
    // State tracking for transition detection
    private volatile HAGroupState lastKnownLocalState;
    private volatile HAGroupState lastKnownPeerState;

    // Subscription storage for HA group state change notifications per client instance
    // Map key format: "clusterType:targetState" -> Set<Listeners>
    private final ConcurrentHashMap<String, CopyOnWriteArraySet<HAGroupStateListener>>
            targetStateSubscribers = new ConcurrentHashMap<>();
    // Scheduled executor for periodic sync job
    private ScheduledExecutorService syncExecutor;

    public static HAGroupStoreClient getInstance(Configuration conf, String haGroupName) throws SQLException {
        return getInstanceForZkUrl(conf, haGroupName, null);
    }

    /**
     * Creates/gets an instance of HAGroupStoreClient.
     * Can return null instance if unable to initialize.
     *
     * @param conf configuration
     * @param haGroupName name of the HA group. Only specified HA group is tracked.
     * @param zkUrl zkUrl to use for the client. Prefer providing this parameter to avoid
     *             the overhead of getting the local zkUrl from the configuration.
     * @return HAGroupStoreClient instance
     */
    public static HAGroupStoreClient getInstanceForZkUrl(Configuration conf, String haGroupName,
            String zkUrl) {
        Preconditions.checkNotNull(haGroupName, "haGroupName cannot be null");
        String localZkUrl = Objects.toString(zkUrl, getLocalZkUrl(conf));
        Preconditions.checkNotNull(localZkUrl, "zkUrl cannot be null");
        HAGroupStoreClient result = instances.getOrDefault(localZkUrl, new ConcurrentHashMap<>())
                .getOrDefault(haGroupName, null);
        if (result == null || !result.isHealthy) {
            synchronized (HAGroupStoreClient.class) {
                result = instances.getOrDefault(localZkUrl, new ConcurrentHashMap<>())
                        .getOrDefault(haGroupName, null);
                if (result == null || !result.isHealthy) {
                    result = new HAGroupStoreClient(conf, null, null, haGroupName, zkUrl);
                    if (!result.isHealthy) {
                        result.close();
                        result = null;
                    } else {
                        instances.putIfAbsent(localZkUrl, new ConcurrentHashMap<>());
                        instances.get(localZkUrl).put(haGroupName, result);
                    }
                }
            }
        }
        return result;
    }

    /**
     * Get the list of HAGroupNames from system table.
     * We can also get the list of HAGroupNames from the system table by providing the zkUrl in
     * where clause, but we need to match the formatted zkUrl with the zkUrl in the system table so
     * that matching is done correctly.
     *
     * @param zkUrl for connecting to Table
     * @return the list of HAGroupNames
     * @throws SQLException in case of unexpected error
     */
    public static List<String> getHAGroupNames(String zkUrl) throws SQLException {
        List<String> result = new ArrayList<>();
        String queryString = String.format("SELECT %s,%s,%s FROM %s", HA_GROUP_NAME, ZK_URL_1,
                ZK_URL_2, SYSTEM_HA_GROUP_NAME);
        try (PhoenixConnection conn = (PhoenixConnection) DriverManager.getConnection(
                JDBC_PROTOCOL_ZK + JDBC_PROTOCOL_SEPARATOR + zkUrl);
             Statement stmt = conn.createStatement();
             ResultSet rs = stmt.executeQuery(queryString)) {
            while (rs.next()) {
                String zkUrl1 = rs.getString(ZK_URL_1);
                String zkUrl2 = rs.getString(ZK_URL_2);
                String formattedZkUrl1 = null;
                String formattedZkUrl2 = null;
                if (StringUtils.isNotBlank(zkUrl1)) {
                    formattedZkUrl1 = JDBCUtil.formatUrl(zkUrl1, RegistryType.ZK);
                }
                if (StringUtils.isNotBlank(zkUrl2)) {
                    formattedZkUrl2 = JDBCUtil.formatUrl(zkUrl2, RegistryType.ZK);
                }
                String formattedZkUrl = JDBCUtil.formatUrl(zkUrl, RegistryType.ZK);
                if (StringUtils.equals(formattedZkUrl1, formattedZkUrl) ||
                        StringUtils.equals(formattedZkUrl2, formattedZkUrl)) {
                    result.add(rs.getString(HA_GROUP_NAME));
                }
            }
        }
        return result;
    }

    @VisibleForTesting
    HAGroupStoreClient(final Configuration conf,
            final PathChildrenCacheListener pathChildrenCacheListener,
            final PathChildrenCacheListener peerPathChildrenCacheListener,
            final String haGroupName,
            final String zkUrl) {
        this.conf = conf;
        this.haGroupName = haGroupName;
        this.zkUrl = zkUrl;
        this.waitTimeForSyncModeInMs =  (long) Math.ceil(conf.getLong(ZK_SESSION_TIMEOUT, DEFAULT_ZK_SESSION_TIMEOUT)
                * ZK_SESSION_TIMEOUT_MULTIPLIER);
        this.rotationTimeMs =
                conf.getLong(QueryServices.REPLICATION_LOG_ROTATION_TIME_MS_KEY,
                        QueryServicesOptions.DEFAULT_REPLICATION_LOG_ROTATION_TIME_MS);
        // Custom Event Listener
        this.peerCustomPathChildrenCacheListener = peerPathChildrenCacheListener;
        try {
            // Initialize Phoenix HA Admin
            this.phoenixHaAdmin = new PhoenixHAAdmin(this.zkUrl,
                    conf, ZK_CONSISTENT_HA_GROUP_RECORD_NAMESPACE);
            // Initialize local cache
            this.pathChildrenCache = initializePathChildrenCache(phoenixHaAdmin,
                    pathChildrenCacheListener, ClusterType.LOCAL);
            // Initialize ZNode if not present in ZK
            initializeZNodeIfNeeded();
            if (this.pathChildrenCache != null) {
                this.isHealthy = true;
                // Initialize peer cache
                maybeInitializePeerPathChildrenCache();
            } else {
                LOGGER.error("PathChildrenCache is not initialized, HAGroupStoreClient for "
                        + haGroupName + " is unhealthy");
            }
            // Start periodic sync job
            startPeriodicSyncJob();

        } catch (Exception e) {
            this.isHealthy = false;
            close();
            LOGGER.error("Unexpected error occurred while initializing HAGroupStoreClient, "
                    + "marking cache as unhealthy", e);
        }
    }

    /**
     * Rebuilds the internal cache by querying for all needed data.
     * This is a blocking operation that does not generate events to listeners.
     *
     * @throws Exception if rebuild fails or client is not healthy
     */
    public void rebuild() throws Exception {
        if (!isHealthy) {
            throw new IOException("HAGroupStoreClient is not healthy");
        }
        LOGGER.info("Rebuilding HAGroupStoreClient for HA group {}", haGroupName);
        initializeZNodeIfNeeded();

        // NOTE: this is a BLOCKING method.
        // Completely rebuild the internal cache by querying for all needed data
        // WITHOUT generating any events to send to listeners.
        if (pathChildrenCache != null) {
            pathChildrenCache.rebuild();
        }
        if (peerPathChildrenCache != null) {
            peerPathChildrenCache.rebuild();
        }
        LOGGER.info("Rebuild Complete for HAGroupStoreClient for HA group {}", haGroupName);
    }

    /**
     * Get HAGroupStoreRecord from local quorum.
     *
     * @return HAGroupStoreRecord for the specified HA group name, or null if not found
     * @throws IOException if the client is not healthy
     */
    public HAGroupStoreRecord getHAGroupStoreRecord() throws IOException {
        if (!isHealthy) {
            throw new IOException("HAGroupStoreClient is not healthy");
        }
        return fetchCacheRecordAndPopulateZKIfNeeded(this.pathChildrenCache, ClusterType.LOCAL).getLeft();
    }

    /**
     * Set the HA group status for the specified HA group name.
     * Checks if the status is needed to be updated based on logic in isUpdateNeeded function.
     *
     * @param haGroupState the HA group state to set
     * @throws IOException if the client is not healthy or the operation fails
     * @throws StaleHAGroupStoreRecordVersionException if the version is stale
     * @throws InvalidClusterRoleTransitionException when transition is not valid
     * @throws SQLException
     */
    public void setHAGroupStatusIfNeeded(HAGroupStoreRecord.HAGroupState haGroupState)
            throws IOException,
            InvalidClusterRoleTransitionException,
            SQLException, StaleHAGroupStoreRecordVersionException {
        Preconditions.checkNotNull(haGroupState, "haGroupState cannot be null");
        if (!isHealthy) {
            throw new IOException("HAGroupStoreClient is not healthy");
        }
        Pair<HAGroupStoreRecord, Stat> cacheRecord = fetchCacheRecordAndPopulateZKIfNeeded(
                this.pathChildrenCache, ClusterType.LOCAL);
        HAGroupStoreRecord currentHAGroupStoreRecord = cacheRecord.getLeft();
        Stat currentHAGroupStoreRecordStat = cacheRecord.getRight();
        if (currentHAGroupStoreRecord == null) {
            throw new IOException("Current HAGroupStoreRecordStat in cache is null, "
                    + "cannot update HAGroupStoreRecord, the record should be initialized "
                    + "in System Table first" + haGroupName);
        }
        if (isUpdateNeeded(currentHAGroupStoreRecord.getHAGroupState(),
                currentHAGroupStoreRecordStat.getMtime(), haGroupState)) {
                // We maintain last sync time as the last time cluster was in sync state.
                // If state changes from ACTIVE_IN_SYNC to ACTIVE_NOT_IN_SYNC, record that time
                // Once state changes back to ACTIVE_IN_SYNC or the role is
                // NOT ACTIVE or ACTIVE_TO_STANDBY
                // set the time to null to mark that we are current(or we don't have any reader).
                Long lastSyncTimeInMs = currentHAGroupStoreRecord
                        .getLastSyncStateTimeInMs();
                ClusterRole clusterRole = haGroupState.getClusterRole();
                if (currentHAGroupStoreRecord.getHAGroupState()
                        == HAGroupStoreRecord.HAGroupState.ACTIVE_IN_SYNC
                        && haGroupState == HAGroupStoreRecord.HAGroupState.ACTIVE_NOT_IN_SYNC) {
                    // We record the last round timestamp by subtracting the rotationTime and then 
                    // taking the beginning of last round (floor) by first integer division and then multiplying again.
                    lastSyncTimeInMs = ((System.currentTimeMillis() - rotationTimeMs)/rotationTimeMs) * (rotationTimeMs);
                }
                HAGroupStoreRecord newHAGroupStoreRecord = new HAGroupStoreRecord(
                        currentHAGroupStoreRecord.getProtocolVersion(),
                        currentHAGroupStoreRecord.getHaGroupName(),
                        haGroupState,
                        lastSyncTimeInMs,
                        currentHAGroupStoreRecord.getPolicy(),
                        currentHAGroupStoreRecord.getPeerZKUrl(),
                        currentHAGroupStoreRecord.getClusterUrl(),
                        currentHAGroupStoreRecord.getPeerClusterUrl(),
                        currentHAGroupStoreRecord.getAdminCRRVersion()
                );
                phoenixHaAdmin.updateHAGroupStoreRecordInZooKeeper(haGroupName,
                        newHAGroupStoreRecord, currentHAGroupStoreRecordStat.getVersion());
                // If cluster role is changing, if so, we update,
                // the system table on best effort basis.
                // We also have a periodic job which syncs the ZK
                // state with System Table periodically.
                if (currentHAGroupStoreRecord.getClusterRole() != clusterRole) {
                    HAGroupStoreRecord peerZkRecord = getHAGroupStoreRecordFromPeer();
                    ClusterRoleRecord.ClusterRole peerClusterRole = peerZkRecord != null
                            ? peerZkRecord.getClusterRole()
                            : ClusterRoleRecord.ClusterRole.UNKNOWN;
                    SystemTableHAGroupRecord systemTableRecord = new SystemTableHAGroupRecord(
                            HighAvailabilityPolicy.valueOf(newHAGroupStoreRecord.getPolicy()),
                            clusterRole,
                            peerClusterRole,
                            newHAGroupStoreRecord.getClusterUrl(),
                            newHAGroupStoreRecord.getPeerClusterUrl(),
                            this.zkUrl,
                            newHAGroupStoreRecord.getPeerZKUrl(),
                            newHAGroupStoreRecord.getAdminCRRVersion()
                    );
                    updateSystemTableHAGroupRecordSilently(haGroupName, systemTableRecord);
                }
        } else {
            LOGGER.info("Not updating HAGroupStoreRecord for HA group {} with state {}",
                    haGroupName, haGroupState);
        }
    }

    /**
     * Returns the ClusterRoleRecord for the cluster pair.
     * Information in System Table for peer cluster might be stale,
     * so we need to get the latest information from ZK.
     *
     * @return ClusterRoleRecord for the cluster pair
     * @throws IOException
     */
    public ClusterRoleRecord getClusterRoleRecord() throws IOException {
        HAGroupStoreRecord currentHAGroupStoreRecord = getHAGroupStoreRecord();
        HAGroupStoreRecord peerHAGroupStoreRecord = getHAGroupStoreRecordFromPeer();
        ClusterRoleRecord.ClusterRole peerClusterRole = peerHAGroupStoreRecord != null
                ? peerHAGroupStoreRecord.getClusterRole()
                : ClusterRole.UNKNOWN;
        return new ClusterRoleRecord(this.haGroupName,
                HighAvailabilityPolicy.valueOf(
                        currentHAGroupStoreRecord.getPolicy()),
                currentHAGroupStoreRecord.getClusterUrl(),
                currentHAGroupStoreRecord.getHAGroupState().getClusterRole(),
                currentHAGroupStoreRecord.getPeerClusterUrl(),
                peerClusterRole,
                currentHAGroupStoreRecord.getAdminCRRVersion());
    }

    /**
     * Get HAGroupStoreRecord from peer cluster.
     *
     * @return HAGroupStoreRecord for the specified HA group name, or null if not found
     * @throws IOException if the client is not healthy
     */
    public HAGroupStoreRecord getHAGroupStoreRecordFromPeer() throws IOException {
        if (!isHealthy) {
            throw new IOException("HAGroupStoreClient is not healthy");
        }
        return fetchCacheRecordAndPopulateZKIfNeeded(this.peerPathChildrenCache, ClusterType.PEER).getLeft();
    }

    private void initializeZNodeIfNeeded() throws IOException, SQLException {
        // Sometimes the ZNode might not be available in local cache yet, so better to check
        // in ZK directly if we need to initialize
        Pair<HAGroupStoreRecord, Stat> cacheRecordFromZK =
                phoenixHaAdmin.getHAGroupStoreRecordInZooKeeper(this.haGroupName);
        HAGroupStoreRecord haGroupStoreRecord = cacheRecordFromZK.getLeft();
        // Only if the ZNode is not present, we need to create it from System Table
        if (haGroupStoreRecord == null) {
            LOGGER.info("HAGroupStoreRecord is not available in ZK, creating from SystemTable Record");
            SystemTableHAGroupRecord systemTableRecord
                    = getSystemTableHAGroupRecord(this.haGroupName);
            Preconditions.checkNotNull(systemTableRecord,
                    "System Table HAGroupRecord cannot be null");
            HAGroupStoreRecord.HAGroupState defaultHAGroupState
                    = systemTableRecord.getClusterRole().getDefaultHAGroupState();
            HAGroupStoreRecord newHAGroupStoreRecord = new HAGroupStoreRecord(
                    HAGroupStoreRecord.DEFAULT_PROTOCOL_VERSION,
                    haGroupName,
                    defaultHAGroupState,
                    0L,
                    systemTableRecord.getPolicy().toString(),
                    systemTableRecord.getPeerZKUrl(),
                    systemTableRecord.getClusterUrl(),
                    systemTableRecord.getPeerClusterUrl(),
                    systemTableRecord.getAdminCRRVersion()
            );
            phoenixHaAdmin.createHAGroupStoreRecordInZooKeeper(newHAGroupStoreRecord);
        }
    }

    /**
     * Inner class to hold system table HA group record
     */
    public static class SystemTableHAGroupRecord {
        private final HighAvailabilityPolicy policy;
        private final ClusterRoleRecord.ClusterRole clusterRole;
        private final ClusterRoleRecord.ClusterRole peerClusterRole;
        private final String clusterUrl;
        private final String peerClusterUrl;
        private final String zkUrl;
        private final String peerZKUrl;
        private final long adminCRRVersion;

        public SystemTableHAGroupRecord(HighAvailabilityPolicy policy,
                                        ClusterRoleRecord.ClusterRole clusterRole,
                                        ClusterRoleRecord.ClusterRole peerClusterRole,
                                        String clusterUrl,
                                        String peerClusterUrl,
                                        String zkUrl,
                                        String peerZKUrl,
                                        long adminCRRVersion) {
            this.policy = policy;
            this.clusterRole = clusterRole;
            this.peerClusterRole = peerClusterRole;
            this.clusterUrl = clusterUrl;
            this.peerClusterUrl = peerClusterUrl;
            this.zkUrl = zkUrl;
            this.peerZKUrl = peerZKUrl;
            this.adminCRRVersion = adminCRRVersion;
        }

        public HighAvailabilityPolicy getPolicy() {
            return policy;
        }

        public ClusterRoleRecord.ClusterRole getClusterRole() {
            return clusterRole;
        }

        public ClusterRoleRecord.ClusterRole getPeerClusterRole() {
            return peerClusterRole;
        }

        public String getClusterUrl() {
            return clusterUrl;
        }

        public String getPeerClusterUrl() {
            return peerClusterUrl;
        }

        public String getZkUrl() {
            return zkUrl;
        }

        public String getPeerZKUrl() {
            return peerZKUrl;
        }

        public long getAdminCRRVersion() {
            return adminCRRVersion;
        }

        @Override
        public boolean equals(Object obj) {
            if (this == obj) {
                return true;
            }
            if (obj == null || getClass() != obj.getClass()) {
                return false;
            }
            SystemTableHAGroupRecord other = (SystemTableHAGroupRecord) obj;
            return Objects.equals(policy, other.policy)
                    && Objects.equals(clusterRole, other.clusterRole)
                    && Objects.equals(peerClusterRole, other.peerClusterRole)
                    && Objects.equals(clusterUrl, other.clusterUrl)
                    && Objects.equals(peerClusterUrl, other.peerClusterUrl)
                    && Objects.equals(zkUrl, other.zkUrl)
                    && Objects.equals(peerZKUrl, other.peerZKUrl)
                    && adminCRRVersion == other.adminCRRVersion;
        }

        @Override
        public int hashCode() {
            return Objects.hash(policy, clusterRole, peerClusterRole, clusterUrl,
                    peerClusterUrl, zkUrl, peerZKUrl, adminCRRVersion);
        }
    }

    private SystemTableHAGroupRecord getSystemTableHAGroupRecord(String haGroupName)
            throws SQLException {
        String queryString = String.format("SELECT * FROM %s WHERE %s = '%s'",
                SYSTEM_HA_GROUP_NAME, HA_GROUP_NAME, haGroupName);
        try (PhoenixConnection conn = (PhoenixConnection) DriverManager.getConnection(
                JDBC_PROTOCOL_ZK + JDBC_PROTOCOL_SEPARATOR + zkUrl);
             Statement stmt = conn.createStatement();
             ResultSet rs = stmt.executeQuery(queryString)) {
            if (rs.next()) {
                HighAvailabilityPolicy policy
                        = HighAvailabilityPolicy.valueOf(rs.getString(POLICY));
                String zkUrl1 = rs.getString(ZK_URL_1);
                String zkUrl2 = rs.getString(ZK_URL_2);
                String clusterRole1 = rs.getString(CLUSTER_ROLE_1);
                String clusterRole2 = rs.getString(CLUSTER_ROLE_2);
                String clusterUrl1 = rs.getString(CLUSTER_URL_1);
                String clusterUrl2 = rs.getString(CLUSTER_URL_2);
                long adminCRRVersion = rs.getLong(VERSION);
                String formattedZkUrl1 = null;
                String formattedZkUrl2 = null;
                if (StringUtils.isNotBlank(zkUrl1)) {
                    formattedZkUrl1 = JDBCUtil.formatUrl(zkUrl1, RegistryType.ZK);
                }
                if (StringUtils.isNotBlank(zkUrl2)) {
                    formattedZkUrl2 = JDBCUtil.formatUrl(zkUrl2, RegistryType.ZK);
                }
                String formattedZkUrl = JDBCUtil.formatUrl(this.zkUrl, RegistryType.ZK);
                String peerZKUrl;
                ClusterRoleRecord.ClusterRole clusterRole;
                ClusterRoleRecord.ClusterRole peerClusterRole;
                String clusterUrl;
                String peerClusterUrl;

                if (StringUtils.equals(formattedZkUrl1, formattedZkUrl)) {
                    peerZKUrl = formattedZkUrl2;
                    clusterRole = ClusterRoleRecord.ClusterRole.from(
                            clusterRole1.getBytes(StandardCharsets.UTF_8));
                    peerClusterRole = ClusterRoleRecord.ClusterRole.from(
                            clusterRole2.getBytes(StandardCharsets.UTF_8));
                    clusterUrl = clusterUrl1;
                    peerClusterUrl = clusterUrl2;
                } else if (StringUtils.equals(formattedZkUrl2, formattedZkUrl)) {
                    peerZKUrl = JDBCUtil.formatUrl(zkUrl1, RegistryType.ZK);
                    clusterRole = ClusterRoleRecord.ClusterRole.from(
                            clusterRole2.getBytes(StandardCharsets.UTF_8));
                    peerClusterRole = ClusterRoleRecord.ClusterRole.from(
                            clusterRole1.getBytes(StandardCharsets.UTF_8));
                    clusterUrl = clusterUrl2;
                    peerClusterUrl = clusterUrl1;
                } else {
                    throw new SQLException("Current zkUrl does not match"
                            + "any zkUrl in System Table for HA group: " + haGroupName);
                }

                Preconditions.checkNotNull(clusterRole,
                        "Cluster role in System Table cannot be null");
                Preconditions.checkNotNull(peerClusterRole,
                        "Peer cluster role in System Table cannot be null");
                Preconditions.checkNotNull(clusterUrl,
                        "Cluster URL in System Table cannot be null");
                Preconditions.checkNotNull(peerZKUrl,
                        "Peer ZK URL in System Table cannot be null");
                Preconditions.checkNotNull(peerClusterUrl,
                        "Peer Cluster URL in System Table cannot be null");

                return new SystemTableHAGroupRecord(policy, clusterRole, peerClusterRole,
                        clusterUrl, peerClusterUrl, formattedZkUrl, peerZKUrl, adminCRRVersion);
            } else {
                throw new SQLException("HAGroupStoreRecord not found for HA group name: " +
                        haGroupName + " in System Table " + SYSTEM_HA_GROUP_NAME);
            }
        }
    }

    // Update the system table on best effort basis for HA group
    // In case of failure, we will log the error and continue.
    private void updateSystemTableHAGroupRecordSilently(String haGroupName,
                                                        SystemTableHAGroupRecord record)
            throws SQLException {
        StringBuilder updateQuery = new StringBuilder("UPSERT INTO " + SYSTEM_HA_GROUP_NAME + " (");
        StringBuilder valuesClause = new StringBuilder(" VALUES (");
        List<Object> parameters = new ArrayList<>();

        // Always include HA_GROUP_NAME as it's the key
        updateQuery.append(HA_GROUP_NAME);
        valuesClause.append("?");
        parameters.add(haGroupName);

        // Update non-null fields only.
        if (record.getPolicy() != null) {
            updateQuery.append(", ").append(POLICY);
            valuesClause.append(", ?");
            parameters.add(record.getPolicy().toString());
        }

        if (record.getClusterRole() != null) {
            updateQuery.append(", ").append(CLUSTER_ROLE_1);
            valuesClause.append(", ?");
            parameters.add(record.getClusterRole().name());
        }

        if (record.getPeerClusterRole() != null) {
            updateQuery.append(", ").append(CLUSTER_ROLE_2);
            valuesClause.append(", ?");
            parameters.add(record.getPeerClusterRole().name());
        }

        if (record.getClusterUrl() != null) {
            updateQuery.append(", ").append(CLUSTER_URL_1);
            valuesClause.append(", ?");
            parameters.add(record.getClusterUrl());
        }

        if (record.getPeerClusterUrl() != null) {
            updateQuery.append(", ").append(CLUSTER_URL_2);
            valuesClause.append(", ?");
            parameters.add(record.getPeerClusterUrl());
        }

        if (record.getZkUrl() != null) {
            updateQuery.append(", ").append(ZK_URL_1);
            valuesClause.append(", ?");
            parameters.add(record.getZkUrl());
        }

        if (record.getPeerZKUrl() != null) {
            updateQuery.append(", ").append(ZK_URL_2);
            valuesClause.append(", ?");
            parameters.add(record.getPeerZKUrl());
        }

        if (record.getAdminCRRVersion() > 0) {
            updateQuery.append(", ").append(VERSION);
            valuesClause.append(", ?");
            parameters.add(record.getAdminCRRVersion());
        }

        updateQuery.append(")").append(valuesClause).append(")");

        try (PhoenixConnection conn = (PhoenixConnection) DriverManager.getConnection(
                JDBC_PROTOCOL_ZK + JDBC_PROTOCOL_SEPARATOR + zkUrl);
             PreparedStatement pstmt = conn.prepareStatement(updateQuery.toString())) {

            for (int i = 0; i < parameters.size(); i++) {
                pstmt.setObject(i + 1, parameters.get(i));
            }

            pstmt.executeUpdate();
            conn.commit();
        } catch (Exception e) {
            LOGGER.error("Failed to update system table on best"
                    + "effort basis for HA group {}, error: {}", haGroupName, e);
        }
    }

    /**
     * Starts the periodic sync job that syncs ZooKeeper data (source of truth) to system table.
     * The job runs at configurable intervals with a random jitter for the initial delay.
     */
    private void startPeriodicSyncJob() {
        if (syncExecutor == null) {
            syncExecutor = Executors.newSingleThreadScheduledExecutor(r -> {
                Thread t = new Thread(r, "HAGroupStoreClient-SyncJob-" + haGroupName);
                t.setDaemon(true);
                return t;
            });

            // Get sync interval from configuration (in seconds)
            long syncIntervalSeconds = conf.getLong(HA_GROUP_STORE_SYNC_INTERVAL_SECONDS,
                    DEFAULT_HA_GROUP_STORE_SYNC_INTERVAL_SECONDS);

            // Add jitter to initial delay
            long jitterSeconds
                    = ThreadLocalRandom.current().nextLong(0, SYNC_JOB_MAX_JITTER_SECONDS + 1);

            LOGGER.info("Starting periodic sync job for HA group {} "
                            + "with initial delay of {} seconds, "
                            + "then every {} seconds",
                    haGroupName,
                    jitterSeconds,
                    syncIntervalSeconds);

            syncExecutor.scheduleAtFixedRate(
                    this::syncZKToSystemTable,
                    jitterSeconds,
                    syncIntervalSeconds,
                    TimeUnit.SECONDS
            );
        }
    }

    /**
     * Syncs data from ZooKeeper (source of truth) to the system table.
     * This method is called periodically to ensure consistency.
     */
    private void syncZKToSystemTable() {
        if (!isHealthy) {
            LOGGER.debug("HAGroupStoreClient is not healthy,"
                    + "skipping sync for HA group {}", haGroupName);
            return;
        }

        LOGGER.debug("Starting periodic sync from ZK to"
                + "system table for HA group {}", haGroupName);
        // Get current data from ZooKeeper
        try {
                HAGroupStoreRecord zkRecord = phoenixHaAdmin.getHAGroupStoreRecordInZooKeeper(haGroupName).getLeft();
            if (zkRecord == null) {
                LOGGER.warn("No ZK record found for HA group {}, skipping sync", haGroupName);
                return;
            }
            // Get peer record for complete information
            HAGroupStoreRecord peerZkRecord = getHAGroupStoreRecordFromPeer();
            ClusterRoleRecord.ClusterRole peerClusterRole = peerZkRecord != null
                    ? peerZkRecord.getClusterRole()
                    : ClusterRoleRecord.ClusterRole.UNKNOWN;
            // Create SystemTableHAGroupRecord from ZK data
            SystemTableHAGroupRecord newSystemTableRecord = new SystemTableHAGroupRecord(
                    HighAvailabilityPolicy.valueOf(zkRecord.getPolicy()),
                    zkRecord.getClusterRole(),
                    peerClusterRole,
                    zkRecord.getClusterUrl(),
                    zkRecord.getPeerClusterUrl(),
                    this.zkUrl,
                    zkRecord.getPeerZKUrl(),
                    zkRecord.getAdminCRRVersion()
            );

            // Read existing record from system table to check if update is needed
            SystemTableHAGroupRecord existingSystemTableRecord = getSystemTableHAGroupRecord(haGroupName);
            if (newSystemTableRecord.equals(existingSystemTableRecord)) {
                LOGGER.debug("System table record is already up-to-date for HA group {}, skipping update",
                        haGroupName);
                return;
            }

            // Update system table with ZK data
            updateSystemTableHAGroupRecordSilently(haGroupName, newSystemTableRecord);
            LOGGER.info("Successfully synced ZK data to system table for HA group {}", haGroupName);
        } catch (IOException | SQLException e) {
            long syncIntervalSeconds = conf.getLong(HA_GROUP_STORE_SYNC_INTERVAL_SECONDS,
                    DEFAULT_HA_GROUP_STORE_SYNC_INTERVAL_SECONDS);
            LOGGER.error("Failed to sync ZK data to system "
                            + "table for HA group on best effort basis {},"
                            + "retrying in {} seconds",
                    haGroupName, syncIntervalSeconds);
        }
    }

    private void maybeInitializePeerPathChildrenCache() throws IOException {
        // There is an edge case when the cache is not initialized yet, but we get CHILD_ADDED event
        // so we need to get the record from ZK.
        HAGroupStoreRecord currentHAGroupStoreRecord
                = phoenixHaAdmin.getHAGroupStoreRecordInZooKeeper(haGroupName).getLeft();
        if (currentHAGroupStoreRecord == null) {
            LOGGER.error("Current HAGroupStoreRecord is null,"
                    + "skipping peer path children cache initialization");
            return;
        }
        String peerZKUrl = currentHAGroupStoreRecord.getPeerZKUrl();
        if (StringUtils.isNotBlank(peerZKUrl)) {
            try {
                // Setup peer connection if needed (first time or ZK Url changed)
                if (peerPathChildrenCache == null
                    || peerPhoenixHaAdmin != null
                        && !StringUtils.equals(peerZKUrl, peerPhoenixHaAdmin.getZkUrl())) {
                    // Clean up existing peer connection if it exists
                    closePeerConnection();
                    // Setup new peer connection
                    this.peerPhoenixHaAdmin
                            = new PhoenixHAAdmin(peerZKUrl, conf,
                            ZK_CONSISTENT_HA_GROUP_RECORD_NAMESPACE);
                    // Create new PeerPathChildrenCache
                    this.peerPathChildrenCache = initializePathChildrenCache(peerPhoenixHaAdmin,
                            this.peerCustomPathChildrenCacheListener, ClusterType.PEER);
                }
            } catch (Exception e) {
                closePeerConnection();
                LOGGER.error("Unable to initialize PeerPathChildrenCache for HAGroupStoreClient",
                        e);
                // Don't think we should mark HAGroupStoreClient as unhealthy if
                // peerCache is unhealthy, if needed we can introduce a config to control behavior.
            }
        } else {
            // Close Peer Cache for this HAGroupName if currentClusterRecord is null
            // or peerZKUrl is blank
            closePeerConnection();
            LOGGER.error("Not initializing PeerPathChildrenCache for HAGroupStoreClient "
                    + "with HAGroupName {} as peerZKUrl is blank", haGroupName);
        }
    }

    private PathChildrenCache initializePathChildrenCache(PhoenixHAAdmin admin,
                                                          PathChildrenCacheListener customListener,
                                                          ClusterType cacheType) {
        LOGGER.info("Initializing {} PathChildrenCache with URL {}", cacheType, admin.getZkUrl());
        PathChildrenCache newPathChildrenCache = null;
        try {
            newPathChildrenCache = new PathChildrenCache(admin.getCurator(),
                    ZKPaths.PATH_SEPARATOR, true);
            final CountDownLatch latch = new CountDownLatch(1);
            newPathChildrenCache.getListenable().addListener(customListener != null
                    ? customListener
                    : createCacheListener(latch, cacheType));
            newPathChildrenCache.start(PathChildrenCache.StartMode.POST_INITIALIZED_EVENT);
            boolean initialized = latch.await(conf.getLong(PHOENIX_HA_GROUP_STORE_CLIENT_INITIALIZATION_TIMEOUT_MS,
                            DEFAULT_HA_GROUP_STORE_CLIENT_INITIALIZATION_TIMEOUT_MS),
                    TimeUnit.MILLISECONDS);
            if (!initialized && customListener == null) {
                newPathChildrenCache.close();
                return null;
            }
            return newPathChildrenCache;
        } catch (Exception e) {
            if (newPathChildrenCache != null) {
                try {
                    newPathChildrenCache.close();
                } catch (IOException ioe) {
                    LOGGER.error("Failed to close {} PathChildrenCache with ZKUrl",
                            cacheType, ioe);
                }
            }
            LOGGER.error("Failed to initialize {} PathChildrenCache", cacheType, e);
            return null;
        }
    }

    private PathChildrenCacheListener createCacheListener(CountDownLatch latch,
                                                          ClusterType cacheType) {
        return (client, event) -> {
            final ChildData childData = event.getData();
            Pair<HAGroupStoreRecord, Stat> eventRecordAndStat
                    = extractHAGroupStoreRecordOrNull(childData);
            HAGroupStoreRecord eventRecord = eventRecordAndStat.getLeft();
            Stat eventStat = eventRecordAndStat.getRight();
            LOGGER.info("HAGroupStoreClient Cache {} received event {} type {} at {} with ZKUrl {} and PeerZKUrl {} for haGroupName {}",
                    cacheType, eventRecord, event.getType(), System.currentTimeMillis(), phoenixHaAdmin.getZkUrl(),
                    peerPhoenixHaAdmin != null ? peerPhoenixHaAdmin.getZkUrl() : "peerPhoenixHaAdmin is null", haGroupName);
            switch (event.getType()) {
                case CHILD_ADDED:
                case CHILD_UPDATED:
                    if (eventRecord != null && Objects.equals(eventRecord.getHaGroupName(), haGroupName)) {
                        handleStateChange(eventRecord, eventStat, cacheType);
                        // Reinitialize peer path children cache if peer url is added or updated.
                        if (cacheType == ClusterType.LOCAL) {
                            maybeInitializePeerPathChildrenCache();
                        }
                    }
                    break;
                case CHILD_REMOVED:
                    break;
                case INITIALIZED:
                    latch.countDown();
                    break;
                case CONNECTION_LOST:
                case CONNECTION_SUSPENDED:
                    if (ClusterType.LOCAL.equals(cacheType)) {
                        isHealthy = false;
                    }
                    LOGGER.warn("{} HAGroupStoreClient cache connection lost/suspended",
                            cacheType);
                    break;
                case CONNECTION_RECONNECTED:
                    if (ClusterType.LOCAL.equals(cacheType)) {
                        isHealthy = true;
                    }
                    LOGGER.info("{} HAGroupStoreClient cache connection reconnected", cacheType);
                    break;
                default:
                    LOGGER.warn("Unexpected {} event type {}, complete event {}",
                            cacheType, event.getType(), event);
            }
        };
    }


    private Pair<HAGroupStoreRecord, Stat> fetchCacheRecordAndPopulateZKIfNeeded(
            PathChildrenCache cache, ClusterType cacheType) {
        if (cache == null) {
            LOGGER.warn("{} HAGroupStoreClient cache is null, returning null", cacheType);
            return Pair.of(null, null);
        }

        String targetPath = toPath(this.haGroupName);
        // Try to get record from current cache data
        Pair<HAGroupStoreRecord, Stat> result = extractRecordAndStat(cache, targetPath, cacheType);
        if (result.getLeft() != null) {
            return result;
        }

        if (cacheType.equals(ClusterType.PEER)) {
            return Pair.of(null, null);
        }
        // If no record found, try to rebuild and fetch again
        LOGGER.info("No record found at path {} for {} cluster, trying to initialize ZNode "
                + "from System Table in case it might have been deleted",
                    targetPath, cacheType);
        try {
            rebuild();
            return extractRecordAndStat(cache, targetPath, cacheType);
        } catch (Exception e) {
            LOGGER.error("Failed to initialize ZNode from System Table, giving up "
                    + "and returning null", e);
            return Pair.of(null, null);
        }
    }

    private Pair<HAGroupStoreRecord, Stat> extractRecordAndStat(PathChildrenCache cache,
                                                                String targetPath,
                                                                ClusterType cacheType) {
        ChildData childData = cache.getCurrentData(targetPath);
        if (childData != null) {
            Pair<HAGroupStoreRecord, Stat> recordAndStat
                    = extractHAGroupStoreRecordOrNull(childData);
            LOGGER.debug("Built {} cluster record: {}", cacheType, recordAndStat.getLeft());
            return recordAndStat;
        }
        return Pair.of(null, null);
    }

    private Pair<HAGroupStoreRecord, Stat> extractHAGroupStoreRecordOrNull(
            final ChildData childData) {
        if (childData != null) {
            byte[] data = childData.getData();
            return Pair.of(HAGroupStoreRecord.fromJson(data).orElse(null), childData.getStat());
        }
        return Pair.of(null, null);
    }


    /**
     * Closes the peer connection and cleans up peer-related resources.
     */
    private void closePeerConnection() {
        try {
            if (peerPathChildrenCache != null) {
                peerPathChildrenCache.close();
                peerPathChildrenCache = null;
            }
            if (peerPhoenixHaAdmin != null) {
                peerPhoenixHaAdmin.close();
                peerPhoenixHaAdmin = null;
            }
        } catch (Exception e) {
            LOGGER.warn("Failed to close peer connection", e);
        }
    }

    /**
     * Shuts down the periodic sync executor gracefully.
     */
    private void shutdownSyncExecutor() {
        if (syncExecutor != null) {
            syncExecutor.shutdown();
            try {
                if (!syncExecutor.awaitTermination(5, TimeUnit.SECONDS)) {
                    syncExecutor.shutdownNow();
                }
            } catch (InterruptedException e) {
                syncExecutor.shutdownNow();
                Thread.currentThread().interrupt();
            }
            syncExecutor = null;
        }
    }

    @Override
    public void close() {
        try {
            LOGGER.info("Closing HAGroupStoreClient");
            // Shutdown sync executor
            shutdownSyncExecutor();
            if (pathChildrenCache != null) {
                pathChildrenCache.close();
                pathChildrenCache = null;
            }
            closePeerConnection();
            LOGGER.info("Closed HAGroupStoreClient");
        } catch (IOException e) {
            LOGGER.error("Exception closing HAGroupStoreClient", e);
        }
    }

    /**
     * Checks if the HAGroupStoreRecord needs to be updated.
     * If the cluster role is allowed to transition to the new state and the status refresh
     * interval has expired, the HAGroupStoreRecord needs to be updated.
     * If the transition is not allowed, an exception is thrown.
     *
     * @param currentHAGroupState the current HAGroupState of the HAGroupStoreRecord
     * @param currentHAGroupStoreRecordMtime the last modified time of the current
     *                                      HAGroupStoreRecord
     * @param newHAGroupState the cluster state to check
     * @return true if the HAGroupStoreRecord needs to be updated, false otherwise
     * @throws InvalidClusterRoleTransitionException if the cluster role transition is invalid
     */
    private boolean isUpdateNeeded(HAGroupStoreRecord.HAGroupState currentHAGroupState,
                                   long currentHAGroupStoreRecordMtime,
                                   HAGroupStoreRecord.HAGroupState newHAGroupState)
            throws InvalidClusterRoleTransitionException {
        long waitTime = 0L;
        if (currentHAGroupState.isTransitionAllowed(newHAGroupState)) {
            if (currentHAGroupState == HAGroupState.ACTIVE_NOT_IN_SYNC
                    && newHAGroupState == HAGroupState.ACTIVE_IN_SYNC
                    || (currentHAGroupState == HAGroupState.ACTIVE_NOT_IN_SYNC_TO_STANDBY
                    && newHAGroupState == HAGroupState.ACTIVE_IN_SYNC_TO_STANDBY)) {
                waitTime = waitTimeForSyncModeInMs;
            }
        } else {
            throw new InvalidClusterRoleTransitionException("Cannot transition from "
                    + currentHAGroupState + " to " + newHAGroupState);
        }
        return ((System.currentTimeMillis() - currentHAGroupStoreRecordMtime) > waitTime);
    }

    // ========== HA Group State Change Subscription Methods ==========

    /**
     * Subscribe to be notified when any transition to a target state occurs.
     *
     * @param targetState the target state to watch for
     * @param clusterType whether to monitor local or peer cluster
     * @param listener the listener to notify when any transition to the target state occurs
     */
    public void subscribeToTargetState(HAGroupState targetState,
                                       ClusterType clusterType, HAGroupStateListener listener) {
        String key = buildTargetStateKey(clusterType, targetState);
        targetStateSubscribers.computeIfAbsent(key, k -> new CopyOnWriteArraySet<>()).add(listener);
        LOGGER.info("Subscribed listener to target state {} for HA group {} on {} cluster",
                targetState, haGroupName, clusterType);
    }

    /**
     * Unsubscribe from target state notifications.
     *
     * @param targetState the target state
     * @param clusterType whether monitoring local or peer cluster
     * @param listener the listener to remove
     */
    public void unsubscribeFromTargetState(HAGroupState targetState,
                                           ClusterType clusterType, HAGroupStateListener listener) {
        String key = buildTargetStateKey(clusterType, targetState);
        CopyOnWriteArraySet<HAGroupStateListener> listeners = targetStateSubscribers.get(key);
        if (listeners != null && listeners.remove(listener)) {
            if (listeners.isEmpty()) {
                targetStateSubscribers.remove(key);
            }
            LOGGER.info("Unsubscribed listener from target state {} for HA group {} on {} cluster",
                    targetState, haGroupName, clusterType);
        }
    }

    /**
     * Handle state change detection and notify subscribers if a transition occurred.
     *
     * @param newRecord the new HA group store record
     * @param cacheType the type of cache (LOCAL or PEER)
     */
    private void handleStateChange(HAGroupStoreRecord newRecord,
                                   Stat newStat, ClusterType cacheType) {
        HAGroupState newState = newRecord.getHAGroupState();
        HAGroupState oldState;
        ClusterType clusterType;

        if (ClusterType.LOCAL.equals(cacheType)) {
            oldState = lastKnownLocalState;
            lastKnownLocalState = newState;
            clusterType = ClusterType.LOCAL;
        } else {
            oldState = lastKnownPeerState;
            lastKnownPeerState = newState;
            clusterType = ClusterType.PEER;
        }

        // Only notify if there's an actual state transition or initial state
        if (oldState == null || !oldState.equals(newState)) {
            LOGGER.info("Detected state transition for HA group {} from {} to {} on {} cluster",
                    haGroupName, oldState, newState, clusterType);
            notifySubscribers(oldState, newState, newStat.getMtime(), clusterType,
                    newRecord.getLastSyncStateTimeInMs());
        }
    }

    /**
     * Notify all relevant subscribers of a state transition.
     *
     * @param fromState the state transitioned from
     * @param toState the state transitioned to
     * @param clusterType the cluster type where the transition occurred
     */
    private void notifySubscribers(HAGroupState fromState,
                                   HAGroupState toState,
                                   long modifiedTime,
                                   ClusterType clusterType,
                                   Long lastSyncStateTimeInMs) {
        LOGGER.debug("Notifying subscribers of state transition "
                        + "for HA group {} from {} to {} on {} cluster",
                haGroupName, fromState, toState, clusterType);
        String targetStateKey = buildTargetStateKey(clusterType, toState);

        // Collect all listeners that need to be notified
        Set<HAGroupStateListener> listenersToNotify = new HashSet<>();

        // Find target state subscribers
        CopyOnWriteArraySet<HAGroupStateListener> targetListeners
                = targetStateSubscribers.get(targetStateKey);
        if (targetListeners != null) {
            listenersToNotify.addAll(targetListeners);
        }

        // Notify all listeners with error isolation
        if (!listenersToNotify.isEmpty()) {
            LOGGER.info("Notifying {} listeners of state transition"
                            + "for HA group {} from {} to {} on {} cluster",
                    listenersToNotify.size(), haGroupName, fromState, toState, clusterType);

            for (HAGroupStateListener listener : listenersToNotify) {
                try {
                    listener.onStateChange(haGroupName, fromState,
                            toState, modifiedTime, clusterType, lastSyncStateTimeInMs);
                } catch (Exception e) {
                    LOGGER.error("Error notifying listener of state transition "
                                    + "for HA group {} from {} to {} on {} cluster",
                            haGroupName, fromState, toState, clusterType, e);
                    // Continue notifying other listeners
                }
            }
        }
    }

    // ========== Helper Methods ==========

    /**
     * Build key for target state subscriptions.
     */
    private String buildTargetStateKey(ClusterType clusterType,
                                       HAGroupState targetState) {
        return clusterType + ":" + targetState;
    }

}