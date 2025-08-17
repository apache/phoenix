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
import org.apache.phoenix.thirdparty.com.google.common.annotations.VisibleForTesting;
import org.apache.phoenix.thirdparty.com.google.common.base.Preconditions;
import org.apache.phoenix.util.JDBCUtil;
import org.apache.zookeeper.data.Stat;
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


/**
 * Main implementation of HAGroupStoreClient with peer support.
 * Write-through cache for HAGroupStore based on {@link HAGroupStoreRecord}.
 * Uses {@link PathChildrenCache} from {@link org.apache.curator.framework.CuratorFramework}.
 */
public class HAGroupStoreClient implements Closeable {

    public static final String ZK_CONSISTENT_HA_GROUP_STATE_NAMESPACE = "phoenix"
                    + ZKPaths.PATH_SEPARATOR + "consistentHA"
                    + ZKPaths.PATH_SEPARATOR + "groupState";
    private static final long HA_GROUP_STORE_CLIENT_INITIALIZATION_TIMEOUT_MS = 30000L;
    // Multiplier for ZK session timeout to account for time it will take for HMaster to abort
    // the region server in case ZK connection is lost from the region server.
    @VisibleForTesting
    static final double ZK_SESSION_TIMEOUT_MULTIPLIER = 1.1;
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
    // Peer ZK URL for peer cluster and HAGroupName
    private String peerZKUrl = null;
    // Peer Custom Event Listener
    private final PathChildrenCacheListener peerCustomPathChildrenCacheListener;
    // Wait time for sync mode
    private final long waitTimeForSyncModeInMs;
    // State tracking for transition detection
    private volatile HAGroupState lastKnownLocalState;
    private volatile HAGroupState lastKnownPeerState;

    // Subscription storage for HA group state change notifications per client instance
    // Map key format: "clusterType:fromState:toState" -> Set<Listeners>
    private final ConcurrentHashMap<String, CopyOnWriteArraySet<HAGroupStateListener>>
            specificTransitionSubscribers = new ConcurrentHashMap<>();
    // Map key format: "clusterType:targetState" -> Set<Listeners>
    private final ConcurrentHashMap<String, CopyOnWriteArraySet<HAGroupStateListener>>
            targetStateSubscribers = new ConcurrentHashMap<>();
    // Policy for the HA group
    private HighAvailabilityPolicy policy;
    private ClusterRole clusterRole;
    private ClusterRole peerClusterRole;
    private String clusterUrl;
    private String peerClusterUrl;
    private long clusterRoleRecordVersion;

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
                String formattedZkUrl1 = JDBCUtil.formatUrl(zkUrl1, RegistryType.ZK);
                String formattedZkUrl2 = JDBCUtil.formatUrl(zkUrl2, RegistryType.ZK);
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
        // Custom Event Listener
        this.peerCustomPathChildrenCacheListener = peerPathChildrenCacheListener;
        try {
            // Initialize HAGroupStoreClient attributes
            initializeHAGroupStoreClientAttributes(haGroupName);
            // Initialize Phoenix HA Admin
            this.phoenixHaAdmin = new PhoenixHAAdmin(this.zkUrl,
                    conf, ZK_CONSISTENT_HA_GROUP_STATE_NAMESPACE);
            // Initialize local cache
            this.pathChildrenCache = initializePathChildrenCache(phoenixHaAdmin,
                    pathChildrenCacheListener, ClusterType.LOCAL);
            // Initialize ZNode if not present in ZK
            initializeZNodeIfNeeded();
            if (this.pathChildrenCache != null) {
                this.isHealthy = true;
                // Initialize peer cache
                maybeInitializePeerPathChildrenCache();
            }

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
        initializeHAGroupStoreClientAttributes(haGroupName);
        initializeZNodeIfNeeded();
        maybeInitializePeerPathChildrenCache();

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
        return fetchCacheRecord(this.pathChildrenCache, ClusterType.LOCAL).getLeft();
    }

    /**
     * Set the HA group status for the specified HA group name.
     * Checks if the status is needed to be updated based on logic in isUpdateNeeded function.
     *
     * @param haGroupState the HA group state to set
     * @throws IOException if the client is not healthy or the operation fails
     * @throws StaleHAGroupStoreRecordVersionException if the version is stale
     * @throws InvalidClusterRoleTransitionException when transition is not valid
     */
    public void setHAGroupStatusIfNeeded(HAGroupStoreRecord.HAGroupState haGroupState)
            throws IOException, StaleHAGroupStoreRecordVersionException,
            InvalidClusterRoleTransitionException {
        Preconditions.checkNotNull(haGroupState, "haGroupState cannot be null");
        if (!isHealthy) {
            throw new IOException("HAGroupStoreClient is not healthy");
        }
        Pair<HAGroupStoreRecord, Stat> cacheRecord = fetchCacheRecord(
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
                // TODO: Verify that for reader this is the correct approach.
                Long lastSyncTimeInMs = currentHAGroupStoreRecord
                        .getLastSyncStateTimeInMs();
                ClusterRole clusterRole = haGroupState.getClusterRole();
                if (currentHAGroupStoreRecord.getHAGroupState()
                        == HAGroupStoreRecord.HAGroupState.ACTIVE_IN_SYNC
                        && haGroupState == HAGroupStoreRecord.HAGroupState.ACTIVE_NOT_IN_SYNC) {
                    lastSyncTimeInMs = System.currentTimeMillis();
                } else if (haGroupState == HAGroupState.ACTIVE_IN_SYNC
                        || !(ClusterRole.ACTIVE.equals(clusterRole)
                            || ClusterRole.ACTIVE_TO_STANDBY.equals(clusterRole))) {
                    lastSyncTimeInMs = null;
                }
                HAGroupStoreRecord newHAGroupStoreRecord = new HAGroupStoreRecord(
                        currentHAGroupStoreRecord.getProtocolVersion(),
                        currentHAGroupStoreRecord.getHaGroupName(),
                        haGroupState,
                        lastSyncTimeInMs
                );
                // TODO: Check if cluster role is changing, if so, we need to update
                // the system table first
                // Lock the row in System Table and make sure update is reflected
                // in all regionservers
                // It should automatically update the ZK record as well.
                phoenixHaAdmin.updateHAGroupStoreRecordInZooKeeper(haGroupName,
                        newHAGroupStoreRecord, currentHAGroupStoreRecordStat.getVersion());
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
        HAGroupStoreRecord peerHAGroupStoreRecord = getHAGroupStoreRecordFromPeer();
        ClusterRoleRecord.ClusterRole peerClusterRole = peerHAGroupStoreRecord != null
                ? peerHAGroupStoreRecord.getClusterRole()
                : ClusterRole.UNKNOWN;
        return new ClusterRoleRecord(this.haGroupName,
                                    this.policy,
                                    this.clusterUrl,
                                    this.clusterRole,
                                    this.peerClusterUrl,
                                    peerClusterRole,
                                    this.clusterRoleRecordVersion);
    }

    /**
     * Get HAGroupStoreRecord from peer cluster.
     *
     * @return HAGroupStoreRecord for the specified HA group name, or null if not found
     * @throws IOException if the client is not healthy
     */
    private HAGroupStoreRecord getHAGroupStoreRecordFromPeer() throws IOException {
        if (!isHealthy) {
            throw new IOException("HAGroupStoreClient is not healthy");
        }
        return fetchCacheRecord(this.peerPathChildrenCache, ClusterType.PEER).getLeft();
    }

    private void initializeZNodeIfNeeded() throws IOException,
            StaleHAGroupStoreRecordVersionException {
        // Sometimes the ZNode might not be available in local cache yet, so better to check
        // in ZK directly if we need to initialize
        Pair<HAGroupStoreRecord, Stat> cacheRecordFromZK =
                phoenixHaAdmin.getHAGroupStoreRecordInZooKeeper(this.haGroupName);
        HAGroupStoreRecord haGroupStoreRecord = cacheRecordFromZK.getLeft();
        HAGroupState defaultHAGroupState = this.clusterRole.getDefaultHAGroupState();
        // Initialize lastSyncTimeInMs only if we start in ACTIVE_NOT_IN_SYNC state
        //  and ZNode is not already present
        Long lastSyncTimeInMs = defaultHAGroupState.equals(HAGroupState.ACTIVE_NOT_IN_SYNC)
                                    ? System.currentTimeMillis()
                                    : null;
        HAGroupStoreRecord newHAGroupStoreRecord = new HAGroupStoreRecord(
            HAGroupStoreRecord.DEFAULT_PROTOCOL_VERSION,
            haGroupName,
            this.clusterRole.getDefaultHAGroupState(),
            lastSyncTimeInMs
        );
        // Only update current ZNode if it doesn't have the same role as present in System Table.
        // If not exists, then create ZNode
        // TODO: Discuss if this approach is what reader needs.
        if (haGroupStoreRecord != null &&
                !haGroupStoreRecord.getClusterRole().equals(this.clusterRole)) {
            phoenixHaAdmin.updateHAGroupStoreRecordInZooKeeper(haGroupName,
                    newHAGroupStoreRecord, cacheRecordFromZK.getRight().getVersion());
        } else if (haGroupStoreRecord == null) {
            phoenixHaAdmin.createHAGroupStoreRecordInZooKeeper(newHAGroupStoreRecord);
        }
    }

    private void initializeHAGroupStoreClientAttributes(String haGroupName) throws SQLException {
        String queryString = String.format("SELECT * FROM %s WHERE %s = '%s'",
                SYSTEM_HA_GROUP_NAME, HA_GROUP_NAME, haGroupName);
        try (PhoenixConnection conn = (PhoenixConnection) DriverManager.getConnection(
                JDBC_PROTOCOL_ZK + JDBC_PROTOCOL_SEPARATOR + zkUrl);
             Statement stmt = conn.createStatement();
             ResultSet rs = stmt.executeQuery(queryString)) {
            if (rs.next()) {
                this.policy = HighAvailabilityPolicy.valueOf(rs.getString(POLICY));
                String zkUrl1 = rs.getString(ZK_URL_1);
                String zkUrl2 = rs.getString(ZK_URL_2);
                String clusterRole1 = rs.getString(CLUSTER_ROLE_1);
                String clusterRole2 = rs.getString(CLUSTER_ROLE_2);
                String clusterUrl1 = rs.getString(CLUSTER_URL_1);
                String clusterUrl2 = rs.getString(CLUSTER_URL_2);
                this.clusterRoleRecordVersion = rs.getLong(VERSION);
                Preconditions.checkNotNull(zkUrl1, "ZK_URL_1 in System Table cannot be null");
                Preconditions.checkNotNull(zkUrl2, "ZK_URL_2 in System Table cannot be null");
                String formattedZkUrl1 = JDBCUtil.formatUrl(zkUrl1, RegistryType.ZK);
                String formattedZkUrl2 = JDBCUtil.formatUrl(zkUrl2, RegistryType.ZK);
                String formattedZkUrl = JDBCUtil.formatUrl(this.zkUrl, RegistryType.ZK);
                if (StringUtils.equals(formattedZkUrl1, formattedZkUrl)) {
                    this.peerZKUrl = formattedZkUrl2;
                    this.clusterRole = ClusterRoleRecord.ClusterRole.from(
                            clusterRole1.getBytes(StandardCharsets.UTF_8));
                    this.peerClusterRole = ClusterRoleRecord.ClusterRole.from(
                            clusterRole2.getBytes(StandardCharsets.UTF_8));
                    this.clusterUrl = clusterUrl1;
                    this.peerClusterUrl = clusterUrl2;
                } else if (StringUtils.equals(formattedZkUrl2, formattedZkUrl)) {
                    this.peerZKUrl = JDBCUtil.formatUrl(zkUrl1, RegistryType.ZK);
                    this.clusterRole = ClusterRoleRecord.ClusterRole.from(
                            clusterRole2.getBytes(StandardCharsets.UTF_8));
                    this.peerClusterRole = ClusterRoleRecord.ClusterRole.from(
                            clusterRole1.getBytes(StandardCharsets.UTF_8));
                    this.clusterUrl = clusterUrl2;
                    this.peerClusterUrl = clusterUrl1;
                }
            } else {
                throw new SQLException("HAGroupStoreRecord not found for HA group name: " +
                        haGroupName + " in System Table " + SYSTEM_HA_GROUP_NAME);
            }
        }
        Preconditions.checkNotNull(this.clusterRole,
                "Cluster role in System Table cannot be null");
        Preconditions.checkNotNull(this.peerClusterRole,
                "Peer cluster role in System Table cannot be null");
        Preconditions.checkNotNull(this.clusterUrl,
                "Cluster URL in System Table cannot be null");
        Preconditions.checkNotNull(this.peerZKUrl,
                "Peer ZK URL in System Table cannot be null");
        Preconditions.checkNotNull(this.peerClusterUrl,
                "Peer Cluster URL in System Table cannot be null");
        Preconditions.checkNotNull(this.clusterRoleRecordVersion,
                "Cluster role record version in System Table cannot be null");
    }

    private void maybeInitializePeerPathChildrenCache() {
        if (StringUtils.isNotBlank(this.peerZKUrl)) {
            try {
                // Setup peer connection if needed (first time or ZK Url changed)
                if (peerPathChildrenCache == null
                    || peerPhoenixHaAdmin != null
                        && !StringUtils.equals(this.peerZKUrl, peerPhoenixHaAdmin.getZkUrl())) {
                    // Clean up existing peer connection if it exists
                    closePeerConnection();
                    // Setup new peer connection
                    this.peerPhoenixHaAdmin
                            = new PhoenixHAAdmin(this.peerZKUrl, conf,
                            ZK_CONSISTENT_HA_GROUP_STATE_NAMESPACE);
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
            boolean initialized = latch.await(HA_GROUP_STORE_CLIENT_INITIALIZATION_TIMEOUT_MS,
                    TimeUnit.MILLISECONDS);
            return initialized ? newPathChildrenCache : null;
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
            LOGGER.info("HAGroupStoreClient Cache {} received event {} type {} at {}",
                    cacheType, eventRecord, event.getType(), System.currentTimeMillis());
            switch (event.getType()) {
                case CHILD_ADDED:
                case CHILD_UPDATED:
                    if (eventRecord != null) {
                        handleStateChange(eventRecord, eventStat, cacheType);
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


    private Pair<HAGroupStoreRecord, Stat> fetchCacheRecord(PathChildrenCache cache,
                                                            ClusterType cacheType) {
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
            LOGGER.info("Built {} cluster record: {}", cacheType, recordAndStat.getLeft());
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

    @Override
    public void close() {
        try {
            LOGGER.info("Closing HAGroupStoreClient");
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
                    && newHAGroupState == HAGroupState.ACTIVE_IN_SYNC) {
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
     * Subscribe to be notified when a specific state transition occurs.
     *
     * @param fromState the state to transition from
     * @param toState the state to transition to
     * @param clusterType whether to monitor local or peer cluster
     * @param listener the listener to notify when the transition occurs
     */
    public void subscribeToTransition(HAGroupState fromState, HAGroupState toState,
                                      ClusterType clusterType, HAGroupStateListener listener) {
        String key = buildTransitionKey(clusterType, fromState, toState);
        specificTransitionSubscribers.computeIfAbsent(key,
                k -> new CopyOnWriteArraySet<>()).add(listener);
        LOGGER.info("Subscribed listener to transition {} -> {} for HA group {} on {} cluster",
                fromState, toState, haGroupName, clusterType);
    }

    /**
     * Unsubscribe from specific state transition notifications.
     *
     * @param fromState the state to transition from
     * @param toState the state to transition to
     * @param clusterType whether monitoring local or peer cluster
     * @param listener the listener to remove
     */
    public void unsubscribeFromTransition(HAGroupState fromState, HAGroupState toState,
                                          ClusterType clusterType, HAGroupStateListener listener) {
        String key = buildTransitionKey(clusterType, fromState, toState);
        CopyOnWriteArraySet<HAGroupStateListener> listeners
                = specificTransitionSubscribers.get(key);
        if (listeners != null && listeners.remove(listener)) {
            if (listeners.isEmpty()) {
                specificTransitionSubscribers.remove(key);
            }
            LOGGER.info("Unsubscribed listener from transition {} -> {} "
                            + "for HA group {} on {} cluster",
                    fromState, toState, haGroupName, clusterType);
        }
    }

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

        // Only notify if there's an actual state transition (not initial state)
        if (oldState != null && !oldState.equals(newState)) {
            LOGGER.info("Detected state transition for HA group {} from {} to {} on {} cluster",
                    haGroupName, oldState, newState, clusterType);
            notifySubscribers(oldState, newState, newStat.getMtime(), clusterType);
        } else if (oldState == null) {
            LOGGER.debug("Initial state detected for HA group {} as {} on {} cluster",
                    haGroupName, newState, clusterType);
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
                                   ClusterType clusterType) {
        LOGGER.debug("Notifying subscribers of state transition "
                        + "for HA group {} from {} to {} on {} cluster",
                haGroupName, fromState, toState, clusterType);

        // Create keys for both subscription types
        String specificTransitionKey = buildTransitionKey(clusterType, fromState, toState);
        String targetStateKey = buildTargetStateKey(clusterType, toState);

        // Collect all listeners that need to be notified
        Set<HAGroupStateListener> listenersToNotify = new HashSet<>();

        // Find specific transition subscribers
        CopyOnWriteArraySet<HAGroupStateListener> specificListeners
                = specificTransitionSubscribers.get(specificTransitionKey);
        if (specificListeners != null) {
            listenersToNotify.addAll(specificListeners);
        }

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
                    listener.onStateChange(haGroupName,
                            fromState, toState, modifiedTime, clusterType);
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
     * Build key for specific transition subscriptions.
     */
    private String buildTransitionKey(ClusterType clusterType,
                                      HAGroupState fromState,
                                      HAGroupState toState) {
        return clusterType + ":" + fromState + ":" + toState;
    }

    /**
     * Build key for target state subscriptions.
     */
    private String buildTargetStateKey(ClusterType clusterType,
                                       HAGroupState targetState) {
        return clusterType + ":" + targetState;
    }

}