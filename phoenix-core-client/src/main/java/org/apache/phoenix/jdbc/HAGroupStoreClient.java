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
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;
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

    public static final String ZK_CONSISTENT_HA_NAMESPACE =
            "phoenix" + ZKPaths.PATH_SEPARATOR + "consistentHA";
    private static final long HA_GROUP_STORE_CLIENT_INITIALIZATION_TIMEOUT_MS = 30000L;
    // Multiplier for ZK session timeout to account for time it will take for HMaster to abort
    // the region server in case ZK connection is lost from the region server.
    private static final double ZK_SESSION_TIMEOUT_MULTIPLIER = 1.1;
    private static final String CACHE_TYPE_LOCAL = "LOCAL";
    private static final String CACHE_TYPE_PEER = "PEER";
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
            String zkUrl) throws SQLException {
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
     * where clause but we need to match the formatted zkUrl with the zkUrl in the system table so
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
            this.phoenixHaAdmin = new PhoenixHAAdmin(this.zkUrl, conf, ZK_CONSISTENT_HA_NAMESPACE);
            // Initialize local cache
            this.pathChildrenCache = initializePathChildrenCache(phoenixHaAdmin,
                    pathChildrenCacheListener, CACHE_TYPE_LOCAL);
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
        return fetchCacheRecord(this.pathChildrenCache, CACHE_TYPE_LOCAL).getLeft();
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
                this.pathChildrenCache, CACHE_TYPE_LOCAL);
        HAGroupStoreRecord currentHAGroupStoreRecord = cacheRecord.getLeft();
        Stat currentHAGroupStoreRecordStat = cacheRecord.getRight();
        if (currentHAGroupStoreRecord == null) {
            throw new IOException("Current HAGroupStoreRecordStat in cache is null, "
                    + "cannot update HAGroupStoreRecord, the record should be initialized "
                    + "in System Table first" + haGroupName);
        }
        if (isUpdateNeeded(currentHAGroupStoreRecord.getHAGroupState(),
                currentHAGroupStoreRecordStat.getMtime(), haGroupState)) {
                HAGroupStoreRecord newHAGroupStoreRecord = new HAGroupStoreRecord(
                        currentHAGroupStoreRecord.getProtocolVersion(),
                        currentHAGroupStoreRecord.getHaGroupName(),
                        haGroupState
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
        return fetchCacheRecord(this.peerPathChildrenCache, CACHE_TYPE_PEER).getLeft();
    }

    private void initializeZNodeIfNeeded() throws IOException,
            StaleHAGroupStoreRecordVersionException {
        // Sometimes the ZNode might not be available in local cache yet, so better to check
        // in ZK directly if we need to initialize
        Pair<HAGroupStoreRecord, Stat> cacheRecordFromZK =
                phoenixHaAdmin.getHAGroupStoreRecordInZooKeeper(this.haGroupName);
        HAGroupStoreRecord haGroupStoreRecord = cacheRecordFromZK.getLeft();
        HAGroupStoreRecord newHAGroupStoreRecord = new HAGroupStoreRecord(
            HAGroupStoreRecord.DEFAULT_PROTOCOL_VERSION,
            haGroupName,
            this.clusterRole.getDefaultHAGroupState()
        );
        // Only update current ZNode if it doesn't have the same role as present in System Table.
        // If not exists, then create ZNode
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
                    || (peerPhoenixHaAdmin != null &&
                            !StringUtils.equals(this.peerZKUrl, peerPhoenixHaAdmin.getZkUrl()))) {
                    // Clean up existing peer connection if it exists
                    closePeerConnection();
                    // Setup new peer connection
                    this.peerPhoenixHaAdmin
                            = new PhoenixHAAdmin(this.peerZKUrl, conf, ZK_CONSISTENT_HA_NAMESPACE);
                    // Create new PeerPathChildrenCache
                    this.peerPathChildrenCache = initializePathChildrenCache(peerPhoenixHaAdmin,
                            this.peerCustomPathChildrenCacheListener, CACHE_TYPE_PEER);
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
            PathChildrenCacheListener customListener, String cacheType) {
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

    private PathChildrenCacheListener createCacheListener(CountDownLatch latch, String cacheType) {
        return (client, event) -> {
            final ChildData childData = event.getData();
            HAGroupStoreRecord eventRecord = extractHAGroupStoreRecordOrNull(childData);
            LOGGER.info("HAGroupStoreClient Cache {} received event {} type {} at {}",
                    cacheType, eventRecord, event.getType(), System.currentTimeMillis());
            switch (event.getType()) {
                // TODO: Add support for event watcher for HAGroupStoreRecord
                // case CHILD_ADDED:
                // case CHILD_UPDATED:
                // case CHILD_REMOVED:
                case INITIALIZED:
                    latch.countDown();
                    break;
                case CONNECTION_LOST:
                case CONNECTION_SUSPENDED:
                    if (CACHE_TYPE_LOCAL.equals(cacheType)) {
                        isHealthy = false;
                    }
                    LOGGER.warn("{} HAGroupStoreClient cache connection lost/suspended",
                            cacheType);
                    break;
                case CONNECTION_RECONNECTED:
                    if (CACHE_TYPE_LOCAL.equals(cacheType)) {
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
            String cacheType) {
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

        if (cacheType.equals(CACHE_TYPE_PEER)) {
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
            String targetPath, String cacheType) {
        ChildData childData = cache.getCurrentData(targetPath);
        if (childData != null) {
            HAGroupStoreRecord record = extractHAGroupStoreRecordOrNull(childData);
            Stat currentStat = childData.getStat();
            LOGGER.info("Built {} cluster record: {}", cacheType, record);
            return Pair.of(record, currentStat);
        }
        return Pair.of(null, null);
    }

    private HAGroupStoreRecord extractHAGroupStoreRecordOrNull(final ChildData childData) {
        if (childData != null) {
            byte[] data = childData.getData();
            return HAGroupStoreRecord.fromJson(data).orElse(null);
        }
        return null;
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

}