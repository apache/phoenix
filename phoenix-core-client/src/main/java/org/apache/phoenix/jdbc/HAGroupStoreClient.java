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


import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.curator.framework.recipes.cache.ChildData;
import org.apache.curator.framework.recipes.cache.PathChildrenCache;
import org.apache.curator.framework.recipes.cache.PathChildrenCacheListener;
import org.apache.curator.utils.ZKPaths;
import org.apache.hadoop.conf.Configuration;
import org.apache.phoenix.exception.InvalidClusterRoleTransitionException;
import org.apache.phoenix.exception.StaleHAGroupStoreRecordVersionException;
import org.apache.phoenix.thirdparty.com.google.common.annotations.VisibleForTesting;
import org.apache.phoenix.thirdparty.com.google.common.base.Preconditions;
import org.apache.zookeeper.data.Stat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import static org.apache.phoenix.jdbc.PhoenixHAAdmin.getLocalZkUrl;
import static org.apache.phoenix.jdbc.PhoenixHAAdmin.toPath;


/**
 * Main implementation of HAGroupStoreClient with peer support.
 * Write-through cache for HAGroupStore based on {@link HAGroupStoreRecord}.
 * Uses {@link PathChildrenCache} from {@link org.apache.curator.framework.CuratorFramework}.
 */
public class HAGroupStoreClient implements Closeable {

    public static final String ZK_CONSISTENT_HA_NAMESPACE = "phoenix" + ZKPaths.PATH_SEPARATOR + "consistentHA";
    private static final long HA_GROUP_STORE_CLIENT_INITIALIZATION_TIMEOUT_MS = 30000L;
    private static final String CACHE_TYPE_LOCAL = "LOCAL";
    private static final String CACHE_TYPE_PEER = "PEER";
    private PhoenixHAAdmin phoenixHaAdmin;
    private PhoenixHAAdmin peerPhoenixHaAdmin;
    private static final Logger LOGGER = LoggerFactory.getLogger(HAGroupStoreClient.class);
    // Map of <ZKUrl, <HAGroupName, HAGroupStoreClientInstance>>
    private static final Map<String, ConcurrentHashMap<String, HAGroupStoreClient>> instances = new ConcurrentHashMap<>();
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
    // Peer ZK URL for peer cluster and HAGroupName
    private String peerZKUrl = null;
    // Peer Custom Event Listener
    private final PathChildrenCacheListener peerCustomPathChildrenCacheListener;


    /**
     * Creates/gets an instance of HAGroupStoreClient.
     * Can return null instance if unable to initialize.
     *
     * @param conf configuration
     * @param haGroupName name of the HA group. Only specified HA group is tracked.
     * @return HAGroupStoreClient instance
     */
    public static HAGroupStoreClient getInstance(Configuration conf, String haGroupName) {
        Preconditions.checkNotNull(haGroupName, "haGroupName cannot be null");
        final String zkUrl = getLocalZkUrl(conf);
        HAGroupStoreClient result = instances.getOrDefault(zkUrl, new ConcurrentHashMap<>()).getOrDefault(haGroupName, null);
        if (result == null || !result.isHealthy) {
            synchronized (HAGroupStoreClient.class) {
                result = instances.getOrDefault(zkUrl, new ConcurrentHashMap<>()).getOrDefault(haGroupName, null);
                if (result == null || !result.isHealthy) {
                    result = new HAGroupStoreClient(conf, null, null, haGroupName);
                    if (!result.isHealthy) {
                        result.close();
                        result = null;
                    } else {
                        instances.putIfAbsent(zkUrl, new ConcurrentHashMap<>());
                        instances.get(zkUrl).put(haGroupName, result);
                    }
                }
            }
        }
        return result;
    }

    /**
     * Get the list of HAGroupNames from the local quorum.
     *
     * @param conf the configuration
     * @return the list of HAGroupNames
     * @throws IOException in case of unexpected error
     */
    public static List<String> getHAGroupNames(Configuration conf) throws IOException {
        try (PhoenixHAAdmin phoenixHaAdmin = new PhoenixHAAdmin(conf, ZK_CONSISTENT_HA_NAMESPACE)) {
            return phoenixHaAdmin.getHAGroupNames();
        }
    }

    @VisibleForTesting
    HAGroupStoreClient(final Configuration conf, final PathChildrenCacheListener pathChildrenCacheListener,
                       final PathChildrenCacheListener peerPathChildrenCacheListener, final String haGroupName) {
        this.conf = conf;
        this.haGroupName = haGroupName;
        // Custom Event Listener
        this.peerCustomPathChildrenCacheListener = peerPathChildrenCacheListener;
        try {
            // Initialize local cache
            this.phoenixHaAdmin = new PhoenixHAAdmin(conf, ZK_CONSISTENT_HA_NAMESPACE);
            this.pathChildrenCache = initializePathChildrenCache(phoenixHaAdmin, pathChildrenCacheListener, CACHE_TYPE_LOCAL);
            if (this.pathChildrenCache != null) {
                this.isHealthy = true;
                maybeInitializePeerPathChildrenCache(fetchCacheRecord(this.pathChildrenCache, CACHE_TYPE_PEER).getLeft());
            }
        } catch (Exception e) {
            this.isHealthy = false;
            close();
            LOGGER.error("Unexpected error occurred while initializing HAGroupStoreClient, marking cache as unhealthy", e);
        }
    }

    private void maybeInitializePeerPathChildrenCache(HAGroupStoreRecord haGroupStoreRecord) {
        if (haGroupStoreRecord != null
                && StringUtils.isNotBlank(haGroupStoreRecord.getPeerZKUrl())) {
            try {
                String newPeerZKUrl = haGroupStoreRecord.getPeerZKUrl();
                // Setup peer connection if needed (first time or URL changed)
                if (peerPathChildrenCache == null || !StringUtils.equals(newPeerZKUrl, peerZKUrl)) {
                    // Clean up existing peer connection if it exists
                    closePeerConnection();
                    // Setup new peer connection
                    this.peerZKUrl = newPeerZKUrl;
                    this.peerPhoenixHaAdmin
                            = new PhoenixHAAdmin(this.peerZKUrl, conf, ZK_CONSISTENT_HA_NAMESPACE);
                    // Create new PeerPathChildrenCache
                    this.peerPathChildrenCache = initializePathChildrenCache(peerPhoenixHaAdmin,
                            this.peerCustomPathChildrenCacheListener, CACHE_TYPE_PEER);
                }
            } catch (Exception e) {
                closePeerConnection();
                LOGGER.error("Unable to initialize PeerPathChildrenCache for HAGroupStoreClient", e);
                // Don't think we should mark HAGroupStoreClient as unhealthy if
                // peerCache is unhealthy, if needed we can introduce a config to control behavior.
            }
        } else {
            // Close Peer Cache for this HAGroupName if currentClusterRecord is null or peerZKUrl is blank
            closePeerConnection();
            LOGGER.error("Not initializing PeerPathChildrenCache for HAGroupStoreClient with HAGroupName {}", haGroupName);
        }
    }

    private PathChildrenCache initializePathChildrenCache(PhoenixHAAdmin admin, PathChildrenCacheListener customListener, String cacheType) {
        LOGGER.info("Initializing {} PathChildrenCache", cacheType);
        PathChildrenCache newPathChildrenCache = null;
        try {
            newPathChildrenCache = new PathChildrenCache(admin.getCurator(), ZKPaths.PATH_SEPARATOR, true);
            final CountDownLatch latch = new CountDownLatch(1);
            newPathChildrenCache.getListenable().addListener(customListener != null
                    ? customListener
                    : createCacheListener(latch, cacheType, haGroupName));
            newPathChildrenCache.start(PathChildrenCache.StartMode.POST_INITIALIZED_EVENT);
            boolean initialized = latch.await(HA_GROUP_STORE_CLIENT_INITIALIZATION_TIMEOUT_MS, TimeUnit.MILLISECONDS);
            return initialized ? newPathChildrenCache : null;
        } catch (Exception e) {
            if (newPathChildrenCache != null) {
                try {
                    newPathChildrenCache.close();
                } catch (IOException ioe) {
                    LOGGER.error("Failed to close {} PathChildrenCache", cacheType, ioe);
                }
            }
            LOGGER.error("Failed to initialize {} PathChildrenCache", cacheType, e);
            return null;
        }
    }

    private PathChildrenCacheListener createCacheListener(CountDownLatch latch, String cacheType, String haGroupName) {
        return (client, event) -> {
            final ChildData childData = event.getData();
            HAGroupStoreRecord eventRecord = extractHAGroupStoreRecordOrNull(childData);
            LOGGER.info("HAGroupStoreClient Cache {} received event {} type {} at {}", cacheType, eventRecord, event.getType(), System.currentTimeMillis());
            switch (event.getType()) {
                case CHILD_ADDED:
                case CHILD_UPDATED:
                    if (eventRecord != null
                            && eventRecord.getHaGroupName() != null
                            && eventRecord.getHaGroupName().equals(haGroupName)) {
                        maybeInitializePeerPathChildrenCache(eventRecord);
                    }
                    break;
                case CHILD_REMOVED:
                    // In case of CHILD_REMOVED, we get the old version of data that was just deleted in event.
                    if (eventRecord != null && eventRecord.getHaGroupName() != null
                            && !eventRecord.getHaGroupName().isEmpty()
                            && eventRecord.getClusterRole() != null
                            && eventRecord.getHaGroupName().equals(haGroupName)) {
                        if (CACHE_TYPE_LOCAL.equals(cacheType)) {
                            LOGGER.info("Received CHILD_REMOVED event for {}, Closing PeerCache", cacheType);
                            closePeerConnection();
                        }
                    }
                    break;
                case INITIALIZED:
                    latch.countDown();
                    break;
                case CONNECTION_LOST:
                case CONNECTION_SUSPENDED:
                    if (CACHE_TYPE_LOCAL.equals(cacheType)) {
                        isHealthy = false;
                    }
                    LOGGER.warn("{} HAGroupStoreClient cache connection lost/suspended", cacheType);
                    break;
                case CONNECTION_RECONNECTED:
                    if (CACHE_TYPE_LOCAL.equals(cacheType)) {
                        isHealthy = true;
                    }
                    LOGGER.info("{} HAGroupStoreClient cache connection reconnected", cacheType);
                    break;
                default:
                    LOGGER.warn("Unexpected {} event type {}, complete event {}", cacheType, event.getType(), event);
            }
        };
    }


    private Pair<HAGroupStoreRecord, Stat> fetchCacheRecord(PathChildrenCache cache, String cacheType) {
        String targetPath = toPath(this.haGroupName);
        ChildData childData = cache.getCurrentData(targetPath);

        if (childData != null) {
            HAGroupStoreRecord record = extractHAGroupStoreRecordOrNull(childData);
            Stat currentStat = cache.getCurrentData(targetPath).getStat();
            LOGGER.info("Built {} cluster record: {}", cacheType, record);
            return Pair.of(record, currentStat);
        } else {
            LOGGER.info("No record found at path {} for {} cluster", targetPath, cacheType);
            return Pair.of(null, null);
        }
    }

    private HAGroupStoreRecord extractHAGroupStoreRecordOrNull(final ChildData childData) {
        if (childData != null) {
            byte[] data = childData.getData();
            return HAGroupStoreRecord.fromJson(data).orElse(null);
        }
        return null;
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
            peerZKUrl = null;
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
     * Checks if the status is needed to be upserted based on logic in {@link #isUpsertNeeded(HAGroupStoreRecord, ClusterRoleRecord.ClusterRole)}.
     *
     * @param clusterRole the cluster role to set
     * @throws IOException if the client is not healthy or the operation fails
     * @throws StaleHAGroupStoreRecordVersionException if the version is stale
     * @throws InvalidClusterRoleTransitionException
     */
    public void setHAGroupStatusIfNeeded(ClusterRoleRecord.ClusterRole clusterRole)
            throws IOException, StaleHAGroupStoreRecordVersionException, InvalidClusterRoleTransitionException {
        if (!isHealthy) {
            throw new IOException("HAGroupStoreClient is not healthy");
        }
        Pair<HAGroupStoreRecord, Stat> cacheRecord = fetchCacheRecord(this.pathChildrenCache, CACHE_TYPE_LOCAL);
        HAGroupStoreRecord currentHAGroupStoreRecord = cacheRecord.getLeft();
        Stat currentHAGroupStoreRecordStat = cacheRecord.getRight();
        if (isUpsertNeeded(currentHAGroupStoreRecord, clusterRole)) {
            if (currentHAGroupStoreRecord == null) {
                HAGroupStoreRecord recordFromSystemTable = fetchHAGroupStoreRecordFromSystemTable(haGroupName);
                if (recordFromSystemTable == null) {
                    throw new IOException("HAGroupStoreRecord not found in system table for HA group " + haGroupName);
                }
                HAGroupStoreRecord newHAGroupStoreRecord = new HAGroupStoreRecord(
                        recordFromSystemTable.getProtocolVersion(),
                        recordFromSystemTable.getHaGroupName(),
                        clusterRole,
                        1,
                        recordFromSystemTable.getPolicy(),
                        System.currentTimeMillis(),
                        recordFromSystemTable.getPeerZKUrl()
                );
                phoenixHaAdmin.createHAGroupStoreRecordInZooKeeper(newHAGroupStoreRecord);
            } else {
                if (currentHAGroupStoreRecordStat == null) {
                    throw new IOException("Current HAGroupStoreRecordStat in cache is null, cannot update HAGroupStoreRecord " + haGroupName);
                }
                HAGroupStoreRecord newHAGroupStoreRecord = new HAGroupStoreRecord(
                        currentHAGroupStoreRecord.getProtocolVersion(),
                        currentHAGroupStoreRecord.getHaGroupName(),
                        clusterRole,
                        currentHAGroupStoreRecord.getVersion() + 1,
                        currentHAGroupStoreRecord.getPolicy(),
                        System.currentTimeMillis(),
                        currentHAGroupStoreRecord.getPeerZKUrl()
                );
                phoenixHaAdmin.updateHAGroupStoreRecordInZooKeeper(haGroupName,
                        newHAGroupStoreRecord, currentHAGroupStoreRecordStat.getVersion());
            }
        }
    }

    private HAGroupStoreRecord fetchHAGroupStoreRecordFromSystemTable(String haGroupName) {
        //TODO: Get record from System Table
        return null;
    }

    /**
     * Checks if the HAGroupStoreRecord needs to be updated.
     * If the current HAGroupStoreRecord is null, it needs to be created.
     * If the current HAGroupStoreRecord is not null, it needs to be updated if the cluster role is different or the status refresh interval has expired.
     *
     * @param currentHAGroupStoreRecord the HAGroupStoreRecord to check
     * @param newClusterRole the cluster role to check
     * @return true if the HAGroupStoreRecord needs to be updated, false otherwise
     * @throws InvalidClusterRoleTransitionException if the cluster role transition is invalid
     */
    private boolean isUpsertNeeded(HAGroupStoreRecord currentHAGroupStoreRecord, ClusterRoleRecord.ClusterRole newClusterRole) throws InvalidClusterRoleTransitionException {
        return currentHAGroupStoreRecord == null
                || (currentHAGroupStoreRecord.getClusterRole() != null
                && ((System.currentTimeMillis() - currentHAGroupStoreRecord.getLastUpdatedTimeInMs())
                > currentHAGroupStoreRecord.getClusterRole().checkTransitionAndGetWaitTime(newClusterRole, conf)));
    }

}