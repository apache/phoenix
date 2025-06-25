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


import org.apache.curator.framework.recipes.cache.ChildData;
import org.apache.curator.framework.recipes.cache.PathChildrenCache;
import org.apache.curator.framework.recipes.cache.PathChildrenCacheListener;
import org.apache.curator.utils.ZKPaths;
import org.apache.hadoop.conf.Configuration;
import org.apache.phoenix.thirdparty.com.google.common.annotations.VisibleForTesting;
import org.apache.phoenix.thirdparty.com.google.common.collect.ImmutableList;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import static org.apache.phoenix.jdbc.PhoenixHAAdmin.getLocalZkUrl;


/**
 * Write-through cache for HAGroupStore.
 * Uses {@link PathChildrenCache} from {@link org.apache.curator.framework.CuratorFramework}.
 */
public class HAGroupStoreClient implements Closeable {

    private static final long HA_GROUP_STORE_CLIENT_INITIALIZATION_TIMEOUT_MS = 30000L;
    private PhoenixHAAdmin phoenixHaAdmin;
    private static final Logger LOGGER = LoggerFactory.getLogger(HAGroupStoreClient.class);
    // Map contains <ClusterRole, Map<HAGroupName(String), ClusterRoleRecord>>
    private final ConcurrentHashMap<ClusterRoleRecord.ClusterRole, ConcurrentHashMap<String, ClusterRoleRecord>> clusterRoleToCRRMap
            = new ConcurrentHashMap<>();
    private PathChildrenCache pathChildrenCache;
    private volatile boolean isHealthy;
    private static volatile Map<String, HAGroupStoreClient> INSTANCES = new ConcurrentHashMap<>();

    /**
     * Creates/gets an instance of HAGroupStoreClient.
     * Can return null instance if unable to initialize.
     *
     * @param conf configuration
     * @return HAGroupStoreClient instance
     */
    public static HAGroupStoreClient getInstance(Configuration conf) {
        final String zkUrl = getLocalZkUrl(conf);
        HAGroupStoreClient result = INSTANCES.get(zkUrl);
        if (result == null || !result.isHealthy) {
            synchronized (HAGroupStoreClient.class) {
                result = INSTANCES.get(zkUrl);
                if (result == null || !result.isHealthy) {
                    result = new HAGroupStoreClient(conf, null);
                    if (!result.isHealthy) {
                        result.close();
                        result = null;
                    }
                    INSTANCES.put(zkUrl, result);
                }
            }
        }
        return result;
    }

    @VisibleForTesting
    HAGroupStoreClient(final Configuration conf, final PathChildrenCacheListener pathChildrenCacheListener) {
        try {
            this.phoenixHaAdmin = new PhoenixHAAdmin(conf);
            final PathChildrenCache pathChildrenCache;
                pathChildrenCache = new PathChildrenCache(phoenixHaAdmin.getCurator(), ZKPaths.PATH_SEPARATOR, true);
            final CountDownLatch latch = new CountDownLatch(1);
            if (pathChildrenCacheListener != null) {
                pathChildrenCache.getListenable().addListener(pathChildrenCacheListener);
            } else {
                pathChildrenCache.getListenable().addListener((client, event) -> {
                    LOGGER.info("HAGroupStoreClient PathChildrenCache Received event for type {}", event.getType());
                    final ChildData childData = event.getData();
                    ClusterRoleRecord eventCRR = extractCRROrNull(childData);
                    switch (event.getType()) {
                        case CHILD_ADDED:
                        case CHILD_UPDATED:
                            if (eventCRR != null && eventCRR.getHaGroupName() != null) {
                                updateClusterRoleRecordMap(eventCRR);
                            }
                            break;
                        case CHILD_REMOVED:
                            // In case of CHILD_REMOVED, we get the old version of data that was just deleted in event.
                            if (eventCRR != null && eventCRR.getHaGroupName() != null
                                    && !eventCRR.getHaGroupName().isEmpty()
                                    && eventCRR.getRole(phoenixHaAdmin.getZkUrl()) != null) {
                                LOGGER.info("Received CHILD_REMOVED event, Removing CRR {} from existing CRR Map {}", eventCRR, clusterRoleToCRRMap);
                                final ClusterRoleRecord.ClusterRole role = eventCRR.getRole(phoenixHaAdmin.getZkUrl());
                                clusterRoleToCRRMap.putIfAbsent(role, new ConcurrentHashMap<>());
                                clusterRoleToCRRMap.get(role).remove(eventCRR.getHaGroupName());
                            }
                            break;
                        case INITIALIZED:
                            latch.countDown();
                            break;
                        case CONNECTION_LOST:
                        case CONNECTION_SUSPENDED:
                            isHealthy = false;
                            break;
                        case CONNECTION_RECONNECTED:
                            isHealthy = true;
                            break;
                        default:
                            LOGGER.warn("Unexpected event type {}, complete event {}", event.getType(), event);
                    }
                });
            }
            pathChildrenCache.start(PathChildrenCache.StartMode.POST_INITIALIZED_EVENT);
            this.pathChildrenCache = pathChildrenCache;
            isHealthy = latch.await(HA_GROUP_STORE_CLIENT_INITIALIZATION_TIMEOUT_MS, TimeUnit.MILLISECONDS);
            buildClusterRoleToCRRMap();
        } catch (Exception e) {
            isHealthy = false;
            LOGGER.error("Unexpected error occurred while initializing HAGroupStoreClient, marking cache as unhealthy", e);
        }
    }

    private ClusterRoleRecord extractCRROrNull(final ChildData childData) {
        if (childData != null) {
            byte[] data = childData.getData();
            return ClusterRoleRecord.fromJson(data).orElse(null);
        }
        return null;
    }

    private void updateClusterRoleRecordMap(final ClusterRoleRecord crr) {
        if (crr != null && crr.getHaGroupName() != null && crr.getRole(phoenixHaAdmin.getZkUrl()) != null) {
            ClusterRoleRecord.ClusterRole role = crr.getRole(phoenixHaAdmin.getZkUrl());
            LOGGER.info("Updating Existing CRR Map {} with new CRR {}", clusterRoleToCRRMap, crr);
            clusterRoleToCRRMap.putIfAbsent(role, new ConcurrentHashMap<>());
            clusterRoleToCRRMap.get(role).put(crr.getHaGroupName(), crr);
            LOGGER.info("Added new CRR {} to CRR Map", crr);
            // Remove any pre-existing mapping with any other role for this HAGroupName
            for (ClusterRoleRecord.ClusterRole mapRole : clusterRoleToCRRMap.keySet()) {
                if (mapRole != role) {
                    ConcurrentHashMap<String, ClusterRoleRecord> roleWiseMap = clusterRoleToCRRMap.get(mapRole);
                    if (roleWiseMap.containsKey(crr.getHaGroupName())) {
                        LOGGER.info("Removing any pre-existing mapping with role {} for HAGroupName {}", mapRole, crr.getHaGroupName());
                        roleWiseMap.remove(crr.getHaGroupName());
                    }
                }
            }
            LOGGER.info("Final Updated CRR Map {}", clusterRoleToCRRMap);
        }
    }

    private void buildClusterRoleToCRRMap() {
        List<ClusterRoleRecord> clusterRoleRecords = pathChildrenCache.getCurrentData().stream().map(this::extractCRROrNull)
                .filter(Objects::nonNull).collect(Collectors.toList());
        for (ClusterRoleRecord crr : clusterRoleRecords) {
            updateClusterRoleRecordMap(crr);
        }
    }

    public void rebuild() throws Exception {
        if (!isHealthy) {
            throw new IOException("HAGroupStoreClient is not healthy");
        }
        LOGGER.info("Rebuilding HAGroupStoreClient for HA groups");
        // NOTE: this is a BLOCKING method.
        // Completely rebuild the internal cache by querying for all needed data
        // WITHOUT generating any events to send to listeners.
        pathChildrenCache.rebuild();
        buildClusterRoleToCRRMap();
        LOGGER.info("Rebuild Complete for HAGroupStoreClient");
    }


    @Override
    public void close() {
        try {
            LOGGER.info("Closing HAGroupStoreClient");
            clusterRoleToCRRMap.clear();
            if (pathChildrenCache != null) {
                pathChildrenCache.close();
            }
            LOGGER.info("Closed HAGroupStoreClient");
        } catch (IOException e) {
            LOGGER.error("Exception closing HAGroupStoreClient", e);
        }
    }

    public List<ClusterRoleRecord> getCRRsByClusterRole(ClusterRoleRecord.ClusterRole clusterRole) throws IOException {
        if (!isHealthy) {
            throw new IOException("HAGroupStoreClient is not healthy");
        }
        return ImmutableList.copyOf(clusterRoleToCRRMap.getOrDefault(clusterRole, new ConcurrentHashMap<>()).values());
    }
}
