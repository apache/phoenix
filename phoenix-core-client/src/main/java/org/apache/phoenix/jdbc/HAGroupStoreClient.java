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
import java.util.List;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;


/**
 * Write-through cache for HAGroupStore.
 * Uses {@link PathChildrenCache} from {@link org.apache.curator.framework.CuratorFramework}.
 */
public class HAGroupStoreClient implements Closeable {

    private static final long HA_GROUP_STORE_CLIENT_INITIALIZATION_TIMEOUT_MS = 30000L;
    private static volatile HAGroupStoreClient haGroupStoreClientInstance;
    private PhoenixHAAdmin phoenixHaAdmin;
    private static final Logger LOGGER = LoggerFactory.getLogger(HAGroupStoreClient.class);
    // Map contains <ClusterRole, Map<HAGroupName(String), HAGroupStore>>
    private final ConcurrentHashMap<HAGroupStore.ClusterRole, ConcurrentHashMap<String, HAGroupStore>> clusterRoleToHAGroupStoreMap
            = new ConcurrentHashMap<>();
    private PathChildrenCache pathChildrenCache;
    private volatile boolean isHealthy;

    /**
     * Creates/gets an instance of HAGroupStoreClient.
     * Can return null instance if unable to initialize.
     *
     * @param conf configuration
     * @return HAGroupStoreClient instance
     */
    public static HAGroupStoreClient getInstance(Configuration conf) {
        if (haGroupStoreClientInstance == null || !haGroupStoreClientInstance.isHealthy) {
            synchronized (HAGroupStoreClient.class) {
                if (haGroupStoreClientInstance == null || !haGroupStoreClientInstance.isHealthy) {
                    haGroupStoreClientInstance = new HAGroupStoreClient(conf, null);
                    if (!haGroupStoreClientInstance.isHealthy) {
                        haGroupStoreClientInstance.close();
                        haGroupStoreClientInstance = null;
                    }
                }
            }
        }
        return haGroupStoreClientInstance;
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
                    HAGroupStore eventHAGroupStore = extractHAGroupStoreOrNull(childData);
                    switch (event.getType()) {
                        case CHILD_ADDED:
                        case CHILD_UPDATED:
                            if (eventHAGroupStore != null && eventHAGroupStore.getHaGroupName() != null) {
                                updateHAGroupStoreMap(eventHAGroupStore);
                            }
                            break;
                        case CHILD_REMOVED:
                            // In case of CHILD_REMOVED, we get the old version of data that was just deleted in event.
                            if (eventHAGroupStore != null && eventHAGroupStore.getHaGroupName() != null
                                    && !eventHAGroupStore.getHaGroupName().isEmpty()
                                    && eventHAGroupStore.getRole(phoenixHaAdmin.getZkUrl()) != null) {
                                LOGGER.info("Received CHILD_REMOVED event, Removing HAGroupStore {} from existing HAGroupStore Map {}", eventHAGroupStore, clusterRoleToHAGroupStoreMap);
                                final HAGroupStore.ClusterRole role = eventHAGroupStore.getRole(phoenixHaAdmin.getZkUrl());
                                clusterRoleToHAGroupStoreMap.putIfAbsent(role, new ConcurrentHashMap<>());
                                clusterRoleToHAGroupStoreMap.get(role).remove(eventHAGroupStore.getHaGroupName());
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
            buildClusterRoleToHAGroupStoreMap();
        } catch (Exception e) {
            isHealthy = false;
            LOGGER.error("Unexpected error occurred while initializing HAGroupStoreClient, marking cache as unhealthy", e);
        }
    }

    private HAGroupStore extractHAGroupStoreOrNull(final ChildData childData) {
        if (childData != null) {
            byte[] data = childData.getData();
            return HAGroupStore.fromJson(data).orElse(null);
        }
        return null;
    }

    private void updateHAGroupStoreMap(final HAGroupStore haGroupStore) {
        if (haGroupStore != null && haGroupStore.getHaGroupName() != null && haGroupStore.getRole(phoenixHaAdmin.getZkUrl()) != null) {
            HAGroupStore.ClusterRole role = haGroupStore.getRole(phoenixHaAdmin.getZkUrl());
            LOGGER.info("Updating Existing HAGroupStore Map {} with new HAGroupStore {}", clusterRoleToHAGroupStoreMap, haGroupStore);
            clusterRoleToHAGroupStoreMap.putIfAbsent(role, new ConcurrentHashMap<>());
            clusterRoleToHAGroupStoreMap.get(role).put(haGroupStore.getHaGroupName(), haGroupStore);
            LOGGER.info("Added new HAGroupStore {} to HAGroupStore Map", haGroupStore);
            // Remove any pre-existing mapping with any other role for this HAGroupName
            for (HAGroupStore.ClusterRole mapRole : clusterRoleToHAGroupStoreMap.keySet()) {
                if (mapRole != role) {
                    ConcurrentHashMap<String, HAGroupStore> roleWiseMap = clusterRoleToHAGroupStoreMap.get(mapRole);
                    if (roleWiseMap.containsKey(haGroupStore.getHaGroupName())) {
                        LOGGER.info("Removing any pre-existing mapping with role {} for HAGroupName {}", mapRole, haGroupStore.getHaGroupName());
                        roleWiseMap.remove(haGroupStore.getHaGroupName());
                    }
                }
            }
            LOGGER.info("Final Updated HAGroupStore Map {}", clusterRoleToHAGroupStoreMap);
        }
    }

    private void buildClusterRoleToHAGroupStoreMap() {
        List<HAGroupStore> haGroupStores = pathChildrenCache.getCurrentData().stream().map(this::extractHAGroupStoreOrNull)
                .filter(Objects::nonNull).collect(Collectors.toList());
        for (HAGroupStore haGroupStore : haGroupStores) {
            updateHAGroupStoreMap(haGroupStore);
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
        buildClusterRoleToHAGroupStoreMap();
        LOGGER.info("Rebuild Complete for HAGroupStoreClient");
    }


    @Override
    public void close() {
        try {
            LOGGER.info("Closing HAGroupStoreClient");
            clusterRoleToHAGroupStoreMap.clear();
            if (pathChildrenCache != null) {
                pathChildrenCache.close();
            }
            LOGGER.info("Closed HAGroupStoreClient");
        } catch (IOException e) {
            LOGGER.error("Exception closing HAGroupStoreClient", e);
        }
    }

    public List<HAGroupStore> getGroupStoresByClusterRole(HAGroupStore.ClusterRole clusterRole) throws IOException {
        if (!isHealthy) {
            throw new IOException("HAGroupStoreClient is not healthy");
        }
        return ImmutableList.copyOf(clusterRoleToHAGroupStoreMap.getOrDefault(clusterRole, new ConcurrentHashMap<>()).values());
    }
}
