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
import org.apache.curator.utils.ZKPaths;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.thirdparty.com.google.common.collect.ImmutableList;
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

import static org.apache.curator.framework.recipes.cache.PathChildrenCacheEvent.Type.INITIALIZED;


/**
 * Write-through cache for PhoenixHA.
 * Uses {@link PathChildrenCache} from {@link org.apache.curator.framework.CuratorFramework}.
 */
public class HAGroupStoreClient implements Closeable {

    private static final long HA_CACHE_INITIALIZATION_TIMEOUT_MS = 30000L;
    private static HAGroupStoreClient cacheInstance;
    private final PhoenixHAAdmin phoenixHaAdmin;
    private static final Logger LOGGER = LoggerFactory.getLogger(HAGroupStoreClient.class);
    // Map contains <ClusterRole, Map<HAGroupName(String), ClusterRoleRecord>>
    private final ConcurrentHashMap<ClusterRoleRecord.ClusterRole, ConcurrentHashMap<String, ClusterRoleRecord>> clusterRoleToCRRMap
            = new ConcurrentHashMap<>();
    private final PathChildrenCache pathChildrenCache;
    private boolean isHealthy;

    /**
     * Creates/gets an instance of HAGroupStoreClient.
     *
     * @param conf configuration
     * @return cache
     */
    public static HAGroupStoreClient getInstance(Configuration conf) throws Exception {
        HAGroupStoreClient result = cacheInstance;
        if (result == null) {
            synchronized (HAGroupStoreClient.class) {
                result = cacheInstance;
                if (result == null) {
                    cacheInstance = result = new HAGroupStoreClient(conf);
                }
            }
        }
        return result;
    }

    private HAGroupStoreClient(final Configuration conf) throws Exception {
        this.phoenixHaAdmin = new PhoenixHAAdmin(conf);
        final PathChildrenCache pathChildrenCache = new PathChildrenCache(phoenixHaAdmin.getCurator(), ZKPaths.PATH_SEPARATOR, true);
        final CountDownLatch latch = new CountDownLatch(1);
        pathChildrenCache.getListenable().addListener((client, event) -> {
            LOGGER.info("HAGroupStoreClient PathChildrenCache Received event for type {}", event.getType());
            final ChildData childData = event.getData();
            if (childData != null || event.getType() == INITIALIZED) {
                ClusterRoleRecord eventCRR = extractCRROrNull(childData);
                switch (event.getType()) {
                    case CHILD_ADDED:
                    case CHILD_UPDATED:
                        if (eventCRR != null && eventCRR.getHaGroupName() != null) {
                            updateClusterRoleRecordMap(eventCRR);
                        }
                        break;
                    case CHILD_REMOVED:
                        // In case of CHILD_REMOVED, we get the old version of data that was just deleted.
                        if (eventCRR != null && eventCRR.getHaGroupName() != null
                                && !eventCRR.getHaGroupName().isEmpty()
                                && eventCRR.getRole(phoenixHaAdmin.getZkUrl()) != null) {
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
            } else {
                LOGGER.info("HAGroupStoreClient PathChildrenCache Received event for type {} but ChildData is null", event.getType());
            }
        });
        pathChildrenCache.start(PathChildrenCache.StartMode.POST_INITIALIZED_EVENT);
        this.pathChildrenCache = pathChildrenCache;
        isHealthy = latch.await(HA_CACHE_INITIALIZATION_TIMEOUT_MS, TimeUnit.MILLISECONDS);
        buildClusterRoleToCRRMap();
    }

    private ClusterRoleRecord extractCRROrNull(final ChildData childData) {
        if (childData != null) {
            byte[] data = childData.getData();
            return ClusterRoleRecord.fromJson(data).orElse(null);
        }
        return null;
    }

    private void updateClusterRoleRecordMap(final ClusterRoleRecord crr) {
        ClusterRoleRecord.ClusterRole role = crr.getRole(phoenixHaAdmin.getZkUrl());
        if (role != null) {
            clusterRoleToCRRMap.putIfAbsent(role, new ConcurrentHashMap<>());
            clusterRoleToCRRMap.get(role).put(crr.getHaGroupName(), crr);
            // Remove any pre-existing mapping with any other role for this HAGroupName
            for(ClusterRoleRecord.ClusterRole mapRole : clusterRoleToCRRMap.keySet()) {
                if (mapRole != role) {
                    ConcurrentHashMap<String, ClusterRoleRecord> roleWiseMap = clusterRoleToCRRMap.get(mapRole);
                    roleWiseMap.remove(crr.getHaGroupName());
                }
            }
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
        return clusterRoleToCRRMap.getOrDefault(clusterRole, new ConcurrentHashMap<>()).values().stream().collect(ImmutableList.toImmutableList());
    }
}
