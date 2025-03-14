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
import org.apache.curator.framework.recipes.cache.PathChildrenCacheEvent;
import org.apache.curator.utils.ZKPaths;
import org.apache.hadoop.conf.Configuration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.io.IOException;
import java.util.Objects;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

/**
 * Write-through cache for PhoenixHA.
 * Uses {@link PathChildrenCache} from {@link org.apache.curator.framework.CuratorFramework}.
 */
public class PhoenixHACache implements Closeable {

    private static final long HA_CACHE_INITIALIZATION_TIMEOUT_MS = 1000L;
    private static PhoenixHACache cacheInstance;
    private final PhoenixHAAdmin phoenixHaAdmin;
    private static final Logger LOGGER = LoggerFactory.getLogger(PhoenixHACache.class);
    private final Set<String> activeToStandbyHAGroups = ConcurrentHashMap.newKeySet();
    private final PathChildrenCache pathChildrenCache;

    /**
     * Creates/gets an instance of PhoenixHACache.
     *
     * @param conf configuration
     * @return cache
     */
    public static PhoenixHACache getInstance(Configuration conf) throws Exception {
        PhoenixHACache result = cacheInstance;
        if (result == null) {
            synchronized (PhoenixHACache.class) {
                result = cacheInstance;
                if (result == null) {
                    cacheInstance = result = new PhoenixHACache(conf);
                }
            }
        }
        return result;
    }

    private PhoenixHACache(final Configuration conf) throws Exception {
        this.phoenixHaAdmin = new PhoenixHAAdmin(conf);
        final PathChildrenCache pathChildrenCache = new PathChildrenCache(phoenixHaAdmin.getCurator(), ZKPaths.PATH_SEPARATOR, true);
        final CountDownLatch latch = new CountDownLatch(1);
        pathChildrenCache.getListenable().addListener((client, event) -> {
            LOGGER.info("PhoenixHACache PathChildrenCache Received event for type {}", event.getType());
            final ChildData childData = event.getData();
            if (childData != null) {
                if (event.getType() == PathChildrenCacheEvent.Type.CHILD_REMOVED) {
                    final ClusterRoleRecord crr = extractCRROrNull(childData);
                    if (crr != null && crr.getHaGroupName() != null && !crr.getHaGroupName().isEmpty()) {
                        activeToStandbyHAGroups.remove(crr.getHaGroupName());
                    }
                } else if (event.getType() == PathChildrenCacheEvent.Type.CHILD_ADDED
                        || event.getType() == PathChildrenCacheEvent.Type.CHILD_UPDATED) {
                    final ClusterRoleRecord crr = extractCRROrNull(childData);
                    if (crr != null && crr.getHaGroupName() != null) {
                        if (crr.getRole(phoenixHaAdmin.getZkUrl()) == ClusterRoleRecord.ClusterRole.ACTIVE_TO_STANDBY) {
                            activeToStandbyHAGroups.add(crr.getHaGroupName());
                        } else {
                            activeToStandbyHAGroups.remove(crr.getHaGroupName());
                        }
                    }
                } else if (event.getType() == PathChildrenCacheEvent.Type.INITIALIZED) {
                    latch.countDown();
                }
            } else {
                LOGGER.info("PhoenixHACache PathChildrenCache Received event for type {} but ChildData is null", event.getType());
            }
        });
        pathChildrenCache.start();
        this.pathChildrenCache = pathChildrenCache;
        latch.await(HA_CACHE_INITIALIZATION_TIMEOUT_MS, TimeUnit.MILLISECONDS);
        buildActiveToStandbySet();
    }

    private ClusterRoleRecord extractCRROrNull(final ChildData childData) {
        if (childData != null) {
            byte[] data = childData.getData();
            return ClusterRoleRecord.fromJson(data).orElse(null);
        }
        return null;
    }

    private void buildActiveToStandbySet() {
        List<ClusterRoleRecord> clusterRoleRecords = pathChildrenCache.getCurrentData().stream().map(this::extractCRROrNull)
                .filter(Objects::nonNull).collect(Collectors.toList());
        List<String> haGroupNames = clusterRoleRecords.stream().map(ClusterRoleRecord::getHaGroupName).collect(Collectors.toList());
        for (ClusterRoleRecord crr : clusterRoleRecords) {
            ClusterRoleRecord.ClusterRole role = crr.getRole(phoenixHaAdmin.getZkUrl());
            if (role == ClusterRoleRecord.ClusterRole.ACTIVE_TO_STANDBY) {
                activeToStandbyHAGroups.add(crr.getHaGroupName());
            } else {
                activeToStandbyHAGroups.remove(crr.getHaGroupName());
            }
        }
        // In case a CRR is deleted and event is not received,
        // we can remove it from activeToStandbyHAGroups set now
        activeToStandbyHAGroups.retainAll(haGroupNames);
    }

    public void rebuild() throws Exception {
        LOGGER.info("Rebuilding PhoenixHACache for HA groups");
        // NOTE: this is a BLOCKING method.
        // Completely rebuild the internal cache by querying for all needed data
        // WITHOUT generating any events to send to listeners.
        pathChildrenCache.rebuild();
        buildActiveToStandbySet();
        LOGGER.info("Rebuild Complete for PhoenixHACache");
    }


    @Override
    public void close() throws IOException {
        LOGGER.info("Closing PhoenixHACache");
        activeToStandbyHAGroups.clear();
        pathChildrenCache.close();
        LOGGER.info("Closed PhoenixHACache");
    }

    /**
     * Checks if mutation is blocked.
     * @return true if mutation is blocked
     */
    public boolean isClusterInActiveToStandby() {
        return !activeToStandbyHAGroups.isEmpty();
    }
}
