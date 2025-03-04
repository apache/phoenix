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


import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.api.CuratorWatcher;
import org.apache.curator.framework.api.GetDataBuilder;
import org.apache.curator.framework.recipes.cache.NodeCache;
import org.apache.curator.utils.ZKPaths;
import org.apache.hadoop.conf.Configuration;
import org.apache.phoenix.thirdparty.com.google.common.annotations.VisibleForTesting;
import org.apache.phoenix.thirdparty.com.google.common.cache.Cache;
import org.apache.phoenix.thirdparty.com.google.common.cache.CacheBuilder;
import org.apache.phoenix.thirdparty.com.google.common.cache.RemovalCause;
import org.apache.phoenix.thirdparty.com.google.common.cache.RemovalListener;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.io.IOException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.HashSet;

import static org.apache.phoenix.jdbc.PhoenixHAAdmin.toPath;
import static org.apache.phoenix.query.QueryServices.HA_CACHE_TTL_MS;
import static org.apache.phoenix.query.QueryServicesOptions.DEFAULT_HA_CACHE_TTL_IN_MS;

/**
 * Write-through cache for PhoenixHA.
 * Uses {@link NodeCache} from {@link org.apache.curator.framework.CuratorFramework}.
 */
public class PhoenixHACache implements Closeable {

    private static PhoenixHACache cacheInstance;
    private final PhoenixHAAdmin phoenixHaAdmin;
    private Cache<String, ClusterRoleRecord> haGroupClusterRoleRecordMap = null;
    private static final Logger LOGGER = LoggerFactory.getLogger(PhoenixHACache.class);
    private final Set<String> activeToStandbyHAGroups = new HashSet<>();

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
        this.haGroupClusterRoleRecordMap = CacheBuilder.newBuilder()
                .expireAfterWrite(Duration.ofMillis(conf.getLong(HA_CACHE_TTL_MS, DEFAULT_HA_CACHE_TTL_IN_MS)))
                .removalListener((RemovalListener<String, ClusterRoleRecord>) notification -> {
                    String key = notification.getKey();
                    LOGGER.info("PhoenixHACache HAGroupCRRMap Received event for key {} because of {}", key, notification.getCause().name());
                    if (notification.getCause() == RemovalCause.EXPIRED) {
                        ClusterRoleRecord clusterRoleRecord = notification.getValue();
                        if (clusterRoleRecord != null) {
                            try {
                                clusterRoleRecord = getCurrentValue(clusterRoleRecord);
                                haGroupClusterRoleRecordMap.put(clusterRoleRecord.getHaGroupName(), clusterRoleRecord);
                                checkAndUpdateMutationBlockingHAGroups(clusterRoleRecord);
                            } catch (Exception e) {
                                LOGGER.error("Failed to rebuild NodeCache for haGroupName " + key, e);
                                throw new RuntimeException(e);
                            }
                        }
                    }
                })
                .build();

        // Create a watcher for Parent ZNode Path
        CuratorWatcher parentWatcher = new CuratorWatcher() {
            @Override
            public void process(WatchedEvent event) throws Exception {
                LOGGER.info("PhoenixHACache ParentWatcher Received event for key {} because of {}", event.getPath(), event.getType());
                if (event.getType() == Watcher.Event.EventType.NodeChildrenChanged) {
                    List<String> haGroupNames = phoenixHaAdmin.getCurator().getChildren().usingWatcher(this).forPath(ZKPaths.PATH_SEPARATOR);
                    List<String> existingKeys = new ArrayList<>(haGroupClusterRoleRecordMap.asMap().keySet());
                    if (haGroupNames != null && !haGroupNames.equals(existingKeys)) {
                        rebuild(null);
                    }
                }
            }
        };
        List<String> haGroupNames = phoenixHaAdmin.getCurator().getChildren().usingWatcher(parentWatcher).forPath(ZKPaths.PATH_SEPARATOR);
        rebuild(haGroupNames);
    }

    // Used for tests to perform maintenance operations on the cache
    // For e.g. the expired entries don't trigger removal listener immediately(it is opportunistic)
    @VisibleForTesting
    void cleanupCache() {
        haGroupClusterRoleRecordMap.cleanUp();
    }


    public void rebuild(List<String> haGroupNames) throws Exception {
        LOGGER.info("Rebuilding PhoenixHACache for HA groups");
        haGroupClusterRoleRecordMap.invalidateAll();
        activeToStandbyHAGroups.clear();
        CuratorFramework curatorFramework = phoenixHaAdmin.getCurator();
        if (haGroupNames == null || haGroupNames.isEmpty()) {
            haGroupNames = curatorFramework.getChildren().forPath(ZKPaths.PATH_SEPARATOR);
        }
        for (String haGroupName : haGroupNames) {
            GetDataBuilder builder = curatorFramework.getData();
            CuratorWatcher watcher = new CuratorWatcher() {
                @Override
                public void process(WatchedEvent event) throws Exception {
                    LOGGER.info("PhoenixHACache NodeWatcher Received event for " + event.getPath() + " because of " + event.getType());
                    if (event.getType() != Watcher.Event.EventType.None) {
                        byte[] currentData = builder.usingWatcher(this).forPath(toPath(haGroupName));
                        ClusterRoleRecord clusterRoleRecord = ClusterRoleRecord.fromJson(currentData).orElse(null);
                        if (clusterRoleRecord != null) {
                            haGroupClusterRoleRecordMap.put(clusterRoleRecord.getHaGroupName(), clusterRoleRecord);
                            checkAndUpdateMutationBlockingHAGroups(clusterRoleRecord);
                        }
                    }
                }
            };

            byte[] currentData = builder.usingWatcher(watcher).forPath(toPath(haGroupName));
            ClusterRoleRecord clusterRoleRecord = ClusterRoleRecord.fromJson(currentData).orElse(null);
            if (clusterRoleRecord != null) {
                haGroupClusterRoleRecordMap.put(clusterRoleRecord.getHaGroupName(), clusterRoleRecord);
                checkAndUpdateMutationBlockingHAGroups(clusterRoleRecord);
            } else {
                LOGGER.error("Failed to CRR for load ha group " + haGroupName);
            }
        }
        LOGGER.info("Rebuild Complete for PhoenixHACache for HA groups {}", haGroupClusterRoleRecordMap.asMap().keySet());
    }

    private ClusterRoleRecord getCurrentValue(ClusterRoleRecord clusterRoleRecord) throws Exception {
        return ClusterRoleRecord.fromJson(phoenixHaAdmin.getCurator().getData().forPath(toPath(clusterRoleRecord.getHaGroupName()))).orElse(null);
    }

    private void checkAndUpdateMutationBlockingHAGroups(final ClusterRoleRecord clusterRoleRecord) {
        if (clusterRoleRecord != null) {
            final ClusterRoleRecord.ClusterRole clusterRole = clusterRoleRecord.getRole(phoenixHaAdmin.getZkUrl());
            if (clusterRole == ClusterRoleRecord.ClusterRole.ACTIVE_TO_STANDBY) {
                activeToStandbyHAGroups.add(clusterRoleRecord.getHaGroupName());
            } else {
                activeToStandbyHAGroups.remove(clusterRoleRecord.getHaGroupName());
            }
        }
    }


    @Override
    public void close() throws IOException {
        LOGGER.info("Closing PhoenixHACache");
    }

    /**
     * Checks if mutation is blocked.
     * @return true if mutation is blocked
     */
    public boolean isClusterInActiveToStandby() {
        return !activeToStandbyHAGroups.isEmpty();
    }
}
