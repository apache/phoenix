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


import org.apache.curator.framework.recipes.cache.NodeCache;
import org.apache.hadoop.conf.Configuration;
import org.apache.phoenix.thirdparty.com.google.common.annotations.VisibleForTesting;
import org.apache.phoenix.thirdparty.com.google.common.cache.Cache;
import org.apache.phoenix.thirdparty.com.google.common.cache.CacheBuilder;
import org.apache.phoenix.thirdparty.com.google.common.cache.RemovalListener;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.io.IOException;
import java.time.Duration;
import java.util.HashSet;
import java.util.List;
import java.util.Optional;
import java.util.Set;

import static org.apache.phoenix.jdbc.PhoenixHAAdmin.toPath;

/**
 * Write-through cache for PhoenixHA.
 * Uses {@link NodeCache} from {@link org.apache.curator.framework.CuratorFramework}.
 */
public class PhoenixHACache implements Closeable {

    private static PhoenixHACache cacheInstance;
    private final PhoenixHAAdmin phoenixHaAdmin;
    private Cache<String, NodeCache> haGroupClusterRoleRecordMap = null;
    private static final Logger LOGGER = LoggerFactory.getLogger(PhoenixHACache.class);
    private static final String HA_CACHE_TTL_MS = "phoenix.ha.cache.ttl.ms";
    private static final long DEFAULT_HA_CACHE_TTL_IN_MS = 10*60*1000L; // 10 minutes
    private final Set<String> activeToStandbyHAGroups =new HashSet<>();

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


    @VisibleForTesting
    PhoenixHACache(final Configuration conf) throws Exception {
        this.phoenixHaAdmin = new PhoenixHAAdmin(conf);
        this.haGroupClusterRoleRecordMap = CacheBuilder.newBuilder()
                .expireAfterWrite(Duration.ofMillis(conf.getLong(HA_CACHE_TTL_MS, DEFAULT_HA_CACHE_TTL_IN_MS)))
                .removalListener((RemovalListener<String, NodeCache>) notification -> {
                    assert notification.getKey() != null;
                    String key = notification.getKey().toString();
                    LOGGER.debug("Refreshing " + key + " because of "
                            + notification.getCause().name());
                    NodeCache nodeCache = notification.getValue();
                    if (nodeCache != null) {
                        try {
                            nodeCache.rebuild();
                            haGroupClusterRoleRecordMap.put(key, nodeCache);
                            checkAndUpdateMutationBlockingHAGroups(nodeCache);
                        } catch (Exception e) {
                            LOGGER.error("Failed to rebuild NodeCache for haGroup " + key, e);
                            throw new RuntimeException(e);
                        }
                    }
                })
                .build();
        initializeHACache();
    }


    /**
     * Checks if mutation is blocked.
     * @return true if mutation is blocked
     */
    public boolean isAnyClusterInActiveToStandby() {
        return !activeToStandbyHAGroups.isEmpty();
    }

    private void initializeHACache() throws Exception {
        activeToStandbyHAGroups.clear();
        List<ClusterRoleRecord> clusterRoleRecords = phoenixHaAdmin.listAllClusterRoleRecordsOnZookeeper();
        for (ClusterRoleRecord clusterRoleRecord : clusterRoleRecords) {
            final String haGroupName = clusterRoleRecord.getHaGroupName();
            try(NodeCache nodeCache = new NodeCache(phoenixHaAdmin.getCurator(), toPath(haGroupName))){
                nodeCache.start();
                haGroupClusterRoleRecordMap.put(haGroupName, nodeCache);
                checkAndUpdateMutationBlockingHAGroups(nodeCache);
            }
        }
    }


    private void checkAndUpdateMutationBlockingHAGroups(final NodeCache nodeCache) {
        Optional<ClusterRoleRecord> clusterRoleRecordOptional = ClusterRoleRecord.fromJson(nodeCache.getCurrentData().getData());
        clusterRoleRecordOptional.ifPresent(clusterRoleRecord -> {
            final ClusterRoleRecord.ClusterRole clusterRole = clusterRoleRecord.getRole(phoenixHaAdmin.getZkUrl());
            if (clusterRole == ClusterRoleRecord.ClusterRole.ACTIVE_TO_STANDBY) {
                activeToStandbyHAGroups.add(clusterRoleRecord.getHaGroupName());
            } else {
                activeToStandbyHAGroups.remove(clusterRoleRecord.getHaGroupName());
            }
        });
    }

    @Override
    public void close() throws IOException {
        for(NodeCache nodeCache: haGroupClusterRoleRecordMap.asMap().values()) {
            if(nodeCache != null) {
                nodeCache.close();
            }
        }
        LOGGER.debug("Closing PhoenixHACache");
    }
}
