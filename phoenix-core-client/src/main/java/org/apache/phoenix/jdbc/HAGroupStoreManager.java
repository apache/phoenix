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

import org.apache.hadoop.conf.Configuration;
import org.apache.phoenix.exception.InvalidClusterRoleTransitionException;
import org.apache.phoenix.exception.StaleHAGroupStoreRecordVersionException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

import static org.apache.phoenix.jdbc.PhoenixHAAdmin.getLocalZkUrl;
import static org.apache.phoenix.query.QueryServices.CLUSTER_ROLE_BASED_MUTATION_BLOCK_ENABLED;
import static org.apache.phoenix.query.QueryServicesOptions
        .DEFAULT_CLUSTER_ROLE_BASED_MUTATION_BLOCK_ENABLED;

/**
 * Implementation of HAGroupStoreManager that uses HAGroupStoreClient.
 * Manages all HAGroupStoreClient instances.
 */
public class HAGroupStoreManager {
    private static final Logger LOGGER = LoggerFactory.getLogger(HAGroupStoreManager.class);
    private final boolean mutationBlockEnabled;
    private final String zkUrl;
    private final Configuration conf;

    // Singleton instance
    private static HAGroupStoreManager instance = null;

    // This is a singleton class and we want to ensure that only one instance is created.
    public static HAGroupStoreManager getInstance(final Configuration conf) {
        if (instance == null) {
            synchronized (HAGroupStoreManager.class) {
                if (instance == null) {
                    instance = new HAGroupStoreManager(conf);
                }
            }
        }
        return instance;
    }

    private HAGroupStoreManager(final Configuration conf) {
        this.mutationBlockEnabled = conf.getBoolean(CLUSTER_ROLE_BASED_MUTATION_BLOCK_ENABLED,
                DEFAULT_CLUSTER_ROLE_BASED_MUTATION_BLOCK_ENABLED);
        this.zkUrl = getLocalZkUrl(conf);
        this.conf = conf;
    }


    /**
     * Checks whether mutation is blocked or not for a specific HA group.
     *
     * @param haGroupName name of the HA group, null for default HA group which tracks
     *                   all HA groups.
     * @return true if mutation is blocked, false otherwise.
     * @throws IOException when HAGroupStoreClient is not healthy.
     */
    public boolean isMutationBlocked(String haGroupName) throws IOException, SQLException {
        if (mutationBlockEnabled) {
            HAGroupStoreClient haGroupStoreClient = HAGroupStoreClient.getInstance(conf,
                    haGroupName, zkUrl);
            if (haGroupStoreClient != null) {
                return haGroupStoreClient.getHAGroupStoreRecord() != null
                        && haGroupStoreClient.getHAGroupStoreRecord().getClusterRole() != null
                        && haGroupStoreClient.getHAGroupStoreRecord().getClusterRole()
                        .isMutationBlocked();
            }
            throw new IOException("HAGroupStoreClient is not initialized");
        }
        return false;
    }

    /**
     * Force rebuilds the HAGroupStoreClient instance for all HA groups.
     * If any HAGroupStoreClient instance is not created, it will be created.
     * @param broadcastUpdate if true, the update will be broadcasted to all
     *                       regionserver endpoints.
     * @throws Exception in case of an error with dependencies or table.
     */
    public void invalidateHAGroupStoreClient(boolean broadcastUpdate) throws Exception {
        List<String> haGroupNames = HAGroupStoreClient.getHAGroupNames(this.zkUrl);
        List<String> failedHAGroupNames = new ArrayList<>();
        for (String haGroupName : haGroupNames) {
            try {
                invalidateHAGroupStoreClient(haGroupName, broadcastUpdate);
            } catch (Exception e) {
                failedHAGroupNames.add(haGroupName);
                LOGGER.error("Failed to invalidate HAGroupStoreClient for " + haGroupName, e);
            }
        }
        if (!failedHAGroupNames.isEmpty()) {
            throw new IOException("Failed to invalidate HAGroupStoreClient for " +
                    failedHAGroupNames
                    + "successfully invalidated HAGroupStoreClient instance for "
                    + haGroupNames.removeAll(failedHAGroupNames) + " HA groups");
        }
    }

    /**
     * Force rebuilds the HAGroupStoreClient for a specific HA group.
     *
     * @param haGroupName name of the HA group, null for default HA group and tracks all HA groups.
     * @param broadcastUpdate if true, the update will be broadcasted to all
     *                       regionserver endpoints.
     * @throws Exception in case of an error with dependencies or table.
     */
    public void invalidateHAGroupStoreClient(final String haGroupName,
            boolean broadcastUpdate) throws Exception {
        HAGroupStoreClient haGroupStoreClient = HAGroupStoreClient.getInstance(conf,
                haGroupName, zkUrl);
        if (haGroupStoreClient != null) {
            haGroupStoreClient.rebuild(broadcastUpdate);
        } else {
            throw new IOException("HAGroupStoreClient is not initialized");
        }
    }

    /**
     * Returns the HAGroupStoreRecord for a specific HA group.
     *
     * @param haGroupName name of the HA group
     * @return Optional HAGroupStoreRecord for the HA group, can be empty if the HA group
     *        is not found.
     * @throws IOException when HAGroupStoreClient is not healthy.
     */
    public Optional<HAGroupStoreRecord> getHAGroupStoreRecord(final String haGroupName)
            throws IOException, SQLException {
        HAGroupStoreClient haGroupStoreClient = HAGroupStoreClient.getInstance(conf,
                haGroupName, zkUrl);
        if (haGroupStoreClient != null) {
            return Optional.ofNullable(haGroupStoreClient.getHAGroupStoreRecord());
        }
        throw new IOException("HAGroupStoreClient is not initialized");
    }

    /**
     * Sets the HAGroupStoreRecord to StoreAndForward mode in local cluster.
     *
     * @param haGroupName name of the HA group
     * @throws IOException when HAGroupStoreClient is not healthy.
     */
    public void setHAGroupStatusToStoreAndForward(final String haGroupName)
            throws IOException, StaleHAGroupStoreRecordVersionException,
            InvalidClusterRoleTransitionException, SQLException {
        HAGroupStoreClient haGroupStoreClient = HAGroupStoreClient.getInstance(conf,
                haGroupName, zkUrl);
        if (haGroupStoreClient != null) {
            haGroupStoreClient.setHAGroupStatusIfNeeded(
                    HAGroupStoreRecord.HAGroupState.ACTIVE_NOT_IN_SYNC);
        } else {
            throw new IOException("HAGroupStoreClient is not initialized");
        }
    }

    /**
     * Sets the HAGroupStoreRecord to Sync mode in local cluster.
     *
     * @param haGroupName name of the HA group
     * @throws IOException when HAGroupStoreClient is not healthy.
     */
    public void setHAGroupStatusRecordToSync(final String haGroupName)
            throws IOException, StaleHAGroupStoreRecordVersionException,
            InvalidClusterRoleTransitionException, SQLException {
        HAGroupStoreClient haGroupStoreClient = HAGroupStoreClient.getInstance(conf,
                haGroupName, zkUrl);
        if (haGroupStoreClient != null) {
            haGroupStoreClient.setHAGroupStatusIfNeeded(
                    HAGroupStoreRecord.HAGroupState.ACTIVE_IN_SYNC);
        } else {
            throw new IOException("HAGroupStoreClient is not initialized");
        }
    }


    /**
     * Returns the ClusterRoleRecord for the cluster pair.
     * If the peer cluster is not connected or peer cluster is not configured, it will
     * return UNKNOWN for peer cluster.
     * Only implemented by HAGroupStoreManagerImpl.
     *
     * @return ClusterRoleRecord for the cluster pair
     * @throws IOException when HAGroupStoreClient is not healthy.
     */
    public ClusterRoleRecord getClusterRoleRecord(String haGroupName)
            throws IOException, SQLException {
        HAGroupStoreClient haGroupStoreClient = HAGroupStoreClient.getInstance(conf,
                haGroupName, zkUrl);
        if (haGroupStoreClient != null) {
            return haGroupStoreClient.getClusterRoleRecord();
        }
        throw new IOException("HAGroupStoreClient is not initialized");
    }
}