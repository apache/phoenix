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
import org.apache.hadoop.hbase.util.Pair;
import org.apache.phoenix.exception.InvalidClusterRoleTransitionException;
import org.apache.phoenix.exception.SQLExceptionCode;
import org.apache.phoenix.exception.SQLExceptionInfo;
import org.apache.phoenix.exception.StaleHAGroupStoreRecordVersionException;
import org.apache.phoenix.jdbc.ClusterRoleRecord.ClusterRole;
import org.apache.phoenix.jdbc.HAGroupStoreRecord.HAGroupState;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;

import static org.apache.phoenix.jdbc.PhoenixHAAdmin.getLocalZkUrl;
import static org.apache.phoenix.query.QueryServices.CLUSTER_ROLE_BASED_MUTATION_BLOCK_ENABLED;
import static org.apache.phoenix.query.QueryServices.HA_GROUP_STALE_FOR_MUTATION_CHECK_ENABLED;
import static org.apache.phoenix.query.QueryServicesOptions
        .DEFAULT_CLUSTER_ROLE_BASED_MUTATION_BLOCK_ENABLED;
import static org.apache.phoenix.query.QueryServicesOptions.DEFAULT_HA_GROUP_STALE_FOR_MUTATION_CHECK_ENABLED;

/**
 * Implementation of HAGroupStoreManager that uses HAGroupStoreClient.
 * Manages all HAGroupStoreClient instances.
 * Supports multiple instances per ZK URL to handle multiple MiniClusters.
 */
public class HAGroupStoreManager {
    private static final Logger LOGGER = LoggerFactory.getLogger(HAGroupStoreManager.class);
    
    // Map of <ZKUrl, HAGroupStoreManagerInstance> for different MiniClusters
    // Can revert this but will fail for tests with one cluster down
    private static final Map<String, HAGroupStoreManager> instances = new ConcurrentHashMap<>();
    
    private final boolean mutationBlockEnabled;
    private final boolean checkStaleCRRForEveryMutation;
    private final String zkUrl;
    private final Configuration conf;

    /**
     * Creates/gets an instance of HAGroupStoreManager.
     * Use the ZK URL from the configuration to determine the instance.
     *
     * @param conf configuration
     * @return HAGroupStoreManager instance
     */
    public static HAGroupStoreManager getInstance(final Configuration conf) {
        return getInstanceForZkUrl(conf, null);
    }

    /**
     * Creates/gets an instance of HAGroupStoreManager for a specific ZK URL.
     * This allows different region servers to have their own instances.
     *
     * @param conf configuration
     * @param zkUrl specific ZK URL to use, null to use local ZK URL from config
     * @return HAGroupStoreManager instance for the specified ZK URL
     */
    public static HAGroupStoreManager getInstanceForZkUrl(final Configuration conf, String zkUrl) {
        String localZkUrl = Objects.toString(zkUrl, getLocalZkUrl(conf));
        Objects.requireNonNull(localZkUrl, "zkUrl cannot be null");
        
        return instances.computeIfAbsent(localZkUrl, url -> {
            LOGGER.info("Creating new HAGroupStoreManager instance for ZK URL: {}", url);
            return new HAGroupStoreManager(conf, url);
        });
    }

    private HAGroupStoreManager(final Configuration conf) {
        this(conf, getLocalZkUrl(conf));
    }

    private HAGroupStoreManager(final Configuration conf, final String zkUrl) {
        this.mutationBlockEnabled = conf.getBoolean(CLUSTER_ROLE_BASED_MUTATION_BLOCK_ENABLED,
                DEFAULT_CLUSTER_ROLE_BASED_MUTATION_BLOCK_ENABLED);
        this.checkStaleCRRForEveryMutation = conf.getBoolean(HA_GROUP_STALE_FOR_MUTATION_CHECK_ENABLED,
                DEFAULT_HA_GROUP_STALE_FOR_MUTATION_CHECK_ENABLED);
        this.zkUrl = zkUrl;
        this.conf = conf;
        LOGGER.info("Started HAGroupStoreManager with ZK URL: {}", zkUrl);
    }

    /**
     * Returns the list of all HA group names.
     * @return list of all HA group names.
     * @throws SQLException if there is an error with querying the table.
     */
    public List<String> getHAGroupNames() throws SQLException {
        return HAGroupStoreClient.getHAGroupNames(this.zkUrl);
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
            HAGroupStoreClient haGroupStoreClient = HAGroupStoreClient.getInstanceForZkUrl(conf,
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

    public boolean isHAGroupOnClientStale(String haGroupName) throws IOException, SQLException {
        if (checkStaleCRRForEveryMutation) {
            HAGroupStoreClient haGroupStoreClient = HAGroupStoreClient.getInstanceForZkUrl(conf,
                    haGroupName, zkUrl);
            if (haGroupStoreClient != null) {
                //If local cluster is not ACTIVE/ACTIVE_TO_STANDBY, it means the Failover CRR is stale on client
                //As they are trying to write/read from a STANDBY cluster.
                return haGroupStoreClient.getPolicy() == HighAvailabilityPolicy.FAILOVER && 
                    !haGroupStoreClient.getHAGroupStoreRecord().getHAGroupState().getClusterRole().isActive();
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
        HAGroupStoreClient haGroupStoreClient = HAGroupStoreClient.getInstanceForZkUrl(conf,
                haGroupName, zkUrl);
        if (haGroupStoreClient != null) {
            haGroupStoreClient.rebuild();
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
        HAGroupStoreClient haGroupStoreClient = HAGroupStoreClient.getInstanceForZkUrl(conf,
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
        HAGroupStoreClient haGroupStoreClient = HAGroupStoreClient.getInstanceForZkUrl(conf,
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
        HAGroupStoreClient haGroupStoreClient = HAGroupStoreClient.getInstanceForZkUrl(conf,
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
        HAGroupStoreClient haGroupStoreClient = HAGroupStoreClient.getInstanceForZkUrl(conf,
                haGroupName, zkUrl);
        if (haGroupStoreClient != null) {
            return haGroupStoreClient.getClusterRoleRecord();
        }
        //If haGroupStoreClient is null, it means the HA group is not configured in the local cluster throw
        //a SQLException with CLUSTER_ROLE_RECORD_NOT_FOUND error code to make client known that CRR for the
        //given HAGroupName doesn't exist.
        throw new SQLExceptionInfo.Builder(SQLExceptionCode.CLUSTER_ROLE_RECORD_NOT_FOUND)
                .setMessage("HAGroupStoreClient is not initialized")
                .build()
                .buildException();
    }
}