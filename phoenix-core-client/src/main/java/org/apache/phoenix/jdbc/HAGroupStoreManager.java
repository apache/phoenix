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
 * Manages all HAGroupStoreClient instances and provides passthrough
 * functionality for HA group state change notifications.
 */
public class HAGroupStoreManager {
    private static volatile HAGroupStoreManager haGroupStoreManagerInstance;
    private static final Logger LOGGER = LoggerFactory.getLogger(HAGroupStoreManager.class);
    private final boolean mutationBlockEnabled;
    private final String zkUrl;
    private final Configuration conf;

    /**
     * Creates/gets an instance of HAGroupStoreManager.
     *
     * @param conf configuration
     * @return HAGroupStoreManager instance
     */
    public static HAGroupStoreManager getInstance(final Configuration conf) {
        if (haGroupStoreManagerInstance == null) {
            synchronized (HAGroupStoreManager.class) {
                if (haGroupStoreManagerInstance == null) {
                    haGroupStoreManagerInstance = new HAGroupStoreManager(conf);
                }
            }
        }
        return haGroupStoreManagerInstance;
    }

    private HAGroupStoreManager(final Configuration conf) {
        this.mutationBlockEnabled = conf.getBoolean(CLUSTER_ROLE_BASED_MUTATION_BLOCK_ENABLED,
                DEFAULT_CLUSTER_ROLE_BASED_MUTATION_BLOCK_ENABLED);
        this.zkUrl = getLocalZkUrl(conf);
        this.conf = conf;
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
            HAGroupStoreClient haGroupStoreClient = getHAGroupStoreClient(haGroupName);
            HAGroupStoreRecord recordWithMetadata
                    = haGroupStoreClient.getHAGroupStoreRecord();
            return recordWithMetadata != null
                    && recordWithMetadata.getClusterRole() != null
                    && recordWithMetadata.getClusterRole()
                    .isMutationBlocked();
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
        HAGroupStoreClient haGroupStoreClient = getHAGroupStoreClient(haGroupName);
        haGroupStoreClient.rebuild();
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
        HAGroupStoreClient haGroupStoreClient = getHAGroupStoreClient(haGroupName);
        return Optional.ofNullable(haGroupStoreClient.getHAGroupStoreRecord());
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
        HAGroupStoreClient haGroupStoreClient = getHAGroupStoreClient(haGroupName);
        haGroupStoreClient.setHAGroupStatusIfNeeded(
                HAGroupStoreRecord.HAGroupState.ACTIVE_NOT_IN_SYNC);
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
        HAGroupStoreClient haGroupStoreClient = getHAGroupStoreClient(haGroupName);
        haGroupStoreClient.setHAGroupStatusIfNeeded(
                HAGroupStoreRecord.HAGroupState.ACTIVE_IN_SYNC);
    }

    /**
     * Sets the HAGroupStoreRecord to degrade reader functionality in local cluster.
     * Transitions from STANDBY to DEGRADED_STANDBY_FOR_READER or from
     * DEGRADED_STANDBY_FOR_WRITER to DEGRADED_STANDBY.
     *
     * @param haGroupName name of the HA group
     * @throws IOException when HAGroupStoreClient is not healthy.
     * @throws InvalidClusterRoleTransitionException when the current state
     *   cannot transition to a degraded reader state
     */
    public void setReaderToDegraded(final String haGroupName)
            throws IOException, StaleHAGroupStoreRecordVersionException,
            InvalidClusterRoleTransitionException, SQLException {
        HAGroupStoreClient haGroupStoreClient = getHAGroupStoreClient(haGroupName);
        HAGroupStoreRecord currentRecord
                = haGroupStoreClient.getHAGroupStoreRecord();

        if (currentRecord == null) {
            throw new IOException("Current HAGroupStoreRecord is null for HA group: "
                    + haGroupName);
        }

        HAGroupStoreRecord.HAGroupState currentState = currentRecord.getHAGroupState();
        HAGroupStoreRecord.HAGroupState targetState
                = HAGroupStoreRecord.HAGroupState.DEGRADED_STANDBY_FOR_READER;

        if (currentState == HAGroupStoreRecord.HAGroupState.DEGRADED_STANDBY_FOR_WRITER) {
            targetState = HAGroupStoreRecord.HAGroupState.DEGRADED_STANDBY;
        }

        haGroupStoreClient.setHAGroupStatusIfNeeded(targetState);
    }

    /**
     * Sets the HAGroupStoreRecord to restore reader functionality in local cluster.
     * Transitions from DEGRADED_STANDBY_FOR_READER to STANDBY or from
     * DEGRADED_STANDBY to DEGRADED_STANDBY_FOR_WRITER.
     *
     * @param haGroupName name of the HA group
     * @throws IOException when HAGroupStoreClient is not healthy.
     * @throws InvalidClusterRoleTransitionException when the current state
     *   cannot transition to a healthy reader state
     */
    public void setReaderToHealthy(final String haGroupName)
            throws IOException, StaleHAGroupStoreRecordVersionException,
            InvalidClusterRoleTransitionException, SQLException {
        HAGroupStoreClient haGroupStoreClient = getHAGroupStoreClient(haGroupName);
        HAGroupStoreRecord currentRecord
                = haGroupStoreClient.getHAGroupStoreRecord();

        if (currentRecord == null) {
            throw new IOException("Current HAGroupStoreRecord is null "
                    + "for HA group: " + haGroupName);
        }

        HAGroupStoreRecord.HAGroupState currentState = currentRecord.getHAGroupState();
        HAGroupStoreRecord.HAGroupState targetState = HAGroupStoreRecord.HAGroupState.STANDBY;

        if (currentState == HAGroupStoreRecord.HAGroupState.DEGRADED_STANDBY) {
            targetState = HAGroupStoreRecord.HAGroupState.DEGRADED_STANDBY_FOR_WRITER;
        }

        haGroupStoreClient.setHAGroupStatusIfNeeded(targetState);
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
        HAGroupStoreClient haGroupStoreClient = getHAGroupStoreClient(haGroupName);
        return haGroupStoreClient.getClusterRoleRecord();
    }

    /**
     * Subscribe to be notified when a specific state transition occurs.
     *
     * @param haGroupName the name of the HA group to monitor
     * @param fromState the state to transition from
     * @param toState the state to transition to
     * @param clusterType whether to monitor local or peer cluster
     * @param listener the listener to notify when the transition occurs
     * @throws SQLException if unable to get HAGroupStoreClient instance
     */
    public void subscribeToTransition(String haGroupName,
                                      HAGroupStoreRecord.HAGroupState fromState,
                                      HAGroupStoreRecord.HAGroupState toState,
                                      ClusterType clusterType,
                                      HAGroupStateListener listener) throws SQLException {
        try {
            HAGroupStoreClient client = getHAGroupStoreClient(haGroupName);
            client.subscribeToTransition(fromState, toState, clusterType, listener);
            LOGGER.debug("Delegated subscription to transition {} -> {} "
                            + "for HA group {} on {} cluster to client",
                    fromState, toState, haGroupName, clusterType);
        } catch (IOException e) {
            throw new SQLException("HAGroupStoreClient not found for HA group: " + haGroupName, e);
        }
    }

    /**
     * Unsubscribe from specific state transition notifications.
     *
     * @param haGroupName the name of the HA group
     * @param fromState the state to transition from
     * @param toState the state to transition to
     * @param clusterType whether monitoring local or peer cluster
     * @param listener the listener to remove
     */
    public void unsubscribeFromTransition(String haGroupName,
                                          HAGroupStoreRecord.HAGroupState fromState,
                                          HAGroupStoreRecord.HAGroupState toState,
                                          ClusterType clusterType,
                                          HAGroupStateListener listener) {
        try {
            HAGroupStoreClient client = getHAGroupStoreClient(haGroupName);
            client.unsubscribeFromTransition(fromState, toState, clusterType, listener);
            LOGGER.debug("Delegated unsubscription from transition {} -> {}"
                            + "for HA group {} on {} cluster to client",
                    fromState, toState, haGroupName, clusterType);
        } catch (IOException | SQLException e) {
            LOGGER.warn("HAGroupStoreClient not found for HA group: {} - cannot unsubscribe: {}",
                       haGroupName, e.getMessage());
        }
    }

    /**
     * Subscribe to be notified when any transition to a target state occurs.
     *
     * @param haGroupName the name of the HA group to monitor
     * @param targetState the target state to watch for
     * @param clusterType whether to monitor local or peer cluster
     * @param listener the listener to notify when any transition to the target state occurs
     * @throws SQLException if unable to get HAGroupStoreClient instance
     */
    public void subscribeToTargetState(String haGroupName,
                                       HAGroupStoreRecord.HAGroupState targetState,
                                       ClusterType clusterType,
                                       HAGroupStateListener listener) throws SQLException {
        try {
            HAGroupStoreClient client = getHAGroupStoreClient(haGroupName);
            client.subscribeToTargetState(targetState, clusterType, listener);
            LOGGER.debug("Delegated subscription to target state {} "
                            + "for HA group {} on {} cluster to client",
                    targetState, haGroupName, clusterType);
        } catch (IOException e) {
            throw new SQLException("HAGroupStoreClient not found for HA group: " + haGroupName, e);
        }
    }

    /**
     * Unsubscribe from target state notifications.
     *
     * @param haGroupName the name of the HA group
     * @param targetState the target state
     * @param clusterType whether monitoring local or peer cluster
     * @param listener the listener to remove
     */
    public void unsubscribeFromTargetState(String haGroupName,
                                           HAGroupStoreRecord.HAGroupState targetState,
                                           ClusterType clusterType,
                                           HAGroupStateListener listener) {
        try {
            HAGroupStoreClient client = getHAGroupStoreClient(haGroupName);
            client.unsubscribeFromTargetState(targetState, clusterType, listener);
            LOGGER.debug("Delegated unsubscription from target state {} "
                            + "for HA group {} on {} cluster to client",
                    targetState, haGroupName, clusterType);
        } catch (IOException | SQLException e) {
            LOGGER.warn("HAGroupStoreClient not found for HA group: {} - cannot unsubscribe: {}",
                       haGroupName, e.getMessage());
        }
    }

    /**
     * Helper method to get HAGroupStoreClient instance with consistent error handling.
     *
     * @param haGroupName name of the HA group
     * @return HAGroupStoreClient instance for the specified HA group
     * @throws IOException when HAGroupStoreClient is not initialized
     * @throws SQLException when unable to get HAGroupStoreClient instance
     */
    private HAGroupStoreClient getHAGroupStoreClient(final String haGroupName)
            throws IOException, SQLException {
        HAGroupStoreClient haGroupStoreClient = HAGroupStoreClient.getInstanceForZkUrl(conf,
                haGroupName, zkUrl);
        if (haGroupStoreClient == null) {
            throw new IOException("HAGroupStoreClient is not initialized "
                    + "for HA group: " + haGroupName);
        }
        return haGroupStoreClient;
    }
}