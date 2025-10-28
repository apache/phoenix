/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.phoenix.jdbc;

import static org.apache.phoenix.jdbc.HAGroupStoreRecord.HAGroupState.ACTIVE_IN_SYNC;
import static org.apache.phoenix.jdbc.HAGroupStoreRecord.HAGroupState.ACTIVE_IN_SYNC_TO_STANDBY;
import static org.apache.phoenix.jdbc.HAGroupStoreRecord.HAGroupState.ACTIVE_NOT_IN_SYNC;
import static org.apache.phoenix.jdbc.HAGroupStoreRecord.HAGroupState.STANDBY;
import static org.apache.phoenix.jdbc.HAGroupStoreRecord.HAGroupState.STANDBY_TO_ACTIVE;
import static org.apache.phoenix.jdbc.PhoenixHAAdmin.getLocalZkUrl;
import static org.apache.phoenix.query.QueryServices.CLUSTER_ROLE_BASED_MUTATION_BLOCK_ENABLED;
import static org.apache.phoenix.query.QueryServices.HA_GROUP_STALE_FOR_MUTATION_CHECK_ENABLED;
import static org.apache.phoenix.query.QueryServicesOptions.DEFAULT_CLUSTER_ROLE_BASED_MUTATION_BLOCK_ENABLED;
import static org.apache.phoenix.query.QueryServicesOptions.DEFAULT_HA_GROUP_STALE_FOR_MUTATION_CHECK_ENABLED;

import java.io.IOException;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import org.apache.hadoop.conf.Configuration;
import org.apache.phoenix.exception.InvalidClusterRoleTransitionException;
import org.apache.phoenix.exception.SQLExceptionCode;
import org.apache.phoenix.exception.SQLExceptionInfo;
import org.apache.phoenix.exception.StaleHAGroupStoreRecordVersionException;
import org.apache.phoenix.jdbc.HAGroupStoreRecord.HAGroupState;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.phoenix.thirdparty.com.google.common.annotations.VisibleForTesting;

/**
 * Implementation of HAGroupStoreManager that uses HAGroupStoreClient. Manages all
 * HAGroupStoreClient instances and provides passthrough functionality for HA group state change
 * notifications. Supports multiple instances per ZK URL to handle multiple MiniClusters.
 */
public class HAGroupStoreManager {
  private static final Logger LOGGER = LoggerFactory.getLogger(HAGroupStoreManager.class);

  // Map of <ZKUrl, HAGroupStoreManagerInstance> for different MiniClusters
  // Can revert this but will fail for tests with one cluster down
  private static Map<String, HAGroupStoreManager> instances = new ConcurrentHashMap<>();

  private final boolean mutationBlockEnabled;
  private final boolean checkStaleCRRForEveryMutation;
  private final String zkUrl;
  private final Configuration conf;
  /**
   * Concurrent set to track HA groups that have already had failover management set up. Prevents
   * duplicate subscriptions for the same HA group. Thread-safe without requiring external
   * synchronization.
   */
  private final Set<String> failoverManagedHAGroups = ConcurrentHashMap.newKeySet();

  /**
   * Functional interface for resolving target local states based on current local state when peer
   * cluster transitions occur.
   */
  @FunctionalInterface
  private interface TargetStateResolver {
    HAGroupStoreRecord.HAGroupState
      determineTarget(HAGroupStoreRecord.HAGroupState currentLocalState);
  }

  /**
   * Static mapping of peer state transitions to local target state resolvers. Defines all supported
   * peer-to-local state transitions for failover management.
   */
  private static final Map<HAGroupStoreRecord.HAGroupState,
    TargetStateResolver> PEER_STATE_TRANSITIONS = createPeerStateTransitions();

  /**
   * Static mapping of local state transitions to local target state resolvers. Defines all
   * supported local-to-local state transitions for failover management.
   */
  private static final Map<HAGroupStoreRecord.HAGroupState,
    TargetStateResolver> LOCAL_STATE_TRANSITIONS = createLocalStateTransitions();

  private static Map<HAGroupStoreRecord.HAGroupState, TargetStateResolver>
    createPeerStateTransitions() {
    Map<HAGroupStoreRecord.HAGroupState, TargetStateResolver> transitions = new HashMap<>();

    // Simple transition (no condition check)
    transitions.put(ACTIVE_IN_SYNC_TO_STANDBY, currentLocal -> STANDBY_TO_ACTIVE);

    // Conditional transitions with state validation
    transitions.put(ACTIVE_IN_SYNC, currentLocal -> {
      if (currentLocal == ACTIVE_IN_SYNC_TO_STANDBY) {
        return HAGroupStoreRecord.HAGroupState.STANDBY;
      }
      if (currentLocal == HAGroupStoreRecord.HAGroupState.DEGRADED_STANDBY) {
        return HAGroupStoreRecord.HAGroupState.STANDBY;
      }
      return null; // No transition
    });

    transitions.put(ACTIVE_NOT_IN_SYNC, currentLocal -> {
      if (currentLocal == ACTIVE_IN_SYNC_TO_STANDBY) {
        return HAGroupStoreRecord.HAGroupState.STANDBY;
      }
      if (currentLocal == HAGroupStoreRecord.HAGroupState.STANDBY) {
        return HAGroupStoreRecord.HAGroupState.DEGRADED_STANDBY;
      }
      return null; // No transition
    });

    transitions.put(HAGroupStoreRecord.HAGroupState.ABORT_TO_STANDBY,
      currentLocal -> currentLocal == ACTIVE_IN_SYNC_TO_STANDBY
        ? HAGroupStoreRecord.HAGroupState.ABORT_TO_ACTIVE_IN_SYNC
        : null);

    return transitions;
  }

  private static Map<HAGroupStoreRecord.HAGroupState, TargetStateResolver>
    createLocalStateTransitions() {
    Map<HAGroupStoreRecord.HAGroupState, TargetStateResolver> transitions = new HashMap<>();
    // Local abort transitions - these are simple transitions (no condition check)
    transitions.put(HAGroupStoreRecord.HAGroupState.ABORT_TO_STANDBY, currentLocal -> STANDBY);
    transitions.put(HAGroupStoreRecord.HAGroupState.ABORT_TO_ACTIVE_IN_SYNC,
      currentLocal -> ACTIVE_IN_SYNC);
    transitions.put(HAGroupStoreRecord.HAGroupState.ABORT_TO_ACTIVE_NOT_IN_SYNC,
      currentLocal -> ACTIVE_NOT_IN_SYNC);
    return transitions;
  }

  /**
   * Creates/gets an instance of HAGroupStoreManager. Use the ZK URL from the configuration to
   * determine the instance.
   * @param conf configuration
   * @return HAGroupStoreManager instance
   */
  public static HAGroupStoreManager getInstance(final Configuration conf) {
    return getInstanceForZkUrl(conf, null);
  }

  /**
   * Creates/gets an instance of HAGroupStoreManager for a specific ZK URL. This allows different
   * region servers to have their own instances.
   * @param conf  configuration
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

  @VisibleForTesting
  HAGroupStoreManager(final Configuration conf) {
    this(conf, getLocalZkUrl(conf));
  }

  private HAGroupStoreManager(final Configuration conf, final String zkUrl) {
    this.mutationBlockEnabled = conf.getBoolean(CLUSTER_ROLE_BASED_MUTATION_BLOCK_ENABLED,
      DEFAULT_CLUSTER_ROLE_BASED_MUTATION_BLOCK_ENABLED);
    this.checkStaleCRRForEveryMutation = conf.getBoolean(HA_GROUP_STALE_FOR_MUTATION_CHECK_ENABLED,
      DEFAULT_HA_GROUP_STALE_FOR_MUTATION_CHECK_ENABLED);
    this.zkUrl = zkUrl;
    this.conf = conf;
    this.failoverManagedHAGroups.clear();
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
   * @param haGroupName name of the HA group, null for default HA group which tracks all HA groups.
   * @return true if mutation is blocked, false otherwise.
   * @throws IOException when HAGroupStoreClient is not healthy.
   */
  public boolean isMutationBlocked(String haGroupName) throws IOException {
    if (mutationBlockEnabled) {
      HAGroupStoreClient haGroupStoreClient =
        getHAGroupStoreClientAndSetupFailoverManagement(haGroupName);
      HAGroupStoreRecord recordWithMetadata = haGroupStoreClient.getHAGroupStoreRecord();
      return recordWithMetadata != null && recordWithMetadata.getClusterRole() != null
        && recordWithMetadata.getClusterRole().isMutationBlocked();
    }
    return false;
  }

  public boolean isHAGroupOnClientStale(String haGroupName) throws IOException {
    if (checkStaleCRRForEveryMutation) {
      HAGroupStoreClient haGroupStoreClient =
        getHAGroupStoreClientAndSetupFailoverManagement(haGroupName);
      // If local cluster is not ACTIVE/ACTIVE_TO_STANDBY, it means the Failover CRR is stale
      // on client as they are trying to write/read from a STANDBY cluster.
      // If we plan to use Standby clusters for scan operations, depends on whether the
      // connection is an SCN connection or not, If a connection is not an SCN connection then
      // Standby cluster can't be used as most recent data is only available through ACTIVE
      // cluster. For SCN connections, future client functionality might allow attempts to
      // open connections to the standby cluster, which would then accept or reject the query
      // based on whether the SCN falls within its consistency point, and will require a change
      // in the logic her, with much bigger change.
      HAGroupStoreRecord hagroupStoreRecord = haGroupStoreClient.getHAGroupStoreRecord();
      return (HighAvailabilityPolicy.valueOf(hagroupStoreRecord.getPolicy())
          == HighAvailabilityPolicy.FAILOVER)
        && !(hagroupStoreRecord.getHAGroupState().getClusterRole().isActive());
    }
    return false;
  }

  /**
   * Force rebuilds the HAGroupStoreClient instance for all HA groups. If any HAGroupStoreClient
   * instance is not created, it will be created.
   * @throws Exception in case of an error with dependencies or table.
   */
  public void invalidateHAGroupStoreClient() throws Exception {
    List<String> haGroupNames = HAGroupStoreClient.getHAGroupNames(this.zkUrl);
    List<String> failedHAGroupNames = new ArrayList<>();
    for (String haGroupName : haGroupNames) {
      try {
        invalidateHAGroupStoreClient(haGroupName);
      } catch (Exception e) {
        failedHAGroupNames.add(haGroupName);
        LOGGER.error("Failed to invalidate HAGroupStoreClient for " + haGroupName, e);
      }
    }
    if (!failedHAGroupNames.isEmpty()) {
      throw new IOException("Failed to invalidate HAGroupStoreClient for " + failedHAGroupNames
        + "successfully invalidated HAGroupStoreClient instance for "
        + haGroupNames.removeAll(failedHAGroupNames) + " HA groups");
    }
  }

  /**
   * Force rebuilds the HAGroupStoreClient for a specific HA group.
   * @param haGroupName name of the HA group, null for default HA group and tracks all HA groups.
   * @throws Exception in case of an error with dependencies or table.
   */
  public void invalidateHAGroupStoreClient(final String haGroupName) throws Exception {
    HAGroupStoreClient haGroupStoreClient =
      getHAGroupStoreClientAndSetupFailoverManagement(haGroupName);
    haGroupStoreClient.rebuild();
  }

  /**
   * Returns the HAGroupStoreRecord for a specific HA group.
   * @param haGroupName name of the HA group
   * @return Optional HAGroupStoreRecord for the HA group, can be empty if the HA group is not
   *         found.
   * @throws IOException when HAGroupStoreClient is not healthy.
   */
  public Optional<HAGroupStoreRecord> getHAGroupStoreRecord(final String haGroupName)
    throws IOException {
    HAGroupStoreClient haGroupStoreClient =
      getHAGroupStoreClientAndSetupFailoverManagement(haGroupName);
    return Optional.ofNullable(haGroupStoreClient.getHAGroupStoreRecord());
  }

  /**
   * Returns the HAGroupStoreRecord for a specific HA group from peer cluster.
   * @param haGroupName name of the HA group
   * @return Optional HAGroupStoreRecord for the HA group from peer cluster can be empty if the HA
   *         group is not found or peer cluster is not available.
   * @throws IOException when HAGroupStoreClient is not healthy.
   */
  public Optional<HAGroupStoreRecord> getPeerHAGroupStoreRecord(final String haGroupName)
    throws IOException {
    HAGroupStoreClient haGroupStoreClient =
      getHAGroupStoreClientAndSetupFailoverManagement(haGroupName);
    return Optional.ofNullable(haGroupStoreClient.getHAGroupStoreRecordFromPeer());
  }

  /**
   * Sets the HAGroupStoreRecord to StoreAndForward mode in local cluster.
   * @param haGroupName name of the HA group
   * @throws StaleHAGroupStoreRecordVersionException if the cached version is invalid, the state
   *                                                 might have been updated by some other RS, check
   *                                                 the state again and retry if the use case still
   *                                                 needs it.
   * @throws InvalidClusterRoleTransitionException   if the transition is invalid, the state might
   *                                                 have been updated by some other RS, check the
   *                                                 state again and retry if the use case still
   *                                                 needs it.
   */
  public void setHAGroupStatusToStoreAndForward(final String haGroupName) throws IOException,
    StaleHAGroupStoreRecordVersionException, InvalidClusterRoleTransitionException, SQLException {
    HAGroupStoreClient haGroupStoreClient =
      getHAGroupStoreClientAndSetupFailoverManagement(haGroupName);
    haGroupStoreClient.setHAGroupStatusIfNeeded(HAGroupStoreRecord.HAGroupState.ACTIVE_NOT_IN_SYNC);
  }

  /**
   * Sets the HAGroupStoreRecord to Sync mode in local cluster.
   * @param haGroupName name of the HA group
   * @throws IOException                             when HAGroupStoreClient is not healthy.
   * @throws StaleHAGroupStoreRecordVersionException if the cached version is invalid, the state
   *                                                 might have been updated by some other RS, check
   *                                                 the state again and retry if the use case still
   *                                                 needs it.
   * @throws InvalidClusterRoleTransitionException   if the transition is invalid, the state might
   *                                                 have been updated by some other RS, check the
   *                                                 state again and retry if the use case still
   *                                                 needs it.
   */
  public void setHAGroupStatusToSync(final String haGroupName) throws IOException,
    StaleHAGroupStoreRecordVersionException, InvalidClusterRoleTransitionException, SQLException {
    HAGroupStoreClient haGroupStoreClient =
      getHAGroupStoreClientAndSetupFailoverManagement(haGroupName);
    HAGroupStoreRecord haGroupStoreRecord = haGroupStoreClient.getHAGroupStoreRecord();
    if (haGroupStoreRecord != null) {
      HAGroupState targetHAGroupState =
        haGroupStoreRecord.getHAGroupState() == HAGroupState.ACTIVE_NOT_IN_SYNC_TO_STANDBY
          ? ACTIVE_IN_SYNC_TO_STANDBY
          : ACTIVE_IN_SYNC;
      haGroupStoreClient.setHAGroupStatusIfNeeded(targetHAGroupState);
    } else {
      throw new IOException("Current HAGroupStoreRecord is null for HA group: " + haGroupName);
    }
  }

  /**
   * Initiates failover on the active cluster by transitioning to the appropriate TO_STANDBY state.
   * Checks current state and transitions to: - ACTIVE_IN_SYNC_TO_STANDBY if currently
   * ACTIVE_IN_SYNC - ACTIVE_NOT_IN_SYNC_TO_STANDBY if currently ACTIVE_NOT_IN_SYNC
   * @param haGroupName name of the HA group
   * @throws IOException                             when HAGroupStoreClient is not healthy.
   * @throws StaleHAGroupStoreRecordVersionException when the version is stale, the state might have
   *                                                 been updated by some other RS, check the state
   *                                                 again and retry if the use case still needs it.
   * @throws InvalidClusterRoleTransitionException   when the transition is not valid, the state
   *                                                 might have been updated by some other RS, check
   *                                                 the state again and retry if the use case still
   *                                                 needs it.
   * @throws SQLException                            when there is an error with the database
   *                                                 operation
   */
  public void initiateFailoverOnActiveCluster(final String haGroupName) throws IOException,
    StaleHAGroupStoreRecordVersionException, InvalidClusterRoleTransitionException, SQLException {
    HAGroupStoreClient haGroupStoreClient =
      getHAGroupStoreClientAndSetupFailoverManagement(haGroupName);

    // Get current state
    HAGroupStoreRecord currentRecord = haGroupStoreClient.getHAGroupStoreRecord();
    if (currentRecord == null) {
      throw new IOException("Current HAGroupStoreRecord is null for HA group: " + haGroupName);
    }

    HAGroupStoreRecord.HAGroupState currentState = currentRecord.getHAGroupState();
    HAGroupStoreRecord.HAGroupState targetState;

    // Determine target state based on current state
    if (currentState == HAGroupStoreRecord.HAGroupState.ACTIVE_IN_SYNC) {
      targetState = HAGroupStoreRecord.HAGroupState.ACTIVE_IN_SYNC_TO_STANDBY;
    } else if (currentState == HAGroupStoreRecord.HAGroupState.ACTIVE_NOT_IN_SYNC) {
      targetState = HAGroupStoreRecord.HAGroupState.ACTIVE_NOT_IN_SYNC_TO_STANDBY;
    } else {
      throw new InvalidClusterRoleTransitionException("Cannot initiate failover from state: "
        + currentState + ". Cluster must be in ACTIVE_IN_SYNC or ACTIVE_NOT_IN_SYNC state.");
    }

    haGroupStoreClient.setHAGroupStatusIfNeeded(targetState);
  }

  /**
   * Sets the HAGroupStoreRecord to abort failover and return to STANDBY in local cluster. This
   * aborts an ongoing failover process by moving the standby cluster to abort state.
   * @param haGroupName name of the HA group
   * @throws IOException                             when HAGroupStoreClient is not healthy.
   * @throws StaleHAGroupStoreRecordVersionException when the version is stale, the state might have
   *                                                 been updated by some other RS, check the state
   *                                                 again and retry if the use case still needs it.
   * @throws InvalidClusterRoleTransitionException   when the transition is not valid, the state
   *                                                 might have been updated by some other RS, check
   *                                                 the state again and retry if the use case still
   *                                                 needs it.
   * @throws SQLException                            when there is an error with the database
   *                                                 operation
   */
  public void setHAGroupStatusToAbortToStandby(final String haGroupName) throws IOException,
    StaleHAGroupStoreRecordVersionException, InvalidClusterRoleTransitionException, SQLException {
    HAGroupStoreClient haGroupStoreClient =
      getHAGroupStoreClientAndSetupFailoverManagement(haGroupName);
    haGroupStoreClient.setHAGroupStatusIfNeeded(HAGroupStoreRecord.HAGroupState.ABORT_TO_STANDBY);
  }

  /**
   * Sets the HAGroupStoreRecord to degrade reader functionality in local cluster. Transitions from
   * STANDBY to DEGRADED_STANDBY_FOR_READER or from DEGRADED_STANDBY_FOR_WRITER to DEGRADED_STANDBY.
   * @param haGroupName name of the HA group
   * @throws IOException                             when HAGroupStoreClient is not healthy.
   * @throws StaleHAGroupStoreRecordVersionException when the version is stale, the state might have
   *                                                 been updated by some other RS, check the state
   *                                                 again and retry if the use case still needs it.
   * @throws InvalidClusterRoleTransitionException   when the transition is not valid, the state
   *                                                 might have been updated by some other RS, check
   *                                                 the state again and retry if the use case still
   *                                                 needs it.
   */
  public void setReaderToDegraded(final String haGroupName) throws IOException,
    StaleHAGroupStoreRecordVersionException, InvalidClusterRoleTransitionException, SQLException {
    HAGroupStoreClient haGroupStoreClient =
      getHAGroupStoreClientAndSetupFailoverManagement(haGroupName);
    HAGroupStoreRecord currentRecord = haGroupStoreClient.getHAGroupStoreRecord();

    if (currentRecord == null) {
      throw new IOException("Current HAGroupStoreRecord is null for HA group: " + haGroupName);
    }
    haGroupStoreClient.setHAGroupStatusIfNeeded(HAGroupStoreRecord.HAGroupState.DEGRADED_STANDBY);
  }

  /**
   * Sets the HAGroupStoreRecord to restore reader functionality in local cluster. Transitions from
   * DEGRADED_STANDBY_FOR_READER to STANDBY or from DEGRADED_STANDBY to DEGRADED_STANDBY_FOR_WRITER.
   * @param haGroupName name of the HA group
   * @throws IOException                             when HAGroupStoreClient is not healthy.
   * @throws StaleHAGroupStoreRecordVersionException when the version is stale, the state might have
   *                                                 been updated by some other RS, check the state
   *                                                 again and retry if the use case still needs it.
   * @throws InvalidClusterRoleTransitionException   when the transition is not valid, the state
   *                                                 might have been updated by some other RS, check
   *                                                 the state again and retry if the use case still
   *                                                 needs it.
   */
  public void setReaderToHealthy(final String haGroupName) throws IOException,
    StaleHAGroupStoreRecordVersionException, InvalidClusterRoleTransitionException, SQLException {
    HAGroupStoreClient haGroupStoreClient =
      getHAGroupStoreClientAndSetupFailoverManagement(haGroupName);
    HAGroupStoreRecord currentRecord = haGroupStoreClient.getHAGroupStoreRecord();

    if (currentRecord == null) {
      throw new IOException("Current HAGroupStoreRecord is null " + "for HA group: " + haGroupName);
    } else if (currentRecord.getHAGroupState() == STANDBY) {
      LOGGER.info("Current HAGroupStoreRecord is already STANDBY for HA group: " + haGroupName);
      return;
    }

    haGroupStoreClient.setHAGroupStatusIfNeeded(HAGroupStoreRecord.HAGroupState.STANDBY);
  }

  /**
   * Returns the ClusterRoleRecord for the cluster pair. If the peer cluster is not connected or
   * peer cluster is not configured, it will return UNKNOWN for peer cluster. Only implemented by
   * HAGroupStoreManagerImpl.
   * @return ClusterRoleRecord for the cluster pair
   */
  public ClusterRoleRecord getClusterRoleRecord(String haGroupName) throws SQLException {
    try {
      HAGroupStoreClient haGroupStoreClient =
        getHAGroupStoreClientAndSetupFailoverManagement(haGroupName);
      return haGroupStoreClient.getClusterRoleRecord();
    } catch (IOException e) {
      // If haGroupStoreClient is null, it means the HA group is not configured in the local cluster
      // throw a SQLException with CLUSTER_ROLE_RECORD_NOT_FOUND error code to make client known
      // that CRR for the given HAGroupName doesn't exist.
      LOGGER.error("Error while getting ClusterRoleRecord for HA group: " + haGroupName, e);
      throw new SQLExceptionInfo.Builder(SQLExceptionCode.CLUSTER_ROLE_RECORD_NOT_FOUND)
        .setMessage("HAGroupStoreClient is not initialized").build().buildException();
    }
  }

  /**
   * Subscribe to be notified when any transition to a target state occurs.
   * @param haGroupName the name of the HA group to monitor
   * @param targetState the target state to watch for
   * @param clusterType whether to monitor local or peer cluster
   * @param listener    the listener to notify when any transition to the target state occurs
   * @throws IOException if unable to get HAGroupStoreClient instance
   */
  public void subscribeToTargetState(String haGroupName,
    HAGroupStoreRecord.HAGroupState targetState, ClusterType clusterType,
    HAGroupStateListener listener) throws IOException {
    HAGroupStoreClient client = getHAGroupStoreClientAndSetupFailoverManagement(haGroupName);
    client.subscribeToTargetState(targetState, clusterType, listener);
    LOGGER.debug(
      "Delegated subscription to target state {} " + "for HA group {} on {} cluster to client",
      targetState, haGroupName, clusterType);
  }

  /**
   * Unsubscribe from target state notifications.
   * @param haGroupName the name of the HA group
   * @param targetState the target state
   * @param clusterType whether monitoring local or peer cluster
   * @param listener    the listener to remove
   */
  public void unsubscribeFromTargetState(String haGroupName,
    HAGroupStoreRecord.HAGroupState targetState, ClusterType clusterType,
    HAGroupStateListener listener) {
    try {
      HAGroupStoreClient client = getHAGroupStoreClientAndSetupFailoverManagement(haGroupName);
      client.unsubscribeFromTargetState(targetState, clusterType, listener);
      LOGGER.debug("Delegated unsubscription from target state {} "
        + "for HA group {} on {} cluster to client", targetState, haGroupName, clusterType);
    } catch (IOException e) {
      LOGGER.warn("HAGroupStoreClient not found for HA group: {} - cannot unsubscribe: {}",
        haGroupName, e.getMessage());
    }
  }

  /**
   * Helper method to get HAGroupStoreClient instance with consistent error handling.
   * @param haGroupName name of the HA group
   * @return HAGroupStoreClient instance for the specified HA group
   * @throws IOException when HAGroupStoreClient is not initialized
   */
  private HAGroupStoreClient getHAGroupStoreClient(final String haGroupName) throws IOException {
    HAGroupStoreClient haGroupStoreClient =
      HAGroupStoreClient.getInstanceForZkUrl(conf, haGroupName, zkUrl);
    if (haGroupStoreClient == null) {
      throw new IOException("HAGroupStoreClient is not initialized for HA group: " + haGroupName);
    }
    return haGroupStoreClient;
  }

  /**
   * Helper method to get HAGroupStoreClient instance and setup failover management. NOTE: As soon
   * as the HAGroupStoreClient is initialized, it will setup the failover management as well.
   * Failover management is only set up once per HA group to prevent duplicate subscriptions.
   * @param haGroupName name of the HA group
   * @return HAGroupStoreClient instance for the specified HA group
   * @throws IOException when HAGroupStoreClient is not initialized
   */
  private HAGroupStoreClient
    getHAGroupStoreClientAndSetupFailoverManagement(final String haGroupName) throws IOException {
    HAGroupStoreClient haGroupStoreClient = getHAGroupStoreClient(haGroupName);
    // Only setup failover management once per HA group using atomic add operation
    if (failoverManagedHAGroups.add(haGroupName)) {
      // add() returns true if the element was not already present
      setupPeerFailoverManagement(haGroupName);
      setupLocalFailoverManagement(haGroupName);
      LOGGER.info("Failover management setup completed for HA group: {}", haGroupName);
    } else {
      LOGGER.debug("Failover management already configured for HA group: {}", haGroupName);
    }

    return haGroupStoreClient;
  }

  // ===== Failover Management Related Methods =====

  public void setupLocalFailoverManagement(String haGroupName) throws IOException {
    HAGroupStoreClient haGroupStoreClient = getHAGroupStoreClient(haGroupName);

    // Generic subscription loop using static local transition mapping
    for (Map.Entry<HAGroupStoreRecord.HAGroupState,
      TargetStateResolver> entry : LOCAL_STATE_TRANSITIONS.entrySet()) {
      subscribeToTargetState(haGroupName, entry.getKey(), ClusterType.LOCAL,
        new FailoverManagementListener(haGroupStoreClient, entry.getValue()));
    }

    LOGGER.info("Setup local failover management for HA group: {} with {} state transitions",
      haGroupName, LOCAL_STATE_TRANSITIONS.size());
  }

  /**
   * Listener implementation for handling peer failover management state transitions. Subscribes to
   * peer state changes and triggers appropriate local state transitions.
   */
  private static class FailoverManagementListener implements HAGroupStateListener {
    private final HAGroupStoreClient client;
    private final TargetStateResolver resolver;

    FailoverManagementListener(HAGroupStoreClient client, TargetStateResolver resolver) {
      this.client = client;
      this.resolver = resolver;
    }

    @Override
    public void onStateChange(String haGroupName, HAGroupState fromState, HAGroupState toState,
      long modifiedTime, ClusterType clusterType, Long lastSyncStateTimeInMs) {
      HAGroupStoreRecord.HAGroupState targetState = null;
      HAGroupStoreRecord.HAGroupState currentLocalState = null;
      // We retry 2 times in case there is some Exception in the setHAGroupStatusIfNeeded method.
      int retries = 2;
      while (retries-- > 0) {
        try {
          // Get current local state
          HAGroupStoreRecord currentRecord = client.getHAGroupStoreRecord();
          if (currentRecord == null) {
            LOGGER.error("Current HAGroupStoreRecord is null for HA group: {} "
              + "in Failover Management, failover may be stalled", haGroupName);
            return;
          }

          // Resolve target state using TargetStateResolver
          currentLocalState = currentRecord.getHAGroupState();
          targetState = resolver.determineTarget(currentLocalState);

          if (targetState == null) {
            return;
          }

          // Execute transition if valid
          client.setHAGroupStatusIfNeeded(targetState);

          LOGGER.info(
            "Failover management transition: peer {} -> {}, " + "local {} -> {} for HA group: {}",
            toState, toState, currentLocalState, targetState, haGroupName);
          return;
        } catch (Exception e) {
          if (isStateAlreadyUpdated(client, haGroupName, targetState)) {
            return;
          }
          LOGGER.error(
            "Failed to set HAGroupStatusIfNeeded for HA group: {} "
              + "in Failover Management, event reaction/failover may be stalled: {}",
            haGroupName, e);
        }
      }
    }
  }

  public void setupPeerFailoverManagement(String haGroupName) throws IOException {
    HAGroupStoreClient haGroupStoreClient = getHAGroupStoreClient(haGroupName);

    // Generic subscription loop using static transition mapping
    for (Map.Entry<HAGroupStoreRecord.HAGroupState,
      TargetStateResolver> entry : PEER_STATE_TRANSITIONS.entrySet()) {
      subscribeToTargetState(haGroupName, entry.getKey(), ClusterType.PEER,
        new FailoverManagementListener(haGroupStoreClient, entry.getValue()));
    }

    LOGGER.info("Setup peer failover management for HA group: {} with {} state transitions",
      haGroupName, PEER_STATE_TRANSITIONS.size());
  }

  /**
   * Checks if the state is already updated in the local cache.
   * @param client      HAGroupStoreClient to check the state for
   * @param haGroupName name of the HA group
   * @param targetState the target state to check against
   * @return true if the state is already updated to targetState, false otherwise
   */
  private static boolean isStateAlreadyUpdated(HAGroupStoreClient client, String haGroupName,
    HAGroupState targetState) {
    try {
      HAGroupStoreRecord currentRecord = client.getHAGroupStoreRecord();
      if (currentRecord != null && currentRecord.getHAGroupState() == targetState) {
        LOGGER.info("HAGroupStoreRecord for HA group {} is already updated"
          + "with state {}, no need to update", haGroupName, targetState);
        return true;
      }
    } catch (IOException e) {
      LOGGER.error("Failed to get HAGroupStoreRecord for HA group: {} "
        + "in Failover Management, event reaction/failover may be stalled", haGroupName, e);
    }
    return false;
  }

}
