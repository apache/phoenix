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

import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.text.SimpleDateFormat;
import java.util.Arrays;
import java.util.Date;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.stream.Collectors;

import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.phoenix.exception.StaleHAGroupStoreRecordVersionException;
import org.apache.phoenix.jdbc.ClusterRoleRecord.ClusterRole;
import org.apache.phoenix.jdbc.HAGroupStoreRecord.HAGroupState;
import org.apache.phoenix.jdbc.PhoenixHAAdmin.HighAvailibilityCuratorProvider;
import org.apache.phoenix.thirdparty.com.google.common.annotations.VisibleForTesting;
import org.apache.phoenix.thirdparty.org.apache.commons.cli.CommandLine;
import org.apache.phoenix.thirdparty.org.apache.commons.cli.DefaultParser;
import org.apache.phoenix.thirdparty.org.apache.commons.cli.Option;
import org.apache.phoenix.thirdparty.org.apache.commons.cli.Options;
import org.apache.zookeeper.data.Stat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.apache.phoenix.jdbc.HAGroupStoreClient.ZK_CONSISTENT_HA_GROUP_RECORD_NAMESPACE;
import static org.apache.phoenix.jdbc.PhoenixDatabaseMetaData.CLUSTER_ROLE_1;
import static org.apache.phoenix.jdbc.PhoenixDatabaseMetaData.CLUSTER_ROLE_2;
import static org.apache.phoenix.jdbc.PhoenixDatabaseMetaData.CLUSTER_URL_1;
import static org.apache.phoenix.jdbc.PhoenixDatabaseMetaData.CLUSTER_URL_2;
import static org.apache.phoenix.jdbc.PhoenixDatabaseMetaData.HA_GROUP_NAME;
import static org.apache.phoenix.jdbc.PhoenixDatabaseMetaData.POLICY;
import static org.apache.phoenix.jdbc.PhoenixDatabaseMetaData.SYSTEM_HA_GROUP_NAME;
import static org.apache.phoenix.jdbc.PhoenixDatabaseMetaData.VERSION;
import static org.apache.phoenix.jdbc.PhoenixDatabaseMetaData.ZK_URL_1;
import static org.apache.phoenix.jdbc.PhoenixDatabaseMetaData.ZK_URL_2;
import static org.apache.phoenix.jdbc.PhoenixHAAdmin.getLocalZkUrl;
import static org.apache.phoenix.util.PhoenixRuntime.JDBC_PROTOCOL_SEPARATOR;
import static org.apache.phoenix.util.PhoenixRuntime.JDBC_PROTOCOL_ZK;

/**
 * Command-line tool to manage HAGroupStoreRecord configurations.
 * Updates both ZooKeeper and System.HA_GROUP table atomically.
 * Requires admin version increment for all updates.
 */
public class PhoenixHAAdminTool extends Configured implements Tool {

    // Return codes
    public static final int RET_SUCCESS = 0;
    public static final int RET_ARGUMENT_ERROR = 1;
    public static final int RET_UPDATE_ERROR = 2;
    public static final int RET_VERSION_CONFLICT = 3;
    public static final int RET_VALIDATION_ERROR = 4;

    private static final Logger LOG = LoggerFactory.getLogger(PhoenixHAAdminTool.class);

    // Commands
    private static final String CMD_UPDATE = "update";
    private static final String CMD_GET = "get";
    private static final String CMD_LIST = "list";
    private static final String CMD_INITIATE_FAILOVER = "initiate-failover";
    private static final String CMD_ABORT_FAILOVER = "abort-failover";
    private static final String CMD_GET_CLUSTER_ROLE_RECORD = "get-cluster-role-record";

    // Common options
    private static final Option HELP_OPT =
        new Option("h", "help", false, "Show help");

    private static final Option HA_GROUP_OPT =
        new Option("g", "ha-group", true, "HA group name (REQUIRED)");

    // Version options (mutually exclusive)
    private static final Option ADMIN_VERSION_OPT =
        new Option("v", "admin-version", true,
            "Explicit admin version (REQUIRED unless using --auto-increment-version)");

    private static final Option AUTO_INCREMENT_VERSION_OPT =
        new Option("av", "auto-increment-version", false,
            "Auto-increment admin version (WARNING: may overwrite concurrent updates)");

    // Configuration field options
    private static final Option POLICY_OPT =
        new Option("p", "policy", true, "HA policy (e.g., FAILOVER)");

    private static final Option STATE_OPT =
        new Option("s", "state", true, "HA group state (requires --force)");

    private static final Option CLUSTER_URL_OPT =
        new Option("c", "cluster-url", true, "Local cluster URL");

    private static final Option PEER_CLUSTER_URL_OPT =
        new Option("pc", "peer-cluster-url", true, "Peer cluster URL");

    private static final Option PEER_ZK_URL_OPT =
        new Option("pz", "peer-zk-url", true, "Peer ZK URL");

    private static final Option PROTOCOL_VERSION_OPT =
        new Option("pv", "protocol-version", true, "Protocol version (default: 1.0)");

    private static final Option LAST_SYNC_TIME_OPT =
        new Option("lst", "last-sync-time", true,
            "Last sync time in milliseconds (requires --force)");

    // Control flags
    private static final Option FORCE_OPT =
        new Option("F", "force", false,
            "Allow haGroupState and lastSyncTime changes");

    private static final Option DRY_RUN_OPT =
        new Option("d", "dry-run", false,
            "Validate and show changes without applying");

    private static final Option TIMEOUT_OPT =
        new Option("t", "timeout", true,
            "Timeout in seconds to wait for state transitions (default: 120)");

    @Override
    public int run(String[] args) throws Exception {
        if (args.length == 0) {
            printUsage();
            return RET_ARGUMENT_ERROR;
        }

        String command = args[0].toLowerCase();
        String[] commandArgs = Arrays.copyOfRange(args, 1, args.length);

        switch (command) {
        case CMD_UPDATE:
            return executeUpdate(commandArgs);
        case CMD_GET:
            return executeGet(commandArgs);
        case CMD_LIST:
            return executeList(commandArgs);
        case CMD_INITIATE_FAILOVER:
            // TODO: If timeout exceeds, we can also abort the failover automatically.
            return executeInitiateFailover(commandArgs);
        case CMD_ABORT_FAILOVER:
            return executeAbortFailover(commandArgs);
        case CMD_GET_CLUSTER_ROLE_RECORD:
            return executeGetClusterRoleRecord(commandArgs);
        default:
            System.err.println("Unknown command: " + command);
            printUsage();
            return RET_ARGUMENT_ERROR;
        }
    }

    /**
     * Execute UPDATE command
     */
    private int executeUpdate(String[] args) throws Exception {
        try {
            CommandLine cmdLine = new DefaultParser().parse(createUpdateOptions(), args);

            if (cmdLine.hasOption(HELP_OPT.getOpt())) {
                printUpdateHelp();
                return RET_SUCCESS;
            }

            // Parse required parameters
            String haGroupName = getRequiredOption(cmdLine, HA_GROUP_OPT, "HA group name");

            // Parse version (mutually exclusive)
            boolean autoIncrement = cmdLine.hasOption(AUTO_INCREMENT_VERSION_OPT.getOpt());
            String adminVersionStr = cmdLine.getOptionValue(ADMIN_VERSION_OPT.getOpt());

            if (autoIncrement && adminVersionStr != null) {
                throw new IllegalArgumentException(
                    "Cannot use both --admin-version and --auto-increment-version");
            }
            if (!autoIncrement && adminVersionStr == null) {
                throw new IllegalArgumentException(
                    "Must provide either --admin-version or --auto-increment-version");
            }

            // Parse configuration fields
            String policy = cmdLine.getOptionValue(POLICY_OPT.getOpt());
            String state = cmdLine.getOptionValue(STATE_OPT.getOpt());
            String clusterUrl = cmdLine.getOptionValue(CLUSTER_URL_OPT.getOpt());
            String peerClusterUrl = cmdLine.getOptionValue(PEER_CLUSTER_URL_OPT.getOpt());
            String peerZkUrl = cmdLine.getOptionValue(PEER_ZK_URL_OPT.getOpt());
            String protocolVersion = cmdLine.getOptionValue(PROTOCOL_VERSION_OPT.getOpt());
            String lastSyncTimeStr = cmdLine.getOptionValue(LAST_SYNC_TIME_OPT.getOpt());

            // Parse flags
            final boolean force = cmdLine.hasOption(FORCE_OPT.getOpt());
            final boolean dryRun = cmdLine.hasOption(DRY_RUN_OPT.getOpt());

            // Validate restrictions
            if ((state != null || lastSyncTimeStr != null) && !force) {
                throw new IllegalArgumentException(
                    "haGroupState and lastSyncTime changes require --force "
                            + "flag as it supposed to be auto-managed");
            }

            // Check at least one field is being updated
            if (policy == null && state == null && clusterUrl == null
                    && peerClusterUrl == null && peerZkUrl == null
                    && protocolVersion == null && lastSyncTimeStr == null) {
                throw new IllegalArgumentException(
                    "Must specify at least one field to update");
            }

            // Determine version
            long adminVersion;
            if (autoIncrement) {
                showAutoIncrementWarning();
                adminVersion = readCurrentVersionAndIncrement(haGroupName);
            } else {
                adminVersion = Long.parseLong(adminVersionStr);
            }

            // Build update object
            HAGroupStoreConfigUpdate update = new HAGroupStoreConfigUpdate(
                haGroupName, protocolVersion, policy, clusterUrl, peerClusterUrl,
                peerZkUrl, adminVersion, parseState(state), parseLong(lastSyncTimeStr));

            // Execute update
            return performUpdate(haGroupName, update, force, dryRun);

        } catch (StaleHAGroupStoreRecordVersionException e) {
            System.err.println(
                    "\n✗ Version conflict - ZK record was modified by another process");
            System.err.println("   Re-run the command to retry");
            return RET_VERSION_CONFLICT;

        } catch (IllegalArgumentException e) {
            System.err.println("\n✗ Invalid argument: " + e.getMessage());
            return RET_ARGUMENT_ERROR;

        } catch (ValidationException e) {
            System.err.println("\n✗ Validation error: " + e.getMessage());
            return RET_VALIDATION_ERROR;

        } catch (Exception e) {
            System.err.println("\n✗ Update failed: " + e.getMessage());
            e.printStackTrace();
            return RET_UPDATE_ERROR;
        }
    }

    /**
     * Execute GET command
     */
    private int executeGet(String[] args) throws Exception {
        try {
            CommandLine cmdLine = new DefaultParser().parse(createGetOptions(), args);

            if (cmdLine.hasOption(HELP_OPT.getOpt())) {
                printGetHelp();
                return RET_SUCCESS;
            }

            String haGroupName = getRequiredOption(cmdLine, HA_GROUP_OPT, "HA group name");

            HAGroupStoreManager manager = HAGroupStoreManager.getInstance(getConf());
            HAGroupStoreRecord record = manager.getHAGroupStoreRecord(haGroupName).orElse(null);

            if (record == null) {
                System.err.println("HA group not found: " + haGroupName);
                return RET_ARGUMENT_ERROR;
            }

            // Print as table with single-item list
            printHAGroupRecordsAsTable(manager, Arrays.asList(haGroupName));

            return RET_SUCCESS;

        } catch (Exception e) {
            return handleCommandError(e);
        }
    }

    /**
     * Execute LIST command
     */
    private int executeList(String[] args) throws Exception {
        try {
            CommandLine cmdLine = new DefaultParser().parse(createListOptions(), args);

            if (cmdLine.hasOption(HELP_OPT.getOpt())) {
                printListHelp();
                return RET_SUCCESS;
            }

            HAGroupStoreManager manager = HAGroupStoreManager.getInstance(getConf());
            List<String> haGroupNames = manager.getHAGroupNames();

            if (haGroupNames.isEmpty()) {
                System.out.println("\nNo HA groups found");
                return RET_SUCCESS;
            }

            printHAGroupRecordsAsTable(manager, haGroupNames);

            return RET_SUCCESS;

        } catch (Exception e) {
            return handleCommandError(e);
        }
    }

    /**
     * Common error handler for simple commands (get, list, version).
     *
     * @param e the exception that occurred
     * @return the appropriate error return code
     */
    private int handleCommandError(Exception e) {
        System.err.println("✗ Error: " + e.getMessage());
        return RET_UPDATE_ERROR;
    }

    /**
     * Print ClusterRoleRecord as text
     */
    private void printClusterRoleRecordAsText(ClusterRoleRecord record) {
        System.out.println("\nCluster Role Record:");
        System.out.println("  HA Group Name:     " + record.getHaGroupName());
        System.out.println("  Policy:            " + record.getPolicy());
        System.out.println("  Registry Type:     " + record.getRegistryType());
        System.out.println("  Cluster 1 URL:     " + record.getUrl1());
        System.out.println("  Cluster 1 Role:    " + record.getRole1());
        System.out.println("  Cluster 2 URL:     " + record.getUrl2());
        System.out.println("  Cluster 2 Role:    " + record.getRole2());
        System.out.println("  Version:           " + record.getVersion());

        // Show active URL if available
        record.getActiveUrl().ifPresent(activeUrl ->
            System.out.println("  Active URL:        " + activeUrl));

        System.out.println();
    }

    /**
     * Execute INITIATE-FAILOVER command
     */
    private int executeInitiateFailover(String[] args) throws Exception {
        try {
            CommandLine cmdLine = new DefaultParser().parse(createInitiateFailoverOptions(), args);

            if (cmdLine.hasOption(HELP_OPT.getOpt())) {
                printInitiateFailoverHelp();
                return RET_SUCCESS;
            }

            String haGroupName = getRequiredOption(cmdLine, HA_GROUP_OPT, "HA group name");

            // Parse timeout (default 120 seconds)
            int timeoutSeconds = 120;
            if (cmdLine.hasOption(TIMEOUT_OPT.getOpt())) {
                timeoutSeconds = Integer.parseInt(cmdLine.getOptionValue(TIMEOUT_OPT.getOpt()));
            }

            // Get current state from ZK to show what will be changed
            String zkUrl = getLocalZkUrl(getConf());
            try (PhoenixHAAdmin admin = new PhoenixHAAdmin(zkUrl, getConf(),
                    HighAvailibilityCuratorProvider.INSTANCE,
                    ZK_CONSISTENT_HA_GROUP_RECORD_NAMESPACE)) {

                Pair<HAGroupStoreRecord, Stat> recordAndStat =
                    admin.getHAGroupStoreRecordInZooKeeper(haGroupName);

                if (recordAndStat.getLeft() == null) {
                    System.err.println("HA group not found: " + haGroupName);
                    return RET_ARGUMENT_ERROR;
                }

                HAGroupStoreRecord currentRecord = recordAndStat.getLeft();
                HAGroupState currentState = currentRecord.getHAGroupState();

                System.out.println("\n[Step 1] Reading current configuration from ZK...");
                System.out.println("  HA Group:      " + haGroupName);
                System.out.println("  Current State: " + currentState);
                System.out.println("  Cluster Role:  " + currentRecord.getClusterRole());

                // Determine target state based on current state
                HAGroupState targetState;
                HAGroupState finalExpectedState;
                if (currentState == HAGroupState.ACTIVE_IN_SYNC) {
                    targetState = HAGroupState.ACTIVE_IN_SYNC_TO_STANDBY;
                    finalExpectedState = HAGroupState.STANDBY;
                } else if (currentState == HAGroupState.ACTIVE_NOT_IN_SYNC) {
                    targetState = HAGroupState.ACTIVE_NOT_IN_SYNC_TO_STANDBY;
                    finalExpectedState = HAGroupState.STANDBY;
                } else {
                    System.err.println("\n✗ Cannot initiate failover from current state: "
                            + currentState);
                    System.err.println(
                            "  Cluster must be in ACTIVE_IN_SYNC or ACTIVE_NOT_IN_SYNC state.");
                    return RET_VALIDATION_ERROR;
                }

                System.out.println("\n[Step 2] Determining target state...");
                System.out.println("  Intermediate State: " + targetState);
                System.out.println("  Final State:        " + finalExpectedState);

                // Perform failover initiation
                System.out.println("\n[Step 3] Initiating failover on active cluster...");
                HAGroupStoreManager manager = new HAGroupStoreManager(getConf());
                manager.initiateFailoverOnActiveCluster(haGroupName);

                System.out.println("  ✓ Failover initiated");

                // Poll for state transitions
                System.out.println("\n[Step 4] Monitoring state transitions (timeout: "
                        + timeoutSeconds + "s)...");
                boolean transitionComplete = pollForStateTransition(
                    manager, haGroupName, finalExpectedState, timeoutSeconds);

                if (transitionComplete) {
                    System.out.println("\n✓ Failover completed successfully");
                    System.out.println("\nFailover Summary:");
                    System.out.println("  HA Group:       " + haGroupName);
                    System.out.println("  Initial State:  " + currentState);
                    System.out.println("  Final State:    " + finalExpectedState);
                    System.out.println();
                    return RET_SUCCESS;
                } else {
                    System.err.println("\n⚠ Failover transition incomplete");
                    System.err.println("  The failover was initiated but did not complete within "
                            + timeoutSeconds + " seconds.");
                    System.err.println("  Check cluster states manually to verify completion.");
                    return RET_UPDATE_ERROR;
                }
            }

        } catch (IllegalArgumentException e) {
            System.err.println("\n✗ Invalid argument: " + e.getMessage());
            return RET_ARGUMENT_ERROR;

        } catch (Exception e) {
            System.err.println("\n✗ Failover initiation failed: " + e.getMessage());
            e.printStackTrace();
            return RET_UPDATE_ERROR;
        }
    }

    /**
     * Execute ABORT-FAILOVER command
     */
    private int executeAbortFailover(String[] args) throws Exception {
        try {
            CommandLine cmdLine = new DefaultParser().parse(createAbortFailoverOptions(), args);

            if (cmdLine.hasOption(HELP_OPT.getOpt())) {
                printAbortFailoverHelp();
                return RET_SUCCESS;
            }

            String haGroupName = getRequiredOption(cmdLine, HA_GROUP_OPT, "HA group name");

            // Parse timeout (default 120 seconds)
            int timeoutSeconds = 120;
            if (cmdLine.hasOption(TIMEOUT_OPT.getOpt())) {
                timeoutSeconds = Integer.parseInt(cmdLine.getOptionValue(TIMEOUT_OPT.getOpt()));
            }

            // Get current state from ZK to show what will be changed
            String zkUrl = getLocalZkUrl(getConf());
            try (PhoenixHAAdmin admin = new PhoenixHAAdmin(zkUrl, getConf(),
                    HighAvailibilityCuratorProvider.INSTANCE,
                    ZK_CONSISTENT_HA_GROUP_RECORD_NAMESPACE)) {

                Pair<HAGroupStoreRecord, Stat> recordAndStat =
                    admin.getHAGroupStoreRecordInZooKeeper(haGroupName);

                if (recordAndStat.getLeft() == null) {
                    System.err.println("HA group not found: " + haGroupName);
                    return RET_ARGUMENT_ERROR;
                }

                HAGroupStoreRecord currentRecord = recordAndStat.getLeft();
                HAGroupState currentState = currentRecord.getHAGroupState();

                System.out.println("\n[Step 1] Reading current configuration from ZK...");
                System.out.println("  HA Group:      " + haGroupName);
                System.out.println("  Current State: " + currentState);
                System.out.println("  Cluster Role:  " + currentRecord.getClusterRole());

                // Validate current state - should be in STANDBY_TO_ACTIVE to abort
                if (currentState != HAGroupState.STANDBY_TO_ACTIVE) {
                    System.err.println("\n✗ Cannot abort failover from current state: "
                            + currentState);
                    System.err.println(
                            "  Cluster must be in STANDBY_TO_ACTIVE state to abort failover.");
                    System.err.println(
                            "  This command should only be run on the standby cluster during an "
                            + "ongoing failover.");
                    return RET_VALIDATION_ERROR;
                }

                System.out.println("\n[Step 2] Determining target state...");
                System.out.println("  Intermediate State: ABORT_TO_STANDBY");
                System.out.println("  Final State:        STANDBY");

                // Perform abort failover
                System.out.println("\n[Step 3] Aborting failover on standby cluster...");
                HAGroupStoreManager manager = new HAGroupStoreManager(getConf());
                manager.setHAGroupStatusToAbortToStandby(haGroupName);

                System.out.println("  ✓ Abort initiated");

                // Poll for state transitions
                System.out.println("\n[Step 4] Monitoring state transitions (timeout: "
                        + timeoutSeconds + "s)...");
                boolean transitionComplete = pollForStateTransition(
                    manager, haGroupName, HAGroupState.STANDBY, timeoutSeconds);

                if (transitionComplete) {
                    System.out.println("\n✓ Failover abort completed successfully");
                    System.out.println("\nAbort Failover Summary:");
                    System.out.println("  HA Group:       " + haGroupName);
                    System.out.println("  Initial State:  " + currentState);
                    System.out.println("  Final State:    STANDBY");
                    System.out.println();
                    return RET_SUCCESS;
                } else {
                    System.err.println("\n⚠ Abort transition incomplete");
                    System.err.println("  The abort was initiated but did not complete within "
                            + timeoutSeconds + " seconds.");
                    System.err.println("  Check cluster states manually to verify completion.");
                    return RET_UPDATE_ERROR;
                }
            }

        } catch (IllegalArgumentException e) {
            System.err.println("\n✗ Invalid argument: " + e.getMessage());
            return RET_ARGUMENT_ERROR;

        } catch (Exception e) {
            System.err.println("\n✗ Abort failover failed: " + e.getMessage());
            e.printStackTrace();
            return RET_UPDATE_ERROR;
        }
    }

    /**
     * Execute GET-CLUSTER-ROLE-RECORD command
     */
    private int executeGetClusterRoleRecord(String[] args) throws Exception {
        try {
            CommandLine cmdLine =
                    new DefaultParser().parse(createGetClusterRoleRecordOptions(), args);

            if (cmdLine.hasOption(HELP_OPT.getOpt())) {
                printGetClusterRoleRecordHelp();
                return RET_SUCCESS;
            }

            String haGroupName = getRequiredOption(cmdLine, HA_GROUP_OPT, "HA group name");

            HAGroupStoreManager manager = HAGroupStoreManager.getInstance(getConf());
            ClusterRoleRecord clusterRoleRecord = manager.getClusterRoleRecord(haGroupName);

            printClusterRoleRecordAsText(clusterRoleRecord);

            return RET_SUCCESS;

        } catch (Exception e) {
            return handleCommandError(e);
        }
    }

    /**
     * Read current admin version from ZK and increment by 1
     */
    private long readCurrentVersionAndIncrement(String haGroupName) throws Exception {
        String zkUrl = getLocalZkUrl(getConf());
        try (PhoenixHAAdmin admin = new PhoenixHAAdmin(zkUrl, getConf(),
                HighAvailibilityCuratorProvider.INSTANCE,
                ZK_CONSISTENT_HA_GROUP_RECORD_NAMESPACE)) {

            Pair<HAGroupStoreRecord, Stat> recordAndStat =
                admin.getHAGroupStoreRecordInZooKeeper(haGroupName);

            if (recordAndStat.getLeft() == null) {
                throw new IllegalArgumentException("HA group not found: " + haGroupName);
            }

            long currentVersion = recordAndStat.getLeft().getAdminCRRVersion();
            long nextVersion = currentVersion + 1;

            System.out.println("Auto-incrementing admin version: " + currentVersion + " -> "
                    + nextVersion);

            return nextVersion;
        }
    }

    /**
     * Simple warning for auto-increment
     */
    private void showAutoIncrementWarning() {
        System.out.println();
        System.out.println("⚠ Using auto-increment version");
        System.out.println("  Risk: Concurrent updates may cause version conflicts");
        System.out.println("  If conflict occurs, re-run the command");
        System.out.println();
    }

    /**
     * Poll for state transition completion with timeout
     *
     * @param manager HAGroupStoreManager instance
     * @param haGroupName HA group name
     * @param finalState Expected final state
     * @param timeoutSeconds Timeout in seconds
     * @return true if transition completed, false if timed out
     */
    private boolean pollForStateTransition(
            HAGroupStoreManager manager,
            String haGroupName,
            HAGroupState finalState,
            int timeoutSeconds) {

        long startTime = System.currentTimeMillis();
        long timeoutMillis = timeoutSeconds * 1000L;
        long pollIntervalMillis = 2000; // Poll every 2 seconds

        HAGroupState lastSeenState = null;
        int dots = 0;

        try {
            while (System.currentTimeMillis() - startTime < timeoutMillis) {
                Optional<HAGroupStoreRecord> recordOpt =
                        manager.getHAGroupStoreRecord(haGroupName);
                Optional<HAGroupStoreRecord> peerRecordOpt =
                        manager.getHAGroupStoreRecord(haGroupName);
                HAGroupState peerState = HAGroupStoreRecord.HAGroupState.UNKNOWN;
                if (peerRecordOpt.isPresent()) {
                    peerState = peerRecordOpt.get().getHAGroupState();
                }

                if (recordOpt.isPresent()) {
                    HAGroupState currentState = recordOpt.get().getHAGroupState();

                    // Print state change
                    if (lastSeenState != currentState) {
                        if (lastSeenState != null) {
                            System.out.println(); // New line after dots
                        }
                        System.out.println("  Current State: " + currentState);
                        System.out.println("  Peer State: " + peerState);
                        lastSeenState = currentState;
                        dots = 0;
                    } else {
                        // Print progress dots
                        System.out.print(".");
                        dots++;
                        if (dots >= 30) { // New line after 30 dots
                            System.out.println();
                            dots = 0;
                        }
                    }

                    // Check if we reached the final state
                    if (currentState == finalState) {
                        if (dots > 0) {
                            System.out.println(); // New line after dots
                        }
                        long elapsedSeconds = (System.currentTimeMillis() - startTime) / 1000;
                        System.out.println("  ✓ Transition completed in " + elapsedSeconds
                                + " seconds");
                        return true;
                    }
                }

                Thread.sleep(pollIntervalMillis);
            }

            // Timeout reached
            if (dots > 0) {
                System.out.println(); // New line after dots
            }
            System.err.println("  ✗ Timeout after " + timeoutSeconds + " seconds");
            if (lastSeenState != null) {
                System.err.println("  Last seen state: " + lastSeenState);
            }
            return false;

        } catch (Exception e) {
            if (dots > 0) {
                System.out.println(); // New line after dots
            }
            System.err.println("  ✗ Error during polling: " + e.getMessage());
            return false;
        }
    }

    /**
     * Perform the update operation
     */
    private int performUpdate(
            String haGroupName,
            HAGroupStoreConfigUpdate update,
            boolean force,
            boolean dryRun) throws Exception {

        String zkUrl = getLocalZkUrl(getConf());

        try (PhoenixHAAdmin admin = new PhoenixHAAdmin(zkUrl, getConf(),
                HighAvailibilityCuratorProvider.INSTANCE,
                ZK_CONSISTENT_HA_GROUP_RECORD_NAMESPACE)) {

            // Step 1: Read current state from ZK
            System.out.println("[Step 1] Reading current configuration from ZK...");

            Pair<HAGroupStoreRecord, Stat> currentState =
                admin.getHAGroupStoreRecordInZooKeeper(haGroupName);

            if (currentState.getLeft() == null) {
                throw new IllegalArgumentException("HA group not found: " + haGroupName);
            }

            HAGroupStoreRecord currentRecord = currentState.getLeft();
            Stat currentStat = currentState.getRight();

            System.out.println("Current Record: ");
            printHAGroupStoreRecord(currentRecord);
            System.out.println("  Current admin version: " + currentRecord.getAdminCRRVersion());
            System.out.println("  Current ZK version: " + currentStat.getVersion());

            // Step 2: Merge configuration
            System.out.println("[Step 2] Merging configuration...");

            HAGroupStoreRecord newRecord = mergeConfiguration(currentRecord, update, force);

            // Step 3: Validate
            System.out.println("[Step 3] Validating update...");

            validateUpdate(currentRecord, newRecord, update.getAdminVersion(), force);

            // Step 4: Show proposed changes
            printProposedChanges(currentRecord, newRecord);

            if (dryRun) {
                System.out.println("\n✓ Dry-run completed. No changes applied.");
                return RET_SUCCESS;
            }

            // Step 5: Perform atomic update
            System.out.println("\n[Step 4] Applying update...");

            performUpdate(admin, haGroupName, newRecord, currentStat.getVersion(), zkUrl);

            System.out.println("\n✓ Update completed successfully");
            printUpdateSummary(haGroupName, currentRecord.getAdminCRRVersion(),
                              newRecord.getAdminCRRVersion());

            return RET_SUCCESS;
        }
    }

    /**
     * Merge new configuration with existing record
     */
    private HAGroupStoreRecord mergeConfiguration(
            HAGroupStoreRecord existing,
            HAGroupStoreConfigUpdate update,
            boolean force) {

        // Determine final state (preserve unless force and provided)
        HAGroupState finalState;
        if (force && update.getHaGroupState() != null) {
            finalState = update.getHaGroupState();
        } else {
            finalState = existing.getHAGroupState();
        }

        // Determine lastSyncTime (preserve unless force and provided)
        Long finalLastSyncTime;
        if (force && update.getLastSyncTime() != null) {
            finalLastSyncTime = update.getLastSyncTime();
        } else {
            finalLastSyncTime = existing.getLastSyncStateTimeInMs();
        }

        return new HAGroupStoreRecord(
            update.getProtocolVersion() != null
                    ? update.getProtocolVersion() : existing.getProtocolVersion(),
            update.getHaGroupName(),
            finalState,
            finalLastSyncTime,
            update.getPolicy() != null ? update.getPolicy() : existing.getPolicy(),
            update.getPeerZKUrl() != null ? update.getPeerZKUrl() : existing.getPeerZKUrl(),
            update.getClusterUrl() != null ? update.getClusterUrl() : existing.getClusterUrl(),
            update.getPeerClusterUrl() != null
                    ? update.getPeerClusterUrl()
                    : existing.getPeerClusterUrl(),
            update.getAdminVersion()
        );
    }

    /**
     * Validate the update
     */
    private void validateUpdate(HAGroupStoreRecord current,
                               HAGroupStoreRecord proposed,
                               long newVersion,
                               boolean force) throws ValidationException {

        // Validation 1: Version must increment
        if (newVersion <= current.getAdminCRRVersion()) {
            throw new ValidationException(
                "Admin version must increment. Current: " + current.getAdminCRRVersion()
                + ", Provided: " + newVersion);
        }

        // Validation 2: HA group name cannot change
        if (!current.getHaGroupName().equals(proposed.getHaGroupName())) {
            throw new ValidationException("Cannot change HA group name");
        }

        // Validation 3: State transition must be valid (if changing)
        if (!current.getHAGroupState().equals(proposed.getHAGroupState())) {
            if (!force) {
                throw new ValidationException(
                    "State change requires --force flag. Current: "
                    + current.getHAGroupState() + ", New: " + proposed.getHAGroupState());
            }
        }

        // Validation 4: Required fields not null/empty
        if (StringUtils.isBlank(proposed.getPolicy())) {
            throw new ValidationException("Policy cannot be null or empty");
        }
        if (StringUtils.isBlank(proposed.getClusterUrl())) {
            throw new ValidationException("Cluster URL cannot be null or empty");
        }
        if (StringUtils.isBlank(proposed.getPeerClusterUrl())) {
            throw new ValidationException("Peer cluster URL cannot be null or empty");
        }
        if (StringUtils.isBlank(proposed.getPeerZKUrl())) {
            throw new ValidationException("Peer ZK URL cannot be null or empty");
        }
    }

    /**
     * Atomically update ZK and System Table
     */
    private void performUpdate(
            PhoenixHAAdmin admin,
            String haGroupName,
            HAGroupStoreRecord newRecord,
            int currentZkVersion,
            String localZkUrl) throws Exception {

        // Update ZK with optimistic locking
        System.out.println("  Updating ZooKeeper (current ZK version: " + currentZkVersion
                + ")...");

        admin.updateHAGroupStoreRecordInZooKeeper(haGroupName, newRecord, currentZkVersion);

        System.out.println("  ✓ ZooKeeper updated successfully");

        // Update System Table
        System.out.println("  Updating System Table...");

        try {
            updateSystemTable(haGroupName, newRecord, localZkUrl, admin);

            System.out.println("  ✓ System Table updated successfully");

        } catch (Exception e) {
            LOG.error("System Table update failed for HA group: " + haGroupName, e);
            System.err.println("  ⚠ Warning: System Table update failed: " + e.getMessage());
            System.err.println("     ZooKeeper update was successful (ZK is source of truth)");
            System.err.println("     Periodic sync job will update System Table automatically");
        }
    }

    /**
     * Update SYSTEM.HA_GROUP table
     */
    private void updateSystemTable(String haGroupName,
                                   HAGroupStoreRecord record,
                                   String localZkUrl,
                                   PhoenixHAAdmin admin) throws SQLException {

        // Try to get peer role (best effort)
        ClusterRole peerRole = ClusterRole.UNKNOWN;
        try {
            if (StringUtils.isNotBlank(record.getPeerZKUrl())) {
                try (PhoenixHAAdmin peerAdmin = new PhoenixHAAdmin(
                        record.getPeerZKUrl(), getConf(),
                        HighAvailibilityCuratorProvider.INSTANCE,
                        ZK_CONSISTENT_HA_GROUP_RECORD_NAMESPACE)) {

                    HAGroupStoreRecord peerRecord =
                        peerAdmin.getHAGroupStoreRecordInZooKeeper(haGroupName).getLeft();
                    if (peerRecord != null) {
                        peerRole = peerRecord.getClusterRole();
                    }
                }
            }
        } catch (Exception e) {
            LOG.warn("Could not read peer record, using UNKNOWN for peer role", e);
        }

        String updateQuery =
            "UPSERT INTO " + SYSTEM_HA_GROUP_NAME + " "
            + "(" + HA_GROUP_NAME + ", " + POLICY + ", " + CLUSTER_ROLE_1 + ", "
            + CLUSTER_ROLE_2 + ", "
            + CLUSTER_URL_1 + ", " + CLUSTER_URL_2 + ", " + ZK_URL_1 + ", " + ZK_URL_2 + ", "
            + VERSION + ") "
            + "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)";

        try (PhoenixConnection conn = (PhoenixConnection) DriverManager.getConnection(
                JDBC_PROTOCOL_ZK + JDBC_PROTOCOL_SEPARATOR + localZkUrl);
             PreparedStatement pstmt = conn.prepareStatement(updateQuery)) {

            pstmt.setString(1, haGroupName);
            pstmt.setString(2, record.getPolicy());
            pstmt.setString(3, record.getClusterRole().name());
            pstmt.setString(4, peerRole.name());
            pstmt.setString(5, record.getClusterUrl());
            pstmt.setString(6, record.getPeerClusterUrl());
            pstmt.setString(7, localZkUrl);
            pstmt.setString(8, record.getPeerZKUrl());
            pstmt.setLong(9, record.getAdminCRRVersion());

            pstmt.executeUpdate();
            conn.commit();
        }
    }

    /**
     * Print proposed changes
     */
    private void printProposedChanges(HAGroupStoreRecord current,
                                     HAGroupStoreRecord proposed) {
        System.out.println("\n=== Proposed Changes ===\n");

        boolean hasChanges = false;

        if (!Objects.equals(current.getPolicy(), proposed.getPolicy())) {
            printFieldChange("Policy", current.getPolicy(), proposed.getPolicy());
            hasChanges = true;
        }

        if (!Objects.equals(current.getHAGroupState(), proposed.getHAGroupState())) {
            printFieldChange("HA Group State", current.getHAGroupState(),
                    proposed.getHAGroupState());
            System.out.println("    ⚠ STATE CHANGE - verify impact before proceeding");
            hasChanges = true;
        }

        if (!Objects.equals(current.getClusterUrl(), proposed.getClusterUrl())) {
            printFieldChange("Cluster URL", current.getClusterUrl(), proposed.getClusterUrl());
            hasChanges = true;
        }

        if (!Objects.equals(current.getPeerClusterUrl(), proposed.getPeerClusterUrl())) {
            printFieldChange("Peer Cluster URL", current.getPeerClusterUrl(),
                    proposed.getPeerClusterUrl());
            hasChanges = true;
        }

        if (!Objects.equals(current.getPeerZKUrl(), proposed.getPeerZKUrl())) {
            printFieldChange("Peer ZK URL", current.getPeerZKUrl(), proposed.getPeerZKUrl());
            hasChanges = true;
        }

        if (!Objects.equals(current.getProtocolVersion(), proposed.getProtocolVersion())) {
            printFieldChange("Protocol Version", current.getProtocolVersion(),
                    proposed.getProtocolVersion());
            hasChanges = true;
        }

        if (!Objects.equals(current.getLastSyncStateTimeInMs(),
                proposed.getLastSyncStateTimeInMs())) {
            printFieldChange("Last Sync Time",
                formatTimestamp(current.getLastSyncStateTimeInMs()),
                formatTimestamp(proposed.getLastSyncStateTimeInMs()));
            hasChanges = true;
        }

        // Always show version change
        printFieldChange("Admin Version",
            String.valueOf(current.getAdminCRRVersion()),
            String.valueOf(proposed.getAdminCRRVersion()));

        if (!hasChanges) {
            System.out.println("  (Only admin version will be updated)");
        }

        System.out.println();
    }

    private void printFieldChange(String fieldName, Object oldValue, Object newValue) {
        System.out.println(String.format("  %-25s: %s -> %s", fieldName, oldValue, newValue));
    }

    private void printUpdateSummary(String haGroupName, long oldVersion, long newVersion) {
        System.out.println("\nUpdate Summary:");
        System.out.println("  HA Group:      " + haGroupName);
        System.out.println("  Admin Version: " + oldVersion + " -> " + newVersion);
        System.out.println("  Status:        ZooKeeper and System Table updated");
        System.out.println();
        System.out.println("Note: Run this tool on peer cluster to update peer configuration");
    }

    /**
     * Print list of HA group records as table
     */
    private void printHAGroupRecordsAsTable(HAGroupStoreManager manager,
            List<String> haGroupNames) {
        System.out.println();
        System.out.println("HA Groups:");
        System.out.println(StringUtils.repeat("=", 150));

        for (String haGroupName : haGroupNames) {
            try {
                HAGroupStoreRecord record = manager.getHAGroupStoreRecord(haGroupName).orElse(null);
                printHAGroupStoreRecord(record);
            } catch (Exception e) {
                LOG.warn("Failed to read HA group: " + haGroupName, e);
                System.out.println("\nHA Group Name:       " + haGroupName);
                System.out.println("  ERROR:             " + e.getMessage());
                System.out.println(StringUtils.repeat("-", 150));
            }
        }

        System.out.println("\nTotal: " + haGroupNames.size() + " HA group(s)");
        System.out.println();
    }

    private void printHAGroupStoreRecord(HAGroupStoreRecord record) {
        if (record != null) {
            System.out.println("\nHA Group Name:       " + record.getHaGroupName());
            System.out.println("  Protocol Version:  " + record.getProtocolVersion());
            System.out.println("  Policy:            " + record.getPolicy());
            System.out.println("  State:             " + record.getHAGroupState());
            System.out.println("  Cluster Role:      " + record.getClusterRole());
            System.out.println("  Cluster URL:       " + record.getClusterUrl());
            System.out.println("  Peer Cluster URL:  " + record.getPeerClusterUrl());
            System.out.println("  Peer ZK URL:       " + record.getPeerZKUrl());
            System.out.println("  Admin Version:     " + record.getAdminCRRVersion());
            System.out.println("  Last Sync Time:    "
                    + formatTimestamp(record.getLastSyncStateTimeInMs()));
            System.out.println(StringUtils.repeat("-", 150));
        }
    }

    /**
     * Create options for update command
     */
    private static Options createUpdateOptions() {
        return new Options()
            .addOption(HELP_OPT)
            .addOption(HA_GROUP_OPT)
            .addOption(ADMIN_VERSION_OPT)
            .addOption(AUTO_INCREMENT_VERSION_OPT)
            .addOption(POLICY_OPT)
            .addOption(STATE_OPT)
            .addOption(CLUSTER_URL_OPT)
            .addOption(PEER_CLUSTER_URL_OPT)
            .addOption(PEER_ZK_URL_OPT)
            .addOption(PROTOCOL_VERSION_OPT)
            .addOption(LAST_SYNC_TIME_OPT)
            .addOption(FORCE_OPT)
            .addOption(DRY_RUN_OPT);
    }

    /**
     * Create options for get command
     */
    private static Options createGetOptions() {
        return new Options()
            .addOption(HELP_OPT)
            .addOption(HA_GROUP_OPT);
    }

    /**
     * Create options for list command
     */
    private static Options createListOptions() {
        return new Options()
            .addOption(HELP_OPT);
    }

    /**
     * Create options for initiate-failover command
     */
    private static Options createInitiateFailoverOptions() {
        return new Options()
            .addOption(HELP_OPT)
            .addOption(HA_GROUP_OPT)
            .addOption(TIMEOUT_OPT);
    }

    /**
     * Create options for abort-failover command
     */
    private static Options createAbortFailoverOptions() {
        return new Options()
            .addOption(HELP_OPT)
            .addOption(HA_GROUP_OPT)
            .addOption(TIMEOUT_OPT);
    }

    /**
     * Create options for get-cluster-role-record command
     */
    private static Options createGetClusterRoleRecordOptions() {
        return new Options()
            .addOption(HELP_OPT)
            .addOption(HA_GROUP_OPT);
    }

    /**
     * Get required option value
     */
    private String getRequiredOption(CommandLine cmdLine, Option option, String description) {
        if (!cmdLine.hasOption(option.getOpt())) {
            throw new IllegalArgumentException("Missing required option: " + description);
        }
        return cmdLine.getOptionValue(option.getOpt());
    }

    /**
     * Parse state from string
     */
    private HAGroupState parseState(String state) {
        if (state == null) {
            return null;
        }

        try {
            return HAGroupState.valueOf(state.toUpperCase());
        } catch (IllegalArgumentException e) {
            throw new IllegalArgumentException(
                "Invalid state: " + state + "\nValid states: "
                + Arrays.stream(HAGroupState.values())
                    .map(Enum::name)
                    .collect(Collectors.joining(", ")));
        }
    }

    /**
     * Parse long from string
     */
    private Long parseLong(String value) {
        return value != null ? Long.parseLong(value) : null;
    }

    /**
     * Format timestamp for display
     */
    private String formatTimestamp(Long timestamp) {
        if (timestamp == null) {
            return "null";
        }
        return new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(new Date(timestamp))
               + " (" + timestamp + ")";
    }

    /**
     * Print main usage message
     */
    private void printUsage() {
        System.out.println();
        System.out.println("Usage: phoenix-consistentha-admin-tool <command> [options]");
        System.out.println();
        System.out.println("Commands:");
        System.out.println("  update                  Update HA group configuration");
        System.out.println("  get                     Show HA group configuration");
        System.out.println("  list                    List all HA groups");
        System.out.println("  get-cluster-role-record Get cluster role record for both clusters");
        System.out.println("  initiate-failover       Initiate failover on active cluster");
        System.out.println("  abort-failover          Abort failover on standby cluster");
        System.out.println();
        System.out.println(
                "Run 'phoenix-consistentha-admin-tool <command> --help' for command-specific help");
        System.out.println();
        System.out.println("Examples:");
        System.out.println(
                "  phoenix-consistentha-admin-tool update -g myHAGroup "
                + "-pz newhost:2181:/hbase -v 5");
        System.out.println("  phoenix-consistentha-admin-tool get -g myHAGroup");
        System.out.println("  phoenix-consistentha-admin-tool list");
        System.out.println(
                "  phoenix-consistentha-admin-tool get-cluster-role-record -g myHAGroup");
        System.out.println("  phoenix-consistentha-admin-tool initiate-failover -g myHAGroup");
        System.out.println("  phoenix-consistentha-admin-tool abort-failover -g myHAGroup");
        System.out.println();
    }

    /**
     * Print update command help
     */
    private void printUpdateHelp() {
        System.out.println();
        System.out.println("Usage: phoenix-consistentha-admin-tool update [options]");
        System.out.println();
        System.out.println("REQUIRED:");
        System.out.println("  -g, --ha-group <name>             HA group name");
        System.out.println();
        System.out.println("VERSION (choose ONE):");
        System.out.println("  -v, --admin-version <version>     Explicit version (recommended)");
        System.out.println("  -av, --auto-increment-version     Auto-increment current version");
        System.out.println(
                "                                    WARNING: May overwrite concurrent updates");
        System.out.println();
        System.out.println("CONFIGURATION FIELDS (at least one required):");
        System.out.println("  -p, --policy <policy>             HA policy (FAILOVER)");
        System.out.println("  -s, --state <state>               HA group state (requires --force)");
        System.out.println("  -c, --cluster-url <url>           Local cluster URL");
        System.out.println("  -pc, --peer-cluster-url <url>     Peer cluster URL");
        System.out.println("  -pz, --peer-zk-url <url>          Peer ZK URL");
        System.out.println("  -pv, --protocol-version <ver>     Protocol version");
        System.out.println("  -lst, --last-sync-time <ms>       Last sync time (requires --force)");
        System.out.println();
        System.out.println("FLAGS:");
        System.out.println(
                "  -F, --force                       Allow state and restricted changes");
        System.out.println("  -d, --dry-run                     Show changes without applying");
        System.out.println("  -h, --help                        Show this help");
        System.out.println();
        System.out.println("Valid States:");
        System.out.println("  " + Arrays.stream(HAGroupState.values())
            .map(Enum::name).collect(Collectors.joining(", ")));
        System.out.println();
        System.out.println("Examples:");
        System.out.println("  # Update peer ZK URL (explicit version)");
        System.out.println(
                "  phoenix-consistentha-admin-tool update -g myHAGroup -pz new:2181:/hbase -v 5");
        System.out.println();
        System.out.println("  # Update peer ZK URL (auto-increment)");
        System.out.println(
                "  phoenix-consistentha-admin-tool update -g myHAGroup -pz new:2181:/hbase -av");
        System.out.println();
        System.out.println("  # Update multiple fields");
        System.out.println(
                "  phoenix-consistentha-admin-tool update -g myHAGroup -pz new:2181 "
                + "-pc new:16000 -v 6");
        System.out.println();
        System.out.println("  # State change (requires --force)");
        System.out.println(
                "  phoenix-consistentha-admin-tool update -g myHAGroup "
                + "-s ABORT_TO_STANDBY -v 7 -F");
        System.out.println();
        System.out.println("  # Dry-run first");
        System.out.println(
                "  phoenix-consistentha-admin-tool update -g myHAGroup -pz new:2181 -v 8 -d");
        System.out.println();
    }

    /**
     * Print get command help
     */
    private void printGetHelp() {
        System.out.println();
        System.out.println("Usage: phoenix-consistentha-admin-tool get [options]");
        System.out.println();
        System.out.println("Description:");
        System.out.println("  Shows the complete HA group configuration including state, policy,");
        System.out.println("  cluster URLs, and version information.");
        System.out.println();
        System.out.println("Options:");
        System.out.println("  -g, --ha-group <name>     HA group name (REQUIRED)");
        System.out.println("  -h, --help                Show this help");
        System.out.println();
        System.out.println("Example:");
        System.out.println("  phoenix-consistentha-admin-tool get -g myHAGroup");
        System.out.println();
    }

    /**
     * Print list command help
     */
    private void printListHelp() {
        System.out.println();
        System.out.println("Usage: phoenix-consistentha-admin-tool list [options]");
        System.out.println();
        System.out.println("Description:");
        System.out.println(
                "  Lists all HA groups with their complete configuration including state,");
        System.out.println("  policy, cluster URLs, and version information.");
        System.out.println();
        System.out.println("Options:");
        System.out.println("  -h, --help                Show this help");
        System.out.println();
        System.out.println("Example:");
        System.out.println("  phoenix-consistentha-admin-tool list");
        System.out.println();
    }

    /**
     * Print initiate-failover command help
     */
    private void printInitiateFailoverHelp() {
        System.out.println();
        System.out.println("Usage: phoenix-consistentha-admin-tool initiate-failover [options]");
        System.out.println();
        System.out.println("Description:");
        System.out.println(
                "  Initiates failover on the active cluster by transitioning to the appropriate");
        System.out.println(
                "  TO_STANDBY state. The cluster must be in ACTIVE_IN_SYNC or ACTIVE_NOT_IN_SYNC");
        System.out.println("  state to initiate failover.");
        System.out.println();
        System.out.println("  State Transitions:");
        System.out.println("  - ACTIVE_IN_SYNC      -> ACTIVE_IN_SYNC_TO_STANDBY -> STANDBY");
        System.out.println(
                "  - ACTIVE_NOT_IN_SYNC  -> ACTIVE_NOT_IN_SYNC_TO_STANDBY -> "
                + "ACTIVE_IN_SYNC_TO_STANDBY -> STANDBY");
        System.out.println();
        System.out.println("Options:");
        System.out.println("  -g, --ha-group <name>     HA group name (REQUIRED)");
        System.out.println(
                "  -t, --timeout <seconds>   Timeout for state transition (default: 120)");
        System.out.println("  -h, --help                Show this help");
        System.out.println();
        System.out.println("Examples:");
        System.out.println("  phoenix-consistentha-admin-tool initiate-failover -g myHAGroup");
        System.out.println(
                "  phoenix-consistentha-admin-tool initiate-failover -g myHAGroup -t 180");
        System.out.println();
        System.out.println(
                "Note: This command polls the cluster state and waits for the failover to");
        System.out.println(
                "      complete within the timeout period. Progress is shown in real-time.");
        System.out.println();
    }

    /**
     * Print abort-failover command help
     */
    private void printAbortFailoverHelp() {
        System.out.println();
        System.out.println("Usage: phoenix-consistentha-admin-tool abort-failover [options]");
        System.out.println();
        System.out.println("Description:");
        System.out.println(
                "  Aborts an ongoing failover on the standby cluster by transitioning to");
        System.out.println(
                "  ABORT_TO_STANDBY state. The cluster must be in STANDBY_TO_ACTIVE state");
        System.out.println("  to abort the failover.");
        System.out.println();
        System.out.println("  State Transition:");
        System.out.println("  - STANDBY_TO_ACTIVE -> ABORT_TO_STANDBY -> STANDBY");
        System.out.println();
        System.out.println("Options:");
        System.out.println("  -g, --ha-group <name>     HA group name (REQUIRED)");
        System.out.println(
                "  -t, --timeout <seconds>   Timeout for state transition (default: 120)");
        System.out.println("  -h, --help                Show this help");
        System.out.println();
        System.out.println("Examples:");
        System.out.println("  phoenix-consistentha-admin-tool abort-failover -g myHAGroup");
        System.out.println("  phoenix-consistentha-admin-tool abort-failover -g myHAGroup -t 180");
        System.out.println();
        System.out.println(
                "Note: This command polls the cluster state and waits for the abort to");
        System.out.println(
                "      complete within the timeout period. Progress is shown in real-time.");
        System.out.println();
    }

    /**
     * Print get-cluster-role-record command help
     */
    private void printGetClusterRoleRecordHelp() {
        System.out.println();
        System.out.println(
                "Usage: phoenix-consistentha-admin-tool get-cluster-role-record [options]");
        System.out.println();
        System.out.println("Description:");
        System.out.println(
                "  Retrieves the complete cluster role record for an HA group, showing the");
        System.out.println(
                "  roles and URLs for both clusters in the HA pair. This provides a unified");
        System.out.println(
                "  view of the cluster configuration including both local and peer cluster");
        System.out.println("  information.");
        System.out.println();
        System.out.println("Options:");
        System.out.println("  -g, --ha-group <name>     HA group name (REQUIRED)");
        System.out.println("  -h, --help                Show this help");
        System.out.println();
        System.out.println("Example:");
        System.out.println(
                "  phoenix-consistentha-admin-tool get-cluster-role-record -g myHAGroup");
        System.out.println();
    }

    /**
     * Configuration update request object
     */
    @VisibleForTesting
    static class HAGroupStoreConfigUpdate {
        private final String haGroupName;
        private final String protocolVersion;
        private final String policy;
        private final String clusterUrl;
        private final String peerClusterUrl;
        private final String peerZKUrl;
        private final long adminVersion;
        private final HAGroupState haGroupState;
        private final Long lastSyncTime;

        HAGroupStoreConfigUpdate(
                String haGroupName,
                String protocolVersion,
                String policy,
                String clusterUrl,
                String peerClusterUrl,
                String peerZKUrl,
                long adminVersion,
                HAGroupState haGroupState,
                Long lastSyncTime) {
            this.haGroupName = haGroupName;
            this.protocolVersion = protocolVersion;
            this.policy = policy;
            this.clusterUrl = clusterUrl;
            this.peerClusterUrl = peerClusterUrl;
            this.peerZKUrl = peerZKUrl;
            this.adminVersion = adminVersion;
            this.haGroupState = haGroupState;
            this.lastSyncTime = lastSyncTime;
        }

        public String getHaGroupName() {
            return haGroupName;
        }
        public String getProtocolVersion() {
            return protocolVersion;
        }
        public String getPolicy() {
            return policy;
        }
        public String getClusterUrl() {
            return clusterUrl;
        }
        public String getPeerClusterUrl() {
            return peerClusterUrl;
        }
        public String getPeerZKUrl() {
            return peerZKUrl;
        }
        public long getAdminVersion() {
            return adminVersion;
        }
        public HAGroupState getHaGroupState() {
            return haGroupState;
        }
        public Long getLastSyncTime() {
            return lastSyncTime;
        }
    }

    /**
     * Validation exception
     */
    @VisibleForTesting
    static class ValidationException extends Exception {
        ValidationException(String message) {
            super(message);
        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = HBaseConfiguration.create();
        int retCode = ToolRunner.run(conf, new PhoenixHAAdminTool(), args);
        System.exit(retCode);
    }
}

