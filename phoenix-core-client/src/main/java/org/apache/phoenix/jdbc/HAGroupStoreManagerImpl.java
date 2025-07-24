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
 * Main implementation of HAGroupStoreManager that uses HAGroupStoreClient.
 */
public class HAGroupStoreManagerImpl implements HAGroupStoreManager {
    private static final Logger LOGGER = LoggerFactory.getLogger(HAGroupStoreManager.class);
    private final boolean mutationBlockEnabled;
    private final String zkUrl;
    private final Configuration conf;

    public HAGroupStoreManagerImpl(final Configuration conf) {
        this.mutationBlockEnabled = conf.getBoolean(CLUSTER_ROLE_BASED_MUTATION_BLOCK_ENABLED,
                DEFAULT_CLUSTER_ROLE_BASED_MUTATION_BLOCK_ENABLED);
        this.zkUrl = getLocalZkUrl(conf);
        this.conf = conf;
    }

    /**
     * Note that we need haGroupName to determine whether mutation should be blocked or not
     * Returning false here as we don't want to block anything unnecessarily.
     */
    @Override
    public boolean isMutationBlocked() {
        return false;
    }

    @Override
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

    @Override
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

    @Override
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

    @Override
    public Optional<HAGroupStoreRecord> getHAGroupStoreRecord(final String haGroupName)
            throws IOException, SQLException {
        HAGroupStoreClient haGroupStoreClient = HAGroupStoreClient.getInstance(conf,
                haGroupName, zkUrl);
        if (haGroupStoreClient != null) {
            return Optional.ofNullable(haGroupStoreClient.getHAGroupStoreRecord());
        }
        throw new IOException("HAGroupStoreClient is not initialized");
    }

    @Override
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

    @Override
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

    @Override
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