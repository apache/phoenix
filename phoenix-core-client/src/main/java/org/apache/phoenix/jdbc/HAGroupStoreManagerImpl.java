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
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

import static org.apache.phoenix.query.QueryServices.CLUSTER_ROLE_BASED_MUTATION_BLOCK_ENABLED;
import static org.apache.phoenix.query.QueryServicesOptions.DEFAULT_CLUSTER_ROLE_BASED_MUTATION_BLOCK_ENABLED;

/**
 * Main implementation of HAGroupStoreManager that uses HAGroupStoreClient.
 */
public class HAGroupStoreManagerImpl implements HAGroupStoreManager {
    private static final Logger LOGGER = LoggerFactory.getLogger(HAGroupStoreManager.class);
    private final boolean mutationBlockEnabled;

    public HAGroupStoreManagerImpl(final Configuration conf) {
        this.mutationBlockEnabled = conf.getBoolean(CLUSTER_ROLE_BASED_MUTATION_BLOCK_ENABLED,
                DEFAULT_CLUSTER_ROLE_BASED_MUTATION_BLOCK_ENABLED);
    }

    @Override
    public boolean isMutationBlocked(Configuration conf) throws IOException {
        List<String> haGroupNames = HAGroupStoreClient.getHAGroupNames(conf);
        for (String haGroupName : haGroupNames) {
            if (isMutationBlocked(conf, haGroupName)) {
                return true;
            }
        }
        return false;
    }

    @Override
    public boolean isMutationBlocked(Configuration conf, String haGroupName) throws IOException {
        if (mutationBlockEnabled) {
            HAGroupStoreClient haGroupStoreClient = HAGroupStoreClient.getInstance(conf, haGroupName);
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
    public void invalidateHAGroupStoreClient(Configuration conf) throws Exception {
        List<String> haGroupNames = HAGroupStoreClient.getHAGroupNames(conf);
        List<String> failedHAGroupNames = new ArrayList<>();
        for (String haGroupName : haGroupNames) {
            try {
                invalidateHAGroupStoreClient(conf, haGroupName);
            } catch (Exception e) {
                failedHAGroupNames.add(haGroupName);
                LOGGER.error("Failed to invalidate HAGroupStoreClient for " + haGroupName, e);
            }
        }
        if (!failedHAGroupNames.isEmpty()) {
            throw new IOException("Failed to invalidate HAGroupStoreClient for " + failedHAGroupNames
                    + "successfully invalidated HAGroupStoreClient instance for " + haGroupNames.removeAll(failedHAGroupNames) + " HA groups");
        }
    }

    @Override
    public void invalidateHAGroupStoreClient(Configuration conf, final String haGroupName) throws Exception {
        HAGroupStoreClient haGroupStoreClient = HAGroupStoreClient.getInstance(conf, haGroupName);
        if (haGroupStoreClient != null) {
            haGroupStoreClient.rebuild();
        } else {
            throw new IOException("HAGroupStoreClient is not initialized");
        }
    }

    @Override
    public Optional<HAGroupStoreRecord> getHAGroupStoreRecord(Configuration conf, final String haGroupName) throws IOException {
        HAGroupStoreClient haGroupStoreClient = HAGroupStoreClient.getInstance(conf, haGroupName);
        if (haGroupStoreClient != null) {
            return Optional.ofNullable(haGroupStoreClient.getHAGroupStoreRecord());
        }
        throw new IOException("HAGroupStoreClient is not initialized");
    }

    @Override
    public void setHAGroupStatusToStoreAndForward(Configuration conf, final String haGroupName)
            throws IOException, StaleHAGroupStoreRecordVersionException, InvalidClusterRoleTransitionException {
        HAGroupStoreClient haGroupStoreClient = HAGroupStoreClient.getInstance(conf, haGroupName);
        if (haGroupStoreClient != null) {
            haGroupStoreClient.setHAGroupStatusIfNeeded(ClusterRoleRecord.ClusterRole.ACTIVE_NOT_IN_SYNC);
        } else {
            throw new IOException("HAGroupStoreClient is not initialized");
        }
    }

    @Override
    public void setHAGroupStatusRecordToSync(Configuration conf, final String haGroupName)
            throws IOException, StaleHAGroupStoreRecordVersionException, InvalidClusterRoleTransitionException {
        HAGroupStoreClient haGroupStoreClient = HAGroupStoreClient.getInstance(conf, haGroupName);
        if (haGroupStoreClient != null) {
            haGroupStoreClient.setHAGroupStatusIfNeeded(ClusterRoleRecord.ClusterRole.ACTIVE);
        } else {
            throw new IOException("HAGroupStoreClient is not initialized");
        }
    }
}