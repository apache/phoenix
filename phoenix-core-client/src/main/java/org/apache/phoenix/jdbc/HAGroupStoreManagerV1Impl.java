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

import java.io.IOException;
import java.util.Optional;

import static org.apache.phoenix.query.QueryServices.CLUSTER_ROLE_BASED_MUTATION_BLOCK_ENABLED;
import static org.apache.phoenix.query.QueryServicesOptions.DEFAULT_CLUSTER_ROLE_BASED_MUTATION_BLOCK_ENABLED;

/**
 * V1 implementation of HAGroupStoreManager that uses HAGroupStoreClientV1.
 */
public class HAGroupStoreManagerV1Impl implements HAGroupStoreManager {
    private final boolean mutationBlockEnabled;

    public HAGroupStoreManagerV1Impl(final Configuration conf) {
        this.mutationBlockEnabled = conf.getBoolean(CLUSTER_ROLE_BASED_MUTATION_BLOCK_ENABLED,
                DEFAULT_CLUSTER_ROLE_BASED_MUTATION_BLOCK_ENABLED);
    }

    /**
     * Checks whether mutation is blocked or not across all HA groups.
     * @throws IOException when HAGroupStoreClient is not healthy.
     */
    @Override
    public boolean isMutationBlocked(Configuration conf) throws IOException {
        return isMutationBlocked(conf, null);
    }

    /**
     * Checks whether mutation is blocked or not for a specific HA group.
     *
     * @param conf
     * @param haGroupName name of the HA group, null for default HA group which tracks all HA groups.
     * @return true if mutation is blocked, false otherwise.
     * @throws IOException when HAGroupStoreClient is not healthy.
     */
    @Override
    public boolean isMutationBlocked(Configuration conf, String haGroupName) throws IOException {
        if (mutationBlockEnabled) {
            HAGroupStoreClientV1 haGroupStoreClient = HAGroupStoreClientV1.getInstance(conf);
            if (haGroupStoreClient != null) {
                return !haGroupStoreClient.getCRRsByClusterRole(ClusterRoleRecord.ClusterRole.ACTIVE_TO_STANDBY).isEmpty();
            }
            throw new IOException("HAGroupStoreClient is not initialized");
        }
        return false;
    }

    /**
     * Force rebuilds the HAGroupStoreClient instance for all HA group.
     * @throws Exception
     */
    @Override
    public void invalidateHAGroupStoreClient(Configuration conf) throws Exception {
        invalidateHAGroupStoreClient(conf, null);
    }

    /**
     * Force rebuilds the HAGroupStoreClient for a specific HA group.
     *
     * @param conf
     * @param haGroupName name of the HA group, null for default HA group and tracks all HA groups.
     * @throws Exception
     */
    @Override
    public void invalidateHAGroupStoreClient(Configuration conf, String haGroupName) throws Exception {
        HAGroupStoreClientV1 haGroupStoreClient = HAGroupStoreClientV1.getInstance(conf);
        if (haGroupStoreClient != null) {
            haGroupStoreClient.rebuild();
        } else {
            throw new IOException("HAGroupStoreClient is not initialized");
        }
    }

    @Override
    public Optional<HAGroupStoreRecord> getHAGroupStoreRecord(Configuration conf, String haGroupName) throws IOException {
        throw new UnsupportedOperationException("HAGroupStoreRecord not supported in HAGroupStoreManagerV1Impl");
    }

    @Override
    public void setHAGroupStatusToStoreAndForward(Configuration conf, String haGroupName) throws IOException {
        throw new UnsupportedOperationException("Setting HAGroupStatus to StoreAndForward is not supported in HAGroupStoreManagerV1Impl");
    }

    @Override
    public void setHAGroupStatusRecordToSync(Configuration conf, String haGroupName) throws IOException {
        throw new UnsupportedOperationException("Setting HAGroupStatusRecord to Sync is not supported in HAGroupStoreManagerV1Impl");
    }
}