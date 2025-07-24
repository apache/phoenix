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

import org.apache.phoenix.exception.InvalidClusterRoleTransitionException;
import org.apache.phoenix.exception.StaleHAGroupStoreRecordVersionException;

import java.io.IOException;
import java.sql.SQLException;
import java.util.List;
import java.util.Optional;

/**
 * Interface for managing HA group store operations including mutation blocking checks
 * and client invalidation.
 */
public interface HAGroupStoreManager {

    /**
     * Checks whether mutation is blocked or not across all HA groups.
     * Note if HAGroupStoreManagerImpl is used, it will return false
     * @throws IOException when HAGroupStoreClient is not healthy.
     */
    boolean isMutationBlocked() throws IOException, SQLException;

    /**
     * Checks whether mutation is blocked or not for a specific HA group.
     *
     * @param haGroupName name of the HA group, null for default HA group which tracks
     *                   all HA groups.
     * @return true if mutation is blocked, false otherwise.
     * @throws IOException when HAGroupStoreClient is not healthy.
     */
    boolean isMutationBlocked(String haGroupName) throws IOException, SQLException;

    /**
     * Force rebuilds the HAGroupStoreClient instance for all HA groups.
     * If any HAGroupStoreClient instance is not created, it will be created.
     * @param broadcastUpdate if true, the update will be broadcasted to all
     *                       regionserver endpoints.
     * @throws Exception
     */
    void invalidateHAGroupStoreClient(boolean broadcastUpdate) throws Exception;

    /**
     * Force rebuilds the HAGroupStoreClient for a specific HA group.
     *
     * @param haGroupName name of the HA group, null for default HA group and tracks all HA groups.
     * @param broadcastUpdate if true, the update will be broadcasted to all
     *                       regionserver endpoints.
     * @throws Exception
     */
    void invalidateHAGroupStoreClient(String haGroupName, boolean broadcastUpdate)
            throws Exception;

    /**
     * Returns the HAGroupStoreRecord for a specific HA group.
     *
     * @param haGroupName name of the HA group
     * @return Optional HAGroupStoreRecord for the HA group, can be empty if the HA group
     *        is not found.
     * @throws IOException when HAGroupStoreClient is not healthy.
     */
    Optional<HAGroupStoreRecord> getHAGroupStoreRecord(String haGroupName)
            throws IOException, SQLException;

    /**
     * Sets the HAGroupStoreRecord to StoreAndForward mode in local cluster.
     *
     * @param haGroupName name of the HA group
     * @throws IOException when HAGroupStoreClient is not healthy.
     */
    void setHAGroupStatusToStoreAndForward(String haGroupName) throws IOException,
            StaleHAGroupStoreRecordVersionException, InvalidClusterRoleTransitionException,
            SQLException;

    /**
     * Sets the HAGroupStoreRecord to Sync mode in local cluster.
     *
     * @param haGroupName name of the HA group
     * @throws IOException when HAGroupStoreClient is not healthy.
     */
    void setHAGroupStatusRecordToSync(String haGroupName) throws IOException,
            StaleHAGroupStoreRecordVersionException, InvalidClusterRoleTransitionException,
            SQLException;

    /**
     * Returns the ClusterRoleRecord for the cluster pair.
     * If the peer cluster is not connected or peer cluster is not configured, it will
     * return UNKNOWN for peer cluster.
     * Only implemented by HAGroupStoreManagerImpl.
     *
     * @return ClusterRoleRecord for the cluster pair
     * @throws IOException when HAGroupStoreClient is not healthy.
     */
    ClusterRoleRecord getClusterRoleRecord(String haGroupName) throws IOException, SQLException;

}