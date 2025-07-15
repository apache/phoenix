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

import java.io.IOException;
import java.util.Optional;

/**
 * Interface for managing HA group store operations including mutation blocking checks
 * and client invalidation.
 */
public interface HAGroupStoreManager {

    /**
     * Checks whether mutation is blocked or not across all HA groups.
     * If any HAGroupStoreClient instance is not created, it will be created.
     * If any HAGroup mutation is blocked, it will return true.
     * @throws IOException when HAGroupStoreClient is not healthy.
     */
    boolean isMutationBlocked(Configuration conf) throws IOException;

    /**
     * Checks whether mutation is blocked or not for a specific HA group.
     *
     * @param conf
     * @param haGroupName name of the HA group, null for default HA group which tracks all HA groups.
     * @return true if mutation is blocked, false otherwise.
     * @throws IOException when HAGroupStoreClient is not healthy.
     */
    boolean isMutationBlocked(Configuration conf, String haGroupName) throws IOException;

    /**
     * Force rebuilds the HAGroupStoreClient instance for all HA groups.
     * If any HAGroupStoreClient instance is not created, it will be created.
     * @throws Exception
     */
    void invalidateHAGroupStoreClient(Configuration conf) throws Exception;

    /**
     * Force rebuilds the HAGroupStoreClient for a specific HA group.
     *
     * @param conf
     * @param haGroupName name of the HA group, null for default HA group and tracks all HA groups.
     * @throws Exception
     */
    void invalidateHAGroupStoreClient(Configuration conf, String haGroupName) throws Exception;

    /**
     * Returns the HAGroupStoreRecord for a specific HA group.
     *
     * @param conf
     * @param haGroupName name of the HA group
     * @return Optional HAGroupStoreRecord for the HA group, can be empty if the HA group is not found.
     * @throws IOException when HAGroupStoreClient is not healthy.
     */
    Optional<HAGroupStoreRecord> getHAGroupStoreRecord(Configuration conf, String haGroupName) throws IOException;

    /**
     * Sets the HAGroupStoreRecord to StoreAndForward mode in local cluster.
     *
     * @param conf
     * @param haGroupName name of the HA group
     * @throws IOException when HAGroupStoreClient is not healthy.
     */
    void setHAGroupStatusToStoreAndForward(Configuration conf, String haGroupName) throws IOException, StaleHAGroupStoreRecordVersionException, InvalidClusterRoleTransitionException;

    /**
     * Sets the HAGroupStoreRecord to Sync mode in local cluster.
     *
     * @param conf
     * @param haGroupName name of the HA group
     * @throws IOException when HAGroupStoreClient is not healthy.
     */
    void setHAGroupStatusRecordToSync(Configuration conf, String haGroupName) throws IOException, StaleHAGroupStoreRecordVersionException, InvalidClusterRoleTransitionException;

}