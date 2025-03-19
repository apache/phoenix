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
import static org.apache.phoenix.query.QueryServices.CLUSTER_ROLE_BASED_MUTATION_BLOCK_ENABLED;
import static org.apache.phoenix.query.QueryServicesOptions.DEFAULT_CLUSTER_ROLE_BASED_MUTATION_BLOCK_ENABLED;

public class HAGroupStoreManager {
    private static HAGroupStoreManager cacheInstance;
    private final HAGroupStoreClient haGroupStoreClient;
    private final boolean mutationBlockEnabled;

    /**
     * Creates/gets an instance of HAGroupStoreManager.
     *
     * @param conf configuration
     * @return cache
     */
    public static HAGroupStoreManager getInstance(Configuration conf) throws Exception {
        HAGroupStoreManager result = cacheInstance;
        if (result == null) {
            synchronized (HAGroupStoreClient.class) {
                result = cacheInstance;
                if (result == null) {
                    cacheInstance = result = new HAGroupStoreManager(conf);
                }
            }
        }
        return result;
    }

    private HAGroupStoreManager(final Configuration conf) throws Exception {
        this.mutationBlockEnabled = conf.getBoolean(CLUSTER_ROLE_BASED_MUTATION_BLOCK_ENABLED,
                DEFAULT_CLUSTER_ROLE_BASED_MUTATION_BLOCK_ENABLED);
        this.haGroupStoreClient = HAGroupStoreClient.getInstance(conf);
    }

    /**
     * Checks whether mutation is blocked or not.
     * @throws IOException when HAGroupStoreClient is not healthy.
     */
    public boolean isMutationBlocked() throws IOException {
        return mutationBlockEnabled
                && !haGroupStoreClient.getCRRsByClusterRole(ClusterRoleRecord.ClusterRole.ACTIVE_TO_STANDBY).isEmpty();
    }

    /**
     * Force rebuilds the HAGroupStoreClient
     * @throws Exception
     */
    public void invalidateHAGroupStoreClient() throws Exception {
        haGroupStoreClient.rebuild();
    }
}
