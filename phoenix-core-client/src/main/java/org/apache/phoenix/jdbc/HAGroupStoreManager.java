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

import static org.apache.phoenix.jdbc.PhoenixHAAdmin.getLocalZkUrl;
import static org.apache.phoenix.query.QueryServices.CLUSTER_ROLE_BASED_MUTATION_BLOCK_ENABLED;
import static org.apache.phoenix.query.QueryServicesOptions.DEFAULT_CLUSTER_ROLE_BASED_MUTATION_BLOCK_ENABLED;

import java.io.IOException;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import org.apache.hadoop.conf.Configuration;

public class HAGroupStoreManager {
  private final boolean mutationBlockEnabled;
  private final Configuration conf;
  private static volatile Map<String, HAGroupStoreManager> INSTANCES = new ConcurrentHashMap<>();

  /**
   * Creates/gets an instance of HAGroupStoreManager. Provides unique instance for each ZK URL
   * @param conf configuration
   * @return HAGroupStoreManager instance
   */
  public static HAGroupStoreManager getInstance(Configuration conf) {
    final String zkUrl = getLocalZkUrl(conf);
    HAGroupStoreManager result = INSTANCES.get(zkUrl);
    if (result == null) {
      synchronized (HAGroupStoreManager.class) {
        result = INSTANCES.get(zkUrl);
        if (result == null) {
          result = new HAGroupStoreManager(conf);
          INSTANCES.put(zkUrl, result);
        }
      }
    }
    return result;
  }

  private HAGroupStoreManager(final Configuration conf) {
    this.mutationBlockEnabled = conf.getBoolean(CLUSTER_ROLE_BASED_MUTATION_BLOCK_ENABLED,
      DEFAULT_CLUSTER_ROLE_BASED_MUTATION_BLOCK_ENABLED);
    this.conf = conf;
  }

  /**
   * Checks whether mutation is blocked or not.
   * @throws IOException when HAGroupStoreClient is not healthy.
   */
  public boolean isMutationBlocked() throws IOException {
    if (mutationBlockEnabled) {
      HAGroupStoreClient haGroupStoreClient = HAGroupStoreClient.getInstance(conf);
      if (haGroupStoreClient != null) {
        return !haGroupStoreClient
          .getCRRsByClusterRole(ClusterRoleRecord.ClusterRole.ACTIVE_TO_STANDBY).isEmpty();
      }
      throw new IOException("HAGroupStoreClient is not initialized");
    }
    return false;
  }

  /**
   * Force rebuilds the HAGroupStoreClient
   */
  public void invalidateHAGroupStoreClient() throws Exception {
    HAGroupStoreClient haGroupStoreClient = HAGroupStoreClient.getInstance(conf);
    if (haGroupStoreClient != null) {
      haGroupStoreClient.rebuild();
    } else {
      throw new IOException("HAGroupStoreClient is not initialized");
    }
  }
}
