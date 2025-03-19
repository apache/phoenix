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
