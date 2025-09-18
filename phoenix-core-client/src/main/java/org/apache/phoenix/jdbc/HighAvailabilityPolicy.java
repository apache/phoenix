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

import static org.apache.phoenix.jdbc.ClusterRoleRecord.ClusterRole.ACTIVE;
import static org.apache.phoenix.jdbc.ClusterRoleRecord.ClusterRole.ACTIVE_TO_STANDBY;
import static org.apache.phoenix.jdbc.ClusterRoleRecord.ClusterRole.STANDBY;
import static org.apache.phoenix.jdbc.ClusterRoleRecord.ClusterRole.STANDBY_TO_ACTIVE;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.Properties;

import org.apache.phoenix.exception.SQLExceptionCode;
import org.apache.phoenix.exception.SQLExceptionInfo;
import org.apache.phoenix.monitoring.GlobalClientMetrics;
import org.apache.phoenix.query.ConnectionQueryServices;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * An HighAvailabilityGroup provides a JDBC connection from given connection string and properties.
 */
public enum HighAvailabilityPolicy {
    FAILOVER {
        @Override
        public Connection provide(HighAvailabilityGroup haGroup, Properties info,
                                  HAURLInfo haURLInfo) throws SQLException {
            FailoverPhoenixContext context = new FailoverPhoenixContext(info, haGroup, haURLInfo);
            return new FailoverPhoenixConnection(context);
        }

        /**
         * Cluster Role Transitions for Failover High Availability Policy, Here we are trying to
         * close connections late to allow existing reads to continue during Failover. Only allowed
         * transitions are mentioned below, check @{link HAGroupStoreRecord.HAGroupState} for more details.
         * ACTIVE --> ACTIVE_TO_STANDBY (Doing Nothing as we are in process of moving the current
         *      to STANDBY and at this step we are blocking write to drain replication this allows
         *      us to continue existing/new reads to continue, so FailoverConnections are allowed)
         * ACTIVE_TO_STANDBY --> STANDBY (Closing all current connections)
         * ACTIVE_TO_STANDBY --> ACTIVE (NOOP on client side, server will resume Mutations)
         *
         * STANDBY --> STANDBY_TO_ACTIVE (NOOP on client side)
         * STANDBY_TO_ACTIVE --> STANDBY (NOOP on client side)
         * 
         * STANDBY_TO_ACTIVE --> ACTIVE (Invalidate CQSI as connections has been closed and now being cleared)
         * 
         * other transitions are not allowed and restricted on Server side.
         * @param haGroup The high availability (HA) group
         * @param oldRecord The older cluster role record cached in this client for the given HA group
         * @param newRecord New cluster role record read from one ZooKeeper cluster znode
         * @throws SQLException
         */
        @Override
        void transitClusterRole(HighAvailabilityGroup haGroup, ClusterRoleRecord oldRecord,
                ClusterRoleRecord newRecord) throws SQLException {

            if (oldRecord.getRole1() == ACTIVE_TO_STANDBY &&
                    (newRecord.getRole1() == STANDBY)) {
                transitStandby(haGroup, oldRecord.getUrl1(), oldRecord.getRegistryType(),
                        newRecord.getRole1());
            }
            if (oldRecord.getRole2() == ACTIVE_TO_STANDBY &&
                    (newRecord.getRole2() == STANDBY)) {
                transitStandby(haGroup, oldRecord.getUrl2(), oldRecord.getRegistryType(),
                        newRecord.getRole2());
            }
            if (oldRecord.getRole1() == STANDBY_TO_ACTIVE && 
                    (newRecord.getRole1() == ACTIVE)) {
                transitActive(haGroup, oldRecord.getUrl1(), oldRecord.getRegistryType());
            }
            if (oldRecord.getRole2() == STANDBY_TO_ACTIVE && 
                    (newRecord.getRole2() == ACTIVE)) {
                transitActive(haGroup, oldRecord.getUrl2(), oldRecord.getRegistryType());
            }
        }

        /**
         * For FAILOVER Policy if there is a change in active url or there is no new active url then close all connections.
         * In below examples only a portion of CRR is shown, url1, url2 are current urls present in
         * clusterRoleRecord and url3, url4 are new urls in clusterRoleRecord 
         * HERE ACTIVE means ACTIVE or ACTIVE_TO_STANDBY
         * (url1, ACTIVE, url2, STANDBY) --> (url1, ACTIVE, url3, STANDBY) //Nothing is needed as only Standby url changed
         * (url1, ACTIVE, url2, STANDBY) --> (url3, ACTIVE, url2, STANDBY) //Active url change close connections
         * (url1, ACTIVE, url2, STANDBY) --> (url3, ACTIVE, url4, STANDBY) //Active url change close connections
         * (url1, ACTIVE, url2, STANDBY) --> (url3, ACTIVE, url1, STANDBY)
         *                          //Here active became standby but other url changed close connections
         * (url1, OFFLINE, url2, STANDBY) --> (url3, ACTIVE, url2, STANDBY) //Nothing to do as there were no connections
         * (url1, ACTIVE, url2, STANDBY) --> (url3, OFFLINE, url2, STANDBY) //Closing old connections as no new active url
         * @param haGroup The high availability (HA) group
         * @param oldRecord The older cluster role record cached in this client for the given HA group
         * @param newRecord New cluster role record read from one ZooKeeper cluster znode
         * @throws SQLException when not able to close connections
         */
        @Override
        void transitClusterUrl(HighAvailabilityGroup haGroup, ClusterRoleRecord oldRecord,
                               ClusterRoleRecord newRecord) throws SQLException {
            Optional<String> activeUrl = oldRecord.getActiveUrl();
            Optional<String> newActiveUrl = newRecord.getActiveUrl();
            if (activeUrl.isPresent()) {
                if (newActiveUrl.isPresent()) {
                    if (activeUrl.get().equals(newActiveUrl.get())) {
                        LOG.info("Active URL is same after cluster role record transition" +
                                "Doing nothing for FailoverPhoenixConnections");
                    } else {
                        LOG.info("Active url of clusterRoleRecord changed from {} to {}, " +
                                        "closing old connections", activeUrl.get(), newActiveUrl.get());
                        closeConnections(haGroup, activeUrl.get(), oldRecord.getRegistryType());
                        invalidateCQSIs(haGroup, activeUrl.get(), oldRecord.getRegistryType());
                    }
                } else {
                    LOG.info("Couldn't find active url in new ClusterRoleRecord," +
                            "Closing old connections");
                    closeConnections(haGroup, activeUrl.get(), oldRecord.getRegistryType());
                    invalidateCQSIs(haGroup, activeUrl.get(), oldRecord.getRegistryType());
                }
            } else {
                LOG.info("Couldn't find active url in old ClusterRoleRecord, " +
                        "Doing nothing for FailoverPhoenixConnections");
            }
        }

        private void transitStandby(HighAvailabilityGroup haGroup, String url,
                                    ClusterRoleRecord.RegistryType registryType,
                                    ClusterRoleRecord.ClusterRole newRole) throws SQLException {
            // Close connections when a previously ACTIVE HBase cluster becomes STANDBY.
            LOG.info("Cluster {} becomes {} in HA group {}, now close all its connections",
                    url, newRole, haGroup.getGroupInfo());
            closeConnections(haGroup, url, registryType);
        }

        private void transitActive(HighAvailabilityGroup haGroup, String url,
                                   ClusterRoleRecord.RegistryType registryType) throws SQLException {
            // Invalidate CQS cache if any that has been closed but has not been cleared
            invalidateCQSIs(haGroup, url, registryType);
        }
    },

    PARALLEL {
        @Override
        public Connection provide(HighAvailabilityGroup haGroup, Properties info,
                                  HAURLInfo haURLInfo) throws SQLException {
            List<Boolean> executorCapacities = PhoenixHAExecutorServiceProvider.hasCapacity(info);
            if (executorCapacities.contains(Boolean.TRUE)) {
                ParallelPhoenixContext context =
                        new ParallelPhoenixContext(info, haGroup,
                                PhoenixHAExecutorServiceProvider.get(info),
                                executorCapacities, haURLInfo);
                return new ParallelPhoenixConnection(context);
            } else {
                // TODO: Once we have operation/primary wait timeout use the same
                // Give regular connection or a failover connection?
                LOG.warn("Falling back to single phoenix connection due to resource constraints");
                GlobalClientMetrics.GLOBAL_HA_PARALLEL_CONNECTION_FALLBACK_COUNTER.increment();
                return haGroup.connectActive(info, haURLInfo);
            }
        }

        @Override
        void transitClusterRole(HighAvailabilityGroup haGroup, ClusterRoleRecord oldRecord,
                ClusterRoleRecord newRecord) {
            //No Action for Parallel Policy
        }

        /**
         * For PARALLEL policy if there is a change in any of the url then ParallelPhoenixConnection
         * objects are invalid
         * @param haGroup The high availability (HA) group
         * @param oldRecord The older cluster role record cached in this client for the given HA group
         * @param newRecord New cluster role record read from one ZooKeeper cluster znode
         * @throws SQLException when not able to close connections
         */
        @Override
        void transitClusterUrl(HighAvailabilityGroup haGroup, ClusterRoleRecord oldRecord,
                               ClusterRoleRecord newRecord) throws SQLException {
            if (!Objects.equals(oldRecord.getUrl1(), newRecord.getUrl1()) &&
                    !Objects.equals(oldRecord.getUrl1(), newRecord.getUrl2())) {
                LOG.info("Cluster {} is changed to {} in HA group {}, now closing all its connections",
                        oldRecord.getUrl1(), newRecord.getUrl1(), haGroup);
                closeConnections(haGroup, oldRecord.getUrl1(), oldRecord.getRegistryType());
                invalidateCQSIs(haGroup, oldRecord.getUrl1(), oldRecord.getRegistryType());
            }
            if (!Objects.equals(oldRecord.getUrl2(), newRecord.getUrl2()) &&
                    !Objects.equals(oldRecord.getUrl2(), newRecord.getUrl1())) {
                LOG.info("Cluster {} is changed to {} in HA group {}, now closing all its connections",
                        oldRecord.getUrl2(), newRecord.getUrl2(), haGroup);
                closeConnections(haGroup, oldRecord.getUrl2(), oldRecord.getRegistryType());
                invalidateCQSIs(haGroup, oldRecord.getUrl2(), oldRecord.getRegistryType());
            }

        }
    };

    private static final Logger LOG = LoggerFactory.getLogger(HighAvailabilityPolicy.class);

    /**
     * Utility to close cqs and all it's connection for specific url of a HAGroup
     * @param haGroup The High Availability (HA) Group
     * @param url The url on which cqs and connections are present
     * @param registryType The registry Type of connections
     * @throws SQLException if fails to close the connections
     */
    private static void closeConnections(HighAvailabilityGroup haGroup, String url,
                                         ClusterRoleRecord.RegistryType registryType) throws SQLException {
        ConnectionQueryServices cqs = null;
        //Close connections for every HAURLInfo's (different principal) conn for a give HAGroup
        for (HAURLInfo haurlInfo : HighAvailabilityGroup.URLS.get(haGroup.getGroupInfo())) {
            try {
                cqs = PhoenixDriver.INSTANCE.getConnectionQueryServices(
                        HighAvailabilityGroup.getJDBCUrl(url, haurlInfo, registryType), haGroup.getProperties());
                cqs.closeAllConnections(new SQLExceptionInfo
                        .Builder(SQLExceptionCode.HA_CLOSED_AFTER_FAILOVER)
                        .setMessage("Phoenix connection got closed due to failover")
                        .setHaGroupInfo(haGroup.getGroupInfo().toString()));
                LOG.info("Closed all connections to cluster {} for HA group {}", url,
                        haGroup.getGroupInfo());
            } finally {
                if (cqs != null) {
                    // CQS is closed, but it is not invalidated from global cache in PhoenixDriver
                    // so that any new connection will get error instead of creating a new CQS,
                    // CQS entry will stay in map until the cache expires and repopulated in cases
                    // of URL changes
                    LOG.info("Closing CQS after clusterRoleRecord change for '{}'", url);
                    cqs.close();
                    LOG.info("Successfully closed CQS after clusterRoleRecord change for '{}'", url);
                }
            }
        }
    }

    /**
     * Utility to invalidate CQS cache for a given url of a haGroup, it's recommended to invalidate
     * cqsi after closing it, as cqsi creation is heavy and is mostly cached at clients and if
     * invalidation doesn't happen it can lead to usage of closed cqsi which won't allow new
     * phoenix connection creation.
     * @param haGroup The High Availability (HA) Group
     * @param url The url for which cqs are present
     * @param registryType The registry Type of connections affiliate to cqs
     * @throws SQLException if fails to invalidate the cqs
     */
    private static void invalidateCQSIs(HighAvailabilityGroup haGroup, String url,
                                        ClusterRoleRecord.RegistryType registryType) throws SQLException {
        for (HAURLInfo haurlInfo : HighAvailabilityGroup.URLS.get(haGroup.getGroupInfo())) {
            String jdbcUrl = HighAvailabilityGroup.getJDBCUrl(url, haurlInfo, registryType);
            LOG.info("invalidating cqs cache for url: " + jdbcUrl);
            PhoenixDriver.INSTANCE.invalidateCache(jdbcUrl,
                    haGroup.getProperties());
        }
    }

    /**
     * Provides a JDBC connection from given connection string and properties.
     *
     * @param haGroup The high availability (HA) group
     * @param info Connection properties
     * @param haurlInfo additional info of client provided url
     * @return a JDBC connection
     * @throws SQLException if fails to provide a connection
     */
    abstract Connection provide(HighAvailabilityGroup haGroup, Properties info, HAURLInfo haurlInfo)
            throws SQLException;

    /**
     * Call-back function when a cluster role record transition is detected in the high availability group.
     *
     * @param haGroup The high availability (HA) group
     * @param oldRecord The older cluster role record cached in this client for the given HA group
     * @param newRecord New cluster role record read from one ZooKeeper cluster znode
     * @throws SQLException if fails to handle the cluster role record transition
     */
    public void transitClusterRoleRecord(HighAvailabilityGroup haGroup, ClusterRoleRecord oldRecord,
                                           ClusterRoleRecord newRecord) throws SQLException {
        if (!oldRecord.getUrl1().equals(newRecord.getUrl1()) ||
                !oldRecord.getUrl2().equals(newRecord.getUrl2())) {
            //If there is a change in url then we need to handle the transitions.
            transitClusterUrl(haGroup, oldRecord, newRecord);
        } else {
            //If url is not changing then we need to check if there is a role transition.
            transitClusterRole(haGroup, oldRecord, newRecord);
        }
    }

    /**
     * Call-back function when only role transition is detected in the high availability group or clusterRoleRecord.
     *
     * @param haGroup The high availability (HA) group
     * @param oldRecord The older cluster role record cached in this client for the given HA group
     * @param newRecord New cluster role record read from one ZooKeeper cluster znode
     * @throws SQLException if fails to handle the cluster role transition
     */
    abstract void transitClusterRole(HighAvailabilityGroup haGroup, ClusterRoleRecord oldRecord,
            ClusterRoleRecord newRecord) throws SQLException;

    /**
     * Call-back function when only url transition is detected in the high availability group or clusterRoleRecord.
     *
     * @param haGroup The high availability (HA) group
     * @param oldRecord The older cluster role record cached in this client for the given HA group
     * @param newRecord New cluster role record read from one ZooKeeper cluster znode
     * @throws SQLException if fails to handle the cluster url transition
     */
    abstract void transitClusterUrl(HighAvailabilityGroup haGroup, ClusterRoleRecord oldRecord,
                                            ClusterRoleRecord newRecord) throws SQLException;

}
