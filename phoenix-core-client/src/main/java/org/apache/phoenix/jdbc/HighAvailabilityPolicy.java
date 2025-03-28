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
import static org.apache.phoenix.jdbc.ClusterRoleRecord.ClusterRole.STANDBY;

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

        @Override
        void transitClusterRole(HighAvailabilityGroup haGroup, ClusterRoleRecord oldRecord,
                ClusterRoleRecord newRecord) throws SQLException {
            if (newRecord.getRole1() == ACTIVE && newRecord.getRole2() == ACTIVE) {
                LOG.warn("Both cluster roles are ACTIVE which is invalid state for FailoverPolicy" +
                        "Doing nothing for Cluster Role Change");
                return;
            }
            if (oldRecord.getRole1() == ACTIVE && newRecord.getRole1() == STANDBY) {
                transitStandby(haGroup, oldRecord.getUrl1(), oldRecord.getRegistryType());
            }
            if (oldRecord.getRole2() == ACTIVE && newRecord.getRole2() == STANDBY) {
                transitStandby(haGroup, oldRecord.getUrl2(), oldRecord.getRegistryType());
            }
            if (oldRecord.getRole1() != ACTIVE && newRecord.getRole1() == ACTIVE) {
                transitActive(haGroup, oldRecord.getUrl1(), oldRecord.getRegistryType());
            }
            if (oldRecord.getRole2() != ACTIVE && newRecord.getRole2() == ACTIVE) {
                transitActive(haGroup, oldRecord.getUrl2(), oldRecord.getRegistryType());
            }
        }

        @Override
        void transitRoleRecordRegistry(HighAvailabilityGroup haGroup, ClusterRoleRecord oldRecord,
                                       ClusterRoleRecord newRecord) throws SQLException {
            Optional<String> activeUrl = oldRecord.getActiveUrl();

            //Close connections for Active HBase cluster as there is a change in Registry Type
            if (activeUrl.isPresent()) {
                LOG.info("Cluster {} has a change in registryType in HA group {}, now closing "
                        + "all its connections", activeUrl.get(), oldRecord.getRegistryType());
                closeConnections(haGroup, activeUrl.get(), oldRecord.getRegistryType());
            } else {
                LOG.info("None of the cluster in HA Group {} is active", haGroup);
            }
        }

        /**
         * For FAILOVER Policy if there is a change in active url or there is no new active url then close all connections.
         * In below examples only a portion of CRR is shown, url1, url2 are current urls present in
         * clusterRoleRecord and url3, url4 are new urls in clusterRoleRecord
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
                    }
                } else {
                    LOG.info("Couldn't find active url in new ClusterRoleRecord," +
                            "Closing old connections");
                    closeConnections(haGroup, activeUrl.get(), oldRecord.getRegistryType());
                }
            } else {
                LOG.info("Couldn't find active url in old ClusterRoleRecord, " +
                        "Doing nothing for FailoverPhoenixConnections");
            }
        }

        private void transitStandby(HighAvailabilityGroup haGroup, String url,
                                    ClusterRoleRecord.RegistryType registryType) throws SQLException {
            // Close connections when a previously ACTIVE HBase cluster becomes STANDBY.
            LOG.info("Cluster {} becomes STANDBY in HA group {}, now close all its connections",
                    url, haGroup.getGroupInfo());
            closeConnections(haGroup, url, registryType);
        }

        private void transitActive(HighAvailabilityGroup haGroup, String url,
                                   ClusterRoleRecord.RegistryType registryType) throws SQLException {
            // Invalidate CQS cache if any that has been closed but has not been cleared
                for (HAURLInfo haurlInfo : HighAvailabilityGroup.URLS.get(haGroup.getGroupInfo())) {
                    String jdbcUrl = HighAvailabilityGroup.getJDBCUrl(url, haurlInfo, registryType);
                    LOG.info("invalidating cqs cache for zkUrl: " + jdbcUrl);
                    PhoenixDriver.INSTANCE.invalidateCache(jdbcUrl,
                            haGroup.getProperties());
                }
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


        @Override
        void transitRoleRecordRegistry(HighAvailabilityGroup haGroup, ClusterRoleRecord oldRecord,
                                       ClusterRoleRecord newRecord) throws SQLException {
            //Close connections of both clusters as there is a change in registryType
            LOG.info("Cluster {} and {} has a change in registryType in HA group {}, now closing all its connections",
                    oldRecord.getUrl1(), oldRecord.getUrl2() , oldRecord.getRegistryType());
            //close connections for cluster 1
            closeConnections(haGroup, oldRecord.getUrl1(), oldRecord.getRegistryType());
            //close connections for cluster 2
            closeConnections(haGroup, oldRecord.getUrl2(), oldRecord.getRegistryType());
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
            }
            if (!Objects.equals(oldRecord.getUrl2(), newRecord.getUrl2()) &&
                    !Objects.equals(oldRecord.getUrl2(), newRecord.getUrl1())) {
                LOG.info("Cluster {} is changed to {} in HA group {}, now closing all its connections",
                        oldRecord.getUrl2(), newRecord.getUrl2(), haGroup);
                closeConnections(haGroup, oldRecord.getUrl2(), oldRecord.getRegistryType());
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
                    // of URL and registryType changes
                    LOG.info("Closing CQS after clusterRoleRecord change for '{}'", url);
                    cqs.close();
                    LOG.info("Successfully closed CQS after clusterRoleRecord change for '{}'", url);
                }
            }
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
        if (oldRecord.getRegistryType() != newRecord.getRegistryType()) {
            transitRoleRecordRegistry(haGroup, oldRecord, newRecord);
        } else if (!oldRecord.getUrl1().equals(newRecord.getUrl1()) ||
                !oldRecord.getUrl2().equals(newRecord.getUrl2())) {
            //If registryType is not changing then we need to check if any of the url is changing
            //as change in registryType closes all the current connections
            transitClusterUrl(haGroup, oldRecord, newRecord);
        } else {
            //If both registryType and url is not changing then we need to check if there is a
            //role transition.
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
     * Call-back function when only registry transition is detected in the high availability group or clusterRoleRecord.
     *
     * @param haGroup The high availability (HA) group
     * @param oldRecord The older cluster role record cached in this client for the given HA group
     * @param newRecord New cluster role record read from one ZooKeeper cluster znode
     * @throws SQLException if fails to handle the cluster role record registry transition
     */
    abstract void transitRoleRecordRegistry(HighAvailabilityGroup haGroup, ClusterRoleRecord oldRecord,
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
