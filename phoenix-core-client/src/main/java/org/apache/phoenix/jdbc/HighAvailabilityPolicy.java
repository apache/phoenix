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
enum HighAvailabilityPolicy {
    FAILOVER {
        @Override
        public Connection provide(HighAvailabilityGroup haGroup, Properties info)
                throws SQLException {
            return new FailoverPhoenixConnection(haGroup, info);
        }
        @Override
        void transitClusterRole(HighAvailabilityGroup haGroup, ClusterRoleRecord oldRecord,
                ClusterRoleRecord newRecord) throws SQLException {
            if (oldRecord.getRole1() == ACTIVE && newRecord.getRole1() == STANDBY) {
                transitStandby(haGroup, oldRecord.getZk1());
            }
            if (oldRecord.getRole2() == ACTIVE && newRecord.getRole2() == STANDBY) {
                transitStandby(haGroup, oldRecord.getZk2());
            }
            if (oldRecord.getRole1() != ACTIVE && newRecord.getRole1() == ACTIVE) {
                transitActive(haGroup, oldRecord.getZk1());
            }
            if (oldRecord.getRole2() != ACTIVE && newRecord.getRole2() == ACTIVE) {
                transitActive(haGroup, oldRecord.getZk2());
            }
        }
        private void transitStandby(HighAvailabilityGroup haGroup, String zkUrl)
                throws SQLException {
            // Close connections when a previously ACTIVE HBase cluster becomes STANDBY.
            LOG.info("Cluster {} becomes STANDBY in HA group {}, now close all its connections",
                    zkUrl, haGroup.getGroupInfo());
            ConnectionQueryServices cqs = null;
            try {
                cqs = PhoenixDriver.INSTANCE.getConnectionQueryServices(
                        haGroup.getGroupInfo().getJDBCUrl(zkUrl), haGroup.getProperties());
                cqs.closeAllConnections(new SQLExceptionInfo
                        .Builder(SQLExceptionCode.HA_CLOSED_AFTER_FAILOVER)
                        .setMessage("Phoenix connection got closed due to failover")
                        .setHaGroupInfo(haGroup.getGroupInfo().toString()));
                LOG.info("Closed all connections to cluster {} for HA group {}", zkUrl,
                        haGroup.getGroupInfo());
            } finally {
                if (cqs != null) {
                    // CQS is closed but it is not invalidated from global cache in PhoenixDriver
                    // so that any new connection will get error instead of creating a new CQS
                    LOG.info("Closing CQS after cluster '{}' becomes STANDBY", zkUrl);
                    cqs.close();
                    LOG.info("Successfully closed CQS after cluster '{}' becomes STANDBY", zkUrl);
                }
            }
        }
        private void transitActive(HighAvailabilityGroup haGroup, String zkUrl)
                throws SQLException {
            // Invalidate CQS cache if any that has been closed but has not been cleared
            LOG.info("invalidating cqs cache for zkUrl: " + zkUrl);
            PhoenixDriver.INSTANCE.invalidateCache(haGroup.getGroupInfo().getJDBCUrl(zkUrl),
                    haGroup.getProperties());
        }
    },

    PARALLEL {
        @Override
        public Connection provide(HighAvailabilityGroup haGroup, Properties info)
                throws SQLException {
            List<Boolean> executorCapacities = PhoenixHAExecutorServiceProvider.hasCapacity(info);
            if (executorCapacities.contains(Boolean.TRUE)) {
                ParallelPhoenixContext context =
                        new ParallelPhoenixContext(info, haGroup,
                                PhoenixHAExecutorServiceProvider.get(info), executorCapacities);
                return new ParallelPhoenixConnection(context);
            } else {
                // TODO: Once we have operation/primary wait timeout use the same
                // Give regular connection or a failover connection?
                LOG.warn("Falling back to single phoenix connection due to resource constraints");
                GlobalClientMetrics.GLOBAL_HA_PARALLEL_CONNECTION_FALLBACK_COUNTER.increment();
                return haGroup.connectActive(info);
            }
        }
        @Override
        void transitClusterRole(HighAvailabilityGroup haGroup, ClusterRoleRecord oldRecord,
                ClusterRoleRecord newRecord) {
            LOG.info("Cluster role changed for parallel HA policy.");
        }
    };

    private static final Logger LOG = LoggerFactory.getLogger(HighAvailabilityGroup.class);

    /**
     * Provides a JDBC connection from given connection string and properties.
     *
     * @param haGroup The high availability (HA) group
     * @param info Connection properties
     * @return a JDBC connection
     * @throws SQLException if fails to provide a connection
     */
    abstract Connection provide(HighAvailabilityGroup haGroup, Properties info) throws SQLException;

    /**
     * Call-back function when a cluster role transition is detected in the high availability group.
     *
     * @param haGroup The high availability (HA) group
     * @param oldRecord The older cluster role record cached in this client for the given HA group
     * @param newRecord New cluster role record read from one ZooKeeper cluster znode
     * @throws SQLException if fails to handle the cluster role transition
     */
    abstract void transitClusterRole(HighAvailabilityGroup haGroup, ClusterRoleRecord oldRecord,
            ClusterRoleRecord newRecord) throws SQLException;
}
