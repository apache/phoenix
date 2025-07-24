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

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import org.apache.commons.lang3.builder.EqualsBuilder;
import org.apache.commons.lang3.builder.HashCodeBuilder;
import org.apache.phoenix.thirdparty.com.google.common.base.Preconditions;
import org.apache.phoenix.util.JDBCUtil;
import org.apache.phoenix.util.JacksonUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.Optional;

/**
 * Immutable class of a cluster role record for a pair of HBase clusters.
 *
 * This is the data model used by:
 * - Admin uses command line tool to write records of this class to ZK nodes
 * - Clients reads and registers watcher to get data of this class from ZK nodes
 *
 * The cluster roles can be updated for a given HA group, in which case a new cluster role record
 * will be saved in either configuration file for Admin tool or the znode data for clients.  For
 * any updates like that, the new cluster role record for that HA group should bump the version.
 * This is to ensure data integrity across updates.  Meanwhile, other fields are not allowed to
 * change for an existing HA group.  If the HA group needs to change its behavior, it will affect
 * all clients, which are not controlled or tracked by Phoenix HA framework.  To assist that
 * scenario like switching HA polices, it is advised to create a new HA group and delete the old HA
 * group after all clients have migrated.
 *
 * This class is immutable.
 */
public class ClusterRoleRecord {
    private static final Logger LOG = LoggerFactory.getLogger(ClusterRoleRecord.class);

    /**
     * Enum for the current state of the cluster.  Exact meaning depends on the Policy but in general Active clusters
     * take traffic, standby and offline do not, and unknown is used if the state cannot be determined.
     */
    public enum ClusterRole {
        ACTIVE, STANDBY, OFFLINE, UNKNOWN, ACTIVE_TO_STANDBY, STANDBY_TO_ACTIVE;

        /**
         * @return true if a cluster with this role can be connected, otherwise false
         */
        public boolean canConnect() {
            return this == ACTIVE || this == STANDBY
                    || this == ACTIVE_TO_STANDBY || this == STANDBY_TO_ACTIVE;
        }

        public static ClusterRole from(byte[] bytes) {
            String value = new String(bytes, StandardCharsets.UTF_8);
            return Arrays.stream(ClusterRole.values())
                    .filter(r -> r.name().equalsIgnoreCase(value))
                    .findFirst()
                    .orElse(UNKNOWN);
        }

        public boolean isMutationBlocked() {
            return this == ACTIVE_TO_STANDBY;
        }

        /**
         * @return the default HAGroupState for this ClusterRole
         */
        @JsonIgnore
        public HAGroupStoreRecord.HAGroupState getDefaultHAGroupState() {
            switch (this) {
                case ACTIVE:
                    return HAGroupStoreRecord.HAGroupState.ACTIVE_NOT_IN_SYNC;
                case STANDBY:
                    return HAGroupStoreRecord.HAGroupState.DEGRADED_STANDBY;
                case OFFLINE:
                    return HAGroupStoreRecord.HAGroupState.OFFLINE;
                case ACTIVE_TO_STANDBY:
                    return HAGroupStoreRecord.HAGroupState.ACTIVE_TO_STANDBY;
                case STANDBY_TO_ACTIVE:
                    return HAGroupStoreRecord.HAGroupState.STANDBY_TO_ACTIVE;
                case UNKNOWN:
                default:
                    return HAGroupStoreRecord.HAGroupState.UNKNOWN;
            }
        }
    }

    /**
     * Enum for HBaseRegistryType being used in current clusterRoleRecord, final connection url
     * are constructed based on RegistryType and urls stored in clusterRoleRecord
     */
    public enum RegistryType {
        ZK, MASTER, RPC
    }

    private final String haGroupName;
    private final HighAvailabilityPolicy policy;
    private final RegistryType registryType;
    private final String url1;
    private final ClusterRole role1;
    private final String url2;
    private final ClusterRole role2;
    private final long version;

    /**
     * To handle backward compatibility with old  ClusterRoleRecords which had zk1 and zk2 as keys
     * for zk urls, This constructor is only being used {@link ClusterRoleRecord#fromJson} when we
     * deserialize Cluster Role Record read from ZooKeeper ZNode. If CRR is in old format we will
     * read zk1 and zk2 and url1 and url2 will be null and if it is in new format zk1 and zk2 will
     * be null in both cases final url is being stored in url1 and url2
     * url will be stored in normalized forms which looks like zk1\\:port1,zk2\\:port2,zk3\\:port3,
     * zk4\\:port4,zk5\\:port5::znode or master1\\:port1,master2\\:port2,master3\\:port3,
     * master4\\:port4,master5\\:port5
     * @param haGroupName HighAvailability Group name / CRR name
     * @param policy Policy used by give CRR
     * @param registryType {@link RegistryType} to be used for given urls
     * @param url1 ZK/HMaster url based on registry type for first cluster
     * @param role1 {@link ClusterRole} which describes the current state of first cluster
     * @param url2 ZK/HMaster url based on registry type for second cluster
     * @param role2 {@link ClusterRole} which describes the current state of second cluster
     * @param version version of a given CRR
     * @param zk1 ZK url of first cluster when CRR is in old format for backward compatibility
     * @param zk2 ZK url of second cluster when CRR is in old format for backward compatibility
     */
    @JsonCreator
    public ClusterRoleRecord(@JsonProperty("haGroupName") String haGroupName,
                             @JsonProperty("policy") HighAvailabilityPolicy policy,
                             @JsonProperty("registryType") RegistryType registryType,
                             @JsonProperty("url1") String url1,
                             @JsonProperty("role1") ClusterRole role1,
                             @JsonProperty("url2") String url2,
                             @JsonProperty("role2") ClusterRole role2,
                             @JsonProperty("version") long version,
                             @JsonProperty("zk1") String zk1,
                             @JsonProperty("zk2") String zk2) {
        this.haGroupName = haGroupName;
        this.policy = policy;
        this.registryType = registryType != null ? registryType : RegistryType.ZK;

        String resolvedUrl1 = (url1 != null) ? url1 : zk1; //For Backward Compatibility
        String resolvedUrl2 = (url2 != null) ? url2 : zk2; //For Backward Compatibility

        //Do we really need to normalize here ?
        //We are normalizing to have urls in specific formats for each registryType for getting
        //accurate comparisons. We are passing registryType as these url most probably won't have
        //protocol in url, and it might be normalized based to wrong registry type, as we normalize
        //w.r.t {@link ConnectionInfo.CLIENT_CONNECTION_REGISTRY_IMPL_CONF_KEY},
        //but considering source of truth of registryType is present in CLusterRoleRecord we are
        //normalizing based on that.
        //url will be in form :- zk1\\:port1,zk2\\:port2,zk3\\:port3,zk4\\:port4,zk5\\:port5::znode
        //or master1\\:port1,master2\\:port2,master3\\:port3,master4\\:port4,master5\\:port5
        url1 = JDBCUtil.formatUrl(resolvedUrl1, this.registryType);
        url2 = JDBCUtil.formatUrl(resolvedUrl2, this.registryType);

        Preconditions.checkArgument(!url1.equals(url2), "Two clusters have the same URLS!");
        Preconditions.checkNotNull(role1, "Role of a cluster cannot be null!");
        Preconditions.checkNotNull(role2, "Role of a cluster cannot be null!");

        // Ignore the given order of url1 and url2
        if (url1.compareTo(url2) < 0) {
            this.url1 = url1;
            this.role1 = role1;
            this.url2 = url2;
            this.role2 = role2;
        } else {
            this.url1 = url2;
            this.role1 = role2;
            this.url2 = url1;
            this.role2 = role1;
        }
        this.version = version;
    }

    public ClusterRoleRecord(String haGroupName, HighAvailabilityPolicy policy,
                             String url1, ClusterRole role1,
                             String url2, ClusterRole role2,
                             long version) {
        this(haGroupName, policy, RegistryType.ZK, url1, role1, url2, role2, version, null, null);
    }

    public ClusterRoleRecord(String haGroupName, HighAvailabilityPolicy policy,
                             RegistryType registryType,
                             String url1, ClusterRole role1,
                             String url2, ClusterRole role2,
                             long version) {
        this(haGroupName, policy, registryType, url1, role1, url2, role2, version, null, null);
    }

    public static Optional<ClusterRoleRecord> fromJson(byte[] bytes) {
        if (bytes == null) {
            return Optional.empty();
        }
        try {
            return Optional.of(JacksonUtil.getObjectReader(ClusterRoleRecord.class).readValue(bytes));
        } catch (Exception e) {
            LOG.error("Fail to deserialize data to a cluster role store", e);
            return Optional.empty();
        }
    }

    public static byte[] toJson(ClusterRoleRecord record) throws IOException {
        return JacksonUtil.getObjectWriter().writeValueAsBytes(record);
    }

    @JsonIgnore
    public Optional<String> getActiveUrl() {
        if (role1 == ClusterRole.ACTIVE) {
            return Optional.of(url1);
        }
        if (role2 == ClusterRole.ACTIVE) {
            return Optional.of(url2);
        }
        return Optional.empty();
    }

    /**
     * @return true if this is newer than the given cluster role record.
     */
    public boolean isNewerThan(ClusterRoleRecord other) {
        if (other == null) {
            return true;
        }
        return this.hasSameInfo(other) && this.version > other.version;
    }

    public boolean hasSameInfo(ClusterRoleRecord other) {
        return haGroupName.equals(other.haGroupName) &&
                policy.equals(other.policy);
    }

    /**
     * @return role by url or UNKNOWN if the Url does not belong to this HA group
     */
    public ClusterRole getRole(String url) {
        if (url1.equals(url)) {
            return role1;
        } else if (url2.equals(url)) {
            return role2;
        } else {
            return ClusterRole.UNKNOWN;
        }
    }

    public String getHaGroupName() {
        return haGroupName;
    }

    public HighAvailabilityPolicy getPolicy() {
        return policy;
    }

    public RegistryType getRegistryType() {
        return registryType;
    }

    public String getUrl1() {
        return url1;
    }

    public ClusterRole getRole1() {
        return role1;
    }

    public String getUrl2() {
        return url2;
    }

    public ClusterRole getRole2() {
        return role2;
    }

    public long getVersion() {
        return version;
    }

    @Override
    public int hashCode() {
        return new HashCodeBuilder()
                .append(haGroupName)
                .append(policy)
                .append(registryType)
                .append(url1)
                .append(role1)
                .append(url2)
                .append(role2)
                .append(version)
                .hashCode();
    }

    @Override
    public boolean equals(Object other) {
        if (other == this) {
            return true;
        } else if (other == null) {
            return false;
        } else if (!(other instanceof ClusterRoleRecord)) {
            return false;
        } else {
            ClusterRoleRecord otherRecord = (ClusterRoleRecord) other;
            return new EqualsBuilder()
                    .append(haGroupName, otherRecord.haGroupName)
                    .append(policy, otherRecord.policy)
                    .append(registryType, otherRecord.registryType)
                    .append(url1, otherRecord.url1)
                    .append(role1, otherRecord.role1)
                    .append(url2, otherRecord.url2)
                    .append(role2, otherRecord.role2)
                    .append(version, otherRecord.version)
                    .isEquals();
        }
    }

    @Override
    public String toString() {
        return "ClusterRoleRecord{"
                + "haGroupName='" + haGroupName + '\''
                + ", policy=" + policy
                + ", registryType=" + registryType
                + ", url1='" + url1 + '\''
                + ", role1=" + role1
                + ", url2='" + url2 + '\''
                + ", role2=" + role2
                + ", version=" + version
                + '}';
    }

    public String toPrettyString() {
        try {
            return JacksonUtil.getObjectWriterPretty().writeValueAsString(this);
        } catch (Exception e) {
            LOG.error("Fail to wrap this object as JSON, retuning the oneliner using toString", e);
            return toString();
        }
    }


}
