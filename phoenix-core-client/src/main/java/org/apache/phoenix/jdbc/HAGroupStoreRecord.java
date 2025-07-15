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
import com.fasterxml.jackson.annotation.JsonProperty;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.builder.EqualsBuilder;
import org.apache.commons.lang3.builder.HashCodeBuilder;
import org.apache.phoenix.thirdparty.com.google.common.base.Preconditions;
import org.apache.phoenix.util.JacksonUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Objects;
import java.util.Optional;

/**
 * Immutable class representing an HA group store record with simplified fields.
 *
 * This is a simplified version of ClusterRoleRecord that contains essential
 * information about an HA group only for a single cluster.
 *
 * This class is immutable.
 */
public class HAGroupStoreRecord {
    private static final Logger LOG = LoggerFactory.getLogger(HAGroupStoreRecord.class);
    public static final String DEFAULT_PROTOCOL_VERSION = "1.0";

    private final String protocolVersion;
    private final String haGroupName;
    private final ClusterRoleRecord.ClusterRole clusterRole;
    private final long version;
    private final String policy;
    private final long lastUpdatedTimeInMs;
    private final String peerZKUrl;

    @JsonCreator
    public HAGroupStoreRecord(@JsonProperty("protocolVersion") String protocolVersion,
                              @JsonProperty("haGroupName") String haGroupName,
                              @JsonProperty("clusterRole") ClusterRoleRecord.ClusterRole clusterRole,
                              @JsonProperty("version") long version,
                              @JsonProperty("policy") String policy,
                              @JsonProperty("lastUpdatedTimeInMs") long lastUpdatedTimeInMs,
                              @JsonProperty("peerZKUrl") String peerZKUrl) {
        Preconditions.checkNotNull(haGroupName, "HA group name cannot be null!");
        Preconditions.checkNotNull(clusterRole, "Cluster role cannot be null!");
        Preconditions.checkNotNull(policy, "Policy cannot be null!");

        this.protocolVersion = Objects.toString(protocolVersion, DEFAULT_PROTOCOL_VERSION);
        this.haGroupName = haGroupName;
        this.clusterRole = clusterRole;
        this.version = version;
        this.policy = policy;
        this.lastUpdatedTimeInMs = lastUpdatedTimeInMs;
        this.peerZKUrl = peerZKUrl;
    }

    public static Optional<HAGroupStoreRecord> fromJson(byte[] bytes) {
        if (bytes == null) {
            return Optional.empty();
        }
        try {
            return Optional.of(JacksonUtil.getObjectReader(HAGroupStoreRecord.class).readValue(bytes));
        } catch (Exception e) {
            LOG.error("Fail to deserialize data to an HA group store record", e);
            return Optional.empty();
        }
    }

    public static byte[] toJson(HAGroupStoreRecord record) throws IOException {
        return JacksonUtil.getObjectWriter().writeValueAsBytes(record);
    }

    /**
     * @return true if this record is newer than the given record.
     */
    public boolean isNewerThan(HAGroupStoreRecord other) {
        if (other == null) {
            return true;
        }
        return this.hasSameInfo(other) && this.version > other.version;
    }

    public boolean hasSameInfo(HAGroupStoreRecord other) {
        return haGroupName.equals(other.haGroupName) &&
                protocolVersion.equals(other.protocolVersion) &&
                policy.equals(other.policy);
    }

    public String getProtocolVersion() {
        return protocolVersion;
    }

    public String getHaGroupName() {
        return haGroupName;
    }

    public ClusterRoleRecord.ClusterRole getClusterRole() {
        return clusterRole;
    }

    public long getVersion() {
        return version;
    }

    public String getPolicy() {
        return policy;
    }

    public long getLastUpdatedTimeInMs() {
        return lastUpdatedTimeInMs;
    }

    public String getPeerZKUrl() {
        return peerZKUrl;
    }

    @Override
    public int hashCode() {
        return new HashCodeBuilder()
                .append(protocolVersion)
                .append(haGroupName)
                .append(clusterRole)
                .append(version)
                .append(policy)
                .append(lastUpdatedTimeInMs)
                .append(peerZKUrl)
                .hashCode();
    }

    @Override
    public boolean equals(Object other) {
        if (other == this) {
            return true;
        } else if (other == null) {
            return false;
        } else if (!(other instanceof HAGroupStoreRecord)) {
            return false;
        } else {
            HAGroupStoreRecord otherRecord = (HAGroupStoreRecord) other;
            return new EqualsBuilder()
                    .append(protocolVersion, otherRecord.protocolVersion)
                    .append(haGroupName, otherRecord.haGroupName)
                    .append(clusterRole, otherRecord.clusterRole)
                    .append(version, otherRecord.version)
                    .append(policy, otherRecord.policy)
                    .append(lastUpdatedTimeInMs, otherRecord.lastUpdatedTimeInMs)
                    .append(peerZKUrl, otherRecord.peerZKUrl)
                    .isEquals();
        }
    }

    @Override
    public String toString() {
        return "HAGroupStoreRecord{"
                + "protocolVersion='" + protocolVersion + '\''
                + ", haGroupName='" + haGroupName + '\''
                + ", clusterRole=" + clusterRole
                + ", version=" + version
                + ", policy='" + policy + '\''
                + ", lastUpdatedTimeInMs=" + lastUpdatedTimeInMs
                + ", peerZKUrl='" + peerZKUrl + '\''
                + '}';
    }

    public String toPrettyString() {
        try {
            return JacksonUtil.getObjectWriterPretty().writeValueAsString(this);
        } catch (Exception e) {
            LOG.error("Fail to wrap this object as JSON, returning the oneliner using toString", e);
            return toString();
        }
    }
}