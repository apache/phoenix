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
import org.apache.phoenix.thirdparty.com.google.common.collect.ImmutableSet;
import org.apache.phoenix.util.JacksonUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;

/**
 * Immutable class representing an HA group store record with simplified fields.
 * This is a simplified version of ClusterRoleRecord that contains essential
 * information about an HA group only for a single cluster.
 */
public class HAGroupStoreRecord {

    private static final Logger LOG = LoggerFactory.getLogger(HAGroupStoreRecord.class);
    public static final String DEFAULT_PROTOCOL_VERSION = "1.0";

    /**
     * Enum representing the HA group state with each state having a corresponding ClusterRole.
     */
    public enum HAGroupState {
        ABORT_TO_ACTIVE_IN_SYNC,
        ABORT_TO_ACTIVE_NOT_IN_SYNC,
        ABORT_TO_STANDBY,
        ACTIVE_IN_SYNC,
        ACTIVE_NOT_IN_SYNC,
        ACTIVE_NOT_IN_SYNC_TO_STANDBY,
        ACTIVE_NOT_IN_SYNC_WITH_OFFLINE_PEER,
        ACTIVE_IN_SYNC_TO_STANDBY,
        ACTIVE_WITH_OFFLINE_PEER,
        DEGRADED_STANDBY,
        DEGRADED_STANDBY_FOR_READER,
        DEGRADED_STANDBY_FOR_WRITER,
        OFFLINE,
        STANDBY,
        STANDBY_TO_ACTIVE,
        UNKNOWN;

        private Set<HAGroupState> allowedTransitions;

        /**
         * Gets the corresponding ClusterRole for this HAGroupState.
         * @return the corresponding ClusterRole
         */
        public ClusterRoleRecord.ClusterRole getClusterRole() {
            switch (this) {
                case ABORT_TO_ACTIVE_IN_SYNC:
                case ABORT_TO_ACTIVE_NOT_IN_SYNC:
                case ACTIVE_IN_SYNC:
                case ACTIVE_NOT_IN_SYNC:
                case ACTIVE_NOT_IN_SYNC_WITH_OFFLINE_PEER:
                case ACTIVE_WITH_OFFLINE_PEER:
                    return ClusterRoleRecord.ClusterRole.ACTIVE;
                case ACTIVE_IN_SYNC_TO_STANDBY:
                case ACTIVE_NOT_IN_SYNC_TO_STANDBY:
                    return ClusterRoleRecord.ClusterRole.ACTIVE_TO_STANDBY;
                case ABORT_TO_STANDBY:
                case DEGRADED_STANDBY:
                case DEGRADED_STANDBY_FOR_READER:
                case DEGRADED_STANDBY_FOR_WRITER:
                case STANDBY:
                    return ClusterRoleRecord.ClusterRole.STANDBY;
                case STANDBY_TO_ACTIVE:
                    return ClusterRoleRecord.ClusterRole.STANDBY_TO_ACTIVE;
                case OFFLINE:
                    return ClusterRoleRecord.ClusterRole.OFFLINE;
                case UNKNOWN:
                default:
                    return ClusterRoleRecord.ClusterRole.UNKNOWN;
            }
        }

        static {
            // Initialize allowed transitions
            ACTIVE_NOT_IN_SYNC.allowedTransitions = ImmutableSet.of(
                    ACTIVE_NOT_IN_SYNC, ACTIVE_IN_SYNC,
                    ACTIVE_NOT_IN_SYNC_TO_STANDBY, ACTIVE_NOT_IN_SYNC_WITH_OFFLINE_PEER
            );

            ACTIVE_IN_SYNC.allowedTransitions = ImmutableSet.of(
                    ACTIVE_NOT_IN_SYNC, ACTIVE_WITH_OFFLINE_PEER, ACTIVE_IN_SYNC_TO_STANDBY
            );

            STANDBY.allowedTransitions = ImmutableSet.of(STANDBY_TO_ACTIVE,
                    DEGRADED_STANDBY_FOR_READER, DEGRADED_STANDBY_FOR_WRITER);
            // This needs to be manually recovered by operator
            OFFLINE.allowedTransitions = ImmutableSet.of();
            // This needs to be manually recovered by operator
            UNKNOWN.allowedTransitions = ImmutableSet.of();
            ACTIVE_NOT_IN_SYNC_TO_STANDBY.allowedTransitions
                    = ImmutableSet.of(ABORT_TO_ACTIVE_NOT_IN_SYNC,
                    ACTIVE_IN_SYNC_TO_STANDBY);
            ACTIVE_IN_SYNC_TO_STANDBY.allowedTransitions
                    = ImmutableSet.of(ABORT_TO_ACTIVE_IN_SYNC, STANDBY);
            STANDBY_TO_ACTIVE.allowedTransitions = ImmutableSet.of(ABORT_TO_STANDBY,
                    ACTIVE_IN_SYNC);
            DEGRADED_STANDBY.allowedTransitions
                    = ImmutableSet.of(DEGRADED_STANDBY_FOR_READER, DEGRADED_STANDBY_FOR_WRITER);
            DEGRADED_STANDBY_FOR_WRITER.allowedTransitions = ImmutableSet.of(STANDBY,
                    DEGRADED_STANDBY);
            DEGRADED_STANDBY_FOR_READER.allowedTransitions = ImmutableSet.of(STANDBY,
                    DEGRADED_STANDBY);
            ACTIVE_WITH_OFFLINE_PEER.allowedTransitions = ImmutableSet.of(ACTIVE_NOT_IN_SYNC);
            ABORT_TO_ACTIVE_IN_SYNC.allowedTransitions = ImmutableSet.of(ACTIVE_IN_SYNC);
            ABORT_TO_ACTIVE_NOT_IN_SYNC.allowedTransitions = ImmutableSet.of(ACTIVE_NOT_IN_SYNC);
            ABORT_TO_STANDBY.allowedTransitions = ImmutableSet.of(STANDBY);
            ACTIVE_NOT_IN_SYNC_WITH_OFFLINE_PEER.allowedTransitions =
                    ImmutableSet.of(ACTIVE_NOT_IN_SYNC);
        }

        /**
         * Checks if the transition from this state to the target state is allowed.
         * @param targetState the state to transition to
         * @return true if the transition is allowed, false otherwise
         */
        public boolean isTransitionAllowed(HAGroupState targetState) {
            return allowedTransitions.contains(targetState);
        }

        public static HAGroupState from(byte[] bytes) {
            if (bytes == null) {
                return UNKNOWN;
            }
            String value = new String(bytes, StandardCharsets.UTF_8);
            return Arrays.stream(HAGroupState.values())
                    .filter(r -> r.name().equalsIgnoreCase(value))
                    .findFirst()
                    .orElse(UNKNOWN);
        }
    }

    private final String protocolVersion;
    private final String haGroupName;
    private final HAGroupState haGroupState;
    private final Long lastSyncStateTimeInMs;

    @JsonCreator
    public HAGroupStoreRecord(@JsonProperty("protocolVersion") String protocolVersion,
                              @JsonProperty("haGroupName") String haGroupName,
                              @JsonProperty("haGroupState") HAGroupState haGroupState,
                              @JsonProperty("lastSyncStateTimeInMs") Long lastSyncStateTimeInMs) {
        Preconditions.checkNotNull(haGroupName, "HA group name cannot be null!");
        Preconditions.checkNotNull(haGroupState, "HA group state cannot be null!");

        this.protocolVersion = Objects.toString(protocolVersion, DEFAULT_PROTOCOL_VERSION);
        this.haGroupName = haGroupName;
        this.haGroupState = haGroupState;
        this.lastSyncStateTimeInMs = lastSyncStateTimeInMs;
    }

    /**
     * Convenience constructor for backward compatibility without lastSyncStateTimeInMs.
     */
    public HAGroupStoreRecord(String protocolVersion,
                              String haGroupName, HAGroupState haGroupState) {
        this(protocolVersion, haGroupName, haGroupState, null);
    }

    public static Optional<HAGroupStoreRecord> fromJson(byte[] bytes) {
        if (bytes == null) {
            return Optional.empty();
        }
        try {
            return Optional.of(JacksonUtil.getObjectReader(HAGroupStoreRecord.class)
                    .readValue(bytes));
        } catch (Exception e) {
            LOG.error("Fail to deserialize data to an HA group store record", e);
            return Optional.empty();
        }
    }

    public static byte[] toJson(HAGroupStoreRecord record) throws IOException {
        return JacksonUtil.getObjectWriter().withoutAttribute("clusterRole")
                .writeValueAsBytes(record);
    }

    public boolean hasSameInfo(HAGroupStoreRecord other) {
        return haGroupName.equals(other.haGroupName) 
                && haGroupState.equals(other.haGroupState) 
                && protocolVersion.equals(other.protocolVersion) 
                && Objects.equals(lastSyncStateTimeInMs, other.lastSyncStateTimeInMs);
    }

    public String getProtocolVersion() {
        return protocolVersion;
    }

    public String getHaGroupName() {
        return haGroupName;
    }

    @JsonProperty("haGroupState")
    public HAGroupState getHAGroupState() {
        return haGroupState;
    }

    public Long getLastSyncStateTimeInMs() {
        return lastSyncStateTimeInMs;
    }

    @JsonIgnore
    public ClusterRoleRecord.ClusterRole getClusterRole() {
        return haGroupState.getClusterRole();
    }

    @Override
    public int hashCode() {
        return new HashCodeBuilder()
                .append(protocolVersion)
                .append(haGroupName)
                .append(haGroupState)
                .append(lastSyncStateTimeInMs)
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
                    .append(haGroupState, otherRecord.haGroupState)
                    .append(lastSyncStateTimeInMs, otherRecord.lastSyncStateTimeInMs)
                    .isEquals();
        }
    }

    @Override
    public String toString() {
        return "HAGroupStoreRecord{"
                + "protocolVersion='" + protocolVersion + '\''
                + ", haGroupName='" + haGroupName + '\''
                + ", haGroupState=" + haGroupState
                + ", lastSyncStateTimeInMs=" + lastSyncStateTimeInMs
                + '}';
    }

    public String toPrettyString() {
        try {
            return JacksonUtil.getObjectWriterPretty().writeValueAsString(this);
        } catch (Exception e) {
            LOG.error("Fail to wrap this object as JSON, returning the oneliner "
                    + "using toString", e);
            return toString();
        }
    }
}