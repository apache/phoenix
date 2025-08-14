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

import org.apache.commons.lang3.builder.EqualsBuilder;
import org.apache.commons.lang3.builder.HashCodeBuilder;

/**
 * Subclass of HAGroupStoreRecord that includes additional metadata.
 * Extends the base HAGroupStoreRecord with a lastModifiedTime field
 * for tracking when the record was last updated.
 */
public class HAGroupStoreRecordWithMetadata extends HAGroupStoreRecord {

    private final Long lastModifiedTime;

    /**
     * Constructor that creates a HAGroupStoreRecordWithMetadata from an existing HAGroupStoreRecord.
     */
    public HAGroupStoreRecordWithMetadata(HAGroupStoreRecord baseRecord, Long lastModifiedTime) {
        super(baseRecord.getProtocolVersion(), baseRecord.getHaGroupName(), baseRecord.getHAGroupState());
        this.lastModifiedTime = lastModifiedTime;
    }

    public Long getLastModifiedTime() {
        return lastModifiedTime;
    }

    @Override
    public int hashCode() {
        return new HashCodeBuilder()
                .appendSuper(super.hashCode())
                .append(lastModifiedTime)
                .hashCode();
    }

    @Override
    public boolean equals(Object other) {
        if (other == this) {
            return true;
        } else if (other == null) {
            return false;
        } else if (!(other instanceof HAGroupStoreRecordWithMetadata)) {
            return false;
        } else {
            HAGroupStoreRecordWithMetadata otherRecord = (HAGroupStoreRecordWithMetadata) other;
            return new EqualsBuilder()
                    .appendSuper(super.equals(other))
                    .append(lastModifiedTime, otherRecord.lastModifiedTime)
                    .isEquals();
        }
    }

    @Override
    public String toString() {
        return "HAGroupStoreRecordWithMetadata{"
                + "protocolVersion='" + getProtocolVersion() + '\''
                + ", haGroupName='" + getHaGroupName() + '\''
                + ", haGroupState=" + getHAGroupState()
                + ", lastModifiedTime=" + lastModifiedTime
                + '}';
    }
}
