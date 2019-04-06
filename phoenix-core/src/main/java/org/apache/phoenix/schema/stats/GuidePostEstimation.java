/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to you under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.phoenix.schema.stats;

import org.apache.phoenix.util.SizedUtil;

public final class GuidePostEstimation {
    public static final long MAX_TIMESTAMP = Long.MAX_VALUE;

    /**
     * The byte count of the guide post
     */
    private long byteCount;

    /**
     * The row count of the guide post
     */
    private long rowCount;

    /**
     * The timestamp at which the guide post was created/updated
     */
    private long timestamp;

    public GuidePostEstimation() {
        this(0, 0, GuidePostEstimation.MAX_TIMESTAMP);
    }

    public GuidePostEstimation(long byteCount, long rowCount, long timestamp) {
        this.byteCount = byteCount;
        this.rowCount = rowCount;
        this.timestamp = timestamp;
    }

    public int getEstimatedSize() {
        return SizedUtil.LONG_SIZE * 3;
    }

    /**
     * Merge the other guide post estimation object into this object which contains the sum of rows,
     * the sum of bytes and the least update time stamp of the two objects.
     * * @param other
     */
    public void merge(GuidePostEstimation other) {
        if (other != null) {
            this.byteCount += other.byteCount;
            this.rowCount += other.rowCount;
            this.timestamp = Math.min(this.timestamp, other.timestamp);
        }
    }

    @Override
    public String toString() {
        return "Byte Count = " + byteCount + ", Row Count = " + rowCount + ", Timestamp = " + timestamp;
    }

    @Override
    public boolean equals(Object o) {
        if (!(o instanceof GuidePostEstimation)) {
            return false;
        }
        GuidePostEstimation that = (GuidePostEstimation)o;
        return this.byteCount == that.byteCount && this.rowCount == that.rowCount && this.timestamp == that.timestamp;
    }

    @Override
    public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = prime * result + (new Long(this.byteCount)).hashCode();
        result = prime * result + (new Long(this.rowCount)).hashCode();
        result = prime * result + (new Long(this.timestamp)).hashCode();

        return result;
    }

    public long getByteCount() {
        return byteCount;
    }

    public void setByteCount(long byteCount) {
        this.byteCount = byteCount;
    }

    public void addByteCount(long byteCount) {
        this.byteCount += byteCount;
    }

    public long getRowCount() {
        return rowCount;
    }

    public void setRowCount(long rowCount) {
        this.rowCount = rowCount;
    }

    public void addRowCount(long rowCount) {
        this.rowCount += rowCount;
    }

    public long getTimestamp() {
        return timestamp;
    }

    public void setTimestamp(long timestamp) {
        this.timestamp = timestamp;
    }
}
