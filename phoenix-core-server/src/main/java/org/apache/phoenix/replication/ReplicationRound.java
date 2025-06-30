package org.apache.phoenix.replication;

import com.google.common.base.Preconditions;

import java.util.Objects;

public class ReplicationRound {

    private final long startTime;
    private final long endTime;

    public ReplicationRound(long startTime, long endTime) {
        Preconditions.checkArgument(startTime < endTime);
        this.startTime = startTime;
        this.endTime = endTime;
    }

    public long getStartTime() {
        return startTime;
    }

    public long getEndTime() {
        return endTime;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        ReplicationRound that = (ReplicationRound) o;
        return startTime == that.startTime && endTime == that.endTime;
    }

    @Override
    public int hashCode() {
        return Objects.hash(startTime, endTime);
    }
}
