package org.apache.phoenix.schema.stats;

public final class GuidePostEstimation {
    /**
     * The row count of the guide post
     */
    private long rowCount;

    /**
     * The byte count of the guide post
     */
    private long byteCount;

    /**
     * The timestamp at which the guide post was created/updated
     */
    private long timestamp;

    public GuidePostEstimation() {
        this(0, 0, Long.MAX_VALUE);
    }

    public GuidePostEstimation(long rowCount, long byteCount, long timestamp) {
        this.rowCount = rowCount;
        this.byteCount = byteCount;
        this.timestamp = timestamp;
    }

    public GuidePostEstimation(GuidePostEstimation other) {
        this.rowCount = other.rowCount;
        this.byteCount = other.byteCount;
        this.timestamp = other.timestamp;
    }

    /**
     * Merge the two guide post estimation objects into one which contains the sum of rows,
     * the sum of bytes and the least update time stamp of the two objects.
     * @param left
     * @param right
     * @return the new guide post estimation object which contains the "Sum" info.
     */
    public static GuidePostEstimation merge(GuidePostEstimation left, GuidePostEstimation right) {
        if (left != null || right != null) {
            GuidePostEstimation estimation = new GuidePostEstimation(left);
            estimation.merge(right);
            return estimation;
        }

        return null;
    }

    /**
     * Merge the other guide post estimation object into this object which contains the sum of rows,
     * the sum of bytes and the least update time stamp of the two objects.
     * * @param other
     */
    public void merge(GuidePostEstimation other) {
        if (other != null) {
            this.rowCount += other.rowCount;
            this.byteCount += byteCount;
            this.timestamp = Math.min(this.timestamp, other.timestamp);
        }
    }

    public long getRowCount() {
        return rowCount;
    }

    public void setRowCount(long rowCount) {
        this.rowCount = rowCount;
    }

    public long getByteCount() {
        return byteCount;
    }

    public void setByteCount(long byteCount) {
        this.byteCount = byteCount;
    }

    public long getTimestamp() {
        return timestamp;
    }

    public void setTimestamp(long timestamp) {
        this.timestamp = timestamp;
    }
}
