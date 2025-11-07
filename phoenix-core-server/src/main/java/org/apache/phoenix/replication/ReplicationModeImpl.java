package org.apache.phoenix.replication;

import java.io.IOException;

import org.apache.phoenix.replication.ReplicationLogGroup.Record;
import org.apache.phoenix.replication.ReplicationLogGroup.ReplicationMode;
import org.apache.phoenix.thirdparty.com.google.common.annotations.VisibleForTesting;

/**
 * Base class for different replication modes.
 * <p>
 * This abstract class manages the lifecycle of a replication mode. It also delegates the
 * append and sync events to the underlying ReplicationLog object.
 * </p>
 */
public abstract class ReplicationModeImpl {
    protected final ReplicationLogGroup logGroup;

    // The mode manages the underlying log to which the append and sync events will be sent
    protected ReplicationLog log;

    protected ReplicationModeImpl(ReplicationLogGroup logGroup) {
        this.logGroup = logGroup;
    }

    /**
     * Invoked when we switch to this mode
     *
     * @throws IOException
     */
    abstract void onEnter() throws IOException;

    /**
     * Invoked when we switch out from this mode
     *
     * @param gracefulShutdown True if graceful, False if forced
     */
    abstract void onExit(boolean gracefulShutdown);

    /**
     * Invoked when there is a failure event on this mode
     *
     * @param cause Failure exception
     * @return new mode to switch to
     * @throws IOException
     */
    abstract ReplicationMode onFailure(Throwable cause) throws IOException;

    abstract ReplicationMode getMode();

    @Override
    public String toString() {
        return getMode().name();
    }

    /** Returns the underlying log abstraction */
    @VisibleForTesting
    ReplicationLog getReplicationLog() {
        return log;
    }

    /**
     * Delegates the append event to the underlying log
     *
     * @param r Mutation
     * @throws IOException
     */
    void append(Record r) throws IOException {
        getReplicationLog().append(r);
    }

    /**
     * Delegates the sync event to the underlying log
     *
     * @throws IOException
     */
    void sync() throws IOException {
        getReplicationLog().sync();
    }

    /** Graceful close */
    void closeReplicationLog() {
        if (log != null) {
            log.close();
        }
    }

    /** Forced close */
    void closeReplicationLogOnError() {
        if (log != null) {
            log.closeOnError();
        }
    }
}
