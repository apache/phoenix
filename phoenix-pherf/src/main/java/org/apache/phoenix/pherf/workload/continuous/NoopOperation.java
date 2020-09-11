package org.apache.phoenix.pherf.workload.continuous;

import org.apache.phoenix.pherf.configuration.Noop;

/**
 * Defines a no op operation, typically used to simulate idle time.
 * @see {@link OperationType#NO_OP}s
 */
public interface NoopOperation extends Operation {
    Noop getNoop();
}
