package org.apache.phoenix.pherf.workload.continuous;

/**
 * An interface that defines the type of operation included in the load profile.
 * @see {@link org.apache.phoenix.pherf.configuration.LoadProfile}
 */
public interface Operation {
    enum OperationType {
        PRE_RUN, UPSERT, SELECT, NO_OP, USER_DEFINED
    }
    String getId();
    OperationType getType();
}
