package org.apache.phoenix.pherf.workload.continuous;

import org.apache.phoenix.pherf.configuration.Upsert;

/**
 * Defines an upsert operation.
 * @see {@link OperationType#UPSERT}
 */
public interface UpsertOperation extends Operation {
    Upsert getUpsert();
}
