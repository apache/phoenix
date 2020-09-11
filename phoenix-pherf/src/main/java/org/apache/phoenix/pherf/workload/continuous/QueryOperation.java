package org.apache.phoenix.pherf.workload.continuous;

import org.apache.phoenix.pherf.configuration.Query;

/**
 * Defines a query operation.
 * @see {@link OperationType#SELECT}
 */
public interface QueryOperation extends Operation {
    Query getQuery();
}
