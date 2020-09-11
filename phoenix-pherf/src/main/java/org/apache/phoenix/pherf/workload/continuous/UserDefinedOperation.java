package org.apache.phoenix.pherf.workload.continuous;

import org.apache.phoenix.pherf.configuration.UserDefined;

/**
 * Defines an user defined operation.
 * @see {@link OperationType#USER_DEFINED}
 */
public interface UserDefinedOperation extends Operation {
    UserDefined getUserFunction();
}
