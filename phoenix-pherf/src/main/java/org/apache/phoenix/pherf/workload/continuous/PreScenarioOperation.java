package org.apache.phoenix.pherf.workload.continuous;

import org.apache.phoenix.pherf.configuration.Ddl;
import org.apache.phoenix.pherf.configuration.Upsert;

import java.util.List;

/**
 * Defines a pre scenario operation.
 * @see {@link OperationType#PRE_RUN}
 */
public interface PreScenarioOperation extends Operation {
    List<Ddl> getPreScenarioDdls();
}
