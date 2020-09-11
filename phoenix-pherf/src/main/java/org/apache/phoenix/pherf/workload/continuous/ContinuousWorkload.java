package org.apache.phoenix.pherf.workload.continuous;

public interface ContinuousWorkload {
    /**
     * Initializes and readies the processor for continuous queue based workloads
     */
    void start();

    /**
     * Stop the processor and cleans up the workload queues.
     */
    void stop();

}
