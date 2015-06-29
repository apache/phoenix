package org.apache.phoenix.pherf.workload;

public interface Workload {
    public Runnable execute() throws Exception;

    /**
     * Use this method to perform any cleanup or forced shutdown of the thread.
     */
    public void complete();
}
