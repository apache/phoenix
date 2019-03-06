package org.apache.phoenix.coprocessor.tasks;

import org.apache.hadoop.hbase.coprocessor.RegionCoprocessorEnvironment;
import org.apache.phoenix.coprocessor.TaskRegionObserver;
import org.apache.phoenix.schema.task.Task;

public abstract class BaseTask {
    protected long timeMaxInterval;
    protected RegionCoprocessorEnvironment env;
    public void init(RegionCoprocessorEnvironment env, Long interval) {
        this.env = env;
        this.timeMaxInterval = interval;
    }
    public abstract TaskRegionObserver.TaskResult run(Task.TaskRecord taskRecord);

    public abstract TaskRegionObserver.TaskResult checkCurrentResult(Task.TaskRecord taskRecord) throws Exception;
}
