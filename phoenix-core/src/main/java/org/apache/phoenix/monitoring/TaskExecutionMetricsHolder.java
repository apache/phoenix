/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.phoenix.monitoring;

import static org.apache.phoenix.monitoring.MetricType.TASK_END_TO_END_TIME;
import static org.apache.phoenix.monitoring.MetricType.TASK_EXECUTED_COUNTER;
import static org.apache.phoenix.monitoring.MetricType.TASK_EXECUTION_TIME;
import static org.apache.phoenix.monitoring.MetricType.TASK_QUEUE_WAIT_TIME;
import static org.apache.phoenix.monitoring.MetricType.TASK_REJECTED_COUNTER;


/**
 * Class to encapsulate the various metrics associated with submitting and executing a task to the phoenix client
 * thread pool.
 */
public class TaskExecutionMetricsHolder {

    private final CombinableMetric taskQueueWaitTime;
    private final CombinableMetric taskEndToEndTime;
    private final CombinableMetric taskExecutionTime;
    private final CombinableMetric numTasks;
    private final CombinableMetric numRejectedTasks;
    public static final TaskExecutionMetricsHolder NO_OP_INSTANCE = new TaskExecutionMetricsHolder(new ReadMetricQueue(false), "");
    
    public TaskExecutionMetricsHolder(ReadMetricQueue readMetrics, String tableName) {
        taskQueueWaitTime = readMetrics.allotMetric(TASK_QUEUE_WAIT_TIME, tableName);
        taskEndToEndTime = readMetrics.allotMetric(TASK_END_TO_END_TIME, tableName);
        taskExecutionTime = readMetrics.allotMetric(TASK_EXECUTION_TIME, tableName);
        numTasks = readMetrics.allotMetric(TASK_EXECUTED_COUNTER, tableName);
        numRejectedTasks = readMetrics.allotMetric(TASK_REJECTED_COUNTER, tableName);
    }

    public CombinableMetric getTaskQueueWaitTime() {
        return taskQueueWaitTime;
    }

    public CombinableMetric getTaskEndToEndTime() {
        return taskEndToEndTime;
    }

    public CombinableMetric getTaskExecutionTime() {
        return taskExecutionTime;
    }

    public CombinableMetric getNumTasks() {
        return numTasks;
    }

    public CombinableMetric getNumRejectedTasks() {
        return numRejectedTasks;
    }

}