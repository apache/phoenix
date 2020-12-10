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
package org.apache.phoenix.coprocessor;

import java.io.IOException;
import java.lang.reflect.Method;
import java.sql.SQLException;
import java.sql.Timestamp;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.TimerTask;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import javax.annotation.concurrent.GuardedBy;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.phoenix.schema.task.SystemTaskParams;
import org.apache.phoenix.thirdparty.com.google.common.base.Strings;
import org.apache.phoenix.thirdparty.com.google.common.collect.ImmutableMap;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.CoprocessorEnvironment;
import org.apache.hadoop.hbase.coprocessor.ObserverContext;
import org.apache.hadoop.hbase.coprocessor.RegionCoprocessor;
import org.apache.hadoop.hbase.coprocessor.RegionCoprocessorEnvironment;
import org.apache.hadoop.hbase.coprocessor.RegionObserver;
import org.apache.phoenix.jdbc.PhoenixConnection;
import org.apache.phoenix.query.QueryServices;
import org.apache.phoenix.query.QueryServicesOptions;
import org.apache.phoenix.schema.PTable;
import org.apache.phoenix.schema.PTable.TaskType;
import org.apache.phoenix.schema.task.Task;
import org.apache.phoenix.util.EnvironmentEdgeManager;
import org.apache.phoenix.util.JacksonUtil;
import org.apache.phoenix.util.QueryUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Coprocessor for task related operations. This coprocessor would only be registered
 * to SYSTEM.TASK table
 */

public class TaskRegionObserver implements RegionObserver, RegionCoprocessor {
    public static final Logger LOGGER = LoggerFactory.getLogger(TaskRegionObserver.class);
    public static final String TASK_DETAILS = "TaskDetails";

    protected ScheduledThreadPoolExecutor executor = new ScheduledThreadPoolExecutor(TaskType.values().length);
    private long timeInterval = QueryServicesOptions.DEFAULT_TASK_HANDLING_INTERVAL_MS;
    private long timeMaxInterval = QueryServicesOptions.DEFAULT_TASK_HANDLING_MAX_INTERVAL_MS;
    @GuardedBy("TaskRegionObserver.class")
    private long initialDelay = QueryServicesOptions.DEFAULT_TASK_HANDLING_INITIAL_DELAY_MS;

    private static Map<TaskType, String> classMap = ImmutableMap.<TaskType, String>builder()
            .put(TaskType.DROP_CHILD_VIEWS, "org.apache.phoenix.coprocessor.tasks.DropChildViewsTask")
            .put(TaskType.INDEX_REBUILD, "org.apache.phoenix.coprocessor.tasks.IndexRebuildTask")
            .build();

    public enum TaskResultCode {
        SUCCESS,
        FAIL,
        SKIPPED,
    }

    public static class TaskResult {
        private TaskResultCode resultCode;
        private String details;

        public TaskResult(TaskResultCode resultCode, String details) {
            this.resultCode = resultCode;
            this.details = details;
        }

        public TaskResultCode getResultCode() {
            return resultCode;
        }

        public String getDetails() {
            return details;
        }

        @Override
        public String toString() {
            String result = resultCode.name();
            if (!Strings.isNullOrEmpty(details)) {
                result = result + " - " + details;
            }
            return result;
        }
    }

    @Override
    public void preClose(final ObserverContext<RegionCoprocessorEnvironment> c,
            boolean abortRequested) {
        executor.shutdownNow();
    }

    @Override
    public Optional<RegionObserver> getRegionObserver() {
        return Optional.of(this);
    }

    @Override
    public void start(CoprocessorEnvironment env) throws IOException {
        Configuration config = env.getConfiguration();
        timeInterval =
                config.getLong(
                    QueryServices.TASK_HANDLING_INTERVAL_MS_ATTRIB,
                    QueryServicesOptions.DEFAULT_TASK_HANDLING_INTERVAL_MS);
        timeMaxInterval =
                config.getLong(
                        QueryServices.TASK_HANDLING_MAX_INTERVAL_MS_ATTRIB,
                        QueryServicesOptions.DEFAULT_TASK_HANDLING_MAX_INTERVAL_MS);
        initialDelay =
                config.getLong(
                        QueryServices.TASK_HANDLING_INITIAL_DELAY_MS_ATTRIB,
                        QueryServicesOptions.DEFAULT_TASK_HANDLING_INITIAL_DELAY_MS);
    }

    @Override
    public void postOpen(ObserverContext<RegionCoprocessorEnvironment> e) {
        final RegionCoprocessorEnvironment env = e.getEnvironment();

        SelfHealingTask task = new SelfHealingTask(e.getEnvironment(), timeMaxInterval);
        executor.scheduleWithFixedDelay(task, initialDelay, timeInterval, TimeUnit.MILLISECONDS);
    }

    public static class SelfHealingTask extends TimerTask {
        protected RegionCoprocessorEnvironment env;
        protected long timeMaxInterval;
        protected boolean accessCheckEnabled;

        public SelfHealingTask(RegionCoprocessorEnvironment env, long timeMaxInterval) {
            this.env = env;
            this.accessCheckEnabled = env.getConfiguration().getBoolean(QueryServices.PHOENIX_ACLS_ENABLED,
                    QueryServicesOptions.DEFAULT_PHOENIX_ACLS_ENABLED);
            this.timeMaxInterval = timeMaxInterval;
        }

        @Override
        public void run() {
            PhoenixConnection connForTask = null;
            try {
                connForTask = QueryUtil.getConnectionOnServer(env.getConfiguration()).unwrap(PhoenixConnection.class);
                String[] excludeStates = new String[] { PTable.TaskStatus.FAILED.toString(),
                        PTable.TaskStatus.COMPLETED.toString() };
                List<Task.TaskRecord> taskRecords = Task.queryTaskTable(connForTask,  excludeStates);
                for (Task.TaskRecord taskRecord : taskRecords){
                    try {
                        TaskType taskType = taskRecord.getTaskType();
                        if (!classMap.containsKey(taskType)) {
                            LOGGER.warn("Don't know how to execute task type: " + taskType.name());
                            continue;
                        }

                        String className = classMap.get(taskType);

                        Class<?> concreteClass = Class.forName(className);

                        Object obj = concreteClass.newInstance();
                        Method runMethod = concreteClass.getDeclaredMethod("run",
                                Task.TaskRecord.class);
                        Method checkCurretResult = concreteClass.getDeclaredMethod("checkCurrentResult", Task.TaskRecord.class);
                        Method initMethod = concreteClass.getSuperclass().getDeclaredMethod("init",
                                RegionCoprocessorEnvironment.class, Long.class);
                        initMethod.invoke(obj, env, timeMaxInterval);

                        // if current status is already Started, check if we need to re-run.
                        // Task can be async and already Started before.
                        TaskResult result = null;
                        if (taskRecord.getStatus() != null && taskRecord.getStatus().equals(PTable.TaskStatus.STARTED.toString())) {
                            result = (TaskResult) checkCurretResult.invoke(obj, taskRecord);
                        }

                        if (result == null) {
                            // reread task record. There might be async setting of task status
                            taskRecord =
                                    Task.queryTaskTable(connForTask, taskRecord.getTimeStamp(),
                                            taskRecord.getSchemaName(), taskRecord.getTableName(),
                                            taskType, taskRecord.getTenantId(), null).get(0);
                            if (taskRecord.getStatus() != null && !taskRecord.getStatus().equals(PTable.TaskStatus.CREATED.toString())) {
                                continue;
                            }

                            // Change task status to STARTED
                            Task.addTask(new SystemTaskParams.SystemTaskParamsBuilder()
                                .setConn(connForTask)
                                .setTaskType(taskRecord.getTaskType())
                                .setTenantId(taskRecord.getTenantId())
                                .setSchemaName(taskRecord.getSchemaName())
                                .setTableName(taskRecord.getTableName())
                                .setTaskStatus(PTable.TaskStatus.STARTED.toString())
                                .setData(taskRecord.getData())
                                .setPriority(taskRecord.getPriority())
                                .setStartTs(taskRecord.getTimeStamp())
                                .setEndTs(null)
                                .setAccessCheckEnabled(true)
                                .build());

                            // invokes the method at runtime
                            result = (TaskResult) runMethod.invoke(obj, taskRecord);
                        }

                        if (result != null) {
                            String taskStatus = PTable.TaskStatus.FAILED.toString();
                            if (result.getResultCode() == TaskResultCode.SUCCESS) {
                                taskStatus = PTable.TaskStatus.COMPLETED.toString();
                            } else if (result.getResultCode() == TaskResultCode.SKIPPED) {
                                // We will pickup this task again
                                continue;
                            }

                            setEndTaskStatus(connForTask, taskRecord, taskStatus);
                        }

                    }
                    catch (Throwable t) {
                        LOGGER.warn("Exception while running self healingtask. " +
                                "It will be retried in the next system task table scan : " +
                                " taskType : " + taskRecord.getTaskType().name() +
                                taskRecord.getSchemaName()  + "." + taskRecord.getTableName() +
                                " with tenant id " + (taskRecord.getTenantId() == null ? " IS NULL" : taskRecord.getTenantId()) +
                                " and timestamp " + taskRecord.getTimeStamp().toString(), t);
                    }
                }
            } catch (Throwable t) {
                LOGGER.error("SelfHealingTask failed!", t);
            } finally {
                if (connForTask != null) {
                    try {
                        connForTask.close();
                    } catch (SQLException ignored) {
                        LOGGER.debug("SelfHealingTask can't close connection", ignored);
                    }
                }
            }
        }

        public static void setEndTaskStatus(PhoenixConnection connForTask, Task.TaskRecord taskRecord, String taskStatus)
                throws IOException, SQLException {
            // update data with details.
            String data = taskRecord.getData();
            if (Strings.isNullOrEmpty(data)) {
                data = "{}";
            }
            JsonNode jsonNode = JacksonUtil.getObjectReader().readTree(data);
            ((ObjectNode) jsonNode).put(TASK_DETAILS, taskStatus);
            data = jsonNode.toString();

            Timestamp endTs = new Timestamp(EnvironmentEdgeManager.currentTimeMillis());
            Task.addTask(new SystemTaskParams.SystemTaskParamsBuilder()
                .setConn(connForTask)
                .setTaskType(taskRecord.getTaskType())
                .setTenantId(taskRecord.getTenantId())
                .setSchemaName(taskRecord.getSchemaName())
                .setTableName(taskRecord.getTableName())
                .setTaskStatus(taskStatus)
                .setData(data)
                .setPriority(taskRecord.getPriority())
                .setStartTs(taskRecord.getTimeStamp())
                .setEndTs(endTs)
                .setAccessCheckEnabled(true)
                .build());
        }
    }
}
