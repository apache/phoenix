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
package org.apache.phoenix.coprocessor.tasks;

import org.apache.phoenix.schema.task.ServerTask;
import org.apache.phoenix.thirdparty.com.google.common.annotations.VisibleForTesting;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.mapreduce.Cluster;
import org.apache.hadoop.mapreduce.Job;
import org.apache.phoenix.coprocessor.TaskRegionObserver;
import org.apache.phoenix.jdbc.PhoenixConnection;
import org.apache.phoenix.mapreduce.transform.TransformTool;
import org.apache.phoenix.schema.PTable;
import org.apache.phoenix.schema.task.SystemTaskParams;
import org.apache.phoenix.schema.task.Task;
import org.apache.phoenix.schema.transform.SystemTransformRecord;
import org.apache.phoenix.schema.transform.Transform;
import org.apache.phoenix.util.QueryUtil;
import org.apache.phoenix.util.SchemaUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.apache.phoenix.mapreduce.util.PhoenixConfigurationUtil.DEFAULT_TRANSFORM_RETRY_COUNT;
import static org.apache.phoenix.mapreduce.util.PhoenixConfigurationUtil.TRANSFORM_RETRY_COUNT_VALUE;
import static org.apache.phoenix.mapreduce.util.PhoenixConfigurationUtilHelper.DEFAULT_TRANSFORM_MONITOR_ENABLED;
import static org.apache.phoenix.mapreduce.util.PhoenixConfigurationUtilHelper.TRANSFORM_MONITOR_ENABLED;

/**
 * Task runs periodically to monitor and orchestrate ongoing transforms in System.Transform table.
 *
 */
public class TransformMonitorTask extends BaseTask  {
    public static final String DEFAULT = "IndexName";

    public static final Logger LOGGER = LoggerFactory.getLogger(TransformMonitorTask.class);

    private static boolean isDisabled = false;

    // Called from testong
    @VisibleForTesting
    public static void disableTransformMonitorTask(boolean disabled) {
        isDisabled = disabled;
    }

    @Override
    public TaskRegionObserver.TaskResult run(Task.TaskRecord taskRecord) {
        Configuration conf = HBaseConfiguration.create(env.getConfiguration());
        Configuration configuration = HBaseConfiguration.addHbaseResources(conf);
        boolean transformMonitorEnabled = configuration.getBoolean(TRANSFORM_MONITOR_ENABLED, DEFAULT_TRANSFORM_MONITOR_ENABLED);
        if (!transformMonitorEnabled || isDisabled) {
            return new TaskRegionObserver.TaskResult(TaskRegionObserver.TaskResultCode.FAIL, "TransformMonitor is disabled");
        }

        try (PhoenixConnection conn = QueryUtil.getConnectionOnServer(conf).unwrap(PhoenixConnection.class)){
            SystemTransformRecord systemTransformRecord = Transform.getTransformRecord(taskRecord.getSchemaName(),
                    taskRecord.getTableName(), null, taskRecord.getTenantId(), conn);
            if (systemTransformRecord == null) {
                return new TaskRegionObserver.TaskResult(TaskRegionObserver.TaskResultCode.FAIL,
                        "No transform record is found");
            }
            String tableName = SchemaUtil.getTableName(systemTransformRecord.getSchemaName(),
                    systemTransformRecord.getLogicalTableName());

            if (systemTransformRecord.getTransformStatus().equals(PTable.TransformStatus.CREATED.name())) {
                LOGGER.info("Transform is created, starting the TransformTool ", tableName);
                // Kick a TransformTool run, it will already update transform record status and job id
                TransformTool transformTool = TransformTool.runTransformTool(systemTransformRecord, conf, false, null, null, false, false);
                if (transformTool == null) {
                    // This is not a map/reduce error. There must be some unexpected issue. So, retrying will not solve the underlying issue.
                    return new TaskRegionObserver.TaskResult(TaskRegionObserver.TaskResultCode.FAIL, "TransformTool run failed. Check the parameters.");
                }
            } else if (systemTransformRecord.getTransformStatus().equals(PTable.TransformStatus.COMPLETED.name())) {
                LOGGER.info("Transform is completed, TransformMonitor is done ", tableName);
                return new TaskRegionObserver.TaskResult(TaskRegionObserver.TaskResultCode.SUCCESS, "");
            } else if (systemTransformRecord.getTransformStatus().equals(PTable.TransformStatus.PENDING_CUTOVER.name())
                    && !PTable.TransformType.isPartialTransform(systemTransformRecord.getTransformType())) {
                LOGGER.info("Transform is pending cutover ", tableName);
                Transform.doCutover(conn, systemTransformRecord);

                PTable.TransformType partialTransform = PTable.TransformType.getPartialTransform(systemTransformRecord.getTransformType());
                if (partialTransform != null) {
                    // Update transform to be partial
                    SystemTransformRecord.SystemTransformBuilder builder = new SystemTransformRecord.SystemTransformBuilder(systemTransformRecord);
                    builder.setTransformType(partialTransform);
                    // Decrement retry count since TransformTool will increment it. Should we set it to 0?
                    builder.setTransformRetryCount(systemTransformRecord.getTransformRetryCount()-1);
                    Transform.upsertTransform(builder.build(), conn);

                    // Fix unverified rows. Running partial transform will make the transform status go back to started
                    long startFromTs = 0;
                    if (systemTransformRecord.getTransformLastStateTs() != null) {
                        startFromTs = systemTransformRecord.getTransformLastStateTs().getTime()-1;
                    }
                    TransformTool.runTransformTool(systemTransformRecord, conf, true, startFromTs, null, true, false);

                    // In the future, if we are changing the PK structure, we need to run indextools as well
                } else {
                    // No partial transform needed so, we update state of the transform
                    LOGGER.warn("No partial type of the transform is found. Completing the transform ", tableName);
                    Transform.updateTransformRecord(conn, systemTransformRecord, PTable.TransformStatus.COMPLETED);
                }
            } else if (systemTransformRecord.getTransformStatus().equals(PTable.TransformStatus.STARTED.name()) ||
                    (systemTransformRecord.getTransformStatus().equals(PTable.TransformStatus.PENDING_CUTOVER.name())
                            && PTable.TransformType.isPartialTransform(systemTransformRecord.getTransformType()))) {
                LOGGER.info(systemTransformRecord.getTransformStatus().equals(PTable.TransformStatus.STARTED.name()) ?
                        "Transform is started, we will monitor ": "Partial transform is going on, we will monitor" , tableName);
                // Monitor the job of transform tool and decide to retry
                String jobId = systemTransformRecord.getTransformJobId();
                if (jobId != null) {
                    Cluster cluster = new Cluster(configuration);

                    Job job = cluster.getJob(org.apache.hadoop.mapreduce.JobID.forName(jobId));
                    if (job == null) {
                        LOGGER.warn(String.format("Transform job with Id=%s is not found", jobId));
                        return new  TaskRegionObserver.TaskResult(TaskRegionObserver.TaskResultCode.SKIPPED, "The job cannot be found");
                    }
                    if (job != null && job.isComplete()) {
                        if (job.isSuccessful()) {
                            LOGGER.warn("TransformTool job is successful. Transform should have been in a COMPLETED state "
                                    + taskRecord.getTableName());
                        } else {
                            // Retry TransformTool run
                            int maxRetryCount = configuration.getInt(TRANSFORM_RETRY_COUNT_VALUE, DEFAULT_TRANSFORM_RETRY_COUNT);
                            if (systemTransformRecord.getTransformRetryCount() < maxRetryCount) {
                                // Retry count will be incremented in TransformTool
                                TransformTool.runTransformTool(systemTransformRecord, conf, false, null, null, false, true);
                            }
                        }
                    }
                }
            } else if (systemTransformRecord.getTransformStatus().equals(PTable.TransformStatus.FAILED.name())) {
                String str = "Transform is marked as failed because either TransformTool is run on the foreground and failed " +
                        "or it is run as async but there is something wrong with the TransformTool parameters";
                LOGGER.error(str);
                return new TaskRegionObserver.TaskResult(TaskRegionObserver.TaskResultCode.FAIL, str);
            } else if (systemTransformRecord.getTransformStatus().equals(PTable.TransformStatus.PAUSED.name())) {
                return new TaskRegionObserver.TaskResult(TaskRegionObserver.TaskResultCode.SUCCESS,
                        "Transform is paused. No need to monitor");
            } else {
                String str = "Transform status is not known " + systemTransformRecord.getString();
                LOGGER.error(str);
                return new TaskRegionObserver.TaskResult(TaskRegionObserver.TaskResultCode.FAIL, str);
            }

            // Update task status to RETRY so that it is retried
            ServerTask.addTask(new SystemTaskParams.SystemTaskParamsBuilder()
                    .setConn(conn)
                    .setTaskType(taskRecord.getTaskType())
                    .setTenantId(taskRecord.getTenantId())
                    .setSchemaName(taskRecord.getSchemaName())
                    .setTableName(taskRecord.getTableName())
                    .setTaskStatus(PTable.TaskStatus.RETRY.toString())
                    .setData(taskRecord.getData())
                    .setPriority(taskRecord.getPriority())
                    .setStartTs(taskRecord.getTimeStamp())
                    .setEndTs(null)
                    .setAccessCheckEnabled(true)
                    .build());
            return null;
        }
        catch (Throwable t) {
            LOGGER.warn("Exception while running transform monitor task. " +
                    "It will be retried in the next system task table scan : " +
                    taskRecord.getSchemaName() + "." + taskRecord.getTableName() +
                    " with tenant id " + (taskRecord.getTenantId() == null ? " IS NULL" : taskRecord.getTenantId()) +
                    " and data " + taskRecord.getData(), t);
            return new TaskRegionObserver.TaskResult(TaskRegionObserver.TaskResultCode.FAIL, t.toString());
        }
    }

    @Override
    public TaskRegionObserver.TaskResult checkCurrentResult(Task.TaskRecord taskRecord)
            throws Exception {
        // We don't need to check MR job result here since the job itself changes task state.
        return null;
    }
}
