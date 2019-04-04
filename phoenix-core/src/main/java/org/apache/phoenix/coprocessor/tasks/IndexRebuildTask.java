package org.apache.phoenix.coprocessor.tasks;

import com.google.common.base.Strings;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.mapreduce.Cluster;
import org.apache.hadoop.mapreduce.Job;
import org.apache.phoenix.coprocessor.TaskRegionObserver;
import org.apache.phoenix.jdbc.PhoenixConnection;
import org.apache.phoenix.mapreduce.index.IndexTool;
import org.apache.phoenix.query.QueryServices;
import org.apache.phoenix.schema.PTable;
import org.apache.phoenix.schema.task.Task;
import org.apache.phoenix.util.QueryUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.Map;

/**
 * Task runs periodically to rebuild indexes for System.Task entries.
 *
 */
public class IndexRebuildTask extends BaseTask  {
    public static final String IndexName = "IndexName";
    public static final String JobID = "JobID";
    public static final Logger logger = LoggerFactory.getLogger(IndexRebuildTask.class);

    @Override
    public TaskRegionObserver.TaskResult run(Task.TaskRecord taskRecord) {
        Connection conn = null;

        try {
            // We have to clone the configuration because env.getConfiguration is readonly.
            Configuration conf = HBaseConfiguration.create(env.getConfiguration());
            conn = QueryUtil.getConnectionOnServer(env.getConfiguration());

            conf.set(QueryServices.TRANSACTIONS_ENABLED, Boolean.TRUE.toString());

            String data = taskRecord.getData();
            if (Strings.isNullOrEmpty(taskRecord.getData())) {
                data = "{}";
            }
            JsonParser jsonParser = new JsonParser();
            JsonObject jsonObject = jsonParser.parse(data).getAsJsonObject();
            String indexName = getIndexName(jsonObject);
            if (Strings.isNullOrEmpty(indexName)) {
                String str = "Index name is not found. Index rebuild cannot continue " +
                        "Data : " + data;
                logger.warn(str);
                return new TaskRegionObserver.TaskResult(TaskRegionObserver.TaskResultCode.FAIL, str);
            }

            boolean shouldDisable = false;
            if (jsonObject.has("DisableBefore")) {
                String disableBefore = jsonObject.get("DisableBefore").toString();
                if (!Strings.isNullOrEmpty(disableBefore)) {
                    shouldDisable = Boolean.valueOf(disableBefore);
                }
            }

            // Run index tool async.
            boolean runForeground = false;
            Map.Entry<Integer, Job> indexToolRes = IndexTool
                    .run(conf, taskRecord.getSchemaName(), taskRecord.getTableName(), indexName, true,
                            false, taskRecord.getTenantId(), shouldDisable, runForeground);
            int status = indexToolRes.getKey();
            if (status != 0) {
                return new TaskRegionObserver.TaskResult(TaskRegionObserver.TaskResultCode.FAIL, "Index tool returned : " + status);
            }

            Job job = indexToolRes.getValue();

            jsonObject.addProperty(JobID, job.getJobID().toString());
            Task.addTask(conn.unwrap(PhoenixConnection.class ), taskRecord.getTaskType(), taskRecord.getTenantId(), taskRecord.getSchemaName(),
                    taskRecord.getTableName(), PTable.TaskStatus.STARTED.toString(), jsonObject.toString(), taskRecord.getPriority(),
                    taskRecord.getTimeStamp(), null, true);
            // It will take some time to finish, so we will check the status in a separate task.
            return null;
        }
        catch (Throwable t) {
            logger.warn("Exception while running index rebuild task. " +
                    "It will be retried in the next system task table scan : " +
                    taskRecord.getSchemaName() + "." + taskRecord.getTableName() +
                    " with tenant id " + (taskRecord.getTenantId() == null ? " IS NULL" : taskRecord.getTenantId()) +
                    " and data " + taskRecord.getData(), t);
            return new TaskRegionObserver.TaskResult(TaskRegionObserver.TaskResultCode.FAIL, t.toString());
        } finally {
            if (conn != null) {
                try {
                    conn.close();
                } catch (SQLException e) {
                    logger.debug("IndexRebuildTask can't close connection");
                }
            }
        }

    }

    private String getIndexName(JsonObject jsonObject) {
        String indexName = null;
        // Get index name from data column.
        if (jsonObject.has(IndexName)) {
            indexName = jsonObject.get(IndexName).toString().replaceAll("\"", "");
        }
        return indexName;
    }

    private String getJobID(String data) {
        if (Strings.isNullOrEmpty(data)) {
            data = "{}";
        }
        JsonParser jsonParser = new JsonParser();
        JsonObject jsonObject = jsonParser.parse(data).getAsJsonObject();
        String jobId = null;
        if (jsonObject.has(JobID)) {
            jobId = jsonObject.get(JobID).toString().replaceAll("\"", "");
        }
        return jobId;
    }

    @Override
    public TaskRegionObserver.TaskResult checkCurrentResult(Task.TaskRecord taskRecord)
            throws Exception {

        String jobID = getJobID(taskRecord.getData());
        if (jobID != null) {
            Configuration conf = HBaseConfiguration.create(env.getConfiguration());
            Configuration configuration = HBaseConfiguration.addHbaseResources(conf);
            Cluster cluster = new Cluster(configuration);

            Job job = cluster.getJob(org.apache.hadoop.mapreduce.JobID.forName(jobID));

            if (job != null && job.isComplete()) {
                if (job.isSuccessful()) {
                    logger.warn("IndexRebuildTask checkCurrentResult job is successful " + taskRecord.getTableName());
                    return new TaskRegionObserver.TaskResult(TaskRegionObserver.TaskResultCode.SUCCESS, "");
                } else {
                    return new TaskRegionObserver.TaskResult(TaskRegionObserver.TaskResultCode.FAIL,
                            "Index is DISABLED");
                }
            }

        }
        return null;
    }
}