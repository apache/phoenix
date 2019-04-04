package org.apache.phoenix.coprocessor.tasks;

import org.apache.phoenix.coprocessor.MetaDataEndpointImpl;
import org.apache.phoenix.coprocessor.MetaDataProtocol;
import org.apache.phoenix.coprocessor.TaskRegionObserver;
import org.apache.phoenix.jdbc.PhoenixConnection;
import org.apache.phoenix.schema.MetaDataClient;
import org.apache.phoenix.schema.task.Task;
import org.apache.phoenix.util.PhoenixRuntime;
import org.apache.phoenix.util.QueryUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.SQLException;
import java.sql.Timestamp;
import java.util.Properties;

/**
 * Task runs periodically to clean up task of child views whose parent is dropped
 *
 */
public class DropChildViewsTask extends BaseTask {
    public static final Logger logger = LoggerFactory.getLogger(DropChildViewsTask.class);

    public TaskRegionObserver.TaskResult run(Task.TaskRecord taskRecord) {
        PhoenixConnection pconn = null;
        Timestamp timestamp = taskRecord.getTimeStamp();
        try {
            String tenantId = taskRecord.getTenantId();
            if (tenantId != null) {
                Properties tenantProps = new Properties();
                tenantProps.setProperty(PhoenixRuntime.TENANT_ID_ATTRIB, tenantId);
                pconn = QueryUtil.getConnectionOnServer(tenantProps, env.getConfiguration()).unwrap(PhoenixConnection.class);
            }
            else {
                pconn = QueryUtil.getConnectionOnServer(env.getConfiguration()).unwrap(PhoenixConnection.class);
            }

            MetaDataProtocol.MetaDataMutationResult result = new MetaDataClient(pconn).updateCache(pconn.getTenantId(),
                    taskRecord.getSchemaName(), taskRecord.getTableName(), true);
            if (result.getMutationCode() != MetaDataProtocol.MutationCode.TABLE_ALREADY_EXISTS) {
                MetaDataEndpointImpl
                        .dropChildViews(env, taskRecord.getTenantIdBytes(), taskRecord.getSchemaNameBytes(), taskRecord.getTableNameBytes());
                return new TaskRegionObserver.TaskResult(TaskRegionObserver.TaskResultCode.SUCCESS, "");
            } else if (System.currentTimeMillis() < timeMaxInterval + timestamp.getTime()) {
                // skip this task as it has not been expired and its parent table has not been dropped yet
                logger.info("Skipping a child view drop task. The parent table has not been dropped yet : " +
                        taskRecord.getSchemaName() + "." + taskRecord.getTableName() +
                        " with tenant id " + (tenantId == null ? " IS NULL" : tenantId) +
                        " and timestamp " + timestamp.toString());
                return new TaskRegionObserver.TaskResult(TaskRegionObserver.TaskResultCode.SKIPPED, "");
            }
            else {
                logger.warn(" A drop child view task has expired and will be marked as failed : " +
                        taskRecord.getSchemaName() + "." + taskRecord.getTableName() +
                        " with tenant id " + (tenantId == null ? " IS NULL" : tenantId) +
                        " and timestamp " + timestamp.toString());
                return new TaskRegionObserver.TaskResult(TaskRegionObserver.TaskResultCode.FAIL, "Expired");
            }
        }
        catch (Throwable t) {
            logger.warn("Exception while dropping a child view task. " +
                    taskRecord.getSchemaName()  + "." + taskRecord.getTableName() +
                    " with tenant id " + (taskRecord.getTenantId() == null ? " IS NULL" : taskRecord.getTenantId()) +
                    " and timestamp " + timestamp.toString(), t);
            return new TaskRegionObserver.TaskResult(TaskRegionObserver.TaskResultCode.FAIL, t.toString());
        } finally {
            if (pconn != null) {
                try {
                    pconn.close();
                } catch (SQLException ignored) {
                    logger.debug("DropChildViewsTask can't close connection", ignored);
                }
            }
        }
    }

    public TaskRegionObserver.TaskResult checkCurrentResult(Task.TaskRecord taskRecord) throws Exception {
        return null;
    }
}