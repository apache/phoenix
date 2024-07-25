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
package org.apache.phoenix.mapreduce.index.automation;

import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import javax.security.auth.login.AppConfigurationEntry;

import com.fasterxml.jackson.databind.JsonNode;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.zookeeper.ZKWatcher;
import org.apache.hadoop.mapreduce.MRJobConfig;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.phoenix.jdbc.PhoenixDatabaseMetaData;
import org.apache.phoenix.mapreduce.index.IndexTool;
import org.apache.phoenix.schema.PIndexState;
import org.apache.phoenix.schema.PTable.IndexType;
import org.apache.phoenix.schema.PTableType;
import org.apache.phoenix.schema.types.PDate;
import org.apache.phoenix.schema.types.PLong;
import org.apache.phoenix.util.JacksonUtil;
import org.apache.phoenix.util.PhoenixMRJobUtil;
import org.apache.phoenix.util.PhoenixMRJobUtil.MR_SCHEDULER_TYPE;
import org.apache.phoenix.util.UpgradeUtil;
import org.apache.phoenix.util.ZKBasedMasterElectionUtil;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class PhoenixMRJobSubmitter {

    // Lock to elect a master node that submits the Phoenix Secondary Index MR Jobs
    private static final String PHOENIX_LOCKS_PARENT =
            "/phoenix/automated-mr-index-build-leader-election";
    public static final String PHOENIX_MR_CLIENT_SCANNER_TIMEOUT_PERIOD =
            "phoenix.mr.client.scanner.timeout.period";
    public static final String PHOENIX_MR_RPC_TIMEOUT =
            "phoenix.mr.rpc.timeout";
    public static final String PHOENIX_MR_TASK_TIMEOUT =
            "phoenix.mr.task.timeout";
    public static final String PHOENIX_MR_CLIENT_RETRIES_NUMBER =
            "phoenix.mr.client.retries.number";
    public static final String PHOENIX_MR_CLIENT_PAUSE =
            "phoenix.mr.client.retries.number";
    public static final String PHOENIX_MR_ZK_RECOVERY_RETRY =
            "phoenix.mr.zk.recovery.retry";
    private static final String AUTO_INDEX_BUILD_LOCK_NAME = "ActiveStandbyElectorLock";
    private static final int DEFAULT_TIMEOUT_IN_MILLIS = 600000;
    public static final int DEFAULT_MR_CLIENT_SCANNER_TIMEOUT_PERIOD = DEFAULT_TIMEOUT_IN_MILLIS;
    public static final int DEFAULT_MR_RPC_TIMEOUT = DEFAULT_TIMEOUT_IN_MILLIS;
    public static final int DEFAULT_MR_TASK_TIMEOUT = DEFAULT_TIMEOUT_IN_MILLIS;
    // Reduced HBase/Zookeeper Client Retries
    public static final int DEFAULT_MR_CLIENT_RETRIES_NUMBER = 10;
    public static final int DEFAULT_MR_CLIENT_PAUSE = 1000;
    public static final int DEFAULT_MR_ZK_RECOVERY_RETRY = 1;
    
    public static final String CANDIDATE_INDEX_INFO_QUERY = "SELECT "
            + PhoenixDatabaseMetaData.INDEX_TYPE + ","
            + PhoenixDatabaseMetaData.DATA_TABLE_NAME + ", "
            + PhoenixDatabaseMetaData.TABLE_SCHEM + ", "
            + PhoenixDatabaseMetaData.TABLE_NAME + ", "
            + PhoenixDatabaseMetaData.ASYNC_CREATED_DATE + ", "
            + PhoenixDatabaseMetaData.ASYNC_REBUILD_TIMESTAMP
            + " FROM "
            + PhoenixDatabaseMetaData.SYSTEM_CATALOG_SCHEMA + ".\"" + PhoenixDatabaseMetaData.SYSTEM_CATALOG_TABLE + "\""
            + " (" + PhoenixDatabaseMetaData.ASYNC_CREATED_DATE + " " + PDate.INSTANCE.getSqlTypeName() + ", "
            +  PhoenixDatabaseMetaData.ASYNC_REBUILD_TIMESTAMP + " " +  PLong.INSTANCE.getSqlTypeName() + ") "
            + " WHERE "
            + PhoenixDatabaseMetaData.COLUMN_NAME + " IS NULL and "
            + PhoenixDatabaseMetaData.COLUMN_FAMILY + " IS NULL  and "
            + "(" + PhoenixDatabaseMetaData.ASYNC_CREATED_DATE + " IS NOT NULL OR "
            + PhoenixDatabaseMetaData.ASYNC_REBUILD_TIMESTAMP + " IS NOT NULL ) and "
            + PhoenixDatabaseMetaData.TABLE_TYPE + " = '" + PTableType.INDEX.getSerializedValue() + "' and "
            + PhoenixDatabaseMetaData.INDEX_STATE + " = '" + PIndexState.BUILDING.getSerializedValue() + "'";
    
    // TODO - Move this to a property?
    private static final int JOB_SUBMIT_POOL_TIMEOUT = 5;
    private Configuration conf;
    private String zkQuorum;
    private static final Logger LOGGER = LoggerFactory.getLogger(PhoenixMRJobSubmitter.class);

    public PhoenixMRJobSubmitter() throws IOException {
        this(null);
    }

    public PhoenixMRJobSubmitter(Configuration conf) throws IOException {
        if (conf == null) {
            conf = HBaseConfiguration.create();
        }
        this.conf = conf;

        // Have Phoenix specific properties for defaults to enable potential override
        conf.setLong(HConstants.HBASE_CLIENT_SCANNER_TIMEOUT_PERIOD, 
                conf.getLong(PHOENIX_MR_CLIENT_SCANNER_TIMEOUT_PERIOD,
                        DEFAULT_MR_CLIENT_SCANNER_TIMEOUT_PERIOD));
        conf.setLong(HConstants.HBASE_RPC_TIMEOUT_KEY, 
                conf.getLong(PHOENIX_MR_RPC_TIMEOUT,
                        DEFAULT_MR_RPC_TIMEOUT));
        conf.setLong(MRJobConfig.TASK_TIMEOUT, 
                conf.getLong(PHOENIX_MR_TASK_TIMEOUT,
                        DEFAULT_MR_TASK_TIMEOUT));

        // Reduced HBase Client Retries
        conf.setInt(HConstants.HBASE_CLIENT_RETRIES_NUMBER, 
                conf.getInt(PHOENIX_MR_CLIENT_RETRIES_NUMBER,
                        DEFAULT_MR_CLIENT_RETRIES_NUMBER));
        conf.setInt(HConstants.HBASE_CLIENT_PAUSE, 
                conf.getInt(PHOENIX_MR_CLIENT_PAUSE,
                        DEFAULT_MR_CLIENT_PAUSE));
        conf.setInt("zookeeper.recovery.retry", 
                conf.getInt(PHOENIX_MR_ZK_RECOVERY_RETRY,
                        DEFAULT_MR_ZK_RECOVERY_RETRY));
        
        String schedulerType =
                conf.get(PhoenixMRJobUtil.PHOENIX_MR_SCHEDULER_TYPE_NAME,
                    MR_SCHEDULER_TYPE.NONE.toString());

        MR_SCHEDULER_TYPE type = MR_SCHEDULER_TYPE.valueOf(schedulerType);

        switch (type) {
        case CAPACITY:
            LOGGER.info("Applying the Capacity Scheduler Queue Configurations");
            PhoenixMRJobUtil.updateCapacityQueueInfo(conf);
            break;
        case FAIR:
            LOGGER.warn("Fair Scheduler type is not yet supported");
            throw new IOException("Fair Scheduler is not yet supported");
        case NONE:
        default:
            break;
        }
        zkQuorum = conf.get(HConstants.ZOOKEEPER_QUORUM);
        // Use UGI.loginUserFromKeytab to login and work with secure clusters
        enableKeyTabSecurity();
    }

    private void enableKeyTabSecurity() throws IOException {

        final String PRINCIPAL = "principal";
        final String KEYTAB = "keyTab";
        // Login with the credentials from the keytab to retrieve the TGT . The
        // renewal of the TGT happens in a Zookeeper thread
        String principal = null;
        String keyTabPath = null;
        AppConfigurationEntry entries[] =
                javax.security.auth.login.Configuration.getConfiguration()
                        .getAppConfigurationEntry("Client");
        LOGGER.info("Security - Fetched App Login Configuration Entries");
        if (entries != null) {
            for (AppConfigurationEntry entry : entries) {
                if (entry.getOptions().get(PRINCIPAL) != null) {
                    principal = (String) entry.getOptions().get(PRINCIPAL);
                }
                if (entry.getOptions().get(KEYTAB) != null) {
                    keyTabPath = (String) entry.getOptions().get(KEYTAB);
                }
            }
            LOGGER.info("Security - Got Principal = " + principal + "");
            if (principal != null && keyTabPath != null) {
                LOGGER.info("Security - Retreiving the TGT with principal:" + principal
                        + " and keytab:" + keyTabPath);
                UserGroupInformation.loginUserFromKeytab(principal, keyTabPath);
                LOGGER.info("Security - Retrieved TGT with principal:" + principal + " and keytab:"
                        + keyTabPath);
            }
        }
    }

    public Map<String, PhoenixAsyncIndex> getCandidateJobs() throws SQLException {
        Connection con = DriverManager.getConnection("jdbc:phoenix:" + zkQuorum);
        return getCandidateJobs(con);
    }

    public Map<String, PhoenixAsyncIndex> getCandidateJobs(Connection con) throws SQLException {
        Properties props = new Properties();
        UpgradeUtil.doNotUpgradeOnFirstConnection(props);
        Map<String, PhoenixAsyncIndex> candidateIndexes = new HashMap<>();
        try (Statement s = con.createStatement();
             ResultSet rs = s.executeQuery(CANDIDATE_INDEX_INFO_QUERY)) {
            while (rs.next()) {
                PhoenixAsyncIndex indexInfo = new PhoenixAsyncIndex();
                indexInfo.setIndexType(IndexType.fromSerializedValue(rs
                        .getByte(PhoenixDatabaseMetaData.INDEX_TYPE)));
                indexInfo.setDataTableName(rs.getString(PhoenixDatabaseMetaData.DATA_TABLE_NAME));
                indexInfo.setTableSchem(rs.getString(PhoenixDatabaseMetaData.TABLE_SCHEM));
                indexInfo.setTableName(rs.getString(PhoenixDatabaseMetaData.TABLE_NAME));
                candidateIndexes.put(String.format(IndexTool.INDEX_JOB_NAME_TEMPLATE,
                        indexInfo.getTableSchem(), indexInfo.getDataTableName(),
                        indexInfo.getTableName()), indexInfo);
            }
        }
        return candidateIndexes;
    }

    public int scheduleIndexBuilds() throws Exception {

        ZKWatcher zookeeperWatcher =
                new ZKWatcher(conf, "phoenixAutomatedMRIndexBuild", null);

        if (!ZKBasedMasterElectionUtil.acquireLock(zookeeperWatcher, PHOENIX_LOCKS_PARENT,
            AUTO_INDEX_BUILD_LOCK_NAME)) {
            LOGGER.info("Some other node is already running Automated Index Build." +
                    " Skipping execution!");
            return -1;
        }
        // 1) Query Phoenix SYSTEM.CATALOG table to get a list of all candidate indexes to be built
        // (in state 'b')
        // 2) Get a list of all ACCEPTED, SUBMITTED AND RUNNING jobs from Yarn Resource Manager
        // 3) Get the jobs to submit (list from 1 - list from 2)

        // Get Candidate indexes to be built
        Map<String, PhoenixAsyncIndex> candidateJobs = getCandidateJobs();
        LOGGER.info("Candidate Indexes to be built as seen from SYSTEM.CATALOG - " + candidateJobs);

        // Get already scheduled Jobs list from Yarn Resource Manager
        Set<String> submittedJobs = getSubmittedYarnApps();
        LOGGER.info("Already Submitted/Running MR index build jobs - " + submittedJobs);

        // Get final jobs to submit
        Set<PhoenixAsyncIndex> jobsToSchedule = getJobsToSubmit(candidateJobs, submittedJobs);

        LOGGER.info("Final indexes to be built - " + jobsToSchedule);
        List<Future<Boolean>> results = new ArrayList<Future<Boolean>>(jobsToSchedule.size());

        int failedJobSubmissionCount = 0;
        int timedoutJobSubmissionCount = 0;
        ExecutorService jobSubmitPool = Executors.newFixedThreadPool(10);
        LOGGER.info("Attempt to submit MR index build jobs for - " + jobsToSchedule);

        try {
            for (PhoenixAsyncIndex indexToBuild : jobsToSchedule) {
                PhoenixMRJobCallable task =
                        new PhoenixMRJobCallable(HBaseConfiguration.create(conf), indexToBuild, "/");
                results.add(jobSubmitPool.submit(task));
            }
            for (Future<Boolean> result : results) {
                try {
                    result.get(JOB_SUBMIT_POOL_TIMEOUT, TimeUnit.MINUTES);
                } catch (InterruptedException e) {
                    failedJobSubmissionCount++;
                } catch (ExecutionException e) {
                    failedJobSubmissionCount++;
                } catch (TimeoutException e) {
                    timedoutJobSubmissionCount++;
                }
            }
        } finally {
            PhoenixMRJobUtil.shutdown(jobSubmitPool);
        }

        LOGGER.info("Result of Attempt to Submit MR index build Jobs - Jobs attempted = "
                + jobsToSchedule.size() + " ; Failed to Submit = " + failedJobSubmissionCount
                + " ; Timed out = " + timedoutJobSubmissionCount);
        return failedJobSubmissionCount;
    }

    public Set<PhoenixAsyncIndex> getJobsToSubmit(Map<String, PhoenixAsyncIndex> candidateJobs,
            Set<String> submittedJobs) {
        Set<PhoenixAsyncIndex> toScheduleJobs =
                new HashSet<PhoenixAsyncIndex>(candidateJobs.values());
        for (String jobId : submittedJobs) {
            if (candidateJobs.containsKey(jobId)) {
                toScheduleJobs.remove(candidateJobs.get(jobId));
            }
        }
        return toScheduleJobs;
    }

    public Set<String> getSubmittedYarnApps() throws Exception {
        String rmAddress = PhoenixMRJobUtil.getActiveResourceManagerAddress(conf, zkQuorum);
        Map<String, String> urlParams = new HashMap<String, String>();
        urlParams.put(YarnApplication.APP_STATES_ELEMENT, YarnApplication.state.NEW.toString()
                + "," + YarnApplication.state.ACCEPTED + "," + YarnApplication.state.SUBMITTED
                + "," + YarnApplication.state.RUNNING);
        String response = PhoenixMRJobUtil.getJobsInformationFromRM(rmAddress, urlParams);
        LOGGER.debug("Already Submitted/Running Apps = " + response);
        JsonNode jsonNode = JacksonUtil.getObjectReader().readTree(response);
        JsonNode appsJson = jsonNode.get(YarnApplication.APPS_ELEMENT);
        Set<String> yarnApplicationSet = new HashSet<String>();

        if (appsJson == null) {
            return yarnApplicationSet;
        }
        JsonNode appJson = appsJson.get(YarnApplication.APP_ELEMENT);
        if (appJson == null) {
            return yarnApplicationSet;
        }
        for (final JsonNode clientVersion : appJson) {
            yarnApplicationSet.add(clientVersion.get("name").textValue());
        }

        return yarnApplicationSet;
    }

    public static void main(String[] args) throws Exception {
        PhoenixMRJobSubmitter t = new PhoenixMRJobSubmitter();
        t.scheduleIndexBuilds();
    }
}
