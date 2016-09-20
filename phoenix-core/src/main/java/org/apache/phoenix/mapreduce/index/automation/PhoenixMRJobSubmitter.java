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

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.zookeeper.ZooKeeperWatcher;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.phoenix.jdbc.PhoenixDatabaseMetaData;
import org.apache.phoenix.mapreduce.index.IndexTool;
import org.apache.phoenix.schema.PIndexState;
import org.apache.phoenix.schema.PTable.IndexType;
import org.apache.phoenix.schema.PTableType;
import org.apache.phoenix.schema.types.PDate;
import org.apache.phoenix.util.PhoenixMRJobUtil;
import org.apache.phoenix.util.UpgradeUtil;
import org.apache.phoenix.util.PhoenixMRJobUtil.MR_SCHEDULER_TYPE;
import org.apache.phoenix.util.ZKBasedMasterElectionUtil;
import org.codehaus.jettison.json.JSONArray;
import org.codehaus.jettison.json.JSONObject;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.reflect.TypeToken;


public class PhoenixMRJobSubmitter {

    // Lock to elect a master node that submits the Phoenix Secondary Index MR Jobs
    private static final String PHOENIX_LOCKS_PARENT =
            "/phoenix/automated-mr-index-build-leader-election";
    private static final String AUTO_INDEX_BUILD_LOCK_NAME = "ActiveStandbyElectorLock";

    public static final String CANDIDATE_INDEX_INFO_QUERY = "SELECT "
            + PhoenixDatabaseMetaData.INDEX_TYPE + ","
            + PhoenixDatabaseMetaData.DATA_TABLE_NAME + ", "
            + PhoenixDatabaseMetaData.TABLE_SCHEM + ", "
            + PhoenixDatabaseMetaData.TABLE_NAME + ", "
            + PhoenixDatabaseMetaData.ASYNC_CREATED_DATE 
            + " FROM "
            + PhoenixDatabaseMetaData.SYSTEM_CATALOG_SCHEMA + ".\"" + PhoenixDatabaseMetaData.SYSTEM_CATALOG_TABLE + "\""
            + " (" + PhoenixDatabaseMetaData.ASYNC_CREATED_DATE + " " + PDate.INSTANCE.getSqlTypeName() + ") "
            + " WHERE "
            + PhoenixDatabaseMetaData.COLUMN_NAME + " IS NULL and "
            + PhoenixDatabaseMetaData.COLUMN_FAMILY + " IS NULL  and "
            + PhoenixDatabaseMetaData.ASYNC_CREATED_DATE + " IS NOT NULL and "
            + PhoenixDatabaseMetaData.TABLE_TYPE + " = '" + PTableType.INDEX.getSerializedValue() + "' and "
            + PhoenixDatabaseMetaData.INDEX_STATE + " = '" + PIndexState.BUILDING.getSerializedValue() + "'";
    
    // TODO - Move this to a property?
    private static final int JOB_SUBMIT_POOL_TIMEOUT = 5;
    private Configuration conf;
    private String zkQuorum;
    private static final Log LOG = LogFactory.getLog(PhoenixMRJobSubmitter.class);

    public PhoenixMRJobSubmitter() throws IOException {
        this(null);
    }

    public PhoenixMRJobSubmitter(Configuration conf) throws IOException {
        if (conf == null) {
            conf = HBaseConfiguration.create();
        }
        this.conf = conf;

        PhoenixMRJobUtil.updateTimeoutsToFailFast(conf);
        String schedulerType =
                conf.get(PhoenixMRJobUtil.PHOENIX_MR_SCHEDULER_TYPE_NAME,
                    MR_SCHEDULER_TYPE.NONE.toString());

        MR_SCHEDULER_TYPE type = MR_SCHEDULER_TYPE.valueOf(schedulerType);

        switch (type) {
        case CAPACITY:
            LOG.info("Applying the Capacity Scheduler Queue Configurations");
            PhoenixMRJobUtil.updateCapacityQueueInfo(conf);
            break;
        case FAIR:
            LOG.warn("Fair Scheduler type is not yet supported");
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
        LOG.info("Security - Fetched App Login Configuration Entries");
        if (entries != null) {
            for (AppConfigurationEntry entry : entries) {
                if (entry.getOptions().get(PRINCIPAL) != null) {
                    principal = (String) entry.getOptions().get(PRINCIPAL);
                }
                if (entry.getOptions().get(KEYTAB) != null) {
                    keyTabPath = (String) entry.getOptions().get(KEYTAB);
                }
            }
            LOG.info("Security - Got Principal = " + principal + "");
            if (principal != null && keyTabPath != null) {
                LOG.info("Security - Retreiving the TGT with principal:" + principal
                        + " and keytab:" + keyTabPath);
                UserGroupInformation.loginUserFromKeytab(principal, keyTabPath);
                LOG.info("Security - Retrieved TGT with principal:" + principal + " and keytab:"
                        + keyTabPath);
            }
        }
    }

    public Map<String, PhoenixAsyncIndex> getCandidateJobs() throws SQLException {
        Properties props = new Properties();
        UpgradeUtil.doNotUpgradeOnFirstConnection(props);
        Connection con = DriverManager.getConnection("jdbc:phoenix:" + zkQuorum);
        Statement s = con.createStatement();
        ResultSet rs = s.executeQuery(CANDIDATE_INDEX_INFO_QUERY);
        Map<String, PhoenixAsyncIndex> candidateIndexes = new HashMap<String, PhoenixAsyncIndex>();
        while (rs.next()) {
            PhoenixAsyncIndex indexInfo = new PhoenixAsyncIndex();
            indexInfo.setIndexType(IndexType.fromSerializedValue(rs
                    .getByte(PhoenixDatabaseMetaData.INDEX_TYPE)));
            indexInfo.setDataTableName(rs.getString(PhoenixDatabaseMetaData.DATA_TABLE_NAME));
            indexInfo.setTableSchem(rs.getString(PhoenixDatabaseMetaData.TABLE_SCHEM));
            indexInfo.setTableName(rs.getString(PhoenixDatabaseMetaData.TABLE_NAME));
            candidateIndexes.put(String.format(IndexTool.INDEX_JOB_NAME_TEMPLATE,
                indexInfo.getDataTableName(), indexInfo.getTableName()), indexInfo);
        }

        return candidateIndexes;
    }

    public int scheduleIndexBuilds() throws Exception {

        ZooKeeperWatcher zookeeperWatcher =
                new ZooKeeperWatcher(conf, "phoenixAutomatedMRIndexBuild", null);

        if (!ZKBasedMasterElectionUtil.acquireLock(zookeeperWatcher, PHOENIX_LOCKS_PARENT,
            AUTO_INDEX_BUILD_LOCK_NAME)) {
            LOG.info("Some other node is already running Automated Index Build. Skipping execution!");
            return -1;
        }
        // 1) Query Phoenix SYSTEM.CATALOG table to get a list of all candidate indexes to be built
        // (in state 'b')
        // 2) Get a list of all ACCEPTED, SUBMITTED AND RUNNING jobs from Yarn Resource Manager
        // 3) Get the jobs to submit (list from 1 - list from 2)

        // Get Candidate indexes to be built
        Map<String, PhoenixAsyncIndex> candidateJobs = getCandidateJobs();
        LOG.info("Candidate Indexes to be built as seen from SYSTEM.CATALOG - " + candidateJobs);

        // Get already scheduled Jobs list from Yarn Resource Manager
        Set<String> submittedJobs = getSubmittedYarnApps();
        LOG.info("Already Submitted/Running MR index build jobs - " + submittedJobs);

        // Get final jobs to submit
        Set<PhoenixAsyncIndex> jobsToSchedule = getJobsToSubmit(candidateJobs, submittedJobs);

        LOG.info("Final indexes to be built - " + jobsToSchedule);
        List<Future<Boolean>> results = new ArrayList<Future<Boolean>>(jobsToSchedule.size());

        int failedJobSubmissionCount = 0;
        int timedoutJobSubmissionCount = 0;
        ExecutorService jobSubmitPool = Executors.newFixedThreadPool(10);
        LOG.info("Attempt to submit MR index build jobs for - " + jobsToSchedule);

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

        LOG.info("Result of Attempt to Submit MR index build Jobs - Jobs attempted = "
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
        toScheduleJobs.removeAll(submittedJobs);
        return toScheduleJobs;
    }

    public Set<String> getSubmittedYarnApps() throws Exception {
        String rmHost = PhoenixMRJobUtil.getActiveResourceManagerHost(conf, zkQuorum);
        Map<String, String> urlParams = new HashMap<String, String>();
        urlParams.put(YarnApplication.APP_STATES_ELEMENT, YarnApplication.state.NEW.toString()
                + "," + YarnApplication.state.ACCEPTED + "," + YarnApplication.state.SUBMITTED
                + "," + YarnApplication.state.RUNNING);
        int rmPort = PhoenixMRJobUtil.getRMPort(conf);
        String response = PhoenixMRJobUtil.getJobsInformationFromRM(rmHost, rmPort, urlParams);
        LOG.debug("Already Submitted/Running Apps = " + response);
        JSONObject jobsJson = new JSONObject(response);
        JSONObject appsJson = jobsJson.optJSONObject(YarnApplication.APPS_ELEMENT);
        Set<String> yarnApplicationSet = new HashSet<String>();

        if (appsJson == null) {
            return yarnApplicationSet;
        }
        JSONArray appJson = appsJson.optJSONArray(YarnApplication.APP_ELEMENT);
        if (appJson == null) {
            return yarnApplicationSet;
        }
        for (int i = 0; i < appJson.length(); i++) {

            Gson gson = new GsonBuilder().create();
            YarnApplication yarnApplication =
                    gson.fromJson(appJson.getJSONObject(i).toString(),
                        new TypeToken<YarnApplication>() {
                        }.getType());
            yarnApplicationSet.add(yarnApplication.getName());
        }

        return yarnApplicationSet;
    }

    public static void main(String[] args) throws Exception {
        PhoenixMRJobSubmitter t = new PhoenixMRJobSubmitter();
        t.scheduleIndexBuilds();
    }
}
