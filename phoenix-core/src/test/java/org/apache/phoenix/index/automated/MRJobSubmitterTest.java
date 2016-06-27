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
package org.apache.phoenix.index.automated;

import static org.junit.Assert.assertEquals;

import java.io.IOException;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Set;

import org.apache.phoenix.mapreduce.index.IndexTool;
import org.apache.phoenix.mapreduce.index.automation.PhoenixAsyncIndex;
import org.apache.phoenix.mapreduce.index.automation.PhoenixMRJobSubmitter;
import org.apache.phoenix.schema.PTable.IndexType;
import org.junit.Before;
import org.junit.Test;

public class MRJobSubmitterTest {

    private Map<String, PhoenixAsyncIndex> candidateJobs =
            new LinkedHashMap<String, PhoenixAsyncIndex>();
    private Set<String> submittedJobs = new HashSet<String>();

    @Before
    public void prepare() {
        PhoenixAsyncIndex index1 = new PhoenixAsyncIndex();
        index1.setDataTableName("DT1");
        index1.setTableName("IT1");
        index1.setIndexType(IndexType.LOCAL);

        candidateJobs.put(String.format(IndexTool.INDEX_JOB_NAME_TEMPLATE,
            index1.getDataTableName(), index1.getTableName()), index1);

        PhoenixAsyncIndex index2 = new PhoenixAsyncIndex();
        index2.setDataTableName("DT2");
        index2.setTableName("IT2");
        index2.setIndexType(IndexType.LOCAL);

        candidateJobs.put(String.format(IndexTool.INDEX_JOB_NAME_TEMPLATE,
            index2.getDataTableName(), index2.getTableName()), index2);
    }

    @Test
    public void testLocalIndexJobsSubmission() throws IOException {

        // Set the index type to LOCAL
        for (String jobId : candidateJobs.keySet()) {
            candidateJobs.get(jobId).setIndexType(IndexType.LOCAL);
        }
        PhoenixMRJobSubmitter submitter = new PhoenixMRJobSubmitter();
        Set<PhoenixAsyncIndex> jobsToSubmit =
                submitter.getJobsToSubmit(candidateJobs, submittedJobs);
        assertEquals(2, jobsToSubmit.size());
    }

    @Test
    public void testGlobalIndexJobsForSubmission() throws IOException {

        // Set the index type to GLOBAL
        for (String jobId : candidateJobs.keySet()) {
            candidateJobs.get(jobId).setIndexType(IndexType.GLOBAL);
        }
        PhoenixMRJobSubmitter submitter = new PhoenixMRJobSubmitter();
        Set<PhoenixAsyncIndex> jobsToSubmit =
                submitter.getJobsToSubmit(candidateJobs, submittedJobs);
        assertEquals(2, jobsToSubmit.size());
        assertEquals(true, jobsToSubmit.containsAll(candidateJobs.values()));
    }

    @Test
    public void testSkipSubmittedJob() throws IOException {
        PhoenixAsyncIndex[] jobs = new PhoenixAsyncIndex[candidateJobs.size()];
        candidateJobs.values().toArray(jobs);
        
        // Mark one job as running
        submittedJobs.add(String.format(IndexTool.INDEX_JOB_NAME_TEMPLATE,
            jobs[0].getDataTableName(), jobs[0].getTableName()));

        PhoenixMRJobSubmitter submitter = new PhoenixMRJobSubmitter();
        Set<PhoenixAsyncIndex> jobsToSubmit =
                submitter.getJobsToSubmit(candidateJobs, submittedJobs);
        
        // Should not contain the running job
        assertEquals(1, jobsToSubmit.size());
        assertEquals(false, jobsToSubmit.containsAll(candidateJobs.values()));
        assertEquals(true, jobsToSubmit.contains(jobs[1]));
    }

    @Test
    public void testSkipAllSubmittedJobs() throws IOException {
        PhoenixAsyncIndex[] jobs = new PhoenixAsyncIndex[candidateJobs.size()];
        candidateJobs.values().toArray(jobs);
        
        // Mark all the candidate jobs as running/in-progress
        submittedJobs.add(String.format(IndexTool.INDEX_JOB_NAME_TEMPLATE,
            jobs[0].getDataTableName(), jobs[0].getTableName()));
        submittedJobs.add(String.format(IndexTool.INDEX_JOB_NAME_TEMPLATE,
            jobs[1].getDataTableName(), jobs[1].getTableName()));

        PhoenixMRJobSubmitter submitter = new PhoenixMRJobSubmitter();
        Set<PhoenixAsyncIndex> jobsToSubmit =
                submitter.getJobsToSubmit(candidateJobs, submittedJobs);
        assertEquals(0, jobsToSubmit.size());
    }
    
    @Test
    public void testNoJobsToSubmit() throws IOException {
        // Clear candidate jobs
        candidateJobs.clear();
        // Add some dummy running jobs to the submitted list
        submittedJobs.add(String.format(IndexTool.INDEX_JOB_NAME_TEMPLATE,
            "d1", "i1"));
        submittedJobs.add(String.format(IndexTool.INDEX_JOB_NAME_TEMPLATE,
            "d2", "i2"));
        PhoenixMRJobSubmitter submitter = new PhoenixMRJobSubmitter();
        Set<PhoenixAsyncIndex> jobsToSubmit =
                submitter.getJobsToSubmit(candidateJobs, submittedJobs);
        assertEquals(0, jobsToSubmit.size());
    }
}