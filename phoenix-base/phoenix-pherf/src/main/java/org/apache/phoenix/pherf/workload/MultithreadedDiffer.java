/*
 * Licensed to the Apache Software Foundation (ASF) under one
 *   or more contributor license agreements.  See the NOTICE file
 *   distributed with this work for additional information
 *   regarding copyright ownership.  The ASF licenses this file
 *   to you under the Apache License, Version 2.0 (the
 *   "License"); you may not use this file except in compliance
 *   with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 *   Unless required by applicable law or agreed to in writing, software
 *   distributed under the License is distributed on an "AS IS" BASIS,
 *   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *   See the License for the specific language governing permissions and
 *   limitations under the License.
 */

package org.apache.phoenix.pherf.workload;

import java.util.Calendar;
import java.util.Date;
import java.util.concurrent.Callable;

import org.apache.phoenix.thirdparty.com.google.common.annotations.VisibleForTesting;
import org.apache.phoenix.pherf.PherfConstants;
import org.apache.phoenix.pherf.configuration.Query;
import org.apache.phoenix.pherf.result.RunTime;
import org.apache.phoenix.pherf.result.ThreadTime;
import org.apache.phoenix.pherf.util.PhoenixUtil;
import org.apache.phoenix.util.EnvironmentEdgeManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

class MultithreadedDiffer implements Callable<Void> {
    private static final Logger LOGGER = LoggerFactory.getLogger(MultithreadedDiffer.class);
    private Thread t;
    private Query query;
    private ThreadTime threadTime;
    private long numberOfExecutions;
    private long executionDurationInMs;
    private QueryVerifier queryVerifier = new QueryVerifier(true);
    private static PhoenixUtil pUtil = PhoenixUtil.create();

    private synchronized ThreadTime getThreadTime() {
        return threadTime;
    }

    /**
     * Query Verification
     *
     * @throws Exception
     */
    private void diffQuery() throws Exception {
        Long start = EnvironmentEdgeManager.currentTimeMillis();
        Date startDate = Calendar.getInstance().getTime();
        String newCSV = queryVerifier.exportCSV(query);
        boolean verifyResult = queryVerifier.doDiff(query, newCSV);
        String explainPlan = pUtil.getExplainPlan(query);
        long elapsedTime = EnvironmentEdgeManager.currentTimeMillis() - start;
        getThreadTime().getRunTimesInMs().add(new RunTime(
                        verifyResult == true ? PherfConstants.DIFF_PASS : PherfConstants.DIFF_FAIL,
                        explainPlan, startDate, -1L,
            elapsedTime, !(elapsedTime >= executionDurationInMs)));
    }

    /**
     * Multithreaded Differ
     *
     * @param threadName
     * @param query
     * @param threadName
     * @param threadTime
     * @param numberOfExecutions
     * @param executionDurationInMs
     */
    MultithreadedDiffer(String threadName, Query query, ThreadTime threadTime,
            long numberOfExecutions, long executionDurationInMs) {
        this.query = query;
        this.threadTime = threadTime;
        this.numberOfExecutions = numberOfExecutions;
        this.executionDurationInMs = executionDurationInMs;
    }

    /**
     * Executes verification runs for a minimum of number of execution or execution duration
     */
    public Void call() throws Exception {
        LOGGER.info("\n\nThread Starting " + t.getName() + " ; " + query.getStatement() + " for "
                + numberOfExecutions + "times\n\n");
        Long start = EnvironmentEdgeManager.currentTimeMillis();
        for (long i = numberOfExecutions; (i > 0 &&
            ((EnvironmentEdgeManager.currentTimeMillis() - start)
                < executionDurationInMs)); i--) {
            try {
                diffQuery();
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
            LOGGER.info("\n\nThread exiting." + t.getName() + "\n\n");
        return null;
    }
}