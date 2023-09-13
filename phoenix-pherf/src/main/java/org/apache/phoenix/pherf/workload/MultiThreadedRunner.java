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

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.Calendar;
import java.util.Date;
import java.util.concurrent.Callable;

import org.apache.phoenix.pherf.PherfConstants;
import org.apache.phoenix.thirdparty.com.google.common.annotations.VisibleForTesting;
import org.apache.hadoop.hbase.util.Pair;
import org.apache.phoenix.pherf.result.DataModelResult;
import org.apache.phoenix.pherf.result.ResultManager;
import org.apache.phoenix.pherf.result.RunTime;
import org.apache.phoenix.pherf.result.ThreadTime;
import org.apache.phoenix.pherf.rules.RulesApplier;
import org.apache.phoenix.util.EnvironmentEdgeManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.phoenix.pherf.PherfConstants.GeneratePhoenixStats;
import org.apache.phoenix.pherf.configuration.Query;
import org.apache.phoenix.pherf.configuration.Scenario;
import org.apache.phoenix.pherf.configuration.WriteParams;
import org.apache.phoenix.pherf.configuration.XMLConfigParser;
import org.apache.phoenix.pherf.util.PhoenixUtil;

class MultiThreadedRunner implements Callable<Void> {
    private static final Logger LOGGER = LoggerFactory.getLogger(MultiThreadedRunner.class);
    private Query query;
    private ThreadTime threadTime;
    private PhoenixUtil pUtil = PhoenixUtil.create();
    private String threadName;
    private DataModelResult dataModelResult;
    private long numberOfExecutions;
    private long executionDurationInMs;
    private static long lastResultWritten = EnvironmentEdgeManager.currentTimeMillis() - 1000;
    private final ResultManager resultManager;
    private final RulesApplier ruleApplier;
    private final Scenario scenario;
    private final WorkloadExecutor workloadExecutor;
    private final XMLConfigParser parser;
    private final boolean writeRuntimeResults;

    /**
     * MultiThreadedRunner
     *
     * @param threadName
     * @param query
     * @param dataModelResult
     * @param threadTime
     * @param numberOfExecutions
     * @param executionDurationInMs
     * @param ruleRunner 
     */
    MultiThreadedRunner(String threadName, Query query, DataModelResult dataModelResult,
            ThreadTime threadTime, long numberOfExecutions, long executionDurationInMs, boolean writeRuntimeResults, RulesApplier ruleApplier, Scenario scenario, WorkloadExecutor workloadExecutor, XMLConfigParser parser) {
        this.query = query;
        this.threadName = threadName;
        this.threadTime = threadTime;
        this.dataModelResult = dataModelResult;
        this.numberOfExecutions = numberOfExecutions;
        this.executionDurationInMs = executionDurationInMs;
        this.ruleApplier = ruleApplier;
        this.scenario = scenario;
       	this.resultManager = new ResultManager(dataModelResult.getName(), writeRuntimeResults);
       	this.workloadExecutor = workloadExecutor;
       	this.parser = parser;
       	this.writeRuntimeResults = writeRuntimeResults;
    }

    /**
     * Executes run for a minimum of number of execution or execution duration
     */
    @Override
    public Void call() throws Exception {
        LOGGER.info("\n\nThread Starting " + threadName + " ; '" + query.getStatement() + "' for "
                + numberOfExecutions + " times\n\n");
        long threadStartTime = EnvironmentEdgeManager.currentTimeMillis();
        for (long i = 0; i < numberOfExecutions; i++) {
            long threadElapsedTime = EnvironmentEdgeManager.currentTimeMillis() - threadStartTime;
            if (threadElapsedTime >= executionDurationInMs) {
                LOGGER.info("Queryset timeout of " + executionDurationInMs + " ms reached; current time is " + threadElapsedTime + " ms."
                        + "\nStopping queryset execution for query " + query.getId() + " on thread " + threadName + "...");
                break;
            }

            synchronized (workloadExecutor) {
                if (!timedQuery(i+1)) {
                    break;
                }
                if (writeRuntimeResults &&
                        (EnvironmentEdgeManager.currentTimeMillis() - lastResultWritten) > 1000) {
                    resultManager.write(dataModelResult, ruleApplier);
                    lastResultWritten = EnvironmentEdgeManager.currentTimeMillis();
                }
            }
        }

        if (!writeRuntimeResults) {
            long duration = EnvironmentEdgeManager.currentTimeMillis() - threadStartTime;
            LOGGER.info("The read query " + query.getStatement() + " for this thread in ("
                    + duration + ") Ms");
        }

        // Make sure all result have been dumped before exiting
        if (writeRuntimeResults) {
            synchronized (workloadExecutor) {
                resultManager.flush();
            }
        }

        LOGGER.info("\n\nThread exiting." + threadName + "\n\n");
        return null;
    }

    private synchronized ThreadTime getThreadTime() {
        return threadTime;
    }

    /**
     * Timed query execution
     *
     * @throws Exception
     * @returns boolean true if query finished without timing out; false otherwise
     */
    private boolean timedQuery(long iterationNumber) throws Exception {
        boolean
                isSelectCountStatement =
                query.getStatement().toUpperCase().trim().contains("COUNT(") ? true : false;

        Connection conn = null;
        PreparedStatement statement = null;
        ResultSet rs = null;
        Long queryStartTime = EnvironmentEdgeManager.currentTimeMillis();
        Date startDate = Calendar.getInstance().getTime();
        String exception = null;
        Long resultRowCount = 0L;
        String queryIteration = threadName + ":" + iterationNumber;
        Long queryElapsedTime = 0L;

        try {
            conn = pUtil.getConnection(query.getTenantId(), scenario.getPhoenixProperties());
            conn.setAutoCommit(true);
            final String statementString = query.getDynamicStatement(ruleApplier, scenario);
            statement = conn.prepareStatement(statementString);
            LOGGER.debug("Executing iteration: " + queryIteration + ": " + statementString);
            
            if (scenario.getWriteParams() != null) {
            	Workload writes = new WriteWorkload(PhoenixUtil.create(), parser,
                        PherfConstants.create().
                                getProperties(PherfConstants.PHERF_PROPERTIES, true),
                        scenario, GeneratePhoenixStats.NO);
            	workloadExecutor.add(writes);
            }
            
            boolean isQuery = statement.execute();
            if (isQuery) {
                rs = statement.getResultSet();
                Pair<Long, Long> r = getResults(rs, queryIteration, isSelectCountStatement, queryStartTime);
                resultRowCount = r.getFirst();
                queryElapsedTime = r.getSecond();
            } else {
                conn.commit();
            }
        } catch (Exception e) {
            LOGGER.error("Exception while executing query iteration " + queryIteration, e);
            exception = e.getMessage();
            throw e;
        } finally {
            getThreadTime().getRunTimesInMs().add(new RunTime(exception, startDate, resultRowCount,
                    queryElapsedTime, queryElapsedTime > query.getTimeoutDuration()));

            if (rs != null) rs.close();
            if (statement != null) statement.close();
            if (conn != null) conn.close();
        }
        return true;
    }

    @VisibleForTesting
    /**
     * @return a Pair whose first value is the resultRowCount, and whose second value is whether the query timed out.
     */
    Pair<Long, Long> getResults(ResultSet rs, String queryIteration, boolean isSelectCountStatement, Long queryStartTime) throws Exception {
        Long resultRowCount = 0L;
        while (rs.next()) {
            if (null != query.getExpectedAggregateRowCount()) {
                if (rs.getLong(1) != query.getExpectedAggregateRowCount())
                    throw new RuntimeException(
                            "Aggregate count " + rs.getLong(1) + " does not match expected "
                                    + query.getExpectedAggregateRowCount());
            }

            if (isSelectCountStatement) {
                resultRowCount = rs.getLong(1);
            } else {
                resultRowCount++;
            }
            long queryElapsedTime = EnvironmentEdgeManager.currentTimeMillis() - queryStartTime;
            if (queryElapsedTime >= query.getTimeoutDuration()) {
                LOGGER.error("Query " + queryIteration + " exceeded timeout of "
                        +  query.getTimeoutDuration() + " ms at " + queryElapsedTime + " ms.");
                return new Pair(resultRowCount, queryElapsedTime);
            }
        }
        return new Pair(resultRowCount, EnvironmentEdgeManager.currentTimeMillis() - queryStartTime);
    }

}