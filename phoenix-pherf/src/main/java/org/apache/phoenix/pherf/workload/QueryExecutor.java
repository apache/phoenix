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

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.phoenix.pherf.PherfConstants.RunMode;
import org.apache.phoenix.pherf.configuration.*;
import org.apache.phoenix.pherf.result.*;
import org.apache.phoenix.pherf.util.PhoenixUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;

public class QueryExecutor implements Workload {
    private static final Logger logger = LoggerFactory.getLogger(QueryExecutor.class);
    private List<DataModel> dataModels;
    private String queryHint;
    private final RunMode runMode;
    private final boolean exportCSV;
    private final ExecutorService pool;
    private final XMLConfigParser parser;
    private final PhoenixUtil util;

    public QueryExecutor(XMLConfigParser parser, PhoenixUtil util, ExecutorService pool) {
        this(parser, util, pool, parser.getDataModels(), null, false, RunMode.PERFORMANCE);
    }

    public QueryExecutor(XMLConfigParser parser, PhoenixUtil util, ExecutorService pool,
            List<DataModel> dataModels, String queryHint, boolean exportCSV, RunMode runMode) {
        this.parser = parser;
        this.queryHint = queryHint;
        this.exportCSV = exportCSV;
        this.runMode = runMode;
        this.dataModels = dataModels;
        this.pool = pool;
        this.util = util;
    }

    @Override public void complete() {

    }

    /**
     * Calls in Multithreaded Query Executor for all datamodels
     *
     * @throws Exception
     */
    public Runnable execute() throws Exception {
        Runnable runnable = null;
        for (DataModel dataModel : dataModels) {
            if (exportCSV) {
                runnable = exportAllScenarios(dataModel);
            } else {
                runnable = executeAllScenarios(dataModel);
            }
        }
        return runnable;
    }

    /**
     * Export all queries results to CSV
     *
     * @param dataModel
     * @throws Exception
     */
    protected Runnable exportAllScenarios(final DataModel dataModel) throws Exception {
        return new Runnable() {
            @Override public void run() {
                try {

                    List<Scenario> scenarios = dataModel.getScenarios();
                    QueryVerifier exportRunner = new QueryVerifier(false);
                    for (Scenario scenario : scenarios) {
                        for (QuerySet querySet : scenario.getQuerySet()) {
                            util.executeQuerySetDdls(querySet);
                            for (Query query : querySet.getQuery()) {
                                exportRunner.exportCSV(query);
                            }
                        }
                    }
                } catch (Exception e) {
                    logger.warn("", e);
                }
            }
        };
    }

    /**
     * Execute all scenarios
     *
     * @param dataModel
     * @throws Exception
     */
    protected Runnable executeAllScenarios(final DataModel dataModel) throws Exception {
        return new Runnable() {
            @Override public void run() {
                List<DataModelResult> dataModelResults = new ArrayList<>();
                DataModelResult
                        dataModelResult =
                        new DataModelResult(dataModel, PhoenixUtil.getZookeeper());
                ResultManager
                        resultManager =
                        new ResultManager(dataModelResult.getName(), QueryExecutor.this.runMode);

                dataModelResults.add(dataModelResult);
                List<Scenario> scenarios = dataModel.getScenarios();
                Configuration conf = HBaseConfiguration.create();
                Map<String, String> phoenixProperty = conf.getValByRegex("phoenix");
                try {

                    for (Scenario scenario : scenarios) {
                        ScenarioResult scenarioResult = new ScenarioResult(scenario);
                        scenarioResult.setPhoenixProperties(phoenixProperty);
                        dataModelResult.getScenarioResult().add(scenarioResult);
                        WriteParams writeParams = scenario.getWriteParams();

                        if (writeParams != null) {
                            int writerThreadCount = writeParams.getWriterThreadCount();
                            for (int i = 0; i < writerThreadCount; i++) {
                                logger.debug("Inserting write workload ( " + i + " ) of ( "
                                        + writerThreadCount + " )");
                                Workload writes = new WriteWorkload(PhoenixUtil.create(), parser);
                                pool.submit(writes.execute());
                            }
                        }

                        for (QuerySet querySet : scenario.getQuerySet()) {
                            QuerySetResult querySetResult = new QuerySetResult(querySet);
                            scenarioResult.getQuerySetResult().add(querySetResult);

                            util.executeQuerySetDdls(querySet);
                            if (querySet.getExecutionType() == ExecutionType.SERIAL) {
                                executeQuerySetSerial(dataModelResult, querySet, querySetResult);
                            } else {
                                executeQuerySetParallel(dataModelResult, querySet, querySetResult);
                            }
                        }
                        resultManager.write(dataModelResult);
                    }
                    resultManager.write(dataModelResults);
                } catch (Exception e) {
                    logger.warn("", e);
                }
            }
        };
    }

    /**
     * Execute query set serially
     *
     * @param dataModelResult
     * @param querySet
     * @param querySetResult
     * @throws InterruptedException
     */
    protected void executeQuerySetSerial(DataModelResult dataModelResult, QuerySet querySet,
            QuerySetResult querySetResult) throws InterruptedException {
        for (Query query : querySet.getQuery()) {
            QueryResult queryResult = new QueryResult(query);
            querySetResult.getQueryResults().add(queryResult);

            for (int cr = querySet.getMinConcurrency(); cr <= querySet.getMaxConcurrency(); cr++) {

                List<Future> threads = new ArrayList<>();

                for (int i = 0; i < cr; i++) {

                    Runnable
                            thread =
                            executeRunner((i + 1) + "," + cr, dataModelResult, queryResult,
                                    querySetResult);
                    threads.add(pool.submit(thread));
                }

                for (Future thread : threads) {
                    try {
                        thread.get();
                    } catch (ExecutionException e) {
                        logger.error("", e);
                    }
                }
            }
        }
    }

    /**
     * Execute query set in parallel
     *
     * @param dataModelResult
     * @param querySet
     * @param querySetResult
     * @throws InterruptedException
     */
    protected void executeQuerySetParallel(DataModelResult dataModelResult, QuerySet querySet,
            QuerySetResult querySetResult) throws InterruptedException {
        for (int cr = querySet.getMinConcurrency(); cr <= querySet.getMaxConcurrency(); cr++) {
            List<Future> threads = new ArrayList<>();
            for (int i = 0; i < cr; i++) {
                for (Query query : querySet.getQuery()) {
                    QueryResult queryResult = new QueryResult(query);
                    querySetResult.getQueryResults().add(queryResult);

                    Runnable
                            thread =
                            executeRunner((i + 1) + "," + cr, dataModelResult, queryResult,
                                    querySetResult);
                    threads.add(pool.submit(thread));
                }

                for (Future thread : threads) {
                    try {
                        thread.get();
                    } catch (ExecutionException e) {
                        logger.error("", e);
                    }
                }
            }
        }
    }

    /**
     * Execute multi-thread runner
     *
     * @param name
     * @param dataModelResult
     * @param queryResult
     * @param querySet
     * @return
     */
    protected Runnable executeRunner(String name, DataModelResult dataModelResult,
            QueryResult queryResult, QuerySet querySet) {
        ThreadTime threadTime = new ThreadTime();
        queryResult.getThreadTimes().add(threadTime);
        threadTime.setThreadName(name);
        queryResult.setHint(this.queryHint);
        logger.info("\nExecuting query " + queryResult.getStatement());
        Runnable thread;
        if (this.runMode == RunMode.FUNCTIONAL) {
            thread =
                    new MultithreadedDiffer(threadTime.getThreadName(), queryResult, threadTime,
                            querySet.getNumberOfExecutions(), querySet.getExecutionDurationInMs());
        } else {
            thread =
                    new MultiThreadedRunner(threadTime.getThreadName(), queryResult,
                            dataModelResult, threadTime, querySet.getNumberOfExecutions(),
                            querySet.getExecutionDurationInMs());
        }
        return thread;
    }
}