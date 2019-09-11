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
import org.apache.phoenix.pherf.configuration.DataModel;
import org.apache.phoenix.pherf.configuration.ExecutionType;
import org.apache.phoenix.pherf.configuration.Query;
import org.apache.phoenix.pherf.configuration.QuerySet;
import org.apache.phoenix.pherf.configuration.Scenario;
import org.apache.phoenix.pherf.configuration.XMLConfigParser;
import org.apache.phoenix.pherf.result.DataModelResult;
import org.apache.phoenix.pherf.result.QueryResult;
import org.apache.phoenix.pherf.result.QuerySetResult;
import org.apache.phoenix.pherf.result.ResultManager;
import org.apache.phoenix.pherf.result.ScenarioResult;
import org.apache.phoenix.pherf.result.ThreadTime;
import org.apache.phoenix.pherf.rules.RulesApplier;
import org.apache.phoenix.pherf.util.PhoenixUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

public class QueryExecutor implements Workload {
    private static final Logger LOGGER = LoggerFactory.getLogger(QueryExecutor.class);
    private List<DataModel> dataModels;
    private String queryHint;
    private final boolean exportCSV;
    private final XMLConfigParser parser;
    private final PhoenixUtil util;
    private final WorkloadExecutor workloadExecutor;
    private final boolean writeRuntimeResults;
    private RulesApplier ruleApplier;

    public QueryExecutor(XMLConfigParser parser, PhoenixUtil util,
            WorkloadExecutor workloadExecutor) {
        this(parser, util, workloadExecutor, parser.getDataModels(), null, false, true);
    }

    public QueryExecutor(XMLConfigParser parser, PhoenixUtil util,
            WorkloadExecutor workloadExecutor, List<DataModel> dataModels, String queryHint,
            boolean exportCSV) {
    	this(parser, util, workloadExecutor, dataModels, queryHint, exportCSV, true);
    }

    public QueryExecutor(XMLConfigParser parser, PhoenixUtil util,
            WorkloadExecutor workloadExecutor, List<DataModel> dataModels, String queryHint,
            boolean exportCSV, boolean writeRuntimeResults) {
        this.parser = parser;
        this.queryHint = queryHint;
        this.exportCSV = exportCSV;
        this.dataModels = dataModels;
        this.util = util;
        this.workloadExecutor = workloadExecutor;
        this.writeRuntimeResults = writeRuntimeResults;
        this.ruleApplier = new RulesApplier(parser);
    }

    @Override
    public void complete() {}

    /**
     * Calls in Multithreaded Query Executor for all datamodels
     *
     * @throws Exception
     */
    @Override
    public Callable<Void> execute() throws Exception {
        Callable<Void> callable = null;
        for (DataModel dataModel : dataModels) {
            if (exportCSV) {
                callable = exportAllScenarios(dataModel);
            } else {
                callable = executeAllScenarios(dataModel);
            }
        }
        return callable;
    }


    /**
     * Export all queries results to CSV
     *
     * @param dataModel
     * @throws Exception
     */
    protected Callable<Void> exportAllScenarios(final DataModel dataModel) throws Exception {
        return new Callable<Void>() {
            @Override
            public Void call() throws Exception {
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
                    LOGGER.error("Scenario throws exception", e);
                    throw e;
                }
                return null;
            }
        };
    }

    /**
     * Execute all scenarios
     *
     * @param dataModel
     * @throws Exception
     */
    protected Callable<Void> executeAllScenarios(final DataModel dataModel) throws Exception {
        return new Callable<Void>() {
            @Override public Void call() throws Exception {
                List<DataModelResult> dataModelResults = new ArrayList<>();
                DataModelResult
                        dataModelResult =
                        new DataModelResult(dataModel, PhoenixUtil.getZookeeper());
                ResultManager
                        resultManager =
                        new ResultManager(dataModelResult.getName());

                dataModelResults.add(dataModelResult);
                List<Scenario> scenarios = dataModel.getScenarios();
                Configuration conf = HBaseConfiguration.create();
                Map<String, String> phoenixProperty = conf.getValByRegex("phoenix");
                try {

                    for (Scenario scenario : scenarios) {
                        ScenarioResult scenarioResult = new ScenarioResult(scenario);
                        scenarioResult.setPhoenixProperties(phoenixProperty);
                        dataModelResult.getScenarioResult().add(scenarioResult);

                        for (QuerySet querySet : scenario.getQuerySet()) {
                            QuerySetResult querySetResult = new QuerySetResult(querySet);
                            scenarioResult.getQuerySetResult().add(querySetResult);

                            util.executeQuerySetDdls(querySet);
                            if (querySet.getExecutionType() == ExecutionType.SERIAL) {
                                executeQuerySetSerial(dataModelResult, querySet, querySetResult, scenario);
                            } else {
                                executeQuerySetParallel(dataModelResult, querySet, querySetResult, scenario);
                            }
                        }
                        resultManager.write(dataModelResult, ruleApplier);
                    }
                    resultManager.write(dataModelResults, ruleApplier);
                    resultManager.flush();
                } catch (Exception e) {
                    LOGGER.error("Scenario throws exception", e);
                    throw e;
                }
                return null;
            }
        };
    }

    /**
     * Execute query set serially
     *
     * @param dataModelResult
     * @param querySet
     * @param querySetResult
     * @param scenario 
     * @throws InterruptedException
     */
    protected void executeQuerySetSerial(DataModelResult dataModelResult, QuerySet querySet,
            QuerySetResult querySetResult, Scenario scenario) throws ExecutionException, InterruptedException {
        for (Query query : querySet.getQuery()) {
            QueryResult queryResult = new QueryResult(query);
            querySetResult.getQueryResults().add(queryResult);

            for (int cr = querySet.getMinConcurrency(); cr <= querySet.getMaxConcurrency(); cr++) {

                List<Future> threads = new ArrayList<>();

                for (int i = 0; i < cr; i++) {

                    Callable
                            thread =
                            executeRunner((i + 1) + "," + cr, dataModelResult, queryResult,
                                    querySetResult, scenario);
                    threads.add(workloadExecutor.getPool().submit(thread));
                }

                for (Future thread : threads) {
                    thread.get();
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
            QuerySetResult querySetResult, Scenario scenario) throws ExecutionException, InterruptedException {
        for (int cr = querySet.getMinConcurrency(); cr <= querySet.getMaxConcurrency(); cr++) {
            List<Future> threads = new ArrayList<>();
            for (int i = 0; i < cr; i++) {
                for (Query query : querySet.getQuery()) {
                    QueryResult queryResult = new QueryResult(query);
                    querySetResult.getQueryResults().add(queryResult);

                    Callable<Void>
                            thread =
                            executeRunner((i + 1) + "," + cr, dataModelResult, queryResult,
                                    querySetResult, scenario);
                    threads.add(workloadExecutor.getPool().submit(thread));
                }

                for (Future thread : threads) {
                    thread.get();
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
     * @param scenario 
     * @return
     */
    protected Callable<Void> executeRunner(String name, DataModelResult dataModelResult,
            QueryResult queryResult, QuerySet querySet, Scenario scenario) {
        ThreadTime threadTime = new ThreadTime();
        queryResult.getThreadTimes().add(threadTime);
        threadTime.setThreadName(name);
        queryResult.setHint(this.queryHint);
        LOGGER.info("\nExecuting query " + queryResult.getStatement());
        Callable<Void> thread;
        if (workloadExecutor.isPerformance()) {
            thread =
                    new MultiThreadedRunner(threadTime.getThreadName(), queryResult,
                            dataModelResult, threadTime, querySet.getNumberOfExecutions(),
                            querySet.getExecutionDurationInMs(), writeRuntimeResults, ruleApplier, scenario, workloadExecutor, parser);
        } else {
            thread =
                    new MultithreadedDiffer(threadTime.getThreadName(), queryResult, threadTime,
                            querySet.getNumberOfExecutions(), querySet.getExecutionDurationInMs());
        }
        return thread;
    }
}