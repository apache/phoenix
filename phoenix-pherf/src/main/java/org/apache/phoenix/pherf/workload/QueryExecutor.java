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
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.apache.phoenix.pherf.PherfConstants.RunMode;
import org.apache.phoenix.pherf.configuration.XMLConfigParser;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.phoenix.pherf.result.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.phoenix.pherf.configuration.DataModel;
import org.apache.phoenix.pherf.configuration.ExecutionType;
import org.apache.phoenix.pherf.configuration.Query;
import org.apache.phoenix.pherf.configuration.QuerySet;
import org.apache.phoenix.pherf.configuration.Scenario;
import org.apache.phoenix.pherf.util.PhoenixUtil;

public class QueryExecutor {
	private static final Logger logger = LoggerFactory.getLogger(QueryExecutor.class);
	private List<DataModel> dataModels;
	private String queryHint;
	private RunMode runMode;
    private final ResultUtil resultUtil;

	public QueryExecutor(XMLConfigParser parser) {
		this.dataModels = parser.getDataModels();
        this.resultUtil = new ResultUtil();
    }
	
	/**
	 * Calls in Multithreaded Query Executor for all datamodels
	 * @throws Exception 
	 */
	public void execute(String queryHint, boolean exportCSV, RunMode runMode) throws Exception {
		this.queryHint = queryHint;
		this.runMode = runMode;
		for (DataModel dataModel: dataModels) {
			if (exportCSV) {
				exportAllScenarios(dataModel);	
			} else {
				executeAllScenarios(dataModel);
			}
		}
	}

	/**
	 * Export all queries results to CSV 
	 * @param dataModel
	 * @throws Exception 
	 */
	protected void exportAllScenarios(DataModel dataModel) throws Exception {
		List<Scenario> scenarios = dataModel.getScenarios();
		QueryVerifier exportRunner = new QueryVerifier(false);
		for (Scenario scenario : scenarios) {
			for (QuerySet querySet : scenario.getQuerySet()) {
				executeQuerySetDdls(querySet);
				for (Query query : querySet.getQuery()) {
					exportRunner.exportCSV(query);
				}
			}
		}
	}
	
	/**
	 * Execute all scenarios
	 * @param dataModel
	 * @throws Exception 
	 */
	protected void executeAllScenarios(DataModel dataModel) throws Exception {
		List<DataModelResult> dataModelResults = new ArrayList<DataModelResult>();
		DataModelResult dataModelResult = new DataModelResult(dataModel, PhoenixUtil.getZookeeper());
        ResultManager resultManager = new ResultManager(dataModelResult.getName(), this.runMode);


		dataModelResults.add(dataModelResult);
		List<Scenario> scenarios = dataModel.getScenarios();
		Configuration conf = HBaseConfiguration.create();
		Map<String, String> phoenixProperty = conf.getValByRegex("phoenix");
		phoenixProperty.putAll(conf.getValByRegex("sfdc"));

		for (Scenario scenario : scenarios) {
			ScenarioResult scenarioResult = new ScenarioResult(scenario);
			scenarioResult.setPhoenixProperties(phoenixProperty);
			dataModelResult.getScenarioResult().add(scenarioResult);

			for (QuerySet querySet : scenario.getQuerySet()) {
				QuerySetResult querySetResult = new QuerySetResult(querySet);
				scenarioResult.getQuerySetResult().add(querySetResult);
				
				executeQuerySetDdls(querySet);
				
				if (querySet.getExecutionType() == ExecutionType.SERIAL) {
					execcuteQuerySetSerial(dataModelResult, querySet, querySetResult, scenarioResult);
				} else {
					execcuteQuerySetParallel(dataModelResult, querySet, querySetResult, scenarioResult);					
				}
			}
            resultManager.write(dataModelResult);
		}
        resultManager.write(dataModelResults);
	}

	/**
	 * Execute all querySet DDLs first based on tenantId if specified. This is executed
	 * first since we don't want to run DDLs in parallel to executing queries.
	 * 
	 * @param querySet
	 * @throws Exception 
	 */
	protected void executeQuerySetDdls(QuerySet querySet) throws Exception {
		PhoenixUtil pUtil = new PhoenixUtil();
		for (Query query : querySet.getQuery()) {
			if (null != query.getDdl()) {
				Connection conn = null;
				try {
					logger.info("\nExecuting DDL:" + query.getDdl() + " on tenantId:" + query.getTenantId());
					pUtil.executeStatement(query.getDdl(), conn = pUtil.getConnection(query.getTenantId()));
				} finally {
					if (null != conn) {
						conn.close();
					}
				}
			}
		}
	}

	/**
	 * Execute query set serially
	 * @param dataModelResult
	 * @param querySet
	 * @param querySetResult
	 * @param scenario
	 * @throws InterruptedException
	 */
	protected void execcuteQuerySetSerial(DataModelResult dataModelResult, QuerySet querySet, QuerySetResult querySetResult, Scenario scenario) throws InterruptedException {
		for (Query query : querySet.getQuery()) {
			QueryResult queryResult = new QueryResult(query);
			querySetResult.getQueryResults().add(queryResult);

			for (int cr = querySet.getMinConcurrency(); cr <= querySet
					.getMaxConcurrency(); cr++) {
				
				List<Thread> threads = new ArrayList<Thread>();
				
				for (int i = 0; i < cr; i++) {

					Thread thread = executeRunner((i + 1) + ","
							+ cr, dataModelResult, queryResult,
							querySetResult);
					threads.add(thread);
				}

				for (Thread thread : threads) {
					thread.join();
				}
			}
		}
	}

	/**
	 * Execute query set in parallel
	 * @param dataModelResult
	 * @param querySet
	 * @param querySetResult
	 * @param scenario
	 * @throws InterruptedException
	 */
	protected void execcuteQuerySetParallel(DataModelResult dataModelResult, QuerySet querySet, QuerySetResult querySetResult, Scenario scenario)
			throws InterruptedException {
		for (int cr = querySet.getMinConcurrency(); cr <= querySet
				.getMaxConcurrency(); cr++) {
			List<Thread> threads = new ArrayList<Thread>();
			for (int i = 0; i < cr; i++) {
				for (Query query : querySet.getQuery()) {
					QueryResult queryResult = new QueryResult(query);
					querySetResult.getQueryResults().add(queryResult);

					Thread thread = executeRunner((i + 1) + ","
							+ cr, dataModelResult, queryResult,
							querySetResult);
					threads.add(thread);
				}
			}
			for (Thread thread : threads) {
				thread.join();
			}
		}
	}
	
	/**
	 * Execute multi-thread runner
	 * @param name
	 * @param dataModelResult
	 * @param queryResult
	 * @param querySet
	 * @return
	 */
	protected Thread executeRunner(String name, DataModelResult dataModelResult, QueryResult queryResult, QuerySet querySet) {
		ThreadTime threadTime = new ThreadTime();
		queryResult.getThreadTimes().add(threadTime);
		threadTime.setThreadName(name);
		queryResult.setHint(this.queryHint);
		logger.info("\nExecuting query "
				+ queryResult.getStatement());
		Thread thread;
		if (this.runMode == RunMode.FUNCTIONAL) {
			thread = new MultithreadedDiffer(
					threadTime.getThreadName(),
					queryResult,
					threadTime, querySet.getNumberOfExecutions(), querySet.getExecutionDurationInMs())
					.start();
		} else {
			thread = new MultithreadedRunner(
					threadTime.getThreadName(),
					queryResult,
					dataModelResult,
					threadTime, querySet.getNumberOfExecutions(), querySet.getExecutionDurationInMs())
					.start();
		}
		return thread;
	}
}
