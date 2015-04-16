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

package org.apache.phoenix.pherf.configuration;

import java.util.ArrayList;
import java.util.List;

import javax.xml.bind.annotation.XmlAttribute;

import org.apache.phoenix.pherf.PherfConstants;

public class QuerySet {
	private List<Query> query = new ArrayList<Query>();
	private String concurrency = PherfConstants.DEFAULT_CONCURRENCY;
	private long numberOfExecutions = PherfConstants.DEFAULT_NUMBER_OF_EXECUTIONS;
	private long executionDurationInMs = PherfConstants.DEFAULT_THREAD_DURATION_IN_MS;
	private ExecutionType executionType = ExecutionType.SERIAL;

	/**
	 * List of queries in each query set
	 * @return
	 */
	public List<Query> getQuery() {
		return query;
	}

	public void setQuery(List<Query> query) {
		this.query = query;
	}

	/**
	 * Target concurrency.
	 * This can be set as a range. Example: 
	 * 3
	 * 1-4
	 * @return
	 */
    @XmlAttribute
	public String getConcurrency() {
		return concurrency;
	}

	public void setConcurrency(String concurrency) {
		this.concurrency = concurrency;
	}
	
	/**
	 * Number of execution of query per thread. Minimum of either number of executions
	 * or execution duration is taken for each thread run
	 * @return
	 */
	@XmlAttribute
	public long getNumberOfExecutions() {
		return numberOfExecutions;
	}

	public void setNumberOfExecutions(long numberOfExecutions) {
		this.numberOfExecutions = numberOfExecutions;
	}
	
	/**
	 * Minimum concurrency level for a query set
	 * @return
	 */
	public int getMinConcurrency() {
		return getConcurrencyMinMax(0);
	}
	
	/**
	 * Maximum concurrency for a query set
	 * @return
	 */
	public int getMaxConcurrency() {
		return getConcurrencyMinMax(1);
	}
	
	private int getConcurrencyMinMax(int idx) {
		if (null == getConcurrency()) {
			return 1;
		}
		String[] concurrencySplit = getConcurrency().split("-");
		if (concurrencySplit.length == 2) {
			return Integer.parseInt(concurrencySplit[idx]);
		}
		return Integer.parseInt(getConcurrency());
	}

	/**
	 * This can be either SERIAL or PARALLEL
	 * @return
	 */
	@XmlAttribute
	public ExecutionType getExecutionType() {
		return executionType;
	}

	public void setExecutionType(ExecutionType executionType) {
		this.executionType = executionType;
	}

	/**
	 * Execution duration of query per thread. Minimum of either number of executions
	 * or execution duration is taken for each thread run
	 * @return
	 */
	@XmlAttribute
	public long getExecutionDurationInMs() {
		return executionDurationInMs;
	}

	public void setExecutionDurationInMs(long executionDurationInMs) {
		this.executionDurationInMs = executionDurationInMs;
	}	
}
