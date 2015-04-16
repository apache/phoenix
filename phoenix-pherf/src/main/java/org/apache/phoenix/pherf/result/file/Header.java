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

package org.apache.phoenix.pherf.result.file;

public enum Header {
    EMPTY(""),
    AGGREGATE_PERFORMANCE("START_TIME,QUERY_GROUP,QUERY,TENANT_ID,AVG_MAX_TIME_MS,AVG_TIME_MS,AVG_MIN_TIME_MS,RUN_COUNT"),
    DETAILED_BASE("BASE_TABLE_NAME,SCENARIO_NAME,ZOOKEEPER,ROW_COUNT,EXECUTION_COUNT,EXECUTION_TYPE,PHOENIX_PROPERTIES"
            + ",START_TIME,QUERY_GROUP,QUERY,TENANT_ID,THREAD_NUMBER,CONCURRENCY_LEVEL"),
    DETAILED_PERFORMANCE(DETAILED_BASE + ",RESULT_ROW_COUNT,RUN_TIME_MS"),
    DETAILED_FUNCTIONAL(DETAILED_BASE + ",DIFF_STATUS,EXPLAIN_PLAN"),
    AGGREGATE_DATA_LOAD("ZK,TABLE_NAME,ROW_COUNT,LOAD_DURATION_IN_MS"),
    MONITOR("STAT_NAME,STAT_VALUE,TIME_STAMP");

    private String header;

    private Header(String header) {
        this.header = header;
    }

    @Override
    public String toString() {
        return header;
    }
}