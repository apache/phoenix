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

package org.apache.phoenix.pherf.result;

import org.apache.phoenix.pherf.configuration.QuerySet;

import java.util.ArrayList;
import java.util.List;

public class QuerySetResult extends QuerySet {

    private List<QueryResult> queryResults = new ArrayList<>();

    public QuerySetResult(QuerySet querySet) {
        this.setConcurrency(querySet.getConcurrency());
        this.setNumberOfExecutions(querySet.getNumberOfExecutions());
        this.setExecutionDurationInMs(querySet.getExecutionDurationInMs());
        this.setExecutionType(querySet.getExecutionType());
    }

    public QuerySetResult() {
    }

    public List<QueryResult> getQueryResults() {
        return queryResults;
    }

    @SuppressWarnings("unused")
    public void setQueryResults(List<QueryResult> queryResults) {
        this.queryResults = queryResults;
    }
}
