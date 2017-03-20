/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * http://www.apache.org/licenses/LICENSE-2.0
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.phoenix.coprocessor;

import com.google.common.collect.Lists;
import org.apache.hadoop.hbase.client.Result;

import java.util.List;

/**
 * Certain operations, such as DROP TABLE are not allowed if there a table has child views. This class wraps the
 * Results of a scanning the Phoenix Metadata for child views for a specific table and stores an additional flag for
 * whether whether SYSTEM.CATALOG has split across multiple regions.
 */
class TableViewFinderResult {

    private List<Result> results = Lists.newArrayList();

    TableViewFinderResult() {
    }

    TableViewFinderResult(List<Result> results) {
        this.results = results;
    }

    public boolean hasViews() {
        return !results.isEmpty();
    }

    List<Result> getResults() {
        return results;
    }

    void addResult(TableViewFinderResult result) {
        this.results.addAll(result.getResults());
    }
}
