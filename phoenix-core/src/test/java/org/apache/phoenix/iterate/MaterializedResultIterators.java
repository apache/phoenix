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
package org.apache.phoenix.iterate;

import java.sql.SQLException;
import java.util.Collections;
import java.util.List;

import org.apache.hadoop.hbase.client.Scan;
import org.apache.phoenix.compile.ExplainPlanAttributes
    .ExplainPlanAttributesBuilder;
import org.apache.phoenix.query.KeyRange;

/**
 * 
 * ResultIteraors implementation backed by in-memory list of PeekingResultIterator
 *
 */
public class MaterializedResultIterators implements ResultIterators {
    private final List<PeekingResultIterator> results;

    public MaterializedResultIterators(List<PeekingResultIterator> results) {
        this.results = results;
    }

    @Override
    public List<PeekingResultIterator> getIterators() throws SQLException {
        return results;
    }

    @Override
    public void explain(List<String> planSteps,
            ExplainPlanAttributesBuilder explainPlanAttributesBuilder) {
    }

    @Override
    public int size() {
        return results.size();
    }

    @Override
    public void explain(List<String> planSteps) {
    }

    @Override
    public List<KeyRange> getSplits() {
    	return Collections.emptyList();
    }

    @Override
    public List<List<Scan>> getScans() {
    	return Collections.emptyList();
    }

    @Override
    public void close() throws SQLException {
    }
}