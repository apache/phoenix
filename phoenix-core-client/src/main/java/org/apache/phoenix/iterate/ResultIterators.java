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
import java.util.List;

import org.apache.hadoop.hbase.client.Scan;
import org.apache.phoenix.compile.ExplainPlanAttributes
    .ExplainPlanAttributesBuilder;
import org.apache.phoenix.query.KeyRange;
import org.apache.phoenix.util.SQLCloseable;

public interface ResultIterators extends SQLCloseable {
    public int size();
    public List<KeyRange> getSplits();
    public List<List<Scan>> getScans();
    public void explain(List<String> planSteps);
    public List<PeekingResultIterator> getIterators() throws SQLException;

    /**
     * Generate ExplainPlan steps and add steps as list of Strings in
     * planSteps argument as readable statement as well as add same generated
     * steps in explainPlanAttributesBuilder so that we prepare ExplainPlan
     * result as an attribute object useful to retrieve individual plan step
     * attributes.
     *
     * @param planSteps Add generated plan in list of planSteps. This argument
     *     is used to provide planSteps as whole statement consisting of
     *     list of Strings.
     * @param explainPlanAttributesBuilder Add generated plan in attributes
     *     object. Having an API to provide planSteps as an object is easier
     *     while comparing individual attributes of ExplainPlan.
     */
    void explain(List<String> planSteps,
            ExplainPlanAttributesBuilder explainPlanAttributesBuilder);

}
