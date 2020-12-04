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
package org.apache.phoenix.compile;

import java.util.Collections;
import java.util.List;

import com.google.common.collect.ImmutableList;

public class ExplainPlan {
    public static final ExplainPlan EMPTY_PLAN = new ExplainPlan(Collections.<String>emptyList());

    private final List<String> planSteps;
    private final ExplainPlanAttributes planStepsAsAttributes;

    public ExplainPlan(List<String> planSteps) {
        this.planSteps = ImmutableList.copyOf(planSteps);
        this.planStepsAsAttributes =
            ExplainPlanAttributes.getDefaultExplainPlan();
    }

    public ExplainPlan(List<String> planSteps,
            ExplainPlanAttributes planStepsAsAttributes) {
        this.planSteps = planSteps;
        this.planStepsAsAttributes = planStepsAsAttributes;
    }

    public List<String> getPlanSteps() {
        return planSteps;
    }

    public ExplainPlanAttributes getPlanStepsAsAttributes() {
        return planStepsAsAttributes;
    }

    @Override
    public String toString() {
        StringBuilder buf = new StringBuilder();
        for (String step : planSteps) {
            buf.append(step);
            buf.append('\n');
        }
        return buf.toString();
    }
}
