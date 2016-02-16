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

import java.sql.ParameterMetaData;
import java.sql.SQLException;
import java.util.Set;

import org.apache.phoenix.execute.MutationState;
import org.apache.phoenix.jdbc.PhoenixStatement.Operation;
import org.apache.phoenix.schema.TableRef;

public class DelegateMutationPlan implements MutationPlan {
    @Override
    public MutationState execute() throws SQLException {
        return plan.execute();
    }

    @Override
    public StatementContext getContext() {
        return plan.getContext();
    }

    @Override
    public TableRef getTargetRef() {
        return plan.getTargetRef();
    }

    @Override
    public ParameterMetaData getParameterMetaData() {
        return plan.getParameterMetaData();
    }

    @Override
    public ExplainPlan getExplainPlan() throws SQLException {
        return plan.getExplainPlan();
    }

    @Override
    public Set<TableRef> getSourceRefs() {
        return plan.getSourceRefs();
    }

    @Override
    public Operation getOperation() {
        return plan.getOperation();
    }

    private final MutationPlan plan;
    
    public DelegateMutationPlan(MutationPlan plan) {
        this.plan = plan;
    }

}
