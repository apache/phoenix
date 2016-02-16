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
import java.util.Collections;
import java.util.Set;

import org.apache.phoenix.jdbc.PhoenixStatement.Operation;
import org.apache.phoenix.schema.TableRef;

public abstract class BaseMutationPlan implements MutationPlan {
    private final StatementContext context;
    private final Operation operation;
    
    public BaseMutationPlan(StatementContext context, Operation operation) {
        this.context = context;
        this.operation = operation;
    }
    
    @Override
    public Operation getOperation() {
        return operation;
    }
    
    @Override
    public StatementContext getContext() {
        return context;
    }

    @Override
    public ParameterMetaData getParameterMetaData() {
        return context.getBindManager().getParameterMetaData();
    }

    @Override
    public ExplainPlan getExplainPlan() throws SQLException {
        return ExplainPlan.EMPTY_PLAN;
    }

    @Override
    public TableRef getTargetRef() {
        return context.getCurrentTable();
    }
    
    @Override
    public Set<TableRef> getSourceRefs() {
        return Collections.emptySet();
    }

}