/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.phoenix.pherf.workload.mt.generators;

import org.apache.phoenix.pherf.workload.mt.operations.Operation;

/**
 * Holds information on the tenant operation details.
 */
public class TenantOperationInfo {
    private final String modelName;
    private final String scenarioName;
    private final String tableName;
    private final String tenantId;
    private final String tenantGroupId;
    private final String operationGroupId;
    private final Operation operation;

    public TenantOperationInfo(String modelName, String scenarioName, String tableName,
            String tenantGroupId, String operationGroupId,
            String tenantId, Operation operation) {
        this.modelName = modelName;
        this.scenarioName = scenarioName;
        this.tableName = tableName;
        this.tenantGroupId = tenantGroupId;
        this.operationGroupId = operationGroupId;
        this.tenantId = tenantId;
        this.operation = operation;
    }

    public String getModelName() { return modelName; }

    public String getScenarioName() { return scenarioName; }

    public String getTableName() {
        return tableName;
    }

    public String getTenantGroupId() {
        return tenantGroupId;
    }

    public String getOperationGroupId() {
        return operationGroupId;
    }

    public Operation getOperation() {
        return operation;
    }

    public String getTenantId() {
        return tenantId;
    }
}
