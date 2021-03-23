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

package org.apache.phoenix.pherf.workload.mt;

import org.apache.phoenix.pherf.result.ResultValue;
import org.apache.phoenix.pherf.workload.mt.tenantoperation.TenantOperationInfo;

import java.util.ArrayList;
import java.util.List;

/**
 * Holds metrics + contextual info on the operation run.
 */
public class OperationStats {
    private final String modelName;
    private final String scenarioName;
    private final String tableName;
    private final String tenantId;
    private final String tenantGroup;
    private final String operationGroup;
    private final Operation.OperationType opType;
    private String handlerId;
    private final int status;
    private final long rowCount;
    private final long durationInMs;
    private final long startTime;

    public OperationStats(
            TenantOperationInfo input,
            long startTime,
            int status,
            long rowCount,
            long durationInMs) {
        this.modelName = input.getModelName();
        this.scenarioName = input.getScenarioName();
        this.tableName = input.getTableName();
        this.tenantId = input.getTenantId();
        this.tenantGroup = input.getTenantGroupId();
        this.operationGroup = input.getOperationGroupId();
        this.opType = input.getOperation().getType();
        this.startTime = startTime;
        this.status = status;
        this.rowCount = rowCount;
        this.durationInMs = durationInMs;
    }

    public String getModelName() { return modelName; }

    public String getScenarioName() { return scenarioName; }

    public String getTenantId() { return tenantId; }

    public Operation.OperationType getOpType() { return opType; }

    public String getTableName() {
        return tableName;
    }

    public String getTenantGroup() {
        return tenantGroup;
    }

    public String getOperationGroup() {
        return operationGroup;
    }

    public int getStatus() {
        return status;
    }

    public long getRowCount() {
        return rowCount;
    }

    public String getHandlerId() { return handlerId; }

    public long getStartTime() { return startTime; }

    public long getDurationInMs() {
        return durationInMs;
    }

    public List<ResultValue> getCsvRepresentation() {
        List<ResultValue> rowValues = new ArrayList<>();
        rowValues.add(new ResultValue(modelName));
        rowValues.add(new ResultValue(scenarioName));
        rowValues.add(new ResultValue(tableName));
        rowValues.add(new ResultValue(tenantId));
        rowValues.add(new ResultValue(handlerId));
        rowValues.add(new ResultValue(tenantGroup));
        rowValues.add(new ResultValue(operationGroup));
        rowValues.add(new ResultValue(opType.name()));
        rowValues.add(new ResultValue(String.valueOf(startTime)));
        rowValues.add(new ResultValue(String.valueOf(status)));
        rowValues.add(new ResultValue(String.valueOf(rowCount)));
        rowValues.add(new ResultValue(String.valueOf(durationInMs)));
        return rowValues;
    }

    public void setHandlerId(String handlerId) {
        this.handlerId = handlerId;
    }
}
