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

package org.apache.phoenix.pherf.workload.mt.operations;

import org.apache.phoenix.pherf.result.ResultValue;
import org.apache.phoenix.pherf.workload.mt.generators.TenantOperationInfo;
import org.apache.phoenix.pherf.workload.mt.operations.Operation;

import java.util.ArrayList;
import java.util.List;

/**
 * Holds metrics + contextual info on the operation run.
 */
public class OperationStats {
    private final TenantOperationInfo input;
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
        this.input = input;
        this.startTime = startTime;
        this.status = status;
        this.rowCount = rowCount;
        this.durationInMs = durationInMs;
    }

    public String getModelName() { return this.input.getModelName(); }

    public String getScenarioName() { return this.input.getScenarioName(); }

    public String getTenantId() { return this.input.getTenantId(); }

    public Operation.OperationType getOpType() { return this.input.getOperation().getType(); }

    public String getTableName() {
        return this.input.getTableName();
    }

    public String getTenantGroup() {
        return this.input.getTenantGroupId();
    }

    public String getOperationGroup() {
        return this.input.getOperationGroupId();
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
        rowValues.add(new ResultValue(getModelName()));
        rowValues.add(new ResultValue(getScenarioName()));
        rowValues.add(new ResultValue(getTableName()));
        rowValues.add(new ResultValue(getTenantId()));
        rowValues.add(new ResultValue(handlerId));
        rowValues.add(new ResultValue(getTenantGroup()));
        rowValues.add(new ResultValue(getOperationGroup()));
        rowValues.add(new ResultValue(getOpType().name()));
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
