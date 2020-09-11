package org.apache.phoenix.pherf.workload.continuous;

import org.apache.phoenix.pherf.result.ResultValue;
import org.apache.phoenix.pherf.workload.continuous.tenantoperation.TenantOperationInfo;

import java.util.ArrayList;
import java.util.List;

/**
 * Holds metrics + contextual info on the operation run.
 */
public class OperationStats {
    private final String tenantId;
    private final String scenarioName;
    private final String tableName;
    private final String tenantGroup;
    private final String operationGroup;
    private final Operation.OperationType opType;
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
        this.scenarioName = input.getScenarioName();
        this.tableName = input.getTableName();
        this.tenantGroup = input.getTenantGroupId();
        this.operationGroup = input.getOperationGroupId();
        this.tenantId = input.getTenantId();
        this.opType = input.getOperation().getType();
        this.startTime = startTime;
        this.status = status;
        this.rowCount = rowCount;
        this.durationInMs = durationInMs;
    }

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

    public long getStartTime() { return startTime; }

    public long getDurationInMs() {
        return durationInMs;
    }

    public List<ResultValue> getCsvRepresentation(String handlerId) {
        List<ResultValue> rowValues = new ArrayList<>();
        rowValues.add(new ResultValue(scenarioName));
        rowValues.add(new ResultValue(handlerId));
        rowValues.add(new ResultValue(tableName));
        rowValues.add(new ResultValue(tenantGroup));
        rowValues.add(new ResultValue(operationGroup));
        rowValues.add(new ResultValue(tenantId));
        rowValues.add(new ResultValue(opType.name()));
        rowValues.add(new ResultValue(String.valueOf(startTime)));
        rowValues.add(new ResultValue(String.valueOf(status)));
        rowValues.add(new ResultValue(String.valueOf(rowCount)));
        rowValues.add(new ResultValue(String.valueOf(durationInMs)));
        return rowValues;
    }
}
