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
package org.apache.phoenix.mapreduce.index.fsck;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import org.apache.phoenix.util.EnvironmentEdgeManager;

/**
 * The diagnostic report of one IndexFsckTool command. The report can render as text, or as JSON
 * that has a schema version.
 */
@JsonInclude(JsonInclude.Include.NON_EMPTY)
public class Report {
  public static final String SCHEMA_VERSION = "1";

  private static final ObjectMapper MAPPER =
    new ObjectMapper().enable(SerializationFeature.INDENT_OUTPUT);

  private final long timestamp = EnvironmentEdgeManager.currentTimeMillis();
  private final String command;
  private final String dataTable;
  private final String indexTable;
  private final String tenantId;
  private final List<Finding> findings = new ArrayList<>();
  private final Map<String, Object> inspection = new LinkedHashMap<>();
  private RepairPlan repairPlan;

  public Report(String command, IndexFsckContext context) {
    this.command = command;
    this.dataTable = context.getDataTableName();
    this.indexTable = context.getIndexTableName();
    this.tenantId = context.getTenantId();
  }

  public String getSchemaVersion() {
    return SCHEMA_VERSION;
  }

  public long getTimestamp() {
    return timestamp;
  }

  public String getCommand() {
    return command;
  }

  public String getDataTable() {
    return dataTable;
  }

  public String getIndexTable() {
    return indexTable;
  }

  public String getTenantId() {
    return tenantId;
  }

  public List<Finding> getFindings() {
    return Collections.unmodifiableList(findings);
  }

  public void addFinding(Finding finding) {
    findings.add(finding);
  }

  public void addFindings(Collection<Finding> toAdd) {
    findings.addAll(toAdd);
  }

  public boolean hasFinding(String rule) {
    return findings.stream().anyMatch(f -> f.getRule().equals(rule));
  }

  public RepairPlan getRepairPlan() {
    return repairPlan;
  }

  public void setRepairPlan(RepairPlan repairPlan) {
    this.repairPlan = repairPlan;
  }

  public Map<String, Object> getInspection() {
    return Collections.unmodifiableMap(inspection);
  }

  public void putInspection(String key, Object value) {
    inspection.put(key, value);
  }

  public boolean hasErrors() {
    return getCount(Severity.ERROR) > 0;
  }

  public long getCount(Severity severity) {
    return findings.stream().filter(f -> f.getSeverity() == severity).count();
  }

  public String toJson() {
    return toJson(this);
  }

  static String toJson(Object value) {
    try {
      return MAPPER.writeValueAsString(value);
    } catch (Exception e) {
      throw new IllegalStateException("Failed to serialize " + value.getClass().getSimpleName(), e);
    }
  }

  public String toText() {
    StringBuilder sb = new StringBuilder();
    sb.append(String.format("%s %s on %s%s%n", command, indexTable, dataTable,
      tenantId != null ? " (tenant " + tenantId + ")" : ""));
    sb.append(String.format("%d error(s), %d warning(s), %d info%n", getCount(Severity.ERROR),
      getCount(Severity.WARN), getCount(Severity.INFO)));
    for (Finding finding : findings) {
      sb.append("  ").append(finding).append(System.lineSeparator());
    }
    if (repairPlan != null) {
      sb.append(repairPlan.isDryRun() ? "Repair plan (dry run):" : "Repair actions:")
        .append(System.lineSeparator());
      for (RepairAction action : repairPlan.getActions()) {
        sb.append("  ").append(action).append(System.lineSeparator());
      }
    }
    for (Map.Entry<String, Object> entry : inspection.entrySet()) {
      sb.append(entry.getKey()).append(": ").append(entry.getValue())
        .append(System.lineSeparator());
    }
    return sb.toString();
  }
}
