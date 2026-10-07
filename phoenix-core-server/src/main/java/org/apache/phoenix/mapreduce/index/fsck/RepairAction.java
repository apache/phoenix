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

import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Objects;

/**
 * Represents a single remediation step in an index repair plan. Captures lifecycle status (PLANNED,
 * EXECUTED, SKIPPED) and execution audit details.
 */
public class RepairAction {
  public enum Status {
    PLANNED,
    EXECUTED,
    SKIPPED
  }

  private final String action;
  private final String description;
  private final Map<String, Object> details = new LinkedHashMap<>();
  private Status status = Status.PLANNED;
  private String skipReason;

  public RepairAction(String action, String description) {
    this.action = Objects.requireNonNull(action);
    this.description = Objects.requireNonNull(description);
  }

  public String getAction() {
    return action;
  }

  public String getDescription() {
    return description;
  }

  public Status getStatus() {
    return status;
  }

  public String getSkipReason() {
    return skipReason;
  }

  public Map<String, Object> getDetails() {
    return Collections.unmodifiableMap(details);
  }

  public void putDetail(String key, Object value) {
    details.put(key, value);
  }

  public void markExecuted() {
    status = Status.EXECUTED;
  }

  public void markSkipped(String reason) {
    status = Status.SKIPPED;
    skipReason = reason;
  }

  @Override
  public String toString() {
    return String.format("[%s] %s: %s%s", status, action, description,
      skipReason != null ? " (skipped: " + skipReason + ")" : "");
  }
}
