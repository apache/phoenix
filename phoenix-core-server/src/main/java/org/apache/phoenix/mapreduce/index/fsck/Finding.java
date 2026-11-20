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
 * An inconsistency or a diagnostic observation that index verification or a consistency check
 * found. Each finding has a severity, a scope, a rule identifier that programs can match, a
 * message, and optional details.
 */
public class Finding {
  private final Severity severity;
  private final String scope;
  private final String rule;
  private final String message;
  private final Map<String, Object> details;

  public Finding(Severity severity, String scope, String rule, String message) {
    this(severity, scope, rule, message, Collections.emptyMap());
  }

  public Finding(Severity severity, String scope, String rule, String message,
    Map<String, Object> details) {
    this.severity = Objects.requireNonNull(severity);
    this.scope = Objects.requireNonNull(scope);
    this.rule = Objects.requireNonNull(rule);
    this.message = Objects.requireNonNull(message);
    this.details = Collections.unmodifiableMap(new LinkedHashMap<>(details));
  }

  public Severity getSeverity() {
    return severity;
  }

  public String getScope() {
    return scope;
  }

  public String getRule() {
    return rule;
  }

  public String getMessage() {
    return message;
  }

  public Map<String, Object> getDetails() {
    return details;
  }

  @Override
  public String toString() {
    return String.format("[%s] [%s] %s: %s%s", severity, scope, rule, message,
      details.isEmpty() ? "" : " " + details);
  }
}
