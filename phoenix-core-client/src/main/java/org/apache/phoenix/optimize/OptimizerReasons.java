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
package org.apache.phoenix.optimize;

/**
 * Closed-set label vocabulary for {@link OptimizerDecision}. The {@code RULE_*} constants name the
 * rule the optimizer used to choose a plan. The {@code REASON_*} constants explain why a candidate
 * index was rejected.
 */
public final class OptimizerReasons {

  // RULE_* — chosen-plan rule labels.
  public static final String RULE_POINT_LOOKUP = "point lookup";
  public static final String RULE_ONLY_CANDIDATE = "only candidate";
  public static final String RULE_CDC_INDEX = "CDC index";
  public static final String RULE_COST_BASED = "cost-based";
  public static final String RULE_HINT = "hint";
  public static final String RULE_DATA_TABLE = "data table";
  public static final String RULE_MORE_BOUND_PK_COLUMNS = "more bound PK columns";
  public static final String RULE_ORDER_PRESERVING = "order-preserving";
  public static final String RULE_NON_LOCAL_PREFERRED = "non-local preferred";
  public static final String RULE_PARTIAL_INDEX_APPLICABLE = "partial index applicable";

  // REASON_* — rejected-index reason labels.
  public static final String REASON_NO_PK_PREFIX_BOUND = "no PK prefix bound";
  public static final String REASON_DOES_NOT_COVER_PROJECTION = "does not cover projection";
  public static final String REASON_PARTIAL_INDEX_PREDICATE_NOT_SATISFIED =
    "partial index predicate not satisfied";
  public static final String REASON_EXCLUDED_BY_NO_INDEX_HINT = "excluded by NO_INDEX hint";
  public static final String REASON_LOCAL_INDEX_LOSES_TO_GLOBAL_BY_RULE =
    "local index loses to global by rule";
  public static final String REASON_COST_BASED_LOSS = "cost-based loss";
  public static final String REASON_DEGENERATE_RANGE = "degenerate range";
  public static final String REASON_FULL_SCAN_WOULD_BE_REQUIRED = "full scan would be required";
  public static final String REASON_NOT_APPLICABLE_TO_JOIN = "not applicable to join";
  public static final String REASON_PATH_EXPRESSION_DOES_NOT_MATCH =
    "path expression does not match";

  /** Builds the functional index rule label of the form {@code "matches <expr>"}. */
  public static String matches(String expression) {
    return "matches " + expression;
  }

  private OptimizerReasons() {
  }
}
