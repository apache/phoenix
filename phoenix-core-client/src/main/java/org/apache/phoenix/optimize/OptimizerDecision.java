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

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Objects;

/** Records the optimizer's index selection rationale. */
public final class OptimizerDecision {
  private final String chosenIndex;
  private final String rule;
  private final String functionalMatch;
  private final List<RejectedIndexEntry> rejectedIndexes;

  public OptimizerDecision(String chosenIndex, String rule, String functionalMatch,
    List<RejectedIndexEntry> rejectedIndexes) {
    this.chosenIndex = chosenIndex;
    this.rule = rule;
    this.functionalMatch = functionalMatch;
    this.rejectedIndexes = rejectedIndexes == null
      ? Collections.emptyList()
      : Collections.unmodifiableList(new ArrayList<>(rejectedIndexes));
  }

  public String getChosenIndex() {
    return chosenIndex;
  }

  public String getRule() {
    return rule;
  }

  /**
   * The functional-index match disclosure of the form {@code "matches <expr>"}, or {@code null}.
   */
  public String getFunctionalMatch() {
    return functionalMatch;
  }

  /** Never null; unmodifiable; possibly empty. */
  public List<RejectedIndexEntry> getRejectedIndexes() {
    return rejectedIndexes;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (!(o instanceof OptimizerDecision)) {
      return false;
    }
    OptimizerDecision that = (OptimizerDecision) o;
    return Objects.equals(chosenIndex, that.chosenIndex) && Objects.equals(rule, that.rule)
      && Objects.equals(functionalMatch, that.functionalMatch)
      && rejectedIndexes.equals(that.rejectedIndexes);
  }

  @Override
  public int hashCode() {
    return Objects.hash(chosenIndex, rule, functionalMatch, rejectedIndexes);
  }

  @Override
  public String toString() {
    return "OptimizerDecision{chosenIndex=" + chosenIndex + ", rule=" + rule + ", functionalMatch="
      + functionalMatch + ", rejectedIndexes=" + rejectedIndexes + "}";
  }
}
