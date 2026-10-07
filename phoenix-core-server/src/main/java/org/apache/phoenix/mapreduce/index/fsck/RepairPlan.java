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

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/** Ordered collection of remediation actions comprising an index repair plan. */
public class RepairPlan {
  private final boolean dryRun;
  private final List<RepairAction> actions = new ArrayList<>();

  public RepairPlan(boolean dryRun) {
    this.dryRun = dryRun;
  }

  public boolean isDryRun() {
    return dryRun;
  }

  public List<RepairAction> getActions() {
    return Collections.unmodifiableList(actions);
  }

  public RepairAction add(String action, String description) {
    RepairAction repairAction = new RepairAction(action, description);
    actions.add(repairAction);
    return repairAction;
  }

  /** Returns the first action matching the specified action name, or null if none found. */
  public RepairAction get(String action) {
    for (RepairAction a : actions) {
      if (a.getAction().equals(action)) {
        return a;
      }
    }
    return null;
  }
}
