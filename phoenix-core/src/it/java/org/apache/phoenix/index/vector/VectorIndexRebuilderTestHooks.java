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
package org.apache.phoenix.index.vector;

import org.apache.phoenix.schema.PTable;

/**
 * Test hooks that run code in the middle of a rebuild migration and in the middle of a scorecard
 * reconciliation.
 */
public final class VectorIndexRebuilderTestHooks {

  /** Runs between the first and second build passes of a rebuild migration. */
  public interface Hook {
    void migrating(PTable index, long buildingGeneration) throws Exception;
  }

  private VectorIndexRebuilderTestHooks() {
  }

  public static void setMigrationHook(Hook hook) {
    VectorIndexRebuilder.setMigrationHookForTesting(hook == null ? null : hook::migrating);
  }

  /** Runs after reconciliation reads the scorecard and before it writes the corrections. */
  public interface ReconcileHook {
    void loaded(String indexName, long generation) throws Exception;
  }

  public static void setReconcileHook(ReconcileHook hook) {
    VectorIndexScorecard.setReconcileHookForTesting(hook == null ? null : hook::loaded);
  }
}
