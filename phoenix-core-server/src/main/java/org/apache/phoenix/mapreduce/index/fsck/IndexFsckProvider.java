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

import java.util.List;

/**
 * The service provider interface (SPI) for index verification, introspection, consistency checks
 * (fsck), and repair. {@link IndexFsckProviders} selects the provider for each index type and
 * algorithm.
 */
public interface IndexFsckProvider {

  /**
   * Verifies the rows of the data table against the rows of the index table with a distributed job.
   * @param context the execution context
   * @return a report with the verification summary and the row findings
   * @throws Exception if the execution fails
   */
  Report verify(IndexFsckContext context) throws Exception;

  /**
   * Checks the auxiliary metadata and the structural invariants that are outside the index rows.
   * @param context the execution context
   * @return a report with the metadata findings and the structural findings
   * @throws Exception if the execution fails
   */
  Report fsck(IndexFsckContext context) throws Exception;

  /**
   * Finds the inconsistencies that a repair can fix and makes an idempotent repair plan. The
   * provider applies the plan only if the context confirms the repair.
   * @param context the execution context
   * @return a report with the repair plan and the audit log of the executed actions
   * @throws Exception if the execution fails
   */
  Report repair(IndexFsckContext context) throws Exception;

  /**
   * Runs a diagnostic subcommand of this provider. An inspection does not change data.
   * @param context        the execution context
   * @param inspectCommand the name of the inspection subcommand
   * @param inspectArgs    the arguments of the inspection subcommand
   * @return a report with the inspection results
   * @throws Exception if the subcommand is not known or the execution fails
   */
  Report inspect(IndexFsckContext context, String inspectCommand, List<String> inspectArgs)
    throws Exception;
}
