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
 * Service Provider Interface (SPI) for index verification, introspection, consistency checking
 * (fsck), and repair.
 */
public interface IndexFsckProvider {

  /**
   * Runs distributed row-level verification between data and index tables.
   * @param context execution context
   * @return Report containing verification summary and row findings
   * @throws Exception on execution failure
   */
  Report verify(IndexFsckContext context) throws Exception;

  /**
   * Validates auxiliary metadata and structural invariants outside the index rows themselves.
   * @param context execution context
   * @return Report containing metadata and structural findings
   * @throws Exception on execution failure
   */
  Report fsck(IndexFsckContext context) throws Exception;

  /**
   * Evaluates repairable inconsistencies identified by verification and consistency checks,
   * constructs an idempotent remediation plan, and applies changes when invoked with confirm=true.
   * @param context execution context
   * @return Report containing repair plan and execution audit log
   * @throws Exception on execution failure
   */
  Report repair(IndexFsckContext context) throws Exception;

  /**
   * Executes non-mutating diagnostic subcommands defined by this provider.
   * @param context        execution context
   * @param inspectCommand inspection subcommand name
   * @param inspectArgs    inspection subcommand arguments
   * @return Report containing inspection results
   * @throws Exception on execution failure
   */
  Report inspect(IndexFsckContext context, String inspectCommand, List<String> inspectArgs)
    throws Exception;
}
