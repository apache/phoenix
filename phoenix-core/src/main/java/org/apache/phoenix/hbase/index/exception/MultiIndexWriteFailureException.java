/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.phoenix.hbase.index.exception;

import java.util.List;

import org.apache.phoenix.hbase.index.table.HTableInterfaceReference;

/**
 * Indicate a failure to write to multiple index tables.
 */
@SuppressWarnings("serial")
public class MultiIndexWriteFailureException extends IndexWriteException {

  private List<HTableInterfaceReference> failures;

  /**
   * @param failures the tables to which the index write did not succeed
   */
  public MultiIndexWriteFailureException(List<HTableInterfaceReference> failures) {
    super("Failed to write to multiple index tables");
    this.failures = failures;

  }

  public List<HTableInterfaceReference> getFailedTables() {
    return this.failures;
  }
}