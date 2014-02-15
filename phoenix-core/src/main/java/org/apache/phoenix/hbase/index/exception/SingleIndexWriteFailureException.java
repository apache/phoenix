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

import org.apache.hadoop.hbase.client.Mutation;

/**
 * Exception thrown if we cannot successfully write to an index table.
 */
@SuppressWarnings("serial")
public class SingleIndexWriteFailureException extends IndexWriteException {

  private String table;

  /**
   * Cannot reach the index, but not sure of the table or the mutations that caused the failure
   * @param msg more description of what happened
   * @param cause original cause
   */
  public SingleIndexWriteFailureException(String msg, Throwable cause) {
    super(msg, cause);
  }

  /**
   * Failed to write the passed mutations to an index table for some reason.
   * @param targetTableName index table to which we attempted to write
   * @param mutations mutations that were attempted
   * @param cause underlying reason for the failure
   */
  public SingleIndexWriteFailureException(String targetTableName, List<Mutation> mutations,
      Exception cause) {
    super("Failed to make index update:\n\t table: " + targetTableName + "\n\t edits: " + mutations
        + "\n\tcause: " + cause == null ? "UNKNOWN" : cause.getMessage(), cause);
    this.table = targetTableName;
  }

  /**
   * @return The table to which we failed to write the index updates. If unknown, returns
   *         <tt>null</tt>
   */
  public String getTableName() {
    return this.table;
  }
}