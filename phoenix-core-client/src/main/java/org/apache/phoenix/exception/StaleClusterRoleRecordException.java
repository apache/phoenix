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
package org.apache.phoenix.exception;

import org.apache.hadoop.hbase.DoNotRetryIOException;

/**
 * Exception thrown when attempting to do operation on STANDBY Cluster of HA Connection, indicating
 * that the client has slate Cluster Role Record info for the HAGroup.
 * <p>
 * Extends {@link DoNotRetryIOException} so HBase's RPC retry layers
 * ({@code AsyncRequestFutureImpl}, {@code RpcRetryingCallerImpl}) fail-fast on the first hit
 * instead of consuming the full retry budget against the now-STANDBY cluster. Phoenix's outer
 * failover-aware retry remains responsible for routing to the new ACTIVE.
 */
public class StaleClusterRoleRecordException extends DoNotRetryIOException {
  private static final long serialVersionUID = 1L;

  /**
   * @param msg reason for the exception
   */
  public StaleClusterRoleRecordException(String msg) {
    super(msg);
  }

  /**
   * @param msg   reason for the exception
   * @param cause the underlying cause
   */
  public StaleClusterRoleRecordException(String msg, Throwable cause) {
    super(msg, cause);
  }

}
