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
 * Exception thrown when an HA client mutation reaches a cluster whose role for the mutation's HA
 * group is ACTIVE_TO_STANDBY, or when a consistent failover stopped the HA group's replication log
 * and is still in progress (or the HA state cannot be read), or when creating the replication log
 * failed during a failover. Either way the write was refused before it was committed: no change was
 * made, unlike {@link ReplicationFailoverInProgressException}, which reports a divergence. As for
 * any exception from {@code preBatchMutate}, HBase reports it for every row of the region call,
 * including rows of an earlier mini-batch of the same call that did commit.
 * <p>
 * Extends {@link DoNotRetryIOException} so HBase's RPC retry layers
 * ({@code AsyncRequestFutureImpl}, {@code RpcRetryingCallerImpl}) fail-fast on the first hit
 * instead of absorbing the brief mutation-block window into the retry budget. Phoenix's outer
 * failover-aware retry remains responsible for routing to the new ACTIVE.
 */
public class MutationBlockedIOException extends DoNotRetryIOException {
  private static final long serialVersionUID = 1L;

  /**
   * @param msg reason for the exception
   */
  public MutationBlockedIOException(String msg) {
    super(msg);
  }
}
