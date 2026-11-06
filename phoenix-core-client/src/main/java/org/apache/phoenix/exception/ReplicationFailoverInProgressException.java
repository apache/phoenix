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
 * Thrown when a write's replication fails while a consistent failover is in progress, being
 * cancelled or just committed, so the write cannot fall back to STORE_AND_FORWARD. The write is
 * already committed locally, so its outcome on the peer is unknown: a divergence, which the region
 * server logs as a {@code PHOENIX_HA_DIVERGENCE} record for repair. The region server does not
 * abort. While the failover is on, later writes are refused before their commit with
 * {@link MutationBlockedIOException}, which makes no change.
 * <p>
 * Extends {@link DoNotRetryIOException} so HBase's RPC retry layers fail fast instead of retrying
 * while the failover settles.
 */
public class ReplicationFailoverInProgressException extends DoNotRetryIOException {
  private static final long serialVersionUID = 1L;

  /**
   * Used by the HBase client to rebuild the exception from a server response
   * ({@code ProtobufUtil.toException}); without it the client sees a plain, retriable IOException.
   * @param msg reason for the exception
   */
  public ReplicationFailoverInProgressException(String msg) {
    super(msg);
  }

  /**
   * @param msg   reason for the exception
   * @param cause the replication failure
   */
  public ReplicationFailoverInProgressException(String msg, Throwable cause) {
    super(msg, cause);
  }
}
