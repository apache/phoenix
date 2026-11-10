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

import java.io.IOException;

/**
 * Thrown before a write is committed locally when its HA group's replication log group is
 * temporarily unusable: closed, or being replaced after a failover. Nothing was written. Retriable,
 * the way a client retries a region that is opening or moving: after a failover a fresh group
 * replaces this one; after a demotion, RegionServer stop or abort, the retry is rejected by the
 * non-active cluster or follows the region to its new server.
 */
public class ReplicationLogGroupUnavailableException extends IOException {
  private static final long serialVersionUID = 1L;

  /**
   * Used by the HBase client to rebuild the exception from a server response
   * ({@code ProtobufUtil.toException}).
   * @param msg reason for the exception
   */
  public ReplicationLogGroupUnavailableException(String msg) {
    super(msg);
  }

  /**
   * @param msg   reason for the exception
   * @param cause the underlying failure
   */
  public ReplicationLogGroupUnavailableException(String msg, Throwable cause) {
    super(msg, cause);
  }
}
