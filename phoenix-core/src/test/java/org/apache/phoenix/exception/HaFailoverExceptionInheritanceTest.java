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

import static org.junit.Assert.assertTrue;

import java.io.IOException;
import org.apache.hadoop.hbase.DoNotRetryIOException;
import org.junit.Test;

/**
 * Unit tests for the HA failover signaling exceptions thrown server-side when a mutation hits an HA
 * group whose cluster role is in transition. Both classes must extend {@link DoNotRetryIOException}
 * so HBase's RPC retry layers fail-fast on the first hit instead of consuming the per-call retry
 * budget; Phoenix's outer failover-aware retry handles routing to the new ACTIVE.
 */
public class HaFailoverExceptionInheritanceTest {

  @Test
  public void mutationBlockedExtendsDoNotRetryIOException() {
    MutationBlockedIOException e = new MutationBlockedIOException("blocked");
    assertTrue("MutationBlockedIOException must be a DoNotRetryIOException so HBase fails fast",
      e instanceof DoNotRetryIOException);
    // Transitive type preserved — existing catch (IOException) sites still match.
    assertTrue(e instanceof IOException);
  }

  @Test
  public void staleClusterRoleRecordExtendsDoNotRetryIOException() {
    StaleClusterRoleRecordException e = new StaleClusterRoleRecordException("stale");
    assertTrue(
      "StaleClusterRoleRecordException must be a DoNotRetryIOException so HBase fails fast",
      e instanceof DoNotRetryIOException);
    assertTrue(e instanceof IOException);
  }

  @Test
  public void staleClusterRoleRecordTwoArgConstructorPreserved() {
    Throwable cause = new RuntimeException("root");
    StaleClusterRoleRecordException e = new StaleClusterRoleRecordException("stale", cause);
    assertTrue(e instanceof DoNotRetryIOException);
  }
}
