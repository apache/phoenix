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
package org.apache.phoenix.jdbc;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import org.apache.hadoop.hbase.client.RetriesExhaustedWithDetailsException;
import org.apache.hadoop.hbase.client.Row;
import org.apache.phoenix.exception.MutationBlockedIOException;
import org.junit.Test;

/**
 * Unit tests for {@link HighAvailabilityUtil#isMutationBlockedIOExceptionExistsInThrowable}. Walks
 * the four detection branches (null guard, direct match, RetriesExhaustedWithDetailsException
 * causes, recursive {@code getCause()}) since the helper is invoked from commit-path error handling
 * where regression of any branch would silently mis-attribute mutation-block rejections.
 */
public class HighAvailabilityUtilTest {

  @Test
  public void testNullThrowableReturnsFalse() {
    assertFalse("null Throwable must short-circuit to false",
      HighAvailabilityUtil.isMutationBlockedIOExceptionExistsInThrowable(null));
  }

  @Test
  public void testDirectMutationBlockedIOExceptionMatches() {
    Throwable e = new MutationBlockedIOException("blocked");
    assertTrue("direct MutationBlockedIOException must match",
      HighAvailabilityUtil.isMutationBlockedIOExceptionExistsInThrowable(e));
  }

  @Test
  public void testRetriesExhaustedCausesAreScanned() {
    MutationBlockedIOException blocked = new MutationBlockedIOException("blocked-in-batch");
    IOException unrelated = new IOException("unrelated");
    RetriesExhaustedWithDetailsException rewde =
      new RetriesExhaustedWithDetailsException(Arrays.<Throwable> asList(unrelated, blocked),
        Collections.<Row> emptyList(), Collections.<String> emptyList());
    assertTrue("RetriesExhaustedWithDetailsException causes must be scanned for MBIOE",
      HighAvailabilityUtil.isMutationBlockedIOExceptionExistsInThrowable(rewde));
  }

  @Test
  public void testRecursiveGetCauseFindsNestedMutationBlocked() {
    MutationBlockedIOException root = new MutationBlockedIOException("root-cause");
    Throwable mid = new RuntimeException("mid", root);
    Throwable outer = new RuntimeException("outer", mid);
    assertTrue("recursive getCause() walk must surface a deeply-nested MBIOE",
      HighAvailabilityUtil.isMutationBlockedIOExceptionExistsInThrowable(outer));
  }
}
