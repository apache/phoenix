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
package org.apache.phoenix.index.vector;

import static org.apache.phoenix.index.vector.VectorIndexRebuilder.isDue;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import com.fasterxml.jackson.databind.JsonNode;
import org.apache.phoenix.util.JacksonUtil;
import org.junit.Test;

public class VectorIndexRebuilderTest {

  private static final long INTERVAL = 86400000L;
  private static final long NOW = 1_000_000_000_000L;

  @Test
  public void testNeverReconciledIsDue() {
    assertTrue(isDue(null, INTERVAL, NOW));
  }

  @Test
  public void testNotDueWithinInterval() {
    assertFalse(isDue(NOW, INTERVAL, NOW));
    assertFalse(isDue(NOW - INTERVAL + 1, INTERVAL, NOW));
  }

  @Test
  public void testDueAtInterval() {
    assertTrue(isDue(NOW - INTERVAL, INTERVAL, NOW));
    assertTrue(isDue(NOW - 10 * INTERVAL, INTERVAL, NOW));
  }

  @Test
  public void testZeroIntervalIsAlwaysDue() {
    assertTrue(isDue(NOW, 0, NOW));
  }

  /** Verifies that future timestamps (e.g. from clock skew) do not trigger reconciliation. */
  @Test
  public void testFutureUpdateIsNotDue() {
    assertFalse(isDue(NOW + INTERVAL, INTERVAL, NOW));
    assertFalse(isDue(NOW + 1, 0, NOW));
  }

  @Test
  public void testRebuildTaskData() throws Exception {
    JsonNode data = JacksonUtil.getObjectReader()
      .readTree(VectorIndexRebuilder.rebuildTaskData(false, "SKEW_RATIO_EXCEEDED: \"x\""));
    assertFalse(data.path("manual").asBoolean(true));
    assertEquals("SKEW_RATIO_EXCEEDED: 'x'", data.path("reason").asText());
  }
}
