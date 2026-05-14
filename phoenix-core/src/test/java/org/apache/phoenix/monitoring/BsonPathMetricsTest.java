/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.phoenix.monitoring;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import org.junit.Test;

public class BsonPathMetricsTest {

  @Test
  public void countersStartAtZeroAndIncrement() {
    long sparse0 = BsonPathMetrics.getSparseSkips();
    long hits0 = BsonPathMetrics.getRewriteHits();
    long misses0 = BsonPathMetrics.getRewriteMisses();

    BsonPathMetrics.incrementSparseSkips();
    BsonPathMetrics.incrementRewriteHits();
    BsonPathMetrics.incrementRewriteMisses();
    BsonPathMetrics.incrementRewriteMisses();

    assertEquals(sparse0 + 1, BsonPathMetrics.getSparseSkips());
    assertEquals(hits0 + 1, BsonPathMetrics.getRewriteHits());
    assertEquals(misses0 + 2, BsonPathMetrics.getRewriteMisses());
    assertTrue(BsonPathMetrics.getSparseSkips() >= 1);
  }
}
