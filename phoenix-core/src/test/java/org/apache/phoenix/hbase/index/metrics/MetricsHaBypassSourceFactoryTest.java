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
package org.apache.phoenix.hbase.index.metrics;

import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertSame;

import org.junit.Test;

/**
 * Unit tests for {@link MetricsHaBypassSourceFactory}. Asserts the eager-init singleton contract so
 * a future refactor that swaps the field for a non-singleton (or a non-thread-safe) shape gets
 * caught here rather than in production JMX double-registration failures.
 */
public class MetricsHaBypassSourceFactoryTest {

  @Test
  public void testGetInstanceReturnsNonNull() {
    assertNotNull("getInstance() must return a non-null singleton",
      MetricsHaBypassSourceFactory.getInstance());
  }

  @Test
  public void testGetInstanceIsIdempotent() {
    MetricsHaBypassSource first = MetricsHaBypassSourceFactory.getInstance();
    MetricsHaBypassSource second = MetricsHaBypassSourceFactory.getInstance();
    assertSame("getInstance() must return the same singleton across calls", first, second);
  }
}
