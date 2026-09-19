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

import static org.junit.Assert.assertEquals;

import org.apache.phoenix.util.EnvironmentEdge;
import org.apache.phoenix.util.EnvironmentEdgeManager;
import org.junit.After;
import org.junit.Test;

public class CentroidManagerTest {

  @After
  public void tearDown() {
    EnvironmentEdgeManager.reset();
  }

  private static void setClock(final long now) {
    EnvironmentEdgeManager.injectEdge(new EnvironmentEdge() {
      @Override
      public long currentTime() {
        return now;
      }
    });
  }

  /**
   * Verifies that the first generation ID is the wall clock time. A recreated index then gets
   * generation IDs that differ from the IDs of the index it replaces.
   */
  @Test
  public void testFirstGenerationIsCurrentTime() {
    setClock(1_700_000_000_000L);
    assertEquals(1_700_000_000_000L, CentroidManager.nextGeneration(null));
  }

  /**
   * Verifies that the next generation ID is always larger than the current ID, also when the clock
   * is not ahead of the current ID.
   */
  @Test
  public void testNextGenerationStrictlyIncreases() {
    setClock(1_000L);
    assertEquals(5_000L, CentroidManager.nextGeneration(4_999L));
    assertEquals(1_001L, CentroidManager.nextGeneration(1_000L));
    setClock(2_000L);
    assertEquals(2_000L, CentroidManager.nextGeneration(1_000L));
  }
}
