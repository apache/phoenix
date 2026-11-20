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
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.inOrder;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import org.apache.phoenix.util.EnvironmentEdge;
import org.apache.phoenix.util.EnvironmentEdgeManager;
import org.junit.After;
import org.junit.Test;
import org.mockito.InOrder;

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

  /**
   * Verifies that the name of an index that a view inherits resolves to the name of the parent
   * index. The inherited name can have prefixes for the view and its ancestor views.
   */
  @Test
  public void testInheritedIndexUsesIndexCentroids() {
    assertEquals("S.IDX", CentroidManager.getCentroidIndexName("S.IDX"));
    assertEquals("IDX", CentroidManager.getCentroidIndexName("V#IDX"));
    assertEquals("S.IDX", CentroidManager.getCentroidIndexName("S.VS.V2#VS.V1#S.IDX"));
  }

  /** Verifies that all stored generations of an untrained index are debris. */
  @Test
  public void testDebrisOfUntrainedIndex() {
    assertEquals(Arrays.asList(10L, 20L),
      list(CentroidManager.debrisGenerations(Arrays.asList(20L, 10L), null, null, null, 0)));
    assertEquals(Arrays.asList(10L, 20L),
      list(CentroidManager.debrisGenerations(Arrays.asList(20L, 10L), null, null, 1000L, 0)));
    assertTrue(
      CentroidManager.debrisGenerations(Collections.emptyList(), null, null, null, 0).isEmpty());
  }

  /**
   * Verifies that the active generation is never debris, and that each other generation is debris
   * without a migration in progress. A generation newer than the active one is also debris.
   */
  @Test
  public void testDebrisKeepsActiveGeneration() {
    assertTrue(
      CentroidManager.debrisGenerations(Arrays.asList(100L), 100L, null, null, 0).isEmpty());
    assertEquals(Arrays.asList(50L, 150L),
      list(CentroidManager.debrisGenerations(Arrays.asList(50L, 100L, 150L), 100L, null, null, 0)));
  }

  /** Verifies that the building generation of a migration in progress is not debris. */
  @Test
  public void testDebrisKeepsBuildingGeneration() {
    assertEquals(Arrays.asList(50L, 200L), list(CentroidManager
      .debrisGenerations(Arrays.asList(50L, 100L, 150L, 200L), 100L, 150L, null, 0)));
  }

  /**
   * Verifies that a generation older than the active one is not debris before the retire time of
   * the active generation. It becomes debris at the retire time.
   */
  @Test
  public void testDebrisKeepsOlderGenerationUntilRetireTime() {
    List<Long> stored = Arrays.asList(50L, 100L);
    assertTrue(CentroidManager.debrisGenerations(stored, 100L, null, 5000L, 4999).isEmpty());
    assertEquals(Arrays.asList(50L),
      list(CentroidManager.debrisGenerations(stored, 100L, null, 5000L, 5000)));
    assertEquals(Arrays.asList(50L),
      list(CentroidManager.debrisGenerations(stored, 100L, null, 5000L, 6000)));
    assertTrue(CentroidManager.isRetirementPending(50L, 100L, 5000L, 4999));
    assertFalse(CentroidManager.isRetirementPending(50L, 100L, 5000L, 5000));
    assertFalse(CentroidManager.isRetirementPending(50L, 100L, null, 0));
  }

  /**
   * Verifies that the retire time does not keep a generation newer than the active one, and that it
   * has no effect on an untrained index.
   */
  @Test
  public void testRetireTimeKeepsOnlyOlderGenerations() {
    assertEquals(Arrays.asList(150L), list(
      CentroidManager.debrisGenerations(Arrays.asList(50L, 100L, 150L), 100L, null, 5000L, 0)));
    assertFalse(CentroidManager.isRetirementPending(150L, 100L, 5000L, 0));
    assertFalse(CentroidManager.isRetirementPending(50L, null, 5000L, 0));
  }

  /**
   * Verifies that the delete of a generation commits in one transaction and restores the auto
   * commit mode of the connection.
   */
  @Test
  public void testDeleteGenerationCommitsOnce() throws Exception {
    Connection conn = mock(Connection.class);
    PreparedStatement ps = mock(PreparedStatement.class);
    when(conn.getAutoCommit()).thenReturn(true);
    when(conn.prepareStatement(anyString())).thenReturn(ps);

    CentroidManager.deleteGeneration(conn, "IDX_DEL", 7L);

    InOrder order = inOrder(conn, ps);
    order.verify(conn).setAutoCommit(false);
    order.verify(ps).executeUpdate();
    order.verify(conn).commit();
    order.verify(conn).setAutoCommit(true);
    verify(conn, never()).rollback();
  }

  /**
   * Verifies that a failed delete rolls back its mutations and restores the auto commit mode. A
   * later commit of the caller then does not apply a part of the delete.
   */
  @Test
  public void testDeleteGenerationRollsBackOnFailure() throws Exception {
    Connection conn = mock(Connection.class);
    PreparedStatement ps = mock(PreparedStatement.class);
    when(conn.getAutoCommit()).thenReturn(true);
    when(conn.prepareStatement(anyString())).thenReturn(ps);
    when(ps.executeUpdate()).thenThrow(new SQLException("injected"));

    try {
      CentroidManager.deleteGeneration(conn, "IDX_DEL", 7L);
      fail("The delete must fail");
    } catch (SQLException e) {
      assertEquals("injected", e.getMessage());
    }
    InOrder order = inOrder(conn);
    order.verify(conn).setAutoCommit(false);
    order.verify(conn).rollback();
    order.verify(conn).setAutoCommit(true);
    verify(conn, never()).commit();
  }

  private static List<Long> list(Collection<Long> generations) {
    return new ArrayList<>(generations);
  }
}
