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

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.fail;
import static org.mockito.Mockito.mock;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.Collections;
import java.util.List;
import org.apache.phoenix.schema.types.PVectorFloat;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class CentroidManagerTest {

  private Connection mockConnection;

  @Before
  public void setUp() {
    mockConnection = mock(Connection.class);
    CentroidManager.clearThreadLocalConnection();
    CentroidManager.setDefaultConnection(null);
  }

  @After
  public void tearDown() {
    CentroidManager.clearThreadLocalConnection();
    CentroidManager.setDefaultConnection(null);
  }

  @Test
  public void testConstructorAndGetter() {
    CentroidManager manager = new CentroidManager(mockConnection);
    assertSame(mockConnection, manager.getConnection());

    try {
      new CentroidManager(null);
      fail("Constructor with null connection should throw NPE");
    } catch (NullPointerException expected) {
    }
  }

  @Test
  public void testThreadLocalAndDefaultConnection() {
    assertNull(CentroidManager.getThreadLocalConnection());
    assertNull(CentroidManager.getDefaultConnection());

    CentroidManager.setDefaultConnection(mockConnection);
    assertSame(mockConnection, CentroidManager.getDefaultConnection());

    Connection threadConn = mock(Connection.class);
    CentroidManager.setThreadLocalConnection(threadConn);
    assertSame(threadConn, CentroidManager.getThreadLocalConnection());

    CentroidManager.clearThreadLocalConnection();
    assertNull(CentroidManager.getThreadLocalConnection());
    assertSame(mockConnection, CentroidManager.getDefaultConnection());
  }

  @Test
  public void testResolveConnectionThrowsWhenNoneConfigured() {
    try {
      CentroidManager.get().persistCentroids("test_idx", 1L, Collections.<byte[]> emptyList());
      fail("Should throw IllegalStateException when no connection is configured");
    } catch (IllegalStateException expected) {
    } catch (SQLException e) {
      fail("Unexpected SQLException: " + e.getMessage());
    }

    CentroidManager.setDefaultConnection(mockConnection);
    assertSame(mockConnection, CentroidManager.get().getConnection());
    assertSame(mockConnection, CentroidManager.get(mockConnection).getConnection());
  }

  @Test
  public void testValidationPersistCentroids() throws Exception {
    try {
      CentroidManager.persistCentroids(null, "test_idx", 1L, Collections.<byte[]> emptyList());
      fail("Should fail on null connection");
    } catch (IllegalArgumentException expected) {
    }

    try {
      CentroidManager.persistCentroids(mockConnection, null, 1L, Collections.<byte[]> emptyList());
      fail("Should fail on null indexName");
    } catch (IllegalArgumentException expected) {
    }

    try {
      CentroidManager.persistCentroids(mockConnection, "   ", 1L, Collections.<byte[]> emptyList());
      fail("Should fail on empty indexName");
    } catch (IllegalArgumentException expected) {
    }

    try {
      CentroidManager.persistCentroids(mockConnection, "test_idx", -1L,
        Collections.<byte[]> emptyList());
      fail("Should fail on negative generation");
    } catch (IllegalArgumentException expected) {
    }

    try {
      CentroidManager.persistCentroids(mockConnection, "test_idx", 1L, (List<byte[]>) null);
      fail("Should fail on null centroids list");
    } catch (IllegalArgumentException expected) {
    }

    try {
      CentroidManager.persistCentroids(mockConnection, "test_idx", 1L, (KMeansResult) null);
      fail("Should fail on null KMeansResult");
    } catch (IllegalArgumentException expected) {
    }

    try {
      CentroidManager.persistCentroidsFromFloatList(mockConnection, "test_idx", 1L,
        (List<float[]>) null);
      fail("Should fail on null float centroids list");
    } catch (IllegalArgumentException expected) {
    }
  }

  @Test
  public void testValidationLoadCentroids() throws Exception {
    try {
      CentroidManager.loadCentroids(null, "test_idx", 1L);
      fail("Should fail on null connection");
    } catch (IllegalArgumentException expected) {
    }

    try {
      CentroidManager.loadCentroids(mockConnection, null, 1L);
      fail("Should fail on null indexName");
    } catch (IllegalArgumentException expected) {
    }

    try {
      CentroidManager.loadCentroids(mockConnection, "", 1L);
      fail("Should fail on empty indexName");
    } catch (IllegalArgumentException expected) {
    }
  }

  @Test
  public void testValidationIncrementAndGetGeneration() throws Exception {
    try {
      CentroidManager.incrementGeneration(null, "test_idx");
      fail("Should fail on null connection");
    } catch (IllegalArgumentException expected) {
    }

    try {
      CentroidManager.incrementGeneration(mockConnection, null);
      fail("Should fail on null indexName");
    } catch (IllegalArgumentException expected) {
    }

    try {
      CentroidManager.getGeneration(null, "test_idx");
      fail("Should fail on null connection");
    } catch (IllegalArgumentException expected) {
    }

    try {
      CentroidManager.getGeneration(mockConnection, "  ");
      fail("Should fail on empty indexName");
    } catch (IllegalArgumentException expected) {
    }
  }

  @Test
  public void testValidationDeleteGeneration() throws Exception {
    try {
      CentroidManager.deleteGeneration(null, "test_idx", 1L);
      fail("Should fail on null connection");
    } catch (IllegalArgumentException expected) {
    }

    try {
      CentroidManager.deleteGeneration(mockConnection, null, 1L);
      fail("Should fail on null indexName");
    } catch (IllegalArgumentException expected) {
    }

    try {
      CentroidManager.deleteAllCentroids(null, "test_idx");
      fail("Should fail on null connection");
    } catch (IllegalArgumentException expected) {
    }

    try {
      CentroidManager.deleteAllCentroids(mockConnection, "");
      fail("Should fail on empty indexName");
    } catch (IllegalArgumentException expected) {
    }
  }

  @Test
  public void testVectorFloatSerializationHelper() {
    float[] v0 = new float[] { 1.5f, 2.5f, -3.5f, 0.0f };
    byte[] bytes = PVectorFloat.INSTANCE.toBytes(v0);
    float[] deserialized = (float[]) PVectorFloat.INSTANCE.toObject(bytes);
    assertArrayEquals(v0, deserialized, 1e-6f);
  }
}
