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
package org.apache.phoenix.end2end;

import static org.junit.Assert.assertEquals;

import com.google.protobuf.RpcCallback;
import com.google.protobuf.RpcController;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicLong;
import org.apache.phoenix.coprocessor.MetaDataEndpointImpl;
import org.apache.phoenix.coprocessor.ServerCachingEndpointImpl;
import org.apache.phoenix.coprocessor.generated.MetaDataProtos.GetTableRequest;
import org.apache.phoenix.coprocessor.generated.MetaDataProtos.MetaDataResponse;
import org.apache.phoenix.coprocessor.generated.ServerCachingProtos.AddServerCacheRequest;
import org.apache.phoenix.coprocessor.generated.ServerCachingProtos.AddServerCacheResponse;
import org.apache.phoenix.query.BaseTest;
import org.apache.phoenix.query.QueryServices;
import org.apache.phoenix.util.PropertiesUtil;
import org.apache.phoenix.util.ReadOnlyProps;
import org.apache.phoenix.util.TestUtil;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;

/**
 * Test GetTable rpc calls for the combination of UCF and useServerMetadata.
 */
@Category(NeedsOwnMiniClusterTest.class)
@RunWith(Parameterized.class)
public class UCFWithServerMetadataIT extends BaseTest {

  private final String updateCacheFrequency;
  private final boolean useServerMetadata;
  private final boolean singleRowUpdate;
  private static final AtomicLong getTableCallCount = new AtomicLong(0);
  private static final AtomicLong addServerCacheCallCount = new AtomicLong(0);

  public UCFWithServerMetadataIT(String updateCacheFrequency, boolean useServerMetadata,
    boolean singleRowUpdate) {
    this.updateCacheFrequency = updateCacheFrequency;
    this.useServerMetadata = useServerMetadata;
    this.singleRowUpdate = singleRowUpdate;
  }

  @Parameters(name = "UpdateCacheFrequency={0}, UseServerMetadata={1}, SingleRowUpdate={2}")
  public static Collection<Object[]> data() {
    return Arrays.asList(new Object[][] { { "60000", true, true }, { "60000", true, false },
      { "60000", false, true }, { "60000", false, false }, { "ALWAYS", true, true },
      { "ALWAYS", true, false }, { "ALWAYS", false, true }, { "ALWAYS", false, false } });
  }

  public static class TrackingMetaDataEndpointImpl extends MetaDataEndpointImpl {

    @Override
    public void getTable(RpcController controller, GetTableRequest request,
      RpcCallback<MetaDataResponse> done) {
      getTableCallCount.incrementAndGet();
      super.getTable(controller, request, done);
    }
  }

  public static class TrackingServerCachingEndpointImpl extends ServerCachingEndpointImpl {

    @Override
    public void addServerCache(RpcController controller, AddServerCacheRequest request,
      RpcCallback<AddServerCacheResponse> done) {
      addServerCacheCallCount.incrementAndGet();
      super.addServerCache(controller, request, done);
    }
  }

  @BeforeClass
  public static synchronized void doSetup() throws Exception {
    Map<String, String> props = new HashMap<>(1);
    props.put(QueryServices.TASK_HANDLING_INITIAL_DELAY_MS_ATTRIB, Long.toString(Long.MAX_VALUE));
    setUpTestDriver(new ReadOnlyProps(props));
  }

  @Before
  public void setUp() {
    getTableCallCount.set(0);
    addServerCacheCallCount.set(0);
  }

  @Test
  public void testUpdateCacheFrequency() throws Exception {
    String dataTableName = generateUniqueName();
    String coveredIndex1 = "CI1_" + generateUniqueName();
    String coveredIndex2 = "CI2_" + generateUniqueName();
    String uncoveredIndex1 = "UI1_" + generateUniqueName();
    String uncoveredIndex2 = "UI2_" + generateUniqueName();
    Properties props = PropertiesUtil.deepCopy(TestUtil.TEST_PROPERTIES);
    props.setProperty(QueryServices.INDEX_USE_SERVER_METADATA_ATTRIB,
      Boolean.toString(useServerMetadata));
    try (Connection conn = DriverManager.getConnection(getUrl(), props)) {
      String createTableDDL =
        String.format("CREATE TABLE %s (id INTEGER PRIMARY KEY, name VARCHAR(50), "
          + "age INTEGER, city VARCHAR(50), salary INTEGER, department VARCHAR(50)"
          + ") UPDATE_CACHE_FREQUENCY=%s", dataTableName, updateCacheFrequency);
      conn.createStatement().execute(createTableDDL);
      attachCustomCoprocessor(conn, dataTableName);
      conn.createStatement().execute(String
        .format("CREATE INDEX %s ON %s (name) INCLUDE (age, city)", coveredIndex1, dataTableName));
      conn.createStatement().execute(String.format(
        "CREATE INDEX %s ON %s (city) INCLUDE (salary, department)", coveredIndex2, dataTableName));
      conn.createStatement()
        .execute(String.format("CREATE INDEX %s ON %s (age)", uncoveredIndex1, dataTableName));
      conn.createStatement()
        .execute(String.format("CREATE INDEX %s ON %s (salary)", uncoveredIndex2, dataTableName));
      String upsertSQL = String.format(
        "UPSERT INTO %s (id, name, age, city, salary, department) VALUES (?, ?, ?, ?, ?, ?)",
        dataTableName);
      int totalRows = 52;
      int batchSize = 8;
      PreparedStatement stmt = conn.prepareStatement(upsertSQL);
      long startGetTableCalls = getTableCallCount.get();
      for (int i = 1; i <= totalRows; i++) {
        stmt.setInt(1, i);
        stmt.setString(2, "Name" + i);
        stmt.setInt(3, 20 + (i % 40));
        stmt.setString(4, "City" + (i % 10));
        stmt.setInt(5, 30000 + (i * 1000));
        stmt.setString(6, "Dept" + (i % 5));
        stmt.executeUpdate();
        if (singleRowUpdate) {
          conn.commit();
        } else if (i % batchSize == 0) {
          conn.commit();
        }
      }
      if (!singleRowUpdate) {
        conn.commit();
      }
      long actualGetTableCalls = getTableCallCount.get() - startGetTableCalls;
      int expectedCalls;
      String caseKey = updateCacheFrequency + "_" + useServerMetadata + "_" + singleRowUpdate;
      switch (caseKey) {
        case "60000_true_true":
        case "60000_true_false":
          expectedCalls = 1;
          break;
        case "60000_false_true":
        case "60000_false_false":
          expectedCalls = 0;
          break;
        case "ALWAYS_true_true":
          expectedCalls = totalRows * 2;
          break;
        case "ALWAYS_true_false":
          expectedCalls = (int) Math.ceil((double) totalRows / batchSize) * 2;
          break;
        case "ALWAYS_false_true":
          expectedCalls = totalRows;
          break;
        case "ALWAYS_false_false":
          expectedCalls = (int) Math.ceil((double) totalRows / batchSize);
          break;
        default:
          throw new IllegalArgumentException("Unexpected test case: " + caseKey);
      }
      assertEquals("Expected exact number of getTable() calls for case: " + caseKey, expectedCalls,
        actualGetTableCalls);
      long actualAddServerCacheCalls = addServerCacheCallCount.get();
      int expectedAddServerCacheCalls;
      switch (caseKey) {
        case "60000_false_false":
        case "ALWAYS_false_false":
          expectedAddServerCacheCalls = (int) Math.ceil((double) totalRows / batchSize);
          break;
        default:
          expectedAddServerCacheCalls = 0;
          break;
      }
      assertEquals("Expected exact number of addServerCache() calls for case: " + caseKey,
        expectedAddServerCacheCalls, actualAddServerCacheCalls);
    }
  }

  private void attachCustomCoprocessor(Connection conn, String dataTableName) throws Exception {
    TestUtil.removeCoprocessor(conn, "SYSTEM.CATALOG", MetaDataEndpointImpl.class);
    TestUtil.addCoprocessor(conn, "SYSTEM.CATALOG", TrackingMetaDataEndpointImpl.class);
    TestUtil.removeCoprocessor(conn, dataTableName, ServerCachingEndpointImpl.class);
    TestUtil.addCoprocessor(conn, dataTableName, TrackingServerCachingEndpointImpl.class);
  }

}
