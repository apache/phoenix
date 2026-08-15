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

import static org.apache.phoenix.end2end.VarBinaryEncodedUpsertSelectTestUtil.SINGLE_PK_COLUMNS;
import static org.apache.phoenix.end2end.VarBinaryEncodedUpsertSelectTestUtil.assertRows;
import static org.apache.phoenix.end2end.VarBinaryEncodedUpsertSelectTestUtil.countHBaseRows;
import static org.apache.phoenix.end2end.VarBinaryEncodedUpsertSelectTestUtil.createTable;
import static org.apache.phoenix.end2end.VarBinaryEncodedUpsertSelectTestUtil.upsertEncodedRow;
import static org.apache.phoenix.hbase.index.IndexCDCConsumer.INDEX_CDC_CONSUMER_RETRY_PAUSE_MS;
import static org.apache.phoenix.hbase.index.IndexCDCConsumer.INDEX_CDC_CONSUMER_TIMESTAMP_BUFFER_MS;
import static org.apache.phoenix.hbase.index.IndexRegionObserver.PHOENIX_INDEX_CDC_MUTATION_SERIALIZE;
import static org.apache.phoenix.util.TestUtil.TEST_PROPERTIES;

import java.sql.Connection;
import java.sql.DriverManager;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import org.apache.phoenix.coprocessorclient.BaseScannerRegionObserverConstants;
import org.apache.phoenix.query.QueryServices;
import org.apache.phoenix.util.PropertiesUtil;
import org.apache.phoenix.util.ReadOnlyProps;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import org.apache.phoenix.thirdparty.com.google.common.collect.Maps;

@Category(NeedsOwnMiniClusterTest.class)
@RunWith(Parameterized.class)
public class VarBinaryEncodedIndexUpsertSelectIT extends ParallelStatsDisabledIT {

  private static final long EVENTUAL_CONSISTENCY_WAIT_MS = 12000;

  private final boolean coveredIndex;
  private final boolean eventual;

  public VarBinaryEncodedIndexUpsertSelectIT(boolean coveredIndex, boolean eventual) {
    this.coveredIndex = coveredIndex;
    this.eventual = eventual;
  }

  @BeforeClass
  public static synchronized void doSetup() throws Exception {
    Map<String, String> props = Maps.newHashMapWithExpectedSize(5);
    props.put(BaseScannerRegionObserverConstants.PHOENIX_MAX_LOOKBACK_AGE_CONF_KEY,
      Integer.toString(60 * 60));
    props.put(QueryServices.USE_STATS_FOR_PARALLELIZATION, Boolean.toString(false));
    props.put(INDEX_CDC_CONSUMER_TIMESTAMP_BUFFER_MS, Integer.toString(200));
    props.put(INDEX_CDC_CONSUMER_RETRY_PAUSE_MS, Integer.toString(5));
    props.put(PHOENIX_INDEX_CDC_MUTATION_SERIALIZE, Boolean.FALSE.toString());
    setUpTestDriver(new ReadOnlyProps(props.entrySet().iterator()));
  }

  @Parameterized.Parameters(name = "VarBinaryIndexUpsertSelectIT_coveredIndex={0}, eventual={1}")
  public static synchronized Collection<Object[]> data() {
    List<Object[]> params = new ArrayList<>();
    for (boolean coveredIndex : new boolean[] { false, true }) {
      for (boolean eventual : new boolean[] { false, true }) {
        params.add(new Object[] { coveredIndex, eventual });
      }
    }
    return params;
  }

  private void waitForEventualConsistency() throws InterruptedException {
    if (eventual) {
      Thread.sleep(EVENTUAL_CONSISTENCY_WAIT_MS);
    }
  }

  @Test
  public void testUpsertSelectVarBinaryEncodedWithIndex() throws Exception {
    Properties props = PropertiesUtil.deepCopy(TEST_PROPERTIES);
    final String sourceTable = generateUniqueName();
    final String targetTable = generateUniqueName();
    final String indexName = generateUniqueName();

    byte[] pk1 = new byte[] { 1, 0, 2 };
    byte[] col1 = new byte[] { 0, -1, 5 };
    byte[] pk2 = new byte[] { 0, 0, 3, 0 };
    byte[] col2 = new byte[] { 7, 0 };

    try (Connection conn = DriverManager.getConnection(getUrl(), props)) {
      createTable(conn, sourceTable, "");
      createTable(conn, targetTable, "");

      String consistencyClause = eventual ? " CONSISTENCY = EVENTUAL" : " CONSISTENCY = STRONG";
      if (this.coveredIndex) {
        conn.createStatement().execute(
          "CREATE INDEX " + indexName + " ON " + targetTable + " (COL1, COL2)" + consistencyClause);
      } else {
        conn.createStatement().execute("CREATE UNCOVERED INDEX " + indexName + " ON " + targetTable
          + " (COL1, COL2)" + consistencyClause);
      }

      upsertEncodedRow(conn, sourceTable, pk1, col1, "TEXT1", true);
      upsertEncodedRow(conn, sourceTable, pk2, col2, "TEXT2", true);
      upsertEncodedRow(conn, targetTable, pk1, col1, "TEXT1", true);
      upsertEncodedRow(conn, targetTable, pk2, col2, "TEXT2", true);
      conn.commit();
      waitForEventualConsistency();

      byte[][] row2 = new byte[][] { pk2, col2 };
      byte[][] row1 = new byte[][] { pk1, col1 };
      Assert.assertEquals("index rows", 2, countHBaseRows(conn, indexName));
      assertRows(conn, targetTable, SINGLE_PK_COLUMNS, row2, row1);

      conn.createStatement().execute("UPSERT INTO " + targetTable
        + " (PK1, COL1, COL2) SELECT PK1, COL1, COL2 FROM " + sourceTable);
      conn.commit();
      waitForEventualConsistency();

      Assert.assertEquals("index rows", 2, countHBaseRows(conn, indexName));
      assertRows(conn, targetTable, SINGLE_PK_COLUMNS, row2, row1);
    }
  }

}
