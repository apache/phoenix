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

import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import org.apache.phoenix.coprocessorclient.MetaDataProtocol.MetaDataMutationResult;
import org.apache.phoenix.coprocessorclient.MetaDataProtocol.MutationCode;
import org.apache.phoenix.jdbc.PhoenixConnection;
import org.apache.phoenix.query.BaseConnectionlessQueryTest;
import org.apache.phoenix.query.ConnectionQueryServices;
import org.apache.phoenix.query.DelegateConnectionQueryServices;
import org.apache.phoenix.schema.PName;
import org.apache.phoenix.schema.PTable;
import org.apache.phoenix.schema.PTableKey;
import org.apache.phoenix.schema.TableNotFoundException;
import org.apache.phoenix.util.EnvironmentEdgeManager;
import org.apache.phoenix.util.PropertiesUtil;
import org.apache.phoenix.util.SchemaUtil;
import org.apache.phoenix.util.TestUtil;
import org.junit.Test;

/**
 * Tests the cache refresh of the vector index rebuild. Other threads update the same client
 * metadata cache at the same time.
 */
public class VectorIndexCacheRefreshTest extends BaseConnectionlessQueryTest {

  /**
   * Acts as the metadata server. It holds a fixed copy of each table. It returns no table when the
   * client timestamp equals the server timestamp, as the metadata endpoint does. On the first read
   * of the index, it also takes the cached data table, as a concurrent reader can.
   */
  private static class ServerQueryServices extends DelegateConnectionQueryServices {
    private final Map<String, PTable> serverTables = new HashMap<>();
    private final String indexName;
    private final String dataTableName;
    private boolean readerRan;
    private PTable readerSnapshot;

    ServerQueryServices(ConnectionQueryServices delegate, PTable dataTable, PTable index) {
      super(delegate);
      this.indexName = index.getName().getString();
      this.dataTableName = dataTable.getName().getString();
      serverTables.put(indexName, index);
      serverTables.put(dataTableName, dataTable);
    }

    @Override
    public MetaDataMutationResult getTable(PName tenantId, byte[] schemaBytes, byte[] tableBytes,
      long tableTimestamp, long clientTimestamp) throws SQLException {
      String name = SchemaUtil.getTableName(schemaBytes, tableBytes);
      if (name.equals(indexName) && !readerRan) {
        readerRan = true;
        try {
          readerSnapshot =
            getMetaDataCache().getTableRef(new PTableKey(null, dataTableName)).getTable();
        } catch (TableNotFoundException e) {
          readerSnapshot = null;
        }
      }
      PTable table = serverTables.get(name);
      if (table == null) {
        return new MetaDataMutationResult(MutationCode.TABLE_NOT_FOUND, 0, null);
      }
      return new MetaDataMutationResult(MutationCode.TABLE_ALREADY_EXISTS, table.getTimeStamp(),
        tableTimestamp == table.getTimeStamp() ? null : table);
    }
  }

  private static boolean listsIndex(PTable dataTable, PTable index) {
    for (PTable listed : dataTable.getIndexes()) {
      if (listed.getName().equals(index.getName())) {
        return true;
      }
    }
    return false;
  }

  private static PhoenixConnection connect() throws SQLException {
    Properties props = PropertiesUtil.deepCopy(TestUtil.TEST_PROPERTIES);
    return DriverManager.getConnection(getUrl(), props).unwrap(PhoenixConnection.class);
  }

  private static void createTableAndIndex(Connection conn, String tableName, String indexName)
    throws SQLException {
    conn.createStatement()
      .execute("CREATE TABLE " + tableName + " (ID INTEGER PRIMARY KEY, V VARCHAR, W VARCHAR)");
    conn.createStatement().execute("CREATE INDEX " + indexName + " ON " + tableName + " (V)");
  }

  /**
   * One reader takes the cached data table before the refresh. A second reader takes it while the
   * refresh reads the index again. Both readers write back their copies after the refresh, at the
   * server timestamp. The data table must still list the index.
   */
  @Test
  public void testRefreshSurvivesLateWriteBackOfDataTable() throws Exception {
    String tableName = generateUniqueName();
    String indexName = generateUniqueName();
    try (PhoenixConnection conn = connect()) {
      createTableAndIndex(conn, tableName, indexName);
      PTable dataTable = conn.getTable(tableName);
      PTable index = conn.getTable(indexName);
      assertTrue(listsIndex(dataTable, index));
      ServerQueryServices server =
        new ServerQueryServices(conn.getQueryServices(), dataTable, index);
      try (PhoenixConnection serverConn =
        new PhoenixConnection(conn, server, PropertiesUtil.deepCopy(TestUtil.TEST_PROPERTIES))) {
        PTable earlySnapshot =
          serverConn.getMetaDataCache().getTableRef(new PTableKey(null, tableName)).getTable();
        VectorIndexRebuilder.refresh(serverConn, index);
        assertTrue(server.readerRan);
        // The server sends no table to the readers, so they write back their own copies.
        serverConn.updateResolvedTimestamp(earlySnapshot,
          EnvironmentEdgeManager.currentTimeMillis());
        if (server.readerSnapshot != null) {
          serverConn.updateResolvedTimestamp(server.readerSnapshot,
            EnvironmentEdgeManager.currentTimeMillis());
        }
        PTable resolved = serverConn.getTableNoCache(tableName);
        assertTrue("The cached data table does not list the index", listsIndex(resolved, index));
      }
    }
  }

  /** A build against a data table that does not list the index fails with a SQLException. */
  @Test
  public void testBuildReportsMissingIndexOnDataTable() throws Exception {
    String tableName = generateUniqueName();
    String indexName = generateUniqueName();
    try (PhoenixConnection conn = connect()) {
      createTableAndIndex(conn, tableName, indexName);
      PTable dataTable = conn.getTable(tableName);
      PTable index = conn.getTable(indexName);
      // A real timestamp keeps a data table copy without the index in the cache.
      conn.removeTable(null, indexName, tableName, dataTable.getTimeStamp());
      try {
        VectorIndexRebuilder.build(conn, index, dataTable, 0, 1);
        fail("Expected a SQLException");
      } catch (SQLException e) {
        assertTrue(e.getMessage(), e.getMessage().contains("does not list index"));
        assertTrue(e.getCause() instanceof IllegalArgumentException);
      }
    }
  }
}
