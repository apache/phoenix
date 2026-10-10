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

import static org.apache.phoenix.jdbc.PhoenixDatabaseMetaData.INDEX_NAME;
import static org.apache.phoenix.jdbc.PhoenixDatabaseMetaData.SYSTEM_VECTOR_CENTROID_NAME;
import static org.apache.phoenix.schema.LiteralTTLExpression.TTL_EXPRESSION_DEFINED_IN_TABLE_DESCRIPTOR;
import static org.apache.phoenix.schema.LiteralTTLExpression.TTL_EXPRESSION_NOT_DEFINED;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.KeepDeletedCells;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.ColumnFamilyDescriptor;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Mutation;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.RegionInfo;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.client.TableDescriptor;
import org.apache.hadoop.hbase.client.TableDescriptorBuilder;
import org.apache.hadoop.hbase.regionserver.BloomType;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.phoenix.coprocessorclient.MetaDataProtocol.MetaDataMutationResult;
import org.apache.phoenix.coprocessorclient.MetaDataProtocol.MutationCode;
import org.apache.phoenix.exception.SQLExceptionCode;
import org.apache.phoenix.jdbc.PhoenixConnection;
import org.apache.phoenix.mapreduce.index.IndexTool;
import org.apache.phoenix.query.QueryConstants;
import org.apache.phoenix.schema.PColumn;
import org.apache.phoenix.schema.PColumnFamily;
import org.apache.phoenix.schema.PIndexState;
import org.apache.phoenix.schema.PTable;
import org.apache.phoenix.schema.PTable.IndexType;
import org.apache.phoenix.schema.PTableType;
import org.apache.phoenix.schema.SaltingUtil;
import org.apache.phoenix.schema.VectorIndexType;
import org.apache.phoenix.schema.task.Task;
import org.apache.phoenix.util.EncodedColumnsUtil;
import org.apache.phoenix.util.IndexUtil;
import org.apache.phoenix.util.MetaDataUtil;
import org.apache.phoenix.util.PhoenixRuntime;
import org.apache.phoenix.util.SchemaUtil;
import org.apache.phoenix.util.TestUtil;
import org.junit.Test;
import org.junit.experimental.categories.Category;

/**
 * Integration tests for the DDL, the validation and the catalog metadata of HNSW vector indexes. An
 * HNSW index is a VECTOR_LOCAL index. Its physical table is the data table, which holds the
 * segments in the hidden column family V#.
 */
@Category(ParallelStatsDisabledTest.class)
public class HnswCatalogIT extends ParallelStatsDisabledIT {

  private static List<String> pkColumnNames(PTable table) {
    List<String> names = new ArrayList<>();
    for (PColumn col : table.getPKColumns()) {
      names.add(col.getName().getString());
    }
    return names;
  }

  /**
   * Tests that the primary key of an HNSW index has the primary key columns of the data table and
   * no centroid ID column. An HNSW index is not salted, also when its data table is salted. An IVF
   * index on the same table has a centroid ID column in its primary key.
   */
  @Test
  public void testIndexRowKeySchema() throws Exception {
    String table = generateUniqueName();
    String hnsw = generateUniqueName();
    String ivf = generateUniqueName();
    try (Connection conn = DriverManager.getConnection(getUrl());
      Statement stmt = conn.createStatement()) {
      stmt.execute("CREATE TABLE " + table + " (TENANT_ID VARCHAR NOT NULL, ID1 VARCHAR NOT NULL, "
        + "ID2 BIGINT NOT NULL, V VECTOR(FLOAT, 4), C1 VARCHAR, "
        + "CONSTRAINT PK PRIMARY KEY (TENANT_ID, ID1, ID2)) MULTI_TENANT=true, SALT_BUCKETS=2");
      stmt.execute("CREATE VECTOR INDEX " + hnsw + " ON " + table
        + " (V) WITH (algorithm='HNSW', metric='COSINE')");
      stmt.execute("CREATE VECTOR INDEX " + ivf + " ON " + table
        + " (V) INCLUDE (C1) WITH (algorithm='IVF', metric='L2', lists=2, sample_size=10)");

      List<String> expected = new ArrayList<>();
      for (String c : new String[] { "TENANT_ID", "ID1", "ID2" }) {
        expected.add(IndexUtil.getIndexColumnName(null, c));
      }
      assertEquals(expected, pkColumnNames(PhoenixRuntime.getTableNoCache(conn, hnsw)));
      assertTrue(pkColumnNames(PhoenixRuntime.getTableNoCache(conn, ivf))
        .contains(MetaDataUtil.VECTOR_CENTROID_ID_COLUMN_NAME));
    }
  }

  /**
   * Tests that a new HNSW index is in BUILDING state and that it starts no centroid training.
   */
  @Test
  public void testInitialBuildState() throws Exception {
    String table = generateUniqueName();
    String index = generateUniqueName();
    try (Connection conn = DriverManager.getConnection(getUrl());
      Statement stmt = conn.createStatement()) {
      stmt.execute(
        "CREATE TABLE " + table + " (ID VARCHAR NOT NULL PRIMARY KEY, V VECTOR(FLOAT, 4))");
      stmt.execute("UPSERT INTO " + table + " VALUES ('r1', ARRAY[1.0, 0.0, 0.0, 0.0])");
      stmt.execute("UPSERT INTO " + table + " VALUES ('r2', ARRAY[0.0, 1.0, 0.0, 0.0])");
      conn.commit();
      stmt.execute("CREATE VECTOR INDEX " + index + " ON " + table
        + " (V) WITH (algorithm='HNSW', metric='COSINE')");

      assertEquals(PIndexState.BUILDING,
        PhoenixRuntime.getTableNoCache(conn, index).getIndexState());
      try (PreparedStatement ps = conn.prepareStatement(
        "SELECT COUNT(*) FROM " + SYSTEM_VECTOR_CENTROID_NAME + " WHERE " + INDEX_NAME + " = ?")) {
        ps.setString(1, index);
        try (ResultSet rs = ps.executeQuery()) {
          assertTrue(rs.next());
          assertEquals(0L, rs.getLong(1));
        }
      }
    }
  }

  @Test
  public void testParameterValidation() throws Exception {
    String table = generateUniqueName();
    String doubleTable = generateUniqueName();
    // Each case is a WITH clause and the error code of its rejection. A clause that does not start
    // with an algorithm gets the HNSW algorithm and the COSINE metric.
    Object[][] cases = { { "M=2", SQLExceptionCode.INVALID_VECTOR_INDEX_PARAMS },
      { "M=100", SQLExceptionCode.INVALID_VECTOR_INDEX_PARAMS },
      { "M='sixteen'", SQLExceptionCode.INVALID_VECTOR_INDEX_PARAMS },
      { "ef_construction=8", SQLExceptionCode.INVALID_VECTOR_INDEX_PARAMS },
      { "ef_construction=1000", SQLExceptionCode.INVALID_VECTOR_INDEX_PARAMS },
      { "alpha=0.5", SQLExceptionCode.INVALID_VECTOR_INDEX_PARAMS },
      { "alpha=3.0", SQLExceptionCode.INVALID_VECTOR_INDEX_PARAMS },
      { "alpha='wide'", SQLExceptionCode.INVALID_VECTOR_INDEX_PARAMS },
      { "quantization='INT4'", SQLExceptionCode.UNSUPPORTED_VECTOR_QUANTIZATION_TYPE },
      { "quantization='PQ'", SQLExceptionCode.INVALID_VECTOR_INDEX_PARAMS },
      { "quantization='PQ', pq_segments=5",
        SQLExceptionCode.VECTOR_QUANTIZATION_DIMENSION_MISMATCH },
      { "quantization='PQ', pq_segments=300", SQLExceptionCode.INVALID_VECTOR_INDEX_PARAMS },
      { "quantization='SQ8', pq_segments=2", SQLExceptionCode.INVALID_VECTOR_INDEX_PARAMS },
      { "pq_segments=2", SQLExceptionCode.INVALID_VECTOR_INDEX_PARAMS },
      { "lists=16", SQLExceptionCode.VECTOR_ALGORITHM_PARAM_MISMATCH },
      { "lists='16x'", SQLExceptionCode.VECTOR_ALGORITHM_PARAM_MISMATCH },
      { "SALT_BUCKETS=4", SQLExceptionCode.CANNOT_SALT_LOCAL_INDEX },
      { "DURABILITY='SKIP_WAL'", SQLExceptionCode.VIEW_WITH_PROPERTIES },
      { "algorithm='HNSW', metric='INNER_PRODUCT'",
        SQLExceptionCode.UNSUPPORTED_VECTOR_DISTANCE_METRIC },
      { "CONSISTENCY=EVENTUAL", SQLExceptionCode.INVALID_VECTOR_INDEX_PARAMS },
      { "algorithm='IVF', metric='L2', lists=2, sample_size=10, M=16",
        SQLExceptionCode.VECTOR_ALGORITHM_PARAM_MISMATCH },
      { "algorithm='DISKANN', metric='L2'", SQLExceptionCode.UNSUPPORTED_VECTOR_INDEX_ALGORITHM },
      { "algorithm='HNSW', metric='MANHATTAN'",
        SQLExceptionCode.UNSUPPORTED_VECTOR_DISTANCE_METRIC } };
    try (Connection conn = DriverManager.getConnection(getUrl());
      Statement stmt = conn.createStatement()) {
      stmt.execute("CREATE TABLE " + table
        + " (ID VARCHAR NOT NULL PRIMARY KEY, V VECTOR(FLOAT, 12), C1 VARCHAR)");
      stmt.execute(
        "CREATE TABLE " + doubleTable + " (ID VARCHAR NOT NULL PRIMARY KEY, V VECTOR(DOUBLE, 12))");
      for (Object[] c : cases) {
        String with = (String) c[0];
        if (!with.startsWith("algorithm")) {
          with = "algorithm='HNSW', metric='COSINE', " + with;
        }
        assertRejected(stmt, "CREATE VECTOR INDEX " + generateUniqueName() + " ON " + table
          + " (V) WITH (" + with + ")", (SQLExceptionCode) c[1]);
      }
      assertRejected(stmt,
        "CREATE VECTOR INDEX " + generateUniqueName() + " ON " + table
          + " (V) INCLUDE (C1) WITH (algorithm='HNSW', metric='COSINE')",
        SQLExceptionCode.HNSW_INCLUDE_NOT_SUPPORTED);
      // Every codec converts DOUBLE vectors to float32, so SQ8 accepts a DOUBLE vector column
      stmt.execute("CREATE VECTOR INDEX " + generateUniqueName() + " ON " + doubleTable
        + " (V) WITH (algorithm='HNSW', metric='COSINE', quantization='SQ8')");
      String rowTimestamp = generateUniqueName();
      stmt.execute("CREATE TABLE " + rowTimestamp + " (ID VARCHAR NOT NULL, TS DATE NOT NULL,"
        + " V VECTOR(FLOAT, 4) CONSTRAINT PK PRIMARY KEY (ID, TS ROW_TIMESTAMP))"
        + " IMMUTABLE_ROWS=true");
      assertRejected(stmt,
        "CREATE VECTOR INDEX " + generateUniqueName() + " ON " + rowTimestamp
          + " (V) WITH (algorithm='HNSW', metric='COSINE')",
        SQLExceptionCode.HNSW_ROW_TIMESTAMP_NOT_SUPPORTED);
      stmt.execute("CREATE VECTOR INDEX " + generateUniqueName() + " ON " + rowTimestamp
        + " (V) WITH (algorithm='IVF', metric='L2', lists=2, sample_size=10)");
    }
  }

  /** Tests that ALTER INDEX rejects eventual consistency for HNSW and IVF indexes. */
  @Test
  public void testAlterToEventualConsistencyRejected() throws Exception {
    String table = generateUniqueName();
    String hnsw = generateUniqueName();
    String ivf = generateUniqueName();
    try (Connection conn = DriverManager.getConnection(getUrl());
      Statement stmt = conn.createStatement()) {
      stmt.execute(
        "CREATE TABLE " + table + " (ID VARCHAR NOT NULL PRIMARY KEY, V VECTOR(FLOAT, 4))");
      stmt.execute("CREATE VECTOR INDEX " + hnsw + " ON " + table
        + " (V) WITH (algorithm='HNSW', metric='COSINE')");
      stmt.execute("CREATE VECTOR INDEX " + ivf + " ON " + table
        + " (V) WITH (algorithm='IVF', metric='L2', lists=2, sample_size=10)");
      assertRejected(stmt, "ALTER INDEX " + hnsw + " ON " + table + " CONSISTENCY=EVENTUAL",
        SQLExceptionCode.INVALID_VECTOR_INDEX_PARAMS);
      assertRejected(stmt, "ALTER INDEX " + ivf + " ON " + table + " CONSISTENCY=EVENTUAL",
        SQLExceptionCode.INVALID_VECTOR_INDEX_PARAMS);
    }
  }

  private static void assertRejected(Statement stmt, String ddl, SQLExceptionCode expected) {
    try {
      stmt.execute(ddl);
      fail("Expected " + expected + " for: " + ddl);
    } catch (SQLException e) {
      assertEquals(ddl, expected.getErrorCode(), e.getErrorCode());
    }
  }

  /**
   * Tests that SYSTEM.CATALOG stores the explicit and the default HNSW parameters. It keeps the
   * algorithm, the metric and the quantization type in upper case.
   */
  @Test
  public void testCatalogPersistence() throws Exception {
    String table = generateUniqueName();
    String explicit = generateUniqueName();
    String defaulted = generateUniqueName();
    try (Connection conn = DriverManager.getConnection(getUrl());
      Statement stmt = conn.createStatement()) {
      stmt.execute(
        "CREATE TABLE " + table + " (ID VARCHAR NOT NULL PRIMARY KEY, V VECTOR(FLOAT, 8))");
      stmt.execute("CREATE VECTOR INDEX " + explicit + " ON " + table
        + " (V) WITH (algorithm='hnsw', metric='cosine', M=32, ef_construction=64, alpha=1.5, "
        + "quantization='pq', pq_segments=2)");
      stmt.execute("CREATE VECTOR INDEX " + defaulted + " ON " + table
        + " (V) WITH (algorithm='HNSW', metric='L2')");

      PTable explicitIndex = PhoenixRuntime.getTableNoCache(conn, explicit);
      assertEquals(IndexType.VECTOR_LOCAL, explicitIndex.getIndexType());
      assertEquals(VectorIndexType.HNSW, explicitIndex.getVectorIndexType());
      assertNull(explicitIndex.getVectorCentroidGeneration());
      PTable.VectorIndex vi = explicitIndex.getVectorIndex();
      assertEquals("HNSW", vi.getAlgorithm());
      assertEquals("COSINE", vi.getDistanceMetric());
      assertEquals(Integer.valueOf(8), vi.getDimension());
      assertEquals(Integer.valueOf(32), vi.getHnswM());
      assertEquals(Integer.valueOf(64), vi.getHnswEfConstruction());
      assertEquals(Double.valueOf(1.5), vi.getHnswAlpha());
      assertEquals("PQ", vi.getQuantizationType());
      assertEquals(Integer.valueOf(2), vi.getPqSegments());
      assertNull(vi.getIvfLists());

      vi = PhoenixRuntime.getTableNoCache(conn, defaulted).getVectorIndex();
      assertEquals("L2", vi.getDistanceMetric());
      assertEquals(Integer.valueOf(16), vi.getHnswM());
      assertEquals(Integer.valueOf(100), vi.getHnswEfConstruction());
      assertEquals(Double.valueOf(1.2), vi.getHnswAlpha());
      assertEquals("NONE", vi.getQuantizationType());
      assertNull(vi.getPqSegments());
    }
  }

  /**
   * Tests the hidden family and the lifecycle of an HNSW index. The first HNSW index adds the
   * family V# to the data table with fixed settings, and creates no HBase table. It makes no other
   * change to the descriptor. Splits do not copy segments to the upper daughter, so V# needs no
   * split policy and no reader generator. The data table keeps its coprocessors and its priority. A
   * second HNSW index leaves the descriptor unchanged. V# keeps its fixed TTL, KEEP_DELETED_CELLS
   * and REPLICATION_SCOPE when CREATE TABLE and ALTER TABLE set other values. The index does not
   * inherit the Phoenix TTL of the data table. An IVF index on the same table has its own index
   * table, which follows the data table. REBUILD ALL ASYNC enqueues no rebuild task, and IndexTool
   * refuses -deleteall. DROP COLUMN of the vector column drops the index, and V# stays. The family
   * name V# is reserved.
   */
  @Test
  public void testHiddenFamily() throws Exception {
    String table = generateUniqueName();
    String hnsw = generateUniqueName();
    String hnsw2 = generateUniqueName();
    String ivf = generateUniqueName();
    try (Connection conn = DriverManager.getConnection(getUrl());
      Statement stmt = conn.createStatement();
      Admin admin = conn.unwrap(PhoenixConnection.class).getQueryServices().getAdmin()) {
      stmt.execute("CREATE TABLE " + table
        + " (ID VARCHAR NOT NULL PRIMARY KEY, V VECTOR(FLOAT, 4), C VARCHAR)"
        + " TTL=86400, KEEP_DELETED_CELLS=true, REPLICATION_SCOPE=1 SPLIT ON ('g', 'p')");
      TableName dataTable = TableName.valueOf(table);
      TableDescriptor before = admin.getDescriptor(dataTable);
      assertNull(before.getColumnFamily(QueryConstants.VECTOR_LOCAL_INDEX_COLUMN_FAMILY_BYTES));
      assertFalse(
        before.hasCoprocessor(QueryConstants.INDEX_HALF_STORE_FILE_READER_GENERATOR_CLASSNAME));

      stmt.execute("CREATE VECTOR INDEX " + hnsw + " ON " + table
        + " (V) WITH (algorithm='HNSW', metric='COSINE')");
      assertFalse(admin.tableExists(TableName.valueOf(hnsw)));
      TableDescriptor desc = admin.getDescriptor(dataTable);
      assertHiddenFamily(desc);
      // The local index DDL flow also writes the data table name to the descriptor
      assertEquals(TableDescriptorBuilder.newBuilder(before)
        .setColumnFamily(
          desc.getColumnFamily(QueryConstants.VECTOR_LOCAL_INDEX_COLUMN_FAMILY_BYTES))
        .setValue(MetaDataUtil.DATA_TABLE_NAME_PROP_NAME, table).build(), desc);
      assertTrue(desc.hasCoprocessor(QueryConstants.INDEX_REGION_OBSERVER_CLASSNAME));
      assertFalse(desc.hasCoprocessor(QueryConstants.GLOBAL_INDEX_CHECKER_CLASSNAME));
      assertEquals(Arrays.asList("", "g", "p"), startKeys(admin, table));

      PTable index = PhoenixRuntime.getTableNoCache(conn, hnsw);
      assertEquals(IndexType.VECTOR_LOCAL, index.getIndexType());
      assertEquals(table, index.getPhysicalName().getString());
      assertEquals(QueryConstants.VECTOR_LOCAL_INDEX_COLUMN_FAMILY,
        index.getDefaultFamilyName().getString());
      assertNull(index.getViewIndexId());
      assertFalse(pkColumnNames(index).contains(MetaDataUtil.getViewIndexIdColumnName()));
      for (PColumnFamily family : PhoenixRuntime.getTableNoCache(conn, table).getColumnFamilies()) {
        assertFalse(
          QueryConstants.VECTOR_LOCAL_INDEX_COLUMN_FAMILY.equals(family.getName().getString()));
      }

      stmt.execute("CREATE VECTOR INDEX " + hnsw2 + " ON " + table
        + " (V) WITH (algorithm='HNSW', metric='L2', quantization='SQ8')");
      assertEquals(desc, admin.getDescriptor(dataTable));

      stmt.execute("CREATE VECTOR INDEX " + ivf + " ON " + table
        + " (V) WITH (algorithm='IVF', metric='L2', lists=2, sample_size=10)");
      assertEquals(IndexType.VECTOR_GLOBAL,
        PhoenixRuntime.getTableNoCache(conn, ivf).getIndexType());
      assertTrue(admin.tableExists(TableName.valueOf(ivf)));
      assertColumnFamily(admin, ivf, 86400, KeepDeletedCells.TRUE, 1);
      for (ColumnFamilyDescriptor cf : admin.getDescriptor(TableName.valueOf(ivf))
        .getColumnFamilies()) {
        assertFalse(cf.isMobEnabled());
      }
      assertEquals(TTL_EXPRESSION_NOT_DEFINED,
        PhoenixRuntime.getTableNoCache(conn, hnsw).getTTLExpression());
      // A literal table TTL is in the column family descriptor, and the IVF index follows it
      assertEquals(TTL_EXPRESSION_DEFINED_IN_TABLE_DESCRIPTOR,
        PhoenixRuntime.getTableNoCache(conn, ivf).getTTLExpression());

      stmt.execute(
        "ALTER TABLE " + table + " SET TTL=7200, KEEP_DELETED_CELLS=true, REPLICATION_SCOPE=1");
      desc = admin.getDescriptor(dataTable);
      assertHiddenFamily(desc);
      ColumnFamilyDescriptor data =
        desc.getColumnFamily(QueryConstants.DEFAULT_COLUMN_FAMILY_BYTES);
      assertEquals(7200, data.getTimeToLive());
      assertEquals(KeepDeletedCells.TRUE, data.getKeepDeletedCells());
      assertEquals(1, data.getScope());
      assertColumnFamily(admin, ivf, 7200, KeepDeletedCells.TRUE, 1);
      assertEquals(TTL_EXPRESSION_NOT_DEFINED,
        PhoenixRuntime.getTableNoCache(conn, hnsw).getTTLExpression());

      stmt.execute("ALTER INDEX " + hnsw + " ON " + table + " REBUILD ALL ASYNC");
      assertEquals(PIndexState.BUILDING,
        PhoenixRuntime.getTableNoCache(conn, hnsw).getIndexState());
      assertTrue(
        Task.queryTaskTable(conn, null, null, table, PTable.TaskType.INDEX_REBUILD, null, hnsw)
          .isEmpty());
      assertEquals(-1, (int) IndexTool.run(new Configuration(getUtility().getConfiguration()), null,
        table, hnsw, false, null, false, true, true).getKey());

      stmt.execute("ALTER TABLE " + table + " DROP COLUMN V");
      assertTrue(PhoenixRuntime.getTableNoCache(conn, table).getIndexes().isEmpty());
      assertTrue(admin.isTableEnabled(dataTable));
      // The hidden family stays after its last index is dropped, as local index families do
      assertHiddenFamily(admin.getDescriptor(dataTable));
      stmt.execute("UPSERT INTO " + table + " (ID, C) VALUES ('a', 'c')");
      conn.commit();
      try (ResultSet rs = stmt.executeQuery("SELECT C FROM " + table)) {
        assertTrue(rs.next());
        assertEquals("c", rs.getString(1));
      }

      // The hidden family name is reserved
      assertRejected(stmt,
        "CREATE TABLE " + generateUniqueName()
          + " (ID VARCHAR NOT NULL PRIMARY KEY, \"V#\".C VARCHAR)",
        SQLExceptionCode.UNALLOWED_COLUMN_FAMILY);
      assertRejected(stmt,
        "CREATE TABLE " + generateUniqueName()
          + " (ID VARCHAR NOT NULL PRIMARY KEY, C VARCHAR) DEFAULT_COLUMN_FAMILY='V#'",
        SQLExceptionCode.UNALLOWED_COLUMN_FAMILY);
      assertRejected(stmt, "ALTER TABLE " + table + " ADD \"V#\".X INTEGER",
        SQLExceptionCode.UNALLOWED_COLUMN_FAMILY);
    }
  }

  /**
   * Tests that the hidden family never gives its fixed settings to a new index. The data table has
   * no default family, and V# sorts before its only data family VEC. A new index then takes TTL,
   * KEEP_DELETED_CELLS and REPLICATION_SCOPE from VEC.
   */
  @Test
  public void testHiddenFamilyIsNotSyncSource() throws Exception {
    String table = generateUniqueName();
    String global = generateUniqueName();
    try (Connection conn = DriverManager.getConnection(getUrl());
      Statement stmt = conn.createStatement();
      Admin admin = conn.unwrap(PhoenixConnection.class).getQueryServices().getAdmin()) {
      stmt.execute("CREATE TABLE " + table
        + " (ID VARCHAR NOT NULL PRIMARY KEY, VEC.V VECTOR(FLOAT, 4), VEC.C VARCHAR)"
        + " TTL=86400, KEEP_DELETED_CELLS=true, REPLICATION_SCOPE=1");
      stmt.execute("CREATE VECTOR INDEX " + generateUniqueName() + " ON " + table
        + " (VEC.V) WITH (algorithm='HNSW', metric='L2')");
      TableDescriptor desc = admin.getDescriptor(TableName.valueOf(table));
      assertHiddenFamily(desc);
      assertEquals(QueryConstants.VECTOR_LOCAL_INDEX_COLUMN_FAMILY,
        desc.getColumnFamilies()[0].getNameAsString());
      stmt.execute("CREATE INDEX " + global + " ON " + table + " (VEC.C)");
      assertColumnFamily(admin, global, 86400, KeepDeletedCells.TRUE, 1);
    }
  }

  /**
   * Tests a literal table TTL when V# is the first family of the table. The data family VEC holds
   * the TTL of the table, and V# has no TTL. Queries must not return a row whose cells are older
   * than the TTL.
   */
  @Test
  public void testLiteralTtlWithHiddenFamilyFirst() throws Exception {
    String table = generateUniqueName();
    try (Connection conn = DriverManager.getConnection(getUrl());
      Statement stmt = conn.createStatement();
      Admin admin = conn.unwrap(PhoenixConnection.class).getQueryServices().getAdmin()) {
      stmt.execute("CREATE TABLE " + table
        + " (ID VARCHAR NOT NULL PRIMARY KEY, VEC.V VECTOR(FLOAT, 4), VEC.C VARCHAR) TTL=86400");
      stmt.execute("CREATE VECTOR INDEX " + generateUniqueName() + " ON " + table
        + " (VEC.V) WITH (algorithm='HNSW', metric='L2')");
      assertEquals(QueryConstants.VECTOR_LOCAL_INDEX_COLUMN_FAMILY,
        admin.getDescriptor(TableName.valueOf(table)).getColumnFamilies()[0].getNameAsString());
      stmt.execute("UPSERT INTO " + table + " VALUES ('live', ARRAY[1.0, 0.0, 0.0, 0.0], 'c')");
      conn.commit();
      // Write a copy of the row with cells that are older than the TTL
      long expired = System.currentTimeMillis() - 2 * 86400 * 1000L;
      try (Table htable =
        conn.unwrap(PhoenixConnection.class).getQueryServices().getTable(Bytes.toBytes(table))) {
        Put put = new Put(Bytes.toBytes("expired"));
        for (Cell cell : htable.get(new Get(Bytes.toBytes("live"))).rawCells()) {
          put.addColumn(CellUtil.cloneFamily(cell), CellUtil.cloneQualifier(cell), expired,
            CellUtil.cloneValue(cell));
        }
        htable.put(put);
      }
      try (ResultSet rs = stmt.executeQuery("SELECT ID FROM " + table)) {
        assertTrue(rs.next());
        assertEquals("live", rs.getString(1));
        assertFalse(rs.next());
      }
    }
  }

  /**
   * Tests that the automatic repair of a local index reads only the data families. A local index
   * file whose first row does not start with the region prefix makes the local index store of the
   * first region inconsistent. Its major compaction then rebuilds the local index from the data
   * rows. The anchor row of the region has only a V# cell, and it gets no local index row.
   */
  @Test
  public void testLocalIndexRepairSkipsHiddenFamily() throws Exception {
    String table = generateUniqueName();
    String local = generateUniqueName();
    try (Connection conn = DriverManager.getConnection(getUrl());
      Statement stmt = conn.createStatement()) {
      stmt.execute("CREATE TABLE " + table
        + " (ID VARCHAR NOT NULL PRIMARY KEY, V VECTOR(FLOAT, 4), C VARCHAR) SPLIT ON ('m')");
      stmt.execute("CREATE LOCAL INDEX " + local + " ON " + table + " (C)");
      stmt.execute("CREATE VECTOR INDEX " + generateUniqueName() + " ON " + table
        + " (V) WITH (algorithm='HNSW', metric='COSINE')");
      upsertRows(conn, table);
      TableName dataTable = TableName.valueOf(table);
      byte[] localFamily = QueryConstants.DEFAULT_LOCAL_INDEX_COLUMN_FAMILY_BYTES;
      try (Table htable =
        conn.unwrap(PhoenixConnection.class).getQueryServices().getTable(Bytes.toBytes(table))) {
        htable.put(
          new Put(new byte[] { 0 }).addColumn(QueryConstants.VECTOR_LOCAL_INDEX_COLUMN_FAMILY_BYTES,
            Bytes.toBytes("q"), Bytes.toBytes("x")));
        TestUtil.flush(getUtility(), dataTable);
        htable.put(new Put(Bytes.toBytes("b")).addColumn(localFamily, Bytes.toBytes("q"),
          Bytes.toBytes("x")));
        TestUtil.flush(getUtility(), dataTable);
        TestUtil.majorCompact(getUtility(), dataTable);
        // The repair removed the stray row, and the local index has one row for each data row
        List<String> rows = new ArrayList<>();
        try (ResultScanner scanner = htable.getScanner(localFamily)) {
          for (Result result : scanner) {
            rows.add(Bytes.toStringBinary(result.getRow()));
          }
        }
        assertFalse(rows.toString(), rows.contains("b"));
        assertEquals(rows.toString(), 3, rows.size());
      }
      try (ResultSet rs = stmt.executeQuery("SELECT /*+ INDEX(" + table + " " + local + ") */ ID"
        + " FROM " + table + " WHERE C = 'c0'")) {
        assertTrue(rs.next());
        assertEquals("a", rs.getString(1));
        assertFalse(rs.next());
      }
    }
  }

  /**
   * Tests that a table with V# rejects a transform, as a table with a local index does. An HNSW
   * index also rejects a transform, as a local index does. A transform writes the rows to a new
   * physical table, but the segments stay in the old one.
   */
  @Test
  public void testTransformRejected() throws Exception {
    String table = generateUniqueName();
    String hnsw = generateUniqueName();
    try (Connection conn = DriverManager.getConnection(getUrl());
      Statement stmt = conn.createStatement()) {
      stmt.execute("CREATE TABLE " + table
        + " (ID VARCHAR NOT NULL PRIMARY KEY, V VECTOR(FLOAT, 4), C VARCHAR) COLUMN_ENCODED_BYTES=0");
      stmt.execute("CREATE VECTOR INDEX " + hnsw + " ON " + table
        + " (V) WITH (algorithm='HNSW', metric='COSINE')");
      String transform =
        " IMMUTABLE_STORAGE_SCHEME=SINGLE_CELL_ARRAY_WITH_OFFSETS, COLUMN_ENCODED_BYTES=2";
      assertRejected(stmt, "ALTER TABLE " + table + " SET" + transform,
        SQLExceptionCode.CANNOT_TRANSFORM_TABLE_WITH_LOCAL_INDEX);
      assertRejected(stmt, "ALTER INDEX " + hnsw + " ON " + table + " ACTIVE" + transform,
        SQLExceptionCode.CANNOT_TRANSFORM_LOCAL_OR_VIEW_INDEX);
      for (String name : new String[] { table, hnsw }) {
        PTable pTable = PhoenixRuntime.getTableNoCache(conn, name);
        assertEquals(name, PTable.ImmutableStorageScheme.ONE_CELL_PER_COLUMN,
          pTable.getImmutableStorageScheme());
        assertEquals(name, PTable.QualifierEncodingScheme.NON_ENCODED_QUALIFIERS,
          pTable.getEncodingScheme());
        assertEquals(name, table, pTable.getPhysicalName().getString());
      }
    }
  }

  /**
   * The physical table of an HNSW index is its data table. Tests that DROP INDEX, a synchronous
   * REBUILD and TRUNCATE TABLE never drop, disable or truncate that table. The data table, its rows
   * and its other indexes stay intact. The test cluster runs without DROP_METADATA, so DROP INDEX
   * deletes the index rows with PostDDLCompiler. PostDDLCompiler must skip the index. Its scan
   * would select the V# cells in the empty column of the index and delete their rows. DROP INDEX
   * keeps the statistics of the data table. With DROP_METADATA, the client drops the tables that
   * the server returns, and these must not include the data table.
   */
  @Test
  public void testDropIndexKeepsDataTable() throws Exception {
    String table = generateUniqueName();
    String global = generateUniqueName();
    String ivf = generateUniqueName();
    String hnsw = generateUniqueName();
    String hnsw2 = generateUniqueName();
    try (Connection conn = DriverManager.getConnection(getUrl());
      Statement stmt = conn.createStatement();
      Admin admin = conn.unwrap(PhoenixConnection.class).getQueryServices().getAdmin()) {
      stmt.execute("CREATE TABLE " + table
        + " (ID VARCHAR NOT NULL PRIMARY KEY, V VECTOR(FLOAT, 4), C VARCHAR) SPLIT ON ('m')");
      upsertRows(conn, table);
      stmt.execute("CREATE INDEX " + global + " ON " + table + " (C)");
      stmt.execute("CREATE VECTOR INDEX " + ivf + " ON " + table
        + " (V) WITH (algorithm='IVF', metric='L2', lists=2, sample_size=4) ASYNC");
      stmt.execute("CREATE VECTOR INDEX " + hnsw + " ON " + table
        + " (V) WITH (algorithm='HNSW', metric='COSINE')");
      stmt.execute("CREATE VECTOR INDEX " + hnsw2 + " ON " + table
        + " (V) WITH (algorithm='HNSW', metric='L2')");
      TableName dataTable = TableName.valueOf(table);
      TableDescriptor desc = admin.getDescriptor(dataTable);
      List<String> indexes = Arrays.asList(global, ivf, hnsw, hnsw2);
      Collections.sort(indexes);
      assertEquals(indexes, indexNames(conn, table));

      stmt.execute("ALTER INDEX " + hnsw + " ON " + table + " REBUILD");
      assertEquals(PIndexState.BUILDING,
        PhoenixRuntime.getTableNoCache(conn, hnsw).getIndexState());
      assertDataTableIntact(conn, admin, table, global, 3);

      // Write V# cells in the empty column of the index. Put one at a data row and one at the
      // anchor row of the first region. Also write statistics for the data table.
      byte[] family = QueryConstants.VECTOR_LOCAL_INDEX_COLUMN_FAMILY_BYTES;
      byte[] empty = EncodedColumnsUtil
        .getEmptyKeyValueInfo(PhoenixRuntime.getTableNoCache(conn, hnsw)).getFirst();
      try (Table htable =
        conn.unwrap(PhoenixConnection.class).getQueryServices().getTable(Bytes.toBytes(table))) {
        htable.put(
          Arrays.asList(new Put(Bytes.toBytes("a")).addColumn(family, empty, Bytes.toBytes("x")),
            new Put(new byte[] { 0 }).addColumn(family, empty, Bytes.toBytes("y"))));
      }
      upsertStats(conn, table, QueryConstants.DEFAULT_COLUMN_FAMILY);

      stmt.execute("DROP INDEX " + hnsw + " ON " + table);
      assertDataTableIntact(conn, admin, table, global, 3);
      assertEquals(desc, admin.getDescriptor(dataTable));
      try (Table htable =
        conn.unwrap(PhoenixConnection.class).getQueryServices().getTable(Bytes.toBytes(table))) {
        String q = Bytes.toStringBinary(empty);
        assertEquals(Arrays.asList("\\x00/" + q + "/Put", "a/" + q + "/Put"), hiddenCells(htable));
      }
      // The server deletes the statistics of a dropped index in a background thread. Wait for that
      // thread to stop, then make sure that the statistics of the data table stay.
      long start = System.currentTimeMillis();
      while (
        System.currentTimeMillis() - start < 1000
          || (System.currentTimeMillis() - start < 60000 && Thread.getAllStackTraces().keySet()
            .stream().anyMatch(t -> "thread-statsdeletehandler".equals(t.getName())))
      ) {
        Thread.sleep(100);
      }
      assertEquals(Collections.singletonList(QueryConstants.DEFAULT_COLUMN_FAMILY),
        statsFamilies(conn, table));
      indexes = Arrays.asList(global, ivf, hnsw2);
      Collections.sort(indexes);
      assertEquals(indexes, indexNames(conn, table));
      assertTrue(admin.tableExists(TableName.valueOf(global)));
      assertTrue(admin.tableExists(TableName.valueOf(ivf)));

      // TRUNCATE truncates the data table and the index tables of the global indexes
      stmt.execute("TRUNCATE TABLE " + table);
      assertDataTableIntact(conn, admin, table, global, 0);
      assertEquals(desc, admin.getDescriptor(dataTable));
      upsertRows(conn, table);

      // Send the catalog drop of MetaDataClient.dropTable directly, to see the tables that the
      // server returns for deletion
      MetaDataMutationResult result = conn.unwrap(PhoenixConnection.class).getQueryServices()
        .dropTable(Arrays.<Mutation> asList(
          new Delete(SchemaUtil.getTableKey(null, null, hnsw2), HConstants.LATEST_TIMESTAMP),
          new Delete(MetaDataUtil.getParentLinkKey(null, null, table, hnsw2),
            HConstants.LATEST_TIMESTAMP)),
          PTableType.INDEX, false);
      assertEquals(MutationCode.TABLE_ALREADY_EXISTS, result.getMutationCode());
      assertTrue(
        result.getTableNamesToDelete() == null || result.getTableNamesToDelete().isEmpty());
      assertDataTableIntact(conn, admin, table, global, 3);
      indexes = Arrays.asList(global, ivf);
      Collections.sort(indexes);
      assertEquals(indexes, indexNames(conn, table));
      assertHiddenFamily(admin.getDescriptor(dataTable));
    }
  }

  private static void upsertRows(Connection conn, String table) throws SQLException {
    try (
      PreparedStatement ps = conn.prepareStatement("UPSERT INTO " + table + " VALUES (?, ?, ?)")) {
      String[] ids = { "a", "n", "z" };
      for (int i = 0; i < ids.length; i++) {
        ps.setString(1, ids[i]);
        ps.setArray(2, conn.createArrayOf("FLOAT", new Float[] { 1f, (float) i, 0f, 0f }));
        ps.setString(3, "c" + i);
        ps.execute();
      }
    }
    conn.commit();
  }

  private static void upsertStats(Connection conn, String table, String family)
    throws SQLException {
    try (PreparedStatement ps = conn.prepareStatement("UPSERT INTO SYSTEM.STATS (PHYSICAL_NAME,"
      + " COLUMN_FAMILY, GUIDE_POST_KEY, GUIDE_POSTS_WIDTH) VALUES (?, ?, ?, ?)")) {
      ps.setString(1, table);
      ps.setString(2, family);
      ps.setBytes(3, Bytes.toBytes("a"));
      ps.setLong(4, 1);
      ps.execute();
    }
    conn.commit();
  }

  // Returns the column families that have rows in SYSTEM.STATS for a table
  private static List<String> statsFamilies(Connection conn, String table) throws SQLException {
    List<String> families = new ArrayList<>();
    try (PreparedStatement ps = conn.prepareStatement(
      "SELECT DISTINCT COLUMN_FAMILY FROM SYSTEM.STATS WHERE PHYSICAL_NAME = ?")) {
      ps.setString(1, table);
      try (ResultSet rs = ps.executeQuery()) {
        while (rs.next()) {
          families.add(rs.getString(1));
        }
      }
    }
    Collections.sort(families);
    return families;
  }

  // Returns all cells of V#, delete markers included, as row/qualifier/type strings
  private static List<String> hiddenCells(Table htable) throws Exception {
    Scan raw = new Scan().addFamily(QueryConstants.VECTOR_LOCAL_INDEX_COLUMN_FAMILY_BYTES)
      .setRaw(true).readAllVersions();
    List<String> cells = new ArrayList<>();
    try (ResultScanner scanner = htable.getScanner(raw)) {
      for (Result result : scanner) {
        for (Cell cell : result.rawCells()) {
          cells.add(Bytes.toStringBinary(CellUtil.cloneRow(cell)) + "/"
            + Bytes.toStringBinary(CellUtil.cloneQualifier(cell)) + "/" + cell.getType());
        }
      }
    }
    return cells;
  }

  // Asserts that the data table is enabled, has its two regions and has the expected row count.
  // Its global index must also serve a query.
  private static void assertDataTableIntact(Connection conn, Admin admin, String table,
    String global, int rows) throws Exception {
    assertTrue(admin.isTableEnabled(TableName.valueOf(table)));
    assertEquals(Arrays.asList("", "m"), startKeys(admin, table));
    try (ResultSet rs =
      conn.createStatement().executeQuery("SELECT /*+ NO_INDEX */ COUNT(*) FROM " + table)) {
      assertTrue(rs.next());
      assertEquals(rows, rs.getInt(1));
    }
    String query =
      "SELECT /*+ INDEX(" + table + " " + global + ") */ ID FROM " + table + " WHERE C = 'c1'";
    try (ResultSet rs = conn.createStatement().executeQuery("EXPLAIN " + query)) {
      assertTrue(rs.next());
      assertTrue(rs.getString(1), rs.getString(1).contains(global));
    }
    try (ResultSet rs = conn.createStatement().executeQuery(query)) {
      if (rows > 0) {
        assertTrue(rs.next());
        assertEquals("n", rs.getString(1));
      }
      assertFalse(rs.next());
    }
  }

  private static List<String> indexNames(Connection conn, String table) throws SQLException {
    List<String> names = new ArrayList<>();
    for (PTable index : PhoenixRuntime.getTableNoCache(conn, table).getIndexes()) {
      names.add(index.getTableName().getString());
    }
    Collections.sort(names);
    return names;
  }

  /**
   * Tests that the hidden family keeps the HBase compaction scanner and its own settings. Phoenix
   * statistics and queries ignore the family. The test uses a table with a literal TTL and a table
   * with a conditional TTL. On each, a major compaction removes a deleted V# cell and its delete
   * marker. It keeps the live cells, also one that is older than the TTL. The live cells are older
   * than the max lookback window. A Phoenix compaction would evaluate the conditional TTL on the
   * anchor row and purge it. UPDATE STATISTICS and the compaction write no statistics for V#.
   * Phoenix queries, also ROW_SIZE() and RAW_ROW_SIZE() queries, never see an anchor row that has
   * only V# cells.
   */
  @Test
  public void testHiddenFamilyCompaction() throws Exception {
    assertHiddenFamilyCompaction("TTL=86400");
    assertHiddenFamilyCompaction("TTL='C IS NULL'");
  }

  /**
   * Tests that on a table with only primary key columns, ROW_SIZE() and RAW_ROW_SIZE() queries
   * never see an anchor row that has only V# cells.
   */
  @Test
  public void testHiddenFamilyRowSizeWithoutColumns() throws Exception {
    String table = generateUniqueName();
    try (Connection conn = DriverManager.getConnection(getUrl());
      Statement stmt = conn.createStatement()) {
      stmt.execute("CREATE TABLE " + table + " (ID VARCHAR NOT NULL, V VECTOR(FLOAT, 4) NOT NULL"
        + " CONSTRAINT PK PRIMARY KEY (ID, V)) IMMUTABLE_ROWS=true");
      stmt.execute("CREATE VECTOR INDEX " + generateUniqueName() + " ON " + table
        + " (V) WITH (algorithm='HNSW', metric='COSINE')");
      stmt.execute("UPSERT INTO " + table + " VALUES ('a', ARRAY[1.0, 0.0, 0.0, 0.0])");
      conn.commit();
      try (Table htable =
        conn.unwrap(PhoenixConnection.class).getQueryServices().getTable(Bytes.toBytes(table))) {
        htable.put(
          new Put(new byte[] { 0 }).addColumn(QueryConstants.VECTOR_LOCAL_INDEX_COLUMN_FAMILY_BYTES,
            Bytes.toBytes("q"), Bytes.toBytes("x")));
      }
      assertRowSizeRows(stmt, table, "primary key only");
    }
  }

  // Asserts that ROW_SIZE() and RAW_ROW_SIZE() queries see only the data row 'a'
  private static void assertRowSizeRows(Statement stmt, String table, String label)
    throws SQLException {
    for (String size : new String[] { "ROW_SIZE()", "RAW_ROW_SIZE()" }) {
      try (ResultSet rs =
        stmt.executeQuery("SELECT ID, SUM(" + size + ") FROM " + table + " GROUP BY ID")) {
        assertTrue(rs.next());
        assertEquals(label + " " + size, "a", rs.getString(1));
        assertTrue(label + " " + size, rs.getLong(2) > 0);
        assertFalse(label + " " + size, rs.next());
      }
    }
  }

  private static void assertHiddenFamilyCompaction(String ttl) throws Exception {
    String table = generateUniqueName();
    String index = generateUniqueName();
    try (Connection conn = DriverManager.getConnection(getUrl());
      Statement stmt = conn.createStatement()) {
      stmt.execute("CREATE TABLE " + table
        + " (ID VARCHAR NOT NULL PRIMARY KEY, V VECTOR(FLOAT, 4), C VARCHAR) GUIDE_POSTS_WIDTH=20, "
        + ttl);
      stmt.execute("CREATE VECTOR INDEX " + index + " ON " + table
        + " (V) WITH (algorithm='HNSW', metric='COSINE')");
      stmt.execute("UPSERT INTO " + table + " VALUES ('a', ARRAY[1.0, 0.0, 0.0, 0.0], 'c')");
      conn.commit();
      byte[] anchor = new byte[] { 0 };
      byte[] family = QueryConstants.VECTOR_LOCAL_INDEX_COLUMN_FAMILY_BYTES;
      long recent = System.currentTimeMillis() - 2 * 3600 * 1000L;
      long old = System.currentTimeMillis() - 2 * 86400 * 1000L;
      try (Table htable =
        conn.unwrap(PhoenixConnection.class).getQueryServices().getTable(Bytes.toBytes(table))) {
        htable.put(new Put(anchor).addColumn(family, Bytes.toBytes("live"), old, Bytes.toBytes("x"))
          .addColumn(family, Bytes.toBytes("current"), recent, Bytes.toBytes("y")));
        htable.put(new Put(anchor).addColumn(family, Bytes.toBytes("retired"), Bytes.toBytes("z")));
        htable.delete(new Delete(anchor).addColumns(family, Bytes.toBytes("retired")));
        TestUtil.flush(getUtility(), TableName.valueOf(table));
        stmt.execute("UPDATE STATISTICS " + table);
        TestUtil.majorCompact(getUtility(), TableName.valueOf(table));
        // The major compaction of the data family also makes the region write a segment of the
        // index. The assertion ignores the cells of that segment
        List<String> cells = new ArrayList<>();
        for (String cell : hiddenCells(htable)) {
          if (!cell.startsWith("\\x00/" + index + "\\x00")) {
            cells.add(cell);
          }
        }
        assertEquals(ttl, Arrays.asList("\\x00/current/Put", "\\x00/live/Put"), cells);
      }
      List<String> families = statsFamilies(conn, table);
      assertTrue(ttl + " " + families, families.contains(QueryConstants.DEFAULT_COLUMN_FAMILY));
      assertFalse(ttl + " " + families,
        families.contains(QueryConstants.VECTOR_LOCAL_INDEX_COLUMN_FAMILY));
      try (ResultSet rs = stmt.executeQuery("SELECT COUNT(*) FROM " + table)) {
        assertTrue(rs.next());
        assertEquals(ttl, 1, rs.getInt(1));
      }
      try (ResultSet rs = stmt.executeQuery("SELECT * FROM " + table)) {
        assertTrue(rs.next());
        assertEquals(ttl, "a", rs.getString(1));
        assertFalse(ttl, rs.next());
      }
      assertRowSizeRows(stmt, table, ttl);
    }
  }

  /**
   * Tests an HNSW index on a salted table whose conditional TTL refers to a column that the index
   * does not hold. The index is not salted. CREATE VECTOR INDEX rejects an explicit SALT_BUCKETS,
   * as for a local index. The index does not inherit the TTL of its table. Thus CREATE VECTOR INDEX
   * and a later ALTER TABLE SET TTL do not require the TTL columns in the index.
   */
  @Test
  public void testSaltedTableWithConditionalTtl() throws Exception {
    String table = generateUniqueName();
    String hnsw = generateUniqueName();
    try (Connection conn = DriverManager.getConnection(getUrl());
      Statement stmt = conn.createStatement()) {
      stmt.execute("CREATE TABLE " + table
        + " (ID VARCHAR NOT NULL PRIMARY KEY, V VECTOR(FLOAT, 4), C VARCHAR, D VARCHAR)"
        + " SALT_BUCKETS=3, TTL='C = ''x'''");
      assertRejected(stmt,
        "CREATE VECTOR INDEX " + generateUniqueName() + " ON " + table
          + " (V) WITH (algorithm='HNSW', metric='COSINE', SALT_BUCKETS=3)",
        SQLExceptionCode.CANNOT_SALT_LOCAL_INDEX);
      stmt.execute("CREATE VECTOR INDEX " + hnsw + " ON " + table
        + " (V) WITH (algorithm='HNSW', metric='COSINE')");

      PTable index = PhoenixRuntime.getTableNoCache(conn, hnsw);
      assertNull(index.getBucketNum());
      assertFalse(pkColumnNames(index).contains(SaltingUtil.SALTING_COLUMN_NAME));
      assertEquals(TTL_EXPRESSION_NOT_DEFINED, index.getTTLExpression());

      stmt.execute("ALTER TABLE " + table + " SET TTL='D = ''y'''");
      assertEquals(TTL_EXPRESSION_NOT_DEFINED,
        PhoenixRuntime.getTableNoCache(conn, hnsw).getTTLExpression());
      stmt.execute("UPSERT INTO " + table + " VALUES ('a', ARRAY[1.0, 0.0, 0.0, 0.0], 'c', 'd')");
      conn.commit();
      try (ResultSet rs = stmt.executeQuery("SELECT COUNT(*) FROM " + table)) {
        assertTrue(rs.next());
        assertEquals(1, rs.getInt(1));
      }
    }
  }

  // Asserts that V# has the fixed settings of the hidden family
  private static void assertHiddenFamily(TableDescriptor desc) {
    ColumnFamilyDescriptor cf =
      desc.getColumnFamily(QueryConstants.VECTOR_LOCAL_INDEX_COLUMN_FAMILY_BYTES);
    assertNotNull(desc.toString(), cf);
    assertTrue(cf.isMobEnabled());
    assertEquals(1024, cf.getMobThreshold());
    assertEquals(1, cf.getMaxVersions());
    assertEquals(HConstants.FOREVER, cf.getTimeToLive());
    assertEquals(KeepDeletedCells.FALSE, cf.getKeepDeletedCells());
    assertEquals(HConstants.REPLICATION_SCOPE_LOCAL, cf.getScope());
    assertEquals(BloomType.ROW, cf.getBloomFilterType());
  }

  private static List<String> startKeys(Admin admin, String table) throws Exception {
    List<String> startKeys = new ArrayList<>();
    for (RegionInfo region : admin.getRegions(TableName.valueOf(table))) {
      startKeys.add(Bytes.toStringBinary(region.getStartKey()));
    }
    Collections.sort(startKeys);
    return startKeys;
  }

  private static void assertColumnFamily(Admin admin, String table, int ttl,
    KeepDeletedCells keepDeletedCells, int scope) throws Exception {
    for (ColumnFamilyDescriptor cf : admin.getDescriptor(TableName.valueOf(table))
      .getColumnFamilies()) {
      assertEquals(table, ttl, cf.getTimeToLive());
      assertEquals(table, keepDeletedCells, cf.getKeepDeletedCells());
      assertEquals(table, scope, cf.getScope());
    }
  }
}
