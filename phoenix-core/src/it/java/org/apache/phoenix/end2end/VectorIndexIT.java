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

import static org.apache.phoenix.jdbc.PhoenixDatabaseMetaData.CENTROID_ID;
import static org.apache.phoenix.jdbc.PhoenixDatabaseMetaData.CENTROID_VECTOR;
import static org.apache.phoenix.jdbc.PhoenixDatabaseMetaData.COLUMN_COUNT_BYTES;
import static org.apache.phoenix.jdbc.PhoenixDatabaseMetaData.COLUMN_SIZE_BYTES;
import static org.apache.phoenix.jdbc.PhoenixDatabaseMetaData.DATA_TABLE_NAME_BYTES;
import static org.apache.phoenix.jdbc.PhoenixDatabaseMetaData.DATA_TYPE_BYTES;
import static org.apache.phoenix.jdbc.PhoenixDatabaseMetaData.GENERATION_ID;
import static org.apache.phoenix.jdbc.PhoenixDatabaseMetaData.INDEX_NAME;
import static org.apache.phoenix.jdbc.PhoenixDatabaseMetaData.INDEX_TYPE_BYTES;
import static org.apache.phoenix.jdbc.PhoenixDatabaseMetaData.NULLABLE_BYTES;
import static org.apache.phoenix.jdbc.PhoenixDatabaseMetaData.ORDINAL_POSITION_BYTES;
import static org.apache.phoenix.jdbc.PhoenixDatabaseMetaData.SYSTEM_VECTOR_CENTROID_NAME;
import static org.apache.phoenix.jdbc.PhoenixDatabaseMetaData.TABLE_FAMILY_BYTES;
import static org.apache.phoenix.jdbc.PhoenixDatabaseMetaData.TABLE_SEQ_NUM_BYTES;
import static org.apache.phoenix.jdbc.PhoenixDatabaseMetaData.TABLE_TYPE_BYTES;
import static org.apache.phoenix.jdbc.PhoenixDatabaseMetaData.VECTOR_DIMENSION_BYTES;
import static org.apache.phoenix.jdbc.PhoenixDatabaseMetaData.VECTOR_DISTANCE_METRIC_BYTES;
import static org.apache.phoenix.jdbc.PhoenixDatabaseMetaData.VECTOR_INDEX_ALGORITHM_BYTES;
import static org.apache.phoenix.jdbc.PhoenixDatabaseMetaData.VECTOR_IVF_LISTS_BYTES;
import static org.apache.phoenix.jdbc.PhoenixDatabaseMetaData.VECTOR_IVF_SAMPLE_SIZE_BYTES;
import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.IOException;
import java.sql.Array;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Random;
import java.util.UUID;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Mutation;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.client.coprocessor.Batch;
import org.apache.hadoop.hbase.ipc.CoprocessorRpcUtils.BlockingRpcCallback;
import org.apache.hadoop.hbase.ipc.ServerRpcController;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.phoenix.cache.VectorCentroidCache;
import org.apache.phoenix.coprocessor.generated.MetaDataProtos;
import org.apache.phoenix.coprocessor.generated.MetaDataProtos.CreateTableRequest;
import org.apache.phoenix.coprocessor.generated.MetaDataProtos.MetaDataResponse;
import org.apache.phoenix.coprocessor.generated.MetaDataProtos.MetaDataService;
import org.apache.phoenix.coprocessorclient.MetaDataProtocol;
import org.apache.phoenix.end2end.index.IndexTestUtil;
import org.apache.phoenix.exception.SQLExceptionCode;
import org.apache.phoenix.hbase.index.util.VersionUtil;
import org.apache.phoenix.index.vector.CentroidManager;
import org.apache.phoenix.jdbc.PhoenixConnection;
import org.apache.phoenix.jdbc.PhoenixDatabaseMetaData;
import org.apache.phoenix.mapreduce.index.IndexTool;
import org.apache.phoenix.mapreduce.index.PhoenixIndexImportDirectMapper;
import org.apache.phoenix.protobuf.ProtobufUtil;
import org.apache.phoenix.query.QueryConstants;
import org.apache.phoenix.query.QueryServices;
import org.apache.phoenix.schema.PColumn;
import org.apache.phoenix.schema.PIndexState;
import org.apache.phoenix.schema.PTable;
import org.apache.phoenix.schema.PTable.IndexType;
import org.apache.phoenix.schema.PTableImpl;
import org.apache.phoenix.schema.PTableType;
import org.apache.phoenix.schema.SortOrder;
import org.apache.phoenix.schema.TableNotFoundException;
import org.apache.phoenix.schema.types.PBson;
import org.apache.phoenix.schema.types.PDataType;
import org.apache.phoenix.schema.types.PDouble;
import org.apache.phoenix.schema.types.PInteger;
import org.apache.phoenix.schema.types.PLong;
import org.apache.phoenix.schema.types.PVarchar;
import org.apache.phoenix.schema.types.PVectorDouble;
import org.apache.phoenix.schema.types.PVectorFloat;
import org.apache.phoenix.util.ByteUtil;
import org.apache.phoenix.util.ClientUtil;
import org.apache.phoenix.util.Closeables;
import org.apache.phoenix.util.EncodedColumnsUtil;
import org.apache.phoenix.util.EnvironmentEdgeManager;
import org.apache.phoenix.util.IndexUtil;
import org.apache.phoenix.util.PropertiesUtil;
import org.apache.phoenix.util.QueryUtil;
import org.apache.phoenix.util.SchemaUtil;
import org.apache.phoenix.util.StringUtil;
import org.apache.phoenix.util.TestUtil;
import org.junit.Test;
import org.junit.experimental.categories.Category;

/** Integration tests for vector index metadata validation and lifecycle management. */
@Category(ParallelStatsDisabledTest.class)
public class VectorIndexIT extends ParallelStatsDisabledIT {

  @Test
  public void testNonVectorColumnRejection() throws Exception {
    String tableName = "T_NON_VEC_" + generateUniqueName();
    String indexName = "IDX_NON_VEC_" + generateUniqueName();

    try (Connection conn = DriverManager.getConnection(getUrl())) {
      try (Statement stmt = conn.createStatement()) {
        stmt.execute(
          "CREATE TABLE " + tableName + " (ID VARCHAR NOT NULL PRIMARY KEY, NON_VEC_COL VARCHAR)");
      }
      PhoenixConnection pconn = conn.unwrap(PhoenixConnection.class);

      try {
        createVectorIndexMetadata(pconn, null, indexName, tableName, "NON_VEC_COL",
          PVarchar.INSTANCE, null, "IVF", "L2", 128, 16, 500);
        fail("Expected vector index creation on VARCHAR column to be rejected");
      } catch (Exception e) {
        assertExpectedSqlException(e, SQLExceptionCode.VECTOR_INDEX_ON_NON_VECTOR_TYPE);
      }
    }
  }

  @Test
  public void testDimensionMismatchRejection() throws Exception {
    String tableName = "T_DIM_MISMATCH_" + generateUniqueName();
    String indexName = "IDX_DIM_MISMATCH_" + generateUniqueName();

    try (Connection conn = DriverManager.getConnection(getUrl())) {
      try (Statement stmt = conn.createStatement()) {
        stmt.execute(
          "CREATE TABLE " + tableName + " (ID VARCHAR NOT NULL PRIMARY KEY, V VECTOR(FLOAT, 128))");
      }
      PhoenixConnection pconn = conn.unwrap(PhoenixConnection.class);

      try {
        createVectorIndexMetadata(pconn, null, indexName, tableName, "V", PVectorFloat.INSTANCE,
          256, "IVF", "L2", 256, 16, 500);
        fail("Expected vector index creation with mismatched dimension to be rejected");
      } catch (Exception e) {
        assertExpectedSqlException(e, SQLExceptionCode.VECTOR_INDEX_DIMENSION_MISMATCH);
      }
    }
  }

  @Test
  public void testInvalidAlgorithmRejection() throws Exception {
    String tableName = "T_INVALID_ALGO_" + generateUniqueName();
    String indexName = "IDX_INVALID_ALGO_" + generateUniqueName();

    try (Connection conn = DriverManager.getConnection(getUrl())) {
      try (Statement stmt = conn.createStatement()) {
        stmt.execute(
          "CREATE TABLE " + tableName + " (ID VARCHAR NOT NULL PRIMARY KEY, V VECTOR(FLOAT, 128))");
      }
      PhoenixConnection pconn = conn.unwrap(PhoenixConnection.class);

      try {
        createVectorIndexMetadata(pconn, null, indexName, tableName, "V", PVectorFloat.INSTANCE,
          128, "UNKNOWN", "L2", 128, 16, 500);
        fail("Expected vector index creation with UNKNOWN algorithm to be rejected");
      } catch (Exception e) {
        assertExpectedSqlException(e, SQLExceptionCode.UNSUPPORTED_VECTOR_INDEX_ALGORITHM);
      }
    }
  }

  @Test
  public void testInvalidDistanceMetricRejection() throws Exception {
    String tableName = "T_INVALID_METRIC_" + generateUniqueName();
    String indexName = "IDX_INVALID_METRIC_" + generateUniqueName();

    try (Connection conn = DriverManager.getConnection(getUrl())) {
      try (Statement stmt = conn.createStatement()) {
        stmt.execute(
          "CREATE TABLE " + tableName + " (ID VARCHAR NOT NULL PRIMARY KEY, V VECTOR(FLOAT, 128))");
      }
      PhoenixConnection pconn = conn.unwrap(PhoenixConnection.class);

      try {
        createVectorIndexMetadata(pconn, null, indexName, tableName, "V", PVectorFloat.INSTANCE,
          128, "IVF", "MANHATTAN", 128, 16, 500);
        fail("Expected vector index creation with unsupported metric to be rejected");
      } catch (Exception e) {
        assertExpectedSqlException(e, SQLExceptionCode.UNSUPPORTED_VECTOR_DISTANCE_METRIC);
      }
    }
  }

  @Test
  public void testInvalidIvfParametersZeroListsRejection() throws Exception {
    String tableName = "T_IVF_ZERO_LISTS_" + generateUniqueName();
    String indexName = "IDX_IVF_ZERO_LISTS_" + generateUniqueName();

    try (Connection conn = DriverManager.getConnection(getUrl())) {
      try (Statement stmt = conn.createStatement()) {
        stmt.execute(
          "CREATE TABLE " + tableName + " (ID VARCHAR NOT NULL PRIMARY KEY, V VECTOR(FLOAT, 128))");
      }
      PhoenixConnection pconn = conn.unwrap(PhoenixConnection.class);

      try {
        createVectorIndexMetadata(pconn, null, indexName, tableName, "V", PVectorFloat.INSTANCE,
          128, "IVF", "L2", 128, 0, 500);
        fail("Expected vector index creation with vectorIvfLists = 0 to be rejected");
      } catch (Exception e) {
        assertExpectedSqlException(e, SQLExceptionCode.INVALID_VECTOR_INDEX_PARAMS);
      }
    }
  }

  @Test
  public void testInvalidIvfParametersSampleSizeLessThanListsRejection() throws Exception {
    String tableName = "T_IVF_SAMPLE_TOO_SMALL_" + generateUniqueName();
    String indexName = "IDX_IVF_SAMPLE_TOO_SMALL_" + generateUniqueName();

    try (Connection conn = DriverManager.getConnection(getUrl())) {
      try (Statement stmt = conn.createStatement()) {
        stmt.execute(
          "CREATE TABLE " + tableName + " (ID VARCHAR NOT NULL PRIMARY KEY, V VECTOR(FLOAT, 128))");
      }
      PhoenixConnection pconn = conn.unwrap(PhoenixConnection.class);

      try {
        createVectorIndexMetadata(pconn, null, indexName, tableName, "V", PVectorFloat.INSTANCE,
          128, "IVF", "L2", 128, 32, 16);
        fail("Expected vector index creation with sample_size < lists to be rejected");
      } catch (Exception e) {
        assertExpectedSqlException(e, SQLExceptionCode.INVALID_VECTOR_INDEX_PARAMS);
      }
    }
  }

  @Test
  public void testNonExistentParentTableRejection() throws Exception {
    String nonExistentBaseTable = "NO_SUCH_BASE_TABLE_" + generateUniqueName();
    String indexName = "IDX_ORPHAN_" + generateUniqueName();

    try (Connection conn = DriverManager.getConnection(getUrl())) {
      PhoenixConnection pconn = conn.unwrap(PhoenixConnection.class);

      try {
        createVectorIndexMetadata(pconn, null, indexName, nonExistentBaseTable, "V",
          PVectorFloat.INSTANCE, 128, "IVF", "L2", 128, 16, 500);
        fail("Expected vector index creation on non-existent base table to be rejected");
      } catch (Exception e) {
        assertExpectedSqlException(e, SQLExceptionCode.PARENT_TABLE_NOT_FOUND);
      }
    }
  }

  @Test
  public void testNonExistentColumnRejection() throws Exception {
    String tableName = "T_MISSING_COL_" + generateUniqueName();
    String indexName = "IDX_MISSING_COL_" + generateUniqueName();

    try (Connection conn = DriverManager.getConnection(getUrl())) {
      try (Statement stmt = conn.createStatement()) {
        stmt.execute(
          "CREATE TABLE " + tableName + " (ID VARCHAR NOT NULL PRIMARY KEY, V VECTOR(FLOAT, 128))");
      }
      PhoenixConnection pconn = conn.unwrap(PhoenixConnection.class);

      try {
        createVectorIndexMetadata(pconn, null, indexName, tableName, "NON_EXISTENT_COL",
          PVectorFloat.INSTANCE, 128, "IVF", "L2", 128, 16, 500);
        fail("Expected vector index creation on non-existent column to be rejected");
      } catch (Exception e) {
        assertExpectedSqlException(e, SQLExceptionCode.COLUMN_NOT_FOUND);
      }
    }
  }

  @Test
  public void testValidVectorIndexMetadataCreation() throws Exception {
    String tableName = "T_VALID_" + generateUniqueName();
    String indexName = "IDX_VALID_" + generateUniqueName();

    try (Connection conn = DriverManager.getConnection(getUrl())) {
      try (Statement stmt = conn.createStatement()) {
        stmt.execute(
          "CREATE TABLE " + tableName + " (ID VARCHAR NOT NULL PRIMARY KEY, V VECTOR(FLOAT, 128))");
      }
      PhoenixConnection pconn = conn.unwrap(PhoenixConnection.class);

      MetaDataResponse response = createVectorIndexMetadata(pconn, null, indexName, tableName, "V",
        PVectorFloat.INSTANCE, 128, "IVF", "L2", 128, 16, 500);
      assertNotNull("Expected non-null response", response);
      assertEquals(MetaDataProtos.MutationCode.TABLE_NOT_FOUND, response.getReturnCode());
      assertTrue("Expected response to contain built table", response.hasTable());

      PTable indexTable = PTableImpl.createFromProto(response.getTable());
      assertNotNull("Vector index table must exist in catalog", indexTable);
      assertEquals("IVF", indexTable.getVectorIndexAlgorithm());
      assertEquals("L2", indexTable.getVectorDistanceMetric());
      assertEquals(Integer.valueOf(128), indexTable.getVectorDimension());
      assertEquals(Integer.valueOf(16), indexTable.getVectorIvfLists());
      assertEquals(Integer.valueOf(500), indexTable.getVectorIvfSampleSize());
    }
  }

  @Test
  public void testValidBsonVectorIndexMetadataCreation() throws Exception {
    String tableName = "T_BSON_" + generateUniqueName();
    String indexName = "IDX_BSON_" + generateUniqueName();

    try (Connection conn = DriverManager.getConnection(getUrl())) {
      try (Statement stmt = conn.createStatement()) {
        stmt.execute("CREATE TABLE " + tableName + " (ID VARCHAR NOT NULL PRIMARY KEY, DOC BSON)");
      }
      PhoenixConnection pconn = conn.unwrap(PhoenixConnection.class);

      MetaDataResponse response = createVectorIndexMetadata(pconn, null, indexName, tableName,
        "DOC", PBson.INSTANCE, null, "IVF", "COSINE", 256, 32, 1000);
      assertNotNull("Expected non-null response", response);
      assertEquals(MetaDataProtos.MutationCode.TABLE_NOT_FOUND, response.getReturnCode());
      assertTrue("Expected response to contain built table", response.hasTable());

      PTable indexTable = PTableImpl.createFromProto(response.getTable());
      assertNotNull("Vector index on BSON column must exist in catalog", indexTable);
      assertEquals("IVF", indexTable.getVectorIndexAlgorithm());
      assertEquals("COSINE", indexTable.getVectorDistanceMetric());
      assertEquals(Integer.valueOf(256), indexTable.getVectorDimension());
      assertEquals(Integer.valueOf(32), indexTable.getVectorIvfLists());
      assertEquals(Integer.valueOf(1000), indexTable.getVectorIvfSampleSize());
    }
  }

  private void assertExpectedSqlException(Exception e, SQLExceptionCode expectedCode) {
    SQLException sqle = null;
    if (e instanceof SQLException) {
      sqle = (SQLException) e;
    } else {
      sqle = ClientUtil.parseServerException(e);
    }
    assertNotNull("Expected parsed SQLException from server error: " + e.getMessage(), sqle);
    assertEquals("Unexpected SQL error code: " + sqle.getMessage(), expectedCode.getErrorCode(),
      sqle.getErrorCode());
    assertEquals("Unexpected SQL state: " + sqle.getSQLState(), expectedCode.getSQLState(),
      sqle.getSQLState());
  }

  private MetaDataResponse createVectorIndexMetadata(PhoenixConnection pconn, String schemaName,
    String indexName, String baseTableName, String indexedColumnName,
    PDataType<?> indexedColumnType, Integer columnSize, String algorithm, String metric,
    Integer dimension, Integer lists, Integer sampleSize) throws Exception {
    byte[] schemaBytes = schemaName == null ? ByteUtil.EMPTY_BYTE_ARRAY : Bytes.toBytes(schemaName);
    byte[] indexTableBytes = Bytes.toBytes(indexName);
    byte[] baseTableBytes = Bytes.toBytes(baseTableName);
    byte[] tableKey = SchemaUtil.getTableKey(null, schemaBytes, indexTableBytes);
    byte[] baseTableKey = SchemaUtil.getTableKey(null, schemaBytes, baseTableBytes);

    List<Mutation> tableMetadata = new ArrayList<>();

    Put headerPut = new Put(tableKey);
    headerPut.addColumn(TABLE_FAMILY_BYTES, TABLE_TYPE_BYTES,
      Bytes.toBytes(PTableType.INDEX.getSerializedValue()));
    headerPut.addColumn(TABLE_FAMILY_BYTES, TABLE_SEQ_NUM_BYTES, PLong.INSTANCE.toBytes(1L));
    headerPut.addColumn(TABLE_FAMILY_BYTES, COLUMN_COUNT_BYTES, PInteger.INSTANCE.toBytes(1));
    headerPut.addColumn(TABLE_FAMILY_BYTES, INDEX_TYPE_BYTES,
      new byte[] { IndexType.VECTOR_GLOBAL.getSerializedValue() });
    headerPut.addColumn(TABLE_FAMILY_BYTES, DATA_TABLE_NAME_BYTES, baseTableBytes);

    if (algorithm != null) {
      headerPut.addColumn(TABLE_FAMILY_BYTES, VECTOR_INDEX_ALGORITHM_BYTES,
        Bytes.toBytes(algorithm));
    }
    if (metric != null) {
      headerPut.addColumn(TABLE_FAMILY_BYTES, VECTOR_DISTANCE_METRIC_BYTES, Bytes.toBytes(metric));
    }
    if (dimension != null) {
      headerPut.addColumn(TABLE_FAMILY_BYTES, VECTOR_DIMENSION_BYTES,
        PInteger.INSTANCE.toBytes(dimension));
    }
    if (lists != null) {
      headerPut.addColumn(TABLE_FAMILY_BYTES, VECTOR_IVF_LISTS_BYTES,
        PInteger.INSTANCE.toBytes(lists));
    }
    if (sampleSize != null) {
      headerPut.addColumn(TABLE_FAMILY_BYTES, VECTOR_IVF_SAMPLE_SIZE_BYTES,
        PInteger.INSTANCE.toBytes(sampleSize));
    }
    tableMetadata.add(headerPut);

    if (indexedColumnName != null) {
      String fullIndexColName = IndexUtil.getIndexColumnName(null, indexedColumnName);
      byte[] colKey = SchemaUtil.getColumnKey(null, schemaName, indexName, fullIndexColName, null);
      Put colPut = new Put(colKey);
      if (indexedColumnType != null) {
        colPut.addColumn(TABLE_FAMILY_BYTES, DATA_TYPE_BYTES,
          PInteger.INSTANCE.toBytes(indexedColumnType.getSqlType()));
      }
      colPut.addColumn(TABLE_FAMILY_BYTES, NULLABLE_BYTES,
        PInteger.INSTANCE.toBytes(ResultSetMetaData.columnNoNulls));
      colPut.addColumn(TABLE_FAMILY_BYTES, ORDINAL_POSITION_BYTES, PInteger.INSTANCE.toBytes(1));
      if (columnSize != null) {
        colPut.addColumn(TABLE_FAMILY_BYTES, COLUMN_SIZE_BYTES,
          PInteger.INSTANCE.toBytes(columnSize));
      }
      tableMetadata.add(colPut);
    }

    Put parentHeaderPut = new Put(baseTableKey);
    tableMetadata.add(parentHeaderPut);

    PTable parentTable = null;
    try {
      parentTable = pconn.getTableNoCache(SchemaUtil.getTableName(schemaName, baseTableName));
    } catch (Exception ignored) {
    }

    final List<Mutation> finalMutations = tableMetadata;
    final PTable finalParentTable = parentTable;

    Table ht = pconn.getQueryServices().getTable(PhoenixDatabaseMetaData.SYSTEM_CATALOG_NAME_BYTES);
    try {
      Batch.Call<MetaDataService, MetaDataResponse> callable =
        new Batch.Call<MetaDataService, MetaDataResponse>() {
          @Override
          public MetaDataResponse call(MetaDataService instance) throws IOException {
            ServerRpcController controller = new ServerRpcController();
            BlockingRpcCallback<MetaDataResponse> rpcCallback = new BlockingRpcCallback<>();
            CreateTableRequest.Builder builder = CreateTableRequest.newBuilder();
            for (Mutation m : finalMutations) {
              builder.addTableMetadataMutations(ProtobufUtil.toProto(m).toByteString());
            }
            builder
              .setClientVersion(VersionUtil.encodeVersion(MetaDataProtocol.PHOENIX_MAJOR_VERSION,
                MetaDataProtocol.PHOENIX_MINOR_VERSION, MetaDataProtocol.PHOENIX_PATCH_NUMBER));
            if (finalParentTable != null) {
              builder.setParentTable(PTableImpl.toProto(finalParentTable));
            }
            instance.createTable(controller, builder.build(), rpcCallback);
            if (controller.getFailedOn() != null) {
              throw controller.getFailedOn();
            }
            return rpcCallback.get();
          }
        };

      Map<byte[], MetaDataResponse> results;
      try {
        results = ht.coprocessorService(MetaDataService.class, tableKey, tableKey, callable);
      } catch (Throwable t) {
        if (t instanceof Exception) {
          throw (Exception) t;
        }
        throw new Exception(t);
      }
      assertNotNull("Expected non-null coprocessor response", results);
      return results.values().iterator().next();
    } finally {
      Closeables.closeQuietly(ht);
    }
  }

  @Test
  public void testCreateVectorIndexTableAndMetadata() throws Exception {
    String tableName = "T_VEC_DDL_" + generateUniqueName();
    String indexName = "IDX_VEC_DDL_" + generateUniqueName();

    try (Connection conn = DriverManager.getConnection(getUrl())) {
      try (Statement stmt = conn.createStatement()) {
        stmt.execute(
          "CREATE TABLE " + tableName + " (ID VARCHAR NOT NULL PRIMARY KEY, V VECTOR(FLOAT, 128))");
        stmt.execute("CREATE VECTOR INDEX " + indexName + " ON " + tableName + " (V) "
          + "WITH (algorithm = 'IVF', metric = 'L2', "
          + "dimension = 128, lists = 16, sample_size = 500)");
      }

      // Verify vector index catalog metadata persisted during DDL execution.
      try (Statement stmt = conn.createStatement()) {
        try (ResultSet rs = stmt.executeQuery(
          "SELECT INDEX_TYPE, INDEX_STATE, VECTOR_INDEX_ALGORITHM, VECTOR_DISTANCE_METRIC, "
            + "VECTOR_DIMENSION, VECTOR_IVF_LISTS, VECTOR_IVF_SAMPLE_SIZE, VECTOR_CENTROID_GENERATION "
            + "FROM SYSTEM.CATALOG WHERE TABLE_NAME = '" + indexName
            + "' AND TABLE_SCHEM IS NULL AND TENANT_ID IS NULL")) {
          assertTrue("Expected row in SYSTEM.CATALOG for index table", rs.next());
          assertEquals(IndexType.VECTOR_GLOBAL.getSerializedValue(), rs.getByte("INDEX_TYPE"));
          assertEquals(PIndexState.BUILDING.getSerializedValue(), rs.getString("INDEX_STATE"));
          assertEquals("IVF", rs.getString("VECTOR_INDEX_ALGORITHM"));
          assertEquals("L2", rs.getString("VECTOR_DISTANCE_METRIC"));
          assertEquals(128, rs.getInt("VECTOR_DIMENSION"));
          assertEquals(16, rs.getInt("VECTOR_IVF_LISTS"));
          assertEquals(500, rs.getInt("VECTOR_IVF_SAMPLE_SIZE"));
          long gen = rs.getLong("VECTOR_CENTROID_GENERATION");
          assertTrue("Centroid generation should be 0 or null", gen == 0 || rs.wasNull());
        }
      }

      // Verify client-side PTable schema representation reflects the vector index properties.
      PhoenixConnection pconn = conn.unwrap(PhoenixConnection.class);
      PTable indexTable = pconn.getTableNoCache(indexName);
      assertNotNull("Index PTable must not be null", indexTable);
      assertTrue("Table must be recognized as vector index", indexTable.isVectorIndex());
      assertEquals(IndexType.VECTOR_GLOBAL, indexTable.getIndexType());
      assertEquals(PIndexState.BUILDING, indexTable.getIndexState());
      assertEquals("IVF", indexTable.getVectorIndexAlgorithm());
      assertEquals("L2", indexTable.getVectorDistanceMetric());
      assertEquals(Integer.valueOf(128), indexTable.getVectorDimension());
      assertEquals(Integer.valueOf(16), indexTable.getVectorIvfLists());
      assertEquals(Integer.valueOf(500), indexTable.getVectorIvfSampleSize());

      // Vector index row keys are prefixed with the centroid partition identifier followed by
      // data table primary key columns.
      List<PColumn> pkColumns = indexTable.getPKColumns();
      assertEquals("Row key must have 2 PK columns", 2, pkColumns.size());
      PColumn pk0 = pkColumns.get(0);
      assertEquals(":CENTROID_ID", pk0.getName().getString());
      assertEquals(PInteger.INSTANCE, pk0.getDataType());
      assertFalse("Centroid ID column must not be nullable", pk0.isNullable());

      PColumn pk1 = pkColumns.get(1);
      assertEquals(":ID", pk1.getName().getString());
      assertEquals(PVarchar.INSTANCE, pk1.getDataType());

      // The indexed vector column resides in a standard column family rather than the primary key.
      PColumn vectorCol = indexTable.getColumnForColumnName("0:V");
      assertNotNull("Vector column 0:V must exist in index table", vectorCol);
      assertEquals(PVectorFloat.INSTANCE, vectorCol.getDataType());
      assertEquals(Integer.valueOf(128), vectorCol.getMaxLength());
      assertNotNull("Vector column must belong to a column family", vectorCol.getFamilyName());
    }
  }

  @Test
  public void testCreateVectorIndexWithIncludeColumns() throws Exception {
    String tableName = "T_VEC_INC_" + generateUniqueName();
    String indexName = "IDX_VEC_INC_" + generateUniqueName();

    try (Connection conn = DriverManager.getConnection(getUrl())) {
      try (Statement stmt = conn.createStatement()) {
        stmt.execute("CREATE TABLE " + tableName
          + " (ID VARCHAR NOT NULL PRIMARY KEY, V VECTOR(FLOAT, 64), LABEL VARCHAR, SCORE DOUBLE)");
        stmt.execute("CREATE VECTOR INDEX " + indexName + " ON " + tableName + " (V) "
          + "INCLUDE (LABEL, SCORE) "
          + "WITH (VECTOR_INDEX_ALGORITHM = 'IVF', VECTOR_DISTANCE_METRIC = 'COSINE', "
          + "VECTOR_DIMENSION = 64, VECTOR_IVF_LISTS = 8, VECTOR_IVF_SAMPLE_SIZE = 250)");
      }

      PhoenixConnection pconn = conn.unwrap(PhoenixConnection.class);
      PTable indexTable = pconn.getTableNoCache(indexName);
      assertNotNull(indexTable);
      assertTrue(indexTable.isVectorIndex());
      assertEquals(IndexType.VECTOR_GLOBAL, indexTable.getIndexType());
      assertEquals(PIndexState.BUILDING, indexTable.getIndexState());

      // Composite primary key comprises the centroid partition ID and data table primary key.
      List<PColumn> pkColumns = indexTable.getPKColumns();
      assertEquals(2, pkColumns.size());
      assertEquals(":CENTROID_ID", pkColumns.get(0).getName().getString());
      assertEquals(":ID", pkColumns.get(1).getName().getString());

      // The indexed vector column and covered non-key columns reside in the data column family.
      PColumn vectorCol = indexTable.getColumnForColumnName("0:V");
      assertNotNull("Vector column 0:V must exist", vectorCol);
      assertEquals(PVectorFloat.INSTANCE, vectorCol.getDataType());

      PColumn labelCol = indexTable.getColumnForColumnName("0:LABEL");
      assertNotNull("Covered column 0:LABEL must exist", labelCol);
      assertEquals(PVarchar.INSTANCE, labelCol.getDataType());

      PColumn scoreCol = indexTable.getColumnForColumnName("0:SCORE");
      assertNotNull("Covered column 0:SCORE must exist", scoreCol);
      assertEquals(PDouble.INSTANCE, scoreCol.getDataType());
    }
  }

  @Test
  public void testCreateVectorIndexWithDoubleVectors() throws Exception {
    String tableName = "T_VEC_DBL_" + generateUniqueName();
    String indexName = "IDX_VEC_DBL_" + generateUniqueName();

    try (Connection conn = DriverManager.getConnection(getUrl())) {
      try (Statement stmt = conn.createStatement()) {
        stmt.execute(
          "CREATE TABLE " + tableName + " (ID VARCHAR NOT NULL PRIMARY KEY, V VECTOR(DOUBLE, 64))");
        stmt.execute("CREATE VECTOR INDEX " + indexName + " ON " + tableName + " (V) "
          + "WITH (algorithm = 'IVF', metric = 'COSINE', "
          + "dimension = 64, lists = 32, sample_size = 1000)");
      }

      // Verify catalog metadata for double-precision vector index.
      try (Statement stmt = conn.createStatement()) {
        try (ResultSet rs = stmt.executeQuery(
          "SELECT INDEX_TYPE, INDEX_STATE, VECTOR_INDEX_ALGORITHM, VECTOR_DISTANCE_METRIC, "
            + "VECTOR_DIMENSION, VECTOR_IVF_LISTS, VECTOR_IVF_SAMPLE_SIZE "
            + "FROM SYSTEM.CATALOG WHERE TABLE_NAME = '" + indexName
            + "' AND TABLE_SCHEM IS NULL AND TENANT_ID IS NULL")) {
          assertTrue("Expected row in SYSTEM.CATALOG for vector index", rs.next());
          assertEquals(IndexType.VECTOR_GLOBAL.getSerializedValue(), rs.getByte("INDEX_TYPE"));
          assertEquals(PIndexState.BUILDING.getSerializedValue(), rs.getString("INDEX_STATE"));
          assertEquals("IVF", rs.getString("VECTOR_INDEX_ALGORITHM"));
          assertEquals("COSINE", rs.getString("VECTOR_DISTANCE_METRIC"));
          assertEquals(64, rs.getInt("VECTOR_DIMENSION"));
          assertEquals(32, rs.getInt("VECTOR_IVF_LISTS"));
          assertEquals(1000, rs.getInt("VECTOR_IVF_SAMPLE_SIZE"));
        }
      }

      PhoenixConnection pconn = conn.unwrap(PhoenixConnection.class);
      PTable indexTable = pconn.getTableNoCache(indexName);
      assertNotNull(indexTable);
      assertTrue(indexTable.isVectorIndex());
      assertEquals(IndexType.VECTOR_GLOBAL, indexTable.getIndexType());
      assertEquals(PIndexState.BUILDING, indexTable.getIndexState());

      // Composite row key contains the centroid partition identifier and data table primary key.
      List<PColumn> pkColumns = indexTable.getPKColumns();
      assertEquals(2, pkColumns.size());
      assertEquals(":CENTROID_ID", pkColumns.get(0).getName().getString());
      assertEquals(":ID", pkColumns.get(1).getName().getString());

      // The indexed column in the non-PK family must have PVectorDouble type
      PColumn vectorCol = indexTable.getColumnForColumnName("0:V");
      assertNotNull("Vector column 0:V must exist in index table", vectorCol);
      assertEquals("Double vector column must have PVectorDouble data type", PVectorDouble.INSTANCE,
        vectorCol.getDataType());
      assertEquals(Integer.valueOf(64), vectorCol.getMaxLength());
    }
  }

  @Test
  public void testVectorIndexStateIsBuildingAndInfersDimension() throws Exception {
    String tableName = "T_VEC_INFER_" + generateUniqueName();
    String indexName = "IDX_VEC_INFER_" + generateUniqueName();

    try (Connection conn = DriverManager.getConnection(getUrl())) {
      try (Statement stmt = conn.createStatement()) {
        stmt.execute(
          "CREATE TABLE " + tableName + " (ID VARCHAR NOT NULL PRIMARY KEY, V VECTOR(FLOAT, 32))");
        // Vector dimensionality defaults to the underlying column length when omitted from options.
        stmt.execute("CREATE VECTOR INDEX " + indexName + " ON " + tableName + " (V) "
          + "WITH (algorithm = 'IVF', metric = 'L2', lists = 4, sample_size = 100)");
      }

      PhoenixConnection pconn = conn.unwrap(PhoenixConnection.class);
      PTable indexTable = pconn.getTableNoCache(indexName);
      assertNotNull(indexTable);
      assertEquals(PIndexState.BUILDING, indexTable.getIndexState());
      assertEquals(Integer.valueOf(32), indexTable.getVectorDimension());
      assertEquals(Integer.valueOf(4), indexTable.getVectorIvfLists());
      assertEquals(Integer.valueOf(100), indexTable.getVectorIvfSampleSize());
    }
  }

  private Connection getDropMetadataConnection() throws Exception {
    Properties props = PropertiesUtil.deepCopy(TestUtil.TEST_PROPERTIES);
    props.setProperty(QueryServices.DROP_METADATA_ATTRIB, Boolean.toString(true));
    props.setProperty(QueryServices.EXTRA_JDBC_ARGUMENTS_ATTRIB, StringUtil.EMPTY_STRING);
    String url = QueryUtil.getConnectionUrl(props, config, generateUniqueName());
    return DriverManager.getConnection(url, props);
  }

  @Test
  public void testDropVectorIndex() throws Exception {
    String tableName = "T_DROP_VEC_" + generateUniqueName();
    String indexName = "IDX_DROP_VEC_" + generateUniqueName();

    try (Connection conn = getDropMetadataConnection()) {
      try (Statement stmt = conn.createStatement()) {
        stmt.execute(
          "CREATE TABLE " + tableName + " (ID VARCHAR NOT NULL PRIMARY KEY, V VECTOR(FLOAT, 128))");
        stmt.execute("CREATE VECTOR INDEX " + indexName + " ON " + tableName + " (V) "
          + "WITH (algorithm = 'IVF', metric = 'L2', lists = 16, sample_size = 500)");
      }

      PhoenixConnection pconn = conn.unwrap(PhoenixConnection.class);
      PTable indexTable = pconn.getTableNoCache(indexName);
      assertNotNull(indexTable);
      String fullIndexName = indexTable.getName().getString();
      String physicalName = indexTable.getPhysicalName().getString();
      org.apache.hadoop.hbase.TableName hbaseTableName =
        org.apache.hadoop.hbase.TableName.valueOf(physicalName);

      // Pre-populate centroid metadata to verify cleanup during index drop.
      String upsertCentroidSql =
        "UPSERT INTO " + SYSTEM_VECTOR_CENTROID_NAME + " (" + INDEX_NAME + ", " + CENTROID_ID + ", "
          + CENTROID_VECTOR + ", " + GENERATION_ID + ") VALUES (?, ?, ?, ?)";
      try (PreparedStatement ps = conn.prepareStatement(upsertCentroidSql)) {
        for (int i = 0; i < 4; i++) {
          ps.setString(1, fullIndexName);
          ps.setInt(2, i);
          ps.setBytes(3, new byte[] { (byte) i, 1, 2, 3 });
          ps.setLong(4, 1L);
          ps.executeUpdate();
        }
      }
      conn.commit();

      // Confirm pre-drop presence of centroid metadata.
      try (PreparedStatement ps = conn.prepareStatement(
        "SELECT COUNT(*) FROM " + SYSTEM_VECTOR_CENTROID_NAME + " WHERE " + INDEX_NAME + " = ?")) {
        ps.setString(1, fullIndexName);
        try (ResultSet rs = ps.executeQuery()) {
          assertTrue(rs.next());
          assertEquals(4, rs.getInt(1));
        }
      }

      // Confirm pre-drop existence of the underlying physical HBase table.
      try (Admin admin = pconn.getQueryServices().getAdmin()) {
        assertTrue("HBase physical table should exist before drop",
          admin.tableExists(hbaseTableName));
      }

      try (Statement stmt = conn.createStatement()) {
        stmt.execute("DROP INDEX " + indexName + " ON " + tableName);
      }

      // Dropping the vector index must clean up centroid records from SYSTEM.VECTOR_CENTROID.
      try (PreparedStatement ps = conn.prepareStatement(
        "SELECT COUNT(*) FROM " + SYSTEM_VECTOR_CENTROID_NAME + " WHERE " + INDEX_NAME + " = ?")) {
        ps.setString(1, fullIndexName);
        try (ResultSet rs = ps.executeQuery()) {
          assertTrue(rs.next());
          assertEquals("Centroids must be removed on drop index", 0, rs.getInt(1));
        }
      }

      // Dropping the vector index must delete the underlying physical HBase table.
      try (Admin admin = pconn.getQueryServices().getAdmin()) {
        assertFalse("HBase physical table should be deleted on drop index",
          admin.tableExists(hbaseTableName));
      }

      // Dropping the vector index must remove the index definition from SYSTEM.CATALOG.
      try (Statement stmt = conn.createStatement();
        ResultSet rs = stmt.executeQuery("SELECT 1 FROM SYSTEM.CATALOG WHERE TABLE_NAME = '"
          + indexName + "' AND TABLE_SCHEM IS NULL AND TENANT_ID IS NULL")) {
        assertFalse("Index row in SYSTEM.CATALOG must be deleted", rs.next());
      }

      // The dropped index should be evicted from the client metadata cache.
      try {
        pconn.getTableNoCache(indexName);
        fail("Expected TableNotFoundException after drop");
      } catch (TableNotFoundException expected) {
      }
    }
  }

  @Test
  public void testDropTableCascadesToVectorIndexCentroids() throws Exception {
    String tableName = "T_CASCADE_VEC_" + generateUniqueName();
    String indexName = "IDX_CASCADE_VEC_" + generateUniqueName();

    try (Connection conn = getDropMetadataConnection()) {
      try (Statement stmt = conn.createStatement()) {
        stmt.execute(
          "CREATE TABLE " + tableName + " (ID VARCHAR NOT NULL PRIMARY KEY, V VECTOR(FLOAT, 64))");
        stmt.execute("CREATE VECTOR INDEX " + indexName + " ON " + tableName + " (V) "
          + "WITH (algorithm = 'IVF', metric = 'L2', lists = 8, sample_size = 250)");
      }

      PhoenixConnection pconn = conn.unwrap(PhoenixConnection.class);
      PTable indexTable = pconn.getTableNoCache(indexName);
      assertNotNull(indexTable);
      String fullIndexName = indexTable.getName().getString();
      String physicalName = indexTable.getPhysicalName().getString();
      org.apache.hadoop.hbase.TableName hbaseTableName =
        org.apache.hadoop.hbase.TableName.valueOf(physicalName);

      // Pre-populate centroid records to test cascading deletion on parent table drop.
      String upsertCentroidSql =
        "UPSERT INTO " + SYSTEM_VECTOR_CENTROID_NAME + " (" + INDEX_NAME + ", " + CENTROID_ID + ", "
          + CENTROID_VECTOR + ", " + GENERATION_ID + ") VALUES (?, ?, ?, ?)";
      try (PreparedStatement ps = conn.prepareStatement(upsertCentroidSql)) {
        for (int i = 0; i < 3; i++) {
          ps.setString(1, fullIndexName);
          ps.setInt(2, i);
          ps.setBytes(3, new byte[] { (byte) i, 4, 5, 6 });
          ps.setLong(4, 1L);
          ps.executeUpdate();
        }
      }
      conn.commit();

      try (Statement stmt = conn.createStatement()) {
        stmt.execute("DROP TABLE " + tableName);
      }

      // Cascading table drop must clean up centroid records for all child vector indexes.
      try (PreparedStatement ps = conn.prepareStatement(
        "SELECT COUNT(*) FROM " + SYSTEM_VECTOR_CENTROID_NAME + " WHERE " + INDEX_NAME + " = ?")) {
        ps.setString(1, fullIndexName);
        try (ResultSet rs = ps.executeQuery()) {
          assertTrue(rs.next());
          assertEquals("Centroids must be removed on cascade drop table", 0, rs.getInt(1));
        }
      }

      // Physical HBase tables for associated vector indexes must be removed with the parent table.
      try (Admin admin = pconn.getQueryServices().getAdmin()) {
        assertFalse("HBase physical table for index should be deleted",
          admin.tableExists(hbaseTableName));
      }
    }
  }

  @Test
  public void testDropVectorIndexWithSchema() throws Exception {
    String schemaName = "S_" + generateUniqueName();
    String tableName = "T_DROP_VEC_" + generateUniqueName();
    String indexName = "IDX_DROP_VEC_" + generateUniqueName();
    String fullTableName = SchemaUtil.getTableName(schemaName, tableName);
    String fullIndexName = SchemaUtil.getTableName(schemaName, indexName);

    try (Connection conn = getDropMetadataConnection()) {
      try (Statement stmt = conn.createStatement()) {
        stmt.execute("CREATE TABLE " + fullTableName
          + " (ID VARCHAR NOT NULL PRIMARY KEY, V VECTOR(FLOAT, 128))");
        stmt.execute("CREATE VECTOR INDEX " + indexName + " ON " + fullTableName + " (V) "
          + "WITH (algorithm = 'IVF', metric = 'L2', lists = 16, sample_size = 500)");
      }

      PhoenixConnection pconn = conn.unwrap(PhoenixConnection.class);
      PTable indexTable = pconn.getTableNoCache(fullIndexName);
      assertNotNull(indexTable);
      assertEquals(fullIndexName, indexTable.getName().getString());

      // Pre-populate centroid records under the full schema-qualified index name.
      String upsertCentroidSql =
        "UPSERT INTO " + SYSTEM_VECTOR_CENTROID_NAME + " (" + INDEX_NAME + ", " + CENTROID_ID + ", "
          + CENTROID_VECTOR + ", " + GENERATION_ID + ") VALUES (?, ?, ?, ?)";
      try (PreparedStatement ps = conn.prepareStatement(upsertCentroidSql)) {
        for (int i = 0; i < 3; i++) {
          ps.setString(1, fullIndexName);
          ps.setInt(2, i);
          ps.setBytes(3, new byte[] { (byte) i, 1, 2, 3 });
          ps.setLong(4, 1L);
          ps.executeUpdate();
        }
      }
      conn.commit();

      try (PreparedStatement ps = conn.prepareStatement(
        "SELECT COUNT(*) FROM " + SYSTEM_VECTOR_CENTROID_NAME + " WHERE " + INDEX_NAME + " = ?")) {
        ps.setString(1, fullIndexName);
        try (ResultSet rs = ps.executeQuery()) {
          assertTrue(rs.next());
          assertEquals(3, rs.getInt(1));
        }
      }

      try (Statement stmt = conn.createStatement()) {
        stmt.execute("DROP INDEX " + indexName + " ON " + fullTableName);
      }

      // Schema-qualified vector index drop must clean up corresponding centroid records.
      try (PreparedStatement ps = conn.prepareStatement(
        "SELECT COUNT(*) FROM " + SYSTEM_VECTOR_CENTROID_NAME + " WHERE " + INDEX_NAME + " = ?")) {
        ps.setString(1, fullIndexName);
        try (ResultSet rs = ps.executeQuery()) {
          assertTrue(rs.next());
          assertEquals("Centroids must be removed on drop index with schema", 0, rs.getInt(1));
        }
      }
    }
  }

  @Test
  public void testDropVectorIndexIfExists() throws Exception {
    String tableName = "T_IF_EXISTS_" + generateUniqueName();
    String indexName = "IDX_IF_EXISTS_" + generateUniqueName();

    try (Connection conn = getDropMetadataConnection()) {
      try (Statement stmt = conn.createStatement()) {
        stmt.execute(
          "CREATE TABLE " + tableName + " (ID VARCHAR NOT NULL PRIMARY KEY, V VECTOR(FLOAT, 32))");
        stmt.execute("CREATE VECTOR INDEX " + indexName + " ON " + tableName + " (V) "
          + "WITH (algorithm = 'IVF', metric = 'L2', lists = 4, sample_size = 100)");
      }

      // Verify the index exists in SYSTEM.CATALOG
      try (Statement stmt = conn.createStatement();
        ResultSet rs = stmt.executeQuery("SELECT 1 FROM SYSTEM.CATALOG WHERE TABLE_NAME = '"
          + indexName + "' AND TABLE_SCHEM IS NULL AND TENANT_ID IS NULL")) {
        assertTrue("Index must exist in SYSTEM.CATALOG before drop", rs.next());
      }

      // DROP INDEX IF EXISTS on an existing index: must remove it
      try (Statement stmt = conn.createStatement()) {
        stmt.execute("DROP INDEX IF EXISTS " + indexName + " ON " + tableName);
      }

      try (Statement stmt = conn.createStatement();
        ResultSet rs = stmt.executeQuery("SELECT 1 FROM SYSTEM.CATALOG WHERE TABLE_NAME = '"
          + indexName + "' AND TABLE_SCHEM IS NULL AND TENANT_ID IS NULL")) {
        assertFalse("Index must be removed from SYSTEM.CATALOG after DROP INDEX IF EXISTS",
          rs.next());
      }

      // Dropping a non-existent index should complete cleanly when IF EXISTS is specified.
      try (Statement stmt = conn.createStatement()) {
        stmt.execute("DROP INDEX IF EXISTS " + indexName + " ON " + tableName);
      }
    }
  }

  @Test
  public void testSynchronousVectorIndexPopulationAndActivation() throws Exception {
    String tableName = "T_VEC_POP_" + generateUniqueName();
    String indexName = "IDX_VEC_POP_" + generateUniqueName();

    try (Connection conn = DriverManager.getConnection(getUrl())) {
      try (Statement stmt = conn.createStatement()) {
        stmt.execute("CREATE TABLE " + tableName
          + " (ID VARCHAR NOT NULL PRIMARY KEY, V VECTOR(FLOAT, 4), LABEL VARCHAR)");
      }

      String upsertSql = "UPSERT INTO " + tableName + " (ID, V, LABEL) VALUES (?, ?, ?)";
      try (PreparedStatement ps = conn.prepareStatement(upsertSql)) {
        for (int i = 0; i < 100; i++) {
          ps.setString(1, String.format("id_%03d", i));
          Float[] vec = new Float[] { (float) (i % 4), (float) ((i + 1) % 4), (float) ((i + 2) % 4),
            (float) ((i + 3) % 4) };
          Array array = conn.createArrayOf("FLOAT", vec);
          ps.setArray(2, array);
          ps.setString(3, "label_" + i);
          ps.executeUpdate();
        }
      }
      conn.commit();

      try (Statement stmt = conn.createStatement()) {
        stmt.execute(
          "CREATE VECTOR INDEX " + indexName + " ON " + tableName + " (V) " + "INCLUDE (LABEL) "
            + "WITH (algorithm = 'IVF', metric = 'L2', lists = 4, sample_size = 100)");
      }

      PhoenixConnection pconn = conn.unwrap(PhoenixConnection.class);
      PTable indexTable = pconn.getTableNoCache(indexName);
      assertNotNull("Index table must exist", indexTable);

      // Synchronous vector index creation immediately transitions the index state to ACTIVE.
      assertEquals("Index state must be ACTIVE after synchronous creation", PIndexState.ACTIVE,
        indexTable.getIndexState());

      // Verify the trained centroids are persisted under the initial generation in
      // SYSTEM.VECTOR_CENTROID.
      String centroidCountSql = "SELECT COUNT(*) FROM " + SYSTEM_VECTOR_CENTROID_NAME + " WHERE "
        + INDEX_NAME + " = ? AND " + GENERATION_ID + " = 1";
      try (PreparedStatement ps = conn.prepareStatement(centroidCountSql)) {
        ps.setString(1, indexName);
        try (ResultSet rs = ps.executeQuery()) {
          assertTrue(rs.next());
          assertEquals("Expected 4 centroids at generation 1", 4, rs.getInt(1));
        }
      }

      // Verify populated index row count matches the base table.
      try (Statement stmt = conn.createStatement();
        ResultSet rs = stmt.executeQuery("SELECT COUNT(*) FROM " + indexName)) {
        assertTrue(rs.next());
        assertEquals("Expected 100 rows in index table", 100, rs.getInt(1));
      }

      // Verify centroid partition assignment values and projected covered columns via SQL scan.
      String selectIndexSql = "SELECT \":CENTROID_ID\", \":ID\", \"0:LABEL\" FROM " + indexName;
      int scannedCount = 0;
      try (Statement stmt = conn.createStatement();
        ResultSet rs = stmt.executeQuery(selectIndexSql)) {
        while (rs.next()) {
          int centroidId = rs.getInt(1);
          String id = rs.getString(2);
          String label = rs.getString(3);

          assertTrue("Centroid ID must be in [0, 3], got " + centroidId,
            centroidId >= 0 && centroidId <= 3);
          assertNotNull("ID must not be null", id);
          assertNotNull("LABEL must not be null", label);
          assertTrue("ID should match prefix id_", id.startsWith("id_"));
          scannedCount++;
        }
      }
      assertEquals("Expected 100 rows scanned from index", 100, scannedCount);

      // Confirm raw HBase row keys are prefixed with the 4-byte big-endian centroid identifier.
      byte[] physicalNameBytes = indexTable.getPhysicalName().getBytes();
      try (Table hTable = pconn.getQueryServices().getTable(physicalNameBytes);
        org.apache.hadoop.hbase.client.ResultScanner scanner =
          hTable.getScanner(new org.apache.hadoop.hbase.client.Scan())) {
        int hbaseRowCount = 0;
        for (org.apache.hadoop.hbase.client.Result r : scanner) {
          byte[] rowKey = r.getRow();
          int centroidId = (Integer) PInteger.INSTANCE.toObject(rowKey, 0, Bytes.SIZEOF_INT,
            PInteger.INSTANCE, SortOrder.getDefault());
          assertTrue("Physical row key centroid ID prefix must be in [0, 3], got " + centroidId,
            centroidId >= 0 && centroidId <= 3);
          hbaseRowCount++;
        }
        assertEquals("Expected 100 physical rows in HBase index table", 100, hbaseRowCount);
      }
    }
  }

  private void setupTableAndKnownCentroids(Connection conn, String tableName, String indexName)
    throws Exception {
    try (Statement stmt = conn.createStatement()) {
      stmt.execute("CREATE TABLE " + tableName
        + " (ID VARCHAR NOT NULL PRIMARY KEY, V VECTOR(FLOAT, 4), LABEL VARCHAR)");
      stmt.execute("CREATE VECTOR INDEX " + indexName + " ON " + tableName + " (V) "
        + "INCLUDE (LABEL) WITH (algorithm = 'IVF', metric = 'L2', lists = 4, sample_size = 100)");
    }

    // Overwrite centroids in SYSTEM.VECTOR_CENTROID with our exact 4 known centroids (generation 1)
    List<float[]> knownCentroids = Arrays.asList(new float[] { 0.0f, 0.0f, 0.0f, 1.0f }, // ID 0
      new float[] { 0.0f, 0.0f, 1.0f, 0.0f }, // ID 1
      new float[] { 1.0f, 0.0f, 0.0f, 0.0f }, // ID 2
      new float[] { 0.0f, 1.0f, 0.0f, 0.0f } // ID 3
    );
    CentroidManager.persistCentroidsFromFloatList(conn, indexName, 1L, knownCentroids);
    CentroidManager.setGenerationAndLists(conn, indexName, 1L, 4);

    // Transition index to ACTIVE state and refresh client cache
    PhoenixConnection pconn = conn.unwrap(PhoenixConnection.class);
    IndexUtil.updateIndexState(pconn, indexName, PIndexState.ACTIVE, 0L);
    pconn.removeTable(pconn.getTenantId(), indexName, null, HConstants.LATEST_TIMESTAMP);
    pconn.removeTable(pconn.getTenantId(), tableName, null, HConstants.LATEST_TIMESTAMP);

    // Prime the centroid cache
    VectorCentroidCache.getInstance().putFloatCentroids(indexName, 1L, knownCentroids);
  }

  private List<byte[]> getHBaseRowKeys(PhoenixConnection pconn, PTable table) throws Exception {
    byte[] physicalNameBytes = table.getPhysicalName().getBytes();
    List<byte[]> rowKeys = new ArrayList<>();
    try (Table hTable = pconn.getQueryServices().getTable(physicalNameBytes);
      org.apache.hadoop.hbase.client.ResultScanner scanner =
        hTable.getScanner(new org.apache.hadoop.hbase.client.Scan())) {
      for (org.apache.hadoop.hbase.client.Result r : scanner) {
        rowKeys.add(r.getRow());
      }
    }
    return rowKeys;
  }

  private int extractCentroidId(byte[] rowKey) {
    return (Integer) PInteger.INSTANCE.toObject(rowKey, 0, Bytes.SIZEOF_INT, PInteger.INSTANCE,
      SortOrder.getDefault());
  }

  @Test
  public void testVectorInsertGeneratesIndexRow() throws Exception {
    String tableName = "T_VEC_INS_" + generateUniqueName();
    String indexName = "IDX_VEC_INS_" + generateUniqueName();

    try (Connection conn = DriverManager.getConnection(getUrl())) {
      setupTableAndKnownCentroids(conn, tableName, indexName);

      PhoenixConnection pconn = conn.unwrap(PhoenixConnection.class);
      PTable indexTable = pconn.getTableNoCache(indexName);

      // Upsert a row with vector [1,0,0,0] (nearest centroid is ID 2)
      String upsertSql = "UPSERT INTO " + tableName + " (ID, V, LABEL) VALUES (?, ?, ?)";
      try (PreparedStatement ps = conn.prepareStatement(upsertSql)) {
        ps.setString(1, "row_1");
        ps.setArray(2, conn.createArrayOf("FLOAT", new Float[] { 1.0f, 0.0f, 0.0f, 0.0f }));
        ps.setString(3, "lbl_1");
        ps.executeUpdate();
      }
      conn.commit();

      // Verify raw HBase index table contains exactly 1 row with centroid ID 2
      List<byte[]> rowKeys = getHBaseRowKeys(pconn, indexTable);
      assertEquals("Expected exactly 1 index row", 1, rowKeys.size());
      assertEquals("Centroid prefix must be 2", 2, extractCentroidId(rowKeys.get(0)));

      // Verify via SQL scan
      String selectSql = "SELECT \":CENTROID_ID\", \":ID\", \"0:LABEL\" FROM " + indexName;
      try (Statement stmt = conn.createStatement(); ResultSet rs = stmt.executeQuery(selectSql)) {
        assertTrue(rs.next());
        assertEquals(2, rs.getInt(1));
        assertEquals("row_1", rs.getString(2));
        assertEquals("lbl_1", rs.getString(3));
        assertFalse(rs.next());
      }
    }
  }

  @Test
  public void testVectorUpdateWithCentroidChange() throws Exception {
    String tableName = "T_VEC_UPD_" + generateUniqueName();
    String indexName = "IDX_VEC_UPD_" + generateUniqueName();

    try (Connection conn = DriverManager.getConnection(getUrl())) {
      setupTableAndKnownCentroids(conn, tableName, indexName);

      PhoenixConnection pconn = conn.unwrap(PhoenixConnection.class);
      PTable indexTable = pconn.getTableNoCache(indexName);

      // Upsert initial row: vector [1,0,0,0] -> centroid ID 2
      String upsertSql = "UPSERT INTO " + tableName + " (ID, V, LABEL) VALUES (?, ?, ?)";
      try (PreparedStatement ps = conn.prepareStatement(upsertSql)) {
        ps.setString(1, "row_1");
        ps.setArray(2, conn.createArrayOf("FLOAT", new Float[] { 1.0f, 0.0f, 0.0f, 0.0f }));
        ps.setString(3, "lbl_1");
        ps.executeUpdate();
      }
      conn.commit();

      List<byte[]> rowKeysBefore = getHBaseRowKeys(pconn, indexTable);
      assertEquals(1, rowKeysBefore.size());
      assertEquals(2, extractCentroidId(rowKeysBefore.get(0)));

      // Update vector to [0,0,0,1] -> nearest centroid ID 0
      try (PreparedStatement ps = conn.prepareStatement(upsertSql)) {
        ps.setString(1, "row_1");
        ps.setArray(2, conn.createArrayOf("FLOAT", new Float[] { 0.0f, 0.0f, 0.0f, 1.0f }));
        ps.setString(3, "lbl_updated");
        ps.executeUpdate();
      }
      conn.commit();

      // Verify old row (centroid 2) is deleted and new row (centroid 0) exists
      List<byte[]> rowKeysAfter = getHBaseRowKeys(pconn, indexTable);
      assertEquals("Expected exactly 1 index row after vector update", 1, rowKeysAfter.size());
      assertEquals("Centroid prefix must be updated to 0", 0,
        extractCentroidId(rowKeysAfter.get(0)));

      // Verify via SQL scan
      String selectSql = "SELECT \":CENTROID_ID\", \":ID\", \"0:LABEL\" FROM " + indexName;
      try (Statement stmt = conn.createStatement(); ResultSet rs = stmt.executeQuery(selectSql)) {
        assertTrue(rs.next());
        assertEquals(0, rs.getInt(1));
        assertEquals("row_1", rs.getString(2));
        assertEquals("lbl_updated", rs.getString(3));
        assertFalse(rs.next());
      }
    }
  }

  @Test
  public void testVectorCoveredColumnUpdateWithoutVectorChange() throws Exception {
    String tableName = "T_VEC_COV_" + generateUniqueName();
    String indexName = "IDX_VEC_COV_" + generateUniqueName();

    try (Connection conn = DriverManager.getConnection(getUrl())) {
      setupTableAndKnownCentroids(conn, tableName, indexName);

      PhoenixConnection pconn = conn.unwrap(PhoenixConnection.class);
      PTable indexTable = pconn.getTableNoCache(indexName);

      // Upsert initial row with vector [1,0,0,0] -> centroid ID 2
      String upsertSql = "UPSERT INTO " + tableName + " (ID, V, LABEL) VALUES (?, ?, ?)";
      try (PreparedStatement ps = conn.prepareStatement(upsertSql)) {
        ps.setString(1, "row_1");
        ps.setArray(2, conn.createArrayOf("FLOAT", new Float[] { 1.0f, 0.0f, 0.0f, 0.0f }));
        ps.setString(3, "initial_label");
        ps.executeUpdate();
      }
      conn.commit();

      List<byte[]> rowKeysBefore = getHBaseRowKeys(pconn, indexTable);
      assertEquals(1, rowKeysBefore.size());
      assertEquals(2, extractCentroidId(rowKeysBefore.get(0)));

      // Update ONLY covered column (same vector, new label)
      try (PreparedStatement ps =
        conn.prepareStatement("UPSERT INTO " + tableName + " (ID, V, LABEL) VALUES (?, ?, ?)")) {
        ps.setString(1, "row_1");
        ps.setArray(2, conn.createArrayOf("FLOAT", new Float[] { 1.0f, 0.0f, 0.0f, 0.0f }));
        ps.setString(3, "updated_label");
        ps.executeUpdate();
      }
      conn.commit();

      // Verify row key (and centroid prefix) is unchanged
      List<byte[]> rowKeysAfter = getHBaseRowKeys(pconn, indexTable);
      assertEquals("Expected exactly 1 index row", 1, rowKeysAfter.size());
      assertEquals("Centroid prefix must still be 2", 2, extractCentroidId(rowKeysAfter.get(0)));

      // Verify covered column value was updated in place
      String selectSql = "SELECT \":CENTROID_ID\", \":ID\", \"0:LABEL\" FROM " + indexName;
      try (Statement stmt = conn.createStatement(); ResultSet rs = stmt.executeQuery(selectSql)) {
        assertTrue(rs.next());
        assertEquals(2, rs.getInt(1));
        assertEquals("row_1", rs.getString(2));
        assertEquals("updated_label", rs.getString(3));
        assertFalse(rs.next());
      }
    }
  }

  @Test
  public void testVectorDeleteRemovesIndexRow() throws Exception {
    String tableName = "T_VEC_DEL_" + generateUniqueName();
    String indexName = "IDX_VEC_DEL_" + generateUniqueName();

    try (Connection conn = DriverManager.getConnection(getUrl())) {
      setupTableAndKnownCentroids(conn, tableName, indexName);

      PhoenixConnection pconn = conn.unwrap(PhoenixConnection.class);
      PTable indexTable = pconn.getTableNoCache(indexName);

      // Upsert a row with vector [1,0,0,0] -> centroid ID 2
      String upsertSql = "UPSERT INTO " + tableName + " (ID, V, LABEL) VALUES (?, ?, ?)";
      try (PreparedStatement ps = conn.prepareStatement(upsertSql)) {
        ps.setString(1, "row_1");
        ps.setArray(2, conn.createArrayOf("FLOAT", new Float[] { 1.0f, 0.0f, 0.0f, 0.0f }));
        ps.setString(3, "lbl_1");
        ps.executeUpdate();
      }
      conn.commit();

      List<byte[]> rowKeysBefore = getHBaseRowKeys(pconn, indexTable);
      assertEquals(1, rowKeysBefore.size());

      // Delete the base table row
      try (Statement stmt = conn.createStatement()) {
        stmt.execute("DELETE FROM " + tableName + " WHERE ID = 'row_1'");
      }
      conn.commit();

      // Verify index table is completely empty
      List<byte[]> rowKeysAfter = getHBaseRowKeys(pconn, indexTable);
      assertEquals("Expected 0 index rows after delete", 0, rowKeysAfter.size());

      // Verify via SQL scan
      String selectSql = "SELECT COUNT(*) FROM " + indexName;
      try (Statement stmt = conn.createStatement(); ResultSet rs = stmt.executeQuery(selectSql)) {
        assertTrue(rs.next());
        assertEquals(0, rs.getInt(1));
      }
    }
  }

  @Test
  public void testNullVectorExcludedFromIndex() throws Exception {
    String tableName = "T_VEC_NULL_" + generateUniqueName();
    String indexName = "IDX_VEC_NULL_" + generateUniqueName();

    try (Connection conn = DriverManager.getConnection(getUrl())) {
      setupTableAndKnownCentroids(conn, tableName, indexName);

      PhoenixConnection pconn = conn.unwrap(PhoenixConnection.class);
      PTable indexTable = pconn.getTableNoCache(indexName);

      // Upsert a row with a null vector
      String upsertSql = "UPSERT INTO " + tableName + " (ID, V, LABEL) VALUES (?, ?, ?)";
      try (PreparedStatement ps = conn.prepareStatement(upsertSql)) {
        ps.setString(1, "row_null");
        ps.setNull(2, java.sql.Types.ARRAY);
        ps.setString(3, "null_label");
        ps.executeUpdate();
      }
      conn.commit();

      // Verify index table has 0 rows (null vector excluded from index)
      List<byte[]> rowKeys = getHBaseRowKeys(pconn, indexTable);
      assertEquals("Null vector must not produce an index row", 0, rowKeys.size());

      // Verify base table has the row
      try (Statement stmt = conn.createStatement(); ResultSet rs =
        stmt.executeQuery("SELECT ID, LABEL FROM " + tableName + " WHERE ID = 'row_null'")) {
        assertTrue("Base table must contain the null vector row", rs.next());
        assertEquals("row_null", rs.getString(1));
        assertEquals("null_label", rs.getString(2));
      }
    }
  }

  @Test
  public void testVectorUnchangedUpdateMaintainsIndexRowAndCoveredColumns() throws Exception {
    String tableName = "T_VEC_ZUPD_" + generateUniqueName();
    String indexName = "IDX_VEC_ZUPD_" + generateUniqueName();

    try (Connection conn = DriverManager.getConnection(getUrl())) {
      setupTableAndKnownCentroids(conn, tableName, indexName);

      PhoenixConnection pconn = conn.unwrap(PhoenixConnection.class);
      PTable indexTable = pconn.getTableNoCache(indexName);

      // Insert initial row
      String upsertSql = "UPSERT INTO " + tableName + " (ID, V, LABEL) VALUES (?, ?, ?)";
      try (PreparedStatement ps = conn.prepareStatement(upsertSql)) {
        ps.setString(1, "row_alloc_u");
        ps.setArray(2, conn.createArrayOf("FLOAT", new Float[] { 1.0f, 0.0f, 0.0f, 0.0f }));
        ps.setString(3, "lbl_initial");
        ps.executeUpdate();
      }
      conn.commit();

      // Verify initial index row
      List<byte[]> rowKeysBefore = getHBaseRowKeys(pconn, indexTable);
      assertEquals(1, rowKeysBefore.size());
      assertEquals(2, extractCentroidId(rowKeysBefore.get(0)));

      // 1. Partial update modifying only the covered non-vector column
      try (Statement stmt = conn.createStatement()) {
        stmt.execute(
          "UPSERT INTO " + tableName + " (ID, LABEL) VALUES ('row_alloc_u', 'lbl_updated')");
      }
      conn.commit();

      // Verify row key is maintained in place (not deleted or recreated)
      List<byte[]> rowKeysAfterPartial = getHBaseRowKeys(pconn, indexTable);
      assertEquals("Expected exactly 1 index row after partial update", 1,
        rowKeysAfterPartial.size());
      assertEquals(2, extractCentroidId(rowKeysAfterPartial.get(0)));

      // Verify unchanged vector column values are preserved and not deleted during partial updates
      PColumn indexVecCol = indexTable.getColumnForColumnName("0:V");
      try (
        Table hTable = pconn.getQueryServices().getTable(indexTable.getPhysicalName().getBytes())) {
        Result r = hTable.get(new Get(rowKeysAfterPartial.get(0)));
        byte[] storedVec =
          r.getValue(indexVecCol.getFamilyName().getBytes(), indexVecCol.getColumnQualifierBytes());
        assertNotNull("index row must still carry the vector after a covered-only update",
          storedVec);
        assertArrayEquals(PVectorFloat.INSTANCE.toBytes(new float[] { 1.0f, 0.0f, 0.0f, 0.0f }),
          storedVec);
      }

      // 2. Full update with identical vector
      try (PreparedStatement ps = conn.prepareStatement(upsertSql)) {
        ps.setString(1, "row_alloc_u");
        ps.setArray(2, conn.createArrayOf("FLOAT", new Float[] { 1.0f, 0.0f, 0.0f, 0.0f }));
        ps.setString(3, "lbl_updated_again");
        ps.executeUpdate();
      }
      conn.commit();

      // Verify row in index table is maintained with centroid prefix 2
      List<byte[]> rowKeys = getHBaseRowKeys(pconn, indexTable);
      assertEquals("Expected exactly 1 index row after full unchanged update", 1, rowKeys.size());
      assertEquals(2, extractCentroidId(rowKeys.get(0)));

      // Verify covered column was updated in place
      String selectSql = "SELECT \":CENTROID_ID\", \":ID\", \"0:LABEL\" FROM " + indexName;
      try (Statement stmt = conn.createStatement(); ResultSet rs = stmt.executeQuery(selectSql)) {
        assertTrue(rs.next());
        assertEquals(2, rs.getInt(1));
        assertEquals("row_alloc_u", rs.getString(2));
        assertEquals("lbl_updated_again", rs.getString(3));
        assertFalse(rs.next());
      }
    }
  }

  @Test
  public void testVectorCoveredColumnTranscoding() throws Exception {
    String tableName = "T_VEC_COV_TR_" + generateUniqueName();
    String indexName = "IDX_VEC_COV_TR_" + generateUniqueName();

    try (Connection conn = DriverManager.getConnection(getUrl())) {
      try (Statement stmt = conn.createStatement()) {
        stmt.execute("CREATE TABLE " + tableName
          + " (ID VARCHAR NOT NULL PRIMARY KEY, V VECTOR(FLOAT, 4), COV_V VECTOR(FLOAT, 3))");
        stmt.execute("CREATE VECTOR INDEX " + indexName + " ON " + tableName + " (V) "
          + "INCLUDE (COV_V) WITH (algorithm = 'IVF', metric = 'L2', lists = 4, sample_size = 100)");
      }

      List<float[]> knownCentroids = Arrays.asList(new float[] { 0.0f, 0.0f, 0.0f, 1.0f }, // ID 0
        new float[] { 0.0f, 0.0f, 1.0f, 0.0f }, // ID 1
        new float[] { 1.0f, 0.0f, 0.0f, 0.0f }, // ID 2
        new float[] { 0.0f, 1.0f, 0.0f, 0.0f } // ID 3
      );
      CentroidManager.persistCentroidsFromFloatList(conn, indexName, 1L, knownCentroids);
      CentroidManager.setGenerationAndLists(conn, indexName, 1L, 4);

      PhoenixConnection pconn = conn.unwrap(PhoenixConnection.class);
      IndexUtil.updateIndexState(pconn, indexName, PIndexState.ACTIVE, 0L);
      pconn.removeTable(pconn.getTenantId(), indexName, null, HConstants.LATEST_TIMESTAMP);
      pconn.removeTable(pconn.getTenantId(), tableName, null, HConstants.LATEST_TIMESTAMP);
      VectorCentroidCache.getInstance().putFloatCentroids(indexName, 1L, knownCentroids);

      // Upsert a row with both indexed vector V and covered vector COV_V
      String upsertSql = "UPSERT INTO " + tableName + " (ID, V, COV_V) VALUES (?, ?, ?)";
      try (PreparedStatement ps = conn.prepareStatement(upsertSql)) {
        ps.setString(1, "cov_row_1");
        ps.setArray(2, conn.createArrayOf("FLOAT", new Float[] { 0.0f, 0.0f, 1.0f, 0.0f })); // Nearest:
                                                                                             // ID 1
        ps.setArray(3, conn.createArrayOf("FLOAT", new Float[] { 1.5f, -2.5f, 3.5f }));
        ps.executeUpdate();
      }
      conn.commit();

      // Verify index table has the covered column transcoded correctly
      String selectSql = "SELECT \":CENTROID_ID\", \":ID\", \"0:COV_V\" FROM " + indexName;
      try (Statement stmt = conn.createStatement(); ResultSet rs = stmt.executeQuery(selectSql)) {
        assertTrue(rs.next());
        assertEquals(1, rs.getInt(1));
        assertEquals("cov_row_1", rs.getString(2));
        Object obj = rs.getObject(3);
        assertNotNull(obj);
        assertTrue("Expected float[] from getObject on VECTOR(FLOAT) column",
          obj instanceof float[]);
        float[] actualFloats = (float[]) obj;
        assertEquals(3, actualFloats.length);
        assertEquals(1.5f, actualFloats[0], 1e-6f);
        assertEquals(-2.5f, actualFloats[1], 1e-6f);
        assertEquals(3.5f, actualFloats[2], 1e-6f);
        assertFalse(rs.next());
      }

      // Update covered vector while main vector is unchanged
      try (PreparedStatement ps = conn.prepareStatement(upsertSql)) {
        ps.setString(1, "cov_row_1");
        ps.setArray(2, conn.createArrayOf("FLOAT", new Float[] { 0.0f, 0.0f, 1.0f, 0.0f }));
        ps.setArray(3, conn.createArrayOf("FLOAT", new Float[] { 9.0f, 8.0f, 7.0f }));
        ps.executeUpdate();
      }
      conn.commit();

      // Verify covered vector was updated in the index row
      try (Statement stmt = conn.createStatement(); ResultSet rs = stmt.executeQuery(selectSql)) {
        assertTrue(rs.next());
        assertEquals(1, rs.getInt(1));
        assertEquals("cov_row_1", rs.getString(2));
        Object obj = rs.getObject(3);
        assertNotNull(obj);
        assertTrue("Expected float[] from getObject on VECTOR(FLOAT) column",
          obj instanceof float[]);
        float[] actualFloats = (float[]) obj;
        assertEquals(3, actualFloats.length);
        assertEquals(9.0f, actualFloats[0], 1e-6f);
        assertEquals(8.0f, actualFloats[1], 1e-6f);
        assertEquals(7.0f, actualFloats[2], 1e-6f);
        assertFalse(rs.next());
      }
    }
  }

  @Test
  public void testMixedVectorCoveredColumnTranscoding() throws Exception {
    String tableName = "T_VEC_MIX_TR_" + generateUniqueName();
    String indexName = "IDX_VEC_MIX_TR_" + generateUniqueName();

    try (Connection conn = DriverManager.getConnection(getUrl())) {
      try (Statement stmt = conn.createStatement()) {
        stmt.execute("CREATE TABLE " + tableName
          + " (ID VARCHAR NOT NULL PRIMARY KEY, V VECTOR(FLOAT, 4), COV_D VECTOR(DOUBLE, 3))");
        stmt.execute("CREATE VECTOR INDEX " + indexName + " ON " + tableName + " (V) "
          + "INCLUDE (COV_D) WITH (algorithm = 'IVF', metric = 'L2', lists = 4, sample_size = 100)");
      }

      List<float[]> knownCentroids = Arrays.asList(new float[] { 0.0f, 0.0f, 0.0f, 1.0f }, // ID 0
        new float[] { 0.0f, 0.0f, 1.0f, 0.0f }, // ID 1
        new float[] { 1.0f, 0.0f, 0.0f, 0.0f }, // ID 2
        new float[] { 0.0f, 1.0f, 0.0f, 0.0f } // ID 3
      );
      CentroidManager.persistCentroidsFromFloatList(conn, indexName, 1L, knownCentroids);
      CentroidManager.setGenerationAndLists(conn, indexName, 1L, 4);

      PhoenixConnection pconn = conn.unwrap(PhoenixConnection.class);
      IndexUtil.updateIndexState(pconn, indexName, PIndexState.ACTIVE, 0L);
      pconn.removeTable(pconn.getTenantId(), indexName, null, HConstants.LATEST_TIMESTAMP);
      pconn.removeTable(pconn.getTenantId(), tableName, null, HConstants.LATEST_TIMESTAMP);
      VectorCentroidCache.getInstance().putFloatCentroids(indexName, 1L, knownCentroids);

      // Upsert a row with indexed VECTOR(FLOAT, 4) and covered VECTOR(DOUBLE, 3)
      String upsertSql = "UPSERT INTO " + tableName + " (ID, V, COV_D) VALUES (?, ?, ?)";
      try (PreparedStatement ps = conn.prepareStatement(upsertSql)) {
        ps.setString(1, "mix_row_1");
        ps.setArray(2, conn.createArrayOf("FLOAT", new Float[] { 0.0f, 0.0f, 1.0f, 0.0f })); // Nearest:
                                                                                             // ID 1
        ps.setArray(3, conn.createArrayOf("DOUBLE",
          new Double[] { 1.12345678901234, -2.98765432109876, 3.14159265358979 }));
        ps.executeUpdate();
      }
      conn.commit();

      // Verify index table has the covered double vector transcoded correctly with 8-byte element
      // stride
      String selectSql = "SELECT \":CENTROID_ID\", \":ID\", \"0:COV_D\" FROM " + indexName;
      try (Statement stmt = conn.createStatement(); ResultSet rs = stmt.executeQuery(selectSql)) {
        assertTrue(rs.next());
        assertEquals(1, rs.getInt(1));
        assertEquals("mix_row_1", rs.getString(2));
        Object obj = rs.getObject(3);
        assertNotNull(obj);
        assertTrue("Expected double[] from getObject on VECTOR(DOUBLE) column",
          obj instanceof double[]);
        double[] actualDoubles = (double[]) obj;
        assertEquals(3, actualDoubles.length);
        assertEquals(1.12345678901234, actualDoubles[0], 1e-12);
        assertEquals(-2.98765432109876, actualDoubles[1], 1e-12);
        assertEquals(3.14159265358979, actualDoubles[2], 1e-12);
        assertFalse(rs.next());
      }

      // Update covered double vector while main float vector is unchanged
      try (PreparedStatement ps = conn.prepareStatement(upsertSql)) {
        ps.setString(1, "mix_row_1");
        ps.setArray(2, conn.createArrayOf("FLOAT", new Float[] { 0.0f, 0.0f, 1.0f, 0.0f }));
        ps.setArray(3, conn.createArrayOf("DOUBLE",
          new Double[] { 9.87654321012345, 8.76543210987654, 7.65432109876543 }));
        ps.executeUpdate();
      }
      conn.commit();

      // Verify covered double vector was updated in the index row
      try (Statement stmt = conn.createStatement(); ResultSet rs = stmt.executeQuery(selectSql)) {
        assertTrue(rs.next());
        assertEquals(1, rs.getInt(1));
        assertEquals("mix_row_1", rs.getString(2));
        Object obj = rs.getObject(3);
        assertNotNull(obj);
        assertTrue("Expected double[] from getObject on VECTOR(DOUBLE) column",
          obj instanceof double[]);
        double[] actualDoubles = (double[]) obj;
        assertEquals(3, actualDoubles.length);
        assertEquals(9.87654321012345, actualDoubles[0], 1e-12);
        assertEquals(8.76543210987654, actualDoubles[1], 1e-12);
        assertEquals(7.65432109876543, actualDoubles[2], 1e-12);
        assertFalse(rs.next());
      }
    }
  }

  @Test
  public void testVerifiedMarkerPresentAfterCommit() throws Exception {
    String tableName = "T_VEC_VER_" + generateUniqueName();
    String indexName = "IDX_VEC_VER_" + generateUniqueName();

    try (Connection conn = DriverManager.getConnection(getUrl())) {
      setupTableAndKnownCentroids(conn, tableName, indexName);

      PhoenixConnection pconn = conn.unwrap(PhoenixConnection.class);
      PTable indexTable = pconn.getTableNoCache(indexName);
      byte[] physicalIndexName = indexTable.getPhysicalName().getBytes();

      // Upsert a row with vector [1,0,0,0] (nearest centroid is ID 2)
      String upsertSql = "UPSERT INTO " + tableName + " (ID, V, LABEL) VALUES (?, ?, ?)";
      try (PreparedStatement ps = conn.prepareStatement(upsertSql)) {
        ps.setString(1, "row_v1");
        ps.setArray(2, conn.createArrayOf("FLOAT", new Float[] { 1.0f, 0.0f, 0.0f, 0.0f }));
        ps.setString(3, "lbl_v1");
        ps.executeUpdate();
      }
      conn.commit();

      byte[] emptyCF = SchemaUtil.getEmptyColumnFamily(indexTable);
      byte[] emptyCQ = EncodedColumnsUtil.getEmptyKeyValueInfo(indexTable).getFirst();

      // Scan index table raw via HBase Table API (bypassing GlobalIndexChecker)
      try (Table hTable = pconn.getQueryServices().getTable(physicalIndexName);
        ResultScanner scanner = hTable.getScanner(new Scan())) {
        Result result = scanner.next();
        assertNotNull("Expected index row in HBase table", result);

        byte[] emptyColVal = result.getValue(emptyCF, emptyCQ);
        assertNotNull("Empty column marker cell must be present on index row", emptyColVal);
        assertTrue("Empty column marker must be VERIFIED_BYTES",
          Bytes.equals(QueryConstants.VERIFIED_BYTES, emptyColVal));

        assertNull("Expected exactly 1 index row", scanner.next());
      }

      // Also verify via IndexTestUtil
      IndexTestUtil.assertRowsForEmptyColValue(conn, indexName, QueryConstants.VERIFIED_BYTES);
    }
  }

  @Test
  public void testReadRepairWithCentroidReassignment() throws Exception {
    String tableName = "T_VEC_RR_" + generateUniqueName();
    String indexName = "IDX_VEC_RR_" + generateUniqueName();

    try (Connection conn = DriverManager.getConnection(getUrl())) {
      setupTableAndKnownCentroids(conn, tableName, indexName);

      PhoenixConnection pconn = conn.unwrap(PhoenixConnection.class);
      PTable indexTable = pconn.getTableNoCache(indexName);
      byte[] physicalIndexName = indexTable.getPhysicalName().getBytes();

      // Upsert a row in data table with vector [1,0,0,0] -> nearest centroid is ID 2
      String upsertSql = "UPSERT INTO " + tableName + " (ID, V, LABEL) VALUES (?, ?, ?)";
      try (PreparedStatement ps = conn.prepareStatement(upsertSql)) {
        ps.setString(1, "repair_row_1");
        ps.setArray(2, conn.createArrayOf("FLOAT", new Float[] { 1.0f, 0.0f, 0.0f, 0.0f }));
        ps.setString(3, "correct_label");
        ps.executeUpdate();
      }
      conn.commit();

      byte[] centroid0RowKey =
        ByteUtil.concat(PInteger.INSTANCE.toBytes(0), Bytes.toBytes("repair_row_1"));
      byte[] centroid2RowKey =
        ByteUtil.concat(PInteger.INSTANCE.toBytes(2), Bytes.toBytes("repair_row_1"));

      byte[] emptyCF = SchemaUtil.getEmptyColumnFamily(indexTable);
      byte[] emptyCQ = EncodedColumnsUtil.getEmptyKeyValueInfo(indexTable).getFirst();
      PColumn labelCol = indexTable.getColumnForColumnName("0:LABEL");
      byte[] labelCF = labelCol.getFamilyName().getBytes();
      byte[] labelCQ = labelCol.getColumnQualifierBytes();

      // Verify that after commit, Centroid 2 row exists and is verified
      try (Table hIndexTable = pconn.getQueryServices().getTable(physicalIndexName)) {
        Result r2 = hIndexTable.get(new Get(centroid2RowKey));
        assertFalse("Centroid 2 row should exist after commit", r2.isEmpty());
        assertTrue(Bytes.equals(QueryConstants.VERIFIED_BYTES, r2.getValue(emptyCF, emptyCQ)));

        // Simulate a prior failed write with a stale unverified row and subsequent deletion marker.
        // Use strictly ascending timestamps to ensure read repair is not masked by the tombstone.
        long deleteTs = EnvironmentEdgeManager.currentTimeMillis();
        hIndexTable.delete(new Delete(centroid2RowKey, deleteTs));

        long staleTs = deleteTs + 1;
        Put stalePut = new Put(centroid0RowKey);
        stalePut.addColumn(emptyCF, emptyCQ, staleTs, QueryConstants.UNVERIFIED_BYTES);
        stalePut.addColumn(labelCF, labelCQ, staleTs, Bytes.toBytes("stale_label"));
        hIndexTable.put(stalePut);

        // Verify pre-repair HBase state:
        // Centroid 0 exists and is unverified, Centroid 2 does not exist
        Result r0Before = hIndexTable.get(new Get(centroid0RowKey));
        assertFalse("Centroid 0 row must exist before repair", r0Before.isEmpty());
        assertTrue("Centroid 0 row must be unverified",
          Bytes.equals(QueryConstants.UNVERIFIED_BYTES, r0Before.getValue(emptyCF, emptyCQ)));

        Result r2Before = hIndexTable.get(new Get(centroid2RowKey));
        assertTrue("Centroid 2 row must not exist before repair", r2Before.isEmpty());
      }

      // Read through the index: this must trigger read repair in GlobalIndexChecker
      String selectSql = "SELECT \":CENTROID_ID\", \":ID\", \"0:LABEL\" FROM " + indexName;
      try (Statement stmt = conn.createStatement(); ResultSet rs = stmt.executeQuery(selectSql)) {
        assertTrue("Query through index should return the repaired row", rs.next());
        assertEquals("Repaired centroid ID must be 2", 2, rs.getInt(1));
        assertEquals("repair_row_1", rs.getString(2));
        assertEquals("correct_label", rs.getString(3));
        assertFalse("Only one row should be returned", rs.next());
      }

      // Verify post-repair HBase state:
      // Stale Centroid 0 row was deleted, correct Centroid 2 row was created with VERIFIED_BYTES
      try (Table hIndexTable = pconn.getQueryServices().getTable(physicalIndexName)) {
        Result r0After = hIndexTable.get(new Get(centroid0RowKey));
        assertTrue("Stale Centroid 0 row must be deleted after read repair", r0After.isEmpty());

        Result r2After = hIndexTable.get(new Get(centroid2RowKey));
        assertFalse("Repaired Centroid 2 row must exist after read repair", r2After.isEmpty());
        assertTrue("Repaired Centroid 2 row must have VERIFIED marker",
          Bytes.equals(QueryConstants.VERIFIED_BYTES, r2After.getValue(emptyCF, emptyCQ)));
        assertEquals("Repaired row must have correct label", "correct_label",
          Bytes.toString(r2After.getValue(labelCF, labelCQ)));
      }
    }
  }

  private int findNearestCentroid(float[] v, List<float[]> centroids) {
    int bestId = -1;
    double bestDistSq = Double.MAX_VALUE;
    for (int c = 0; c < centroids.size(); c++) {
      float[] centroid = centroids.get(c);
      double distSq = 0.0;
      for (int d = 0; d < v.length; d++) {
        double diff = v[d] - centroid[d];
        distSq += diff * diff;
      }
      if (distSq < bestDistSq) {
        bestDistSq = distSq;
        bestId = c;
      }
    }
    return bestId;
  }

  @Test
  public void testIndexToolPopulation() throws Exception {
    String tableName = "T_VEC_IT_" + generateUniqueName();
    String indexName = "IDX_VEC_IT_" + generateUniqueName();

    try (Connection conn = DriverManager.getConnection(getUrl())) {
      try (Statement stmt = conn.createStatement()) {
        stmt.execute("CREATE TABLE " + tableName
          + " (ID VARCHAR NOT NULL PRIMARY KEY, V VECTOR(FLOAT, 4), LABEL VARCHAR)");
      }

      // Upsert 500 rows with 4D float vectors and commit
      Random rng = new Random(42);
      float[][] rawVectors = new float[500][4];
      String upsertSql = "UPSERT INTO " + tableName + " (ID, V, LABEL) VALUES (?, ?, ?)";
      try (PreparedStatement ps = conn.prepareStatement(upsertSql)) {
        for (int i = 0; i < 500; i++) {
          String id = String.format("row_%03d", i);
          Float[] vec = new Float[4];
          for (int d = 0; d < 4; d++) {
            float val = rng.nextFloat() * 10f;
            vec[d] = val;
            rawVectors[i][d] = val;
          }
          ps.setString(1, id);
          ps.setArray(2, conn.createArrayOf("FLOAT", vec));
          ps.setString(3, "label_" + i);
          ps.executeUpdate();
        }
      }
      conn.commit();

      // Create ASYNC vector index with lists = 4
      try (Statement stmt = conn.createStatement()) {
        stmt.execute("CREATE VECTOR INDEX " + indexName + " ON " + tableName + " (V) "
          + "INCLUDE (LABEL) WITH (algorithm = 'IVF', metric = 'L2', lists = 4, sample_size = 100) ASYNC");
      }

      // Populate 4 known orthogonal centroids in SYSTEM.VECTOR_CENTROID (generation 1)
      List<float[]> knownCentroids = Arrays.asList(new float[] { 10.0f, 0.0f, 0.0f, 0.0f },
        new float[] { 0.0f, 10.0f, 0.0f, 0.0f }, new float[] { 0.0f, 0.0f, 10.0f, 0.0f },
        new float[] { 0.0f, 0.0f, 0.0f, 10.0f });
      CentroidManager.persistCentroidsFromFloatList(conn, indexName, 1L, knownCentroids);
      CentroidManager.setGenerationAndLists(conn, indexName, 1L, 4);

      VectorCentroidCache.resetInstance();

      // Assert index table initially has 0 rows and state is BUILDING
      PhoenixConnection pconn = conn.unwrap(PhoenixConnection.class);
      PTable pIndex = pconn.getTableNoCache(indexName);
      assertEquals(PIndexState.BUILDING, pIndex.getIndexState());
      List<byte[]> initialRowKeys = getHBaseRowKeys(pconn, pIndex);
      assertTrue("Index table should initially have 0 rows", initialRowKeys.isEmpty());

      // Run IndexTool with -runfg
      IndexTool indexingTool = new IndexTool();
      Configuration conf = new Configuration(getUtility().getConfiguration());
      indexingTool.setConf(conf);
      String[] args = new String[] { "-dt", tableName, "-it", indexName, "-op",
        "/tmp/" + UUID.randomUUID().toString(), "-runfg" };
      int status = indexingTool.run(args);
      assertEquals(0, status);
      assertEquals(PhoenixIndexImportDirectMapper.class, indexingTool.getJob().getMapperClass());

      // Assert index state transitioned to ACTIVE
      pconn.removeTable(pconn.getTenantId(), indexName, null, HConstants.LATEST_TIMESTAMP);
      pIndex = pconn.getTableNoCache(indexName);
      assertEquals(PIndexState.ACTIVE, pIndex.getIndexState());

      // Assert index table contains exactly 500 rows in HBase
      byte[] physicalIndexName = pIndex.getPhysicalName().getBytes();
      byte[] emptyCF = SchemaUtil.getEmptyColumnFamily(pIndex);
      byte[] emptyCQ = EncodedColumnsUtil.getEmptyKeyValueInfo(pIndex).getFirst();
      int hbaseRowCount = 0;
      try (Table hIndexTable = pconn.getQueryServices().getTable(physicalIndexName)) {
        Scan scan = new Scan();
        try (ResultScanner scanner = hIndexTable.getScanner(scan)) {
          for (Result r : scanner) {
            hbaseRowCount++;
            byte[] rowKey = r.getRow();
            int centroidId = extractCentroidId(rowKey);
            String idStr = (String) PVarchar.INSTANCE.toObject(rowKey, Bytes.SIZEOF_INT,
              rowKey.length - Bytes.SIZEOF_INT);
            int rowIdx = Integer.parseInt(idStr.replace("row_", ""));
            int expectedCentroid = findNearestCentroid(rawVectors[rowIdx], knownCentroids);
            assertEquals("Centroid ID for " + idStr + " mismatch", expectedCentroid, centroidId);

            byte[] emptyVal = r.getValue(emptyCF, emptyCQ);
            assertNotNull("Empty column must exist", emptyVal);
            assertTrue("Index row must be VERIFIED",
              Bytes.equals(QueryConstants.VERIFIED_BYTES, emptyVal));
          }
        }
      }
      assertEquals(500, hbaseRowCount);

      // Verify query via SQL on the index
      String selectSql = "SELECT \":CENTROID_ID\", \":ID\", \"0:LABEL\" FROM " + indexName;
      int countFromSelect = 0;
      try (Statement s = conn.createStatement(); ResultSet rs = s.executeQuery(selectSql)) {
        while (rs.next()) {
          countFromSelect++;
          assertNotNull(rs.getString(2));
          assertNotNull(rs.getString(3));
        }
      }
      assertEquals(500, countFromSelect);
    }
  }

  @Test
  public void testIndexToolAutoTrainAndPopulate() throws Exception {
    String tableName = "T_VEC_AUTO_" + generateUniqueName();
    String indexName = "IDX_VEC_AUTO_" + generateUniqueName();

    try (Connection conn = DriverManager.getConnection(getUrl())) {
      try (Statement stmt = conn.createStatement()) {
        stmt.execute("CREATE TABLE " + tableName
          + " (ID VARCHAR NOT NULL PRIMARY KEY, V VECTOR(FLOAT, 4), LABEL VARCHAR)");
      }

      // Upsert 100 rows with 4D float vectors
      Random rng = new Random(123);
      String upsertSql = "UPSERT INTO " + tableName + " (ID, V, LABEL) VALUES (?, ?, ?)";
      try (PreparedStatement ps = conn.prepareStatement(upsertSql)) {
        for (int i = 0; i < 100; i++) {
          Float[] vec = new Float[] { rng.nextFloat() * 5f, rng.nextFloat() * 5f,
            rng.nextFloat() * 5f, rng.nextFloat() * 5f };
          ps.setString(1, String.format("row_%03d", i));
          ps.setArray(2, conn.createArrayOf("FLOAT", vec));
          ps.setString(3, "label_" + i);
          ps.executeUpdate();
        }
      }
      conn.commit();

      // Create ASYNC vector index without pre-populating centroids
      try (Statement stmt = conn.createStatement()) {
        stmt.execute("CREATE VECTOR INDEX " + indexName + " ON " + tableName + " (V) "
          + "INCLUDE (LABEL) WITH (algorithm = 'IVF', metric = 'L2', lists = 4, sample_size = 100) ASYNC");
      }

      VectorCentroidCache.resetInstance();

      // Run IndexTool: will auto-train centroids during job configuration
      IndexTool indexingTool = new IndexTool();
      Configuration conf = new Configuration(getUtility().getConfiguration());
      indexingTool.setConf(conf);
      String[] args = new String[] { "-dt", tableName, "-it", indexName, "-op",
        "/tmp/" + UUID.randomUUID().toString(), "-runfg" };
      int status = indexingTool.run(args);
      assertEquals(0, status);

      // Verify centroids were auto-trained and persisted to SYSTEM.VECTOR_CENTROID
      List<byte[]> trainedCentroids = CentroidManager.loadCentroids(conn, indexName, 1L);
      assertEquals("Auto-training should produce 4 centroids", 4, trainedCentroids.size());

      // Assert index state transitioned to ACTIVE
      PhoenixConnection pconn = conn.unwrap(PhoenixConnection.class);
      pconn.removeTable(pconn.getTenantId(), indexName, null, HConstants.LATEST_TIMESTAMP);
      PTable pIndex = pconn.getTableNoCache(indexName);
      assertEquals(PIndexState.ACTIVE, pIndex.getIndexState());

      // Assert index table contains 100 rows
      List<byte[]> rowKeys = getHBaseRowKeys(pconn, pIndex);
      assertEquals(100, rowKeys.size());
    }
  }

  @Test
  public void testIndexToolRebuildWithGeneration() throws Exception {
    String tableName = "T_VEC_REBUILD_" + generateUniqueName();
    String indexName = "IDX_VEC_REBUILD_" + generateUniqueName();

    try (Connection conn = DriverManager.getConnection(getUrl())) {
      try (Statement stmt = conn.createStatement()) {
        stmt.execute("CREATE TABLE " + tableName
          + " (ID VARCHAR NOT NULL PRIMARY KEY, V VECTOR(FLOAT, 4), LABEL VARCHAR)");
      }

      // Upsert 100 rows
      Random rng = new Random(456);
      String upsertSql = "UPSERT INTO " + tableName + " (ID, V, LABEL) VALUES (?, ?, ?)";
      try (PreparedStatement ps = conn.prepareStatement(upsertSql)) {
        for (int i = 0; i < 100; i++) {
          Float[] vec = new Float[] { rng.nextFloat() * 5f, rng.nextFloat() * 5f,
            rng.nextFloat() * 5f, rng.nextFloat() * 5f };
          ps.setString(1, String.format("row_%03d", i));
          ps.setArray(2, conn.createArrayOf("FLOAT", vec));
          ps.setString(3, "label_" + i);
          ps.executeUpdate();
        }
      }
      conn.commit();

      try (Statement stmt = conn.createStatement()) {
        stmt.execute("CREATE VECTOR INDEX " + indexName + " ON " + tableName + " (V) "
          + "INCLUDE (LABEL) WITH (algorithm = 'IVF', metric = 'L2', lists = 4, sample_size = 100) ASYNC");
      }

      // Populate generation 2 centroids
      List<float[]> gen2Centroids = Arrays.asList(new float[] { 100.0f, 0.0f, 0.0f, 0.0f },
        new float[] { 0.0f, 100.0f, 0.0f, 0.0f }, new float[] { 0.0f, 0.0f, 100.0f, 0.0f },
        new float[] { 0.0f, 0.0f, 0.0f, 100.0f });
      CentroidManager.persistCentroidsFromFloatList(conn, indexName, 2L, gen2Centroids);
      CentroidManager.setGenerationAndLists(conn, indexName, 2L, 4);

      VectorCentroidCache.resetInstance();

      // Run IndexTool with -g 2
      IndexTool indexingTool = new IndexTool();
      Configuration conf = new Configuration(getUtility().getConfiguration());
      indexingTool.setConf(conf);
      String[] args = new String[] { "-dt", tableName, "-it", indexName, "-g", "2", "-op",
        "/tmp/" + UUID.randomUUID().toString(), "-runfg" };
      int status = indexingTool.run(args);
      assertEquals(0, status);

      // Verify index table has 100 rows
      PhoenixConnection pconn = conn.unwrap(PhoenixConnection.class);
      pconn.removeTable(pconn.getTenantId(), indexName, null, HConstants.LATEST_TIMESTAMP);
      PTable pIndex = pconn.getTableNoCache(indexName);
      assertEquals(PIndexState.ACTIVE, pIndex.getIndexState());

      List<byte[]> rowKeys = getHBaseRowKeys(pconn, pIndex);
      assertEquals(100, rowKeys.size());
    }
  }
}
