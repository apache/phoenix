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

import static org.apache.phoenix.jdbc.PhoenixDatabaseMetaData.COLUMN_COUNT_BYTES;
import static org.apache.phoenix.jdbc.PhoenixDatabaseMetaData.COLUMN_SIZE_BYTES;
import static org.apache.phoenix.jdbc.PhoenixDatabaseMetaData.DATA_TABLE_NAME_BYTES;
import static org.apache.phoenix.jdbc.PhoenixDatabaseMetaData.DATA_TYPE_BYTES;
import static org.apache.phoenix.jdbc.PhoenixDatabaseMetaData.INDEX_TYPE_BYTES;
import static org.apache.phoenix.jdbc.PhoenixDatabaseMetaData.NULLABLE_BYTES;
import static org.apache.phoenix.jdbc.PhoenixDatabaseMetaData.ORDINAL_POSITION_BYTES;
import static org.apache.phoenix.jdbc.PhoenixDatabaseMetaData.TABLE_FAMILY_BYTES;
import static org.apache.phoenix.jdbc.PhoenixDatabaseMetaData.TABLE_SEQ_NUM_BYTES;
import static org.apache.phoenix.jdbc.PhoenixDatabaseMetaData.TABLE_TYPE_BYTES;
import static org.apache.phoenix.jdbc.PhoenixDatabaseMetaData.VECTOR_DIMENSION_BYTES;
import static org.apache.phoenix.jdbc.PhoenixDatabaseMetaData.VECTOR_DISTANCE_METRIC_BYTES;
import static org.apache.phoenix.jdbc.PhoenixDatabaseMetaData.VECTOR_INDEX_ALGORITHM_BYTES;
import static org.apache.phoenix.jdbc.PhoenixDatabaseMetaData.VECTOR_IVF_LISTS_BYTES;
import static org.apache.phoenix.jdbc.PhoenixDatabaseMetaData.VECTOR_IVF_SAMPLE_SIZE_BYTES;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import org.apache.hadoop.hbase.client.Mutation;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.client.coprocessor.Batch;
import org.apache.hadoop.hbase.ipc.CoprocessorRpcUtils.BlockingRpcCallback;
import org.apache.hadoop.hbase.ipc.ServerRpcController;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.phoenix.coprocessor.generated.MetaDataProtos;
import org.apache.phoenix.coprocessor.generated.MetaDataProtos.CreateTableRequest;
import org.apache.phoenix.coprocessor.generated.MetaDataProtos.MetaDataResponse;
import org.apache.phoenix.coprocessor.generated.MetaDataProtos.MetaDataService;
import org.apache.phoenix.coprocessorclient.MetaDataProtocol;
import org.apache.phoenix.exception.SQLExceptionCode;
import org.apache.phoenix.hbase.index.util.VersionUtil;
import org.apache.phoenix.jdbc.PhoenixConnection;
import org.apache.phoenix.jdbc.PhoenixDatabaseMetaData;
import org.apache.phoenix.protobuf.ProtobufUtil;
import org.apache.phoenix.schema.PTable;
import org.apache.phoenix.schema.PTable.IndexType;
import org.apache.phoenix.schema.PTableImpl;
import org.apache.phoenix.schema.PTableType;
import org.apache.phoenix.schema.types.PBson;
import org.apache.phoenix.schema.types.PDataType;
import org.apache.phoenix.schema.types.PInteger;
import org.apache.phoenix.schema.types.PLong;
import org.apache.phoenix.schema.types.PVarchar;
import org.apache.phoenix.schema.types.PVectorFloat;
import org.apache.phoenix.util.ByteUtil;
import org.apache.phoenix.util.ClientUtil;
import org.apache.phoenix.util.Closeables;
import org.apache.phoenix.util.IndexUtil;
import org.apache.phoenix.util.SchemaUtil;
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
}
