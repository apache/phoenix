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
import static org.apache.phoenix.jdbc.PhoenixDatabaseMetaData.CLUSTER_SIZE;
import static org.apache.phoenix.jdbc.PhoenixDatabaseMetaData.COLUMN_COUNT_BYTES;
import static org.apache.phoenix.jdbc.PhoenixDatabaseMetaData.COLUMN_SIZE_BYTES;
import static org.apache.phoenix.jdbc.PhoenixDatabaseMetaData.DATA_TABLE_NAME_BYTES;
import static org.apache.phoenix.jdbc.PhoenixDatabaseMetaData.DATA_TYPE_BYTES;
import static org.apache.phoenix.jdbc.PhoenixDatabaseMetaData.GENERATION_ID;
import static org.apache.phoenix.jdbc.PhoenixDatabaseMetaData.INDEX_NAME;
import static org.apache.phoenix.jdbc.PhoenixDatabaseMetaData.INDEX_TYPE_BYTES;
import static org.apache.phoenix.jdbc.PhoenixDatabaseMetaData.LAST_SCORECARD_UPDATE;
import static org.apache.phoenix.jdbc.PhoenixDatabaseMetaData.NULLABLE_BYTES;
import static org.apache.phoenix.jdbc.PhoenixDatabaseMetaData.ORDINAL_POSITION_BYTES;
import static org.apache.phoenix.jdbc.PhoenixDatabaseMetaData.REASSIGN_COUNT;
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
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Mutation;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.RegionInfo;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.client.coprocessor.Batch;
import org.apache.hadoop.hbase.ipc.CoprocessorRpcUtils.BlockingRpcCallback;
import org.apache.hadoop.hbase.ipc.ServerRpcController;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.phoenix.cache.VectorCentroidCache;
import org.apache.phoenix.coprocessor.TaskRegionObserver;
import org.apache.phoenix.coprocessor.generated.MetaDataProtos;
import org.apache.phoenix.coprocessor.generated.MetaDataProtos.CreateTableRequest;
import org.apache.phoenix.coprocessor.generated.MetaDataProtos.MetaDataResponse;
import org.apache.phoenix.coprocessor.generated.MetaDataProtos.MetaDataService;
import org.apache.phoenix.coprocessor.tasks.VectorIndexRebuildTask;
import org.apache.phoenix.coprocessorclient.MetaDataProtocol;
import org.apache.phoenix.exception.SQLExceptionCode;
import org.apache.phoenix.execute.VectorIndexScanPlan;
import org.apache.phoenix.hbase.index.covered.update.ColumnReference;
import org.apache.phoenix.hbase.index.util.VersionUtil;
import org.apache.phoenix.index.IndexMaintainer;
import org.apache.phoenix.index.vector.CentroidManager;
import org.apache.phoenix.index.vector.GenerationSummary;
import org.apache.phoenix.index.vector.ScorecardAccumulator;
import org.apache.phoenix.index.vector.ScorecardRow;
import org.apache.phoenix.index.vector.VectorIndexScorecard;
import org.apache.phoenix.index.vector.VectorIndexScorecard.DriftEvaluationResult;
import org.apache.phoenix.jdbc.PhoenixConnection;
import org.apache.phoenix.jdbc.PhoenixDatabaseMetaData;
import org.apache.phoenix.jdbc.PhoenixPreparedStatement;
import org.apache.phoenix.protobuf.ProtobufUtil;
import org.apache.phoenix.query.QueryServices;
import org.apache.phoenix.schema.PColumn;
import org.apache.phoenix.schema.PIndexState;
import org.apache.phoenix.schema.PTable;
import org.apache.phoenix.schema.PTable.IndexType;
import org.apache.phoenix.schema.PTableImpl;
import org.apache.phoenix.schema.PTableType;
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
import org.apache.phoenix.util.IndexUtil;
import org.apache.phoenix.util.PropertiesUtil;
import org.apache.phoenix.util.QueryUtil;
import org.apache.phoenix.util.ReadOnlyProps;
import org.apache.phoenix.util.SchemaUtil;
import org.apache.phoenix.util.StringUtil;
import org.apache.phoenix.util.TestUtil;
import org.bson.BinaryVector;
import org.bson.BsonBinary;
import org.bson.BsonBinarySubType;
import org.bson.BsonDocument;
import org.bson.BsonNull;
import org.bson.BsonString;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;

/** Integration tests for vector index metadata validation and lifecycle management. */
@Category(ParallelStatsDisabledTest.class)
public class VectorIndexIT extends ParallelStatsDisabledIT {

  private static String SHARED_VEC128_TABLE;
  private static String SHARED_VARCHAR_TABLE;
  private static String SHARED_BSON_TABLE;

  @BeforeClass
  public static synchronized void doSetup() throws Exception {
    setUpTestDriver(ReadOnlyProps.EMPTY_PROPS);
    SHARED_VEC128_TABLE = "T_SHARED_VEC128_" + generateUniqueName();
    SHARED_VARCHAR_TABLE = "T_SHARED_VARCHAR_" + generateUniqueName();
    SHARED_BSON_TABLE = "T_SHARED_BSON_" + generateUniqueName();

    try (Connection conn = DriverManager.getConnection(getUrl());
      Statement stmt = conn.createStatement()) {
      stmt.execute("CREATE TABLE " + SHARED_VEC128_TABLE
        + " (ID VARCHAR NOT NULL PRIMARY KEY, V VECTOR(FLOAT, 128))");
      stmt.execute("CREATE TABLE " + SHARED_VARCHAR_TABLE
        + " (ID VARCHAR NOT NULL PRIMARY KEY, NON_VEC_COL VARCHAR)");
      stmt.execute(
        "CREATE TABLE " + SHARED_BSON_TABLE + " (ID VARCHAR NOT NULL PRIMARY KEY, DOC BSON)");
    }
  }

  @Test
  public void testNonVectorColumnRejection() throws Exception {
    String indexName = "IDX_NON_VEC_" + generateUniqueName();

    try (Connection conn = DriverManager.getConnection(getUrl())) {
      PhoenixConnection pconn = conn.unwrap(PhoenixConnection.class);

      try {
        createVectorIndexMetadata(pconn, null, indexName, SHARED_VARCHAR_TABLE, "NON_VEC_COL",
          PVarchar.INSTANCE, null, "IVF", "L2", 128, 16, 500);
        fail("Expected vector index creation on VARCHAR column to be rejected");
      } catch (Exception e) {
        assertExpectedSqlException(e, SQLExceptionCode.VECTOR_INDEX_ON_NON_VECTOR_TYPE);
      }
    }
  }

  @Test
  public void testDimensionMismatchRejection() throws Exception {
    String indexName = "IDX_DIM_MISMATCH_" + generateUniqueName();

    try (Connection conn = DriverManager.getConnection(getUrl())) {
      PhoenixConnection pconn = conn.unwrap(PhoenixConnection.class);

      try {
        createVectorIndexMetadata(pconn, null, indexName, SHARED_VEC128_TABLE, "V",
          PVectorFloat.INSTANCE, 256, "IVF", "L2", 256, 16, 500);
        fail("Expected vector index creation with mismatched dimension to be rejected");
      } catch (Exception e) {
        assertExpectedSqlException(e, SQLExceptionCode.VECTOR_INDEX_DIMENSION_MISMATCH);
      }
    }
  }

  @Test
  public void testInvalidAlgorithmRejection() throws Exception {
    String indexName = "IDX_INVALID_ALGO_" + generateUniqueName();

    try (Connection conn = DriverManager.getConnection(getUrl())) {
      PhoenixConnection pconn = conn.unwrap(PhoenixConnection.class);

      try {
        createVectorIndexMetadata(pconn, null, indexName, SHARED_VEC128_TABLE, "V",
          PVectorFloat.INSTANCE, 128, "UNKNOWN", "L2", 128, 16, 500);
        fail("Expected vector index creation with UNKNOWN algorithm to be rejected");
      } catch (Exception e) {
        assertExpectedSqlException(e, SQLExceptionCode.UNSUPPORTED_VECTOR_INDEX_ALGORITHM);
      }
    }
  }

  @Test
  public void testInvalidDistanceMetricRejection() throws Exception {
    String indexName = "IDX_INVALID_METRIC_" + generateUniqueName();

    try (Connection conn = DriverManager.getConnection(getUrl())) {
      PhoenixConnection pconn = conn.unwrap(PhoenixConnection.class);

      try {
        createVectorIndexMetadata(pconn, null, indexName, SHARED_VEC128_TABLE, "V",
          PVectorFloat.INSTANCE, 128, "IVF", "MANHATTAN", 128, 16, 500);
        fail("Expected vector index creation with unsupported metric to be rejected");
      } catch (Exception e) {
        assertExpectedSqlException(e, SQLExceptionCode.UNSUPPORTED_VECTOR_DISTANCE_METRIC);
      }
    }
  }

  @Test
  public void testInvalidIvfParametersZeroListsRejection() throws Exception {
    String indexName = "IDX_IVF_ZERO_LISTS_" + generateUniqueName();

    try (Connection conn = DriverManager.getConnection(getUrl())) {
      PhoenixConnection pconn = conn.unwrap(PhoenixConnection.class);

      try {
        createVectorIndexMetadata(pconn, null, indexName, SHARED_VEC128_TABLE, "V",
          PVectorFloat.INSTANCE, 128, "IVF", "L2", 128, 0, 500);
        fail("Expected vector index creation with vectorIvfLists = 0 to be rejected");
      } catch (Exception e) {
        assertExpectedSqlException(e, SQLExceptionCode.INVALID_VECTOR_INDEX_PARAMS);
      }
    }
  }

  @Test
  public void testInvalidIvfParametersSampleSizeLessThanListsRejection() throws Exception {
    String indexName = "IDX_IVF_SAMPLE_TOO_SMALL_" + generateUniqueName();

    try (Connection conn = DriverManager.getConnection(getUrl())) {
      PhoenixConnection pconn = conn.unwrap(PhoenixConnection.class);

      try {
        createVectorIndexMetadata(pconn, null, indexName, SHARED_VEC128_TABLE, "V",
          PVectorFloat.INSTANCE, 128, "IVF", "L2", 128, 32, 16);
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
    String indexName = "IDX_MISSING_COL_" + generateUniqueName();

    try (Connection conn = DriverManager.getConnection(getUrl())) {
      PhoenixConnection pconn = conn.unwrap(PhoenixConnection.class);

      try {
        createVectorIndexMetadata(pconn, null, indexName, SHARED_VEC128_TABLE, "NON_EXISTENT_COL",
          PVectorFloat.INSTANCE, 128, "IVF", "L2", 128, 16, 500);
        fail("Expected vector index creation on non-existent column to be rejected");
      } catch (Exception e) {
        assertExpectedSqlException(e, SQLExceptionCode.COLUMN_NOT_FOUND);
      }
    }
  }

  @Test
  public void testValidVectorIndexMetadataCreation() throws Exception {
    String indexName = "IDX_VALID_" + generateUniqueName();

    try (Connection conn = DriverManager.getConnection(getUrl())) {
      PhoenixConnection pconn = conn.unwrap(PhoenixConnection.class);

      MetaDataResponse response = createVectorIndexMetadata(pconn, null, indexName,
        SHARED_VEC128_TABLE, "V", PVectorFloat.INSTANCE, 128, "IVF", "L2", 128, 16, 500);
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
    String indexName = "IDX_BSON_" + generateUniqueName();

    try (Connection conn = DriverManager.getConnection(getUrl())) {
      PhoenixConnection pconn = conn.unwrap(PhoenixConnection.class);

      MetaDataResponse response = createVectorIndexMetadata(pconn, null, indexName,
        SHARED_BSON_TABLE, "DOC", PBson.INSTANCE, null, "IVF", "COSINE", 256, 32, 1000);
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

  // --- Phase 10: BSON vector extraction ---

  private static final int BSON_DIM = 8;

  /** Constructs a BSON binary vector payload of subtype 9, FLOAT32, little-endian. */
  private static BsonBinary bsonVector(float[] v) {
    return new BsonBinary(BinaryVector.floatVector(v));
  }

  /** Generates a test vector with the specified first component. */
  private static float[] vecX(float x) {
    float[] v = new float[BSON_DIM];
    v[0] = x;
    return v;
  }

  private static BsonDocument embeddingDoc(float[] v, String category) {
    BsonDocument doc = new BsonDocument("embedding", bsonVector(v));
    doc.put("category", new BsonString(category));
    return doc;
  }

  private static void upsertDoc(Connection conn, String table, String id, BsonDocument doc)
    throws SQLException {
    try (PreparedStatement ps = conn.prepareStatement("UPSERT INTO " + table + " VALUES (?, ?)")) {
      ps.setString(1, id);
      ps.setObject(2, doc);
      ps.executeUpdate();
    }
    conn.commit();
  }

  private static int countRows(Connection conn, String table) throws SQLException {
    try (Statement stmt = conn.createStatement();
      ResultSet rs = stmt.executeQuery("SELECT COUNT(*) FROM " + table)) {
      assertTrue(rs.next());
      return rs.getInt(1);
    }
  }

  private static Float[] boxed(float[] v) {
    Float[] b = new Float[v.length];
    for (int i = 0; i < v.length; i++) {
      b[i] = v[i];
    }
    return b;
  }

  private static List<String> runSearch(Connection conn, String sql, float[] q)
    throws SQLException {
    List<String> ids = new ArrayList<>();
    try (PreparedStatement ps = conn.prepareStatement(sql)) {
      ps.setArray(1, conn.createArrayOf("FLOAT", boxed(q)));
      try (ResultSet rs = ps.executeQuery()) {
        while (rs.next()) {
          ids.add(rs.getString(1));
        }
      }
    }
    return ids;
  }

  private static String explain(Connection conn, String sql, float[] q) throws SQLException {
    try (PreparedStatement ps = conn.prepareStatement("EXPLAIN " + sql)) {
      ps.setArray(1, conn.createArrayOf("FLOAT", boxed(q)));
      return QueryUtil.getExplainPlan(ps.executeQuery());
    }
  }

  /**
   * Defines four test centroids spaced along the primary dimension for deterministic partitioning.
   */
  private static List<float[]> fourCentroids() {
    List<float[]> centroids = new ArrayList<>();
    for (int c = 0; c < 4; c++) {
      centroids.add(vecX(c * 10.0f));
    }
    return centroids;
  }

  /** Maps index row keys to centroid identifiers and row keys for validation. */
  private static Map<String, Integer> indexCentroidById(PhoenixConnection pconn, String indexName)
    throws Exception {
    PTable indexTable = pconn.getTableNoCache(indexName);
    Map<String, Integer> byId = new HashMap<>();
    for (byte[] rk : VectorIndexTestUtil.getHBaseRowKeys(pconn, indexTable)) {
      int cid = VectorIndexTestUtil.extractCentroidId(rk, false);
      String id =
        (String) PVarchar.INSTANCE.toObject(rk, Bytes.SIZEOF_INT, rk.length - Bytes.SIZEOF_INT);
      byId.put(id, cid);
    }
    return byId;
  }

  @Test
  public void testBsonFunctionalVectorIndexWriteLifecycle() throws Exception {
    String tableName = "T_BSON_FUNC_" + generateUniqueName();
    String indexName = "IDX_BSON_FUNC_" + generateUniqueName();

    try (Connection conn = DriverManager.getConnection(getUrl())) {
      try (Statement stmt = conn.createStatement()) {
        stmt.execute("CREATE TABLE " + tableName + " (ID VARCHAR NOT NULL PRIMARY KEY, DOC BSON)");
        stmt.execute("CREATE VECTOR INDEX " + indexName + " ON " + tableName
          + " (BSON_VECTOR_VALUE(doc, 'embedding', " + BSON_DIM
          + ")) WITH (algorithm = 'IVF', metric = 'L2', lists = 4, sample_size = 100)");
      }
      List<float[]> centroids = fourCentroids();
      VectorIndexTestUtil.activateWithKnownCentroids(conn, tableName, indexName, centroids, 1L);
      PhoenixConnection pconn = conn.unwrap(PhoenixConnection.class);

      // Verify that functional vector indexes configure a functional vector column reference,
      // while standard column indexes do not.
      PTable dataTable = pconn.getTableNoCache(tableName);
      PTable indexTable = pconn.getTableNoCache(indexName);
      IndexMaintainer maintainer = indexTable.getIndexMaintainer(dataTable, pconn);
      assertNotNull("BSON functional index must carry a functional vector column ref",
        maintainer.getFunctionalVectorColRef());
      String colTable = "T_COL_VEC_" + generateUniqueName();
      String colIndex = "IDX_COL_VEC_" + generateUniqueName();
      try (Statement stmt = conn.createStatement()) {
        stmt.execute("CREATE TABLE " + colTable
          + " (ID VARCHAR NOT NULL PRIMARY KEY, V VECTOR(FLOAT, " + BSON_DIM + "))");
        stmt.execute("CREATE VECTOR INDEX " + colIndex + " ON " + colTable
          + " (V) WITH (algorithm = 'IVF', metric = 'L2', lists = 4, sample_size = 100)");
      }
      PTable colDataTable = pconn.getTableNoCache(colTable);
      assertNull("column vector index must not carry a functional vector column ref",
        pconn.getTableNoCache(colIndex).getIndexMaintainer(colDataTable, pconn)
          .getFunctionalVectorColRef());

      // Upsert rows across centroids along with documents containing missing or null vectors.
      Map<String, float[]> vectors = new LinkedHashMap<>();
      for (int i = 0; i < 40; i++) {
        float[] v = vecX(i);
        vectors.put("id_" + i, v);
        upsertDoc(conn, tableName, "id_" + i, embeddingDoc(v, "c" + (i % 3)));
      }
      for (int i = 0; i < 5; i++) {
        upsertDoc(conn, tableName, "noemb_" + i,
          new BsonDocument("category", new BsonString("none")));
      }
      upsertDoc(conn, tableName, "nullemb", new BsonDocument("embedding", BsonNull.VALUE));
      assertEquals(46, countRows(conn, tableName));
      assertEquals(40, countRows(conn, indexName));

      // Verify rows are partitioned under their nearest centroid.
      Map<String, Integer> byId = indexCentroidById(pconn, indexName);
      assertEquals(40, byId.size());
      for (Map.Entry<String, float[]> e : vectors.entrySet()) {
        int expected = VectorIndexTestUtil.nearestCentroid(e.getValue(), centroids, "L2");
        assertEquals("centroid of " + e.getKey(), Integer.valueOf(expected), byId.get(e.getKey()));
      }

      // Verify the stored index cell contains the transcoded PVectorFloat big-endian
      // representation.
      ColumnReference vecRef = maintainer.getFunctionalVectorColRef();
      byte[] physical = indexTable.getPhysicalName().getBytes();
      byte[] rowKeyOf7 = null;
      for (byte[] rk : VectorIndexTestUtil.getHBaseRowKeys(pconn, indexTable)) {
        String id =
          (String) PVarchar.INSTANCE.toObject(rk, Bytes.SIZEOF_INT, rk.length - Bytes.SIZEOF_INT);
        if ("id_7".equals(id)) {
          rowKeyOf7 = rk;
        }
      }
      assertNotNull(rowKeyOf7);
      try (Table hTable = pconn.getQueryServices().getTable(physical)) {
        Result r = hTable.get(new Get(rowKeyOf7));
        byte[] stored = r.getValue(vecRef.getFamily(), vecRef.getQualifier());
        assertNotNull("index row must carry the functional vector cell", stored);
        assertArrayEquals(PVectorFloat.INSTANCE.toBytes(vecX(7)), stored);
      }

      // Verify updates to non-vector fields do not reassign or mutate the index row.
      upsertDoc(conn, tableName, "id_7", embeddingDoc(vecX(7), "renamed"));
      assertEquals(40, countRows(conn, indexName));
      assertEquals(byId.get("id_7"), indexCentroidById(pconn, indexName).get("id_7"));

      // Verify vector updates correctly re-partition the index row to a new centroid.
      assertEquals(Integer.valueOf(1), byId.get("id_7"));
      upsertDoc(conn, tableName, "id_7", embeddingDoc(vecX(35.5f), "renamed"));
      vectors.put("id_7", vecX(35.5f));
      assertEquals(40, countRows(conn, indexName));
      assertEquals(Integer.valueOf(3), indexCentroidById(pconn, indexName).get("id_7"));

      // Verify removing the vector field removes the entry from the index.
      upsertDoc(conn, tableName, "id_8", new BsonDocument("category", new BsonString("gone")));
      vectors.remove("id_8");
      assertEquals(46, countRows(conn, tableName));
      assertEquals(39, countRows(conn, indexName));
      assertFalse(indexCentroidById(pconn, indexName).containsKey("id_8"));

      // Validate that malformed vector payloads fail mutation processing.
      BsonBinary[] malformed = new BsonBinary[] { bsonVector(new float[BSON_DIM + 1]),
        new BsonBinary(BinaryVector.int8Vector(new byte[BSON_DIM])),
        new BsonBinary(BsonBinarySubType.BINARY, bsonVector(vecX(1)).getData()) };
      for (BsonBinary bad : malformed) {
        try {
          upsertDoc(conn, tableName, "bad", new BsonDocument("embedding", bad));
          fail("Malformed embedding " + bad + " must be rejected at write time");
        } catch (SQLException expected) {
          conn.rollback();
        }
      }
      assertEquals(46, countRows(conn, tableName));
      assertEquals(39, countRows(conn, indexName));

      // Verify index scan results reflect all prior updates, removals, and centroid assignments.
      float[] q = vecX(35.3f);
      String sql = "SELECT ID FROM " + tableName + " ORDER BY L2_DISTANCE(BSON_VECTOR_VALUE(doc, "
        + "'embedding', " + BSON_DIM + "), ?) LIMIT 3";
      assertTrue(explain(conn, sql, q).contains(indexName));
      assertEquals(VectorIndexTestUtil.bruteForceTopK(vectors, q, "L2", 3),
        runSearch(conn, sql, q));
    }
  }

  @Test
  public void testBsonFunctionalVectorIndexSearch() throws Exception {
    String tableName = "T_BSON_QUERY_" + generateUniqueName();
    String indexName = "IDX_BSON_QUERY_" + generateUniqueName();

    try (Connection conn = DriverManager.getConnection(getUrl())) {
      try (Statement stmt = conn.createStatement()) {
        stmt.execute("CREATE TABLE " + tableName + " (ID VARCHAR NOT NULL PRIMARY KEY, DOC BSON)");
        stmt.execute("CREATE VECTOR INDEX " + indexName + " ON " + tableName
          + " (BSON_VECTOR_VALUE(doc, 'embedding', " + BSON_DIM
          + ")) WITH (algorithm = 'IVF', metric = 'L2', lists = 4, sample_size = 100)");
      }
      VectorIndexTestUtil.activateWithKnownCentroids(conn, tableName, indexName, fourCentroids(),
        1L);

      // Upsert test vectors spanning centroid boundaries to validate multi-centroid probing and
      // distance ordering.
      Map<String, float[]> vectors = new LinkedHashMap<>();
      float[] xs = { 1f, 4f, 6f, 9f, 11f, 14f, 16f, 19f, 21f, 24f, 26f, 29f };
      for (int i = 0; i < xs.length; i++) {
        float[] v = vecX(xs[i]);
        v[1] = 0.1f * i;
        vectors.put("r" + i, v);
        upsertDoc(conn, tableName, "r" + i, embeddingDoc(v, "c" + (i % 2)));
      }

      String sql = "SELECT ID FROM " + tableName + " ORDER BY L2_DISTANCE(BSON_VECTOR_VALUE(doc, "
        + "'embedding', " + BSON_DIM + "), ?) LIMIT 3";
      for (float qx : new float[] { 5f, 15f, 25f, 0f }) {
        float[] q = vecX(qx);
        String plan = explain(conn, sql, q);
        assertTrue("Plan must use the BSON functional vector index: " + plan,
          plan.contains(indexName) && plan.contains("CLIENT PROBING"));
        assertEquals("query x=" + qx, VectorIndexTestUtil.bruteForceTopK(vectors, q, "L2", 3),
          runSearch(conn, sql, q));
      }

      // Verify distance function operand commutativity.
      String flipped = "SELECT ID FROM " + tableName
        + " ORDER BY L2_DISTANCE(?, BSON_VECTOR_VALUE(doc, 'embedding', " + BSON_DIM + ")) LIMIT 3";
      float[] q = vecX(15f);
      assertTrue(explain(conn, flipped, q).contains(indexName));
      assertEquals(VectorIndexTestUtil.bruteForceTopK(vectors, q, "L2", 3),
        runSearch(conn, flipped, q));
    }
  }

  @Test
  public void testBsonVectorIndexWithCoveredVectorColumnOfAnotherDimension() throws Exception {
    // Verify that index dimension validation targets the indexed expression rather than
    // unrelated covered vector columns.
    String tableName = "T_BSON_COV_" + generateUniqueName();
    String indexName = "IDX_BSON_COV_" + generateUniqueName();

    try (Connection conn = DriverManager.getConnection(getUrl())) {
      try (Statement stmt = conn.createStatement()) {
        stmt.execute("CREATE TABLE " + tableName
          + " (ID VARCHAR NOT NULL PRIMARY KEY, DOC BSON, V4 VECTOR(FLOAT, 4))");
        stmt.execute("CREATE VECTOR INDEX " + indexName + " ON " + tableName
          + " (BSON_VECTOR_VALUE(doc, 'embedding', " + BSON_DIM + ")) INCLUDE (V4)"
          + " WITH (algorithm = 'IVF', metric = 'L2', lists = 2, sample_size = 100)");
      }

      PhoenixConnection pconn = conn.unwrap(PhoenixConnection.class);
      PTable indexTable = pconn.getTableNoCache(indexName);
      assertEquals(Integer.valueOf(BSON_DIM), indexTable.getVectorDimension());
      // Ensure the maintainer indexes the functional expression rather than covered vector columns.
      IndexMaintainer maintainer =
        indexTable.getIndexMaintainer(pconn.getTableNoCache(tableName), pconn);
      assertNotNull("BSON functional index must carry a functional vector column ref",
        maintainer.getFunctionalVectorColRef());

      VectorIndexTestUtil.activateWithKnownCentroids(conn, tableName, indexName,
        Arrays.asList(vecX(0f), vecX(10f)), 1L);

      try (PreparedStatement ps =
        conn.prepareStatement("UPSERT INTO " + tableName + " VALUES (?, ?, ?)")) {
        ps.setString(1, "r1");
        ps.setObject(2, new BsonDocument("embedding", bsonVector(vecX(11f))));
        ps.setArray(3, conn.createArrayOf("FLOAT", new Float[] { 1f, 2f, 3f, 4f }));
        ps.executeUpdate();
      }
      conn.commit();

      assertEquals(1, countRows(conn, indexName));
      assertEquals(Integer.valueOf(1), indexCentroidById(pconn, indexName).get("r1"));
    }
  }

  @Test
  public void testBsonVectorIndexNotUsedForDifferentExpression() throws Exception {
    // Ensure queries ordering by different paths, dimensions, or unindexed vector columns
    // do not route to this index.
    String tableName = "T_BSON_OTHER_" + generateUniqueName();
    String indexName = "IDX_BSON_OTHER_" + generateUniqueName();

    try (Connection conn = DriverManager.getConnection(getUrl())) {
      try (Statement stmt = conn.createStatement()) {
        stmt.execute("CREATE TABLE " + tableName
          + " (ID VARCHAR NOT NULL PRIMARY KEY, DOC BSON, V VECTOR(FLOAT, " + BSON_DIM + "))");
        stmt.execute("CREATE VECTOR INDEX " + indexName + " ON " + tableName
          + " (BSON_VECTOR_VALUE(doc, 'embedding', " + BSON_DIM + ")) INCLUDE (DOC, V)"
          + " WITH (algorithm = 'IVF', metric = 'L2', lists = 2, sample_size = 100)");
      }
      VectorIndexTestUtil.activateWithKnownCentroids(conn, tableName, indexName,
        Arrays.asList(vecX(0f), vecX(10f)), 1L);

      // Upsert test vectors with distinct distributions across paths to verify query plan
      // isolation.
      String[] ids = { "A1", "A2", "A3", "B1", "B2", "B3" };
      float[] embX = { 2f, 3f, 4f, 6f, 7f, 8f };
      float[] otherX = { 100f, 101f, 102f, 0f, 1f, 2f };
      Map<String, float[]> otherRows = new LinkedHashMap<>();
      try (PreparedStatement ps =
        conn.prepareStatement("UPSERT INTO " + tableName + " VALUES (?, ?, ?)")) {
        for (int i = 0; i < ids.length; i++) {
          BsonDocument doc = new BsonDocument("embedding", bsonVector(vecX(embX[i])));
          doc.put("other", bsonVector(vecX(otherX[i])));
          ps.setString(1, ids[i]);
          ps.setObject(2, doc);
          ps.setArray(3, conn.createArrayOf("FLOAT", boxed(vecX(otherX[i]))));
          ps.executeUpdate();
          otherRows.put(ids[i], vecX(otherX[i]));
        }
        conn.commit();
      }

      float[] q = vecX(0f);
      List<String> expected = VectorIndexTestUtil.bruteForceTopK(otherRows, q, "L2", 2);
      String[] sqls = {
        "SELECT ID FROM " + tableName + " ORDER BY L2_DISTANCE(BSON_VECTOR_VALUE(doc, 'other', "
          + BSON_DIM + "), ?) LIMIT 2",
        "SELECT ID FROM " + tableName + " ORDER BY L2_DISTANCE(V, ?) LIMIT 2" };
      for (String sql : sqls) {
        String plan = explain(conn, sql, q);
        assertFalse("Index on 'embedding' must not serve: " + sql + "\n" + plan,
          plan.contains(indexName));
        assertEquals(sql, expected, runSearch(conn, sql, q));
      }

      // Verify that queries with mismatched vector dimensions bypass the index.
      String dimSql =
        "SELECT ID FROM " + tableName + " ORDER BY L2_DISTANCE(BSON_VECTOR_VALUE(doc, 'embedding', "
          + (BSON_DIM + 1) + "), ?) LIMIT 2";
      assertFalse(explain(conn, dimSql, new float[BSON_DIM + 1]).contains(indexName));

      // Verify matching vector expression queries successfully utilize the index.
      Map<String, float[]> embRows = new LinkedHashMap<>();
      for (int i = 0; i < ids.length; i++) {
        embRows.put(ids[i], vecX(embX[i]));
      }
      String embSql = "SELECT ID FROM " + tableName
        + " ORDER BY L2_DISTANCE(BSON_VECTOR_VALUE(doc, 'embedding', " + BSON_DIM + "), ?) LIMIT 2";
      assertTrue(explain(conn, embSql, q).contains(indexName));
      assertEquals(VectorIndexTestUtil.bruteForceTopK(embRows, q, "L2", 2),
        runSearch(conn, embSql, q));
    }
  }

  @Test
  public void testServerSideBsonVectorProjection() throws Exception {
    String tableName = "T_BSON_PROJ_" + generateUniqueName();
    try (Connection conn = DriverManager.getConnection(getUrl())) {
      try (Statement stmt = conn.createStatement()) {
        stmt.execute("CREATE TABLE " + tableName + " (ID VARCHAR NOT NULL PRIMARY KEY, DOC BSON)");
      }

      float[] v1 = new float[] { 1.5f, -2.5f, 3.0e-3f };
      float[] v2 = new float[] { 4.0f, 5.0f, 6.0f };
      upsertDoc(conn, tableName, "row1",
        new BsonDocument("data", new BsonDocument("vec", bsonVector(v1))));
      upsertDoc(conn, tableName, "row2",
        new BsonDocument("data", new BsonDocument("vec", bsonVector(v2))));
      upsertDoc(conn, tableName, "row3",
        new BsonDocument("data", new BsonDocument("other", new BsonString("hello"))));

      // Verify projection pushdown evaluates the extraction server-side.
      String projSql =
        "SELECT ID, BSON_VECTOR_VALUE(doc, 'data.vec', 3) FROM " + tableName + " ORDER BY ID";
      try (Statement stmt = conn.createStatement();
        ResultSet rs = stmt.executeQuery("EXPLAIN " + projSql)) {
        String plan = QueryUtil.getExplainPlan(rs);
        assertTrue("Expected server-side BSON projection: " + plan,
          plan.contains("SERVER BSON PROJECTION 1"));
        assertTrue("Expected the pushed down extraction to be disclosed: " + plan,
          plan.contains("BSON_VECTOR_VALUE(DOC, 'data.vec', 3)"));
      }
      try (Statement stmt = conn.createStatement(); ResultSet rs = stmt.executeQuery(projSql)) {
        assertTrue(rs.next());
        assertEquals("row1", rs.getString(1));
        assertArrayEquals(v1, (float[]) rs.getObject(2), 1e-6f);
        assertTrue(rs.next());
        assertEquals("row2", rs.getString(1));
        assertArrayEquals(v2, (float[]) rs.getObject(2), 1e-6f);
        assertTrue(rs.next());
        assertEquals("row3", rs.getString(1));
        assertNull(rs.getObject(2));
        assertTrue(rs.wasNull());
        assertFalse(rs.next());
      }

      // Verify server-side projection evaluation combined with filter predicates and distance
      // functions.
      String distSql = "SELECT L2_DISTANCE(BSON_VECTOR_VALUE(doc, 'data.vec', 3), ARRAY[1.5, -2.5,"
        + " 0.003]) FROM " + tableName + " WHERE ID = 'row1'";
      try (Statement stmt = conn.createStatement(); ResultSet rs = stmt.executeQuery(distSql)) {
        assertTrue(rs.next());
        assertEquals(0.0, rs.getDouble(1), 1e-6);
        assertFalse(rs.next());
      }

      // Verify client-side fallback behavior when the full document column is also projected.
      String fullDocSql = "SELECT doc, BSON_VECTOR_VALUE(doc, 'data.vec', 3) FROM " + tableName
        + " WHERE ID = 'row1'";
      try (Statement stmt = conn.createStatement();
        ResultSet rs = stmt.executeQuery("EXPLAIN " + fullDocSql)) {
        String plan = QueryUtil.getExplainPlan(rs);
        assertFalse("No server-side projection when the document is selected: " + plan,
          plan.contains("SERVER BSON PROJECTION"));
      }
      try (Statement stmt = conn.createStatement(); ResultSet rs = stmt.executeQuery(fullDocSql)) {
        assertTrue(rs.next());
        BsonDocument returnedDoc = (BsonDocument) rs.getObject(1);
        assertArrayEquals(v1,
          returnedDoc.getDocument("data").getBinary("vec").asVector().asFloat32Vector().getData(),
          0f);
        assertArrayEquals(v1, (float[]) rs.getObject(2), 1e-6f);
        assertFalse(rs.next());
      }

      // Verify server-side evaluation raises an exception on dimension mismatch.
      String badDim = "SELECT BSON_VECTOR_VALUE(doc, 'data.vec', 2) FROM " + tableName;
      try (Statement stmt = conn.createStatement(); ResultSet rs = stmt.executeQuery(badDim)) {
        while (rs.next()) {
          rs.getObject(1);
        }
        fail("Expected evaluation exception on dimension mismatch");
      } catch (SQLException e) {
        assertTrue("Unexpected message: " + e.getMessage(),
          e.getMessage().contains("dimension mismatch"));
      }
    }
  }

  @Test
  public void testSingleCellVectorIndexAdmissionAndQuery() throws Exception {
    String tableName = "T_VEC_SC_ADM_" + generateUniqueName();
    String indexName = "IDX_VEC_SC_ADM_" + generateUniqueName();
    try (Connection conn = DriverManager.getConnection(getUrl())) {
      try (Statement stmt = conn.createStatement()) {
        stmt.execute("CREATE TABLE " + tableName
          + " (ID VARCHAR NOT NULL PRIMARY KEY, V VECTOR(FLOAT, 4)) "
          + "IMMUTABLE_ROWS=true, IMMUTABLE_STORAGE_SCHEME=SINGLE_CELL_ARRAY_WITH_OFFSETS, COLUMN_ENCODED_BYTES=2");
        stmt.execute("CREATE VECTOR INDEX " + indexName + " ON " + tableName + " (V) "
          + "WITH (algorithm = 'IVF', metric = 'L2', lists = 2, sample_size = 100)");
      }

      PhoenixConnection pconn = conn.unwrap(PhoenixConnection.class);
      PTable indexTable = pconn.getTableNoCache(indexName);
      assertEquals("Index table must inherit SINGLE_CELL_ARRAY_WITH_OFFSETS",
        PTable.ImmutableStorageScheme.SINGLE_CELL_ARRAY_WITH_OFFSETS,
        indexTable.getImmutableStorageScheme());

      List<float[]> centroids = Arrays.asList(new float[] { 0.0f, 0.0f, 0.0f, 0.0f },
        new float[] { 10.0f, 0.0f, 0.0f, 0.0f });
      VectorIndexTestUtil.activateWithKnownCentroids(conn, tableName, indexName, centroids, 1L);

      Map<String, float[]> rows = new LinkedHashMap<>();
      rows.put("row_A1", new float[] { 1.0f, 0.0f, 0.0f, 0.0f });
      rows.put("row_A2", new float[] { 2.0f, 0.0f, 0.0f, 0.0f });
      rows.put("row_B1", new float[] { 9.0f, 0.0f, 0.0f, 0.0f });
      rows.put("row_B2", new float[] { 10.0f, 0.0f, 0.0f, 0.0f });

      String upsertSql = "UPSERT INTO " + tableName + " (ID, V) VALUES (?, ?)";
      try (PreparedStatement ps = conn.prepareStatement(upsertSql)) {
        for (Map.Entry<String, float[]> entry : rows.entrySet()) {
          ps.setString(1, entry.getKey());
          float[] v = entry.getValue();
          ps.setArray(2, conn.createArrayOf("FLOAT", new Float[] { v[0], v[1], v[2], v[3] }));
          ps.executeUpdate();
        }
        conn.commit();
      }

      float[] queryVec = new float[] { 0.5f, 0.0f, 0.0f, 0.0f };
      Float[] boxedQ = new Float[] { queryVec[0], queryVec[1], queryVec[2], queryVec[3] };
      int k = 2;
      List<String> expectedTopK = VectorIndexTestUtil.bruteForceTopK(rows, queryVec, "L2", k);

      String querySql = "SELECT ID FROM " + tableName + " ORDER BY L2_DISTANCE(V, ?) LIMIT " + k;
      try (PreparedStatement ps = conn.prepareStatement("EXPLAIN " + querySql)) {
        ps.setArray(1, conn.createArrayOf("FLOAT", boxedQ));
        try (ResultSet rs = ps.executeQuery()) {
          String plan = QueryUtil.getExplainPlan(rs);
          assertTrue("EXPLAIN plan must reference index " + indexName + ": " + plan,
            plan.contains(indexName));
          assertTrue("EXPLAIN plan must contain CLIENT PROBING: " + plan,
            plan.contains("CLIENT PROBING"));
        }
      }

      List<String> actualIds = new ArrayList<>();
      try (PreparedStatement ps = conn.prepareStatement(querySql)) {
        ps.setArray(1, conn.createArrayOf("FLOAT", boxedQ));
        try (ResultSet rs = ps.executeQuery()) {
          while (rs.next()) {
            actualIds.add(rs.getString(1));
          }
        }
      }
      assertEquals("Returned IDs must match brute-force top-k", expectedTopK, actualIds);
    }
  }

  @Test
  public void testAdaptiveProbeExpansionSatisfiesLimit() throws Exception {
    String tableName = "T_ADAPT_SAT_" + generateUniqueName();
    String indexName = "IDX_ADAPT_SAT_" + generateUniqueName();
    try (Connection conn = DriverManager.getConnection(getUrl())) {
      try (Statement stmt = conn.createStatement()) {
        stmt.execute("CREATE TABLE " + tableName
          + " (ID VARCHAR NOT NULL PRIMARY KEY, V VECTOR(FLOAT, 4), CATEGORY VARCHAR) "
          + "IMMUTABLE_ROWS=true");
        stmt.execute(
          "CREATE VECTOR INDEX " + indexName + " ON " + tableName + " (V) INCLUDE (CATEGORY) "
            + "WITH (algorithm = 'IVF', metric = 'L2', lists = 4, sample_size = 100)");
      }

      List<float[]> centroids = Arrays.asList(new float[] { 0.0f, 0.0f, 0.0f, 0.0f },
        new float[] { 10.0f, 0.0f, 0.0f, 0.0f }, new float[] { 20.0f, 0.0f, 0.0f, 0.0f },
        new float[] { 30.0f, 0.0f, 0.0f, 0.0f });
      VectorIndexTestUtil.activateWithKnownCentroids(conn, tableName, indexName, centroids, 1L);

      Map<String, float[]> rows = new LinkedHashMap<>();
      Map<String, String> categories = new LinkedHashMap<>();
      for (int c = 0; c < 4; c++) {
        float base = c * 10.0f;
        for (int a = 1; a <= 4; a++) {
          String id = "c" + c + "_a" + a;
          rows.put(id, new float[] { base + a * 0.1f, 0.0f, 0.0f, 0.0f });
          categories.put(id, "A");
        }
        String idB = "c" + c + "_b1";
        rows.put(idB, new float[] { base + 0.5f, 0.0f, 0.0f, 0.0f });
        categories.put(idB, "B");
      }

      String upsertSql = "UPSERT INTO " + tableName + " (ID, V, CATEGORY) VALUES (?, ?, ?)";
      try (PreparedStatement ps = conn.prepareStatement(upsertSql)) {
        for (Map.Entry<String, float[]> entry : rows.entrySet()) {
          ps.setString(1, entry.getKey());
          float[] v = entry.getValue();
          ps.setArray(2, conn.createArrayOf("FLOAT", new Float[] { v[0], v[1], v[2], v[3] }));
          ps.setString(3, categories.get(entry.getKey()));
          ps.executeUpdate();
        }
        conn.commit();
      }

      float[] queryVec = new float[] { 0.0f, 0.0f, 0.0f, 0.0f };
      Float[] boxedQ = new Float[] { queryVec[0], queryVec[1], queryVec[2], queryVec[3] };

      // Adaptive probe expansion continues across centroids until query limit is satisfied.
      String querySql = "SELECT /*+ VECTOR_PROBE_COUNT(1) */ ID, CATEGORY FROM " + tableName
        + " WHERE CATEGORY = 'B' ORDER BY L2_DISTANCE(V, ?) LIMIT 3";

      List<String> actualIds = new ArrayList<>();
      try (PhoenixPreparedStatement pps =
        conn.prepareStatement(querySql).unwrap(PhoenixPreparedStatement.class)) {
        pps.setArray(1, conn.createArrayOf("FLOAT", boxedQ));
        try (ResultSet rs = pps.executeQuery()) {
          while (rs.next()) {
            actualIds.add(rs.getString(1));
            assertEquals("B", rs.getString(2));
          }
        }
        VectorIndexScanPlan plan = (VectorIndexScanPlan) pps.getQueryPlan();
        assertEquals("Should probe 3 batches to satisfy LIMIT 3", 3,
          plan.getLastProbedBatchCount());
        assertEquals("Should probe 3 centroids", 3, plan.getLastProbedCentroidCount());
      }
      assertEquals("Expected top 3 B rows from c0, c1, c2",
        Arrays.asList("c0_b1", "c1_b1", "c2_b1"), actualIds);
    }
  }

  @Test
  public void testAdaptiveProbeExpansionMaxProbeLimit() throws Exception {
    String tableName = "T_ADAPT_MAX_" + generateUniqueName();
    String indexName = "IDX_ADAPT_MAX_" + generateUniqueName();
    try (Connection conn = DriverManager.getConnection(getUrl())) {
      try (Statement stmt = conn.createStatement()) {
        stmt.execute("CREATE TABLE " + tableName
          + " (ID VARCHAR NOT NULL PRIMARY KEY, V VECTOR(FLOAT, 4), CATEGORY VARCHAR) "
          + "IMMUTABLE_ROWS=true");
        stmt.execute(
          "CREATE VECTOR INDEX " + indexName + " ON " + tableName + " (V) INCLUDE (CATEGORY) "
            + "WITH (algorithm = 'IVF', metric = 'L2', lists = 4, sample_size = 100)");
      }

      List<float[]> centroids = Arrays.asList(new float[] { 0.0f, 0.0f, 0.0f, 0.0f },
        new float[] { 10.0f, 0.0f, 0.0f, 0.0f }, new float[] { 20.0f, 0.0f, 0.0f, 0.0f },
        new float[] { 30.0f, 0.0f, 0.0f, 0.0f });
      VectorIndexTestUtil.activateWithKnownCentroids(conn, tableName, indexName, centroids, 1L);

      Map<String, float[]> rows = new LinkedHashMap<>();
      Map<String, String> categories = new LinkedHashMap<>();
      for (int c = 0; c < 4; c++) {
        float base = c * 10.0f;
        for (int a = 1; a <= 4; a++) {
          String id = "c" + c + "_a" + a;
          rows.put(id, new float[] { base + a * 0.1f, 0.0f, 0.0f, 0.0f });
          categories.put(id, "A");
        }
        String idB = "c" + c + "_b1";
        rows.put(idB, new float[] { base + 0.5f, 0.0f, 0.0f, 0.0f });
        categories.put(idB, "B");
      }

      String upsertSql = "UPSERT INTO " + tableName + " (ID, V, CATEGORY) VALUES (?, ?, ?)";
      try (PreparedStatement ps = conn.prepareStatement(upsertSql)) {
        for (Map.Entry<String, float[]> entry : rows.entrySet()) {
          ps.setString(1, entry.getKey());
          float[] v = entry.getValue();
          ps.setArray(2, conn.createArrayOf("FLOAT", new Float[] { v[0], v[1], v[2], v[3] }));
          ps.setString(3, categories.get(entry.getKey()));
          ps.executeUpdate();
        }
        conn.commit();
      }

      float[] queryVec = new float[] { 0.0f, 0.0f, 0.0f, 0.0f };
      Float[] boxedQ = new Float[] { queryVec[0], queryVec[1], queryVec[2], queryVec[3] };

      // Query hint bounds probe expansion batches.
      String hintSql = "SELECT /*+ VECTOR_PROBE_COUNT(1) MAX_PROBE_LIMIT(2) */ ID, CATEGORY FROM "
        + tableName + " WHERE CATEGORY = 'B' ORDER BY L2_DISTANCE(V, ?) LIMIT 3";
      List<String> actualHintIds = new ArrayList<>();
      try (PhoenixPreparedStatement pps =
        conn.prepareStatement(hintSql).unwrap(PhoenixPreparedStatement.class)) {
        pps.setArray(1, conn.createArrayOf("FLOAT", boxedQ));
        try (ResultSet rs = pps.executeQuery()) {
          while (rs.next()) {
            actualHintIds.add(rs.getString(1));
          }
        }
        VectorIndexScanPlan plan = (VectorIndexScanPlan) pps.getQueryPlan();
        assertEquals(2, plan.getMaxProbeLimit());
        assertEquals("Max probe limit 2 must stop probing at 2 batches", 2,
          plan.getLastProbedBatchCount());
        assertEquals(2, plan.getLastProbedCentroidCount());
      }
      assertEquals("Partial result should contain first 2 candidates",
        Arrays.asList("c0_b1", "c1_b1"), actualHintIds);

      // Connection property bounds probe expansion batches.
      Properties sessionProps = new Properties();
      sessionProps.setProperty(QueryServices.VECTOR_MAX_PROBE_LIMIT_ATTRIB, "2");
      try (Connection conn2 = DriverManager.getConnection(getUrl(), sessionProps)) {
        String sessionSql = "SELECT /*+ VECTOR_PROBE_COUNT(1) */ ID, CATEGORY FROM " + tableName
          + " WHERE CATEGORY = 'B' ORDER BY L2_DISTANCE(V, ?) LIMIT 3";
        List<String> actualSessionIds = new ArrayList<>();
        try (PhoenixPreparedStatement pps =
          conn2.prepareStatement(sessionSql).unwrap(PhoenixPreparedStatement.class)) {
          pps.setArray(1, conn2.createArrayOf("FLOAT", boxedQ));
          try (ResultSet rs = pps.executeQuery()) {
            while (rs.next()) {
              actualSessionIds.add(rs.getString(1));
            }
          }
          VectorIndexScanPlan plan = (VectorIndexScanPlan) pps.getQueryPlan();
          assertEquals(2, plan.getMaxProbeLimit());
          assertEquals(2, plan.getLastProbedBatchCount());
          assertEquals(2, plan.getLastProbedCentroidCount());
        }
        assertEquals(Arrays.asList("c0_b1", "c1_b1"), actualSessionIds);
      }
    }
  }

  @Test
  public void testAdaptiveProbeExpansionAllCentroidsProbed() throws Exception {
    String tableName = "T_ADAPT_ALL_" + generateUniqueName();
    String indexName = "IDX_ADAPT_ALL_" + generateUniqueName();
    try (Connection conn = DriverManager.getConnection(getUrl())) {
      try (Statement stmt = conn.createStatement()) {
        stmt.execute("CREATE TABLE " + tableName
          + " (ID VARCHAR NOT NULL PRIMARY KEY, V VECTOR(FLOAT, 4), CATEGORY VARCHAR) "
          + "IMMUTABLE_ROWS=true");
        stmt.execute(
          "CREATE VECTOR INDEX " + indexName + " ON " + tableName + " (V) INCLUDE (CATEGORY) "
            + "WITH (algorithm = 'IVF', metric = 'L2', lists = 4, sample_size = 100)");
      }

      List<float[]> centroids = Arrays.asList(new float[] { 0.0f, 0.0f, 0.0f, 0.0f },
        new float[] { 10.0f, 0.0f, 0.0f, 0.0f }, new float[] { 20.0f, 0.0f, 0.0f, 0.0f },
        new float[] { 30.0f, 0.0f, 0.0f, 0.0f });
      VectorIndexTestUtil.activateWithKnownCentroids(conn, tableName, indexName, centroids, 1L);

      Map<String, float[]> rows = new LinkedHashMap<>();
      Map<String, String> categories = new LinkedHashMap<>();
      for (int c = 0; c < 4; c++) {
        float base = c * 10.0f;
        for (int a = 1; a <= 4; a++) {
          String id = "c" + c + "_a" + a;
          rows.put(id, new float[] { base + a * 0.1f, 0.0f, 0.0f, 0.0f });
          categories.put(id, "A");
        }
        String idB = "c" + c + "_b1";
        rows.put(idB, new float[] { base + 0.5f, 0.0f, 0.0f, 0.0f });
        categories.put(idB, "B");
      }

      String upsertSql = "UPSERT INTO " + tableName + " (ID, V, CATEGORY) VALUES (?, ?, ?)";
      try (PreparedStatement ps = conn.prepareStatement(upsertSql)) {
        for (Map.Entry<String, float[]> entry : rows.entrySet()) {
          ps.setString(1, entry.getKey());
          float[] v = entry.getValue();
          ps.setArray(2, conn.createArrayOf("FLOAT", new Float[] { v[0], v[1], v[2], v[3] }));
          ps.setString(3, categories.get(entry.getKey()));
          ps.executeUpdate();
        }
        conn.commit();
      }

      float[] queryVec = new float[] { 0.0f, 0.0f, 0.0f, 0.0f };
      Float[] boxedQ = new Float[] { queryVec[0], queryVec[1], queryVec[2], queryVec[3] };

      // Adaptive probing exhausts all centroids when candidate count remains below limit.
      String querySql = "SELECT /*+ VECTOR_PROBE_COUNT(1) */ ID, CATEGORY FROM " + tableName
        + " WHERE CATEGORY = 'B' ORDER BY L2_DISTANCE(V, ?) LIMIT 10";

      List<String> actualIds = new ArrayList<>();
      try (PhoenixPreparedStatement pps =
        conn.prepareStatement(querySql).unwrap(PhoenixPreparedStatement.class)) {
        pps.setArray(1, conn.createArrayOf("FLOAT", boxedQ));
        try (ResultSet rs = pps.executeQuery()) {
          while (rs.next()) {
            actualIds.add(rs.getString(1));
          }
        }
        VectorIndexScanPlan plan = (VectorIndexScanPlan) pps.getQueryPlan();
        assertEquals("Should probe all 4 centroid batches", 4, plan.getLastProbedBatchCount());
        assertEquals("Should probe all 4 centroids", 4, plan.getLastProbedCentroidCount());
      }
      assertEquals("Should return all 4 B rows", Arrays.asList("c0_b1", "c1_b1", "c2_b1", "c3_b1"),
        actualIds);
    }
  }

  @Test
  public void testFilterFirstPlanSelectionHighSelectivity() throws Exception {
    String tableName = "T_FF_HIGH_" + generateUniqueName();
    String regularIdx = "IDX_AUTHOR_" + generateUniqueName();
    String vectorIdx = "IDX_VEC_" + generateUniqueName();
    try (Connection conn = DriverManager.getConnection(getUrl())) {
      try (Statement stmt = conn.createStatement()) {
        stmt.execute("CREATE TABLE " + tableName
          + " (ID VARCHAR NOT NULL PRIMARY KEY, V VECTOR(FLOAT, 4), AUTHOR VARCHAR) "
          + "IMMUTABLE_ROWS=true, GUIDE_POSTS_WIDTH=100");
        stmt.execute("CREATE INDEX " + regularIdx + " ON " + tableName + " (AUTHOR) INCLUDE (V)");
        stmt.execute("CREATE VECTOR INDEX " + vectorIdx + " ON " + tableName + " (V) "
          + "INCLUDE (AUTHOR) WITH (algorithm = 'IVF', metric = 'L2', lists = 4, sample_size = 100)");
      }

      List<float[]> centroids = Arrays.asList(new float[] { 0.0f, 0.0f, 0.0f, 0.0f },
        new float[] { 10.0f, 0.0f, 0.0f, 0.0f }, new float[] { 20.0f, 0.0f, 0.0f, 0.0f },
        new float[] { 30.0f, 0.0f, 0.0f, 0.0f });
      VectorIndexTestUtil.activateWithKnownCentroids(conn, tableName, vectorIdx, centroids, 1L);

      // Highly selective predicate distribution.
      String upsertSql = "UPSERT INTO " + tableName + " (ID, V, AUTHOR) VALUES (?, ?, ?)";
      try (PreparedStatement ps = conn.prepareStatement(upsertSql)) {
        for (int i = 0; i < 1000; i++) {
          String id = "row_" + i;
          float[] v = new float[] { (float) (i % 4) * 10.0f, 0.0f, 0.0f, 0.0f };
          String author = (i < 10) ? "Alice" : "Bob";
          ps.setString(1, id);
          ps.setArray(2, conn.createArrayOf("FLOAT", boxed(v)));
          ps.setString(3, author);
          ps.addBatch();
          if (i % 250 == 0) {
            ps.executeBatch();
          }
        }
        ps.executeBatch();
        conn.commit();
      }

      // Collect table and index statistics for selectivity estimation.
      try (Statement stmt = conn.createStatement()) {
        stmt.execute("UPDATE STATISTICS " + tableName);
      }

      String querySql = "SELECT ID FROM " + tableName
        + " WHERE AUTHOR = 'Alice' ORDER BY L2_DISTANCE(V, ?) LIMIT 5";
      float[] q = new float[] { 0.0f, 0.0f, 0.0f, 0.0f };
      String plan = explain(conn, querySql, q);

      // Highly selective relational filter favors secondary index over vector index.
      assertTrue("Plan must use secondary index " + regularIdx + ": " + plan,
        plan.contains(regularIdx));
      assertFalse("Plan should not use CLIENT PROBING from vector index: " + plan,
        plan.contains("CLIENT PROBING"));
      assertFalse("Plan should not use vector index " + vectorIdx + ": " + plan,
        plan.contains(vectorIdx));

      try (PreparedStatement ps = conn.prepareStatement(querySql)) {
        ps.setArray(1, conn.createArrayOf("FLOAT", boxed(q)));
        try (ResultSet rs = ps.executeQuery()) {
          int count = 0;
          while (rs.next()) {
            count++;
          }
          assertEquals(5, count);
        }
      }
    }
  }

  @Test
  public void testLowSelectivityUsesVectorIndex() throws Exception {
    String tableName = "T_FF_LOW_" + generateUniqueName();
    String regularIdx = "IDX_AUTHOR_" + generateUniqueName();
    String vectorIdx = "IDX_VEC_" + generateUniqueName();
    try (Connection conn = DriverManager.getConnection(getUrl())) {
      try (Statement stmt = conn.createStatement()) {
        stmt.execute("CREATE TABLE " + tableName
          + " (ID VARCHAR NOT NULL PRIMARY KEY, V VECTOR(FLOAT, 4), AUTHOR VARCHAR) "
          + "IMMUTABLE_ROWS=true, GUIDE_POSTS_WIDTH=100");
        stmt.execute("CREATE INDEX " + regularIdx + " ON " + tableName + " (AUTHOR) INCLUDE (V)");
        stmt.execute("CREATE VECTOR INDEX " + vectorIdx + " ON " + tableName + " (V) "
          + "INCLUDE (AUTHOR) WITH (algorithm = 'IVF', metric = 'L2', lists = 4, sample_size = 100)");
      }

      List<float[]> centroids = Arrays.asList(new float[] { 0.0f, 0.0f, 0.0f, 0.0f },
        new float[] { 10.0f, 0.0f, 0.0f, 0.0f }, new float[] { 20.0f, 0.0f, 0.0f, 0.0f },
        new float[] { 30.0f, 0.0f, 0.0f, 0.0f });
      VectorIndexTestUtil.activateWithKnownCentroids(conn, tableName, vectorIdx, centroids, 1L);

      // Unselective predicate distribution.
      String upsertSql = "UPSERT INTO " + tableName + " (ID, V, AUTHOR) VALUES (?, ?, ?)";
      try (PreparedStatement ps = conn.prepareStatement(upsertSql)) {
        for (int i = 0; i < 1000; i++) {
          String id = "row_" + i;
          float[] v = new float[] { (float) (i % 4) * 10.0f, 0.0f, 0.0f, 0.0f };
          String author = (i < 900) ? "Alice" : "Bob";
          ps.setString(1, id);
          ps.setArray(2, conn.createArrayOf("FLOAT", boxed(v)));
          ps.setString(3, author);
          ps.addBatch();
          if (i % 250 == 0) {
            ps.executeBatch();
          }
        }
        ps.executeBatch();
        conn.commit();
      }

      // Collect table and index statistics for selectivity estimation.
      try (Statement stmt = conn.createStatement()) {
        stmt.execute("UPDATE STATISTICS " + tableName);
      }

      String querySql = "SELECT ID FROM " + tableName
        + " WHERE AUTHOR = 'Alice' ORDER BY L2_DISTANCE(V, ?) LIMIT 5";
      float[] q = new float[] { 0.0f, 0.0f, 0.0f, 0.0f };
      String plan = explain(conn, querySql, q);

      // Unselective relational filter favors vector index scan over secondary index lookup.
      assertTrue("Plan must use vector index " + vectorIdx + ": " + plan, plan.contains(vectorIdx));
      assertTrue("Plan must contain CLIENT PROBING: " + plan, plan.contains("CLIENT PROBING"));
      assertFalse("Plan should not use secondary index " + regularIdx + ": " + plan,
        plan.contains(regularIdx));
    }
  }

  /**
   * Verifies that unfiltered vector queries bypass adaptive probe expansion.
   */
  @Test
  public void testUnfilteredQueryDoesNotAdaptivelyProbe() throws Exception {
    String tableName = "T_NO_ADAPT_" + generateUniqueName();
    String indexName = "IDX_NO_ADAPT_" + generateUniqueName();
    try (Connection conn = DriverManager.getConnection(getUrl())) {
      try (Statement stmt = conn.createStatement()) {
        stmt.execute("CREATE TABLE " + tableName
          + " (ID VARCHAR NOT NULL PRIMARY KEY, V VECTOR(FLOAT, 4), CATEGORY VARCHAR) "
          + "IMMUTABLE_ROWS=true");
        stmt.execute(
          "CREATE VECTOR INDEX " + indexName + " ON " + tableName + " (V) INCLUDE (CATEGORY) "
            + "WITH (algorithm = 'IVF', metric = 'L2', lists = 4, sample_size = 100)");
      }

      List<float[]> centroids = Arrays.asList(new float[] { 0.0f, 0.0f, 0.0f, 0.0f },
        new float[] { 10.0f, 0.0f, 0.0f, 0.0f }, new float[] { 20.0f, 0.0f, 0.0f, 0.0f },
        new float[] { 30.0f, 0.0f, 0.0f, 0.0f });
      VectorIndexTestUtil.activateWithKnownCentroids(conn, tableName, indexName, centroids, 1L);

      String upsertSql = "UPSERT INTO " + tableName + " (ID, V, CATEGORY) VALUES (?, ?, ?)";
      try (PreparedStatement ps = conn.prepareStatement(upsertSql)) {
        for (int c = 0; c < 4; c++) {
          for (int a = 1; a <= 2; a++) {
            float[] v = new float[] { c * 10.0f + a * 0.1f, 0.0f, 0.0f, 0.0f };
            ps.setString(1, "c" + c + "_" + a);
            ps.setArray(2, conn.createArrayOf("FLOAT", boxed(v)));
            ps.setString(3, "A");
            ps.executeUpdate();
          }
        }
        conn.commit();
      }

      Float[] boxedQ = boxed(new float[] { 0.0f, 0.0f, 0.0f, 0.0f });
      String sql = "SELECT /*+ VECTOR_PROBE_COUNT(1) */ ID FROM " + tableName
        + " ORDER BY L2_DISTANCE(V, ?) LIMIT 5";

      List<String> actualIds = new ArrayList<>();
      try (PhoenixPreparedStatement pps =
        conn.prepareStatement(sql).unwrap(PhoenixPreparedStatement.class)) {
        pps.setArray(1, conn.createArrayOf("FLOAT", boxedQ));
        try (ResultSet rs = pps.executeQuery()) {
          while (rs.next()) {
            actualIds.add(rs.getString(1));
          }
        }
        VectorIndexScanPlan plan = (VectorIndexScanPlan) pps.getQueryPlan();
        assertFalse("Unfiltered query must not use the adaptive probe iterator",
          plan.isAdaptiveProbingApplicable());
        assertEquals("Unfiltered query must not expand its probe set", 0,
          plan.getLastProbedBatchCount());
      }
      assertEquals("Probing one centroid must return only that posting list",
        Arrays.asList("c0_1", "c0_2"), actualIds);
    }
  }

  /**
   * Evaluates plan ranking when both selective and unselective secondary indexes compete with a
   * vector index.
   */
  @Test
  public void testFilterFirstAmongMultipleSecondaryIndexes() throws Exception {
    String tableName = "T_FF_MULTI_" + generateUniqueName();
    String selectiveIdx = "IDX_AUTHOR_" + generateUniqueName();
    String unselectiveIdx = "IDX_STATUS_" + generateUniqueName();
    String vectorIdx = "IDX_VEC_" + generateUniqueName();
    try (Connection conn = DriverManager.getConnection(getUrl())) {
      try (Statement stmt = conn.createStatement()) {
        stmt.execute("CREATE TABLE " + tableName
          + " (ID VARCHAR NOT NULL PRIMARY KEY, V VECTOR(FLOAT, 4), AUTHOR VARCHAR, "
          + "STATUS VARCHAR) IMMUTABLE_ROWS=true, GUIDE_POSTS_WIDTH=100");
        stmt.execute(
          "CREATE INDEX " + selectiveIdx + " ON " + tableName + " (AUTHOR) INCLUDE (V, STATUS)");
        stmt.execute(
          "CREATE INDEX " + unselectiveIdx + " ON " + tableName + " (STATUS) INCLUDE (V, AUTHOR)");
        stmt.execute("CREATE VECTOR INDEX " + vectorIdx + " ON " + tableName + " (V) "
          + "INCLUDE (AUTHOR, STATUS) WITH (algorithm = 'IVF', metric = 'L2', lists = 4, "
          + "sample_size = 100)");
      }

      List<float[]> centroids = Arrays.asList(new float[] { 0.0f, 0.0f, 0.0f, 0.0f },
        new float[] { 10.0f, 0.0f, 0.0f, 0.0f }, new float[] { 20.0f, 0.0f, 0.0f, 0.0f },
        new float[] { 30.0f, 0.0f, 0.0f, 0.0f });
      VectorIndexTestUtil.activateWithKnownCentroids(conn, tableName, vectorIdx, centroids, 1L);

      // Establish contrasting selectivity between AUTHOR (selective) and STATUS (unselective).
      String upsertSql =
        "UPSERT INTO " + tableName + " (ID, V, AUTHOR, STATUS) VALUES (?, ?, ?, ?)";
      try (PreparedStatement ps = conn.prepareStatement(upsertSql)) {
        for (int i = 0; i < 1000; i++) {
          float[] v = new float[] { (float) (i % 4) * 10.0f, 0.0f, 0.0f, 0.0f };
          ps.setString(1, "row_" + i);
          ps.setArray(2, conn.createArrayOf("FLOAT", boxed(v)));
          ps.setString(3, (i < 10) ? "Alice" : "Bob");
          ps.setString(4, (i < 900) ? "OK" : "GONE");
          ps.addBatch();
          if (i % 250 == 0) {
            ps.executeBatch();
          }
        }
        ps.executeBatch();
        conn.commit();
      }
      try (Statement stmt = conn.createStatement()) {
        stmt.execute("UPDATE STATISTICS " + tableName);
      }

      String querySql = "SELECT ID FROM " + tableName
        + " WHERE AUTHOR = 'Alice' AND STATUS = 'OK' ORDER BY L2_DISTANCE(V, ?) LIMIT 5";
      float[] q = new float[] { 0.0f, 0.0f, 0.0f, 0.0f };
      String plan = explain(conn, querySql, q);

      assertTrue("Plan must use the selective index " + selectiveIdx + ": " + plan,
        plan.contains(selectiveIdx));
      assertFalse("Plan must not use the unselective index " + unselectiveIdx + ": " + plan,
        plan.contains(unselectiveIdx));
      assertFalse("Plan must not use the vector index " + vectorIdx + ": " + plan,
        plan.contains(vectorIdx));

      try (PreparedStatement ps = conn.prepareStatement(querySql)) {
        ps.setArray(1, conn.createArrayOf("FLOAT", boxed(q)));
        try (ResultSet rs = ps.executeQuery()) {
          int count = 0;
          while (rs.next()) {
            count++;
          }
          assertEquals(5, count);
        }
      }
    }
  }

  /**
   * Evaluates filter-first plan selection when an uncovered secondary index evaluates the
   * relational predicate and joins back to the data table for vector scoring.
   */
  @Test
  public void testFilterFirstUncoveredSecondaryIndex() throws Exception {
    String tableName = "T_FF_UNCOV_" + generateUniqueName();
    String regularIdx = "IDX_AUTHOR_" + generateUniqueName();
    String vectorIdx = "IDX_VEC_" + generateUniqueName();
    try (Connection conn = DriverManager.getConnection(getUrl())) {
      try (Statement stmt = conn.createStatement()) {
        stmt.execute("CREATE TABLE " + tableName
          + " (ID VARCHAR NOT NULL PRIMARY KEY, V VECTOR(FLOAT, 4), AUTHOR VARCHAR) "
          + "IMMUTABLE_ROWS=true, GUIDE_POSTS_WIDTH=100");
        // Uncovered secondary index on relational predicate column.
        stmt.execute("CREATE INDEX " + regularIdx + " ON " + tableName + " (AUTHOR)");
        stmt.execute("CREATE VECTOR INDEX " + vectorIdx + " ON " + tableName + " (V) "
          + "INCLUDE (AUTHOR) WITH (algorithm = 'IVF', metric = 'L2', lists = 4, sample_size = 100)");
      }

      List<float[]> centroids = Arrays.asList(new float[] { 0.0f, 0.0f, 0.0f, 0.0f },
        new float[] { 10.0f, 0.0f, 0.0f, 0.0f }, new float[] { 20.0f, 0.0f, 0.0f, 0.0f },
        new float[] { 30.0f, 0.0f, 0.0f, 0.0f });
      VectorIndexTestUtil.activateWithKnownCentroids(conn, tableName, vectorIdx, centroids, 1L);

      // Highly selective predicate distribution.
      String upsertSql = "UPSERT INTO " + tableName + " (ID, V, AUTHOR) VALUES (?, ?, ?)";
      try (PreparedStatement ps = conn.prepareStatement(upsertSql)) {
        for (int i = 0; i < 1000; i++) {
          float[] v = new float[] { (float) (i % 4) * 10.0f, 0.0f, 0.0f, 0.0f };
          ps.setString(1, "row_" + i);
          ps.setArray(2, conn.createArrayOf("FLOAT", boxed(v)));
          ps.setString(3, (i < 10) ? "Alice" : "Bob");
          ps.addBatch();
          if (i % 250 == 0) {
            ps.executeBatch();
          }
        }
        ps.executeBatch();
        conn.commit();
      }
      try (Statement stmt = conn.createStatement()) {
        stmt.execute("UPDATE STATISTICS " + tableName);
      }

      String querySql = "SELECT ID FROM " + tableName
        + " WHERE AUTHOR = 'Alice' ORDER BY L2_DISTANCE(V, ?) LIMIT 5";
      float[] q = new float[] { 0.0f, 0.0f, 0.0f, 0.0f };
      String plan = explain(conn, querySql, q);

      assertTrue("Plan must use secondary index " + regularIdx + ": " + plan,
        plan.contains(regularIdx));
      assertFalse("Plan should not use the vector index " + vectorIdx + ": " + plan,
        plan.contains(vectorIdx));

      try (PreparedStatement ps = conn.prepareStatement(querySql)) {
        ps.setArray(1, conn.createArrayOf("FLOAT", boxed(q)));
        try (ResultSet rs = ps.executeQuery()) {
          int count = 0;
          while (rs.next()) {
            count++;
          }
          assertEquals(5, count);
        }
      }
    }
  }

  @Test
  public void testCoveredBsonFilter() throws Exception {
    String tableName = "T_BSON_COV_FILT_" + generateUniqueName();
    String indexName = "IDX_BSON_COV_FILT_" + generateUniqueName();

    try (Connection conn = DriverManager.getConnection(getUrl())) {
      try (Statement stmt = conn.createStatement()) {
        stmt.execute("CREATE TABLE " + tableName
          + " (PK VARCHAR NOT NULL PRIMARY KEY, DOC BSON) IMMUTABLE_ROWS=true");
        stmt.execute("CREATE VECTOR INDEX " + indexName + " ON " + tableName
          + " (BSON_VECTOR_VALUE(doc, 'embedding', " + BSON_DIM + ")) INCLUDE (DOC)"
          + " WITH (algorithm = 'IVF', metric = 'L2', lists = 4, sample_size = 100)");
      }
      VectorIndexTestUtil.activateWithKnownCentroids(conn, tableName, indexName, fourCentroids(),
        1L);

      Map<String, float[]> scienceRows = new LinkedHashMap<>();
      Map<String, float[]> scienceOrMathRows = new LinkedHashMap<>();

      // Seed rows across known centroid partitions.
      float[] xs = { 1f, 3f, 4f, 9f, 11f, 13f, 19f, 21f, 23f, 28f, 29f, 31f };
      String[] cats = { "science", "art", "science", "art", "math", "science", "art", "math",
        "science", "art", "math", "science" };
      for (int i = 0; i < xs.length; i++) {
        String id = "r" + i;
        float[] v = vecX(xs[i]);
        v[1] = 0.1f * i;
        if ("science".equals(cats[i])) {
          scienceRows.put(id, v);
          scienceOrMathRows.put(id, v);
        } else if ("math".equals(cats[i])) {
          scienceOrMathRows.put(id, v);
        }
        upsertDoc(conn, tableName, id, embeddingDoc(v, cats[i]));
      }

      float[] q = vecX(5f);
      Float[] boxedQ = boxed(q);

      // Covered equality predicate on BSON document field.
      String eqSql = "SELECT PK FROM " + tableName
        + " WHERE BSON_VALUE(doc, 'category') = 'science' ORDER BY L2_DISTANCE(BSON_VECTOR_VALUE(doc, 'embedding', "
        + BSON_DIM + "), ?) LIMIT 3";

      String plan = explain(conn, eqSql, q);
      assertTrue("Plan must use the vector index: " + plan, plan.contains(indexName));
      assertTrue("Plan must use CLIENT PROBING: " + plan, plan.contains("CLIENT PROBING"));
      assertFalse("Covered plan must NOT contain SERVER MERGE: " + plan,
        plan.contains("SERVER MERGE"));

      try (PhoenixPreparedStatement pps =
        conn.prepareStatement(eqSql).unwrap(PhoenixPreparedStatement.class)) {
        pps.setArray(1, conn.createArrayOf("FLOAT", boxedQ));
        VectorIndexScanPlan vPlan = (VectorIndexScanPlan) pps.optimizeQuery();
        assertFalse("Covered filter must not set filterTimeUncoveredLookup",
          vPlan.isFilterTimeUncoveredLookup());
        assertFalse("Covered filter must not set projectionTimeUncoveredLookup",
          vPlan.isProjectionTimeUncoveredLookup());
      }

      List<String> expectedScienceTop3 =
        VectorIndexTestUtil.bruteForceTopK(scienceRows, q, "L2", 3);
      List<String> actualScienceTop3 = runSearch(conn, eqSql, q);
      assertEquals(expectedScienceTop3, actualScienceTop3);

      // Covered IN predicate on BSON document field.
      String inSql = "SELECT PK FROM " + tableName
        + " WHERE BSON_VALUE(doc, 'category') IN ('science', 'math') ORDER BY L2_DISTANCE(BSON_VECTOR_VALUE(doc, 'embedding', "
        + BSON_DIM + "), ?) LIMIT 4";

      String inPlan = explain(conn, inSql, q);
      assertTrue("Plan must use the vector index: " + inPlan, inPlan.contains(indexName));
      assertTrue("Plan must use CLIENT PROBING: " + inPlan, inPlan.contains("CLIENT PROBING"));
      assertFalse("Covered plan must NOT contain SERVER MERGE: " + inPlan,
        inPlan.contains("SERVER MERGE"));

      List<String> expectedMathTop4 =
        VectorIndexTestUtil.bruteForceTopK(scienceOrMathRows, q, "L2", 4);
      List<String> actualMathTop4 = runSearch(conn, inSql, q);
      assertEquals(expectedMathTop4, actualMathTop4);
    }
  }

  @Test
  public void testUncoveredBsonFilter() throws Exception {
    String tableName = "T_BSON_UNCOV_FILT_" + generateUniqueName();
    String indexName = "IDX_BSON_UNCOV_FILT_" + generateUniqueName();

    try (Connection conn = DriverManager.getConnection(getUrl())) {
      try (Statement stmt = conn.createStatement()) {
        stmt.execute("CREATE TABLE " + tableName
          + " (PK VARCHAR NOT NULL PRIMARY KEY, DOC BSON) IMMUTABLE_ROWS=true");
        // Uncovered vector index requiring base table projection for document content.
        stmt.execute("CREATE VECTOR INDEX " + indexName + " ON " + tableName
          + " (BSON_VECTOR_VALUE(doc, 'embedding', " + BSON_DIM + "))"
          + " WITH (algorithm = 'IVF', metric = 'L2', lists = 4, sample_size = 100)");
      }
      VectorIndexTestUtil.activateWithKnownCentroids(conn, tableName, indexName, fourCentroids(),
        1L);

      Map<String, float[]> scienceRows = new LinkedHashMap<>();
      Map<String, float[]> scienceOrMathRows = new LinkedHashMap<>();

      // Seed rows across known centroid partitions.
      float[] xs = { 1f, 3f, 4f, 9f, 11f, 13f, 19f, 21f, 23f, 28f, 29f, 31f };
      String[] cats = { "science", "art", "science", "art", "math", "science", "art", "math",
        "science", "art", "math", "science" };
      for (int i = 0; i < xs.length; i++) {
        String id = "r" + i;
        float[] v = vecX(xs[i]);
        v[1] = 0.1f * i;
        if ("science".equals(cats[i])) {
          scienceRows.put(id, v);
          scienceOrMathRows.put(id, v);
        } else if ("math".equals(cats[i])) {
          scienceOrMathRows.put(id, v);
        }
        upsertDoc(conn, tableName, id, embeddingDoc(v, cats[i]));
      }

      float[] q = vecX(5f);
      Float[] boxedQ = boxed(q);

      // Uncovered equality predicate requiring base table lookup.
      String eqSql = "SELECT PK FROM " + tableName
        + " WHERE BSON_VALUE(doc, 'category') = 'science' ORDER BY L2_DISTANCE(BSON_VECTOR_VALUE(doc, 'embedding', "
        + BSON_DIM + "), ?) LIMIT 3";

      String plan = explain(conn, eqSql, q);
      assertTrue("Plan must use the vector index: " + plan, plan.contains(indexName));
      assertTrue("Plan must use CLIENT PROBING: " + plan, plan.contains("CLIENT PROBING"));
      assertTrue("Uncovered filter must show SERVER MERGE for base table lookup: " + plan,
        plan.contains("SERVER MERGE"));

      try (PhoenixPreparedStatement pps =
        conn.prepareStatement(eqSql).unwrap(PhoenixPreparedStatement.class)) {
        pps.setArray(1, conn.createArrayOf("FLOAT", boxedQ));
        VectorIndexScanPlan vPlan = (VectorIndexScanPlan) pps.optimizeQuery();
        assertTrue("Uncovered filter must set filterTimeUncoveredLookup",
          vPlan.isFilterTimeUncoveredLookup());
        assertFalse("Uncovered filter must not set projectionTimeUncoveredLookup",
          vPlan.isProjectionTimeUncoveredLookup());
      }

      List<String> expectedScienceTop3 =
        VectorIndexTestUtil.bruteForceTopK(scienceRows, q, "L2", 3);
      List<String> actualScienceTop3 = runSearch(conn, eqSql, q);
      assertEquals(expectedScienceTop3, actualScienceTop3);

      // Uncovered IN predicate requiring base table lookup.
      String inSql = "SELECT PK FROM " + tableName
        + " WHERE BSON_VALUE(doc, 'category') IN ('science', 'math') ORDER BY L2_DISTANCE(BSON_VECTOR_VALUE(doc, 'embedding', "
        + BSON_DIM + "), ?) LIMIT 4";

      String inPlan = explain(conn, inSql, q);
      assertTrue("Plan must use the vector index: " + inPlan, inPlan.contains(indexName));
      assertTrue("Plan must use CLIENT PROBING: " + inPlan, inPlan.contains("CLIENT PROBING"));
      assertTrue("Uncovered filter must show SERVER MERGE for base table lookup: " + inPlan,
        inPlan.contains("SERVER MERGE"));

      List<String> expectedMathTop4 =
        VectorIndexTestUtil.bruteForceTopK(scienceOrMathRows, q, "L2", 4);
      List<String> actualMathTop4 = runSearch(conn, inSql, q);
      assertEquals(expectedMathTop4, actualMathTop4);
    }
  }

  private void setupDeterministicVectorIndex(Connection conn, String tableName, String indexName)
    throws Exception {
    try (Statement stmt = conn.createStatement()) {
      stmt.execute(
        "CREATE TABLE " + tableName + " (ID VARCHAR NOT NULL PRIMARY KEY, V VECTOR(FLOAT, 4))");
      stmt.execute("CREATE VECTOR INDEX " + indexName + " ON " + tableName + " (V) "
        + "WITH (algorithm = 'IVF', metric = 'L2', lists = 4, sample_size = 100) ASYNC");
    }

    List<float[]> knownCentroids = Arrays.asList(new float[] { 10.0f, 0.0f, 0.0f, 0.0f }, // centroid
                                                                                          // 0
      new float[] { 0.0f, 10.0f, 0.0f, 0.0f }, // centroid 1
      new float[] { 0.0f, 0.0f, 10.0f, 0.0f }, // centroid 2
      new float[] { 0.0f, 0.0f, 0.0f, 10.0f } // centroid 3
    );
    VectorIndexTestUtil.activateWithKnownCentroids(conn, tableName, indexName, knownCentroids, 1L);
    ScorecardAccumulator.getInstance().clear();
  }

  private static Float[] make4D(float a, float b, float c, float d) {
    return new Float[] { a, b, c, d };
  }

  private Map<Integer, Long> getScorecardCounts(Connection conn, String indexName, long generation)
    throws Exception {
    Map<Integer, Long> map = new HashMap<>();
    String sql = "SELECT " + CENTROID_ID + ", " + CLUSTER_SIZE + " FROM "
      + SYSTEM_VECTOR_CENTROID_NAME + " WHERE " + INDEX_NAME + " = ? AND " + GENERATION_ID
      + " = ? AND " + CENTROID_ID + " >= 0 ORDER BY " + CENTROID_ID;
    try (PreparedStatement ps = conn.prepareStatement(sql)) {
      ps.setString(1, SchemaUtil.normalizeFullTableName(indexName));
      ps.setLong(2, generation);
      try (ResultSet rs = ps.executeQuery()) {
        while (rs.next()) {
          map.put(rs.getInt(1), rs.getLong(2));
        }
      }
    }
    return map;
  }

  private Map<Integer, Long> getPhysicalIndexCounts(Connection conn, String indexName)
    throws Exception {
    Map<Integer, Long> map = new HashMap<>();
    String escapedIndex =
      SchemaUtil.getEscapedFullTableName(SchemaUtil.normalizeFullTableName(indexName));
    String centroidCol = IndexUtil.getIndexColumnName(null, CENTROID_ID);
    String sql = "SELECT \"" + centroidCol + "\", COUNT(*) FROM " + escapedIndex + " GROUP BY \""
      + centroidCol + "\"";
    try (Statement stmt = conn.createStatement(); ResultSet rs = stmt.executeQuery(sql)) {
      while (rs.next()) {
        map.put(rs.getInt(1), rs.getLong(2));
      }
    }
    return map;
  }

  @Test
  public void testInlineCountsExactWithoutScan() throws Exception {
    String tableName = "T_INLINE_EXACT_" + generateUniqueName();
    String indexName = "IDX_INLINE_EXACT_" + generateUniqueName();

    try (Connection conn = DriverManager.getConnection(getUrl())) {
      setupDeterministicVectorIndex(conn, tableName, indexName);

      // 1. Apply 100 inserts (25 to each centroid)
      String upsertSql = "UPSERT INTO " + tableName + " VALUES (?, ?)";
      try (PreparedStatement ps = conn.prepareStatement(upsertSql)) {
        for (int i = 0; i < 25; i++) {
          ps.setString(1, "row_c0_" + i);
          ps.setArray(2, conn.createArrayOf("FLOAT", make4D(10f, 0f, 0f, 0f)));
          ps.executeUpdate();

          ps.setString(1, "row_c1_" + i);
          ps.setArray(2, conn.createArrayOf("FLOAT", make4D(0f, 10f, 0f, 0f)));
          ps.executeUpdate();

          ps.setString(1, "row_c2_" + i);
          ps.setArray(2, conn.createArrayOf("FLOAT", make4D(0f, 0f, 10f, 0f)));
          ps.executeUpdate();

          ps.setString(1, "row_c3_" + i);
          ps.setArray(2, conn.createArrayOf("FLOAT", make4D(0f, 0f, 0f, 10f)));
          ps.executeUpdate();
        }
      }
      conn.commit();

      // 2. Apply 20 deletes (5 from each centroid)
      String deleteSql = "DELETE FROM " + tableName + " WHERE ID = ?";
      try (PreparedStatement ps = conn.prepareStatement(deleteSql)) {
        for (int i = 0; i < 5; i++) {
          ps.setString(1, "row_c0_" + i);
          ps.executeUpdate();
          ps.setString(1, "row_c1_" + i);
          ps.executeUpdate();
          ps.setString(1, "row_c2_" + i);
          ps.executeUpdate();
          ps.setString(1, "row_c3_" + i);
          ps.executeUpdate();
        }
      }
      conn.commit();

      // 3. Apply 15 inter-centroid updates: rows 5..19 move from centroid 0 to centroid 1
      try (PreparedStatement ps = conn.prepareStatement(upsertSql)) {
        for (int i = 5; i < 20; i++) {
          ps.setString(1, "row_c0_" + i);
          ps.setArray(2, conn.createArrayOf("FLOAT", make4D(0f, 10f, 0f, 0f)));
          ps.executeUpdate();
        }
      }
      conn.commit();

      // 4. Apply 15 intra-centroid updates: rows 5..19 in centroid 2 stay in centroid 2
      try (PreparedStatement ps = conn.prepareStatement(upsertSql)) {
        for (int i = 5; i < 20; i++) {
          ps.setString(1, "row_c2_" + i);
          ps.setArray(2, conn.createArrayOf("FLOAT", make4D(0f, 0f, 9.8f, 0.1f)));
          ps.executeUpdate();
        }
      }
      conn.commit();

      // Flush accumulator directly
      ScorecardAccumulator.getInstance().flush(conn);

      // Verify scorecard counts match physical index table counts without reconciliation
      Map<Integer, Long> scorecardCounts = getScorecardCounts(conn, indexName, 1L);
      Map<Integer, Long> physicalCounts = getPhysicalIndexCounts(conn, indexName);

      // Expected centroid counts derived from applied insert, delete, and repartition operations:
      // c0: 25 inserted - 5 deleted - 15 moved out = 5
      // c1: 25 inserted - 5 deleted + 15 moved in = 35
      // c2: 25 inserted - 5 deleted = 20 (intra-centroid updates unchanged)
      // c3: 25 inserted - 5 deleted = 20
      Map<Integer, Long> expected = new HashMap<>();
      expected.put(0, 5L);
      expected.put(1, 35L);
      expected.put(2, 20L);
      expected.put(3, 20L);

      for (int c = 0; c < 4; c++) {
        assertEquals("Scorecard cluster size for centroid " + c, expected.get(c),
          scorecardCounts.get(c));
        assertEquals("Index table row count for centroid " + c, expected.get(c),
          physicalCounts.getOrDefault(c, 0L));
      }
    }
  }

  @Test
  public void testReplayNeutrality() throws Exception {
    String tableName = "T_REPLAY_" + generateUniqueName();
    String indexName = "IDX_REPLAY_" + generateUniqueName();

    try (Connection conn = DriverManager.getConnection(getUrl())) {
      setupDeterministicVectorIndex(conn, tableName, indexName);

      String upsertSql = "UPSERT INTO " + tableName + " VALUES (?, ?)";
      try (PreparedStatement ps = conn.prepareStatement(upsertSql)) {
        for (int i = 0; i < 10; i++) {
          ps.setString(1, "r_" + i);
          ps.setArray(2, conn.createArrayOf("FLOAT", make4D(10f, 0f, 0f, 0f)));
          ps.executeUpdate();
        }
      }
      conn.commit();
      ScorecardAccumulator.getInstance().flush(conn);

      Map<Integer, Long> initialCounts = getScorecardCounts(conn, indexName, 1L);
      assertEquals(Long.valueOf(10L), initialCounts.get(0));

      // Re-apply unchanged vectors
      try (PreparedStatement ps = conn.prepareStatement(upsertSql)) {
        for (int i = 0; i < 10; i++) {
          ps.setString(1, "r_" + i);
          ps.setArray(2, conn.createArrayOf("FLOAT", make4D(10f, 0f, 0f, 0f)));
          ps.executeUpdate();
        }
      }
      conn.commit();
      ScorecardAccumulator.getInstance().flush(conn);

      Map<Integer, Long> replayedCounts = getScorecardCounts(conn, indexName, 1L);
      assertEquals("Replay must not duplicate cluster sizes", initialCounts, replayedCounts);
    }
  }

  @Test
  public void testDeletesDecrement() throws Exception {
    String tableName = "T_DEL_DEC_" + generateUniqueName();
    String indexName = "IDX_DEL_DEC_" + generateUniqueName();

    try (Connection conn = DriverManager.getConnection(getUrl())) {
      setupDeterministicVectorIndex(conn, tableName, indexName);

      String upsertSql = "UPSERT INTO " + tableName + " VALUES (?, ?)";
      try (PreparedStatement ps = conn.prepareStatement(upsertSql)) {
        for (int i = 0; i < 40; i++) {
          ps.setString(1, "r_" + i);
          int c = i % 4;
          float[] v = new float[4];
          v[c] = 10.0f;
          ps.setArray(2, conn.createArrayOf("FLOAT", new Float[] { v[0], v[1], v[2], v[3] }));
          ps.executeUpdate();
        }
      }
      conn.commit();
      ScorecardAccumulator.getInstance().flush(conn);

      Map<Integer, Long> initialCounts = getScorecardCounts(conn, indexName, 1L);
      for (int c = 0; c < 4; c++) {
        assertEquals(Long.valueOf(10L), initialCounts.get(c));
      }

      // Delete 10 rows belonging to centroid 0 (i = 0, 4, 8, ...)
      String delSql = "DELETE FROM " + tableName + " WHERE ID = ?";
      try (PreparedStatement ps = conn.prepareStatement(delSql)) {
        for (int i = 0; i < 40; i += 4) {
          ps.setString(1, "r_" + i);
          ps.executeUpdate();
        }
      }
      conn.commit();
      ScorecardAccumulator.getInstance().flush(conn);

      Map<Integer, Long> afterDeleteCounts = getScorecardCounts(conn, indexName, 1L);
      assertEquals(Long.valueOf(0L), afterDeleteCounts.get(0));
      assertEquals(Long.valueOf(10L), afterDeleteCounts.get(1));
      assertEquals(Long.valueOf(10L), afterDeleteCounts.get(2));
      assertEquals(Long.valueOf(10L), afterDeleteCounts.get(3));
    }
  }

  @Test
  public void testReassignmentMovesTheCount() throws Exception {
    String tableName = "T_REASSIGN_" + generateUniqueName();
    String indexName = "IDX_REASSIGN_" + generateUniqueName();

    try (Connection conn = DriverManager.getConnection(getUrl())) {
      setupDeterministicVectorIndex(conn, tableName, indexName);

      String upsertSql = "UPSERT INTO " + tableName + " VALUES (?, ?)";
      try (PreparedStatement ps = conn.prepareStatement(upsertSql)) {
        ps.setString(1, "move_row");
        ps.setArray(2, conn.createArrayOf("FLOAT", make4D(10f, 0f, 0f, 0f))); // centroid 0
        ps.executeUpdate();
      }
      conn.commit();
      ScorecardAccumulator.getInstance().flush(conn);

      Map<Integer, Long> step1Counts = getScorecardCounts(conn, indexName, 1L);
      assertEquals(Long.valueOf(1L), step1Counts.get(0));
      assertEquals(Long.valueOf(0L), step1Counts.get(2));

      // Update vector to move from centroid 0 to centroid 2
      try (PreparedStatement ps = conn.prepareStatement(upsertSql)) {
        ps.setString(1, "move_row");
        ps.setArray(2, conn.createArrayOf("FLOAT", make4D(0f, 0f, 10f, 0f))); // centroid 2
        ps.executeUpdate();
      }
      conn.commit();
      ScorecardAccumulator.getInstance().flush(conn);

      Map<Integer, Long> step2Counts = getScorecardCounts(conn, indexName, 1L);
      assertEquals("Centroid 0 count must decrement by 1", Long.valueOf(0L), step2Counts.get(0));
      assertEquals("Centroid 2 count must increment by 1", Long.valueOf(1L), step2Counts.get(2));

      // Check reassign count on arriving centroid 2
      String reassignSql = "SELECT " + REASSIGN_COUNT + " FROM " + SYSTEM_VECTOR_CENTROID_NAME
        + " WHERE " + INDEX_NAME + " = ? AND " + GENERATION_ID + " = ? AND " + CENTROID_ID + " = 2";
      try (PreparedStatement ps = conn.prepareStatement(reassignSql)) {
        ps.setString(1, SchemaUtil.normalizeFullTableName(indexName));
        ps.setLong(2, 1L);
        try (ResultSet rs = ps.executeQuery()) {
          assertTrue(rs.next());
          assertEquals("Centroid 2 REASSIGN_COUNT must increment to 1", 1L, rs.getLong(1));
        }
      }
    }
  }

  @Test
  public void testSameCentroidUpdateDoesNotMoveCount() throws Exception {
    String tableName = "T_SAME_C_" + generateUniqueName();
    String indexName = "IDX_SAME_C_" + generateUniqueName();

    try (Connection conn = DriverManager.getConnection(getUrl())) {
      setupDeterministicVectorIndex(conn, tableName, indexName);

      String upsertSql = "UPSERT INTO " + tableName + " VALUES (?, ?)";
      try (PreparedStatement ps = conn.prepareStatement(upsertSql)) {
        ps.setString(1, "same_c_row");
        ps.setArray(2, conn.createArrayOf("FLOAT", make4D(10f, 0f, 0f, 0f))); // centroid 0
        ps.executeUpdate();
      }
      conn.commit();
      ScorecardAccumulator.getInstance().flush(conn);

      Map<Integer, Long> initial = getScorecardCounts(conn, indexName, 1L);
      assertEquals(Long.valueOf(1L), initial.get(0));

      // Update to new vector that is still closest to centroid 0
      try (PreparedStatement ps = conn.prepareStatement(upsertSql)) {
        ps.setString(1, "same_c_row");
        ps.setArray(2, conn.createArrayOf("FLOAT", make4D(9.8f, 0.1f, 0f, 0f)));
        ps.executeUpdate();
      }
      conn.commit();
      ScorecardAccumulator.getInstance().flush(conn);

      Map<Integer, Long> afterSameUpdate = getScorecardCounts(conn, indexName, 1L);
      assertEquals("Cluster sizes must remain unchanged", initial, afterSameUpdate);
    }
  }

  @Test
  public void testBaselineSeededAtIndexBuild() throws Exception {
    String tableName = "T_SEED_BL_" + generateUniqueName();
    String indexName = "IDX_SEED_BL_" + generateUniqueName();

    try (Connection conn = DriverManager.getConnection(getUrl())) {
      try (Statement stmt = conn.createStatement()) {
        stmt.execute(
          "CREATE TABLE " + tableName + " (ID VARCHAR NOT NULL PRIMARY KEY, V VECTOR(FLOAT, 4))");
      }

      String upsertSql = "UPSERT INTO " + tableName + " VALUES (?, ?)";
      try (PreparedStatement ps = conn.prepareStatement(upsertSql)) {
        for (int i = 0; i < 100; i++) {
          ps.setString(1, "seed_row_" + i);
          float[] v = new float[4];
          v[i % 4] = 10.0f;
          ps.setArray(2, conn.createArrayOf("FLOAT", new Float[] { v[0], v[1], v[2], v[3] }));
          ps.executeUpdate();
        }
      }
      conn.commit();

      // Create vector index synchronously: training + population + baseline seeding
      try (Statement stmt = conn.createStatement()) {
        stmt.execute("CREATE VECTOR INDEX " + indexName + " ON " + tableName + " (V) "
          + "WITH (algorithm = 'IVF', metric = 'L2', lists = 4, sample_size = 100)");
      }

      // Assert initial scorecard rows exist with CLUSTER_SIZE summing to 100 before any mutations
      Map<Integer, Long> scorecardCounts = getScorecardCounts(conn, indexName, 1L);
      assertFalse("Scorecard rows must exist", scorecardCounts.isEmpty());
      long sum = 0;
      for (long count : scorecardCounts.values()) {
        sum += count;
      }
      assertEquals("Baseline scorecard cluster sizes must sum to 100", 100L, sum);
    }
  }

  @Test
  public void testConcurrentFlushAccumulation() throws Exception {
    String tableName = "T_CONC_FLUSH_" + generateUniqueName();
    String indexName = "IDX_CONC_FLUSH_" + generateUniqueName();

    try (Connection conn1 = DriverManager.getConnection(getUrl());
      Connection conn2 = DriverManager.getConnection(getUrl())) {
      setupDeterministicVectorIndex(conn1, tableName, indexName);

      // Verify atomic accumulation under concurrent flushes from distinct connections.
      ScorecardAccumulator acc1 = new ScorecardAccumulator(null);
      ScorecardAccumulator acc2 = new ScorecardAccumulator(null);
      acc1.accumulate(indexName, 1L, 0, 5L, 2L);
      acc2.accumulate(indexName, 1L, 0, 7L, 3L);

      final CyclicBarrier barrier = new CyclicBarrier(2);
      ExecutorService pool = Executors.newFixedThreadPool(2);
      try {
        List<Future<?>> futures = new ArrayList<>();
        for (final Object[] work : new Object[][] { { acc1, conn1 }, { acc2, conn2 } }) {
          futures.add(pool.submit(() -> {
            barrier.await(30, TimeUnit.SECONDS);
            ((ScorecardAccumulator) work[0]).flush((Connection) work[1]);
            return null;
          }));
        }
        for (Future<?> f : futures) {
          f.get(60, TimeUnit.SECONDS);
        }
      } finally {
        pool.shutdownNow();
        acc1.close();
        acc2.close();
      }

      Map<Integer, Long> counts = getScorecardCounts(conn1, indexName, 1L);
      assertEquals("Concurrent flushes must both land", Long.valueOf(12L), counts.get(0));
      assertEquals("Reassign counts accumulate the same way", Long.valueOf(5L),
        reassignCountOf(conn1, indexName, 1L, 0));
    }
  }

  private Long reassignCountOf(Connection conn, String indexName, long generation, int centroidId)
    throws Exception {
    String sql = "SELECT " + REASSIGN_COUNT + " FROM " + SYSTEM_VECTOR_CENTROID_NAME + " WHERE "
      + INDEX_NAME + " = ? AND " + GENERATION_ID + " = ? AND " + CENTROID_ID + " = ?";
    try (PreparedStatement ps = conn.prepareStatement(sql)) {
      ps.setString(1, SchemaUtil.normalizeFullTableName(indexName));
      ps.setLong(2, generation);
      ps.setInt(3, centroidId);
      try (ResultSet rs = ps.executeQuery()) {
        assertTrue(rs.next());
        long v = rs.getLong(1);
        return rs.wasNull() ? null : v;
      }
    }
  }

  @Test
  public void testReconciliationRepairsDivergence() throws Exception {
    String tableName = "T_RECON_REP_" + generateUniqueName();
    String indexName = "IDX_RECON_REP_" + generateUniqueName();

    try (Connection conn = DriverManager.getConnection(getUrl())) {
      setupDeterministicVectorIndex(conn, tableName, indexName);

      // Insert 20 rows (5 per centroid)
      String upsertSql = "UPSERT INTO " + tableName + " VALUES (?, ?)";
      try (PreparedStatement ps = conn.prepareStatement(upsertSql)) {
        for (int i = 0; i < 20; i++) {
          ps.setString(1, "r_" + i);
          float[] v = new float[4];
          v[i % 4] = 10.0f;
          ps.setArray(2, conn.createArrayOf("FLOAT", new Float[] { v[0], v[1], v[2], v[3] }));
          ps.executeUpdate();
        }
      }
      conn.commit();
      ScorecardAccumulator.getInstance().flush(conn);

      // Modify centroid 1 counts to simulate divergence
      String corruptSql = "UPSERT INTO " + SYSTEM_VECTOR_CENTROID_NAME + " (" + INDEX_NAME + ", "
        + GENERATION_ID + ", " + CENTROID_ID + ", " + CLUSTER_SIZE + ", " + REASSIGN_COUNT
        + ") VALUES (?, 1, 1, 999, 88)";
      try (PreparedStatement ps = conn.prepareStatement(corruptSql)) {
        ps.setString(1, SchemaUtil.normalizeFullTableName(indexName));
        ps.executeUpdate();
      }
      conn.commit();

      long beforeReconcile = System.currentTimeMillis();
      VectorIndexScorecard.reconcile(conn, indexName, 1L);

      Map<Integer, Long> restored = getScorecardCounts(conn, indexName, 1L);
      assertEquals("CLUSTER_SIZE must be restored to physical count 5", Long.valueOf(5L),
        restored.get(1));

      String checkSql = "SELECT " + REASSIGN_COUNT + ", " + LAST_SCORECARD_UPDATE + " FROM "
        + SYSTEM_VECTOR_CENTROID_NAME + " WHERE " + INDEX_NAME + " = ? AND " + GENERATION_ID
        + " = 1 AND " + CENTROID_ID + " = 1";
      try (PreparedStatement ps = conn.prepareStatement(checkSql)) {
        ps.setString(1, SchemaUtil.normalizeFullTableName(indexName));
        try (ResultSet rs = ps.executeQuery()) {
          assertTrue(rs.next());
          assertEquals("REASSIGN_COUNT must reset to 0", 0L, rs.getLong(1));
          assertTrue("LAST_SCORECARD_UPDATE must advance", rs.getLong(2) >= beforeReconcile);
        }
      }
    }
  }

  @Test
  public void testEmptyPostingListRecordedAsZero() throws Exception {
    String tableName = "T_EMPTY_PL_" + generateUniqueName();
    String indexName = "IDX_EMPTY_PL_" + generateUniqueName();

    try (Connection conn = DriverManager.getConnection(getUrl())) {
      setupDeterministicVectorIndex(conn, tableName, indexName);

      // Insert rows only to centroids 0, 1, 2. Centroid 3 receives no rows.
      String upsertSql = "UPSERT INTO " + tableName + " VALUES (?, ?)";
      try (PreparedStatement ps = conn.prepareStatement(upsertSql)) {
        for (int i = 0; i < 9; i++) {
          ps.setString(1, "r_" + i);
          float[] v = new float[4];
          v[i % 3] = 10.0f;
          ps.setArray(2, conn.createArrayOf("FLOAT", new Float[] { v[0], v[1], v[2], v[3] }));
          ps.executeUpdate();
        }
      }
      conn.commit();

      VectorIndexScorecard.reconcile(conn, indexName, 1L);

      Map<Integer, Long> counts = getScorecardCounts(conn, indexName, 1L);
      assertTrue("Row for empty centroid 3 must exist", counts.containsKey(3));
      assertEquals("Empty centroid 3 must have cluster size 0", Long.valueOf(0L), counts.get(3));
    }
  }

  @Test
  public void testSkewTriggersAndBalancedDoesNot() throws Exception {
    String skewTable = "T_SKEW_EVAL_" + generateUniqueName();
    String skewIndex = "IDX_SKEW_EVAL_" + generateUniqueName();

    String balTable = "T_BAL_EVAL_" + generateUniqueName();
    String balIndex = "IDX_BAL_EVAL_" + generateUniqueName();

    Configuration conf = HBaseConfiguration.create();
    conf.setLong(QueryServices.VECTOR_INDEX_DRIFT_MIN_CLUSTER_SIZE_ATTRIB, 50L);
    conf.setDouble(QueryServices.VECTOR_INDEX_DRIFT_SKEW_RATIO_THRESHOLD_ATTRIB, 3.5);

    try (Connection conn = DriverManager.getConnection(getUrl())) {
      // 1. Skewed index: 100 rows all assigned to centroid 0
      setupDeterministicVectorIndex(conn, skewTable, skewIndex);
      String upsertSql = "UPSERT INTO " + skewTable + " VALUES (?, ?)";
      try (PreparedStatement ps = conn.prepareStatement(upsertSql)) {
        for (int i = 0; i < 100; i++) {
          ps.setString(1, "s_" + i);
          ps.setArray(2, conn.createArrayOf("FLOAT", make4D(10f, 0f, 0f, 0f)));
          ps.executeUpdate();
        }
      }
      conn.commit();
      VectorIndexScorecard.reconcile(conn, skewIndex, 1L);
      DriftEvaluationResult skewResult = VectorIndexScorecard.evaluate(conn, skewIndex, 1L, conf);
      assertTrue("Skewed data must trigger rebuild", skewResult.shouldRebuild());
      assertNotNull(skewResult.getTriggerReason());
      assertTrue(skewResult.getTriggerReason().contains("SKEW_RATIO_EXCEEDED"));

      // 2. Balanced index: 100 rows evenly spread across 4 centroids (25 each)
      setupDeterministicVectorIndex(conn, balTable, balIndex);
      upsertSql = "UPSERT INTO " + balTable + " VALUES (?, ?)";
      try (PreparedStatement ps = conn.prepareStatement(upsertSql)) {
        for (int i = 0; i < 100; i++) {
          ps.setString(1, "b_" + i);
          float[] v = new float[4];
          v[i % 4] = 10.0f;
          ps.setArray(2, conn.createArrayOf("FLOAT", new Float[] { v[0], v[1], v[2], v[3] }));
          ps.executeUpdate();
        }
      }
      conn.commit();
      VectorIndexScorecard.reconcile(conn, balIndex, 1L);
      DriftEvaluationResult balResult = VectorIndexScorecard.evaluate(conn, balIndex, 1L, conf);
      assertFalse("Balanced data must not trigger rebuild", balResult.shouldRebuild());
      assertNull(balResult.getTriggerReason());
    }
  }

  @Test
  public void testSmallIndexSuppression() throws Exception {
    String tableName = "T_SMALL_SUPP_" + generateUniqueName();
    String indexName = "IDX_SMALL_SUPP_" + generateUniqueName();

    Configuration conf = HBaseConfiguration.create(); // default min.cluster.size is 1000

    try (Connection conn = DriverManager.getConnection(getUrl())) {
      setupDeterministicVectorIndex(conn, tableName, indexName);

      // Insert 10 rows all assigned to centroid 0 (total 10 < min.cluster.size 1000)
      String upsertSql = "UPSERT INTO " + tableName + " VALUES (?, ?)";
      try (PreparedStatement ps = conn.prepareStatement(upsertSql)) {
        for (int i = 0; i < 10; i++) {
          ps.setString(1, "sm_" + i);
          ps.setArray(2, conn.createArrayOf("FLOAT", make4D(10f, 0f, 0f, 0f)));
          ps.executeUpdate();
        }
      }
      conn.commit();
      VectorIndexScorecard.reconcile(conn, indexName, 1L);

      DriftEvaluationResult result = VectorIndexScorecard.evaluate(conn, indexName, 1L, conf);
      assertFalse("Index below min.cluster.size must suppress rebuild", result.shouldRebuild());
      assertNull(result.getTriggerReason());
    }
  }

  @Test
  public void testScorecardIsGenerationScoped() throws Exception {
    String indexName = "IDX_GEN_SCOPE_" + generateUniqueName();

    try (Connection conn = DriverManager.getConnection(getUrl())) {
      // Seed scorecard rows for generation 1 and generation 2 directly
      long ts1 = 1000L;
      long ts2 = 2000L;
      List<ScorecardRow> gen1Rows = Arrays.asList(new ScorecardRow(indexName, 1L, 0, 50L, 2L, ts1),
        new ScorecardRow(indexName, 1L, 1, 60L, 3L, ts1));
      List<ScorecardRow> gen2Rows = Arrays.asList(new ScorecardRow(indexName, 2L, 0, 100L, 5L, ts2),
        new ScorecardRow(indexName, 2L, 1, 120L, 6L, ts2));
      CentroidManager.persistScorecard(conn, gen1Rows);
      CentroidManager.persistScorecard(conn, gen2Rows);

      // Reconcile or update generation 2
      List<ScorecardRow> updatedGen2 =
        Arrays.asList(new ScorecardRow(indexName, 2L, 0, 300L, 0L, 5000L),
          new ScorecardRow(indexName, 2L, 1, 400L, 0L, 5000L));
      CentroidManager.persistScorecard(conn, updatedGen2);

      // Verify generation 1 rows are completely unchanged
      List<ScorecardRow> loadedGen1 = CentroidManager.loadScorecard(conn, indexName, 1L);
      assertEquals(2, loadedGen1.size());
      assertEquals(Long.valueOf(50L), loadedGen1.get(0).getClusterSize());
      assertEquals(Long.valueOf(2L), loadedGen1.get(0).getReassignCount());
      assertEquals(Long.valueOf(ts1), loadedGen1.get(0).getLastScorecardUpdate());
      assertEquals(Long.valueOf(60L), loadedGen1.get(1).getClusterSize());
      assertEquals(Long.valueOf(3L), loadedGen1.get(1).getReassignCount());
      assertEquals(Long.valueOf(ts1), loadedGen1.get(1).getLastScorecardUpdate());
    }
  }

  /**
   * Verifies scorecard persistence and durability across region flush and reopen operations.
   */
  @Test
  public void testScorecardSurvivesRegionReopen() throws Exception {
    String tableName = "T_RESTART_DUR_" + generateUniqueName();
    String indexName = "IDX_RESTART_DUR_" + generateUniqueName();

    try (Connection conn = DriverManager.getConnection(getUrl())) {
      setupDeterministicVectorIndex(conn, tableName, indexName);

      String upsertSql = "UPSERT INTO " + tableName + " VALUES (?, ?)";
      try (PreparedStatement ps = conn.prepareStatement(upsertSql)) {
        for (int i = 0; i < 20; i++) {
          ps.setString(1, "r_" + i);
          float[] v = new float[4];
          v[i % 4] = 10.0f;
          ps.setArray(2, conn.createArrayOf("FLOAT", new Float[] { v[0], v[1], v[2], v[3] }));
          ps.executeUpdate();
        }
      }
      conn.commit();
      ScorecardAccumulator.getInstance().flush(conn);
    }

    // Flush memstore to HFile and reopen region to verify disk-level persistence.
    TableName centroidTable = TableName.valueOf(SYSTEM_VECTOR_CENTROID_NAME);
    try (Admin admin = getUtility().getAdmin()) {
      admin.flush(centroidTable);
      List<RegionInfo> regions = admin.getRegions(centroidTable);
      assertFalse("SYSTEM.VECTOR_CENTROID must have at least one region", regions.isEmpty());
      for (RegionInfo region : regions) {
        admin.unassign(region.getRegionName());
      }
      getUtility().waitUntilNoRegionsInTransition(60000);
    }

    VectorCentroidCache.resetInstance();
    try (Connection freshConn = DriverManager.getConnection(getUrl())) {
      Map<Integer, Long> counts = getScorecardCounts(freshConn, indexName, 1L);
      for (int c = 0; c < 4; c++) {
        assertEquals("Cluster size for centroid " + c + " must survive a region reopen",
          Long.valueOf(5L), counts.get(c));
      }
    }
  }

  @Test
  public void testGenerationalRebuild() throws Exception {
    String tableName = "T_GEN_REB_" + generateUniqueName();
    String indexName = "IDX_GEN_REB_" + generateUniqueName();

    try (Connection conn = DriverManager.getConnection(getUrl())) {
      setupDeterministicVectorIndex(conn, tableName, indexName);

      // Insert 100 rows (25 for each centroid)
      String upsertSql = "UPSERT INTO " + tableName + " VALUES (?, ?)";
      try (PreparedStatement ps = conn.prepareStatement(upsertSql)) {
        for (int i = 0; i < 100; i++) {
          ps.setString(1, "row_" + i);
          float[] v = new float[4];
          v[i % 4] = 10.0f;
          ps.setArray(2, conn.createArrayOf("FLOAT", new Float[] { v[0], v[1], v[2], v[3] }));
          ps.executeUpdate();
        }
      }
      conn.commit();

      // Trigger asynchronous rebuild
      Configuration conf = HBaseConfiguration.create();
      PhoenixConnection pconn = conn.unwrap(PhoenixConnection.class);
      VectorIndexRebuildTask.rebuild(pconn, conf, indexName, true, "MANUAL_REBUILD");

      // Assert SYSTEM.VECTOR_CENTROID contains generation 2 centroids and generation 1 rows are
      // deleted
      List<Long> gens = CentroidManager.listGenerations(conn, indexName);
      assertEquals(1, gens.size());
      assertEquals(Long.valueOf(2L), gens.get(0));

      List<byte[]> gen2Centroids = CentroidManager.loadCentroids(conn, indexName, 2L);
      assertFalse("Generation 2 centroids must exist", gen2Centroids.isEmpty());

      List<byte[]> gen1Centroids = CentroidManager.loadCentroids(conn, indexName, 1L);
      assertTrue("Generation 1 centroids must be deleted", gen1Centroids.isEmpty());

      // Assert SYSTEM.CATALOG records active generation as 2
      assertEquals(2L, CentroidManager.getGeneration(conn, indexName));
      pconn.removeTable(pconn.getTenantId(), indexName, null, HConstants.LATEST_TIMESTAMP);
      PTable pIndex = pconn.getTableNoCache(indexName);
      assertEquals(Long.valueOf(2L), pIndex.getVectorCentroidGeneration());
    }
  }

  @Test
  public void testGenerationsCoexistMidRebuild() throws Exception {
    String tableName = "T_MID_REB_" + generateUniqueName();
    String indexName = "IDX_MID_REB_" + generateUniqueName();

    try (Connection conn = DriverManager.getConnection(getUrl())) {
      setupDeterministicVectorIndex(conn, tableName, indexName);

      String upsertSql = "UPSERT INTO " + tableName + " VALUES (?, ?)";
      try (PreparedStatement ps = conn.prepareStatement(upsertSql)) {
        for (int i = 0; i < 40; i++) {
          ps.setString(1, "r_" + i);
          float[] v = new float[4];
          v[i % 4] = 10.0f;
          ps.setArray(2, conn.createArrayOf("FLOAT", new Float[] { v[0], v[1], v[2], v[3] }));
          ps.executeUpdate();
        }
      }
      conn.commit();

      boolean[] hookRan = new boolean[] { false };
      VectorIndexRebuildTask.setTestHook((idx, buildingGen) -> {
        hookRan[0] = true;
        try (Connection hookConn = DriverManager.getConnection(getUrl())) {
          // Assert SELECT DISTINCT GENERATION_ID returns both 1 and 2
          List<Long> gens = CentroidManager.listGenerations(hookConn, idx);
          assertTrue("Must contain generation 1", gens.contains(1L));
          assertTrue("Must contain generation 2", gens.contains(2L));

          // Assert system catalog reports generation 1 as active
          assertEquals("Catalog must report gen 1 as active mid-rebuild", 1L,
            CentroidManager.getGeneration(hookConn, idx));
        }
      });

      try {
        PhoenixConnection pconn = conn.unwrap(PhoenixConnection.class);
        VectorIndexRebuildTask.rebuild(pconn, HBaseConfiguration.create(), indexName, true, "TEST");
        assertTrue("Test hook must have executed mid-rebuild", hookRan[0]);
      } finally {
        VectorIndexRebuildTask.clearTestHook();
      }
    }
  }

  @Test
  public void testQueryContinuityDuringRebuild() throws Exception {
    String tableName = "T_QUERY_CONT_" + generateUniqueName();
    String indexName = "IDX_QUERY_CONT_" + generateUniqueName();

    try (Connection conn = DriverManager.getConnection(getUrl())) {
      setupDeterministicVectorIndex(conn, tableName, indexName);

      String upsertSql = "UPSERT INTO " + tableName + " VALUES (?, ?)";
      try (PreparedStatement ps = conn.prepareStatement(upsertSql)) {
        for (int i = 0; i < 20; i++) {
          ps.setString(1, "row_" + i);
          float[] v = new float[4];
          v[i % 4] = 10.0f;
          ps.setArray(2, conn.createArrayOf("FLOAT", new Float[] { v[0], v[1], v[2], v[3] }));
          ps.executeUpdate();
        }
      }
      conn.commit();

      boolean[] hookRan = new boolean[] { false };
      VectorIndexRebuildTask.setTestHook((idx, buildingGen) -> {
        hookRan[0] = true;
        try (Connection hookConn = DriverManager.getConnection(getUrl())) {
          // While rebuild is paused mid-execution, execute vector search query
          String querySql = "SELECT ID FROM " + tableName + " ORDER BY L2_DISTANCE(V, ?) LIMIT 2";
          try (PreparedStatement ps = hookConn.prepareStatement(querySql)) {
            ps.setArray(1,
              hookConn.createArrayOf("FLOAT", new Float[] { 10.0f, 0.0f, 0.0f, 0.0f }));
            try (ResultSet rs = ps.executeQuery()) {
              assertTrue(rs.next());
              String id = rs.getString(1);
              assertTrue("Must serve rows from centroid 0 of gen 1",
                id.equals("row_0") || id.equals("row_4") || id.equals("row_8")
                  || id.equals("row_12") || id.equals("row_16"));
            }
          }
        }
      });

      try {
        PhoenixConnection pconn = conn.unwrap(PhoenixConnection.class);
        VectorIndexRebuildTask.rebuild(pconn, HBaseConfiguration.create(), indexName, true, "TEST");
        assertTrue("Hook must have run", hookRan[0]);
      } finally {
        VectorIndexRebuildTask.clearTestHook();
      }
    }
  }

  /**
   * Verifies query execution and probe policy behaviors (EXPAND and EXACT) while an index rebuild
   * is in progress.
   */
  @Test
  public void testProbeAdjustmentWhileRebuildInProgress() throws Exception {
    String tableName = "T_REB_PROBE_" + generateUniqueName();
    String indexName = "IDX_REB_PROBE_" + generateUniqueName();

    try (Connection conn = DriverManager.getConnection(getUrl())) {
      setupDeterministicVectorIndex(conn, tableName, indexName);

      String upsertSql = "UPSERT INTO " + tableName + " VALUES (?, ?)";
      try (PreparedStatement ps = conn.prepareStatement(upsertSql)) {
        for (int i = 0; i < 40; i++) {
          ps.setString(1, "r_" + i);
          float[] v = new float[4];
          v[i % 4] = 10.0f;
          ps.setArray(2, conn.createArrayOf("FLOAT", new Float[] { v[0], v[1], v[2], v[3] }));
          ps.executeUpdate();
        }
      }
      conn.commit();

      Float[] probeVector = make4D(10.0f, 0.0f, 0.0f, 0.0f);
      // For lists=4, default probe count is round(sqrt(4)) = 2.
      String baseline = explainVectorQuery(conn, tableName, probeVector, null);
      assertTrue(baseline, baseline.contains("CLIENT PROBING 2 OF 4 CENTROIDS"));
      assertFalse("no rebuild is running yet", baseline.contains("REBUILD IN PROGRESS"));
      List<String> expected = topKByDistance(conn, tableName, probeVector, 4, true);

      final List<String> failures = new ArrayList<>();
      final boolean[] hookRan = new boolean[] { false };
      VectorIndexRebuildTask.setTestHook((idx, buildingGen) -> {
        hookRan[0] = true;
        try {
          // Invalidate cached centroids so the subsequent query reloads state and detects the
          // active rebuild.
          VectorCentroidCache.getInstance().invalidate(idx);
          try (Connection expandConn = DriverManager.getConnection(getUrl())) {
            String plan = explainVectorQuery(expandConn, tableName, probeVector, null);
            if (!plan.contains("CLIENT PROBING 4 OF 4 CENTROIDS")) {
              failures.add("EXPAND should have widened 2 probes to 4, plan was: " + plan);
            }
            if (!plan.contains("REBUILD IN PROGRESS: EXPANDED FROM 2")) {
              failures.add("EXPAND should say why it widened, plan was: " + plan);
            }
            if (!topKByDistance(expandConn, tableName, probeVector, 4, false).equals(expected)) {
              failures.add("EXPAND returned the wrong rows mid-rebuild");
            }
          }

          VectorCentroidCache.getInstance().invalidate(idx);
          Properties exactProps = new Properties();
          exactProps.setProperty(QueryServices.VECTOR_INDEX_REBUILD_PROBE_POLICY_ATTRIB, "EXACT");
          try (Connection exactConn = DriverManager.getConnection(getUrl(), exactProps)) {
            String plan = explainVectorQuery(exactConn, tableName, probeVector, exactProps);
            if (!plan.contains("CLIENT EXACT VECTOR EVALUATION")) {
              failures.add("EXACT should stop probing, plan was: " + plan);
            }
            if (plan.contains("CLIENT PROBING")) {
              failures.add("EXACT must not also probe, plan was: " + plan);
            }
            if (!topKByDistance(exactConn, tableName, probeVector, 4, false).equals(expected)) {
              failures.add("EXACT returned the wrong rows mid-rebuild");
            }
          }
        } catch (SQLException e) {
          failures.add("mid-rebuild query failed: " + e);
        }
      });

      try {
        PhoenixConnection pconn = conn.unwrap(PhoenixConnection.class);
        VectorIndexRebuildTask.rebuild(pconn, HBaseConfiguration.create(), indexName, true, "TEST");
      } finally {
        VectorIndexRebuildTask.clearTestHook();
      }
      assertTrue("the rebuild hook must have run", hookRan[0]);
      assertTrue(failures.toString(), failures.isEmpty());

      // Verify probe expansion ceases after rebuild completion.
      VectorCentroidCache.getInstance().invalidate(indexName);
      try (Connection after = DriverManager.getConnection(getUrl())) {
        String plan = explainVectorQuery(after, tableName, probeVector, null);
        assertFalse("a completed rebuild must not keep widening probes: " + plan,
          plan.contains("REBUILD IN PROGRESS"));
      }
    }
  }

  private static String explainVectorQuery(Connection conn, String tableName, Float[] probeVector,
    Properties props) throws SQLException {
    String sql = "EXPLAIN SELECT ID FROM " + tableName + " ORDER BY L2_DISTANCE(V, ?) LIMIT 4";
    StringBuilder plan = new StringBuilder();
    try (PreparedStatement ps = conn.prepareStatement(sql)) {
      ps.setArray(1, conn.createArrayOf("FLOAT", probeVector));
      try (ResultSet rs = ps.executeQuery()) {
        while (rs.next()) {
          plan.append(rs.getString(1)).append('\n');
        }
      }
    }
    return plan.toString();
  }

  @Test
  public void testPostRebuildQueryCorrectness() throws Exception {
    String tableName = "T_POST_REB_Q_" + generateUniqueName();
    String indexName = "IDX_POST_REB_Q_" + generateUniqueName();

    try (Connection conn = DriverManager.getConnection(getUrl())) {
      setupDeterministicVectorIndex(conn, tableName, indexName);

      // Insert data points offset from initial centroids to induce centroid repositioning.
      String upsertSql = "UPSERT INTO " + tableName + " VALUES (?, ?)";
      try (PreparedStatement ps = conn.prepareStatement(upsertSql)) {
        for (int i = 0; i < 40; i++) {
          ps.setString(1, "r_" + i);
          float[] v = new float[4];
          v[i % 4] = 10.0f;
          v[(i + 1) % 4] = (i % 10) * 0.5f;
          ps.setArray(2, conn.createArrayOf("FLOAT", new Float[] { v[0], v[1], v[2], v[3] }));
          ps.executeUpdate();
        }
      }
      conn.commit();

      Float[] probe = make4D(10.0f, 1.5f, 0.0f, 0.0f);
      List<String> beforeRebuild = topKByDistance(conn, tableName, probe, 5, false);

      PhoenixConnection pconn = conn.unwrap(PhoenixConnection.class);
      VectorIndexRebuildTask.rebuild(pconn, HBaseConfiguration.create(), indexName, true, "MANUAL");

      // Verify post-rebuild index scan results match a brute-force data table scan without
      // duplicates.
      List<String> indexed = topKByDistance(conn, tableName, probe, 5, false);
      List<String> bruteForce = topKByDistance(conn, tableName, probe, 5, true);
      assertEquals("post-rebuild results must match a brute-force scan", bruteForce, indexed);
      assertEquals("no row may appear twice after the generation switch", indexed.size(),
        new HashSet<>(indexed).size());
      assertEquals("rebuilding must not change which rows are nearest", beforeRebuild, indexed);
    }
  }

  /** Runs the nearest-neighbour query, optionally forcing a full data table scan. */
  private List<String> topKByDistance(Connection conn, String tableName, Float[] probe, int k,
    boolean noIndex) throws SQLException {
    String sql = "SELECT " + (noIndex ? "/*+ NO_INDEX */ " : "") + "ID FROM " + tableName
      + " ORDER BY L2_DISTANCE(V, ?) LIMIT " + k;
    List<String> ids = new ArrayList<>();
    try (PreparedStatement ps = conn.prepareStatement(sql)) {
      ps.setArray(1, conn.createArrayOf("FLOAT", probe));
      try (ResultSet rs = ps.executeQuery()) {
        while (rs.next()) {
          ids.add(rs.getString(1));
        }
      }
    }
    return ids;
  }

  @Test
  public void testScorecardReseededOnSwitch() throws Exception {
    String tableName = "T_RESEED_" + generateUniqueName();
    String indexName = "IDX_RESEED_" + generateUniqueName();

    try (Connection conn = DriverManager.getConnection(getUrl())) {
      setupDeterministicVectorIndex(conn, tableName, indexName);

      String upsertSql = "UPSERT INTO " + tableName + " VALUES (?, ?)";
      try (PreparedStatement ps = conn.prepareStatement(upsertSql)) {
        for (int i = 0; i < 100; i++) {
          ps.setString(1, "r_" + i);
          float[] v = new float[4];
          v[i % 4] = 10.0f;
          ps.setArray(2, conn.createArrayOf("FLOAT", new Float[] { v[0], v[1], v[2], v[3] }));
          ps.executeUpdate();
        }
      }
      conn.commit();

      PhoenixConnection pconn = conn.unwrap(PhoenixConnection.class);
      VectorIndexRebuildTask.rebuild(pconn, HBaseConfiguration.create(), indexName, true, "MANUAL");

      // Assert generation 2 scorecard reports non-null CLUSTER_SIZE summing to row count
      List<ScorecardRow> gen2Scorecard = CentroidManager.loadScorecard(conn, indexName, 2L);
      assertFalse("Generation 2 scorecard must exist", gen2Scorecard.isEmpty());
      long totalClusterSize = 0L;
      for (ScorecardRow row : gen2Scorecard) {
        assertNotNull("CLUSTER_SIZE must be non-null", row.getClusterSize());
        totalClusterSize += row.getClusterSize();
      }
      assertEquals("Generation 2 cluster size must sum to row count", 100L, totalClusterSize);

      // Assert generation 1 scorecard rows are removed
      List<ScorecardRow> gen1Scorecard = CentroidManager.loadScorecard(conn, indexName, 1L);
      assertTrue("Generation 1 scorecard rows must be removed", gen1Scorecard.isEmpty());
    }
  }

  @Test
  public void testRetirementDeletesInRegion() throws Exception {
    String indexName = "IDX_RETIRE_EXPLAIN_" + generateUniqueName();

    try (Connection conn = DriverManager.getConnection(getUrl())) {
      String plan = CentroidManager.getDeleteGenerationExplainPlan(conn, indexName, 1L);
      assertTrue("Plan must report DELETE ROWS SERVER SELECT, was: " + plan,
        plan.contains("DELETE ROWS SERVER SELECT"));
      assertTrue("Plan must include SYSTEM.VECTOR_CENTROID, was: " + plan,
        plan.contains(SYSTEM_VECTOR_CENTROID_NAME));
    }
  }

  @Test
  public void testRetirementDoesNotCommitUnrelatedWork() throws Exception {
    String unrelatedTable = "T_UNRELATED_" + generateUniqueName();
    String indexName = "IDX_RETIRE_NO_COMMIT_" + generateUniqueName();

    try (Connection conn = DriverManager.getConnection(getUrl())) {
      try (Statement stmt = conn.createStatement()) {
        stmt.execute("CREATE TABLE " + unrelatedTable + " (ID VARCHAR PRIMARY KEY, VAL VARCHAR)");
      }

      // Seed dummy generation 1 rows
      List<float[]> centroids = Arrays.asList(new float[] { 1.0f, 0.0f, 0.0f, 0.0f });
      CentroidManager.persistCentroidsFromFloatList(conn, indexName, 1L, centroids);

      // On a connection with autoCommit off, issue an unrelated uncommitted upsert
      conn.setAutoCommit(false);
      try (PreparedStatement ps =
        conn.prepareStatement("UPSERT INTO " + unrelatedTable + " VALUES (?, ?)")) {
        ps.setString(1, "k1");
        ps.setString(2, "v1");
        ps.executeUpdate();
      }

      // Call deleteGeneration
      CentroidManager.deleteGeneration(conn, indexName, 1L);

      // Assert generation 1 rows are gone
      assertTrue(CentroidManager.loadCentroids(conn, indexName, 1L).isEmpty());

      // Assert unrelated upsert remains uncommitted (not visible to separate connection)
      try (Connection conn2 = DriverManager.getConnection(getUrl())) {
        try (Statement s = conn2.createStatement();
          ResultSet rs = s.executeQuery("SELECT * FROM " + unrelatedTable + " WHERE ID = 'k1'")) {
          assertFalse("Unrelated upsert must remain uncommitted", rs.next());
        }
      }

      // Commit the unrelated upsert and verify it is now visible
      conn.commit();
      try (Connection conn2 = DriverManager.getConnection(getUrl())) {
        try (Statement s = conn2.createStatement();
          ResultSet rs = s.executeQuery("SELECT * FROM " + unrelatedTable + " WHERE ID = 'k1'")) {
          assertTrue("Unrelated upsert visible after commit", rs.next());
        }
      }
    }
  }

  @Test
  public void testAutomaticTriggerFromDrift() throws Exception {
    String tableName = "T_AUTO_DRIFT_" + generateUniqueName();
    String indexName = "IDX_AUTO_DRIFT_" + generateUniqueName();

    try (Connection conn = DriverManager.getConnection(getUrl())) {
      setupDeterministicVectorIndex(conn, tableName, indexName);

      // Insert 100 rows all assigned to centroid 0 (skewed data)
      String upsertSql = "UPSERT INTO " + tableName + " VALUES (?, ?)";
      try (PreparedStatement ps = conn.prepareStatement(upsertSql)) {
        for (int i = 0; i < 100; i++) {
          ps.setString(1, "r_" + i);
          float[] v = new float[] { 10.0f, 0.0f, 0.0f, 0.0f };
          ps.setArray(2, conn.createArrayOf("FLOAT", new Float[] { v[0], v[1], v[2], v[3] }));
          ps.executeUpdate();
        }
      }
      conn.commit();
      ScorecardAccumulator.getInstance().flush(conn);

      Configuration conf = HBaseConfiguration.create();
      conf.setBoolean(QueryServices.VECTOR_INDEX_REBUILD_AUTO_ENABLED_ATTRIB, true);
      conf.setLong(QueryServices.VECTOR_INDEX_DRIFT_MIN_CLUSTER_SIZE_ATTRIB, 50L);
      conf.setDouble(QueryServices.VECTOR_INDEX_DRIFT_SKEW_RATIO_THRESHOLD_ATTRIB, 3.5);

      PhoenixConnection pconn = conn.unwrap(PhoenixConnection.class);
      TaskRegionObserver.TaskResult result =
        VectorIndexRebuildTask.rebuild(pconn, conf, indexName, false, null);

      assertEquals(TaskRegionObserver.TaskResultCode.SUCCESS, result.getResultCode());
      assertEquals(2L, CentroidManager.getGeneration(conn, indexName));

      GenerationSummary summary = CentroidManager.loadGenerationSummary(conn, indexName, 2L);
      assertNotNull("Generation 2 summary must exist", summary);
      assertNotNull("TRIGGER_REASON must record skew", summary.getTriggerReason());
      assertTrue("Trigger reason must mention skew",
        summary.getTriggerReason().toLowerCase().contains("skew"));
    }
  }

  @Test
  public void testReconciliationGatesTheRebuild() throws Exception {
    String tableName = "T_RECON_GATE_" + generateUniqueName();
    String indexName = "IDX_RECON_GATE_" + generateUniqueName();

    try (Connection conn = DriverManager.getConnection(getUrl())) {
      setupDeterministicVectorIndex(conn, tableName, indexName);

      // Insert balanced data (25 rows in each of the 4 centroids = 100 rows)
      String upsertSql = "UPSERT INTO " + tableName + " VALUES (?, ?)";
      try (PreparedStatement ps = conn.prepareStatement(upsertSql)) {
        for (int i = 0; i < 100; i++) {
          ps.setString(1, "r_" + i);
          float[] v = new float[4];
          v[i % 4] = 10.0f;
          ps.setArray(2, conn.createArrayOf("FLOAT", new Float[] { v[0], v[1], v[2], v[3] }));
          ps.executeUpdate();
        }
      }
      conn.commit();
      ScorecardAccumulator.getInstance().flush(conn);

      // Manually corrupt the inline scorecard to simulate severe skew
      CentroidManager.persistScorecardRow(conn, indexName, 1L, 0, 999999L, 0L, null);

      Configuration conf = HBaseConfiguration.create();
      conf.setBoolean(QueryServices.VECTOR_INDEX_REBUILD_AUTO_ENABLED_ATTRIB, true);
      conf.setLong(QueryServices.VECTOR_INDEX_DRIFT_MIN_CLUSTER_SIZE_ATTRIB, 50L);
      conf.setDouble(QueryServices.VECTOR_INDEX_DRIFT_SKEW_RATIO_THRESHOLD_ATTRIB, 3.5);

      PhoenixConnection pconn = conn.unwrap(PhoenixConnection.class);
      TaskRegionObserver.TaskResult result =
        VectorIndexRebuildTask.rebuild(pconn, conf, indexName, false, null);

      // Generation remains unchanged because reconciliation corrected the scorecard before
      // evaluation.
      assertEquals(TaskRegionObserver.TaskResultCode.SKIPPED, result.getResultCode());
      assertEquals(1L, CentroidManager.getGeneration(conn, indexName));

      // Assert scorecard was reconciled to accurate values (25 per centroid)
      List<ScorecardRow> scorecard = CentroidManager.loadScorecard(conn, indexName, 1L);
      for (ScorecardRow row : scorecard) {
        assertEquals("Cluster size must be restored to accurate value 25", Long.valueOf(25L),
          row.getClusterSize());
      }
    }
  }

  @Test
  public void testRebuildStormGuard() throws Exception {
    String tableName = "T_STORM_GUARD_" + generateUniqueName();
    String indexName = "IDX_STORM_GUARD_" + generateUniqueName();

    try (Connection conn = DriverManager.getConnection(getUrl())) {
      setupDeterministicVectorIndex(conn, tableName, indexName);

      // Stamp a recent LAST_REBUILD_TIME on generation 1 summary
      long now = System.currentTimeMillis();
      CentroidManager.persistGenerationSummary(conn, indexName, 1L, null, null, null, now, now);

      // Insert skewed data
      String upsertSql = "UPSERT INTO " + tableName + " VALUES (?, ?)";
      try (PreparedStatement ps = conn.prepareStatement(upsertSql)) {
        for (int i = 0; i < 100; i++) {
          ps.setString(1, "r_" + i);
          float[] v = new float[] { 10.0f, 0.0f, 0.0f, 0.0f };
          ps.setArray(2, conn.createArrayOf("FLOAT", new Float[] { v[0], v[1], v[2], v[3] }));
          ps.executeUpdate();
        }
      }
      conn.commit();
      ScorecardAccumulator.getInstance().flush(conn);

      Configuration conf = HBaseConfiguration.create();
      conf.setBoolean(QueryServices.VECTOR_INDEX_REBUILD_AUTO_ENABLED_ATTRIB, true);
      conf.setLong(QueryServices.VECTOR_INDEX_DRIFT_MIN_CLUSTER_SIZE_ATTRIB, 50L);
      conf.setDouble(QueryServices.VECTOR_INDEX_DRIFT_SKEW_RATIO_THRESHOLD_ATTRIB, 3.5);
      conf.setLong(QueryServices.VECTOR_INDEX_REBUILD_MIN_INTERVAL_MS_ATTRIB, 86400000L);

      PhoenixConnection pconn = conn.unwrap(PhoenixConnection.class);
      TaskRegionObserver.TaskResult result =
        VectorIndexRebuildTask.rebuild(pconn, conf, indexName, false, null);

      // Generation must not advance due to storm guard
      assertEquals(TaskRegionObserver.TaskResultCode.SKIPPED, result.getResultCode());
      assertEquals(1L, CentroidManager.getGeneration(conn, indexName));
    }
  }

  @Test
  public void testAutomaticRebuildOffByDefault() throws Exception {
    String tableName = "T_AUTO_OFF_" + generateUniqueName();
    String indexName = "IDX_AUTO_OFF_" + generateUniqueName();

    try (Connection conn = DriverManager.getConnection(getUrl())) {
      setupDeterministicVectorIndex(conn, tableName, indexName);

      // Insert skewed data
      String upsertSql = "UPSERT INTO " + tableName + " VALUES (?, ?)";
      try (PreparedStatement ps = conn.prepareStatement(upsertSql)) {
        for (int i = 0; i < 100; i++) {
          ps.setString(1, "r_" + i);
          float[] v = new float[] { 10.0f, 0.0f, 0.0f, 0.0f };
          ps.setArray(2, conn.createArrayOf("FLOAT", new Float[] { v[0], v[1], v[2], v[3] }));
          ps.executeUpdate();
        }
      }
      conn.commit();
      ScorecardAccumulator.getInstance().flush(conn);

      Configuration conf = HBaseConfiguration.create();
      // rebuild.auto.enabled = false (default)
      conf.setBoolean(QueryServices.VECTOR_INDEX_REBUILD_AUTO_ENABLED_ATTRIB, false);
      conf.setLong(QueryServices.VECTOR_INDEX_DRIFT_MIN_CLUSTER_SIZE_ATTRIB, 50L);
      conf.setDouble(QueryServices.VECTOR_INDEX_DRIFT_SKEW_RATIO_THRESHOLD_ATTRIB, 3.5);

      PhoenixConnection pconn = conn.unwrap(PhoenixConnection.class);
      TaskRegionObserver.TaskResult result =
        VectorIndexRebuildTask.rebuild(pconn, conf, indexName, false, null);

      // Generation must not advance
      assertEquals(TaskRegionObserver.TaskResultCode.SKIPPED, result.getResultCode());
      assertEquals(1L, CentroidManager.getGeneration(conn, indexName));

      // Assessment must be recorded on the summary row
      GenerationSummary summary = CentroidManager.loadGenerationSummary(conn, indexName, 1L);
      assertNotNull("Summary row must exist", summary);
      assertNotNull("Assessment must be recorded in TRIGGER_REASON", summary.getTriggerReason());
      assertTrue("Trigger reason must mention skew",
        summary.getTriggerReason().toLowerCase().contains("skew"));
    }
  }

  @Test
  public void testAlterIndexRebuildSucceeds() throws Exception {
    String tableName = "T_ALT_REBUILD_" + generateUniqueName();
    String indexName = "IDX_ALT_REBUILD_" + generateUniqueName();

    try (Connection conn = DriverManager.getConnection(getUrl())) {
      setupDeterministicVectorIndex(conn, tableName, indexName);

      // Insert 100 rows across 4 centroids
      String upsertSql = "UPSERT INTO " + tableName + " VALUES (?, ?)";
      try (PreparedStatement ps = conn.prepareStatement(upsertSql)) {
        for (int i = 0; i < 100; i++) {
          ps.setString(1, "r_" + i);
          float[] v = new float[4];
          v[i % 4] = 10.0f;
          ps.setArray(2, conn.createArrayOf("FLOAT", new Float[] { v[0], v[1], v[2], v[3] }));
          ps.executeUpdate();
        }
      }
      conn.commit();

      assertEquals(1L, CentroidManager.getGeneration(conn, indexName));

      try (Statement stmt = conn.createStatement()) {
        stmt.execute("ALTER INDEX " + indexName + " ON " + tableName + " REBUILD");
      }
      assertEquals(2L, CentroidManager.getGeneration(conn, indexName));

      // Verify subsequent rebuild using the optional VECTOR keyword increments the generation.
      try (Statement stmt = conn.createStatement()) {
        stmt.execute("ALTER VECTOR INDEX " + indexName + " ON " + tableName + " REBUILD");
      }
      assertEquals(3L, CentroidManager.getGeneration(conn, indexName));
      assertEquals("only the live generation survives a rebuild", Arrays.asList(3L),
        CentroidManager.listGenerations(conn, indexName));
    }
  }

  @Test
  public void testNewCentroidsTrained() throws Exception {
    String tableName = "T_NEW_CENTROIDS_" + generateUniqueName();
    String indexName = "IDX_NEW_CENTROIDS_" + generateUniqueName();

    try (Connection conn = DriverManager.getConnection(getUrl())) {
      setupDeterministicVectorIndex(conn, tableName, indexName);

      // Insert sample data offset from baseline unit-axis centroids to prompt retraining.
      String upsertSql = "UPSERT INTO " + tableName + " VALUES (?, ?)";
      try (PreparedStatement ps = conn.prepareStatement(upsertSql)) {
        for (int i = 0; i < 100; i++) {
          ps.setString(1, "r_" + i);
          float[] v = new float[4];
          v[i % 4] = 2.0f;
          v[(i + 2) % 4] = 1.0f;
          ps.setArray(2, conn.createArrayOf("FLOAT", new Float[] { v[0], v[1], v[2], v[3] }));
          ps.executeUpdate();
        }
      }
      conn.commit();

      List<float[]> gen1 = CentroidManager.loadCentroidsAsFloatVectors(conn, indexName, 1L);
      assertEquals(4, gen1.size());

      try (Statement stmt = conn.createStatement()) {
        stmt.execute("ALTER INDEX " + indexName + " ON " + tableName + " REBUILD");
      }

      List<float[]> gen2 = CentroidManager.loadCentroidsAsFloatVectors(conn, indexName, 2L);
      assertEquals(4, gen2.size());

      // Verify retrained centroids converge near sample data distribution.
      for (float[] centroid : gen2) {
        double norm = 0.0;
        for (float c : centroid) {
          norm += c * c;
        }
        assertTrue(
          "retrained centroid " + Arrays.toString(centroid)
            + " must sit near the data, not on the seeded magnitude-10 axes",
          Math.sqrt(norm) < 5.0);
      }

      assertTrue("Generation 1 centroids must be retired",
        CentroidManager.loadCentroids(conn, indexName, 1L).isEmpty());
    }
  }

  @Test
  public void testManualTriggerBypassesGuards() throws Exception {
    String tableName = "T_MANUAL_BYPASS_" + generateUniqueName();
    String indexName = "IDX_MANUAL_BYPASS_" + generateUniqueName();

    try (Connection conn = DriverManager.getConnection(getUrl())) {
      setupDeterministicVectorIndex(conn, tableName, indexName);

      // Stamp LAST_REBUILD_TIME within the minimum rebuild interval.
      long now = System.currentTimeMillis();
      CentroidManager.persistGenerationSummary(conn, indexName, 1L, null, null, null, now, now);

      // Insert 100 rows
      String upsertSql = "UPSERT INTO " + tableName + " VALUES (?, ?)";
      try (PreparedStatement ps = conn.prepareStatement(upsertSql)) {
        for (int i = 0; i < 100; i++) {
          ps.setString(1, "r_" + i);
          float[] v = new float[4];
          v[i % 4] = 10.0f;
          ps.setArray(2, conn.createArrayOf("FLOAT", new Float[] { v[0], v[1], v[2], v[3] }));
          ps.executeUpdate();
        }
      }
      conn.commit();

      // Ensure rebuild.auto.enabled is false (default) and execute ALTER INDEX ... REBUILD
      try (Statement stmt = conn.createStatement()) {
        stmt.execute("ALTER INDEX " + indexName + " ON " + tableName + " REBUILD");
      }

      // Assert generation advances despite guards
      assertEquals(2L, CentroidManager.getGeneration(conn, indexName));

      // Assert TRIGGER_REASON records manual execution
      GenerationSummary summary = CentroidManager.loadGenerationSummary(conn, indexName, 2L);
      assertNotNull("Summary row must exist for generation 2", summary);
      assertNotNull("TRIGGER_REASON must not be null", summary.getTriggerReason());
      assertTrue("TRIGGER_REASON must record manual execution, was: " + summary.getTriggerReason(),
        summary.getTriggerReason().toLowerCase().contains("manual"));
    }
  }
}
