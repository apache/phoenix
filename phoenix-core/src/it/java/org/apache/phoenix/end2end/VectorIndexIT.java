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
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Mutation;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
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
import org.apache.phoenix.hbase.index.covered.update.ColumnReference;
import org.apache.phoenix.hbase.index.util.VersionUtil;
import org.apache.phoenix.index.IndexMaintainer;
import org.apache.phoenix.jdbc.PhoenixConnection;
import org.apache.phoenix.jdbc.PhoenixDatabaseMetaData;
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
}
