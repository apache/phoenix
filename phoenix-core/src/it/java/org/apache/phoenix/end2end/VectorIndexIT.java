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
import java.util.List;
import java.util.Map;
import java.util.Properties;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Mutation;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.client.TableDescriptor;
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
import org.apache.phoenix.schema.types.PVectorFloat;
import org.apache.phoenix.util.ByteUtil;
import org.apache.phoenix.util.ClientUtil;
import org.apache.phoenix.util.Closeables;
import org.apache.phoenix.util.IndexUtil;
import org.apache.phoenix.util.MetaDataUtil;
import org.apache.phoenix.util.PropertiesUtil;
import org.apache.phoenix.util.QueryUtil;
import org.apache.phoenix.util.SchemaUtil;
import org.apache.phoenix.util.StringUtil;
import org.apache.phoenix.util.TestUtil;
import org.junit.Test;
import org.junit.experimental.categories.Category;

/**
 * Tests how MetaDataEndpointImpl checks vector index metadata and writes it to SYSTEM.CATALOG.
 */
@Category(ParallelStatsDisabledTest.class)
public class VectorIndexIT extends ParallelStatsDisabledIT {

  /** Tests that the server rejects a vector index with an algorithm that it does not support. */
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

  /** Tests that the server rejects a vector index with a distance metric that it does not know. */
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

  /** Tests that the server rejects IVF metadata with a list count of zero. */
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

  /** Tests that the server rejects IVF metadata with a sample size less than the list count. */
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

  /** Tests that the server rejects vector metadata on a table that is not an index. */
  @Test
  public void testVectorMetadataOnNonIndexTableRejection() throws Exception {
    String tableName = "T_NOT_INDEX_" + generateUniqueName();
    String otherName = "T_OTHER_" + generateUniqueName();

    try (Connection conn = DriverManager.getConnection(getUrl())) {
      try (Statement stmt = conn.createStatement()) {
        stmt.execute(
          "CREATE TABLE " + tableName + " (ID VARCHAR NOT NULL PRIMARY KEY, V VECTOR(FLOAT, 128))");
      }
      PhoenixConnection pconn = conn.unwrap(PhoenixConnection.class);

      try {
        createVectorIndexMetadata(pconn, null, otherName, tableName, "V", PVectorFloat.INSTANCE,
          128, "IVF", "L2", 128, 16, 500, PTableType.TABLE);
        fail("Expected vector index metadata on a non-index table to be rejected");
      } catch (Exception e) {
        assertExpectedSqlException(e, SQLExceptionCode.INVALID_VECTOR_INDEX_PARAMS);
      }
    }
  }

  /**
   * Tests that the server accepts a vector index on a VECTOR column and returns a PTable that has
   * the requested vector metadata.
   */
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

  /**
   * Tests that the server accepts a vector index on a BSON column and returns a PTable that has the
   * requested vector metadata.
   */
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
    return createVectorIndexMetadata(pconn, schemaName, indexName, baseTableName, indexedColumnName,
      indexedColumnType, columnSize, algorithm, metric, dimension, lists, sampleSize,
      PTableType.INDEX);
  }

  private MetaDataResponse createVectorIndexMetadata(PhoenixConnection pconn, String schemaName,
    String indexName, String baseTableName, String indexedColumnName,
    PDataType<?> indexedColumnType, Integer columnSize, String algorithm, String metric,
    Integer dimension, Integer lists, Integer sampleSize, PTableType tableType) throws Exception {
    byte[] schemaBytes = schemaName == null ? ByteUtil.EMPTY_BYTE_ARRAY : Bytes.toBytes(schemaName);
    byte[] indexTableBytes = Bytes.toBytes(indexName);
    byte[] baseTableBytes = Bytes.toBytes(baseTableName);
    byte[] tableKey = SchemaUtil.getTableKey(null, schemaBytes, indexTableBytes);
    byte[] baseTableKey = SchemaUtil.getTableKey(null, schemaBytes, baseTableBytes);

    List<Mutation> tableMetadata = new ArrayList<>();

    Put headerPut = new Put(tableKey);
    headerPut.addColumn(TABLE_FAMILY_BYTES, TABLE_TYPE_BYTES,
      Bytes.toBytes(tableType.getSerializedValue()));
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

  /**
   * Verifies that CREATE VECTOR INDEX writes the vector metadata to the catalog and creates the
   * expected index table layout.
   */
  @Test
  public void testCreateVectorIndexTableAndMetadata() throws Exception {
    String tableName = "T_VEC_DDL_" + generateUniqueName();
    String indexName = "IDX_VEC_DDL_" + generateUniqueName();

    try (Connection conn = DriverManager.getConnection(getUrl())) {
      try (Statement stmt = conn.createStatement()) {
        stmt.execute(
          "CREATE TABLE " + tableName + " (ID VARCHAR NOT NULL PRIMARY KEY, V VECTOR(FLOAT, 128))");
        stmt.execute("CREATE VECTOR INDEX " + indexName + " ON " + tableName + " (V) "
          + "WITH (algorithm = 'IVF', metric = 'L2', lists = 16, sample_size = 500)");
      }

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

      // The row key starts with the centroid ID. The data table primary key columns follow it.
      List<PColumn> pkColumns = indexTable.getPKColumns();
      assertEquals("Row key must have 2 PK columns", 2, pkColumns.size());
      PColumn pk0 = pkColumns.get(0);
      assertEquals(MetaDataUtil.VECTOR_CENTROID_ID_COLUMN_NAME, pk0.getName().getString());
      assertEquals(PInteger.INSTANCE, pk0.getDataType());
      assertFalse("Centroid ID column must not be nullable", pk0.isNullable());

      PColumn pk1 = pkColumns.get(1);
      assertEquals(":ID", pk1.getName().getString());
      assertEquals(PVarchar.INSTANCE, pk1.getDataType());

      // The indexed vector is a column in a column family. It is not part of the row key.
      PColumn vectorCol = indexTable.getColumnForColumnName("0:V");
      assertNotNull("Vector column 0:V must exist in index table", vectorCol);
      assertEquals(PVectorFloat.INSTANCE, vectorCol.getDataType());
      assertEquals(Integer.valueOf(128), vectorCol.getMaxLength());
      assertNotNull("Vector column must belong to a column family", vectorCol.getFamilyName());

      // The vector options are catalog metadata only. The HBase table descriptor must not contain
      // them.
      try (Admin admin = pconn.getQueryServices().getAdmin()) {
        TableDescriptor td = admin.getDescriptor(
          org.apache.hadoop.hbase.TableName.valueOf(indexTable.getPhysicalName().getString()));
        for (String key : new String[] { "METRIC", "ALGORITHM", "LISTS", "SAMPLE_SIZE",
          PhoenixDatabaseMetaData.VECTOR_INDEX_ALGORITHM,
          PhoenixDatabaseMetaData.VECTOR_DISTANCE_METRIC, PhoenixDatabaseMetaData.VECTOR_DIMENSION,
          PhoenixDatabaseMetaData.VECTOR_IVF_LISTS, PhoenixDatabaseMetaData.VECTOR_IVF_SAMPLE_SIZE,
          PhoenixDatabaseMetaData.VECTOR_CENTROID_GENERATION }) {
          assertNull("Index descriptor must not carry " + key, td.getValue(key));
        }
      }
    }
  }

  /**
   * Verifies that a data column named CENTROID_ID does not collide with the centroid ID column at
   * the start of the index row key.
   */
  @Test
  public void testCentroidColumnDoesNotCollideWithDataColumn() throws Exception {
    String tableName = "T_CENTROID_PK_" + generateUniqueName();
    String indexName = "IDX_CENTROID_PK_" + generateUniqueName();

    try (Connection conn = DriverManager.getConnection(getUrl())) {
      try (Statement stmt = conn.createStatement()) {
        stmt.execute("CREATE TABLE " + tableName
          + " (CENTROID_ID INTEGER NOT NULL PRIMARY KEY, V VECTOR(FLOAT, 4))");
        stmt.execute("CREATE VECTOR INDEX " + indexName + " ON " + tableName + " (V) "
          + "WITH (algorithm = 'IVF', metric = 'L2', lists = 2, sample_size = 10)");
      }
      PTable indexTable = conn.unwrap(PhoenixConnection.class).getTableNoCache(indexName);
      List<PColumn> pkColumns = indexTable.getPKColumns();
      assertEquals(2, pkColumns.size());
      assertEquals(MetaDataUtil.VECTOR_CENTROID_ID_COLUMN_NAME,
        pkColumns.get(0).getName().getString());
      assertEquals(":CENTROID_ID", pkColumns.get(1).getName().getString());
    }
  }

  /** Verifies that the INCLUDE columns of a vector index go into the default column family. */
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
          + "WITH (algorithm = 'IVF', metric = 'COSINE', lists = 8, sample_size = 250)");
      }

      PhoenixConnection pconn = conn.unwrap(PhoenixConnection.class);
      PTable indexTable = pconn.getTableNoCache(indexName);
      assertNotNull(indexTable);
      assertTrue(indexTable.isVectorIndex());
      assertEquals(IndexType.VECTOR_GLOBAL, indexTable.getIndexType());
      assertEquals(PIndexState.BUILDING, indexTable.getIndexState());

      List<PColumn> pkColumns = indexTable.getPKColumns();
      assertEquals(2, pkColumns.size());
      assertEquals(MetaDataUtil.VECTOR_CENTROID_ID_COLUMN_NAME,
        pkColumns.get(0).getName().getString());
      assertEquals(":ID", pkColumns.get(1).getName().getString());

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

  /** Verifies index creation and catalog metadata for a double-precision vector column. */
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

      List<PColumn> pkColumns = indexTable.getPKColumns();
      assertEquals(2, pkColumns.size());
      assertEquals(MetaDataUtil.VECTOR_CENTROID_ID_COLUMN_NAME,
        pkColumns.get(0).getName().getString());
      assertEquals(":ID", pkColumns.get(1).getName().getString());
    }
  }

  /**
   * Verifies that a new vector index starts in the BUILDING state and gets its dimension from the
   * data column.
   */
  @Test
  public void testVectorIndexStateIsBuildingAndInfersDimension() throws Exception {
    String tableName = "T_VEC_INFER_" + generateUniqueName();
    String indexName = "IDX_VEC_INFER_" + generateUniqueName();

    try (Connection conn = DriverManager.getConnection(getUrl())) {
      try (Statement stmt = conn.createStatement()) {
        stmt.execute(
          "CREATE TABLE " + tableName + " (ID VARCHAR NOT NULL PRIMARY KEY, V VECTOR(FLOAT, 32))");
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

  /**
   * Verifies that DROP INDEX removes the centroids, the SYSTEM.CATALOG rows, and the physical table
   * of a vector index.
   */
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

      try (PreparedStatement ps = conn.prepareStatement(
        "SELECT COUNT(*) FROM " + SYSTEM_VECTOR_CENTROID_NAME + " WHERE " + INDEX_NAME + " = ?")) {
        ps.setString(1, fullIndexName);
        try (ResultSet rs = ps.executeQuery()) {
          assertTrue(rs.next());
          assertEquals(4, rs.getInt(1));
        }
      }

      try (Admin admin = pconn.getQueryServices().getAdmin()) {
        assertTrue("HBase physical table should exist before drop",
          admin.tableExists(hbaseTableName));
      }

      try (Statement stmt = conn.createStatement()) {
        stmt.execute("DROP INDEX " + indexName + " ON " + tableName);
      }

      try (PreparedStatement ps = conn.prepareStatement(
        "SELECT COUNT(*) FROM " + SYSTEM_VECTOR_CENTROID_NAME + " WHERE " + INDEX_NAME + " = ?")) {
        ps.setString(1, fullIndexName);
        try (ResultSet rs = ps.executeQuery()) {
          assertTrue(rs.next());
          assertEquals("Centroids must be removed on drop index", 0, rs.getInt(1));
        }
      }

      try (Admin admin = pconn.getQueryServices().getAdmin()) {
        assertFalse("HBase physical table should be deleted on drop index",
          admin.tableExists(hbaseTableName));
      }

      try (Statement stmt = conn.createStatement();
        ResultSet rs = stmt.executeQuery("SELECT 1 FROM SYSTEM.CATALOG WHERE TABLE_NAME = '"
          + indexName + "' AND TABLE_SCHEM IS NULL AND TENANT_ID IS NULL")) {
        assertFalse("Index row in SYSTEM.CATALOG must be deleted", rs.next());
      }

      try {
        pconn.getTableNoCache(indexName);
        fail("Expected TableNotFoundException after drop");
      } catch (TableNotFoundException expected) {
      }
    }
  }

  /**
   * Verifies that DROP TABLE also removes the centroids and the physical table of each vector index
   * on the data table.
   */
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

      try (PreparedStatement ps = conn.prepareStatement(
        "SELECT COUNT(*) FROM " + SYSTEM_VECTOR_CENTROID_NAME + " WHERE " + INDEX_NAME + " = ?")) {
        ps.setString(1, fullIndexName);
        try (ResultSet rs = ps.executeQuery()) {
          assertTrue(rs.next());
          assertEquals("Centroids must be removed on cascade drop table", 0, rs.getInt(1));
        }
      }

      try (Admin admin = pconn.getQueryServices().getAdmin()) {
        assertFalse("HBase physical table for index should be deleted",
          admin.tableExists(hbaseTableName));
      }
    }
  }

  /** Verifies that DROP INDEX removes the centroids of a vector index in a schema. */
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

  /** Verifies that DROP INDEX removes the centroids of a vector index with a quoted name. */
  @Test
  public void testDropQuotedVectorIndexRemovesCentroids() throws Exception {
    String schemaName = "s" + generateUniqueName().toLowerCase();
    String tableName = "t" + generateUniqueName().toLowerCase();
    String indexName = "idx" + generateUniqueName().toLowerCase();
    String fullTableName = "\"" + schemaName + "\".\"" + tableName + "\"";

    try (Connection conn = getDropMetadataConnection()) {
      try (Statement stmt = conn.createStatement()) {
        stmt.execute("CREATE TABLE " + fullTableName
          + " (ID VARCHAR NOT NULL PRIMARY KEY, V VECTOR(FLOAT, 4))");
        stmt.execute("CREATE VECTOR INDEX \"" + indexName + "\" ON " + fullTableName + " (V) "
          + "WITH (algorithm = 'IVF', metric = 'L2', lists = 2, sample_size = 10)");
      }
      PTable indexTable =
        conn.unwrap(PhoenixConnection.class).getTableNoCache(schemaName + "." + indexName);
      // The centroid rows use the catalog name of the index as the key. This name keeps the case
      // of the quoted identifiers.
      String key = indexTable.getName().getString();
      assertEquals(schemaName + "." + indexName, key);
      try (PreparedStatement ps = conn.prepareStatement(
        "UPSERT INTO " + SYSTEM_VECTOR_CENTROID_NAME + " (" + INDEX_NAME + ", " + CENTROID_ID + ", "
          + CENTROID_VECTOR + ", " + GENERATION_ID + ") VALUES (?, ?, ?, ?)")) {
        for (int i = 0; i < 2; i++) {
          ps.setString(1, key);
          ps.setInt(2, i);
          ps.setBytes(3, new byte[] { (byte) i });
          ps.setLong(4, 1L);
          ps.executeUpdate();
        }
      }
      conn.commit();

      try (Statement stmt = conn.createStatement()) {
        stmt.execute("DROP INDEX \"" + indexName + "\" ON " + fullTableName);
      }
      try (PreparedStatement ps = conn.prepareStatement(
        "SELECT COUNT(*) FROM " + SYSTEM_VECTOR_CENTROID_NAME + " WHERE " + INDEX_NAME + " = ?")) {
        ps.setString(1, key);
        try (ResultSet rs = ps.executeQuery()) {
          assertTrue(rs.next());
          assertEquals(0, rs.getInt(1));
        }
      }
    }
  }

  /**
   * Verifies that DROP INDEX IF EXISTS removes an existing vector index, and completes without
   * error after the index no longer exists.
   */
  @Test
  public void testDropVectorIndexIfExists() throws Exception {
    String tableName = "T_IF_EXISTS_" + generateUniqueName();
    String indexName = "IDX_NON_EXISTENT_" + generateUniqueName();

    try (Connection conn = DriverManager.getConnection(getUrl())) {
      try (Statement stmt = conn.createStatement()) {
        stmt.execute(
          "CREATE TABLE " + tableName + " (ID VARCHAR NOT NULL PRIMARY KEY, V VECTOR(FLOAT, 32))");
        stmt.execute("DROP INDEX IF EXISTS " + indexName + " ON " + tableName);
      }
    }
  }
}
