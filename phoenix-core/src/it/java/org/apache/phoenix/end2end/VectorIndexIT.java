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
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Mutation;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.client.TableDescriptor;
import org.apache.hadoop.hbase.client.coprocessor.Batch;
import org.apache.hadoop.hbase.ipc.CoprocessorRpcUtils.BlockingRpcCallback;
import org.apache.hadoop.hbase.ipc.ServerRpcController;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.mapreduce.Counters;
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
import org.apache.phoenix.mapreduce.index.PhoenixIndexToolJobCounters;
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

      // The index keeps the indexed vector column in the data family with the PVectorDouble type
      PColumn vectorCol = indexTable.getColumnForColumnName("0:V");
      assertNotNull("Vector column 0:V must exist in index table", vectorCol);
      assertEquals("Double vector column must have PVectorDouble data type", PVectorDouble.INSTANCE,
        vectorCol.getDataType());
      assertEquals(Integer.valueOf(64), vectorCol.getMaxLength());
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
    String indexName = "IDX_IF_EXISTS_" + generateUniqueName();

    try (Connection conn = getDropMetadataConnection()) {
      try (Statement stmt = conn.createStatement()) {
        stmt.execute(
          "CREATE TABLE " + tableName + " (ID VARCHAR NOT NULL PRIMARY KEY, V VECTOR(FLOAT, 32))");
        stmt.execute("CREATE VECTOR INDEX " + indexName + " ON " + tableName + " (V) "
          + "WITH (algorithm = 'IVF', metric = 'L2', lists = 4, sample_size = 100)");
      }

      // Make sure that SYSTEM.CATALOG has the index before the drop
      try (Statement stmt = conn.createStatement();
        ResultSet rs = stmt.executeQuery("SELECT 1 FROM SYSTEM.CATALOG WHERE TABLE_NAME = '"
          + indexName + "' AND TABLE_SCHEM IS NULL AND TENANT_ID IS NULL")) {
        assertTrue("Index must exist in SYSTEM.CATALOG before drop", rs.next());
      }

      // DROP INDEX IF EXISTS removes the index if it exists
      try (Statement stmt = conn.createStatement()) {
        stmt.execute("DROP INDEX IF EXISTS " + indexName + " ON " + tableName);
      }

      try (Statement stmt = conn.createStatement();
        ResultSet rs = stmt.executeQuery("SELECT 1 FROM SYSTEM.CATALOG WHERE TABLE_NAME = '"
          + indexName + "' AND TABLE_SCHEM IS NULL AND TENANT_ID IS NULL")) {
        assertFalse("Index must be removed from SYSTEM.CATALOG after DROP INDEX IF EXISTS",
          rs.next());
      }

      // DROP INDEX IF EXISTS must not fail if the index does not exist
      try (Statement stmt = conn.createStatement()) {
        stmt.execute("DROP INDEX IF EXISTS " + indexName + " ON " + tableName);
      }
    }
  }

  private static void loadClusteredVectors(Connection conn, String tableName, int rows)
    throws SQLException {
    try (PreparedStatement ps =
      conn.prepareStatement("UPSERT INTO " + tableName + " (ID, V, LABEL) VALUES (?, ?, ?)")) {
      for (int i = 0; i < rows; i++) {
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
  }

  private static int countCentroids(Connection conn, String indexName, Long generation)
    throws SQLException {
    String sql = "SELECT COUNT(*) FROM " + SYSTEM_VECTOR_CENTROID_NAME + " WHERE " + INDEX_NAME
      + " = ?" + (generation == null ? "" : " AND " + GENERATION_ID + " = ?");
    try (PreparedStatement ps = conn.prepareStatement(sql)) {
      ps.setString(1, indexName);
      if (generation != null) {
        ps.setLong(2, generation);
      }
      try (ResultSet rs = ps.executeQuery()) {
        assertTrue(rs.next());
        return rs.getInt(1);
      }
    }
  }

  /**
   * Verifies synchronous vector index population, building verified index rows keyed by centroid ID
   * and transitioning index state to ACTIVE.
   */
  @Test
  public void testSynchronousVectorIndexPopulationAndActivation() throws Exception {
    String tableName = "T_VEC_POP_" + generateUniqueName();
    String indexName = "IDX_VEC_POP_" + generateUniqueName();
    try (Connection conn = DriverManager.getConnection(getUrl())) {
      try (Statement stmt = conn.createStatement()) {
        stmt.execute("CREATE TABLE " + tableName
          + " (ID VARCHAR NOT NULL PRIMARY KEY, V VECTOR(FLOAT, 4), LABEL VARCHAR)");
      }
      loadClusteredVectors(conn, tableName, 100);
      long before = System.currentTimeMillis();
      try (Statement stmt = conn.createStatement()) {
        stmt.execute(
          "CREATE VECTOR INDEX " + indexName + " ON " + tableName + " (V) " + "INCLUDE (LABEL) "
            + "WITH (algorithm = 'IVF', metric = 'L2', lists = 4, sample_size = 100)");
      }
      PhoenixConnection pconn = conn.unwrap(PhoenixConnection.class);
      PTable index = pconn.getTableNoCache(indexName);
      assertEquals(PIndexState.ACTIVE, index.getIndexState());
      Long generation = index.getVectorCentroidGeneration();
      assertNotNull("Training records a generation", generation);
      assertTrue("The first generation is the training time", generation >= before);
      int lists = index.getVectorIvfLists();
      assertTrue(lists >= 4);
      assertEquals(lists, countCentroids(conn, indexName, generation));

      String sql = "SELECT \"_CENTROID_ID\", \":ID\", \"0:LABEL\" FROM " + indexName;
      int rows = 0;
      try (ResultSet rs = conn.createStatement().executeQuery(sql)) {
        while (rs.next()) {
          rows++;
          assertTrue(rs.getInt(1) >= 0 && rs.getInt(1) < lists);
          assertNotNull(rs.getString(2));
          assertNotNull(rs.getString(3));
        }
      }
      assertEquals(100, rows);
      IndexTestUtil.assertRowsForEmptyColValue(conn, indexName, QueryConstants.VERIFIED_BYTES);
      assertIndexVerifies(tableName, indexName, 100);
    }
  }

  /**
   * Verifies that centroid training is deferred when the table contains fewer vectors than
   * requested lists.
   */
  @Test
  public void testTrainingDeferredOnEmptyTable() throws Exception {
    String tableName = "T_VEC_" + generateUniqueName();
    String indexName = "IDX_VEC_" + generateUniqueName();
    try (Connection conn = DriverManager.getConnection(getUrl())) {
      try (Statement stmt = conn.createStatement()) {
        stmt.execute("CREATE TABLE " + tableName
          + " (ID VARCHAR NOT NULL PRIMARY KEY, V VECTOR(FLOAT, 4), LABEL VARCHAR)");
        stmt.execute("CREATE VECTOR INDEX " + indexName + " ON " + tableName
          + " (V) WITH (algorithm = 'IVF', metric = 'L2', lists = 4, sample_size = 100)");
      }
      PTable index = conn.unwrap(PhoenixConnection.class).getTableNoCache(indexName);
      assertEquals(PIndexState.BUILDING, index.getIndexState());
      assertNull(index.getVectorCentroidGeneration());
      assertEquals(0, countCentroids(conn, indexName, null));
    }
  }

  /**
   * Makes sure that the centroid rows of a quoted index name use its exact case. DROP INDEX must
   * delete the rows, and a re-created index must get a higher generation ID. The higher ID prevents
   * reuse of a stale cached model.
   */
  @Test
  public void testQuotedIndexNameTrainDropRecreate() throws Exception {
    String schema = generateUniqueName();
    String tableName = schema + ".T_" + generateUniqueName();
    String indexName = "myIdx";
    String fullIndexName = schema + "." + indexName;
    try (Connection conn = DriverManager.getConnection(getUrl())) {
      try (Statement stmt = conn.createStatement()) {
        stmt.execute("CREATE TABLE " + tableName
          + " (ID VARCHAR NOT NULL PRIMARY KEY, V VECTOR(FLOAT, 4), LABEL VARCHAR)");
      }
      loadClusteredVectors(conn, tableName, 40);
      String ddl = "CREATE VECTOR INDEX \"" + indexName + "\" ON " + tableName
        + " (V) WITH (algorithm = 'IVF', metric = 'L2', lists = 2, sample_size = 40)";
      try (Statement stmt = conn.createStatement()) {
        stmt.execute(ddl);
      }
      PhoenixConnection pconn = conn.unwrap(PhoenixConnection.class);
      PTable index = pconn.getTableNoCache(fullIndexName);
      assertEquals(fullIndexName, index.getName().getString());
      Long firstGeneration = index.getVectorCentroidGeneration();
      assertNotNull(firstGeneration);
      assertTrue(countCentroids(conn, fullIndexName, firstGeneration) >= 2);
      assertEquals(0, countCentroids(conn, fullIndexName.toUpperCase(), null));

      try (Statement stmt = conn.createStatement()) {
        stmt.execute("DROP INDEX \"" + indexName + "\" ON " + tableName);
      }
      assertEquals(0, countCentroids(conn, fullIndexName, null));

      try (Statement stmt = conn.createStatement()) {
        stmt.execute(ddl);
      }
      Long secondGeneration = pconn.getTableNoCache(fullIndexName).getVectorCentroidGeneration();
      assertNotNull(secondGeneration);
      assertTrue(secondGeneration > firstGeneration);
    }
  }

  /**
   * Makes sure that the server evaluates the RAND() sample filter on each row, so that the sample
   * keeps about the requested fraction of rows.
   */
  @Test
  public void testTrainingSampleFilterIsEvaluatedPerRowOnServer() throws Exception {
    String tableName = "T_VEC_" + generateUniqueName();
    try (Connection conn = DriverManager.getConnection(getUrl())) {
      try (Statement stmt = conn.createStatement()) {
        stmt.execute("CREATE TABLE " + tableName
          + " (ID VARCHAR NOT NULL PRIMARY KEY, V VECTOR(FLOAT, 4), LABEL VARCHAR)");
      }
      loadClusteredVectors(conn, tableName, 1000);
      String sql = "SELECT V FROM " + tableName + " WHERE V IS NOT NULL AND RAND() < 0.3";
      String plan = QueryUtil.getExplainPlan(conn.createStatement().executeQuery("EXPLAIN " + sql));
      assertTrue(plan, plan.contains("SERVER FILTER BY") && plan.contains("RAND()"));
      int rows = 0;
      try (ResultSet rs = conn.createStatement().executeQuery(sql)) {
        while (rs.next()) {
          rows++;
        }
      }
      assertTrue("Sampled " + rows + " of 1000 at p=0.3", rows > 200 && rows < 400);
    }
  }

  private static final List<float[]> KNOWN_CENTROIDS =
    Arrays.asList(new float[] { 0.0f, 0.0f, 0.0f, 1.0f }, // ID 0
      new float[] { 0.0f, 0.0f, 1.0f, 0.0f }, // ID 1
      new float[] { 1.0f, 0.0f, 0.0f, 0.0f }, // ID 2
      new float[] { 0.0f, 1.0f, 0.0f, 0.0f }); // ID 3

  /**
   * Initializes test table and active vector index configured with deterministic centroid
   * positions.
   */
  private void setupTableAndKnownCentroids(Connection conn, String tableName, String indexName)
    throws Exception {
    setupTableAndKnownCentroids(conn, tableName, indexName, "FLOAT", "");
  }

  private void setupTableAndKnownCentroids(Connection conn, String tableName, String indexName,
    String elementType, String tableOptions) throws Exception {
    try (Statement stmt = conn.createStatement()) {
      stmt.execute("CREATE TABLE " + tableName + " (ID VARCHAR NOT NULL PRIMARY KEY, V VECTOR("
        + elementType + ", 4), LABEL VARCHAR) " + tableOptions);
      stmt.execute("CREATE VECTOR INDEX " + indexName + " ON " + tableName + " (V) "
        + "INCLUDE (LABEL) WITH (algorithm = 'IVF', metric = 'L2', lists = 4, sample_size = 100)");
    }
    recordKnownCentroids(conn, indexName, KNOWN_CENTROIDS);
    PhoenixConnection pconn = conn.unwrap(PhoenixConnection.class);
    IndexUtil.updateIndexState(pconn, indexName, PIndexState.ACTIVE, 0L);
    pconn.removeTable(pconn.getTenantId(), indexName, null, HConstants.LATEST_TIMESTAMP);
    pconn.removeTable(pconn.getTenantId(), tableName, null, HConstants.LATEST_TIMESTAMP);
  }

  /** Persists specified centroids as a new generation for the given index. */
  private static long recordKnownCentroids(Connection conn, String indexName,
    List<float[]> centroids) throws SQLException {
    PhoenixConnection pconn = conn.unwrap(PhoenixConnection.class);
    try (PhoenixConnection internal = CentroidManager.newInternalConnection(pconn)) {
      PTable index = internal.getTableNoCache(indexName);
      long generation = CentroidManager.nextGeneration(index.getVectorCentroidGeneration());
      CentroidManager.persistCentroids(internal, indexName, generation, centroids);
      CentroidManager.setGenerationAndLists(internal, index, generation, centroids.size());
      return generation;
    }
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

      // Upsert test vector mapping to centroid ID 2
      String upsertSql = "UPSERT INTO " + tableName + " (ID, V, LABEL) VALUES (?, ?, ?)";
      try (PreparedStatement ps = conn.prepareStatement(upsertSql)) {
        ps.setString(1, "row_1");
        ps.setArray(2, conn.createArrayOf("FLOAT", new Float[] { 1.0f, 0.0f, 0.0f, 0.0f }));
        ps.setString(3, "lbl_1");
        ps.executeUpdate();
      }
      conn.commit();

      // Verify physical HBase index row key contains assigned centroid ID
      List<byte[]> rowKeys = getHBaseRowKeys(pconn, indexTable);
      assertEquals("Expected exactly 1 index row", 1, rowKeys.size());
      assertEquals("Centroid prefix must be 2", 2, extractCentroidId(rowKeys.get(0)));

      // Verify index scan via SQL
      String selectSql = "SELECT \"_CENTROID_ID\", \":ID\", \"0:LABEL\" FROM " + indexName;
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

      // Insert initial row mapping to centroid 2
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

      // Update vector to value mapping to centroid 0
      try (PreparedStatement ps = conn.prepareStatement(upsertSql)) {
        ps.setString(1, "row_1");
        ps.setArray(2, conn.createArrayOf("FLOAT", new Float[] { 0.0f, 0.0f, 0.0f, 1.0f }));
        ps.setString(3, "lbl_updated");
        ps.executeUpdate();
      }
      conn.commit();

      // Verify index row key relocation from old centroid to new centroid
      List<byte[]> rowKeysAfter = getHBaseRowKeys(pconn, indexTable);
      assertEquals("Expected exactly 1 index row after vector update", 1, rowKeysAfter.size());
      assertEquals("Centroid prefix must be updated to 0", 0,
        extractCentroidId(rowKeysAfter.get(0)));

      // Verify updated row visibility via SQL scan
      String selectSql = "SELECT \"_CENTROID_ID\", \":ID\", \"0:LABEL\" FROM " + indexName;
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

      // Insert initial row mapping to centroid 2
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

      // Update covered column without modifying vector value
      try (PreparedStatement ps =
        conn.prepareStatement("UPSERT INTO " + tableName + " (ID, V, LABEL) VALUES (?, ?, ?)")) {
        ps.setString(1, "row_1");
        ps.setArray(2, conn.createArrayOf("FLOAT", new Float[] { 1.0f, 0.0f, 0.0f, 0.0f }));
        ps.setString(3, "updated_label");
        ps.executeUpdate();
      }
      conn.commit();

      // Verify index row key and centroid prefix remain unchanged
      List<byte[]> rowKeysAfter = getHBaseRowKeys(pconn, indexTable);
      assertEquals("Expected exactly 1 index row", 1, rowKeysAfter.size());
      assertEquals("Centroid prefix must still be 2", 2, extractCentroidId(rowKeysAfter.get(0)));

      // Verify in-place update of covered column
      String selectSql = "SELECT \"_CENTROID_ID\", \":ID\", \"0:LABEL\" FROM " + indexName;
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

      // Insert initial row mapping to centroid 2
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

      // Delete base table row
      try (Statement stmt = conn.createStatement()) {
        stmt.execute("DELETE FROM " + tableName + " WHERE ID = 'row_1'");
      }
      conn.commit();

      // Verify physical index row deletion
      List<byte[]> rowKeysAfter = getHBaseRowKeys(pconn, indexTable);
      assertEquals("Expected 0 index rows after delete", 0, rowKeysAfter.size());

      // Verify index table scan returns zero rows
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

      // Insert row with null vector
      String upsertSql = "UPSERT INTO " + tableName + " (ID, V, LABEL) VALUES (?, ?, ?)";
      try (PreparedStatement ps = conn.prepareStatement(upsertSql)) {
        ps.setString(1, "row_null");
        ps.setNull(2, java.sql.Types.ARRAY);
        ps.setString(3, "null_label");
        ps.executeUpdate();
      }
      conn.commit();

      // Verify null vector does not generate index row
      List<byte[]> rowKeys = getHBaseRowKeys(pconn, indexTable);
      assertEquals("Null vector must not produce an index row", 0, rowKeys.size());

      // Verify base table row presence
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

      // Verify initial index row key and centroid prefix
      List<byte[]> rowKeysBefore = getHBaseRowKeys(pconn, indexTable);
      assertEquals(1, rowKeysBefore.size());
      assertEquals(2, extractCentroidId(rowKeysBefore.get(0)));

      // Partial update modifying covered column only
      try (Statement stmt = conn.createStatement()) {
        stmt.execute(
          "UPSERT INTO " + tableName + " (ID, LABEL) VALUES ('row_alloc_u', 'lbl_updated')");
      }
      conn.commit();

      // Verify index row key remains unchanged
      List<byte[]> rowKeysAfterPartial = getHBaseRowKeys(pconn, indexTable);
      assertEquals("Expected exactly 1 index row after partial update", 1,
        rowKeysAfterPartial.size());
      assertEquals(2, extractCentroidId(rowKeysAfterPartial.get(0)));

      // Verify vector column payload is preserved during partial update
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

      // Full update supplying identical vector
      try (PreparedStatement ps = conn.prepareStatement(upsertSql)) {
        ps.setString(1, "row_alloc_u");
        ps.setArray(2, conn.createArrayOf("FLOAT", new Float[] { 1.0f, 0.0f, 0.0f, 0.0f }));
        ps.setString(3, "lbl_updated_again");
        ps.executeUpdate();
      }
      conn.commit();

      // Verify index row key remains unchanged after full update with identical vector
      List<byte[]> rowKeys = getHBaseRowKeys(pconn, indexTable);
      assertEquals("Expected exactly 1 index row after full unchanged update", 1, rowKeys.size());
      assertEquals(2, extractCentroidId(rowKeys.get(0)));

      // Verify updated covered column values
      String selectSql = "SELECT \"_CENTROID_ID\", \":ID\", \"0:LABEL\" FROM " + indexName;
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
  public void testVerifiedMarkerPresentAfterCommit() throws Exception {
    String tableName = "T_VEC_VER_" + generateUniqueName();
    String indexName = "IDX_VEC_VER_" + generateUniqueName();

    try (Connection conn = DriverManager.getConnection(getUrl())) {
      setupTableAndKnownCentroids(conn, tableName, indexName);

      PhoenixConnection pconn = conn.unwrap(PhoenixConnection.class);
      PTable indexTable = pconn.getTableNoCache(indexName);
      byte[] physicalIndexName = indexTable.getPhysicalName().getBytes();

      // Insert row mapping to centroid 2
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

      // Verify VERIFIED empty column cell directly on physical HBase table
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

      // Verify verification status via IndexTestUtil
      IndexTestUtil.assertRowsForEmptyColValue(conn, indexName, QueryConstants.VERIFIED_BYTES);
    }
  }

  /**
   * Verifies read repair cleans up stale index entries under outdated centroids and rebuilds
   * verified rows matching the current vector.
   */
  @Test
  public void testReadRepairWithCentroidReassignment() throws Exception {
    String tableName = "T_VEC_RR_" + generateUniqueName();
    String indexName = "IDX_VEC_RR_" + generateUniqueName();
    try (Connection conn = DriverManager.getConnection(getUrl())) {
      setupTableAndKnownCentroids(conn, tableName, indexName);
      PhoenixConnection pconn = conn.unwrap(PhoenixConnection.class);

      // Populate base table row while index is disabled
      IndexUtil.updateIndexState(pconn, indexName, PIndexState.DISABLE, 0L);
      pconn.removeTable(pconn.getTenantId(), tableName, null, HConstants.LATEST_TIMESTAMP);
      try (PreparedStatement ps =
        conn.prepareStatement("UPSERT INTO " + tableName + " (ID, V, LABEL) VALUES (?, ?, ?)")) {
        ps.setString(1, "repair_row_1");
        // Vector maps to centroid 2
        ps.setArray(2, conn.createArrayOf("FLOAT", new Float[] { 1.0f, 0.0f, 0.0f, 0.0f }));
        ps.setString(3, "correct_label");
        ps.executeUpdate();
      }
      conn.commit();
      IndexUtil.updateIndexState(pconn, indexName, PIndexState.BUILDING, 0L);
      IndexUtil.updateIndexState(pconn, indexName, PIndexState.ACTIVE, 0L);
      pconn.removeTable(pconn.getTenantId(), indexName, null, HConstants.LATEST_TIMESTAMP);
      pconn.removeTable(pconn.getTenantId(), tableName, null, HConstants.LATEST_TIMESTAMP);

      PTable indexTable = pconn.getTableNoCache(indexName);
      byte[] physicalIndexName = indexTable.getPhysicalName().getBytes();
      byte[] centroid0RowKey =
        ByteUtil.concat(PInteger.INSTANCE.toBytes(0), Bytes.toBytes("repair_row_1"));
      byte[] centroid2RowKey =
        ByteUtil.concat(PInteger.INSTANCE.toBytes(2), Bytes.toBytes("repair_row_1"));
      byte[] emptyCF = SchemaUtil.getEmptyColumnFamily(indexTable);
      byte[] emptyCQ = EncodedColumnsUtil.getEmptyKeyValueInfo(indexTable).getFirst();
      PColumn labelCol = indexTable.getColumnForColumnName("0:LABEL");
      byte[] labelCF = labelCol.getFamilyName().getBytes();
      byte[] labelCQ = labelCol.getColumnQualifierBytes();

      // Simulate partial failure leaving unverified index row under incorrect centroid 0
      try (Table hIndexTable = pconn.getQueryServices().getTable(physicalIndexName)) {
        assertTrue(getHBaseRowKeys(pconn, indexTable).isEmpty());
        Put stalePut = new Put(centroid0RowKey);
        stalePut.addColumn(emptyCF, emptyCQ, QueryConstants.UNVERIFIED_BYTES);
        stalePut.addColumn(labelCF, labelCQ, Bytes.toBytes("stale_label"));
        hIndexTable.put(stalePut);
      }

      // Read repair triggers during index query
      String selectSql = "SELECT \"_CENTROID_ID\", \":ID\", \"0:LABEL\" FROM " + indexName;
      try (Statement stmt = conn.createStatement(); ResultSet rs = stmt.executeQuery(selectSql)) {
        assertTrue("Query through index should return the repaired row", rs.next());
        assertEquals("Repaired centroid ID must be 2", 2, rs.getInt(1));
        assertEquals("repair_row_1", rs.getString(2));
        assertEquals("correct_label", rs.getString(3));
        assertFalse("Only one row should be returned", rs.next());
      }

      try (Table hIndexTable = pconn.getQueryServices().getTable(physicalIndexName)) {
        Result r2 = hIndexTable.get(new Get(centroid2RowKey));
        assertFalse("Repaired centroid 2 row must exist after read repair", r2.isEmpty());
        assertArrayEquals(QueryConstants.VERIFIED_BYTES, r2.getValue(emptyCF, emptyCQ));
        assertEquals("correct_label", Bytes.toString(r2.getValue(labelCF, labelCQ)));
        // Stale unverified row remains unserved until background cleanup
        Result r0 = hIndexTable.get(new Get(centroid0RowKey));
        assertTrue(r0.isEmpty()
          || Bytes.equals(QueryConstants.UNVERIFIED_BYTES, r0.getValue(emptyCF, emptyCQ)));
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

  /** Verifies covered vector columns of differing element widths are preserved in index rows. */
  @Test
  public void testCoveredDoubleVectorColumn() throws Exception {
    String tableName = "T_VEC_COVD_" + generateUniqueName();
    String indexName = "IDX_VEC_COVD_" + generateUniqueName();
    try (Connection conn = DriverManager.getConnection(getUrl())) {
      try (Statement stmt = conn.createStatement()) {
        stmt.execute("CREATE TABLE " + tableName + " (ID VARCHAR NOT NULL PRIMARY KEY, "
          + "V VECTOR(FLOAT, 4), LABEL VARCHAR, COV_D VECTOR(DOUBLE, 3))");
        stmt.execute("CREATE VECTOR INDEX " + indexName + " ON " + tableName + " (V) "
          + "INCLUDE (LABEL, COV_D) WITH (algorithm = 'IVF', metric = 'L2', lists = 4, "
          + "sample_size = 100)");
      }
      recordKnownCentroids(conn, indexName, KNOWN_CENTROIDS);
      PhoenixConnection pconn = conn.unwrap(PhoenixConnection.class);
      IndexUtil.updateIndexState(pconn, indexName, PIndexState.ACTIVE, 0L);
      pconn.removeTable(pconn.getTenantId(), indexName, null, HConstants.LATEST_TIMESTAMP);
      pconn.removeTable(pconn.getTenantId(), tableName, null, HConstants.LATEST_TIMESTAMP);

      String upsert = "UPSERT INTO " + tableName + " (ID, V, COV_D) VALUES (?, ?, ?)";
      try (PreparedStatement ps = conn.prepareStatement(upsert)) {
        ps.setString(1, "r1");
        ps.setArray(2, conn.createArrayOf("FLOAT", new Float[] { 1.0f, 0.0f, 0.0f, 0.0f }));
        ps.setArray(3, conn.createArrayOf("DOUBLE", new Double[] { 1.5, -2.25, 3.125 }));
        ps.executeUpdate();
      }
      conn.commit();
      // Update covered vector column only
      try (PreparedStatement ps =
        conn.prepareStatement("UPSERT INTO " + tableName + " (ID, COV_D) VALUES (?, ?)")) {
        ps.setString(1, "r1");
        ps.setArray(2, conn.createArrayOf("DOUBLE", new Double[] { 4.5, 5.5, -6.5 }));
        ps.executeUpdate();
      }
      conn.commit();

      PTable indexTable = pconn.getTableNoCache(indexName);
      List<byte[]> rowKeys = getHBaseRowKeys(pconn, indexTable);
      assertEquals(1, rowKeys.size());
      assertEquals(2, extractCentroidId(rowKeys.get(0)));
      PColumn covCol = indexTable.getColumnForColumnName("0:COV_D");
      PColumn vecCol = indexTable.getColumnForColumnName("0:V");
      try (
        Table hTable = pconn.getQueryServices().getTable(indexTable.getPhysicalName().getBytes())) {
        Result r = hTable.get(new Get(rowKeys.get(0)));
        assertArrayEquals(PVectorDouble.INSTANCE.toBytes(new double[] { 4.5, 5.5, -6.5 }),
          r.getValue(covCol.getFamilyName().getBytes(), covCol.getColumnQualifierBytes()));
        assertArrayEquals(PVectorFloat.INSTANCE.toBytes(new float[] { 1.0f, 0.0f, 0.0f, 0.0f }),
          r.getValue(vecCol.getFamilyName().getBytes(), vecCol.getColumnQualifierBytes()));
      }
    }
  }

  /** Verifies lifecycle maintenance of double precision indexes across centroid boundaries. */
  @Test
  public void testDoubleVectorIndexMaintenance() throws Exception {
    String tableName = "T_VEC_DBL_" + generateUniqueName();
    String indexName = "IDX_VEC_DBL_" + generateUniqueName();
    try (Connection conn = DriverManager.getConnection(getUrl())) {
      setupTableAndKnownCentroids(conn, tableName, indexName, "DOUBLE", "");
      PhoenixConnection pconn = conn.unwrap(PhoenixConnection.class);
      PTable indexTable = pconn.getTableNoCache(indexName);
      String upsert = "UPSERT INTO " + tableName + " (ID, V, LABEL) VALUES (?, ?, ?)";
      try (PreparedStatement ps = conn.prepareStatement(upsert)) {
        ps.setString(1, "d1");
        ps.setArray(2, conn.createArrayOf("DOUBLE", new Double[] { 0.9, 0.1, 0.0, 0.0 }));
        ps.setString(3, "a");
        ps.executeUpdate();
      }
      conn.commit();
      List<byte[]> rowKeys = getHBaseRowKeys(pconn, indexTable);
      assertEquals(1, rowKeys.size());
      assertEquals(2, extractCentroidId(rowKeys.get(0)));

      try (PreparedStatement ps = conn.prepareStatement(upsert)) {
        ps.setString(1, "d1");
        ps.setArray(2, conn.createArrayOf("DOUBLE", new Double[] { 0.0, 0.1, 0.0, 0.9 }));
        ps.setString(3, "b");
        ps.executeUpdate();
      }
      conn.commit();
      rowKeys = getHBaseRowKeys(pconn, indexTable);
      assertEquals(1, rowKeys.size());
      assertEquals(0, extractCentroidId(rowKeys.get(0)));

      conn.createStatement().execute("DELETE FROM " + tableName + " WHERE ID = 'd1'");
      conn.commit();
      assertEquals(0, getHBaseRowKeys(pconn, indexTable).size());
    }
  }

  /** Runs IndexTool verification comparing physical index rows against server rebuilt rows. */
  private static void assertIndexVerifies(String tableName, String indexName, long rows)
    throws Exception {
    IndexTool tool = IndexToolIT.runIndexTool(false, null, tableName, indexName, null, 0,
      IndexTool.IndexVerifyType.ONLY);
    Counters counters = tool.getJob().getCounters();
    assertEquals(rows, counters
      .findCounter(PhoenixIndexToolJobCounters.BEFORE_REBUILD_VALID_INDEX_ROW_COUNT).getValue());
    assertEquals(0, counters
      .findCounter(PhoenixIndexToolJobCounters.BEFORE_REBUILD_INVALID_INDEX_ROW_COUNT).getValue());
    assertEquals(0, counters
      .findCounter(PhoenixIndexToolJobCounters.BEFORE_REBUILD_MISSING_INDEX_ROW_COUNT).getValue());
  }

  private static float[][] loadRandomVectors(Connection conn, String tableName, String tenantCol,
    String[] tenants, int rows, long seed) throws SQLException {
    Random rng = new Random(seed);
    float[][] vectors = new float[rows][4];
    String upsert = tenantCol == null
      ? "UPSERT INTO " + tableName + " (ID, V, LABEL) VALUES (?, ?, ?)"
      : "UPSERT INTO " + tableName + " (" + tenantCol + ", ID, V, LABEL) VALUES (?, ?, ?, ?)";
    try (PreparedStatement ps = conn.prepareStatement(upsert)) {
      for (int i = 0; i < rows; i++) {
        Float[] vec = new Float[4];
        for (int d = 0; d < 4; d++) {
          vec[d] = rng.nextFloat() * 10f;
          vectors[i][d] = vec[d];
        }
        int p = 1;
        if (tenantCol != null) {
          ps.setString(p++, tenants[i % tenants.length]);
        }
        ps.setString(p++, String.format("row_%03d", i));
        ps.setArray(p++, conn.createArrayOf("FLOAT", vec));
        ps.setString(p, "label_" + i);
        ps.executeUpdate();
      }
    }
    conn.commit();
    return vectors;
  }

  /**
   * Verifies IndexTool execution for ASYNC vector indexes: initial centroid training, population of
   * verified index rows, and transition to ACTIVE state.
   */
  @Test
  public void testIndexToolTrainsAndBuilds() throws Exception {
    String tableName = "T_VEC_IT_" + generateUniqueName();
    String indexName = "IDX_VEC_IT_" + generateUniqueName();
    try (Connection conn = DriverManager.getConnection(getUrl())) {
      conn.createStatement().execute("CREATE TABLE " + tableName
        + " (ID VARCHAR NOT NULL PRIMARY KEY, V VECTOR(FLOAT, 4), LABEL VARCHAR)");
      float[][] vectors = loadRandomVectors(conn, tableName, null, null, 500, 42);
      conn.createStatement()
        .execute("CREATE VECTOR INDEX " + indexName + " ON " + tableName + " (V) INCLUDE (LABEL)"
          + " WITH (algorithm = 'IVF', metric = 'L2', lists = 4, sample_size = 100) ASYNC");
      PhoenixConnection pconn = conn.unwrap(PhoenixConnection.class);
      PTable pIndex = pconn.getTableNoCache(indexName);
      assertEquals(PIndexState.BUILDING, pIndex.getIndexState());
      assertNull(pIndex.getVectorCentroidGeneration());

      // Execute IndexTool build
      IndexToolIT.runIndexTool(false, null, tableName, indexName);

      pIndex = pconn.getTableNoCache(indexName);
      assertEquals(PIndexState.ACTIVE, pIndex.getIndexState());
      Long generation = pIndex.getVectorCentroidGeneration();
      assertNotNull("IndexTool trains the first generation", generation);
      List<float[]> centroids = CentroidManager.loadCentroids(conn, indexName, generation);
      assertEquals(pIndex.getVectorIvfLists().intValue(), centroids.size());

      byte[] emptyCF = SchemaUtil.getEmptyColumnFamily(pIndex);
      byte[] emptyCQ = EncodedColumnsUtil.getEmptyKeyValueInfo(pIndex).getFirst();
      int rows = 0;
      try (
        Table hIndexTable = pconn.getQueryServices().getTable(pIndex.getPhysicalName().getBytes());
        ResultScanner scanner = hIndexTable.getScanner(new Scan())) {
        for (Result r : scanner) {
          rows++;
          byte[] rowKey = r.getRow();
          String id = (String) PVarchar.INSTANCE.toObject(rowKey, Bytes.SIZEOF_INT,
            rowKey.length - Bytes.SIZEOF_INT);
          int rowIdx = Integer.parseInt(id.replace("row_", ""));
          assertEquals("Centroid of " + id, findNearestCentroid(vectors[rowIdx], centroids),
            extractCentroidId(rowKey));
          assertArrayEquals(QueryConstants.VERIFIED_BYTES, r.getValue(emptyCF, emptyCQ));
        }
      }
      assertEquals(500, rows);
      assertIndexVerifies(tableName, indexName, 500);
    }
  }

  /** Verifies IndexTool defers centroid training and index build when data is insufficient. */
  @Test
  public void testIndexToolDefersTrainingOnTooFewVectors() throws Exception {
    String tableName = "T_VEC_IT_" + generateUniqueName();
    String indexName = "IDX_VEC_IT_" + generateUniqueName();
    try (Connection conn = DriverManager.getConnection(getUrl())) {
      conn.createStatement().execute("CREATE TABLE " + tableName
        + " (ID VARCHAR NOT NULL PRIMARY KEY, V VECTOR(FLOAT, 4), LABEL VARCHAR)");
      loadRandomVectors(conn, tableName, null, null, 2, 7);
      conn.createStatement().execute("CREATE VECTOR INDEX " + indexName + " ON " + tableName
        + " (V)" + " WITH (algorithm = 'IVF', metric = 'L2', lists = 4, sample_size = 100) ASYNC");
      IndexTool tool = new IndexTool();
      tool.setConf(new Configuration(getUtility().getConfiguration()));
      assertEquals(0, tool.run(new String[] { "-dt", tableName, "-it", indexName, "-runfg" }));
      PTable pIndex = conn.unwrap(PhoenixConnection.class).getTableNoCache(indexName);
      assertEquals(PIndexState.BUILDING, pIndex.getIndexState());
      assertNull(pIndex.getVectorCentroidGeneration());
    }
  }

  /** Verifies IndexTool build of multi-tenant vector indexes with composite row keys. */
  @Test
  public void testIndexToolBuildsMultiTenantVectorIndex() throws Exception {
    String tableName = "T_VEC_MT_" + generateUniqueName();
    String indexName = "IDX_VEC_MT_" + generateUniqueName();
    String[] tenants = { "TA", "TB" };
    try (Connection conn = DriverManager.getConnection(getUrl())) {
      conn.createStatement()
        .execute("CREATE TABLE " + tableName + " (TENANT_ID VARCHAR NOT NULL, ID VARCHAR NOT NULL,"
          + " V VECTOR(FLOAT, 4), LABEL VARCHAR CONSTRAINT PK PRIMARY KEY (TENANT_ID, ID))"
          + " MULTI_TENANT = true");
      float[][] vectors = loadRandomVectors(conn, tableName, "TENANT_ID", tenants, 200, 11);
      conn.createStatement()
        .execute("CREATE VECTOR INDEX " + indexName + " ON " + tableName + " (V) INCLUDE (LABEL)"
          + " WITH (algorithm = 'IVF', metric = 'L2', lists = 4, sample_size = 100) ASYNC");

      IndexToolIT.runIndexTool(false, null, tableName, indexName);

      PhoenixConnection pconn = conn.unwrap(PhoenixConnection.class);
      PTable pIndex = pconn.getTableNoCache(indexName);
      assertEquals(PIndexState.ACTIVE, pIndex.getIndexState());
      List<float[]> centroids =
        CentroidManager.loadCentroids(conn, indexName, pIndex.getVectorCentroidGeneration());
      int rows = 0;
      try (
        Table hIndexTable = pconn.getQueryServices().getTable(pIndex.getPhysicalName().getBytes());
        ResultScanner scanner = hIndexTable.getScanner(new Scan())) {
        for (Result r : scanner) {
          rows++;
          byte[] rowKey = r.getRow();
          // Row key format: [TENANT_ID][separator][centroid id][ID]
          int sep = Bytes.indexOf(rowKey, QueryConstants.SEPARATOR_BYTE);
          String tenant = Bytes.toString(rowKey, 0, sep);
          int centroid = (Integer) PInteger.INSTANCE.toObject(rowKey, sep + 1, Bytes.SIZEOF_INT);
          String id = Bytes.toString(rowKey, sep + 1 + Bytes.SIZEOF_INT,
            rowKey.length - sep - 1 - Bytes.SIZEOF_INT);
          int rowIdx = Integer.parseInt(id.replace("row_", ""));
          assertEquals(tenants[rowIdx % tenants.length], tenant);
          assertEquals("Centroid of " + id, findNearestCentroid(vectors[rowIdx], centroids),
            centroid);
        }
      }
      assertEquals(200, rows);
      assertIndexVerifies(tableName, indexName, 200);
    }
  }

  /** Verifies client side index maintenance on immutable tables matches server side row keys. */
  @Test
  public void testImmutableTableClientMaintenanceMatchesServerBuild() throws Exception {
    String tableName = "T_VEC_IMM_" + generateUniqueName();
    String indexName = "IDX_VEC_IMM_" + generateUniqueName();
    try (Connection conn = DriverManager.getConnection(getUrl())) {
      setupTableAndKnownCentroids(conn, tableName, indexName, "FLOAT", "IMMUTABLE_ROWS = true");
      float[][] vectors = loadRandomVectors(conn, tableName, null, null, 50, 3);
      PhoenixConnection pconn = conn.unwrap(PhoenixConnection.class);
      PTable pIndex = pconn.getTableNoCache(indexName);
      List<byte[]> rowKeys = getHBaseRowKeys(pconn, pIndex);
      assertEquals(50, rowKeys.size());
      for (byte[] rowKey : rowKeys) {
        String id = Bytes.toString(rowKey, Bytes.SIZEOF_INT, rowKey.length - Bytes.SIZEOF_INT);
        int rowIdx = Integer.parseInt(id.replace("row_", ""));
        assertEquals(findNearestCentroid(vectors[rowIdx], KNOWN_CENTROIDS),
          extractCentroidId(rowKey));
      }
      assertIndexVerifies(tableName, indexName, 50);
    }
  }

  /** Verifies transactional constraints reject vector index creation. */
  @Test
  public void testVectorIndexRejectedOnTransactionalTable() throws Exception {
    String txTable = "T_VEC_TX_" + generateUniqueName();
    String plainTable = "T_VEC_NTX_" + generateUniqueName();
    try (Connection conn = DriverManager.getConnection(getUrl())) {
      conn.createStatement().execute(
        "CREATE TABLE " + txTable + " (ID VARCHAR NOT NULL PRIMARY KEY, V VECTOR(FLOAT, 3))"
          + " TRANSACTIONAL=true, TRANSACTION_PROVIDER='OMID'");
      try {
        conn.createStatement().execute("CREATE VECTOR INDEX IDX_" + generateUniqueName() + " ON "
          + txTable + " (V) WITH (metric='L2', algorithm='IVF', lists=2, sample_size=10)");
        fail("CREATE VECTOR INDEX on a transactional table must be rejected");
      } catch (SQLException e) {
        assertEquals(SQLExceptionCode.VECTOR_INDEX_ON_TRANSACTIONAL_TABLE.getErrorCode(),
          e.getErrorCode());
      }

      conn.createStatement().execute(
        "CREATE TABLE " + plainTable + " (ID VARCHAR NOT NULL PRIMARY KEY, V VECTOR(FLOAT, 3))");
      conn.createStatement().execute("CREATE VECTOR INDEX IDX_" + generateUniqueName() + " ON "
        + plainTable + " (V) WITH (metric='L2', algorithm='IVF', lists=2, sample_size=10)");
      try {
        conn.createStatement().execute(
          "ALTER TABLE " + plainTable + " SET TRANSACTIONAL=true, TRANSACTION_PROVIDER='OMID'");
        fail("Making a table with a vector index transactional must be rejected");
      } catch (SQLException e) {
        assertEquals(SQLExceptionCode.VECTOR_INDEX_ON_TRANSACTIONAL_TABLE.getErrorCode(),
          e.getErrorCode());
      }
    }
  }
}
