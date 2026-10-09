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
package org.apache.phoenix.index;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.IOException;
import java.sql.Connection;
import java.sql.Date;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.Time;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Mutation;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.Pair;
import org.apache.phoenix.cache.VectorCentroidCache;
import org.apache.phoenix.cache.VectorCentroidCache.CachedCentroids;
import org.apache.phoenix.coprocessor.generated.ServerCachingProtos;
import org.apache.phoenix.end2end.index.IndexTestUtil;
import org.apache.phoenix.hbase.index.AbstractValueGetter;
import org.apache.phoenix.hbase.index.IndexRegionObserver;
import org.apache.phoenix.hbase.index.ValueGetter;
import org.apache.phoenix.hbase.index.covered.update.ColumnReference;
import org.apache.phoenix.hbase.index.table.HTableInterfaceReference;
import org.apache.phoenix.hbase.index.util.GenericKeyValueBuilder;
import org.apache.phoenix.hbase.index.util.ImmutableBytesPtr;
import org.apache.phoenix.hbase.index.util.KeyValueBuilder;
import org.apache.phoenix.index.vector.ScorecardAccumulator;
import org.apache.phoenix.jdbc.PhoenixConnection;
import org.apache.phoenix.optimize.DistanceMetric;
import org.apache.phoenix.query.BaseConnectionlessQueryTest;
import org.apache.phoenix.query.QueryConstants;
import org.apache.phoenix.schema.DelegateTable;
import org.apache.phoenix.schema.IllegalDataException;
import org.apache.phoenix.schema.PColumn;
import org.apache.phoenix.schema.PTable;
import org.apache.phoenix.schema.PTableKey;
import org.apache.phoenix.schema.types.PInteger;
import org.apache.phoenix.schema.types.PVectorFloat;
import org.apache.phoenix.util.ByteUtil;
import org.apache.phoenix.util.EnvironmentEdgeManager;
import org.apache.phoenix.util.IndexUtil;
import org.apache.phoenix.util.PhoenixRuntime;
import org.apache.phoenix.util.PropertiesUtil;
import org.apache.phoenix.util.SchemaUtil;
import org.apache.phoenix.util.TestUtil;
import org.apache.phoenix.util.ViewUtil;
import org.bson.BinaryVector;
import org.bson.BsonBinary;
import org.bson.BsonDocument;
import org.junit.Test;

import org.apache.phoenix.thirdparty.com.google.common.collect.ArrayListMultimap;
import org.apache.phoenix.thirdparty.com.google.common.collect.ListMultimap;
import org.apache.phoenix.thirdparty.com.google.common.collect.Maps;

public class IndexMaintainerTest extends BaseConnectionlessQueryTest {
  private static final String DEFAULT_SCHEMA_NAME = "";
  private static final String DEFAULT_TABLE_NAME = "rkTest";

  private void testIndexRowKeyBuilding(String dataColumns, String pk, String indexColumns,
    Object[] values) throws Exception {
    testIndexRowKeyBuilding(DEFAULT_SCHEMA_NAME, DEFAULT_TABLE_NAME, dataColumns, pk, indexColumns,
      values, "", "", "");
  }

  private void testIndexRowKeyBuilding(String dataColumns, String pk, String indexColumns,
    Object[] values, String includeColumns) throws Exception {
    testIndexRowKeyBuilding(DEFAULT_SCHEMA_NAME, DEFAULT_TABLE_NAME, dataColumns, pk, indexColumns,
      values, includeColumns, "", "");
  }

  private void testIndexRowKeyBuilding(String dataColumns, String pk, String indexColumns,
    Object[] values, String includeColumns, String dataProps, String indexProps) throws Exception {
    testIndexRowKeyBuilding(DEFAULT_SCHEMA_NAME, DEFAULT_TABLE_NAME, dataColumns, pk, indexColumns,
      values, "", dataProps, indexProps);
  }

  private static ValueGetter newValueGetter(final byte[] row,
    final Map<ColumnReference, byte[]> valueMap) {
    return new AbstractValueGetter() {

      @Override
      public ImmutableBytesWritable getLatestValue(ColumnReference ref, long ts) {
        return new ImmutableBytesPtr(valueMap.get(ref));
      }

      @Override
      public byte[] getRowKey() {
        return row;
      }

    };
  }

  private void testIndexRowKeyBuilding(String schemaName, String tableName, String dataColumns,
    String pk, String indexColumns, Object[] values, String includeColumns, String dataProps,
    String indexProps) throws Exception {
    KeyValueBuilder builder = GenericKeyValueBuilder.INSTANCE;
    testIndexRowKeyBuilding(schemaName, tableName, dataColumns, pk, indexColumns, values,
      includeColumns, dataProps, indexProps, builder);
  }

  private void testIndexRowKeyBuilding(String schemaName, String tableName, String dataColumns,
    String pk, String indexColumns, Object[] values, String includeColumns, String dataProps,
    String indexProps, KeyValueBuilder builder) throws Exception {
    Connection conn = DriverManager.getConnection(getUrl());
    String fullTableName = SchemaUtil.getTableName(SchemaUtil.normalizeIdentifier(schemaName),
      SchemaUtil.normalizeIdentifier(tableName));
    String fullIndexName = SchemaUtil.getTableName(SchemaUtil.normalizeIdentifier(schemaName),
      SchemaUtil.normalizeIdentifier("idx"));
    conn.createStatement().execute("CREATE TABLE " + fullTableName + "(" + dataColumns
      + " CONSTRAINT pk PRIMARY KEY (" + pk + "))  " + (dataProps.isEmpty() ? "" : dataProps));
    try {
      conn.createStatement()
        .execute("CREATE INDEX idx ON " + fullTableName + "(" + indexColumns + ") "
          + (includeColumns.isEmpty() ? "" : "INCLUDE (" + includeColumns + ") ")
          + (indexProps.isEmpty() ? "" : indexProps));
      PhoenixConnection pconn = conn.unwrap(PhoenixConnection.class);
      PTable table = pconn.getTable(new PTableKey(pconn.getTenantId(), fullTableName));
      PTable index = pconn.getTable(new PTableKey(pconn.getTenantId(), fullIndexName));
      ImmutableBytesWritable ptr = new ImmutableBytesWritable();
      table.getIndexMaintainers(ptr, pconn);
      List<IndexMaintainer> c1 = IndexMaintainer.deserialize(ptr, builder, true);
      assertEquals(1, c1.size());
      IndexMaintainer im1 = c1.get(0);

      StringBuilder buf = new StringBuilder("UPSERT INTO " + fullTableName + " VALUES(");
      for (int i = 0; i < values.length; i++) {
        buf.append("?,");
      }
      buf.setCharAt(buf.length() - 1, ')');
      PreparedStatement stmt = conn.prepareStatement(buf.toString());
      for (int i = 0; i < values.length; i++) {
        stmt.setObject(i + 1, values[i]);
      }
      stmt.execute();
      Iterator<Pair<byte[], List<Cell>>> iterator = PhoenixRuntime.getUncommittedDataIterator(conn);
      List<Cell> dataKeyValues = iterator.next().getSecond();
      Map<ColumnReference, byte[]> valueMap = Maps.newHashMapWithExpectedSize(dataKeyValues.size());
      ImmutableBytesWritable rowKeyPtr =
        new ImmutableBytesWritable(dataKeyValues.get(0).getRowArray(),
          dataKeyValues.get(0).getRowOffset(), dataKeyValues.get(0).getRowLength());
      byte[] row = rowKeyPtr.copyBytes();
      Put dataMutation = new Put(row);
      for (Cell kv : dataKeyValues) {
        valueMap.put(
          new ColumnReference(kv.getFamilyArray(), kv.getFamilyOffset(), kv.getFamilyLength(),
            kv.getQualifierArray(), kv.getQualifierOffset(), kv.getQualifierLength()),
          CellUtil.cloneValue(kv));
        dataMutation.add(kv);
      }
      ValueGetter valueGetter = newValueGetter(row, valueMap);

      List<Mutation> indexMutations =
        IndexTestUtil.generateIndexData(index, table, dataMutation, ptr, builder);
      assertEquals(1, indexMutations.size());
      assertTrue(indexMutations.get(0) instanceof Put);
      Mutation indexMutation = indexMutations.get(0);
      ImmutableBytesWritable indexKeyPtr = new ImmutableBytesWritable(indexMutation.getRow());
      ptr.set(rowKeyPtr.get(), rowKeyPtr.getOffset(), rowKeyPtr.getLength());
      byte[] mutablelndexRowKey =
        im1.buildRowKey(valueGetter, ptr, null, null, HConstants.LATEST_TIMESTAMP);
      byte[] immutableIndexRowKey = indexKeyPtr.copyBytes();
      assertArrayEquals(immutableIndexRowKey, mutablelndexRowKey);
      for (ColumnReference ref : im1.getCoveredColumns()) {
        valueMap.get(ref);
      }
      byte[] dataRowKey = im1.buildDataRowKey(indexKeyPtr, null);
      assertArrayEquals(dataRowKey, CellUtil.cloneRow(dataKeyValues.get(0)));
    } finally {
      try {
        conn.rollback();
        conn.createStatement().execute("DROP TABLE " + fullTableName);
      } finally {
        conn.close();
      }
    }
  }

  @Test
  public void testRowKeyVarOnlyIndex() throws Exception {
    testIndexRowKeyBuilding("k1 VARCHAR, k2 DECIMAL", "k1,k2", "k2, k1", new Object[] { "a", 1.1 });
  }

  @Test
  public void testVarFixedndex() throws Exception {
    testIndexRowKeyBuilding("k1 VARCHAR, k2 INTEGER NOT NULL, v VARCHAR", "k1,k2", "k2, k1",
      new Object[] { "a", 1.1 });
  }

  @Test
  public void testCompositeRowKeyVarFixedIndex() throws Exception {
    // TODO: using 1.1 for INTEGER didn't give error
    testIndexRowKeyBuilding("k1 VARCHAR, k2 INTEGER NOT NULL, v VARCHAR", "k1,k2", "k2, k1",
      new Object[] { "a", 1 });
  }

  @Test
  public void testCompositeRowKeyVarFixedAtEndIndex() throws Exception {
    // Forces trailing zero in index key for fixed length
    for (int i = 0; i < 10; i++) {
      testIndexRowKeyBuilding("k1 VARCHAR, k2 INTEGER NOT NULL, k3 VARCHAR, v VARCHAR", "k1,k2,k3",
        "k1, k3, k2", new Object[] { "a", i, "b" });
    }
  }

  @Test
  public void testSingleKeyValueIndex() throws Exception {
    testIndexRowKeyBuilding("k1 VARCHAR, k2 INTEGER, v VARCHAR", "k1", "v",
      new Object[] { "a", 1, "b" });
  }

  @Test
  public void testMultiKeyValueIndex() throws Exception {
    testIndexRowKeyBuilding(
      "k1 CHAR(1) NOT NULL, k2 INTEGER NOT NULL, v1 DECIMAL, v2 CHAR(2), v3 BIGINT", "k1, k2",
      "v2, k2, v1", new Object[] { "a", 1, 2.2, "bb" });
  }

  @Test
  public void testMultiKeyValueCoveredIndex() throws Exception {
    testIndexRowKeyBuilding(
      "k1 CHAR(1) NOT NULL, k2 INTEGER NOT NULL, v1 DECIMAL, v2 CHAR(2), v3 BIGINT, v4 CHAR(10)",
      "k1, k2", "v2, k2, v1", new Object[] { "a", 1, 2.2, "bb" }, "v3, v4");
  }

  @Test
  public void testSingleKeyValueDescIndex() throws Exception {
    testIndexRowKeyBuilding("k1 VARCHAR, k2 INTEGER, v VARCHAR", "k1", "v DESC",
      new Object[] { "a", 1, "b" });
  }

  @Test
  public void testCompositeRowKeyVarFixedDescIndex() throws Exception {
    testIndexRowKeyBuilding("k1 VARCHAR, k2 INTEGER NOT NULL, v VARCHAR", "k1,k2", "k2 DESC, k1",
      new Object[] { "a", 1 });
  }

  @Test
  public void testCompositeRowKeyTimeIndex() throws Exception {
    long timeInMillis = System.currentTimeMillis();
    long timeInNanos = System.nanoTime();
    Timestamp ts = new Timestamp(timeInMillis);
    ts.setNanos((int) (timeInNanos % 1000000000));
    testIndexRowKeyBuilding("ts1 DATE NOT NULL, ts2 TIME NOT NULL, ts3 TIMESTAMP NOT NULL",
      "ts1,ts2,ts3", "ts2, ts1",
      new Object[] { new Date(timeInMillis), new Time(timeInMillis), ts });
  }

  @Test
  public void testCompositeRowKeyBytesIndex() throws Exception {
    long timeInMillis = System.currentTimeMillis();
    long timeInNanos = System.nanoTime();
    Timestamp ts = new Timestamp(timeInMillis);
    ts.setNanos((int) (timeInNanos % 1000000000));
    testIndexRowKeyBuilding("b1 BINARY(3) NOT NULL, v VARCHAR", "b1,v", "v, b1",
      new Object[] { new byte[] { 41, 42, 43 }, "foo" });
  }

  @Test
  public void testCompositeDescRowKeyVarFixedDescIndex() throws Exception {
    testIndexRowKeyBuilding("k1 VARCHAR, k2 INTEGER NOT NULL, v VARCHAR", "k1, k2 DESC",
      "k2 DESC, k1", new Object[] { "a", 1 });
  }

  @Test
  public void testCompositeDescRowKeyVarDescIndex() throws Exception {
    testIndexRowKeyBuilding("k1 VARCHAR, k2 DECIMAL NOT NULL, v VARCHAR", "k1, k2 DESC",
      "k2 DESC, k1", new Object[] { "a", 1.1, "b" });
  }

  @Test
  public void testCompositeDescRowKeyVarAscIndex() throws Exception {
    testIndexRowKeyBuilding("k1 VARCHAR, k2 DECIMAL NOT NULL, v VARCHAR", "k1, k2 DESC", "k2, k1",
      new Object[] { "a", 1.1, "b" });
  }

  @Test
  public void testCompositeDescRowKeyVarFixedDescSaltedIndex() throws Exception {
    testIndexRowKeyBuilding("k1 VARCHAR, k2 INTEGER NOT NULL, v VARCHAR", "k1, k2 DESC",
      "k2 DESC, k1", new Object[] { "a", 1 }, "", "", "SALT_BUCKETS=4");
  }

  @Test
  public void testCompositeDescRowKeyVarFixedDescSaltedIndexSaltedTable() throws Exception {
    testIndexRowKeyBuilding("k1 VARCHAR, k2 INTEGER NOT NULL, v VARCHAR", "k1, k2 DESC",
      "k2 DESC, k1", new Object[] { "a", 1 }, "", "SALT_BUCKETS=3", "SALT_BUCKETS=3");
  }

  @Test
  public void testMultiKeyValueCoveredSaltedIndex() throws Exception {
    testIndexRowKeyBuilding(
      "k1 CHAR(1) NOT NULL, k2 INTEGER NOT NULL, v1 DECIMAL, v2 CHAR(2), v3 BIGINT, v4 CHAR(10)",
      "k1, k2", "v2 DESC, k2 DESC, v1", new Object[] { "a", 1, 2.2, "bb" }, "v3, v4", "",
      "SALT_BUCKETS=4");
  }

  @Test
  public void tesIndexWithBigInt() throws Exception {
    testIndexRowKeyBuilding(
      "k1 CHAR(1) NOT NULL, k2 INTEGER NOT NULL, v1 BIGINT, v2 CHAR(2), v3 BIGINT, v4 CHAR(10)",
      "k1, k2", "v1 DESC, k2 DESC", new Object[] { "a", 1, 2.2, "bb" });
  }

  @Test
  public void tesIndexWithAscBoolean() throws Exception {
    testIndexRowKeyBuilding(
      "k1 CHAR(1) NOT NULL, k2 INTEGER NOT NULL, v1 BOOLEAN, v2 CHAR(2), v3 BIGINT, v4 CHAR(10)",
      "k1, k2", "v1, k2 DESC", new Object[] { "a", 1, true, "bb" });
  }

  @Test
  public void tesIndexWithAscNullBoolean() throws Exception {
    testIndexRowKeyBuilding(
      "k1 CHAR(1) NOT NULL, k2 INTEGER NOT NULL, v1 BOOLEAN, v2 CHAR(2), v3 BIGINT, v4 CHAR(10)",
      "k1, k2", "v1, k2 DESC", new Object[] { "a", 1, null, "bb" });
  }

  @Test
  public void tesIndexWithAscFalseBoolean() throws Exception {
    testIndexRowKeyBuilding(
      "k1 CHAR(1) NOT NULL, k2 INTEGER NOT NULL, v1 BOOLEAN, v2 CHAR(2), v3 BIGINT, v4 CHAR(10)",
      "k1, k2", "v1, k2 DESC", new Object[] { "a", 1, false, "bb" });
  }

  @Test
  public void tesIndexWithDescBoolean() throws Exception {
    testIndexRowKeyBuilding(
      "k1 CHAR(1) NOT NULL, k2 INTEGER NOT NULL, v1 BOOLEAN, v2 CHAR(2), v3 BIGINT, v4 CHAR(10)",
      "k1, k2", "v1 DESC, k2 DESC", new Object[] { "a", 1, true, "bb" });
  }

  @Test
  public void tesIndexWithDescFalseBoolean() throws Exception {
    testIndexRowKeyBuilding(
      "k1 CHAR(1) NOT NULL, k2 INTEGER NOT NULL, v1 BOOLEAN, v2 CHAR(2), v3 BIGINT, v4 CHAR(10)",
      "k1, k2", "v1 DESC, k2 DESC", new Object[] { "a", 1, false, "bb" });
  }

  @Test
  public void tesIndexedExpressionSerialization() throws Exception {
    Properties props = PropertiesUtil.deepCopy(TestUtil.TEST_PROPERTIES);
    Connection conn = DriverManager.getConnection(getUrl(), props);
    try {
      conn.setAutoCommit(true);
      conn.createStatement().execute(
        "CREATE TABLE IF NOT EXISTS FHA (ORGANIZATION_ID CHAR(15) NOT NULL, PARENT_ID CHAR(15) NOT NULL, CREATED_DATE DATE NOT NULL, ENTITY_HISTORY_ID CHAR(15) NOT NULL, FIELD_HISTORY_ARCHIVE_ID CHAR(15), CREATED_BY_ID VARCHAR, FIELD VARCHAR, DATA_TYPE VARCHAR, OLDVAL_STRING VARCHAR, NEWVAL_STRING VARCHAR, OLDVAL_FIRST_NAME VARCHAR, NEWVAL_FIRST_NAME VARCHAR, OLDVAL_LAST_NAME VARCHAR, NEWVAL_LAST_NAME VARCHAR, OLDVAL_NUMBER DECIMAL, NEWVAL_NUMBER DECIMAL, OLDVAL_DATE DATE,  NEWVAL_DATE DATE, ARCHIVE_PARENT_TYPE VARCHAR, ARCHIVE_FIELD_NAME VARCHAR, ARCHIVE_TIMESTAMP DATE, ARCHIVE_PARENT_NAME VARCHAR, DIVISION INTEGER, CONNECTION_ID VARCHAR CONSTRAINT PK PRIMARY KEY (ORGANIZATION_ID, PARENT_ID, CREATED_DATE DESC, ENTITY_HISTORY_ID )) VERSIONS=1,MULTI_TENANT=true");
      conn.createStatement().execute(
        "CREATE INDEX IDX ON FHA (FIELD_HISTORY_ARCHIVE_ID, UPPER(OLDVAL_STRING) || UPPER(NEWVAL_STRING), NEWVAL_DATE - NEWVAL_DATE)");
      PhoenixConnection pconn = conn.unwrap(PhoenixConnection.class);
      PTable table = pconn.getTable(new PTableKey(pconn.getTenantId(), "FHA"));
      ImmutableBytesWritable ptr = new ImmutableBytesWritable();
      table.getIndexMaintainers(ptr, pconn);
      List<IndexMaintainer> indexMaintainerList =
        IndexMaintainer.deserialize(ptr, GenericKeyValueBuilder.INSTANCE, true);
      assertEquals(1, indexMaintainerList.size());
      IndexMaintainer indexMaintainer = indexMaintainerList.get(0);
      Set<ColumnReference> indexedColumns = indexMaintainer.getIndexedColumns();
      assertEquals("Unexpected Number of indexed columns ", indexedColumns.size(), 4);
    } finally {
      conn.close();
    }
  }

  @Test
  public void testDeleteColumnMutation() throws Exception {
    String tableName = "T_" + generateUniqueName();
    String indexName1 = "I_" + generateUniqueName();
    String indexName2 = "I_" + generateUniqueName();
    String ddl = String.format(
      "create table %s (id varchar primary key, " + "col1 varchar, col2 varchar, col3 bigint)",
      tableName);
    String index1 =
      String.format("create index %s on %s (col2) include (col1) ", indexName1, tableName);
    String index2 = String.format(
      "create index %s on %s (col2) include (col1) "
        + "COLUMN_ENCODED_BYTES=2, IMMUTABLE_STORAGE_SCHEME=SINGLE_CELL_ARRAY_WITH_OFFSETS",
      indexName2, tableName);
    try (Connection conn = DriverManager.getConnection(getUrl())) {
      conn.createStatement().execute(ddl);
      conn.createStatement().execute(index1);
      conn.createStatement().execute(index2);
      PhoenixConnection pconn = conn.unwrap(PhoenixConnection.class);
      PTable table = pconn.getTable(tableName);
      ImmutableBytesWritable ptr = new ImmutableBytesWritable();
      table.getIndexMaintainers(ptr, pconn);
      List<IndexMaintainer> ims =
        IndexMaintainer.deserialize(ptr, GenericKeyValueBuilder.INSTANCE, true);
      assertEquals(2, ims.size());
      String dml = String.format("upsert into %s values ('a', 'ab', 'abc', 2)", tableName);
      assertDeleteColumnMutation(tableName, dml, false, pconn, ims);
      pconn.getMutationState().rollback();
      dml = String.format("upsert into %s (id, col2) values  ('a', 'ab')", tableName);
      assertDeleteColumnMutation(tableName, dml, true, pconn, ims);
      pconn.getMutationState().rollback();
    }
  }

  private static void assertDeleteColumnMutation(String tableName, String dml,
    boolean isPartialUpdate, PhoenixConnection pconn, List<IndexMaintainer> ims) throws Exception {
    pconn.createStatement().execute(dml);
    Iterator<Pair<byte[], List<Mutation>>> iterator = pconn.getMutationState().toMutations();
    while (iterator.hasNext()) {
      Pair<byte[], List<Mutation>> mutationPair = iterator.next();
      List<Mutation> batchMutations = mutationPair.getSecond();
      assertEquals(1, batchMutations.size());
      assertTrue(batchMutations.get(0) instanceof Put);
      Put dataRow = (Put) batchMutations.get(0);
      ValueGetter nextDataRowVG = new IndexUtil.SimpleValueGetter(dataRow);
      long ts = EnvironmentEdgeManager.currentTimeMillis();
      ImmutableBytesPtr rowKey = new ImmutableBytesPtr(dataRow.getRow());
      for (IndexMaintainer im : ims) {
        Put indexPut = im.buildUpdateMutation(GenericKeyValueBuilder.INSTANCE, nextDataRowVG,
          rowKey, ts, null, null, false, null);
        if (indexPut == null) {
          // No covered column. Just prepare an index row with the empty column
          byte[] indexRowKey = im.buildRowKey(nextDataRowVG, rowKey, null, null, ts, null);
          indexPut = new Put(indexRowKey);
        }
        indexPut.addColumn(im.getEmptyKeyValueFamily().copyBytesIfNecessary(),
          im.getEmptyKeyValueQualifier(), ts, QueryConstants.UNVERIFIED_BYTES);
        Delete deleteCol = im.buildDeleteColumnMutation(indexPut, ts);
        if (
          im.getIndexStorageScheme() == PTable.ImmutableStorageScheme.SINGLE_CELL_ARRAY_WITH_OFFSETS
        ) {
          assertNull(deleteCol);
        } else {
          if (isPartialUpdate) {
            assertNotNull(deleteCol);
          } else {
            assertNull(deleteCol);
          }
        }
      }
    }
  }

  /** Makes an IndexMaintainer for a 2D vector index whose centroids are already in the cache. */
  private IndexMaintainer createVectorIndexMaintainer(PhoenixConnection pconn, String tableName,
    String indexName, String elementType, long generation) throws Exception {
    pconn.createStatement().execute("CREATE TABLE " + tableName
      + " (ID VARCHAR PRIMARY KEY, V VECTOR(" + elementType + ", 2), LABEL VARCHAR)");
    pconn.createStatement()
      .execute("CREATE VECTOR INDEX " + indexName + " ON " + tableName
        + " (V) INCLUDE (LABEL) WITH (algorithm = 'IVF', metric = 'L2', lists = 2,"
        + " sample_size = 10) ASYNC");
    PTable dataTable = pconn.getTable(tableName);
    PTable index = pconn.getTable(indexName);
    // The client does not send a maintainer for an untrained ASYNC vector index
    assertNull(index.getVectorCentroidGeneration());
    assertFalse(IndexMaintainer.sendIndexMaintainer(index));
    PTable trained = new DelegateTable(index) {
      @Override
      public Long getVectorCentroidGeneration() {
        return generation;
      }
    };
    assertTrue(IndexMaintainer.sendIndexMaintainer(trained));
    VectorCentroidCache.getInstance(HBaseConfiguration.create()).put(index.getName().getString(),
      generation, new CachedCentroids(Arrays.asList(new float[] { 0, 0 }, new float[] { 10, 10 }),
        DistanceMetric.L2));
    return IndexMaintainer.create(dataTable, trained, pconn);
  }

  private static Put dataRow(PhoenixConnection pconn, String sql) throws Exception {
    pconn.createStatement().execute(sql);
    Iterator<Pair<byte[], List<Mutation>>> iterator = pconn.getMutationState().toMutations();
    Put put = (Put) iterator.next().getSecond().get(0);
    pconn.rollback();
    return put;
  }

  @Test
  public void testVectorIndexRowKey() throws Exception {
    testVectorIndexRowKey("FLOAT");
  }

  @Test
  public void testDoubleVectorIndexRowKey() throws Exception {
    testVectorIndexRowKey("DOUBLE");
  }

  private void testVectorIndexRowKey(String elementType) throws Exception {
    String tableName = "T_" + generateUniqueName();
    String indexName = "I_" + generateUniqueName();
    try (Connection conn = DriverManager.getConnection(getUrl())) {
      PhoenixConnection pconn = conn.unwrap(PhoenixConnection.class);
      IndexMaintainer im =
        createVectorIndexMaintainer(pconn, tableName, indexName, elementType, 7L);
      assertTrue(im.isVectorIndex());
      assertEquals(DistanceMetric.L2, im.getDistanceMetric());
      assertEquals(Long.valueOf(7L), im.getCentroidGeneration());
      // The first row key slot is the centroid ID, a non-null INTEGER
      assertEquals(PInteger.INSTANCE, im.getIndexRowKeySchema().getField(0).getDataType());

      Put near = dataRow(pconn, "UPSERT INTO " + tableName + " VALUES ('a', ARRAY[9, 8], 'x')");
      assertTrue(im.shouldPrepareIndexMutations(near));
      long ts = EnvironmentEdgeManager.currentTimeMillis();
      ImmutableBytesPtr rowKey = new ImmutableBytesPtr(near.getRow());
      Put indexPut = im.buildUpdateMutation(GenericKeyValueBuilder.INSTANCE,
        new IndexUtil.SimpleValueGetter(near), rowKey, ts, null, null, false, null);
      assertArrayEquals(ByteUtil.concat(PInteger.INSTANCE.toBytes(1), Bytes.toBytes("a")),
        indexPut.getRow());
      // The index row also keeps the indexed vector value
      PColumn vecCol = pconn.getTable(indexName).getColumnForColumnName("0:V");
      assertEquals(1,
        indexPut.get(vecCol.getFamilyName().getBytes(), vecCol.getColumnQualifierBytes()).size());

      // The maintainer attributes stay the same after serialization and deserialization
      IndexMaintainer fromProto = IndexMaintainer.fromProto(IndexMaintainer.toProto(im),
        pconn.getTable(tableName).getRowKeySchema(), false);
      assertTrue(fromProto.isVectorIndex());
      assertEquals(Long.valueOf(7L), fromProto.getCentroidGeneration());
      assertArrayEquals(indexPut.getRow(),
        fromProto.buildRowKey(new IndexUtil.SimpleValueGetter(near), rowKey, null, null, ts));

      // A row without a vector has no index row and gets no index mutations
      Put noVector = dataRow(pconn, "UPSERT INTO " + tableName + " (ID, LABEL) VALUES ('b', 'y')");
      assertFalse(im.shouldPrepareIndexMutations(noVector));
      assertNull(im.buildRowKey(new IndexUtil.SimpleValueGetter(noVector),
        new ImmutableBytesPtr(noVector.getRow()), null, null, ts));

      Put same = dataRow(pconn, "UPSERT INTO " + tableName + " VALUES ('a', ARRAY[9, 8], 'z')");
      Put moved = dataRow(pconn, "UPSERT INTO " + tableName + " VALUES ('a', ARRAY[1, 0], 'x')");
      assertTrue(
        im.isVectorUnchanged(im.getIndexedVector(near, false), im.getIndexedVector(same, false)));
      assertFalse(
        im.isVectorUnchanged(im.getIndexedVector(near, false), im.getIndexedVector(moved, false)));
      assertFalse(im.isVectorUnchanged(im.getIndexedVector(noVector, false),
        im.getIndexedVector(near, false)));
    }
  }

  private static Put bsonDataRow(PhoenixConnection pconn, String tableName, String id,
    BsonDocument doc) throws Exception {
    try (PreparedStatement ps =
      pconn.prepareStatement("UPSERT INTO " + tableName + " VALUES (?, ?)")) {
      ps.setString(1, id);
      ps.setObject(2, doc);
      ps.executeUpdate();
    }
    Iterator<Pair<byte[], List<Mutation>>> iterator = pconn.getMutationState().toMutations();
    Put put = (Put) iterator.next().getSecond().get(0);
    pconn.rollback();
    return put;
  }

  @Test
  public void testFunctionalVectorIndexMaintainer() throws Exception {
    String tableName = "T_" + generateUniqueName();
    String indexName = "I_" + generateUniqueName();
    try (Connection conn = DriverManager.getConnection(getUrl())) {
      PhoenixConnection pconn = conn.unwrap(PhoenixConnection.class);
      pconn.createStatement()
        .execute("CREATE TABLE " + tableName + " (ID VARCHAR PRIMARY KEY, DOC BSON)");
      pconn.createStatement()
        .execute("CREATE VECTOR INDEX " + indexName + " ON " + tableName
          + " (BSON_VECTOR_VALUE(DOC, 'v', 2)) WITH (algorithm = 'IVF', metric = 'L2',"
          + " lists = 2, sample_size = 10) ASYNC");
      PTable index = pconn.getTable(indexName);
      PTable trained = new DelegateTable(index) {
        @Override
        public Long getVectorCentroidGeneration() {
          return 3L;
        }
      };
      VectorCentroidCache.getInstance(HBaseConfiguration.create()).put(index.getName().getString(),
        3L, new CachedCentroids(Arrays.asList(new float[] { 0, 0 }, new float[] { 10, 10 }),
          DistanceMetric.L2));
      IndexMaintainer im = IndexMaintainer.create(pconn.getTable(tableName), trained, pconn);
      ColumnReference vectorColumn = im.getFunctionalVectorColumn();
      assertNotNull(vectorColumn);

      // The index row update must hold the computed vector in the PVectorFloat encoding
      Put near = bsonDataRow(pconn, tableName, "a",
        new BsonDocument("v", new BsonBinary(BinaryVector.floatVector(new float[] { 9, 8 }))));
      long ts = EnvironmentEdgeManager.currentTimeMillis();
      ImmutableBytesPtr rowKey = new ImmutableBytesPtr(near.getRow());
      Put indexPut = im.buildUpdateMutation(GenericKeyValueBuilder.INSTANCE,
        new IndexUtil.SimpleValueGetter(near), rowKey, ts, null, null, false, null);
      assertArrayEquals(ByteUtil.concat(PInteger.INSTANCE.toBytes(1), Bytes.toBytes("a")),
        indexPut.getRow());
      assertArrayEquals(PVectorFloat.INSTANCE.toBytes(new float[] { 9, 8 }), CellUtil
        .cloneValue(indexPut.get(vectorColumn.getFamily(), vectorColumn.getQualifier()).get(0)));
      IndexMaintainer fromProto = IndexMaintainer.fromProto(IndexMaintainer.toProto(im),
        pconn.getTable(tableName).getRowKeySchema(), false);
      assertEquals(vectorColumn, fromProto.getFunctionalVectorColumn());

      // A malformed vector in the new row state makes the write fail. A malformed vector in the
      // current row state gives no vector and no index row.
      Put malformed = bsonDataRow(pconn, tableName, "b",
        new BsonDocument("v", new BsonBinary(BinaryVector.int8Vector(new byte[] { 1, 2 }))));
      assertNull(im.getIndexedVector(malformed, false));
      assertFalse(im.shouldPrepareIndexMutations(malformed, null));
      assertNull(im.buildRowKey(new IndexUtil.SimpleValueGetter(malformed),
        new ImmutableBytesPtr(malformed.getRow()), null, null, ts));
      try {
        im.shouldPrepareIndexMutations(malformed);
        fail("A malformed vector being written must fail");
      } catch (IllegalDataException expected) {
      }
      try {
        im.buildUpdateMutation(GenericKeyValueBuilder.INSTANCE,
          new IndexUtil.SimpleValueGetter(malformed), new ImmutableBytesPtr(malformed.getRow()), ts,
          null, null, false, null);
        fail("A malformed vector being written must fail");
      } catch (IllegalDataException expected) {
      }

      // A NaN element makes the vector malformed in the same way
      Put nan = bsonDataRow(pconn, tableName, "c", new BsonDocument("v",
        new BsonBinary(BinaryVector.floatVector(new float[] { 1, Float.NaN }))));
      assertNull(im.getIndexedVector(nan, false));
      assertFalse(im.shouldPrepareIndexMutations(nan, null));
      try {
        im.shouldPrepareIndexMutations(nan);
        fail("A vector with a NaN element being written must fail");
      } catch (IllegalDataException expected) {
      }
    }
  }

  /**
   * Verifies that a single cell index row holds the correct covered column value when the data
   * table and the index use different qualifier encoding schemes.
   */
  @Test
  public void testSingleCellCoveredValueAcrossEncodingSchemes() throws Exception {
    String tableName = "T_" + generateUniqueName();
    String indexName = "I_" + generateUniqueName();
    try (Connection conn = DriverManager.getConnection(getUrl())) {
      PhoenixConnection pconn = conn.unwrap(PhoenixConnection.class);
      pconn.createStatement()
        .execute("CREATE TABLE " + tableName
          + " (ID VARCHAR PRIMARY KEY, K VARCHAR, C VARCHAR) IMMUTABLE_ROWS=true,"
          + " IMMUTABLE_STORAGE_SCHEME=SINGLE_CELL_ARRAY_WITH_OFFSETS, COLUMN_ENCODED_BYTES=2");
      pconn.createStatement().execute("CREATE INDEX " + indexName + " ON " + tableName
        + " (K) INCLUDE (C) COLUMN_ENCODED_BYTES=4");
      PTable dataTable = pconn.getTable(tableName);
      PTable index = pconn.getTable(indexName);
      assertEquals(PTable.QualifierEncodingScheme.TWO_BYTE_QUALIFIERS,
        dataTable.getEncodingScheme());
      assertEquals(PTable.QualifierEncodingScheme.FOUR_BYTE_QUALIFIERS, index.getEncodingScheme());
      IndexMaintainer im = index.getIndexMaintainer(dataTable, pconn);

      Put dataRow = dataRow(pconn, "UPSERT INTO " + tableName + " VALUES ('a', 'k', 'covered')");
      Put indexPut = im.buildUpdateMutation(GenericKeyValueBuilder.INSTANCE,
        new IndexUtil.SimpleValueGetter(dataRow), new ImmutableBytesPtr(dataRow.getRow()),
        EnvironmentEdgeManager.currentTimeMillis(), null, null, false, null);
      PColumn indexCol = index.getColumnForColumnName("0:C");
      List<Cell> cells = indexPut.get(indexCol.getFamilyName().getBytes(),
        QueryConstants.SINGLE_KEYVALUE_COLUMN_QUALIFIER_BYTES);
      assertEquals(1, cells.size());
      ImmutableBytesWritable ptr = new ImmutableBytesWritable(CellUtil.cloneValue(cells.get(0)));
      assertTrue(index.getImmutableStorageScheme().getDecoder().decode(ptr,
        index.getEncodingScheme().decode(indexCol.getColumnQualifierBytes())
          - QueryConstants.ENCODED_CQ_COUNTER_INITIAL_VALUE + 1));
      assertEquals("covered", Bytes.toString(ptr.copyBytes()));
    }
  }

  @Test
  public void testNonVectorIndexMaintainerHasNoVectorFields() throws Exception {
    String tableName = "T_" + generateUniqueName();
    try (Connection conn = DriverManager.getConnection(getUrl())) {
      conn.createStatement()
        .execute("CREATE TABLE " + tableName + " (ID VARCHAR PRIMARY KEY, C VARCHAR)");
      conn.createStatement().execute("CREATE INDEX I_" + tableName + " ON " + tableName + " (C)");
      PhoenixConnection pconn = conn.unwrap(PhoenixConnection.class);
      PTable table = pconn.getTable(tableName);
      IndexMaintainer im = table.getIndexes().get(0).getIndexMaintainer(table, pconn);
      ServerCachingProtos.IndexMaintainer proto = IndexMaintainer.toProto(im);
      assertFalse(proto.hasVectorAlgorithm());
      assertFalse(IndexMaintainer.fromProto(proto, table.getRowKeySchema(), false).isVectorIndex());
    }
  }

  /**
   * Verifies that the maintainer of an untrained vector index loads no centroids and makes no index
   * row. A client makes this maintainer for a delete from an immutable table.
   */
  @Test
  public void testUntrainedVectorIndexKeepsNoIndexRows() throws Exception {
    String tableName = "T_" + generateUniqueName();
    String indexName = "I_" + generateUniqueName();
    try (Connection conn = DriverManager.getConnection(getUrl())) {
      PhoenixConnection pconn = conn.unwrap(PhoenixConnection.class);
      pconn.createStatement().execute(
        "CREATE IMMUTABLE TABLE " + tableName + " (ID VARCHAR PRIMARY KEY, V VECTOR(FLOAT, 2))");
      pconn.createStatement().execute("CREATE VECTOR INDEX " + indexName + " ON " + tableName
        + " (V) WITH (algorithm = 'IVF', metric = 'L2', lists = 2, sample_size = 10) ASYNC");
      PTable index = pconn.getTable(indexName);
      assertNull(index.getVectorCentroidGeneration());
      IndexMaintainer im = IndexMaintainer.create(pconn.getTable(tableName), index, pconn);
      im.loadCentroids(pconn);
      Put row = dataRow(pconn, "UPSERT INTO " + tableName + " VALUES ('a', ARRAY[9, 8])");
      ValueGetter getter = new IndexUtil.SimpleValueGetter(row);
      ImmutableBytesPtr rowKey = new ImmutableBytesPtr(row.getRow());
      long ts = EnvironmentEdgeManager.currentTimeMillis();
      assertNull(im.buildRowKey(getter, rowKey, null, null, ts));
      assertNull(im.buildUpdateMutation(GenericKeyValueBuilder.INSTANCE, getter, rowKey, ts, null,
        null, false));
    }
  }

  /**
   * Verifies that a client keeps the centroids that it loaded before a write. The client has no
   * server connection, so it cannot reload centroids that the cache evicts.
   */
  @Test
  public void testClientHoldsLoadedCentroids() throws Exception {
    String tableName = "T_" + generateUniqueName();
    String indexName = "I_" + generateUniqueName();
    try (Connection conn = DriverManager.getConnection(getUrl())) {
      PhoenixConnection pconn = conn.unwrap(PhoenixConnection.class);
      IndexMaintainer im = createVectorIndexMaintainer(pconn, tableName, indexName, "FLOAT", 7L);
      im.loadCentroids(pconn);
      VectorCentroidCache.getInstance(HBaseConfiguration.create()).invalidate(indexName);
      Put near = dataRow(pconn, "UPSERT INTO " + tableName + " VALUES ('a', ARRAY[9, 8], 'x')");
      Put indexPut = im.buildUpdateMutation(GenericKeyValueBuilder.INSTANCE,
        new IndexUtil.SimpleValueGetter(near), new ImmutableBytesPtr(near.getRow()),
        EnvironmentEdgeManager.currentTimeMillis(), null, null, false, null);
      assertArrayEquals(ByteUtil.concat(PInteger.INSTANCE.toBytes(1), Bytes.toBytes("a")),
        indexPut.getRow());
    }
  }

  /**
   * Verifies that a maintainer keeps the centroids that it resolved for its first row. A model that
   * the cache evicts, or a model larger than the cache, must not reload for each later row.
   */
  @Test
  public void testMaintainerHoldsResolvedCentroids() throws Exception {
    String tableName = "T_" + generateUniqueName();
    String indexName = "I_" + generateUniqueName();
    try (Connection conn = DriverManager.getConnection(getUrl())) {
      PhoenixConnection pconn = conn.unwrap(PhoenixConnection.class);
      IndexMaintainer im = createVectorIndexMaintainer(pconn, tableName, indexName, "FLOAT", 7L);
      Put near = dataRow(pconn, "UPSERT INTO " + tableName + " VALUES ('a', ARRAY[9, 8], 'x')");
      Put far = dataRow(pconn, "UPSERT INTO " + tableName + " VALUES ('b', ARRAY[1, 0], 'x')");
      long ts = EnvironmentEdgeManager.currentTimeMillis();
      assertArrayEquals(ByteUtil.concat(PInteger.INSTANCE.toBytes(1), Bytes.toBytes("a")),
        im.buildRowKey(new IndexUtil.SimpleValueGetter(near), new ImmutableBytesPtr(near.getRow()),
          null, null, ts));
      // This process has no server connection, so a reload fails
      VectorCentroidCache.getInstance(HBaseConfiguration.create()).invalidate(indexName);
      assertArrayEquals(ByteUtil.concat(PInteger.INSTANCE.toBytes(0), Bytes.toBytes("b")),
        im.buildRowKey(new IndexUtil.SimpleValueGetter(far), new ImmutableBytesPtr(far.getRow()),
          null, null, ts));
    }
  }

  /**
   * Verifies that a view with a WHERE clause inherits a vector index with a name that has a view
   * prefix. The inherited index must assign rows with the centroids of the parent index.
   */
  @Test
  public void testViewInheritedVectorIndexUsesIndexCentroids() throws Exception {
    String tableName = "T_" + generateUniqueName();
    String indexName = "I_" + generateUniqueName();
    String viewName = "V_" + generateUniqueName();
    try (Connection conn = DriverManager.getConnection(getUrl())) {
      PhoenixConnection pconn = conn.unwrap(PhoenixConnection.class);
      pconn.createStatement().execute(
        "CREATE TABLE " + tableName + " (ID VARCHAR PRIMARY KEY, K VARCHAR, V VECTOR(FLOAT, 2))");
      // A view inherits an index only if the index covers the WHERE columns of the view
      pconn.createStatement()
        .execute("CREATE VECTOR INDEX " + indexName + " ON " + tableName
          + " (V) INCLUDE (K) WITH (algorithm = 'IVF', metric = 'L2', lists = 2, sample_size = 10)"
          + " ASYNC");
      pconn.createStatement()
        .execute("CREATE VIEW " + viewName + " AS SELECT * FROM " + tableName + " WHERE K = 'k'");
      // Inherit the index in the same way as the view resolution from SYSTEM.CATALOG
      PTable view = pconn.getTable(viewName);
      List<PTable> inheritedIndexes = new ArrayList<>();
      ViewUtil.addIndexesFromParent(pconn, view, pconn.getTable(tableName), inheritedIndexes);
      PTable inherited = inheritedIndexes.get(0);
      assertEquals(viewName + QueryConstants.CHILD_VIEW_INDEX_NAME_SEPARATOR + indexName,
        inherited.getName().getString());
      PTable trained = new DelegateTable(inherited) {
        @Override
        public Long getVectorCentroidGeneration() {
          return 5L;
        }
      };
      VectorCentroidCache.getInstance(HBaseConfiguration.create()).put(indexName, 5L,
        new CachedCentroids(Arrays.asList(new float[] { 0, 0 }, new float[] { 10, 10 }),
          DistanceMetric.L2));
      IndexMaintainer im = IndexMaintainer.create(view, trained, pconn);
      im.loadCentroids(pconn);
      Put near = dataRow(pconn, "UPSERT INTO " + viewName + " (ID, V) VALUES ('a', ARRAY[9, 8])");
      Put indexPut = im.buildUpdateMutation(GenericKeyValueBuilder.INSTANCE,
        new IndexUtil.SimpleValueGetter(near), new ImmutableBytesPtr(near.getRow()),
        EnvironmentEdgeManager.currentTimeMillis(), null, null, false, null);
      assertArrayEquals(ByteUtil.concat(PInteger.INSTANCE.toBytes(1), Bytes.toBytes("a")),
        indexPut.getRow());
    }
  }

  /**
   * Returns a copy of a Put that counts the reads of one column. Each read is one evaluation of an
   * expression on that column.
   */
  private static Put countingReads(Put put, PColumn column, AtomicInteger reads)
    throws IOException {
    byte[] family = column.getFamilyName().getBytes();
    byte[] qualifier = column.getColumnQualifierBytes();
    return new Put(put) {
      @Override
      public List<Cell> get(byte[] f, byte[] q) {
        if (Bytes.equals(family, f) && Bytes.equals(qualifier, q)) {
          reads.incrementAndGet();
        }
        return super.get(f, q);
      }
    };
  }

  private static List<Mutation> indexMutationsForRow(IndexMaintainer im, Put current, Put next,
    long ts) throws IOException {
    ListMultimap<HTableInterfaceReference, Mutation> updates = ArrayListMultimap.create();
    IndexRegionObserver.generateIndexMutationsForRow(
      new ImmutableBytesPtr(next != null ? next.getRow() : current.getRow()), current, next, ts,
      null, QueryConstants.UNVERIFIED_BYTES, Collections.singletonList(new Pair<>(im,
        new HTableInterfaceReference(new ImmutableBytesPtr(im.getIndexTableName())))),
      updates);
    return new ArrayList<>(updates.values());
  }

  /**
   * Verifies that a row write evaluates the indexed vector of each row state only once, for all
   * index mutations and scorecard checks of the write. For a BSON vector, each evaluation is one
   * document decode.
   */
  @Test
  public void testIndexedVectorEvaluatedOncePerRowState() throws Exception {
    String tableName = "T_" + generateUniqueName();
    String indexName = "I_" + generateUniqueName();
    try (Connection conn = DriverManager.getConnection(getUrl())) {
      PhoenixConnection pconn = conn.unwrap(PhoenixConnection.class);
      pconn.createStatement()
        .execute("CREATE TABLE " + tableName + " (ID VARCHAR PRIMARY KEY, DOC BSON)");
      pconn.createStatement()
        .execute("CREATE VECTOR INDEX " + indexName + " ON " + tableName
          + " (BSON_VECTOR_VALUE(DOC, 'v', 2)) WITH (algorithm = 'IVF', metric = 'L2',"
          + " lists = 2, sample_size = 10) ASYNC");
      PTable dataTable = pconn.getTable(tableName);
      PTable trained = new DelegateTable(pconn.getTable(indexName)) {
        @Override
        public Long getVectorCentroidGeneration() {
          return 3L;
        }
      };
      VectorCentroidCache.getInstance(HBaseConfiguration.create()).put(indexName, 3L,
        new CachedCentroids(Arrays.asList(new float[] { 0, 0 }, new float[] { 10, 10 }),
          DistanceMetric.L2));
      IndexMaintainer im = IndexMaintainer.create(dataTable, trained, pconn);
      PColumn doc = dataTable.getColumnForColumnName("DOC");
      Put near = bsonDataRow(pconn, tableName, "a",
        new BsonDocument("v", new BsonBinary(BinaryVector.floatVector(new float[] { 9, 8 }))));
      Put far = bsonDataRow(pconn, tableName, "a",
        new BsonDocument("v", new BsonBinary(BinaryVector.floatVector(new float[] { 1, 0 }))));
      byte[] nearRowKey = ByteUtil.concat(PInteger.INSTANCE.toBytes(1), Bytes.toBytes("a"));
      byte[] farRowKey = ByteUtil.concat(PInteger.INSTANCE.toBytes(0), Bytes.toBytes("a"));
      long ts = EnvironmentEdgeManager.currentTimeMillis();
      AtomicInteger currentReads = new AtomicInteger();
      AtomicInteger nextReads = new AtomicInteger();

      // An unchanged vector writes the index row again at the same row key
      Put current = countingReads(near, doc, currentReads);
      List<Mutation> mutations =
        indexMutationsForRow(im, current, countingReads(near, doc, nextReads), ts);
      assertEquals(1, mutations.size());
      assertArrayEquals(nearRowKey, mutations.get(0).getRow());
      Map<ScorecardAccumulator.Key, long[]> deltas = Maps.newHashMap();
      ScorecardAccumulator.collect(im, current, im.getIndexedVector(near, false), mutations,
        deltas);
      assertEquals(1, currentReads.get());
      assertEquals(1, nextReads.get());
      // The row stays in its posting list, so the scorecard gets no delta
      assertTrue(deltas.isEmpty());

      // A changed vector moves the index row to its new centroid
      currentReads.set(0);
      nextReads.set(0);
      mutations = indexMutationsForRow(im, countingReads(near, doc, currentReads),
        countingReads(far, doc, nextReads), ts);
      assertEquals(2, mutations.size());
      assertTrue(mutations.get(0) instanceof Put);
      assertArrayEquals(farRowKey, mutations.get(0).getRow());
      assertTrue(mutations.get(1) instanceof Delete);
      assertArrayEquals(nearRowKey, mutations.get(1).getRow());
      ScorecardAccumulator.collect(im, near, im.getIndexedVector(near, false), mutations, deltas);
      assertEquals(1, currentReads.get());
      assertEquals(1, nextReads.get());
      assertArrayEquals(new long[] { -1, 0, 0 },
        deltas.get(new ScorecardAccumulator.Key(indexName, 3L, 1)));
      assertArrayEquals(new long[] { 1, 1, 1 },
        deltas.get(new ScorecardAccumulator.Key(indexName, 3L, 0)));

      // A data row delete also deletes the index row
      currentReads.set(0);
      mutations = indexMutationsForRow(im, countingReads(near, doc, currentReads), null, ts);
      assertEquals(1, mutations.size());
      assertTrue(mutations.get(0) instanceof Delete);
      assertArrayEquals(nearRowKey, mutations.get(0).getRow());
      assertEquals(1, currentReads.get());

      // During a migration, an unchanged vector moves its index row to the building generation.
      // The write uses the new row key as the current row key and does not assign the vector again.
      PTable migrating = new DelegateTable(trained) {
        @Override
        public Long getVectorBuildingGeneration() {
          return 4L;
        }
      };
      VectorCentroidCache.getInstance(HBaseConfiguration.create()).put(indexName, 4L,
        new CachedCentroids(Arrays.asList(new float[] { 0, 0 }, new float[] { 10, 10 }),
          DistanceMetric.L2, 2));
      IndexMaintainer migratingIm = IndexMaintainer.create(dataTable, migrating, pconn);
      currentReads.set(0);
      nextReads.set(0);
      mutations = indexMutationsForRow(migratingIm, countingReads(near, doc, currentReads),
        countingReads(near, doc, nextReads), ts);
      assertEquals(2, mutations.size());
      assertTrue(mutations.get(0) instanceof Put);
      assertArrayEquals(ByteUtil.concat(PInteger.INSTANCE.toBytes(3), Bytes.toBytes("a")),
        mutations.get(0).getRow());
      assertTrue(mutations.get(1) instanceof Delete);
      assertArrayEquals(nearRowKey, mutations.get(1).getRow());
      assertEquals(1, currentReads.get());
      assertEquals(1, nextReads.get());
    }
  }
}
