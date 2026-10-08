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
package org.apache.phoenix.mapreduce.index.fsck.hnsw;

import io.github.jbellis.jvector.vector.VectorSimilarityFunction;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.phoenix.hbase.index.hnsw.HnswSegment;
import org.apache.phoenix.index.IndexMaintainer;
import org.apache.phoenix.index.vector.VectorIndexTrainer;
import org.apache.phoenix.jdbc.PhoenixConnection;
import org.apache.phoenix.jdbc.PhoenixResultSet;
import org.apache.phoenix.schema.PColumn;
import org.apache.phoenix.schema.PTable;
import org.apache.phoenix.util.SchemaUtil;

/**
 * Encapsulates table metadata, vector index configuration, and index maintenance state for HNSW
 * verification and repair tooling.
 */
public class HnswIndexContext {
  private final PhoenixConnection connection;
  private final PTable dataTable;
  private final PTable indexTable;
  private final IndexMaintainer maintainer;

  public HnswIndexContext(PhoenixConnection connection, PTable dataTable, PTable indexTable)
    throws SQLException {
    this.connection = connection;
    this.dataTable = dataTable;
    this.indexTable = indexTable;
    this.maintainer = indexTable.getIndexMaintainer(dataTable, connection);
  }

  public PhoenixConnection getConnection() {
    return connection;
  }

  public PTable getDataTable() {
    return dataTable;
  }

  public PTable getIndexTable() {
    return indexTable;
  }

  public PTable.VectorIndex getVectorIndex() {
    return indexTable.getVectorIndex();
  }

  public IndexMaintainer getMaintainer() {
    return maintainer;
  }

  public TableName getDataPhysicalName() {
    return TableName.valueOf(dataTable.getPhysicalName().getBytes());
  }

  public TableName getIndexPhysicalName() {
    return TableName.valueOf(indexTable.getPhysicalName().getBytes());
  }

  /** Returns the column family storing HNSW segment cells. */
  public byte[] getFamily() {
    return SchemaUtil.getEmptyColumnFamily(indexTable);
  }

  public String getMetric() {
    return getVectorIndex().getDistanceMetric();
  }

  public VectorSimilarityFunction getSimilarity() {
    return HnswSegment.similarityFunction(getMetric());
  }

  /**
   * Returns the primary key columns of the data table, excluding salt and tenant prefix columns.
   */
  public List<PColumn> getKeyColumns() {
    List<PColumn> columns = new ArrayList<>(dataTable.getPKColumns());
    if (dataTable.getBucketNum() != null) {
      columns.remove(0);
    }
    if (dataTable.isMultiTenant() && connection.getTenantId() != null) {
      columns.remove(0);
    }
    return columns;
  }

  /** Parses string representations into typed primary key column values in schema order. */
  public List<Object> parseKey(List<String> values) {
    List<PColumn> columns = getKeyColumns();
    if (values.size() != columns.size()) {
      List<String> names = new ArrayList<>();
      for (PColumn column : columns) {
        names.add(column.getName().getString());
      }
      throw new IllegalArgumentException("A data row is named by values for " + names);
    }
    List<Object> key = new ArrayList<>();
    for (int i = 0; i < columns.size(); i++) {
      key.add(columns.get(i).getDataType().toObject(values.get(i)));
    }
    return key;
  }

  /**
   * Retrieves the raw row key and corresponding vector for a given primary key, or null if the row
   * does not exist.
   */
  public KeyedVector readRow(List<Object> key) throws SQLException {
    StringBuilder where = new StringBuilder();
    for (PColumn column : getKeyColumns()) {
      where.append(where.length() == 0 ? "" : " AND ")
        .append(SchemaUtil.getEscapedFullColumnName(column.getName().getString())).append(" = ?");
    }
    try (PreparedStatement ps = connection.prepareStatement("SELECT /*+ NO_INDEX */ "
      + VectorIndexTrainer.getIndexedVectorColumn(indexTable).getExpressionStr() + " FROM "
      + SchemaUtil.getEscapedFullTableName(dataTable.getName().getString()) + " WHERE " + where)) {
      for (int i = 0; i < key.size(); i++) {
        ps.setObject(i + 1, key.get(i));
      }
      try (ResultSet rs = ps.executeQuery()) {
        if (!rs.next()) {
          return null;
        }
        ImmutableBytesWritable row = new ImmutableBytesWritable();
        rs.unwrap(PhoenixResultSet.class).getCurrentRow().getKey(row);
        return new KeyedVector(row.copyBytes(), VectorIndexTrainer.toFloats(rs.getObject(1)));
      }
    }
  }

  /** Container associating a data table row key with its extracted float vector. */
  public static final class KeyedVector {
    private final byte[] key;
    private final float[] vector;

    KeyedVector(byte[] key, float[] vector) {
      this.key = key;
      this.vector = vector;
    }

    public byte[] getKey() {
      return key;
    }

    public float[] getVector() {
      return vector;
    }
  }
}
