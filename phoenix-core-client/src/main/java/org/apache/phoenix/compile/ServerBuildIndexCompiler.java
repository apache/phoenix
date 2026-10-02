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
package org.apache.phoenix.compile;

import static org.apache.phoenix.schema.types.PDataType.TRUE_BYTES;
import static org.apache.phoenix.util.ScanUtil.addEmptyColumnToScan;

import java.sql.Array;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.phoenix.cache.VectorCentroidCache;
import org.apache.phoenix.coprocessorclient.BaseScannerRegionObserverConstants;
import org.apache.phoenix.coprocessorclient.MetaDataProtocol;
import org.apache.phoenix.execute.BaseQueryPlan;
import org.apache.phoenix.execute.MutationState;
import org.apache.phoenix.hbase.index.covered.update.ColumnReference;
import org.apache.phoenix.index.IndexMaintainer;
import org.apache.phoenix.index.PhoenixIndexCodec;
import org.apache.phoenix.jdbc.PhoenixConnection;
import org.apache.phoenix.jdbc.PhoenixDatabaseMetaData;
import org.apache.phoenix.jdbc.PhoenixResultSet;
import org.apache.phoenix.jdbc.PhoenixStatement;
import org.apache.phoenix.query.QueryServices;
import org.apache.phoenix.schema.PColumn;
import org.apache.phoenix.schema.PColumnFamily;
import org.apache.phoenix.schema.PTable;
import org.apache.phoenix.schema.SortOrder;
import org.apache.phoenix.schema.TableRef;
import org.apache.phoenix.schema.tuple.Tuple;
import org.apache.phoenix.schema.types.PLong;
import org.apache.phoenix.schema.types.PVectorDouble;
import org.apache.phoenix.schema.types.PVectorFloat;
import org.apache.phoenix.util.ByteUtil;
import org.apache.phoenix.util.IndexUtil;
import org.apache.phoenix.util.ScanUtil;

/**
 * Class that compiles plan to generate initial data values after a DDL command for index table.
 */
public class ServerBuildIndexCompiler {
  protected final PhoenixConnection connection;
  protected final String tableName;
  protected PTable dataTable;
  protected QueryPlan plan;

  protected class RowCountMutationPlan extends BaseMutationPlan {
    protected RowCountMutationPlan(StatementContext context, PhoenixStatement.Operation operation) {
      super(context, operation);
    }

    @Override
    public MutationState execute() throws SQLException {
      connection.getMutationState().commitDDLFence(dataTable);
      Tuple tuple = plan.iterator().next();
      long rowCount = 0;
      if (tuple != null) {
        Cell kv = tuple.getValue(0);
        ImmutableBytesWritable tmpPtr =
          new ImmutableBytesWritable(kv.getValueArray(), kv.getValueOffset(), kv.getValueLength());
        // A single Cell will be returned with the count(*) - we decode that here
        rowCount = PLong.INSTANCE.getCodec().decodeLong(tmpPtr, SortOrder.getDefault());
      }
      // The contract is to return a MutationState that contains the number of rows modified. In
      // this
      // case, it's the number of rows in the data table which corresponds to the number of index
      // rows that were added.
      return new MutationState(0, 0, connection, rowCount);
    }

    @Override
    public QueryPlan getQueryPlan() {
      return plan;
    }
  };

  /**
   * Mutation plan for populating a vector index from base table rows. Scans the data table,
   * classifies each vector into its nearest centroid partition via the centroid cache, and writes
   * the index row keyed by centroid identifier and base table primary key.
   */
  protected class VectorIndexMutationPlan extends BaseMutationPlan {
    private final PTable index;
    private final String upsertSql;
    private final int vectorIndexInSelected;
    private final PColumn vectorIndexColumn;
    private final int nonCentroidColCount;

    protected VectorIndexMutationPlan(StatementContext context,
      PhoenixStatement.Operation operation, PTable index, String upsertSql,
      int vectorIndexInSelected, PColumn vectorIndexColumn, int nonCentroidColCount) {
      super(context, operation);
      this.index = index;
      this.upsertSql = upsertSql;
      this.vectorIndexInSelected = vectorIndexInSelected;
      this.vectorIndexColumn = vectorIndexColumn;
      this.nonCentroidColCount = nonCentroidColCount;
    }

    @Override
    public TableRef getTargetRef() {
      return new TableRef(index);
    }

    @Override
    public QueryPlan getQueryPlan() {
      return plan;
    }

    @Override
    public MutationState execute() throws SQLException {
      connection.getMutationState().commitDDLFence(dataTable);
      long rowCount = 0;
      String indexFullName = index.getName().getString();
      String metric =
        index.getVectorDistanceMetric() != null ? index.getVectorDistanceMetric() : "L2";
      long generation =
        index.getVectorCentroidGeneration() != null && index.getVectorCentroidGeneration() > 0L
          ? index.getVectorCentroidGeneration()
          : 1L;
      VectorCentroidCache centroidCache =
        VectorCentroidCache.getInstance(connection.getQueryServices().getConfiguration());

      try (
        PhoenixResultSet rs =
          new PhoenixResultSet(plan.iterator(), plan.getProjector(), plan.getContext());
        PreparedStatement ps = connection.prepareStatement(upsertSql)) {
        int mutateBatchSize = connection.getMutateBatchSize();
        while (rs.next()) {
          if (vectorIndexInSelected >= 0) {
            byte[] vectorBytes = rs.getBytes(vectorIndexInSelected + 1);
            if (vectorBytes == null || vectorBytes.length == 0) {
              // Null vectors are excluded from vector indexes
              continue;
            }
            float[] queryFloats;
            if (
              vectorIndexColumn != null && vectorIndexColumn.getDataType() instanceof PVectorDouble
            ) {
              double[] doubles = PVectorDouble.readElements(vectorBytes, 0, vectorBytes.length);
              queryFloats = new float[doubles.length];
              for (int d = 0; d < doubles.length; d++) {
                queryFloats[d] = (float) doubles[d];
              }
            } else {
              queryFloats = PVectorFloat.readElements(vectorBytes, 0, vectorBytes.length);
            }
            int centroidId =
              centroidCache.findNearestCentroid(indexFullName, generation, queryFloats, metric);
            ps.setInt(1, centroidId);
          } else {
            ps.setInt(1, 0);
          }

          for (int k = 0; k < nonCentroidColCount; k++) {
            int selectIdx = k + 1;
            int psIdx = k + 2;
            if (selectIdx - 1 == vectorIndexInSelected) {
              Array array = rs.getArray(selectIdx);
              ps.setArray(psIdx, array);
            } else {
              Object obj = rs.getObject(selectIdx);
              if (obj == null) {
                ps.setNull(psIdx, rs.getMetaData().getColumnType(selectIdx));
              } else {
                ps.setObject(psIdx, obj);
              }
            }
          }
          ps.executeUpdate();
          rowCount++;
          if (rowCount % mutateBatchSize == 0) {
            connection.commit();
          }
        }
        connection.commit();
      }
      return new MutationState(0, 0, connection, rowCount);
    }
  }

  public ServerBuildIndexCompiler(PhoenixConnection connection, String tableName) {
    this.connection = connection;
    this.tableName = tableName;
  }

  private static void addColumnsToScan(Set<ColumnReference> columns, Scan scan, PTable index) {
    for (ColumnReference columnRef : columns) {
      if (
        index.getImmutableStorageScheme()
            == PTable.ImmutableStorageScheme.SINGLE_CELL_ARRAY_WITH_OFFSETS
      ) {
        scan.addFamily(columnRef.getFamily());
      } else {
        scan.addColumn(columnRef.getFamily(), columnRef.getQualifier());
      }
    }
  }

  public MutationPlan compile(PTable index) throws SQLException {
    if (index.isVectorIndex()) {
      return compileVectorIndex(index);
    }
    try (final PhoenixStatement statement = new PhoenixStatement(connection)) {
      String query = "SELECT /*+ NO_INDEX */ count(*) FROM " + tableName;
      this.plan = statement.compileQuery(query);
      TableRef tableRef = plan.getTableRef();
      Scan scan = plan.getContext().getScan();
      ImmutableBytesWritable ptr = new ImmutableBytesWritable();
      dataTable = tableRef.getTable();
      if (IndexUtil.isGlobalIndex(index) && dataTable.isTransactional()) {
        throw new IllegalArgumentException(
          "ServerBuildIndexCompiler does not support global indexes on transactional tables");
      }
      IndexMaintainer indexMaintainer = index.getIndexMaintainer(dataTable, connection);
      // By default, we'd use a FirstKeyOnly filter as nothing else needs to be projected for
      // count(*).
      // However, in this case, we need to project all of the data columns that contribute to the
      // index.
      addColumnsToScan(indexMaintainer.getAllColumns(), scan, index);
      if (indexMaintainer.getIndexWhereColumns() != null) {
        addColumnsToScan(indexMaintainer.getIndexWhereColumns(), scan, index);
      }
      IndexMaintainer.serialize(dataTable, ptr, Collections.singletonList(index),
        plan.getContext().getConnection());
      scan.setAttribute(PhoenixIndexCodec.INDEX_NAME_FOR_IDX_MAINTAINER,
        index.getTableName().getBytes());
      ScanUtil.annotateScanWithMetadataAttributes(dataTable, scan);
      // Set the scan attributes that UngroupedAggregateRegionObserver will switch on.
      // For local indexes, the BaseScannerRegionObserver.LOCAL_INDEX_BUILD_PROTO attribute, and
      // for global indexes PhoenixIndexCodec.INDEX_PROTO_MD attribute is set to the serialized form
      // of index
      // metadata to build index rows from data table rows. For global indexes, we also need to set
      // (1) the
      // BaseScannerRegionObserver.REBUILD_INDEXES attribute in order to signal
      // UngroupedAggregateRegionObserver
      // that this scan is for building global indexes and (2) the MetaDataProtocol.PHOENIX_VERSION
      // attribute
      // that will be passed as a mutation attribute for the scanned mutations that will be applied
      // on
      // the index table possibly remotely
      if (index.getIndexType() == PTable.IndexType.LOCAL) {
        scan.setAttribute(BaseScannerRegionObserverConstants.LOCAL_INDEX_BUILD_PROTO,
          ByteUtil.copyKeyBytesIfNecessary(ptr));
      } else {
        scan.setAttribute(PhoenixIndexCodec.INDEX_PROTO_MD, ByteUtil.copyKeyBytesIfNecessary(ptr));
        scan.setAttribute(BaseScannerRegionObserverConstants.REBUILD_INDEXES, TRUE_BYTES);
        ScanUtil.setClientVersion(scan, MetaDataProtocol.PHOENIX_VERSION);
        scan.setAttribute(BaseScannerRegionObserverConstants.INDEX_REBUILD_PAGING, TRUE_BYTES);
        // Serialize page row size only if we're overriding, else use server side value
        String rebuildPageRowSize = connection.getQueryServices().getProps()
          .get(QueryServices.INDEX_REBUILD_PAGE_SIZE_IN_ROWS);
        if (rebuildPageRowSize != null) {
          scan.setAttribute(BaseScannerRegionObserverConstants.INDEX_REBUILD_PAGE_ROWS,
            Bytes.toBytes(Long.parseLong(rebuildPageRowSize)));
        }
        BaseQueryPlan.serializeViewConstantsIntoScan(scan, dataTable);
        addEmptyColumnToScan(scan, indexMaintainer.getDataEmptyKeyValueCF(),
          indexMaintainer.getEmptyKeyValueQualifier());
      }
      if (dataTable.isTransactional()) {
        scan.setAttribute(BaseScannerRegionObserverConstants.TX_STATE,
          connection.getMutationState().encodeTransaction());
      }

      // Go through MutationPlan abstraction so that we can create local indexes
      // with a connectionless connection (which makes testing easier).
      return new RowCountMutationPlan(plan.getContext(), PhoenixStatement.Operation.UPSERT);
    }
  }

  /**
   * Compiles an execution plan for populating a vector index. Projects required base table columns
   * and maps them to the corresponding index row schema prefixed with the centroid partition
   * identifier.
   */
  public MutationPlan compileVectorIndex(final PTable index) throws SQLException {
    boolean isSalted = index.getBucketNum() != null;
    boolean isMultiTenant = connection.getTenantId() != null && index.isMultiTenant();
    boolean isViewIndex = index.getViewIndexId() != null;
    int posOffset = (isSalted ? 1 : 0) + (isMultiTenant ? 1 : 0) + (isViewIndex ? 1 : 0);

    List<PColumn> indexPKColumns = index.getPKColumns();
    List<String> indexColumnNames = new ArrayList<>();
    List<String> dataColumnExprs = new ArrayList<>();
    int vectorIndexInSelected = -1;
    PColumn vectorIndexColumn = null;

    String centroidColName =
      IndexUtil.getIndexColumnName(null, PhoenixDatabaseMetaData.CENTROID_ID);

    for (int i = posOffset; i < indexPKColumns.size(); i++) {
      PColumn col = indexPKColumns.get(i);
      String indexColName = col.getName().getString();
      if (centroidColName.equals(indexColName)) {
        continue;
      }
      String dataColExpr = IndexUtil.getCaseSensitiveDataColumnFullName(indexColName);
      indexColumnNames.add(indexColName);
      dataColumnExprs.add(dataColExpr);
      if (col.getDataType() != null && col.getDataType().isVectorType()) {
        vectorIndexInSelected = dataColumnExprs.size() - 1;
        vectorIndexColumn = col;
      }
    }

    for (PColumnFamily family : index.getColumnFamilies()) {
      for (PColumn col : family.getColumns()) {
        if (col.getViewConstant() == null) {
          String indexColName = col.getName().getString();
          String dataColExpr = IndexUtil.getCaseSensitiveDataColumnFullName(indexColName);
          indexColumnNames.add(indexColName);
          dataColumnExprs.add(dataColExpr);
          if (col.getDataType() != null && col.getDataType().isVectorType()) {
            vectorIndexInSelected = dataColumnExprs.size() - 1;
            vectorIndexColumn = col;
          }
        }
      }
    }

    StringBuilder selectSql = new StringBuilder();
    selectSql.append("SELECT /*+ NO_INDEX */ ");
    for (int i = 0; i < dataColumnExprs.size(); i++) {
      if (i > 0) {
        selectSql.append(", ");
      }
      selectSql.append(dataColumnExprs.get(i));
    }
    selectSql.append(" FROM ").append(tableName);

    StringBuilder upsertSql = new StringBuilder();
    upsertSql.append("UPSERT /*+ NO_INDEX */ INTO ");
    String indexSchema = index.getSchemaName() != null ? index.getSchemaName().getString() : "";
    if (indexSchema.length() > 0) {
      upsertSql.append('"').append(indexSchema).append("\".");
    }
    upsertSql.append('"').append(index.getTableName().getString()).append("\" (");
    upsertSql.append('"').append(centroidColName).append('"');
    for (String colName : indexColumnNames) {
      upsertSql.append(", \"").append(colName).append('"');
    }
    upsertSql.append(") VALUES (?");
    for (int i = 0; i < indexColumnNames.size(); i++) {
      upsertSql.append(", ?");
    }
    upsertSql.append(")");

    try (final PhoenixStatement statement = new PhoenixStatement(connection)) {
      this.plan = statement.compileQuery(selectSql.toString());
      TableRef tableRef = plan.getTableRef();
      this.dataTable = tableRef.getTable();
      return new VectorIndexMutationPlan(plan.getContext(), PhoenixStatement.Operation.UPSERT,
        index, upsertSql.toString(), vectorIndexInSelected, vectorIndexColumn,
        indexColumnNames.size());
    }
  }
}
