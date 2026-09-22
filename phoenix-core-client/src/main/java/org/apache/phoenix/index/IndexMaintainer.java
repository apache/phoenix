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

import static org.apache.phoenix.schema.PTable.QualifierEncodingScheme.NON_ENCODED_QUALIFIERS;

import com.google.protobuf.InvalidProtocolBufferException;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInput;
import java.io.DataInputStream;
import java.io.DataOutput;
import java.io.DataOutputStream;
import java.io.IOException;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellComparator;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.DoNotRetryIOException;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Durability;
import org.apache.hadoop.hbase.client.Mutation;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.Pair;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableUtils;
import org.apache.phoenix.cache.VectorCentroidCache;
import org.apache.phoenix.compat.hbase.ByteStringer;
import org.apache.phoenix.compile.ColumnResolver;
import org.apache.phoenix.compile.FromCompiler;
import org.apache.phoenix.compile.IndexExpressionCompiler;
import org.apache.phoenix.compile.StatementContext;
import org.apache.phoenix.coprocessor.generated.ServerCachingProtos;
import org.apache.phoenix.coprocessor.generated.ServerCachingProtos.ColumnInfo;
import org.apache.phoenix.expression.CoerceExpression;
import org.apache.phoenix.expression.Expression;
import org.apache.phoenix.expression.ExpressionType;
import org.apache.phoenix.expression.KeyValueColumnExpression;
import org.apache.phoenix.expression.LiteralExpression;
import org.apache.phoenix.expression.SingleCellColumnExpression;
import org.apache.phoenix.expression.SingleCellConstructorExpression;
import org.apache.phoenix.expression.function.PartitionIdFunction;
import org.apache.phoenix.expression.visitor.KeyValueExpressionVisitor;
import org.apache.phoenix.hbase.index.AbstractValueGetter;
import org.apache.phoenix.hbase.index.ValueGetter;
import org.apache.phoenix.hbase.index.covered.update.ColumnReference;
import org.apache.phoenix.hbase.index.util.GenericKeyValueBuilder;
import org.apache.phoenix.hbase.index.util.ImmutableBytesPtr;
import org.apache.phoenix.hbase.index.util.KeyValueBuilder;
import org.apache.phoenix.jdbc.PhoenixConnection;
import org.apache.phoenix.jdbc.PhoenixDatabaseMetaData;
import org.apache.phoenix.jdbc.PhoenixStatement;
import org.apache.phoenix.parse.FunctionParseNode;
import org.apache.phoenix.parse.ParseNode;
import org.apache.phoenix.parse.SQLParser;
import org.apache.phoenix.parse.StatelessTraverseAllParseNodeVisitor;
import org.apache.phoenix.parse.UDFParseNode;
import org.apache.phoenix.query.QueryConstants;
import org.apache.phoenix.schema.AmbiguousColumnException;
import org.apache.phoenix.schema.ColumnFamilyNotFoundException;
import org.apache.phoenix.schema.ColumnNotFoundException;
import org.apache.phoenix.schema.PColumn;
import org.apache.phoenix.schema.PColumnFamily;
import org.apache.phoenix.schema.PDatum;
import org.apache.phoenix.schema.PIndexState;
import org.apache.phoenix.schema.PTable;
import org.apache.phoenix.schema.PTable.ImmutableStorageScheme;
import org.apache.phoenix.schema.PTable.IndexType;
import org.apache.phoenix.schema.PTable.QualifierEncodingScheme;
import org.apache.phoenix.schema.PTableType;
import org.apache.phoenix.schema.RowKeySchema;
import org.apache.phoenix.schema.SaltingUtil;
import org.apache.phoenix.schema.SortOrder;
import org.apache.phoenix.schema.TableRef;
import org.apache.phoenix.schema.ValueSchema;
import org.apache.phoenix.schema.ValueSchema.Field;
import org.apache.phoenix.schema.transform.TransformMaintainer;
import org.apache.phoenix.schema.tuple.BaseTuple;
import org.apache.phoenix.schema.tuple.MultiKeyValueTuple;
import org.apache.phoenix.schema.tuple.ValueGetterTuple;
import org.apache.phoenix.schema.types.IndexConsistency;
import org.apache.phoenix.schema.types.PBoolean;
import org.apache.phoenix.schema.types.PDataType;
import org.apache.phoenix.schema.types.PInteger;
import org.apache.phoenix.schema.types.PVarbinaryEncoded;
import org.apache.phoenix.schema.types.PVectorDouble;
import org.apache.phoenix.schema.types.PVectorFloat;
import org.apache.phoenix.transaction.PhoenixTransactionProvider.Feature;
import org.apache.phoenix.util.BitSet;
import org.apache.phoenix.util.ByteUtil;
import org.apache.phoenix.util.CDCUtil;
import org.apache.phoenix.util.EncodedColumnsUtil;
import org.apache.phoenix.util.EnvironmentEdgeManager;
import org.apache.phoenix.util.ExpressionUtil;
import org.apache.phoenix.util.IndexUtil;
import org.apache.phoenix.util.MetaDataUtil;
import org.apache.phoenix.util.SchemaUtil;
import org.apache.phoenix.util.TransactionUtil;
import org.apache.phoenix.util.TrustedByteArrayOutputStream;

import org.apache.phoenix.thirdparty.com.google.common.base.Preconditions;
import org.apache.phoenix.thirdparty.com.google.common.base.Predicate;
import org.apache.phoenix.thirdparty.com.google.common.collect.Iterators;
import org.apache.phoenix.thirdparty.com.google.common.collect.Lists;
import org.apache.phoenix.thirdparty.com.google.common.collect.Maps;
import org.apache.phoenix.thirdparty.com.google.common.collect.Sets;

/**
 * Class that builds index row key from data row key and current state of row and caches any covered
 * columns. Client-side serializes into byte array using #serialize(PTable, ImmutableBytesWritable)
 * and transmits to server-side through either the
 * {@link org.apache.phoenix.index.PhoenixIndexCodec#INDEX_PROTO_MD} Mutation attribute or as a
 * separate RPC call using {@link org.apache.phoenix.cache.ServerCacheClient})
 * @since 2.1.0
 */
public class IndexMaintainer implements Writable, Iterable<ColumnReference> {

  private static final int EXPRESSION_NOT_PRESENT = -1;
  private static final int ESTIMATED_EXPRESSION_SIZE = 8;

  public static IndexMaintainer create(PTable dataTable, PTable index, PhoenixConnection connection)
    throws SQLException {
    return create(dataTable, null, index, connection);
  }

  public static IndexMaintainer create(PTable dataTable, PTable cdcTable, PTable index,
    PhoenixConnection connection) throws SQLException {
    if (
      dataTable.getType() == PTableType.INDEX || index.getType() != PTableType.INDEX
        || !dataTable.getIndexes().contains(index)
    ) {
      throw new IllegalArgumentException();
    }
    IndexMaintainer maintainer = new IndexMaintainer(dataTable, cdcTable, index, connection);
    return maintainer;
  }

  /**
   * Determines whether the client should send IndexMaintainer for the given Index table.
   * @param index PTable for the index table.
   * @return True if the client needs to send IndexMaintainer for the given Index.
   */
  public static boolean sendIndexMaintainer(PTable index) {
    PIndexState indexState = index.getIndexState();
    if (index.getIndexType() == IndexType.VECTOR_GLOBAL || index.isVectorIndex()) {
      if (indexState.isDisabled() || indexState == PIndexState.PENDING_ACTIVE) {
        return false;
      }
      // Vector index row keys require trained centroids to determine cluster assignment.
      // Once centroids exist, index maintenance proceeds normally to capture concurrent writes.
      return index.getVectorCentroidGeneration() != null;
    }
    return !(indexState.isDisabled() || PIndexState.PENDING_ACTIVE == indexState);
  }

  public static Iterator<PTable> maintainedIndexes(Iterator<PTable> indexes) {
    return Iterators.filter(indexes, new Predicate<PTable>() {
      @Override
      public boolean apply(PTable index) {
        return sendIndexMaintainer(index);
      }
    });
  }

  public static Iterator<PTable> maintainedGlobalIndexesWithMatchingStorageScheme(
    final PTable dataTable, Iterator<PTable> indexes) {
    return Iterators.filter(indexes, new Predicate<PTable>() {
      @Override
      public boolean apply(PTable index) {
        return sendIndexMaintainer(index) && IndexUtil.isGlobalIndex(index)
          && (dataTable.getImmutableStorageScheme() == index.getImmutableStorageScheme()
            && !CDCUtil.isCDCIndex(index));
      }
    });
  }

  public static Iterator<PTable> maintainedLocalOrGlobalIndexesWithoutMatchingStorageScheme(
    final PTable dataTable, Iterator<PTable> indexes, PhoenixConnection connection) {
    return Iterators.filter(indexes, new Predicate<PTable>() {
      @Override
      public boolean apply(PTable index) {
        return sendIndexMaintainer(index) && ((IndexUtil.isGlobalIndex(index)
          && (dataTable.getImmutableStorageScheme() != index.getImmutableStorageScheme()
            || IndexUtil.isServerSideImmutableIndexMaintenanceEnabled(dataTable, connection)))
          || index.getIndexType() == IndexType.LOCAL || CDCUtil.isCDCIndex(index));
      }
    });
  }

  public static Iterator<PTable> maintainedLocalIndexes(Iterator<PTable> indexes) {
    return Iterators.filter(indexes, new Predicate<PTable>() {
      @Override
      public boolean apply(PTable index) {
        return sendIndexMaintainer(index) && index.getIndexType() == IndexType.LOCAL;
      }
    });
  }

  /**
   * For client-side to serialize all IndexMaintainers for a given table
   * @param dataTable data table
   * @param ptr       bytes pointer to hold returned serialized value
   */
  public static void serialize(PTable dataTable, ImmutableBytesWritable ptr,
    PhoenixConnection connection) throws SQLException {
    List<PTable> indexes = dataTable.getIndexes();
    serializeServerMaintainedIndexes(dataTable, ptr, indexes, connection);
  }

  public static void serializeServerMaintainedIndexes(PTable dataTable, ImmutableBytesWritable ptr,
    List<PTable> indexes, PhoenixConnection connection) throws SQLException {
    Iterator<PTable> indexesItr = Collections.emptyListIterator();
    boolean onlyLocalIndexes = dataTable.isImmutableRows() || dataTable.isTransactional();
    if (onlyLocalIndexes) {
      if (
        !dataTable.isTransactional() || !dataTable.getTransactionProvider().getTransactionProvider()
          .isUnsupported(Feature.MAINTAIN_LOCAL_INDEX_ON_SERVER)
      ) {
        indexesItr = maintainedLocalOrGlobalIndexesWithoutMatchingStorageScheme(dataTable,
          indexes.iterator(), connection);
      }
    } else {
      indexesItr = maintainedIndexes(indexes.iterator());
    }

    serialize(dataTable, ptr, Lists.newArrayList(indexesItr), connection);
  }

  /**
   * For client-side to serialize all IndexMaintainers for a given table
   * @param dataTable data table
   * @param ptr       bytes pointer to hold returned serialized value
   * @param indexes   indexes to serialize
   */
  public static void serialize(PTable dataTable, ImmutableBytesWritable ptr, List<PTable> indexes,
    PhoenixConnection connection) throws SQLException {
    if (indexes.isEmpty() && dataTable.getTransformingNewTable() == null) {
      ptr.set(ByteUtil.EMPTY_BYTE_ARRAY);
      return;
    }
    int nIndexes = indexes.size();
    if (dataTable.getTransformingNewTable() != null) {
      // If the transforming new table is in CREATE_DISABLE state, the mutations don't go into the
      // table.
      boolean disabled = dataTable.getTransformingNewTable().isIndexStateDisabled();
      if (disabled && nIndexes == 0) {
        ptr.set(ByteUtil.EMPTY_BYTE_ARRAY);
        return;
      }
      if (!disabled) {
        nIndexes++;
      }
    }
    ByteArrayOutputStream stream = new ByteArrayOutputStream();
    DataOutputStream output = new DataOutputStream(stream);
    try {
      // Encode data table salting in sign of number of indexes
      WritableUtils.writeVInt(output, nIndexes * (dataTable.getBucketNum() == null ? 1 : -1));
      // Write out data row key schema once, since it's the same for all index maintainers
      dataTable.getRowKeySchema().write(output);
      for (PTable index : indexes) {
        org.apache.phoenix.coprocessor.generated.ServerCachingProtos.IndexMaintainer proto =
          IndexMaintainer.toProto(index.getIndexMaintainer(dataTable, connection));
        byte[] protoBytes = proto.toByteArray();
        WritableUtils.writeVInt(output, protoBytes.length);
        output.write(protoBytes);
      }
      if (dataTable.getTransformingNewTable() != null) {
        // We're not serializing the TransformMaintainer if the new transformed table is disabled
        boolean disabled = dataTable.getTransformingNewTable().isIndexStateDisabled();
        if (!disabled) {
          ServerCachingProtos.TransformMaintainer proto = TransformMaintainer.toProto(
            dataTable.getTransformingNewTable().getTransformMaintainer(dataTable, connection));
          byte[] protoBytes = proto.toByteArray();
          WritableUtils.writeVInt(output, protoBytes.length);
          output.write(protoBytes);
        }
      }
    } catch (IOException e) {
      throw new RuntimeException(e); // Impossible
    }
    ptr.set(stream.toByteArray(), 0, stream.size());
  }

  /**
   * For client-side to append serialized IndexMaintainers of keyValueIndexes
   * @param table            data table
   * @param indexMetaDataPtr bytes pointer to hold returned serialized value
   * @param keyValueIndexes  indexes to serialize
   */
  public static void serializeAdditional(PTable table, ImmutableBytesWritable indexMetaDataPtr,
    List<PTable> keyValueIndexes, PhoenixConnection connection) throws SQLException {
    int nMutableIndexes =
      indexMetaDataPtr.getLength() == 0 ? 0 : ByteUtil.vintFromBytes(indexMetaDataPtr);
    int nIndexes = nMutableIndexes + keyValueIndexes.size();
    int estimatedSize = indexMetaDataPtr.getLength() + 1; // Just in case new size increases buffer
    if (indexMetaDataPtr.getLength() == 0) {
      estimatedSize += table.getRowKeySchema().getEstimatedByteSize();
    }
    for (PTable index : keyValueIndexes) {
      estimatedSize += index.getIndexMaintainer(table, connection).getEstimatedByteSize();
    }
    TrustedByteArrayOutputStream stream = new TrustedByteArrayOutputStream(estimatedSize + 1);
    DataOutput output = new DataOutputStream(stream);
    try {
      // Encode data table salting in sign of number of indexes
      WritableUtils.writeVInt(output, nIndexes * (table.getBucketNum() == null ? 1 : -1));
      // Serialize current mutable indexes, subtracting the vint size from the length
      // as its still included
      if (indexMetaDataPtr.getLength() > 0) {
        output.write(indexMetaDataPtr.get(), indexMetaDataPtr.getOffset(),
          indexMetaDataPtr.getLength() - WritableUtils.getVIntSize(nMutableIndexes));
      } else {
        table.getRowKeySchema().write(output);
      }
      // Serialize mutable indexes afterwards
      for (PTable index : keyValueIndexes) {
        IndexMaintainer maintainer = index.getIndexMaintainer(table, connection);
        byte[] protoBytes = IndexMaintainer.toProto(maintainer).toByteArray();
        WritableUtils.writeVInt(output, protoBytes.length);
        output.write(protoBytes);
      }
    } catch (IOException e) {
      throw new RuntimeException(e); // Impossible
    }
    indexMetaDataPtr.set(stream.getBuffer(), 0, stream.size());
  }

  public static List<IndexMaintainer> deserialize(ImmutableBytesWritable metaDataPtr,
    KeyValueBuilder builder, boolean useProtoForIndexMaintainer) {
    return deserialize(metaDataPtr.get(), metaDataPtr.getOffset(), metaDataPtr.getLength(),
      useProtoForIndexMaintainer);
  }

  public static List<IndexMaintainer> deserialize(byte[] buf, boolean useProtoForIndexMaintainer) {
    return deserialize(buf, 0, buf.length, useProtoForIndexMaintainer);
  }

  private static List<IndexMaintainer> deserialize(byte[] buf, int offset, int length,
    boolean useProtoForIndexMaintainer) {
    List<IndexMaintainer> maintainers = Collections.emptyList();
    if (length > 0) {
      ByteArrayInputStream stream = new ByteArrayInputStream(buf, offset, length);
      DataInput input = new DataInputStream(stream);
      try {
        int size = WritableUtils.readVInt(input);
        boolean isDataTableSalted = size < 0;
        size = Math.abs(size);
        RowKeySchema rowKeySchema = new RowKeySchema();
        rowKeySchema.readFields(input);
        maintainers = Lists.newArrayListWithExpectedSize(size);
        for (int i = 0; i < size; i++) {
          if (useProtoForIndexMaintainer) {
            int protoSize = WritableUtils.readVInt(input);
            byte[] b = new byte[protoSize];
            input.readFully(b);
            try {
              org.apache.phoenix.coprocessor.generated.ServerCachingProtos.IndexMaintainer proto =
                ServerCachingProtos.IndexMaintainer.parseFrom(b);
              maintainers.add(IndexMaintainer.fromProto(proto, rowKeySchema, isDataTableSalted));
            } catch (InvalidProtocolBufferException e) {
              org.apache.phoenix.coprocessor.generated.ServerCachingProtos.TransformMaintainer proto =
                ServerCachingProtos.TransformMaintainer.parseFrom(b);
              maintainers
                .add(TransformMaintainer.fromProto(proto, rowKeySchema, isDataTableSalted));
            }
          } else {
            IndexMaintainer maintainer = new IndexMaintainer(rowKeySchema, isDataTableSalted);
            maintainer.readFields(input);
            maintainers.add(maintainer);
          }
        }
      } catch (IOException e) {
        throw new RuntimeException(e); // Impossible
      }
    }
    return maintainers;
  }

  public static IndexMaintainer getIndexMaintainer(List<IndexMaintainer> maintainers,
    byte[] indexTableName) {
    Iterator<IndexMaintainer> maintainerIterator = maintainers.iterator();
    while (maintainerIterator.hasNext()) {
      IndexMaintainer maintainer = maintainerIterator.next();
      if (Bytes.compareTo(indexTableName, maintainer.getIndexTableName()) == 0) {
        return maintainer;
      }
    }
    return null;
  }

  private byte[] viewIndexId;
  private PDataType viewIndexIdType;
  private boolean isMultiTenant;
  private PTableType parentTableType;
  // indexed expressions that are not present in the row key of the data table, the expression can
  // also refer to a regular column
  private List<Expression> indexedExpressions;
  // columns required to evaluate all expressions in indexedExpressions (this does not include
  // columns in the data row key)
  private Set<ColumnReference> indexedColumns;

  // columns required to create index row i.e. indexedColumns + coveredColumns (this does not
  // include columns in the data row key)
  private Set<ColumnReference> allColumns;
  // TODO remove this in the next major release
  private List<PDataType> indexedColumnTypes;
  private int indexDataColumnCount;
  private RowKeyMetaData rowKeyMetaData;
  private byte[] indexTableName;
  private int nIndexSaltBuckets;
  private int nDataTableSaltBuckets;
  private byte[] dataEmptyKeyValueCF;
  private ImmutableBytesPtr emptyKeyValueCFPtr;
  private int nDataCFs;
  private boolean indexWALDisabled;
  private boolean isLocalIndex;
  private boolean immutableRows;
  // Transient state
  private final boolean isDataTableSalted;
  private final RowKeySchema dataRowKeySchema;

  private int estimatedIndexRowKeyBytes;
  private int estimatedExpressionSize;
  private int[] dataPkPosition;
  private int maxTrailingNulls;
  private ColumnReference indexEmptyKeyValueRef;
  private ColumnReference dataEmptyKeyValueRef;
  private boolean rowKeyOrderOptimizable;

  /**** START: New member variables added in 4.10 *****/
  private QualifierEncodingScheme encodingScheme;
  private ImmutableStorageScheme immutableStorageScheme;
  private QualifierEncodingScheme dataEncodingScheme;
  private ImmutableStorageScheme dataImmutableStorageScheme;
  /*
   * Information for columns of data tables that are being indexed. The first part of the pair is
   * column family name and second part is the column name. The reason we need to track this state
   * is because for certain storage schemes like
   * ImmutableStorageScheme#SINGLE_CELL_ARRAY_WITH_OFFSETS, the column for which we need to generate
   * an index table put/delete is different from the columns that are indexed in the phoenix schema.
   * This information helps us determine whether or not certain operations like DROP COLUMN should
   * impact the index.
   */
  private Set<Pair<String, String>> indexedColumnsInfo;
  /*
   * Map of covered columns where a key is column reference for a column in the data table and value
   * is column reference for corresponding column in the index table.
   */
  private Map<ColumnReference, ColumnReference> coveredColumnsMap;
  /**** END: New member variables added in 4.10 *****/

  // **** START: New member variables added in 4.16 ****/
  private String logicalIndexName;

  private boolean isUncovered;
  private Expression indexWhere;
  private Set<ColumnReference> indexWhereColumns;
  private boolean isCDCIndex;
  private IndexConsistency indexConsistency;

  /** Vector indexing algorithm identifier (e.g. "IVF"); non-null indicates a vector index. */
  private String vectorAlgorithm;
  /** Distance metric for similarity evaluation (e.g. "L2", "COSINE"). */
  private String distanceMetric;
  /** Dimensionality of indexed vector data. */
  private Integer vectorDimension;
  /** Centroid generation recorded during index training. */
  private Long centroidGeneration;
  /** Ordinal position of the vector expression within {@code indexedExpressions}. */
  private int vectorExpressionOrdinal = -1;
  /** Mapping of covered vector columns to their respective data types. */
  private Map<ColumnReference, PDataType> coveredVectorColumnTypes = Collections.emptyMap();
  /** Column reference for the functional vector expression in the index table, if applicable. */
  private ColumnReference functionalVectorColRef;

  public IndexMaintainer(RowKeySchema dataRowKeySchema, boolean isDataTableSalted) {
    this.dataRowKeySchema =
      dataRowKeySchema != null ? dataRowKeySchema : new RowKeySchema.RowKeySchemaBuilder(0).build();
    this.isDataTableSalted = isDataTableSalted;
    this.indexConsistency = null;
    this.indexedColumns = Collections.emptySet();
    this.indexedColumnTypes = Collections.emptyList();
    this.coveredColumnsMap = Collections.emptyMap();
    this.indexTableName = ByteUtil.EMPTY_BYTE_ARRAY;
    this.dataEmptyKeyValueCF = QueryConstants.DEFAULT_COLUMN_FAMILY_BYTES;
    this.emptyKeyValueCFPtr = QueryConstants.DEFAULT_COLUMN_FAMILY_BYTES_PTR;
    this.indexedExpressions = Collections.emptyList();
    this.rowKeyMetaData = newRowKeyMetaData(0);
    this.coveredVectorColumnTypes = Collections.emptyMap();
    this.indexedColumnsInfo = Collections.emptySet();
    this.encodingScheme = QualifierEncodingScheme.NON_ENCODED_QUALIFIERS;
    this.immutableStorageScheme = ImmutableStorageScheme.ONE_CELL_PER_COLUMN;
    this.dataEncodingScheme = QualifierEncodingScheme.NON_ENCODED_QUALIFIERS;
    this.dataImmutableStorageScheme = ImmutableStorageScheme.ONE_CELL_PER_COLUMN;
    this.logicalIndexName = "";
  }

  private IndexMaintainer(final PTable dataTable, final PTable index, PhoenixConnection connection)
    throws SQLException {
    this(dataTable, null, index, connection);
  }

  private IndexMaintainer(final PTable dataTable, final PTable cdcTable, final PTable index,
    PhoenixConnection connection) throws SQLException {
    this(dataTable.getRowKeySchema(), dataTable.getBucketNum() != null);
    this.rowKeyOrderOptimizable = index.rowKeyOrderOptimizable();
    this.isMultiTenant = dataTable.isMultiTenant();
    this.viewIndexId = index.getViewIndexId() == null
      ? null
      : index.getviewIndexIdType().toBytes(index.getViewIndexId());
    this.viewIndexIdType = index.getviewIndexIdType();
    this.isLocalIndex = index.getIndexType() == IndexType.LOCAL;
    this.isUncovered = index.getIndexType() == IndexType.UNCOVERED_GLOBAL;
    this.encodingScheme = index.getEncodingScheme();
    this.isCDCIndex = CDCUtil.isCDCIndex(index);
    this.indexConsistency = index.getIndexConsistency();
    if (index.isVectorIndex()) {
      this.vectorAlgorithm = index.getVectorIndexAlgorithm();
      this.distanceMetric = index.getVectorDistanceMetric();
      this.vectorDimension = index.getVectorDimension();
      this.centroidGeneration = index.getVectorCentroidGeneration();
    }

    // null check for b/w compatibility
    this.encodingScheme = index.getEncodingScheme() == null
      ? QualifierEncodingScheme.NON_ENCODED_QUALIFIERS
      : index.getEncodingScheme();
    this.immutableStorageScheme = index.getImmutableStorageScheme() == null
      ? ImmutableStorageScheme.ONE_CELL_PER_COLUMN
      : index.getImmutableStorageScheme();
    this.dataEncodingScheme = dataTable.getEncodingScheme() == null
      ? QualifierEncodingScheme.NON_ENCODED_QUALIFIERS
      : dataTable.getEncodingScheme();
    this.dataImmutableStorageScheme = dataTable.getImmutableStorageScheme() == null
      ? ImmutableStorageScheme.ONE_CELL_PER_COLUMN
      : dataTable.getImmutableStorageScheme();
    this.nDataTableSaltBuckets = isDataTableSalted ? dataTable.getBucketNum() : PTable.NO_SALTING;

    byte[] indexTableName = index.getPhysicalName().getBytes();
    // Use this for the nDataSaltBuckets as we need this for local indexes
    // TODO: persist nDataSaltBuckets separately, but maintain b/w compat.
    Integer nIndexSaltBuckets = isLocalIndex ? dataTable.getBucketNum() : index.getBucketNum();
    boolean indexWALDisabled = index.isWALDisabled();
    int indexPosOffset = (index.getBucketNum() == null ? 0 : 1) + (this.isMultiTenant ? 1 : 0)
      + (this.viewIndexId == null ? 0 : 1);
    int nIndexColumns = index.getColumns().size() - indexPosOffset;
    int nIndexPKColumns = index.getPKColumns().size() - indexPosOffset;
    // number of expressions that are indexed that are not present in the row key of the data table
    int indexedExpressionCount = 0;
    for (int i = indexPosOffset; i < index.getPKColumns().size(); i++) {
      PColumn indexColumn = index.getPKColumns().get(i);
      String indexColumnName = indexColumn.getName().getString();
      String dataFamilyName = IndexUtil.getDataColumnFamilyName(indexColumnName);
      String dataColumnName = IndexUtil.getDataColumnName(indexColumnName);
      try {
        PColumn dataColumn = dataFamilyName.equals("")
          ? dataTable.getColumnForColumnName(dataColumnName)
          : dataTable.getColumnFamily(dataFamilyName).getPColumnForColumnName(dataColumnName);
        if (SchemaUtil.isPKColumn(dataColumn)) continue;
      } catch (ColumnNotFoundException e) {
        // This column must be an expression
      } catch (Exception e) {
        throw new IllegalArgumentException(e);
      }
      indexedExpressionCount++;
    }

    int dataPosOffset = (isDataTableSalted ? 1 : 0) + (this.isMultiTenant ? 1 : 0);

    // For indexes on views, we need to remember which data columns are "constants"
    // These are the values in a VIEW where clause. For these, we don't put them in the
    // index, as they're the same for every row in the index. The data table can be
    // either a VIEW or PROJECTED
    List<PColumn> dataPKColumns = dataTable.getPKColumns();
    this.indexDataColumnCount = dataPKColumns.size();
    PTable parentTable = dataTable;
    // We need to get the PK column for the table on which the index is created
    if (
      !dataTable.getName()
        .equals(cdcTable != null ? cdcTable.getParentName() : index.getParentName())
    ) {
      try {
        String tenantId = (index.getTenantId() != null) ? index.getTenantId().getString() : null;
        parentTable = connection.getTable(tenantId, index.getParentName().getString());
        this.indexDataColumnCount = parentTable.getPKColumns().size();
      } catch (SQLException e) {
        throw new RuntimeException(e);
      }
    }
    this.parentTableType = parentTable.getType();

    int indexPkColumnCount = this.indexDataColumnCount + indexedExpressionCount
      - (this.isDataTableSalted ? 1 : 0) - (this.isMultiTenant ? 1 : 0);
    this.rowKeyMetaData = newRowKeyMetaData(indexPkColumnCount);
    BitSet bitSet = this.rowKeyMetaData.getViewConstantColumnBitSet();

    int nDataPKColumns = this.indexDataColumnCount - dataPosOffset;
    for (int i = dataPosOffset; i < dataPKColumns.size(); i++) {
      PColumn dataPKColumn = dataPKColumns.get(i);
      if (dataPKColumn.getViewConstant() != null) {
        bitSet.set(i);
        nDataPKColumns--;
      }
    }
    this.indexTableName = indexTableName;
    this.indexedColumnTypes =
      Lists.<PDataType> newArrayListWithExpectedSize(nIndexPKColumns - nDataPKColumns);
    this.indexedExpressions = Lists.newArrayListWithExpectedSize(nIndexPKColumns - nDataPKColumns);
    this.coveredColumnsMap = Maps.newHashMapWithExpectedSize(nIndexColumns - nIndexPKColumns);
    this.nIndexSaltBuckets = nIndexSaltBuckets == null ? PTable.NO_SALTING : nIndexSaltBuckets;
    this.dataEmptyKeyValueCF = SchemaUtil.getEmptyColumnFamily(dataTable);
    this.emptyKeyValueCFPtr = SchemaUtil.getEmptyColumnFamilyPtr(index);
    this.nDataCFs = dataTable.getColumnFamilies().size();
    this.indexWALDisabled = indexWALDisabled;
    // TODO: check whether index is immutable or not. Currently it's always false so checking
    // data table is with immutable rows or not.
    this.immutableRows = dataTable.isImmutableRows();
    int indexColByteSize = 0;
    ColumnResolver resolver = null;
    List<ParseNode> parseNodes = new ArrayList<ParseNode>(1);
    UDFParseNodeVisitor visitor = new UDFParseNodeVisitor();
    String centroidColName =
      IndexUtil.getIndexColumnName(null, PhoenixDatabaseMetaData.CENTROID_ID);
    for (int i = indexPosOffset; i < index.getPKColumns().size(); i++) {
      PColumn indexColumn = index.getPKColumns().get(i);
      String expressionStr;
      if (index.isVectorIndex() && centroidColName.equals(indexColumn.getName().getString())) {
        PColumn vecCol = IndexUtil.findVectorColumn(index);
        expressionStr = vecCol != null && vecCol.getExpressionStr() != null
          ? vecCol.getExpressionStr()
          : (vecCol != null
            ? IndexUtil.getCaseSensitiveDataColumnFullName(vecCol.getName().getString())
            : null);
      } else {
        expressionStr = IndexUtil.getIndexColumnExpressionStr(indexColumn);
      }
      try {
        ParseNode parseNode = SQLParser.parseCondition(expressionStr);
        parseNode.accept(visitor);
        parseNodes.add(parseNode);
      } catch (SQLException e) {
        throw new RuntimeException(e);
      }
    }
    try {
      resolver =
        FromCompiler.getResolver(connection, new TableRef(dataTable), visitor.getUdfParseNodes());
    } catch (SQLException e) {
      throw new RuntimeException(e); // Impossible
    }
    StatementContext context = new StatementContext(new PhoenixStatement(connection), resolver);
    this.indexedColumnsInfo = Sets.newHashSetWithExpectedSize(nIndexColumns - nIndexPKColumns);

    IndexExpressionCompiler expressionIndexCompiler = new IndexExpressionCompiler(context);
    for (int i = indexPosOffset; i < index.getPKColumns().size(); i++) {
      PColumn indexColumn = index.getPKColumns().get(i);
      int indexPos = i - indexPosOffset;
      Expression expression = null;
      try {
        expressionIndexCompiler.reset();
        expression = parseNodes.get(indexPos).accept(expressionIndexCompiler);
      } catch (SQLException e) {
        throw new RuntimeException(e); // Impossible
      }
      if (expressionIndexCompiler.getColumnRef() != null) {
        // get the column of the data column that corresponds to this index column
        String indexColName = indexColumn.getName().getString();
        boolean isCentroidCol = index.isVectorIndex() && centroidColName.equals(indexColName);
        PColumn column = isCentroidCol
          ? expressionIndexCompiler.getColumnRef().getColumn()
          : IndexUtil.getDataColumn(dataTable, indexColName);
        boolean isPKColumn = SchemaUtil.isPKColumn(column);
        if (isPKColumn) {
          int dataPkPos = dataTable.getPKColumns().indexOf(column)
            - (dataTable.getBucketNum() == null ? 0 : 1) - (this.isMultiTenant ? 1 : 0);
          this.rowKeyMetaData.setIndexPkPosition(dataPkPos, indexPos);
          indexedColumnsInfo.add(new Pair<>((String) null, column.getName().getString()));
        } else {
          indexColByteSize += column.getDataType().isFixedWidth()
            ? SchemaUtil.getFixedByteSize(column)
            : ValueSchema.ESTIMATED_VARIABLE_LENGTH_SIZE;
          try {
            // Surround constant with cast so that we can still know the original type. Otherwise,
            // if we lose the type,
            // (for example when VARCHAR becomes CHAR), it can lead to problems in the type
            // translation we do between data tables and indexes.
            if (column.isNullable() && ExpressionUtil.isConstant(expression)) {
              expression = CoerceExpression.create(expression, indexColumn.getDataType());
            }
            this.indexedExpressions.add(expression);
            indexedColumnsInfo
              .add(new Pair<>(column.getFamilyName().getString(), column.getName().getString()));
          } catch (SQLException e) {
            throw new RuntimeException(e); // Impossible
          }
        }
      } else {
        indexColByteSize += expression.getDataType().isFixedWidth()
          ? SchemaUtil.getFixedByteSize(expression)
          : ValueSchema.ESTIMATED_VARIABLE_LENGTH_SIZE;
        this.indexedExpressions.add(expression);
        KeyValueExpressionVisitor kvVisitor = new KeyValueExpressionVisitor() {
          @Override
          public Void visit(KeyValueColumnExpression colExpression) {
            return addDataColInfo(dataTable, colExpression);
          }

          @Override
          public Void visit(SingleCellColumnExpression expression) {
            return addDataColInfo(dataTable, expression);
          }

          private Void addDataColInfo(final PTable dataTable, Expression expression) {
            Preconditions.checkArgument(expression instanceof SingleCellColumnExpression
              || expression instanceof KeyValueColumnExpression);

            KeyValueColumnExpression colExpression = null;
            if (expression instanceof SingleCellColumnExpression) {
              colExpression = ((SingleCellColumnExpression) expression).getKeyValueExpression();
            } else {
              colExpression = ((KeyValueColumnExpression) expression);
            }
            byte[] cf = colExpression.getColumnFamily();
            byte[] cq = colExpression.getColumnQualifier();
            try {
              PColumn dataColumn = cf == null
                ? dataTable.getColumnForColumnQualifier(null, cq)
                : dataTable.getColumnFamily(cf).getPColumnForColumnQualifier(cq);
              if (dataColumn == null) {
                if (
                  Bytes.compareTo(cf, dataEmptyKeyValueCF) == 0 && Bytes.compareTo(cq,
                    EncodedColumnsUtil.getEmptyKeyValueInfo(dataEncodingScheme).getFirst()) == 0
                ) {
                  return null;
                } else {
                  throw new ColumnNotFoundException(dataTable.getSchemaName().getString(),
                    dataTable.getTableName().getString(), Bytes.toString(cf), Bytes.toString(cq));
                }
              } else {
                indexedColumnsInfo.add(new Pair<>(dataColumn.getFamilyName().getString(),
                  dataColumn.getName().getString()));
              }
            } catch (ColumnNotFoundException | ColumnFamilyNotFoundException
              | AmbiguousColumnException e) {
              if (dataTable.hasOnlyPkColumns()) {
                return null;
              }
              throw new RuntimeException(e);
            }
            return null;
          }

        };
        expression.accept(kvVisitor);
      }
      // set the sort order of the expression correctly
      if (indexColumn.getSortOrder() == SortOrder.DESC) {
        this.rowKeyMetaData.getDescIndexColumnBitSet().set(indexPos);
      }
    }
    this.estimatedExpressionSize =
      expressionIndexCompiler.getTotalNodeCount() * ESTIMATED_EXPRESSION_SIZE;
    Map<ColumnReference, PDataType> vectorCols = new HashMap<>();
    for (int i = 0; i < index.getColumnFamilies().size(); i++) {
      PColumnFamily family = index.getColumnFamilies().get(i);
      for (PColumn indexColumn : family.getColumns()) {
        PColumn dataColumn =
          IndexUtil.getDataColumnOrNull(dataTable, indexColumn.getName().getString());
        // This can happen during deletion where we don't need covered columns
        if (dataColumn != null) {
          byte[] dataColumnCq = dataColumn.getColumnQualifierBytes();
          byte[] indexColumnCq = indexColumn.getColumnQualifierBytes();
          ColumnReference dataColRef =
            new ColumnReference(dataColumn.getFamilyName().getBytes(), dataColumnCq);
          this.coveredColumnsMap.put(dataColRef,
            new ColumnReference(indexColumn.getFamilyName().getBytes(), indexColumnCq));
          if (dataColumn.getDataType() != null && dataColumn.getDataType().isVectorType()) {
            vectorCols.put(dataColRef, dataColumn.getDataType());
          }
        }
      }
    }
    this.coveredVectorColumnTypes = vectorCols;
    if (index.isVectorIndex()) {
      // For expression-based vector indexes without a backing data table column, maintain the
      // target column as a functional vector column rather than a copied covered column.
      PColumn vecCol = IndexUtil.findVectorColumn(index);
      if (
        vecCol != null
          && IndexUtil.getDataColumnOrNull(dataTable, vecCol.getName().getString()) == null
      ) {
        this.functionalVectorColRef =
          new ColumnReference(vecCol.getFamilyName().getBytes(), vecCol.getColumnQualifierBytes());
      }
    }
    this.estimatedIndexRowKeyBytes = estimateIndexRowKeyByteSize(indexColByteSize);
    this.logicalIndexName = index.getName().getString();
    if (index.getIndexWhere() != null) {
      this.indexWhere = index.getIndexWhereExpression(connection);
      this.indexWhereColumns = index.getIndexWhereColumns(connection);
    }

    initCachedState();
  }

  public void setDataImmutableStorageScheme(ImmutableStorageScheme sc) {
    this.dataImmutableStorageScheme = sc;
  }

  public void setDataEncodingScheme(QualifierEncodingScheme sc) {
    this.dataEncodingScheme = sc;
  }

  public byte[] buildRowKey(ValueGetter valueGetter, ImmutableBytesWritable rowKeyPtr,
    byte[] regionStartKey, byte[] regionEndKey, long ts) {
    return buildRowKey(valueGetter, rowKeyPtr, regionStartKey, regionEndKey, ts, null);
  }

  public byte[] buildRowKey(ValueGetter valueGetter, ImmutableBytesWritable rowKeyPtr,
    byte[] regionStartKey, byte[] regionEndKey, long ts, byte[] encodedRegionName) {
    if (vectorAlgorithm != null) {
      return buildVectorRowKey(valueGetter, rowKeyPtr, ts);
    }
    if (isCDCIndex && encodedRegionName == null) {
      throw new IllegalArgumentException("Encoded region name is required for a CDC index");
    }
    ImmutableBytesWritable ptr = new ImmutableBytesWritable();
    boolean prependRegionStartKey = isLocalIndex && regionStartKey != null;
    boolean isIndexSalted = !isLocalIndex && !isCDCIndex && nIndexSaltBuckets > 0;
    int prefixKeyLength = prependRegionStartKey
      ? (regionStartKey.length != 0 ? regionStartKey.length : regionEndKey.length)
      : 0;
    TrustedByteArrayOutputStream stream = new TrustedByteArrayOutputStream(
      estimatedIndexRowKeyBytes + (prependRegionStartKey ? prefixKeyLength : 0));
    DataOutput output = new DataOutputStream(stream);

    try {
      // For local indexes, we must prepend the row key with the start region key
      if (prependRegionStartKey) {
        if (regionStartKey.length == 0) {
          output.write(new byte[prefixKeyLength]);
        } else {
          output.write(regionStartKey);
        }
      }
      if (isIndexSalted) {
        output.write(0); // will be set at end to index salt byte
      }
      // The dataRowKeySchema includes the salt byte field,
      // so we must adjust for that here.
      int dataPosOffset = isDataTableSalted ? 1 : 0;
      BitSet viewConstantColumnBitSet = this.rowKeyMetaData.getViewConstantColumnBitSet();
      int nIndexedColumns = getIndexPkColumnCount() - getNumViewConstants();
      int[][] dataRowKeyLocator = new int[2][nIndexedColumns];
      // Skip data table salt byte
      int maxRowKeyOffset = rowKeyPtr.getOffset() + rowKeyPtr.getLength();
      dataRowKeySchema.iterator(rowKeyPtr, ptr, dataPosOffset);

      if (viewIndexId != null) {
        output.write(viewIndexId);
      }

      if (isMultiTenant) {
        dataRowKeySchema.next(ptr, dataPosOffset, maxRowKeyOffset);
        output.write(ptr.get(), ptr.getOffset(), ptr.getLength());
        if (!dataRowKeySchema.getField(dataPosOffset).getDataType().isFixedWidth()) {
          output.write(SchemaUtil.getSeparatorBytes(
            dataRowKeySchema.getField(dataPosOffset).getDataType(), rowKeyOrderOptimizable,
            ptr.getLength() == 0, dataRowKeySchema.getField(dataPosOffset).getSortOrder()));
        }
        dataPosOffset++;
      }

      // Write index row key
      for (int i = dataPosOffset; i < indexDataColumnCount; i++) {
        Boolean hasValue = dataRowKeySchema.next(ptr, i, maxRowKeyOffset);
        // Ignore view constants from the data table, as these
        // don't need to appear in the index (as they're the
        // same for all rows in this index)
        if (!viewConstantColumnBitSet.get(i) || isIndexOnBaseTable()) {
          int pos = rowKeyMetaData.getIndexPkPosition(i - dataPosOffset);
          if (Boolean.TRUE.equals(hasValue)) {
            dataRowKeyLocator[0][pos] = ptr.getOffset();
            dataRowKeyLocator[1][pos] = ptr.getLength();
          } else {
            dataRowKeyLocator[0][pos] = 0;
            dataRowKeyLocator[1][pos] = 0;
          }
        }
      }
      BitSet descIndexColumnBitSet = rowKeyMetaData.getDescIndexColumnBitSet();
      Iterator<Expression> expressionIterator = indexedExpressions.iterator();
      int trailingVariableWidthColumnNum = 0;
      PDataType[] indexedColumnDataTypes = new PDataType[nIndexedColumns];
      for (int i = 0; i < nIndexedColumns; i++) {
        PDataType dataColumnType;
        boolean isNullable;
        SortOrder dataSortOrder;
        if (dataPkPosition[i] == EXPRESSION_NOT_PRESENT) {
          Expression expression = expressionIterator.next();
          dataColumnType = expression.getDataType();
          dataSortOrder = expression.getSortOrder();
          isNullable = expression.isNullable();
          if (expression instanceof PartitionIdFunction) {
            if (i != 0) {
              throw new DoNotRetryIOException(
                "PARTITION_ID() has to be the prefix " + "of the index row key!");
            } else if (!isCDCIndex) {
              throw new DoNotRetryIOException(
                "PARTITION_ID() should be used only for" + " CDC Indexes!");
            }
            ptr.set(encodedRegionName);
          } else {
            expression.evaluate(new ValueGetterTuple(valueGetter, ts), ptr);
          }
        } else {
          Field field = dataRowKeySchema.getField(dataPkPosition[i]);
          dataColumnType = field.getDataType();
          ptr.set(rowKeyPtr.get(), dataRowKeyLocator[0][i], dataRowKeyLocator[1][i]);
          dataSortOrder = field.getSortOrder();
          isNullable = field.isNullable();
        }
        boolean isDataColumnInverted = dataSortOrder != SortOrder.ASC;
        PDataType indexColumnType = IndexUtil.getIndexColumnDataType(isNullable, dataColumnType);
        indexedColumnDataTypes[i] = indexColumnType;
        boolean isBytesComparable = dataColumnType.isBytesComparableWith(indexColumnType);
        boolean isIndexColumnDesc = descIndexColumnBitSet.get(i);
        if (isBytesComparable && isDataColumnInverted == isIndexColumnDesc) {
          output.write(ptr.get(), ptr.getOffset(), ptr.getLength());
        } else {
          if (!isBytesComparable) {
            indexColumnType.coerceBytes(ptr, dataColumnType, dataSortOrder, SortOrder.getDefault());
          }
          if (isDataColumnInverted != isIndexColumnDesc) {
            writeInverted(ptr.get(), ptr.getOffset(), ptr.getLength(), output);
          } else {
            output.write(ptr.get(), ptr.getOffset(), ptr.getLength());
          }
        }

        if (!indexColumnType.isFixedWidth()) {
          output.write(SchemaUtil.getSeparatorBytes(indexColumnType, rowKeyOrderOptimizable,
            ptr.getLength() == 0, isIndexColumnDesc ? SortOrder.DESC : SortOrder.ASC));
          trailingVariableWidthColumnNum++;
        } else {
          trailingVariableWidthColumnNum = 0;
        }
      }
      byte[] indexRowKey = stream.getBuffer();
      // Remove trailing nulls
      int length = stream.size();
      int minLength = length - maxTrailingNulls;
      // The existing code does not eliminate the separator if the data type is not nullable. It not
      // clear why.
      // The actual bug is in the calculation of maxTrailingNulls with view indexes. So, in order
      // not to impact some other cases, we should keep minLength check here.
      int indexColumnIdx = nIndexedColumns - 1;
      while (trailingVariableWidthColumnNum > 0 && length > minLength) {
        if (indexColumnIdx < 0) {
          break;
        }
        if (indexedColumnDataTypes[indexColumnIdx] != PVarbinaryEncoded.INSTANCE) {
          if (indexRowKey[length - 1] == QueryConstants.SEPARATOR_BYTE) {
            length--;
          } else {
            break;
          }
        } else {
          byte[] sepBytes = QueryConstants.VARBINARY_ENCODED_SEPARATOR_BYTES;
          if (
            length >= 2 && indexRowKey[length - 1] == sepBytes[1]
              && indexRowKey[length - 2] == sepBytes[0]
          ) {
            length -= 2;
          } else {
            break;
          }
        }
        trailingVariableWidthColumnNum--;
        indexColumnIdx--;
      }

      if (isIndexSalted) {
        // Set salt byte
        byte saltByte = SaltingUtil.getSaltingByte(indexRowKey, SaltingUtil.NUM_SALTING_BYTES,
          length - SaltingUtil.NUM_SALTING_BYTES, nIndexSaltBuckets);
        indexRowKey[0] = saltByte;
      }
      return indexRowKey.length == length ? indexRowKey : Arrays.copyOf(indexRowKey, length);
    } catch (IOException e) {
      throw new RuntimeException(e); // Impossible
    } finally {
      try {
        stream.close();
      } catch (IOException e) {
        throw new RuntimeException(e); // Impossible
      }
    }
  }

  /*
   * Build the data row key from the index row key
   */
  public byte[] buildDataRowKey(ImmutableBytesWritable indexRowKeyPtr, byte[][] viewConstants) {
    RowKeySchema indexRowKeySchema = getIndexRowKeySchema();
    ImmutableBytesWritable ptr = new ImmutableBytesWritable();
    TrustedByteArrayOutputStream stream =
      new TrustedByteArrayOutputStream(estimatedIndexRowKeyBytes);
    DataOutput output = new DataOutputStream(stream);
    // Increment dataPosOffset until all have been written
    int dataPosOffset = 0;
    int viewConstantsIndex = 0;
    try {
      int indexPosOffset = !isLocalIndex && !isCDCIndex && nIndexSaltBuckets > 0 ? 1 : 0;
      int maxRowKeyOffset = indexRowKeyPtr.getOffset() + indexRowKeyPtr.getLength();
      indexRowKeySchema.iterator(indexRowKeyPtr, ptr, indexPosOffset);
      if (isDataTableSalted) {
        dataPosOffset++;
        output.write(0); // will be set at end to salt byte
      }
      if (viewIndexId != null) {
        indexRowKeySchema.next(ptr, indexPosOffset++, maxRowKeyOffset);
      }
      if (isMultiTenant) {
        indexRowKeySchema.next(ptr, indexPosOffset, maxRowKeyOffset);
        output.write(ptr.get(), ptr.getOffset(), ptr.getLength());
        if (!dataRowKeySchema.getField(dataPosOffset).getDataType().isFixedWidth()) {
          output.write(SchemaUtil.getSeparatorBytes(
            dataRowKeySchema.getField(dataPosOffset).getDataType(), rowKeyOrderOptimizable,
            ptr.getLength() == 0, dataRowKeySchema.getField(dataPosOffset).getSortOrder()));
        }
        indexPosOffset++;
        dataPosOffset++;
      }
      indexPosOffset = (!isLocalIndex && !isCDCIndex && nIndexSaltBuckets > 0 ? 1 : 0)
        + (isMultiTenant ? 1 : 0) + (viewIndexId == null ? 0 : 1);
      BitSet viewConstantColumnBitSet = this.rowKeyMetaData.getViewConstantColumnBitSet();
      BitSet descIndexColumnBitSet = rowKeyMetaData.getDescIndexColumnBitSet();
      int trailingVariableWidthColumnNum = 0;
      for (int i = dataPosOffset; i < dataRowKeySchema.getFieldCount(); i++) {
        // Write view constants from the data table, as these
        // won't appear in the index (as they're the
        // same for all rows in this index)
        if (viewConstantColumnBitSet.get(i)) {
          output.write(viewConstants[viewConstantsIndex++]);
        } else {
          int pos = rowKeyMetaData.getIndexPkPosition(i - dataPosOffset);
          Boolean hasValue =
            indexRowKeySchema.iterator(indexRowKeyPtr, ptr, pos + indexPosOffset + 1);
          if (Boolean.TRUE.equals(hasValue)) {
            // Write data row key value taking into account coercion and inversion
            // if necessary
            Field dataField = dataRowKeySchema.getField(i);
            Field indexField = indexRowKeySchema.getField(pos + indexPosOffset);
            PDataType indexColumnType = indexField.getDataType();
            PDataType dataColumnType = dataField.getDataType();
            SortOrder dataSortOrder = dataField.getSortOrder();
            SortOrder indexSortOrder = indexField.getSortOrder();
            boolean isDataColumnInverted = dataSortOrder != SortOrder.ASC;
            boolean isBytesComparable = dataColumnType.isBytesComparableWith(indexColumnType);
            if (isBytesComparable && isDataColumnInverted == descIndexColumnBitSet.get(pos)) {
              output.write(ptr.get(), ptr.getOffset(), ptr.getLength());
            } else {
              if (!isBytesComparable) {
                dataColumnType.coerceBytes(ptr, indexColumnType, indexSortOrder,
                  SortOrder.getDefault());
              }
              if (descIndexColumnBitSet.get(pos) != isDataColumnInverted) {
                writeInverted(ptr.get(), ptr.getOffset(), ptr.getLength(), output);
              } else {
                output.write(ptr.get(), ptr.getOffset(), ptr.getLength());
              }
            }
          }
        }
        // Write separator byte(s) if variable length
        if (!dataRowKeySchema.getField(i).getDataType().isFixedWidth()) {
          output.write(SchemaUtil.getSeparatorBytes(dataRowKeySchema.getField(i).getDataType(),
            rowKeyOrderOptimizable, ptr.getLength() == 0,
            dataRowKeySchema.getField(i).getSortOrder()));
          trailingVariableWidthColumnNum++;
        } else {
          trailingVariableWidthColumnNum = 0;
        }
      }
      int length = stream.size();
      byte[] dataRowKey = stream.getBuffer();
      // Remove trailing nulls
      int indexColumnIdx = dataRowKeySchema.getFieldCount() - 1;
      while (trailingVariableWidthColumnNum > 0) {
        PDataType<?> dataType = dataRowKeySchema.getField(indexColumnIdx).getDataType();
        if (dataType != PVarbinaryEncoded.INSTANCE) {
          if (dataRowKey[length - 1] == QueryConstants.SEPARATOR_BYTE) {
            length--;
          } else {
            break;
          }
        } else {
          byte[] sepBytes = QueryConstants.VARBINARY_ENCODED_SEPARATOR_BYTES;
          if (
            length >= 2 && dataRowKey[length - 1] == sepBytes[1]
              && dataRowKey[length - 2] == sepBytes[0]
          ) {
            length -= 2;
          } else {
            break;
          }
        }
        trailingVariableWidthColumnNum--;
        indexColumnIdx--;
      }
      if (isDataTableSalted) {
        // Set salt byte
        byte saltByte = SaltingUtil.getSaltingByte(dataRowKey, SaltingUtil.NUM_SALTING_BYTES,
          length - SaltingUtil.NUM_SALTING_BYTES, nDataTableSaltBuckets);
        dataRowKey[0] = saltByte;
      }
      return dataRowKey.length == length ? dataRowKey : Arrays.copyOf(dataRowKey, length);
    } catch (IOException e) {
      throw new RuntimeException(e); // Impossible
    } finally {
      try {
        stream.close();
      } catch (IOException e) {
        throw new RuntimeException(e); // Impossible
      }
    }
  }

  public byte[] getIndexRowKey(final Put dataRow) {
    ValueGetter valueGetter = new IndexUtil.SimpleValueGetter(dataRow);
    return buildRowKey(valueGetter, new ImmutableBytesWritable(dataRow.getRow()), null, null,
      IndexUtil.getMaxTimestamp(dataRow));
  }

  public boolean checkIndexRow(final byte[] indexRowKey, final Put dataRow) {
    if (!shouldPrepareIndexMutations(dataRow)) {
      return false;
    }
    byte[] builtIndexRowKey = getIndexRowKey(dataRow);
    if (
      Bytes.compareTo(builtIndexRowKey, 0, builtIndexRowKey.length, indexRowKey, 0,
        indexRowKey.length) != 0
    ) {
      return false;
    }
    return true;
  }

  public byte[] getIndexRowKey(final Put dataRow, byte[] encodedRegionName) {
    ValueGetter valueGetter = new IndexUtil.SimpleValueGetter(dataRow);
    return buildRowKey(valueGetter, new ImmutableBytesWritable(dataRow.getRow()), null, null,
      IndexUtil.getMaxTimestamp(dataRow), encodedRegionName);
  }

  public boolean checkIndexRow(final byte[] indexRowKey, final byte[] dataRowKey, final Put dataRow,
    final byte[][] viewConstants) {
    if (!shouldPrepareIndexMutations(dataRow)) {
      return false;
    }
    byte[] builtDataRowKey =
      buildDataRowKey(new ImmutableBytesWritable(indexRowKey), viewConstants);
    if (
      Bytes.compareTo(builtDataRowKey, 0, builtDataRowKey.length, dataRowKey, 0, dataRowKey.length)
          != 0
    ) {
      return false;
    }
    return true;
  }

  /**
   * Determines if the index row for a given data row should be prepared. For full indexes, index
   * rows should always be prepared. For the partial indexes, the index row should be prepared only
   * if the index where clause is satisfied on the given data row.
   * @param dataRowState data row represented as a put mutation, that is list of put cells
   * @return always true for full indexes, and true for partial indexes if the index where
   *         expression evaluates to true on the given data row
   */

  public boolean shouldPrepareIndexMutations(Put dataRowState) {
    if (vectorAlgorithm != null) {
      if (dataRowState == null) {
        return false;
      }
      ValueGetter vg = new IndexUtil.SimpleValueGetter(dataRowState);
      ImmutableBytesWritable ptr = new ImmutableBytesWritable();
      if (vectorExpressionOrdinal >= 0 && vectorExpressionOrdinal < indexedExpressions.size()) {
        Expression vecExpr = indexedExpressions.get(vectorExpressionOrdinal);
        vecExpr.evaluate(new ValueGetterTuple(vg, HConstants.LATEST_TIMESTAMP), ptr);
      }
      if (ptr.get() == null || ptr.getLength() == 0) {
        return false;
      }
    }
    if (getIndexWhere() == null) {
      // It is a full index and the index row should be prepared.
      return true;
    }
    List<Cell> cols = IndexUtil.readColumnsFromRow(dataRowState, getIndexWhereColumns());
    // Cells should be sorted as they are searched using a binary search during expression
    // evaluation
    Collections.sort(cols, CellComparator.getInstance());
    MultiKeyValueTuple tuple = new MultiKeyValueTuple(cols);
    ImmutableBytesWritable ptr = new ImmutableBytesWritable();
    ptr.set(ByteUtil.EMPTY_BYTE_ARRAY);
    if (!getIndexWhere().evaluate(tuple, ptr)) {
      return false;
    }
    Object value = PBoolean.INSTANCE.toObject(ptr);
    return value.equals(Boolean.TRUE);
  }

  public Boolean isAgedEnough(long ts, long ageThreshold) {
    return (EnvironmentEdgeManager.currentTimeMillis() - ts) > ageThreshold;
  }

  public Delete createDelete(byte[] indexRowKey, long ts, boolean singleVersion) {
    if (singleVersion) {
      return buildRowDeleteMutation(indexRowKey, IndexMaintainer.DeleteType.SINGLE_VERSION, ts);
    } else {
      return buildRowDeleteMutation(indexRowKey, IndexMaintainer.DeleteType.ALL_VERSIONS, ts);
    }
  }

  /*
   * return the view index id from the index row key
   */
  public byte[] getViewIndexIdFromIndexRowKey(ImmutableBytesWritable indexRowKeyPtr) {
    assert (isLocalIndex);
    ImmutableBytesPtr ptr = new ImmutableBytesPtr(indexRowKeyPtr.get(),
      (indexRowKeyPtr.getOffset() + (!isLocalIndex && nIndexSaltBuckets > 0 ? 1 : 0)),
      viewIndexId.length);
    return ptr.copyBytesIfNecessary();
  }

  private volatile RowKeySchema indexRowKeySchema;

  // We have enough information to generate the index row key schema
  private RowKeySchema generateIndexRowKeySchema() {
    int nIndexedColumns = getIndexPkColumnCount() + (isMultiTenant ? 1 : 0)
      + (!isLocalIndex && nIndexSaltBuckets > 0 ? 1 : 0) + (viewIndexId != null ? 1 : 0)
      - getNumViewConstants();
    RowKeySchema.RowKeySchemaBuilder builder =
      new RowKeySchema.RowKeySchemaBuilder(nIndexedColumns);
    builder.rowKeyOrderOptimizable(rowKeyOrderOptimizable);
    if (!isLocalIndex && nIndexSaltBuckets > 0) {
      builder.addField(SaltingUtil.SALTING_COLUMN, false, SortOrder.ASC);
      nIndexedColumns--;
    }
    int dataPosOffset = isDataTableSalted ? 1 : 0;
    if (viewIndexId != null) {
      nIndexedColumns--;
      builder.addField(new PDatum() {

        @Override
        public boolean isNullable() {
          return false;
        }

        @Override
        public PDataType getDataType() {
          return viewIndexIdType;
        }

        @Override
        public Integer getMaxLength() {
          return null;
        }

        @Override
        public Integer getScale() {
          return null;
        }

        @Override
        public SortOrder getSortOrder() {
          return SortOrder.getDefault();
        }

      }, false, SortOrder.getDefault());
    }
    if (isMultiTenant) {
      Field field = dataRowKeySchema.getField(dataPosOffset++);
      builder.addField(field, field.isNullable(), field.getSortOrder());
      nIndexedColumns--;
    }

    Field[] indexFields = new Field[nIndexedColumns];
    BitSet viewConstantColumnBitSet = this.rowKeyMetaData.getViewConstantColumnBitSet();
    // Add Field for all data row pk columns
    for (int i = dataPosOffset; i < dataRowKeySchema.getFieldCount(); i++) {
      // Ignore view constants from the data table, as these
      // don't need to appear in the index (as they're the
      // same for all rows in this index)
      if (!viewConstantColumnBitSet.get(i)) {
        int pos = rowKeyMetaData.getIndexPkPosition(i - dataPosOffset);
        indexFields[pos] = dataRowKeySchema.getField(i);
      }
    }
    BitSet descIndexColumnBitSet = rowKeyMetaData.getDescIndexColumnBitSet();
    Iterator<Expression> expressionItr = indexedExpressions.iterator();
    for (int i = 0; i < indexFields.length; i++) {
      Field indexField = indexFields[i];
      PDataType dataTypeToBe;
      SortOrder sortOrderToBe;
      boolean isNullableToBe;
      Integer maxLengthToBe;
      Integer scaleToBe;
      if (indexField == null) {
        Expression e = expressionItr.next();
        if (isVectorIndex() && i == 0) {
          isNullableToBe = false;
          dataTypeToBe = PInteger.INSTANCE;
          sortOrderToBe = descIndexColumnBitSet.get(i) ? SortOrder.DESC : SortOrder.ASC;
          maxLengthToBe = null;
          scaleToBe = null;
        } else {
          isNullableToBe = e.isNullable();
          dataTypeToBe = IndexUtil.getIndexColumnDataType(isNullableToBe, e.getDataType());
          sortOrderToBe = descIndexColumnBitSet.get(i) ? SortOrder.DESC : SortOrder.ASC;
          maxLengthToBe = e.getMaxLength();
          scaleToBe = e.getScale();
        }
      } else {
        isNullableToBe = indexField.isNullable();
        dataTypeToBe = IndexUtil.getIndexColumnDataType(isNullableToBe, indexField.getDataType());
        sortOrderToBe = descIndexColumnBitSet.get(i) ? SortOrder.DESC : SortOrder.ASC;
        maxLengthToBe = indexField.getMaxLength();
        scaleToBe = indexField.getScale();
      }
      final PDataType dataType = dataTypeToBe;
      final SortOrder sortOrder = sortOrderToBe;
      final boolean isNullable = isNullableToBe;
      final Integer maxLength = maxLengthToBe;
      final Integer scale = scaleToBe;
      builder.addField(new PDatum() {

        @Override
        public boolean isNullable() {
          return isNullable;
        }

        @Override
        public PDataType getDataType() {
          return dataType;
        }

        @Override
        public Integer getMaxLength() {
          return maxLength;
        }

        @Override
        public Integer getScale() {
          return scale;
        }

        @Override
        public SortOrder getSortOrder() {
          return sortOrder;
        }

      }, true, sortOrder);
    }
    return builder.build();
  }

  private int getNumViewConstants() {
    if (isIndexOnBaseTable()) {
      return 0;
    }
    BitSet bitSet = this.rowKeyMetaData.getViewConstantColumnBitSet();
    int num = 0;
    for (int i = 0; i < dataRowKeySchema.getFieldCount(); i++) {
      if (bitSet.get(i)) num++;
    }
    return num;
  }

  public RowKeySchema getIndexRowKeySchema() {
    if (indexRowKeySchema != null) {
      return indexRowKeySchema;
    }
    synchronized (this) {
      if (indexRowKeySchema == null) {
        indexRowKeySchema = generateIndexRowKeySchema();
      }
    }
    return indexRowKeySchema;
  }

  public Put buildUpdateMutation(KeyValueBuilder kvBuilder, ValueGetter valueGetter,
    ImmutableBytesWritable dataRowKeyPtr, long ts, byte[] regionStartKey, byte[] regionEndKey,
    boolean verified) throws IOException {
    return buildUpdateMutation(kvBuilder, valueGetter, dataRowKeyPtr, ts, regionStartKey,
      regionEndKey, verified, null);
  }

  public Put buildUpdateMutation(KeyValueBuilder kvBuilder, ValueGetter valueGetter,
    ImmutableBytesWritable dataRowKeyPtr, long ts, byte[] regionStartKey, byte[] regionEndKey,
    boolean verified, byte[] encodedRegionName) throws IOException {
    return buildUpdateMutation(kvBuilder, valueGetter, dataRowKeyPtr, ts, regionStartKey,
      regionEndKey, verified, encodedRegionName, false);
  }

  public Put buildUpdateMutation(KeyValueBuilder kvBuilder, ValueGetter valueGetter,
    ImmutableBytesWritable dataRowKeyPtr, long ts, byte[] regionStartKey, byte[] regionEndKey,
    boolean verified, byte[] encodedRegionName, boolean isVectorUnchanged) throws IOException {
    byte[] indexRowKey;
    ImmutableBytesWritable vectorValue = null;
    if (vectorAlgorithm != null) {
      // Reuse the evaluated vector expression across row key centroid calculation and the
      // functional vector column value to avoid redundant evaluations.
      vectorValue = getVectorValue(valueGetter, ts);
      indexRowKey = buildVectorRowKey(valueGetter, dataRowKeyPtr, ts, vectorValue);
    } else {
      indexRowKey = this.buildRowKey(valueGetter, dataRowKeyPtr, regionStartKey, regionEndKey, ts,
        encodedRegionName);
    }
    if (indexRowKey == null) {
      return null;
    }
    return buildUpdateMutation(kvBuilder, valueGetter, dataRowKeyPtr, ts, regionStartKey,
      regionEndKey, indexRowKey, this.getEmptyKeyValueFamily(), coveredColumnsMap,
      indexEmptyKeyValueRef, indexWALDisabled, dataImmutableStorageScheme, immutableStorageScheme,
      encodingScheme, dataEncodingScheme, verified, this, isVectorUnchanged, vectorValue);
  }

  public static Put buildUpdateMutation(KeyValueBuilder kvBuilder, ValueGetter valueGetter,
    ImmutableBytesWritable dataRowKeyPtr, long ts, byte[] regionStartKey, byte[] regionEndKey,
    byte[] destRowKey, ImmutableBytesPtr emptyKeyValueCFPtr,
    Map<ColumnReference, ColumnReference> coveredColumnsMap, ColumnReference destEmptyKeyValueRef,
    boolean destWALDisabled, ImmutableStorageScheme srcImmutableStorageScheme,
    ImmutableStorageScheme destImmutableStorageScheme, QualifierEncodingScheme destEncodingScheme,
    QualifierEncodingScheme srcEncodingScheme, boolean verified) throws IOException {
    return buildUpdateMutation(kvBuilder, valueGetter, dataRowKeyPtr, ts, regionStartKey,
      regionEndKey, destRowKey, emptyKeyValueCFPtr, coveredColumnsMap, destEmptyKeyValueRef,
      destWALDisabled, srcImmutableStorageScheme, destImmutableStorageScheme, destEncodingScheme,
      srcEncodingScheme, verified, null, false);
  }

  public static Put buildUpdateMutation(KeyValueBuilder kvBuilder, ValueGetter valueGetter,
    ImmutableBytesWritable dataRowKeyPtr, long ts, byte[] regionStartKey, byte[] regionEndKey,
    byte[] destRowKey, ImmutableBytesPtr emptyKeyValueCFPtr,
    Map<ColumnReference, ColumnReference> coveredColumnsMap, ColumnReference destEmptyKeyValueRef,
    boolean destWALDisabled, ImmutableStorageScheme srcImmutableStorageScheme,
    ImmutableStorageScheme destImmutableStorageScheme, QualifierEncodingScheme destEncodingScheme,
    QualifierEncodingScheme srcEncodingScheme, boolean verified, IndexMaintainer maintainer,
    boolean isVectorUnchanged) throws IOException {
    return buildUpdateMutation(kvBuilder, valueGetter, dataRowKeyPtr, ts, regionStartKey,
      regionEndKey, destRowKey, emptyKeyValueCFPtr, coveredColumnsMap, destEmptyKeyValueRef,
      destWALDisabled, srcImmutableStorageScheme, destImmutableStorageScheme, destEncodingScheme,
      srcEncodingScheme, verified, maintainer, isVectorUnchanged, null);
  }

  public static Put buildUpdateMutation(KeyValueBuilder kvBuilder, ValueGetter valueGetter,
    ImmutableBytesWritable dataRowKeyPtr, long ts, byte[] regionStartKey, byte[] regionEndKey,
    byte[] destRowKey, ImmutableBytesPtr emptyKeyValueCFPtr,
    Map<ColumnReference, ColumnReference> coveredColumnsMap, ColumnReference destEmptyKeyValueRef,
    boolean destWALDisabled, ImmutableStorageScheme srcImmutableStorageScheme,
    ImmutableStorageScheme destImmutableStorageScheme, QualifierEncodingScheme destEncodingScheme,
    QualifierEncodingScheme srcEncodingScheme, boolean verified, IndexMaintainer maintainer,
    boolean isVectorUnchanged, ImmutableBytesWritable vectorValue) throws IOException {
    // Determine whether a functional vector column must be evaluated and stored.
    boolean hasFunctionalVector = maintainer != null && maintainer.functionalVectorColRef != null;
    Set<ColumnReference> coveredColumns = coveredColumnsMap.keySet();
    Put put = null;
    // New row being inserted: add the empty key value
    ImmutableBytesWritable latestValue = null;
    if (
      valueGetter == null || coveredColumns.isEmpty()
        || (latestValue = valueGetter.getLatestValue(destEmptyKeyValueRef, ts)) == null
        || latestValue == ValueGetter.HIDDEN_BY_DELETE
    ) {
      // We need to track whether or not our empty key value is hidden by a Delete Family marker at
      // the same timestamp.
      // If it is, these Puts will be masked so should not be emitted.
      if (latestValue == ValueGetter.HIDDEN_BY_DELETE) {
        return null;
      }
      put = new Put(destRowKey);
      // add the keyvalue for the empty row
      put.add(kvBuilder.buildPut(new ImmutableBytesPtr(destRowKey), emptyKeyValueCFPtr,
        destEmptyKeyValueRef.getQualifierWritable(), ts,
        verified
          ? QueryConstants.VERIFIED_BYTES_PTR
          : QueryConstants.EMPTY_COLUMN_VALUE_BYTES_PTR));
      put.setDurability(!destWALDisabled ? Durability.USE_DEFAULT : Durability.SKIP_WAL);
    }

    ImmutableBytesPtr rowKey = new ImmutableBytesPtr(destRowKey);
    if (destImmutableStorageScheme != ImmutableStorageScheme.ONE_CELL_PER_COLUMN) {
      // map from index column family to list of pair of index column and data column (for covered
      // columns)
      Map<ImmutableBytesPtr, List<Pair<ColumnReference, ColumnReference>>> familyToColListMap =
        Maps.newHashMap();
      for (ColumnReference ref : coveredColumns) {
        ColumnReference indexColRef = coveredColumnsMap.get(ref);
        ImmutableBytesPtr cf = new ImmutableBytesPtr(indexColRef.getFamily());
        if (!familyToColListMap.containsKey(cf)) {
          familyToColListMap.put(cf, Lists.<Pair<ColumnReference, ColumnReference>> newArrayList());
        }
        familyToColListMap.get(cf).add(Pair.newPair(indexColRef, ref));
      }
      if (hasFunctionalVector) {
        ImmutableBytesPtr cf = maintainer.functionalVectorColRef.getFamilyWritable();
        if (!familyToColListMap.containsKey(cf)) {
          familyToColListMap.put(cf, Lists.<Pair<ColumnReference, ColumnReference>> newArrayList());
        }
      }
      // iterate over each column family and create a byte[] containing all the columns
      for (Entry<ImmutableBytesPtr,
        List<Pair<ColumnReference, ColumnReference>>> entry : familyToColListMap.entrySet()) {
        byte[] columnFamily = entry.getKey().copyBytesIfNecessary();
        List<Pair<ColumnReference, ColumnReference>> colRefPairs = entry.getValue();
        int maxEncodedColumnQualifier = Integer.MIN_VALUE;
        // find the max col qualifier
        for (Pair<ColumnReference, ColumnReference> colRefPair : colRefPairs) {
          maxEncodedColumnQualifier = Math.max(maxEncodedColumnQualifier,
            destEncodingScheme.decode(colRefPair.getFirst().getQualifier()));
        }
        // In single-cell encoding schemes, include the functional vector in the composite cell.
        boolean functionalVectorInFamily = hasFunctionalVector
          && maintainer.functionalVectorColRef.getFamilyWritable().equals(entry.getKey());
        if (functionalVectorInFamily) {
          maxEncodedColumnQualifier = Math.max(maxEncodedColumnQualifier,
            destEncodingScheme.decode(maintainer.functionalVectorColRef.getQualifier()));
        }
        Expression[] colValues =
          EncodedColumnsUtil.createColumnExpressionArray(maxEncodedColumnQualifier);
        // set the values of the columns
        for (Pair<ColumnReference, ColumnReference> colRefPair : colRefPairs) {
          ColumnReference indexColRef = colRefPair.getFirst();
          ColumnReference dataColRef = colRefPair.getSecond();
          byte[] value = null;
          if (maintainer != null && maintainer.isIndexedVectorColumn(dataColRef)) {
            value = getIndexedVectorBytes(maintainer, dataColRef, valueGetter, ts, vectorValue);
          } else if (
            srcImmutableStorageScheme == ImmutableStorageScheme.SINGLE_CELL_ARRAY_WITH_OFFSETS
          ) {
            Expression expression = new SingleCellColumnExpression(new PDatum() {
              @Override
              public boolean isNullable() {
                return false;
              }

              @Override
              public SortOrder getSortOrder() {
                return null;
              }

              @Override
              public Integer getScale() {
                return null;
              }

              @Override
              public Integer getMaxLength() {
                return null;
              }

              @Override
              public PDataType getDataType() {
                return null;
              }
            }, dataColRef.getFamily(), dataColRef.getQualifier(), destEncodingScheme,
              destImmutableStorageScheme);
            ImmutableBytesPtr ptr = new ImmutableBytesPtr();
            expression.evaluate(new ValueGetterTuple(valueGetter, ts), ptr);
            value = ptr.copyBytesIfNecessary();
            if (
              value != null && maintainer != null && maintainer.isCoveredVectorColumn(dataColRef)
            ) {
              byte[] transcoded = new byte[value.length];
              if (maintainer.isDoubleVector(dataColRef)) {
                PVectorDouble.transcodeBytes(value, 0, value.length,
                  maintainer.getVectorSortOrder(dataColRef), transcoded, 0, SortOrder.ASC);
              } else {
                PVectorFloat.transcodeBytes(value, 0, value.length,
                  maintainer.getVectorSortOrder(dataColRef), transcoded, 0, SortOrder.ASC);
              }
              value = transcoded;
            }
          } else {
            // Data table is ONE_CELL_PER_COLUMN. Get the col value.
            ImmutableBytesWritable dataValue = valueGetter.getLatestValue(dataColRef, ts);
            if (dataValue != null && dataValue != ValueGetter.HIDDEN_BY_DELETE) {
              if (maintainer != null && maintainer.isCoveredVectorColumn(dataColRef)) {
                byte[] transcoded = new byte[dataValue.getLength()];
                if (maintainer.isDoubleVector(dataColRef)) {
                  PVectorDouble.transcodeBytes(dataValue.get(), dataValue.getOffset(),
                    dataValue.getLength(), maintainer.getVectorSortOrder(dataColRef), transcoded, 0,
                    SortOrder.ASC);
                } else {
                  PVectorFloat.transcodeBytes(dataValue.get(), dataValue.getOffset(),
                    dataValue.getLength(), maintainer.getVectorSortOrder(dataColRef), transcoded, 0,
                    SortOrder.ASC);
                }
                value = transcoded;
              } else {
                value = dataValue.copyBytes();
              }
            }
          }
          if (value != null) {
            int indexArrayPos = destEncodingScheme.decode(indexColRef.getQualifier())
              - QueryConstants.ENCODED_CQ_COUNTER_INITIAL_VALUE + 1;
            colValues[indexArrayPos] = new LiteralExpression(value);
          }
        }
        if (functionalVectorInFamily) {
          byte[] funcVecBytes = getFunctionalVectorBytes(maintainer, valueGetter, ts, vectorValue);
          if (funcVecBytes != null) {
            int indexArrayPos =
              destEncodingScheme.decode(maintainer.functionalVectorColRef.getQualifier())
                - QueryConstants.ENCODED_CQ_COUNTER_INITIAL_VALUE + 1;
            colValues[indexArrayPos] = new LiteralExpression(funcVecBytes);
          }
        }

        List<Expression> children = Arrays.asList(colValues);
        // we use SingleCellConstructorExpression to serialize multiple columns into a single byte[]
        SingleCellConstructorExpression singleCellConstructorExpression =
          new SingleCellConstructorExpression(destImmutableStorageScheme, children);
        ImmutableBytesWritable ptr = new ImmutableBytesWritable();
        singleCellConstructorExpression.evaluate(new BaseTuple() {
        }, ptr);
        if (put == null) {
          put = new Put(destRowKey);
          put.setDurability(!destWALDisabled ? Durability.USE_DEFAULT : Durability.SKIP_WAL);
        }
        ImmutableBytesPtr colFamilyPtr = new ImmutableBytesPtr(columnFamily);
        // this is a little bit of extra work for installations that are running <0.94.14, but that
        // should be rare and is a short-term set of wrappers - it shouldn't kill GC
        put.add(kvBuilder.buildPut(rowKey, colFamilyPtr,
          QueryConstants.SINGLE_KEYVALUE_COLUMN_QUALIFIER_BYTES_PTR, ts, ptr));
      }
    } else {
      if (srcImmutableStorageScheme == destImmutableStorageScheme) { // both ONE_CELL
        for (ColumnReference ref : coveredColumns) {
          ColumnReference indexColRef = coveredColumnsMap.get(ref);
          ImmutableBytesPtr cq = indexColRef.getQualifierWritable();
          ImmutableBytesPtr cf = indexColRef.getFamilyWritable();
          ImmutableBytesWritable value = valueGetter.getLatestValue(ref, ts);
          if (value != null && value != ValueGetter.HIDDEN_BY_DELETE) {
            if (maintainer != null && maintainer.isCoveredVectorColumn(ref)) {
              if (isVectorUnchanged && maintainer.isIndexedVectorColumn(ref)) {
                // Omit unchanged indexed vector column mutation
                continue;
              }
              byte[] transcoded = new byte[value.getLength()];
              if (maintainer.isDoubleVector(ref)) {
                PVectorDouble.transcodeBytes(value.get(), value.getOffset(), value.getLength(),
                  maintainer.getVectorSortOrder(ref), transcoded, 0, SortOrder.ASC);
              } else {
                PVectorFloat.transcodeBytes(value.get(), value.getOffset(), value.getLength(),
                  maintainer.getVectorSortOrder(ref), transcoded, 0, SortOrder.ASC);
              }
              value = new ImmutableBytesPtr(transcoded);
            }
            if (put == null) {
              put = new Put(destRowKey);
              put.setDurability(!destWALDisabled ? Durability.USE_DEFAULT : Durability.SKIP_WAL);
            }
            put.add(kvBuilder.buildPut(rowKey, cf, cq, ts, value));
          }
        }
      } else {
        // Src is SINGLE_CELL, destination is ONE_CELL
        Map<ImmutableBytesPtr, List<Pair<ColumnReference, ColumnReference>>> familyToColListMap =
          Maps.newHashMap();
        for (ColumnReference ref : coveredColumns) {
          ColumnReference indexColRef = coveredColumnsMap.get(ref);
          ImmutableBytesPtr cf = new ImmutableBytesPtr(indexColRef.getFamily());
          if (!familyToColListMap.containsKey(cf)) {
            familyToColListMap.put(cf,
              Lists.<Pair<ColumnReference, ColumnReference>> newArrayList());
          }
          familyToColListMap.get(cf).add(Pair.newPair(indexColRef, ref));
        }
        // iterate over each column family and create a byte[] containing all the columns
        for (Entry<ImmutableBytesPtr,
          List<Pair<ColumnReference, ColumnReference>>> entry : familyToColListMap.entrySet()) {
          byte[] columnFamily = entry.getKey().copyBytesIfNecessary();
          List<Pair<ColumnReference, ColumnReference>> colRefPairs = entry.getValue();
          int maxEncodedColumnQualifier = Integer.MIN_VALUE;
          // find the max col qualifier
          for (Pair<ColumnReference, ColumnReference> colRefPair : colRefPairs) {
            maxEncodedColumnQualifier = Math.max(maxEncodedColumnQualifier,
              srcEncodingScheme.decode(colRefPair.getSecond().getQualifier()));
          }
          // set the values of the columns
          for (Pair<ColumnReference, ColumnReference> colRefPair : colRefPairs) {
            ColumnReference indexColRef = colRefPair.getFirst();
            ColumnReference dataColRef = colRefPair.getSecond();
            byte[] valueBytes = null;
            Expression expression = new SingleCellColumnExpression(new PDatum() {
              @Override
              public boolean isNullable() {
                return false;
              }

              @Override
              public SortOrder getSortOrder() {
                return null;
              }

              @Override
              public Integer getScale() {
                return null;
              }

              @Override
              public Integer getMaxLength() {
                return null;
              }

              @Override
              public PDataType getDataType() {
                return null;
              }
            }, dataColRef.getFamily(), dataColRef.getQualifier(), srcEncodingScheme,
              srcImmutableStorageScheme);
            ImmutableBytesPtr ptr = new ImmutableBytesPtr();
            expression.evaluate(new ValueGetterTuple(valueGetter, ts), ptr);
            valueBytes = ptr.copyBytesIfNecessary();

            if (valueBytes != null) {
              if (maintainer != null && maintainer.isCoveredVectorColumn(dataColRef)) {
                if (isVectorUnchanged && maintainer.isIndexedVectorColumn(dataColRef)) {
                  continue;
                }
                byte[] transcoded = new byte[valueBytes.length];
                if (maintainer.isDoubleVector(dataColRef)) {
                  PVectorDouble.transcodeBytes(valueBytes, 0, valueBytes.length,
                    maintainer.getVectorSortOrder(dataColRef), transcoded, 0, SortOrder.ASC);
                } else {
                  PVectorFloat.transcodeBytes(valueBytes, 0, valueBytes.length,
                    maintainer.getVectorSortOrder(dataColRef), transcoded, 0, SortOrder.ASC);
                }
                valueBytes = transcoded;
              }
              ImmutableBytesPtr cq = indexColRef.getQualifierWritable();
              ImmutableBytesPtr cf = indexColRef.getFamilyWritable();
              if (put == null) {
                put = new Put(destRowKey);
                put.setDurability(!destWALDisabled ? Durability.USE_DEFAULT : Durability.SKIP_WAL);
              }
              put.add(
                kvBuilder.buildPut(rowKey, cf, cq, ts, new ImmutableBytesWritable(valueBytes)));
            }
          }
        }
      }
      // Retain the existing functional vector cell when the vector value is unchanged.
      if (hasFunctionalVector && !isVectorUnchanged) {
        byte[] funcVecBytes = getFunctionalVectorBytes(maintainer, valueGetter, ts, vectorValue);
        if (funcVecBytes != null) {
          if (put == null) {
            put = new Put(destRowKey);
            put.setDurability(!destWALDisabled ? Durability.USE_DEFAULT : Durability.SKIP_WAL);
          }
          put.add(kvBuilder.buildPut(rowKey, maintainer.functionalVectorColRef.getFamilyWritable(),
            maintainer.functionalVectorColRef.getQualifierWritable(), ts,
            new ImmutableBytesPtr(funcVecBytes)));
        }
      }
    }
    return put;
  }

  /**
   * For mutable covered indexes, an index update is a full update. However, if some included
   * columns are set to null in the upsert statement we need to write a DeleteColumn cell to such
   * columns.
   * @param indexUpdate Put mutation updating index which includes at a minimum the empty column
   * @param ts          The update timestamp
   * @return Delete mutation with DeleteColumn cells for all covered columns that are missing in the
   *         indexUpdate Put mutation
   */
  public Delete buildDeleteColumnMutation(Put indexUpdate, long ts) throws IOException {
    if (getIndexStorageScheme() == ImmutableStorageScheme.SINGLE_CELL_ARRAY_WITH_OFFSETS) {
      // for single cell storage format no need to build a delete column mutation
      return null;
    }
    if (coveredColumnsMap == null || coveredColumnsMap.isEmpty()) {
      // no covered columns in the index no need to build a delete mutation
      return null;
    }
    int colsSet =
      indexUpdate.getFamilyCellMap().values().stream().mapToInt(elem -> elem.size()).sum();
    int fullUpdateCols = coveredColumnsMap.size() + 1; // add 1 for the empty column
    if (
      functionalVectorColRef != null && indexUpdate.has(functionalVectorColRef.getFamily(),
        functionalVectorColRef.getQualifier())
    ) {
      // Account for the functional vector column contained within the same mutation.
      fullUpdateCols++;
    }
    if (fullUpdateCols == colsSet) {
      // Index row update is always a full update except when some columns are explicitly
      // set to null. Do a quick size check to determine if some covered columns are being
      // set to null or not.
      return null;
    }
    ImmutableBytesPtr rowKey = new ImmutableBytesPtr(indexUpdate.getRow());
    Delete delete = new Delete(rowKey.get());
    for (Entry<ColumnReference, ColumnReference> coveredCol : coveredColumnsMap.entrySet()) {
      ColumnReference indexCol = coveredCol.getValue();
      if (!indexUpdate.has(indexCol.getFamily(), indexCol.getQualifier())) {
        if (isIndexedVectorColumn(coveredCol.getKey())) {
          // Unchanged indexed vector columns are omitted from Put mutations and must not be deleted
          continue;
        }
        KeyValue kv = GenericKeyValueBuilder.INSTANCE.buildDeleteColumns(rowKey,
          indexCol.getFamilyWritable(), indexCol.getQualifierWritable(), ts);
        delete.add(kv);
      }
    }
    // Prevent emitting an empty Delete mutation that would delete the entire row
    return delete.isEmpty() ? null : delete;
  }

  public enum DeleteType {
    SINGLE_VERSION,
    ALL_VERSIONS
  };

  private DeleteType getDeleteTypeOrNull(Collection<? extends Cell> pendingUpdates) {
    return getDeleteTypeOrNull(pendingUpdates, this.nDataCFs);
  }

  private DeleteType getDeleteTypeOrNull(Collection<? extends Cell> pendingUpdates, int nCFs) {
    int nDeleteCF = 0;
    int nDeleteVersionCF = 0;
    for (Cell kv : pendingUpdates) {
      if (kv.getType() == Cell.Type.DeleteFamilyVersion) {
        nDeleteVersionCF++;
      } else if (
        kv.getType() == Cell.Type.DeleteFamily
          // Since we don't include the index rows in the change set for txn tables, we need to
          // detect row deletes that have transformed by TransactionProcessor
          || TransactionUtil.isDeleteFamily(kv)
      ) {
        nDeleteCF++;
      }
    }
    // This is what a delete looks like on the server side for mutable indexing...
    // Should all be one or the other for DeleteFamily versus DeleteFamilyVersion, but just in case
    // not
    DeleteType deleteType = null;
    if (nDeleteVersionCF > 0 && nDeleteVersionCF >= nCFs) {
      deleteType = DeleteType.SINGLE_VERSION;
    } else {
      int nDelete = nDeleteCF + nDeleteVersionCF;
      if (nDelete > 0 && nDelete >= nCFs) {
        deleteType = DeleteType.ALL_VERSIONS;
      }
    }
    return deleteType;
  }

  public boolean isRowDeleted(Collection<? extends Cell> pendingUpdates) {
    return getDeleteTypeOrNull(pendingUpdates) != null;
  }

  public boolean isRowDeleted(Mutation m) {
    if (m.getFamilyCellMap().size() < this.nDataCFs) {
      return false;
    }
    for (List<Cell> cells : m.getFamilyCellMap().values()) {
      if (getDeleteTypeOrNull(cells, 1) == null) { // Checking CFs one by one
        return false;
      }
    }
    return true;
  }

  private boolean hasIndexedColumnChanged(ValueGetter oldState,
    Collection<? extends Cell> pendingUpdates, long ts) throws IOException {
    if (pendingUpdates.isEmpty()) {
      return false;
    }
    Map<ColumnReference, Cell> newState = Maps.newHashMapWithExpectedSize(pendingUpdates.size());
    for (Cell kv : pendingUpdates) {
      newState.put(new ColumnReference(CellUtil.cloneFamily(kv), CellUtil.cloneQualifier(kv)), kv);
    }
    for (ColumnReference ref : indexedColumns) {
      Cell newValue = newState.get(ref);
      if (newValue != null) { // Indexed column has potentially changed
        ImmutableBytesWritable oldValue = oldState.getLatestValue(ref, ts);
        boolean newValueSetAsNull =
          (newValue.getType() == Cell.Type.DeleteColumn || newValue.getType() == Cell.Type.Delete
            || CellUtil.matchingValue(newValue, HConstants.EMPTY_BYTE_ARRAY));
        boolean oldValueSetAsNull = oldValue == null || oldValue.getLength() == 0;
        // If the new column value has to be set as null and the older value is null too,
        // then just skip to the next indexed column.
        if (newValueSetAsNull && oldValueSetAsNull) {
          continue;
        }
        if (oldValueSetAsNull || newValueSetAsNull) {
          return true;
        }
        // If the old value is different than the new value, the index row needs to be deleted
        if (
          Bytes.compareTo(oldValue.get(), oldValue.getOffset(), oldValue.getLength(),
            newValue.getValueArray(), newValue.getValueOffset(), newValue.getValueLength()) != 0
        ) {
          return true;
        }
      }
    }
    return false;
  }

  public Delete buildRowDeleteMutation(byte[] indexRowKey, DeleteType deleteType, long ts) {
    byte[] emptyCF = emptyKeyValueCFPtr.copyBytesIfNecessary();
    Delete delete = new Delete(indexRowKey);

    for (ColumnReference ref : getCoveredColumns()) {
      ColumnReference indexColumn = coveredColumnsMap.get(ref);
      // If table delete was single version, then index delete should be as well
      if (deleteType == DeleteType.SINGLE_VERSION) {
        delete.addFamilyVersion(indexColumn.getFamily(), ts);
      } else {
        delete.addFamily(indexColumn.getFamily(), ts);
      }
    }
    if (deleteType == DeleteType.SINGLE_VERSION) {
      delete.addFamilyVersion(emptyCF, ts);
    } else {
      delete.addFamily(emptyCF, ts);
    }
    delete.setDurability(!indexWALDisabled ? Durability.USE_DEFAULT : Durability.SKIP_WAL);
    return delete;
  }

  /**
   * Used for immutable indexes that only index PK column values. In that case, we can handle a data
   * row deletion, since we can build the corresponding index row key.
   */
  public Delete buildDeleteMutation(KeyValueBuilder kvBuilder, ImmutableBytesWritable dataRowKeyPtr,
    long ts, byte[] encodedRegionName) throws IOException {
    return buildDeleteMutation(kvBuilder, null, dataRowKeyPtr, Collections.<Cell> emptyList(), ts,
      null, null, encodedRegionName);
  }

  public Delete buildDeleteMutation(KeyValueBuilder kvBuilder, ValueGetter oldState,
    ImmutableBytesWritable dataRowKeyPtr, Collection<Cell> pendingUpdates, long ts,
    byte[] regionStartKey, byte[] regionEndKey) throws IOException {
    return buildDeleteMutation(kvBuilder, oldState, dataRowKeyPtr, pendingUpdates, ts,
      regionStartKey, regionEndKey, null);
  }

  public Delete buildDeleteMutation(KeyValueBuilder kvBuilder, ValueGetter oldState,
    ImmutableBytesWritable dataRowKeyPtr, Collection<Cell> pendingUpdates, long ts,
    byte[] regionStartKey, byte[] regionEndKey, byte[] encodedRegionName) throws IOException {
    byte[] indexRowKey = this.buildRowKey(oldState, dataRowKeyPtr, regionStartKey, regionEndKey, ts,
      encodedRegionName);
    if (indexRowKey == null) {
      return null;
    }
    // Delete the entire row if any of the indexed columns changed
    DeleteType deleteType = null;
    if (
      oldState == null || (deleteType = getDeleteTypeOrNull(pendingUpdates)) != null
        || hasIndexedColumnChanged(oldState, pendingUpdates, ts)
    ) { // Deleting the entire row
      return buildRowDeleteMutation(indexRowKey, deleteType, ts);
    }
    Delete delete = null;
    Set<ColumnReference> dataTableColRefs = coveredColumnsMap.keySet();
    // Delete columns for missing key values
    for (Cell kv : pendingUpdates) {
      if (kv.getType() != Cell.Type.Put) {
        ColumnReference ref =
          new ColumnReference(kv.getFamilyArray(), kv.getFamilyOffset(), kv.getFamilyLength(),
            kv.getQualifierArray(), kv.getQualifierOffset(), kv.getQualifierLength());
        if (dataTableColRefs.contains(ref)) {
          if (delete == null) {
            delete = new Delete(indexRowKey);
            delete.setDurability(!indexWALDisabled ? Durability.USE_DEFAULT : Durability.SKIP_WAL);
          }
          ColumnReference indexColumn = coveredColumnsMap.get(ref);
          // If point delete for data table, then use point delete for index as well
          if (kv.getType() == Cell.Type.Delete) {
            delete.addColumn(indexColumn.getFamily(), indexColumn.getQualifier(), ts);
          } else {
            delete.addColumns(indexColumn.getFamily(), indexColumn.getQualifier(), ts);
          }
        }
      }
    }
    return delete;
  }

  public byte[] getIndexTableName() {
    return indexTableName;
  }

  public Set<ColumnReference> getCoveredColumns() {
    return coveredColumnsMap.keySet();
  }

  public Set<ColumnReference> getAllColumns() {
    return allColumns;
  }

  private void addColumnRefForScan(Set<ColumnReference> from, Set<ColumnReference> to) {
    for (ColumnReference colRef : from) {
      if (getDataImmutableStorageScheme() == ImmutableStorageScheme.ONE_CELL_PER_COLUMN) {
        to.add(colRef);
      } else {
        to.add(new ColumnReference(colRef.getFamily(),
          QueryConstants.SINGLE_KEYVALUE_COLUMN_QUALIFIER_BYTES));
      }
    }
  }

  public Set<ColumnReference> getAllColumnsForDataTable() {
    Set<ColumnReference> result = Sets.newLinkedHashSetWithExpectedSize(indexedExpressions.size()
      + coveredColumnsMap.size() + (indexWhereColumns == null ? 0 : indexWhereColumns.size()));
    addColumnRefForScan(indexedColumns, result);
    addColumnRefForScan(coveredColumnsMap.keySet(), result);
    if (indexWhereColumns != null) {
      addColumnRefForScan(indexWhereColumns, result);
    }
    return result;
  }

  public ImmutableBytesPtr getEmptyKeyValueFamily() {
    // Since the metadata of an index table will never change,
    // we can infer this based on the family of the first covered column
    // If if there are no covered columns, we know it's our default name
    return emptyKeyValueCFPtr;
  }

  /**
   * The logical index name. For global indexes on base tables this will be the same as the physical
   * index table name (unless namespaces are enabled, then . gets replaced with : for the physical
   * table name). For view indexes, the logical and physical names will be different because all
   * view indexes of a base table are stored in the same physical table
   * @return The logical index name
   */
  public String getLogicalIndexName() {
    return logicalIndexName;
  }

  @Deprecated // Only called by code older than our 4.10 release
  @Override
  public void readFields(DataInput input) throws IOException {
    int encodedIndexSaltBucketsAndMultiTenant = WritableUtils.readVInt(input);
    isMultiTenant = encodedIndexSaltBucketsAndMultiTenant < 0;
    nIndexSaltBuckets = Math.abs(encodedIndexSaltBucketsAndMultiTenant) - 1;
    int encodedIndexedColumnsAndViewId = WritableUtils.readVInt(input);
    boolean hasViewIndexId = encodedIndexedColumnsAndViewId < 0;
    if (hasViewIndexId) {
      // Fixed length
      // Use legacy viewIndexIdType for clients older than 4.10 release
      viewIndexId = new byte[MetaDataUtil.getLegacyViewIndexIdDataType().getByteSize()];
      viewIndexIdType = MetaDataUtil.getLegacyViewIndexIdDataType();
      input.readFully(viewIndexId);
    }
    int nIndexedColumns = Math.abs(encodedIndexedColumnsAndViewId) - 1;
    indexedColumns = Sets.newLinkedHashSetWithExpectedSize(nIndexedColumns);
    for (int i = 0; i < nIndexedColumns; i++) {
      byte[] cf = Bytes.readByteArray(input);
      byte[] cq = Bytes.readByteArray(input);
      indexedColumns.add(new ColumnReference(cf, cq));
    }
    indexedColumnTypes = Lists.newArrayListWithExpectedSize(nIndexedColumns);
    for (int i = 0; i < nIndexedColumns; i++) {
      PDataType type = PDataType.values()[WritableUtils.readVInt(input)];
      indexedColumnTypes.add(type);
    }
    int encodedCoveredolumnsAndLocalIndex = WritableUtils.readVInt(input);
    isLocalIndex = encodedCoveredolumnsAndLocalIndex < 0;
    int nCoveredColumns = Math.abs(encodedCoveredolumnsAndLocalIndex) - 1;
    coveredColumnsMap = Maps.newHashMapWithExpectedSize(nCoveredColumns);
    for (int i = 0; i < nCoveredColumns; i++) {
      byte[] dataTableCf = Bytes.readByteArray(input);
      byte[] dataTableCq = Bytes.readByteArray(input);
      ColumnReference dataTableRef = new ColumnReference(dataTableCf, dataTableCq);
      byte[] indexTableCf =
        isLocalIndex ? IndexUtil.getLocalIndexColumnFamily(dataTableCf) : dataTableCf;
      byte[] indexTableCq = IndexUtil.getIndexColumnName(dataTableCf, dataTableCq);
      ColumnReference indexTableRef = new ColumnReference(indexTableCf, indexTableCq);
      coveredColumnsMap.put(dataTableRef, indexTableRef);
    }
    // Hack to serialize whether the index row key is optimizable
    int len = WritableUtils.readVInt(input);
    if (len < 0) {
      rowKeyOrderOptimizable = false;
      len *= -1;
    } else {
      rowKeyOrderOptimizable = true;
    }
    indexTableName = new byte[len];
    input.readFully(indexTableName, 0, len);
    dataEmptyKeyValueCF = Bytes.readByteArray(input);
    len = WritableUtils.readVInt(input);
    // TODO remove this in the next major release
    boolean isNewClient = false;
    if (len < 0) {
      isNewClient = true;
      len = Math.abs(len);
    }
    byte[] emptyKeyValueCF = new byte[len];
    input.readFully(emptyKeyValueCF, 0, len);
    emptyKeyValueCFPtr = new ImmutableBytesPtr(emptyKeyValueCF);

    if (isNewClient) {
      int numIndexedExpressions = WritableUtils.readVInt(input);
      indexedExpressions = Lists.newArrayListWithExpectedSize(numIndexedExpressions);
      for (int i = 0; i < numIndexedExpressions; i++) {
        Expression expression =
          ExpressionType.values()[WritableUtils.readVInt(input)].newInstance();
        expression.readFields(input);
        indexedExpressions.add(expression);
      }
    } else {
      indexedExpressions = Lists.newArrayListWithExpectedSize(indexedColumns.size());
      Iterator<ColumnReference> colReferenceIter = indexedColumns.iterator();
      Iterator<PDataType> dataTypeIter = indexedColumnTypes.iterator();
      while (colReferenceIter.hasNext()) {
        ColumnReference colRef = colReferenceIter.next();
        final PDataType dataType = dataTypeIter.next();
        indexedExpressions.add(new KeyValueColumnExpression(new PDatum() {

          @Override
          public boolean isNullable() {
            return true;
          }

          @Override
          public SortOrder getSortOrder() {
            return SortOrder.getDefault();
          }

          @Override
          public Integer getScale() {
            return null;
          }

          @Override
          public Integer getMaxLength() {
            return null;
          }

          @Override
          public PDataType getDataType() {
            return dataType;
          }
        }, colRef.getFamily(), colRef.getQualifier()));
      }
    }

    rowKeyMetaData = newRowKeyMetaData();
    rowKeyMetaData.readFields(input);
    int nDataCFs = WritableUtils.readVInt(input);
    // Encode indexWALDisabled in nDataCFs
    indexWALDisabled = nDataCFs < 0;
    this.nDataCFs = Math.abs(nDataCFs) - 1;
    int encodedEstimatedIndexRowKeyBytesAndImmutableRows = WritableUtils.readVInt(input);
    this.immutableRows = encodedEstimatedIndexRowKeyBytesAndImmutableRows < 0;
    this.estimatedIndexRowKeyBytes = Math.abs(encodedEstimatedIndexRowKeyBytesAndImmutableRows);
    // Needed for backward compatibility. Clients older than 4.10 will have non-encoded tables.
    this.immutableStorageScheme = ImmutableStorageScheme.ONE_CELL_PER_COLUMN;
    this.encodingScheme = QualifierEncodingScheme.NON_ENCODED_QUALIFIERS;
    this.dataImmutableStorageScheme = ImmutableStorageScheme.ONE_CELL_PER_COLUMN;
    this.dataEncodingScheme = QualifierEncodingScheme.NON_ENCODED_QUALIFIERS;
    this.indexConsistency = null;
    initCachedState();
  }

  public static IndexMaintainer fromProto(ServerCachingProtos.IndexMaintainer proto,
    RowKeySchema dataTableRowKeySchema, boolean isDataTableSalted) throws IOException {
    IndexMaintainer maintainer = new IndexMaintainer(dataTableRowKeySchema, isDataTableSalted);
    maintainer.nIndexSaltBuckets = proto.getSaltBuckets();
    maintainer.isMultiTenant = proto.getIsMultiTenant();
    maintainer.viewIndexId = proto.hasViewIndexId() ? proto.getViewIndexId().toByteArray() : null;
    maintainer.viewIndexIdType = proto.hasViewIndexIdType()
      ? PDataType.fromTypeId(proto.getViewIndexIdType())
      : MetaDataUtil.getLegacyViewIndexIdDataType();
    List<ServerCachingProtos.ColumnReference> indexedColumnsList = proto.getIndexedColumnsList();
    maintainer.indexedColumns = new HashSet<ColumnReference>(indexedColumnsList.size());
    for (ServerCachingProtos.ColumnReference colRefFromProto : indexedColumnsList) {
      maintainer.indexedColumns.add(new ColumnReference(colRefFromProto.getFamily().toByteArray(),
        colRefFromProto.getQualifier().toByteArray()));
    }
    List<Integer> indexedColumnTypes = proto.getIndexedColumnTypeOrdinalList();
    maintainer.indexedColumnTypes = new ArrayList<PDataType>(indexedColumnTypes.size());
    for (Integer typeOrdinal : indexedColumnTypes) {
      maintainer.indexedColumnTypes.add(PDataType.values()[typeOrdinal]);
    }
    maintainer.indexTableName = proto.getIndexTableName().toByteArray();
    maintainer.indexDataColumnCount = dataTableRowKeySchema.getFieldCount();
    if (proto.getIndexDataColumnCount() != -1) {
      maintainer.indexDataColumnCount = proto.getIndexDataColumnCount();
    }
    maintainer.rowKeyOrderOptimizable = proto.getRowKeyOrderOptimizable();
    maintainer.dataEmptyKeyValueCF = proto.getDataTableEmptyKeyValueColFamily().toByteArray();
    ServerCachingProtos.ImmutableBytesWritable emptyKeyValueColFamily =
      proto.getEmptyKeyValueColFamily();
    maintainer.emptyKeyValueCFPtr =
      new ImmutableBytesPtr(emptyKeyValueColFamily.getByteArray().toByteArray(),
        emptyKeyValueColFamily.getOffset(), emptyKeyValueColFamily.getLength());
    maintainer.indexedExpressions = new ArrayList<>();
    try (ByteArrayInputStream stream =
      new ByteArrayInputStream(proto.getIndexedExpressions().toByteArray())) {
      DataInput input = new DataInputStream(stream);
      while (stream.available() > 0) {
        int expressionOrdinal = WritableUtils.readVInt(input);
        Expression expression = ExpressionType.values()[expressionOrdinal].newInstance();
        expression.readFields(input);
        maintainer.indexedExpressions.add(expression);
      }
    }
    maintainer.rowKeyMetaData = newRowKeyMetaData(maintainer, dataTableRowKeySchema,
      maintainer.indexedExpressions.size(), isDataTableSalted, maintainer.isMultiTenant);
    try (ByteArrayInputStream stream =
      new ByteArrayInputStream(proto.getRowKeyMetadata().toByteArray())) {
      DataInput input = new DataInputStream(stream);
      maintainer.rowKeyMetaData.readFields(input);
    }
    maintainer.nDataCFs = proto.getNumDataTableColFamilies();
    maintainer.indexWALDisabled = proto.getIndexWalDisabled();
    maintainer.estimatedIndexRowKeyBytes = proto.getIndexRowKeyByteSize();
    maintainer.immutableRows = proto.getImmutable();
    List<ColumnInfo> indexedColumnInfoList = proto.getIndexedColumnInfoList();
    maintainer.indexedColumnsInfo = Sets.newHashSet();
    for (ColumnInfo info : indexedColumnInfoList) {
      maintainer.indexedColumnsInfo.add(new Pair<>(info.getFamilyName(), info.getColumnName()));
    }
    // proto doesn't support single byte so need an explicit cast here
    maintainer.encodingScheme =
      PTable.QualifierEncodingScheme.fromSerializedValue((byte) proto.getEncodingScheme());
    maintainer.immutableStorageScheme =
      PTable.ImmutableStorageScheme.fromSerializedValue((byte) proto.getImmutableStorageScheme());
    maintainer.dataEncodingScheme =
      PTable.QualifierEncodingScheme.fromSerializedValue((byte) proto.getDataEncodingScheme());
    maintainer.dataImmutableStorageScheme = PTable.ImmutableStorageScheme
      .fromSerializedValue((byte) proto.getDataImmutableStorageScheme());
    maintainer.isLocalIndex = proto.getIsLocalIndex();
    if (proto.hasParentTableType()) {
      maintainer.parentTableType = PTableType.fromValue(proto.getParentTableType());
    }

    List<ServerCachingProtos.ColumnReference> dataTableColRefsForCoveredColumnsList =
      proto.getDataTableColRefForCoveredColumnsList();
    List<ServerCachingProtos.ColumnReference> indexTableColRefsForCoveredColumnsList =
      proto.getIndexTableColRefForCoveredColumnsList();
    maintainer.coveredColumnsMap =
      Maps.newHashMapWithExpectedSize(dataTableColRefsForCoveredColumnsList.size());
    boolean encodedColumnNames = maintainer.encodingScheme != NON_ENCODED_QUALIFIERS;
    Iterator<ServerCachingProtos.ColumnReference> indexTableColRefItr =
      indexTableColRefsForCoveredColumnsList.iterator();
    for (ServerCachingProtos.ColumnReference colRefFromProto : dataTableColRefsForCoveredColumnsList) {
      ColumnReference dataTableColRef = new ColumnReference(
        colRefFromProto.getFamily().toByteArray(), colRefFromProto.getQualifier().toByteArray());
      ColumnReference indexTableColRef;
      if (encodedColumnNames) {
        ServerCachingProtos.ColumnReference fromProto = indexTableColRefItr.next();
        indexTableColRef = new ColumnReference(fromProto.getFamily().toByteArray(),
          fromProto.getQualifier().toByteArray());
      } else {
        byte[] cq =
          IndexUtil.getIndexColumnName(dataTableColRef.getFamily(), dataTableColRef.getQualifier());
        byte[] cf = maintainer.isLocalIndex
          ? IndexUtil.getLocalIndexColumnFamily(dataTableColRef.getFamily())
          : dataTableColRef.getFamily();
        indexTableColRef = new ColumnReference(cf, cq);
      }
      maintainer.coveredColumnsMap.put(dataTableColRef, indexTableColRef);
    }
    maintainer.logicalIndexName = proto.getLogicalIndexName();
    if (proto.hasIsUncovered()) {
      maintainer.isUncovered = proto.getIsUncovered();
    } else {
      maintainer.isUncovered = false;
    }
    if (proto.hasIndexWhere()) {
      try (ByteArrayInputStream stream =
        new ByteArrayInputStream(proto.getIndexWhere().toByteArray())) {
        DataInput input = new DataInputStream(stream);
        int expressionOrdinal = WritableUtils.readVInt(input);
        Expression expression = ExpressionType.values()[expressionOrdinal].newInstance();
        expression.readFields(input);
        maintainer.indexWhere = expression;
        List<ServerCachingProtos.ColumnReference> indexWhereColumnsList =
          proto.getIndexWhereColumnsList();
        maintainer.indexWhereColumns = new HashSet<>(indexWhereColumnsList.size());
        for (ServerCachingProtos.ColumnReference colRefFromProto : indexWhereColumnsList) {
          maintainer.indexWhereColumns
            .add(new ColumnReference(colRefFromProto.getFamily().toByteArray(),
              colRefFromProto.getQualifier().toByteArray()));
        }
      }
    } else {
      maintainer.indexWhere = null;
      maintainer.indexWhereColumns = null;
    }
    if (proto.hasIsCDCIndex()) {
      maintainer.isCDCIndex = proto.getIsCDCIndex();
    } else {
      maintainer.isCDCIndex = false;
    }
    if (proto.hasIndexConsistency()) {
      maintainer.indexConsistency = IndexConsistency.valueOf(proto.getIndexConsistency());
    } else {
      maintainer.indexConsistency = null;
    }
    maintainer.nDataTableSaltBuckets =
      proto.hasDataTableSaltBuckets() ? proto.getDataTableSaltBuckets() : -1;
    maintainer.vectorAlgorithm = proto.hasVectorAlgorithm() ? proto.getVectorAlgorithm() : null;
    maintainer.distanceMetric = proto.hasDistanceMetric() ? proto.getDistanceMetric() : null;
    maintainer.vectorDimension = proto.hasVectorDimension() ? proto.getVectorDimension() : null;
    maintainer.centroidGeneration =
      proto.hasCentroidGeneration() ? proto.getCentroidGeneration() : null;
    if (proto.getCoveredVectorColumnsCount() > 0) {
      maintainer.coveredVectorColumnTypes =
        Maps.newHashMapWithExpectedSize(proto.getCoveredVectorColumnsCount());
      for (int i = 0; i < proto.getCoveredVectorColumnsCount(); i++) {
        ServerCachingProtos.ColumnReference colRefFromProto = proto.getCoveredVectorColumns(i);
        ColumnReference ref = new ColumnReference(colRefFromProto.getFamily().toByteArray(),
          colRefFromProto.getQualifier().toByteArray());
        PDataType type = PDataType.values()[proto.getCoveredVectorColumnTypeOrdinal(i)];
        maintainer.coveredVectorColumnTypes.put(ref, type);
      }
    } else {
      maintainer.coveredVectorColumnTypes = Collections.emptyMap();
    }
    if (proto.hasFunctionalVectorColRef()) {
      ServerCachingProtos.ColumnReference colRef = proto.getFunctionalVectorColRef();
      maintainer.functionalVectorColRef =
        new ColumnReference(colRef.getFamily().toByteArray(), colRef.getQualifier().toByteArray());
    } else {
      maintainer.functionalVectorColRef = null;
    }
    maintainer.initCachedState();
    return maintainer;
  }

  @Deprecated // Only called by code older than our 4.10 release
  @Override
  public void write(DataOutput output) throws IOException {
    // Encode nIndexSaltBuckets and isMultiTenant together
    WritableUtils.writeVInt(output, (nIndexSaltBuckets + 1) * (isMultiTenant ? -1 : 1));
    // Encode indexedColumns.size() and whether or not there's a viewIndexId
    WritableUtils.writeVInt(output, (indexedColumns.size() + 1) * (viewIndexId != null ? -1 : 1));
    if (viewIndexId != null) {
      output.write(viewIndexId);
    }
    for (ColumnReference ref : indexedColumns) {
      Bytes.writeByteArray(output, ref.getFamily());
      Bytes.writeByteArray(output, ref.getQualifier());
    }
    // TODO remove indexedColumnTypes in the next major release
    for (int i = 0; i < indexedColumnTypes.size(); i++) {
      PDataType type = indexedColumnTypes.get(i);
      WritableUtils.writeVInt(output, type.ordinal());
    }
    // Encode coveredColumns.size() and whether or not this is a local index
    WritableUtils.writeVInt(output, (coveredColumnsMap.size() + 1) * (isLocalIndex ? -1 : 1));
    for (ColumnReference ref : coveredColumnsMap.keySet()) {
      Bytes.writeByteArray(output, ref.getFamily());
      Bytes.writeByteArray(output, ref.getQualifier());
    }
    // TODO: remove when rowKeyOrderOptimizable hack no longer needed
    WritableUtils.writeVInt(output, indexTableName.length * (rowKeyOrderOptimizable ? 1 : -1));
    output.write(indexTableName, 0, indexTableName.length);
    Bytes.writeByteArray(output, dataEmptyKeyValueCF);
    // TODO in order to maintain b/w compatibility encode emptyKeyValueCFPtr.getLength() as a
    // negative value (so we can distinguish between new and old clients)
    // when indexedColumnTypes is removed, remove this
    WritableUtils.writeVInt(output, -emptyKeyValueCFPtr.getLength());
    output.write(emptyKeyValueCFPtr.get(), emptyKeyValueCFPtr.getOffset(),
      emptyKeyValueCFPtr.getLength());

    WritableUtils.writeVInt(output, indexedExpressions.size());
    for (Expression expression : indexedExpressions) {
      WritableUtils.writeVInt(output, ExpressionType.valueOf(expression).ordinal());
      expression.write(output);
    }

    rowKeyMetaData.write(output);
    // Encode indexWALDisabled in nDataCFs
    WritableUtils.writeVInt(output, (nDataCFs + 1) * (indexWALDisabled ? -1 : 1));
    // Encode estimatedIndexRowKeyBytes and immutableRows together.
    WritableUtils.writeVInt(output, estimatedIndexRowKeyBytes * (immutableRows ? -1 : 1));
  }

  public static ServerCachingProtos.IndexMaintainer toProto(IndexMaintainer maintainer)
    throws IOException {
    ServerCachingProtos.IndexMaintainer.Builder builder =
      ServerCachingProtos.IndexMaintainer.newBuilder();
    builder.setSaltBuckets(maintainer.nIndexSaltBuckets);
    builder.setIsMultiTenant(maintainer.isMultiTenant);
    if (maintainer.viewIndexId != null) {
      builder.setViewIndexId(ByteStringer.wrap(maintainer.viewIndexId));
      builder.setViewIndexIdType(maintainer.viewIndexIdType.getSqlType());
    }
    for (ColumnReference colRef : maintainer.indexedColumns) {
      ServerCachingProtos.ColumnReference.Builder cRefBuilder =
        ServerCachingProtos.ColumnReference.newBuilder();
      cRefBuilder.setFamily(ByteStringer.wrap(colRef.getFamily()));
      cRefBuilder.setQualifier(ByteStringer.wrap(colRef.getQualifier()));
      builder.addIndexedColumns(cRefBuilder.build());
    }
    for (PDataType dataType : maintainer.indexedColumnTypes) {
      builder.addIndexedColumnTypeOrdinal(dataType.ordinal());
    }
    for (Entry<ColumnReference, ColumnReference> e : maintainer.coveredColumnsMap.entrySet()) {
      ServerCachingProtos.ColumnReference.Builder cRefBuilder =
        ServerCachingProtos.ColumnReference.newBuilder();
      ColumnReference dataTableColRef = e.getKey();
      cRefBuilder.setFamily(ByteStringer.wrap(dataTableColRef.getFamily()));
      cRefBuilder.setQualifier(ByteStringer.wrap(dataTableColRef.getQualifier()));
      builder.addDataTableColRefForCoveredColumns(cRefBuilder.build());
      if (maintainer.encodingScheme != NON_ENCODED_QUALIFIERS) {
        // We need to serialize the colRefs of index tables only in case of encoded column names.
        ColumnReference indexTableColRef = e.getValue();
        cRefBuilder = ServerCachingProtos.ColumnReference.newBuilder();
        cRefBuilder.setFamily(ByteStringer.wrap(indexTableColRef.getFamily()));
        cRefBuilder.setQualifier(ByteStringer.wrap(indexTableColRef.getQualifier()));
        builder.addIndexTableColRefForCoveredColumns(cRefBuilder.build());
      }
    }
    builder.setIsLocalIndex(maintainer.isLocalIndex);
    if (maintainer.parentTableType != null) {
      builder.setParentTableType(maintainer.parentTableType.toString());
    }
    builder.setIndexDataColumnCount(maintainer.indexDataColumnCount);
    builder.setIndexTableName(ByteStringer.wrap(maintainer.indexTableName));
    builder.setRowKeyOrderOptimizable(maintainer.rowKeyOrderOptimizable);
    builder.setDataTableEmptyKeyValueColFamily(ByteStringer.wrap(maintainer.dataEmptyKeyValueCF));
    ServerCachingProtos.ImmutableBytesWritable.Builder ibwBuilder =
      ServerCachingProtos.ImmutableBytesWritable.newBuilder();
    ibwBuilder.setByteArray(ByteStringer.wrap(maintainer.emptyKeyValueCFPtr.get()));
    ibwBuilder.setLength(maintainer.emptyKeyValueCFPtr.getLength());
    ibwBuilder.setOffset(maintainer.emptyKeyValueCFPtr.getOffset());
    builder.setEmptyKeyValueColFamily(ibwBuilder.build());
    try (ByteArrayOutputStream stream = new ByteArrayOutputStream()) {
      DataOutput output = new DataOutputStream(stream);
      for (Expression expression : maintainer.indexedExpressions) {
        WritableUtils.writeVInt(output, ExpressionType.valueOf(expression).ordinal());
        expression.write(output);
      }
      builder.setIndexedExpressions(ByteStringer.wrap(stream.toByteArray()));
    }
    try (ByteArrayOutputStream stream = new ByteArrayOutputStream()) {
      DataOutput output = new DataOutputStream(stream);
      maintainer.rowKeyMetaData.write(output);
      builder.setRowKeyMetadata(ByteStringer.wrap(stream.toByteArray()));
    }
    builder.setNumDataTableColFamilies(maintainer.nDataCFs);
    builder.setIndexWalDisabled(maintainer.indexWALDisabled);
    builder.setIndexRowKeyByteSize(maintainer.estimatedIndexRowKeyBytes);
    builder.setImmutable(maintainer.immutableRows);
    if (maintainer.indexedColumnsInfo != null) {
      for (Pair<String, String> p : maintainer.indexedColumnsInfo) {
        ServerCachingProtos.ColumnInfo.Builder ciBuilder =
          ServerCachingProtos.ColumnInfo.newBuilder();
        if (p.getFirst() != null) {
          ciBuilder.setFamilyName(p.getFirst());
        }
        ciBuilder.setColumnName(p.getSecond());
        builder.addIndexedColumnInfo(ciBuilder.build());
      }
    }
    builder.setEncodingScheme(maintainer.encodingScheme != null
      ? maintainer.encodingScheme.getSerializedMetadataValue()
      : QualifierEncodingScheme.NON_ENCODED_QUALIFIERS.getSerializedMetadataValue());
    builder.setImmutableStorageScheme(maintainer.immutableStorageScheme != null
      ? maintainer.immutableStorageScheme.getSerializedMetadataValue()
      : ImmutableStorageScheme.ONE_CELL_PER_COLUMN.getSerializedMetadataValue());
    if (maintainer.logicalIndexName != null) {
      builder.setLogicalIndexName(maintainer.logicalIndexName);
    }
    builder.setDataEncodingScheme(maintainer.dataEncodingScheme != null
      ? maintainer.dataEncodingScheme.getSerializedMetadataValue()
      : QualifierEncodingScheme.NON_ENCODED_QUALIFIERS.getSerializedMetadataValue());
    builder.setDataImmutableStorageScheme(maintainer.dataImmutableStorageScheme != null
      ? maintainer.dataImmutableStorageScheme.getSerializedMetadataValue()
      : ImmutableStorageScheme.ONE_CELL_PER_COLUMN.getSerializedMetadataValue());
    builder.setIsUncovered(maintainer.isUncovered);
    if (maintainer.indexWhere != null) {
      try (ByteArrayOutputStream stream = new ByteArrayOutputStream()) {
        DataOutput output = new DataOutputStream(stream);
        WritableUtils.writeVInt(output, ExpressionType.valueOf(maintainer.indexWhere).ordinal());
        maintainer.indexWhere.write(output);
        builder.setIndexWhere(ByteStringer.wrap(stream.toByteArray()));
        for (ColumnReference colRef : maintainer.indexWhereColumns) {
          ServerCachingProtos.ColumnReference.Builder cRefBuilder =
            ServerCachingProtos.ColumnReference.newBuilder();
          cRefBuilder.setFamily(ByteStringer.wrap(colRef.getFamily()));
          cRefBuilder.setQualifier(ByteStringer.wrap(colRef.getQualifier()));
          builder.addIndexWhereColumns(cRefBuilder.build());
        }
      }
    }
    builder.setIsCDCIndex(maintainer.isCDCIndex);
    if (maintainer.indexConsistency != null) {
      builder.setIndexConsistency(maintainer.indexConsistency.name());
    }
    if (maintainer.isDataTableSalted) {
      builder.setDataTableSaltBuckets(maintainer.nDataTableSaltBuckets);
    }
    if (maintainer.vectorAlgorithm != null) {
      builder.setVectorAlgorithm(maintainer.vectorAlgorithm);
      if (maintainer.distanceMetric != null) {
        builder.setDistanceMetric(maintainer.distanceMetric);
      }
      if (maintainer.vectorDimension != null) {
        builder.setVectorDimension(maintainer.vectorDimension);
      }
      if (maintainer.centroidGeneration != null) {
        builder.setCentroidGeneration(maintainer.centroidGeneration);
      }
    }
    if (
      maintainer.coveredVectorColumnTypes != null && !maintainer.coveredVectorColumnTypes.isEmpty()
    ) {
      for (Entry<ColumnReference, PDataType> e : maintainer.coveredVectorColumnTypes.entrySet()) {
        ServerCachingProtos.ColumnReference.Builder cRefBuilder =
          ServerCachingProtos.ColumnReference.newBuilder();
        cRefBuilder.setFamily(ByteStringer.wrap(e.getKey().getFamily()));
        cRefBuilder.setQualifier(ByteStringer.wrap(e.getKey().getQualifier()));
        builder.addCoveredVectorColumns(cRefBuilder.build());
        builder.addCoveredVectorColumnTypeOrdinal(e.getValue().ordinal());
      }
    }
    if (maintainer.functionalVectorColRef != null) {
      ServerCachingProtos.ColumnReference.Builder cRefBuilder =
        ServerCachingProtos.ColumnReference.newBuilder();
      cRefBuilder.setFamily(ByteStringer.wrap(maintainer.functionalVectorColRef.getFamily()));
      cRefBuilder.setQualifier(ByteStringer.wrap(maintainer.functionalVectorColRef.getQualifier()));
      builder.setFunctionalVectorColRef(cRefBuilder.build());
    }
    return builder.build();
  }

  public int getEstimatedByteSize() {
    int size = WritableUtils.getVIntSize(nIndexSaltBuckets);
    size += WritableUtils.getVIntSize(estimatedIndexRowKeyBytes);
    size += WritableUtils.getVIntSize(indexedColumns.size());
    size += viewIndexId == null ? 0 : viewIndexId.length;
    for (ColumnReference ref : indexedColumns) {
      size += WritableUtils.getVIntSize(ref.getFamily().length);
      size += ref.getFamily().length;
      size += WritableUtils.getVIntSize(ref.getQualifier().length);
      size += ref.getQualifier().length;
    }
    for (int i = 0; i < indexedColumnTypes.size(); i++) {
      PDataType type = indexedColumnTypes.get(i);
      size += WritableUtils.getVIntSize(type.ordinal());
    }
    Set<ColumnReference> dataTableColRefs = coveredColumnsMap.keySet();
    size += WritableUtils.getVIntSize(dataTableColRefs.size());
    for (ColumnReference ref : dataTableColRefs) {
      size += WritableUtils.getVIntSize(ref.getFamilyWritable().getLength());
      size += ref.getFamily().length;
      size += WritableUtils.getVIntSize(ref.getQualifierWritable().getLength());
      size += ref.getQualifier().length;
    }
    size += indexTableName.length + WritableUtils.getVIntSize(indexTableName.length);
    size += rowKeyMetaData.getByteSize();
    size += dataEmptyKeyValueCF.length + WritableUtils.getVIntSize(dataEmptyKeyValueCF.length);
    size +=
      emptyKeyValueCFPtr.getLength() + WritableUtils.getVIntSize(emptyKeyValueCFPtr.getLength());
    size += WritableUtils.getVIntSize(nDataCFs + 1);
    size += WritableUtils.getVIntSize(indexedExpressions.size());
    for (Expression expression : indexedExpressions) {
      size += WritableUtils.getVIntSize(ExpressionType.valueOf(expression).ordinal());
    }
    size += estimatedExpressionSize;
    return size;
  }

  public Expression getIndexWhere() {
    return indexWhere;
  }

  public Set<ColumnReference> getIndexWhereColumns() {
    return indexWhereColumns;
  }

  private int estimateIndexRowKeyByteSize(int indexColByteSize) {
    int estimatedIndexRowKeyBytes = indexColByteSize + dataRowKeySchema.getEstimatedValueLength()
      + (nIndexSaltBuckets == 0 || isLocalIndex || this.isDataTableSalted
        ? 0
        : SaltingUtil.NUM_SALTING_BYTES);
    return estimatedIndexRowKeyBytes;
  }

  /**
   * Init calculated state reading/creating
   */
  private void initCachedState() {
    byte[] indexEmptyKvQualifier =
      EncodedColumnsUtil.getEmptyKeyValueInfo(encodingScheme).getFirst();
    byte[] dataEmptyKvQualifier =
      EncodedColumnsUtil.getEmptyKeyValueInfo(dataEncodingScheme).getFirst();
    indexEmptyKeyValueRef = new ColumnReference(dataEmptyKeyValueCF, indexEmptyKvQualifier);
    dataEmptyKeyValueRef = new ColumnReference(dataEmptyKeyValueCF, dataEmptyKvQualifier);
    this.allColumns =
      Sets.newLinkedHashSetWithExpectedSize(indexedExpressions.size() + coveredColumnsMap.size());
    // columns that are required to evaluate all expressions in indexedExpressions (not including
    // columns in data row key)
    this.indexedColumns = Sets.newLinkedHashSetWithExpectedSize(indexedExpressions.size());
    for (Expression expression : indexedExpressions) {
      KeyValueExpressionVisitor visitor = new KeyValueExpressionVisitor() {
        @Override
        public Void visit(KeyValueColumnExpression expression) {
          if (
            indexedColumns.add(
              new ColumnReference(expression.getColumnFamily(), expression.getColumnQualifier()))
          ) {
            indexedColumnTypes.add(expression.getDataType());
          }
          return null;
        }
      };
      expression.accept(visitor);
    }
    allColumns.addAll(indexedColumns);
    for (ColumnReference colRef : coveredColumnsMap.keySet()) {
      if (immutableStorageScheme == ImmutableStorageScheme.ONE_CELL_PER_COLUMN) {
        allColumns.add(colRef);
      } else {
        allColumns.add(new ColumnReference(colRef.getFamily(),
          QueryConstants.SINGLE_KEYVALUE_COLUMN_QUALIFIER_BYTES));
      }
    }

    int dataPkOffset = (isDataTableSalted ? 1 : 0) + (isMultiTenant ? 1 : 0);
    int nIndexPkColumns = getIndexPkColumnCount();
    dataPkPosition = new int[nIndexPkColumns];
    Arrays.fill(dataPkPosition, EXPRESSION_NOT_PRESENT);
    int numViewConstantColumns = 0;
    BitSet viewConstantColumnBitSet = rowKeyMetaData.getViewConstantColumnBitSet();
    for (int i = dataPkOffset; i < indexDataColumnCount; i++) {
      if (!viewConstantColumnBitSet.get(i) || isIndexOnBaseTable()) {
        int indexPkPosition = rowKeyMetaData.getIndexPkPosition(i - dataPkOffset);
        this.dataPkPosition[indexPkPosition] = i;
      } else {
        numViewConstantColumns++;
      }
    }

    // Calculate the max number of trailing nulls that we should get rid of after building the index
    // row key.
    // We only get rid of nulls for variable length types, so we have to be careful to consider the
    // type of the
    // index table, not the data type of the data table
    int expressionsPos = indexedExpressions.size();
    int indexPkPos = nIndexPkColumns - numViewConstantColumns - 1;
    while (indexPkPos >= 0) {
      int dataPkPos = dataPkPosition[indexPkPos];
      boolean isDataNullable;
      PDataType dataType;
      if (dataPkPos == EXPRESSION_NOT_PRESENT) {
        isDataNullable = true;
        dataType = indexedExpressions.get(--expressionsPos).getDataType();
      } else {
        Field dataField = dataRowKeySchema.getField(dataPkPos);
        dataType = dataField.getDataType();
        isDataNullable = dataField.isNullable();
      }
      PDataType indexDataType = IndexUtil.getIndexColumnDataType(isDataNullable, dataType);
      if (indexDataType.isFixedWidth()) {
        break;
      }
      indexPkPos--;
    }
    maxTrailingNulls = nIndexPkColumns - indexPkPos - 1;

    // Resolve indexed vector expression ordinal for row key construction.
    if (vectorAlgorithm != null) {
      vectorExpressionOrdinal = -1;
      for (int i = 0; i < indexedExpressions.size(); i++) {
        PDataType t = indexedExpressions.get(i).getDataType();
        if (t instanceof PVectorFloat || t instanceof PVectorDouble) {
          vectorExpressionOrdinal = i;
          break;
        }
      }
      if (this.coveredVectorColumnTypes == null) {
        this.coveredVectorColumnTypes = Collections.emptyMap();
      }
    }
  }

  private int getIndexPkColumnCount() {
    return getIndexPkColumnCount(indexDataColumnCount, indexedExpressions.size(), isDataTableSalted,
      isMultiTenant);
  }

  private static int getIndexPkColumnCount(int indexDataColumnCount, int numIndexExpressions,
    boolean isDataTableSalted, boolean isMultiTenant) {
    return indexDataColumnCount + numIndexExpressions - (isDataTableSalted ? 1 : 0)
      - (isMultiTenant ? 1 : 0);
  }

  private RowKeyMetaData newRowKeyMetaData() {
    return getIndexPkColumnCount() < 0xFF
      ? new ByteSizeRowKeyMetaData()
      : new IntSizedRowKeyMetaData();
  }

  private static RowKeyMetaData newRowKeyMetaData(IndexMaintainer i, RowKeySchema rowKeySchema,
    int numIndexExpressions, boolean isDataTableSalted, boolean isMultiTenant) {
    int indexPkColumnCount = getIndexPkColumnCount(i.indexDataColumnCount, numIndexExpressions,
      isDataTableSalted, isMultiTenant);
    return indexPkColumnCount < 0xFF
      ? i.new ByteSizeRowKeyMetaData()
      : i.new IntSizedRowKeyMetaData();
  }

  private RowKeyMetaData newRowKeyMetaData(int capacity) {
    return capacity < 0xFF
      ? new ByteSizeRowKeyMetaData(capacity)
      : new IntSizedRowKeyMetaData(capacity);
  }

  private static void writeInverted(byte[] buf, int offset, int length, DataOutput output)
    throws IOException {
    for (int i = offset; i < offset + length; i++) {
      byte b = SortOrder.invert(buf[i]);
      output.write(b);
    }
  }

  private abstract class RowKeyMetaData implements Writable {
    private BitSet descIndexColumnBitSet;
    private BitSet viewConstantColumnBitSet;

    private RowKeyMetaData() {
    }

    private RowKeyMetaData(int nIndexedColumns) {
      descIndexColumnBitSet = BitSet.withCapacity(nIndexedColumns);
      viewConstantColumnBitSet = BitSet.withCapacity(dataRowKeySchema.getMaxFields()); // Size based
                                                                                       // on number
                                                                                       // of data PK
                                                                                       // columns
    }

    protected int getByteSize() {
      return BitSet.getByteSize(getIndexPkColumnCount()) * 3
        + BitSet.getByteSize(dataRowKeySchema.getMaxFields());
    }

    protected abstract int getIndexPkPosition(int dataPkPosition);

    protected abstract int setIndexPkPosition(int dataPkPosition, int indexPkPosition);

    @Override
    public void readFields(DataInput input) throws IOException {
      int length = getIndexPkColumnCount();
      descIndexColumnBitSet = BitSet.read(input, length);
      int vclength = dataRowKeySchema.getMaxFields();
      viewConstantColumnBitSet = BitSet.read(input, vclength);
    }

    @Override
    public void write(DataOutput output) throws IOException {
      int length = getIndexPkColumnCount();
      BitSet.write(output, descIndexColumnBitSet, length);
      int vclength = dataRowKeySchema.getMaxFields();
      BitSet.write(output, viewConstantColumnBitSet, vclength);
    }

    private BitSet getDescIndexColumnBitSet() {
      return descIndexColumnBitSet;
    }

    private BitSet getViewConstantColumnBitSet() {
      return viewConstantColumnBitSet;
    }
  }

  private static int BYTE_OFFSET = 127;

  private class ByteSizeRowKeyMetaData extends RowKeyMetaData {
    private byte[] indexPkPosition;

    private ByteSizeRowKeyMetaData() {
    }

    private ByteSizeRowKeyMetaData(int nIndexedColumns) {
      super(nIndexedColumns);
      this.indexPkPosition = new byte[nIndexedColumns];
    }

    @Override
    protected int getIndexPkPosition(int dataPkPosition) {
      // Use offset for byte so that we can get full range of 0 - 255
      // We use -128 as marker for a non row key index column,
      // that's why our offset if 127 instead of 128
      return this.indexPkPosition[dataPkPosition] + BYTE_OFFSET;
    }

    @Override
    protected int setIndexPkPosition(int dataPkPosition, int indexPkPosition) {
      return this.indexPkPosition[dataPkPosition] = (byte) (indexPkPosition - BYTE_OFFSET);
    }

    @Override
    public void write(DataOutput output) throws IOException {
      super.write(output);
      output.write(indexPkPosition);
    }

    @Override
    protected int getByteSize() {
      return super.getByteSize() + indexPkPosition.length;
    }

    @Override
    public void readFields(DataInput input) throws IOException {
      super.readFields(input);
      this.indexPkPosition = new byte[getIndexPkColumnCount()];
      input.readFully(indexPkPosition);
    }
  }

  private class IntSizedRowKeyMetaData extends RowKeyMetaData {
    private int[] indexPkPosition;

    private IntSizedRowKeyMetaData() {
    }

    private IntSizedRowKeyMetaData(int nIndexedColumns) {
      super(nIndexedColumns);
      this.indexPkPosition = new int[nIndexedColumns];
    }

    @Override
    protected int getIndexPkPosition(int dataPkPosition) {
      return this.indexPkPosition[dataPkPosition];
    }

    @Override
    protected int setIndexPkPosition(int dataPkPosition, int indexPkPosition) {
      return this.indexPkPosition[dataPkPosition] = indexPkPosition;
    }

    @Override
    public void write(DataOutput output) throws IOException {
      super.write(output);
      for (int i = 0; i < indexPkPosition.length; i++) {
        output.writeInt(indexPkPosition[i]);
      }
    }

    @Override
    protected int getByteSize() {
      return super.getByteSize() + indexPkPosition.length * Bytes.SIZEOF_INT;
    }

    @Override
    public void readFields(DataInput input) throws IOException {
      super.readFields(input);
      this.indexPkPosition = new int[getIndexPkColumnCount()];
      for (int i = 0; i < indexPkPosition.length; i++) {
        indexPkPosition[i] = input.readInt();
      }
    }
  }

  @Override
  public Iterator<ColumnReference> iterator() {
    return allColumns.iterator();
  }

  public ValueGetter createGetterFromKeyValues(final byte[] rowKey,
    Collection<? extends Cell> pendingUpdates) {
    final Map<ColumnReference, ImmutableBytesPtr> valueMap =
      Maps.newHashMapWithExpectedSize(pendingUpdates.size());
    for (Cell kv : pendingUpdates) {
      // create new pointers to each part of the kv
      ImmutableBytesPtr value =
        new ImmutableBytesPtr(kv.getValueArray(), kv.getValueOffset(), kv.getValueLength());
      valueMap
        .put(new ColumnReference(kv.getFamilyArray(), kv.getFamilyOffset(), kv.getFamilyLength(),
          kv.getQualifierArray(), kv.getQualifierOffset(), kv.getQualifierLength()), value);
    }
    return new AbstractValueGetter() {
      @Override
      public ImmutableBytesWritable getLatestValue(ColumnReference ref, long ts) {
        if (ref.equals(indexEmptyKeyValueRef)) return null;
        return valueMap.get(ref);
      }

      @Override
      public byte[] getRowKey() {
        return rowKey;
      }
    };
  }

  public byte[] getDataEmptyKeyValueCF() {
    return dataEmptyKeyValueCF;
  }

  public boolean isLocalIndex() {
    return isLocalIndex;
  }

  public boolean isUncovered() {
    return isUncovered;
  }

  public boolean isCDCIndex() {
    return isCDCIndex;
  }

  public boolean isMultiTenant() {
    return isMultiTenant;
  }

  public boolean isImmutableRows() {
    return immutableRows;
  }

  public boolean isIndexOnBaseTable() {
    if (parentTableType == null) {
      return false;
    }
    return parentTableType == PTableType.TABLE;
  }

  public Set<ColumnReference> getIndexedColumns() {
    return indexedColumns;
  }

  public IndexConsistency getIndexConsistency() {
    return indexConsistency;
  }

  /** Returns the vector indexing algorithm identifier, or null if not a vector index. */
  public String getVectorAlgorithm() {
    return vectorAlgorithm;
  }

  /** Returns true if this maintainer manages a VECTOR_GLOBAL index. */
  public boolean isVectorIndex() {
    return vectorAlgorithm != null;
  }

  /** Returns true if the given data column reference is a covered vector column. */
  public boolean isCoveredVectorColumn(ColumnReference ref) {
    return coveredVectorColumnTypes != null && coveredVectorColumnTypes.containsKey(ref);
  }

  /**
   * Returns the column expression for the indexed vector column, unwrapping any single cell
   * container to expose the underlying data column coordinates.
   */
  private KeyValueColumnExpression getIndexedVectorKeyValueExpression() {
    if (vectorExpressionOrdinal < 0 || vectorExpressionOrdinal >= indexedExpressions.size()) {
      return null;
    }
    Expression expr = indexedExpressions.get(vectorExpressionOrdinal);
    if (expr instanceof SingleCellColumnExpression) {
      return ((SingleCellColumnExpression) expr).getKeyValueExpression();
    }
    if (expr instanceof KeyValueColumnExpression) {
      return (KeyValueColumnExpression) expr;
    }
    return null;
  }

  /** Returns true if the given column reference is the indexed vector column. */
  public boolean isIndexedVectorColumn(ColumnReference ref) {
    if (ref == null) {
      return false;
    }
    KeyValueColumnExpression kve = getIndexedVectorKeyValueExpression();
    if (kve != null) {
      return Bytes.compareTo(ref.getFamily(), kve.getColumnFamily()) == 0
        && Bytes.compareTo(ref.getQualifier(), kve.getColumnQualifier()) == 0;
    }
    return false;
  }

  /** Returns the name of the data column that is the indexed vector column. */
  public String getIndexedVectorColumnName(PTable dataTable) {
    if (dataTable != null) {
      KeyValueColumnExpression kve = getIndexedVectorKeyValueExpression();
      if (kve != null) {
        byte[] cf = kve.getColumnFamily();
        byte[] cq = kve.getColumnQualifier();
        try {
          PColumn col = (cf == null || cf.length == 0)
            ? dataTable.getColumnForColumnQualifier(null, cq)
            : dataTable.getColumnFamily(cf).getPColumnForColumnQualifier(cq);
          if (col != null) {
            return col.getName().getString();
          }
        } catch (SQLException e) {
          // fallback
        }
      }
    }
    return null;
  }

  /**
   * Returns true if the vector column (covered column or indexed expression) has double elements.
   */
  public boolean isDoubleVector(ColumnReference ref) {
    if (ref != null && coveredVectorColumnTypes != null) {
      PDataType type = coveredVectorColumnTypes.get(ref);
      if (type != null) {
        return type instanceof PVectorDouble;
      }
    }
    KeyValueColumnExpression kve = getIndexedVectorKeyValueExpression();
    if (kve != null) {
      return kve.getDataType() instanceof PVectorDouble;
    }
    if (vectorExpressionOrdinal >= 0 && vectorExpressionOrdinal < indexedExpressions.size()) {
      return indexedExpressions.get(vectorExpressionOrdinal).getDataType() instanceof PVectorDouble;
    }
    return false;
  }

  /** Returns the map of covered vector columns to their PDataType. */
  public Map<ColumnReference, PDataType> getCoveredVectorColumnTypes() {
    return coveredVectorColumnTypes;
  }

  /** Returns the sort order of the vector column expression. */
  public SortOrder getVectorSortOrder() {
    KeyValueColumnExpression kve = getIndexedVectorKeyValueExpression();
    if (kve != null) {
      return kve.getSortOrder();
    }
    if (vectorExpressionOrdinal >= 0 && vectorExpressionOrdinal < indexedExpressions.size()) {
      return indexedExpressions.get(vectorExpressionOrdinal).getSortOrder();
    }
    return SortOrder.ASC;
  }

  /** Returns the sort order for the given vector column reference. */
  public SortOrder getVectorSortOrder(ColumnReference ref) {
    if (isIndexedVectorColumn(ref)) {
      return getVectorSortOrder();
    }
    return SortOrder.ASC;
  }

  public ColumnReference getFunctionalVectorColRef() {
    return functionalVectorColRef;
  }

  /**
   * Returns ASC-encoded bytes for the functional vector column value, reusing precomputed
   * {@code vectorValue} if provided. Returns null if the vector value is absent or empty.
   */
  private static byte[] getFunctionalVectorBytes(IndexMaintainer maintainer,
    ValueGetter valueGetter, long ts, ImmutableBytesWritable vectorValue) {
    if (maintainer == null || maintainer.functionalVectorColRef == null) {
      return null;
    }
    ImmutableBytesWritable vecVal =
      vectorValue != null ? vectorValue : maintainer.getVectorValue(valueGetter, ts);
    if (vecVal == null || vecVal.get() == null || vecVal.getLength() == 0) {
      return null;
    }
    SortOrder sortOrder = maintainer.getVectorSortOrder();
    if (sortOrder != SortOrder.ASC) {
      byte[] valueBytes = new byte[vecVal.getLength()];
      if (maintainer.isDoubleVector(maintainer.functionalVectorColRef)) {
        PVectorDouble.transcodeBytes(vecVal.get(), vecVal.getOffset(), vecVal.getLength(),
          sortOrder, valueBytes, 0, SortOrder.ASC);
      } else {
        PVectorFloat.transcodeBytes(vecVal.get(), vecVal.getOffset(), vecVal.getLength(), sortOrder,
          valueBytes, 0, SortOrder.ASC);
      }
      return valueBytes;
    }
    return vecVal.copyBytes();
  }

  /**
   * Returns ASC encoded bytes for the indexed vector column, transcoding from the column's sort
   * order when necessary.
   */
  private static byte[] getIndexedVectorBytes(IndexMaintainer maintainer,
    ColumnReference dataColRef, ValueGetter valueGetter, long ts,
    ImmutableBytesWritable vectorValue) {
    if (maintainer == null) {
      return null;
    }
    ImmutableBytesWritable vecVal =
      vectorValue != null ? vectorValue : maintainer.getVectorValue(valueGetter, ts);
    if (vecVal == null || vecVal.get() == null || vecVal.getLength() == 0) {
      return null;
    }
    SortOrder sortOrder = maintainer.getVectorSortOrder(dataColRef);
    if (sortOrder != SortOrder.ASC) {
      byte[] valueBytes = new byte[vecVal.getLength()];
      if (maintainer.isDoubleVector(dataColRef)) {
        PVectorDouble.transcodeBytes(vecVal.get(), vecVal.getOffset(), vecVal.getLength(),
          sortOrder, valueBytes, 0, SortOrder.ASC);
      } else {
        PVectorFloat.transcodeBytes(vecVal.get(), vecVal.getOffset(), vecVal.getLength(), sortOrder,
          valueBytes, 0, SortOrder.ASC);
      }
      return valueBytes;
    }
    return vecVal.copyBytes();
  }

  /** Evaluates and returns the vector column value for the current row state. */
  public ImmutableBytesWritable getVectorValue(ValueGetter valueGetter, long ts) {
    if (
      valueGetter == null || vectorExpressionOrdinal < 0
        || vectorExpressionOrdinal >= indexedExpressions.size()
    ) {
      return null;
    }
    ImmutableBytesWritable ptr = new ImmutableBytesWritable();
    Expression vectorExpression = indexedExpressions.get(vectorExpressionOrdinal);
    vectorExpression.evaluate(new ValueGetterTuple(valueGetter, ts), ptr);
    if (ptr.get() == null || ptr.getLength() == 0) {
      return null;
    }
    return ptr;
  }

  /** Evaluates whether the indexed vector value is unchanged between two row mutation states. */
  public boolean isVectorUnchanged(Put currentDataRowState, Put nextDataRowState) {
    if (currentDataRowState == null || nextDataRowState == null || !isVectorIndex()) {
      return false;
    }
    ValueGetter currentVG = new IndexUtil.SimpleValueGetter(currentDataRowState);
    ValueGetter nextVG = new IndexUtil.SimpleValueGetter(nextDataRowState);
    ImmutableBytesWritable currentPtr = getVectorValue(currentVG, HConstants.LATEST_TIMESTAMP);
    ImmutableBytesWritable nextPtr = getVectorValue(nextVG, HConstants.LATEST_TIMESTAMP);
    if (currentPtr == null || nextPtr == null) {
      return false;
    }
    return Bytes.equals(currentPtr.get(), currentPtr.getOffset(), currentPtr.getLength(),
      nextPtr.get(), nextPtr.getOffset(), nextPtr.getLength());
  }

  /** Returns the distance metric name, or null if this is not a vector index. */
  public String getDistanceMetric() {
    return distanceMetric;
  }

  public String getVectorIndexAlgorithm() {
    return vectorAlgorithm;
  }

  public void setVectorAlgorithm(String vectorAlgorithm) {
    this.vectorAlgorithm = vectorAlgorithm;
  }

  public String getVectorDistanceMetric() {
    return distanceMetric;
  }

  public void setDistanceMetric(String distanceMetric) {
    this.distanceMetric = distanceMetric;
  }

  public Integer getVectorDimension() {
    return vectorDimension;
  }

  public void setVectorDimension(Integer vectorDimension) {
    this.vectorDimension = vectorDimension;
  }

  public void setVectorDimension(int vectorDimension) {
    this.vectorDimension = vectorDimension;
  }

  public Long getCentroidGeneration() {
    return centroidGeneration;
  }

  public Long getVectorCentroidGeneration() {
    return centroidGeneration;
  }

  public void setCentroidGeneration(Long centroidGeneration) {
    this.centroidGeneration = centroidGeneration;
  }

  public void setCentroidGeneration(long centroidGeneration) {
    this.centroidGeneration = centroidGeneration;
  }

  private byte[] buildVectorRowKey(ValueGetter valueGetter, ImmutableBytesWritable rowKeyPtr,
    long ts) {
    if (valueGetter == null) {
      return null;
    }
    return buildVectorRowKey(valueGetter, rowKeyPtr, ts, getVectorValue(valueGetter, ts));
  }

  /**
   * Builds the vector index row key from an evaluated vector value. Returns null when the vector is
   * absent, excluding the row from the index.
   */
  private byte[] buildVectorRowKey(ValueGetter valueGetter, ImmutableBytesWritable rowKeyPtr,
    long ts, ImmutableBytesWritable vectorValue) {
    if (
      valueGetter == null || vectorValue == null || vectorValue.get() == null
        || vectorValue.getLength() == 0
    ) {
      return null;
    }

    // Assign against the maintainer's centroid generation, loading on cache miss.
    VectorCentroidCache.CachedCentroids cachedCentroids =
      VectorCentroidCache.getInstance().getCentroidsForWrite(logicalIndexName, centroidGeneration);
    int centroidId = cachedCentroids.findNearestCentroid(vectorValue.get(), vectorValue.getOffset(),
      vectorValue.getLength(), distanceMetric);
    ImmutableBytesWritable ptr = new ImmutableBytesWritable();

    boolean isIndexSalted = nIndexSaltBuckets > 0;
    TrustedByteArrayOutputStream stream =
      new TrustedByteArrayOutputStream(estimatedIndexRowKeyBytes);
    DataOutput output = new DataOutputStream(stream);

    try {
      if (isIndexSalted) {
        output.write(0); // will be set at end to index salt byte
      }
      int dataPosOffset = isDataTableSalted ? 1 : 0;
      BitSet viewConstantColumnBitSet = this.rowKeyMetaData.getViewConstantColumnBitSet();
      int nIndexedColumns = getIndexPkColumnCount() - getNumViewConstants();
      int[][] dataRowKeyLocator = new int[2][nIndexedColumns];
      int maxRowKeyOffset = rowKeyPtr.getOffset() + rowKeyPtr.getLength();
      dataRowKeySchema.iterator(rowKeyPtr, ptr, dataPosOffset);

      if (viewIndexId != null) {
        output.write(viewIndexId);
      }

      if (isMultiTenant) {
        dataRowKeySchema.next(ptr, dataPosOffset, maxRowKeyOffset);
        output.write(ptr.get(), ptr.getOffset(), ptr.getLength());
        if (!dataRowKeySchema.getField(dataPosOffset).getDataType().isFixedWidth()) {
          output.write(SchemaUtil.getSeparatorBytes(
            dataRowKeySchema.getField(dataPosOffset).getDataType(), rowKeyOrderOptimizable,
            ptr.getLength() == 0, dataRowKeySchema.getField(dataPosOffset).getSortOrder()));
        }
        dataPosOffset++;
      }

      for (int i = dataPosOffset; i < indexDataColumnCount; i++) {
        Boolean hasValue = dataRowKeySchema.next(ptr, i, maxRowKeyOffset);
        if (!viewConstantColumnBitSet.get(i) || isIndexOnBaseTable()) {
          int pos = rowKeyMetaData.getIndexPkPosition(i - dataPosOffset);
          if (Boolean.TRUE.equals(hasValue)) {
            dataRowKeyLocator[0][pos] = ptr.getOffset();
            dataRowKeyLocator[1][pos] = ptr.getLength();
          } else {
            dataRowKeyLocator[0][pos] = 0;
            dataRowKeyLocator[1][pos] = 0;
          }
        }
      }

      // Encode centroid ID prefix
      byte[] centroidBytes = new byte[Bytes.SIZEOF_INT];
      PInteger.INSTANCE.toBytes(centroidId, centroidBytes, 0);
      output.write(centroidBytes);

      BitSet descIndexColumnBitSet = rowKeyMetaData.getDescIndexColumnBitSet();
      int trailingVariableWidthColumnNum = 0;
      PDataType[] indexedColumnDataTypes = new PDataType[nIndexedColumns];
      indexedColumnDataTypes[0] = PInteger.INSTANCE;

      for (int i = 1; i < nIndexedColumns; i++) {
        PDataType dataColumnType;
        boolean isNullable;
        SortOrder dataSortOrder;
        if (dataPkPosition[i] == EXPRESSION_NOT_PRESENT) {
          Expression expression = indexedExpressions.get(i);
          dataColumnType = expression.getDataType();
          dataSortOrder = expression.getSortOrder();
          isNullable = expression.isNullable();
          expression.evaluate(new ValueGetterTuple(valueGetter, ts), ptr);
        } else {
          Field field = dataRowKeySchema.getField(dataPkPosition[i]);
          dataColumnType = field.getDataType();
          ptr.set(rowKeyPtr.get(), dataRowKeyLocator[0][i], dataRowKeyLocator[1][i]);
          dataSortOrder = field.getSortOrder();
          isNullable = field.isNullable();
        }
        boolean isDataColumnInverted = dataSortOrder != SortOrder.ASC;
        PDataType indexColumnType = IndexUtil.getIndexColumnDataType(isNullable, dataColumnType);
        indexedColumnDataTypes[i] = indexColumnType;
        boolean isBytesComparable = dataColumnType.isBytesComparableWith(indexColumnType);
        boolean isIndexColumnDesc = descIndexColumnBitSet.get(i);
        if (isBytesComparable && isDataColumnInverted == isIndexColumnDesc) {
          output.write(ptr.get(), ptr.getOffset(), ptr.getLength());
        } else {
          if (!isBytesComparable) {
            indexColumnType.coerceBytes(ptr, dataColumnType, dataSortOrder, SortOrder.getDefault());
          }
          if (isDataColumnInverted != isIndexColumnDesc) {
            writeInverted(ptr.get(), ptr.getOffset(), ptr.getLength(), output);
          } else {
            output.write(ptr.get(), ptr.getOffset(), ptr.getLength());
          }
        }

        if (!indexColumnType.isFixedWidth()) {
          output.write(SchemaUtil.getSeparatorBytes(indexColumnType, rowKeyOrderOptimizable,
            ptr.getLength() == 0, isIndexColumnDesc ? SortOrder.DESC : SortOrder.ASC));
          trailingVariableWidthColumnNum++;
        } else {
          trailingVariableWidthColumnNum = 0;
        }
      }

      byte[] indexRowKey = stream.getBuffer();
      int length = stream.size();
      int minLength = length - maxTrailingNulls;
      int indexColumnIdx = nIndexedColumns - 1;
      while (trailingVariableWidthColumnNum > 0 && length > minLength) {
        if (indexColumnIdx < 0) {
          break;
        }
        if (indexedColumnDataTypes[indexColumnIdx] != PVarbinaryEncoded.INSTANCE) {
          if (indexRowKey[length - 1] == QueryConstants.SEPARATOR_BYTE) {
            length--;
          } else {
            break;
          }
        } else {
          byte[] sepBytes = QueryConstants.VARBINARY_ENCODED_SEPARATOR_BYTES;
          if (
            length >= 2 && indexRowKey[length - 1] == sepBytes[1]
              && indexRowKey[length - 2] == sepBytes[0]
          ) {
            length -= 2;
          } else {
            break;
          }
        }
        trailingVariableWidthColumnNum--;
        indexColumnIdx--;
      }

      if (isIndexSalted) {
        byte saltByte = SaltingUtil.getSaltingByte(indexRowKey, SaltingUtil.NUM_SALTING_BYTES,
          length - SaltingUtil.NUM_SALTING_BYTES, nIndexSaltBuckets);
        indexRowKey[0] = saltByte;
      }
      return indexRowKey.length == length ? indexRowKey : Arrays.copyOf(indexRowKey, length);
    } catch (IOException e) {
      throw new RuntimeException(e);
    } finally {
      try {
        stream.close();
      } catch (IOException e) {
        throw new RuntimeException(e);
      }
    }
  }

  public static class UDFParseNodeVisitor extends StatelessTraverseAllParseNodeVisitor {

    private Map<String, UDFParseNode> udfParseNodes;

    public UDFParseNodeVisitor() {
      udfParseNodes = new HashMap<String, UDFParseNode>(1);
    }

    @Override
    public boolean visitEnter(FunctionParseNode node) throws SQLException {
      if (node instanceof UDFParseNode) {
        udfParseNodes.put(node.getName(), (UDFParseNode) node);
      }
      return super.visitEnter(node);
    }

    public Map<String, UDFParseNode> getUdfParseNodes() {
      return udfParseNodes;
    }
  }

  public byte[] getEmptyKeyValueQualifier() {
    return indexEmptyKeyValueRef.getQualifier();
  }

  public byte[] getEmptyKeyValueQualifierForDataTable() {
    return dataEmptyKeyValueRef.getQualifier();
  }

  public Set<Pair<String, String>> getIndexedColumnInfo() {
    return indexedColumnsInfo;
  }

  public ImmutableStorageScheme getIndexStorageScheme() {
    return immutableStorageScheme;
  }

  public ImmutableStorageScheme getDataImmutableStorageScheme() {
    return dataImmutableStorageScheme;
  }

  public QualifierEncodingScheme getDataEncodingScheme() {
    return dataEncodingScheme;
  }

  public byte[] getViewIndexId() {
    return viewIndexId;
  }
}
