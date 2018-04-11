/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.phoenix.iterate;

import com.google.common.collect.Lists;
import com.google.common.collect.Sets;

import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.coprocessor.RegionCoprocessorEnvironment;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.regionserver.Region;
import org.apache.hadoop.hbase.regionserver.RegionScanner;
import org.apache.hadoop.io.WritableUtils;
import org.apache.phoenix.cache.GlobalCache;
import org.apache.phoenix.cache.TenantCache;
import org.apache.phoenix.coprocessor.BaseRegionScanner;
import org.apache.phoenix.coprocessor.BaseScannerRegionObserver;
import org.apache.phoenix.coprocessor.HashJoinRegionScanner;
import org.apache.phoenix.execute.MutationState;
import org.apache.phoenix.execute.TupleProjector;
import org.apache.phoenix.expression.Expression;
import org.apache.phoenix.expression.KeyValueColumnExpression;
import org.apache.phoenix.expression.OrderByExpression;
import org.apache.phoenix.expression.SingleCellColumnExpression;
import org.apache.phoenix.expression.function.ArrayIndexFunction;
import org.apache.phoenix.hbase.index.covered.update.ColumnReference;
import org.apache.phoenix.hbase.index.util.ImmutableBytesPtr;
import org.apache.phoenix.index.IndexMaintainer;
import org.apache.phoenix.join.HashJoinInfo;
import org.apache.phoenix.mapreduce.util.PhoenixConfigurationUtil;
import org.apache.phoenix.memory.MemoryManager;
import org.apache.phoenix.query.QueryConstants;
import org.apache.phoenix.schema.KeyValueSchema;
import org.apache.phoenix.schema.PTable;
import org.apache.phoenix.schema.ValueBitSet;
import org.apache.phoenix.schema.tuple.ResultTuple;
import org.apache.phoenix.schema.tuple.Tuple;
import org.apache.phoenix.schema.types.PInteger;
import org.apache.phoenix.transaction.PhoenixTransactionContext;
import org.apache.phoenix.util.EncodedColumnsUtil;
import org.apache.phoenix.util.IndexUtil;
import org.apache.phoenix.util.ScanUtil;
import org.apache.phoenix.util.ServerUtil;

import java.io.ByteArrayInputStream;
import java.io.DataInputStream;
import java.io.IOException;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;

import static org.apache.phoenix.util.EncodedColumnsUtil.getMinMaxQualifiersFromScan;

public class NonAggregateRegionScannerFactory extends RegionScannerFactory {

  public NonAggregateRegionScannerFactory(RegionCoprocessorEnvironment env) {
    this.env = env;
  }

  @Override
  public RegionScanner getRegionScanner(final Scan scan, final RegionScanner s) throws Throwable {
      ImmutableBytesWritable ptr = new ImmutableBytesWritable();
    int offset = 0;
    if (ScanUtil.isLocalIndex(scan)) {
            /*
             * For local indexes, we need to set an offset on row key expressions to skip
             * the region start key.
             */
      Region region = getRegion();
      offset = region.getRegionInfo().getStartKey().length != 0 ?
          region.getRegionInfo().getStartKey().length :
          region.getRegionInfo().getEndKey().length;
      ScanUtil.setRowKeyOffset(scan, offset);
    }
    byte[] scanOffsetBytes = scan.getAttribute(BaseScannerRegionObserver.SCAN_OFFSET);
    Integer scanOffset = null;
    if (scanOffsetBytes != null) {
      scanOffset = (Integer)PInteger.INSTANCE.toObject(scanOffsetBytes);
    }
    RegionScanner innerScanner = s;
    PTable.QualifierEncodingScheme encodingScheme = EncodedColumnsUtil.getQualifierEncodingScheme(scan);
    boolean useNewValueColumnQualifier = EncodedColumnsUtil.useNewValueColumnQualifier(scan);

    Set<KeyValueColumnExpression> arrayKVRefs = Sets.newHashSet();
    Expression[] arrayFuncRefs = deserializeArrayPositionalExpressionInfoFromScan(scan, innerScanner, arrayKVRefs);
    KeyValueSchema.KeyValueSchemaBuilder builder = new KeyValueSchema.KeyValueSchemaBuilder(0);
    for (Expression expression : arrayFuncRefs) {
        builder.addField(expression);
    }
    KeyValueSchema kvSchema = builder.build();
    ValueBitSet kvSchemaBitSet = ValueBitSet.newInstance(kvSchema);
    TupleProjector tupleProjector = null;
    Region dataRegion = null;
    IndexMaintainer indexMaintainer = null;
    byte[][] viewConstants = null;
    PhoenixTransactionContext tx = null;
    ColumnReference[] dataColumns = IndexUtil.deserializeDataTableColumnsToJoin(scan);
    if (dataColumns != null) {
      tupleProjector = IndexUtil.getTupleProjector(scan, dataColumns);
      dataRegion = env.getRegion();
      boolean useProto = false;
      byte[] localIndexBytes = scan.getAttribute(BaseScannerRegionObserver.LOCAL_INDEX_BUILD_PROTO);
      useProto = localIndexBytes != null;
      if (localIndexBytes == null) {
        localIndexBytes = scan.getAttribute(BaseScannerRegionObserver.LOCAL_INDEX_BUILD);
      }
      List<IndexMaintainer> indexMaintainers =
          localIndexBytes == null ? null : IndexMaintainer.deserialize(localIndexBytes, useProto);
      indexMaintainer = indexMaintainers.get(0);
      viewConstants = IndexUtil.deserializeViewConstantsFromScan(scan);
      byte[] txState = scan.getAttribute(BaseScannerRegionObserver.TX_STATE);
      tx = MutationState.decodeTransaction(txState);
    }

    final TupleProjector p = TupleProjector.deserializeProjectorFromScan(scan);
    final HashJoinInfo j = HashJoinInfo.deserializeHashJoinFromScan(scan);
    boolean useQualifierAsIndex = EncodedColumnsUtil.useQualifierAsIndex(getMinMaxQualifiersFromScan(scan))
        && scan.getAttribute(BaseScannerRegionObserver.TOPN) != null;
    // setting dataRegion in case of a non-coprocessor environment
    if (dataRegion == null &&
        env.getConfiguration().get(PhoenixConfigurationUtil.SNAPSHOT_NAME_KEY) != null) {
      dataRegion = env.getRegion();
    }
    innerScanner = getWrappedScanner(env, innerScanner, arrayKVRefs, arrayFuncRefs, offset, scan, dataColumns,
        tupleProjector, dataRegion, indexMaintainer, tx, viewConstants, kvSchema, kvSchemaBitSet, j == null ? p : null,
        ptr, useQualifierAsIndex);

    final ImmutableBytesPtr tenantId = ScanUtil.getTenantId(scan);
    if (j != null) {
      innerScanner = new HashJoinRegionScanner(innerScanner, p, j, tenantId, env, useQualifierAsIndex,
          useNewValueColumnQualifier);
    }
    if (scanOffset != null) {
      innerScanner = getOffsetScanner(innerScanner, new OffsetResultIterator(
              new RegionScannerResultIterator(innerScanner, getMinMaxQualifiersFromScan(scan), encodingScheme), scanOffset),
          scan.getAttribute(QueryConstants.LAST_SCAN) != null);
    }
    final OrderedResultIterator iterator = deserializeFromScan(scan, innerScanner);
    if (iterator == null) {
      return innerScanner;
    }
    // TODO:the above wrapped scanner should be used here also
    return getTopNScanner(env, innerScanner, iterator, tenantId);
  }

  private static OrderedResultIterator deserializeFromScan(Scan scan, RegionScanner s) {
    byte[] topN = scan.getAttribute(BaseScannerRegionObserver.TOPN);
    if (topN == null) {
      return null;
    }
    ByteArrayInputStream stream = new ByteArrayInputStream(topN); // TODO: size?
    try {
      DataInputStream input = new DataInputStream(stream);
      int thresholdBytes = WritableUtils.readVInt(input);
      int limit = WritableUtils.readVInt(input);
      int estimatedRowSize = WritableUtils.readVInt(input);
      int size = WritableUtils.readVInt(input);
      List<OrderByExpression> orderByExpressions = Lists.newArrayListWithExpectedSize(size);
      for (int i = 0; i < size; i++) {
        OrderByExpression orderByExpression = new OrderByExpression();
        orderByExpression.readFields(input);
        orderByExpressions.add(orderByExpression);
      }
      PTable.QualifierEncodingScheme encodingScheme = EncodedColumnsUtil.getQualifierEncodingScheme(scan);
      ResultIterator inner = new RegionScannerResultIterator(s, EncodedColumnsUtil.getMinMaxQualifiersFromScan(scan), encodingScheme);
      return new OrderedResultIterator(inner, orderByExpressions, thresholdBytes, limit >= 0 ? limit : null, null,
          estimatedRowSize);
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

  private Expression[] deserializeArrayPositionalExpressionInfoFromScan(Scan scan, RegionScanner s,
                                                                        Set<KeyValueColumnExpression> arrayKVRefs) {
    byte[] specificArrayIdx = scan.getAttribute(BaseScannerRegionObserver.SPECIFIC_ARRAY_INDEX);
    if (specificArrayIdx == null) {
      return null;
    }
    ByteArrayInputStream stream = new ByteArrayInputStream(specificArrayIdx);
    try {
      DataInputStream input = new DataInputStream(stream);
      int arrayKVRefSize = WritableUtils.readVInt(input);
      for (int i = 0; i < arrayKVRefSize; i++) {
        PTable.ImmutableStorageScheme scheme = EncodedColumnsUtil.getImmutableStorageScheme(scan);
        KeyValueColumnExpression kvExp = scheme != PTable.ImmutableStorageScheme.ONE_CELL_PER_COLUMN ? new SingleCellColumnExpression(scheme)
            : new KeyValueColumnExpression();
        kvExp.readFields(input);
        arrayKVRefs.add(kvExp);
      }
      int arrayKVFuncSize = WritableUtils.readVInt(input);
      Expression[] arrayFuncRefs = new Expression[arrayKVFuncSize];
      for (int i = 0; i < arrayKVFuncSize; i++) {
        ArrayIndexFunction arrayIdxFunc = new ArrayIndexFunction();
        arrayIdxFunc.readFields(input);
        arrayFuncRefs[i] = arrayIdxFunc;
      }
      return arrayFuncRefs;
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


  private RegionScanner getOffsetScanner(final RegionScanner s,
      final OffsetResultIterator iterator, final boolean isLastScan) throws IOException {
    final Tuple firstTuple;
    final Region region = getRegion();
    region.startRegionOperation();
    try {
      Tuple tuple = iterator.next();
      if (tuple == null && !isLastScan) {
        List<KeyValue> kvList = new ArrayList<KeyValue>(1);
        KeyValue kv = new KeyValue(QueryConstants.OFFSET_ROW_KEY_BYTES, QueryConstants.OFFSET_FAMILY,
            QueryConstants.OFFSET_COLUMN, PInteger.INSTANCE.toBytes(iterator.getRemainingOffset()));
        kvList.add(kv);
        Result r = new Result(kvList);
        firstTuple = new ResultTuple(r);
      } else {
        firstTuple = tuple;
      }
    } catch (Throwable t) {
      ServerUtil.throwIOException(getRegion().getRegionInfo().getRegionNameAsString(), t);
      return null;
    } finally {
      region.closeRegionOperation();
    }
    return new BaseRegionScanner(s) {
      private Tuple tuple = firstTuple;

      @Override
      public boolean isFilterDone() {
        return tuple == null;
      }

      @Override
      public boolean next(List<Cell> results) throws IOException {
        try {
          if (isFilterDone()) { return false; }
          for (int i = 0; i < tuple.size(); i++) {
            results.add(tuple.getValue(i));
          }
          tuple = iterator.next();
          return !isFilterDone();
        } catch (Throwable t) {
          ServerUtil.throwIOException(getRegion().getRegionInfo().getRegionNameAsString(), t);
          return false;
        }
      }

      @Override
      public void close() throws IOException {
        try {
          s.close();
        } finally {
          try {
            if (iterator != null) {
              iterator.close();
            }
          } catch (SQLException e) {
            ServerUtil.throwIOException(getRegion().getRegionInfo().getRegionNameAsString(), e);
          }
        }
      }
    };
  }

  /**
   *  Return region scanner that does TopN.
   *  We only need to call startRegionOperation and closeRegionOperation when
   *  getting the first Tuple (which forces running through the entire region)
   *  since after this everything is held in memory
   */
  private RegionScanner getTopNScanner(RegionCoprocessorEnvironment env, final RegionScanner s,
      final OrderedResultIterator iterator, ImmutableBytesPtr tenantId) throws Throwable {

    final Tuple firstTuple;
    TenantCache tenantCache = GlobalCache.getTenantCache(env, tenantId);
    long estSize = iterator.getEstimatedByteSize();
    final MemoryManager.MemoryChunk chunk = tenantCache.getMemoryManager().allocate(estSize);
    final Region region = getRegion();
    region.startRegionOperation();
    try {
      // Once we return from the first call to next, we've run through and cached
      // the topN rows, so we no longer need to start/stop a region operation.
      firstTuple = iterator.next();
      // Now that the topN are cached, we can resize based on the real size
      long actualSize = iterator.getByteSize();
      chunk.resize(actualSize);
    } catch (Throwable t) {
      ServerUtil.throwIOException(region.getRegionInfo().getRegionNameAsString(), t);
      return null;
    } finally {
      region.closeRegionOperation();
    }
    return new BaseRegionScanner(s) {
      private Tuple tuple = firstTuple;

      @Override
      public boolean isFilterDone() {
        return tuple == null;
      }

      @Override
      public boolean next(List<Cell> results) throws IOException {
        try {
          if (isFilterDone()) {
            return false;
          }

          for (int i = 0; i < tuple.size(); i++) {
            results.add(tuple.getValue(i));
          }

          tuple = iterator.next();
          return !isFilterDone();
        } catch (Throwable t) {
          ServerUtil.throwIOException(region.getRegionInfo().getRegionNameAsString(), t);
          return false;
        }
      }

      @Override
      public void close() throws IOException {
        try {
          s.close();
        } finally {
          try {
            if(iterator != null) {
              iterator.close();
            }
          } catch (SQLException e) {
            ServerUtil.throwIOException(region.getRegionInfo().getRegionNameAsString(), e);
          } finally {
            chunk.close();
          }
        }
      }
    };
  }
}
