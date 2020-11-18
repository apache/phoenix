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

import static org.apache.phoenix.coprocessor.ScanRegionObserver.WILDCARD_SCAN_INCLUDES_DYNAMIC_COLUMNS;
import static org.apache.phoenix.schema.types.PDataType.TRUE_BYTES;
import static org.apache.phoenix.util.ScanUtil.getDummyResult;
import static org.apache.phoenix.util.ScanUtil.getPageSizeMsForRegionScanner;
import static org.apache.phoenix.util.ScanUtil.isDummy;

import org.apache.hadoop.hbase.CellUtil;
import org.apache.phoenix.thirdparty.com.google.common.collect.ImmutableList;

import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.RegionInfo;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.coprocessor.RegionCoprocessorEnvironment;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.regionserver.Region;
import org.apache.hadoop.hbase.regionserver.RegionScanner;
import org.apache.hadoop.hbase.regionserver.ScannerContext;
import org.apache.hadoop.hbase.regionserver.ScannerContextUtil;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.Pair;
import org.apache.phoenix.coprocessor.generated.DynamicColumnMetaDataProtos;
import org.apache.phoenix.execute.TupleProjector;
import org.apache.phoenix.expression.Expression;
import org.apache.phoenix.expression.KeyValueColumnExpression;
import org.apache.phoenix.hbase.index.covered.update.ColumnReference;
import org.apache.phoenix.index.IndexMaintainer;
import org.apache.phoenix.query.QueryConstants;
import org.apache.phoenix.schema.KeyValueSchema;
import org.apache.phoenix.schema.PColumn;
import org.apache.phoenix.schema.PColumnImpl;
import org.apache.phoenix.schema.ValueBitSet;
import org.apache.phoenix.schema.tuple.MultiKeyValueTuple;
import org.apache.phoenix.schema.tuple.PositionBasedResultTuple;
import org.apache.phoenix.schema.tuple.ResultTuple;
import org.apache.phoenix.schema.tuple.Tuple;
import org.apache.phoenix.transaction.PhoenixTransactionContext;
import org.apache.phoenix.util.EncodedColumnsUtil;
import org.apache.phoenix.util.EnvironmentEdgeManager;
import org.apache.phoenix.util.IndexUtil;
import org.apache.phoenix.util.ScanUtil;
import org.apache.phoenix.util.ServerUtil;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.ListIterator;
import java.util.Set;

public abstract class RegionScannerFactory {

  protected RegionCoprocessorEnvironment env;

  /**
   * Returns the region based on the value of the
   * region context
   * @return
   */
  public Region getRegion() {
    return env.getRegion();
  }

  /**
   * Returns a processed region scanner based on the query
   * conditions. Thie functionality is abstracted out of
   * the non-aggregate region observer class for better
   * usage
   * @param scan input scan
   * @param s input region scanner
   * @return
   * @throws Throwable
   */
  public abstract RegionScanner getRegionScanner(final Scan scan, final RegionScanner s) throws Throwable;

  /**
   * Return wrapped scanner that catches unexpected exceptions (i.e. Phoenix bugs) and
   * re-throws as DoNotRetryIOException to prevent needless retrying hanging the query
   * for 30 seconds. Unfortunately, until HBASE-7481 gets fixed, there's no way to do
   * the same from a custom filter.
   * @param arrayKVRefs
   * @param arrayFuncRefs
   * @param offset starting position in the rowkey.
   * @param scan
   * @param tupleProjector
   * @param dataRegion
   * @param indexMaintainer
   * @param tx current transaction
   * @param viewConstants
   */
  public RegionScanner getWrappedScanner(final RegionCoprocessorEnvironment env,
      final RegionScanner s, final Set<KeyValueColumnExpression> arrayKVRefs,
      final Expression[] arrayFuncRefs, final int offset, final Scan scan,
      final ColumnReference[] dataColumns, final TupleProjector tupleProjector,
      final Region dataRegion, final IndexMaintainer indexMaintainer,
      PhoenixTransactionContext tx,
      final byte[][] viewConstants, final KeyValueSchema kvSchema,
      final ValueBitSet kvSchemaBitSet, final TupleProjector projector,
      final ImmutableBytesWritable ptr, final boolean useQualifierAsListIndex) {
    return new RegionScanner() {

      private RegionInfo regionInfo = env.getRegionInfo();
      private byte[] actualStartKey = getActualStartKey();
      private boolean useNewValueColumnQualifier = EncodedColumnsUtil.useNewValueColumnQualifier(scan);
      final long pageSizeMs = getPageSizeMsForRegionScanner(scan);

      // Get the actual scan start row of local index. This will be used to compare the row
      // key of the results less than scan start row when there are references.
      public byte[] getActualStartKey() {
        return ScanUtil.isLocalIndex(scan) ? ScanUtil.getActualStartRow(scan, regionInfo)
            : null;
      }

      @Override
      public boolean next(List<Cell> results) throws IOException {
        try {
          boolean next = s.next(results);
          if (isDummy(results)) {
            return true;
          }
          return next;
        } catch (Throwable t) {
          ServerUtil.throwIOException(getRegion().getRegionInfo().getRegionNameAsString(), t);
          return false; // impossible
        }
      }

      @Override
      public boolean next(List<Cell> result, ScannerContext scannerContext) throws IOException {
          throw new IOException("Next with scannerContext should not be called in Phoenix environment");
      }

      @Override
      public void close() throws IOException {
        s.close();
      }

      @Override
      public RegionInfo getRegionInfo() {
        return s.getRegionInfo();
      }

      @Override
      public boolean isFilterDone() throws IOException {
        return s.isFilterDone();
      }

      @Override
      public boolean reseek(byte[] row) throws IOException {
        return s.reseek(row);
      }

      @Override
      public long getMvccReadPoint() {
        return s.getMvccReadPoint();
      }

      @Override
      public boolean nextRaw(List<Cell> result) throws IOException {
        try {
          boolean next = s.nextRaw(result);
          if (isDummy(result)) {
            return true;
          }
          Cell arrayElementCell = null;
          if (result.size() == 0) {
            return next;
          }
          if (arrayFuncRefs != null && arrayFuncRefs.length > 0 && arrayKVRefs.size() > 0) {
            int arrayElementCellPosition = replaceArrayIndexElement(arrayKVRefs, arrayFuncRefs, result);
            arrayElementCell = result.get(arrayElementCellPosition);
          }
          if (ScanUtil.isLocalIndex(scan) && !ScanUtil.isAnalyzeTable(scan)) {
            if(actualStartKey!=null) {
              next = scanTillScanStartRow(s, arrayKVRefs, arrayFuncRefs, result,
                  null, arrayElementCell);
              if (result.isEmpty() || isDummy(result)) {
                return next;
              }
            }
            /* In the following, c is only used when data region is null.
            dataRegion will never be null in case of non-coprocessor call,
            therefore no need to refactor
             */
            IndexUtil.wrapResultUsingOffset(env, result, offset, dataColumns,
                tupleProjector, dataRegion, indexMaintainer, viewConstants, ptr);
          }
          if (projector != null) {
            Tuple toProject = useQualifierAsListIndex ? new PositionBasedResultTuple(result) :
                    new ResultTuple(Result.create(result));

            Pair<Tuple, byte[]> mergedTupleDynColsPair = getTupleWithDynColsIfRequired(result,
                    projector.projectResults(toProject, useNewValueColumnQualifier));
            Tuple tupleWithDynColsIfReqd = mergedTupleDynColsPair.getFirst();
            byte[] serializedDynColsList = mergedTupleDynColsPair.getSecond();

            result.clear();
            result.add(tupleWithDynColsIfReqd.mergeWithDynColsListBytesAndGetValue(0,
                    serializedDynColsList));
            if (arrayElementCell != null) {
              result.add(arrayElementCell);
            }
          }
          // There is a scanattribute set to retrieve the specific array element
          return next;
        } catch (Throwable t) {
          ServerUtil.throwIOException(getRegion().getRegionInfo().getRegionNameAsString(), t);
          return false; // impossible
        }
      }

      /**
       * Iterate over the list of cells returned from the scan and use the dynamic column metadata
       * to create a tuple projector for dynamic columns. Finally, merge this with the projected
       * values corresponding to the known columns
       * @param result list of cells returned from the scan
       * @param tuple projected value tuple from known schema/columns
       * @return A pair, whose first part is a combined projected value tuple containing the
       * known column values along with resolved dynamic column values and whose second part is
       * the serialized list of dynamic column PColumns. In case dynamic columns are not
       * to be exposed or are not present, this returns the original tuple and an empty byte array.
       * @throws IOException Thrown if there is an error parsing protobuf or merging projected
       * values
       */
      private Pair<Tuple, byte[]> getTupleWithDynColsIfRequired(List<Cell> result, Tuple tuple)
        throws IOException {
        // We only care about dynamic column cells if the scan has this attribute set
        if (Bytes.equals(scan.getAttribute(WILDCARD_SCAN_INCLUDES_DYNAMIC_COLUMNS), TRUE_BYTES)) {
          List<PColumn> dynCols = new ArrayList<>();
          List<Cell> dynColCells = new ArrayList<>();
          TupleProjector dynColTupleProj = TupleProjector.getDynamicColumnsTupleProjector(result,
              dynCols, dynColCells);
          if (dynColTupleProj != null) {
            Tuple toProject = useQualifierAsListIndex ? new PositionBasedResultTuple(dynColCells) :
                new ResultTuple(Result.create(dynColCells));
            Tuple dynColsProjectedTuple = dynColTupleProj
                .projectResults(toProject, useNewValueColumnQualifier);

            ValueBitSet destBitSet = projector.getValueBitSet();
            // In case we are not projecting any non-row key columns, the field count for the
            // current projector will be 0, so we simply use the dynamic column projector's
            // value bitset as the destination bitset.
            if (projector.getSchema().getFieldCount() == 0) {
              destBitSet = dynColTupleProj.getValueBitSet();
            }
            // Add dynamic column data at the end of the projected tuple
            Tuple mergedTuple = TupleProjector.mergeProjectedValue(
                (TupleProjector.ProjectedValueTuple)tuple, destBitSet, dynColsProjectedTuple,
                dynColTupleProj.getValueBitSet(), projector.getSchema().getFieldCount(),
                useNewValueColumnQualifier);

            // We send the serialized list of PColumns for dynamic columns back to the client
            // so that the client can process the corresponding projected values
            DynamicColumnMetaDataProtos.DynamicColumnMetaData.Builder dynColsListBuilder =
                DynamicColumnMetaDataProtos.DynamicColumnMetaData.newBuilder();
            for (PColumn dynCol : dynCols) {
              dynColsListBuilder.addDynamicColumns(PColumnImpl.toProto(dynCol));
            }
            return new Pair<>(mergedTuple, dynColsListBuilder.build().toByteArray());
          }
        }
        return new Pair<>(tuple, new byte[0]);
      }

      @Override
      public boolean nextRaw(List<Cell> result, ScannerContext scannerContext)
          throws IOException {
        boolean res = next(result);
        ScannerContextUtil.incrementSizeProgress(scannerContext, result);
        ScannerContextUtil.updateTimeProgress(scannerContext);
        return res;
      }

      /**
       * When there is a merge in progress while scanning local indexes we might get the key values less than scan start row.
       * In that case we need to scan until get the row key more or  equal to scan start key.
       * TODO try to fix this case in LocalIndexStoreFileScanner when there is a merge.
       */
      private boolean scanTillScanStartRow(final RegionScanner s,
          final Set<KeyValueColumnExpression> arrayKVRefs,
          final Expression[] arrayFuncRefs, List<Cell> result,
          ScannerContext scannerContext, Cell arrayElementCell) throws IOException {
        boolean next = true;
        Cell firstCell = result.get(0);
        long startTime = EnvironmentEdgeManager.currentTimeMillis();
        while (Bytes.compareTo(firstCell.getRowArray(), firstCell.getRowOffset(),
            firstCell.getRowLength(), actualStartKey, 0, actualStartKey.length) < 0) {
          if (EnvironmentEdgeManager.currentTimeMillis() - startTime >= pageSizeMs) {
            byte[] rowKey = CellUtil.cloneRow(result.get(0));
            result.clear();
            getDummyResult(rowKey, result);
            return true;
          }
          result.clear();
          if(scannerContext == null) {
            next = s.nextRaw(result);
          } else {
            next = s.nextRaw(result, scannerContext);
          }
          if (result.isEmpty()) {
            return next;
          }
          if (isDummy(result)) {
            return true;
          }
          if (arrayFuncRefs != null && arrayFuncRefs.length > 0 && arrayKVRefs.size() > 0) {
            int arrayElementCellPosition = replaceArrayIndexElement(arrayKVRefs, arrayFuncRefs, result);
            arrayElementCell = result.get(arrayElementCellPosition);
          }
          firstCell = result.get(0);
        }
        return next;
      }

      private int replaceArrayIndexElement(final Set<KeyValueColumnExpression> arrayKVRefs,
          final Expression[] arrayFuncRefs, List<Cell> result) {
        // make a copy of the results array here, as we're modifying it below
        MultiKeyValueTuple tuple = new MultiKeyValueTuple(ImmutableList.copyOf(result));
        // The size of both the arrays would be same?
        // Using KeyValueSchema to set and retrieve the value
        // collect the first kv to get the row
        Cell rowKv = result.get(0);
        for (KeyValueColumnExpression kvExp : arrayKVRefs) {
          if (kvExp.evaluate(tuple, ptr)) {
            ListIterator<Cell> itr = result.listIterator();
            while (itr.hasNext()) {
              Cell kv = itr.next();
              if (Bytes.equals(kvExp.getColumnFamily(), 0, kvExp.getColumnFamily().length,
                  kv.getFamilyArray(), kv.getFamilyOffset(), kv.getFamilyLength())
                  && Bytes.equals(kvExp.getColumnQualifier(), 0, kvExp.getColumnQualifier().length,
                  kv.getQualifierArray(), kv.getQualifierOffset(), kv.getQualifierLength())) {
                // remove the kv that has the full array values.
                itr.remove();
                break;
              }
            }
          }
        }
        byte[] value = kvSchema.toBytes(tuple, arrayFuncRefs,
            kvSchemaBitSet, ptr);
        // Add a dummy kv with the exact value of the array index
        result.add(new KeyValue(rowKv.getRowArray(), rowKv.getRowOffset(), rowKv.getRowLength(),
            QueryConstants.ARRAY_VALUE_COLUMN_FAMILY, 0, QueryConstants.ARRAY_VALUE_COLUMN_FAMILY.length,
            QueryConstants.ARRAY_VALUE_COLUMN_QUALIFIER, 0,
            QueryConstants.ARRAY_VALUE_COLUMN_QUALIFIER.length, HConstants.LATEST_TIMESTAMP,
            KeyValue.Type.codeToType(rowKv.getTypeByte()), value, 0, value.length));
        return getArrayCellPosition(result);
      }

      @Override
      public long getMaxResultSize() {
        return s.getMaxResultSize();
      }

      @Override
      public int getBatch() {
        return s.getBatch();
      }
    };
  }

    // PHOENIX-4791 Share position of array element cell
    public static int getArrayCellPosition(List<Cell> result) {
        return result.size() - 1;
    }
}
