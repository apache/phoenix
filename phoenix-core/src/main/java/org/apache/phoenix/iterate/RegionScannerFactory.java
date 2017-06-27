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

import com.google.common.collect.ImmutableList;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.coprocessor.RegionCoprocessorEnvironment;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.regionserver.Region;
import org.apache.hadoop.hbase.regionserver.RegionScanner;
import org.apache.hadoop.hbase.regionserver.ScannerContext;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.phoenix.execute.TupleProjector;
import org.apache.phoenix.expression.Expression;
import org.apache.phoenix.expression.KeyValueColumnExpression;
import org.apache.phoenix.hbase.index.covered.update.ColumnReference;
import org.apache.phoenix.index.IndexMaintainer;
import org.apache.phoenix.query.QueryConstants;
import org.apache.phoenix.schema.KeyValueSchema;
import org.apache.phoenix.schema.PTable;
import org.apache.phoenix.schema.ValueBitSet;
import org.apache.phoenix.schema.tuple.*;
import org.apache.phoenix.util.IndexUtil;
import org.apache.phoenix.util.ScanUtil;
import org.apache.phoenix.util.ServerUtil;
import org.apache.tephra.Transaction;

import java.io.IOException;
import java.util.List;
import java.util.ListIterator;
import java.util.Set;

public abstract class RegionScannerFactory {

  protected RegionCoprocessorEnvironment env;
  protected boolean useNewValueColumnQualifier;
  protected PTable.QualifierEncodingScheme encodingScheme;

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
      Transaction tx,
      final byte[][] viewConstants, final KeyValueSchema kvSchema,
      final ValueBitSet kvSchemaBitSet, final TupleProjector projector,
      final ImmutableBytesWritable ptr, final boolean useQualifierAsListIndex) {
    return new RegionScanner() {

      private boolean hasReferences = checkForReferenceFiles();
      private HRegionInfo regionInfo = env.getRegionInfo();
      private byte[] actualStartKey = getActualStartKey();

      // If there are any reference files after local index region merge some cases we might
      // get the records less than scan start row key. This will happen when we replace the
      // actual region start key with merge region start key. This method gives whether are
      // there any reference files in the region or not.
      private boolean checkForReferenceFiles() {
        if(!ScanUtil.isLocalIndex(scan)) return false;
        for (byte[] family : scan.getFamilies()) {
          if (getRegion().getStore(family).hasReferences()) {
            return true;
          }
        }
        return false;
      }

      // Get the actual scan start row of local index. This will be used to compare the row
      // key of the results less than scan start row when there are references.
      public byte[] getActualStartKey() {
        return ScanUtil.isLocalIndex(scan) ? ScanUtil.getActualStartRow(scan, regionInfo)
            : null;
      }

      @Override
      public boolean next(List<Cell> results) throws IOException {
        try {
          return s.next(results);
        } catch (Throwable t) {
          ServerUtil.throwIOException(getRegion().getRegionInfo().getRegionNameAsString(), t);
          return false; // impossible
        }
      }

      @Override
      public boolean next(List<Cell> result, ScannerContext scannerContext) throws IOException {
        try {
          return s.next(result, scannerContext);
        } catch (Throwable t) {
          ServerUtil.throwIOException(getRegion().getRegionInfo().getRegionNameAsString(), t);
          return false; // impossible
        }
      }

      @Override
      public void close() throws IOException {
        s.close();
      }

      @Override
      public HRegionInfo getRegionInfo() {
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
          Cell arrayElementCell = null;
          if (result.size() == 0) {
            return next;
          }
          if (arrayFuncRefs != null && arrayFuncRefs.length > 0 && arrayKVRefs.size() > 0) {
            int arrayElementCellPosition = replaceArrayIndexElement(arrayKVRefs, arrayFuncRefs, result);
            arrayElementCell = result.get(arrayElementCellPosition);
          }
          if (ScanUtil.isLocalIndex(scan) && !ScanUtil.isAnalyzeTable(scan)) {
            if(hasReferences && actualStartKey!=null) {
              next = scanTillScanStartRow(s, arrayKVRefs, arrayFuncRefs, result,
                  null, arrayElementCell);
              if (result.isEmpty()) {
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
            Tuple toProject = useQualifierAsListIndex ? new PositionBasedResultTuple(result) : new ResultTuple(
                Result.create(result));
            Tuple tuple = projector.projectResults(toProject, useNewValueColumnQualifier);
            result.clear();
            result.add(tuple.getValue(0));
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

      @Override
      public boolean nextRaw(List<Cell> result, ScannerContext scannerContext)
          throws IOException {
        try {
          boolean next = s.nextRaw(result, scannerContext);
          Cell arrayElementCell = null;
          if (result.size() == 0) {
            return next;
          }
          if (arrayFuncRefs != null && arrayFuncRefs.length > 0 && arrayKVRefs.size() > 0) {
            int arrayElementCellPosition = replaceArrayIndexElement(arrayKVRefs, arrayFuncRefs, result);
            arrayElementCell = result.get(arrayElementCellPosition);
          }
          if ((offset > 0 || ScanUtil.isLocalIndex(scan))  && !ScanUtil.isAnalyzeTable(scan)) {
            if(hasReferences && actualStartKey!=null) {
              next = scanTillScanStartRow(s, arrayKVRefs, arrayFuncRefs, result,
                  scannerContext, arrayElementCell);
              if (result.isEmpty()) {
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
            Tuple toProject = useQualifierAsListIndex ? new PositionBasedMultiKeyValueTuple(result) : new ResultTuple(Result.create(result));
            Tuple tuple = projector.projectResults(toProject, useNewValueColumnQualifier);
            result.clear();
            result.add(tuple.getValue(0));
            if(arrayElementCell != null)
              result.add(arrayElementCell);
          }
          // There is a scanattribute set to retrieve the specific array element
          return next;
        } catch (Throwable t) {
          ServerUtil.throwIOException(getRegion().getRegionInfo().getRegionNameAsString(), t);
          return false; // impossible
        }
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
        while (Bytes.compareTo(firstCell.getRowArray(), firstCell.getRowOffset(),
            firstCell.getRowLength(), actualStartKey, 0, actualStartKey.length) < 0) {
          result.clear();
          if(scannerContext == null) {
            next = s.nextRaw(result);
          } else {
            next = s.nextRaw(result, scannerContext);
          }
          if (result.isEmpty()) {
            return next;
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
        return result.size() - 1;
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
}
