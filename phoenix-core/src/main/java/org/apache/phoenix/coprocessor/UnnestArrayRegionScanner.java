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
package org.apache.phoenix.coprocessor;

import static org.apache.phoenix.util.EncodedColumnsUtil.getMinMaxQualifiersFromScan;

import java.io.IOException;
import java.sql.SQLException;
import java.util.List;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.client.RegionInfo;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.regionserver.RegionScanner;
import org.apache.hadoop.hbase.regionserver.ScannerContext;
import org.apache.hadoop.hbase.regionserver.ScannerContextUtil;
import org.apache.hadoop.hbase.util.Pair;
import org.apache.phoenix.expression.ColumnExpression;
import org.apache.phoenix.expression.function.ArrayElemRefExpression;
import org.apache.phoenix.iterate.RegionScannerResultIterator;
import org.apache.phoenix.iterate.UnnestArrayResultIterator;
import org.apache.phoenix.schema.PTable;
import org.apache.phoenix.schema.tuple.Tuple;

public class UnnestArrayRegionScanner implements RegionScanner {
    private RegionScanner s;
    private UnnestArrayResultIterator iterator;
    public UnnestArrayRegionScanner(List<ColumnExpression> unnestArrayKVRefs,
            ArrayElemRefExpression[] unnestArrayKVExprs, ImmutableBytesWritable ptr,RegionScanner s, Scan scan, PTable.QualifierEncodingScheme encodingScheme){
        this.s = s;
        Pair<Integer, Integer> minMaxQualifier =  getMinMaxQualifiersFromScan(scan);
        if( scan.getAttribute(BaseScannerRegionObserver.SPECIFIC_ARRAY_INDEX) != null){
             minMaxQualifier = null;
        }
        RegionScannerResultIterator rsResultIterator = new RegionScannerResultIterator(s, minMaxQualifier, encodingScheme);
        this.iterator = new UnnestArrayResultIterator(rsResultIterator,unnestArrayKVRefs,unnestArrayKVExprs,ptr);
    }
    @Override public RegionInfo getRegionInfo() {
       return s.getRegionInfo();
    }

    @Override public boolean isFilterDone() throws IOException {
        return s.isFilterDone();
    }

    @Override public boolean reseek(byte[] bytes) throws IOException {
        return s.reseek(bytes);
    }

    @Override public long getMaxResultSize() {
        return s.getMaxResultSize();
    }

    @Override public long getMvccReadPoint() {
        return s.getMvccReadPoint();
    }

    @Override public int getBatch() {
        return s.getBatch();
    }

    @Override public boolean nextRaw(List<Cell> result) throws IOException {
        try {
            Tuple tuple = iterator.next();
            if(tuple != null) {
                getCells(tuple, result);
                return true;
            }else{
                return false;
            }
        }catch(SQLException e){
            //throw e;
            return false;
        }
    }

    @Override public boolean nextRaw(List<Cell> result, ScannerContext scannerContext)
            throws IOException {
        boolean res = next(result);
        ScannerContextUtil.incrementSizeProgress(scannerContext, result);
        ScannerContextUtil.updateTimeProgress(scannerContext);
        return res;
    }

    @Override public boolean next(List<Cell> list, ScannerContext scannerContext)
            throws IOException {
        throw new IOException("Next with scannerContext should not be called in Phoenix environment");
    }

    @Override public void close() throws IOException {
        s.close();
    }

    private void getCells(Tuple tuple, List<Cell> cells){
        for(int i = 0; i < tuple.size();i++){
            cells.add(tuple.getValue(i));
        }
    }
}
