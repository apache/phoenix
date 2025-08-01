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
package org.apache.phoenix.iterate;

import static org.apache.phoenix.util.ScanUtil.isDummy;

import java.io.IOException;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.regionserver.RegionScanner;
import org.apache.hadoop.hbase.regionserver.ScannerContext;
import org.apache.hadoop.hbase.util.Pair;
import org.apache.phoenix.schema.PTable.QualifierEncodingScheme;
import org.apache.phoenix.schema.tuple.EncodedColumnQualiferCellsList;
import org.apache.phoenix.schema.tuple.MultiKeyValueTuple;
import org.apache.phoenix.schema.tuple.PositionBasedMultiKeyValueTuple;
import org.apache.phoenix.schema.tuple.ResultTuple;
import org.apache.phoenix.schema.tuple.Tuple;
import org.apache.phoenix.util.ClientUtil;
import org.apache.phoenix.util.EncodedColumnsUtil;

public class RegionScannerResultIterator extends BaseResultIterator {
  private final RegionScanner scanner;
  private final Pair<Integer, Integer> minMaxQualifiers;
  private final boolean useQualifierAsIndex;
  private final QualifierEncodingScheme encodingScheme;
  private final ScannerContext regionScannerContext;

  public RegionScannerResultIterator(Scan scan, RegionScanner scanner,
    Pair<Integer, Integer> minMaxQualifiers, QualifierEncodingScheme encodingScheme) {
    this.scanner = scanner;
    this.useQualifierAsIndex = EncodedColumnsUtil.useQualifierAsIndex(minMaxQualifiers);
    this.minMaxQualifiers = minMaxQualifiers;
    this.encodingScheme = encodingScheme;
    if (scan.isScanMetricsEnabled()) {
      regionScannerContext =
        ScannerContext.newBuilder().setTrackMetrics(scan.isScanMetricsEnabled()).build();
    } else {
      regionScannerContext = null;
    }
  }

  @Override
  public Tuple next() throws SQLException {
    // XXX: No access here to the region instance to enclose this with startRegionOperation /
    // stopRegionOperation
    synchronized (scanner) {
      try {
        // TODO: size
        List<Cell> results = useQualifierAsIndex
          ? new EncodedColumnQualiferCellsList(minMaxQualifiers.getFirst(),
            minMaxQualifiers.getSecond(), encodingScheme)
          : new ArrayList<Cell>();
        // Results are potentially returned even when the return value of s.next is false
        // since this is an indication of whether or not there are more values after the
        // ones returned
        boolean hasMore;
        if (regionScannerContext == null) {
          hasMore = scanner.nextRaw(results);
        } else {
          hasMore = scanner.nextRaw(results, regionScannerContext);
        }

        if (!hasMore && results.isEmpty()) {
          return null;
        }
        if (isDummy(results)) {
          return new ResultTuple(Result.create(results));
        }
        // We instantiate a new tuple because in all cases currently we hang on to it
        // (i.e. to compute and hold onto the TopN).
        Tuple tuple =
          useQualifierAsIndex ? new PositionBasedMultiKeyValueTuple() : new MultiKeyValueTuple();
        tuple.setKeyValues(results);
        return tuple;
      } catch (IOException e) {
        throw ClientUtil.parseServerException(e);
      }
    }
  }

  public ScannerContext getRegionScannerContext() {
    return regionScannerContext;
  }

  @Override
  public String toString() {
    return "RegionScannerResultIterator [scanner=" + scanner + "]";
  }
}
