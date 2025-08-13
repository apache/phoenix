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

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.HRegionLocation;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.phoenix.compile.ExplainPlanAttributes.ExplainPlanAttributesBuilder;
import org.apache.phoenix.query.QueryConstants;
import org.apache.phoenix.schema.tuple.ResultTuple;
import org.apache.phoenix.schema.tuple.Tuple;

/**
 * ResultIterator that returns segment scan start and end keys for a table. Each row contains the
 * start key and end key of a segment.
 */
public class SegmentResultIterator extends BaseResultIterator {

  private final Iterator<Segment> segmentIterator;
  private int key;

  public SegmentResultIterator(List<HRegionLocation> regions, int totalSegments) {
    this.segmentIterator = getSegments(regions, totalSegments).iterator();
    this.key = 0;
  }

  @Override
  public Tuple next() throws SQLException {
    if (!segmentIterator.hasNext()) {
      return null;
    }

    Segment segment = segmentIterator.next();

    byte[] startKey = segment.getStartKey();
    byte[] endKey = segment.getEndKey();

    byte[] rowKey = Bytes.toBytes(key);
    key++;

    List<Cell> cells = new ArrayList<>();
    cells.add(new KeyValue(rowKey, QueryConstants.DEFAULT_COLUMN_FAMILY_BYTES, Bytes.toBytes("0"),
      startKey));
    cells.add(
      new KeyValue(rowKey, QueryConstants.DEFAULT_COLUMN_FAMILY_BYTES, Bytes.toBytes("1"), endKey));

    Result result = Result.create(cells);
    return new ResultTuple(result);
  }

  public static class Segment {
    byte[] startKey;
    byte[] endKey;

    public Segment(byte[] startKey, byte[] endKey) {
      this.startKey = startKey;
      this.endKey = endKey;
    }

    public byte[] getStartKey() {
      return startKey;
    }

    public byte[] getEndKey() {
      return endKey;
    }
  }

  public static List<Segment> getSegments(List<HRegionLocation> regions, int numBuckets) {
    int size = regions.size();
    List<Segment> segments = new ArrayList<>();
    if (size <= numBuckets) {
      for (HRegionLocation region : regions) {
        segments.add(new Segment(region.getRegion().getStartKey(), region.getRegion().getEndKey()));
      }
    } else {
      int q = size / numBuckets;
      int r = size % numBuckets;

      int currentIndex = 0;
      for (int i = 0; i < numBuckets; i++) {
        int bucketSize = q + (i < r ? 1 : 0);
        HRegionLocation first = regions.get(currentIndex);
        HRegionLocation last = regions.get(currentIndex + bucketSize - 1);

        segments.add(new Segment(first.getRegion().getStartKey(), last.getRegion().getEndKey()));
        currentIndex += bucketSize;
      }
    }
    return segments;
  }

  @Override
  public void explain(List<String> planSteps) {
    planSteps.add("CLIENT SEGMENT SCAN");
  }

  @Override
  public void explain(List<String> planSteps,
    ExplainPlanAttributesBuilder explainPlanAttributesBuilder) {
    planSteps.add("CLIENT SEGMENT SCAN");
  }
}
