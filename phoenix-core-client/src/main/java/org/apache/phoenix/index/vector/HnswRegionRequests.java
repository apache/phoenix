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
package org.apache.phoenix.index.vector;

import java.io.IOException;
import java.util.List;
import java.util.function.Function;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.client.RegionInfo;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.filter.FirstKeyOnlyFilter;

/**
 * Sends HNSW requests, such as a segment build or a segment purge, to the regions of a data table.
 * A request is a scan of the key range of one region for at most one row, with a scan attribute
 * that names the request. The region acts on the request when the scan opens, and answers with one
 * cell.
 */
public final class HnswRegionRequests {

  /** Receives the answer of one region. */
  @FunctionalInterface
  public interface Answer {
    /**
     * @param region  the region of the request
     * @param value   the value of the cell that the region returned, or null
     * @param failure the failure of the request, or null
     * @throws IOException to end the requests with this failure
     */
    void accept(RegionInfo region, byte[] value, IOException failure) throws IOException;
  }

  private HnswRegionRequests() {
  }

  /**
   * Sends a request to each given region of the data table, one region at a time.
   * @param data      the data table
   * @param regions   the regions that get the request
   * @param attribute the scan attribute of the request
   * @param request   the value of the attribute for each region
   * @param answer    receives the answer or the failure of each region
   * @throws IOException if {@code answer} throws it
   */
  public static void send(Table data, List<RegionInfo> regions, String attribute,
    Function<RegionInfo, byte[]> request, Answer answer) throws IOException {
    for (RegionInfo region : regions) {
      Scan scan = new Scan().withStartRow(region.getStartKey()).withStopRow(region.getEndKey())
        .setLimit(1).setFilter(new FirstKeyOnlyFilter());
      scan.setAttribute(attribute, request.apply(region));
      byte[] value = null;
      IOException failure = null;
      try (ResultScanner scanner = data.getScanner(scan)) {
        Result result = scanner.next();
        if (result != null && !result.isEmpty()) {
          value = CellUtil.cloneValue(result.rawCells()[0]);
        }
      } catch (IOException e) {
        failure = e;
      }
      answer.accept(region, value, failure);
    }
  }
}
