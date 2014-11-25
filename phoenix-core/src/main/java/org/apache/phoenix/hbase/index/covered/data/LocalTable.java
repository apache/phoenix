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
package org.apache.phoenix.hbase.index.covered.data;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;

import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.Mutation;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.coprocessor.RegionCoprocessorEnvironment;
import org.apache.hadoop.hbase.regionserver.HRegion;
import org.apache.hadoop.hbase.regionserver.RegionScanner;

import org.apache.phoenix.hbase.index.covered.update.ColumnReference;
import org.apache.phoenix.hbase.index.util.IndexManagementUtil;

/**
 * Wrapper around a lazily instantiated, local HTable.
 * <p>
 * Previously, we had used various row and batch caches. However, this ends up being very
 * complicated when attempting manage updating and invalidating the cache with no real gain as any
 * row accessed multiple times will likely be in HBase's block cache, invalidating any extra caching
 * we are doing here. In the end, its simpler and about as efficient to just get the current state
 * of the row from HBase and let HBase manage caching the row from disk on its own.
 */
public class LocalTable implements LocalHBaseState {

  private RegionCoprocessorEnvironment env;

  public LocalTable(RegionCoprocessorEnvironment env) {
    this.env = env;
  }

  @Override
  public Result getCurrentRowState(Mutation m, Collection<? extends ColumnReference> columns)
      throws IOException {
    byte[] row = m.getRow();
    // need to use a scan here so we can get raw state, which Get doesn't provide.
    Scan s = IndexManagementUtil.newLocalStateScan(Collections.singletonList(columns));
    s.setStartRow(row);
    s.setStopRow(row);
    HRegion region = this.env.getRegion();
    RegionScanner scanner = region.getScanner(s);
    List<Cell> kvs = new ArrayList<Cell>(1);
    boolean more = scanner.next(kvs);
    assert !more : "Got more than one result when scanning" + " a single row in the primary table!";

    Result r = Result.create(kvs);
    scanner.close();
    return r;
  }
}