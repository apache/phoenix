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
package org.apache.phoenix.replication;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Mutation;
import org.apache.hadoop.hbase.client.Put;

/**
 * Groups a flat cell stream into Put/Delete mutations, mirroring the algorithm HBase's
 * ReplicationSink uses to reconstruct mutations from a WALEdit. A new mutation is started whenever
 * the row key or the put-vs-delete disposition differs from the immediately preceding cell;
 * consecutive cells sharing both are collected into one mutation. There is no precondition on the
 * input ordering: any cell stream produces valid mutations. Ordering only affects how the cells are
 * partitioned into Mutation objects (a row that recurs non-consecutively yields a separate mutation
 * per run), not correctness -- cell order is preserved, so replaying the resulting mutations in
 * order reproduces the effect of applying the input cells in order.
 */
public final class MutationCellGrouper {

  private MutationCellGrouper() {
  }

  private static boolean isNewRowOrType(Cell previousCell, Cell cell) {
    return previousCell == null || previousCell.getType() != cell.getType()
      || !CellUtil.matchingRows(previousCell, cell);
  }

  /** Group a cell stream into Put/Delete mutations using the row+type boundary algorithm. */
  public static List<Mutation> splitCellsIntoMutations(Iterable<Cell> cells) throws IOException {
    List<Mutation> result = new ArrayList<>();
    Cell previousCell = null;
    Mutation current = null;
    for (Cell cell : cells) {
      if (isNewRowOrType(previousCell, cell)) {
        if (current != null) {
          result.add(current);
        }
        if (CellUtil.isDelete(cell)) {
          current = new Delete(cell.getRowArray(), cell.getRowOffset(), cell.getRowLength());
        } else {
          current = new Put(cell.getRowArray(), cell.getRowOffset(), cell.getRowLength());
        }
      }
      if (CellUtil.isDelete(cell)) {
        ((Delete) current).add(cell);
      } else {
        ((Put) current).add(cell);
      }
      previousCell = cell;
    }
    if (current != null) {
      result.add(current);
    }
    return result;
  }

}
