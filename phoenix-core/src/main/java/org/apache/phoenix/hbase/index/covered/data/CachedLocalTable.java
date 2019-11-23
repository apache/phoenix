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
import java.util.List;

import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.client.Mutation;
import org.apache.phoenix.hbase.index.covered.update.ColumnReference;
import org.apache.phoenix.hbase.index.util.ImmutableBytesPtr;
import org.apache.phoenix.hbase.index.util.IndexManagementUtil;

import java.util.HashMap;

public class CachedLocalTable implements LocalHBaseState {

    private final HashMap<ImmutableBytesPtr, List<Cell>> rowKeyPtrToCells;

    public CachedLocalTable(HashMap<ImmutableBytesPtr, List<Cell>> rowKeyPtrToCells) {
        this.rowKeyPtrToCells = rowKeyPtrToCells;
    }

    @Override
    public List<Cell> getCurrentRowState(
            Mutation mutation,
            Collection<? extends ColumnReference> columnReferences,
            boolean ignoreNewerMutations) throws IOException {
        byte[] rowKey = mutation.getRow();
        List<Cell> cells = this.rowKeyPtrToCells.get(new ImmutableBytesPtr(rowKey));

        if(cells == null || cells.isEmpty()) {
            return cells;
        }

        if(!ignoreNewerMutations) {
            return cells;
        }
        /**
         * because of previous {@link IndexManagementUtil#flattenMutationsByTimestamp}(which is called
         * in {@link IndexRegionObserver#groupMutations} or {@link Indexer#preBatchMutateWithExceptions}),
         * all cells in the mutation have the same rowKey and timestamp.
         */
        long timestamp =
                IndexManagementUtil.getMutationTimestampWhenAllCellTimestampIsSame(mutation);
        List<Cell> newCells = new ArrayList<Cell>();
        for(Cell cell : cells) {
            if(cell.getTimestamp() < timestamp ) {
                newCells.add(cell);
            }
        }
        return newCells;
    }

}
