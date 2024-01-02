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
package org.apache.phoenix.util;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.ListIterator;

import org.apache.hadoop.hbase.ArrayBackedTag;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellScanner;
import org.apache.hadoop.hbase.PhoenixTagType;
import org.apache.hadoop.hbase.PrivateCellUtil;
import org.apache.hadoop.hbase.RawCell;
import org.apache.hadoop.hbase.Tag;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Durability;
import org.apache.hadoop.hbase.client.Mutation;
import org.apache.hadoop.hbase.regionserver.MiniBatchOperationInProgress;
import org.apache.hadoop.hbase.regionserver.Region;
import org.apache.phoenix.hbase.index.OffsetCell;
import org.apache.phoenix.query.QueryServices;

public class ServerIndexUtil {
    public static void writeLocalUpdates(Region region, final List<Mutation> mutations, boolean skipWAL) throws IOException {
        if(skipWAL) {
            for (Mutation m : mutations) {
                m.setDurability(Durability.SKIP_WAL);
            }
        }
        region.batchMutate(
                mutations.toArray(new Mutation[mutations.size()]));
    }

    public static void wrapResultUsingOffset(List<Cell> result, final int offset) throws IOException {
        ListIterator<Cell> itr = result.listIterator();
        while (itr.hasNext()) {
            final Cell cell = itr.next();
            // TODO: Create DelegateCell class instead
            Cell newCell = new OffsetCell(cell, offset);
            itr.set(newCell);
        }
    }

    /**
     * Set Cell Tags to delete markers with source of operation attribute.
     * @param miniBatchOp miniBatchOp
     * @throws IOException IOException
     */
    public static void setDeleteAttributes(
            MiniBatchOperationInProgress<Mutation> miniBatchOp)
            throws IOException {
        for (int i = 0; i < miniBatchOp.size(); i++) {
            Mutation m = miniBatchOp.getOperation(i);
            if (!(m instanceof Delete)) {
                // Ignore if it is not Delete type.
                continue;
            }
            byte[] sourceOpAttr =
                    m.getAttribute(QueryServices.SOURCE_OPERATION_ATTRIB);
            if (sourceOpAttr == null) {
                continue;
            }
            Tag sourceOpTag = new ArrayBackedTag(
                    PhoenixTagType.SOURCE_OPERATION_TAG_TYPE, sourceOpAttr);
            List<Cell> updatedCells = new ArrayList<>();
            for (CellScanner cellScanner = m.cellScanner();
                 cellScanner.advance();) {
                Cell cell = cellScanner.current();
                RawCell rawCell = (RawCell) cell;
                List<Tag> tags = new ArrayList<>();
                Iterator<Tag> tagsIterator = rawCell.getTags();
                while (tagsIterator.hasNext()) {
                    tags.add(tagsIterator.next());
                }
                tags.add(sourceOpTag);
                // TODO: PrivateCellUtil's IA is Private.
                // HBASE-25328 adds a builder methodfor creating Tag which
                // will be LP with IA.coproc
                Cell updatedCell = PrivateCellUtil.createCell(cell, tags);
                updatedCells.add(updatedCell);
            }
            m.getFamilyCellMap().clear();
            // Clear and add new Cells to the Mutation.
            for (Cell cell : updatedCells) {
                Delete d = (Delete) m;
                d.add(cell);
            }
        }
    }
}
