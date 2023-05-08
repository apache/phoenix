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
package org.apache.phoenix.filter;

import java.io.IOException;
import java.util.List;

import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hbase.thirdparty.com.google.common.base.Preconditions;
import org.apache.phoenix.util.ScanUtil;

import static org.apache.phoenix.query.QueryConstants.VERIFIED_BYTES;

/**
 * This filter overrides the behavior of delegate so that we do not jump to
 * the next row if the row is unverified and doesn't match the filter since
 * it is possible that a previous verified version of the same row could match
 * the filter and thus should be included in the results.
 * For tables using encoded columns, the empty column is the first column the
 * filter processes, so we can check whether it is verified or not.
 * If no encoding is used, the empty column is the last column to be processed
 * by the filter, so we have to wait to determine whether the row is verified
 * or not.
 */
public class UnverifiedRowFilter extends DelegateFilter {

    private final byte[] emptyCF;
    private final byte[] emptyCQ;
    private boolean verified = false;
    // save the code from delegate filter while waiting for the empty column
    private ReturnCode recordedRetCode = null;

    private void init() {
        verified = false;
        recordedRetCode = null;
    }
    public UnverifiedRowFilter(Filter delegate, byte[] emptyCF, byte[] emptyCQ) {
        super(delegate);
        Preconditions.checkArgument(emptyCF != null,
                "Column family must not be null");
        Preconditions.checkArgument(emptyCQ != null,
                "Column qualifier must not be null");
        this.emptyCF = emptyCF;
        this.emptyCQ = emptyCQ;
        init();
    }

    @Override
    public void reset() throws IOException {
        init();
        delegate.reset();
    }

    @Override
    public ReturnCode filterKeyValue(Cell v) throws IOException {
        return filterCell(v);
    }

    @Override
    public ReturnCode filterCell(final Cell cell) throws IOException {
        if (verified) {
            // we have processed the empty column and found that it is verified
            return delegate.filterCell(cell);
        }

        if (ScanUtil.isEmptyColumn(cell, emptyCF, emptyCQ)) {
            verified = Bytes.compareTo(
                    cell.getValueArray(), cell.getValueOffset(), cell.getValueLength(),
                    VERIFIED_BYTES, 0, VERIFIED_BYTES.length) == 0;
            if (verified) {
                // if we saved the return code while waiting for the empty
                // column, use that code else call the delegate
                return recordedRetCode != null ? recordedRetCode : delegate.filterCell(cell);
            } else {
                // it is an unverified row, no need to look at more columns
                // include it so that it can be repaired and evaluated again by the filter
                return ReturnCode.INCLUDE_AND_SEEK_NEXT_ROW;
            }
        }
        // we haven't seen the empty column yet so don't know whether
        // the row is verified or not

        if (recordedRetCode != null) {
            // we already have recorded the return code from the wrapped
            // delegate filter so skip this column
            return ReturnCode.NEXT_COL;
        }
        ReturnCode ret = delegate.filterCell(cell);
        if (ret == ReturnCode.NEXT_ROW
                || ret == ReturnCode.INCLUDE_AND_SEEK_NEXT_ROW) {
            // Save the return code but don't move to the next row.
            // Continue processing the current row till we find the empty column
            recordedRetCode = ret;
            ret = ReturnCode.NEXT_COL;
        }
        return ret;
    }

    @Override
    public void filterRowCells(List<Cell> kvs) throws IOException {
        if (verified) {
            delegate.filterRowCells(kvs);
        }
    }

    @Override
    public boolean filterRow() throws IOException {
        if (verified) {
            return delegate.filterRow();
        }
        return false;
    }
}
