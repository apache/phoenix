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

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.List;

import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.DoNotRetryIOException;
import org.apache.hadoop.hbase.exceptions.DeserializationException;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.filter.FilterBase;
import org.apache.hadoop.hbase.util.Writables;
import org.apache.hadoop.io.Writable;
import org.apache.phoenix.util.EnvironmentEdgeManager;

/**
 * This is a top level Phoenix filter which is injected to a scan at the server side. If the scan
 * already has a filter then PagingFilter wraps it. This filter is for server pagination. It makes
 * sure that the scan does not take more than pageSizeInMs.
 *
 * PagingRegionScanner initializes PagingFilter before retrieving a row. The state of PagingFilter
 * consists of three variables startTime, isStopped, and currentCell. During this
 * initialization, starTime is set to the current time, isStopped to false, and currentCell to null.
 *
 * PagingFilter implements the paging state machine in three filter methods that are
 * hasFilterRow(), filterAllRemaining(), and filterRowKey(). These methods are called in the
 * following order for each row: hasFilterRow(), filterAllRemaining(), filterRowKey(), and
 * filterAllRemaining(). Please note that filterAllRemaining() is called twice (before and after
 * filterRowKey()). Sometimes, filterAllRemaining() is called multiple times back to back.
 *
 * In hasFilterRow(), if currentCell is not null, meaning that at least one row has been
 * scanned, and it is time to page out, then PagingFilter sets isStopped to true.
 *
 * In filterAllRemaining(), PagingFilter returns true if isStopped is true. Returning true from this
 * method causes the HBase region scanner to signal the caller (that is PagingRegionScanner in this
 * case) that there are no more rows to scan by returning false from the next() call. In that case,
 * PagingRegionScanner checks if PagingFilter is stopped. If PagingFilter is stopped, then it means
 * the last next() call paged out rather than the scan operation reached at its last row.
 * Please note it is crucial that PagingFilter returns true in the first filterAllRemaining() call
 * for a given row. This allows to the HBase region scanner to resume the scanning rows when the
 * next() method is called even though the region scanner already signaled the caller that there
 * were no more rows to scan. PagingRegionScanner leverages this behavior to resume the scan
 * operation using the same scanner instead closing the current one and starting a new scanner. If
 * this specific HBase region scanner behavior changes, it will cause server paging test failures.
 * To fix them, the PagingRegionScanner code needs to change such that PagingRegionScanner needs to
 * create a new scanner with adjusted start row to resume the scan operation after PagingFilter
 * stops.
 *
 * If the scan operation has not been terminated by PageFilter, HBase subsequently calls
 * filterRowKey(). In this method, PagingFilter records the last row that is scanned.
 *
 */
public class PagingFilter extends FilterBase implements Writable {
    private long pageSizeMs;
    private long startTime;
    // tracks the row we last visited
    private Cell currentCell;
    private boolean isStopped;
    private Filter delegate = null;

    public PagingFilter() {
    }

    public PagingFilter(Filter delegate, long pageSizeMs) {
        this.delegate = delegate;
        this.pageSizeMs = pageSizeMs;
    }

    public Filter getDelegateFilter() {
        return delegate;
    }

    public void setDelegateFilter (Filter delegate) {
        this.delegate = delegate;
    }

    public byte[] getCurrentRowKeyToBeExcluded() {
        byte[] rowKeyAtStop = null;
        if (currentCell != null) {
            rowKeyAtStop = CellUtil.cloneRow(currentCell);
        }
        return rowKeyAtStop;
    }

    public boolean isStopped() {
        return isStopped;
    }

    public void init() {
        isStopped = false;
        currentCell = null;
        startTime = EnvironmentEdgeManager.currentTimeMillis();
    }

    @Override
    public boolean hasFilterRow() {
        if (currentCell != null
                && EnvironmentEdgeManager.currentTimeMillis() - startTime >= pageSizeMs) {
            isStopped = true;
        }
        return true;
    }

    @Override
    public boolean filterAllRemaining() throws IOException {
        if (isStopped) {
            return true;
        }
        if (delegate != null) {
            return delegate.filterAllRemaining();
        }
        return super.filterAllRemaining();
    }

    @Override
    public boolean filterRowKey(Cell cell) throws IOException {
        currentCell = cell;
        if (delegate != null) {
            return delegate.filterRowKey(cell);
        }
        return super.filterRowKey(cell);
    }

    @Override
    public void reset() throws IOException {
        if (delegate != null) {
            delegate.reset();
            return;
        }
        super.reset();
    }

    @Override
    public Cell getNextCellHint(Cell currentKV) throws IOException {
        if (delegate != null) {
            return delegate.getNextCellHint(currentKV);
        }
        return super.getNextCellHint(currentKV);
    }

    @Override
    public boolean filterRow() throws IOException {
        if (delegate != null) {
            return delegate.filterRow();
        }
        return super.filterRow();
    }

    @Override
    public Cell transformCell(Cell v) throws IOException {
        if (delegate != null) {
            return delegate.transformCell(v);
        }
        return super.transformCell(v);
    }

    @Override
    public void filterRowCells(List<Cell> kvs) throws IOException {
        if (delegate != null) {
            delegate.filterRowCells(kvs);
            return;
        }
        super.filterRowCells(kvs);
    }

    @Override
    public void setReversed(boolean reversed) {
        if (delegate != null) {
            delegate.setReversed(reversed);
        }
        super.setReversed(reversed);
    }

    @Override
    public boolean isReversed() {
        if (delegate != null) {
            return delegate.isReversed();
        }
        return super.isReversed();
    }

    @Override
    public boolean isFamilyEssential(byte[] name) throws IOException {
        if (delegate != null) {
            return delegate.isFamilyEssential(name);
        }
        return super.isFamilyEssential(name);
    }

    @Override
    public ReturnCode filterKeyValue(Cell v) throws IOException {
        if (delegate != null) {
            return delegate.filterKeyValue(v);
        }
        return super.filterKeyValue(v);
    }

    @Override
    public Filter.ReturnCode filterCell(Cell c) throws IOException {
        if (delegate != null) {
            return delegate.filterCell(c);
        }
        return super.filterCell(c);
    }

    public static PagingFilter parseFrom(final byte [] pbBytes) throws DeserializationException {
        try {
            return (PagingFilter) Writables.getWritable(pbBytes, new PagingFilter());
        } catch (IOException e) {
            throw new DeserializationException(e);
        }
    }

    @Override
    public void write(DataOutput out) throws IOException {
        out.writeLong(pageSizeMs);
        if (delegate != null) {
            out.writeUTF(delegate.getClass().getName());
            byte[] b = delegate.toByteArray();
            out.writeInt(b.length);
            out.write(b);
        } else {
            out.writeUTF("");
        }
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        pageSizeMs = in.readLong();
        String className = in.readUTF();
        if (className.length() == 0) {
            return;
        }
        Class cls = null;
        try {
            cls = Class.forName(className);
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
            throw new DoNotRetryIOException(e);
        }

        Method m = null;
        try {
            m = cls.getDeclaredMethod("parseFrom", byte[].class);
        } catch (NoSuchMethodException e) {
            e.printStackTrace();
            throw new DoNotRetryIOException(e);
        }
        int length = in.readInt();
        byte[] b = new byte[length];
        in.readFully(b);
        try {
            delegate = (Filter) m.invoke(null, b);
        } catch (IllegalAccessException e) {
            e.printStackTrace();
            throw new DoNotRetryIOException(e);
        } catch (InvocationTargetException e) {
            e.printStackTrace();
            throw new DoNotRetryIOException(e);
        }
    }

    @Override
    public byte[] toByteArray() throws IOException {
        return Writables.getBytes(this);
    }
}
