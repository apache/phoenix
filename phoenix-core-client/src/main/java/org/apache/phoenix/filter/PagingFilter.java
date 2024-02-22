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
 * This is a top level Phoenix filter which is injected to a scan at the server side. If the scan has
 * already a filter then PagingFilter wraps it. This filter for server paging. It makes sure that
 * the scan does not take more than pageSizeInMs.
 */
public class PagingFilter extends FilterBase implements Writable {
    private enum State {
        INITIAL, STARTED, TIME_TO_STOP, STOPPED
    }
    State state;
    private long pageSizeMs;
    private long startTime;
    // tracks the row we last visited
    private Cell currentCell;
    private Filter delegate = null;

    public PagingFilter() {
        init();
    }

    public PagingFilter(Filter delegate, long pageSizeMs) {
        init();
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
        return state == State.STOPPED;
    }

    public void init() {
        state = State.INITIAL;
        currentCell = null;
    }

    @Override
    public void reset() throws IOException {
        long currentTime = EnvironmentEdgeManager.currentTimeMillis();
        // reset can be called multiple times for the same row sometimes even before we have
        // scanned even one row. The order in which it is called is not very predictable.
        // So we need to ensure that we have seen at least one row before we page.
        // The currentCell != null check ensures that.
        if (state == State.STARTED && currentCell != null
                && currentTime - startTime >= pageSizeMs) {
            state = State.TIME_TO_STOP;
        }
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
    public boolean filterRowKey(Cell cell) throws IOException {
        currentCell = cell;
        if (delegate != null) {
            return delegate.filterRowKey(cell);
        }
        return super.filterRowKey(cell);
    }

    @Override
    public boolean filterAllRemaining() throws IOException {
        if (state == State.TIME_TO_STOP) {
            state = State.STOPPED;
            return true;
        }
        if (state == State.STOPPED) {
            return true;
        }
        if (delegate != null) {
            return delegate.filterAllRemaining();
        }
        return super.filterAllRemaining();
    }

    @Override
    /**
     * This is called once for every row in the beginning.
     */
    public boolean hasFilterRow() {
        if (state == State.INITIAL) {
            startTime = EnvironmentEdgeManager.currentTimeMillis();
            state = State.STARTED;
        }
        return true;
    }

    @Override
    public boolean filterRow() throws IOException {
        if (state == State.TIME_TO_STOP) {
            return true;
        }
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
        currentCell = c;
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
