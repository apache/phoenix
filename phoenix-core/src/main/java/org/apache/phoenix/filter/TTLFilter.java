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
import org.apache.hadoop.hbase.DoNotRetryIOException;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.exceptions.DeserializationException;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.filter.FilterBase;
import org.apache.hadoop.hbase.util.Writables;
import org.apache.hadoop.io.Writable;
import org.apache.phoenix.util.EnvironmentEdgeManager;
import org.apache.phoenix.util.ScanUtil;

/**
 * This is a filter which is injected to a scan at the server side. If the scan has
 * already a filter then TTLFilter wraps it.  This filter is for masking expired rows.
 */
public class TTLFilter extends FilterBase implements Writable {
    private Filter delegate = null;
    private byte[] emptyCQ;
    private long ttlWindowStartMs;
    private boolean expired = false;
    private ReturnCode returnCodeWhenExpired;

    public TTLFilter() {}
    public TTLFilter(Filter delegate,  byte[] emptyCQ, long ttlWindowStartMs) {
        this.delegate = delegate;
        this.emptyCQ = emptyCQ;
        this.ttlWindowStartMs = ttlWindowStartMs;
    }

    @Override
    public void reset() throws IOException {
        expired = false;
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

    public boolean filterRowKey(byte[] buffer, int offset, int length) throws IOException {
        if (delegate != null) {
            return delegate.filterRowKey(buffer, offset, length);
        }
        return super.filterRowKey(buffer, offset, length);
    }

    @Override
    public boolean filterRowKey(Cell cell) throws IOException {
        if (delegate != null) {
            return delegate.filterRowKey(cell);
        }
        return super.filterRowKey(cell);
    }

    @Override
    public boolean filterAllRemaining() throws IOException {
        if (delegate != null) {
            return delegate.filterAllRemaining();
        }
        return super.filterAllRemaining();
    }

    @Override
    public boolean hasFilterRow() {
        if (delegate != null) {
            return delegate.hasFilterRow();
        }
        return super.hasFilterRow();
    }

    @Override
    public boolean filterRow() throws IOException {
        if (expired) {
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

    private ReturnCode skipExpiredRow(Cell c, ReturnCode rc) {
        if (!expired &&
                (!ScanUtil.isEmptyColumn(c, emptyCQ) || c.getTimestamp() >= ttlWindowStartMs)) {
            return rc;
        }
        expired = true;
        returnCodeWhenExpired = rc == ReturnCode.INCLUDE_AND_SEEK_NEXT_ROW ?
             ReturnCode.SEEK_NEXT_USING_HINT : ReturnCode.NEXT_ROW;
        return returnCodeWhenExpired;
    }

    @Override
    public ReturnCode filterKeyValue(Cell v) throws IOException {
        if (expired) {
            return returnCodeWhenExpired;
        }
        ReturnCode rc;
        if (delegate != null) {
            rc = delegate.filterKeyValue(v);
        } else {
            rc = super.filterKeyValue(v);
        }
        return skipExpiredRow(v, rc);
    }

    @Override
    public ReturnCode filterCell(Cell c) throws IOException {
        if (expired) {
            return returnCodeWhenExpired;
        }
        ReturnCode rc;
        if (delegate != null) {
            rc = delegate.filterCell(c);
        } else {
            rc = super.filterCell(c);
        }
        return skipExpiredRow(c, rc);
    }

    public static TTLFilter parseFrom(final byte [] pbBytes) throws DeserializationException {
        try {
            return (TTLFilter) Writables.getWritable(pbBytes, new TTLFilter());
        } catch (IOException e) {
            throw new DeserializationException(e);
        }
    }

    @Override
    public void write(DataOutput out) throws IOException {
        out.writeLong(ttlWindowStartMs);
        out.writeInt(emptyCQ.length);
        out.write(emptyCQ);
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
        ttlWindowStartMs = in.readLong();
        int length = in.readInt();
        emptyCQ = new byte[length];
        in.readFully(emptyCQ, 0, length);
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
        length = in.readInt();
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
