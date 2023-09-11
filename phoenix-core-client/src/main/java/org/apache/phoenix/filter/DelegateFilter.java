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
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.filter.FilterBase;

public class DelegateFilter extends FilterBase {

    protected Filter delegate = null;

    public DelegateFilter(Filter delegate) {
        this.delegate = delegate;
    }

    @Override
    public void reset() throws IOException {
        delegate.reset();
    }

    @Override
    public boolean filterRowKey(byte[] buffer, int offset, int length) throws IOException {
        return delegate.filterRowKey(buffer, offset, length);
    }

    @Override
    public boolean filterRowKey(Cell cell) throws IOException {
        return delegate.filterRowKey(cell);
    }

    @Override
    public ReturnCode filterCell(Cell v) throws IOException {
        return delegate.filterCell(v);
    }

    @Override
    public boolean filterAllRemaining() throws IOException {
        return delegate.filterAllRemaining();
    }

    @Override
    public ReturnCode filterKeyValue(Cell v) throws IOException {
        return delegate.filterKeyValue(v);
    }

    @Override
    public Cell transformCell(Cell v) throws IOException {
        return delegate.transformCell(v);
    }

    @Override
    public void filterRowCells(List<Cell> kvs) throws IOException {
        delegate.filterRowCells(kvs);
    }

    @Override
    public boolean hasFilterRow() {
        return delegate.hasFilterRow();
    }

    @Override
    public boolean filterRow() throws IOException {
        return delegate.filterRow();
    }

    @Override
    public Cell getNextCellHint(Cell currentKV) throws IOException {
        return delegate.getNextCellHint(currentKV);
    }

    @Override
    public boolean isFamilyEssential(byte[] name) throws IOException {
        return delegate.isFamilyEssential(name);
    }

    @Override
    public byte[] toByteArray() throws IOException {
        return delegate.toByteArray();
    }

    @Override
    public String toString() {
        return delegate.toString();
    }

    @Override
    public void setReversed(boolean reversed) {
        delegate.setReversed(reversed);
    }

    @Override
    public boolean isReversed() {
        return delegate.isReversed();
    }
}
