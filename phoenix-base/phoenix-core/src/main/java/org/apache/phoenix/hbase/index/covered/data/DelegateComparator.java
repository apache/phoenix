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

import java.util.Comparator;

import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellComparator;

public class DelegateComparator implements CellComparator {
    
    private CellComparator delegate;

    public DelegateComparator(CellComparator delegate) {
        this.delegate=delegate;
    }

    @Override
    public int compare(Cell leftCell, Cell rightCell) {
        return delegate.compare(leftCell, rightCell);
    }

    @Override
    public int compareRows(Cell leftCell, Cell rightCell) {
        return delegate.compareRows(leftCell, rightCell);
    }

    @Override
    public int compareRows(Cell cell, byte[] bytes, int offset, int length) {
        return delegate.compareRows(cell, bytes, offset, length);
    }

    @Override
    public int compareWithoutRow(Cell leftCell, Cell rightCell) {
        return delegate.compareWithoutRow(leftCell, rightCell);
    }

    @Override
    public int compareFamilies(Cell leftCell, Cell rightCell) {
        return delegate.compareFamilies(leftCell, rightCell);
    }

    @Override
    public int compareQualifiers(Cell leftCell, Cell rightCell) {
        return delegate.compareQualifiers(leftCell, rightCell);
    }

    @Override
    public int compareTimestamps(Cell leftCell, Cell rightCell) {
        return delegate.compareTimestamps(leftCell, rightCell);
    }

    @Override
    public int compareTimestamps(long leftCellts, long rightCellts) {
        return delegate.compareTimestamps(leftCellts, rightCellts);
    }

    @Override
    public int compare(Cell leftCell, Cell rightCell, boolean ignoreSequenceid) {
        return delegate.compare(leftCell, rightCell, ignoreSequenceid);
    }

    @Override
    public Comparator getSimpleComparator() {
        return delegate.getSimpleComparator();
    }

}
