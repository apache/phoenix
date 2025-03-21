/**
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
package org.apache.phoenix.coprocessor;

import java.io.IOException;
import java.util.List;

import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.DoNotRetryIOException;
import org.apache.hadoop.hbase.client.RegionInfo;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.regionserver.RegionScanner;
import org.apache.hadoop.hbase.regionserver.ScannerContext;
import org.apache.hadoop.hbase.regionserver.ScannerContextUtil;

import static org.apache.phoenix.util.ScanUtil.isDummy;

public class DelegateRegionScanner implements RegionScanner {

    protected RegionScanner delegate;

    public DelegateRegionScanner(RegionScanner scanner) {
        this.delegate = scanner;
    }

    @Override
    public boolean isFilterDone() throws IOException {
        return delegate.isFilterDone();
    }

    @Override
    public boolean reseek(byte[] row) throws IOException {
        return delegate.reseek(row);
    }

    @Override
    public long getMvccReadPoint() {
        return delegate.getMvccReadPoint();
    }

    @Override
    public void close() throws IOException {
        delegate.close();
    }

    @Override
    public long getMaxResultSize() {
        return delegate.getMaxResultSize();
    }

    @Override
    public boolean next(List<Cell> result, ScannerContext scannerContext) throws IOException {
        return next(result, false, scannerContext);
    }

    @Override
    public boolean next(List<Cell> result) throws IOException {
        return next(result, false, null);
    }

    @Override
    public boolean nextRaw(List<Cell> result, ScannerContext scannerContext) throws IOException {
        return next(result, true, scannerContext);
    }

    @Override
    public boolean nextRaw(List<Cell> result) throws IOException {
        return next(result, true, null);
    }

    @Override
    public int getBatch() {
        return delegate.getBatch();
    }


    @Override
    public RegionInfo getRegionInfo() {
        return delegate.getRegionInfo();
    }

    public RegionScanner getNewRegionScanner(Scan scan) throws IOException {
        try {
            return ((DelegateRegionScanner) delegate).getNewRegionScanner(scan);
        } catch (ClassCastException e) {
            throw new DoNotRetryIOException(e);
        }
    }

    private boolean next(List<Cell> result, boolean raw, ScannerContext scannerContext)
            throws IOException {
        if (scannerContext != null) {
            ScannerContext noLimitContext = ScannerContextUtil
                    .copyNoLimitScanner(scannerContext);
            boolean hasMore = raw
                    ? delegate.nextRaw(result, noLimitContext)
                    : delegate.next(result, noLimitContext);
            if (isDummy(result)) {
                // when a dummy row is returned by a lower layer, set returnImmediately
                // on the ScannerContext to force HBase to return a response to the client
                ScannerContextUtil.setReturnImmediately(scannerContext);
            }
            ScannerContextUtil.updateMetrics(noLimitContext, scannerContext);
            return hasMore;
        }
        return raw ? delegate.nextRaw(result) : delegate.next(result);
    }
}