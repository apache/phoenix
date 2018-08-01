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
 */package org.apache.phoenix.coprocessor;

import java.io.IOException;
import java.util.List;

import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.client.RegionInfo;
import org.apache.hadoop.hbase.regionserver.RegionScanner;
import org.apache.hadoop.hbase.regionserver.ScannerContext;

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
        throw new IOException("Next with scannerContext should not be called in Phoenix environment");
    }

    @Override
    public boolean next(List<Cell> result) throws IOException {
        return delegate.next(result);
    }

    @Override
    public boolean nextRaw(List<Cell> result, ScannerContext scannerContext) throws IOException {
        throw new IOException("NextRaw with scannerContext should not be called in Phoenix environment");
    }

    @Override
    public boolean nextRaw(List<Cell> arg0) throws IOException {
        return delegate.nextRaw(arg0);
    }

    @Override
    public int getBatch() {
        return delegate.getBatch();
    }


    @Override
    public RegionInfo getRegionInfo() {
        return delegate.getRegionInfo();
    }
}