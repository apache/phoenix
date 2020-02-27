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
package org.apache.phoenix.compat.hbase;

import java.io.IOException;
import java.util.List;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.coprocessor.ObserverContext;
import org.apache.hadoop.hbase.coprocessor.RegionCoprocessorEnvironment;
import org.apache.hadoop.hbase.coprocessor.RegionObserver;
import org.apache.hadoop.hbase.regionserver.InternalScanner;
import org.apache.hadoop.hbase.regionserver.KeyValueScanner;
import org.apache.hadoop.hbase.regionserver.ScanType;
import org.apache.hadoop.hbase.regionserver.Store;
import org.apache.hadoop.hbase.regionserver.compactions.CompactionRequest;
import org.apache.hadoop.hbase.util.Pair;

public abstract class CompatDelegateRegionObserver implements RegionObserver {

    protected final RegionObserver delegate;

    public CompatDelegateRegionObserver(RegionObserver delegate) {
        this.delegate = delegate;
    }

    @Override
    public InternalScanner preFlushScannerOpen(ObserverContext<RegionCoprocessorEnvironment> c,
            Store store, KeyValueScanner memstoreScanner, InternalScanner s, long readPoint)
            throws IOException {
        return delegate.preFlushScannerOpen(c, store, memstoreScanner, s, readPoint);
    }

    @Override
    public InternalScanner preCompactScannerOpen(ObserverContext<RegionCoprocessorEnvironment> c,
            Store store, List<? extends KeyValueScanner> scanners, ScanType scanType,
            long earliestPutTs, InternalScanner s, CompactionRequest request, long readPoint)
            throws IOException {
        return delegate.preCompactScannerOpen(c, store, scanners, scanType, earliestPutTs, s,
            request, readPoint);
    }

    @Override
    public void preCommitStoreFile(ObserverContext<RegionCoprocessorEnvironment> ctx, byte[] family,
            List<Pair<Path, Path>> pairs) throws IOException {
        delegate.preCommitStoreFile(ctx, family, pairs);
    }

    @Override
    public void postCommitStoreFile(ObserverContext<RegionCoprocessorEnvironment> ctx,
            byte[] family, Path srcPath, Path dstPath) throws IOException {
        delegate.postCommitStoreFile(ctx, family, srcPath, dstPath);
    }
}