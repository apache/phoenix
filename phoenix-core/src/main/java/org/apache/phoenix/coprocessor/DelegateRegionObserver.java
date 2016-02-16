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
package org.apache.phoenix.coprocessor;

import java.io.IOException;
import java.util.List;
import java.util.NavigableSet;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CoprocessorEnvironment;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.client.Append;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Durability;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Increment;
import org.apache.hadoop.hbase.client.Mutation;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.coprocessor.ObserverContext;
import org.apache.hadoop.hbase.coprocessor.RegionCoprocessorEnvironment;
import org.apache.hadoop.hbase.coprocessor.RegionObserver;
import org.apache.hadoop.hbase.filter.ByteArrayComparable;
import org.apache.hadoop.hbase.filter.CompareFilter.CompareOp;
import org.apache.hadoop.hbase.io.FSDataInputStreamWrapper;
import org.apache.hadoop.hbase.io.Reference;
import org.apache.hadoop.hbase.io.hfile.CacheConfig;
import org.apache.hadoop.hbase.regionserver.DeleteTracker;
import org.apache.hadoop.hbase.regionserver.InternalScanner;
import org.apache.hadoop.hbase.regionserver.KeyValueScanner;
import org.apache.hadoop.hbase.regionserver.MiniBatchOperationInProgress;
import org.apache.hadoop.hbase.regionserver.Region;
import org.apache.hadoop.hbase.regionserver.Region.Operation;
import org.apache.hadoop.hbase.regionserver.RegionScanner;
import org.apache.hadoop.hbase.regionserver.ScanType;
import org.apache.hadoop.hbase.regionserver.Store;
import org.apache.hadoop.hbase.regionserver.StoreFile;
import org.apache.hadoop.hbase.regionserver.StoreFile.Reader;
import org.apache.hadoop.hbase.regionserver.compactions.CompactionRequest;
import org.apache.hadoop.hbase.regionserver.wal.HLogKey;
import org.apache.hadoop.hbase.regionserver.wal.WALEdit;
import org.apache.hadoop.hbase.util.Pair;
import org.apache.hadoop.hbase.wal.WALKey;

import com.google.common.collect.ImmutableList;

public class DelegateRegionObserver implements RegionObserver {

    protected final RegionObserver delegate;
    
    public DelegateRegionObserver(RegionObserver delegate) {
        this.delegate = delegate;
    }

    @Override
    public void start(CoprocessorEnvironment env) throws IOException {
        delegate.start(env);
    }

    @Override
    public void stop(CoprocessorEnvironment env) throws IOException {
        delegate.stop(env);
    }

    @Override
    public void preOpen(ObserverContext<RegionCoprocessorEnvironment> c) throws IOException {
        delegate.preOpen(c);
    }

    @Override
    public void postOpen(ObserverContext<RegionCoprocessorEnvironment> c) {
        delegate.postOpen(c);
    }

    @Override
    public void postLogReplay(ObserverContext<RegionCoprocessorEnvironment> c) {
        delegate.postLogReplay(c);
    }

    @Override
    public InternalScanner preFlushScannerOpen(ObserverContext<RegionCoprocessorEnvironment> c,
            Store store, KeyValueScanner memstoreScanner, InternalScanner s) throws IOException {
        return delegate.preFlushScannerOpen(c, store, memstoreScanner, s);
    }

    @Override
    public void preFlush(ObserverContext<RegionCoprocessorEnvironment> c) throws IOException {
        delegate.preFlush(c);
    }

    @Override
    public InternalScanner preFlush(ObserverContext<RegionCoprocessorEnvironment> c, Store store,
            InternalScanner scanner) throws IOException {
        return delegate.preFlush(c, store, scanner);
    }

    @Override
    public void postFlush(ObserverContext<RegionCoprocessorEnvironment> c) throws IOException {
        delegate.postFlush(c);
    }

    @Override
    public void postFlush(ObserverContext<RegionCoprocessorEnvironment> c, Store store,
            StoreFile resultFile) throws IOException {
        delegate.postFlush(c, store, resultFile);
    }

    @Override
    public void preCompactSelection(ObserverContext<RegionCoprocessorEnvironment> c, Store store,
            List<StoreFile> candidates, CompactionRequest request) throws IOException {
        delegate.preCompactSelection(c, store, candidates, request);
    }

    @Override
    public void preCompactSelection(ObserverContext<RegionCoprocessorEnvironment> c, Store store,
            List<StoreFile> candidates) throws IOException {
        delegate.preCompactSelection(c, store, candidates);
    }

    @Override
    public void postCompactSelection(ObserverContext<RegionCoprocessorEnvironment> c, Store store,
            ImmutableList<StoreFile> selected, CompactionRequest request) {
        delegate.postCompactSelection(c, store, selected, request);
    }

    @Override
    public void postCompactSelection(ObserverContext<RegionCoprocessorEnvironment> c, Store store,
            ImmutableList<StoreFile> selected) {
        delegate.postCompactSelection(c, store, selected);
    }

    @Override
    public InternalScanner preCompact(ObserverContext<RegionCoprocessorEnvironment> c, Store store,
            InternalScanner scanner, ScanType scanType, CompactionRequest request)
            throws IOException {
        return delegate.preCompact(c, store, scanner, scanType, request);
    }

    @Override
    public InternalScanner preCompact(ObserverContext<RegionCoprocessorEnvironment> c, Store store,
            InternalScanner scanner, ScanType scanType) throws IOException {
        return delegate.preCompact(c, store, scanner, scanType);
    }

    @Override
    public InternalScanner preCompactScannerOpen(ObserverContext<RegionCoprocessorEnvironment> c,
            Store store, List<? extends KeyValueScanner> scanners, ScanType scanType,
            long earliestPutTs, InternalScanner s, CompactionRequest request) throws IOException {
        return delegate.preCompactScannerOpen(c, store, scanners, scanType, earliestPutTs, s,
            request);
    }

    @Override
    public InternalScanner preCompactScannerOpen(ObserverContext<RegionCoprocessorEnvironment> c,
            Store store, List<? extends KeyValueScanner> scanners, ScanType scanType,
            long earliestPutTs, InternalScanner s) throws IOException {
        return delegate.preCompactScannerOpen(c, store, scanners, scanType, earliestPutTs, s);
    }

    @Override
    public void postCompact(ObserverContext<RegionCoprocessorEnvironment> c, Store store,
            StoreFile resultFile, CompactionRequest request) throws IOException {
        delegate.postCompact(c, store, resultFile, request);
    }

    @Override
    public void postCompact(ObserverContext<RegionCoprocessorEnvironment> c, Store store,
            StoreFile resultFile) throws IOException {
        delegate.postCompact(c, store, resultFile);
    }

    @Override
    public void preSplit(ObserverContext<RegionCoprocessorEnvironment> c) throws IOException {
        delegate.preSplit(c);
    }

    @Override
    public void preSplit(ObserverContext<RegionCoprocessorEnvironment> c, byte[] splitRow)
            throws IOException {
        delegate.preSplit(c, splitRow);
    }

    @Override
    public void postSplit(ObserverContext<RegionCoprocessorEnvironment> c, Region l, Region r)
            throws IOException {
        delegate.postSplit(c, l, r);
    }

    @Override
    public void preSplitBeforePONR(ObserverContext<RegionCoprocessorEnvironment> ctx,
            byte[] splitKey, List<Mutation> metaEntries) throws IOException {
        delegate.preSplitBeforePONR(ctx, splitKey, metaEntries);
    }

    @Override
    public void preSplitAfterPONR(ObserverContext<RegionCoprocessorEnvironment> ctx)
            throws IOException {
        delegate.preSplitAfterPONR(ctx);
    }

    @Override
    public void preRollBackSplit(ObserverContext<RegionCoprocessorEnvironment> ctx)
            throws IOException {
        delegate.preRollBackSplit(ctx);
    }

    @Override
    public void postRollBackSplit(ObserverContext<RegionCoprocessorEnvironment> ctx)
            throws IOException {
        delegate.postRollBackSplit(ctx);
    }

    @Override
    public void postCompleteSplit(ObserverContext<RegionCoprocessorEnvironment> ctx)
            throws IOException {
        delegate.postCompleteSplit(ctx);
    }

    @Override
    public void preClose(ObserverContext<RegionCoprocessorEnvironment> c, boolean abortRequested)
            throws IOException {
        delegate.preClose(c, abortRequested);
    }

    @Override
    public void postClose(ObserverContext<RegionCoprocessorEnvironment> c, boolean abortRequested) {
        delegate.postClose(c, abortRequested);
    }

    @Override
    public void preGetClosestRowBefore(ObserverContext<RegionCoprocessorEnvironment> c, byte[] row,
            byte[] family, Result result) throws IOException {
        delegate.preGetClosestRowBefore(c, row, family, result);
    }

    @Override
    public void postGetClosestRowBefore(ObserverContext<RegionCoprocessorEnvironment> c,
            byte[] row, byte[] family, Result result) throws IOException {
        delegate.postGetClosestRowBefore(c, row, family, result);
    }

    @Override
    public void
            preGetOp(ObserverContext<RegionCoprocessorEnvironment> c, Get get, List<Cell> result)
                    throws IOException {
        delegate.preGetOp(c, get, result);
    }

    @Override
    public void postGetOp(ObserverContext<RegionCoprocessorEnvironment> c, Get get,
            List<Cell> result) throws IOException {
        delegate.postGetOp(c, get, result);
    }

    @Override
    public boolean preExists(ObserverContext<RegionCoprocessorEnvironment> c, Get get,
            boolean exists) throws IOException {
        return delegate.preExists(c, get, exists);
    }

    @Override
    public boolean postExists(ObserverContext<RegionCoprocessorEnvironment> c, Get get,
            boolean exists) throws IOException {
        return delegate.postExists(c, get, exists);
    }

    @Override
    public void prePut(ObserverContext<RegionCoprocessorEnvironment> c, Put put, WALEdit edit,
            Durability durability) throws IOException {
        delegate.prePut(c, put, edit, durability);
    }

    @Override
    public void postPut(ObserverContext<RegionCoprocessorEnvironment> c, Put put, WALEdit edit,
            Durability durability) throws IOException {
        delegate.postPut(c, put, edit, durability);
    }

    @Override
    public void preDelete(ObserverContext<RegionCoprocessorEnvironment> c, Delete delete,
            WALEdit edit, Durability durability) throws IOException {
        delegate.preDelete(c, delete, edit, durability);
    }

    @Override
    public void prePrepareTimeStampForDeleteVersion(
            ObserverContext<RegionCoprocessorEnvironment> c, Mutation mutation, Cell cell,
            byte[] byteNow, Get get) throws IOException {
        delegate.prePrepareTimeStampForDeleteVersion(c, mutation, cell, byteNow, get);
    }

    @Override
    public void postDelete(ObserverContext<RegionCoprocessorEnvironment> c, Delete delete,
            WALEdit edit, Durability durability) throws IOException {
        delegate.postDelete(c, delete, edit, durability);
    }

    @Override
    public void preBatchMutate(ObserverContext<RegionCoprocessorEnvironment> c,
            MiniBatchOperationInProgress<Mutation> miniBatchOp) throws IOException {
        delegate.preBatchMutate(c, miniBatchOp);
    }

    @Override
    public void postBatchMutate(ObserverContext<RegionCoprocessorEnvironment> c,
            MiniBatchOperationInProgress<Mutation> miniBatchOp) throws IOException {
        delegate.postBatchMutate(c, miniBatchOp);
    }

    @Override
    public void postStartRegionOperation(ObserverContext<RegionCoprocessorEnvironment> ctx,
            Operation operation) throws IOException {
        delegate.postStartRegionOperation(ctx, operation);
    }

    @Override
    public void postCloseRegionOperation(ObserverContext<RegionCoprocessorEnvironment> ctx,
            Operation operation) throws IOException {
        delegate.postCloseRegionOperation(ctx, operation);
    }

    @Override
    public void postBatchMutateIndispensably(ObserverContext<RegionCoprocessorEnvironment> ctx,
            MiniBatchOperationInProgress<Mutation> miniBatchOp, boolean success) throws IOException {
        delegate.postBatchMutateIndispensably(ctx, miniBatchOp, success);
    }

    @Override
    public boolean preCheckAndPut(ObserverContext<RegionCoprocessorEnvironment> c, byte[] row,
            byte[] family, byte[] qualifier, CompareOp compareOp, ByteArrayComparable comparator,
            Put put, boolean result) throws IOException {
        return delegate.preCheckAndPut(c, row, family, qualifier, compareOp, comparator, put,
            result);
    }

    @Override
    public boolean preCheckAndPutAfterRowLock(ObserverContext<RegionCoprocessorEnvironment> c,
            byte[] row, byte[] family, byte[] qualifier, CompareOp compareOp,
            ByteArrayComparable comparator, Put put, boolean result) throws IOException {
        return delegate.preCheckAndPutAfterRowLock(c, row, family, qualifier, compareOp,
            comparator, put, result);
    }

    @Override
    public boolean postCheckAndPut(ObserverContext<RegionCoprocessorEnvironment> c, byte[] row,
            byte[] family, byte[] qualifier, CompareOp compareOp, ByteArrayComparable comparator,
            Put put, boolean result) throws IOException {
        return delegate.postCheckAndPut(c, row, family, qualifier, compareOp, comparator, put,
            result);
    }

    @Override
    public boolean preCheckAndDelete(ObserverContext<RegionCoprocessorEnvironment> c, byte[] row,
            byte[] family, byte[] qualifier, CompareOp compareOp, ByteArrayComparable comparator,
            Delete delete, boolean result) throws IOException {
        return delegate.preCheckAndDelete(c, row, family, qualifier, compareOp, comparator, delete,
            result);
    }

    @Override
    public boolean preCheckAndDeleteAfterRowLock(ObserverContext<RegionCoprocessorEnvironment> c,
            byte[] row, byte[] family, byte[] qualifier, CompareOp compareOp,
            ByteArrayComparable comparator, Delete delete, boolean result) throws IOException {
        return delegate.preCheckAndDeleteAfterRowLock(c, row, family, qualifier, compareOp,
            comparator, delete, result);
    }

    @Override
    public boolean postCheckAndDelete(ObserverContext<RegionCoprocessorEnvironment> c, byte[] row,
            byte[] family, byte[] qualifier, CompareOp compareOp, ByteArrayComparable comparator,
            Delete delete, boolean result) throws IOException {
        return delegate.postCheckAndDelete(c, row, family, qualifier, compareOp, comparator,
            delete, result);
    }

    @Override
    public long preIncrementColumnValue(ObserverContext<RegionCoprocessorEnvironment> c,
            byte[] row, byte[] family, byte[] qualifier, long amount, boolean writeToWAL)
            throws IOException {
        return delegate.preIncrementColumnValue(c, row, family, qualifier, amount, writeToWAL);
    }

    @Override
    public long postIncrementColumnValue(ObserverContext<RegionCoprocessorEnvironment> c,
            byte[] row, byte[] family, byte[] qualifier, long amount, boolean writeToWAL,
            long result) throws IOException {
        return delegate.postIncrementColumnValue(c, row, family, qualifier, amount, writeToWAL,
            result);
    }

    @Override
    public Result preAppend(ObserverContext<RegionCoprocessorEnvironment> c, Append append)
            throws IOException {
        return delegate.preAppend(c, append);
    }

    @Override
    public Result preAppendAfterRowLock(ObserverContext<RegionCoprocessorEnvironment> c,
            Append append) throws IOException {
        return delegate.preAppendAfterRowLock(c, append);
    }

    @Override
    public Result postAppend(ObserverContext<RegionCoprocessorEnvironment> c, Append append,
            Result result) throws IOException {
        return delegate.postAppend(c, append, result);
    }

    @Override
    public Result
            preIncrement(ObserverContext<RegionCoprocessorEnvironment> c, Increment increment)
                    throws IOException {
        return delegate.preIncrement(c, increment);
    }

    @Override
    public Result preIncrementAfterRowLock(ObserverContext<RegionCoprocessorEnvironment> c,
            Increment increment) throws IOException {
        return delegate.preIncrementAfterRowLock(c, increment);
    }

    @Override
    public Result postIncrement(ObserverContext<RegionCoprocessorEnvironment> c,
            Increment increment, Result result) throws IOException {
        return delegate.postIncrement(c, increment, result);
    }

    @Override
    public RegionScanner preScannerOpen(ObserverContext<RegionCoprocessorEnvironment> c, Scan scan,
            RegionScanner s) throws IOException {
        return delegate.preScannerOpen(c, scan, s);
    }

    @Override
    public KeyValueScanner preStoreScannerOpen(ObserverContext<RegionCoprocessorEnvironment> c,
            Store store, Scan scan, NavigableSet<byte[]> targetCols, KeyValueScanner s)
            throws IOException {
        return delegate.preStoreScannerOpen(c, store, scan, targetCols, s);
    }

    @Override
    public RegionScanner postScannerOpen(ObserverContext<RegionCoprocessorEnvironment> c,
            Scan scan, RegionScanner s) throws IOException {
        return delegate.postScannerOpen(c, scan, s);
    }

    @Override
    public boolean preScannerNext(ObserverContext<RegionCoprocessorEnvironment> c,
            InternalScanner s, List<Result> result, int limit, boolean hasNext) throws IOException {
        return delegate.preScannerNext(c, s, result, limit, hasNext);
    }

    @Override
    public boolean postScannerNext(ObserverContext<RegionCoprocessorEnvironment> c,
            InternalScanner s, List<Result> result, int limit, boolean hasNext) throws IOException {
        return delegate.postScannerNext(c, s, result, limit, hasNext);
    }

    @Override
    public boolean postScannerFilterRow(ObserverContext<RegionCoprocessorEnvironment> c,
            InternalScanner s, byte[] currentRow, int offset, short length, boolean hasMore)
            throws IOException {
        return delegate.postScannerFilterRow(c, s, currentRow, offset, length, hasMore);
    }

    @Override
    public void preScannerClose(ObserverContext<RegionCoprocessorEnvironment> c, InternalScanner s)
            throws IOException {
        delegate.preScannerClose(c, s);
    }

    @Override
    public void
            postScannerClose(ObserverContext<RegionCoprocessorEnvironment> c, InternalScanner s)
                    throws IOException {
        delegate.postScannerClose(c, s);
    }

    @Override
    public void preWALRestore(ObserverContext<? extends RegionCoprocessorEnvironment> ctx,
            HRegionInfo info, WALKey logKey, WALEdit logEdit) throws IOException {
        delegate.preWALRestore(ctx, info, logKey, logEdit);
    }

    @Override
    public void preWALRestore(ObserverContext<RegionCoprocessorEnvironment> ctx, HRegionInfo info,
            HLogKey logKey, WALEdit logEdit) throws IOException {
        delegate.preWALRestore(ctx, info, logKey, logEdit);
    }

    @Override
    public void postWALRestore(ObserverContext<? extends RegionCoprocessorEnvironment> ctx,
            HRegionInfo info, WALKey logKey, WALEdit logEdit) throws IOException {
        delegate.postWALRestore(ctx, info, logKey, logEdit);
    }

    @Override
    public void postWALRestore(ObserverContext<RegionCoprocessorEnvironment> ctx, HRegionInfo info,
            HLogKey logKey, WALEdit logEdit) throws IOException {
        delegate.postWALRestore(ctx, info, logKey, logEdit);
    }

    @Override
    public void preBulkLoadHFile(ObserverContext<RegionCoprocessorEnvironment> ctx,
            List<Pair<byte[], String>> familyPaths) throws IOException {
        delegate.preBulkLoadHFile(ctx, familyPaths);
    }

    @Override
    public boolean postBulkLoadHFile(ObserverContext<RegionCoprocessorEnvironment> ctx,
            List<Pair<byte[], String>> familyPaths, boolean hasLoaded) throws IOException {
        return delegate.postBulkLoadHFile(ctx, familyPaths, hasLoaded);
    }

    @Override
    public Reader preStoreFileReaderOpen(ObserverContext<RegionCoprocessorEnvironment> ctx,
            FileSystem fs, Path p, FSDataInputStreamWrapper in, long size, CacheConfig cacheConf,
            Reference r, Reader reader) throws IOException {
        return delegate.preStoreFileReaderOpen(ctx, fs, p, in, size, cacheConf, r, reader);
    }

    @Override
    public Reader postStoreFileReaderOpen(ObserverContext<RegionCoprocessorEnvironment> ctx,
            FileSystem fs, Path p, FSDataInputStreamWrapper in, long size, CacheConfig cacheConf,
            Reference r, Reader reader) throws IOException {
        return delegate.postStoreFileReaderOpen(ctx, fs, p, in, size, cacheConf, r, reader);
    }

    @Override
    public Cell postMutationBeforeWAL(ObserverContext<RegionCoprocessorEnvironment> ctx,
            MutationType opType, Mutation mutation, Cell oldCell, Cell newCell) throws IOException {
        return delegate.postMutationBeforeWAL(ctx, opType, mutation, oldCell, newCell);
    }

    @Override
    public DeleteTracker postInstantiateDeleteTracker(
            ObserverContext<RegionCoprocessorEnvironment> ctx, DeleteTracker delTracker)
            throws IOException {
        return delegate.postInstantiateDeleteTracker(ctx, delTracker);
    }
    
   
    
}
