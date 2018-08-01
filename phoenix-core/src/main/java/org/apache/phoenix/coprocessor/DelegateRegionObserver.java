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
import java.util.Map;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.client.Append;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Durability;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Increment;
import org.apache.hadoop.hbase.client.Mutation;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.RegionInfo;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.coprocessor.ObserverContext;
import org.apache.hadoop.hbase.coprocessor.RegionCoprocessorEnvironment;
import org.apache.hadoop.hbase.coprocessor.RegionObserver;
import org.apache.hadoop.hbase.regionserver.InternalScanner;
import org.apache.hadoop.hbase.regionserver.MiniBatchOperationInProgress;
import org.apache.hadoop.hbase.regionserver.Region.Operation;
import org.apache.hadoop.hbase.regionserver.RegionScanner;
import org.apache.hadoop.hbase.regionserver.Store;
import org.apache.hadoop.hbase.regionserver.StoreFile;
import org.apache.hadoop.hbase.regionserver.querymatcher.DeleteTracker;
import org.apache.hadoop.hbase.util.Pair;
import org.apache.hadoop.hbase.wal.WALEdit;
import org.apache.hadoop.hbase.wal.WALKey;

public class DelegateRegionObserver implements RegionObserver {

    protected final RegionObserver delegate;

    public DelegateRegionObserver(RegionObserver delegate) {
        this.delegate = delegate;
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
    public void preFlush(org.apache.hadoop.hbase.coprocessor.ObserverContext<RegionCoprocessorEnvironment> c,
            org.apache.hadoop.hbase.regionserver.FlushLifeCycleTracker tracker) throws IOException {
        delegate.preFlush(c, tracker);
        ;
    }

    @Override
    public InternalScanner preFlush(org.apache.hadoop.hbase.coprocessor.ObserverContext<RegionCoprocessorEnvironment> c,
            Store store, InternalScanner scanner, org.apache.hadoop.hbase.regionserver.FlushLifeCycleTracker tracker)
            throws IOException {
        return delegate.preFlush(c, store, scanner, tracker);
    }

    @Override
    public void postFlush(org.apache.hadoop.hbase.coprocessor.ObserverContext<RegionCoprocessorEnvironment> c,
            org.apache.hadoop.hbase.regionserver.FlushLifeCycleTracker tracker) throws IOException {
        delegate.postFlush(c, tracker);
    }
    

    @Override
    public void postFlush(org.apache.hadoop.hbase.coprocessor.ObserverContext<RegionCoprocessorEnvironment> c,
            Store store, StoreFile resultFile, org.apache.hadoop.hbase.regionserver.FlushLifeCycleTracker tracker)
            throws IOException {
        delegate.postFlush(c, store, resultFile, tracker);
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
    public void preScannerOpen(org.apache.hadoop.hbase.coprocessor.ObserverContext<RegionCoprocessorEnvironment> c,
            Scan scan) throws IOException {
        delegate.preScannerOpen(c, scan);
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
    public void preWALRestore(ObserverContext<? extends RegionCoprocessorEnvironment> ctx, RegionInfo info,
            WALKey logKey, WALEdit logEdit) throws IOException {
        delegate.preWALRestore(ctx, info, logKey, logEdit);
    }
  
   
    @Override
    public void postWALRestore(ObserverContext<? extends RegionCoprocessorEnvironment> ctx, RegionInfo info,
            WALKey logKey, WALEdit logEdit) throws IOException {
        delegate.postWALRestore(ctx, info, logKey, logEdit);
    }

    

    @Override
    public void preBulkLoadHFile(ObserverContext<RegionCoprocessorEnvironment> ctx,
            List<Pair<byte[], String>> familyPaths) throws IOException {
        delegate.preBulkLoadHFile(ctx, familyPaths);
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

    @Override
    public void preCommitStoreFile(ObserverContext<RegionCoprocessorEnvironment> ctx, byte[] family,
            List<Pair<Path, Path>> pairs) throws IOException {
         delegate.preCommitStoreFile(ctx, family, pairs);
        
    }

    @Override
    public void postCommitStoreFile(ObserverContext<RegionCoprocessorEnvironment> ctx, byte[] family, Path srcPath,
            Path dstPath) throws IOException {
         delegate.postCommitStoreFile(ctx, family, srcPath, dstPath);
        
    }

    @Override
    public void postBulkLoadHFile(ObserverContext<RegionCoprocessorEnvironment> ctx,
            List<Pair<byte[], String>> stagingFamilyPaths, Map<byte[], List<Path>> finalPaths)
            throws IOException {
        delegate.postBulkLoadHFile(ctx, stagingFamilyPaths, finalPaths);
    }
    
   
    
}
