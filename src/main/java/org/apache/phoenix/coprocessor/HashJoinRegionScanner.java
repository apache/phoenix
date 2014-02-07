/*
 * Copyright 2014 The Apache Software Foundation
 *
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
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;
import java.util.Queue;

import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.coprocessor.RegionCoprocessorEnvironment;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.regionserver.RegionScanner;

import org.apache.hbase.index.util.ImmutableBytesPtr;
import org.apache.phoenix.cache.GlobalCache;
import org.apache.phoenix.cache.HashCache;
import org.apache.phoenix.cache.TenantCache;
import org.apache.phoenix.join.HashJoinInfo;
import org.apache.phoenix.parse.JoinTableNode.JoinType;
import org.apache.phoenix.schema.tuple.ResultTuple;
import org.apache.phoenix.schema.tuple.Tuple;
import org.apache.phoenix.util.TupleUtil;

public class HashJoinRegionScanner implements RegionScanner {
    
    private final RegionScanner scanner;
    private final ScanProjector projector;
    private final HashJoinInfo joinInfo;
    private Queue<List<KeyValue>> resultQueue;
    private boolean hasMore;
    private TenantCache cache;
    
    public HashJoinRegionScanner(RegionScanner scanner, ScanProjector projector, HashJoinInfo joinInfo, ImmutableBytesWritable tenantId, RegionCoprocessorEnvironment env) throws IOException {
        this.scanner = scanner;
        this.projector = projector;
        this.joinInfo = joinInfo;
        this.resultQueue = new LinkedList<List<KeyValue>>();
        this.hasMore = true;
        if (joinInfo != null) {
            if (tenantId == null)
                throw new IOException("Could not find tenant id for hash cache.");
            for (JoinType type : joinInfo.getJoinTypes()) {
                if (type == JoinType.Right)
                    throw new IOException("The hashed table should not be LHS.");
            }
            this.cache = GlobalCache.getTenantCache(env, tenantId);
        }
    }
    
    @SuppressWarnings("unchecked")
    private void processResults(List<KeyValue> result, boolean hasLimit) throws IOException {
        if (result.isEmpty())
            return;
        
        if (projector != null) {
            List<KeyValue> kvs = new ArrayList<KeyValue>(result.size());
            for (KeyValue kv : result) {
                kvs.add(projector.getProjectedKeyValue(kv));
            }
            if (joinInfo != null) {
                result = kvs;
            } else {
                resultQueue.offer(kvs);               
            }
        }
        
        if (joinInfo != null) {
            if (hasLimit)
                throw new UnsupportedOperationException("Cannot support join operations in scans with limit");
            
            int count = joinInfo.getJoinIds().length;
            List<Tuple>[] tuples = new List[count];
            Tuple tuple = new ResultTuple(new Result(result));
            boolean cont = true;
            for (int i = 0; i < count; i++) {
                ImmutableBytesPtr key = TupleUtil.getConcatenatedValue(tuple, joinInfo.getJoinExpressions()[i]);
                HashCache hashCache = (HashCache)cache.getServerCache(joinInfo.getJoinIds()[i]);
                tuples[i] = hashCache.get(key);
                JoinType type = joinInfo.getJoinTypes()[i];
                if (type == JoinType.Inner && (tuples[i] == null || tuples[i].isEmpty())) {
                    cont = false;
                    break;
                }
            }
            if (cont) {
                resultQueue.offer(result);
                for (int i = 0; i < count; i++) {
                    if (tuples[i] == null || tuples[i].isEmpty())
                        continue;
                    int j = resultQueue.size();
                    while (j-- > 0) {
                        List<KeyValue> lhs = resultQueue.poll();
                        for (Tuple t : tuples[i]) {
                            List<KeyValue> rhs = ((ResultTuple) t).getResult().list();
                            List<KeyValue> joined = new ArrayList<KeyValue>(lhs.size() + rhs.size());
                            joined.addAll(lhs);
                            joined.addAll(rhs); // we don't replace rowkey here, for further reference to the rowkey fields, needs to specify family as well.
                            resultQueue.offer(joined);
                        }
                    }
                }
            }
        }
    }
    
    private boolean shouldAdvance() {
        if (!resultQueue.isEmpty())
            return false;
        
        return hasMore;
    }
    
    private boolean nextInQueue(List<KeyValue> results) {
        if (resultQueue.isEmpty())
            return false;
        
        results.addAll(resultQueue.poll());
        return resultQueue.isEmpty() ? hasMore : true;
    }

    @Override
    public long getMvccReadPoint() {
        return scanner.getMvccReadPoint();
    }

    @Override
    public HRegionInfo getRegionInfo() {
        return scanner.getRegionInfo();
    }

    @Override
    public boolean isFilterDone() {
        return scanner.isFilterDone() && resultQueue.isEmpty();
    }

    @Override
    public boolean nextRaw(List<KeyValue> result, String metric) throws IOException {
        while (shouldAdvance()) {
            List<KeyValue> tempResult = new ArrayList<KeyValue>();
            hasMore = scanner.nextRaw(tempResult, metric);
            processResults(tempResult, false);
        }
        
        return nextInQueue(result);
    }

    @Override
    public boolean nextRaw(List<KeyValue> result, int limit, String metric)
            throws IOException {
        while (shouldAdvance()) {
            List<KeyValue> tempResult = new ArrayList<KeyValue>();
            hasMore = scanner.nextRaw(tempResult, limit, metric);
            processResults(tempResult, true);
        }
        
        return nextInQueue(result);
    }

    @Override
    public boolean reseek(byte[] row) throws IOException {
        return scanner.reseek(row);
    }

    @Override
    public void close() throws IOException {
        scanner.close();
    }

    @Override
    public boolean next(List<KeyValue> result) throws IOException {
        while (shouldAdvance()) {
            List<KeyValue> tempResult = new ArrayList<KeyValue>();
            hasMore = scanner.next(tempResult);
            processResults(tempResult, false);
        }
        
        return nextInQueue(result);
    }

    @Override
    public boolean next(List<KeyValue> result, String metric) throws IOException {
        while (shouldAdvance()) {
            List<KeyValue> tempResult = new ArrayList<KeyValue>();
            hasMore = scanner.next(tempResult, metric);
            processResults(tempResult, false);
        }
        
        return nextInQueue(result);
    }

    @Override
    public boolean next(List<KeyValue> result, int limit) throws IOException {
        while (shouldAdvance()) {
            List<KeyValue> tempResult = new ArrayList<KeyValue>();
            hasMore = scanner.next(tempResult, limit);
            processResults(tempResult, true);
        }
        
        return nextInQueue(result);
    }

    @Override
    public boolean next(List<KeyValue> result, int limit, String metric)
            throws IOException {
        while (shouldAdvance()) {
            List<KeyValue> tempResult = new ArrayList<KeyValue>();
            hasMore = scanner.next(tempResult, limit, metric);
            processResults(tempResult, true);
        }
        
        return nextInQueue(result);
    }

}
