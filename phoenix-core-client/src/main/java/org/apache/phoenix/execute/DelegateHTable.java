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
package org.apache.phoenix.execute;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.CompareOperator;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Append;
import org.apache.hadoop.hbase.client.CheckAndMutate;
import org.apache.hadoop.hbase.client.CheckAndMutateResult;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Durability;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Increment;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.RegionLocator;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Row;
import org.apache.hadoop.hbase.client.RowMutations;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.client.TableDescriptor;
import org.apache.hadoop.hbase.client.coprocessor.Batch.Call;
import org.apache.hadoop.hbase.client.coprocessor.Batch.Callback;
import org.apache.hadoop.hbase.ipc.CoprocessorRpcChannel;
import org.apache.phoenix.compat.hbase.CompatDelegateHTable;

import com.google.protobuf.Descriptors.MethodDescriptor;
import com.google.protobuf.Message;
import com.google.protobuf.Service;
import com.google.protobuf.ServiceException;

public class DelegateHTable extends CompatDelegateHTable implements Table {

    public DelegateHTable(Table delegate) {
        super(delegate);
    }

    @Override
    public TableName getName() {
        return delegate.getName();
    }

    @Override
    public Configuration getConfiguration() {
        return delegate.getConfiguration();
    }

    @Override
    public boolean exists(Get get) throws IOException {
        return delegate.exists(get);
    }

    @Override
    public void batch(List<? extends Row> actions, Object[] results) throws IOException,
            InterruptedException {
        delegate.batch(actions, results);
    }

    @Override
    public <R> void batchCallback(List<? extends Row> actions, Object[] results,
            Callback<R> callback) throws IOException, InterruptedException {
        delegate.batchCallback(actions, results, callback);
    }

    @Override
    public Result get(Get get) throws IOException {
        return delegate.get(get);
    }

    @Override
    public Result[] get(List<Get> gets) throws IOException {
        return delegate.get(gets);
    }

    @Override
    public ResultScanner getScanner(Scan scan) throws IOException {
        return delegate.getScanner(scan);
    }

    @Override
    public ResultScanner getScanner(byte[] family) throws IOException {
        return delegate.getScanner(family);
    }

    @Override
    public ResultScanner getScanner(byte[] family, byte[] qualifier) throws IOException {
        return delegate.getScanner(family, qualifier);
    }

    @Override
    public void put(Put put) throws IOException {
        delegate.put(put);
    }

    @Override
    public void put(List<Put> puts) throws IOException {
        delegate.put(puts);
    }

    @Override
    public void delete(Delete delete) throws IOException {
        delegate.delete(delete);
    }

    @Override
    public void delete(List<Delete> deletes) throws IOException {
        delegate.delete(deletes);
    }

    @Override
    public Result append(Append append) throws IOException {
        return delegate.append(append);
    }

    @Override
    public Result increment(Increment increment) throws IOException {
        return delegate.increment(increment);
    }

    @Override
    public long incrementColumnValue(byte[] row, byte[] family, byte[] qualifier, long amount)
            throws IOException {
        return delegate.incrementColumnValue(row, family, qualifier, amount);
    }

    @Override
    public long incrementColumnValue(byte[] row, byte[] family, byte[] qualifier, long amount,
            Durability durability) throws IOException {
        return delegate.incrementColumnValue(row, family, qualifier, amount, durability);
    }

    @Override
    public void close() throws IOException {
        delegate.close();
    }

    @Override
    public CoprocessorRpcChannel coprocessorService(byte[] row) {
        return delegate.coprocessorService(row);
    }

    @Override
    public <T extends Service, R> Map<byte[], R> coprocessorService(Class<T> service,
            byte[] startKey, byte[] endKey, Call<T, R> callable) throws ServiceException, Throwable {
        return delegate.coprocessorService(service, startKey, endKey, callable);
    }

    @Override
    public <T extends Service, R> void coprocessorService(Class<T> service, byte[] startKey,
            byte[] endKey, Call<T, R> callable, Callback<R> callback) throws ServiceException,
            Throwable {
        delegate.coprocessorService(service, startKey, endKey, callable, callback);
        
    }

    @Override
    public <R extends Message> Map<byte[], R> batchCoprocessorService(
            MethodDescriptor methodDescriptor, Message request, byte[] startKey, byte[] endKey,
            R responsePrototype) throws ServiceException, Throwable {
        return delegate.batchCoprocessorService(methodDescriptor, request, startKey, endKey, responsePrototype);
    }

    @Override
    public <R extends Message> void batchCoprocessorService(MethodDescriptor methodDescriptor,
            Message request, byte[] startKey, byte[] endKey, R responsePrototype,
            Callback<R> callback) throws ServiceException, Throwable {
        delegate.batchCoprocessorService(methodDescriptor, request, startKey, endKey, responsePrototype);
    }

    @Override
    public CheckAndMutateResult checkAndMutate(CheckAndMutate checkAndMutate) throws IOException {
        return delegate.checkAndMutate(checkAndMutate);
    }

    @Override
    public TableDescriptor getDescriptor() throws IOException {
        return delegate.getDescriptor();
    }

    @Override
    public boolean[] exists(List<Get> gets) throws IOException {
        return delegate.exists(gets);
    }

    @Override
    public long getRpcTimeout(TimeUnit unit) {
        return delegate.getRpcTimeout(unit);
    }

    @Override
    public long getReadRpcTimeout(TimeUnit unit) {
        return delegate.getReadRpcTimeout(unit);
    }

    @Override
    public long getWriteRpcTimeout(TimeUnit unit) {
        return delegate.getWriteRpcTimeout(unit);
    }

    @Override
    public long getOperationTimeout(TimeUnit unit) {
        return delegate.getOperationTimeout(unit);
    }

    @Override
    public RegionLocator getRegionLocator() throws IOException {
        return delegate.getRegionLocator();
    }
}
