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

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Append;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Durability;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HTableInterface;
import org.apache.hadoop.hbase.client.Increment;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Row;
import org.apache.hadoop.hbase.client.RowMutations;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.coprocessor.Batch.Call;
import org.apache.hadoop.hbase.client.coprocessor.Batch.Callback;
import org.apache.hadoop.hbase.filter.CompareFilter.CompareOp;
import org.apache.hadoop.hbase.ipc.CoprocessorRpcChannel;

import com.google.protobuf.Descriptors.MethodDescriptor;
import com.google.protobuf.Message;
import com.google.protobuf.Service;
import com.google.protobuf.ServiceException;

public class DelegateHTable implements HTableInterface {
    protected final HTableInterface delegate;

    public DelegateHTable(HTableInterface delegate) {
        this.delegate = delegate;
    }

    @Override
    public byte[] getTableName() {
        return delegate.getTableName();
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
    public HTableDescriptor getTableDescriptor() throws IOException {
        return delegate.getTableDescriptor();
    }

    @Override
    public boolean exists(Get get) throws IOException {
        return delegate.exists(get);
    }

    @Override
    public Boolean[] exists(List<Get> gets) throws IOException {
        return delegate.exists(gets);
    }

    @Override
    public void batch(List<? extends Row> actions, Object[] results) throws IOException, InterruptedException {
        delegate.batch(actions, results);
    }

    @SuppressWarnings("deprecation")
    @Override
    public Object[] batch(List<? extends Row> actions) throws IOException, InterruptedException {
        return delegate.batch(actions);
    }

    @Override
    public <R> void batchCallback(List<? extends Row> actions, Object[] results, Callback<R> callback)
            throws IOException, InterruptedException {
        delegate.batchCallback(actions, results, callback);
    }

    @SuppressWarnings("deprecation")
    @Override
    public <R> Object[] batchCallback(List<? extends Row> actions, Callback<R> callback) throws IOException,
            InterruptedException {
        return delegate.batchCallback(actions, callback);
    }

    @Override
    public Result get(Get get) throws IOException {
        return delegate.get(get);
    }

    @Override
    public Result[] get(List<Get> gets) throws IOException {
        return delegate.get(gets);
    }

    @SuppressWarnings("deprecation")
    @Override
    public Result getRowOrBefore(byte[] row, byte[] family) throws IOException {
        return delegate.getRowOrBefore(row, family);
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
    public boolean checkAndPut(byte[] row, byte[] family, byte[] qualifier, byte[] value, Put put) throws IOException {
        return delegate.checkAndPut(row, family, qualifier, value, put);
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
    public boolean checkAndDelete(byte[] row, byte[] family, byte[] qualifier, byte[] value, Delete delete)
            throws IOException {
        return delegate.checkAndDelete(row, family, qualifier, value, delete);
    }

    @Override
    public void mutateRow(RowMutations rm) throws IOException {
        delegate.mutateRow(rm);
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
    public long incrementColumnValue(byte[] row, byte[] family, byte[] qualifier, long amount) throws IOException {
        return delegate.incrementColumnValue(row, family, qualifier, amount);
    }

    @Override
    public long incrementColumnValue(byte[] row, byte[] family, byte[] qualifier, long amount, Durability durability)
            throws IOException {
        return delegate.incrementColumnValue(row, family, qualifier, amount, durability);
    }

    @SuppressWarnings("deprecation")
    @Override
    public long incrementColumnValue(byte[] row, byte[] family, byte[] qualifier, long amount, boolean writeToWAL)
            throws IOException {
        return delegate.incrementColumnValue(row, family, qualifier, amount, writeToWAL);
    }

    @Override
    public boolean isAutoFlush() {
        return delegate.isAutoFlush();
    }

    @Override
    public void flushCommits() throws IOException {
        delegate.flushCommits();
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
    public <T extends Service, R> Map<byte[], R> coprocessorService(Class<T> service, byte[] startKey, byte[] endKey,
            Call<T, R> callable) throws ServiceException, Throwable {
        return delegate.coprocessorService(service, startKey, endKey, callable);
    }

    @Override
    public <T extends Service, R> void coprocessorService(Class<T> service, byte[] startKey, byte[] endKey,
            Call<T, R> callable, Callback<R> callback) throws ServiceException, Throwable {
        delegate.coprocessorService(service, startKey, endKey, callable, callback);
    }

    @SuppressWarnings("deprecation")
    @Override
    public void setAutoFlush(boolean autoFlush) {
        delegate.setAutoFlush(autoFlush);
    }

    @Override
    public void setAutoFlush(boolean autoFlush, boolean clearBufferOnFail) {
        delegate.setAutoFlush(autoFlush, clearBufferOnFail);
    }

    @Override
    public void setAutoFlushTo(boolean autoFlush) {
        delegate.setAutoFlushTo(autoFlush);
    }

    @Override
    public long getWriteBufferSize() {
        return delegate.getWriteBufferSize();
    }

    @Override
    public void setWriteBufferSize(long writeBufferSize) throws IOException {
        delegate.setWriteBufferSize(writeBufferSize);
    }

    @Override
    public <R extends Message> Map<byte[], R> batchCoprocessorService(MethodDescriptor methodDescriptor,
            Message request, byte[] startKey, byte[] endKey, R responsePrototype) throws ServiceException, Throwable {
        return delegate.batchCoprocessorService(methodDescriptor, request, startKey, endKey, responsePrototype);
    }

    @Override
    public <R extends Message> void batchCoprocessorService(MethodDescriptor methodDescriptor, Message request,
            byte[] startKey, byte[] endKey, R responsePrototype, Callback<R> callback) throws ServiceException,
            Throwable {
        delegate.batchCoprocessorService(methodDescriptor, request, startKey, endKey, responsePrototype, callback);
    }

    @Override
    public boolean checkAndMutate(byte[] row, byte[] family, byte[] qualifier, CompareOp compareOp, byte[] value,
            RowMutations mutation) throws IOException {
        return delegate.checkAndMutate(row, family, qualifier, compareOp, value, mutation);
    }

	@Override
	public boolean[] existsAll(List<Get> gets) throws IOException {
		return delegate.existsAll(gets);
	}

	@Override
	public boolean checkAndPut(byte[] row, byte[] family, byte[] qualifier,
			CompareOp compareOp, byte[] value, Put put) throws IOException {
		return delegate.checkAndPut(row, family, qualifier, value, put);
	}

	@Override
	public boolean checkAndDelete(byte[] row, byte[] family, byte[] qualifier,
			CompareOp compareOp, byte[] value, Delete delete)
			throws IOException {
		return delegate.checkAndDelete(row, family, qualifier, compareOp, value, delete);
	}

}
