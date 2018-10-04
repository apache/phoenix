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
package org.apache.phoenix.transaction;

import java.io.IOException;
import java.sql.SQLException;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Append;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Durability;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Increment;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Row;
import org.apache.hadoop.hbase.client.RowMutations;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.client.coprocessor.Batch.Call;
import org.apache.hadoop.hbase.client.coprocessor.Batch.Callback;
import org.apache.hadoop.hbase.filter.CompareFilter.CompareOp;
import org.apache.hadoop.hbase.ipc.CoprocessorRpcChannel;

import com.google.protobuf.Descriptors.MethodDescriptor;
import com.google.protobuf.Message;
import com.google.protobuf.Service;
import com.google.protobuf.ServiceException;

public class OmidTransactionTable implements Table {

    public OmidTransactionTable() throws SQLException {
    }

    public OmidTransactionTable(PhoenixTransactionContext ctx, Table hTable) throws SQLException {
    }

    public OmidTransactionTable(PhoenixTransactionContext ctx, Table hTable, boolean isConflictFree) throws SQLException  {
    }

    @Override
    public Result get(Get get) throws IOException {
        return null;
    }

    @Override
    public void put(Put put) throws IOException {
    }

    @Override
    public void delete(Delete delete) throws IOException {
    }

    @Override
    public ResultScanner getScanner(Scan scan) throws IOException {
        return null;
    }

    @Override
    public Configuration getConfiguration() {
        return null;
    }

    @Override
    public HTableDescriptor getTableDescriptor() throws IOException {
        return null;
    }

    @Override
    public boolean exists(Get get) throws IOException {
        return false;
    }

    @Override
    public Result[] get(List<Get> gets) throws IOException {
        return null;
    }

    @Override
    public ResultScanner getScanner(byte[] family) throws IOException {
        return null;
    }

    @Override
    public ResultScanner getScanner(byte[] family, byte[] qualifier)
            throws IOException {
        return null;
    }

    @Override
    public void put(List<Put> puts) throws IOException {
    }

    @Override
    public void delete(List<Delete> deletes) throws IOException {
    }

    @Override
    public void close() throws IOException {
    }

    @Override
    public TableName getName() {
        return null;
    }

    @Override
    public boolean[] existsAll(List<Get> gets) throws IOException {
        throw new UnsupportedOperationException();
    }

    @Override
    public void batch(List<? extends Row> actions, Object[] results)
            throws IOException, InterruptedException {
    }

    @Override
    public Object[] batch(List<? extends Row> actions) throws IOException,
            InterruptedException {
        return null;
    }

    @Override
    public <R> void batchCallback(List<? extends Row> actions,
            Object[] results, Callback<R> callback) throws IOException,
            InterruptedException {
        throw new UnsupportedOperationException();
    }

    @Override
    public <R> Object[] batchCallback(List<? extends Row> actions,
            Callback<R> callback) throws IOException, InterruptedException {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean checkAndPut(byte[] row, byte[] family, byte[] qualifier,
            byte[] value, Put put) throws IOException {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean checkAndPut(byte[] row, byte[] family, byte[] qualifier,
            CompareOp compareOp, byte[] value, Put put) throws IOException {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean checkAndDelete(byte[] row, byte[] family, byte[] qualifier,
            byte[] value, Delete delete) throws IOException {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean checkAndDelete(byte[] row, byte[] family, byte[] qualifier,
            CompareOp compareOp, byte[] value, Delete delete)
            throws IOException {
        throw new UnsupportedOperationException();
    }

    @Override
    public void mutateRow(RowMutations rm) throws IOException {
        throw new UnsupportedOperationException();
    }

    @Override
    public Result append(Append append) throws IOException {
        throw new UnsupportedOperationException();
    }

    @Override
    public Result increment(Increment increment) throws IOException {
        throw new UnsupportedOperationException();
    }

    @Override
    public long incrementColumnValue(byte[] row, byte[] family,
            byte[] qualifier, long amount) throws IOException {
        throw new UnsupportedOperationException();
    }

    @Override
    public long incrementColumnValue(byte[] row, byte[] family,
            byte[] qualifier, long amount, Durability durability)
            throws IOException {
        throw new UnsupportedOperationException();
    }

    @Override
    public CoprocessorRpcChannel coprocessorService(byte[] row) {
        throw new UnsupportedOperationException();
    }

    @Override
    public <T extends Service, R> Map<byte[], R> coprocessorService(
            Class<T> service, byte[] startKey, byte[] endKey,
            Call<T, R> callable) throws ServiceException, Throwable {
        throw new UnsupportedOperationException();
    }

    @Override
    public <T extends Service, R> void coprocessorService(Class<T> service,
            byte[] startKey, byte[] endKey, Call<T, R> callable,
            Callback<R> callback) throws ServiceException, Throwable {
        throw new UnsupportedOperationException();
    }

    @Override
    public <R extends Message> Map<byte[], R> batchCoprocessorService(
            MethodDescriptor methodDescriptor, Message request,
            byte[] startKey, byte[] endKey, R responsePrototype)
            throws ServiceException, Throwable {
        throw new UnsupportedOperationException();
    }

    @Override
    public <R extends Message> void batchCoprocessorService(
            MethodDescriptor methodDescriptor, Message request,
            byte[] startKey, byte[] endKey, R responsePrototype,
            Callback<R> callback) throws ServiceException, Throwable {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean checkAndMutate(byte[] row, byte[] family, byte[] qualifier,
            CompareOp compareOp, byte[] value, RowMutations mutation)
            throws IOException {
        throw new UnsupportedOperationException();
    }

    @Override
    public long getWriteBufferSize() {
        throw new UnsupportedOperationException();
    }

    @Override
    public void setWriteBufferSize(long writeBufferSize) throws IOException {
        throw new UnsupportedOperationException();
    }
}
