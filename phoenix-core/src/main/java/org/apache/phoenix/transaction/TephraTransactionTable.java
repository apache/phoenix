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
import java.util.List;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.CompareOperator;
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
import org.apache.hadoop.hbase.client.TableDescriptor;
import org.apache.hadoop.hbase.client.coprocessor.Batch.Call;
import org.apache.hadoop.hbase.client.coprocessor.Batch.Callback;
import org.apache.hadoop.hbase.filter.CompareFilter.CompareOp;
import org.apache.hadoop.hbase.ipc.CoprocessorRpcChannel;
import org.apache.tephra.TxConstants;
import org.apache.tephra.hbase.TransactionAwareHTable;
import org.apache.phoenix.schema.PTable;
import org.apache.phoenix.schema.PTableType;

import com.google.protobuf.Descriptors.MethodDescriptor;
import com.google.protobuf.Message;
import com.google.protobuf.Service;
import com.google.protobuf.ServiceException;

public class TephraTransactionTable implements PhoenixTransactionalTable {

    private TransactionAwareHTable transactionAwareHTable;

    private TephraTransactionContext tephraTransactionContext;

    public TephraTransactionTable(PhoenixTransactionContext ctx, Table hTable) {
        this(ctx, hTable, null);
    }

    public TephraTransactionTable(PhoenixTransactionContext ctx, Table hTable, PTable pTable) {

        assert(ctx instanceof TephraTransactionContext);

        tephraTransactionContext = (TephraTransactionContext) ctx;

        transactionAwareHTable = new TransactionAwareHTable(hTable, (pTable != null && pTable.isImmutableRows()) ? TxConstants.ConflictDetection.NONE : TxConstants.ConflictDetection.ROW);

        tephraTransactionContext.addTransactionAware(transactionAwareHTable);

        if (pTable != null && pTable.getType() != PTableType.INDEX) {
            tephraTransactionContext.markDMLFence(pTable);
        }
    }

    @Override
    public Result get(Get get) throws IOException {
        return transactionAwareHTable.get(get);
    }

    @Override
    public void put(Put put) throws IOException {
        transactionAwareHTable.put(put);
    }

    @Override
    public void delete(Delete delete) throws IOException {
        transactionAwareHTable.delete(delete);
    }

    @Override
    public ResultScanner getScanner(Scan scan) throws IOException {
        return transactionAwareHTable.getScanner(scan);
    }

    @Override
    public byte[] getTableName() {
        return transactionAwareHTable.getTableName();
    }

    @Override
    public Configuration getConfiguration() {
        return transactionAwareHTable.getConfiguration();
    }

    @Override
    public HTableDescriptor getTableDescriptor() throws IOException {
        return transactionAwareHTable.getTableDescriptor();
    }

    @Override
    public boolean exists(Get get) throws IOException {
        return transactionAwareHTable.exists(get);
    }

    @Override
    public Result[] get(List<Get> gets) throws IOException {
        return transactionAwareHTable.get(gets);
    }

    @Override
    public ResultScanner getScanner(byte[] family) throws IOException {
        return transactionAwareHTable.getScanner(family);
    }

    @Override
    public ResultScanner getScanner(byte[] family, byte[] qualifier)
            throws IOException {
        return transactionAwareHTable.getScanner(family, qualifier);
    }

    @Override
    public void put(List<Put> puts) throws IOException {
        transactionAwareHTable.put(puts);
    }

    @Override
    public void delete(List<Delete> deletes) throws IOException {
        transactionAwareHTable.delete(deletes);
    }

    @Override
    public void setAutoFlush(boolean autoFlush) {
        transactionAwareHTable.setAutoFlush(autoFlush);
    }

    @Override
    public boolean isAutoFlush() {
        return transactionAwareHTable.isAutoFlush();
    }

    @Override
    public long getWriteBufferSize() {
        return transactionAwareHTable.getWriteBufferSize();
    }

    @Override
    public void setWriteBufferSize(long writeBufferSize) throws IOException {
        transactionAwareHTable.setWriteBufferSize(writeBufferSize);
    }

    @Override
    public void flushCommits() throws IOException {
        transactionAwareHTable.flushCommits();
    }

    @Override
    public void close() throws IOException {
        transactionAwareHTable.close();
    }


    @Override
    public TableName getName() {
        return transactionAwareHTable.getName();
    }

    @Override
    public boolean[] existsAll(List<Get> gets) throws IOException {
        return transactionAwareHTable.existsAll(gets);
    }

    @Override
    public void batch(List<? extends Row> actions, Object[] results)
            throws IOException, InterruptedException {
        transactionAwareHTable.batch(actions, results);
    }

    @Override
    public <R> void batchCallback(List<? extends Row> actions,
            Object[] results, Callback<R> callback) throws IOException,
            InterruptedException {
        transactionAwareHTable.batchCallback(actions, results, callback);
    }

    @Override
    public boolean checkAndPut(byte[] row, byte[] family, byte[] qualifier,
            byte[] value, Put put) throws IOException {
        return transactionAwareHTable.checkAndPut(row, family, qualifier, value, put);
    }

    @Override
    public boolean checkAndPut(byte[] row, byte[] family, byte[] qualifier,
            CompareOp compareOp, byte[] value, Put put) throws IOException {
        return transactionAwareHTable.checkAndPut(row, family, qualifier, compareOp, value, put);
    }

    @Override
    public boolean checkAndDelete(byte[] row, byte[] family, byte[] qualifier,
            byte[] value, Delete delete) throws IOException {
        return transactionAwareHTable.checkAndDelete(row, family, qualifier, value, delete);
    }

    @Override
    public boolean checkAndDelete(byte[] row, byte[] family, byte[] qualifier,
            CompareOp compareOp, byte[] value, Delete delete)
            throws IOException {
        return transactionAwareHTable.checkAndDelete(row, family, qualifier, compareOp, value, delete);
    }

    @Override
    public void mutateRow(RowMutations rm) throws IOException {
        transactionAwareHTable.mutateRow(rm);
    }

    @Override
    public Result append(Append append) throws IOException {
        return transactionAwareHTable.append(append);
    }

    @Override
    public Result increment(Increment increment) throws IOException {
        return transactionAwareHTable.increment(increment);
    }

    @Override
    public long incrementColumnValue(byte[] row, byte[] family,
            byte[] qualifier, long amount) throws IOException {
        return transactionAwareHTable.incrementColumnValue(row, family, qualifier, amount);
    }

    @Override
    public long incrementColumnValue(byte[] row, byte[] family,
            byte[] qualifier, long amount, Durability durability)
            throws IOException {
        return transactionAwareHTable.incrementColumnValue(row, family, qualifier, amount, durability);
    }

    @Override
    public CoprocessorRpcChannel coprocessorService(byte[] row) {
        return transactionAwareHTable.coprocessorService(row);
    }

    @Override
    public <T extends Service, R> Map<byte[], R> coprocessorService(
            Class<T> service, byte[] startKey, byte[] endKey,
            Call<T, R> callable) throws ServiceException, Throwable {
        return transactionAwareHTable.coprocessorService(service, startKey, endKey, callable);
    }

    @Override
    public <T extends Service, R> void coprocessorService(Class<T> service,
            byte[] startKey, byte[] endKey, Call<T, R> callable,
            Callback<R> callback) throws ServiceException, Throwable {
        transactionAwareHTable.coprocessorService(service, startKey, endKey, callable, callback);
    }

    @Override
    public <R extends Message> Map<byte[], R> batchCoprocessorService(
            MethodDescriptor methodDescriptor, Message request,
            byte[] startKey, byte[] endKey, R responsePrototype)
            throws ServiceException, Throwable {
        return transactionAwareHTable.batchCoprocessorService(methodDescriptor, request, startKey, endKey, responsePrototype);
    }

    @Override
    public <R extends Message> void batchCoprocessorService(
            MethodDescriptor methodDescriptor, Message request,
            byte[] startKey, byte[] endKey, R responsePrototype,
            Callback<R> callback) throws ServiceException, Throwable {
        transactionAwareHTable.batchCoprocessorService(methodDescriptor, request, startKey, endKey, responsePrototype, callback);
    }

    @Override
    public boolean checkAndMutate(byte[] row, byte[] family, byte[] qualifier,
            CompareOp compareOp, byte[] value, RowMutations mutation)
            throws IOException {
        return transactionAwareHTable.checkAndMutate(row, family, qualifier, compareOp, value, mutation);
    }

    @Override
    public void setOperationTimeout(int operationTimeout) {
        transactionAwareHTable.setOperationTimeout(operationTimeout);
    }

    @Override
    public int getOperationTimeout() {
        return transactionAwareHTable.getOperationTimeout();
    }

    @Override
    public void setRpcTimeout(int rpcTimeout) {
        transactionAwareHTable.setRpcTimeout(rpcTimeout);
    }

    @Override
    public int getRpcTimeout() {
        return transactionAwareHTable.getRpcTimeout();
    }

    @Override
    public TableDescriptor getDescriptor() throws IOException {
        return transactionAwareHTable.getDescriptor();
    }

    @Override
    public boolean checkAndPut(byte[] row, byte[] family, byte[] qualifier, CompareOperator op,
            byte[] value, Put put) throws IOException {
        return transactionAwareHTable.checkAndPut(row, family, qualifier, op, value, put);
    }

    @Override
    public boolean checkAndDelete(byte[] row, byte[] family, byte[] qualifier, CompareOperator op,
            byte[] value, Delete delete) throws IOException {
        return transactionAwareHTable.checkAndDelete(row, family, qualifier, op, value, delete);
    }

    @Override
    public boolean checkAndMutate(byte[] row, byte[] family, byte[] qualifier, CompareOperator op,
            byte[] value, RowMutations mutation) throws IOException {
        return transactionAwareHTable.checkAndMutate(row, family, qualifier, op, value, mutations);
    }

    @Override
    public int getReadRpcTimeout() {
        return transactionAwareHTable.getReadRpcTimeout();
    }

    @Override
    public void setReadRpcTimeout(int readRpcTimeout) {
        transactionAwareHTable.setReadRpcTimeout(readRpcTimeout);
    }

    @Override
    public int getWriteRpcTimeout() {
        return transactionAwareHTable.getWriteRpcTimeout();
    }

    @Override
    public void setWriteRpcTimeout(int writeRpcTimeout) {
        return transactionAwareHTable.setWriteRpcTimeout(writeRpcTimeout);
    }
}
