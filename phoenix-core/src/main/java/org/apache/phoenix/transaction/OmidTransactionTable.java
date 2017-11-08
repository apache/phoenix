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

public class OmidTransactionTable implements PhoenixTransactionalTable {

    public OmidTransactionTable(PhoenixTransactionContext ctx, Table hTable) {
        // TODO Auto-generated constructor stub
    }

    @Override
    public Result get(Get get) throws IOException {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public void put(Put put) throws IOException {
        // TODO Auto-generated method stub

    }

    @Override
    public void delete(Delete delete) throws IOException {
        // TODO Auto-generated method stub

    }

    @Override
    public ResultScanner getScanner(Scan scan) throws IOException {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public byte[] getTableName() {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public Configuration getConfiguration() {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public HTableDescriptor getTableDescriptor() throws IOException {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public boolean exists(Get get) throws IOException {
        // TODO Auto-generated method stub
        return false;
    }

    @Override
    public Result[] get(List<Get> gets) throws IOException {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public ResultScanner getScanner(byte[] family) throws IOException {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public ResultScanner getScanner(byte[] family, byte[] qualifier)
            throws IOException {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public void put(List<Put> puts) throws IOException {
        // TODO Auto-generated method stub
    }

    @Override
    public void delete(List<Delete> deletes) throws IOException {
        // TODO Auto-generated method stub
    }

    @Override
    public void setAutoFlush(boolean autoFlush) {
        // TODO Auto-generated method stub
    }

    @Override
    public boolean isAutoFlush() {
        // TODO Auto-generated method stub
        return false;
    }

    @Override
    public long getWriteBufferSize() {
        // TODO Auto-generated method stub
        return 0;
    }

    @Override
    public void setWriteBufferSize(long writeBufferSize) throws IOException {
        // TODO Auto-generated method stub
    }

    @Override
    public void flushCommits() throws IOException {
        // TODO Auto-generated method stub
    }

    @Override
    public void close() throws IOException {
        // TODO Auto-generated method stub
    }

    @Override
    public TableName getName() {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public boolean[] existsAll(List<Get> gets) throws IOException {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public void batch(List<? extends Row> actions, Object[] results)
            throws IOException, InterruptedException {
        // TODO Auto-generated method stub
    }

    @Override
    public Object[] batch(List<? extends Row> actions) throws IOException,
            InterruptedException {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public <R> void batchCallback(List<? extends Row> actions,
            Object[] results, Callback<R> callback) throws IOException,
            InterruptedException {
        // TODO Auto-generated method stub
    }

    @Override
    public <R> Object[] batchCallback(List<? extends Row> actions,
            Callback<R> callback) throws IOException, InterruptedException {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public boolean checkAndPut(byte[] row, byte[] family, byte[] qualifier,
            byte[] value, Put put) throws IOException {
        // TODO Auto-generated method stub
        return false;
    }

    @Override
    public boolean checkAndPut(byte[] row, byte[] family, byte[] qualifier,
            CompareOp compareOp, byte[] value, Put put) throws IOException {
        // TODO Auto-generated method stub
        return false;
    }

    @Override
    public boolean checkAndDelete(byte[] row, byte[] family, byte[] qualifier,
            byte[] value, Delete delete) throws IOException {
        // TODO Auto-generated method stub
        return false;
    }

    @Override
    public boolean checkAndDelete(byte[] row, byte[] family, byte[] qualifier,
            CompareOp compareOp, byte[] value, Delete delete)
            throws IOException {
        // TODO Auto-generated method stub
        return false;
    }

    @Override
    public void mutateRow(RowMutations rm) throws IOException {
        // TODO Auto-generated method stub
    }

    @Override
    public Result append(Append append) throws IOException {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public Result increment(Increment increment) throws IOException {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public long incrementColumnValue(byte[] row, byte[] family,
            byte[] qualifier, long amount) throws IOException {
        // TODO Auto-generated method stub
        return 0;
    }

    @Override
    public long incrementColumnValue(byte[] row, byte[] family,
            byte[] qualifier, long amount, Durability durability)
            throws IOException {
        // TODO Auto-generated method stub
        return 0;
    }

    @Override
    public CoprocessorRpcChannel coprocessorService(byte[] row) {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public <T extends Service, R> Map<byte[], R> coprocessorService(
            Class<T> service, byte[] startKey, byte[] endKey,
            Call<T, R> callable) throws ServiceException, Throwable {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public <T extends Service, R> void coprocessorService(Class<T> service,
            byte[] startKey, byte[] endKey, Call<T, R> callable,
            Callback<R> callback) throws ServiceException, Throwable {
        // TODO Auto-generated method stub
    }

    @Override
    public <R extends Message> Map<byte[], R> batchCoprocessorService(
            MethodDescriptor methodDescriptor, Message request,
            byte[] startKey, byte[] endKey, R responsePrototype)
            throws ServiceException, Throwable {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public <R extends Message> void batchCoprocessorService(
            MethodDescriptor methodDescriptor, Message request,
            byte[] startKey, byte[] endKey, R responsePrototype,
            Callback<R> callback) throws ServiceException, Throwable {
        // TODO Auto-generated method stub
    }

    @Override
    public boolean checkAndMutate(byte[] row, byte[] family, byte[] qualifier,
            CompareOp compareOp, byte[] value, RowMutations mutation)
            throws IOException {
        // TODO Auto-generated method stub
        return false;
    }

    @Override
    public int getOperationTimeout() {
        // TODO Auto-generated method stub
        return 0;
    }

    @Override
    public int getRpcTimeout() {
        // TODO Auto-generated method stub
        return 0;
    }

    @Override
    public void setOperationTimeout(int arg0) {
        // TODO Auto-generated method stub
        
    }

    @Override
    public void setRpcTimeout(int arg0) {
        // TODO Auto-generated method stub
        
    }

}
