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
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

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
import org.apache.omid.transaction.TTable;
import org.apache.omid.transaction.Transaction;
import org.apache.phoenix.compat.hbase.CompatOmidTransactionTable;
import org.apache.phoenix.exception.SQLExceptionCode;
import org.apache.phoenix.exception.SQLExceptionInfo;

import com.google.protobuf.Descriptors.MethodDescriptor;
import com.google.protobuf.Message;
import com.google.protobuf.Service;
import com.google.protobuf.ServiceException;

public class OmidTransactionTable extends CompatOmidTransactionTable implements Table {
    // Copied from HBase ProtobufUtil since it's not accessible
    final static Result EMPTY_RESULT_EXISTS_TRUE = Result.create(null, true);

    private TTable tTable;
    private Transaction tx;
    private final boolean addShadowCells;

    public OmidTransactionTable() throws SQLException {
        this.tTable = null;
        this.tx = null;
        this.addShadowCells = false;
    }

    public OmidTransactionTable(PhoenixTransactionContext ctx, Table hTable) throws SQLException {
        this(ctx, hTable, false);
    }

    public OmidTransactionTable(PhoenixTransactionContext ctx, Table hTable, boolean isConflictFree) throws SQLException  {
        this(ctx, hTable, isConflictFree, false);
    }

    public OmidTransactionTable(PhoenixTransactionContext ctx, Table hTable, boolean isConflictFree, boolean addShadowCells) throws SQLException  {
        assert(ctx instanceof OmidTransactionContext);

        OmidTransactionContext omidTransactionContext = (OmidTransactionContext) ctx;
        this.addShadowCells = addShadowCells;
        try {
            tTable = new TTable(hTable, true, isConflictFree);
        } catch (IOException e) {
            throw new SQLExceptionInfo.Builder(
                    SQLExceptionCode.TRANSACTION_FAILED)
            .setMessage(e.getMessage()).setRootCause(e).build()
            .buildException();
        }

        this.tx = omidTransactionContext.getTransaction();
    }

    @Override
    public Result get(Get get) throws IOException {
        return tTable.get(tx, get);
    }

    @Override
    public void put(Put put) throws IOException {
        tTable.put(tx, put, addShadowCells);
    }

    @Override
    public void delete(Delete delete) throws IOException {
        tTable.delete(tx, delete);
    }

    @Override
    public ResultScanner getScanner(Scan scan) throws IOException {
        scan.setTimeRange(0, Long.MAX_VALUE);
        return tTable.getScanner(tx, scan);
    }

    @Override
    public Configuration getConfiguration() {
        return tTable.getConfiguration();
    }

    @Override
    public HTableDescriptor getTableDescriptor() throws IOException {
        return tTable.getTableDescriptor();
    }

    @Override
    public boolean exists(Get get) throws IOException {
       return tTable.exists(tx, get);
    }

    @Override
    public Result[] get(List<Get> gets) throws IOException {
        return tTable.get(tx, gets);
    }

    @Override
    public ResultScanner getScanner(byte[] family) throws IOException {
        return tTable.getScanner(tx, family);
    }

    @Override
    public ResultScanner getScanner(byte[] family, byte[] qualifier)
            throws IOException {
        return tTable.getScanner(tx, family, qualifier);
    }

    @Override
    public void put(List<Put> puts) throws IOException {
        tTable.put(tx, puts, addShadowCells);
    }

    @Override
    public void delete(List<Delete> deletes) throws IOException {
        tTable.delete(tx, deletes);
    }

    @Override
    public void close() throws IOException {
        tTable.close();
    }

    @Override
    public TableName getName() {
        byte[] name = tTable.getTableName();
        return TableName.valueOf(name);
    }

    @Override
    public boolean[] existsAll(List<Get> gets) throws IOException {
        throw new UnsupportedOperationException();
    }

    @Override
    public void batch(List<? extends Row> actions, Object[] results)
            throws IOException, InterruptedException {
        tTable.batch(tx, actions, addShadowCells);
        if (results != null) {
            Arrays.fill(results, EMPTY_RESULT_EXISTS_TRUE);
        }
    }

    @Override
    public <R> void batchCallback(List<? extends Row> actions,
            Object[] results, Callback<R> callback) throws IOException,
            InterruptedException {
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
    public int getOperationTimeout() {
        throw new UnsupportedOperationException();
    }

    @Override
    public int getRpcTimeout() {
        throw new UnsupportedOperationException();
    }

    @Override
    public void setOperationTimeout(int arg0) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void setRpcTimeout(int arg0) {
        throw new UnsupportedOperationException();
    }

    @Override
    public int getWriteRpcTimeout() {
        throw new UnsupportedOperationException();
    }

    @Override
    public void setWriteRpcTimeout(int writeRpcTimeout) {
        throw new UnsupportedOperationException();
    }

    @Override
    public int getReadRpcTimeout() {
        throw new UnsupportedOperationException();
    }

    @Override
    public void setReadRpcTimeout(int readRpcTimeout) {
        throw new UnsupportedOperationException();
    }

    @Override
    public TableDescriptor getDescriptor() throws IOException {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean[] exists(List<Get> gets) throws IOException {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean checkAndPut(byte[] row, byte[] family, byte[] qualifier, CompareOperator op, byte[] value, Put put)
            throws IOException {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean checkAndDelete(byte[] row, byte[] family, byte[] qualifier, CompareOperator op, byte[] value,
            Delete delete) throws IOException {
        throw new UnsupportedOperationException();
    }

    @Override
    public CheckAndMutateBuilder checkAndMutate(byte[] row, byte[] family) {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean checkAndMutate(byte[] row, byte[] family, byte[] qualifier, CompareOperator op, byte[] value,
            RowMutations mutation) throws IOException {
        throw new UnsupportedOperationException();
    }

    @Override
    public long getRpcTimeout(TimeUnit unit) {
        throw new UnsupportedOperationException();
    }

    @Override
    public long getReadRpcTimeout(TimeUnit unit) {
        throw new UnsupportedOperationException();
    }

    @Override
    public long getWriteRpcTimeout(TimeUnit unit) {
        throw new UnsupportedOperationException();
    }

    @Override
    public long getOperationTimeout(TimeUnit unit) {
        throw new UnsupportedOperationException();
    }

}
