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
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.client.coprocessor.Batch.Call;
import org.apache.hadoop.hbase.client.coprocessor.Batch.Callback;
import org.apache.hadoop.hbase.filter.CompareFilter.CompareOp;
import org.apache.hadoop.hbase.ipc.CoprocessorRpcChannel;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hdfs.server.namenode.UnsupportedActionException;
import org.apache.omid.transaction.TTable;
import org.apache.omid.transaction.Transaction;
import org.apache.phoenix.exception.SQLExceptionCode;
import org.apache.phoenix.exception.SQLExceptionInfo;
import org.apache.phoenix.index.PhoenixIndexCodec;
import org.apache.phoenix.schema.PTableType;
import org.apache.phoenix.schema.PTable;

import com.google.protobuf.Descriptors.MethodDescriptor;
import com.google.protobuf.Message;
import com.google.protobuf.Service;
import com.google.protobuf.ServiceException;

public class OmidTransactionTable implements PhoenixTransactionalTable {

    private TTable tTable;
    private Transaction tx;
    private boolean conflictFree;

    public OmidTransactionTable() throws SQLException {
        this.tTable = null;
        this.tx = null;
        this.conflictFree = false;
    }

    public OmidTransactionTable(PhoenixTransactionContext ctx, HTableInterface hTable) throws SQLException {
        this(ctx, hTable, false);
    }

    public OmidTransactionTable(PhoenixTransactionContext ctx, HTableInterface hTable, boolean isImmutable) throws SQLException  {
        assert(ctx instanceof OmidTransactionContext);

        OmidTransactionContext omidTransactionContext = (OmidTransactionContext) ctx;

        try {
            tTable = new TTable(hTable, true);
        } catch (IOException e) {
            throw new SQLExceptionInfo.Builder(
                    SQLExceptionCode.TRANSACTION_FAILED)
            .setMessage(e.getMessage()).setRootCause(e).build()
            .buildException();
        }

        this.tx = omidTransactionContext.getTransaction();

//        if (pTable != null && pTable.getType() != PTableType.INDEX) {
//            omidTransactionContext.markDMLFence(pTable);
//        }

        this.conflictFree = isImmutable;
    }

    @Override
    public Result get(Get get) throws IOException {
        return tTable.get(tx, get);
    }

    @Override
    public void put(Put put) throws IOException {
        tTable.put(tx, put);
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
    public byte[] getTableName() {
        return tTable.getTableName();
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
        throw new UnsupportedActionException("Function put(List<Put>) is not supported");
    }

    @Override
    public void delete(List<Delete> deletes) throws IOException {
        tTable.delete(tx, deletes);
    }

    @Override
    public void setAutoFlush(boolean autoFlush) {
        tTable.setAutoFlush(autoFlush);
    }

    @Override
    public boolean isAutoFlush() {
        return tTable.isAutoFlush();
    }

    @Override
    public long getWriteBufferSize() {
        return tTable.getWriteBufferSize();
    }

    @Override
    public void setWriteBufferSize(long writeBufferSize) throws IOException {
        tTable.setWriteBufferSize(writeBufferSize);
    }

    @Override
    public void flushCommits() throws IOException {
        tTable.flushCommits();
    }

    @Override
    public void close() throws IOException {
        tTable.close();
    }

    @Override
    public Put MarkPutAsCommitted(Put put, long timestamp, long commitTimestamp) throws IOException {
        return TTable.markPutAsCommitted(put, timestamp, timestamp);
    }

    @Override
    public long incrementColumnValue(byte[] row, byte[] family,
            byte[] qualifier, long amount, boolean writeToWAL)
            throws IOException {
        // TODO Auto-generated method stub
        return 0;
    }

    @Override
    public Boolean[] exists(List<Get> gets) throws IOException {
            // TODO Auto-generated method stub
            return null;
    }

    @Override
    public void setAutoFlush(boolean autoFlush, boolean clearBufferOnFail) {
        // TODO Auto-generated method stub
    }

    @Override
    public void setAutoFlushTo(boolean autoFlush) {
        tTable.setAutoFlush(autoFlush);
    }

    @Override
    public Result getRowOrBefore(byte[] row, byte[] family) throws IOException {
        throw new UnsupportedActionException("Function getRowOrBefore is not supported");
//        return null;
    }

    @Override
    public TableName getName() {
        assert(false);
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public boolean[] existsAll(List<Get> gets) throws IOException {
        throw new UnsupportedActionException("Function existsAll is not supported");
    }

    @Override
    public void batch(List<? extends Row> actions, Object[] results)
            throws IOException, InterruptedException {
        assert(false);

        // TODO Auto-generated method stub
    }

    @Override
    public Object[] batch(List<? extends Row> actions) throws IOException,
            InterruptedException {
        List<Put> putList = new ArrayList<Put>();

       for (Row row : actions) {
           if (row instanceof Put) {
               Put put = (Put) row;
               if (conflictFree) {
                   tTable.markPutAsConflictFreeMutation(put);
               }
               putList.add(put);
           } else {
               // TODO implement delete batch
               assert (row instanceof Delete);
               this.delete((Delete) row);
           }
       }

       tTable.put(tx, putList);

       return null;
    }

    @Override
    public <R> void batchCallback(List<? extends Row> actions,
            Object[] results, Callback<R> callback) throws IOException,
            InterruptedException {
        assert(false);

        // TODO Auto-generated method stub
    }

    @Override
    public <R> Object[] batchCallback(List<? extends Row> actions,
            Callback<R> callback) throws IOException, InterruptedException {
        assert(false);

        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public boolean checkAndPut(byte[] row, byte[] family, byte[] qualifier,
            byte[] value, Put put) throws IOException {
        assert(false);

        // TODO Auto-generated method stub
        return false;
    }

    @Override
    public boolean checkAndPut(byte[] row, byte[] family, byte[] qualifier,
            CompareOp compareOp, byte[] value, Put put) throws IOException {
        assert(false);

        // TODO Auto-generated method stub
        return false;
    }

    @Override
    public boolean checkAndDelete(byte[] row, byte[] family, byte[] qualifier,
            byte[] value, Delete delete) throws IOException {
        // TODO Auto-generated method stub
        assert(false);

        return false;
    }

    @Override
    public boolean checkAndDelete(byte[] row, byte[] family, byte[] qualifier,
            CompareOp compareOp, byte[] value, Delete delete)
            throws IOException {
        assert(false);

        // TODO Auto-generated method stub
        return false;
    }

    @Override
    public void mutateRow(RowMutations rm) throws IOException {
        assert(false);

        // TODO Auto-generated method stub
    }

    @Override
    public Result append(Append append) throws IOException {
        assert(false);

        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public Result increment(Increment increment) throws IOException {
        assert(false);

        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public long incrementColumnValue(byte[] row, byte[] family,
            byte[] qualifier, long amount) throws IOException {
        assert(false);

        // TODO Auto-generated method stub
        return 0;
    }

    @Override
    public long incrementColumnValue(byte[] row, byte[] family,
            byte[] qualifier, long amount, Durability durability)
            throws IOException {
        assert(false);

        // TODO Auto-generated method stub
        return 0;
    }

    @Override
    public CoprocessorRpcChannel coprocessorService(byte[] row) {
        assert(false);

        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public <T extends Service, R> Map<byte[], R> coprocessorService(
            Class<T> service, byte[] startKey, byte[] endKey,
            Call<T, R> callable) throws ServiceException, Throwable {
        assert(false);

        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public <T extends Service, R> void coprocessorService(Class<T> service,
            byte[] startKey, byte[] endKey, Call<T, R> callable,
            Callback<R> callback) throws ServiceException, Throwable {
        assert(false);

        // TODO Auto-generated method stub
    }

    @Override
    public <R extends Message> Map<byte[], R> batchCoprocessorService(
            MethodDescriptor methodDescriptor, Message request,
            byte[] startKey, byte[] endKey, R responsePrototype)
            throws ServiceException, Throwable {
        assert(false);

        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public <R extends Message> void batchCoprocessorService(
            MethodDescriptor methodDescriptor, Message request,
            byte[] startKey, byte[] endKey, R responsePrototype,
            Callback<R> callback) throws ServiceException, Throwable {
        assert(false);

        // TODO Auto-generated method stub
    }

    @Override
    public boolean checkAndMutate(byte[] row, byte[] family, byte[] qualifier,
            CompareOp compareOp, byte[] value, RowMutations mutation)
            throws IOException {
        assert(false);

        // TODO Auto-generated method stub
        return false;
    }

    @Override
    public int getOperationTimeout() {
        assert(false);

        // TODO Auto-generated method stub
        return 0;
    }

    @Override
    public int getRpcTimeout() {
        assert(false);

        // TODO Auto-generated method stub
        return 0;
    }

    @Override
    public void setOperationTimeout(int arg0) {
        assert(false);

        // TODO Auto-generated method stub
        
    }

    @Override
    public void setRpcTimeout(int arg0) {
        assert(false);

        // TODO Auto-generated method stub
        
    }

//    @Override
//    public int getReadRpcTimeout() {
//      assert(false);
//
//        // TODO Auto-generated method stub
//      return 0;
//    }
//
//    @Override
//    public void setReadRpcTimeout(int readRpcTimeout) {
//      assert(false);
//
//      // TODO Auto-generated method stub
//    }
//
//    @Override
//    public int getWriteRpcTimeout() {
//      assert(false);
//
//      // TODO Auto-generated method stub
//      return 0;
//    }
//
//    @Override
//    public void setWriteRpcTimeout(int writeRpcTimeout) {
//      assert(false);
//
//      // TODO Auto-generated method stub
//
//    }
}
