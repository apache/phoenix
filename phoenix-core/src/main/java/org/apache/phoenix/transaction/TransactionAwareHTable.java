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
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.NavigableMap;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.CompareOperator;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Append;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Durability;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Increment;
import org.apache.hadoop.hbase.client.Mutation;
import org.apache.hadoop.hbase.client.OperationWithAttributes;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Row;
import org.apache.hadoop.hbase.client.RowMutations;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.client.TableDescriptor;
import org.apache.hadoop.hbase.client.coprocessor.Batch;
import org.apache.hadoop.hbase.client.coprocessor.Batch.Callback;
import org.apache.hadoop.hbase.filter.CompareFilter;
import org.apache.hadoop.hbase.ipc.CoprocessorRpcChannel;
import org.apache.tephra.AbstractTransactionAwareTable;
import org.apache.tephra.Transaction;
import org.apache.tephra.TransactionAware;
import org.apache.tephra.TxConstants;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.protobuf.Descriptors.MethodDescriptor;
import com.google.protobuf.Message;
import com.google.protobuf.Service;
import com.google.protobuf.ServiceException;

/**
 * A Transaction Aware HTable implementation for HBase 1.1. Operations are committed as usual, but upon a failed or
 * aborted transaction, they are rolled back to the state before the transaction was started.
 */
public class TransactionAwareHTable extends AbstractTransactionAwareTable implements Table, TransactionAware {

    private static final Logger LOG = LoggerFactory.getLogger(TransactionAwareHTable.class);
    private final Table hTable;

    /**
     * Create a transactional aware instance of the passed HTable
     *
     * @param hTable
     *            underlying HBase table to use
     */
    public TransactionAwareHTable(Table hTable) {
        this(hTable, false);
    }

    /**
     * Create a transactional aware instance of the passed HTable
     *
     * @param hTable
     *            underlying HBase table to use
     * @param conflictLevel
     *            level of conflict detection to perform (defaults to {@code COLUMN})
     */
    public TransactionAwareHTable(Table hTable, TxConstants.ConflictDetection conflictLevel) {
        this(hTable, conflictLevel, false);
    }

    /**
     * Create a transactional aware instance of the passed HTable, with the option of allowing non-transactional
     * operations.
     * 
     * @param hTable
     *            underlying HBase table to use
     * @param allowNonTransactional
     *            if true, additional operations (checkAndPut, increment, checkAndDelete) will be available, though
     *            non-transactional
     */
    public TransactionAwareHTable(Table hTable, boolean allowNonTransactional) {
        this(hTable, TxConstants.ConflictDetection.COLUMN, allowNonTransactional);
    }

    /**
     * Create a transactional aware instance of the passed HTable, with the option of allowing non-transactional
     * operations.
     * 
     * @param hTable
     *            underlying HBase table to use
     * @param conflictLevel
     *            level of conflict detection to perform (defaults to {@code COLUMN})
     * @param allowNonTransactional
     *            if true, additional operations (checkAndPut, increment, checkAndDelete) will be available, though
     *            non-transactional
     */
    public TransactionAwareHTable(Table hTable, TxConstants.ConflictDetection conflictLevel,
            boolean allowNonTransactional) {
        super(conflictLevel, allowNonTransactional);
        this.hTable = hTable;
    }

    /* AbstractTransactionAwareTable implementation */

    @Override
    protected byte[] getTableKey() {
        return hTable.getName().getName();
    }

    @Override
    protected boolean doCommit() throws IOException {
        return true;
    }

    @Override
    protected boolean doRollback() throws Exception {
        try {
            // pre-size arraylist of deletes
            int size = 0;
            for (Set<ActionChange> cs : changeSets.values()) {
                size += cs.size();
            }
            List<Delete> rollbackDeletes = new ArrayList<>(size);
            for (Map.Entry<Long, Set<ActionChange>> entry : changeSets.entrySet()) {
                long transactionTimestamp = entry.getKey();
                for (ActionChange change : entry.getValue()) {
                    byte[] row = change.getRow();
                    byte[] family = change.getFamily();
                    byte[] qualifier = change.getQualifier();
                    Delete rollbackDelete = new Delete(row);
                    makeRollbackOperation(rollbackDelete);
                    switch (conflictLevel) {
                    case ROW:
                    case NONE:
                        // issue family delete for the tx write pointer
                        rollbackDelete.addFamilyVersion(change.getFamily(), transactionTimestamp);
                        break;
                    case COLUMN:
                        if (family != null && qualifier == null) {
                            rollbackDelete.addFamilyVersion(family, transactionTimestamp);
                        } else if (family != null && qualifier != null) {
                            rollbackDelete.addColumn(family, qualifier, transactionTimestamp);
                        }
                        break;
                    default:
                        throw new IllegalStateException("Unknown conflict detection level: " + conflictLevel);
                    }
                    rollbackDeletes.add(rollbackDelete);
                }
            }
            hTable.delete(rollbackDeletes);
            return true;
        } finally {
            tx = null;
            changeSets.clear();
        }
    }

    /* HTableInterface implementation */


    @Override
    public TableName getName() {
        return hTable.getName();
    }

    @Override
    public Configuration getConfiguration() {
        return hTable.getConfiguration();
    }

    @Override
    public HTableDescriptor getTableDescriptor() throws IOException {
        return hTable.getTableDescriptor();
    }

    @Override
    public boolean exists(Get get) throws IOException {
        if (tx == null) { throw new IOException("Transaction not started"); }
        return hTable.exists(transactionalizeAction(get));
    }


    @Override
    public void batch(List<? extends Row> actions, Object[] results) throws IOException, InterruptedException {
        if (tx == null) { throw new IOException("Transaction not started"); }
        hTable.batch(transactionalizeActions(actions), results);
    }

    @Override
    public <R> void batchCallback(List<? extends Row> actions, Object[] results, Batch.Callback<R> callback)
            throws IOException, InterruptedException {
        if (tx == null) { throw new IOException("Transaction not started"); }
        hTable.batchCallback(transactionalizeActions(actions), results, callback);
    }

   

    @Override
    public Result get(Get get) throws IOException {
        if (tx == null) { throw new IOException("Transaction not started"); }
        return hTable.get(transactionalizeAction(get));
    }

    @Override
    public Result[] get(List<Get> gets) throws IOException {
        if (tx == null) { throw new IOException("Transaction not started"); }
        ArrayList<Get> transactionalizedGets = new ArrayList<>();
        for (Get get : gets) {
            transactionalizedGets.add(transactionalizeAction(get));
        }
        return hTable.get(transactionalizedGets);
    }

    @Override
    public ResultScanner getScanner(Scan scan) throws IOException {
        if (tx == null) { throw new IOException("Transaction not started"); }
        return hTable.getScanner(transactionalizeAction(scan));
    }

    @Override
    public ResultScanner getScanner(byte[] family) throws IOException {
        if (tx == null) { throw new IOException("Transaction not started"); }
        Scan scan = new Scan();
        scan.addFamily(family);
        return hTable.getScanner(transactionalizeAction(scan));
    }

    @Override
    public ResultScanner getScanner(byte[] family, byte[] qualifier) throws IOException {
        if (tx == null) { throw new IOException("Transaction not started"); }
        Scan scan = new Scan();
        scan.addColumn(family, qualifier);
        return hTable.getScanner(transactionalizeAction(scan));
    }

    @Override
    public void put(Put put) throws IOException {
        if (tx == null) { throw new IOException("Transaction not started"); }
        Put txPut = transactionalizeAction(put);
        hTable.put(txPut);
    }

    @Override
    public void put(List<Put> puts) throws IOException {
        if (tx == null) { throw new IOException("Transaction not started"); }
        List<Put> transactionalizedPuts = new ArrayList<>(puts.size());
        for (Put put : puts) {
            Put txPut = transactionalizeAction(put);
            transactionalizedPuts.add(txPut);
        }
        hTable.put(transactionalizedPuts);
    }

    @Override
    public boolean checkAndPut(byte[] row, byte[] family, byte[] qualifier, byte[] value, Put put) throws IOException {
        if (allowNonTransactional) {
            return hTable.checkAndPut(row, family, qualifier, value, put);
        } else {
            throw new UnsupportedOperationException("Operation is not supported transactionally");
        }
    }

    @Override
    public void delete(Delete delete) throws IOException {
        if (tx == null) { throw new IOException("Transaction not started"); }
        hTable.delete(transactionalizeAction(delete));
    }

    @Override
    public void delete(List<Delete> deletes) throws IOException {
        if (tx == null) { throw new IOException("Transaction not started"); }
        List<Delete> transactionalizedDeletes = new ArrayList<>(deletes.size());
        for (Delete delete : deletes) {
            Delete txDelete = transactionalizeAction(delete);
            transactionalizedDeletes.add(txDelete);
        }
        hTable.delete(transactionalizedDeletes);
    }

    @Override
    public boolean checkAndDelete(byte[] row, byte[] family, byte[] qualifier, byte[] value, Delete delete)
            throws IOException {
        if (allowNonTransactional) {
            return hTable.checkAndDelete(row, family, qualifier, value, delete);
        } else {
            throw new UnsupportedOperationException("Operation is not supported transactionally");
        }
    }

    @Override
    public boolean checkAndDelete(byte[] bytes, byte[] bytes1, byte[] bytes2, CompareFilter.CompareOp compareOp,
            byte[] bytes3, Delete delete) throws IOException {
        if (allowNonTransactional) {
            return hTable.checkAndDelete(bytes, bytes1, bytes2, compareOp, bytes3, delete);
        } else {
            throw new UnsupportedOperationException("Operation is not supported transactionally");
        }
    }

    @Override
    public boolean checkAndPut(byte[] bytes, byte[] bytes1, byte[] bytes2, CompareFilter.CompareOp compareOp,
            byte[] bytes3, Put put) throws IOException {
        if (allowNonTransactional) {
            return hTable.checkAndPut(bytes, bytes1, bytes2, compareOp, bytes3, put);
        } else {
            throw new UnsupportedOperationException("Operation is not supported transactionally");
        }
    }

    @Override
    public boolean[] existsAll(List<Get> gets) throws IOException {
        if (tx == null) { throw new IOException("Transaction not started"); }
        List<Get> transactionalizedGets = new ArrayList<>(gets.size());
        for (Get get : gets) {
            transactionalizedGets.add(transactionalizeAction(get));
        }
        return hTable.existsAll(transactionalizedGets);
    }

    @Override
    public boolean checkAndMutate(byte[] row, byte[] family, byte[] qualifier, CompareFilter.CompareOp compareOp,
            byte[] value, RowMutations rowMutations) throws IOException {
        if (allowNonTransactional) { return hTable.checkAndMutate(row, family, qualifier, compareOp, value,
                rowMutations); }

        throw new UnsupportedOperationException("checkAndMutate operation is not supported transactionally");
    }

    @Override
    public void mutateRow(RowMutations rm) throws IOException {
        if (tx == null) { throw new IOException("Transaction not started"); }
        RowMutations transactionalMutations = new RowMutations(rm.getRow());
        for (Mutation mutation : rm.getMutations()) {
            if (mutation instanceof Put) {
                transactionalMutations.add(transactionalizeAction((Put)mutation));
            } else if (mutation instanceof Delete) {
                transactionalMutations.add(transactionalizeAction((Delete)mutation));
            }
        }
        hTable.mutateRow(transactionalMutations);
    }

    @Override
    public Result append(Append append) throws IOException {
        if (allowNonTransactional) {
            return hTable.append(append);
        } else {
            throw new UnsupportedOperationException("Operation is not supported transactionally");
        }
    }

    @Override
    public Result increment(Increment increment) throws IOException {
        if (allowNonTransactional) {
            return hTable.increment(increment);
        } else {
            throw new UnsupportedOperationException("Operation is not supported transactionally");
        }
    }

    @Override
    public long incrementColumnValue(byte[] row, byte[] family, byte[] qualifier, long amount) throws IOException {
        if (allowNonTransactional) {
            return hTable.incrementColumnValue(row, family, qualifier, amount);
        } else {
            throw new UnsupportedOperationException("Operation is not supported transactionally");
        }
    }

    @Override
    public long incrementColumnValue(byte[] row, byte[] family, byte[] qualifier, long amount, Durability durability)
            throws IOException {
        if (allowNonTransactional) {
            return hTable.incrementColumnValue(row, family, qualifier, amount, durability);
        } else {
            throw new UnsupportedOperationException("Operation is not supported transactionally");
        }
    }

    @Override
    public void close() throws IOException {
        hTable.close();
    }

    @Override
    public CoprocessorRpcChannel coprocessorService(byte[] row) {
        return hTable.coprocessorService(row);
    }

    @Override
    public <T extends Service, R> Map<byte[], R> coprocessorService(Class<T> service, byte[] startKey, byte[] endKey,
            Batch.Call<T, R> callable) throws ServiceException, Throwable {
        return hTable.coprocessorService(service, startKey, endKey, callable);
    }

    @Override
    public <T extends Service, R> void coprocessorService(Class<T> service, byte[] startKey, byte[] endKey,
            Batch.Call<T, R> callable, Batch.Callback<R> callback) throws ServiceException, Throwable {
        hTable.coprocessorService(service, startKey, endKey, callable, callback);
    }

    @Override
    public <R extends Message> Map<byte[], R> batchCoprocessorService(MethodDescriptor methodDescriptor,
            Message request, byte[] startKey, byte[] endKey, R responsePrototype) throws ServiceException, Throwable {
        return hTable.batchCoprocessorService(methodDescriptor, request, startKey, endKey, responsePrototype);
    }

    @Override
    public <R extends Message> void batchCoprocessorService(MethodDescriptor methodDescriptor, Message request,
            byte[] startKey, byte[] endKey, R responsePrototype, Callback<R> callback)
            throws ServiceException, Throwable {
        hTable.batchCoprocessorService(methodDescriptor, request, startKey, endKey, responsePrototype, callback);
    }

    // Helpers to get copies of objects with the timestamp set to the current transaction timestamp.

    private Get transactionalizeAction(Get get) throws IOException {
        addToOperation(get, tx);
        return get;
    }

    private Scan transactionalizeAction(Scan scan) throws IOException {
        addToOperation(scan, tx);
        return scan;
    }

    private Put transactionalizeAction(Put put) throws IOException {
        Put txPut = new Put(put.getRow(), tx.getWritePointer());
        Set<Map.Entry<byte[], List<Cell>>> familyMap = put.getFamilyCellMap().entrySet();
        if (!familyMap.isEmpty()) {
            for (Map.Entry<byte[], List<Cell>> family : familyMap) {
                List<Cell> familyValues = family.getValue();
                if (!familyValues.isEmpty()) {
                    for (Cell value : familyValues) {
                        txPut.addColumn(CellUtil.cloneFamily(value), CellUtil.cloneQualifier(value), tx.getWritePointer(), CellUtil.cloneValue(value));
                        addToChangeSet(txPut.getRow(), CellUtil.cloneFamily(value), CellUtil.cloneQualifier(value));
                    }
                }
            }
        }
        for (Map.Entry<String, byte[]> entry : put.getAttributesMap().entrySet()) {
            txPut.setAttribute(entry.getKey(), entry.getValue());
        }
        txPut.setDurability(put.getDurability());
        addToOperation(txPut, tx);
        return txPut;
    }

    private Delete transactionalizeAction(Delete delete) throws IOException {
        long transactionTimestamp = tx.getWritePointer();

        byte[] deleteRow = delete.getRow();
        Delete txDelete = new Delete(deleteRow, transactionTimestamp);

        Map<byte[], List<Cell>> familyToDelete = delete.getFamilyCellMap();
        if (familyToDelete.isEmpty()) {
            // perform a row delete if we are using row-level conflict detection
            if (conflictLevel == TxConstants.ConflictDetection.ROW
                    || conflictLevel == TxConstants.ConflictDetection.NONE) {
                // Row delete leaves delete markers in all column families of the table
                // Therefore get all the column families of the hTable from the HTableDescriptor and add them to the
                // changeSet
                for (HColumnDescriptor columnDescriptor : hTable.getTableDescriptor().getColumnFamilies()) {
                    // no need to identify individual columns deleted
                    addToChangeSet(deleteRow, columnDescriptor.getName(), null);
                }
            } else {
                Result result = get(new Get(delete.getRow()));
                // Delete everything
                NavigableMap<byte[], NavigableMap<byte[], byte[]>> resultMap = result.getNoVersionMap();
                for (Map.Entry<byte[], NavigableMap<byte[], byte[]>> familyEntry : resultMap.entrySet()) {
                    NavigableMap<byte[], byte[]> familyColumns = result.getFamilyMap(familyEntry.getKey());
                    for (Map.Entry<byte[], byte[]> column : familyColumns.entrySet()) {
                        txDelete.addColumns(familyEntry.getKey(), column.getKey(), transactionTimestamp);
                        addToChangeSet(deleteRow, familyEntry.getKey(), column.getKey());
                    }
                }
            }
        } else {
            for (Map.Entry<byte[], List<Cell>> familyEntry : familyToDelete.entrySet()) {
                byte[] family = familyEntry.getKey();
                List<Cell> entries = familyEntry.getValue();
                boolean isFamilyDelete = false;
                if (entries.size() == 1) {
                    Cell cell = entries.get(0);
                    isFamilyDelete = CellUtil.isDeleteFamily(cell);
                }
                if (isFamilyDelete) {
                    if (conflictLevel == TxConstants.ConflictDetection.ROW
                            || conflictLevel == TxConstants.ConflictDetection.NONE) {
                        // no need to identify individual columns deleted
                        txDelete.addFamily(family);
                        addToChangeSet(deleteRow, family, null);
                    } else {
                        Result result = get(new Get(delete.getRow()).addFamily(family));
                        // Delete entire family
                        NavigableMap<byte[], byte[]> familyColumns = result.getFamilyMap(family);
                        for (Map.Entry<byte[], byte[]> column : familyColumns.entrySet()) {
                            txDelete.addColumns(family, column.getKey(), transactionTimestamp);
                            addToChangeSet(deleteRow, family, column.getKey());
                        }
                    }
                } else {
                    for (Cell value : entries) {
                        txDelete.addColumn(CellUtil.cloneFamily(value), CellUtil.cloneQualifier(value), transactionTimestamp);
                        addToChangeSet(deleteRow, CellUtil.cloneFamily(value), CellUtil.cloneQualifier(value));
                    }
                }
            }
        }
        for (Map.Entry<String, byte[]> entry : delete.getAttributesMap().entrySet()) {
            txDelete.setAttribute(entry.getKey(), entry.getValue());
        }
        txDelete.setDurability(delete.getDurability());
        addToOperation(txDelete, tx);
        return txDelete;
    }

    private List<? extends Row> transactionalizeActions(List<? extends Row> actions) throws IOException {
        List<Row> transactionalizedActions = new ArrayList<>(actions.size());
        for (Row action : actions) {
            if (action instanceof Get) {
                transactionalizedActions.add(transactionalizeAction((Get)action));
            } else if (action instanceof Put) {
                transactionalizedActions.add(transactionalizeAction((Put)action));
            } else if (action instanceof Delete) {
                transactionalizedActions.add(transactionalizeAction((Delete)action));
            } else {
                transactionalizedActions.add(action);
            }
        }
        return transactionalizedActions;
    }

    public void addToOperation(OperationWithAttributes op, Transaction tx) throws IOException {
        op.setAttribute(TxConstants.TX_OPERATION_ATTRIBUTE_KEY, txCodec.encode(tx));
    }

    protected void makeRollbackOperation(Delete delete) {
        delete.setAttribute(TxConstants.TX_ROLLBACK_ATTRIBUTE_KEY, new byte[0]);
    }

    @Override
    public TableDescriptor getDescriptor() throws IOException {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public boolean[] exists(List<Get> gets) throws IOException {
        if (tx == null) { throw new IOException("Transaction not started"); }
        List<Get> transactionalizedGets = new ArrayList<>(gets.size());
        for (Get get : gets) {
            transactionalizedGets.add(transactionalizeAction(get));
        }
        return hTable.exists(transactionalizedGets);
    }

    @Override
    public boolean checkAndPut(byte[] row, byte[] family, byte[] qualifier, CompareOperator op, byte[] value, Put put)
            throws IOException {
        if(allowNonTransactional){
            return hTable.checkAndPut(row, family, qualifier, value, put);
        }else{
            throw new UnsupportedOperationException("Operation is not supported transactionally");
        }
        
    }

    @Override
    public boolean checkAndDelete(byte[] row, byte[] family, byte[] qualifier, CompareOperator op, byte[] value,
            Delete delete) throws IOException {
        if(allowNonTransactional){
            return hTable.checkAndDelete(row, family, qualifier, op, value, delete);
        }else{
            throw new UnsupportedOperationException("Operation is not supported transactionally");
        }
    }

    @Override
    public boolean checkAndMutate(byte[] row, byte[] family, byte[] qualifier, CompareOperator op, byte[] value,
            RowMutations mutation) throws IOException {
        if(allowNonTransactional){
            return hTable.checkAndMutate(row, family, qualifier, op, value, mutation);
        }else{
            throw new UnsupportedOperationException("Operation is not supported transactionally");
        }
    }

    @Override
    public long getRpcTimeout(TimeUnit unit) {
        return hTable.getRpcTimeout(unit);
    }

    @Override
    public int getRpcTimeout() {
       return hTable.getRpcTimeout();
    }

    @Override
    public void setRpcTimeout(int rpcTimeout) {
         hTable.setRpcTimeout(rpcTimeout);
        
    }

    @Override
    public long getReadRpcTimeout(TimeUnit unit) {
        return hTable.getReadRpcTimeout(unit);
    }

    @Override
    public int getReadRpcTimeout() {
        return hTable.getReadRpcTimeout();
    }

    @Override
    public void setReadRpcTimeout(int readRpcTimeout) {
        hTable.setReadRpcTimeout(readRpcTimeout);
        
    }

    @Override
    public long getWriteRpcTimeout(TimeUnit unit) {
        return hTable.getWriteRpcTimeout(unit);
    }

    @Override
    public int getWriteRpcTimeout() {
        return hTable.getWriteRpcTimeout();
    }

    @Override
    public void setWriteRpcTimeout(int writeRpcTimeout) {
        hTable.setWriteRpcTimeout(writeRpcTimeout);
        
    }

    @Override
    public long getOperationTimeout(TimeUnit unit) {
        return hTable.getOperationTimeout(unit);
    }

    @Override
    public int getOperationTimeout() {
        return hTable.getOperationTimeout();
    }

    @Override
    public void setOperationTimeout(int operationTimeout) {
        hTable.setOperationTimeout(operationTimeout);;
    }
}
    
