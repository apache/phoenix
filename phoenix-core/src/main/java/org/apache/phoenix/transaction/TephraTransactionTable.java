package org.apache.phoenix.transaction;

import java.io.IOException;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HTableInterface;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.tephra.hbase.TransactionAwareHTable;

public class TephraTransactionTable implements PhoenixTransactionalTable {

    private TransactionAwareHTable transactionAwareHTable;
    
    private TephraTransactionContext tephraTransactionContext;
    
    public TephraTransactionTable(PhoenixTransactionContext ctx, HTableInterface hTable) {

        assert(ctx instanceof TephraTransactionContext);

        tephraTransactionContext = (TephraTransactionContext) ctx;

        transactionAwareHTable = new TransactionAwareHTable(hTable);

        tephraTransactionContext.addTransactionAware(transactionAwareHTable);
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

}
