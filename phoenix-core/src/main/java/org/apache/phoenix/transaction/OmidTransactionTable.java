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

public class OmidTransactionTable implements PhoenixTransactionalTable {

    public OmidTransactionTable(PhoenixTransactionContext ctx) {
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
    public HTableInterface getHTable() {
        // TODO Auto-generated method stub
        return null;
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

}
