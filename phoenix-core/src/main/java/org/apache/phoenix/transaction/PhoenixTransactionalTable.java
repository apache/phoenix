package org.apache.phoenix.transaction;

import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.HTableInterface;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HTableDescriptor;

import java.io.IOException;
import java.util.List;

public interface PhoenixTransactionalTable extends HTableInterface {

    /**
     * Transaction version of {@link HTableInterface#get(Get get)}
     * @param get
     * @return
     * @throws IOException
     */
    public Result get(Get get) throws IOException;

    /**
     * Transactional version of {@link HTableInterface#put(Put put)}
     * @param put
     * @throws IOException
     */
    public void put(Put put) throws IOException;

    /**
     * Transactional version of {@link HTableInterface#delete(Delete delete)}
     *
     * @param delete
     * @throws IOException
     */
    public void delete(Delete delete) throws IOException;

    /**
     * Transactional version of {@link HTableInterface#getScanner(Scan scan)}
     *
     * @param scan
     * @return ResultScanner
     * @throws IOException
     */
    public ResultScanner getScanner(Scan scan) throws IOException;

    /**
     * Returns Htable name
     */
    public byte[] getTableName();

    /**
     * Returns Htable configuration object
     */
    public Configuration getConfiguration();

    /**
     * Returns HTableDescriptor of Htable
     * @throws IOException
     */
    public HTableDescriptor getTableDescriptor() throws IOException;

    /**
     * Checks if cell exists
     * @throws IOException
     */
    public boolean exists(Get get) throws IOException;

    /**
     * Transactional version of {@link HTableInterface#get(List gets)}
     * @throws IOException
     */
    public Result[] get(List<Get> gets) throws IOException;

    /**
     * Transactional version of {@link HTableInterface#getScanner(byte[] family)}
     * @throws IOException
     */
    public ResultScanner getScanner(byte[] family) throws IOException;

    /**
     * Transactional version of {@link HTableInterface#getScanner(byte[] family, byte[] qualifier)}
     * @throws IOException
     */
    public ResultScanner getScanner(byte[] family, byte[] qualifier) throws IOException;

    /**
     * Transactional version of {@link HTableInterface#put(List puts)}
     * @throws IOException
     */
    public void put(List<Put> puts) throws IOException;

    /**
     * Transactional version of {@link HTableInterface#delete(List deletes)}
     * @throws IOException
     */
    public void delete(List<Delete> deletes) throws IOException;

    /**
     * Delegates to {@link HTable#setAutoFlush(boolean autoFlush)}
     */
    public void setAutoFlush(boolean autoFlush);

    /**
     * Delegates to {@link HTable#isAutoFlush()}
     */
    public boolean isAutoFlush();

    /**
     * Delegates to see HTable.getWriteBufferSize()
     */
    public long getWriteBufferSize();

    /**
     * Delegates to see HTable.setWriteBufferSize()
     */
    public void setWriteBufferSize(long writeBufferSize) throws IOException;

    /**
     * Delegates to see HTable.flushCommits()
     */
    public void flushCommits() throws IOException;

    /**
     * Releases resources
     * @throws IOException
     */
    public void close() throws IOException;
}
