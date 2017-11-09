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

import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HTableDescriptor;

import java.io.IOException;
import java.util.List;

public interface PhoenixTransactionalTable extends Table {

    /**
     * Transaction version of {@link Table#get(Get get)}
     * @param get
     * @throws IOException
     */
    public Result get(Get get) throws IOException;

    /**
     * Transactional version of {@link Table#put(Put put)}
     * @param put
     * @throws IOException
     */
    public void put(Put put) throws IOException;

    /**
     * Transactional version of {@link Table#delete(Delete delete)}
     *
     * @param delete
     * @throws IOException
     */
    public void delete(Delete delete) throws IOException;

    /**
     * Transactional version of {@link Table#getScanner(Scan scan)}
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
     * Transactional version of {@link Table#get(List gets)}
     * @throws IOException
     */
    public Result[] get(List<Get> gets) throws IOException;

    /**
     * Transactional version of {@link Table#getScanner(byte[] family)}
     * @throws IOException
     */
    public ResultScanner getScanner(byte[] family) throws IOException;

    /**
     * Transactional version of {@link Table#getScanner(byte[] family, byte[] qualifier)}
     * @throws IOException
     */
    public ResultScanner getScanner(byte[] family, byte[] qualifier) throws IOException;

    /**
     * Transactional version of {@link Table#put(List puts)}
     * @throws IOException
     */
    public void put(List<Put> puts) throws IOException;

    /**
     * Transactional version of {@link Table#delete(List deletes)}
     * @throws IOException
     */
    public void delete(List<Delete> deletes) throws IOException;

    /**
     * Delegates to {@link Table#setAutoFlush(boolean autoFlush)}
     */
    public void setAutoFlush(boolean autoFlush);

    /**
     * Delegates to {@link Table#isAutoFlush()}
     */
    public boolean isAutoFlush();

    /**
     * Delegates to see Table.getWriteBufferSize()
     */
    public long getWriteBufferSize();

    /**
     * Delegates to see Table.setWriteBufferSize()
     */
    public void setWriteBufferSize(long writeBufferSize) throws IOException;

    /**
     * Delegates to see Table.flushCommits()
     */
    public void flushCommits() throws IOException;

    /**
     * Releases resources
     * @throws IOException
     */
    public void close() throws IOException;
}
