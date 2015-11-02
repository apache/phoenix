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
package org.apache.phoenix.query;

import java.io.IOException;
import java.util.concurrent.ExecutorService;

import org.apache.hadoop.hbase.client.HConnection;
import org.apache.hadoop.hbase.client.HTableInterface;

/**
 * Creates clients to access HBase tables.
 *
 * 
 * @since 0.2
 */
public interface HTableFactory {
    /**
     * Creates an HBase client using an externally managed HConnection and Thread pool.
     *
     * @param tableName Name of the table.
     * @param connection HConnection to use.
     * @param pool ExecutorService to use.
     * @return An client to access an HBase table.
     * @throws IOException if a server or network exception occurs
     */
    HTableInterface getTable(byte[] tableName, HConnection connection, ExecutorService pool) throws IOException;

    /**
     * Default implementation.  Uses standard HBase HTables.
     */
    static class HTableFactoryImpl implements HTableFactory {
        @Override
        public HTableInterface getTable(byte[] tableName, HConnection connection, ExecutorService pool) throws IOException {
            // Let the HBase client manage the thread pool instead of passing ours through
            return connection.getTable(tableName);
        }
    }
}
