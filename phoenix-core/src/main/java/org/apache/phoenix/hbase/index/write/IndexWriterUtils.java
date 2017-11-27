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
package org.apache.phoenix.hbase.index.write;

import java.io.IOException;
import java.util.concurrent.ExecutorService;

import javax.annotation.concurrent.GuardedBy;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.coprocessor.RegionCoprocessorEnvironment;
import org.apache.phoenix.hbase.index.table.HTableFactory;
import org.apache.phoenix.hbase.index.util.ImmutableBytesPtr;
import org.apache.phoenix.hbase.index.util.IndexManagementUtil;

public class IndexWriterUtils {

  private static final Log LOG = LogFactory.getLog(IndexWriterUtils.class);

  /**
   * Maximum number of threads to allow per-table when writing. Each writer thread (from
   * {@link IndexWriterUtils#NUM_CONCURRENT_INDEX_WRITER_THREADS_CONF_KEY}) has a single HTable.
   * However, each table is backed by a threadpool to manage the updates to that table. this
   * specifies the number of threads to allow in each of those tables. Generally, you shouldn't need
   * to change this, unless you have a small number of indexes to which most of the writes go.
   * Defaults to: {@value #DEFAULT_NUM_PER_TABLE_THREADS}.
   * <p>
   * For tables to which there are not a lot of writes, the thread pool automatically will decrease
   * the number of threads to one (though it can burst up to the specified max for any given table),
   * so increasing this to meet the max case is reasonable.
   * <p>
   * Setting this value too small can cause <b>catastrophic cluster failure</b>. The way HTable's
   * underlying pool works is such that is does direct hand-off of tasks to threads. This works fine
   * because HTables are assumed to work in a single-threaded context, so we never get more threads
   * than regionservers. In a multi-threaded context, we can easily grow to more than that number of
   * threads. Currently, HBase doesn't support a custom thread-pool to back the HTable via the
   * coprocesor hooks, so we can't modify this behavior.
   */
  private static final String INDEX_WRITER_PER_TABLE_THREADS_CONF_KEY =
      "index.writer.threads.pertable.max";
  private static final int DEFAULT_NUM_PER_TABLE_THREADS = Integer.MAX_VALUE;

  /** Configuration key that HBase uses to set the max number of threads for an HTable */
  public static final String HTABLE_THREAD_KEY = "hbase.htable.threads.max";
   public static final String INDEX_WRITES_THREAD_MAX_PER_REGIONSERVER_KEY = "phoenix.index.writes.threads.max";
   public static final String HTABLE_KEEP_ALIVE_KEY = "hbase.htable.threads.keepalivetime";

   public static final String INDEX_WRITER_RPC_RETRIES_NUMBER = "phoenix.index.writes.rpc.retries.number";
   /**
    * Based on the logic in HBase's AsyncProcess, a default of 11 retries with a pause of 100ms
    * approximates 48 sec total retry time (factoring in backoffs).  The total time should be less
    * than HBase's rpc timeout (default of 60 sec) or else the client will retry before receiving
    * the response
    */
   public static final int DEFAULT_INDEX_WRITER_RPC_RETRIES_NUMBER = 11;
   public static final String INDEX_WRITER_RPC_PAUSE = "phoenix.index.writes.rpc.pause";
   public static final int DEFAULT_INDEX_WRITER_RPC_PAUSE = 100;

  private IndexWriterUtils() {
    // private ctor for utilites
  }

    public static HTableFactory getDefaultDelegateHTableFactory(RegionCoprocessorEnvironment env) {
        // create a simple delegate factory, setup the way we need
        Configuration conf = env.getConfiguration();
        // set the number of threads allowed per table.
        int htableThreads =
                conf.getInt(IndexWriterUtils.INDEX_WRITER_PER_TABLE_THREADS_CONF_KEY,
                    IndexWriterUtils.DEFAULT_NUM_PER_TABLE_THREADS);
        LOG.trace("Creating HTableFactory with " + htableThreads + " threads for each HTable.");
        IndexManagementUtil.setIfNotSet(conf, HTABLE_THREAD_KEY, htableThreads);
        return new CoprocessorHConnectionTableFactory(conf, env);
    }

    /**
     * {@code HTableFactory} that creates HTables by using a {@link CoprocessorHConnection} This
     * factory was added as a workaround to the bug reported in
     * https://issues.apache.org/jira/browse/HBASE-18359
     */
    private static class CoprocessorHConnectionTableFactory implements HTableFactory {
        @GuardedBy("CoprocessorHConnectionTableFactory.this")
        private Connection connection;
        private final Configuration conf;
        private RegionCoprocessorEnvironment env;

        CoprocessorHConnectionTableFactory(Configuration conf, RegionCoprocessorEnvironment env) {
            this.conf = conf;
            this.env = env;
        }

        private synchronized Connection getConnection(Configuration conf) throws IOException {
            if (connection == null || connection.isClosed()) {
                connection = env.createConnection(conf);
            }
            return connection;
        }

        @Override
        public Table getTable(ImmutableBytesPtr tablename) throws IOException {
            return getConnection(conf).getTable(TableName.valueOf(tablename.copyBytesIfNecessary()));
        }

        @Override
        public synchronized void shutdown() {
            try {
                if (connection != null && !connection.isClosed()) {
                    connection.close();
                }
            } catch (Throwable e) {
                LOG.warn("Error while trying to close the HConnection used by CoprocessorHConnectionTableFactory", e);
            }
        }

        @Override
        public Table getTable(ImmutableBytesPtr tablename, ExecutorService pool)
                throws IOException {
            return getConnection(conf).getTable(TableName.valueOf(tablename.copyBytesIfNecessary()), pool);
        }
    }
}
