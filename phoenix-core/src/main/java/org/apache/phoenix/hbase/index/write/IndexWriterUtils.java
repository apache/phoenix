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

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.CoprocessorEnvironment;
import org.apache.phoenix.hbase.index.table.CoprocessorHTableFactory;
import org.apache.phoenix.hbase.index.table.HTableFactory;
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
  private IndexWriterUtils() {
    // private ctor for utilites
  }

  public static HTableFactory getDefaultDelegateHTableFactory(CoprocessorEnvironment env) {
    // create a simple delegate factory, setup the way we need
    Configuration conf = env.getConfiguration();
    // set the number of threads allowed per table.
    int htableThreads =
        conf.getInt(IndexWriterUtils.INDEX_WRITER_PER_TABLE_THREADS_CONF_KEY, IndexWriterUtils.DEFAULT_NUM_PER_TABLE_THREADS);
    LOG.trace("Creating HTableFactory with " + htableThreads + " threads for each HTable.");
    IndexManagementUtil.setIfNotSet(conf, HTABLE_THREAD_KEY, htableThreads);
    return new CoprocessorHTableFactory(env);
  }
}
