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
package org.apache.phoenix.hbase.index.table;

import static org.apache.phoenix.hbase.index.write.IndexWriterUtils.HTABLE_KEEP_ALIVE_KEY;
import static org.apache.phoenix.hbase.index.write.IndexWriterUtils.INDEX_WRITES_THREAD_MAX_PER_REGIONSERVER_KEY;

import java.io.IOException;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.SynchronousQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.commons.collections.map.LRUMap;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.client.HTableInterface;
import org.apache.hadoop.hbase.coprocessor.RegionCoprocessorEnvironment;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.Threads;
import org.apache.phoenix.execute.DelegateHTable;
import org.apache.phoenix.hbase.index.util.ImmutableBytesPtr;

import com.google.common.annotations.VisibleForTesting;;

/**
 * A simple cache that just uses usual GC mechanisms to cleanup unused {@link HTableInterface}s.
 * When requesting an {@link HTableInterface} via {@link #getTable}, you may get the same table as
 * last time, or it may be a new table.
 * <p>
 * You <b>should not call {@link HTableInterface#close()} </b> that is handled when the table goes
 * out of scope. Along the same lines, you must ensure to not keep a reference to the table for
 * longer than necessary - this leak will ensure that the table never gets closed.
 */
public class CachingHTableFactory implements HTableFactory {

  /**
   * LRUMap that closes the {@link HTableInterface} when the table is evicted
   */
  @SuppressWarnings("serial")
  public class HTableInterfaceLRUMap extends LRUMap {

    public HTableInterfaceLRUMap(int cacheSize) {
      super(cacheSize, true);
    }

    @Override
    protected boolean removeLRU(LinkEntry entry) {
      HTableInterface table = (HTableInterface) entry.getValue();
      if (LOG.isDebugEnabled()) {
        LOG.debug("Closing connection to table: " + Bytes.toString(table.getTableName())
            + " because it was evicted from the cache.");
      }
      try {
         synchronized (this) { // the whole operation of closing and checking the count should be atomic
                                    // and should not conflict with getTable()
          if (((CachedHTableWrapper)table).getReferenceCount() <= 0) {
            table.close();
            return true;
          }
        }
      } catch (IOException e) {
        LOG.info("Failed to correctly close HTable: " + Bytes.toString(table.getTableName())
            + " ignoring since being removed from queue.");
      }
      return false;
    }
  }

  public static int getCacheSize(Configuration conf) {
    return conf.getInt(CACHE_SIZE_KEY, DEFAULT_CACHE_SIZE);
  }

  private static final Log LOG = LogFactory.getLog(CachingHTableFactory.class);
  private static final String CACHE_SIZE_KEY = "index.tablefactory.cache.size";
  private static final int DEFAULT_CACHE_SIZE = 1000;

  private HTableFactory delegate;

  @SuppressWarnings("rawtypes")
  Map openTables;
  private ThreadPoolExecutor pool;

  public CachingHTableFactory(HTableFactory tableFactory, Configuration conf, RegionCoprocessorEnvironment env) {
    this(tableFactory, getCacheSize(conf), env);
  }

  public CachingHTableFactory(HTableFactory factory, int cacheSize, RegionCoprocessorEnvironment env) {
    this.delegate = factory;
    openTables = new HTableInterfaceLRUMap(cacheSize);
        this.pool = new ThreadPoolExecutor(1,
                env.getConfiguration().getInt(INDEX_WRITES_THREAD_MAX_PER_REGIONSERVER_KEY, Integer.MAX_VALUE),
                env.getConfiguration().getInt(HTABLE_KEEP_ALIVE_KEY, 60), TimeUnit.SECONDS,
                new SynchronousQueue<Runnable>(), Threads.newDaemonThreadFactory("CachedHtables"));
        pool.allowCoreThreadTimeOut(true);
  }
  
  @Override
  @SuppressWarnings("unchecked")
  public HTableInterface getTable(ImmutableBytesPtr tablename, ExecutorService pool) throws IOException {
    ImmutableBytesPtr tableBytes = new ImmutableBytesPtr(tablename);
    synchronized (openTables) {
      CachedHTableWrapper table = (CachedHTableWrapper) openTables.get(tableBytes);
      if (table == null) {
        table = new CachedHTableWrapper(delegate.getTable(tablename, pool));
        openTables.put(tableBytes, table);
      }
      table.incrementReferenceCount();
      return table;
    }
  }

  @Override
    public void shutdown() {
        this.delegate.shutdown();
        this.pool.shutdown();
        try {
            boolean terminated = false;
            do {
                // wait until the pool has terminated
                terminated = this.pool.awaitTermination(60, TimeUnit.SECONDS);
            } while (!terminated);
        } catch (InterruptedException e) {
            this.pool.shutdownNow();
            LOG.warn("waitForTermination interrupted");
        }
    }

    public static class CachedHTableWrapper extends DelegateHTable {

        private AtomicInteger referenceCount = new AtomicInteger();

        public CachedHTableWrapper(HTableInterface table) {
            super(table);
        }

        @Override
        public synchronized void close() throws IOException {
            if (getReferenceCount() > 0) {
                this.referenceCount.decrementAndGet();
            } else {
                // During LRU eviction
                super.close();
            }
        }

        public void incrementReferenceCount() {
            this.referenceCount.incrementAndGet();
        }

        public int getReferenceCount() {
            return this.referenceCount.get();
        }

    }

    @Override
    public HTableInterface getTable(ImmutableBytesPtr tablename) throws IOException {
        return getTable(tablename, this.pool);
    }
    
    @VisibleForTesting
    public ThreadPoolExecutor getPool(){
        return this.pool;
    }
}