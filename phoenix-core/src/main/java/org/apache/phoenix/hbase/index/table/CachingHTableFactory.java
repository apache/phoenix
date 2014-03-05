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

import java.io.IOException;
import java.util.Map;

import org.apache.commons.collections.map.LRUMap;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.client.HTableInterface;
import org.apache.hadoop.hbase.util.Bytes;

import org.apache.phoenix.hbase.index.util.ImmutableBytesPtr;

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
      super(cacheSize);
    }

    @Override
    protected boolean removeLRU(LinkEntry entry) {
      HTableInterface table = (HTableInterface) entry.getValue();
      if (LOG.isDebugEnabled()) {
        LOG.debug("Closing connection to table: " + Bytes.toString(table.getTableName())
            + " because it was evicted from the cache.");
      }
      try {
        table.close();
      } catch (IOException e) {
        LOG.info("Failed to correctly close HTable: " + Bytes.toString(table.getTableName())
            + " ignoring since being removed from queue.");
      }
      return true;
    }
  }

  public static int getCacheSize(Configuration conf) {
    return conf.getInt(CACHE_SIZE_KEY, DEFAULT_CACHE_SIZE);
  }

  private static final Log LOG = LogFactory.getLog(CachingHTableFactory.class);
  private static final String CACHE_SIZE_KEY = "index.tablefactory.cache.size";
  private static final int DEFAULT_CACHE_SIZE = 10;

  private HTableFactory delegate;

  @SuppressWarnings("rawtypes")
  Map openTables;

  public CachingHTableFactory(HTableFactory tableFactory, Configuration conf) {
    this(tableFactory, getCacheSize(conf));
  }

  public CachingHTableFactory(HTableFactory factory, int cacheSize) {
    this.delegate = factory;
    openTables = new HTableInterfaceLRUMap(cacheSize);
  }

  @Override
  @SuppressWarnings("unchecked")
  public HTableInterface getTable(ImmutableBytesPtr tablename) throws IOException {
    ImmutableBytesPtr tableBytes = new ImmutableBytesPtr(tablename);
    synchronized (openTables) {
      HTableInterface table = (HTableInterface) openTables.get(tableBytes);
      if (table == null) {
        table = delegate.getTable(tablename);
        openTables.put(tableBytes, table);
      }
      return table;
    }
  }

  @Override
  public void shutdown() {
    this.delegate.shutdown();
  }
}