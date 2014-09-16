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

package org.apache.phoenix.cache.aggcache;

import static org.apache.phoenix.query.QueryConstants.AGG_TIMESTAMP;
import static org.apache.phoenix.query.QueryConstants.SINGLE_COLUMN;
import static org.apache.phoenix.query.QueryConstants.SINGLE_COLUMN_FAMILY;
import static org.apache.phoenix.query.QueryServices.GROUPBY_MAX_CACHE_SIZE_ATTRIB;
import static org.apache.phoenix.query.QueryServices.GROUPBY_SPILL_FILES_ATTRIB;
import static org.apache.phoenix.query.QueryServicesOptions.DEFAULT_GROUPBY_MAX_CACHE_MAX;
import static org.apache.phoenix.query.QueryServicesOptions.DEFAULT_GROUPBY_SPILL_FILES;

import java.io.IOException;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.coprocessor.RegionCoprocessorEnvironment;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.regionserver.RegionScanner;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.phoenix.cache.GlobalCache;
import org.apache.phoenix.cache.TenantCache;
import org.apache.phoenix.cache.aggcache.SpillManager.CacheEntry;
import org.apache.phoenix.coprocessor.BaseRegionScanner;
import org.apache.phoenix.coprocessor.GroupByCache;
import org.apache.phoenix.coprocessor.GroupedAggregateRegionObserver;
import org.apache.phoenix.expression.aggregator.Aggregator;
import org.apache.phoenix.expression.aggregator.ServerAggregators;
import org.apache.phoenix.hbase.index.util.ImmutableBytesPtr;
import org.apache.phoenix.memory.InsufficientMemoryException;
import org.apache.phoenix.memory.MemoryManager.MemoryChunk;
import org.apache.phoenix.util.Closeables;
import org.apache.phoenix.util.KeyValueUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * The main entry point is in GroupedAggregateRegionObserver. It instantiates a SpillableGroupByCache and invokes a
 * get() method on it. There is no: "if key not exists -> put into map" case, since the cache is a Loading cache and
 * therefore handles the put under the covers. I tried to implement the final cache element accesses (RegionScanner
 * below) streaming, i.e. there is just an iterator on it and removed the existing result materialization.
 * SpillableGroupByCache implements a LRU cache using a LinkedHashMap with access order. There is a configurable an
 * upper and lower size limit in bytes which are used as follows to compute the initial cache size in number of
 * elements: Max(lowerBoundElements, Min(upperBoundElements, estimatedCacheSize)). Once the number of cached elements
 * exceeds this number, the cache size is increased by a factor of 1.5. This happens until the additional memory to grow
 * the cache cannot be requested. At this point the Cache starts spilling elements. As long as no eviction happens no
 * spillable data structures are allocated, this only happens as soon as the first element is evicted from the cache. We
 * cannot really make any assumptions on which keys arrive at the map, but assume the LRU would at least cover the cases
 * where some keys have a slight skew and they should stay memory resident. Once a key gets evicted, the spillManager is
 * instantiated. It basically takes care of spilling an element to disk and does all the SERDE work. It pre-allocates a
 * configurable number of SpillFiles (spill partition) which are memory mapped temp files. The SpillManager keeps a list
 * of these and hash distributes the keys within this list. Once an element gets spilled, it is serialized and will only
 * get deserialized again, when it is requested from the client, i.e. loaded back into the LRU cache. The SpillManager
 * holds a single SpillMap object in memory for every spill partition (SpillFile). The SpillMap is an in memory Map
 * representation of a single page of spilled serialized key/value pairs. To achieve fast key lookup the key is hash
 * partitioned into random pages of the current spill file. The code implements an extendible hashing approach which
 * dynamically adjusts the hash function, in order to adapt to growing number of storage pages and avoiding long chains
 * of overflow buckets. For an excellent discussion of the algorithm please refer to the following online resource:
 * http://db.inf.uni-tuebingen.de/files/teaching/ws1011/db2/db2-hash-indexes.pdf . For this, each SpillFile keeps a
 * directory of pointers to Integer.MAX_VALUE 4K pages in memory, which allows each directory to address more pages than
 * a single memory mapped temp file could theoretically store. In case directory doubling, requests a page index that
 * exceeds the limits of the initial temp file limits, the implementation dynamically allocates additional temp files to
 * the SpillFile. The directory starts with a global depth of 1 and therefore a directory size of 2 buckets. Only during
 * bucket split and directory doubling more than one page is temporarily kept in memory until all elements have been
 * redistributed. The current implementation conducts bucket splits as long as an element does not fit onto a page. No
 * overflow chain is created, which might be an alternative. For get requests, each directory entry maintains a
 * bloomFilter to prevent page-in operations in case an element has never been spilled before. The deserialization is
 * only triggered when a key a loaded back into the LRU cache. The aggregators are returned from the LRU cache and the
 * next value is computed. In case the key is not found on any page, the Loader create new aggregators for it.
 */

public class SpillableGroupByCache implements GroupByCache {

    private static final Logger logger = LoggerFactory.getLogger(SpillableGroupByCache.class);

    // Min size of 1st level main memory cache in bytes --> lower bound
    private static final int SPGBY_CACHE_MIN_SIZE = 4096; // 4K

    // TODO Generally better to use Collection API with generics instead of
    // array types
    private final LinkedHashMap<ImmutableBytesWritable, Aggregator[]> cache;
    private SpillManager spillManager = null;
    private long totalNumElements;
    private final ServerAggregators aggregators;
    private final RegionCoprocessorEnvironment env;
    private final MemoryChunk chunk;

    /*
     * inner class that makes cache queryable for other classes that should not get the full instance. Queryable view of
     * the cache
     */
    public class QueryCache {
        public boolean isKeyContained(ImmutableBytesPtr key) {
            return cache.containsKey(key);
        }
    }

    /**
     * Instantiates a Loading LRU Cache that stores key / aggregator[] tuples used for group by queries
     * 
     * @param estSize
     * @param estValueSize
     * @param aggs
     * @param ctxt
     */
    public SpillableGroupByCache(final RegionCoprocessorEnvironment env, ImmutableBytesWritable tenantId,
            ServerAggregators aggs, final int estSizeNum) {
        totalNumElements = 0;
        this.aggregators = aggs;
        this.env = env;

        final int estValueSize = aggregators.getEstimatedByteSize();
        final TenantCache tenantCache = GlobalCache.getTenantCache(env, tenantId);

        // Compute Map initial map
        final Configuration conf = env.getConfiguration();
        final long maxCacheSizeConf = conf.getLong(GROUPBY_MAX_CACHE_SIZE_ATTRIB, DEFAULT_GROUPBY_MAX_CACHE_MAX);
        final int numSpillFilesConf = conf.getInt(GROUPBY_SPILL_FILES_ATTRIB, DEFAULT_GROUPBY_SPILL_FILES);

        final int maxSizeNum = (int)(maxCacheSizeConf / estValueSize);
        final int minSizeNum = (SPGBY_CACHE_MIN_SIZE / estValueSize);

        // use upper and lower bounds for the cache size
        final int maxCacheSize = Math.max(minSizeNum, Math.min(maxSizeNum, estSizeNum));
        final long estSize = GroupedAggregateRegionObserver.sizeOfUnorderedGroupByMap(maxCacheSize, estValueSize);
        try {
            this.chunk = tenantCache.getMemoryManager().allocate(estSize);
        } catch (InsufficientMemoryException ime) {
            logger.error("Requested Map size exceeds memory limit, please decrease max size via config paramter: "
                    + GROUPBY_MAX_CACHE_SIZE_ATTRIB);
            throw ime;
        }

        if (logger.isDebugEnabled()) {
            logger.debug("Instantiating LRU groupby cache of element size: " + maxCacheSize);
        }

        // LRU cache implemented as LinkedHashMap with access order
        cache = new LinkedHashMap<ImmutableBytesWritable, Aggregator[]>(maxCacheSize, 0.75f, true) {
            boolean spill = false;
            int cacheSize = maxCacheSize;

            @Override
            protected boolean removeEldestEntry(Map.Entry<ImmutableBytesWritable, Aggregator[]> eldest) {
                if (!spill && size() > cacheSize) { // increase allocation
                    cacheSize *= 1.5f;
                    long estSize = GroupedAggregateRegionObserver.sizeOfUnorderedGroupByMap(cacheSize, estValueSize);
                    try {
                        chunk.resize(estSize);
                    } catch (InsufficientMemoryException im) {
                        // Cannot extend Map anymore, start spilling
                        spill = true;
                    }
                }

                if (spill) {
                    try {
                        if (spillManager == null) {
                            // Lazy instantiation of spillable data
                            // structures
                            //
                            // Only create spill data structs if LRU
                            // cache is too small
                            spillManager = new SpillManager(numSpillFilesConf, aggregators, env.getConfiguration(),
                                    new QueryCache());
                        }
                        spillManager.spill(eldest.getKey(), eldest.getValue());
                    } catch (IOException ioe) {
                        // Ensure that we always close and delete the temp files
                        try {
                            throw new RuntimeException(ioe);
                        } finally {
                            Closeables.closeQuietly(SpillableGroupByCache.this);
                        }
                    }
                    return true;
                }

                return false;
            }
        };
    }

    /**
     * Size function returns the current number of cached elements
     */
    @Override
    public long size() {
        return totalNumElements;
    }

    /**
     * Extract an element from the Cache If element is not present in in-memory cache / or in spill files cache
     * implements an implicit put() of a new key/value tuple and loads it into the cache
     */
    @Override
    public Aggregator[] cache(ImmutableBytesWritable cacheKey) {
        ImmutableBytesPtr key = new ImmutableBytesPtr(cacheKey);
        Aggregator[] rowAggregators = cache.get(key);
        if (rowAggregators == null) {
            // If Aggregators not found for this distinct
            // value, clone our original one (we need one
            // per distinct value)
            if (spillManager != null) {
                // Spill manager present, check if key has been
                // spilled before
                try {
                    rowAggregators = spillManager.loadEntry(key);
                } catch (IOException ioe) {
                    // Ensure that we always close and delete the temp files
                    try {
                        throw new RuntimeException(ioe);
                    } finally {
                        Closeables.closeQuietly(SpillableGroupByCache.this);
                    }
                }
            }
            if (rowAggregators == null) {
                // No, key never spilled before, create a new tuple
                rowAggregators = aggregators.newAggregators(env.getConfiguration());
                if (logger.isDebugEnabled()) {
                    logger.debug("Adding new aggregate bucket for row key "
                            + Bytes.toStringBinary(key.get(), key.getOffset(), key.getLength()));
                }
            }
            if (cache.put(key, rowAggregators) == null) {
                totalNumElements++;
            }
        }
        return rowAggregators;
    }

    /**
     * Iterator over the cache and the spilled data structures by returning CacheEntries. CacheEntries are either
     * extracted from the LRU cache or from the spillable data structures.The key/value tuples are returned in
     * non-deterministic order.
     */
    private final class EntryIterator implements Iterator<Map.Entry<ImmutableBytesWritable, Aggregator[]>> {
        final Iterator<Map.Entry<ImmutableBytesWritable, Aggregator[]>> cacheIter;
        final Iterator<byte[]> spilledCacheIter;

        private EntryIterator() {
            cacheIter = cache.entrySet().iterator();
            if (spillManager != null) {
                spilledCacheIter = spillManager.newDataIterator();
            } else {
                spilledCacheIter = null;
            }
        }

        @Override
        public boolean hasNext() {
            return cacheIter.hasNext();
        }

        @Override
        public Map.Entry<ImmutableBytesWritable, Aggregator[]> next() {
            if (spilledCacheIter != null && spilledCacheIter.hasNext()) {
                try {
                    byte[] value = spilledCacheIter.next();
                    // Deserialize into a CacheEntry
                    Map.Entry<ImmutableBytesWritable, Aggregator[]> spilledEntry = spillManager.toCacheEntry(value);

                    boolean notFound = false;
                    // check against map and return only if not present
                    while (cache.containsKey(spilledEntry.getKey())) {
                        // LRU Cache entries always take precedence,
                        // since they are more up to date
                        if (spilledCacheIter.hasNext()) {
                            value = spilledCacheIter.next();
                            spilledEntry = spillManager.toCacheEntry(value);
                        } else {
                            notFound = true;
                            break;
                        }
                    }
                    if (!notFound) {
                        // Return a spilled entry, this only happens if the
                        // entry was not
                        // found in the LRU cache
                        return spilledEntry;
                    }
                } catch (IOException ioe) {
                    // TODO rework error handling
                    throw new RuntimeException(ioe);
                }
            }
            // Spilled elements exhausted
            // Finally return all elements from LRU cache
            Map.Entry<ImmutableBytesWritable, Aggregator[]> entry = cacheIter.next();
            return new CacheEntry<ImmutableBytesWritable>(entry.getKey(), entry.getValue());
        }

        /**
         * Remove??? Denied!!!
         */
        @Override
        public void remove() {
            throw new IllegalAccessError("Remove is not supported for this type of iterator");
        }
    }

    /**
     * Closes cache and releases spill resources
     * 
     * @throws IOException
     */
    @Override
    public void close() throws IOException {
        // Close spillable resources
        Closeables.closeQuietly(spillManager);
        Closeables.closeQuietly(chunk);
    }

    @Override
    public RegionScanner getScanner(final RegionScanner s) {
        final Iterator<Entry<ImmutableBytesWritable, Aggregator[]>> cacheIter = new EntryIterator();

        // scanner using the spillable implementation
        return new BaseRegionScanner() {
            @Override
            public HRegionInfo getRegionInfo() {
                return s.getRegionInfo();
            }

            @Override
            public void close() throws IOException {
                try {
                    s.close();
                } finally {
                    // Always close gbCache and swallow possible Exceptions
                    Closeables.closeQuietly(SpillableGroupByCache.this);
                }
            }

            @Override
            public boolean next(List<Cell> results) throws IOException {
                if (!cacheIter.hasNext()) { return false; }
                Map.Entry<ImmutableBytesWritable, Aggregator[]> ce = cacheIter.next();
                ImmutableBytesWritable key = ce.getKey();
                Aggregator[] aggs = ce.getValue();
                byte[] value = aggregators.toBytes(aggs);
                if (logger.isDebugEnabled()) {
                    logger.debug("Adding new distinct group: "
                            + Bytes.toStringBinary(key.get(), key.getOffset(), key.getLength()) + " with aggregators "
                            + aggs.toString() + " value = " + Bytes.toStringBinary(value));
                }
                results.add(KeyValueUtil.newKeyValue(key.get(), key.getOffset(), key.getLength(), SINGLE_COLUMN_FAMILY,
                        SINGLE_COLUMN, AGG_TIMESTAMP, value, 0, value.length));
                return cacheIter.hasNext();
            }

            @Override
            public long getMaxResultSize() {
              return s.getMaxResultSize();
            }
        };
    }
}