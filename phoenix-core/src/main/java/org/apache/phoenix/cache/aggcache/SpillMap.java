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

import java.io.IOException;
import java.nio.BufferOverflowException;
import java.nio.MappedByteBuffer;
import java.util.AbstractMap;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;

import org.apache.hadoop.hbase.util.Bytes;

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.hash.BloomFilter;
import com.google.common.hash.Funnels;

import org.apache.phoenix.hbase.index.util.ImmutableBytesPtr;

/**
 * Class implements an active spilled partition serialized tuples are first written into an in-memory data structure
 * that represents a single page. As the page fills up, it is written to the current spillFile or spill partition For
 * fast tuple discovery, the class maintains a per page bloom-filter and never de-serializes elements. The element
 * spilling employs an extentible hashing technique.
 */
public class SpillMap extends AbstractMap<ImmutableBytesPtr, byte[]> implements Iterable<byte[]> {

    // Threshold is typically the page size
    private final int thresholdBytes;
    private final int pageInserts;
    // Global directory depth
    private int globalDepth;
    private int curMapBufferIndex;
    private SpillFile spillFile;
    // Directory of hash buckets --> extendible hashing implementation
    private MappedByteBufferMap[] directory;
    private final SpillableGroupByCache.QueryCache cache;

    public SpillMap(SpillFile file, int thresholdBytes, int estValueSize, SpillableGroupByCache.QueryCache cache)
            throws IOException {
        this.thresholdBytes = thresholdBytes - Bytes.SIZEOF_INT;
        this.pageInserts = thresholdBytes / estValueSize;
        this.spillFile = file;
        this.cache = cache;

        // Init the e-hashing directory structure
        globalDepth = 1;
        directory = new MappedByteBufferMap[(1 << globalDepth)];

        for (int i = 0; i < directory.length; i++) {
            // Create an empty bucket list
            directory[i] = new MappedByteBufferMap(i, this.thresholdBytes, pageInserts, file);
            directory[i].flushBuffer();
        }
        directory[0].pageIn();
        curMapBufferIndex = 0;
    }

    // Get the directoy index for a specific key
    private int getBucketIndex(ImmutableBytesPtr key) {
        // Get key hash
        int hashCode = key.hashCode();

        // Mask all but globalDepth low n bits
        return hashCode & ((1 << globalDepth) - 1);
    }

    // Function redistributes the elements in the current index
    // to two new buckets, based on the bit at localDepth + 1 position.
    // Optionally this function also doubles the directory to allow
    // for bucket splits
    private void redistribute(int index, ImmutableBytesPtr keyNew, byte[] valueNew) {
        // Get the respective bucket
        MappedByteBufferMap byteMap = directory[index];

        // Get the actual bucket index, that the directory index points to
        int mappedIdx = byteMap.pageIndex;

        int localDepth = byteMap.localDepth;
        ArrayList<Integer> buckets = Lists.newArrayList();
        // Get all directory entries that point to the same bucket.
        // TODO: can be made faster!
        for (int i = 0; i < directory.length; i++) {
            if (directory[i].pageIndex == mappedIdx) {
                buckets.add(i);
            }
        }

        // Assuming no directory doubling for now
        // compute the two new bucket Ids for splitting
        // SpillFile adds new files dynamically in case the directory points to pageIDs
        // that exceed the size limit of a single file.

        // TODO verify if some sort of de-fragmentation might be helpful
        int tmpIndex = index ^ ((1 << localDepth));
        int b1Index = Math.min(index, tmpIndex);
        int b2Index = Math.max(index, tmpIndex);

        // Create two new split buckets
        MappedByteBufferMap b1 = new MappedByteBufferMap(b1Index, thresholdBytes, pageInserts, spillFile);
        MappedByteBufferMap b2 = new MappedByteBufferMap(b2Index, thresholdBytes, pageInserts, spillFile);

        // redistribute old elements into b1 and b2
        for (Entry<ImmutableBytesPtr, byte[]> element : byteMap.pageMap.entrySet()) {
            ImmutableBytesPtr key = element.getKey();
            byte[] value = element.getValue();
            // Only add key during redistribution if its not in the cache
            // Otherwise this is an good point to reduce the number of spilled elements
            if (!cache.isKeyContained(key)) {
                // Re-distribute element onto the new 2 split buckets
                if ((key.hashCode() & ((1 << localDepth))) != 0) {
                    b2.addElement(null, key, value);
                } else {
                    b1.addElement(null, key, value);
                }
            }
        }

        // Clear and GC the old now redistributed bucket
        byteMap.pageMap.clear();
        byteMap = null;

        // Increase local bucket depths
        b1.localDepth = localDepth + 1;
        b2.localDepth = localDepth + 1;
        boolean doubleDir = false;

        if (globalDepth < (localDepth + 1)) {
            // Double directory structure and re-adjust pointers
            doubleDir = true;

            b2Index = doubleDirectory(b2Index, keyNew);
        }

        if (!doubleDir) {
            // This is a bit more tricky, we have to cover scenarios where
            // globalDepth - localDepth > 1
            // Here even after bucket splitting, multiple directory entries point to
            // the new buckets
            for (int i = 0; i < buckets.size(); i++) {
                if ((buckets.get(i) & (1 << (localDepth))) != 0) {
                    directory[buckets.get(i)] = b2;
                } else {
                    directory[buckets.get(i)] = b1;
                }
            }
        } else {
            // Update the directory indexes in case of directory doubling
            directory[b1Index] = b1;
            directory[b2Index] = b2;
        }
    }

    // Doubles the directory and readjusts pointers.
    private int doubleDirectory(int b2Index, ImmutableBytesPtr keyNew) {
        // Double the directory in size, second half points to original first half
        int newDirSize = 1 << (globalDepth + 1);

        // Ensure that the new directory size does not exceed size limits
        Preconditions.checkArgument(newDirSize < Integer.MAX_VALUE);

        // Double it!
        MappedByteBufferMap[] newDirectory = new MappedByteBufferMap[newDirSize];
        for (int i = 0; i < directory.length; i++) {
            newDirectory[i] = directory[i];
            newDirectory[i + directory.length] = directory[i];
        }
        directory = newDirectory;
        newDirectory = null;

        // Adjust the index for new split bucket, according to the directory double
        b2Index = (keyNew.hashCode() & ((1 << globalDepth) - 1)) | (1 << globalDepth);

        // Increment global depth
        globalDepth++;

        return b2Index;
    }

    /**
     * Get a key from the spillable data structures. page is determined via hash partitioning, and a bloomFilter check
     * is used to determine if its worth paging in the data.
     */
    @Override
    public byte[] get(Object key) {
        if (!(key instanceof ImmutableBytesPtr)) {
            // TODO ... work on type safety
        }
        ImmutableBytesPtr ikey = (ImmutableBytesPtr)key;
        byte[] value = null;

        int bucketIndex = getBucketIndex(ikey);
        MappedByteBufferMap byteMap = directory[bucketIndex];

        // Decision based on bucket ID, not the directory ID due to the n:1 relationship
        if (directory[curMapBufferIndex].pageIndex != byteMap.pageIndex) {
            // map not paged in
            MappedByteBufferMap curByteMap = directory[curMapBufferIndex];

            // Use bloomFilter to check if key was spilled before
            if (byteMap.containsKey(ikey.copyBytesIfNecessary())) {
                // ensure consistency and flush current memory page to disk
                // fflush current buffer
                curByteMap.flushBuffer();
                // page in new buffer
                byteMap.pageIn();
                // update index
                curMapBufferIndex = bucketIndex;
            }
        }
        // get KV from current map
        value = byteMap.getPagedInElement(ikey);
        return value;
    }

    // Similar as get(Object key) function, however
    // always pages in page a key is spilled to, no bloom filter decision
    private byte[] getAlways(ImmutableBytesPtr key) {
        byte[] value = null;
        int bucketIndex = getBucketIndex(key);
        MappedByteBufferMap byteMap = directory[bucketIndex];

        if (directory[curMapBufferIndex].pageIndex != byteMap.pageIndex) {
            MappedByteBufferMap curByteMap = directory[curMapBufferIndex];
            // ensure consistency and flush current memory page to disk
            curByteMap.flushBuffer();

            byteMap.pageIn();
            curMapBufferIndex = bucketIndex;
        }
        // get KV from current queue
        value = byteMap.getPagedInElement(key);
        return value;
    }

    /**
     * Spill a key First we discover if the key has been spilled before and load it into memory: #ref get() if it was
     * loaded before just replace the old value in the memory page if it was not loaded before try to store it in the
     * current page alternatively if not enough memory available, request new page.
     */
    @Override
    public byte[] put(ImmutableBytesPtr key, byte[] value) {
        boolean redistributed = false;
        // page in element and replace if present
        byte[] spilledValue = getAlways(key);

        MappedByteBufferMap byteMap = directory[curMapBufferIndex];
        int index = curMapBufferIndex;

        // TODO: We split buckets until the new element fits onto a
        // one of the new buckets. Might consider the use of an overflow
        // bucket, especially in case the directory runs out of page IDs.
        while (!byteMap.canFit(spilledValue, value)) {
            // Element does not fit... Split the bucket!
            redistribute(index, key, value);
            redistributed = true;

            index = getBucketIndex(key);
            byteMap = directory[index];
        }
        // Ensure that all pages that were paged in during redistribution are flushed back out
        // to disk to keep memory footprint small.
        if (redistributed) {
            for (int i = 0; i < directory.length; i++) {
                if (directory[i].pageIndex != byteMap.pageIndex) {
                    directory[i].flushBuffer();
                }
            }
            // Ensure the page that receives the new key is in memory
            spilledValue = getAlways(key);
        }
        byteMap.addElement(spilledValue, key, value);

        return value;
    }

    /**
     * Function returns the current spill file
     */
    public SpillFile getSpillFile() {
        return spillFile;
    }

    /**
     * This inner class represents the currently mapped file region. It uses a Map to represent the current in memory
     * page for easy get() and update() calls on an individual key The class keeps track of the current size of the in
     * memory page and handles flushing and paging in respectively
     */
    private static class MappedByteBufferMap {
        private SpillFile spillFile;
        private int pageIndex;
        private final int thresholdBytes;
        private long totalResultSize;
        private boolean pagedIn;
        private int localDepth;
        // dirtyPage flag tracks if a paged in page was modified
        // if not, no need to flush it back out to disk
        private boolean dirtyPage;
        // Use a map for in memory page representation
        Map<ImmutableBytesPtr, byte[]> pageMap = Maps.newHashMap();
        // Used to determine is an element was written to this page before or not
        BloomFilter<byte[]> bFilter;

        public MappedByteBufferMap(int id, int thresholdBytes, int pageInserts, SpillFile spillFile) {
            this.spillFile = spillFile;
            // size threshold of a page
            this.thresholdBytes = thresholdBytes;
            this.pageIndex = id;
            pageMap.clear();
            bFilter = BloomFilter.create(Funnels.byteArrayFunnel(), pageInserts);
            pagedIn = true;
            totalResultSize = 0;
            localDepth = 1;
            dirtyPage = true;
        }

        private boolean containsKey(byte[] key) {
            return bFilter.mightContain(key);
        }

        private boolean canFit(byte[] curValue, byte[] newValue) {
            if (thresholdBytes < newValue.length) {
                // TODO resize page size if single element is too big,
                // Can this ever happen?
                throw new RuntimeException("page size too small to store a single KV element");
            }

            int resultSize = newValue.length + Bytes.SIZEOF_INT;
            if (curValue != null) {
                // Key existed before
                // Ensure to compensate for potential larger byte[] for agg
                resultSize = Math.max(0, resultSize - (curValue.length + Bytes.SIZEOF_INT));
            }

            if ((thresholdBytes - totalResultSize) <= (resultSize)) {
                // KV does not fit
                return false;
            }
            // KV fits
            return true;
        }

        // Flush the current page to the memory mapped byte buffer
        private void flushBuffer() throws BufferOverflowException {
            if (pagedIn) {
                MappedByteBuffer buffer;
                // Only flush if page was changed
                if (dirtyPage) {
                    Collection<byte[]> values = pageMap.values();
                    buffer = spillFile.getPage(pageIndex);
                    buffer.clear();
                    // number of elements
                    buffer.putInt(values.size());
                    for (byte[] value : values) {
                        // element length
                        buffer.putInt(value.length);
                        // element
                        buffer.put(value, 0, value.length);
                    }
                }
                buffer = null;
                // Reset page stats
                pageMap.clear();
                totalResultSize = 0;
            }
            pagedIn = false;
            dirtyPage = false;
        }

        // load memory mapped region into a map for fast element access
        private void pageIn() throws IndexOutOfBoundsException {
            if (!pagedIn) {
                // Map the memory region
                MappedByteBuffer buffer = spillFile.getPage(pageIndex);
                int numElements = buffer.getInt();
                for (int i = 0; i < numElements; i++) {
                    int kvSize = buffer.getInt();
                    byte[] data = new byte[kvSize];
                    buffer.get(data, 0, kvSize);
                    try {
                        pageMap.put(SpillManager.getKey(data), data);
                        totalResultSize += (data.length + Bytes.SIZEOF_INT);
                    } catch (IOException ioe) {
                        // Error during key access on spilled resource
                        // TODO rework error handling
                        throw new RuntimeException(ioe);
                    }
                }
                pagedIn = true;
                dirtyPage = false;
            }
        }

        /**
         * Return a cache element currently page into memory Direct access via mapped page map
         * 
         * @param key
         * @return
         */
        public byte[] getPagedInElement(ImmutableBytesPtr key) {
            return pageMap.get(key);
        }

        /**
         * Inserts / Replaces cache element in the currently loaded page. Direct access via mapped page map
         * 
         * @param key
         * @param value
         */
        public void addElement(byte[] spilledValue, ImmutableBytesPtr key, byte[] value) {

            // put Element into map
            pageMap.put(key, value);
            // Update bloom filter
            bFilter.put(key.copyBytesIfNecessary());
            // track current Map size to prevent Buffer overflows
            if (spilledValue != null) {
                // if previous key was present, just add the size difference
                totalResultSize += Math.max(0, value.length - (spilledValue.length));
            } else {
                // Add new size information
                totalResultSize += (value.length + Bytes.SIZEOF_INT);
            }

            dirtyPage = true;
        }

        /**
         * Returns a value iterator over the pageMap
         */
        public Iterator<byte[]> getPageMapEntries() {
            pageIn();
            return pageMap.values().iterator();
        }
    }

    /**
     * Iterate over all spilled elements, including the ones that are currently paged into memory
     */
    @Override
    public Iterator<byte[]> iterator() {
        directory[curMapBufferIndex].flushBuffer();

        return new Iterator<byte[]>() {
            int pageIndex = 0;
            Iterator<byte[]> entriesIter = directory[pageIndex].getPageMapEntries();
            HashSet<Integer> dups = new HashSet<Integer>();

            @Override
            public boolean hasNext() {
                if (!entriesIter.hasNext()) {
                    boolean found = false;
                    // Clear in memory map

                    while (!found) {
                        pageIndex++;
                        if (pageIndex >= directory.length) { return false; }
                        directory[pageIndex - 1].pageMap.clear();
                        // get keys from all spilled pages
                        if (!dups.contains(directory[pageIndex].pageIndex)) {
                            dups.add(directory[pageIndex].pageIndex);
                            entriesIter = directory[pageIndex].getPageMapEntries();
                            if (entriesIter.hasNext()) {
                                found = true;
                            }
                        }
                    }
                }
                dups.add(directory[pageIndex].pageIndex);
                return true;
            }

            @Override
            public byte[] next() {
                // get elements from in memory map first
                return entriesIter.next();
            }

            @Override
            public void remove() {
                throw new IllegalAccessError("Iterator does not support removal operation");
            }
        };
    }

    // TODO implement this method to make the SpillMap a true Map implementation
    @Override
    public Set<java.util.Map.Entry<ImmutableBytesPtr, byte[]>> entrySet() {
        throw new IllegalAccessError("entrySet is not supported for this type of cache");
    }
}
